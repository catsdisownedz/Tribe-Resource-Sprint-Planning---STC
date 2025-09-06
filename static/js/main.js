function $(id){ return document.getElementById(id); }
function qs(sel, root=document){ return root.querySelector(sel); }
function qsa(sel, root=document){ return Array.from(root.querySelectorAll(sel)); }

let _loadingAssignments = false; // prevent overlapping loads

async function fetchJSON(url){
  const r = await fetch(url);
  const t = await r.text();
  if (!r.ok) { throw new Error(t.slice(0, 400)); }
  try { return JSON.parse(t); } catch { throw new Error(t.slice(0, 400)); }
}

function getFilters(){
  return {
    tribe: $("fTribe").value.trim(),
    app: $("fApp").value.trim(),
    resource: $("fResource").value.trim(),
    role: $("fRole").value.trim(),
    type: $("fType").value.trim(),
  };
}

function toQS(obj){
  const p = new URLSearchParams();
  for (const [k,v] of Object.entries(obj)) if (v) p.set(k,v);
  const s = p.toString();
  return s ? ("?" + s) : "";
}

function sprintsToText(row){
  const on = [];
  for (let i=1;i<=6;i++) if (row[`s${i}`]) on.push(`S${i}`);
  return on.length ? on.join(", ") : "—";
}

// --- FAST local availability (no network) ---
// Caches populated once and refreshed with loadAssignments()
const _ASG = { rows: [], byResRole: new Map() }; // key: `${resource}::${row.role||""}`
const _CAP = new Map(); // key: `${tribe}::${resource}::${role||""}` -> { reserved, type }

function _keyResRole(name, role){ return `${name}::${role||""}`; }
function _keyCap(tribe, name, role){ return `${tribe}::${name}::${role||""}`; }

async function warmAvailabilityCaches(){
  // We already call /api/assignments in loadAssignments(); just make sure we also load temp caps once.
  // Safe to call multiple times; it just refreshes the maps.
  try {
    const temps = await fetchJSON("/api/temp-assignments");
    _CAP.clear();
    for (const t of (temps.items || [])){
      _CAP.set(_keyCap(t.tribe, t.resource_name, t.role), { reserved: Number(t.reserved||0), type: t.type || "Shared" });
    }
  } catch {}
}

// Compute what /api/availability would return, using cached assignments+temps
function availabilityFromCaches(row){
  const resKey = _keyResRole(row.resource_name, row.role);
  const list = _ASG.byResRole.get(resKey) || [];

  // blocked by OTHER tribes on this resource
  const blocked = [0,0,0,0,0,0];
  // sprints owned by THIS tribe on this resource
  const mine = [0,0,0,0,0,0];
  let booked_by_tribe = 0;

  for (const r of list){
    const isMine = (r.tribe_name === row.tribe_name);
    for (let i=1;i<=6;i++){
      const on = !!r[`s${i}`];
      if (!isMine && on) blocked[i-1] = 1;
      if (isMine && on) { mine[i-1] = 1; booked_by_tribe++; }
    }
  }

  // cap per tribe from temp_assignments (fallbacks keep UX snappy)
  const capInfo = _CAP.get(_keyCap(row.tribe_name, row.resource_name, row.role)) || {};
  const assign_type = capInfo.type || row.assignment_type || "Shared";
  const cap_per_tribe = assign_type === "Dedicated" ? 6 : Number(capInfo.reserved || 6);

  return { blocked, mine, cap_per_tribe, booked_by_tribe, assign_type };
}

function createSprintCell(row){
  const td = document.createElement("td");
  td.dataset.mode = "view";
  td.textContent = sprintsToText(row);
  return td;
}

// ---- availability fetcher with simple cache (kept for background refresh only) ----
const _availCache = new Map(); // key = `${row.tribe_name}::${row.resource_name}::${row.role||""}`
async function fetchAvailabilityForRow(row){
  const key = `${row.tribe_name}::${row.resource_name}::${row.role||""}`;
  if (_availCache.has(key)) return _availCache.get(key);
  const params = new URLSearchParams({
    tribe: row.tribe_name,
    resource_name: row.resource_name,
    role: row.role || ""
  });
  const r = await fetch(`/api/availability?${params.toString()}`);
  const t = await r.text();
  let j; try { j = JSON.parse(t); } catch { j = { error: t }; }
  if(!r.ok) throw new Error(j.error || "failed to fetch availability");
  _availCache.set(key, j);
  return j;
}

function createActionCell(row, tr, tdS){
  const tdAct = document.createElement("td");
  const btn = document.createElement("button");
  btn.className = "btn btn-sm btn-outline-primary";
  btn.textContent = "Edit";

  btn.addEventListener("click", async () => {
    // prevent double-activations while we’re preparing edit UI
    if (btn.dataset.busy === "1") return;
  
    if (tdS.dataset.mode === "view") {
      btn.dataset.busy = "1";          // lock
      btn.disabled = true;              // optional UX
      btn.textContent = "…";            // optional UX

      // INSTANT: compute availability locally (no network)
      const avail = availabilityFromCaches(row);
      const blocked = avail.blocked;
      const mine    = avail.mine;
      const capPerTribe   = avail.cap_per_tribe;
      const bookedByTribe = avail.booked_by_tribe;

      // (OPTIONAL) also kick off a background refresh to keep data fresh,
      // but DO NOT await it (so UI stays instant)
      fetchAvailabilityForRow(row).catch(()=>{ /* ignore */ });
  
      const wrap = document.createElement("div");
      wrap.className = "d-flex align-items-center flex-wrap gap-2";
  
      let initialChecked = 0;
      for (let i=1;i<=6;i++){ if (row[`s${i}`]) initialChecked++; }
      const baseUsed = Math.max(0, bookedByTribe - initialChecked);
  
      for (let i=1;i<=6;i++){
        const chk = document.createElement("input");
        chk.type = "checkbox";
        chk.className = "form-check-input me-1";
        chk.id = `s${i}-${row.id}`;
        chk.checked = !!row[`s${i}`];
  
        const isBlocked = !!blocked[i-1];
        const isMine    = !!mine[i-1];
        chk.disabled = isBlocked && !isMine && !chk.checked;
  
        if (chk.disabled) chk.title = "This sprint is already booked by another tribe";
  
        chk.addEventListener("change", () => {
          const checkedCount = Array.from(wrap.querySelectorAll('input[type="checkbox"]')).filter(x => x.checked).length;
          const totalAfter = baseUsed + checkedCount;
          if (totalAfter > capPerTribe) {
            chk.checked = !chk.checked;
            alert(`You can book at most ${capPerTribe} sprint(s) for this resource.`);
          }
        });
  
        const lab = document.createElement("label");
        lab.className = "form-check-label me-2" + (chk.disabled ? " opacity-50" : "");
        lab.htmlFor = chk.id;
        lab.textContent = `S${i}`;
        wrap.appendChild(chk); wrap.appendChild(lab);
      }
  
      tdS.innerHTML = "";
      tdS.appendChild(wrap);
  
      // now that inputs exist, flip to edit mode and unlock the button
      tdS.dataset.mode = "edit";
      btn.textContent = "Save";
      btn.disabled = false;
      btn.dataset.busy = "0";  
    } else {
      // SAVE branch
      const probe = document.getElementById(`s1-${row.id}`);
      if (!probe) return; // UI not mounted yet; ignore accidental save

      const body = {};
      for (let i=1;i<=6;i++){
        const el = document.getElementById(`s${i}-${row.id}`);
        body[`s${i}`] = !!(el && el.checked);
      }
      try {
        const r = await fetch(`/api/assignments/${row.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });
        const text = await r.text();
        let j; try { j = JSON.parse(text); } catch { j = { error: text }; }
        if (!r.ok) throw new Error(j.error || "failed");

        // reflect in UI
        for (let i=1;i<=6;i++) row[`s${i}`] = body[`s${i}`];
        tdS.textContent = sprintsToText(row);
        tdS.dataset.mode = "view";
        btn.textContent = "Edit";
        const note = tr.querySelector(".edited-note");
        if (note) note.textContent = `edited at ${new Date().toLocaleString()}`;

        // refresh caches & table so instant-availability stays correct
        loadAssignments();
      } catch (err) {
        alert(String(err.message || err));
      }
    }
  });

  tdAct.appendChild(btn);
  return tdAct;
}

function buildRow(row){
  const tr = document.createElement("tr");
  tr.dataset.id = String(row.id);

  for (const key of ["tribe_name","app_name","role","resource_name","assignment_type"]){
    const td = document.createElement("td");
    td.textContent = row[key];
    tr.appendChild(td);
  }

  const tdS = createSprintCell(row);
  tr.appendChild(tdS);

  const tdAct = createActionCell(row, tr, tdS);
  tr.appendChild(tdAct);

  const tdNote = document.createElement("td");
  tdNote.className = "text-end edited-note";
  tdNote.textContent = row.edited ? `edited at ${new Date(row.updated_at || Date.now()).toLocaleString()}` : "";
  tr.appendChild(tdNote);

  tr.dataset.key = `${row.tribe_name}__${row.app_name}__${row.resource_name}__${row.role}`;
  return tr;
}

async function loadAssignments(){
  if (_loadingAssignments) return; // don't overlap
  _loadingAssignments = true;
  try {
    const tbody = $("assignBody");
    const latest = await fetchJSON("/api/assignments" + toQS(getFilters()));

    // --- refresh local caches for instant edit mode ---
    _ASG.rows = latest;
    _ASG.byResRole.clear();
    for (const r of latest){
      const k = _keyResRole(r.resource_name, r.role);
      if (!_ASG.byResRole.has(k)) _ASG.byResRole.set(k, []);
      _ASG.byResRole.get(k).push(r);
    }
    
    // index existing
    const existing = new Map();
    tbody.querySelectorAll("tr[data-id]").forEach(tr => existing.set(tr.dataset.id, tr));

    // add/update
    const frag = document.createDocumentFragment();
    for (const row of latest){
      let tr = existing.get(String(row.id));
      if (tr){
        // update text if needed
        const cells = tr.querySelectorAll("td");
        const values = [row.tribe_name,row.app_name,row.role,row.resource_name,row.assignment_type];
        for (let i=0;i<5;i++){ if (cells[i] && cells[i].textContent !== values[i]) cells[i].textContent = values[i]; }
        const tdS = cells[5];
        if (tdS && tdS.dataset.mode === "view") tdS.textContent = sprintsToText(row);
        const note = cells[7];
        if (note) note.textContent = row.edited ? `edited at ${new Date(row.updated_at || Date.now()).toLocaleString()}` : "";
        existing.delete(String(row.id));
      } else {
        frag.appendChild(buildRow(row));
      }
    }

    // remove stale
    for (const [id, tr] of existing.entries()) tr.remove();

    // append new ones
    tbody.appendChild(frag);
  } finally {
    _loadingAssignments = false;
  }
}

function initView(){
  ["fTribe","fApp","fRole","fResource","fType"].forEach(id => {
    const el = $(id); if(el) el.addEventListener("input", loadAssignments);
  });
  const exportBtn = $("exportBtn");
  if(exportBtn){
    exportBtn.addEventListener("click", () => {
      const url = "/api/export" + toQS(getFilters());
      window.location = url;
    });
  }
  loadAssignments();
  setInterval(loadAssignments, 30000); // lighter polling; we refresh on edits anyway
}

document.addEventListener("DOMContentLoaded", () => {
  initView();  // initial load

  warmAvailabilityCaches(); 
  // When user switches tabs, trigger a fresh fetch
  document.getElementById('view-tab')?.addEventListener('shown.bs.tab', () => {
    if (typeof loadAssignments === 'function') loadAssignments();
  });
  document.getElementById('book-tab')?.addEventListener('shown.bs.tab', () => {
    if (window.loadTempAssignments) window.loadTempAssignments();
  });

  // Ensure initial render without having to switch tabs
  window.loadTempAssignments?.();

  // If booking page set a flag, reload master assignments immediately
  if (sessionStorage.getItem('reloadAssignments') === '1') {
    sessionStorage.removeItem('reloadAssignments');
    if (typeof loadAssignments === 'function') loadAssignments();
  }
});
