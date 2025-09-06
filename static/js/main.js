// static/js/main.js
function $(id){ return document.getElementById(id); }
function qs(sel, root=document){ return root.querySelector(sel); }
function qsa(sel, root=document){ return Array.from(root.querySelectorAll(sel)); }

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

function createSprintCell(row){
  const td = document.createElement("td");
  td.dataset.mode = "view";
  td.textContent = sprintsToText(row);
  return td;
}

// ---- NEW: availability fetcher for edit mode ----
// ---- availability fetcher with simple cache ----
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
    if (tdS.dataset.mode === "view") {
      // Switch to edit mode
      tdS.dataset.mode = "edit";

      // Load availability for this row (tribe + resource)
      let availability;
      try {
        availability = await fetchAvailabilityForRow(row);
      } catch (e) {
        alert(String(e.message || e));
        tdS.dataset.mode = "view";
        return;
      }

      const blocked = availability.blocked || [0,0,0,0,0,0];
      const mine    = availability.mine    || [0,0,0,0,0,0];
      const capPerTribe   = availability.cap_per_tribe ?? 6;
      const bookedByTribe = availability.booked_by_tribe ?? 0;
      

      const wrap = document.createElement("div");
      wrap.className = "d-flex align-items-center flex-wrap gap-2";

      // initial count in this specific row
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
        // Allow if sprint is ours (across any row) or already checked here.
        // Only block if another tribe holds it.
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
      btn.textContent = "Save";
    } else {
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
  const tbody = $("assignBody");
  const latest = await fetchJSON("/api/assignments" + toQS(getFilters()));

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
  setInterval(loadAssignments, 10000);
}

document.addEventListener("DOMContentLoaded", () => {
  initView();  // initial load

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

