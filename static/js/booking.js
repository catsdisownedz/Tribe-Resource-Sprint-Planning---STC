(function(){
  // ----- Cache DOM -----
  const els = {
    tribe:     document.getElementById('bTribe'),
    type:      document.getElementById('bType'),
    app:       document.getElementById('bApp'),
    resource:  document.getElementById('bResource'),
    role:      document.getElementById('bRole'),
    tableBody: document.getElementById('tempBody'),
    ok:        document.getElementById('selectOk')
  };
  if (!els.tableBody) return; // not on this page

  // ----- Small helpers -----
  const debounce = (fn, ms=250) => {
    let t = null;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  };

  const buildQuery = () => {
    const p = new URLSearchParams();
    if (els.tribe?.value.trim())    p.set('tribe', els.tribe.value.trim());
    if (els.type?.value.trim())     p.set('type', els.type.value.trim());
    if (els.app?.value.trim())      p.set('app', els.app.value.trim());
    if (els.resource?.value.trim()) p.set('resource', els.resource.value.trim());
    if (els.role?.value.trim())     p.set('role', els.role.value.trim());
    return p.toString();
  };

  const setLoading = (isLoading) => {
    if (isLoading) {
      els.tableBody.innerHTML = `<tr><td colspan="7" class="text-center py-3">Loading…</td></tr>`;
    }
  };

  const renderRows = (items=[]) => {
    if (!Array.isArray(items) || items.length === 0) {
      els.tableBody.innerHTML = `<tr><td colspan="7" class="text-center py-3 text-muted">No matching rows</td></tr>`;
      els.ok?.setAttribute('disabled', 'true');
      return;
    }

    const rowsHtml = items.map(r => {
      const reserved = Number(r.reserved ?? r.reserved_sprints ?? 0);
      return `
        <tr data-id="${r.temp_id}">
          <td>${r.tribe ?? r.tribe_name ?? ''}</td>
          <td>${r.type ?? r.assign_type ?? ''}</td>
          <td>${r.app ?? r.app_name ?? ''}</td>
          <td>${r.resource_name ?? ''}</td>
          <td>${r.role ?? ''}</td>
          <td class="text-center">${reserved}</td>
          <td class="text-end">
            <button class="btn btn-sm btn-outline-primary act-select">Select</button>
          </td>
        </tr>
      `;
    }).join('');
    els.tableBody.innerHTML = rowsHtml;
    els.ok?.setAttribute('disabled', 'true'); // reset until a row is chosen
  };

  // ----- Load function (public) -----
  async function load({source} = {}) {
    setLoading(true);
    const q = buildQuery();
    const url = q ? `/api/temp-assignments?${q}` : `/api/temp-assignments`;
    try {
      const resp = await fetch(url);
      const text = await resp.text();
      if (!resp.ok) throw new Error(text);
      const json = JSON.parse(text);
      renderRows(json.items || []);
    } catch (err) {
      els.tableBody.innerHTML = `<tr><td colspan="7" class="text-danger py-3">Failed to load: ${String(err).slice(0,200)}</td></tr>`;
    }
  }

  // expose to other scripts (main.js calls this on tab show)
  window.loadTempAssignments = load;

  // ----- Events -----
  const onFilterInput = debounce(() => load({source: 'filter'}), 300);
  [els.tribe, els.type, els.app, els.resource, els.role].forEach(el => {
    el?.addEventListener('input', onFilterInput);
  });

  // row click → select
  els.tableBody.addEventListener('click', (e) => {
    const tr = e.target.closest('tr');
    if (!tr) return;

    // button click navigates immediately
    if (e.target.classList.contains('act-select')) {
      const id = tr.getAttribute('data-id');
      if (id) window.location.href = `/booking/${id}`;
      return;
    }

    // otherwise just mark selection
    els.tableBody.querySelectorAll('tr').forEach(r => r.classList.remove('table-active'));
    tr.classList.add('table-active');
    els.ok?.removeAttribute('disabled');
  });

  // double click row → navigate
  els.tableBody.addEventListener('dblclick', (e) => {
    const tr = e.target.closest('tr');
    if (!tr) return;
    const id = tr.getAttribute('data-id');
    if (id) window.location.href = `/booking/${id}`;
  });

  // OK button
  els.ok?.addEventListener('click', () => {
    const tr = els.tableBody.querySelector('tr.table-active');
    if (!tr) return;
    const id = tr.getAttribute('data-id');
    if (id) window.location.href = `/booking/${id}`;
  });

  // ----- Initial load (so the first tab shows data immediately) -----
  // This ensures you DON'T need to switch tabs 2–3 times.
  load({source: 'dom-ready'});
})();

