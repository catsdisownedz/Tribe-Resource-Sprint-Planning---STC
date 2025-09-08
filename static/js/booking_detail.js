// static/js/booking_detail.js
(function () {
  const boxes = Array.from(document.querySelectorAll('input[type="checkbox"][data-sprint]'));
  if (!boxes.length) return;

  const bookBtn   = document.getElementById('btn-book');
  const counterEl = document.getElementById('reservedCounter');

  const BOOKED = Array.isArray(window.BOOKED_SPRINTS) ? window.BOOKED_SPRINTS.map(Number) : [];
  const TEMP_ID = Number(window.TEMP_ID ?? 0);
  const RESERVED_LIMIT = Number(window.RESERVED_LIMIT ?? 0);
  const BOOKED_BY_ME = Number(window.BOOKED_BY_TRIBE ?? 0);

  const REMAINING = RESERVED_LIMIT > 0 ? Math.max(0, RESERVED_LIMIT - BOOKED_BY_ME) : 6;
  const CAP = REMAINING > 0 ? REMAINING : 0;
  const bookedSet = new Set(BOOKED);


  function ensureNoticeHost(){
    let host = document.getElementById('notices');
    if (!host){
      host = document.createElement('div');
      host.id = 'notices';
      host.className = 'position-fixed top-0 end-0 p-3';
      host.style.zIndex = '2000';
      document.body.appendChild(host);
    }
    return host;
  }
  function notify(type, message, {timeout=3000, dismissible=true} = {}){
    const host = ensureNoticeHost();
    const el = document.createElement('div');
    el.className = `alert alert-${type} ${dismissible ? 'alert-dismissible' : ''} fade show shadow`;
    el.role = 'alert';
    el.innerHTML = message;
    if (dismissible){
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'btn-close';
      btn.setAttribute('data-bs-dismiss','alert');
      btn.ariaLabel = 'Close';
      el.appendChild(btn);
    }
    host.appendChild(el);
    if (timeout > 0){
      setTimeout(() => {
        el.classList.remove('show');
        setTimeout(() => el.remove(), 150);
      }, timeout);
    }
    return el;
  }
  
  function selectedCount() {
    return boxes.filter(b => b.checked).length;
  }
  function setDisabled(box, shouldDisable, reason = '') {
    box.disabled = !!shouldDisable;
    const label = box.closest('label') || box.parentElement;
    if (label) {
      if (shouldDisable) {
        label.classList.add('opacity-50');
        if (reason) label.title = reason;
      } else {
        label.classList.remove('opacity-50');
        label.removeAttribute('title');
      }
    }
  }

  boxes.forEach(box => {
    const sprint = Number(box.getAttribute('data-sprint'));
    if (bookedSet.has(sprint)) {
      setDisabled(box, true, 'Already booked');
    }
  });

  function refreshCap() {
    const pickedNow = selectedCount();
    const totalIfSaved = BOOKED_BY_ME + pickedNow;
    if (counterEl) counterEl.textContent =
      `${totalIfSaved} / ${RESERVED_LIMIT}${RESERVED_LIMIT === 0 ? ' (no cap)' : ''}`;

    boxes.forEach(box => {
      const sprint = Number(box.getAttribute('data-sprint'));
      if (bookedSet.has(sprint)) return;
      if (pickedNow >= CAP && !box.checked) {
        setDisabled(box, true, 'Reached reserved limit');
      } else {
        setDisabled(box, false);
      }
    });
  }
  boxes.forEach(b => b.addEventListener('change', refreshCap));
  refreshCap();

  if (bookBtn) {
    bookBtn.addEventListener('click', async () => {
      const chosen = boxes.filter(b => b.checked).map(b => Number(b.getAttribute('data-sprint')));
      if (chosen.length === 0) {
        notify('warning', 'Pick at least one sprint.');
        return;
      }
  
      // Start saving state UI
      const labels = Array.from(document.querySelectorAll('label.sprint'));
      bookBtn.disabled = true;
      bookBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Booking…`;
      labels.forEach(l => l.classList.add('opacity-50','pe-none'));
  
      try {
        const res = await fetch(`/api/book-temp/${TEMP_ID}`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ sprints: chosen })
        });
  
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          const msg = data?.error || (Array.isArray(data?.details) ? data.details.join('<br>') : '') || `Booking failed (${res.status})`;
          notify('danger', msg || 'Booking failed.');
          // restore UI
          bookBtn.disabled = false;
          bookBtn.textContent = 'Book';
          labels.forEach(l => l.classList.remove('opacity-50','pe-none'));
          return;
        }
  
        // Success: show the strong green banner (no X), then redirect
        document.dispatchEvent(new CustomEvent('booking-success'));
        sessionStorage.setItem('reloadAssignments', '1');
  
        // keep spinner while we wait briefly, then go home
        setTimeout(() => { window.location.href = '/'; }, 1200);
      } catch (e) {
        console.error(e);
        notify('danger', 'Network error while booking.');
        // restore UI
        bookBtn.disabled = false;
        bookBtn.textContent = 'Book';
        labels.forEach(l => l.classList.remove('opacity-50','pe-none'));
      }
    });
  }
  
})();
