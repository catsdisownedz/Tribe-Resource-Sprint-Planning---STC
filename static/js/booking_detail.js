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
      if (chosen.length === 0) { alert('Pick at least one sprint'); return; }

      try {
        const res = await fetch(`/api/book-temp/${TEMP_ID}`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ sprints: chosen })
        });

        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          const msg = data?.error || data?.details?.join('\n') || `Booking failed (${res.status})`;
          alert(msg);
          return;
        }

        // Show in-page success notice (optional visual only)
        document.dispatchEvent(new CustomEvent('booking-success'));
        // Make sure master table updates on home
        sessionStorage.setItem('reloadAssignments','1');
        // small pause so the notice is visible
        setTimeout(()=>{ window.location.href = '/'; }, 1200);
      } catch (e) {
        console.error(e);
        alert('Network error while booking');
      }
    });
  }
})();
