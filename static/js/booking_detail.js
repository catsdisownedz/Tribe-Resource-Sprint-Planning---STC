// static/js/booking_detail.js
(function () {
  // Grab the 6 sprint checkboxes (they must have data-sprint="1..6")
  const boxes = Array.from(document.querySelectorAll('input[type="checkbox"][data-sprint]'));
  if (!boxes.length) return;

  const bookBtn   = document.getElementById('btn-book');
  const counterEl = document.getElementById('reservedCounter'); // optional <span id="reservedCounter"></span>

  // Globals injected by booking_detail.html
  const BOOKED = Array.isArray(window.BOOKED_SPRINTS) ? window.BOOKED_SPRINTS.map(Number) : [];
  const TEMP_ID = Number(window.TEMP_ID ?? 0);
  const RESERVED_LIMIT = Number(window.RESERVED_LIMIT ?? 0);
  const BOOKED_BY_ME = Number(window.BOOKED_BY_TRIBE ?? 0); // NEW


  // Treat 0 as “no cap” so UX isn’t blocked if there’s no limit configured
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

  // 1) Block previously booked sprints immediately
  boxes.forEach(box => {
    const sprint = Number(box.getAttribute('data-sprint'));
    if (bookedSet.has(sprint)) {
      setDisabled(box, true, 'Already booked');
    }
  });

  // 2) Live enforce reserved cap
  function refreshCap() {
    const pickedNow = selectedCount();
    const totalIfSaved = BOOKED_BY_ME + pickedNow; // show what your total would be
    if (counterEl) counterEl.textContent =
      `${totalIfSaved} / ${RESERVED_LIMIT}${RESERVED_LIMIT === 0 ? ' (no cap)' : ''}`;
  
    boxes.forEach(box => {
      const sprint = Number(box.getAttribute('data-sprint'));
      if (bookedSet.has(sprint)) {
        // stays disabled forever (someone already took it)
        return;
      }
      if (pickedNow >= CAP && !box.checked) {
        setDisabled(box, true, 'Reached reserved limit');
      } else {
        setDisabled(box, false);
      }
    });
  }

  boxes.forEach(b => b.addEventListener('change', refreshCap));
  refreshCap();

  // 3) Book button logic
  if (bookBtn) {
    bookBtn.addEventListener('click', async () => {
      const chosen = boxes
        .filter(b => b.checked)
        .map(b => Number(b.getAttribute('data-sprint')));

      if (chosen.length === 0) {
        alert('Pick at least one sprint');
        return;
      }

      try {
        const res = await fetch(`/api/book-temp/${TEMP_ID}`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ sprints: chosen })
        });

        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          const msg = data?.error || data?.details?.join('\n') || `Booking failed (${res.status})`;
          alert(msg);
          return;
        }

        const data = await res.json().catch(() => ({}));
        // Optional success message from backend:
        if (data?.message) {
          // You can show a toast/snackbar instead of alert if you like.
          // alert(data.message);
        }
        // Redirect to home after success
        window.location.href = '/';
      } catch (e) {
        console.error(e);
        alert('Network error while booking');
      }
    });
  }
})();
