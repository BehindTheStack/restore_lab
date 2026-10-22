'use strict';
(() => {
  const dialog = document.getElementById('details-modal');
  const title = document.getElementById('modal-title');
  const content = document.getElementById('modal-content');
  const back = document.getElementById('modal-back');
  let opener = null;
  let pending = null;
  let activeHref = null;
  let modalHistory = [];

  async function loadDetails(href) {
    if (pending) pending.abort();
    const request = new AbortController();
    pending = request;
    activeHref = href;
    back.hidden = modalHistory.length === 0;
    title.textContent = 'Carregando detalhes';
    const loading = document.createElement('p');
    loading.setAttribute('role', 'status');
    loading.textContent = 'Carregando…';
    content.replaceChildren(loading);
    content.setAttribute('aria-busy', 'true');
    const url = new URL(href, window.location.href);
    url.searchParams.set('format', 'json');
    try {
      const response = await fetch(url, {
        signal: request.signal,
        credentials: 'same-origin',
        cache: 'no-store',
        headers: {Accept: 'application/json'}
      });
      if (!response.ok) throw new Error(response.status === 404 ? 'Detalhes não encontrados no catálogo local.' : 'Não foi possível consultar os detalhes. Tente novamente.');
      const data = await response.json();
      if (pending !== request || !dialog.open) return;
      title.textContent = data.title;
      // HTML is produced by the local renderer, which escapes all source values.
      content.innerHTML = data.html;
      content.scrollTop = 0;
    } catch (error) {
      if (error.name === 'AbortError' || pending !== request || !dialog.open) return;
      title.textContent = 'Falha ao carregar detalhes';
      const message = document.createElement('p');
      message.setAttribute('role', 'alert');
      message.textContent = error.message;
      content.replaceChildren(message);
    } finally {
      if (pending === request) content.setAttribute('aria-busy', 'false');
    }
  }

  document.addEventListener('click', event => {
    const link = event.target.closest('a[data-modal]');
    if (!link || event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    const url = new URL(link.href, window.location.href);
    if (url.origin !== window.location.origin) return;
    event.preventDefault();
    if (!dialog.open) {
      opener = link;
      modalHistory = [];
      dialog.showModal();
    } else if (activeHref && activeHref !== link.href) {
      modalHistory.push(activeHref);
    }
    loadDetails(link.href);
  });

  document.getElementById('modal-close').addEventListener('click', () => dialog.close());
  dialog.addEventListener('keydown', event => {
    if (event.key !== 'Tab') return;
    const controls = [...dialog.querySelectorAll('button:not([disabled]), a[href], summary, [tabindex]:not([tabindex="-1"])')]
      .filter(node => node.getClientRects().length > 0);
    const first = controls[0];
    const last = controls[controls.length - 1];
    if (!first) return;
    if (event.shiftKey && (document.activeElement === first || !dialog.contains(document.activeElement))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (document.activeElement === last || !dialog.contains(document.activeElement))) {
      event.preventDefault();
      first.focus();
    }
  });
  back.addEventListener('click', () => {
    const previous = modalHistory.pop();
    if (previous) loadDetails(previous);
  });
  dialog.addEventListener('click', event => {
    if (event.target !== dialog) return;
    const box = dialog.getBoundingClientRect();
    if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) dialog.close();
  });
  dialog.addEventListener('close', () => {
    if (pending) pending.abort();
    pending = null;
    activeHref = null;
    modalHistory = [];
    if (opener && opener.isConnected) opener.focus();
    opener = null;
  });

  function refreshWhenIdle() {
    // Keep dialogs readable and preserve the current pagination URL on refresh.
    if (!dialog.open && !document.hidden) {
      window.location.reload();
      return;
    }
    window.setTimeout(refreshWhenIdle, 20000);
  }
  window.setTimeout(refreshWhenIdle, 20000);
})();
