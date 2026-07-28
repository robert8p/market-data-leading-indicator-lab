(() => {
  const refreshSeconds = Number(document.body.dataset.autoRefreshSeconds || 0);
  if (refreshSeconds > 0) {
    window.setTimeout(() => {
      if (!document.hidden && !document.querySelector('input:focus, select:focus, textarea:focus')) {
        window.location.reload();
      }
    }, refreshSeconds * 1000);
  }

  document.querySelectorAll('[data-progress]').forEach((bar) => {
    const target = Math.max(0, Math.min(100, Number(bar.dataset.progress || 0)));
    bar.style.width = '0%';
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => { bar.style.width = `${target}%`; });
    });
  });
})();
