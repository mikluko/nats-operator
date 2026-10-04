(() => {
  document.querySelectorAll("[data-path]").forEach((element) => {
    element.textContent = location.pathname;
  });
})();
