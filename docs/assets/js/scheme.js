(() => {
  const root = document.documentElement;
  const buttons = document.querySelectorAll("[data-scheme-set]");
  const mark = () => {
    const current = root.dataset.scheme || "auto";
    buttons.forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.schemeSet === current)));
  };
  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const scheme = button.dataset.schemeSet;
      if (scheme === "auto") {
        delete root.dataset.scheme;
      } else {
        root.dataset.scheme = scheme;
      }
      try {
        if (scheme === "auto") {
          localStorage.removeItem("scheme");
        } else {
          localStorage.setItem("scheme", scheme);
        }
      } catch {}
      mark();
    });
  });
  mark();
})();
