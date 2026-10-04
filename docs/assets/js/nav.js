(() => {
  const toggle = document.querySelector("[data-nav-toggle]");
  const nav = document.getElementById("nav");
  if (!toggle || !nav) return;
  const set = (open) => {
    document.documentElement.classList.toggle("nav-open", open);
    toggle.setAttribute("aria-expanded", String(open));
  };
  toggle.addEventListener("click", (event) => {
    event.stopPropagation();
    set(toggle.getAttribute("aria-expanded") !== "true");
  });
  document.addEventListener("click", (event) => {
    if (!nav.contains(event.target)) set(false);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") set(false);
  });
})();
