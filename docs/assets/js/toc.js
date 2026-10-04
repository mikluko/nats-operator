(() => {
  const toc = document.querySelector("[data-toc]");
  if (!toc) return;
  const links = new Map();
  toc.querySelectorAll("a[href^='#']").forEach((link) => {
    const heading = document.getElementById(decodeURIComponent(link.hash.slice(1)));
    if (heading) links.set(heading, link);
  });
  if (!links.size) return;
  const visible = new Set();
  let current = null;
  const mark = (heading) => {
    if (heading === current) return;
    if (current) links.get(current).classList.remove("is-current");
    current = heading;
    links.get(current).classList.add("is-current");
  };
  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        visible.add(entry.target);
      } else {
        visible.delete(entry.target);
      }
    });
    const first = [...links.keys()].find((heading) => visible.has(heading));
    if (first) mark(first);
  }, { rootMargin: "0px 0px -60% 0px" });
  links.forEach((_, heading) => observer.observe(heading));
  mark(links.keys().next().value);
})();
