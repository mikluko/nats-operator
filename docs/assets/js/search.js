(() => {
  const dialog = document.getElementById("search");
  if (!dialog) return;
  const input = dialog.querySelector("input");
  const list = dialog.querySelector(".search-results");
  const count = dialog.querySelector(".search-count");
  const limit = 20;
  let index = null;
  let selected = 0;

  const load = () => {
    index ??= fetch(dialog.dataset.index)
      .then((response) => response.json())
      .then((entries) => entries.map((entry) => ({
        ...entry,
        title: entry.t.toLowerCase(),
        text: entry.c.toLowerCase(),
      })));
    return index;
  };

  const score = (entry, terms) => {
    let total = 0;
    for (const term of terms) {
      const inTitle = entry.title.includes(term);
      if (!inTitle && !entry.text.includes(term)) return 0;
      total += inTitle ? (entry.title.startsWith(term) ? 15 : 10) : 1;
    }
    return total;
  };

  const snippet = (entry, terms) => {
    const at = Math.max(0, ...terms.map((term) => entry.text.indexOf(term)).filter((i) => i >= 0).slice(0, 1));
    const start = Math.max(0, at - 40);
    const end = Math.min(entry.c.length, start + 180);
    return (start ? "…" : "") + entry.c.slice(start, end) + (end < entry.c.length ? "…" : "");
  };

  const select = (i) => {
    const rows = list.children;
    if (!rows.length) return;
    selected = (i + rows.length) % rows.length;
    [...rows].forEach((row, n) => row.setAttribute("aria-selected", String(n === selected)));
    rows[selected].scrollIntoView({ block: "nearest" });
  };

  const run = async () => {
    const terms = input.value.toLowerCase().split(/\s+/).filter(Boolean);
    list.replaceChildren();
    count.textContent = "";
    if (!terms.length) return;
    const entries = await load();
    const hits = entries
      .map((entry) => ({ entry, score: score(entry, terms) }))
      .filter((hit) => hit.score > 0)
      .sort((a, b) => b.score - a.score);
    count.textContent = `${hits.length} ${hits.length === 1 ? "result" : "results"}`;
    for (const { entry } of hits.slice(0, limit)) {
      const row = document.createElement("a");
      row.className = "search-result";
      row.href = entry.u;
      row.setAttribute("role", "option");
      const body = document.createElement("div");
      const title = document.createElement("strong");
      title.textContent = entry.t;
      body.append(title, snippet(entry, terms));
      const crumb = document.createElement("span");
      crumb.className = "search-crumb";
      crumb.textContent = entry.p;
      row.append(body, crumb);
      list.append(row);
    }
    select(0);
  };

  const open = () => {
    if (dialog.open) return;
    dialog.showModal();
    input.select();
    load();
  };

  document.querySelectorAll("[data-search-open]").forEach((trigger) => trigger.addEventListener("click", open));
  document.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      open();
    }
  });
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close();
  });
  list.addEventListener("click", () => dialog.close());
  input.addEventListener("input", run);
  input.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      select(selected + 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      select(selected - 1);
    } else if (event.key === "Enter" && list.children[selected]) {
      event.preventDefault();
      dialog.close();
      location.href = list.children[selected].href;
    }
  });
})();
