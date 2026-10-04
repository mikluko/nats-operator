try {
  const scheme = localStorage.getItem("scheme");
  if (scheme === "light" || scheme === "dark") {
    document.documentElement.dataset.scheme = scheme;
  }
} catch {}
