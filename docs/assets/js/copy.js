(() => {
  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const pre = button.closest(".code").querySelector("pre");
      await navigator.clipboard.writeText(pre.textContent);
      button.textContent = "copied";
      setTimeout(() => { button.textContent = "copy"; }, 1500);
    });
  });
})();
