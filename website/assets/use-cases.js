(() => {
  const form = document.querySelector("[data-use-case-filters]");
  const grid = document.querySelector("[data-use-case-grid]");
  const count = document.querySelector("[data-use-case-count]");

  if (!form || !grid) {
    return;
  }

  const cards = Array.from(grid.querySelectorAll(".use-case-card"));
  const controls = Array.from(form.querySelectorAll("[data-use-case-filter]"));

  function matches(card, key, value) {
    if (!value) {
      return true;
    }
    if (key === "input") {
      return card.dataset.inputs.toLowerCase().includes(value.toLowerCase());
    }
    if (key === "output") {
      return card.dataset.outputs.toLowerCase().includes(value.toLowerCase());
    }
    if (key === "execution") {
      return card.dataset.executionMode === value;
    }
    if (key === "evidence") {
      return card.dataset.evidenceLevel === value;
    }
    if (key === "platform") {
      return card.dataset.platform === value;
    }
    return card.dataset.status === value;
  }

  function applyFilters() {
    const active = controls.map((control) => [
      control.dataset.useCaseFilter,
      control.value,
    ]);
    let visible = 0;
    for (const card of cards) {
      const show = active.every(([key, value]) => matches(card, key, value));
      card.hidden = !show;
      if (show) {
        visible += 1;
      }
    }
    if (count) {
      count.textContent = `${visible} use case${visible === 1 ? "" : "s"} shown`;
    }
  }

  form.addEventListener("change", applyFilters);
  form.addEventListener("reset", () => {
    window.setTimeout(applyFilters, 0);
  });
  applyFilters();
})();
