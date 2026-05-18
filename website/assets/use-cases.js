(() => {
  function tokens(value) {
    return String(value || "")
      .toLowerCase()
      .split(/\s+/)
      .filter(Boolean);
  }

  function matches(item, key, value) {
    if (!value) {
      return true;
    }
    const datasetKey = {
      input: "inputs",
      output: "outputs",
      execution: "executionMode",
      evidence: "evidenceLevel",
      platform: "platform",
      status: "status",
    }[key] || key;
    return tokens(item.dataset[datasetKey]).includes(value.toLowerCase());
  }

  function setupFilterSet({
    formSelector,
    gridSelector,
    itemSelector,
    countSelector,
    filterAttribute,
    itemLabel,
  }) {
    const form = document.querySelector(formSelector);
    const grid = document.querySelector(gridSelector);
    const count = document.querySelector(countSelector);

    if (!form || !grid) {
      return;
    }

    const items = Array.from(grid.querySelectorAll(itemSelector));
    const controls = Array.from(form.querySelectorAll(`[${filterAttribute}]`));

    function applyFilters() {
      const active = controls.map((control) => [
        control.getAttribute(filterAttribute),
        control.value,
      ]);
      let visible = 0;
      for (const item of items) {
        const show = active.every(([key, value]) => matches(item, key, value));
        item.hidden = !show;
        if (show) {
          visible += 1;
        }
      }
      if (count) {
        count.textContent = `${visible} ${itemLabel}${visible === 1 ? "" : "s"} shown`;
      }
    }

    form.addEventListener("change", applyFilters);
    form.addEventListener("reset", () => {
      window.setTimeout(applyFilters, 0);
    });
    applyFilters();
  }

  setupFilterSet({
    formSelector: "[data-use-case-filters]",
    gridSelector: "[data-use-case-grid]",
    itemSelector: ".use-case-card",
    countSelector: "[data-use-case-count]",
    filterAttribute: "data-use-case-filter",
    itemLabel: "use case",
  });

  setupFilterSet({
    formSelector: "[data-status-matrix-filters]",
    gridSelector: "[data-status-matrix-grid]",
    itemSelector: ".status-matrix-row",
    countSelector: "[data-status-matrix-count]",
    filterAttribute: "data-status-matrix-filter",
    itemLabel: "status row",
  });
})();
