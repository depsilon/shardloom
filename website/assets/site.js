(() => {
  function tokens(value) {
    return (value || "").split(/\s+/).filter(Boolean);
  }

  function matches(card, controls) {
    const searchable = card.textContent.toLowerCase();
    return controls.every((control) => {
      const key = control.dataset.filter;
      const value = control.value.trim().toLowerCase();
      if (!value) {
        return true;
      }
      if (key === "search") {
        return searchable.includes(value);
      }
      return tokens(card.dataset[key]).includes(value);
    });
  }

  function update(scope) {
    const cards = Array.from(scope.querySelectorAll("[data-filter-card]"));
    const controls = Array.from(scope.querySelectorAll("[data-filter]"));
    let visible = 0;
    for (const card of cards) {
      const show = matches(card, controls);
      card.hidden = !show;
      if (show) {
        visible += 1;
      }
    }
    const count = scope.querySelector("[data-filter-count]");
    if (count) {
      count.textContent = `${visible} of ${cards.length} shown`;
    }
  }

  for (const scope of document.querySelectorAll("[data-filter-scope]")) {
    const controls = Array.from(scope.querySelectorAll("[data-filter]"));
    for (const control of controls) {
      control.addEventListener("input", () => update(scope));
      control.addEventListener("change", () => update(scope));
    }
    update(scope);
  }
})();
