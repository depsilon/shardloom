# SPDX-License-Identifier: Apache-2.0
"""Constrained interaction coverage and deterministic bounded selections."""
from __future__ import annotations

from itertools import combinations, product


def feasible_cells(factors, *, allowed=lambda _: True):
    names = tuple(factors)
    if not names or any(not values for values in factors.values()):
        raise ValueError("coverage requires nonempty factor domains")
    return [dict(zip(names, values)) for values in product(*(factors[name] for name in names))
            if allowed(dict(zip(names, values)))]


def interactions(cell, strength):
    if not 1 <= strength <= len(cell):
        raise ValueError("invalid interaction strength")
    return {tuple((name, cell[name]) for name in names) for names in combinations(sorted(cell), strength)}


def covering_selection(cells, *, strength=2):
    """Greedy deterministic set cover over *feasible* interactions only.

    This is a bounded corpus selector, not a claim of a minimal covering array.
    Impossible interactions are excluded only by the supplied explicit constraint.
    """
    footprints = [interactions(cell, strength) for cell in cells]
    remaining = set().union(*footprints)
    selected = []
    while remaining:
        index = max(range(len(cells)), key=lambda i: (len(footprints[i] & remaining), -i))
        selected.append(cells[index])
        remaining.difference_update(footprints[index])
    return selected


def coverage_record(cells, selected, *, strength=2):
    universe = set().union(*(interactions(cell, strength) for cell in cells))
    observed = set().union(*(interactions(cell, strength) for cell in selected))
    if observed - universe:
        raise ValueError("selection contains infeasible interactions")
    return {"strength": strength, "feasible_cells": len(cells), "selected_cells": len(selected),
            "feasible_interactions": len(universe), "covered_interactions": len(observed),
            "missing_interactions": [dict(item) for item in sorted(universe - observed)],
            "complete": universe == observed,
            "scope": "declared constrained factor space only; not arbitrary query coverage"}
