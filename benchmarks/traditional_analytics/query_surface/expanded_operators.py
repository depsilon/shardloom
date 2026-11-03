# SPDX-License-Identifier: Apache-2.0
"""Map expanded corpus nodes onto their real interface declarations."""

from . import expanded_asof, expanded_nested, expanded_reuse, expanded_windows
from .adapters import AdapterGap


def declare(node, surface, children, *providers):
    if node.op == "window_case":
        return getattr(expanded_windows, surface)(node, children[0], *providers)
    if node.op == "asof_case":
        return getattr(expanded_asof, surface)(node, children, *providers)
    if node.op in {"explode_case", "membership_case"}:
        return getattr(expanded_nested, surface)(node, children, *providers)
    if node.op in {"materialized_case", "recursive_case"} and surface in {"sql", "polars_plan"}:
        return getattr(expanded_reuse, surface)(node, children, *providers)
    raise AdapterGap(f"{surface} has no independent declaration of {node.op}")
