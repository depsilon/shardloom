# SPDX-License-Identifier: Apache-2.0
"""Lazy, structured diagnostic and evidence views for ShardLoom."""

from __future__ import annotations

from ._exports import names_for_audience as _names_for_audience
from ._exports import resolve_export as _resolve_export

__all__ = _names_for_audience("diagnostics")


def __getattr__(name: str) -> object:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = _resolve_export(name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return list(__all__)

