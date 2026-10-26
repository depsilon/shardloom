# SPDX-License-Identifier: Apache-2.0
"""Reusable immutable window declarations for the shared native SQL binder."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
import math


class _Unbounded(Enum):
    PRECEDING = "UNBOUNDED PRECEDING"
    FOLLOWING = "UNBOUNDED FOLLOWING"


def _bound(value, unit, *, start):
    if value is None:
        value = _Unbounded.PRECEDING if start else _Unbounded.FOLLOWING
    if isinstance(value, _Unbounded):
        if (start and value is _Unbounded.FOLLOWING) or (not start and value is _Unbounded.PRECEDING):
            raise ValueError("window frame has an invalid unbounded endpoint")
        return value.value
    if type(value) is int:
        if abs(value) > (1 << 64) - 1:
            raise ValueError("window offset exceeds UInt64")
    elif unit != "RANGE" or type(value) not in (float, Decimal):
        raise TypeError("ROWS/GROUPS offsets require integers; RANGE also admits exact decimal or finite float")
    elif (isinstance(value, float) and not math.isfinite(value)) or (isinstance(value, Decimal) and not value.is_finite()):
        raise ValueError("window offset must be finite")
    if value == 0:
        return "CURRENT ROW"
    from .query import _sql_literal
    magnitude = value.copy_abs() if isinstance(value, Decimal) else abs(value)
    return f"{_sql_literal(magnitude)} {'PRECEDING' if value < 0 else 'FOLLOWING'}"


@dataclass(frozen=True, slots=True)
class WindowSpec:
    """Window partition/order/frame metadata, reusable across expressions."""

    _partition: tuple[str, ...] = ()
    _order: tuple[str, ...] = ()
    _frame: str | None = None
    _exclusion: str | None = None
    source_bindings: tuple = ()
    resource_limits: object = None

    def partition_by(self, *columns) -> "WindowSpec":
        from .query import _normalize_columns, _predicate_sources, _predicate_limits
        return replace(self, _partition=_normalize_columns(columns),
            source_bindings=_predicate_sources(self, columns), resource_limits=_predicate_limits(self, columns))

    def order_by(self, *columns, descending: bool = False, nulls: str | None = None) -> "WindowSpec":
        from .query import _normalize_columns, _predicate_sources, _predicate_limits
        if type(descending) is not bool or nulls not in (None, "first", "last"):
            raise ValueError("window order requires a bool direction and nulls='first' or 'last'")
        suffix = (" DESC" if descending else " ASC") + ("" if nulls is None else f" NULLS {nulls.upper()}")
        return replace(self, _order=tuple(column + suffix for column in _normalize_columns(columns)),
            source_bindings=_predicate_sources(self, columns), resource_limits=_predicate_limits(self, columns))

    def _between(self, unit, start, end) -> "WindowSpec":
        left, right = _bound(start, unit, start=True), _bound(end, unit, start=False)
        if type(start) in (int, float, Decimal) and type(end) in (int, float, Decimal) and start > end:
            raise ValueError("window frame start must not follow its end")
        return replace(self, _frame=f"{unit} BETWEEN {left} AND {right}")

    def rows_between(self, start, end) -> "WindowSpec":
        return self._between("ROWS", start, end)

    def range_between(self, start, end) -> "WindowSpec":
        return self._between("RANGE", start, end)

    def groups_between(self, start, end) -> "WindowSpec":
        return self._between("GROUPS", start, end)

    def exclude(self, value: str) -> "WindowSpec":
        if value not in ("current_row", "group", "ties", "no_others"):
            raise ValueError("window exclusion must be current_row, group, ties or no_others")
        return replace(self, _exclusion=value.replace("_", " ").upper())

    @property
    def sql(self) -> str:
        parts = []
        if self._partition:
            parts.append("PARTITION BY " + ",".join(self._partition))
        if self._order:
            parts.append("ORDER BY " + ",".join(self._order))
        if self._frame:
            parts.append(self._frame)
        if self._exclusion:
            if not self._frame:
                raise ValueError("window exclusion requires an explicit frame")
            parts.append("EXCLUDE " + self._exclusion)
        return " ".join(parts)


class Window:
    """Factories for WindowSpec, with explicit signed offsets and unbounded ends."""

    current_row = 0
    unbounded_preceding = _Unbounded.PRECEDING
    unbounded_following = _Unbounded.FOLLOWING

    @staticmethod
    def partition_by(*columns) -> WindowSpec:
        return WindowSpec().partition_by(*columns)

    @staticmethod
    def order_by(*columns, descending=False, nulls=None) -> WindowSpec:
        return WindowSpec().order_by(*columns, descending=descending, nulls=nulls)

    @staticmethod
    def rows_between(start, end) -> WindowSpec:
        return WindowSpec().rows_between(start, end)

    @staticmethod
    def range_between(start, end) -> WindowSpec:
        return WindowSpec().range_between(start, end)

    @staticmethod
    def groups_between(start, end) -> WindowSpec:
        return WindowSpec().groups_between(start, end)
