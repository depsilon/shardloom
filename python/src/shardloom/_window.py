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


@dataclass(frozen=True, slots=True)
class _TemporalOffset:
    """Signed temporal frame offset retained as an inert SQL declaration."""

    family: str
    signed_value: int
    unit: str | None = None
    month_end: str | None = None
    ambiguity: str | None = None

    @property
    def sql(self):
        magnitude = abs(self.signed_value)
        if self.family == "duration":
            unit = {"s": "SECOND", "ms": "MILLISECOND", "us": "MICROSECOND", "ns": "NANOSECOND"}[self.unit]
            return f"INTERVAL '{magnitude}' {unit}"
        if self.family == "calendar_days":
            return f"CALENDAR_DAYS({magnitude}, '{self.ambiguity}')"
        return f"CALENDAR_MONTHS({magnitude}, '{self.month_end}', '{self.ambiguity}')"


def _temporal_offset(value, family, *, unit=None, month_end=None, ambiguity=None):
    if type(value) is not int:
        raise TypeError("temporal window offset must be an exact int")
    if abs(value) > (1 << 64) - 1:
        raise ValueError("temporal window offset exceeds UInt64")
    if family == "duration" and unit not in ("s", "ms", "us", "ns"):
        raise ValueError("duration unit must be s, ms, us or ns")
    if family == "calendar_months" and month_end not in ("reject", "clamp"):
        raise ValueError("month_end must be reject or clamp")
    if family != "duration" and ambiguity not in ("reject", "earlier", "later"):
        raise ValueError("ambiguity must be reject, earlier or later")
    return _TemporalOffset(family, value, unit, month_end, ambiguity)


def _bound(value, unit, *, start):
    if value is None:
        value = _Unbounded.PRECEDING if start else _Unbounded.FOLLOWING
    if isinstance(value, _Unbounded):
        if (start and value is _Unbounded.FOLLOWING) or (not start and value is _Unbounded.PRECEDING):
            raise ValueError("window frame has an invalid unbounded endpoint")
        return value.value
    if isinstance(value, _TemporalOffset):
        if unit != "RANGE":
            raise ValueError("temporal offsets are supported only for RANGE frames")
        return f"{value.sql} {'PRECEDING' if value.signed_value < 0 else 'FOLLOWING'}"
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
        temporal_start = start if isinstance(start, _TemporalOffset) else None
        temporal_end = end if isinstance(end, _TemporalOffset) else None
        if temporal_start is not None and temporal_end is not None:
            same_domain = (
                temporal_start.family == temporal_end.family
                and temporal_start.unit == temporal_end.unit
                and temporal_start.month_end == temporal_end.month_end
                and temporal_start.ambiguity == temporal_end.ambiguity
            )
            if same_domain and temporal_start.signed_value > temporal_end.signed_value:
                raise ValueError("window frame start must not follow its end")
        elif type(start) in (int, float, Decimal) and type(end) in (int, float, Decimal) and start > end:
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

    @staticmethod
    def duration(value: int, *, unit="us") -> _TemporalOffset:
        """Declare a signed fixed-duration RANGE offset; positive is FOLLOWING."""
        return _temporal_offset(value, "duration", unit=unit)

    @staticmethod
    def calendar_days(value: int, *, ambiguity="reject") -> _TemporalOffset:
        """Declare signed calendar days; ambiguity rejects by default."""
        return _temporal_offset(value, "calendar_days", ambiguity=ambiguity)

    @staticmethod
    def calendar_months(value: int, *, month_end="reject", ambiguity="reject") -> _TemporalOffset:
        """Declare signed calendar months; month end and ambiguity reject by default."""
        return _temporal_offset(value, "calendar_months", month_end=month_end, ambiguity=ambiguity)
