"""Shared local runtime resource defaults for ShardLoom Python surfaces."""

from __future__ import annotations

import os


SHARDLOOM_MEMORY_GB_ENV = "SHARDLOOM_MEMORY_GB"
SHARDLOOM_MAX_PARALLELISM_ENV = "SHARDLOOM_MAX_PARALLELISM"


def _positive_int_env(name: str, default: int, *, floor: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(value, floor) if value > 0 else default


DEFAULT_LOCAL_RUNTIME_MEMORY_GB = _positive_int_env(SHARDLOOM_MEMORY_GB_ENV, 4)
DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM = _positive_int_env(
    SHARDLOOM_MAX_PARALLELISM_ENV,
    2,
    floor=2,
)
DEFAULT_INTERNAL_SMOKE_MEMORY_GB = 1
DEFAULT_INTERNAL_SMOKE_MAX_PARALLELISM = 1
