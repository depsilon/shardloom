"""Explicit execution allocation; importing this module never reads the environment."""

from __future__ import annotations

import os
import sys
from typing import Mapping

from ._compat import dataclass
from .errors import NO_FALLBACK_REASON
from .models import Diagnostic, FallbackStatus

BYTES_PER_GIB = 1 << 30
MAX_MEMORY_BYTES = (1 << 64) - 1
MAX_EXECUTION_LANES = sys.maxsize * 2 + 1
RESOURCE_ORIGINS = frozenset({"execution_call", "context", "session", "environment", "platform"})


class ShardLoomResourceConfigurationError(ValueError):
    """Invalid or incomplete allocation, rejected before any input is inspected."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.fallback = FallbackStatus(
            attempted=False, allowed=False, engine=None, reason=NO_FALLBACK_REASON,
        )
        self.diagnostics = (Diagnostic(
            code="SL_CONFIGURATION_ERROR", severity="error", category="configuration",
            message=message, feature="execution_resources", reason=message,
            suggested_next_step="Configure memory_gb (GiB) or memory_bytes and max_parallelism on a context/session or execution call.",
            fallback=self.fallback,
        ),)


def _positive_int(name: str, value: object, maximum: int) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        raise ShardLoomResourceConfigurationError(
            f"{name} must be a positive integer no greater than {maximum}; booleans, floats and strings are not execution allocations"
        )
    return value


def _origin(value: object) -> str:
    if not isinstance(value, str) or value not in RESOURCE_ORIGINS:
        raise ShardLoomResourceConfigurationError("unknown execution resource origin")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionResourceLimits:
    """Explicit deployment ceilings; this object does not authenticate their source."""

    memory_bytes: int | None = None
    max_parallelism: int | None = None

    def __post_init__(self) -> None:
        if self.memory_bytes is not None:
            _positive_int("memory_bytes ceiling", self.memory_bytes, MAX_MEMORY_BYTES)
        if self.max_parallelism is not None:
            _positive_int("max_parallelism ceiling", self.max_parallelism, MAX_EXECUTION_LANES)

    def intersect(self, other: ExecutionResourceLimits) -> ExecutionResourceLimits:
        """Tighten an existing ceiling without removing or increasing it."""

        def smaller(left: int | None, right: int | None) -> int | None:
            if left is None:
                return right
            return left if right is None else min(left, right)

        return ExecutionResourceLimits(
            smaller(self.memory_bytes, other.memory_bytes),
            smaller(self.max_parallelism, other.max_parallelism),
        )


@dataclass(frozen=True, slots=True)
class ExecutionResources:
    """Immutable exact-byte allocation, independent of reservation or measured use.

    Maximum parallelism permits up to that many integer execution lanes. It is
    neither a fractional CPU quota nor an assertion that every lane is busy.
    Native code must attach this allocation to its existing shared memory owner.
    """

    memory_bytes: int
    max_parallelism: int
    memory_origin: str = "execution_call"
    parallelism_origin: str = "execution_call"
    limits: ExecutionResourceLimits | None = None

    def __post_init__(self) -> None:
        _positive_int("memory_bytes", self.memory_bytes, MAX_MEMORY_BYTES)
        _positive_int("max_parallelism", self.max_parallelism, MAX_EXECUTION_LANES)
        _origin(self.memory_origin)
        _origin(self.parallelism_origin)
        if self.limits is not None:
            if not isinstance(self.limits, ExecutionResourceLimits):
                raise ShardLoomResourceConfigurationError("limits must be ExecutionResourceLimits")
            if self.limits.memory_bytes is not None and self.memory_bytes > self.limits.memory_bytes:
                raise ShardLoomResourceConfigurationError("memory allocation exceeds the authorized memory_bytes ceiling")
            if self.limits.max_parallelism is not None and self.max_parallelism > self.limits.max_parallelism:
                raise ShardLoomResourceConfigurationError("max_parallelism exceeds the authorized execution-lane ceiling")

    @property
    def whole_gib(self) -> int | None:
        """Exact whole GiB, or None for an allocation that must remain in bytes."""

        return self.memory_bytes // BYTES_PER_GIB if self.memory_bytes % BYTES_PER_GIB == 0 else None

    @classmethod
    def from_gib(
        cls, memory_gb: int, max_parallelism: int, *, origin: str = "execution_call",
        limits: ExecutionResourceLimits | None = None,
    ) -> ExecutionResources:
        """Construct an allocation using binary GiB, with checked byte conversion."""

        return resolve_resources(
            memory_gb=memory_gb, max_parallelism=max_parallelism, origin=origin, limits=limits,
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ExecutionResources:
        """Deliberately load a complete allocation from environment variables.

        Accept SHARDLOOM_MEMORY_GB or SHARDLOOM_MEMORY_BYTES, and require
        SHARDLOOM_MAX_PARALLELISM. Invalid and missing values are errors. No
        environment is consulted by ordinary explicit resolution or import.
        """

        source = os.environ if env is None else env

        def value(name: str) -> int | None:
            raw = source.get(name)
            if raw is None:
                return None
            if not isinstance(raw, str) or not raw or any(char < "0" or char > "9" for char in raw):
                raise ShardLoomResourceConfigurationError(f"{name} must contain a positive decimal integer")
            # Check length before parsing untrusted, potentially enormous input.
            if len(raw) > 20:
                raise ShardLoomResourceConfigurationError(f"{name} exceeds the supported integer range")
            return int(raw)

        return resolve_resources(
            memory_gb=value("SHARDLOOM_MEMORY_GB"),
            memory_bytes=value("SHARDLOOM_MEMORY_BYTES"),
            max_parallelism=value("SHARDLOOM_MAX_PARALLELISM"), origin="environment",
        )


def resolve_resources(
    *, memory_gb: int | None = None, memory_bytes: int | None = None,
    max_parallelism: int | None = None, inherited: ExecutionResources | None = None,
    resources: ExecutionResources | None = None,
    origin: str = "execution_call", limits: ExecutionResourceLimits | None = None,
) -> ExecutionResources:
    """Resolve supplied overrides against an explicit allocation, never a default.

    Supplied invalid values always fail, even when valid inheritance exists.
    Missing fields inherit individually; both must be present after resolution.
    An inherited deployment ceiling cannot be removed or increased by a call.
    """

    origin = _origin(origin)
    if inherited is not None and not isinstance(inherited, ExecutionResources):
        raise ShardLoomResourceConfigurationError("inherited allocation must be ExecutionResources")
    if resources is not None:
        if not isinstance(resources, ExecutionResources):
            raise ShardLoomResourceConfigurationError("resources must be ExecutionResources")
        resource_limits = resources.limits
        if inherited is not None and inherited.limits is not None:
            resource_limits = (inherited.limits if resource_limits is None
                               else inherited.limits.intersect(resource_limits))
        inherited = ExecutionResources(
            resources.memory_bytes, resources.max_parallelism,
            resources.memory_origin, resources.parallelism_origin, resource_limits,
        )
    if limits is not None and not isinstance(limits, ExecutionResourceLimits):
        raise ShardLoomResourceConfigurationError("limits must be ExecutionResourceLimits")
    if memory_gb is not None and memory_bytes is not None:
        raise ShardLoomResourceConfigurationError("supply only one of memory_gb (GiB) and memory_bytes")
    memory_origin = origin
    parallelism_origin = origin
    if memory_gb is not None:
        memory_bytes = _positive_int("memory_gb", memory_gb, MAX_MEMORY_BYTES // BYTES_PER_GIB) * BYTES_PER_GIB
    elif memory_bytes is not None:
        memory_bytes = _positive_int("memory_bytes", memory_bytes, MAX_MEMORY_BYTES)
    elif inherited is not None:
        memory_bytes, memory_origin = inherited.memory_bytes, inherited.memory_origin
    if max_parallelism is not None:
        max_parallelism = _positive_int("max_parallelism", max_parallelism, MAX_EXECUTION_LANES)
    elif inherited is not None:
        max_parallelism, parallelism_origin = inherited.max_parallelism, inherited.parallelism_origin
    missing = []
    if memory_bytes is None:
        missing.append("memory_gb (or memory_bytes)")
    if max_parallelism is None:
        missing.append("max_parallelism")
    if missing:
        raise ShardLoomResourceConfigurationError(
            "missing required execution resources: " + " and ".join(missing)
        )
    if inherited is not None and inherited.limits is not None:
        limits = inherited.limits if limits is None else inherited.limits.intersect(limits)
    return ExecutionResources(memory_bytes, max_parallelism, memory_origin, parallelism_origin, limits)


def optional_resources(
    *, memory_gb: int | None = None, memory_bytes: int | None = None,
    max_parallelism: int | None = None, inherited: ExecutionResources | None = None,
    resources: ExecutionResources | None = None, origin: str = "execution_call",
    limits: ExecutionResourceLimits | None = None,
) -> ExecutionResources | None:
    """Permit a wholly unconfigured lazy declaration, but never a partial one."""

    # A deployment ceiling is not a job allocation. Owners retain it separately
    # while a lazy declaration waits for both execution values.
    merge_resource_limits(limits)
    if all(value is None for value in (
        memory_gb, memory_bytes, max_parallelism, inherited, resources,
    )):
        return None
    return resolve_resources(
        memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
        inherited=inherited, resources=resources, origin=origin, limits=limits,
    )


def merge_resource_limits(
    *limits: ExecutionResourceLimits | None,
) -> ExecutionResourceLimits | None:
    """Intersect explicit ceilings independently of a complete job allocation."""

    result = None
    for value in limits:
        if value is not None:
            if not isinstance(value, ExecutionResourceLimits):
                raise ShardLoomResourceConfigurationError("limits must be ExecutionResourceLimits")
            result = value if result is None else result.intersect(value)
    return result


def resource_command_args(resources: ExecutionResources | None) -> list[str]:
    """Transport a validated declaration without rounding bytes or losing origin."""

    if resources is None:
        return []
    args = [
        "--memory-bytes", str(resources.memory_bytes),
        "--max-parallelism", str(resources.max_parallelism),
        "--memory-origin", resources.memory_origin,
        "--parallelism-origin", resources.parallelism_origin,
    ]
    if resources.limits is not None:
        if resources.limits.memory_bytes is not None:
            args.extend(["--memory-limit-bytes", str(resources.limits.memory_bytes)])
        if resources.limits.max_parallelism is not None:
            args.extend(["--parallelism-limit", str(resources.limits.max_parallelism)])
    return args
