"""Native Vortex route helpers for benchmark-range user workflows."""

from __future__ import annotations

import os
from typing import Any, Sequence

from ._compat import dataclass
from .client import ShardLoomClient
from .models import OutputEnvelope


_NATIVE_EXECUTION_MODES = frozenset({"native_vortex", "prepared_vortex", "auto"})


def _normalize_execution_mode(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in _NATIVE_EXECUTION_MODES:
        raise ValueError(
            "native Vortex routes require execution_mode to be one of "
            f"{sorted(_NATIVE_EXECUTION_MODES)}; got {value!r}"
        )
    return normalized


def _as_check(default: bool, override: bool | None) -> bool:
    return default if override is None else override


@dataclass(frozen=True, slots=True)
class NativeVortexQuery:
    """Deferred single-scenario query over native Vortex input artifacts."""

    route: "NativeVortexRoute"
    scenario: str
    workspace: str | os.PathLike[str] | None = None
    memory_gb: int | None = None
    max_parallelism: int | None = None

    @property
    def route_id(self) -> str:
        """Return the native route id represented by this query."""

        return "native_vortex_query"

    @property
    def start_state(self) -> str:
        """Return the native Vortex start state."""

        return "native_vortex_file"

    @property
    def execution_mode(self) -> str:
        """Return the selected execution mode."""

        return self.route.execution_mode

    def collect(self, *, check: bool | None = None) -> OutputEnvelope:
        """Run the native Vortex scenario and return the ShardLoom envelope."""

        return self.route.run(
            self.scenario,
            workspace=self.workspace,
            memory_gb=self.memory_gb,
            max_parallelism=self.max_parallelism,
            check=check,
        )

    def write_vortex(
        self,
        workspace: str | os.PathLike[str] | None = None,
        *,
        check: bool | None = None,
    ) -> OutputEnvelope:
        """Run the scenario and request a native Vortex result sink."""

        return self.route.run(
            self.scenario,
            workspace=workspace or self.workspace,
            write_result_vortex=True,
            memory_gb=self.memory_gb,
            max_parallelism=self.max_parallelism,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class NativeVortexRoute:
    """Explicit native `.vortex` input route over ShardLoom's Vortex runtime family."""

    client: ShardLoomClient
    fact_vortex: str | os.PathLike[str]
    dim_vortex: str | os.PathLike[str]
    cdc_delta_vortex: str | os.PathLike[str] | None = None
    workspace: str | os.PathLike[str] | None = None
    execution_mode: str = "native_vortex"
    memory_gb: int | None = None
    max_parallelism: int | None = None
    check: bool = True

    @classmethod
    def from_inputs(
        cls,
        *,
        client: ShardLoomClient,
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        execution_mode: str = "native_vortex",
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> "NativeVortexRoute":
        """Build a native Vortex route handle with explicit execution policy."""

        return cls(
            client=client,
            fact_vortex=fact_vortex,
            dim_vortex=dim_vortex,
            cdc_delta_vortex=cdc_delta_vortex,
            workspace=workspace,
            execution_mode=_normalize_execution_mode(execution_mode),
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    @property
    def route_id(self) -> str:
        """Return the route id represented by this handle."""

        return "native_vortex_query"

    @property
    def batch_route_id(self) -> str:
        """Return the native/prepared batch route id represented by this handle."""

        return "prepared_vortex_warm_query"

    @property
    def start_state(self) -> str:
        """Return the user-visible route start state."""

        return "native_vortex_file"

    @property
    def source_route(self) -> str:
        """Return the source route used by this handle."""

        return "Vortex-native local file/source"

    @property
    def preparation_route(self) -> str:
        """Return the preparation route status for native Vortex input."""

        return "not_required_native_vortex_input"

    @property
    def vortex_normalization_point(self) -> str:
        """Return the route's Vortex normalization boundary."""

        return "native_vortex_boundary"

    @property
    def route_runtime_status(self) -> str:
        """Return the route runtime support status."""

        return "global_runtime_supported"

    @property
    def fallback_attempted(self) -> bool:
        """Whether the route handle itself has attempted fallback execution."""

        return False

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the route handle itself has invoked an external engine."""

        return False

    @property
    def timing_contract(self) -> str:
        """Return the compact timing contract for native Vortex user/agent display."""

        return (
            "The route starts at native Vortex input. No compatibility preparation is included; "
            "runtime evidence keeps Vortex scan/operator/result-sink and no-fallback fields "
            "separate from evidence rendering."
        )

    def route_fields(self) -> dict[str, Any]:
        """Return a side-effect-free route summary for agents and diagnostics."""

        return {
            "route_id": self.route_id,
            "batch_route_id": self.batch_route_id,
            "start_state": self.start_state,
            "source_route": self.source_route,
            "preparation_route": self.preparation_route,
            "execution_mode": self.execution_mode,
            "fact_vortex": str(self.fact_vortex),
            "dim_vortex": str(self.dim_vortex),
            "cdc_delta_vortex": (
                None if self.cdc_delta_vortex is None else str(self.cdc_delta_vortex)
            ),
            "workspace": None if self.workspace is None else str(self.workspace),
            "memory_gb": self.memory_gb,
            "max_parallelism": self.max_parallelism,
            "vortex_normalization_point": self.vortex_normalization_point,
            "route_runtime_status": self.route_runtime_status,
            "preparation_included_in_route": False,
            "query_timing_starts_after_preparation": False,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "timing_contract": self.timing_contract,
        }

    def query(
        self,
        scenario: str,
        *,
        workspace: str | os.PathLike[str] | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
    ) -> NativeVortexQuery:
        """Build a single-scenario native Vortex query over this route."""

        return NativeVortexQuery(
            route=self,
            scenario=scenario,
            workspace=workspace or self.workspace,
            memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
            max_parallelism=(
                max_parallelism if max_parallelism is not None else self.max_parallelism
            ),
        )

    def run(
        self,
        scenario: str,
        *,
        workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool | None = None,
    ) -> OutputEnvelope:
        """Run one benchmark-range scenario through the native Vortex route."""

        return self.client.traditional_analytics_vortex_run(
            scenario,
            self.fact_vortex,
            self.dim_vortex,
            cdc_delta_vortex=self.cdc_delta_vortex,
            workspace=workspace or self.workspace,
            write_result_vortex=write_result_vortex,
            execution_mode=self.execution_mode,
            memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
            max_parallelism=(
                max_parallelism if max_parallelism is not None else self.max_parallelism
            ),
            check=_as_check(self.check, check),
        )

    def run_batch(
        self,
        scenarios: str | Sequence[str],
        *,
        workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool | None = None,
    ) -> OutputEnvelope:
        """Run one or more benchmark-range scenarios through the native Vortex batch route."""

        return self.client.traditional_analytics_vortex_batch_run(
            scenarios,
            self.fact_vortex,
            self.dim_vortex,
            cdc_delta_vortex=self.cdc_delta_vortex,
            workspace=workspace or self.workspace,
            write_result_vortex=write_result_vortex,
            execution_mode=self.execution_mode,
            evidence_level=evidence_level,
            memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
            max_parallelism=(
                max_parallelism if max_parallelism is not None else self.max_parallelism
            ),
            check=_as_check(self.check, check),
        )
