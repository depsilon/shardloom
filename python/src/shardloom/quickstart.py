"""End-to-end quickstart proof helpers for local ShardLoom checkouts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .client import (
    CompatibilitySourceSmokeReport,
    LocalVortexPrimitiveSmokeReport,
    PythonClientSmokeReport,
    ShardLoomClient,
    WorkflowReadinessSmokeReport,
)
from .models import OutputEnvelope
from .query import LazyFrame, UnsupportedWorkflowReport, read_vortex

DEFAULT_QUICKSTART_FIXTURE = Path(
    "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"
)
DEFAULT_QUICKSTART_CAPABILITY_SCOPES = (
    "python",
    "adapters",
    "certification",
    "migration",
)


@dataclass(frozen=True, slots=True)
class QuickstartProofReport:
    """Aggregated local quickstart proof envelopes."""

    smoke: PythonClientSmokeReport
    capabilities: Mapping[str, OutputEnvelope]
    workflow: LazyFrame
    workflow_report: UnsupportedWorkflowReport
    compatibility_sources: CompatibilitySourceSmokeReport
    readiness: WorkflowReadinessSmokeReport
    local_vortex: LocalVortexPrimitiveSmokeReport | None

    @property
    def commands(self) -> tuple[str, ...]:
        """Return all CLI commands executed by the quickstart proof."""

        commands: list[str] = []
        commands.extend(self.smoke.commands)
        commands.extend(envelope.command for envelope in self.capabilities.values())
        commands.extend(envelope.command for envelope in self.workflow_report.envelopes)
        commands.extend(self.compatibility_sources.commands)
        commands.extend(self.readiness.commands)
        if self.local_vortex is not None:
            commands.extend(self.local_vortex.commands)
        return tuple(commands)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any quickstart surface reported attempted fallback execution."""

        return (
            self.smoke.fallback_attempted
            or any(envelope.fallback.attempted for envelope in self.capabilities.values())
            or self.workflow_report.fallback_attempted
            or self.compatibility_sources.fallback_attempted
            or self.readiness.fallback_attempted
            or (
                self.local_vortex.fallback_attempted
                if self.local_vortex is not None
                else False
            )
        )

    @property
    def all_no_write_planning(self) -> bool:
        """Whether all planning surfaces stayed no-write and report-only."""

        return (
            self.compatibility_sources.all_plan_only
            and self.readiness.all_no_write
            and self.readiness.all_report_only_or_planned
            and _envelope_bool_is_false(self.workflow_report.input_plan, "data_read")
            and _envelope_bool_is_false(self.workflow_report.input_plan, "data_materialized")
            and _envelope_bool_is_false(self.workflow_report.input_plan, "write_io")
            and not self.workflow_report.fallback_attempted
        )

    @property
    def local_execution_ran(self) -> bool:
        """Whether the optional certified local Vortex primitive smoke was requested."""

        return self.local_vortex is not None

    @property
    def local_execution_certified(self) -> bool:
        """Whether the optional local Vortex primitive smoke emitted certified evidence."""

        return self.local_vortex is not None and self.local_vortex.all_certified


def quickstart_proof(
    client: ShardLoomClient,
    *,
    fixture: str | os.PathLike[str] = DEFAULT_QUICKSTART_FIXTURE,
    capability_scopes: Sequence[str] = DEFAULT_QUICKSTART_CAPABILITY_SCOPES,
    predicate: str = "gte:value:3",
    columns: str | Sequence[str] = ("metric",),
    run_local_vortex: bool = False,
    memory_gb: int = 1,
    max_parallelism: int = 2,
) -> QuickstartProofReport:
    """Run the repository quickstart proof through explicit ShardLoom CLI calls."""

    workflow = (
        read_vortex(fixture, client=client)
        .filter(predicate)
        .select(*_columns_tuple(columns))
        .limit(5)
    )
    local_vortex = None
    if run_local_vortex:
        local_vortex = client.local_vortex_primitive_smoke(
            fixture,
            predicate=predicate,
            columns=columns,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
    return QuickstartProofReport(
        smoke=client.smoke_check(),
        capabilities={
            scope: client.capabilities(scope)
            for scope in capability_scopes
        },
        workflow=workflow,
        workflow_report=workflow.unsupported_report(check=False),
        compatibility_sources=client.compatibility_source_smoke(),
        readiness=client.workflow_readiness_smoke(),
        local_vortex=local_vortex,
    )


def _columns_tuple(columns: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(columns, str):
        values = tuple(part.strip() for part in columns.split(",") if part.strip())
    else:
        values = tuple(str(column).strip() for column in columns if str(column).strip())
    if not values:
        raise ValueError("columns must not be empty")
    return values


def _envelope_bool_is_false(envelope: OutputEnvelope, key: str) -> bool:
    value = envelope.field(key)
    return value is None or envelope.field_bool(key) is False
