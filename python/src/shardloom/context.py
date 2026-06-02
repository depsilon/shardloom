"""Side-effect-free user context helpers for the ShardLoom Python client."""

from __future__ import annotations

import os
import json
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Mapping, Sequence

from ._compat import dataclass
from .client import (
    Binary,
    ClaimGateCloseoutReport,
    ComputeCapabilityMatrix,
    DEFAULT_PROFILE_ORDER,
    EngineCapabilityMatrix,
    EngineSelectionPlan,
    GeneratedSourceWriteReport,
    HybridOverlayRunReport,
    LiveChangeContractPlan,
    LiveFixtureRunReport,
    PythonClientSmokeReport,
    RestApiContractPlan,
    RestApiDataPlane,
    RestApiDiscoveryContract,
    RestApiEventStream,
    RestApiLocalLifecycle,
    RestApiPlanPreview,
    RestApiSecurityGovernance,
    SemanticConformanceSuite,
    ShardLoomClient,
    WorkloadCertificationDossier,
    VortexIngestSmokeReport,
)
from .models import Diagnostic, OutputEnvelope
from .native_route import NativeVortexRoute
from .prepared_route import CompatibilityPreparedVortexRoute
from .query import (
    GeneratedRangeSource,
    GeneratedRowsSource,
    GeneratedSqlSource,
    LazyFrame,
    SqlWorkflow,
    UnsupportedWorkflowOperationReport,
    WorkflowSource,
    calendar as generated_calendar,
    dataframe_generated_with_column as generated_dataframe_generated_with_column,
    dataframe_source_free_projection as generated_dataframe_source_free_projection,
    from_arrow_ipc,
    from_arrow_table,
    from_pandas,
    from_rows,
    literal_table as generated_literal_table,
    range as generated_range,
    read as read_source,
    read_arrow_ipc,
    read_avro,
    sql_literal_select as generated_sql_literal_select,
    sql_values as generated_sql_values,
    read_csv,
    read_json,
    read_orc,
    read_parquet,
    read_vortex,
    sequence as generated_sequence,
    sql as sql_workflow,
)
from .session import ShardLoomSession

DEFAULT_CAPABILITY_SCOPES = (
    "python",
    "deployment",
    "data-etl",
    "dataframe",
    "compatibility",
    "adapters",
    "functions",
    "operators",
    "sql",
    "certification",
    "engines",
    "workflow",
    "remote-api",
    "api-surfaces",
    "cross-cg",
)
SUPPORTED_ENGINE_MODES = ("auto", "batch", "live", "hybrid")
_OBJECT_STORE_GENERATED_OUTPUT_DEFAULT_ROWS: tuple[Mapping[str, object], ...] = (
    {"value": 1},
)


@dataclass(frozen=True, slots=True)
class CapabilityPosture:
    """Normalized support, claim, and effect posture for one capability view."""

    scope: str
    command_status: str
    support_status: str | None
    claim_gate_status: str | None
    claim_gate_statuses: tuple[str, ...]
    severity: str | None
    supported: bool
    report_only: bool
    unsupported: bool
    claim_grade: bool
    no_runtime: bool
    runtime_execution: bool
    data_read: bool
    write_io: bool
    object_store_io: bool
    catalog_io: bool
    no_effects: bool
    fallback_attempted: bool
    fallback_allowed: bool
    no_fallback: bool
    external_engine_invoked: bool
    blocker_ids: tuple[str, ...]
    required_evidence: tuple[str, ...]
    required_gates: tuple[str, ...]
    materialization_boundaries: tuple[str, ...]
    suggested_next_action: str | None


@dataclass(frozen=True, slots=True)
class DataFrameMethodCapability:
    """Support, evidence, and claim boundary for one Python DataFrame-style method."""

    method: str
    family: str
    support_status: str
    claim_gate_status: str
    diagnostic_operation: str | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    runtime_execution: bool
    data_read: bool
    write_io: bool
    materialization_required: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    claim_boundary: str

    @property
    def supported_plan_only(self) -> bool:
        """Whether the method is supported only as a lazy/report declaration."""

        return self.support_status in {
            "lazy_plan_supported",
            "lazy_group_handle_supported",
            "source_declaration_supported",
        }

    @property
    def unsupported(self) -> bool:
        """Whether the method is currently an unsupported diagnostic surface."""

        return "unsupported" in self.support_status

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the row preserves the no-fallback/no-external-engine boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked


@dataclass(frozen=True, slots=True)
class DataFrameMethodCapabilityMatrix:
    """Report-only Python DataFrame/query-builder method capability matrix."""

    capability: "CapabilityView"
    rows: tuple[DataFrameMethodCapability, ...]

    @classmethod
    def from_capability(
        cls,
        capability: "CapabilityView",
    ) -> "DataFrameMethodCapabilityMatrix":
        """Build the static method matrix from a DataFrame capability view."""

        return cls(capability=capability, rows=DATAFRAME_METHOD_CAPABILITY_ROWS)

    @property
    def scope(self) -> str:
        """Return the underlying capability scope."""

        return self.capability.scope

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return method names in stable matrix order."""

        return tuple(row.method for row in self.rows)

    @property
    def family_order(self) -> tuple[str, ...]:
        """Return families in first-seen stable order."""

        return tuple(dict.fromkeys(row.family for row in self.rows))

    @property
    def plan_only_methods(self) -> tuple[str, ...]:
        """Return methods that are supported only as no-read lazy declarations."""

        return tuple(row.method for row in self.rows if row.supported_plan_only)

    @property
    def unsupported_methods(self) -> tuple[str, ...]:
        """Return methods that expose deterministic unsupported diagnostics."""

        return tuple(row.method for row in self.rows if row.unsupported)

    @property
    def claim_gate_statuses(self) -> tuple[str, ...]:
        """Return claim-gate statuses across DataFrame method rows."""

        return tuple(dict.fromkeys(row.claim_gate_status for row in self.rows))

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every method row preserves no fallback and no external engine."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def any_runtime_execution(self) -> bool:
        """Whether any method row reports runtime execution."""

        return any(row.runtime_execution for row in self.rows)

    @property
    def any_data_read(self) -> bool:
        """Whether any method row reports data reads."""

        return any(row.data_read for row in self.rows)

    @property
    def any_write_io(self) -> bool:
        """Whether any method row reports write I/O."""

        return any(row.write_io for row in self.rows)

    def family(self, name: str) -> tuple[DataFrameMethodCapability, ...]:
        """Return rows for a method family."""

        normalized = name.strip().lower().replace("-", "_")
        return tuple(row for row in self.rows if row.family == normalized)

    def row(self, method: str) -> DataFrameMethodCapability:
        """Return one matrix row by Python method name."""

        normalized = method.strip()
        for row in self.rows:
            if row.method == normalized:
                return row
        raise KeyError(f"DataFrame method {method!r} is not in the capability matrix")


@dataclass(frozen=True, slots=True)
class GeneratedObjectStoreOutputReport:
    """Typed view over generated rows staged into a local-emulator object-store write."""

    target_uri: str
    staging_path: str
    output_format: str
    provider_profile: str
    generated_report: GeneratedSourceWriteReport
    object_store_report: OutputEnvelope

    @property
    def envelope(self) -> OutputEnvelope:
        """Return the final object-store write envelope."""

        return self.object_store_report

    @property
    def command(self) -> str:
        """Return the final object-store write command name."""

        return self.object_store_report.command

    @property
    def status(self) -> str:
        """Return the final object-store write status."""

        return self.object_store_report.status

    @property
    def generated_source_created(self) -> bool:
        """Whether the generated-source staging certificate was emitted."""

        return self.generated_report.generated_source_certificate_status not in {
            "",
            "not_applicable",
            "not_emitted",
            "not_requested",
        }

    @property
    def object_store_write_status(self) -> str | None:
        """Return the object-store write status field."""

        return self.object_store_report.field("object_store_write_status")

    @property
    def commit_protocol_status(self) -> str | None:
        """Return the object-store commit protocol status."""

        return self.object_store_report.field("commit_protocol_status")

    @property
    def rollback_status(self) -> str | None:
        """Return local-emulator rollback status when requested."""

        return self.object_store_report.field("rollback_status")

    @property
    def object_store_io(self) -> bool:
        """Whether the final route performed object-store IO."""

        return self.object_store_report.field_bool("object_store_io", False) is True

    @property
    def object_store_write_io(self) -> bool:
        """Whether the final route performed object-store write IO."""

        return self.object_store_report.field_bool("object_store_write_io", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the route performed write IO."""

        return self.object_store_write_io

    @property
    def runtime_execution(self) -> bool:
        """Whether the scoped generated-output object-store route executed."""

        return self.generated_source_created and self.object_store_write_io

    @property
    def fallback_attempted(self) -> bool:
        """Whether any route stage attempted fallback execution."""

        return (
            self.generated_report.fallback_attempted
            or self.object_store_report.fallback.attempted
            or self.object_store_report.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether any route stage invoked an external engine."""

        return (
            self.generated_report.external_engine_invoked
            or self.object_store_report.field_bool("external_engine_invoked", False) is True
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the final object-store route claim-gate status."""

        return self.object_store_report.field("claim_gate_status")


@dataclass(frozen=True, slots=True)
class FoundryGeneratedOutputReport:
    """Typed view over the local Foundry-style generated-output dataset route."""

    output_ref: str
    result_dataset_path: str
    evidence_dataset_path: str
    generated_report: GeneratedSourceWriteReport
    result_dataset_report: Mapping[str, object]
    evidence_dataset_report: Mapping[str, object]

    @property
    def envelope(self) -> OutputEnvelope:
        """Return the ShardLoom generated-source write envelope."""

        return self.generated_report.envelope

    @property
    def command(self) -> str:
        """Return the ShardLoom command used for generated rows."""

        return self.generated_report.envelope.command

    @property
    def status(self) -> str:
        """Return the generated-output status."""

        return self.generated_report.envelope.status

    @property
    def runtime_execution(self) -> bool:
        """Whether the local Foundry-style generated-output route executed."""

        return self.generated_report.envelope.status == "success"

    @property
    def generated_source_created(self) -> bool:
        """Whether generated-source evidence was emitted."""

        return self.generated_report.generated_source_certificate_status not in {
            "",
            "not_applicable",
            "not_emitted",
            "not_requested",
        }

    @property
    def foundry_style_output_api_invoked(self) -> bool:
        """Whether the local Foundry-style output API boundary wrote datasets."""

        return (
            self.result_dataset_report.get("foundry_style_output_api_invoked") is True
            and self.evidence_dataset_report.get("foundry_style_output_api_invoked") is True
        )

    @property
    def foundry_runtime_invoked(self) -> bool:
        """Whether real Foundry runtime was invoked."""

        return False

    @property
    def foundry_output_api_invoked(self) -> bool:
        """Whether real Foundry output APIs were invoked."""

        return False

    @property
    def write_io(self) -> bool:
        """Whether the local Foundry-style route wrote output artifacts."""

        return self.foundry_style_output_api_invoked

    @property
    def fallback_attempted(self) -> bool:
        """Whether the route attempted fallback execution."""

        return self.generated_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the route invoked an external engine."""

        return self.generated_report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str:
        """Return the generated-output claim-gate status."""

        if self.runtime_execution and self.foundry_style_output_api_invoked:
            return "fixture_smoke_only"
        return "not_claim_grade"


@dataclass(frozen=True, slots=True)
class FrontDoorParityRow:
    """SQL/Python/DataFrame parity posture for one user-facing workflow family."""

    row_id: str
    workflow: str
    support_status: str
    runtime_gap_status: str
    sql_surface: str
    python_surface: str
    dataframe_surface: str
    shared_runtime_path: str
    parity_status: str
    performance_equivalence_status: str
    runtime_execution: bool
    data_read: bool
    write_io: bool
    materialization_required: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_boundary: str

    @property
    def equivalent_admitted_scope(self) -> bool:
        """Whether this row is admitted across front doors for its declared scope."""

        return self.parity_status == "equivalent_admitted_scope"

    @property
    def broad_gap(self) -> bool:
        """Whether this row names a remaining blocker for the broad user goal."""

        return self.blocker_id is not None

    @property
    def precise_runtime_gap(self) -> bool:
        """Whether broad-gap rows avoid generic unsupported/blocked posture."""

        generic = {"unsupported", "blocked", "not_complete", "not complete"}
        return self.runtime_gap_status not in generic

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether parity inspection preserves the no-fallback boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked


@dataclass(frozen=True, slots=True)
class FrontDoorParityMatrix:
    """Report-only parity matrix for SQL, Python, and DataFrame front doors."""

    rows: tuple[FrontDoorParityRow, ...]

    @property
    def schema_version(self) -> str:
        """Return the parity matrix schema version."""

        return "shardloom.front_door_parity_matrix.v1"

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return row ids in stable matrix order."""

        return tuple(row.row_id for row in self.rows)

    @property
    def admitted_rows(self) -> tuple[FrontDoorParityRow, ...]:
        """Return rows with scoped cross-front-door parity."""

        return tuple(row for row in self.rows if row.equivalent_admitted_scope)

    @property
    def broad_gap_rows(self) -> tuple[FrontDoorParityRow, ...]:
        """Return rows still blocking broad SQL/Python/DataFrame parity."""

        return tuple(row for row in self.rows if row.broad_gap)

    @property
    def runtime_gap_status_counts(self) -> Mapping[str, int]:
        """Return runtime gap status counts in deterministic insertion order."""

        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.runtime_gap_status] = counts.get(row.runtime_gap_status, 0) + 1
        return counts

    @property
    def all_broad_gaps_have_precise_runtime_status(self) -> bool:
        """Whether broad gaps avoid generic unsupported/blocked labels."""

        return all(row.precise_runtime_gap for row in self.broad_gap_rows)

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves the no-fallback/no-external-engine boundary."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def scoped_local_front_door_parity_supported(self) -> bool:
        """Whether the currently admitted local workflow families have parity."""

        required = {
            "local_file_filter_project_limit",
            "local_file_join_aggregate_sort_window",
            "generated_source_output",
            "schema_quality_preview",
            "local_vortex_primitive_runtime",
            "decoded_materialization_interop",
        }
        admitted = {row.row_id for row in self.admitted_rows}
        return required.issubset(admitted)

    @property
    def flexible_anything_claim_allowed(self) -> bool:
        """Whether broad arbitrary SQL/Python/DataFrame parity can be claimed."""

        return False

    @property
    def performance_equivalence_claim_allowed(self) -> bool:
        """Whether cross-front-door performance equivalence can be claimed."""

        return False

    def row(self, row_id: str) -> FrontDoorParityRow:
        """Return one parity row by id."""

        normalized = row_id.strip()
        for row in self.rows:
            if row.row_id == normalized:
                return row
        raise KeyError(f"front-door parity row {row_id!r} is not in the matrix")


@dataclass(frozen=True, slots=True)
class UserRouteCapabilityRow:
    """User/agent route-selection row for one input/output workflow family."""

    route_id: str
    route_display_name: str
    input_family: str
    input_examples: tuple[str, ...]
    front_doors: tuple[str, ...]
    desired_outputs: tuple[str, ...]
    recommended_user_surface: str
    start_state: str
    vortex_normalization_point: str
    source_route: str
    preparation_route: str
    execution_mode: str
    execution_route: str
    output_route: str
    evidence_route: str
    materialization_decode_boundary: str
    source_state_fingerprint: str
    source_schema_fingerprint: str
    source_parse_plan_id: str
    source_split_manifest_id: str
    source_anomaly_count: int | str
    source_quarantine_required: bool | str
    prepared_state_fingerprint: str
    prepared_state_reuse_scope: str
    prepared_state_reuse_manifest_path: str
    prepared_state_reuse_policy: str
    prepared_state_reuse_hit: bool | str
    prepared_state_reuse_reason: str
    prepared_state_reuse_manifest_digest: str
    prepared_state_invalidation_reason: str
    nearest_runnable_route: str
    required_feature_gate: str
    runtime_blocker_code: str
    route_runtime_status: str
    benchmark_range: bool
    route_comparable_to_external_end_to_end: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    blocker_id: str | None
    owner: str
    required_evidence: tuple[str, ...]
    claim_gate_status: str
    performance_claim_allowed: bool
    production_claim_allowed: bool
    spark_replacement_claim_allowed: bool
    claim_boundary: str

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the route preserves ShardLoom's no-fallback boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked

    @property
    def runtime_supported(self) -> bool:
        """Whether the route is admitted for scoped runtime use."""

        return self.route_runtime_status == "scoped_runtime_supported"


@dataclass(frozen=True, slots=True)
class UserRouteCapabilityReport:
    """Side-effect-free route-selection report for users and LLM agents."""

    rows: tuple[UserRouteCapabilityRow, ...]

    @property
    def schema_version(self) -> str:
        """Return the report schema version."""

        return "shardloom.user_route_capability_report.v1"

    @property
    def report_id(self) -> str:
        """Return the stable report id."""

        return "gar-runtime-impl-6d.user_route_capability_report"

    @property
    def route_order(self) -> tuple[str, ...]:
        """Return route ids in stable route-selection order."""

        return tuple(row.route_id for row in self.rows)

    @property
    def claim_gate_status(self) -> str:
        """Return the route-report claim gate status."""

        return "not_claim_grade"

    @property
    def flexible_anything_claim_allowed(self) -> bool:
        """Whether broad arbitrary SQL/Python/DataFrame support can be claimed."""

        return False

    @property
    def performance_equivalence_claim_allowed(self) -> bool:
        """Whether front-door performance equivalence can be claimed."""

        return False

    @property
    def production_claim_allowed(self) -> bool:
        """Whether production readiness can be claimed from this route report."""

        return False

    @property
    def spark_replacement_claim_allowed(self) -> bool:
        """Whether Spark replacement can be claimed from this route report."""

        return False

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves no fallback and no external engine invocation."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def local_benchmark_range_routes(self) -> tuple[UserRouteCapabilityRow, ...]:
        """Return routes in the local benchmark-range user surface."""

        return tuple(row for row in self.rows if row.benchmark_range)

    @property
    def unsupported_local_benchmark_route_ids(self) -> tuple[str, ...]:
        """Return benchmark-range routes that are still generically unsupported."""

        return tuple(
            row.route_id
            for row in self.local_benchmark_range_routes
            if row.route_runtime_status == "unsupported"
        )

    @property
    def route_runtime_status_counts(self) -> Mapping[str, int]:
        """Return route runtime status counts in deterministic insertion order."""

        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.route_runtime_status] = counts.get(row.route_runtime_status, 0) + 1
        return counts

    @property
    def vortex_normalization_contract(self) -> str:
        """Return the route-normalization contract shared by report rows."""

        return (
            "Native .vortex sources start at the Vortex boundary; compatibility local files "
            "enter through SourceState and either transient Vortex-preparable execution or "
            "vortex_ingest into VortexPreparedState; generated rows become Vortex-preparable "
            "batches; materialized pandas/Arrow/NumPy snapshots are explicit materialized inputs "
            "that must re-enter through a Vortex-preparable route before runtime-ready claims."
        )

    def route(self, route_id: str) -> UserRouteCapabilityRow:
        """Return one route row by id."""

        normalized = route_id.strip()
        for row in self.rows:
            if row.route_id == normalized:
                return row
        raise KeyError(f"user route {route_id!r} is not in the capability report")

    def routes_for(
        self,
        *,
        input_family: str | None = None,
        desired_output: str | None = None,
    ) -> tuple[UserRouteCapabilityRow, ...]:
        """Return routes matching an input family and/or desired output token."""

        input_token = input_family.strip() if input_family is not None else None
        output_token = desired_output.strip() if desired_output is not None else None
        matches: list[UserRouteCapabilityRow] = []
        for row in self.rows:
            if input_token is not None and row.input_family != input_token:
                continue
            if output_token is not None and output_token not in row.desired_outputs:
                continue
            matches.append(row)
        return tuple(matches)


@dataclass(frozen=True, slots=True)
class LocalVortexPrimitiveRouteRow:
    """Operation-level route row for scoped local Vortex primitive front doors."""

    route_id: str
    primitive: str
    sql_surface: str
    python_surface: str
    dataframe_surface: str
    context_surface: str
    session_surface: str
    cli_command: str
    cli_args_template: str
    start_state: str
    vortex_normalization_point: str
    execution_mode: str
    output_route: str
    evidence_route: str
    materialization_decode_boundary: str
    supports_source_order_limit: bool
    route_runtime_status: str
    fallback_attempted: bool
    external_engine_invoked: bool
    required_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this primitive route preserves ShardLoom's no-fallback boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked

    @property
    def runtime_supported(self) -> bool:
        """Whether this primitive route is admitted for scoped runtime use."""

        return self.route_runtime_status == "scoped_runtime_supported"


@dataclass(frozen=True, slots=True)
class LocalVortexPrimitiveRouteReport:
    """Side-effect-free operation map for local Vortex primitive user routes."""

    rows: tuple[LocalVortexPrimitiveRouteRow, ...]

    @property
    def schema_version(self) -> str:
        """Return the report schema version."""

        return "shardloom.local_vortex_primitive_route_report.v1"

    @property
    def report_id(self) -> str:
        """Return the stable report id."""

        return "gar-runtime-impl-6d.local_vortex_primitive_routes"

    @property
    def route_order(self) -> tuple[str, ...]:
        """Return primitive route ids in stable order."""

        return tuple(row.route_id for row in self.rows)

    @property
    def all_runtime_supported(self) -> bool:
        """Whether every primitive route is scoped runtime-supported."""

        return all(row.runtime_supported for row in self.rows)

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every primitive route preserves no fallback and no external engine use."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def command_coverage(self) -> tuple[str, ...]:
        """Return CLI commands covered by the primitive route map."""

        return tuple(dict.fromkeys(row.cli_command for row in self.rows))

    @property
    def source_order_limit_route_ids(self) -> tuple[str, ...]:
        """Return primitive routes that expose source-order LIMIT."""

        return tuple(
            row.route_id for row in self.rows if row.supports_source_order_limit
        )

    def route(self, route_id: str) -> LocalVortexPrimitiveRouteRow:
        """Return one primitive route row by id."""

        normalized = route_id.strip()
        for row in self.rows:
            if row.route_id == normalized:
                return row
        raise KeyError(f"local Vortex primitive route {route_id!r} is not in the report")


@dataclass(frozen=True, slots=True)
class LocalFileBenchmarkRouteRow:
    """Scenario-level route row for local-file benchmark families."""

    scenario_id: str
    scenario_name: str
    scenario_suite: str
    scenario_category: str
    dataset_profiles: tuple[str, ...]
    route_id: str
    route_display_name: str
    alternate_route_ids: tuple[str, ...]
    front_doors: tuple[str, ...]
    sql_surface: str
    python_surface: str
    dataframe_surface: str
    context_surface: str
    session_surface: str
    cli_surface: str
    start_state: str
    vortex_normalization_point: str
    source_route: str
    preparation_route: str
    selected_execution_mode: str
    output_route: str
    evidence_route: str
    materialization_decode_boundary: str
    source_state_fingerprint: str
    source_schema_fingerprint: str
    source_parse_plan_id: str
    source_split_manifest_id: str
    source_anomaly_count: int | str
    source_quarantine_required: bool | str
    prepared_state_fingerprint: str
    prepared_state_reuse_scope: str
    prepared_state_reuse_manifest_path: str
    prepared_state_reuse_policy: str
    prepared_state_reuse_hit: bool | str
    prepared_state_reuse_reason: str
    prepared_state_reuse_manifest_digest: str
    prepared_state_invalidation_reason: str
    nearest_runnable_route: str
    required_feature_gate: str
    runtime_blocker_code: str
    route_runtime_status: str
    fallback_attempted: bool
    external_engine_invoked: bool
    blocker_id: str | None
    owner: str
    required_evidence: tuple[str, ...]
    next_verifier: str
    claim_gate_status: str
    performance_claim_allowed: bool
    production_claim_allowed: bool
    spark_replacement_claim_allowed: bool
    claim_boundary: str

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this scenario route preserves ShardLoom's no-fallback boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked

    @property
    def runtime_mapped(self) -> bool:
        """Whether this scenario is mapped to a non-unsupported runtime posture."""

        return self.route_runtime_status != "unsupported"


@dataclass(frozen=True, slots=True)
class LocalFileBenchmarkRouteReport:
    """Side-effect-free scenario map for local-file benchmark route coverage."""

    rows: tuple[LocalFileBenchmarkRouteRow, ...]

    @property
    def schema_version(self) -> str:
        """Return the report schema version."""

        return "shardloom.local_file_benchmark_route_report.v1"

    @property
    def report_id(self) -> str:
        """Return the stable report id."""

        return "gar-runtime-impl-6d.local_file_benchmark_routes"

    @property
    def scenario_ids(self) -> tuple[str, ...]:
        """Return scenario ids in stable report order."""

        return tuple(row.scenario_id for row in self.rows)

    @property
    def unsupported_scenario_ids(self) -> tuple[str, ...]:
        """Return scenario ids that are still generically unsupported."""

        return tuple(
            row.scenario_id
            for row in self.rows
            if row.route_runtime_status == "unsupported"
        )

    @property
    def route_runtime_status_counts(self) -> Mapping[str, int]:
        """Return route runtime status counts in deterministic insertion order."""

        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.route_runtime_status] = counts.get(row.route_runtime_status, 0) + 1
        return counts

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every scenario route preserves no fallback and no external engine use."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def all_mapped_without_generic_unsupported(self) -> bool:
        """Whether every scenario avoids generic unsupported status."""

        return all(row.runtime_mapped for row in self.rows)

    @property
    def claim_gate_status(self) -> str:
        """Return the scenario-report claim gate status."""

        return "not_claim_grade"

    @property
    def performance_claim_allowed(self) -> bool:
        """Whether performance claims can be made from this report."""

        return False

    @property
    def production_claim_allowed(self) -> bool:
        """Whether production readiness can be claimed from this report."""

        return False

    @property
    def spark_replacement_claim_allowed(self) -> bool:
        """Whether Spark replacement can be claimed from this report."""

        return False

    def scenario(self, scenario_id: str) -> LocalFileBenchmarkRouteRow:
        """Return one scenario route row by id."""

        normalized = scenario_id.strip()
        for row in self.rows:
            if row.scenario_id == normalized:
                return row
        raise KeyError(
            f"local file benchmark scenario {scenario_id!r} is not in the route report"
        )


def _df_method(
    method: str,
    family: str,
    support_status: str,
    *,
    diagnostic_operation: str | None = None,
    blocker_id: str | None = None,
    required_evidence: Sequence[str] = (),
    runtime_execution: bool = False,
    data_read: bool = False,
    materialization_required: bool = False,
    write_io: bool = False,
    claim_boundary: str,
) -> DataFrameMethodCapability:
    return DataFrameMethodCapability(
        method=method,
        family=family,
        support_status=support_status,
        claim_gate_status="not_claim_grade",
        diagnostic_operation=diagnostic_operation,
        blocker_id=blocker_id,
        required_evidence=tuple(required_evidence),
        runtime_execution=runtime_execution,
        data_read=data_read,
        write_io=write_io,
        materialization_required=materialization_required,
        fallback_attempted=False,
        external_engine_invoked=False,
        claim_boundary=claim_boundary,
    )


def _front_door_row(
    row_id: str,
    workflow: str,
    support_status: str,
    *,
    runtime_gap_status: str | None = None,
    sql_surface: str,
    python_surface: str,
    dataframe_surface: str,
    shared_runtime_path: str,
    parity_status: str,
    performance_equivalence_status: str,
    required_evidence: Sequence[str],
    runtime_execution: bool = False,
    data_read: bool = False,
    write_io: bool = False,
    materialization_required: bool = False,
    blocker_id: str | None = None,
    claim_boundary: str,
) -> FrontDoorParityRow:
    return FrontDoorParityRow(
        row_id=row_id,
        workflow=workflow,
        support_status=support_status,
        runtime_gap_status=(
            runtime_gap_status
            if runtime_gap_status is not None
            else "admitted_scope"
        ),
        sql_surface=sql_surface,
        python_surface=python_surface,
        dataframe_surface=dataframe_surface,
        shared_runtime_path=shared_runtime_path,
        parity_status=parity_status,
        performance_equivalence_status=performance_equivalence_status,
        runtime_execution=runtime_execution,
        data_read=data_read,
        write_io=write_io,
        materialization_required=materialization_required,
        fallback_attempted=False,
        external_engine_invoked=False,
        blocker_id=blocker_id,
        required_evidence=tuple(required_evidence),
        claim_boundary=claim_boundary,
    )


def _route_diagnostic_packet(
    *,
    route_id: str,
    start_state: str,
    vortex_normalization_point: str,
    route_runtime_status: str,
    blocker_id: str | None,
    input_examples: Sequence[str] = (),
) -> dict[str, object]:
    """Return side-effect-free route diagnostics for users and agents."""

    normalization = vortex_normalization_point
    examples = " ".join(input_examples).lower()
    source_backed = (
        "SourceState" in normalization
        or "raw_" in start_state
        or "compat" in start_state
        or "materialized" in start_state
    )
    prepared_backed = (
        "VortexPreparedState" in normalization
        and "no persistent VortexPreparedState" not in normalization
    ) or start_state == "VortexPreparedState"
    if not prepared_backed:
        reuse_packet: dict[str, object] = {
            "prepared_state_reuse_scope": "not_applicable_no_prepared_state",
            "prepared_state_reuse_manifest_path": "not_applicable_no_prepared_state",
            "prepared_state_reuse_policy": "not_applicable_no_prepared_state",
            "prepared_state_reuse_hit": "not_applicable_no_prepared_state",
            "prepared_state_reuse_reason": "not_applicable_no_prepared_state",
            "prepared_state_reuse_manifest_digest": "not_applicable_no_prepared_state",
            "prepared_state_invalidation_reason": "not_applicable_no_prepared_state",
        }
    elif "already_prepared_vortex_state" in normalization or start_state == "VortexPreparedState":
        reuse_packet = {
            "prepared_state_reuse_scope": "explicit_prepared_state_input",
            "prepared_state_reuse_manifest_path": "not_required_existing_prepared_state",
            "prepared_state_reuse_policy": "explicit_prepared_state_admission.v1",
            "prepared_state_reuse_hit": "true_when_artifact_admitted",
            "prepared_state_reuse_reason": "explicit_prepared_state_input",
            "prepared_state_reuse_manifest_digest": "runtime_prepared_state_digest_pending",
            "prepared_state_invalidation_reason": (
                "artifact_admission_failure_or_policy_mismatch"
            ),
        }
    else:
        reuse_packet = {
            "prepared_state_reuse_scope": "workspace_manifest_local_vortex_artifacts",
            "prepared_state_reuse_manifest_path": (
                "<workspace>/.shardloom/prepared-vortex-reuse-manifest.json"
            ),
            "prepared_state_reuse_policy": (
                "shardloom.python.prepared_vortex_reuse_manifest.v1"
            ),
            "prepared_state_reuse_hit": "runtime_evaluated",
            "prepared_state_reuse_reason": "runtime_evaluated_workspace_manifest_lookup",
            "prepared_state_reuse_manifest_digest": (
                "runtime_prepared_state_reuse_manifest_digest_pending"
            ),
            "prepared_state_invalidation_reason": (
                "runtime_evaluated_on_reuse_miss_or_block"
            ),
        }
    runnable = route_runtime_status in {
        "scoped_runtime_supported",
        "prepared_route_supported",
    }
    nearest_by_route = {
        "quarantine_output_route": "local_file_prepare_once_first_query",
        "broad_sql_python_dataframe_runtime": "local_file_direct_transient_route",
        "object_store_lakehouse_runtime": "local_file_cold_certified_route",
        "performance_equivalence_evidence": "local_file_prepare_once_batch",
    }
    feature_gate = "none"
    if any(token in examples for token in ("parquet", "arrow", "avro", "orc")):
        feature_gate = "compat_format_gate_for_parquet_arrow_ipc_avro_orc_when_selected"
    if blocker_id:
        lowered = blocker_id.lower()
        if "quarantine" in lowered:
            feature_gate = "quarantine_output_route"
        elif "object" in lowered or "lakehouse" in lowered:
            feature_gate = "object_store_lakehouse_runtime"
        elif "broad" in lowered or "language" in lowered:
            feature_gate = "broad_sql_python_dataframe_runtime_expansion"
        elif "benchmark" in lowered:
            feature_gate = "front_door_benchmark_claim_evidence"

    return {
        "source_state_fingerprint": (
            "runtime_source_state_fingerprint_pending"
            if source_backed
            else "not_applicable_native_or_source_free_route"
        ),
        "source_schema_fingerprint": (
            "runtime_source_schema_fingerprint_pending"
            if source_backed
            else "not_applicable_native_or_source_free_route"
        ),
        "source_parse_plan_id": (
            f"parse-plan://{route_id}"
            if source_backed
            else "not_applicable_native_or_source_free_route"
        ),
        "source_split_manifest_id": (
            f"split-manifest://{route_id}"
            if source_backed
            else "not_applicable_native_or_source_free_route"
        ),
        "source_anomaly_count": (
            "not_evaluated_until_source_admission" if source_backed else 0
        ),
        "source_quarantine_required": (
            "not_evaluated_until_source_admission" if source_backed else False
        ),
        "prepared_state_fingerprint": (
            "runtime_prepared_state_fingerprint_pending"
            if prepared_backed
            else "not_applicable_no_prepared_state"
        ),
        **reuse_packet,
        "nearest_runnable_route": (
            route_id if runnable else nearest_by_route.get(route_id, "local_file_direct_transient_route")
        ),
        "required_feature_gate": feature_gate,
        "runtime_blocker_code": blocker_id or "none",
    }


def _user_route(
    route_id: str,
    route_display_name: str,
    input_family: str,
    *,
    input_examples: Sequence[str],
    front_doors: Sequence[str],
    desired_outputs: Sequence[str],
    recommended_user_surface: str,
    start_state: str,
    vortex_normalization_point: str,
    source_route: str,
    preparation_route: str,
    execution_mode: str,
    execution_route: str,
    output_route: str,
    evidence_route: str,
    materialization_decode_boundary: str,
    route_runtime_status: str,
    benchmark_range: bool,
    route_comparable_to_external_end_to_end: bool,
    owner: str,
    required_evidence: Sequence[str],
    claim_boundary: str,
    blocker_id: str | None = None,
    claim_gate_status: str = "not_claim_grade",
    performance_claim_allowed: bool = False,
    production_claim_allowed: bool = False,
    spark_replacement_claim_allowed: bool = False,
) -> UserRouteCapabilityRow:
    diagnostic_packet = _route_diagnostic_packet(
        route_id=route_id,
        start_state=start_state,
        vortex_normalization_point=vortex_normalization_point,
        route_runtime_status=route_runtime_status,
        blocker_id=blocker_id,
        input_examples=input_examples,
    )
    return UserRouteCapabilityRow(
        route_id=route_id,
        route_display_name=route_display_name,
        input_family=input_family,
        input_examples=tuple(input_examples),
        front_doors=tuple(front_doors),
        desired_outputs=tuple(desired_outputs),
        recommended_user_surface=recommended_user_surface,
        start_state=start_state,
        vortex_normalization_point=vortex_normalization_point,
        source_route=source_route,
        preparation_route=preparation_route,
        execution_mode=execution_mode,
        execution_route=execution_route,
        output_route=output_route,
        evidence_route=evidence_route,
        materialization_decode_boundary=materialization_decode_boundary,
        source_state_fingerprint=str(diagnostic_packet["source_state_fingerprint"]),
        source_schema_fingerprint=str(diagnostic_packet["source_schema_fingerprint"]),
        source_parse_plan_id=str(diagnostic_packet["source_parse_plan_id"]),
        source_split_manifest_id=str(diagnostic_packet["source_split_manifest_id"]),
        source_anomaly_count=diagnostic_packet["source_anomaly_count"],
        source_quarantine_required=diagnostic_packet["source_quarantine_required"],
        prepared_state_fingerprint=str(diagnostic_packet["prepared_state_fingerprint"]),
        prepared_state_reuse_scope=str(diagnostic_packet["prepared_state_reuse_scope"]),
        prepared_state_reuse_manifest_path=str(
            diagnostic_packet["prepared_state_reuse_manifest_path"]
        ),
        prepared_state_reuse_policy=str(diagnostic_packet["prepared_state_reuse_policy"]),
        prepared_state_reuse_hit=diagnostic_packet["prepared_state_reuse_hit"],
        prepared_state_reuse_reason=str(
            diagnostic_packet["prepared_state_reuse_reason"]
        ),
        prepared_state_reuse_manifest_digest=str(
            diagnostic_packet["prepared_state_reuse_manifest_digest"]
        ),
        prepared_state_invalidation_reason=str(
            diagnostic_packet["prepared_state_invalidation_reason"]
        ),
        nearest_runnable_route=str(diagnostic_packet["nearest_runnable_route"]),
        required_feature_gate=str(diagnostic_packet["required_feature_gate"]),
        runtime_blocker_code=str(diagnostic_packet["runtime_blocker_code"]),
        route_runtime_status=route_runtime_status,
        benchmark_range=benchmark_range,
        route_comparable_to_external_end_to_end=route_comparable_to_external_end_to_end,
        fallback_attempted=False,
        external_engine_invoked=False,
        blocker_id=blocker_id,
        owner=owner,
        required_evidence=tuple(required_evidence),
        claim_gate_status=claim_gate_status,
        performance_claim_allowed=performance_claim_allowed,
        production_claim_allowed=production_claim_allowed,
        spark_replacement_claim_allowed=spark_replacement_claim_allowed,
        claim_boundary=claim_boundary,
    )


def _local_vortex_primitive_route(
    route_id: str,
    primitive: str,
    *,
    sql_surface: str,
    python_surface: str,
    dataframe_surface: str,
    context_surface: str,
    session_surface: str,
    cli_command: str,
    cli_args_template: str,
    supports_source_order_limit: bool = False,
    required_evidence: Sequence[str],
) -> LocalVortexPrimitiveRouteRow:
    return LocalVortexPrimitiveRouteRow(
        route_id=route_id,
        primitive=primitive,
        sql_surface=sql_surface,
        python_surface=python_surface,
        dataframe_surface=dataframe_surface,
        context_surface=context_surface,
        session_surface=session_surface,
        cli_command=cli_command,
        cli_args_template=cli_args_template,
        start_state="native_vortex_file",
        vortex_normalization_point="native_vortex_boundary",
        execution_mode="native_vortex",
        output_route="machine-readable primitive report and bounded scoped collect output",
        evidence_route="local primitive command envelope, execution certificate, Native I/O, no-fallback evidence",
        materialization_decode_boundary="primitive report boundary; decoded rows only when the bounded collect surface explicitly asks for them",
        supports_source_order_limit=supports_source_order_limit,
        route_runtime_status="scoped_runtime_supported",
        fallback_attempted=False,
        external_engine_invoked=False,
        required_evidence=tuple(required_evidence),
        claim_gate_status="not_claim_grade",
        claim_boundary=_LOCAL_VORTEX_PRIMITIVE_RUNTIME_BOUNDARY,
    )


def _local_file_benchmark_route(
    scenario_id: str,
    scenario_name: str,
    scenario_suite: str,
    scenario_category: str,
    *,
    dataset_profiles: Sequence[str],
    route_id: str,
    route_display_name: str,
    selected_execution_mode: str,
    sql_surface: str,
    python_surface: str,
    dataframe_surface: str,
    context_surface: str,
    session_surface: str,
    cli_surface: str,
    source_route: str,
    preparation_route: str,
    output_route: str,
    evidence_route: str,
    materialization_decode_boundary: str,
    route_runtime_status: str,
    owner: str,
    required_evidence: Sequence[str],
    next_verifier: str,
    claim_boundary: str,
    alternate_route_ids: Sequence[str] = (),
    start_state: str = "raw_compat_source",
    vortex_normalization_point: str = (
        "local compatibility source -> SourceState -> vortex_ingest -> VortexPreparedState"
    ),
    blocker_id: str | None = None,
    front_doors: Sequence[str] = ("SQL", "Python", "DataFrame", "context", "session", "CLI"),
) -> LocalFileBenchmarkRouteRow:
    diagnostic_packet = _route_diagnostic_packet(
        route_id=route_id,
        start_state=start_state,
        vortex_normalization_point=vortex_normalization_point,
        route_runtime_status=route_runtime_status,
        blocker_id=blocker_id,
        input_examples=(scenario_name, *dataset_profiles),
    )
    source_split_manifest_id = str(diagnostic_packet["source_split_manifest_id"])
    if scenario_id == "many_small_files_scan":
        source_split_manifest_id = "split-manifest://many_small_files_scan"
    return LocalFileBenchmarkRouteRow(
        scenario_id=scenario_id,
        scenario_name=scenario_name,
        scenario_suite=scenario_suite,
        scenario_category=scenario_category,
        dataset_profiles=tuple(dataset_profiles),
        route_id=route_id,
        route_display_name=route_display_name,
        alternate_route_ids=tuple(alternate_route_ids),
        front_doors=tuple(front_doors),
        sql_surface=sql_surface,
        python_surface=python_surface,
        dataframe_surface=dataframe_surface,
        context_surface=context_surface,
        session_surface=session_surface,
        cli_surface=cli_surface,
        start_state=start_state,
        vortex_normalization_point=vortex_normalization_point,
        source_route=source_route,
        preparation_route=preparation_route,
        selected_execution_mode=selected_execution_mode,
        output_route=output_route,
        evidence_route=evidence_route,
        materialization_decode_boundary=materialization_decode_boundary,
        source_state_fingerprint=str(diagnostic_packet["source_state_fingerprint"]),
        source_schema_fingerprint=str(diagnostic_packet["source_schema_fingerprint"]),
        source_parse_plan_id=str(diagnostic_packet["source_parse_plan_id"]),
        source_split_manifest_id=source_split_manifest_id,
        source_anomaly_count=diagnostic_packet["source_anomaly_count"],
        source_quarantine_required=diagnostic_packet["source_quarantine_required"],
        prepared_state_fingerprint=str(diagnostic_packet["prepared_state_fingerprint"]),
        prepared_state_reuse_scope=str(diagnostic_packet["prepared_state_reuse_scope"]),
        prepared_state_reuse_manifest_path=str(
            diagnostic_packet["prepared_state_reuse_manifest_path"]
        ),
        prepared_state_reuse_policy=str(diagnostic_packet["prepared_state_reuse_policy"]),
        prepared_state_reuse_hit=diagnostic_packet["prepared_state_reuse_hit"],
        prepared_state_reuse_reason=str(
            diagnostic_packet["prepared_state_reuse_reason"]
        ),
        prepared_state_reuse_manifest_digest=str(
            diagnostic_packet["prepared_state_reuse_manifest_digest"]
        ),
        prepared_state_invalidation_reason=str(
            diagnostic_packet["prepared_state_invalidation_reason"]
        ),
        nearest_runnable_route=str(diagnostic_packet["nearest_runnable_route"]),
        required_feature_gate=str(diagnostic_packet["required_feature_gate"]),
        runtime_blocker_code=str(diagnostic_packet["runtime_blocker_code"]),
        route_runtime_status=route_runtime_status,
        fallback_attempted=False,
        external_engine_invoked=False,
        blocker_id=blocker_id,
        owner=owner,
        required_evidence=tuple(required_evidence),
        next_verifier=next_verifier,
        claim_gate_status="not_claim_grade",
        performance_claim_allowed=False,
        production_claim_allowed=False,
        spark_replacement_claim_allowed=False,
        claim_boundary=claim_boundary,
    )


_LAZY_DECLARATION_BOUNDARY = (
    "Side-effect-free lazy declaration only; no data read, runtime execution, "
    "write I/O, DataFrame runtime, or performance claim."
)
_UNSUPPORTED_BOUNDARY = (
    "Deterministic unsupported diagnostic only; no DataFrame runtime, data read, "
    "write I/O, external engine, fallback, or production claim."
)
_MATERIALIZATION_BOUNDARY = (
    "Scoped bounded decoded materialization only for admitted local-source ShardLoom rows and "
    "explicit materialized input snapshots. Optional pandas/Arrow/NumPy packages are containers "
    "or compatibility encoders, not execution engines; no object-store/table source, external "
    "engine, fallback, broad notebook runtime, or production performance claim."
)
_WRITE_BOUNDARY = (
    "Write/export diagnostic only; no file write, sink commit, external engine, "
    "fallback, or production output claim."
)
_GENERATED_OUTPUT_BOUNDARY = (
    "Scoped local generated-output smokes only; user rows, engine-native range/sequence, and "
    "source-free SQL VALUES/literal SELECT/generate_series/range, including scoped value-column "
    "range projections, write local JSONL/CSV with "
    "generated-source and output evidence, but no broad DataFrame runtime, broad SQL runtime, "
    "object-store/lakehouse, Foundry, performance, or production claim."
)
_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY = (
    "Scoped local-source query-builder runtime only: local CSV, flat JSON/JSONL/NDJSON, and "
    "feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC lower through ShardLoom SQL "
    "local-source execution for admitted projection, filter, bounded limit, computed-column, "
    "scalar/grouped aggregate, multi-key top-N, aggregate-output ordering, inner/outer/semi/anti "
    "equi-join, cross-join, expression-condition join, join-aggregate, ranking-window, and local "
    "output/fanout workflows. No pandas/Polars "
    "backend, object-store/table source, broad SQL/DataFrame runtime, external engine, fallback, "
    "or production claim."
)
_LOCAL_VORTEX_PRIMITIVE_RUNTIME_BOUNDARY = (
    "Scoped local Vortex primitive runtime only: SQL COUNT/project/filter/filter-project forms "
    "over a single local .vortex source and read_vortex(...).count(), filter(...).count(), "
    "select(...).collect(), filter(...).collect(), and filter(...).select(...).limit(...).collect() "
    "lower to ShardLoom's explicit Vortex local primitive commands backed by upstream Vortex "
    "scan/read APIs. This is not decoded row materialization, broad SQL Vortex parity, "
    "read-transform-write parity, object-store/table runtime, external engine fallback, or a "
    "performance-equivalence claim."
)
_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY = (
    "Scoped bounded Python object materialization from ShardLoom-emitted inline JSONL for admitted "
    "local-source query-builder workflows only; object-store/table source, external engine, "
    "fallback, or production notebook/DataFrame claim."
)

LOCAL_VORTEX_PRIMITIVE_ROUTE_ROWS: tuple[LocalVortexPrimitiveRouteRow, ...] = (
    _local_vortex_primitive_route(
        "vortex_count_all",
        "count_all",
        sql_surface="ctx.sql(\"SELECT COUNT(*) FROM 'orders.vortex'\").collect()",
        python_surface="read_vortex('orders.vortex').count()",
        dataframe_surface="read_vortex('orders.vortex').count()",
        context_surface="ctx.read_vortex('orders.vortex').count()",
        session_surface="session.read_vortex('orders.vortex').count()",
        cli_command="vortex-run",
        cli_args_template="vortex-run <dataset.vortex> count <memory_gb> <max_parallelism> --format json",
        required_evidence=("vortex_run_count", "execution_certificate", "native_io_certificate"),
    ),
    _local_vortex_primitive_route(
        "vortex_count_where",
        "count_where",
        sql_surface=(
            "ctx.sql(\"SELECT COUNT(*) FROM 'orders.vortex' WHERE value >= 3\").collect()"
        ),
        python_surface="read_vortex('orders.vortex').filter('gte:value:3').count()",
        dataframe_surface="read_vortex('orders.vortex').where(col('value') >= 3).count()",
        context_surface="ctx.read_vortex('orders.vortex').filter('gte:value:3').count()",
        session_surface="session.read_vortex('orders.vortex').filter('gte:value:3').count()",
        cli_command="vortex-count-where",
        cli_args_template=(
            "vortex-count-where <dataset.vortex> <tiny-predicate> --execute-local-primitive "
            "<memory_gb> <max_parallelism> --format json"
        ),
        required_evidence=(
            "vortex_count_where",
            "filtered_count_local_execution",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_filter_collect",
        "filter_predicate",
        sql_surface="ctx.sql(\"SELECT * FROM 'orders.vortex' WHERE value >= 3\").collect()",
        python_surface="read_vortex('orders.vortex').filter('gte:value:3').collect()",
        dataframe_surface="read_vortex('orders.vortex').where(col('value') >= 3).collect()",
        context_surface="ctx.read_vortex('orders.vortex').filter('gte:value:3').collect()",
        session_surface="session.read_vortex('orders.vortex').filter('gte:value:3').collect()",
        cli_command="vortex-filter",
        cli_args_template=(
            "vortex-filter <dataset.vortex> <tiny-predicate> --execute-local-primitive "
            "<memory_gb> <max_parallelism> --format json"
        ),
        required_evidence=(
            "vortex_filter",
            "filter_local_execution",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_filter_limit_collect",
        "filter_predicate_source_order_limit",
        sql_surface=(
            "ctx.sql(\"SELECT * FROM 'orders.vortex' WHERE value >= 3 LIMIT 5\").collect()"
        ),
        python_surface="read_vortex('orders.vortex').filter('gte:value:3').limit(5).collect()",
        dataframe_surface=(
            "read_vortex('orders.vortex').where(col('value') >= 3).limit(5).collect()"
        ),
        context_surface="ctx.read_vortex('orders.vortex').filter('gte:value:3').limit(5).collect()",
        session_surface=(
            "session.read_vortex('orders.vortex').filter('gte:value:3').limit(5).collect()"
        ),
        cli_command="vortex-filter",
        cli_args_template=(
            "vortex-filter <dataset.vortex> <tiny-predicate> --limit <n> "
            "--execute-local-primitive <memory_gb> <max_parallelism> --format json"
        ),
        supports_source_order_limit=True,
        required_evidence=(
            "vortex_filter_limit",
            "filter_local_execution",
            "source_order_limit",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_project_collect",
        "project_columns",
        sql_surface="ctx.sql(\"SELECT metric FROM 'orders.vortex'\").collect()",
        python_surface="read_vortex('orders.vortex').select('metric').collect()",
        dataframe_surface="read_vortex('orders.vortex').select('metric').collect()",
        context_surface="ctx.read_vortex('orders.vortex').select('metric').collect()",
        session_surface="session.read_vortex('orders.vortex').select('metric').collect()",
        cli_command="vortex-project",
        cli_args_template=(
            "vortex-project <dataset.vortex> <columns> --execute-local-primitive "
            "<memory_gb> <max_parallelism> --format json"
        ),
        required_evidence=(
            "vortex_project",
            "project_local_execution",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_project_limit_collect",
        "project_columns_source_order_limit",
        sql_surface="ctx.sql(\"SELECT metric FROM 'orders.vortex' LIMIT 5\").collect()",
        python_surface="read_vortex('orders.vortex').select('metric').limit(5).collect()",
        dataframe_surface="read_vortex('orders.vortex').select('metric').limit(5).collect()",
        context_surface="ctx.read_vortex('orders.vortex').select('metric').limit(5).collect()",
        session_surface="session.read_vortex('orders.vortex').select('metric').limit(5).collect()",
        cli_command="vortex-project",
        cli_args_template=(
            "vortex-project <dataset.vortex> <columns> --limit <n> --execute-local-primitive "
            "<memory_gb> <max_parallelism> --format json"
        ),
        supports_source_order_limit=True,
        required_evidence=(
            "vortex_project_limit",
            "project_local_execution",
            "source_order_limit",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_select_star_limit_collect",
        "select_star_source_order_limit",
        sql_surface="ctx.sql(\"SELECT * FROM 'orders.vortex' LIMIT 5\").collect()",
        python_surface="read_vortex('orders.vortex').select('*').limit(5).collect()",
        dataframe_surface="read_vortex('orders.vortex').select('*').limit(5).collect()",
        context_surface="ctx.read_vortex('orders.vortex').select('*').limit(5).collect()",
        session_surface="session.read_vortex('orders.vortex').select('*').limit(5).collect()",
        cli_command="vortex-project",
        cli_args_template=(
            "vortex-project <dataset.vortex> '*' --limit <n> --execute-local-primitive "
            "<memory_gb> <max_parallelism> --format json"
        ),
        supports_source_order_limit=True,
        required_evidence=(
            "vortex_project_star_limit",
            "project_local_execution",
            "source_order_limit",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_filter_project_collect",
        "filter_and_project",
        sql_surface=(
            "ctx.sql(\"SELECT metric FROM 'orders.vortex' WHERE value >= 3\").collect()"
        ),
        python_surface="read_vortex('orders.vortex').filter('gte:value:3').select('metric').collect()",
        dataframe_surface=(
            "read_vortex('orders.vortex').where(col('value') >= 3).select('metric').collect()"
        ),
        context_surface=(
            "ctx.read_vortex('orders.vortex').filter('gte:value:3').select('metric').collect()"
        ),
        session_surface=(
            "session.read_vortex('orders.vortex').filter('gte:value:3').select('metric').collect()"
        ),
        cli_command="vortex-filter-project",
        cli_args_template=(
            "vortex-filter-project <dataset.vortex> <tiny-predicate> <columns> "
            "--execute-local-primitive <memory_gb> <max_parallelism> --format json"
        ),
        required_evidence=(
            "vortex_filter_project",
            "filter_project_local_execution",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
    _local_vortex_primitive_route(
        "vortex_filter_project_limit_collect",
        "filter_and_project_source_order_limit",
        sql_surface=(
            "ctx.sql(\"SELECT metric FROM 'orders.vortex' WHERE value >= 3 LIMIT 5\").collect()"
        ),
        python_surface=(
            "read_vortex('orders.vortex').filter('gte:value:3').select('metric').limit(5).collect()"
        ),
        dataframe_surface=(
            "read_vortex('orders.vortex').where(col('value') >= 3).select('metric').limit(5).collect()"
        ),
        context_surface=(
            "ctx.read_vortex('orders.vortex').filter('gte:value:3').select('metric').limit(5).collect()"
        ),
        session_surface=(
            "session.read_vortex('orders.vortex').filter('gte:value:3').select('metric').limit(5).collect()"
        ),
        cli_command="vortex-filter-project",
        cli_args_template=(
            "vortex-filter-project <dataset.vortex> <tiny-predicate> <columns> --limit <n> "
            "--execute-local-primitive <memory_gb> <max_parallelism> --format json"
        ),
        supports_source_order_limit=True,
        required_evidence=(
            "vortex_filter_project_limit",
            "filter_project_local_execution",
            "source_order_limit",
            "execution_certificate",
            "native_io_certificate",
        ),
    ),
)

DATAFRAME_METHOD_CAPABILITY_ROWS: tuple[DataFrameMethodCapability, ...] = (
    _df_method(
        "read_vortex",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_csv",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_json",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_parquet",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_arrow_ipc",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_avro",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "read_orc",
        "source",
        "source_declaration_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "from_rows",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "range",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "literal_table",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "calendar",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "sequence",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "sql_values",
        "sql_frontend",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "sql_parser",
            "sql_binder",
            "sql_planner",
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "sql_literal_select",
        "sql_frontend",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "sql_parser",
            "sql_binder",
            "sql_planner",
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "dataframe_source_free_projection",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "dataframe_literal_projection_contract",
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "dataframe_generated_with_column",
        "source_free_generation",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        required_evidence=(
            "generated_row_literal_projection",
            "range_projection_expression_semantics",
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _df_method(
        "object_store_generated_output",
        "output",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "generated_source_certificate",
            "object_store_write_smoke",
            "object_store_write_policy",
            "output_commit_protocol",
            "no_fallback_evidence",
        ),
        claim_boundary="scoped local-emulator generated-output object-store fixture only; no live S3/GCS/ADLS, lakehouse table commit, production, or performance claim",
    ),
    _df_method(
        "foundry_generated_output",
        "platform",
        "fixture_smoke_supported",
        runtime_execution=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "generated_source_certificate",
            "foundry_style_result_dataset",
            "foundry_style_evidence_dataset",
            "foundry_output_api_invoked_false",
            "foundry_spark_invoked_false",
            "no_fallback_evidence",
        ),
        claim_boundary="local Foundry-style generated-output dataset proof only; no real Foundry runtime, output API, package, production, Marketplace, Spark, object-store, or performance claim",
    ),
    _df_method(
        "filter",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "where",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "select",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "project",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=(
            "Alias for select(...). Plan declaration only until an admitted collect/write/"
            "materialization terminal runs through ShardLoom runtime evidence."
        ),
    ),
    _df_method(
        "limit",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "with_column",
        "expression",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "expression_engine",
            "computed_projection_evidence",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "with_columns",
        "expression",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "expression_engine",
            "computed_projection_evidence",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=(
            "Alias over repeated with_column(...) calls for admitted scoped local-source "
            "computed projections and source-free generated rows/ranges. No broad expression "
            "runtime, external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "assign",
        "expression",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "expression_engine",
            "computed_projection_evidence",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=(
            "Pandas-style alias for with_columns(...), preserving the same admitted "
            "ShardLoom computed-projection runtime boundaries and no-fallback evidence."
        ),
    ),
    _df_method(
        "join",
        "join",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "join_operator",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "group_by",
        "aggregation",
        "lazy_group_handle_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "groupby",
        "aggregation",
        "lazy_group_handle_supported",
        claim_boundary="Alias for group_by(...). Lazy grouped handle only until agg/aggregate/count terminal lowering is admitted.",
    ),
    _df_method(
        "agg",
        "aggregation",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "aggregate_operator",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "aggregate",
        "aggregation",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "aggregate_operator",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "sort",
        "ordering",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "sort_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "order_by",
        "ordering",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "sort_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="Alias for sort(...), preserving scoped local-source top-N/order-by runtime evidence and no-fallback boundaries.",
    ),
    _df_method(
        "sort_by",
        "ordering",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "sort_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="Alias for sort(...), preserving scoped local-source top-N/order-by runtime evidence and no-fallback boundaries.",
    ),
    _df_method(
        "sort_values",
        "ordering",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "sort_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="Pandas-style alias for sort(...), preserving scoped local-source top-N/order-by runtime evidence and no-fallback boundaries.",
    ),
    _df_method(
        "distinct",
        "deduplication",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "distinct_projection_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="Scoped row-level duplicate removal over bounded local-source projection, aggregate/HAVING, window, and join output rows; broader arbitrary DISTINCT grammar remains explicitly gated.",
    ),
    _df_method(
        "drop_duplicates",
        "deduplication",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "distinct_projection_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="Pandas-style alias for distinct(), preserving scoped row-level SELECT DISTINCT evidence and no-fallback boundaries.",
    ),
    _df_method(
        "unique",
        "deduplication",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "distinct_projection_operator",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary="DataFrame-style alias for distinct(), preserving scoped row-level SELECT DISTINCT evidence and no-fallback boundaries.",
    ),
    _df_method(
        "window",
        "window",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "window_operator",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _df_method(
        "schema_contract",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "schema_validation",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Alias for validate_schema(...): exact bounded schema validation over admitted "
            "local-source SQL/Python/DataFrame workflows. No broad schema registry, object-store/"
            "table enforcement, production contract management, external engine, or fallback claim."
        ),
    ),
    _df_method(
        "schema",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "schema_discovery",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "describe_schema",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "schema_discovery",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "validate_schema",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "schema_validation",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "data_quality_check",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "data_quality_runtime",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "data_quality",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "data_quality_runtime",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "data_quality_summary",
        "schema_quality",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "data_quality_runtime",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "collect",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "vortex_local_primitive_runtime",
            "materialization_boundary",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local CSV and flat JSONL/NDJSON projection/optional-filter/limit, "
            "scalar aggregate, multi-key group-by aggregate, single-key top-N, "
            "and scoped local-source join collect smoke only. For Vortex sources, collect "
            "is admitted only for scoped local primitive reports for filter, project, and "
            "filter-project with optional source-order limit, not decoded row materialization. "
            "No broad DataFrame runtime, object-store/table source, external engine, fallback, "
            "or production claim."
        ),
    ),
    _df_method(
        "count",
        "aggregation",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "vortex_local_primitive_runtime",
            "execution_certificate",
            "native_io_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        claim_boundary=(
            "Scoped local-source count runtime for admitted SQL local-source query-builder "
            "shapes plus scoped local Vortex count/count-where primitive runtime. Vortex count "
            "uses ShardLoom's explicit Vortex primitive commands backed by upstream Vortex "
            "scan/read APIs. No broad SQL/DataFrame runtime, decoded row materialization, "
            "object-store/table source, external engine, fallback, or production performance claim."
        ),
    ),
    _df_method(
        "write",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "local_jsonl_csv_or_feature_gated_structured_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local CSV and flat JSONL/NDJSON projection/optional-filter/limit, "
            "scalar aggregate, multi-key group-by aggregate, single-key top-N, "
            "and scoped local-source join JSONL/CSV and feature-gated flat "
            "scalar Parquet/Arrow IPC/Avro/ORC output smoke only; no broad DataFrame "
            "runtime, object-store/table sink, "
            "external engine, fallback, fanout, or production claim."
        ),
    ),
    _df_method(
        "write_jsonl",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "local_jsonl_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Alias over scoped local JSONL output smokes for admitted local CSV and flat "
            "JSONL/NDJSON workflows; no broad DataFrame runtime, object-store/table sink, "
            "external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "write_csv",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "local_csv_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Alias over scoped local CSV output smokes for admitted local CSV and flat "
            "JSONL/NDJSON workflows; no broad DataFrame runtime, object-store/table sink, "
            "external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "fanout",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "local_output_fanout",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local-source SQL query-builder fanout smoke for admitted local JSONL/CSV "
            "and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC/Vortex output targets "
            "only; scoped replay/fidelity evidence is local-artifact proof, not broad writer "
            "fidelity, object-store/table sink, external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "to_pandas",
        "materialization",
        "optional_dependency_runtime_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "decoded_materialization_policy",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow",
        "materialization",
        "optional_dependency_runtime_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "arrow_interop_boundary",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow_table",
        "materialization",
        "optional_dependency_runtime_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "arrow_interop_boundary",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow_ipc",
        "materialization",
        "optional_dependency_runtime_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "arrow_interop_boundary",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_numpy",
        "materialization",
        "optional_dependency_runtime_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "decoded_materialization_policy",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_python_objects",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "materialization_boundary",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "preview",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "materialization_boundary",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local CSV and flat JSONL/NDJSON preview/select-star limit smoke only; "
            "no notebook display, broad DataFrame runtime, object-store/table source, "
            "external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "head",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "materialization_boundary",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Alias over the scoped local CSV and flat JSONL/NDJSON preview/select-star "
            "limit smoke; no decoded row-object materialization, broad DataFrame runtime, "
            "object-store/table source, external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "take",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "materialization_boundary",
            "execution_certificate",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Alias over the scoped local CSV and flat JSONL/NDJSON preview/select-star "
            "limit smoke; no decoded row-object materialization, broad DataFrame runtime, "
            "object-store/table source, external engine, fallback, or production claim."
        ),
    ),
    _df_method(
        "display",
        "materialization",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "notebook_display_contract",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "write_vortex",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "feature_gated_local_vortex_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
            "upstream_vortex_write_called",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Feature-gated flat scalar local Vortex output smoke for admitted local-source "
            "query-builder workflows only; local reopen/replay evidence is scoped and not a broad "
            "Vortex writer, object-store/table commit, external engine, fallback, fanout claim "
            "beyond scoped local smoke, or production claim."
        ),
    ),
    _df_method(
        "write_parquet",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "feature_gated_local_parquet_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Feature-gated flat scalar local Parquet output smoke for admitted local-source "
            "query-builder workflows only; no broad Parquet type/nesting, metadata-fidelity, "
            "object-store/table, external engine, fallback, fanout, or production claim."
        ),
    ),
    _df_method(
        "write_arrow_ipc",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "feature_gated_local_arrow_ipc_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Feature-gated flat scalar local Arrow IPC output smoke for admitted local-source "
            "query-builder workflows only; no zero-copy, nested type, object-store/table, "
            "external engine, fallback, fanout, or production claim."
        ),
    ),
    _df_method(
        "write_avro",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "feature_gated_local_avro_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Feature-gated flat scalar local Avro output smoke for admitted local-source "
            "query-builder workflows only; no schema-evolution/logical-type completeness, "
            "object-store/table, external engine, fallback, fanout, or production claim."
        ),
    ),
    _df_method(
        "write_orc",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "feature_gated_local_orc_output",
            "output_native_io_certificate",
            "result_replay_verified",
            "output_fidelity_report_status",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Feature-gated flat scalar local ORC output smoke for admitted local-source "
            "query-builder workflows only; no stripe/statistics runtime, object-store/table, "
            "external engine, fallback, fanout, or production claim."
        ),
    ),
    _df_method(
        "quarantine",
        "write",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "quarantine_policy",
            "local_quarantine_sink_write_evidence",
            "output_native_io_certificate",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local-source quarantine only. Bounded runtime rows are classified through "
            "ShardLoom local-source evidence; pushdownable not-null quarantine rows can be written "
            "to admitted local sinks through sql-local-source-smoke. Broad quarantine policy, "
            "object-store/table quarantine, unique-check sink pushdown, production remediation, "
            "and performance claims remain blocked."
        ),
    ),
    _df_method(
        "from_pandas",
        "input_boundary",
        "materialized_input_boundary_supported",
        required_evidence=(
            "materialized_input_boundary",
            "generated_source_user_rows",
            "input_fidelity_boundary",
            "no_fallback_evidence",
        ),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "from_arrow_table",
        "input_boundary",
        "materialized_input_boundary_supported",
        required_evidence=(
            "materialized_input_boundary",
            "generated_source_user_rows",
            "arrow_interop_boundary",
            "input_fidelity_boundary",
            "no_fallback_evidence",
        ),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "from_arrow_ipc",
        "input_boundary",
        "optional_dependency_input_boundary_supported",
        required_evidence=(
            "materialized_input_boundary",
            "generated_source_user_rows",
            "arrow_interop_boundary",
            "optional_dependency_policy",
            "input_fidelity_boundary",
            "no_fallback_evidence",
        ),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "sql",
        "sql_frontend",
        "fixture_smoke_supported",
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "sql_frontend_runtime_ladder",
            "sql_local_source_smoke",
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Scoped ctx.sql local-source collect/write and source-free generated-output write "
            "smokes only; broad parse/bind/plan/execute SQL, catalogs, object-store/table SQL, "
            "external engine, fallback, production SQL/DataFrame, and performance claims remain "
            "blocked."
        ),
    ),
    _df_method(
        "profile",
        "observability",
        "fixture_smoke_supported",
        required_evidence=(
            "sql_local_source_smoke",
            "bounded_inline_jsonl_result",
            "profile_runtime",
            "schema_observability",
            "no_fallback_evidence",
        ),
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        claim_boundary=(
            "Scoped local-source bounded runtime profile only. The profile reuses the admitted "
            "sql-local-source-smoke path and reports inline JSONL materialization, schema/null-count "
            "observability, and no-fallback evidence. Broad runtime profiling, resource tracing, "
            "quarantine output, production observability, and performance claims remain blocked."
        ),
    ),
)


FRONT_DOOR_PARITY_ROWS: tuple[FrontDoorParityRow, ...] = (
    _front_door_row(
        "local_file_filter_project_limit",
        "local file read, filter, project, distinct, limit, collect, and local write",
        "scoped_runtime_supported",
        sql_surface="ctx.sql(\"SELECT [DISTINCT] ... FROM 'local.csv' WHERE ... LIMIT ...\").collect()/write_*",
        python_surface="ctx.sql(...), ctx.read(...), LazyFrame.collect(), LazyFrame.write_*",
        dataframe_surface="ctx.read_csv(...).filter(...).select(...).distinct().limit(...).collect()/write_*",
        shared_runtime_path="sql-local-source-smoke",
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_path_no_benchmark_claim",
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "sql_local_source_smoke",
            "python_query_builder_tests",
            "execution_certificate",
            "native_io_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "SQL, Python, and DataFrame-style local file filter/project/distinct/limit workflows "
            "lower to the same ShardLoom local-source runtime surface. Local compatibility inputs "
            "are adapters that must expose their adapter-to-Vortex normalization boundary before "
            "broad runtime-ready claims. This does not claim arbitrary SQL, remote/table sources, "
            "or benchmarked performance equivalence."
        ),
    ),
    _front_door_row(
        "local_file_join_aggregate_sort_window",
        "local file joins, grouped/scalar aggregates, top-N, computed columns, and windows",
        "scoped_runtime_supported",
        sql_surface="ctx.sql(\"SELECT ... JOIN/GROUP BY/ORDER BY/window ... FROM 'local.csv'\")",
        python_surface="ctx.sql(...), LazyFrame.join/group_by/agg/sort/window",
        dataframe_surface="ctx.read(...).join(...).group_by(...).agg(...).sort(...).window(...)",
        shared_runtime_path="sql-local-source-smoke",
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_path_no_benchmark_claim",
        runtime_execution=True,
        data_read=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "sql_local_source_smoke",
            "join_operator",
            "aggregate_operator",
            "sort_operator",
            "window_operator",
            "python_query_builder_tests",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Scoped local SQL and DataFrame-style expressions share ShardLoom's local-source "
            "runtime for admitted join, aggregate, sort, computed-column, and window shapes. "
            "The route must become explicit about its Vortex-normalized execution boundary before "
            "broader runtime claims. Unsupported SQL grammar, arbitrary expressions, remote "
            "sources, and production semantic completeness remain outside this row."
        ),
    ),
    _front_door_row(
        "generated_source_output",
        "source-free generated rows, ranges, sequences, SQL VALUES, and literal projections",
        "scoped_runtime_supported",
        sql_surface="ctx.sql_values(...), ctx.sql_literal_select(...), ctx.sql(...).write_*",
        python_surface="ctx.from_rows(...), ctx.range(...), ctx.sequence(...), ctx.calendar(...)",
        dataframe_surface="ctx.dataframe_source_free_projection(...), ctx.dataframe_generated_with_column(...)",
        shared_runtime_path="generated-source-* smoke family",
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_family_no_benchmark_claim",
        runtime_execution=True,
        write_io=True,
        materialization_required=True,
        required_evidence=(
            "generated_source_user_rows_smoke",
            "generated_source_range_smoke",
            "generated_source_sequence_smoke",
            "generated_source_sql_smoke",
            "output_native_io_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Generated SQL, Python, and DataFrame-style source-free workflows are admitted for "
            "local output smokes. Generated rows are an input adapter and must re-enter through a "
            "Vortex-preparable route for runtime-ready claims. This is generated-output parity, "
            "not broad SQL/DataFrame runtime or remote sink support."
        ),
    ),
    _front_door_row(
        "schema_quality_preview",
        "schema inspection, validation, data-quality summaries, preview/head/take",
        "scoped_runtime_supported",
        sql_surface="ctx.sql(...).schema/validate_schema/data_quality/preview/head/take",
        python_surface="LazyFrame.schema/validate_schema/data_quality/preview/head/take",
        dataframe_surface="DataFrame-style schema/data-quality/preview helpers",
        shared_runtime_path="sql-local-source-smoke inline bounded result",
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_path_no_benchmark_claim",
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        required_evidence=(
            "sql_schema_quality_surface",
            "schema_report_contract",
            "data_quality_report_contract",
            "front_door_equivalence_tests",
        ),
        claim_boundary=(
            "Scoped local SQL, Python, and DataFrame-style schema/data-quality/preview helpers "
            "share the sql-local-source-smoke inline bounded-result path. This is not broad SQL "
            "grammar, object-store/table schema discovery, notebook display, or benchmark-backed "
            "performance equivalence."
        ),
    ),
    _front_door_row(
        "local_vortex_primitive_runtime",
        "local Vortex count, count-where, filter, project, and filter-project primitive reports",
        "scoped_runtime_supported",
        sql_surface=(
            "ctx.sql(\"SELECT COUNT(*)/columns FROM 'local.vortex' WHERE ... LIMIT ...\").collect()"
        ),
        python_surface="ctx.read_vortex(...).count/filter/select/collect scoped primitive reports",
        dataframe_surface="read_vortex(...).filter/select/count/collect scoped primitive reports",
        shared_runtime_path=(
            "vortex-run/vortex-count-where/vortex-filter/vortex-project/vortex-filter-project"
        ),
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_family_no_benchmark_claim",
        runtime_execution=True,
        data_read=True,
        required_evidence=(
            "vortex_local_primitive_runtime",
            "sql_vortex_primitive_tests",
            "python_query_builder_tests",
            "execution_certificate",
            "native_io_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Scoped SQL, Python, and DataFrame-style local Vortex primitive report workflows "
            "share ShardLoom's explicit Vortex primitive command family for count, count-where, "
            "filter, project, and filter-project with optional source-order limit. Native "
            "`.vortex` input is already at the Vortex boundary, so this row is the direct "
            "Vortex-normalized case. This is not decoded row materialization, broad Vortex "
            "read-transform-write parity, object-store runtime, or benchmark-backed performance "
            "equivalence."
        ),
    ),
    _front_door_row(
        "native_vortex_general_runtime",
        "general Vortex-native read, transform, and write workflows",
        "broad_runtime_expansion_pending",
        runtime_gap_status="front_door_connection_pending",
        sql_surface="scoped SQL local Vortex primitive reports supported; broad Vortex SQL is tracked in GAR-RUNTIME-IMPL-6D",
        python_surface="ctx.read_vortex(...).count/filter/select/limit scoped local primitive reports; broad workflow expansion is tracked in GAR-RUNTIME-IMPL-6D",
        dataframe_surface="read_vortex(...).filter/select/count/limit/collect scoped primitive reports; broader read-transform-write expansion is tracked in GAR-RUNTIME-IMPL-6D",
        shared_runtime_path="scoped Vortex local primitive runtime plus GAR-RUNTIME-IMPL-6D runtime expansion checklist",
        parity_status="front_door_gap",
        performance_equivalence_status="not_claim_grade",
        blocker_id="cg19.cg21.general_vortex_front_door_runtime_missing",
        required_evidence=(
            "vortex_input_normalization_boundary",
            "vortex_reader_runtime",
            "vortex_writer_runtime",
            "operator_kernel_coverage",
            "execution_certificate",
            "native_io_certificate",
            "front_door_equivalence_benchmarks",
        ),
        claim_boundary=(
            "Scoped SQL/Python/DataFrame-style local Vortex count/filter/project/filter-project "
            "primitive reports execute through ShardLoom's Vortex primitive runtime, but broad "
            "intuitive SQL/Python/DataFrame Vortex read-transform-write parity with equivalent "
            "runtime and performance evidence is tracked as a runtime expansion checklist item."
        ),
    ),
    _front_door_row(
        "decoded_materialization_interop",
        "pandas, Arrow table, Arrow IPC, NumPy, and notebook display materialization",
        "scoped_runtime_supported",
        sql_surface="ctx.sql(...).to_python_objects/to_pandas/to_arrow/to_numpy/display",
        python_surface="from_pandas/from_arrow_table/from_arrow_ipc and LazyFrame to_* helpers",
        dataframe_surface="DataFrame-style bounded materialization and notebook preview helpers",
        shared_runtime_path=(
            "sql-local-source-smoke inline bounded result; generated-source user rows for "
            "materialized inputs"
        ),
        parity_status="equivalent_admitted_scope",
        performance_equivalence_status="same_runtime_path_no_benchmark_claim",
        runtime_execution=True,
        data_read=True,
        materialization_required=True,
        required_evidence=(
            "decoded_materialization_policy",
            "arrow_interop_boundary",
            "bounded_materialization_runtime",
            "notebook_display_contract",
            "no_fallback_evidence",
            "optional_dependency_policy",
        ),
        claim_boundary=(
            "Decoded Python/Arrow/NumPy interop is admitted only for bounded local-source "
            "ShardLoom results and explicit materialized input snapshots. Optional packages are "
            "containers or compatibility encoders, not execution engines; materialized snapshots "
            "must re-enter a Vortex-preparable route before runtime-ready claims. This row does "
            "not claim object-store/table materialization, arbitrary SQL/DataFrame breadth, or "
            "benchmark-backed performance equivalence."
        ),
    ),
    _front_door_row(
        "object_store_lakehouse_catalog",
        "object-store, lakehouse/table, catalog, commit, and remote sink workflows",
        "production_io_runtime_expansion_pending",
        runtime_gap_status="runtime_expansion_pending",
        sql_surface="remote/table SQL runtime expansion is tracked in GAR-RUNTIME-IMPL-6D",
        python_surface="object-store/table helper smokes and plans only",
        dataframe_surface="DataFrame remote/table read/write runtime expansion is tracked in GAR-RUNTIME-IMPL-6D",
        shared_runtime_path="object-store/table planning surfaces plus GAR-RUNTIME-IMPL-6D runtime expansion checklist",
        parity_status="front_door_gap",
        performance_equivalence_status="not_claim_grade",
        blocker_id="cg9.cg10.cg21.production_io_front_door_missing",
        required_evidence=(
            "vortex_input_normalization_boundary",
            "object_store_runtime",
            "credential_policy",
            "catalog_table_runtime",
            "commit_protocol",
            "retry_recovery_evidence",
            "front_door_equivalence_tests",
        ),
        claim_boundary=(
            "Local object-store/table smokes and plans do not yet certify broad remote/table SQL, "
            "Python, or DataFrame workflows; each route must identify its object-source to "
            "Vortex-normalized execution boundary, and that runtime expansion is explicitly "
            "queued in GAR-RUNTIME-IMPL-6D."
        ),
    ),
    _front_door_row(
        "arbitrary_sql_python_dataframe_breadth",
        "arbitrary user SQL, Python expressions, DataFrame APIs, UDFs, and effects",
        "broad_language_runtime_expansion_pending",
        runtime_gap_status="front_door_connection_pending",
        sql_surface="broad SQL parse/bind/plan/execute expansion is tracked in GAR-RUNTIME-IMPL-6D",
        python_surface="arbitrary Python function/UDF/effect runtime expansion is tracked in GAR-RUNTIME-IMPL-6D",
        dataframe_surface="full DataFrame API parity expansion is tracked in GAR-RUNTIME-IMPL-6D",
        shared_runtime_path="capability reports plus GAR-RUNTIME-IMPL-6D runtime expansion checklist",
        parity_status="front_door_gap",
        performance_equivalence_status="not_claim_grade",
        blocker_id="cg20.cg21.broad_language_surface_missing",
        required_evidence=(
            "vortex_input_normalization_boundary",
            "sql_grammar_coverage",
            "expression_kernel_registry",
            "udf_effect_policy",
            "semantic_conformance_suite",
            "front_door_equivalence_tests",
            "benchmark_evidence",
        ),
        claim_boundary=(
            "The broad 'build anything' claim remains not-claim-grade until SQL, Python, "
            "DataFrame, function/UDF, semantic conformance, and benchmark evidence converge on "
            "the same Vortex-normalized ShardLoom-native execution plan through "
            "GAR-RUNTIME-IMPL-6D."
        ),
    ),
    _front_door_row(
        "performance_equivalence",
        "same-result and same-performance expectation across SQL, Python, and DataFrame front doors",
        "benchmark_publication_pending",
        runtime_gap_status="benchmark_publication_pending",
        sql_surface="not benchmark-certified against equivalent Python/DataFrame workflows",
        python_surface="not benchmark-certified against equivalent SQL/DataFrame workflows",
        dataframe_surface="not benchmark-certified against equivalent SQL/Python workflows",
        shared_runtime_path="GAR-RUNTIME-IMPL-6D benchmark publication and execution-certificate evidence required",
        parity_status="front_door_gap",
        performance_equivalence_status="not_claim_grade",
        blocker_id="cg6.front_door_performance_equivalence_benchmark_missing",
        required_evidence=(
            "vortex_input_normalization_boundary",
            "front_door_equivalent_workload_manifest",
            "correctness_evidence",
            "benchmark_manifest",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Shared runtime paths support a scoped expectation that overhead should converge, but "
            "performance equivalence is not claim-grade until equivalent front-door benchmarks "
            "show the same Vortex-normalized runtime boundary and are published and reproducible."
        ),
    ),
)


_ALL_USER_FRONT_DOORS = ("SQL", "Python", "DataFrame", "context", "session", "CLI")
_PYTHON_FRONT_DOORS = ("Python", "DataFrame", "context", "session")

_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY = (
    "Scoped local compatibility-file benchmark scenario route: raw local CSV/JSONL/Parquet/"
    "Arrow IPC/Avro/ORC fixture inputs enter SourceState, prepare through vortex_ingest into "
    "VortexPreparedState, execute through ShardLoom prepared/native benchmark runtime, and emit "
    "local result/evidence artifacts with no external engine fallback. This is not broad arbitrary "
    "SQL/Python/DataFrame support, object-store/table runtime, production readiness, performance "
    "superiority, or Spark replacement."
)
_LOCAL_FILE_DIRECT_BENCHMARK_BOUNDARY = (
    "Scoped direct local compatibility-file route: raw local CSV/JSONL and feature-gated flat "
    "scalar compatibility formats enter SourceState and execute through ShardLoom's local-source "
    "runtime with transient Vortex-preparable arrays. This is not Vortex-native persistence, broad "
    "SQL/Python/DataFrame support, production readiness, performance superiority, or fallback."
)

LOCAL_FILE_BENCHMARK_ROUTE_ROWS: tuple[LocalFileBenchmarkRouteRow, ...] = (
    _local_file_benchmark_route(
        "selective_filter",
        "selective filter",
        "local_analytics",
        "scan_and_pruning",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "null_heavy",
            "partitioned_by_date",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_direct_transient_route",
        route_display_name="ShardLoom Direct Transient Route",
        alternate_route_ids=(
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
        ),
        selected_execution_mode="direct_compatibility_transient",
        sql_surface="ctx.sql(\"SELECT SUM(metric) FROM 'fact.csv' WHERE flag = true\").collect()",
        python_surface="ctx.read('fact.csv').filter(sl.col('flag') == True).agg(sum_metric=('metric', 'sum')).collect()",
        dataframe_surface="ctx.read('fact.csv').where(sl.col('flag') == True).agg(sum_metric=('metric', 'sum')).collect()",
        context_surface="ctx.read('fact.csv').filter(sl.col('flag') == True).collect()",
        session_surface="session.read('fact.csv').filter(sl.col('flag') == True).collect()",
        cli_surface="shardloom sql-local-source-smoke \"SELECT SUM(metric) FROM 'fact.csv' WHERE flag = true\" --format json",
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="direct_compatibility_transient_no_persistent_preparation",
        output_route="bounded report, local compatibility output, or feature-gated local Vortex sink",
        evidence_route="sql-local-source-smoke envelope, execution certificate, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="bounded decoded preview or explicit local sink boundary only",
        route_runtime_status="scoped_runtime_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.selective_filter",
        required_evidence=(
            "sql_local_source_smoke",
            "traditional_analytics.direct_compatibility_transient.selective_filter",
            "no_fallback_evidence",
        ),
        next_verifier="python3 scripts/check_user_route_capability_report.py --output target/user-route-capability-report.json",
        claim_boundary=_LOCAL_FILE_DIRECT_BENCHMARK_BOUNDARY,
        vortex_normalization_point=(
            "local compatibility source -> SourceState -> transient Vortex-preparable arrays; "
            "prepared routes are available when persistence/reuse is requested"
        ),
    ),
    _local_file_benchmark_route(
        "filter_projection_limit",
        "filter + projection + limit",
        "local_analytics",
        "scan_and_pruning",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "wide_table",
            "very_wide_table",
            "null_heavy",
            "partitioned_by_date",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_direct_transient_route",
        route_display_name="ShardLoom Direct Transient Route",
        alternate_route_ids=(
            "local_file_prepare_once_first_query",
            "local_file_prepare_once_batch",
        ),
        selected_execution_mode="direct_compatibility_transient",
        sql_surface="ctx.sql(\"SELECT id, metric FROM 'fact.csv' WHERE metric >= 10 ORDER BY id LIMIT 100\").collect()",
        python_surface="ctx.read('fact.csv').filter(sl.col('metric') >= 10).select('id', 'metric').limit(100).collect()",
        dataframe_surface="ctx.read('fact.csv').where(sl.col('metric') >= 10).select('id', 'metric').limit(100).collect()",
        context_surface="ctx.read('fact.csv').select('id', 'metric').limit(100).collect()",
        session_surface="session.read('fact.csv').select('id', 'metric').limit(100).collect()",
        cli_surface="shardloom sql-local-source-smoke \"SELECT id, metric FROM 'fact.csv' WHERE metric >= 10 LIMIT 100\" --format json",
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="direct_compatibility_transient_no_persistent_preparation",
        output_route="bounded report, local compatibility output, or feature-gated local Vortex sink",
        evidence_route="sql-local-source-smoke envelope, execution certificate, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="bounded decoded preview or explicit local sink boundary only",
        route_runtime_status="scoped_runtime_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.filter_projection_limit",
        required_evidence=(
            "sql_local_source_smoke",
            "traditional_analytics.direct_compatibility_transient.filter_projection_limit",
            "no_fallback_evidence",
        ),
        next_verifier="python3 scripts/check_user_route_capability_report.py --output target/user-route-capability-report.json",
        claim_boundary=_LOCAL_FILE_DIRECT_BENCHMARK_BOUNDARY,
        vortex_normalization_point=(
            "local compatibility source -> SourceState -> transient Vortex-preparable arrays; "
            "prepared routes are available when persistence/reuse is requested"
        ),
    ),
    _local_file_benchmark_route(
        "group_by_aggregation",
        "group by aggregation",
        "local_analytics",
        "aggregation",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "null_heavy",
            "partitioned_by_date",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query(\"SELECT group_key, SUM(metric) FROM fact GROUP BY group_key\").collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('group_by_aggregation').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().group_by('group_key').agg(total=('metric', 'sum')).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('group_by_aggregation')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('group_by_aggregation').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run group_by_aggregation fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared query result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, route timing, execution certificate, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="decode/materialization only after prepared query output or sink is declared",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.group_by_aggregation",
        required_evidence=(
            "traditional_analytics.prepared_native.group_by_aggregation",
            "VortexPreparedState",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "multi_key_group_by",
        "multi-key group by",
        "local_analytics",
        "aggregation",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "high_cardinality_strings",
            "null_heavy",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query(\"SELECT group_key, category, SUM(metric) FROM fact GROUP BY group_key, category\").collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('multi_key_group_by').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().group_by('group_key', 'category').agg(total=('metric', 'sum')).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('multi_key_group_by')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('multi_key_group_by').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run multi_key_group_by fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared query result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, route timing, execution certificate, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="decode/materialization only after prepared query output or sink is declared",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.multi_key_group_by",
        required_evidence=(
            "traditional_analytics.prepared_native.multi_key_group_by",
            "VortexPreparedState",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "join_aggregate",
        "join + aggregate",
        "local_analytics",
        "joins",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "partitioned_by_date",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('join_aggregate').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('join_aggregate').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare(dim='dim.csv').join('dim').group_by('dim_label').agg(total=('metric', 'sum')).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('join_aggregate')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('join_aggregate').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run join_aggregate fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local fact/dimension compatibility source adapters",
        preparation_route="vortex_ingest_prepare_once_for_fact_and_dimension",
        output_route="prepared join aggregate result, bounded report, or local result sink",
        evidence_route="prepared fact/dim evidence, route timing, execution certificate, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="join residual state stays ShardLoom-native; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.join_aggregate",
        required_evidence=(
            "traditional_analytics.prepared_native.join_aggregate",
            "prepared_fact_and_dimension",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "sort_top_k",
        "sort and top-k",
        "local_analytics",
        "sort_and_window",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "wide_table",
            "very_wide_table",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query(\"SELECT id, metric FROM fact ORDER BY metric DESC LIMIT 10\").collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('sort_top_k').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().sort('metric', descending=True).limit(10).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('sort_top_k')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('sort_top_k').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run sort_top_k fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared top-k result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, ShardLoom native top-k residual evidence, route timing, and no-fallback evidence",
        materialization_decode_boundary="ordered residual state is ShardLoom-native; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.sort_top_k",
        required_evidence=(
            "traditional_analytics.prepared_native.sort_and_top_k",
            "shardloom_native_top_k_residual",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex enabled_top_n_per_group_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "row_number_window",
        "row number window",
        "local_analytics",
        "sort_and_window",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "null_heavy",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('row_number_window').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('row_number_window').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().with_row_number(partition_by='group_key', order_by='metric').collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('row_number_window')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('row_number_window').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run row_number_window fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared row-number result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, ShardLoom native window residual evidence, route timing, and no-fallback evidence",
        materialization_decode_boundary="window residual state is ShardLoom-native; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.row_number_window",
        required_evidence=(
            "traditional_analytics.prepared_native.row_number_window",
            "shardloom_native_window_residual",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex traditional_analytics::tests::enabled_row_number_window_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark --lib",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "top_n_per_group",
        "top-N per group",
        "local_analytics",
        "sort_and_window",
        dataset_profiles=(
            "tiny_smoke",
            "narrow_fact_dim",
            "skewed_keys",
            "null_heavy",
            "well_clustered",
            "poorly_clustered",
        ),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('top_n_per_group').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('top_n_per_group').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().top_n(3, partition_by='group_key', order_by='metric').collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('top_n_per_group')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('top_n_per_group').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run top_n_per_group fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared per-group top-N result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, ShardLoom native per-group top-N residual evidence, route timing, and no-fallback evidence",
        materialization_decode_boundary="per-group top-N residual state is ShardLoom-native; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.top_n_per_group",
        required_evidence=(
            "traditional_analytics.prepared_native.top_n_per_group",
            "shardloom_native_per_group_top_n_residual",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex enabled_top_n_per_group_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "clean_cast_filter_write",
        "clean/cast/filter/write",
        "etl_workflows",
        "etl_write",
        dataset_profiles=("dirty_csv", "schema_drift"),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('dirty.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('clean_cast_filter_write').write_vortex('target/clean-result')",
        python_surface="ctx.prepare_vortex('dirty.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('clean_cast_filter_write').write_vortex('target/clean-result')",
        dataframe_surface="ctx.read('dirty.csv').prepare().with_column('metric', sl.col('dirty_numeric').cast('float64')).filter(sl.col('dirty_flag') == False).write_jsonl('target/clean.jsonl')",
        context_surface="ctx.local_file_benchmark_route_report().scenario('clean_cast_filter_write')",
        session_surface="session.prepare_vortex('dirty.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('clean_cast_filter_write').write_vortex('target/clean-result')",
        cli_surface="shardloom traditional-analytics-prepare-batch-run clean_cast_filter_write fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv --write-result-vortex",
        source_route="dirty local compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="local result sink, JSONL/CSV compatibility output, or feature-gated Vortex result artifact",
        evidence_route="prepared-state evidence, result-sink replay proof, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="dirty values are normalized in ShardLoom route; decoded output only at declared local sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.clean_cast_filter_write",
        required_evidence=(
            "traditional_analytics.prepared_native.clean_cast_filter_write",
            "result_sink_replay",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex enabled_clean_cast_filter_write_uses_prepared_native_vortex_scan --features vortex-traditional-analytics-benchmark --lib",
        claim_boundary=(
            _LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY
            + " Dirty CSV support is fixture-scoped and does not claim general data-cleaning "
            "or production write semantics."
        ),
    ),
    _local_file_benchmark_route(
        "partition_pruning",
        "partition pruning",
        "layout_and_pruning",
        "scan_and_pruning",
        dataset_profiles=("partitioned_by_date", "well_clustered", "poorly_clustered"),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('partition_pruning').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('partition_pruning').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().filter(sl.col('event_date') >= '2026-01-01').collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('partition_pruning')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('partition_pruning').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run partition_pruning fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local partitioned fixture compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared partition-filter result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, partition fixture coverage, route timing, and no-fallback evidence",
        materialization_decode_boundary="partition/date predicate residual stays ShardLoom-native unless a declared sink requires decode",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.partition_pruning",
        required_evidence=(
            "traditional_analytics.prepared_native.partition_pruning",
            "prepared_vortex_scan_pushdown_matrix",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex traditional_analytics::tests::enabled_partition_pruning_uses_prepared_native_date_range_scan --features vortex-traditional-analytics-benchmark --lib",
        claim_boundary=(
            _LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY
            + " This route proves local partition fixture execution, not object-store/table "
            "partition pruning or broad metadata-pruning claims."
        ),
    ),
    _local_file_benchmark_route(
        "many_small_files_scan",
        "many-small-files scan",
        "local_analytics",
        "scan_and_pruning",
        dataset_profiles=("many_small_files", "few_large_files"),
        route_id="local_file_prepare_once_batch",
        route_display_name="ShardLoom Prepare-Once Batch",
        alternate_route_ids=("local_file_prepare_once_first_query",),
        selected_execution_mode="shardloom-prepare-batch",
        sql_surface="ctx.prepare_vortex('fact-parts/', dim='dim.csv', workspace='target/shardloom-prepared', input_format='csv').query('many_small_files_scan').collect()",
        python_surface="ctx.prepare_vortex('fact-parts/', dim='dim.csv', workspace='target/shardloom-prepared', input_format='csv').query('many_small_files_scan').collect()",
        dataframe_surface="ctx.read('fact-parts/').prepare(dim='dim.csv', workspace='target/shardloom-prepared').select('metric').collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('many_small_files_scan')",
        session_surface="session.prepare_vortex('fact-parts/', dim='dim.csv', workspace='target/shardloom-prepared', input_format='csv').query('many_small_files_scan').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run many_small_files_scan fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        start_state="raw_local_split_compat_sources",
        source_route="local split-file compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once_for_split_manifest",
        output_route="prepared split-file scan result, bounded report, or local result sink",
        evidence_route="prepared split manifest evidence, route timing, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="split-file inputs normalize before query; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.many_small_files_scan",
        required_evidence=(
            "traditional_analytics.prepared_native.many_small_files_scan",
            "split_manifest",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=(
            _LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY
            + " Many-small-files support is local fixture/split-manifest scoped and is not "
            "object-store listing, distributed scheduling, or scan-pushdown support."
        ),
    ),
    _local_file_benchmark_route(
        "null_heavy_aggregate",
        "null-heavy aggregate",
        "local_analytics",
        "aggregation",
        dataset_profiles=("null_heavy",),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('null_heavy_aggregate').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('null_heavy_aggregate').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().agg(non_null=('nullable_metric_00', 'count')).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('null_heavy_aggregate')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('null_heavy_aggregate').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run null_heavy_aggregate fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local null-heavy compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared null-heavy aggregate result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, null-heavy fixture coverage, route timing, and no-fallback evidence",
        materialization_decode_boundary="null semantics remain inside ShardLoom route; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.null_heavy_aggregate",
        required_evidence=(
            "traditional_analytics.prepared_native.null_heavy_aggregate",
            "null_heavy_fixture",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "high_cardinality_string_group_distinct",
        "high-cardinality string group/distinct",
        "local_analytics",
        "aggregation",
        dataset_profiles=("high_cardinality_strings",),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('high_cardinality_string_group_distinct').collect()",
        python_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('high_cardinality_string_group_distinct').collect()",
        dataframe_surface="ctx.read('fact.csv').prepare().group_by('category').agg(unique=('category', 'n_unique')).collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('high_cardinality_string_group_distinct')",
        session_surface="session.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query('high_cardinality_string_group_distinct').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run high_cardinality_string_group_distinct fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local high-cardinality string compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once",
        output_route="prepared high-cardinality group/distinct result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, high-cardinality fixture coverage, route timing, and no-fallback evidence",
        materialization_decode_boundary="string grouping state remains ShardLoom-native; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.high_cardinality_string_group_distinct",
        required_evidence=(
            "traditional_analytics.prepared_native.high_cardinality_string_group_distinct",
            "high_cardinality_fixture",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-contract-tests --test traditional_benchmark_harness",
        claim_boundary=_LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY,
    ),
    _local_file_benchmark_route(
        "nested_json_field_scan",
        "nested JSON field scan",
        "etl_workflows",
        "messy_lakehouse_data",
        dataset_profiles=("nested_json",),
        route_id="local_file_prepare_once_first_query",
        route_display_name="ShardLoom Prepare-Once First Query",
        alternate_route_ids=("local_file_prepare_once_batch",),
        selected_execution_mode="prepared_vortex",
        sql_surface="ctx.prepare_vortex('nested_fact.jsonl', dim='dim.jsonl', workspace='target/shardloom-prepared', input_format='jsonl').query('nested_json_field_scan').collect()",
        python_surface="ctx.prepare_vortex('nested_fact.jsonl', dim='dim.jsonl', workspace='target/shardloom-prepared', input_format='jsonl').query('nested_json_field_scan').collect()",
        dataframe_surface="ctx.read_json('nested_fact.jsonl').prepare().select('nested_payload').collect()",
        context_surface="ctx.local_file_benchmark_route_report().scenario('nested_json_field_scan')",
        session_surface="session.prepare_vortex('nested_fact.jsonl', dim='dim.jsonl', workspace='target/shardloom-prepared', input_format='jsonl').query('nested_json_field_scan').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run nested_json_field_scan fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv",
        source_route="local nested JSON sidecar compatibility source adapter",
        preparation_route="vortex_ingest_prepare_once_for_nested_fixture",
        output_route="prepared nested-field fixture result, bounded report, or local result sink",
        evidence_route="prepared-state evidence, nested JSON fixture coverage, route timing, and no-fallback evidence",
        materialization_decode_boundary="nested fixture values are admitted for this benchmark route only; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.nested_json_field_scan",
        required_evidence=(
            "traditional_analytics.prepared_native.nested_json_field_scan",
            "nested_json_fixture",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex nested_json_field_scan_runs_jsonl_fixture --features vortex-traditional-analytics-benchmark --lib",
        claim_boundary=(
            _LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY
            + " Nested JSON is fixture-scoped route support and does not claim native nested "
            "field pruning, arbitrary nested schema execution, or broad JSON analytics support."
        ),
    ),
    _local_file_benchmark_route(
        "small_change_over_large_base",
        "small change over large base",
        "incremental_state",
        "incremental_state",
        dataset_profiles=("cdc_delta_overlay",),
        route_id="local_file_prepare_once_batch",
        route_display_name="ShardLoom Prepare-Once Batch",
        alternate_route_ids=("local_file_prepare_once_first_query",),
        selected_execution_mode="shardloom-prepare-batch",
        sql_surface="ctx.prepare_vortex('base.csv', dim='dim.csv', workspace='target/shardloom-prepared', cdc_delta='cdc_delta.csv').query('small_change_over_large_base').collect()",
        python_surface="ctx.prepare_vortex('base.csv', dim='dim.csv', workspace='target/shardloom-prepared', cdc_delta='cdc_delta.csv').query('small_change_over_large_base').collect()",
        dataframe_surface="ctx.read('base.csv').prepare(dim='dim.csv', workspace='target/shardloom-prepared', cdc_delta='cdc_delta.csv').run('small_change_over_large_base')",
        context_surface="ctx.local_file_benchmark_route_report().scenario('small_change_over_large_base')",
        session_surface="session.prepare_vortex('base.csv', dim='dim.csv', workspace='target/shardloom-prepared', cdc_delta='cdc_delta.csv').query('small_change_over_large_base').collect()",
        cli_surface="shardloom traditional-analytics-prepare-batch-run small_change_over_large_base fact.csv dim.csv --workspace target/shardloom-prepared --input-format csv --cdc-delta cdc_delta.csv",
        start_state="raw_compat_source_plus_cdc_delta_overlay",
        source_route="local base compatibility source plus explicit CDC delta sidecar",
        preparation_route="vortex_ingest_prepare_once_for_base_and_cdc_delta",
        output_route="prepared CDC-overlay fixture result, bounded report, or local result sink",
        evidence_route="base and cdc_delta prepared-state evidence, route timing, Native I/O, and no-fallback evidence",
        materialization_decode_boundary="CDC overlay is an explicit local fixture route; decoded output only at declared result sink",
        route_runtime_status="prepared_route_supported",
        owner="GAR-RUNTIME-IMPL-6D-3.small_change_over_large_base",
        required_evidence=(
            "traditional_analytics.prepared_native.small_change_over_large_base",
            "cdc_delta_vortex",
            "no_fallback_evidence",
        ),
        next_verifier="cargo test -p shardloom-vortex small_change_over_large_base_imports_cdc_delta_fixture --features vortex-traditional-analytics-benchmark --lib",
        claim_boundary=(
            _LOCAL_FILE_PREPARED_BENCHMARK_BOUNDARY
            + " CDC overlay support is the deterministic local benchmark fixture route, not "
            "general deletes, upserts, table transaction semantics, streaming CDC, or "
            "production incremental processing."
        ),
    ),
)

USER_ROUTE_CAPABILITY_ROWS: tuple[UserRouteCapabilityRow, ...] = (
    _user_route(
        "local_file_direct_transient_route",
        "ShardLoom Direct Transient Route",
        "local_compat_file",
        input_examples=("orders.csv", "events.jsonl", "flat.json", "local.parquet"),
        front_doors=_ALL_USER_FRONT_DOORS,
        desired_outputs=(
            "machine_readable_report",
            "bounded_preview",
            "local_compat_output",
            "feature_gated_local_vortex_output",
        ),
        recommended_user_surface="ctx.read(path).filter(...).select(...).limit(...).collect()/write_*",
        start_state="raw_compat_source",
        vortex_normalization_point=(
            "local compatibility source -> SourceState -> transient Vortex-preparable arrays; "
            "no persistent VortexPreparedState is created on this route"
        ),
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="direct_compatibility_transient_no_persistent_preparation",
        execution_mode="direct_compatibility_transient",
        execution_route="sql-local-source-smoke local-source ShardLoom runtime",
        output_route="bounded report, local JSONL/CSV, feature-gated Parquet/Arrow IPC/Avro/ORC/Vortex sink",
        evidence_route="OutputEnvelope fields plus execution, Native I/O, replay, and no-fallback evidence",
        materialization_decode_boundary="bounded decoded preview or explicit local sink boundary only",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.local_file_direct_transient_route",
        required_evidence=(
            "sql_local_source_smoke",
            "execution_certificate",
            "native_io_certificate",
            "output_fidelity_report_status",
            "no_fallback_evidence",
        ),
        claim_boundary=_LOCAL_QUERY_BUILDER_RUNTIME_BOUNDARY,
    ),
    _user_route(
        "local_file_cold_certified_route",
        "ShardLoom Cold Certified Route",
        "local_compat_file",
        input_examples=(
            "fact.csv + dim.csv",
            "fact.jsonl + dim.jsonl",
            "fact.parquet + dim.parquet",
            "fact.arrow + dim.arrow",
            "fact.avro + dim.avro",
            "fact.orc + dim.orc",
        ),
        front_doors=_ALL_USER_FRONT_DOORS,
        desired_outputs=("machine_readable_report", "evidence_certificate", "result_sink"),
        recommended_user_surface=(
            "ctx.read_csv('fact.csv').prepare_vortex(workspace='target/shardloom-prepared') "
            "for single-source preparation, or ctx.prepare_vortex('fact.csv', dim='dim.csv', "
            "workspace='target/shardloom-prepared').prepare() for benchmark-range fact/dim routes"
        ),
        start_state="raw_compat_source",
        vortex_normalization_point="SourceState -> vortex_ingest -> VortexPreparedState -> reopen/scan verification",
        source_route="compatibility_import_certified",
        preparation_route="vortex_ingest_certified",
        execution_mode="compatibility_import_certified",
        execution_route="certified cold prepare, reopen/scan, query, and evidence route",
        output_route="result sink plus certificate/evidence report",
        evidence_route="route-runtime fields, VortexPreparedState evidence, stage timings, and no-fallback evidence",
        materialization_decode_boundary="decode/materialization only at declared result sink or bounded report",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.local_file_cold_certified_route",
        required_evidence=(
            "source_state",
            "vortex_prepared_state",
            "compatibility_import_certified",
            "execution_certificate",
            "route_stage_timing",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Cold certified route evidence covers raw compatibility input through certified "
            "Vortex preparation, reopen/scan, query, and evidence for local benchmark-range rows. "
            "It is not a production or performance-superiority claim."
        ),
    ),
    _user_route(
        "local_file_prepare_once_first_query",
        "ShardLoom Prepare-Once First Query",
        "local_compat_file",
        input_examples=(
            "fact.csv + dim.csv",
            "fact.jsonl + dim.jsonl",
            "fact.parquet + dim.parquet",
            "fact.arrow + dim.arrow",
            "fact.avro + dim.avro",
            "fact.orc + dim.orc",
        ),
        front_doors=_ALL_USER_FRONT_DOORS,
        desired_outputs=("prepared_query_result", "machine_readable_report", "result_sink"),
        recommended_user_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').query(...).collect()/write_vortex(...)",
        start_state="raw_compat_source",
        vortex_normalization_point="SourceState -> vortex_ingest -> VortexPreparedState before first query",
        source_route="compatibility_import_certified local input adapter",
        preparation_route="vortex_ingest_prepare_once",
        execution_mode="prepared_vortex",
        execution_route="prepared_vortex first query after preparation",
        output_route="prepared query result, bounded report, or local result sink",
        evidence_route="prepared-state creation evidence, preparation_included_in_route=true, query_timing_starts_after_preparation=true, first-query route fields",
        materialization_decode_boundary="decode/materialization only after prepared query output is declared",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.local_file_prepare_once_first_query",
        required_evidence=(
            "vortex_ingest",
            "VortexPreparedState",
            "prepared_state_lookup_or_create_ms",
            "prepared_query_execution",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Prepare-once first-query route is the primary raw compatibility input to prepared "
            "Vortex user route. It includes preparation in the route boundary and remains "
            "local evidence only until broader correctness, claim, and benchmark evidence lands."
        ),
    ),
    _user_route(
        "local_file_prepare_once_batch",
        "ShardLoom Prepare-Once Batch",
        "local_compat_file",
        input_examples=(
            "fact.csv + dim.csv",
            "fact.jsonl + dim.jsonl",
            "fact.parquet + dim.parquet",
            "fact.arrow + dim.arrow",
            "fact.avro + dim.avro",
            "fact.orc + dim.orc",
        ),
        front_doors=_ALL_USER_FRONT_DOORS,
        desired_outputs=("amortized_prepared_queries", "machine_readable_report", "result_sink"),
        recommended_user_surface="ctx.prepare_vortex('fact.csv', dim='dim.csv', workspace='target/shardloom-prepared').run_batch([...])",
        start_state="raw_compat_source",
        vortex_normalization_point="SourceState -> vortex_ingest once -> reused VortexPreparedState",
        source_route="compatibility_import_certified local input adapter",
        preparation_route="vortex_ingest_prepare_once_reused_for_batch",
        execution_mode="shardloom-prepare-batch",
        execution_route="prepared_vortex batch scenarios in one ShardLoom process",
        output_route="one report/result per prepared scenario plus amortization evidence",
        evidence_route="prepare_batch_scale_route, prepared_state_reused=true, batch stage timing, no-fallback evidence",
        materialization_decode_boundary="decode/materialization only for each declared result sink",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.local_file_prepare_once_batch",
        required_evidence=(
            "VortexPreparedState",
            "prepared_state_reused",
            "batch_scenario_manifest",
            "route_stage_timing",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Prepare-once batch evidence shows realistic local prepared-state reuse. It does not "
            "authorize production, distributed, or performance-superiority claims."
        ),
    ),
    _user_route(
        "prepared_vortex_warm_query",
        "ShardLoom Warm Prepared Query",
        "prepared_vortex_artifact",
        input_examples=("target/prepared/orders.vortex-prepared", "VortexPreparedState"),
        front_doors=("Python", "context", "session", "CLI"),
        desired_outputs=("prepared_query_result", "machine_readable_report", "result_sink"),
        recommended_user_surface="prepared.query(...).collect()/write_*",
        start_state="VortexPreparedState",
        vortex_normalization_point="already_prepared_vortex_state",
        source_route="prepared Vortex state lookup",
        preparation_route="not_included_existing_VortexPreparedState",
        execution_mode="prepared_vortex",
        execution_route="prepared_vortex warm query",
        output_route="prepared query result, bounded report, or local result sink",
        evidence_route="prepared_state_reused=true, preparation_included=false, route-runtime fields",
        materialization_decode_boundary="decode/materialization only after warm prepared query output is declared",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D.prepared_vortex_warm_query",
        required_evidence=(
            "VortexPreparedState",
            "prepared_state_reused",
            "preparation_included=false",
            "prepared_query_execution",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Warm prepared query evidence starts after VortexPreparedState exists. It is useful "
            "runtime evidence but is not a raw-source end-to-end comparison by itself."
        ),
    ),
    _user_route(
        "native_vortex_query",
        "ShardLoom Native Vortex Primitive Query",
        "local_vortex_file",
        input_examples=("orders.vortex", "local .vortex artifact"),
        front_doors=_ALL_USER_FRONT_DOORS,
        desired_outputs=("machine_readable_report", "count_report", "filter_report", "project_report", "bounded_preview"),
        recommended_user_surface="ctx.native_vortex_route('fact.vortex', 'dim.vortex', execution_mode='native_vortex', memory_gb=4, max_parallelism=1).query(...).collect()/write_vortex(...)",
        start_state="native_vortex_file",
        vortex_normalization_point="native_vortex_boundary",
        source_route="Vortex-native local file/source",
        preparation_route="not_required_native_vortex_input",
        execution_mode="native_vortex",
        execution_route="ShardLoom local Vortex primitive runtime family",
        output_route="machine-readable native route report, bounded scoped collect output, or Vortex result sink",
        evidence_route="traditional-analytics-vortex-run/vortex-batch envelope, Native I/O, execution mode, resource policy, and no-fallback evidence",
        materialization_decode_boundary="Vortex metadata/encoded boundary; decoded output only when requested by result/report boundary",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D.native_vortex_query",
        required_evidence=(
            "vortex_local_primitive_runtime",
            "native_vortex_input",
            "native_io_certificate",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=_LOCAL_VORTEX_PRIMITIVE_RUNTIME_BOUNDARY,
    ),
    _user_route(
        "local_vortex_primitive_report",
        "ShardLoom Local Vortex Primitive Report",
        "local_vortex_file",
        input_examples=("orders.vortex",),
        front_doors=("SQL", "Python", "DataFrame", "context", "session", "CLI"),
        desired_outputs=("count_report", "filter_report", "project_report", "bounded_preview"),
        recommended_user_surface="ctx.sql(\"SELECT ... FROM 'local.vortex'\").collect(), ctx.read_vortex(...).count/filter/select/limit/collect, or ctx.local_vortex_primitive_route_report()",
        start_state="native_vortex_file",
        vortex_normalization_point="native_vortex_boundary",
        source_route="Vortex local primitive source",
        preparation_route="not_required_native_vortex_input",
        execution_mode="native_vortex",
        execution_route="vortex-run/vortex-count-where/vortex-filter/vortex-project/vortex-filter-project",
        output_route="machine-readable primitive report and bounded scoped collect output",
        evidence_route="local primitive command envelope, execution certificate, Native I/O, no-fallback evidence",
        materialization_decode_boundary="primitive report boundary; no broad decoded row materialization",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D.local_vortex_primitive_report",
        required_evidence=(
            "vortex_count",
            "vortex_count_where",
            "vortex_filter",
            "vortex_project",
            "vortex_filter_project",
            "no_fallback_evidence",
        ),
        claim_boundary=_LOCAL_VORTEX_PRIMITIVE_RUNTIME_BOUNDARY,
    ),
    _user_route(
        "generated_rows_local_output",
        "ShardLoom Generated Rows Local Output",
        "generated_rows",
        input_examples=("from_rows([...])", "range(0, 10)", "sql_values(...)"),
        front_doors=("SQL", "Python", "DataFrame", "context", "CLI"),
        desired_outputs=("local_jsonl", "local_csv", "feature_gated_local_vortex_output", "fanout"),
        recommended_user_surface="ctx.from_rows(...).write_* or ctx.sql_values(...).write_*",
        start_state="source_free_generated_rows",
        vortex_normalization_point="generated rows -> Vortex-preparable batch",
        source_route="generated-source user rows/range/sequence/calendar/SQL literal source",
        preparation_route="generated_source_to_vortex_preparable_batch",
        execution_mode="generated_source_smoke",
        execution_route="generated-source-* local output smoke family",
        output_route="local JSONL/CSV and feature-gated local Vortex output/fanout",
        evidence_route="generated-source certificate, OutputPlan, output Native I/O, replay evidence",
        materialization_decode_boundary="generated rows are materialized input rows; output decode only at declared sink",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.generated_rows_local_output",
        required_evidence=(
            "generated_source_certificate",
            "output_native_io_certificate",
            "execution_certificate",
            "result_replay_verified",
            "no_fallback_evidence",
        ),
        claim_boundary=_GENERATED_OUTPUT_BOUNDARY,
    ),
    _user_route(
        "materialized_python_snapshot_reentry",
        "ShardLoom Materialized Python Snapshot Re-Entry",
        "materialized_python_arrow_numpy",
        input_examples=("from_pandas(df)", "from_arrow_table(table)", "from_arrow_ipc(bytes)"),
        front_doors=_PYTHON_FRONT_DOORS,
        desired_outputs=("local_jsonl", "local_csv", "machine_readable_report", "generated_rows_reentry"),
        recommended_user_surface="ctx.from_pandas(df).write_* or ctx.from_arrow_table(table).write_*",
        start_state="materialized_python_or_arrow_snapshot",
        vortex_normalization_point="materialized snapshot -> generated rows -> Vortex-preparable route",
        source_route="explicit materialized input boundary",
        preparation_route="materialized_input_snapshot_to_generated_source_user_rows",
        execution_mode="generated_source_smoke",
        execution_route="generated-source user rows local output smoke",
        output_route="local JSONL/CSV report and generated-source evidence",
        evidence_route="materialized input boundary, generated-source certificate, no-fallback evidence",
        materialization_decode_boundary="materialized input is explicit; no hidden pandas/Arrow execution engine",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D.materialized_python_snapshot_reentry",
        required_evidence=(
            "materialized_input_boundary",
            "generated_source_user_rows",
            "input_fidelity_boundary",
            "optional_dependency_policy",
            "no_fallback_evidence",
        ),
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _user_route(
        "bounded_decoded_preview",
        "ShardLoom Bounded Decoded Preview",
        "local_compat_file",
        input_examples=("orders.csv", "events.jsonl"),
        front_doors=("SQL", "Python", "DataFrame", "context", "session"),
        desired_outputs=("bounded_preview", "python_objects", "pandas_optional", "arrow_optional", "numpy_optional"),
        recommended_user_surface="ctx.read(path).limit(n).to_python_objects()/to_pandas()/to_arrow()/display()",
        start_state="raw_compat_source",
        vortex_normalization_point="local source -> SourceState -> ShardLoom runtime result -> bounded decoded container",
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="route_specific_direct_or_prepared_source_state",
        execution_mode="direct_compatibility_transient",
        execution_route="sql-local-source-smoke inline bounded result",
        output_route="bounded decoded preview or optional container",
        evidence_route="bounded materialization policy, optional dependency policy, no-fallback evidence",
        materialization_decode_boundary="bounded explicit decode only after ShardLoom runtime result",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D.bounded_decoded_preview",
        required_evidence=(
            "bounded_materialization_runtime",
            "decoded_materialization_policy",
            "optional_dependency_policy",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _user_route(
        "schema_quality_preview",
        "ShardLoom Schema And Data-Quality Preview",
        "local_compat_file",
        input_examples=("orders.csv", "events.jsonl"),
        front_doors=("SQL", "Python", "DataFrame", "context", "session"),
        desired_outputs=(
            "schema_report",
            "validation_report",
            "data_quality_report",
            "quarantine_report",
            "preview",
        ),
        recommended_user_surface=(
            "ctx.read(path).schema()/validate_schema()/data_quality()/quarantine()/preview()"
        ),
        start_state="raw_compat_source",
        vortex_normalization_point="local source -> SourceState -> ShardLoom runtime bounded evidence rows",
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="route_specific_direct_or_prepared_source_state",
        execution_mode="direct_compatibility_transient",
        execution_route="sql-local-source-smoke inline bounded schema/data-quality result",
        output_route="machine-readable schema, validation, data-quality, quarantine, preview report",
        evidence_route="schema/data-quality/quarantine report fields, execution certificate, no-fallback evidence",
        materialization_decode_boundary="bounded decoded report rows only",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D.schema_quality_preview",
        required_evidence=(
            "sql_schema_quality_surface",
            "schema_report_contract",
            "data_quality_report_contract",
            "quarantine_report_contract",
            "front_door_equivalence_tests",
            "no_fallback_evidence",
        ),
        claim_boundary=_LOCAL_QUERY_BUILDER_OBJECT_MATERIALIZATION_BOUNDARY,
    ),
    _user_route(
        "quarantine_output_route",
        "ShardLoom Quarantine Output Route",
        "local_compat_file",
        input_examples=("orders.csv", "events.jsonl"),
        front_doors=("Python", "DataFrame", "context", "session", "CLI"),
        desired_outputs=("quarantine_output", "policy_report"),
        recommended_user_surface=(
            "ctx.read(path).quarantine(local_path, 'not_null:column', output_format='jsonl')"
        ),
        start_state="raw_compat_source",
        vortex_normalization_point="local source -> SourceState -> ShardLoom runtime bounded evidence rows",
        source_route="UniversalIngress/InputAdapter local compatibility source",
        preparation_route="route_specific_direct_or_prepared_source_state",
        execution_mode="direct_compatibility_transient",
        execution_route="sql-local-source-smoke bounded classification and pushdownable not-null quarantine",
        output_route="local quarantine sink through sql-local-source-smoke for pushdownable not-null rows",
        evidence_route="quarantine report, local sink certificate, replay evidence, no-fallback evidence",
        materialization_decode_boundary="bounded inline JSONL classification before scoped quarantine sink write",
        route_runtime_status="scoped_runtime_supported",
        benchmark_range=False,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D:last_order.quarantine_output_route",
        required_evidence=(
            "sql_local_source_smoke",
            "quarantine_policy",
            "local_quarantine_sink_write_evidence",
            "output_native_io_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Scoped local-source not-null quarantine and bounded report evidence only; object-store/"
            "table quarantine, broad policy remediation, unique-check sink pushdown, production "
            "governance, external effects, and performance claims remain blocked."
        ),
    ),
    _user_route(
        "broad_sql_python_dataframe_runtime",
        "ShardLoom Broad SQL/Python/DataFrame Runtime Expansion",
        "arbitrary_user_expression",
        input_examples=("arbitrary SQL", "multi-stage DataFrame pipeline", "typed Python expression"),
        front_doors=("SQL", "Python", "DataFrame", "context", "session"),
        desired_outputs=("any_supported_result", "native_vortex_output", "compatibility_output"),
        recommended_user_surface="GAR-RUNTIME-IMPL-6D broad language surface after semantic coverage lands",
        start_state="user_expression",
        vortex_normalization_point="front-door expression -> ShardLoom plan -> Vortex-normalized runtime path pending",
        source_route="pending broad parser/binder/expression registry route",
        preparation_route="pending route-specific Vortex preparation",
        execution_mode="pending_broad_language_runtime",
        execution_route="broad SQL grammar, expression registry, DataFrame API, UDF, and effect policy pending",
        output_route="deterministic diagnostic until broad output route evidence lands",
        evidence_route="semantic conformance, execution certificate, Native I/O, benchmark evidence pending",
        materialization_decode_boundary="must be explicit per operator/output; hidden materialization is not allowed",
        route_runtime_status="runtime_expansion_pending",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D:last_order.broad_language_surface",
        blocker_id="cg20.cg21.broad_language_surface_missing",
        required_evidence=(
            "sql_grammar_coverage",
            "expression_kernel_registry",
            "semantic_conformance_suite",
            "front_door_equivalence_tests",
            "benchmark_evidence",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "The broad 'build anything' claim remains not-claim-grade until SQL, Python, "
            "DataFrame, function/UDF, semantic conformance, and benchmark evidence converge on "
            "the same Vortex-normalized ShardLoom-native execution plan."
        ),
    ),
    _user_route(
        "object_store_lakehouse_runtime",
        "ShardLoom Object-Store And Lakehouse Runtime Expansion",
        "object_store_lakehouse_catalog",
        input_examples=("s3://bucket/table", "Iceberg table", "Delta-compatible table"),
        front_doors=("SQL", "Python", "DataFrame", "context", "session", "CLI"),
        desired_outputs=("remote_result", "table_commit", "native_vortex_output", "compatibility_output"),
        recommended_user_surface="object-store/table helpers after credential, commit, and recovery evidence lands",
        start_state="remote_or_table_source",
        vortex_normalization_point="object-store or table source -> Vortex-normalized runtime path pending",
        source_route="pending object-store/table/catalog source route",
        preparation_route="pending table/object-source to Vortex preparation and commit protocol",
        execution_mode="pending_production_io_runtime",
        execution_route="object-store/table runtime, catalog, commit, rollback, retry, and recovery pending",
        output_route="blocked diagnostic or report-only evidence until production I/O runtime lands",
        evidence_route="credential policy, table/runtime evidence, commit/recovery evidence pending",
        materialization_decode_boundary="remote output transfer and table commit boundaries must be explicit",
        route_runtime_status="runtime_expansion_pending",
        benchmark_range=False,
        route_comparable_to_external_end_to_end=False,
        owner="GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_catalog",
        blocker_id="cg9.cg10.cg21.production_io_front_door_missing",
        required_evidence=(
            "vortex_input_normalization_boundary",
            "object_store_runtime",
            "credential_policy",
            "catalog_table_runtime",
            "commit_protocol",
            "retry_recovery_evidence",
        ),
        claim_boundary=(
            "Object-store, lakehouse/table, catalog, commit, rollback, and remote result "
            "delivery remain runtime-expansion work and cannot be claimed from local smokes."
        ),
    ),
    _user_route(
        "performance_equivalence_evidence",
        "ShardLoom Front-Door Performance Equivalence Evidence",
        "equivalent_front_door_workload",
        input_examples=("same workload expressed in SQL, Python, and DataFrame APIs"),
        front_doors=("SQL", "Python", "DataFrame", "context", "session"),
        desired_outputs=("benchmark_evidence", "claim_evidence"),
        recommended_user_surface="front-door equivalent benchmark manifest after GAR-RUNTIME-IMPL-6D runtime routes land",
        start_state="equivalent_workload_manifest",
        vortex_normalization_point="route-specific Vortex boundary must be recorded in each benchmark row",
        source_route="front-door workload manifest",
        preparation_route="route-specific direct, cold, prepare-once, warm, or native preparation",
        execution_mode="claim_evidence_pending",
        execution_route="scoped runtime paths exist; equivalent front-door benchmark publication pending",
        output_route="benchmark publication and claim evidence pending",
        evidence_route="correctness, execution certificate, no-fallback, route timings, benchmark manifest pending",
        materialization_decode_boundary="must match across front doors or be declared as timing scope difference",
        route_runtime_status="benchmark_publication_pending",
        benchmark_range=True,
        route_comparable_to_external_end_to_end=True,
        owner="GAR-RUNTIME-IMPL-6D:last_order.performance_equivalence",
        blocker_id="cg6.front_door_performance_equivalence_benchmark_missing",
        required_evidence=(
            "front_door_equivalent_workload_manifest",
            "correctness_evidence",
            "benchmark_manifest",
            "execution_certificate",
            "no_fallback_evidence",
        ),
        claim_boundary=(
            "Performance equivalence is not claim-grade until equivalent SQL, Python, and "
            "DataFrame workloads publish reproducible results with the same route boundaries."
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class ETLWorkflowCapabilityRow:
    """Support, evidence, and claim boundary for one user-facing ETL workflow family."""

    workflow_id: str
    title: str
    status: str
    execution_mode: str
    engine_mode: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    evidence_fields: tuple[str, ...]
    blocker_id: str | None
    claim_gate_status: str
    runtime_execution: bool
    data_read: bool
    write_io: bool
    object_store_io: bool
    table_runtime: bool
    production_claim_allowed: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    claim_boundary: str

    @property
    def ready_or_smoke_supported(self) -> bool:
        """Whether the row has a scoped local ready/smoke path."""

        return self.status in {"ready_local", "smoke_supported"}

    @property
    def report_only(self) -> bool:
        """Whether the row is inspectable posture without runtime support."""

        return self.status == "report_only"

    @property
    def blocked(self) -> bool:
        """Whether the row is deliberately blocked until future evidence exists."""

        return self.status == "blocked"

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the row preserves the no-fallback/no-external-engine boundary."""

        return not self.fallback_attempted and not self.external_engine_invoked


@dataclass(frozen=True, slots=True)
class ETLWorkflowCapabilityMatrix:
    """Report-only ETL workflow matrix for user-facing local paths and blockers."""

    capability: "CapabilityView"
    rows: tuple[ETLWorkflowCapabilityRow, ...]

    @classmethod
    def from_capability(cls, capability: "CapabilityView") -> "ETLWorkflowCapabilityMatrix":
        """Build the static ETL workflow matrix from the workflow capability view."""

        rows = (
            ETL_WORKFLOW_CAPABILITY_ROWS
            if capability.field("etl_workflow_matrix_schema_version")
            else ()
        )
        return cls(capability=capability, rows=rows)

    @property
    def schema_version(self) -> str | None:
        """Return the CLI-advertised ETL workflow matrix schema version."""

        return self.capability.field("etl_workflow_matrix_schema_version")

    @property
    def matrix_id(self) -> str | None:
        """Return the CLI-advertised ETL workflow matrix identifier."""

        return self.capability.field("etl_workflow_matrix_id")

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return stable ETL workflow row IDs."""

        return tuple(row.workflow_id for row in self.rows)

    @property
    def supported_local_rows(self) -> tuple[str, ...]:
        """Return rows with scoped local ready/smoke evidence."""

        return tuple(row.workflow_id for row in self.rows if row.ready_or_smoke_supported)

    @property
    def report_only_rows(self) -> tuple[str, ...]:
        """Return rows that expose report-only posture."""

        return tuple(row.workflow_id for row in self.rows if row.report_only)

    @property
    def blocked_rows(self) -> tuple[str, ...]:
        """Return rows that remain blocked."""

        return tuple(row.workflow_id for row in self.rows if row.blocked)

    @property
    def claim_gate_statuses(self) -> tuple[str, ...]:
        """Return claim-gate statuses in stable first-seen order."""

        return tuple(dict.fromkeys(row.claim_gate_status for row in self.rows))

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves no fallback and no external engine invocation."""

        return all(row.no_fallback_no_external_engine for row in self.rows)

    @property
    def production_etl_claim_allowed(self) -> bool:
        """Whether any row allows a production ETL claim."""

        return any(row.production_claim_allowed for row in self.rows)

    @property
    def object_store_or_table_runtime_supported(self) -> bool:
        """Whether object-store/table runtime is supported by any ETL row."""

        return any(row.object_store_io or row.table_runtime for row in self.rows)

    def row(self, workflow_id: str) -> ETLWorkflowCapabilityRow:
        """Return one ETL workflow row by ID."""

        normalized = workflow_id.strip().lower().replace("-", "_")
        for row in self.rows:
            if row.workflow_id == normalized:
                return row
        raise KeyError(f"ETL workflow {workflow_id!r} is not in the capability matrix")


def _etl_workflow_row(
    workflow_id: str,
    title: str,
    status: str,
    execution_mode: str,
    engine_mode: str,
    *,
    inputs: Sequence[str],
    outputs: Sequence[str],
    evidence_fields: Sequence[str],
    blocker_id: str | None = None,
    runtime_execution: bool = False,
    data_read: bool = False,
    write_io: bool = False,
    object_store_io: bool = False,
    table_runtime: bool = False,
    claim_boundary: str,
) -> ETLWorkflowCapabilityRow:
    return ETLWorkflowCapabilityRow(
        workflow_id=workflow_id,
        title=title,
        status=status,
        execution_mode=execution_mode,
        engine_mode=engine_mode,
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        evidence_fields=tuple(evidence_fields),
        blocker_id=blocker_id,
        claim_gate_status="not_claim_grade",
        runtime_execution=runtime_execution,
        data_read=data_read,
        write_io=write_io,
        object_store_io=object_store_io,
        table_runtime=table_runtime,
        production_claim_allowed=False,
        fallback_attempted=False,
        external_engine_invoked=False,
        claim_boundary=claim_boundary,
    )


_LOCAL_TECHNICAL_PREVIEW_BOUNDARY = (
    "Scoped local technical-preview evidence only; not production ETL, broad SQL/DataFrame, "
    "object-store/lakehouse, Foundry, package, performance, or Spark-displacement proof."
)
_REPORT_ONLY_WORKFLOW_BOUNDARY = (
    "Report-only workflow posture; inspectable diagnostics do not authorize runtime support."
)
_BLOCKED_WORKFLOW_BOUNDARY = (
    "Blocked until scoped runtime, correctness, certificate, Native I/O, policy, and no-fallback "
    "evidence exists."
)

ETL_WORKFLOW_CAPABILITY_ROWS: tuple[ETLWorkflowCapabilityRow, ...] = (
    _etl_workflow_row(
        "first_10_minutes_local_smoke",
        "First 10 minutes local smoke",
        "ready_local",
        "no_dataset_smoke",
        "batch_status",
        inputs=("none",),
        outputs=("status_report", "capabilities_report", "smoke_report"),
        evidence_fields=("fallback_attempted=false", "external_engine_invoked=false"),
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "local_csv_parquet_certified_workload",
        "Local CSV/Parquet certified workload",
        "smoke_supported",
        "compatibility_import_certified",
        "batch",
        inputs=("local_csv", "local_parquet"),
        outputs=("local_vortex_artifact", "result_sink_evidence"),
        evidence_fields=("execution_certificate", "native_io_certificate", "claim_gate_status"),
        runtime_execution=True,
        data_read=True,
        write_io=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "prepared_native_vortex_batch_smoke",
        "Prepared/native Vortex batch smoke",
        "smoke_supported",
        "prepared_vortex/native_vortex",
        "batch",
        inputs=("prepared_vortex_artifact", "native_vortex_fixture"),
        outputs=("prepared_native_timing_rows", "source_backed_scan_evidence"),
        evidence_fields=("source_backed_scan_used", "source_state_reuse_hit", "claim_gate_status"),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "source_free_user_rows_jsonl_csv",
        "Source-free user rows JSONL/CSV",
        "smoke_supported",
        "source_free_generated_output",
        "batch",
        inputs=("python_rows",),
        outputs=("local_jsonl_or_csv_output", "generated_source_certificate"),
        evidence_fields=("input_dataset_count=0", "generated_source_created=true"),
        runtime_execution=True,
        write_io=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "source_free_range_jsonl_csv",
        "Source-free range JSONL/CSV",
        "smoke_supported",
        "source_free_generated_output",
        "batch",
        inputs=("range_generator",),
        outputs=("local_jsonl_or_csv_output", "generated_source_certificate"),
        evidence_fields=("generated_source_kind=range", "output_native_io_certificate_status"),
        runtime_execution=True,
        write_io=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "source_free_literal_table_jsonl_csv",
        "Source-free literal table JSONL/CSV",
        "smoke_supported",
        "source_free_generated_output",
        "batch",
        inputs=("literal_table_rows",),
        outputs=("local_jsonl_or_csv_output", "generated_source_certificate"),
        evidence_fields=("generated_source_kind=literal_table", "output_native_io_certificate_status"),
        runtime_execution=True,
        write_io=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "source_free_calendar_jsonl_csv",
        "Source-free calendar JSONL/CSV",
        "smoke_supported",
        "source_free_generated_output",
        "batch",
        inputs=("calendar_generator",),
        outputs=("local_jsonl_or_csv_output", "generated_source_certificate"),
        evidence_fields=("generated_source_kind=calendar", "output_native_io_certificate_status"),
        runtime_execution=True,
        write_io=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "dirty_csv_fixture",
        "Dirty CSV fixture",
        "smoke_supported",
        "compatibility_import_certified",
        "batch",
        inputs=("dirty_csv_fixture",),
        outputs=("benchmark_evidence_rows",),
        evidence_fields=("source_metadata_snapshot_status", "claim_gate_status"),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "nested_json_fixture",
        "Nested JSON fixture",
        "smoke_supported",
        "compatibility_import_certified",
        "batch",
        inputs=("nested_json_fixture",),
        outputs=("benchmark_evidence_rows",),
        evidence_fields=("scenario_family", "materialization_boundary", "claim_gate_status"),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "cdc_overlay_fixture",
        "CDC overlay fixture",
        "smoke_supported",
        "compatibility_import_certified",
        "batch",
        inputs=("base_fixture", "append_delta_fixture"),
        outputs=("local_cdc_overlay_evidence",),
        evidence_fields=("cdc_overlay_status", "claim_gate_status"),
        runtime_execution=True,
        data_read=True,
        claim_boundary=_LOCAL_TECHNICAL_PREVIEW_BOUNDARY,
    ),
    _etl_workflow_row(
        "sql_dataframe_capability_posture",
        "SQL/DataFrame capability posture",
        "report_only",
        "report_only",
        "none",
        inputs=("sql_text", "dataframe_api_request"),
        outputs=("capability_report", "deterministic_unsupported_diagnostics"),
        evidence_fields=("support_status=report_only", "claim_gate_status=not_claim_grade"),
        blocker_id="cg21.workflow.sql.frontend_unsupported",
        claim_boundary=_REPORT_ONLY_WORKFLOW_BOUNDARY,
    ),
    _etl_workflow_row(
        "data_quality_api",
        "Data-quality API posture",
        "report_only",
        "report_only",
        "none",
        inputs=("data_quality_rule",),
        outputs=("deterministic_unsupported_diagnostics",),
        evidence_fields=("data_quality_report", "claim_gate_status=not_claim_grade"),
        blocker_id="cg21.workflow.data_quality.checks_unsupported",
        claim_boundary=_REPORT_ONLY_WORKFLOW_BOUNDARY,
    ),
    _etl_workflow_row(
        "object_store_runtime",
        "Object-store runtime",
        "blocked",
        "report_only_blocked",
        "none",
        inputs=("s3_uri", "gcs_uri", "adls_uri"),
        outputs=("object_store_plan", "deterministic_blocker"),
        evidence_fields=("object_store_io=false", "credential_policy_status"),
        blocker_id="cg21.workflow.object_store_read.runtime_unsupported",
        claim_boundary=_BLOCKED_WORKFLOW_BOUNDARY,
    ),
    _etl_workflow_row(
        "table_lakehouse_runtime",
        "Table/lakehouse runtime",
        "blocked",
        "report_only_blocked",
        "none",
        inputs=("iceberg_table", "delta_table", "hudi_table"),
        outputs=("table_compatibility_matrix", "deterministic_blocker"),
        evidence_fields=("table_scan_status", "commit_protocol_status"),
        blocker_id="gar-0033.table_lakehouse_runtime_blocked",
        claim_boundary=_BLOCKED_WORKFLOW_BOUNDARY,
    ),
    _etl_workflow_row(
        "production_etl_certification",
        "Production ETL certification",
        "blocked",
        "report_only_blocked",
        "none",
        inputs=("production_workload",),
        outputs=("claim_gate_blocker",),
        evidence_fields=("release_gate_status", "workload_certification_dossier"),
        blocker_id="gar-0033.production_etl_certification_blocked",
        claim_boundary=_BLOCKED_WORKFLOW_BOUNDARY,
    ),
)


@dataclass(frozen=True, slots=True)
class GeneratedSourceCaseCapability:
    """Support and claim posture for one generated-source contract case."""

    case: str
    support_status: str | None
    generated_source_certificate_status: str | None
    generated_source_created: bool | None
    output_io_performed: bool | None
    blocker_id: str | None
    claim_gate_status: str | None


@dataclass(frozen=True, slots=True)
class GeneratedSourceCertificateContract:
    """Typed view over report-only GeneratedSourceCertificate contract fields."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the generated-source contract schema version."""

        return self.capability.field("generated_source_contract_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the generated-source contract report identifier."""

        return self.capability.field("generated_source_contract_report_id")

    @property
    def certificate_schema_version(self) -> str | None:
        """Return the future GeneratedSourceCertificate schema version."""

        return self.capability.field("generated_source_certificate_schema_version")

    @property
    def support_status_vocabulary(self) -> tuple[str, ...]:
        """Return supported generated-source posture status tokens."""

        return _split_csv(
            self.capability.field("generated_source_support_status_vocabulary")
        )

    @property
    def case_order(self) -> tuple[str, ...]:
        """Return generated-source cases in stable report order."""

        return _split_csv(self.capability.field("generated_source_case_order"))

    @property
    def required_field_order(self) -> tuple[str, ...]:
        """Return fields required before generated-output runtime can be claimed."""

        return _split_csv(self.capability.field("generated_source_required_field_order"))

    @property
    def claim_gate_status(self) -> str | None:
        """Return the contract-level claim gate status."""

        return self.capability.field("generated_source_contract_claim_gate_status")

    @property
    def present(self) -> bool:
        """Whether this capability view exposes the generated-source contract."""

        return self.schema_version is not None

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether the contract reports no fallback and no external engine invocation."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_contract_fallback_attempted", True
            )
            is False
            and self.capability.envelope.field_bool(
                "generated_source_contract_external_engine_invoked", True
            )
            is False
            and not self.capability.fallback_attempted
            and not self.capability.external_engine_invoked
        )

    @property
    def no_object_store_or_foundry_runtime(self) -> bool:
        """Whether object-store and Foundry generated-output runtime remain uninvoked."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_contract_object_store_io_performed", True
            )
            is False
            and self.capability.envelope.field_bool(
                "generated_source_contract_foundry_runtime_invoked", True
            )
            is False
        )

    @property
    def broad_sql_dataframe_claim_allowed(self) -> bool:
        """Whether broad SQL/DataFrame generated-output claims are allowed."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_contract_broad_sql_dataframe_claim_allowed", False
            )
            is True
        )

    @property
    def no_dataset_smoke_separate_from_generated_output(self) -> bool:
        """Whether no-dataset smoke remains distinct from generated-output execution."""

        smoke = self.row("no_dataset_smoke")
        return (
            smoke.support_status == "smoke_only"
            and smoke.generated_source_certificate_status
            == "not_applicable_no_generated_rows"
            and smoke.generated_source_created is False
            and smoke.output_io_performed is False
            and self.capability.envelope.field_bool("source_io_performed", True)
            is False
        )

    def row(self, case: str) -> GeneratedSourceCaseCapability:
        """Return the generated-source contract row for a case."""

        normalized = case.strip().lower().replace("-", "_")
        if normalized not in {
            "no_dataset_smoke",
            "user_generated_source",
            "engine_native_generated_source",
        }:
            raise KeyError(f"generated-source case {case!r} is not in the contract")
        return GeneratedSourceCaseCapability(
            case=normalized,
            support_status=self.capability.field(f"{normalized}_support_status"),
            generated_source_certificate_status=self.capability.field(
                f"{normalized}_generated_source_certificate_status"
            ),
            generated_source_created=self.capability.envelope.field_bool(
                f"{normalized}_generated_source_created"
            ),
            output_io_performed=self.capability.envelope.field_bool(
                f"{normalized}_output_io_performed"
            ),
            blocker_id=self.capability.field(f"{normalized}_blocker_id"),
            claim_gate_status=self.capability.field(f"{normalized}_claim_gate_status"),
        )

    @property
    def no_dataset_smoke(self) -> GeneratedSourceCaseCapability:
        """Return the no-dataset smoke contract row."""

        return self.row("no_dataset_smoke")

    @property
    def user_generated_source(self) -> GeneratedSourceCaseCapability:
        """Return the report-only user-generated source contract row."""

        return self.row("user_generated_source")

    @property
    def engine_native_generated_source(self) -> GeneratedSourceCaseCapability:
        """Return the report-only engine-native generated-source contract row."""

        return self.row("engine_native_generated_source")


@dataclass(frozen=True, slots=True)
class GeneratedSourceApiAdmissionRow:
    """Support and evidence posture for one source-free generated-output API form."""

    row_id: str
    support_status: str | None
    runtime_execution: bool | None
    data_read: bool | None
    write_io: bool | None
    source_io_performed: bool | None
    generated_source_created: bool | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    fallback_execution_allowed: bool | None

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the row preserves no-fallback and no-external-engine posture."""

        return (
            self.fallback_attempted is False
            and self.external_engine_invoked is False
            and self.fallback_execution_allowed is False
        )

    @property
    def fixture_smoke_supported(self) -> bool:
        """Whether the row is a scoped fixture-smoke runtime surface."""

        return self.support_status == "fixture_smoke_supported"

    @property
    def report_only(self) -> bool:
        """Whether the row is capability vocabulary without runtime execution."""

        return self.support_status == "report_only"


@dataclass(frozen=True, slots=True)
class GeneratedSourceApiAdmissionMatrix:
    """Typed view over source-free SQL/DataFrame/Python/API admission rows."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the source-free API admission matrix schema version."""

        return self.capability.field("generated_source_api_admission_schema_version")

    @property
    def matrix_id(self) -> str | None:
        """Return the source-free API admission matrix identifier."""

        return self.capability.field("generated_source_api_admission_matrix_id")

    @property
    def present(self) -> bool:
        """Whether this capability exposes the source-free API admission matrix."""

        return self.schema_version is not None

    @property
    def support_status_vocabulary(self) -> tuple[str, ...]:
        """Return supported posture tokens for admission rows."""

        return _split_csv(
            self.capability.field("generated_source_api_admission_support_status_vocabulary")
        )

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return source-free admission row IDs in stable order."""

        return _split_csv(self.capability.field("generated_source_api_admission_row_order"))

    @property
    def python_row_order(self) -> tuple[str, ...]:
        """Return Python admission rows."""

        return _split_csv(
            self.capability.field("generated_source_api_admission_python_row_order")
        )

    @property
    def sql_row_order(self) -> tuple[str, ...]:
        """Return SQL admission rows."""

        return _split_csv(
            self.capability.field("generated_source_api_admission_sql_row_order")
        )

    @property
    def dataframe_row_order(self) -> tuple[str, ...]:
        """Return DataFrame admission rows."""

        return _split_csv(
            self.capability.field("generated_source_api_admission_dataframe_row_order")
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the admission-matrix claim gate status."""

        return self.capability.field("generated_source_api_admission_claim_gate_status")

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every exposed row preserves no fallback and no external engine."""

        keys = (
            "generated_source_api_admission_fallback_attempted",
            "generated_source_api_admission_external_engine_invoked",
            "generated_source_api_admission_fallback_execution_allowed",
        )
        if not all(self.capability.field(key) is not None for key in keys):
            return False
        return (
            self.capability.envelope.field_bool(keys[0], True) is False
            and self.capability.envelope.field_bool(keys[1], True) is False
            and self.capability.envelope.field_bool(keys[2], True) is False
            and all(self.row(row_id).no_fallback_no_external_engine for row_id in self.row_order)
        )

    @property
    def broad_sql_dataframe_claim_allowed(self) -> bool:
        """Whether broad SQL/DataFrame generated-output claims are allowed."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_api_admission_broad_sql_dataframe_claim_allowed",
                False,
            )
            is True
        )

    def row(self, row_id: str) -> GeneratedSourceApiAdmissionRow:
        """Return one source-free API admission row by ID."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"source-free generated-output admission row {row_id!r} is not present")
        return GeneratedSourceApiAdmissionRow(
            row_id=normalized,
            support_status=self.capability.field(f"{normalized}_support_status"),
            runtime_execution=self.capability.envelope.field_bool(
                f"{normalized}_runtime_execution"
            ),
            data_read=self.capability.envelope.field_bool(f"{normalized}_data_read"),
            write_io=self.capability.envelope.field_bool(f"{normalized}_write_io"),
            source_io_performed=self.capability.envelope.field_bool(
                f"{normalized}_source_io_performed"
            ),
            generated_source_created=self.capability.envelope.field_bool(
                f"{normalized}_generated_source_created"
            ),
            blocker_id=self.capability.field(f"{normalized}_blocker_id"),
            required_evidence=_split_csv(
                self.capability.field(f"{normalized}_required_evidence")
            ),
            claim_gate_status=self.capability.field(f"{normalized}_claim_gate_status"),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{normalized}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{normalized}_external_engine_invoked"
            ),
            fallback_execution_allowed=self.capability.envelope.field_bool(
                f"{normalized}_fallback_execution_allowed"
            ),
        )


@dataclass(frozen=True, slots=True)
class GeneratedSourceEvidenceAlignmentRow:
    """One generated-source cross-surface alignment row."""

    row_id: str
    support_status: str | None
    source_free_case: str | None
    runtime_execution: bool | None
    generated_source_certificate_status: str | None
    output_native_io_certificate_status: str | None
    openlineage_facet_status: str | None
    opentelemetry_span_status: str | None
    bayesian_confidence_status: str | None
    foundry_boundary_ref: str | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this row preserves no fallback and no external engine execution."""

        return self.fallback_attempted is False and self.external_engine_invoked is False


@dataclass(frozen=True, slots=True)
class GeneratedSourceEvidenceAlignmentReport:
    """Typed view over GAR-NOVEL-1A generated-source evidence alignment fields."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the generated-source evidence alignment schema version."""

        return self.capability.field("generated_source_evidence_alignment_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the generated-source evidence alignment report identifier."""

        return self.capability.field("generated_source_evidence_alignment_report_id")

    @property
    def docs_ref(self) -> str | None:
        """Return the architecture document that owns the alignment model."""

        return self.capability.field("generated_source_evidence_alignment_docs_ref")

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return alignment row IDs in stable order."""

        return _split_csv(
            self.capability.field("generated_source_evidence_alignment_row_order")
        )

    @property
    def present(self) -> bool:
        """Whether this capability exposes the GAR-NOVEL-1A alignment report."""

        return self.schema_version is not None

    @property
    def openlineage_export_enabled(self) -> bool:
        """Whether OpenLineage event export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_evidence_alignment_openlineage_export_enabled",
                False,
            )
            is True
        )

    @property
    def opentelemetry_export_enabled(self) -> bool:
        """Whether OpenTelemetry export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_evidence_alignment_opentelemetry_export_enabled",
                False,
            )
            is True
        )

    @property
    def opentelemetry_network_exporter_enabled(self) -> bool:
        """Whether an OpenTelemetry network exporter is enabled."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_evidence_alignment_opentelemetry_network_exporter_enabled",
                False,
            )
            is True
        )

    @property
    def bayesian_confidence_enabled(self) -> bool:
        """Whether Bayesian claim-confidence runtime decisioning is enabled."""

        return (
            self.capability.envelope.field_bool(
                "generated_source_evidence_alignment_bayesian_confidence_enabled",
                False,
            )
            is True
        )

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether the alignment report and rows preserve no-fallback policy."""

        keys = (
            "generated_source_evidence_alignment_fallback_attempted",
            "generated_source_evidence_alignment_external_engine_invoked",
            "generated_source_evidence_alignment_all_rows_no_fallback_no_external_engine",
        )
        return (
            self.capability.envelope.field_bool(keys[0], True) is False
            and self.capability.envelope.field_bool(keys[1], True) is False
            and self.capability.envelope.field_bool(keys[2], False) is True
            and all(self.row(row_id).no_fallback_no_external_engine for row_id in self.row_order)
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the alignment-level claim gate status."""

        return self.capability.field("generated_source_evidence_alignment_claim_gate_status")

    def row(self, row_id: str) -> GeneratedSourceEvidenceAlignmentRow:
        """Return one generated-source evidence alignment row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(
                f"generated-source evidence alignment row {row_id!r} is not present"
            )
        prefix = f"generated_source_evidence_alignment_row_{normalized}"
        return GeneratedSourceEvidenceAlignmentRow(
            row_id=normalized,
            support_status=self.capability.field(f"{prefix}_support_status"),
            source_free_case=self.capability.field(f"{prefix}_source_free_case"),
            runtime_execution=self.capability.envelope.field_bool(
                f"{prefix}_runtime_execution"
            ),
            generated_source_certificate_status=self.capability.field(
                f"{prefix}_generated_source_certificate_status"
            ),
            output_native_io_certificate_status=self.capability.field(
                f"{prefix}_output_native_io_certificate_status"
            ),
            openlineage_facet_status=self.capability.field(
                f"{prefix}_openlineage_facet_status"
            ),
            opentelemetry_span_status=self.capability.field(
                f"{prefix}_opentelemetry_span_status"
            ),
            bayesian_confidence_status=self.capability.field(
                f"{prefix}_bayesian_confidence_status"
            ),
            foundry_boundary_ref=self.capability.field(f"{prefix}_foundry_boundary_ref"),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
        )


@dataclass(frozen=True, slots=True)
class OpenLineageFacetMappingRow:
    """One report-only ShardLoom-owned OpenLineage custom facet mapping row."""

    row_id: str
    facet_name: str | None
    facet_key: str | None
    openlineage_entity: str | None
    shardloom_evidence_fields: tuple[str, ...]
    schema_url_placeholder: str | None
    schema_version: str | None
    producer: str | None
    facet_status: str | None
    export_enabled: bool | None
    event_emitted: bool | None
    network_call_performed: bool | None
    redaction_required: bool | None
    retention_policy_required: bool | None
    claim_gate_status: str | None
    claim_boundary: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None

    @property
    def report_only_no_export(self) -> bool:
        """Whether this facet row is a schema placeholder with no export effects."""

        return (
            self.facet_status == "report_only_schema_placeholder"
            and self.export_enabled is False
            and self.event_emitted is False
            and self.network_call_performed is False
        )

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this row preserves no fallback and no external engine execution."""

        return self.fallback_attempted is False and self.external_engine_invoked is False


@dataclass(frozen=True, slots=True)
class OpenLineageFacetMappingReport:
    """Typed view over GAR-NOVEL-1B OpenLineage facet mapping fields."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the report schema version."""

        return self.capability.field("openlineage_facet_mapping_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the report identifier."""

        return self.capability.field("openlineage_facet_mapping_report_id")

    @property
    def gar_id(self) -> str | None:
        """Return the GAR item that owns this report."""

        return self.capability.field("openlineage_facet_mapping_gar_id")

    @property
    def docs_ref(self) -> str | None:
        """Return the architecture document that owns the mapping."""

        return self.capability.field("openlineage_facet_mapping_docs_ref")

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return facet mapping row IDs in stable order."""

        return _split_csv(self.capability.field("openlineage_facet_mapping_row_order"))

    @property
    def present(self) -> bool:
        """Whether this capability exposes the GAR-NOVEL-1B mapping report."""

        return self.schema_version is not None

    @property
    def export_enabled(self) -> bool:
        """Whether OpenLineage export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_export_enabled",
                False,
            )
            is True
        )

    @property
    def event_emitted(self) -> bool:
        """Whether this report emitted an OpenLineage event."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_event_emitted",
                False,
            )
            is True
        )

    @property
    def network_call_performed(self) -> bool:
        """Whether this report performed a network call."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_network_call_performed",
                False,
            )
            is True
        )

    @property
    def schema_published(self) -> bool:
        """Whether public OpenLineage facet schemas have been published."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_schema_published",
                False,
            )
            is True
        )

    @property
    def all_rows_report_only(self) -> bool:
        """Whether all row mappings are report-only no-export placeholders."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_all_rows_report_only",
                False,
            )
            is True
            and all(self.row(row_id).report_only_no_export for row_id in self.row_order)
        )

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether the report and all rows preserve no-fallback policy."""

        return (
            self.capability.envelope.field_bool(
                "openlineage_facet_mapping_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
            and all(self.row(row_id).no_fallback_no_external_engine for row_id in self.row_order)
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the mapping-level claim gate status."""

        return self.capability.field("openlineage_facet_mapping_claim_gate_status")

    def row(self, row_id: str) -> OpenLineageFacetMappingRow:
        """Return one OpenLineage facet mapping row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"OpenLineage facet mapping row {row_id!r} is not present")
        prefix = f"openlineage_facet_mapping_row_{normalized}"
        return OpenLineageFacetMappingRow(
            row_id=normalized,
            facet_name=self.capability.field(f"{prefix}_facet_name"),
            facet_key=self.capability.field(f"{prefix}_facet_key"),
            openlineage_entity=self.capability.field(f"{prefix}_openlineage_entity"),
            shardloom_evidence_fields=_split_csv(
                self.capability.field(f"{prefix}_shardloom_evidence_fields")
            ),
            schema_url_placeholder=self.capability.field(
                f"{prefix}_schema_url_placeholder"
            ),
            schema_version=self.capability.field(f"{prefix}_schema_version"),
            producer=self.capability.field(f"{prefix}_producer"),
            facet_status=self.capability.field(f"{prefix}_facet_status"),
            export_enabled=self.capability.envelope.field_bool(
                f"{prefix}_export_enabled"
            ),
            event_emitted=self.capability.envelope.field_bool(
                f"{prefix}_event_emitted"
            ),
            network_call_performed=self.capability.envelope.field_bool(
                f"{prefix}_network_call_performed"
            ),
            redaction_required=self.capability.envelope.field_bool(
                f"{prefix}_redaction_required"
            ),
            retention_policy_required=self.capability.envelope.field_bool(
                f"{prefix}_retention_policy_required"
            ),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
        )


@dataclass(frozen=True, slots=True)
class OpenTelemetryTraceExportSpanRow:
    """One report-only OpenTelemetry span mapping row."""

    row_id: str
    span_name: str | None
    span_kind: str | None
    timing_fields: tuple[str, ...]
    shardloom_attribute_allowlist: tuple[str, ...]
    redaction_policy: str | None
    sensitive_fields: tuple[str, ...]
    metric_refs: tuple[str, ...]
    span_status: str | None
    export_enabled: bool | None
    span_emitted: bool | None
    metric_emitted: bool | None
    log_emitted: bool | None
    network_exporter_enabled: bool | None
    redaction_required: bool | None
    retention_policy_required: bool | None
    claim_gate_status: str | None
    claim_boundary: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None

    @property
    def report_only_no_export(self) -> bool:
        """Whether this row is a non-emitting report-only span placeholder."""

        return (
            self.span_status == "report_only_not_emitted"
            and self.export_enabled is False
            and self.span_emitted is False
            and self.metric_emitted is False
            and self.log_emitted is False
            and self.network_exporter_enabled is False
        )

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this row preserves no fallback and no external engine execution."""

        return self.fallback_attempted is False and self.external_engine_invoked is False


@dataclass(frozen=True, slots=True)
class OpenTelemetryTraceExportContractReport:
    """Typed view over GAR-NOVEL-1C OpenTelemetry trace-export contract fields."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the OpenTelemetry trace-export contract schema version."""

        return self.capability.field("opentelemetry_trace_export_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the report identifier."""

        return self.capability.field("opentelemetry_trace_export_report_id")

    @property
    def gar_id(self) -> str | None:
        """Return the GAR item that owns this report."""

        return self.capability.field("opentelemetry_trace_export_gar_id")

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return span mapping row IDs in stable order."""

        return _split_csv(self.capability.field("opentelemetry_trace_export_row_order"))

    @property
    def present(self) -> bool:
        """Whether this capability exposes the GAR-NOVEL-1C contract."""

        return self.schema_version is not None

    @property
    def trace_export_enabled(self) -> bool:
        """Whether trace export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_trace_export_enabled",
                False,
            )
            is True
        )

    @property
    def metric_export_enabled(self) -> bool:
        """Whether metric export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_metric_export_enabled",
                False,
            )
            is True
        )

    @property
    def log_export_enabled(self) -> bool:
        """Whether log export is enabled by this capability view."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_log_export_enabled",
                False,
            )
            is True
        )

    @property
    def network_exporter_enabled(self) -> bool:
        """Whether any network exporter is enabled."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_network_exporter_enabled",
                False,
            )
            is True
        )

    @property
    def otlp_exporter_configured(self) -> bool:
        """Whether an OTLP exporter is configured."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_otlp_exporter_configured",
                False,
            )
            is True
        )

    @property
    def trace_emitted(self) -> bool:
        """Whether this report emitted trace data."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_trace_emitted",
                False,
            )
            is True
        )

    @property
    def network_call_performed(self) -> bool:
        """Whether this report performed a network call."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_network_call_performed",
                False,
            )
            is True
        )

    @property
    def all_rows_report_only(self) -> bool:
        """Whether all rows are report-only non-exporting span placeholders."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_all_rows_report_only",
                False,
            )
            is True
            and all(self.row(row_id).report_only_no_export for row_id in self.row_order)
        )

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether the report and all rows preserve no-fallback policy."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
            and all(self.row(row_id).no_fallback_no_external_engine for row_id in self.row_order)
        )

    @property
    def no_export_side_effects(self) -> bool:
        """Whether the report has no exporter/backend/runtime side effects."""

        return (
            self.capability.envelope.field_bool(
                "opentelemetry_trace_export_no_export_side_effects",
                False,
            )
            is True
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the mapping-level claim gate status."""

        return self.capability.field("opentelemetry_trace_export_claim_gate_status")

    def row(self, row_id: str) -> OpenTelemetryTraceExportSpanRow:
        """Return one OpenTelemetry span mapping row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"OpenTelemetry trace export row {row_id!r} is not present")
        prefix = f"opentelemetry_trace_export_span_{normalized}"
        return OpenTelemetryTraceExportSpanRow(
            row_id=normalized,
            span_name=self.capability.field(f"{prefix}_span_name"),
            span_kind=self.capability.field(f"{prefix}_span_kind"),
            timing_fields=_split_csv(self.capability.field(f"{prefix}_timing_fields")),
            shardloom_attribute_allowlist=_split_csv(
                self.capability.field(f"{prefix}_shardloom_attribute_allowlist")
            ),
            redaction_policy=self.capability.field(f"{prefix}_redaction_policy"),
            sensitive_fields=_split_csv(self.capability.field(f"{prefix}_sensitive_fields")),
            metric_refs=_split_csv(self.capability.field(f"{prefix}_metric_refs")),
            span_status=self.capability.field(f"{prefix}_span_status"),
            export_enabled=self.capability.envelope.field_bool(
                f"{prefix}_export_enabled"
            ),
            span_emitted=self.capability.envelope.field_bool(f"{prefix}_span_emitted"),
            metric_emitted=self.capability.envelope.field_bool(
                f"{prefix}_metric_emitted"
            ),
            log_emitted=self.capability.envelope.field_bool(f"{prefix}_log_emitted"),
            network_exporter_enabled=self.capability.envelope.field_bool(
                f"{prefix}_network_exporter_enabled"
            ),
            redaction_required=self.capability.envelope.field_bool(
                f"{prefix}_redaction_required"
            ),
            retention_policy_required=self.capability.envelope.field_bool(
                f"{prefix}_retention_policy_required"
            ),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
        )


@dataclass(frozen=True, slots=True)
class UniversalCompatibilityRow:
    """One row from the universal source/sink compatibility scoreboard."""

    surface_id: str
    surface: str | None
    surface_family: str | None
    direction: str | None
    support_status: str | None
    runtime_supported: bool | None
    smoke_supported: bool | None
    report_only: bool | None
    credential_required: bool | None
    network_required: bool | None
    source_io_performed: bool | None
    output_io_performed: bool | None
    native_io_certificate_status: str | None
    generated_source_certificate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    claim_gate_status: str | None
    blocker_id: str | None
    required_future_evidence: tuple[str, ...]
    claim_boundary: str | None

    @property
    def supported_for_runtime_claims(self) -> bool:
        """Whether this row can be treated as a runtime support claim."""

        return self.support_status == "runtime-supported" and self.runtime_supported is True

    @property
    def blocked_or_report_only(self) -> bool:
        """Whether this row is deliberately not runtime-supported."""

        return self.support_status in {"blocked", "report-only", "not-planned"}

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the row preserves ShardLoom's no-fallback boundary."""

        return self.fallback_attempted is False and self.external_engine_invoked is False


@dataclass(frozen=True, slots=True)
class SourceFreeGeneratedOutputCompatibilityRow:
    """One compatibility-level source-free generated-output admission row."""

    row_id: str
    user_visible_surface: str | None
    surface_family: str | None
    support_status: str | None
    runtime_execution: bool | None
    data_read: bool | None
    write_io: bool | None
    source_io_performed: bool | None
    generated_source_created: bool | None
    output_io_performed: bool | None
    source_native_io_certificate_status: str | None
    output_native_io_certificate_status: str | None
    generated_source_certificate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    claim_boundary: str | None

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether this row preserves no fallback and no external engine invocation."""

        return self.fallback_attempted is False and self.external_engine_invoked is False

    @property
    def fixture_smoke_supported(self) -> bool:
        """Whether this row is a scoped local generated-output smoke surface."""

        return self.support_status == "smoke-supported"

    @property
    def report_only(self) -> bool:
        """Whether this row is capability/report vocabulary only."""

        return self.support_status == "report-only"


@dataclass(frozen=True, slots=True)
class SourceFreeGeneratedOutputCompatibilityContract:
    """Compatibility scoreboard projection for source-free generated-output surfaces."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the generated-output compatibility contract schema version."""

        return self.capability.field(
            "universal_compatibility_generated_output_contract_schema_version"
        )

    @property
    def contract_id(self) -> str | None:
        """Return the generated-output compatibility contract identifier."""

        return self.capability.field("universal_compatibility_generated_output_contract_id")

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return source-free generated-output rows in stable order."""

        return _split_csv(self.capability.field("universal_compatibility_generated_output_row_order"))

    @property
    def python_row_order(self) -> tuple[str, ...]:
        """Return Python generated-output compatibility rows."""

        return _split_csv(
            self.capability.field("universal_compatibility_generated_output_python_row_order")
        )

    @property
    def sql_row_order(self) -> tuple[str, ...]:
        """Return SQL source-free generated-output compatibility rows."""

        return _split_csv(
            self.capability.field("universal_compatibility_generated_output_sql_row_order")
        )

    @property
    def dataframe_row_order(self) -> tuple[str, ...]:
        """Return DataFrame source-free generated-output compatibility rows."""

        return _split_csv(
            self.capability.field("universal_compatibility_generated_output_dataframe_row_order")
        )

    @property
    def rows(self) -> tuple[SourceFreeGeneratedOutputCompatibilityRow, ...]:
        """Return all source-free generated-output compatibility rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def claim_gate_status(self) -> str | None:
        """Return the generated-output compatibility claim gate status."""

        return self.capability.field("universal_compatibility_generated_output_claim_gate_status")

    @property
    def no_dataset_smoke_separate(self) -> bool:
        """Whether no-dataset smoke remains separate from generated-output execution."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_no_dataset_smoke_separate",
                False,
            )
            is True
        )

    @property
    def local_output_only(self) -> bool:
        """Whether generated-output support remains local-output-only."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_local_output_only",
                False,
            )
            is True
        )

    @property
    def output_certificate_required(self) -> bool:
        """Whether generated-output data claims require output Native I/O evidence."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_output_certificate_required",
                False,
            )
            is True
        )

    @property
    def object_store_runtime_supported(self) -> bool:
        """Whether object-store generated-output runtime is supported."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_object_store_runtime_supported",
                False,
            )
            is True
        )

    @property
    def foundry_runtime_supported(self) -> bool:
        """Whether Foundry generated-output runtime is supported."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_foundry_runtime_supported",
                False,
            )
            is True
        )

    @property
    def broad_sql_dataframe_claim_allowed(self) -> bool:
        """Whether broad SQL/DataFrame generated-output claims are allowed."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed",
                False,
            )
            is True
        )

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves no fallback and no external engine invocation."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_all_rows_fallback_attempted_false",
                False,
            )
            is True
            and self.capability.envelope.field_bool(
                "universal_compatibility_generated_output_all_rows_external_engine_invoked_false",
                False,
            )
            is True
            and all(row.no_fallback_no_external_engine for row in self.rows)
        )

    def row(self, row_id: str) -> SourceFreeGeneratedOutputCompatibilityRow:
        """Return one source-free generated-output compatibility row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"source-free generated-output row {row_id!r} is not present")
        prefix = f"universal_compatibility_generated_output_row_{normalized}"
        return SourceFreeGeneratedOutputCompatibilityRow(
            row_id=normalized,
            user_visible_surface=self.capability.field(f"{prefix}_user_visible_surface"),
            surface_family=self.capability.field(f"{prefix}_surface_family"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            runtime_execution=self.capability.envelope.field_bool(
                f"{prefix}_runtime_execution"
            ),
            data_read=self.capability.envelope.field_bool(f"{prefix}_data_read"),
            write_io=self.capability.envelope.field_bool(f"{prefix}_write_io"),
            source_io_performed=self.capability.envelope.field_bool(
                f"{prefix}_source_io_performed"
            ),
            generated_source_created=self.capability.envelope.field_bool(
                f"{prefix}_generated_source_created"
            ),
            output_io_performed=self.capability.envelope.field_bool(
                f"{prefix}_output_io_performed"
            ),
            source_native_io_certificate_status=self.capability.field(
                f"{prefix}_source_native_io_certificate_status"
            ),
            output_native_io_certificate_status=self.capability.field(
                f"{prefix}_output_native_io_certificate_status"
            ),
            generated_source_certificate_status=self.capability.field(
                f"{prefix}_generated_source_certificate_status"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class ObjectStoreAdmissionLadderRow:
    """One S3/GCS/ADLS object-store admission ladder row."""

    row_id: str
    provider_scope: str | None
    stage: str | None
    support_status: str | None
    credential_policy_status: str | None
    credential_resolution_performed: bool | None
    network_probe_allowed: bool | None
    provider_probe_allowed: bool | None
    byte_range_read_allowed: bool | None
    full_file_read_allowed: bool | None
    local_cache_allowed: bool | None
    write_io_allowed: bool | None
    commit_protocol_allowed: bool | None
    object_store_io: bool | None
    write_io: bool | None
    native_io_certificate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    claim_boundary: str | None

    @property
    def no_effects_no_fallback(self) -> bool:
        """Whether the ladder row remains side-effect-free and fallback-free."""

        return (
            self.credential_resolution_performed is False
            and self.network_probe_allowed is False
            and self.provider_probe_allowed is False
            and self.byte_range_read_allowed is False
            and self.full_file_read_allowed is False
            and self.local_cache_allowed is False
            and self.write_io_allowed is False
            and self.commit_protocol_allowed is False
            and self.object_store_io is False
            and self.write_io is False
            and self.fallback_attempted is False
            and self.external_engine_invoked is False
        )


@dataclass(frozen=True, slots=True)
class ObjectStoreAdmissionLadder:
    """Compatibility scoreboard projection for object-store runtime admission."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the object-store admission ladder schema version."""

        return self.capability.field("universal_compatibility_object_store_ladder_schema_version")

    @property
    def ladder_id(self) -> str | None:
        """Return the object-store admission ladder identifier."""

        return self.capability.field("universal_compatibility_object_store_ladder_id")

    @property
    def provider_scope(self) -> tuple[str, ...]:
        """Return providers covered by this ladder."""

        return _split_csv(self.capability.field("universal_compatibility_object_store_ladder_provider_scope"))

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return admission ladder rows in stable order."""

        return _split_csv(self.capability.field("universal_compatibility_object_store_ladder_row_order"))

    @property
    def rows(self) -> tuple[ObjectStoreAdmissionLadderRow, ...]:
        """Return all object-store admission ladder rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def runtime_supported(self) -> bool:
        """Whether object-store runtime is supported by this ladder."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_ladder_runtime_supported",
                False,
            )
            is True
        )

    @property
    def public_no_credential_read_supported(self) -> bool:
        """Whether the public no-credential fixture profile is admitted."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_ladder_public_no_credential_read_supported",
                False,
            )
            is True
        )

    @property
    def all_rows_no_effects(self) -> bool:
        """Whether every ladder row preserves no-effects/no-fallback posture."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_ladder_all_rows_no_effects",
                False,
            )
            is True
            and all(row.no_effects_no_fallback for row in self.rows)
        )

    @property
    def all_live_provider_effects_disabled(self) -> bool:
        """Whether live provider credential/network/cache/write effects stay disabled."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_ladder_all_live_provider_effects_disabled",
                False,
            )
            is True
        )

    @property
    def all_no_fallback_no_external_engine(self) -> bool:
        """Whether every ladder row preserves no-fallback/no-external-engine posture."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_ladder_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
        )

    def row(self, row_id: str) -> ObjectStoreAdmissionLadderRow:
        """Return one object-store admission ladder row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"object-store admission ladder row {row_id!r} is not present")
        prefix = f"universal_compatibility_object_store_ladder_row_{normalized}"
        return ObjectStoreAdmissionLadderRow(
            row_id=normalized,
            provider_scope=self.capability.field(f"{prefix}_provider_scope"),
            stage=self.capability.field(f"{prefix}_stage"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            credential_policy_status=self.capability.field(f"{prefix}_credential_policy_status"),
            credential_resolution_performed=self.capability.envelope.field_bool(
                f"{prefix}_credential_resolution_performed"
            ),
            network_probe_allowed=self.capability.envelope.field_bool(
                f"{prefix}_network_probe_allowed"
            ),
            provider_probe_allowed=self.capability.envelope.field_bool(
                f"{prefix}_provider_probe_allowed"
            ),
            byte_range_read_allowed=self.capability.envelope.field_bool(
                f"{prefix}_byte_range_read_allowed"
            ),
            full_file_read_allowed=self.capability.envelope.field_bool(
                f"{prefix}_full_file_read_allowed"
            ),
            local_cache_allowed=self.capability.envelope.field_bool(
                f"{prefix}_local_cache_allowed"
            ),
            write_io_allowed=self.capability.envelope.field_bool(f"{prefix}_write_io_allowed"),
            commit_protocol_allowed=self.capability.envelope.field_bool(
                f"{prefix}_commit_protocol_allowed"
            ),
            object_store_io=self.capability.envelope.field_bool(f"{prefix}_object_store_io"),
            write_io=self.capability.envelope.field_bool(f"{prefix}_write_io"),
            native_io_certificate_status=self.capability.field(
                f"{prefix}_native_io_certificate_status"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class TableFormatBoundaryMatrixRow:
    """One Iceberg/Delta/Hudi table-format boundary matrix row."""

    row_id: str
    format_scope: str | None
    behavior: str | None
    support_status: str | None
    local_metadata_smoke_related: bool | None
    table_format_dependency_required: bool | None
    catalog_io_allowed: bool | None
    object_store_io_allowed: bool | None
    table_metadata_read_allowed: bool | None
    table_data_read_allowed: bool | None
    delete_tombstone_runtime_allowed: bool | None
    write_io_allowed: bool | None
    commit_allowed: bool | None
    rollback_allowed: bool | None
    native_io_certificate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    claim_boundary: str | None

    @property
    def no_io_no_fallback(self) -> bool:
        """Whether the row remains I/O-free and fallback-free."""

        return (
            self.catalog_io_allowed is False
            and self.object_store_io_allowed is False
            and self.table_metadata_read_allowed is False
            and self.table_data_read_allowed is False
            and self.delete_tombstone_runtime_allowed is False
            and self.write_io_allowed is False
            and self.commit_allowed is False
            and self.rollback_allowed is False
            and self.fallback_attempted is False
            and self.external_engine_invoked is False
        )


@dataclass(frozen=True, slots=True)
class TableFormatBoundaryMatrix:
    """Compatibility scoreboard projection for Iceberg/Delta/Hudi boundaries."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the table-format matrix schema version."""

        return self.capability.field("universal_compatibility_table_format_matrix_schema_version")

    @property
    def matrix_id(self) -> str | None:
        """Return the table-format matrix identifier."""

        return self.capability.field("universal_compatibility_table_format_matrix_id")

    @property
    def format_scope(self) -> tuple[str, ...]:
        """Return table formats covered by this matrix."""

        return _split_csv(self.capability.field("universal_compatibility_table_format_matrix_format_scope"))

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return matrix rows in stable order."""

        return _split_csv(self.capability.field("universal_compatibility_table_format_matrix_row_order"))

    @property
    def rows(self) -> tuple[TableFormatBoundaryMatrixRow, ...]:
        """Return all table-format boundary rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def runtime_supported(self) -> bool:
        """Whether table-format runtime is supported by this matrix."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_table_format_matrix_runtime_supported",
                False,
            )
            is True
        )

    @property
    def local_metadata_smoke_available(self) -> bool:
        """Whether scoped local metadata smoke evidence exists."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_table_format_matrix_local_metadata_smoke_available",
                False,
            )
            is True
        )

    @property
    def all_rows_no_io_no_fallback(self) -> bool:
        """Whether every matrix row preserves no-I/O/no-fallback posture."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_table_format_matrix_all_rows_no_io_no_fallback",
                False,
            )
            is True
            and all(row.no_io_no_fallback for row in self.rows)
        )

    def row(self, row_id: str) -> TableFormatBoundaryMatrixRow:
        """Return one table-format matrix row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"table-format boundary matrix row {row_id!r} is not present")
        prefix = f"universal_compatibility_table_format_matrix_row_{normalized}"
        return TableFormatBoundaryMatrixRow(
            row_id=normalized,
            format_scope=self.capability.field(f"{prefix}_format_scope"),
            behavior=self.capability.field(f"{prefix}_behavior"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            local_metadata_smoke_related=self.capability.envelope.field_bool(
                f"{prefix}_local_metadata_smoke_related"
            ),
            table_format_dependency_required=self.capability.envelope.field_bool(
                f"{prefix}_table_format_dependency_required"
            ),
            catalog_io_allowed=self.capability.envelope.field_bool(
                f"{prefix}_catalog_io_allowed"
            ),
            object_store_io_allowed=self.capability.envelope.field_bool(
                f"{prefix}_object_store_io_allowed"
            ),
            table_metadata_read_allowed=self.capability.envelope.field_bool(
                f"{prefix}_table_metadata_read_allowed"
            ),
            table_data_read_allowed=self.capability.envelope.field_bool(
                f"{prefix}_table_data_read_allowed"
            ),
            delete_tombstone_runtime_allowed=self.capability.envelope.field_bool(
                f"{prefix}_delete_tombstone_runtime_allowed"
            ),
            write_io_allowed=self.capability.envelope.field_bool(f"{prefix}_write_io_allowed"),
            commit_allowed=self.capability.envelope.field_bool(f"{prefix}_commit_allowed"),
            rollback_allowed=self.capability.envelope.field_bool(f"{prefix}_rollback_allowed"),
            native_io_certificate_status=self.capability.field(
                f"{prefix}_native_io_certificate_status"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class DatabaseWarehouseBoundaryMatrixRow:
    """One database/warehouse import-export boundary matrix row."""

    row_id: str
    endpoint_scope: str | None
    endpoint_family: str | None
    connector_type: str | None
    support_status: str | None
    credential_required: bool | None
    network_required: bool | None
    driver_dependency_required: bool | None
    credential_resolution_performed: bool | None
    network_probe_performed: bool | None
    driver_loaded: bool | None
    import_runtime_supported: bool | None
    export_runtime_supported: bool | None
    query_pushdown_supported: bool | None
    external_baseline_only: bool | None
    native_io_certificate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    claim_boundary: str | None

    @property
    def no_effects_no_fallback(self) -> bool:
        """Whether the row remains free of connector effects and fallback execution."""

        return (
            self.credential_resolution_performed is False
            and self.network_probe_performed is False
            and self.driver_loaded is False
            and self.import_runtime_supported is False
            and self.export_runtime_supported is False
            and self.query_pushdown_supported is False
            and self.fallback_attempted is False
            and self.external_engine_invoked is False
        )


@dataclass(frozen=True, slots=True)
class DatabaseWarehouseBoundaryMatrix:
    """Compatibility scoreboard projection for database/warehouse boundaries."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the database/warehouse matrix schema version."""

        return self.capability.field(
            "universal_compatibility_database_warehouse_matrix_schema_version"
        )

    @property
    def matrix_id(self) -> str | None:
        """Return the database/warehouse matrix identifier."""

        return self.capability.field("universal_compatibility_database_warehouse_matrix_id")

    @property
    def endpoint_scope(self) -> tuple[str, ...]:
        """Return database and warehouse endpoints covered by this matrix."""

        return _split_csv(
            self.capability.field(
                "universal_compatibility_database_warehouse_matrix_endpoint_scope"
            )
        )

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return matrix rows in stable order."""

        return _split_csv(
            self.capability.field(
                "universal_compatibility_database_warehouse_matrix_row_order"
            )
        )

    @property
    def rows(self) -> tuple[DatabaseWarehouseBoundaryMatrixRow, ...]:
        """Return all database/warehouse boundary rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def runtime_supported(self) -> bool:
        """Whether database/warehouse connector runtime is supported by this matrix."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_database_warehouse_matrix_runtime_supported",
                False,
            )
            is True
        )

    @property
    def all_rows_no_effects(self) -> bool:
        """Whether every matrix row preserves no-effects/no-fallback posture."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_database_warehouse_matrix_all_rows_no_effects",
                False,
            )
            is True
            and all(row.no_effects_no_fallback for row in self.rows)
        )

    def row(self, row_id: str) -> DatabaseWarehouseBoundaryMatrixRow:
        """Return one database/warehouse matrix row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"database/warehouse boundary matrix row {row_id!r} is not present")
        prefix = f"universal_compatibility_database_warehouse_matrix_row_{normalized}"
        return DatabaseWarehouseBoundaryMatrixRow(
            row_id=normalized,
            endpoint_scope=self.capability.field(f"{prefix}_endpoint_scope"),
            endpoint_family=self.capability.field(f"{prefix}_endpoint_family"),
            connector_type=self.capability.field(f"{prefix}_connector_type"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            credential_required=self.capability.envelope.field_bool(
                f"{prefix}_credential_required"
            ),
            network_required=self.capability.envelope.field_bool(f"{prefix}_network_required"),
            driver_dependency_required=self.capability.envelope.field_bool(
                f"{prefix}_driver_dependency_required"
            ),
            credential_resolution_performed=self.capability.envelope.field_bool(
                f"{prefix}_credential_resolution_performed"
            ),
            network_probe_performed=self.capability.envelope.field_bool(
                f"{prefix}_network_probe_performed"
            ),
            driver_loaded=self.capability.envelope.field_bool(f"{prefix}_driver_loaded"),
            import_runtime_supported=self.capability.envelope.field_bool(
                f"{prefix}_import_runtime_supported"
            ),
            export_runtime_supported=self.capability.envelope.field_bool(
                f"{prefix}_export_runtime_supported"
            ),
            query_pushdown_supported=self.capability.envelope.field_bool(
                f"{prefix}_query_pushdown_supported"
            ),
            external_baseline_only=self.capability.envelope.field_bool(
                f"{prefix}_external_baseline_only"
            ),
            native_io_certificate_status=self.capability.field(
                f"{prefix}_native_io_certificate_status"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class UniversalCompatibilityScoreboard:
    """Typed view over the universal source/sink compatibility scoreboard."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the scoreboard schema version."""

        return self.capability.field("universal_compatibility_scoreboard_schema_version")

    @property
    def scoreboard_id(self) -> str | None:
        """Return the scoreboard identifier."""

        return self.capability.field("universal_compatibility_scoreboard_id")

    @property
    def docs_ref(self) -> str | None:
        """Return the human-readable source document path."""

        return self.capability.field("universal_compatibility_scoreboard_docs_ref")

    @property
    def data_ref(self) -> str | None:
        """Return the machine-readable source document path."""

        return self.capability.field("universal_compatibility_scoreboard_data_ref")

    @property
    def support_status_vocabulary(self) -> tuple[str, ...]:
        """Return status tokens used by the scoreboard."""

        return _split_csv(
            self.capability.field("universal_compatibility_support_status_vocabulary")
        )

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return scoreboard row IDs in stable order."""

        return _split_csv(self.capability.field("universal_compatibility_row_order"))

    @property
    def rows(self) -> tuple[UniversalCompatibilityRow, ...]:
        """Return all scoreboard rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def runtime_supported_count(self) -> int:
        """Return runtime-supported row count."""

        return (
            self.capability.envelope.field_int(
                "universal_compatibility_runtime_supported_count", 0
            )
            or 0
        )

    @property
    def smoke_supported_count(self) -> int:
        """Return smoke-supported row count."""

        return (
            self.capability.envelope.field_int(
                "universal_compatibility_smoke_supported_count", 0
            )
            or 0
        )

    @property
    def report_only_count(self) -> int:
        """Return report-only row count."""

        return (
            self.capability.envelope.field_int(
                "universal_compatibility_report_only_count", 0
            )
            or 0
        )

    @property
    def blocked_count(self) -> int:
        """Return blocked row count."""

        return (
            self.capability.envelope.field_int("universal_compatibility_blocked_count", 0)
            or 0
        )

    @property
    def claim_boundary(self) -> str | None:
        """Return the scoreboard-level claim boundary."""

        return self.capability.field("universal_compatibility_claim_boundary")

    @property
    def all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves no fallback and no external engine."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_all_rows_fallback_attempted_false", False
            )
            is True
            and self.capability.envelope.field_bool(
                "universal_compatibility_all_rows_external_engine_invoked_false", False
            )
            is True
            and all(row.no_fallback_no_external_engine for row in self.rows)
        )

    @property
    def object_store_runtime_supported(self) -> bool:
        """Whether object-store runtime is supported by this scoreboard."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_object_store_runtime_supported", False
            )
            is True
        )

    @property
    def table_runtime_supported(self) -> bool:
        """Whether table/lakehouse runtime is supported by this scoreboard."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_table_runtime_supported", False
            )
            is True
        )

    @property
    def foundry_runtime_supported(self) -> bool:
        """Whether Foundry runtime is supported by this scoreboard."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_foundry_runtime_supported", False
            )
            is True
        )

    @property
    def sql_dataframe_runtime_supported(self) -> bool:
        """Whether broad SQL/DataFrame runtime is supported by this scoreboard."""

        return (
            self.capability.envelope.field_bool(
                "universal_compatibility_sql_dataframe_runtime_supported", False
            )
            is True
        )

    @property
    def source_free_generated_output_contract(
        self,
    ) -> SourceFreeGeneratedOutputCompatibilityContract:
        """Return the compatibility-level source-free generated-output contract."""

        return SourceFreeGeneratedOutputCompatibilityContract(self.capability)

    @property
    def object_store_admission_ladder(self) -> ObjectStoreAdmissionLadder:
        """Return the S3/GCS/ADLS object-store admission ladder."""

        return ObjectStoreAdmissionLadder(self.capability)

    @property
    def table_format_boundary_matrix(self) -> TableFormatBoundaryMatrix:
        """Return the Iceberg/Delta/Hudi table-format boundary matrix."""

        return TableFormatBoundaryMatrix(self.capability)

    @property
    def database_warehouse_boundary_matrix(self) -> DatabaseWarehouseBoundaryMatrix:
        """Return the database/warehouse import-export boundary matrix."""

        return DatabaseWarehouseBoundaryMatrix(self.capability)

    def row(self, surface_id: str) -> UniversalCompatibilityRow:
        """Return one scoreboard row by surface ID."""

        normalized = surface_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"compatibility surface {surface_id!r} is not in the scoreboard")
        prefix = f"universal_compatibility_row_{normalized}"
        return UniversalCompatibilityRow(
            surface_id=normalized,
            surface=self.capability.field(f"{prefix}_surface"),
            surface_family=self.capability.field(f"{prefix}_surface_family"),
            direction=self.capability.field(f"{prefix}_direction"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            runtime_supported=self.capability.envelope.field_bool(
                f"{prefix}_runtime_supported"
            ),
            smoke_supported=self.capability.envelope.field_bool(
                f"{prefix}_smoke_supported"
            ),
            report_only=self.capability.envelope.field_bool(f"{prefix}_report_only"),
            credential_required=self.capability.envelope.field_bool(
                f"{prefix}_credential_required"
            ),
            network_required=self.capability.envelope.field_bool(
                f"{prefix}_network_required"
            ),
            source_io_performed=self.capability.envelope.field_bool(
                f"{prefix}_source_io_performed"
            ),
            output_io_performed=self.capability.envelope.field_bool(
                f"{prefix}_output_io_performed"
            ),
            native_io_certificate_status=self.capability.field(
                f"{prefix}_native_io_certificate_status"
            ),
            generated_source_certificate_status=self.capability.field(
                f"{prefix}_generated_source_certificate_status"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_future_evidence=_split_csv(
                self.capability.field(f"{prefix}_required_future_evidence")
            ),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class WrapperConnectorRegistryRow:
    """One wrapper/connector implementation registry row."""

    row_id: str
    family: str | None
    planned_package: str | None
    maturity: str | None
    primary_transport: str | None
    support_status: str | None
    user_visible_surface: str | None
    implementation_evidence: tuple[str, ...]
    deterministic_diagnostic_code: str | None
    required_evidence: tuple[str, ...]
    explicit_execution_available: bool | None
    dependency_added: bool | None
    network_listener_started: bool | None
    data_plane_bridge_supported: bool | None
    external_engine_invoked: bool | None
    fallback_attempted: bool | None
    claim_gate_status: str | None
    claim_boundary: str | None

    @property
    def ready_local(self) -> bool:
        """Whether this wrapper surface is ready for scoped local use."""

        return self.support_status == "ready_local"

    @property
    def blocked(self) -> bool:
        """Whether this wrapper/connector remains blocked."""

        return self.support_status == "blocked"

    @property
    def no_dependency_network_or_fallback(self) -> bool:
        """Whether the row remains dependency-free, listener-free, and fallback-free."""

        return (
            self.dependency_added is False
            and self.network_listener_started is False
            and self.external_engine_invoked is False
            and self.fallback_attempted is False
        )


@dataclass(frozen=True, slots=True)
class WrapperConnectorRegistry:
    """Typed view over the wrapper/connector implementation registry."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the registry schema version."""

        return self.capability.field("wrapper_connector_registry_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the registry report identifier."""

        return self.capability.field("wrapper_connector_registry_report_id")

    @property
    def docs_ref(self) -> str | None:
        """Return the registry reference document path."""

        return self.capability.field("wrapper_connector_registry_docs_ref")

    @property
    def support_status_vocabulary(self) -> tuple[str, ...]:
        """Return the support status vocabulary."""

        return _split_csv(
            self.capability.field("wrapper_connector_registry_support_status_vocabulary")
        )

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return registry rows in stable order."""

        return _split_csv(self.capability.field("wrapper_connector_registry_row_order"))

    @property
    def rows(self) -> tuple[WrapperConnectorRegistryRow, ...]:
        """Return all wrapper/connector registry rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def ready_local_count(self) -> int:
        """Return the number of scoped local ready rows."""

        return (
            self.capability.envelope.field_int(
                "wrapper_connector_registry_ready_local_count", 0
            )
            or 0
        )

    @property
    def report_only_count(self) -> int:
        """Return the number of report-only rows."""

        return (
            self.capability.envelope.field_int(
                "wrapper_connector_registry_report_only_count", 0
            )
            or 0
        )

    @property
    def blocked_count(self) -> int:
        """Return the number of blocked rows."""

        return (
            self.capability.envelope.field_int(
                "wrapper_connector_registry_blocked_count", 0
            )
            or 0
        )

    @property
    def diagnostic_codes(self) -> tuple[str, ...]:
        """Return deterministic diagnostic codes for unavailable wrappers/connectors."""

        return _split_csv(self.capability.field("wrapper_connector_registry_diagnostic_codes"))

    @property
    def claim_gate_status(self) -> str | None:
        """Return the registry claim gate status."""

        return self.capability.field("wrapper_connector_registry_claim_gate_status")

    @property
    def wrapper_ecosystem_claim_allowed(self) -> bool:
        """Whether a broad wrapper ecosystem claim is allowed."""

        return (
            self.capability.envelope.field_bool(
                "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
                False,
            )
            is True
        )

    @property
    def all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether all rows preserve no-fallback/no-external-engine posture."""

        return (
            self.capability.envelope.field_bool(
                "wrapper_connector_registry_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
            and all(row.no_dependency_network_or_fallback for row in self.rows)
        )

    def row(self, row_id: str) -> WrapperConnectorRegistryRow:
        """Return one wrapper/connector registry row."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(f"wrapper/connector registry row {row_id!r} is not present")
        prefix = f"wrapper_connector_registry_row_{normalized}"
        return WrapperConnectorRegistryRow(
            row_id=normalized,
            family=self.capability.field(f"{prefix}_family"),
            planned_package=self.capability.field(f"{prefix}_planned_package"),
            maturity=self.capability.field(f"{prefix}_maturity"),
            primary_transport=self.capability.field(f"{prefix}_primary_transport"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            user_visible_surface=self.capability.field(f"{prefix}_user_visible_surface"),
            implementation_evidence=_split_csv(
                self.capability.field(f"{prefix}_implementation_evidence")
            ),
            deterministic_diagnostic_code=self.capability.field(
                f"{prefix}_deterministic_diagnostic_code"
            ),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            explicit_execution_available=self.capability.envelope.field_bool(
                f"{prefix}_explicit_execution_available"
            ),
            dependency_added=self.capability.envelope.field_bool(f"{prefix}_dependency_added"),
            network_listener_started=self.capability.envelope.field_bool(
                f"{prefix}_network_listener_started"
            ),
            data_plane_bridge_supported=self.capability.envelope.field_bool(
                f"{prefix}_data_plane_bridge_supported"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class DataFrameNotebookPackageReadinessRow:
    """One DataFrame/notebook/package readiness row."""

    row_id: str
    family: str | None
    surface: str | None
    support_status: str | None
    local_install_smoke: bool | None
    package_publication_allowed: bool | None
    dataframe_runtime_supported: bool | None
    notebook_runtime_supported: bool | None
    deterministic_diagnostic_code: str | None
    blocker_id: str | None
    required_evidence: tuple[str, ...]
    claim_gate_status: str | None
    fallback_attempted: bool | None
    external_engine_invoked: bool | None
    claim_boundary: str | None

    @property
    def ready_local(self) -> bool:
        """Whether this readiness row has local non-runtime readiness evidence."""

        return self.support_status == "ready_local"

    @property
    def smoke_supported(self) -> bool:
        """Whether this readiness row is supported only as a scoped smoke proof."""

        return self.support_status == "smoke_supported"

    @property
    def report_only(self) -> bool:
        """Whether this readiness row is report-only posture."""

        return self.support_status == "report_only"

    @property
    def blocked(self) -> bool:
        """Whether this readiness row remains blocked."""

        return self.support_status == "blocked"

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the row preserves no fallback and no external engine invocation."""

        return self.fallback_attempted is False and self.external_engine_invoked is False

    @property
    def no_runtime_claims(self) -> bool:
        """Whether this row avoids publication/DataFrame/notebook runtime claims."""

        return (
            self.package_publication_allowed is False
            and self.dataframe_runtime_supported is False
            and self.notebook_runtime_supported is False
        )


@dataclass(frozen=True, slots=True)
class DataFrameNotebookPackageReadinessReport:
    """Typed view over GAR-0010-B DataFrame/notebook/package readiness posture."""

    capability: "CapabilityView"

    @property
    def schema_version(self) -> str | None:
        """Return the readiness report schema version."""

        return self.capability.field("dataframe_notebook_package_readiness_schema_version")

    @property
    def report_id(self) -> str | None:
        """Return the readiness report identifier."""

        return self.capability.field("dataframe_notebook_package_readiness_report_id")

    @property
    def docs_ref(self) -> str | None:
        """Return the readiness report reference document."""

        return self.capability.field("dataframe_notebook_package_readiness_docs_ref")

    @property
    def source_refs(self) -> tuple[str, ...]:
        """Return governing source references for this readiness report."""

        return _split_csv(
            self.capability.field("dataframe_notebook_package_readiness_source_refs")
        )

    @property
    def support_status_vocabulary(self) -> tuple[str, ...]:
        """Return status values used by this readiness report."""

        return _split_csv(
            self.capability.field(
                "dataframe_notebook_package_readiness_support_status_vocabulary"
            )
        )

    @property
    def row_order(self) -> tuple[str, ...]:
        """Return readiness rows in stable report order."""

        return _split_csv(
            self.capability.field("dataframe_notebook_package_readiness_row_order")
        )

    @property
    def rows(self) -> tuple[DataFrameNotebookPackageReadinessRow, ...]:
        """Return all readiness rows."""

        return tuple(self.row(row_id) for row_id in self.row_order)

    @property
    def ready_local_count(self) -> int:
        """Return rows with local non-runtime readiness evidence."""

        return (
            self.capability.envelope.field_int(
                "dataframe_notebook_package_readiness_ready_local_count", 0
            )
            or 0
        )

    @property
    def smoke_supported_count(self) -> int:
        """Return rows with scoped smoke support."""

        return (
            self.capability.envelope.field_int(
                "dataframe_notebook_package_readiness_smoke_supported_count", 0
            )
            or 0
        )

    @property
    def report_only_count(self) -> int:
        """Return report-only rows."""

        return (
            self.capability.envelope.field_int(
                "dataframe_notebook_package_readiness_report_only_count", 0
            )
            or 0
        )

    @property
    def blocked_count(self) -> int:
        """Return blocked rows."""

        return (
            self.capability.envelope.field_int(
                "dataframe_notebook_package_readiness_blocked_count", 0
            )
            or 0
        )

    @property
    def local_install_smoke_supported(self) -> bool:
        """Whether local install/source-tree smoke is available."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_local_install_smoke_supported",
                False,
            )
            is True
        )

    @property
    def installed_package_smoke_distinct_from_runtime_support(self) -> bool:
        """Whether the report separates package smoke from runtime support."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_installed_package_smoke_distinct_from_runtime_support",
                False,
            )
            is True
        )

    @property
    def dataframe_runtime_supported(self) -> bool:
        """Whether broad DataFrame runtime is supported."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_dataframe_runtime_supported",
                False,
            )
            is True
        )

    @property
    def notebook_runtime_supported(self) -> bool:
        """Whether notebook runtime/rich display is supported."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_notebook_runtime_supported",
                False,
            )
            is True
        )

    @property
    def package_publication_ready(self) -> bool:
        """Whether public package publication gates are ready."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_package_publication_ready",
                False,
            )
            is True
        )

    @property
    def package_publication_claim_allowed(self) -> bool:
        """Whether a public package publication claim is allowed."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_package_publication_claim_allowed",
                False,
            )
            is True
        )

    @property
    def dataframe_runtime_claim_allowed(self) -> bool:
        """Whether a broad DataFrame runtime claim is allowed."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_dataframe_runtime_claim_allowed",
                False,
            )
            is True
        )

    @property
    def notebook_runtime_claim_allowed(self) -> bool:
        """Whether a notebook runtime claim is allowed."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_notebook_runtime_claim_allowed",
                False,
            )
            is True
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the readiness report claim gate status."""

        return self.capability.field("dataframe_notebook_package_readiness_claim_gate_status")

    @property
    def claim_boundary(self) -> str | None:
        """Return the readiness report claim boundary."""

        return self.capability.field("dataframe_notebook_package_readiness_claim_boundary")

    @property
    def all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether every row preserves no fallback and no external engine invocation."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_fallback_attempted", True
            )
            is False
            and self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_external_engine_invoked", True
            )
            is False
            and all(row.no_fallback_no_external_engine for row in self.rows)
        )

    @property
    def all_rows_no_runtime_claims(self) -> bool:
        """Whether every row avoids runtime/publication claim expansion."""

        return (
            self.capability.envelope.field_bool(
                "dataframe_notebook_package_readiness_all_rows_no_runtime_claims",
                False,
            )
            is True
            and all(row.no_runtime_claims for row in self.rows)
        )

    def row(self, row_id: str) -> DataFrameNotebookPackageReadinessRow:
        """Return one readiness row by ID."""

        normalized = row_id.strip().lower().replace("-", "_")
        if normalized not in self.row_order:
            raise KeyError(
                f"DataFrame/notebook/package readiness row {row_id!r} is not present"
            )
        prefix = f"dataframe_notebook_package_readiness_row_{normalized}"
        return DataFrameNotebookPackageReadinessRow(
            row_id=normalized,
            family=self.capability.field(f"{prefix}_family"),
            surface=self.capability.field(f"{prefix}_surface"),
            support_status=self.capability.field(f"{prefix}_support_status"),
            local_install_smoke=self.capability.envelope.field_bool(
                f"{prefix}_local_install_smoke"
            ),
            package_publication_allowed=self.capability.envelope.field_bool(
                f"{prefix}_package_publication_allowed"
            ),
            dataframe_runtime_supported=self.capability.envelope.field_bool(
                f"{prefix}_dataframe_runtime_supported"
            ),
            notebook_runtime_supported=self.capability.envelope.field_bool(
                f"{prefix}_notebook_runtime_supported"
            ),
            deterministic_diagnostic_code=self.capability.field(
                f"{prefix}_deterministic_diagnostic_code"
            ),
            blocker_id=self.capability.field(f"{prefix}_blocker_id"),
            required_evidence=_split_csv(self.capability.field(f"{prefix}_required_evidence")),
            claim_gate_status=self.capability.field(f"{prefix}_claim_gate_status"),
            fallback_attempted=self.capability.envelope.field_bool(
                f"{prefix}_fallback_attempted"
            ),
            external_engine_invoked=self.capability.envelope.field_bool(
                f"{prefix}_external_engine_invoked"
            ),
            claim_boundary=self.capability.field(f"{prefix}_claim_boundary"),
        )


@dataclass(frozen=True, slots=True)
class CapabilityView:
    """Typed convenience view over one capability-discovery envelope."""

    scope: str
    envelope: OutputEnvelope

    @property
    def status(self) -> str:
        """Return the capability envelope status."""

        return self.envelope.status

    @property
    def fields(self) -> Mapping[str, str]:
        """Return capability fields as a mapping."""

        return self.envelope.field_map

    @property
    def _fields_without_source_free_admission_metadata(self) -> Mapping[str, str]:
        """Return fields excluding static source-free admission row effect metadata."""

        excluded_prefixes = (
            "generated_source_api_admission_",
            "generated_source_evidence_alignment_",
            "python_ctx_",
            "python_generated_source_",
            "sql_literal_",
            "sql_values",
            "sql_source_free_",
            "sql_generate_series_",
            "dataframe_source_free_",
            "dataframe_generated_",
        )
        return {
            key: value
            for key, value in self.fields.items()
            if not key.startswith(excluded_prefixes)
        }

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return capability diagnostics."""

        return self.envelope.diagnostics

    @property
    def fallback_attempted(self) -> bool:
        """Whether this capability command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or _any_field_bool(
                self.fields,
                exact=("fallback_attempted",),
                suffixes=("_fallback_attempted",),
            )
        )

    @property
    def fallback_allowed(self) -> bool:
        """Whether this capability view allows fallback execution."""

        return (
            self.envelope.fallback.allowed
            or _any_field_bool(
                self.fields,
                exact=("fallback_allowed", "fallback_execution_allowed"),
                suffixes=("_fallback_allowed", "_fallback_execution_allowed"),
            )
        )

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        """Return stable blocker IDs surfaced by this capability view."""

        values: list[str] = []
        for key, value in self.fields.items():
            if key == "blocker_id" or key.endswith("_blocker_id"):
                values.append(value)
            elif key == "blocker_ids" or key.endswith("_blocker_ids"):
                values.extend(_split_csv(value))
        return tuple(dict.fromkeys(part for part in values if part))

    @property
    def severity(self) -> str | None:
        """Return the top-level unsupported/blocked severity when present."""

        return self.envelope.field("severity")

    @property
    def required_evidence(self) -> tuple[str, ...]:
        """Return required evidence surfaces named by the capability view."""

        values: list[str] = []
        for key, value in self.fields.items():
            if key == "required_evidence" or key.endswith("_required_evidence"):
                values.extend(_split_csv(value))
        return tuple(dict.fromkeys(part for part in values if part))

    @property
    def suggested_next_action(self) -> str | None:
        """Return the top-level suggested next action when present."""

        return self.envelope.field("suggested_next_action")

    @property
    def no_runtime(self) -> bool:
        """Whether this view declares no runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether this view declares no fallback execution."""

        return self.envelope.field_bool("no_fallback", False) is True

    @property
    def no_effects(self) -> bool:
        """Whether this view declares no external effects."""

        return self.envelope.field_bool("no_effects", False) is True or not any(
            (
                self.data_read,
                self.write_io,
                self.object_store_io,
                self.catalog_io,
                self.external_engine_invoked,
            )
        )

    @property
    def support_status(self) -> str | None:
        """Return the explicit support/capability status when present."""

        return _first_field_value(
            self.fields,
            exact=(
                "support_status",
                "capability_status",
                "certification_status",
                "status",
                "maturity",
            ),
            suffixes=(
                "_support_status",
                "_capability_status",
                "_certification_status",
            ),
        )

    @property
    def claim_gate_statuses(self) -> tuple[str, ...]:
        """Return claim-gate statuses exposed by this capability view."""

        values = _field_values(
            self.fields,
            exact=("claim_gate_status", "planner_readiness_claim_gate_status"),
            suffixes=("_claim_gate_status",),
        )
        return tuple(dict.fromkeys(value for value in values if value))

    @property
    def claim_gate_status(self) -> str | None:
        """Return the first explicit claim-gate status when present."""

        return _first_field_value(
            self.fields,
            exact=("claim_gate_status", "planner_readiness_claim_gate_status"),
            suffixes=("_claim_gate_status",),
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether this capability view reports runtime execution."""

        return _any_field_bool(
            self._fields_without_source_free_admission_metadata,
            exact=(
                "runtime_execution",
                "runtime_execution_performed",
                "query_execution",
                "local_execution_performed",
            ),
            suffixes=(
                "_runtime_execution",
                "_runtime_execution_performed",
                "_query_execution",
                "_local_execution_performed",
            ),
        )

    @property
    def data_read(self) -> bool:
        """Whether this capability view reports data-read I/O."""

        return _any_field_bool(
            self._fields_without_source_free_admission_metadata,
            exact=("data_read", "read_io"),
            suffixes=("_data_read", "_read_io"),
        )

    @property
    def write_io(self) -> bool:
        """Whether this capability view reports write I/O."""

        return _any_field_bool(
            self._fields_without_source_free_admission_metadata,
            exact=("write_io", "manifest_write_io"),
            suffixes=("_write_io", "_manifest_write_io"),
        )

    @property
    def object_store_io(self) -> bool:
        """Whether this capability view reports object-store I/O."""

        return _any_field_bool(
            self.fields,
            exact=("object_store_io", "object_store_read_io", "object_store_write_io"),
            suffixes=(
                "_object_store_io",
                "_object_store_read_io",
                "_object_store_write_io",
            ),
        )

    @property
    def catalog_io(self) -> bool:
        """Whether this capability view reports catalog I/O."""

        return _any_field_bool(
            self.fields,
            exact=("catalog_io", "catalog_probe", "catalog_read_io", "catalog_write_io"),
            suffixes=(
                "_catalog_io",
                "_catalog_probe",
                "_catalog_read_io",
                "_catalog_write_io",
            ),
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether this capability view reports external-engine invocation."""

        return _any_field_bool(
            self.fields,
            exact=("external_engine_invoked", "external_engine_execution"),
            suffixes=("_external_engine_invoked", "_external_engine_execution"),
        )

    @property
    def capability_state(self) -> str | None:
        """Return the best available support or certification state field."""

        return self.support_status

    @property
    def required_gates(self) -> tuple[str, ...]:
        """Return required/blocking gate field names that are explicitly true."""

        gates: list[str] = []
        for key, value in self.fields.items():
            normalized = value.strip().lower()
            if normalized != "true":
                continue
            if (
                key.endswith("_required")
                or key.endswith("_required_before_claim")
                or key.endswith("_blocked")
                or "required_gate" in key
            ):
                gates.append(key)
        return tuple(gates)

    @property
    def materialization_boundaries(self) -> tuple[str, ...]:
        """Return materialization-related field names emitted by the capability surface."""

        return tuple(
            key
            for key, value in self.fields.items()
            if "materialization" in key and value not in {"", "false", "none"}
        )

    @property
    def planner_readiness_rows(self) -> tuple[str, ...]:
        """Return SQL/DataFrame planner-readiness row IDs when exposed."""

        return _split_csv(self.envelope.field("planner_readiness_row_order"))

    @property
    def sql_planner_readiness_rows(self) -> tuple[str, ...]:
        """Return SQL planner-readiness row IDs when exposed."""

        return _split_csv(self.envelope.field("planner_readiness_sql_row_order"))

    @property
    def dataframe_planner_readiness_rows(self) -> tuple[str, ...]:
        """Return DataFrame planner-readiness row IDs when exposed."""

        return _split_csv(self.envelope.field("planner_readiness_dataframe_row_order"))

    @property
    def dataframe_method_matrix(self) -> DataFrameMethodCapabilityMatrix:
        """Return the report-only Python DataFrame method capability matrix."""

        return DataFrameMethodCapabilityMatrix.from_capability(self)

    @property
    def etl_workflow_matrix(self) -> ETLWorkflowCapabilityMatrix:
        """Return the report-only ETL workflow capability matrix."""

        return ETLWorkflowCapabilityMatrix.from_capability(self)

    @property
    def generated_source_contract(self) -> GeneratedSourceCertificateContract:
        """Return source-free generated-output contract posture exposed by this capability."""

        return GeneratedSourceCertificateContract(self)

    @property
    def generated_source_api_admission(self) -> GeneratedSourceApiAdmissionMatrix:
        """Return source-free SQL/DataFrame/Python/API admission posture."""

        return GeneratedSourceApiAdmissionMatrix(self)

    @property
    def generated_source_evidence_alignment(self) -> GeneratedSourceEvidenceAlignmentReport:
        """Return GAR-NOVEL-1A generated-source evidence/export alignment posture."""

        return GeneratedSourceEvidenceAlignmentReport(self)

    @property
    def openlineage_facet_mapping(self) -> OpenLineageFacetMappingReport:
        """Return GAR-NOVEL-1B OpenLineage facet mapping posture."""

        return OpenLineageFacetMappingReport(self)

    @property
    def opentelemetry_trace_export_contract(self) -> OpenTelemetryTraceExportContractReport:
        """Return GAR-NOVEL-1C OpenTelemetry trace-export contract posture."""

        return OpenTelemetryTraceExportContractReport(self)

    @property
    def universal_compatibility_scoreboard(self) -> UniversalCompatibilityScoreboard:
        """Return universal source/sink compatibility coverage posture."""

        return UniversalCompatibilityScoreboard(self)

    @property
    def wrapper_connector_registry(self) -> WrapperConnectorRegistry:
        """Return wrapper and connector implementation status posture."""

        return WrapperConnectorRegistry(self)

    @property
    def dataframe_notebook_package_readiness(
        self,
    ) -> DataFrameNotebookPackageReadinessReport:
        """Return DataFrame/notebook/package surface readiness posture."""

        return DataFrameNotebookPackageReadinessReport(self)

    @property
    def planner_readiness_claim_gate_status(self) -> str | None:
        """Return the planner-readiness claim gate status when present."""

        return self.envelope.field("planner_readiness_claim_gate_status")

    @property
    def planner_readiness_non_executing(self) -> bool:
        """Whether planner readiness reports avoided parser, planner, runtime, and fallback work."""

        keys = (
            "planner_readiness_parser_executed",
            "planner_readiness_binder_executed",
            "planner_readiness_planner_executed",
            "planner_readiness_runtime_execution",
            "planner_readiness_dataframe_runtime",
            "planner_readiness_external_engine_invoked",
            "planner_readiness_fallback_attempted",
        )
        if not any(self.envelope.field(key) is not None for key in keys):
            return False
        return all(
            self.envelope.field_bool(key, False) is False
            for key in keys
        )

    @property
    def posture(self) -> CapabilityPosture:
        """Return a normalized no-scraping capability posture."""

        support_status = self.support_status
        support_token = _status_token(support_status)
        claim_gate_status = self.claim_gate_status
        unsupported = (
            self.status in {"error", "unsupported", "blocked"}
            or "unsupported" in support_token
            or "blocked" in support_token
            or (
                self.severity in {"error", "fatal"}
                and (self.blocker_ids or self.no_runtime)
            )
        )
        supported = (
            not unsupported
            and (
                support_token.startswith("supported")
                or support_token.startswith("runtime_supported")
                or support_token.startswith("supported_with_")
                or support_token in {
                    "fixture_certified",
                    "fixture_smoke_only",
                    "scoped_local_smoke_only",
                }
            )
        )
        report_only = (
            not supported
            and (
                "report_only" in support_token
                or support_token in {"planned", "declared", "not_implemented", ""}
                or unsupported
            )
        )
        claim_token = _status_token(claim_gate_status)
        claim_grade = claim_token in {"claim_grade", "claim_grade_allowed"}
        return CapabilityPosture(
            scope=self.scope,
            command_status=self.status,
            support_status=support_status,
            claim_gate_status=claim_gate_status,
            claim_gate_statuses=self.claim_gate_statuses,
            severity=self.severity,
            supported=supported,
            report_only=report_only,
            unsupported=unsupported,
            claim_grade=claim_grade,
            no_runtime=self.no_runtime,
            runtime_execution=self.runtime_execution,
            data_read=self.data_read,
            write_io=self.write_io,
            object_store_io=self.object_store_io,
            catalog_io=self.catalog_io,
            no_effects=self.no_effects,
            fallback_attempted=self.fallback_attempted,
            fallback_allowed=self.fallback_allowed,
            no_fallback=self.no_fallback,
            external_engine_invoked=self.external_engine_invoked,
            blocker_ids=self.blocker_ids,
            required_evidence=self.required_evidence,
            required_gates=self.required_gates,
            materialization_boundaries=self.materialization_boundaries,
            suggested_next_action=self.suggested_next_action,
        )

    def field(self, key: str, default: str | None = None) -> str | None:
        """Return a capability field value."""

        return self.envelope.field(key, default)


@dataclass(frozen=True, slots=True)
class ContextCapabilities:
    """Aggregated side-effect-free capability discovery results."""

    status: OutputEnvelope
    views: Mapping[str, CapabilityView]
    input_adapters: OutputEnvelope | None = None

    @property
    def fallback_attempted(self) -> bool:
        """Whether any capability/discovery command attempted fallback execution."""

        adapter_fallback = (
            self.input_adapters.fallback.attempted
            if self.input_adapters is not None
            else False
        )
        return (
            self.status.fallback.attempted
            or adapter_fallback
            or any(view.fallback_attempted for view in self.views.values())
        )

    @property
    def python(self) -> CapabilityView:
        """Return Python wrapper capability state."""

        return self.scope("python")

    @property
    def deployment(self) -> CapabilityView:
        """Return packaging/deployment capability state."""

        return self.scope("deployment")

    @property
    def adapters(self) -> CapabilityView:
        """Return adapter capability state."""

        return self.scope("adapters")

    @property
    def functions(self) -> CapabilityView:
        """Return function capability state."""

        return self.scope("functions")

    @property
    def operators(self) -> CapabilityView:
        """Return operator capability state."""

        return self.scope("operators")

    @property
    def sql_support(self) -> CapabilityView:
        """Return SQL capability state."""

        return self.scope("sql")

    @property
    def certification(self) -> CapabilityView:
        """Return certification capability state."""

        return self.scope("certification")

    @property
    def engines(self) -> CapabilityView:
        """Return CG-22 engine-mode capability state."""

        return self.scope("engines")

    @property
    def workflow(self) -> CapabilityView:
        """Return CG-21 workflow capability parity state."""

        return self.scope("workflow")

    @property
    def data_etl(self) -> CapabilityView:
        """Return data/ETL surface capability state."""

        return self.scope("data-etl")

    @property
    def dataframe(self) -> CapabilityView:
        """Return DataFrame/query-builder capability state."""

        return self.scope("dataframe")

    @property
    def compatibility(self) -> CapabilityView:
        """Return universal compatibility scoreboard capability state."""

        return self.scope("compatibility")

    @property
    def dataframe_method_matrix(self) -> DataFrameMethodCapabilityMatrix:
        """Return DataFrame/query-builder method support and claim boundaries."""

        return self.dataframe.dataframe_method_matrix

    @property
    def user_route_capability_report(self) -> UserRouteCapabilityReport:
        """Return user/agent route-selection and Vortex-normalization posture."""

        return UserRouteCapabilityReport(rows=USER_ROUTE_CAPABILITY_ROWS)

    @property
    def local_vortex_primitive_route_report(self) -> LocalVortexPrimitiveRouteReport:
        """Return operation-level local Vortex primitive route coverage."""

        return LocalVortexPrimitiveRouteReport(rows=LOCAL_VORTEX_PRIMITIVE_ROUTE_ROWS)

    @property
    def local_file_benchmark_route_report(self) -> LocalFileBenchmarkRouteReport:
        """Return scenario-level local-file benchmark route coverage."""

        return LocalFileBenchmarkRouteReport(rows=LOCAL_FILE_BENCHMARK_ROUTE_ROWS)

    @property
    def dataframe_notebook_package_readiness(
        self,
    ) -> DataFrameNotebookPackageReadinessReport:
        """Return DataFrame/notebook/package readiness and claim boundaries."""

        return self.dataframe.dataframe_notebook_package_readiness

    @property
    def etl_workflow_matrix(self) -> ETLWorkflowCapabilityMatrix:
        """Return ETL workflow support, blockers, and claim boundaries."""

        return self.workflow.etl_workflow_matrix

    @property
    def universal_compatibility_scoreboard(self) -> UniversalCompatibilityScoreboard:
        """Return universal source/sink compatibility coverage posture."""

        return self.compatibility.universal_compatibility_scoreboard

    @property
    def api_surfaces(self) -> CapabilityView:
        """Return API-surface capability state."""

        return self.scope("api-surfaces")

    @property
    def wrapper_connector_registry(self) -> WrapperConnectorRegistry:
        """Return wrapper/connector implementation status posture."""

        return self.api_surfaces.wrapper_connector_registry

    @property
    def observability(self) -> CapabilityView:
        """Return observability/lineage capability state."""

        return self.scope("observability")

    @property
    def remote_api(self) -> CapabilityView:
        """Return CG-23 remote/API capability parity state."""

        return self.scope("remote-api")

    @property
    def cross_cg(self) -> CapabilityView:
        """Return CG-21/CG-22/CG-23 parity state."""

        return self.scope("cross-cg")

    def scope(self, name: str) -> CapabilityView:
        """Return a capability view by scope name."""

        key = _normalize_scope_name(name)
        try:
            return self.views[key]
        except KeyError as exc:
            raise KeyError(f"capability scope {name!r} was not collected") from exc


class ShardLoomContext:
    """High-level Python context for side-effect-free discovery and explicit work.

    Constructing a context does not run the ShardLoom CLI, inspect datasets, probe
    catalogs, touch object stores, or invoke external engines. Methods run only
    explicit ShardLoom CLI JSON commands through the wrapped client.
    """

    def __init__(
        self,
        client: ShardLoomClient | None = None,
        *,
        engine: str = "auto",
    ) -> None:
        self.client = client if client is not None else ShardLoomClient.from_env()
        self.engine = _normalize_engine_mode(engine)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        engine: str = "auto",
        profile_order: Sequence[str] | None = None,
        **kwargs: object,
    ) -> "ShardLoomContext":
        """Create a context from environment configuration without running commands."""

        return cls(
            ShardLoomClient.from_env(
                env=env,
                profile_order=profile_order,
                **kwargs,
            ),
            engine=engine,
        )

    @classmethod
    def from_repo(
        cls,
        repo_root: str | os.PathLike[str] | None = None,
        *,
        engine: str = "auto",
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        **kwargs: object,
    ) -> "ShardLoomContext":
        """Create a source-tree context without running commands."""

        return cls(
            ShardLoomClient.from_repo(
                repo_root=repo_root,
                profile_order=profile_order,
                **kwargs,
            ),
            engine=engine,
        )

    def smoke_check(self, *, check: bool = True) -> PythonClientSmokeReport:
        """Run the no-dataset Python client smoke check."""

        return self.client.smoke_check(check=check)

    def capabilities(
        self,
        scopes: Sequence[str] | None = None,
        *,
        include_input_adapters: bool = True,
        check: bool = True,
    ) -> ContextCapabilities:
        """Collect side-effect-free capability envelopes for common workflow scopes."""

        selected_scopes = (
            tuple(DEFAULT_CAPABILITY_SCOPES) if scopes is None else tuple(scopes)
        )
        views = {
            _normalize_scope_name(scope): self._capability_view(scope, check=check)
            for scope in selected_scopes
        }
        input_adapters = (
            self.client.input_adapters(check=check) if include_input_adapters else None
        )
        return ContextCapabilities(
            status=self.client.status(check=check),
            views=views,
            input_adapters=input_adapters,
        )

    def adapters(self, *, check: bool = True) -> CapabilityView:
        """Return adapter capability discovery without probing adapters."""

        return self._capability_view("adapters", check=check)

    def adapter_registry(self, *, check: bool = True) -> OutputEnvelope:
        """Return the no-probe input adapter registry envelope."""

        return self.client.input_adapters(check=check)

    def extension_registry(self, *, check: bool = True) -> OutputEnvelope:
        """Return the side-effect-free extension registry snapshot."""

        return self.client.extension_registry(check=check)

    def extension_inspect(
        self,
        extension_id: str,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Inspect extension manifest metadata without loading extension code."""

        return self.client.extension_inspect(extension_id, check=check)

    def udf_runtime_plan(
        self,
        runtime: str = "unknown",
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return UDF runtime posture, including the admitted built-in fixture."""

        return self.client.udf_runtime_plan(runtime, check=check)

    def udf_local_scalar_fixture_smoke(
        self,
        values: Sequence[int | None] | str,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the built-in deterministic nullable-int64 scalar UDF fixture."""

        return self.client.udf_local_scalar_fixture_smoke(values, check=check)

    def functions(self, *, check: bool = True) -> CapabilityView:
        """Return function capability discovery."""

        return self._capability_view("functions", check=check)

    def operators(self, *, check: bool = True) -> CapabilityView:
        """Return operator capability discovery."""

        return self._capability_view("operators", check=check)

    def sql_support(self, *, check: bool = True) -> CapabilityView:
        """Return SQL capability discovery."""

        return self._capability_view("sql", check=check)

    def dataframe_method_matrix(
        self,
        *,
        check: bool = True,
    ) -> DataFrameMethodCapabilityMatrix:
        """Return the report-only DataFrame/query-builder method capability matrix."""

        return self._capability_view("dataframe", check=check).dataframe_method_matrix

    def front_door_parity_matrix(
        self,
        *,
        check: bool | None = None,
    ) -> FrontDoorParityMatrix:
        """Return SQL/Python/DataFrame front-door parity and broad-gap posture."""

        _ = check
        return FrontDoorParityMatrix(rows=FRONT_DOOR_PARITY_ROWS)

    def user_route_capability_report(
        self,
        *,
        check: bool | None = None,
    ) -> UserRouteCapabilityReport:
        """Return user/agent route choices with Vortex normalization boundaries."""

        _ = check
        return UserRouteCapabilityReport(rows=USER_ROUTE_CAPABILITY_ROWS)

    def local_vortex_primitive_route_report(
        self,
        *,
        check: bool | None = None,
    ) -> LocalVortexPrimitiveRouteReport:
        """Return operation-level local Vortex primitive route coverage."""

        _ = check
        return LocalVortexPrimitiveRouteReport(rows=LOCAL_VORTEX_PRIMITIVE_ROUTE_ROWS)

    def local_file_benchmark_route_report(
        self,
        *,
        check: bool | None = None,
    ) -> LocalFileBenchmarkRouteReport:
        """Return scenario-level local-file benchmark route coverage."""

        _ = check
        return LocalFileBenchmarkRouteReport(rows=LOCAL_FILE_BENCHMARK_ROUTE_ROWS)

    def dataframe_notebook_package_readiness(
        self,
        *,
        check: bool = True,
    ) -> DataFrameNotebookPackageReadinessReport:
        """Return DataFrame/notebook/package readiness and publication posture."""

        return self._capability_view(
            "dataframe",
            check=check,
        ).dataframe_notebook_package_readiness

    def etl_workflow_matrix(
        self,
        *,
        check: bool = True,
    ) -> ETLWorkflowCapabilityMatrix:
        """Return the report-only ETL workflow capability matrix."""

        return self._capability_view("workflow", check=check).etl_workflow_matrix

    def compatibility_scoreboard(
        self,
        *,
        check: bool = True,
    ) -> UniversalCompatibilityScoreboard:
        """Return the universal source/sink compatibility coverage scoreboard."""

        return self._capability_view(
            "compatibility",
            check=check,
        ).universal_compatibility_scoreboard

    def wrapper_connector_registry(
        self,
        *,
        check: bool = True,
    ) -> WrapperConnectorRegistry:
        """Return the wrapper/connector implementation registry."""

        return self._capability_view(
            "api-surfaces",
            check=check,
        ).wrapper_connector_registry

    def deployment(self, *, check: bool = True) -> CapabilityView:
        """Return deployment/package capability discovery."""

        return self._capability_view("deployment", check=check)

    def observability(self, *, check: bool = True) -> CapabilityView:
        """Return observability/lineage capability discovery."""

        return self._capability_view("observability", check=check)

    def certification(self, *, check: bool = True) -> CapabilityView:
        """Return certification capability discovery."""

        return self._capability_view("certification", check=check)

    def engines(self, *, check: bool = True) -> CapabilityView:
        """Return CG-22 engine-mode capability discovery."""

        return self._capability_view("engines", check=check)

    def workflow_capabilities(self, *, check: bool = True) -> CapabilityView:
        """Return CG-21 workflow capability parity discovery."""

        return self._capability_view("workflow", check=check)

    def remote_api_capabilities(self, *, check: bool = True) -> CapabilityView:
        """Return CG-23 remote/API capability parity discovery."""

        return self._capability_view("remote-api", check=check)

    def cross_cg_capability_parity(self, *, check: bool = True) -> CapabilityView:
        """Return integrated CG-21/CG-22/CG-23 capability parity discovery."""

        return self._capability_view("cross-cg", check=check)

    def engine_selection(
        self,
        *,
        boundedness: str = "snapshot",
        update_mode: str = "snapshot",
        output_mode: str = "snapshot",
        check: bool = False,
    ) -> EngineSelectionPlan:
        """Return engine selection/rejection for this context's requested engine."""

        return self.client.engine_selection_plan(
            self.engine,
            boundedness=boundedness,
            update_mode=update_mode,
            output_mode=output_mode,
            check=check,
        )

    def engine_capability_matrix(self, *, check: bool = True) -> EngineCapabilityMatrix:
        """Return the CG-22 per-engine capability matrix."""

        return self.client.engine_capability_matrix(check=check)

    def workload_certification_dossier(
        self,
        scenario: str = "local-vortex-count",
        *,
        check: bool = True,
    ) -> WorkloadCertificationDossier:
        """Return a cross-CG workload certification dossier."""

        return self.client.workload_certification_dossier(scenario, check=check)

    def claim_gate_closeout(self, *, check: bool = True) -> ClaimGateCloseoutReport:
        """Return the P7 claim-gate and release-readiness closeout report."""

        return self.client.claim_gate_closeout(check=check)

    def compute_capability_matrix(self, *, check: bool = True) -> ComputeCapabilityMatrix:
        """Return the P7.4 report-only compute capability coverage matrix."""

        return self.client.compute_capability_matrix(check=check)

    def semantic_conformance_suite(self, *, check: bool = True) -> SemanticConformanceSuite:
        """Return the P7.4 ShardLoomNative semantic conformance suite."""

        return self.client.semantic_conformance_suite(check=check)

    def sql_parse(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL parsing."""

        return self._sql_unsupported("sql-parse", statement, check=check)

    def sql_bind(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL binding."""

        return self._sql_unsupported("sql-bind", statement, check=check)

    def sql_plan(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL planning."""

        return self._sql_unsupported("sql-plan", statement, check=check)

    def sql_execute(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL execution."""

        return self._sql_unsupported("sql-execute", statement, check=check)

    def sql(
        self,
        statement: str,
        *,
        check: bool | None = None,
    ) -> SqlWorkflow:
        """Create a scoped SQL workflow using this context's client."""

        _ = check
        return sql_workflow(statement, client=self.client)

    def sequence(
        self,
        start: int,
        end: int,
        *,
        step: int = 1,
        column: str = "value",
    ) -> GeneratedRangeSource:
        """Create a scoped source-free sequence for local output smokes."""

        return generated_sequence(
            start,
            end,
            step=step,
            column=column,
            client=self.client,
        )

    def sql_values(
        self,
        values_clause: object,
    ) -> GeneratedSqlSource:
        """Create a scoped SQL VALUES generated source for local output smokes."""

        return generated_sql_values(
            values_clause,
            client=self.client,
        )

    def sql_literal_select(
        self,
        expression: object,
    ) -> GeneratedSqlSource:
        """Create a scoped SQL literal SELECT generated source for local output smokes."""

        return generated_sql_literal_select(
            expression,
            client=self.client,
        )

    def dataframe_source_free_projection(
        self,
        *expressions: object,
        check: bool | None = None,
    ) -> GeneratedRowsSource:
        """Create a scoped source-free literal projection for local output smokes."""

        _ = check
        return generated_dataframe_source_free_projection(
            *expressions,
            client=self.client,
        )

    def dataframe_generated_with_column(
        self,
        name: str,
        expression: object,
        *,
        check: bool | None = None,
    ) -> GeneratedRowsSource:
        """Create a scoped source-free generated DataFrame with one literal column.

        This is the executable helper for the admitted generated-output row. It
        does not execute broad DataFrame expressions; source-backed generated
        rows and range expressions stay on `ctx.from_rows(...).with_column(...)`
        and `ctx.range(...).with_column(...)`.
        """

        _ = check
        return generated_dataframe_generated_with_column(
            name,
            expression,
            client=self.client,
        )

    def generated_output_to_object_store(
        self,
        target_uri: str | os.PathLike[str],
        *,
        rows: Sequence[Mapping[str, object]] | None = None,
        staging_path: str | os.PathLike[str] | None = None,
        output_format: str = "jsonl",
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        check: bool = True,
    ) -> GeneratedObjectStoreOutputReport | UnsupportedWorkflowOperationReport:
        """Write generated rows through the scoped local-emulator object-store route."""

        target_ref = _require_non_empty_text("object-store target URI", target_uri)
        normalized_profile = _require_non_empty_text("object-store profile", profile)
        if _object_store_generated_output_requires_report_only(
            target_ref,
            normalized_profile,
        ):
            return self._source_free_unsupported(
                "object-store-generated-output",
                "object_store_generated_output",
                target_ref,
                check=check,
            )

        normalized_format = _normalize_generated_object_store_output_format(output_format)
        staging_ref = (
            _require_non_empty_text("object-store generated-output staging path", staging_path)
            if staging_path is not None
            else _generated_object_store_staging_path(target_ref, normalized_format)
        )
        generated_rows = (
            _OBJECT_STORE_GENERATED_OUTPUT_DEFAULT_ROWS if rows is None else rows
        )
        generated_report = from_rows(
            generated_rows,
            client=self.client,
        ).write(
            staging_ref,
            output_format=normalized_format,
            allow_overwrite=True,
            check=check,
        )
        object_store_report = self.client.object_store_write_smoke(
            generated_report.output_path,
            target_ref,
            profile=normalized_profile,
            idempotency_key=idempotency_key,
            allow_overwrite=allow_overwrite,
            rollback_after_commit=rollback_after_commit,
            check=check,
        )
        return GeneratedObjectStoreOutputReport(
            target_uri=target_ref,
            staging_path=generated_report.output_path,
            output_format=normalized_format,
            provider_profile=normalized_profile,
            generated_report=generated_report,
            object_store_report=object_store_report,
        )

    def foundry_generated_output(
        self,
        output_ref: str | os.PathLike[str],
        *,
        rows: Sequence[Mapping[str, object]] | None = None,
        evidence_ref: str | os.PathLike[str] | None = None,
        allow_overwrite: bool = False,
        check: bool = False,
    ) -> FoundryGeneratedOutputReport | UnsupportedWorkflowOperationReport:
        """Write generated rows through the local Foundry-style dataset proof route."""

        result_ref = _require_non_empty_text("Foundry output reference", output_ref)
        if _foundry_generated_output_requires_report_only(result_ref):
            return self._source_free_unsupported(
                "foundry-generated-output",
                "foundry_generated_output",
                result_ref,
                check=check,
            )

        result_dataset = Path(result_ref)
        evidence_dataset = (
            Path(_require_non_empty_text("Foundry evidence dataset reference", evidence_ref))
            if evidence_ref is not None
            else _default_foundry_evidence_dataset_path(result_dataset)
        )
        generated_rows = _OBJECT_STORE_GENERATED_OUTPUT_DEFAULT_ROWS if rows is None else rows
        result_part = result_dataset / "part-00000.jsonl"
        generated_report = from_rows(
            generated_rows,
            client=self.client,
        ).write(
            result_part,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )
        result_dataset_report = _write_foundry_style_dataset_metadata(
            result_dataset,
            result_part,
            dataset_role="result_dataset",
            row_count=generated_report.generated_source_row_count,
            content_digest=generated_report.sink_artifact_digest,
            metadata={
                "generated_source_kind": generated_report.generated_source_kind,
                "generated_source_certificate_status": (
                    generated_report.generated_source_certificate_status
                ),
                "output_native_io_certificate_status": (
                    generated_report.output_native_io_certificate_status
                ),
            },
        )
        evidence_dataset_report = _write_foundry_style_dataset(
            evidence_dataset,
            (
                {
                    "proof_step": "generated_output",
                    "command": generated_report.envelope.command,
                    "status": generated_report.envelope.status,
                    "fields": generated_report.envelope.field_map,
                    "evidence_summary": generated_report.evidence_summary.as_dict(),
                    "claim_summary": generated_report.claim_summary.as_dict(),
                },
            ),
            dataset_role="evidence_dataset",
            metadata={
                "result_dataset_ref": str(result_dataset),
                "generated_source_certificate_status": (
                    generated_report.generated_source_certificate_status
                ),
                "output_native_io_certificate_status": (
                    generated_report.output_native_io_certificate_status
                ),
            },
        )
        return FoundryGeneratedOutputReport(
            output_ref=result_ref,
            result_dataset_path=str(result_dataset),
            evidence_dataset_path=str(evidence_dataset),
            generated_report=generated_report,
            result_dataset_report=result_dataset_report,
            evidence_dataset_report=evidence_dataset_report,
        )

    def rest_api_contract_plan(self, *, check: bool = True) -> RestApiContractPlan:
        """Return the CG-23 REST/OpenAPI contract plan."""

        return self.client.rest_api_contract_plan(check=check)

    def serve_discovery_contract(
        self,
        *,
        bind: str = "127.0.0.1:8787",
        check: bool = True,
    ) -> RestApiDiscoveryContract:
        """Return `serve --mode discovery` contract output without starting a server."""

        return self.client.serve_discovery_contract(bind=bind, check=check)

    def rest_api_plan_preview(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiPlanPreview:
        """Return a side-effect-free REST plan preview scenario."""

        return self.client.rest_api_plan_preview(scenario, check=check)

    def rest_api_local_lifecycle(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiLocalLifecycle:
        """Return the certified local REST lifecycle/result-delivery bundle."""

        return self.client.rest_api_local_lifecycle(scenario, check=check)

    def rest_api_event_stream(
        self,
        scenario: str = "certified-live-fixture",
        *,
        check: bool = True,
    ) -> RestApiEventStream:
        """Return the live/hybrid REST event stream contract bundle."""

        return self.client.rest_api_event_stream(scenario, check=check)

    def rest_api_security_governance(
        self,
        scenario: str = "safe-local-default",
        *,
        check: bool = True,
    ) -> RestApiSecurityGovernance:
        """Return the REST security/governance/observability/agent contract bundle."""

        return self.client.rest_api_security_governance(scenario, check=check)

    def rest_api_data_plane(
        self,
        scenario: str = "artifact-reference-default",
        *,
        check: bool = True,
    ) -> RestApiDataPlane:
        """Return the REST data-plane/standards boundary contract bundle."""

        return self.client.rest_api_data_plane(scenario, check=check)

    def live_change_contract_plan(self, *, check: bool = True) -> LiveChangeContractPlan:
        """Return the CG-22 live change contract."""

        return self.client.live_change_contract_plan(check=check)

    def live_fixture_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        check: bool = True,
    ) -> LiveFixtureRunReport:
        """Run the explicit CG-22 in-memory live fixture."""

        return self.client.live_fixture_run(operator, argument, check=check)

    def hybrid_overlay_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        check: bool = True,
    ) -> HybridOverlayRunReport:
        """Run the explicit CG-22 in-memory hybrid overlay fixture."""

        return self.client.hybrid_overlay_run(operator, argument, check=check)

    def read_vortex(self, uri: str | os.PathLike[str]) -> LazyFrame:
        """Declare a lazy native Vortex source using this context's client."""

        return read_vortex(uri, client=self.client, engine_mode=self.engine)

    def native_vortex_route(
        self,
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        *,
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        execution_mode: str = "native_vortex",
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> NativeVortexRoute:
        """Create an explicit native `.vortex` benchmark-range route handle."""

        return NativeVortexRoute.from_inputs(
            client=self.client,
            fact_vortex=fact_vortex,
            dim_vortex=dim_vortex,
            cdc_delta_vortex=cdc_delta_vortex,
            workspace=workspace,
            execution_mode=execution_mode,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    def read(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy local source by inferring the adapter from the path extension."""

        return read_source(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_csv(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy CSV compatibility source using this context's client."""

        return read_csv(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_json(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy JSON/NDJSON compatibility source using this context's client."""

        return read_json(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_parquet(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy Parquet compatibility source using this context's client."""

        return read_parquet(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_arrow_ipc(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy Arrow IPC compatibility source using this context's client."""

        return read_arrow_ipc(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_avro(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy Avro compatibility source using this context's client."""

        return read_avro(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def read_orc(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a lazy ORC compatibility source using this context's client."""

        return read_orc(uri, schema=schema, client=self.client, engine_mode=self.engine)

    def prepare_vortex(
        self,
        source_path: str | os.PathLike[str],
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        dim: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        cdc_delta: str | os.PathLike[str] | None = None,
        result_workspace: str | os.PathLike[str] | None = None,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        check: bool = True,
    ) -> VortexIngestSmokeReport | CompatibilityPreparedVortexRoute:
        """Prepare local compatibility input through an explicit Vortex route.

        Passing ``source_path`` and ``target_vortex_path`` with no route arguments preserves the
        lower-level ``vortex-ingest-smoke`` diagnostic helper. Passing ``workspace`` plus ``dim``
        or a second positional dimension path returns a route handle for
        ``compatibility_import_certified -> prepared_vortex`` and prepare-once batch execution.
        """

        route_requested = any(
            value is not None
            for value in (
                dim,
                workspace,
                input_format,
                cdc_delta,
                result_workspace,
                evidence_level,
                memory_gb,
                max_parallelism,
            )
        )
        if not route_requested:
            if target_vortex_path is None:
                raise ValueError(
                    "prepare_vortex requires either a target_vortex_path for the lower-level "
                    "vortex-ingest-smoke helper or workspace plus dim/second positional input "
                    "for the compatibility prepared route"
                )
            return self.client.vortex_ingest_smoke(
                source_path,
                target_vortex_path,
                allow_overwrite=allow_overwrite,
                certification_level=certification_level,
                check=check,
            )

        dim_input = dim if dim is not None else target_vortex_path
        if dim_input is None:
            raise ValueError(
                "compatibility prepared routes require a dimension input via dim=... or the "
                "second positional argument"
            )
        if workspace is None:
            raise ValueError(
                "compatibility prepared routes require workspace=... so caller-owned "
                "VortexPreparedState artifacts and route evidence have an explicit location"
            )
        if allow_overwrite:
            raise ValueError(
                "allow_overwrite applies only to the lower-level vortex-ingest-smoke helper; "
                "prepared-route result writes use write_vortex(...)/run_batch(..., "
                "write_result_vortex=True)"
            )
        if certification_level != "ingest_certified":
            raise ValueError(
                "certification_level applies only to the lower-level vortex-ingest-smoke helper; "
                "the compatibility prepared route uses the certified traditional-analytics "
                "preparation evidence emitted by ShardLoom"
            )
        return CompatibilityPreparedVortexRoute.from_inputs(
            client=self.client,
            fact_input=source_path,
            dim_input=dim_input,
            workspace=workspace,
            input_format=input_format,
            cdc_delta_input=cdc_delta,
            result_workspace=result_workspace,
            evidence_level=evidence_level,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    def object_store_read_smoke(
        self,
        local_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        byte_range: tuple[int, int] | None = None,
        public_fixture_path: str | os.PathLike[str] | None = None,
        fixture_listing: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run an explicit object-store read smoke for an admitted fixture profile."""

        return self.client.object_store_read_smoke(
            local_object_path,
            profile=profile,
            byte_range=byte_range,
            public_fixture_path=public_fixture_path,
            fixture_listing=fixture_listing,
            check=check,
        )

    def object_store_write_smoke(
        self,
        source_path: str | os.PathLike[str],
        target_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit local-emulator staged object-store write smoke."""

        return self.client.object_store_write_smoke(
            source_path,
            target_object_path,
            profile=profile,
            idempotency_key=idempotency_key,
            allow_overwrite=allow_overwrite,
            rollback_after_commit=rollback_after_commit,
            check=check,
        )

    def local_table_metadata_read_smoke(
        self,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the scoped local-manifest table metadata read smoke."""

        return self.client.local_table_metadata_read_smoke(check=check)

    def local_table_append_commit_rehearsal_smoke(
        self,
        target_manifest_path: str | os.PathLike[str],
        *,
        profile: str = "local-manifest",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local-manifest table append commit rehearsal smoke."""

        return self.client.local_table_append_commit_rehearsal_smoke(
            target_manifest_path,
            profile=profile,
            idempotency_key=idempotency_key,
            allow_overwrite=allow_overwrite,
            rollback_after_commit=rollback_after_commit,
            check=check,
        )

    def sqlite_local_import_export_smoke(
        self,
        database_path: str | os.PathLike[str],
        *,
        table: str,
        export_jsonl: str | os.PathLike[str],
        roundtrip_db: str | os.PathLike[str],
        order_by: str | None = None,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local SQLite file import/export fixture smoke."""

        return self.client.sqlite_local_import_export_smoke(
            database_path,
            table=table,
            export_jsonl=export_jsonl,
            roundtrip_db=roundtrip_db,
            order_by=order_by,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def session(self, *, session_id: str | None = None) -> ShardLoomSession:
        """Create a caller-owned local session for scoped prepared-state reuse."""

        return ShardLoomSession(
            self.client,
            engine=self.engine,
            session_id=session_id,
        )

    def from_rows(self, rows: Sequence[Mapping[str, object]]) -> GeneratedRowsSource:
        """Create a scoped source-free generated row set using this context's client."""

        return from_rows(rows, client=self.client)

    def literal_table(self, rows: Sequence[Mapping[str, object]]) -> GeneratedRowsSource:
        """Create a scoped source-free literal table using this context's client."""

        return generated_literal_table(rows, client=self.client)

    def range(
        self,
        start: int,
        end: int,
        *,
        step: int = 1,
        column: str = "value",
    ) -> GeneratedRangeSource:
        """Create a scoped ShardLoom-native range generator using this context's client."""

        return generated_range(
            start,
            end,
            step=step,
            column=column,
            client=self.client,
        )

    def calendar(
        self,
        start: str | date,
        end: str | date,
        *,
        column: str = "date",
        include_parts: bool = True,
    ) -> GeneratedRowsSource:
        """Create a scoped source-free calendar/date dimension using this context's client."""

        return generated_calendar(
            start,
            end,
            column=column,
            include_parts=include_parts,
            client=self.client,
        )

    def from_pandas(
        self,
        dataframe: object,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for a pandas in-memory input boundary."""

        return from_pandas(
            dataframe,
            client=self.client,
            engine_mode=self.engine,
            check=check,
        )

    def from_arrow_table(
        self,
        table: object,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for an Arrow table input boundary."""

        return from_arrow_table(
            table,
            client=self.client,
            engine_mode=self.engine,
            check=check,
        )

    def from_arrow_ipc(
        self,
        source: object,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for an Arrow IPC input boundary."""

        return from_arrow_ipc(
            source,
            client=self.client,
            engine_mode=self.engine,
            check=check,
        )

    def _capability_view(self, scope: str, *, check: bool) -> CapabilityView:
        normalized = _normalize_scope_name(scope)
        return CapabilityView(
            scope=normalized,
            envelope=self.client.capabilities(normalized, check=check),
        )

    def _sql_unsupported(
        self,
        operation: str,
        statement: str,
        *,
        check: bool,
    ) -> UnsupportedWorkflowOperationReport:
        workflow = LazyFrame(
            source=WorkflowSource("sql", "sql:statement"),
            client=self.client,
            engine_mode=self.engine,
        )
        envelope = self.client.workflow_unsupported_plan(
            operation,
            "sql(statement)",
            statement,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=workflow,
            operation=operation,
            envelope=envelope,
        )

    def _source_free_unsupported(
        self,
        operation: str,
        source_free_case: str,
        target_ref: str,
        *,
        check: bool,
    ) -> UnsupportedWorkflowOperationReport:
        workflow = LazyFrame(
            source=WorkflowSource("generated_source", f"source_free:{source_free_case}"),
            client=self.client,
            engine_mode=self.engine,
        )
        envelope = self.client.workflow_unsupported_plan(
            operation,
            f"source_free({source_free_case})",
            target_ref,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=workflow,
            operation=operation,
            envelope=envelope,
        )


def context(
    *,
    client: ShardLoomClient | None = None,
    engine: str = "auto",
    binary: Binary | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
    repo_root: str | os.PathLike[str] | None = None,
    profile_order: Sequence[str] | None = None,
    timeout: float | None = None,
) -> ShardLoomContext:
    """Return a side-effect-free ShardLoom context.

    Passing `repo_root` selects source-tree binary resolution; otherwise the
    context uses environment/PATH resolution. The function only constructs a
    client and does not run the CLI.
    """

    if client is not None:
        if any(
            value is not None
            for value in (binary, env, cwd, repo_root, profile_order, timeout)
        ):
            raise ValueError("client cannot be combined with client configuration arguments")
        return ShardLoomContext(client, engine=engine)
    if repo_root is not None:
        return ShardLoomContext.from_repo(
            repo_root,
            binary=binary,
            env=env,
            cwd=cwd,
            profile_order=profile_order or DEFAULT_PROFILE_ORDER,
            timeout=timeout,
            engine=engine,
        )
    return ShardLoomContext.from_env(
        env=env,
        binary=binary,
        cwd=cwd,
        profile_order=profile_order,
        timeout=timeout,
        engine=engine,
    )


def session(
    *,
    client: ShardLoomClient | None = None,
    engine: str = "auto",
    binary: Binary | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
    repo_root: str | os.PathLike[str] | None = None,
    profile_order: Sequence[str] | None = None,
    timeout: float | None = None,
    session_id: str | None = None,
) -> ShardLoomSession:
    """Return a caller-owned local ShardLoom session.

    This is a convenience wrapper over `context(...).session(...)`; constructing
    it does not run the CLI or create a daemon/global cache.
    """

    return context(
        client=client,
        engine=engine,
        binary=binary,
        env=env,
        cwd=cwd,
        repo_root=repo_root,
        profile_order=profile_order,
        timeout=timeout,
    ).session(session_id=session_id)


def _normalize_scope_name(scope: str) -> str:
    normalized = scope.strip().lower().replace("_", "-")
    if normalized == "sql-support":
        return "sql"
    return normalized


def _require_non_empty_text(label: str, value: object) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label} must not be empty")
    return text


def _object_store_generated_output_requires_report_only(
    target_ref: str,
    profile: str,
) -> bool:
    if profile != "local-emulator":
        return True
    _scheme, separator, _rest = target_ref.partition("://")
    return bool(separator)


def _normalize_generated_object_store_output_format(output_format: str) -> str:
    normalized = output_format.strip().lower()
    aliases = {
        "json-lines": "jsonl",
        "ndjson": "jsonl",
        "inline-jsonl": "jsonl",
        "arrow": "arrow-ipc",
        "arrow_ipc": "arrow-ipc",
        "ipc": "arrow-ipc",
        "feather": "arrow-ipc",
        "vtx": "vortex",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in {"jsonl", "csv", "parquet", "arrow-ipc", "avro", "orc", "vortex"}:
        return normalized
    raise ValueError(
        "object-store generated output currently supports JSONL, CSV, and "
        "feature-gated Parquet/Arrow IPC/Avro/ORC/Vortex staging formats"
    )


def _generated_object_store_staging_path(target_ref: str, output_format: str) -> str:
    target_path = Path(target_ref)
    parent = target_path.parent
    staging_parent = (
        parent / ".shardloom-generated-output-staging"
        if str(parent) not in {"", "."}
        else Path(".shardloom-generated-output-staging")
    )
    target_name = target_path.name or "generated-output"
    digest = sha256(f"{target_ref}|{output_format}".encode("utf-8")).hexdigest()[:16]
    extension = {
        "jsonl": "jsonl",
        "csv": "csv",
        "parquet": "parquet",
        "arrow-ipc": "arrow",
        "avro": "avro",
        "orc": "orc",
        "vortex": "vortex",
    }[output_format]
    return str(staging_parent / f"{target_name}.{digest}.{extension}")


def _foundry_generated_output_requires_report_only(output_ref: str) -> bool:
    _scheme, separator, _rest = output_ref.partition("://")
    return bool(separator)


def _default_foundry_evidence_dataset_path(result_dataset: Path) -> Path:
    if result_dataset.name == "result-dataset":
        return result_dataset.parent / "evidence-dataset"
    return result_dataset.parent / f"{result_dataset.name}-evidence"


def _write_foundry_style_dataset_metadata(
    dataset_path: Path,
    part_path: Path,
    *,
    dataset_role: str,
    row_count: int,
    content_digest: str | None,
    metadata: Mapping[str, object],
) -> Mapping[str, object]:
    dataset_path.mkdir(parents=True, exist_ok=True)
    report: dict[str, object] = {
        "schema_version": "shardloom.python.foundry_style_dataset_output.v1",
        "dataset_role": dataset_role,
        "dataset_api": "local_foundry_style_output_dataset_api",
        "dataset_path": str(dataset_path),
        "part_path": str(part_path),
        "row_count": row_count,
        "content_digest": content_digest,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "foundry_style_output_api_invoked": True,
        "direct_s3_write_invoked": False,
        "object_store_write_invoked": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "fixture_smoke_only",
        "metadata": dict(metadata),
    }
    (dataset_path / "_dataset_metadata.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def _write_foundry_style_dataset(
    dataset_path: Path,
    rows: Sequence[Mapping[str, object]],
    *,
    dataset_role: str,
    metadata: Mapping[str, object],
) -> Mapping[str, object]:
    dataset_path.mkdir(parents=True, exist_ok=True)
    part_path = dataset_path / "part-00000.jsonl"
    normalized_rows = [dict(row) for row in rows]
    part_text = "".join(
        json.dumps(row, sort_keys=True) + "\n" for row in normalized_rows
    )
    part_path.write_text(part_text, encoding="utf-8")
    digest = "sha256:" + sha256(part_text.encode("utf-8")).hexdigest()
    return _write_foundry_style_dataset_metadata(
        dataset_path,
        part_path,
        dataset_role=dataset_role,
        row_count=len(normalized_rows),
        content_digest=digest,
        metadata=metadata,
    )


def _join_non_empty_text(label: str, values: Sequence[object]) -> str:
    if not values:
        raise ValueError(f"{label} must not be empty")
    return ",".join(_require_non_empty_text(label, value) for value in values)


def _split_csv(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _field_values(
    fields: Mapping[str, str],
    *,
    exact: Sequence[str] = (),
    suffixes: Sequence[str] = (),
) -> tuple[str, ...]:
    values: list[str] = []
    exact_set = set(exact)
    for key, value in fields.items():
        if key in exact_set or any(key.endswith(suffix) for suffix in suffixes):
            values.append(value)
    return tuple(values)


def _first_field_value(
    fields: Mapping[str, str],
    *,
    exact: Sequence[str] = (),
    suffixes: Sequence[str] = (),
) -> str | None:
    exact_set = set(exact)
    for key in exact:
        value = fields.get(key)
        if value:
            return value
    for key, value in fields.items():
        if key in exact_set:
            continue
        if value and any(key.endswith(suffix) for suffix in suffixes):
            return value
    return None


def _any_field_bool(
    fields: Mapping[str, str],
    *,
    exact: Sequence[str] = (),
    suffixes: Sequence[str] = (),
) -> bool:
    return any(
        _parse_bool(value) is True
        for value in _field_values(fields, exact=exact, suffixes=suffixes)
    )


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def _status_token(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().lower().replace("-", "_")


def _normalize_engine_mode(engine: str) -> str:
    normalized = engine.strip().lower().replace("_", "-")
    if normalized not in SUPPORTED_ENGINE_MODES:
        raise ValueError(f"engine must be one of {SUPPORTED_ENGINE_MODES}; got {engine!r}")
    return normalized
