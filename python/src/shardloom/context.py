"""Side-effect-free user context helpers for the ShardLoom Python client."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Sequence

from .client import (
    Binary,
    ClaimGateCloseoutReport,
    ComputeCapabilityMatrix,
    DEFAULT_PROFILE_ORDER,
    EngineCapabilityMatrix,
    EngineSelectionPlan,
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
)
from .models import Diagnostic, OutputEnvelope
from .query import (
    GeneratedRangeSource,
    GeneratedRowsSource,
    LazyFrame,
    UnsupportedWorkflowOperationReport,
    WorkflowSource,
    from_arrow_ipc,
    from_arrow_table,
    from_pandas,
    from_rows,
    range as generated_range,
    read_csv,
    read_json,
    read_parquet,
    read_vortex,
)

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


def _df_method(
    method: str,
    family: str,
    support_status: str,
    *,
    diagnostic_operation: str | None = None,
    blocker_id: str | None = None,
    required_evidence: Sequence[str] = (),
    runtime_execution: bool = False,
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
        data_read=False,
        write_io=write_io,
        materialization_required=materialization_required,
        fallback_attempted=False,
        external_engine_invoked=False,
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
    "Materialization boundary diagnostic only; no row materialization, decode, "
    "external engine, fallback, or production notebook/DataFrame claim."
)
_WRITE_BOUNDARY = (
    "Write/export diagnostic only; no file write, sink commit, external engine, "
    "fallback, or production output claim."
)
_GENERATED_OUTPUT_BOUNDARY = (
    "Scoped local generated-output smokes only; user rows and engine-native range write local JSONL "
    "with generated-source and output evidence, but no broad DataFrame runtime, SQL runtime, "
    "object-store/lakehouse, Foundry, performance, or production claim."
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
        "filter",
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
        "limit",
        "lazy_plan",
        "lazy_plan_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "with_column",
        "expression",
        "unsupported_diagnostic_available",
        diagnostic_operation="with-column",
        blocker_id="cg21.workflow.with_column.expression_unsupported",
        required_evidence=("expression_engine", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "join",
        "join",
        "unsupported_diagnostic_available",
        diagnostic_operation="join",
        blocker_id="cg21.workflow.join.operator_unsupported",
        required_evidence=("join_operator", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "group_by",
        "aggregation",
        "lazy_group_handle_supported",
        claim_boundary=_LAZY_DECLARATION_BOUNDARY,
    ),
    _df_method(
        "agg",
        "aggregation",
        "unsupported_diagnostic_available",
        diagnostic_operation="agg",
        blocker_id="cg21.workflow.agg.operator_unsupported",
        required_evidence=("aggregate_operator", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "aggregate",
        "aggregation",
        "unsupported_diagnostic_available",
        diagnostic_operation="aggregate",
        blocker_id="cg21.workflow.aggregate.operator_unsupported",
        required_evidence=("aggregate_operator", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "sort",
        "ordering",
        "unsupported_diagnostic_available",
        diagnostic_operation="sort",
        blocker_id="cg21.workflow.sort.operator_unsupported",
        required_evidence=("sort_operator", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "window",
        "window",
        "unsupported_diagnostic_available",
        diagnostic_operation="window",
        blocker_id="cg21.workflow.window.operator_unsupported",
        required_evidence=("window_operator", "execution_certificate", "native_io_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "schema_contract",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="schema-contract",
        blocker_id="cg21.workflow.schema_contract.enforcement_unsupported",
        required_evidence=("schema_contract_runtime", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "schema",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="schema",
        blocker_id="cg21.workflow.schema.discovery_unsupported",
        required_evidence=("schema_discovery", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "describe_schema",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="describe-schema",
        blocker_id="cg21.workflow.describe_schema.report_unsupported",
        required_evidence=("schema_discovery", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "validate_schema",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="validate-schema",
        blocker_id="cg21.workflow.validate_schema.validation_unsupported",
        required_evidence=("schema_validation", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "data_quality_check",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="data-quality",
        blocker_id="cg21.workflow.data_quality.checks_unsupported",
        required_evidence=("data_quality_runtime", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "data_quality",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="data-quality",
        blocker_id="cg21.workflow.data_quality.checks_unsupported",
        required_evidence=("data_quality_runtime", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "data_quality_summary",
        "schema_quality",
        "unsupported_diagnostic_available",
        diagnostic_operation="data-quality-summary",
        blocker_id="cg21.workflow.data_quality_summary.report_unsupported",
        required_evidence=("data_quality_runtime", "diagnostic_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "collect",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="collect",
        blocker_id="cg21.workflow.collect.materialization_unsupported",
        required_evidence=("materialization_boundary", "execution_certificate"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_pandas",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-pandas",
        blocker_id="cg21.workflow.to_pandas.decoded_dataframe_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-arrow",
        blocker_id="cg21.workflow.to_arrow.decoded_columnar_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow_table",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-arrow-table",
        blocker_id="cg21.workflow.to_arrow_table.decoded_table_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_arrow_ipc",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-arrow-ipc",
        blocker_id="cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_numpy",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-numpy",
        blocker_id="cg21.workflow.to_numpy.python_array_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "to_python_objects",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="to-python-objects",
        blocker_id="cg21.workflow.to_python_objects.object_materialization_unsupported",
        required_evidence=("materialization_boundary", "decode_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "preview",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="preview",
        blocker_id="cg21.workflow.preview.materialization_unsupported",
        required_evidence=("materialization_boundary", "notebook_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "display",
        "materialization",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="display",
        blocker_id="cg21.workflow.display.rich_display_unsupported",
        required_evidence=("materialization_boundary", "notebook_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "write_vortex",
        "write",
        "unsupported_write_diagnostic",
        diagnostic_operation="write-vortex",
        blocker_id="cg21.workflow.write_vortex.write_policy_unsupported",
        required_evidence=("sink_write_evidence", "native_io_certificate", "commit_evidence"),
        write_io=False,
        claim_boundary=_WRITE_BOUNDARY,
    ),
    _df_method(
        "write_parquet",
        "write",
        "unsupported_write_diagnostic",
        diagnostic_operation="write-parquet",
        blocker_id="cg21.workflow.write_parquet.compatibility_export_unsupported",
        required_evidence=("sink_write_evidence", "fidelity_loss_report", "commit_evidence"),
        write_io=False,
        claim_boundary=_WRITE_BOUNDARY,
    ),
    _df_method(
        "quarantine",
        "write",
        "unsupported_write_diagnostic",
        diagnostic_operation="quarantine",
        blocker_id="cg21.workflow.quarantine.output_unsupported",
        required_evidence=("quarantine_policy", "sink_write_evidence", "commit_evidence"),
        write_io=False,
        claim_boundary=_WRITE_BOUNDARY,
    ),
    _df_method(
        "from_pandas",
        "input_boundary",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="from-pandas",
        blocker_id="cg21.workflow.from_pandas.materialized_input_unsupported",
        required_evidence=("materialization_boundary", "input_fidelity_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "from_arrow_table",
        "input_boundary",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="from-arrow-table",
        blocker_id="cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported",
        required_evidence=("materialization_boundary", "input_fidelity_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "from_arrow_ipc",
        "input_boundary",
        "unsupported_materialization_diagnostic",
        diagnostic_operation="from-arrow-ipc",
        blocker_id="cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported",
        required_evidence=("materialization_boundary", "input_fidelity_evidence"),
        materialization_required=True,
        claim_boundary=_MATERIALIZATION_BOUNDARY,
    ),
    _df_method(
        "sql",
        "sql_frontend",
        "unsupported_diagnostic_available",
        diagnostic_operation="sql",
        blocker_id="cg21.workflow.sql.frontend_unsupported",
        required_evidence=("sql_parser", "sql_binder", "sql_planner", "execution_certificate"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
    ),
    _df_method(
        "profile",
        "observability",
        "unsupported_diagnostic_available",
        diagnostic_operation="profile",
        blocker_id="cg21.workflow.profile.runtime_profile_unsupported",
        required_evidence=("profile_runtime", "observability_evidence"),
        claim_boundary=_UNSUPPORTED_BOUNDARY,
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
    def generated_source_contract(self) -> GeneratedSourceCertificateContract:
        """Return source-free generated-output contract posture exposed by this capability."""

        return GeneratedSourceCertificateContract(self)

    @property
    def generated_source_api_admission(self) -> GeneratedSourceApiAdmissionMatrix:
        """Return source-free SQL/DataFrame/Python/API admission posture."""

        return GeneratedSourceApiAdmissionMatrix(self)

    @property
    def universal_compatibility_scoreboard(self) -> UniversalCompatibilityScoreboard:
        """Return universal source/sink compatibility coverage posture."""

        return UniversalCompatibilityScoreboard(self)

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
    def universal_compatibility_scoreboard(self) -> UniversalCompatibilityScoreboard:
        """Return universal source/sink compatibility coverage posture."""

        return self.compatibility.universal_compatibility_scoreboard

    @property
    def api_surfaces(self) -> CapabilityView:
        """Return API-surface capability state."""

        return self.scope("api-surfaces")

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

    def deployment(self, *, check: bool = True) -> CapabilityView:
        """Return deployment/package capability discovery."""

        return self._capability_view("deployment", check=check)

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
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL workflow execution."""

        return self._sql_unsupported("sql", statement, check=check)

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

    def from_rows(self, rows: Sequence[Mapping[str, object]]) -> GeneratedRowsSource:
        """Create a scoped source-free generated row set using this context's client."""

        return from_rows(rows, client=self.client)

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


def _normalize_scope_name(scope: str) -> str:
    normalized = scope.strip().lower().replace("_", "-")
    if normalized == "sql-support":
        return "sql"
    return normalized


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
