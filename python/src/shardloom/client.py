"""Subprocess client for ShardLoom's CLI JSON protocol."""

from __future__ import annotations

import csv
import json
import math
import os
import platform
import re
import selectors
import shutil
import subprocess
import threading
import time
from importlib import resources as importlib_resources
from pathlib import Path
from typing import Any, Mapping, Sequence, Union

from ._compat import dataclass
from ._version import __version__
from .errors import (
    ShardLoomBinaryNotFoundError,
    ShardLoomCommandError,
    ShardLoomProtocolError,
    ShardLoomTimeoutError,
)
from .models import (
    ClaimSummary,
    Diagnostic,
    EvidenceSummary,
    OutputEnvelope,
    RuntimeActivationSummary,
)
from .execution_resources import (
    ExecutionResourceLimits, ExecutionResources, optional_resources, merge_resource_limits,
    resolve_resources, resource_command_args,
)

CommandPart = Union[str, os.PathLike[str]]
Binary = Union[CommandPart, Sequence[CommandPart]]
FanoutOutputs = Union[Mapping[str, CommandPart], Sequence[tuple[str, CommandPart]]]
DEFAULT_PROFILE_ORDER = ("release", "debug")
ETL_INPUT_FORMATS = frozenset(
    {"csv", "tsv", "jsonl", "ndjson", "parquet", "arrow-ipc", "arrow_ipc", "avro", "orc", "vortex"}
)
DECIMAL_CAST_TYPED_DECIMAL_OUTPUT_BOUNDARY = (
    "jsonl_exact_decimal_string_csv_exact_decimal_text_"
    "parquet_arrow_avro_vortex_typed_decimal_orc_blocked"
)
COMPLEX_PROJECTION_TYPED_NESTED_OUTPUT_BOUNDARY = (
    "typed_nested_compatibility_sink_with_result_jsonl_evidence"
)
ENV_BINARY = "SHARDLOOM_BIN"
ENV_REPO_ROOT = "SHARDLOOM_REPO_ROOT"
ENV_PROFILE_ORDER = "SHARDLOOM_PROFILE_ORDER"
ENV_TIMEOUT_SECONDS = "SHARDLOOM_TIMEOUT_SECONDS"
ENV_PERSISTENT_WORKER = "SHARDLOOM_PERSISTENT_WORKER"
BUNDLED_CLI_RESOURCE_DIR = "bin"
DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS = (
    ("csv", "examples/local/fact.csv"),
    ("jsonl", "examples/local/events.jsonl"),
    ("parquet", "examples/local/fact.parquet"),
    ("arrow_ipc", "examples/local/fact.arrow"),
)
DEFAULT_WORKFLOW_READINESS_TARGET_URI = "file://tmp/shardloom-output-readiness/out.vortex"
DEFAULT_WORKFLOW_READINESS_COMPATIBILITY_TARGET_URI = (
    "file://tmp/shardloom-output-readiness/out.parquet"
)
DEFAULT_WORKFLOW_READINESS_WORKSPACE = "target/shardloom-output-readiness-stage"
DEFAULT_WORKFLOW_READINESS_REMOTE_SOURCES = (
    ("s3_parquet", "s3://bucket/data.parquet"),
    ("gcs_parquet", "gs://bucket/data.parquet"),
    ("azure_parquet", "abfs://container/data.parquet"),
    ("http_parquet", "https://example.invalid/data.parquet"),
)
DEFAULT_VORTEX_WRITE_INTENT_SIGNALS = (
    "native-vortex-target",
    "schema-known",
    "schema-compatible",
    "delete-semantics-known",
    "tombstone-semantics-known",
    "commit-protocol-available",
    "staged-output-required",
)
DEFAULT_VORTEX_OUTPUT_PAYLOAD_SIGNALS = (
    "write-intent-ready",
    "staged-output-ready",
    "finalized-manifest-ready",
    "payload-content-available",
    "local-workspace",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_STAGED_MANIFEST_SIGNALS = (
    "draft-ready",
    "workspace-known",
    "marker-written",
    "local-workspace",
)
DEFAULT_VORTEX_COMMIT_MARKER_SIGNALS = (
    "commit-protocol-ready",
    "manifest-finalization-available",
    "local-workspace",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_COMMIT_INTENT_SIGNALS = (
    "commit-requested",
    "staged-manifest-draft-written",
    "manifest-finalization-available",
    "commit-protocol-available",
    "schema-known",
    "schema-compatible",
    "delete-semantics-known",
    "tombstone-semantics-known",
    "recovery-ready",
    "retry-gate-open",
    "cancellation-gate-open",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_COMMIT_PROTOCOL_SIGNALS = (
    "commit-intent-ready",
    "draft-manifest-ready",
    "manifest-finalization-available",
    "commit-marker-available",
    "recovery-ready",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_LOCAL_COMMIT_RECOVERY_SIGNALS = (
    "rollback-requested",
    "committed-manifest-written",
    "local-workspace",
    "cleanup-allowed",
)
_URI_WITH_AUTHORITY_RE = re.compile(
    r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?P<body>[^\s'\"<>)\],;]+)"
)
_MAX_COMMAND_PART_REDACTION_CHARS = 16_384


class _ShardLoomWorkerStartupError(RuntimeError):
    """Private signal for falling back before a request reaches the worker."""


def _split_field_list(value: str | None) -> tuple[str, ...]:
    """Split comma-separated evidence values while preserving parenthesized dtype args."""
    from ._client_report_values import _split_field_list as _impl
    return _impl(value)


def _env_truthy(value: str) -> bool:
    return value.strip().lower() not in {"", "0", "false", "no", "off", "disabled"}


@dataclass(frozen=True, slots=True)
class VortexIngestSmokeReport:
    """Typed view over a scoped local `vortex_ingest` prepare-once smoke."""

    envelope: OutputEnvelope

    @property
    def source_path(self) -> str:
        """Return the local source path admitted by UniversalIngress."""

        return _required_field(self.envelope, "source_path")

    @property
    def target_vortex_path(self) -> str:
        """Return the local Vortex artifact path written by the smoke."""

        return _required_field(self.envelope, "target_vortex_path")

    @property
    def workspace_path_safety_status(self) -> str | None:
        """Return the enforced workspace path safety status for the Vortex output."""

        return self.envelope.field("vortex_ingest_output_workspace_path_safety_status")

    @property
    def output_commit_status(self) -> str | None:
        """Return the local output commit status for the Vortex artifact."""

        return self.envelope.field("vortex_ingest_output_commit_status")

    @property
    def source_format(self) -> str:
        """Return the local source format."""

        return _required_field(self.envelope, "source_format")

    @property
    def source_adapter_id(self) -> str | None:
        """Return the selected local input adapter identifier."""

        return self.envelope.field("source_adapter_id")

    @property
    def source_adapter_registry_entry_id(self) -> str | None:
        """Return the selected local input adapter registry row."""

        return self.envelope.field("source_adapter_registry_entry_id")

    @property
    def source_adapter_feature_gate(self) -> str | None:
        """Return the feature gate required by the selected source adapter."""

        return self.envelope.field("source_adapter_feature_gate")

    @property
    def source_adapter_boundary(self) -> str | None:
        """Return the read/ingest boundary used by the selected source adapter."""

        return self.envelope.field("source_adapter_boundary")

    @property
    def vortex_ingest_status(self) -> str:
        """Return the `vortex_ingest` route status."""

        return _required_field(self.envelope, "vortex_ingest_status")

    @property
    def requested_memory_gb(self) -> int | None:
        """Return the requested Vortex preparation memory envelope."""

        return self.envelope.field_int("vortex_ingest_requested_memory_gb")

    @property
    def requested_max_parallelism(self) -> int | None:
        """Return the requested Vortex preparation parallelism envelope."""

        return self.envelope.field_int("vortex_ingest_requested_max_parallelism")

    @property
    def prepared_state_id(self) -> str:
        """Return the prepared-state identifier."""

        return _required_field(self.envelope, "prepared_state_id")

    @property
    def prepared_state_digest(self) -> str:
        """Return the prepared-state digest."""

        return _required_field(self.envelope, "prepared_state_digest")

    @property
    def vortex_artifact_digest(self) -> str:
        """Return the local Vortex artifact digest."""

        return _required_field(self.envelope, "vortex_artifact_digest")

    @property
    def input_row_count(self) -> int:
        """Return the input row count written into the prepared artifact."""

        return self.envelope.field_int("input_row_count", 0) or 0

    @property
    def writer_row_count(self) -> int:
        """Return the upstream Vortex writer row count."""

        return self.envelope.field_int("writer_row_count", 0) or 0

    @property
    def reopen_row_count(self) -> int:
        """Return the row count observed after reopening/scanning the artifact."""

        return self.envelope.field_int("reopen_row_count", 0) or 0

    @property
    def reopen_verification_status(self) -> str | None:
        """Return the Vortex reopen verification status."""

        return self.envelope.field("reopen_verification_status")

    @property
    def certification_level(self) -> str | None:
        """Return the requested ingest certification depth."""

        return self.envelope.field("certification_level")

    @property
    def certification_status(self) -> str | None:
        """Return the certification status for the prepare-once route."""

        return self.envelope.field("certification_status")

    @property
    def source_state_materialization_layout(self) -> str | None:
        """Return the SourceState materialization layout."""

        return self.envelope.field("source_state_materialization_layout")

    @property
    def source_state_parse_normalization(self) -> str | None:
        """Return the SourceState parse/normalization path."""

        return self.envelope.field("source_state_parse_normalization")

    @property
    def source_state_columnar_preserved(self) -> bool:
        """Whether the prepare-once route preserved a columnar SourceState."""

        return self.envelope.field_bool("source_state_columnar_preserved", False) is True

    @property
    def source_state_record_batch_count(self) -> int:
        """Return the number of columnar record batches in SourceState."""

        return self.envelope.field_int("source_state_record_batch_count", 0) or 0

    @property
    def source_to_columnar_millis(self) -> int:
        """Return source-to-columnar preparation time in milliseconds."""

        return self.envelope.field_int("source_to_columnar_millis", 0) or 0

    @property
    def vortex_array_build_millis(self) -> int:
        """Return Vortex array-build time in milliseconds."""

        return self.envelope.field_int("vortex_array_build_millis", 0) or 0

    @property
    def vortex_array_build_provider_kind(self) -> str | None:
        """Return the Vortex array-build provider kind."""

        return self.envelope.field("vortex_array_build_provider_kind")

    @property
    def vortex_array_build_provider_surface(self) -> str | None:
        """Return the Vortex array-build provider API surface."""

        return self.envelope.field("vortex_array_build_provider_surface")

    @property
    def vortex_array_build_strategy(self) -> str | None:
        """Return the Vortex array-build strategy."""

        return self.envelope.field("vortex_array_build_strategy")

    @property
    def vortex_array_build_input_layout(self) -> str | None:
        """Return the Vortex array-build input layout."""

        return self.envelope.field("vortex_array_build_input_layout")

    @property
    def vortex_array_build_record_batch_count(self) -> int:
        """Return the number of record batches handed to the Vortex provider."""

        return self.envelope.field_int("vortex_array_build_record_batch_count", 0) or 0

    @property
    def vortex_array_build_manual_scalar_copy_avoided(self) -> bool:
        """Whether the Vortex array build avoided the manual scalar-copy path."""

        return (
            self.envelope.field_bool(
                "vortex_array_build_manual_scalar_copy_avoided", False
            )
            is True
        )

    @property
    def vortex_preparation_spine_status(self) -> str | None:
        """Return the local Vortex preparation-spine status."""

        return self.envelope.field("vortex_preparation_spine_status")

    @property
    def vortex_preparation_spine_vortex_first_decision(self) -> str | None:
        """Return the Vortex-first provider decision for preparation."""

        return self.envelope.field("vortex_preparation_spine_vortex_first_decision")

    @property
    def vortex_preparation_spine_provider_kind(self) -> str | None:
        """Return the admitted preparation-spine provider kind."""

        return self.envelope.field("vortex_preparation_spine_provider_kind")

    @property
    def vortex_preparation_spine_provider_api_surface(self) -> str | None:
        """Return the provider API surface checked for preparation."""

        return self.envelope.field("vortex_preparation_spine_provider_api_surface")

    @property
    def vortex_preparation_spine_source_split_count(self) -> int:
        """Return the number of source splits recorded by the preparation spine."""

        return self.envelope.field_int(
            "vortex_preparation_spine_source_split_count", 0
        ) or 0

    @property
    def vortex_preparation_spine_source_split_refs(self) -> tuple[str, ...]:
        """Return source split refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_split_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_source_byte_range_refs(self) -> tuple[str, ...]:
        """Return source byte-range refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_byte_range_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_source_row_range_refs(self) -> tuple[str, ...]:
        """Return source row-range refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_row_range_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_native_io_certificate_status(self) -> str | None:
        """Return the Native I/O certificate posture for the preparation spine."""

        return self.envelope.field(
            "vortex_preparation_spine_native_io_certificate_status"
        )

    @property
    def vortex_scout_ingress_status(self) -> str | None:
        """Return scoped scout ingress and triage status."""

        return self.envelope.field("vortex_scout_ingress_status")

    @property
    def vortex_scout_ingress_anomaly_count(self) -> int:
        """Return the number of source anomalies reported by scout ingress."""

        return self.envelope.field_int("vortex_scout_ingress_anomaly_count", 0) or 0

    @property
    def vortex_scout_ingress_anomaly_families(self) -> tuple[str, ...]:
        """Return anomaly families reported by scout ingress."""

        value = self.envelope.field("vortex_scout_ingress_anomaly_families")
        return () if value in {None, "", "none"} else tuple(value.split(","))

    @property
    def vortex_scout_ingress_schema_drift_status(self) -> str | None:
        """Return schema-drift status reported by scout ingress."""

        return self.envelope.field("vortex_scout_ingress_schema_drift_status")

    @property
    def vortex_scout_ingress_unsupported_shape_status(self) -> str | None:
        """Return unsupported-shape status reported by scout ingress."""

        return self.envelope.field("vortex_scout_ingress_unsupported_shape_status")

    @property
    def vortex_scout_ingress_quarantine_required(self) -> bool:
        """Whether scout ingress requires quarantine planning."""

        return (
            self.envelope.field_bool(
                "vortex_scout_ingress_quarantine_required",
                False,
            )
            is True
        )

    @property
    def vortex_scout_ingress_quarantine_output_plan_status(self) -> str | None:
        """Return quarantine-output planning status."""

        return self.envelope.field("vortex_scout_ingress_quarantine_output_plan_status")

    @property
    def vortex_scout_ingress_unsupported_diagnostic_code(self) -> str | None:
        """Return the scout diagnostic code for blocked source admission."""

        return self.envelope.field("vortex_scout_ingress_unsupported_diagnostic_code")

    @property
    def vortex_scout_ingress_no_standalone_lane_status(self) -> str | None:
        """Return whether scout ingress stayed in the vortex_ingest route."""

        return self.envelope.field("vortex_scout_ingress_no_standalone_lane_status")

    @property
    def vortex_layout_write_advisor_status(self) -> str | None:
        """Return scoped layout/write advisor status."""

        return self.envelope.field("vortex_layout_write_advisor_status")

    @property
    def vortex_layout_write_advisor_strategy_admitted(self) -> bool:
        """Whether the layout/write advisor admitted the local strategy."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_strategy_admitted",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_runtime_decision_applied(self) -> bool:
        """Whether the admitted layout/write decision governed the writer path."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_runtime_decision_applied",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_selected_strategy(self) -> str | None:
        """Return the writer-validated layout/write strategy selection."""

        return self.envelope.field("vortex_layout_write_advisor_selected_strategy")

    @property
    def vortex_layout_write_advisor_strategy_decision_digest(self) -> str | None:
        """Return the stable digest for the writer-validated strategy decision."""

        return self.envelope.field(
            "vortex_layout_write_advisor_strategy_decision_digest"
        )

    @property
    def vortex_layout_write_advisor_provider_admitted(self) -> bool:
        """Whether the selected writer provider was admitted for the route."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_provider_admitted",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_blocker(self) -> str | None:
        """Return the layout/write runtime blocker, or ``none`` when applied."""

        return self.envelope.field("vortex_layout_write_advisor_blocker")

    @property
    def vortex_layout_write_advisor_layout_strategy(self) -> str | None:
        """Return the admitted or blocked layout strategy."""

        return self.envelope.field("vortex_layout_write_advisor_layout_strategy")

    @property
    def vortex_layout_write_advisor_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether layout/write advisor evidence stayed in vortex_ingest."""

        return self.envelope.field(
            "vortex_layout_write_advisor_no_standalone_lane_status"
        )

    @property
    def vortex_copy_budget_status(self) -> str | None:
        """Return scoped copy-budget and buffer-lifecycle status."""

        return self.envelope.field("vortex_copy_budget_status")

    @property
    def vortex_copy_budget_measurement_status(self) -> str | None:
        """Return copy-budget measurement completeness status."""

        return self.envelope.field("vortex_copy_budget_measurement_status")

    @property
    def vortex_copy_budget_buffer_reuse_status(self) -> str | None:
        """Return buffer-reuse admission or blocker status."""

        return self.envelope.field("vortex_copy_budget_buffer_reuse_status")

    @property
    def vortex_copy_budget_unsafe_lifetime_shortcut_status(self) -> str | None:
        """Return unsafe-lifetime shortcut policy status."""

        return self.envelope.field("vortex_copy_budget_unsafe_lifetime_shortcut_status")

    @property
    def vortex_copy_budget_no_standalone_lane_status(self) -> str | None:
        """Return whether copy-budget evidence stayed in vortex_ingest."""

        return self.envelope.field("vortex_copy_budget_no_standalone_lane_status")

    @property
    def vortex_capillary_preparation_status(self) -> str | None:
        """Return scoped capillary cold-preparation status."""

        return self.envelope.field("vortex_capillary_preparation_status")

    @property
    def vortex_capillary_preparation_activation_result(self) -> str | None:
        """Return whether capillary task planning was activated or skipped."""

        return self.envelope.field("vortex_capillary_preparation_activation_result")

    @property
    def vortex_capillary_preparation_activation_reason(self) -> str | None:
        """Return the deterministic capillary activation or skip reason."""

        return self.envelope.field("vortex_capillary_preparation_activation_reason")

    @property
    def vortex_capillary_preparation_activation_observed_split_count(self) -> int:
        """Return the split count observed by the capillary activation gate."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_activation_observed_split_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_task_count(self) -> int:
        """Return the number of cold-preparation capillary tasks."""

        return self.envelope.field_int("vortex_capillary_preparation_task_count", 0) or 0

    @property
    def vortex_capillary_preparation_task_roles(self) -> tuple[str, ...]:
        """Return the reported capillary task role sequence."""

        value = self.envelope.field("vortex_capillary_preparation_task_roles")
        return () if value in {None, "", "none"} else tuple(value.split(","))

    @property
    def vortex_capillary_preparation_execution_window_count(self) -> int:
        """Return the number of applied capillary execution windows."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_execution_window_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_execution_window_ids(self) -> tuple[str, ...]:
        """Return applied capillary execution-window IDs."""

        value = self.envelope.field("vortex_capillary_preparation_execution_window_ids")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_capillary_preparation_scheduler_applied(self) -> bool:
        """Whether capillary execution windows shaped the cold-preparation route."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_scheduler_applied", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_scheduler_application_reason(self) -> str | None:
        """Return the capillary scheduler admission or block reason."""

        return self.envelope.field(
            "vortex_capillary_preparation_scheduler_application_reason"
        )

    @property
    def vortex_capillary_preparation_prewrite_status(self) -> str | None:
        """Return whether capillary control gated the local pre-write route."""

        return self.envelope.field("vortex_capillary_preparation_prewrite_status")

    @property
    def vortex_capillary_preparation_prewrite_scheduler_applied(self) -> bool:
        """Whether the capillary scheduler was applied before local array build."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_scheduler_applied", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_prewrite_execution_window_count(self) -> int:
        """Return the number of pre-write capillary execution windows."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_prewrite_execution_window_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_prewrite_execution_window_ids(
        self,
    ) -> tuple[str, ...]:
        """Return pre-write capillary execution-window IDs."""

        value = self.envelope.field(
            "vortex_capillary_preparation_prewrite_execution_window_ids"
        )
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_capillary_preparation_prewrite_array_build_gate_status(
        self,
    ) -> str | None:
        """Return the pre-write capillary gate status for local array build."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_array_build_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_write_gate_status(self) -> str | None:
        """Return the pre-write capillary gate status for Vortex write."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_write_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_reopen_gate_status(self) -> str | None:
        """Return the pre-write capillary gate status for reopen verification."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_reopen_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_sink_evidence_gate_status(
        self,
    ) -> str | None:
        """Return the pre-write capillary gate status for sink evidence."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_sink_evidence_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_fallback_attempted(self) -> bool:
        """Whether pre-write capillary control attempted fallback execution."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_fallback_attempted", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_prewrite_external_engine_invoked(self) -> bool:
        """Whether pre-write capillary control invoked an external engine."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_external_engine_invoked",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_native_io_certificate_status(
        self,
    ) -> str | None:
        """Return the Native I/O certificate posture for capillary preparation."""

        return self.envelope.field(
            "vortex_capillary_preparation_native_io_certificate_status"
        )

    @property
    def vortex_capillary_preparation_pulseweave_status(self) -> str | None:
        """Return PulseWeave status for capillary cold-preparation control."""

        return self.envelope.field("vortex_capillary_preparation_pulseweave_status")

    @property
    def vortex_capillary_preparation_pulseweave_runtime_decision_applied(
        self,
    ) -> bool:
        """Whether PulseWeave applied to capillary cold-preparation tasks."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_pulseweave_runtime_decision_applied",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_pulseweave_decision_digest(
        self,
    ) -> str | None:
        """Return the PulseWeave decision digest for capillary preparation."""

        return self.envelope.field(
            "vortex_capillary_preparation_pulseweave_decision_digest"
        )

    @property
    def vortex_capillary_preparation_proofbound_claim_allowed(self) -> bool:
        """Whether ProofBound admitted automatic capillary PulseWeave control."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_proofbound_claim_allowed",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether capillary evidence stayed in the vortex_ingest route."""

        return self.envelope.field(
            "vortex_capillary_preparation_no_standalone_lane_status"
        )

    @property
    def vortex_differential_preparation_status(self) -> str | None:
        """Return scoped differential-preparation overlay status when requested."""

        return self.envelope.field("vortex_differential_preparation_status")

    @property
    def vortex_differential_preparation_update_mode(self) -> str | None:
        """Return the requested differential-preparation update mode."""

        return self.envelope.field("vortex_differential_preparation_update_mode")

    @property
    def vortex_differential_preparation_delta_row_count(self) -> int:
        """Return the number of delta rows admitted into the overlay manifest."""

        return self.envelope.field_int(
            "vortex_differential_preparation_delta_row_count", 0
        ) or 0

    @property
    def vortex_differential_preparation_overlay_applied(self) -> bool:
        """Whether the differential-preparation overlay was applied."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_overlay_applied", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_base_reprepare_performed(self) -> bool:
        """Whether the differential path rewrote the base prepared artifact."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_base_reprepare_performed", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_delta_artifact_written(self) -> bool:
        """Whether the differential path wrote a delta-only Vortex artifact."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_delta_artifact_written", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_native_io_certificate_status(
        self,
    ) -> str | None:
        """Return the Native I/O certificate posture for differential preparation."""

        return self.envelope.field(
            "vortex_differential_preparation_native_io_certificate_status"
        )

    @property
    def vortex_differential_preparation_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether differential evidence stayed in the vortex_ingest route."""

        return self.envelope.field(
            "vortex_differential_preparation_no_standalone_lane_status"
        )

    @property
    def vortex_differential_preparation_refinement_status(self) -> str | None:
        """Return automatic differential refinement admission status."""

        return self.envelope.field("vortex_differential_preparation_refinement_status")

    @property
    def vortex_differential_preparation_refinement_mode(self) -> str | None:
        """Return the differential refinement mode."""

        return self.envelope.field("vortex_differential_preparation_refinement_mode")

    @property
    def vortex_differential_preparation_automatic_detection_status(
        self,
    ) -> str | None:
        """Return automatic append-only detection status."""

        return self.envelope.field(
            "vortex_differential_preparation_automatic_detection_status"
        )

    @property
    def vortex_differential_preparation_blocker_id(self) -> str | None:
        """Return the deterministic differential/refinement blocker, when any."""

        return self.envelope.field("vortex_differential_preparation_blocker_id")

    @property
    def vortex_differential_preparation_refinement_manifest_path(
        self,
    ) -> str | None:
        """Return the automatic refinement manifest path, when emitted."""

        return self.envelope.field(
            "vortex_differential_preparation_refinement_manifest_path"
        )

    @property
    def vortex_differential_preparation_refinement_manifest_digest(
        self,
    ) -> str | None:
        """Return the automatic refinement manifest digest, when emitted."""

        return self.envelope.field(
            "vortex_differential_preparation_refinement_manifest_digest"
        )

    @property
    def vortex_differential_preparation_refinement_manifest_written(self) -> bool:
        """Whether the automatic refinement manifest was written."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_refinement_manifest_written",
                False,
            )
            is True
        )

    @property
    def vortex_differential_preparation_refined_prepared_state_id(
        self,
    ) -> str | None:
        """Return the logical refined prepared-state id."""

        return self.envelope.field(
            "vortex_differential_preparation_refined_prepared_state_id"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_family(
        self,
    ) -> str | None:
        """Return the first admitted overlay consumer family."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_family"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_status(
        self,
    ) -> str | None:
        """Return the overlay consumer admission status."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_status"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_correctness_digest(
        self,
    ) -> str | None:
        """Return the overlay consumer correctness digest."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_correctness_digest"
        )

    @property
    def source_io_performed(self) -> bool:
        """Whether source I/O was performed by the smoke."""

        return self.envelope.field_bool("source_io_performed", False) is True

    @property
    def prepared_state_created(self) -> bool:
        """Whether this smoke created a local `VortexPreparedState`."""

        return self.envelope.field_bool("prepared_state_created", False) is True

    @property
    def prepared_state_reused(self) -> bool:
        """Whether this smoke reused a local `VortexPreparedState`."""

        return self.envelope.field_bool("prepared_state_reused", False) is True

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether artifact-adjacent prepared-state reuse hit for this smoke."""

        return self.envelope.field_bool("prepared_state_reuse_hit", False) is True

    @property
    def prepared_state_reuse_reason(self) -> str | None:
        """Return the prepared-state reuse hit, miss, or invalidation reason."""

        return self.envelope.field("prepared_state_reuse_reason")

    @property
    def prepared_state_reuse_manifest_digest(self) -> str | None:
        """Return the prepared-state reuse manifest digest when emitted."""

        return self.envelope.field("prepared_state_reuse_manifest_digest")

    @property
    def prepared_state_invalidation_reason(self) -> str | None:
        """Return the fail-closed prepared-state invalidation reason."""

        return self.envelope.field("prepared_state_invalidation_reason") or self.envelope.field(
            "invalidation_reason"
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether the smoke command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the smoke command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def claim_gate_status(self) -> str:
        """Return the route claim-gate status."""

        return _required_field(self.envelope, "claim_gate_status")




@dataclass(frozen=True, slots=True)
class LocalVortexPrimitiveSmokeReport:
    """Result of the explicit local Vortex primitive smoke workflow."""

    count: OutputEnvelope
    count_where: OutputEnvelope
    filter: OutputEnvelope
    project: OutputEnvelope
    filter_project: OutputEnvelope

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return command envelopes in execution order."""

        return (
            self.count,
            self.count_where,
            self.filter,
            self.project,
            self.filter_project,
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return the local primitive commands executed by the smoke workflow."""

        return tuple(
            envelope.field("public_workflow_resolved_internal_command") or envelope.command
            for envelope in self.envelopes
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether any local primitive command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or _any_bool_field(envelope, LOCAL_VORTEX_FALLBACK_ATTEMPTED_FIELDS)
            for envelope in self.envelopes
        )

    @property
    def all_certified(self) -> bool:
        """Whether every command emitted the expected certified local evidence fields."""

        return all(
            _all_bool_fields(envelope, LOCAL_VORTEX_CERTIFIED_FIELDS)
            for envelope in self.envelopes
        )

    @property
    def uncertified_commands(self) -> tuple[str, ...]:
        """Return commands that did not emit both Native I/O and correctness certification."""

        return tuple(
            envelope.field("public_workflow_resolved_internal_command") or envelope.command
            for envelope in self.envelopes
            if not _all_bool_fields(envelope, LOCAL_VORTEX_CERTIFIED_FIELDS)
        )


@dataclass(frozen=True, slots=True)
class CompatibilitySourcePlan:
    """One report-only compatibility-source input plan."""

    source_name: str
    dataset_uri: str
    plan: OutputEnvelope


@dataclass(frozen=True, slots=True)
class CompatibilitySourceSmokeReport:
    """Report-only compatibility-source planning smoke envelopes."""

    input_adapters: OutputEnvelope
    native_io_envelope: OutputEnvelope
    sources: tuple[CompatibilitySourcePlan, ...]

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return smoke envelopes in execution order."""

        return (
            self.input_adapters,
            self.native_io_envelope,
            *(source.plan for source in self.sources),
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return commands executed by the compatibility-source smoke workflow."""

        return tuple(envelope.command for envelope in self.envelopes)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any report-only command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or envelope.field_bool("fallback_attempted", False) is True
            for envelope in self.envelopes
        )

    @property
    def all_plan_only(self) -> bool:
        """Whether every source plan stayed side-effect-free and report-only."""
        from ._client_report_views import CompatibilitySourceSmokeReport_all_plan_only as _impl
        return _impl(self)

    @property
    def compatibility_source_names(self) -> tuple[str, ...]:
        """Return source labels that planned as structured compatibility inputs."""

        return tuple(
            source.source_name
            for source in self.sources
            if source.plan.field_bool("compatibility_structured", False) is True
            and source.plan.field_bool("native_vortex", True) is False
        )

    @property
    def planned_source_names(self) -> tuple[str, ...]:
        """Return source labels that are visible but not certified for execution."""

        return tuple(
            source.source_name
            for source in self.sources
            if source.plan.field("capability_status") == "planned"
        )


@dataclass(frozen=True, slots=True)
class WorkflowReadinessPlan:
    """One named envelope in the no-write workflow readiness smoke."""

    name: str
    envelope: OutputEnvelope


@dataclass(frozen=True, slots=True)
class WorkflowReadinessSmokeReport:
    """No-write planning smoke for output, remote, and evidence readiness."""

    output_commit: tuple[WorkflowReadinessPlan, ...]
    table_remote: tuple[WorkflowReadinessPlan, ...]
    evidence: tuple[WorkflowReadinessPlan, ...]

    @property
    def plans(self) -> tuple[WorkflowReadinessPlan, ...]:
        """Return all readiness plans in execution order."""

        return (*self.output_commit, *self.table_remote, *self.evidence)

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all readiness envelopes in execution order."""

        return tuple(plan.envelope for plan in self.plans)

    @property
    def plan_names(self) -> tuple[str, ...]:
        """Return stable readiness plan labels in execution order."""

        return tuple(plan.name for plan in self.plans)

    @property
    def commands(self) -> tuple[str, ...]:
        """Return CLI commands executed by the readiness smoke."""

        return tuple(envelope.command for envelope in self.envelopes)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any readiness command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or envelope.field_bool("fallback_attempted", False) is True
            for envelope in self.envelopes
        )

    @property
    def all_no_write(self) -> bool:
        """Whether every readiness envelope preserved the no-write/no-effect contract."""

        return all(_envelope_preserves_no_write(envelope) for envelope in self.envelopes)

    @property
    def all_report_only_or_planned(self) -> bool:
        """Whether every envelope stayed in a planning/report-only lifecycle."""

        return all(_envelope_is_report_only_or_planned(envelope) for envelope in self.envelopes)

    @property
    def blocked_plan_names(self) -> tuple[str, ...]:
        """Return readiness plans that report blockers or incomplete evidence."""

        return tuple(
            plan.name
            for plan in self.plans
            if _envelope_reports_blocked_or_incomplete(plan.envelope)
        )


@dataclass(frozen=True, slots=True)
class EngineSelectionPlan:
    """Typed convenience view over the CG-22 engine-selection report."""

    envelope: OutputEnvelope

    @property
    def requested_engine_mode(self) -> str | None:
        """Return the requested engine mode."""

        return self.envelope.field("requested_engine_mode")

    @property
    def selected_engine_mode(self) -> str | None:
        """Return the selected engine mode, or `none` when rejected."""

        return self.envelope.field("selected_engine_mode")

    @property
    def selection_status(self) -> str | None:
        """Return the engine-selection status."""

        return self.envelope.field("selection_status")

    @property
    def rejection_reasons(self) -> tuple[str, ...]:
        """Return deterministic engine rejection reasons."""

        value = self.envelope.field("rejection_reasons", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split(";") if part.strip())

    @property
    def fallback_attempted(self) -> bool:
        """Whether engine selection reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether engine selection invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class EngineCapabilityMatrix:
    """Typed convenience view over the CG-22 engine capability matrix."""

    envelope: OutputEnvelope

    @property
    def engine_modes(self) -> tuple[str, ...]:
        """Return engine modes represented in the matrix."""

        value = self.envelope.field("engine_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def live_hybrid_claim_blocked_count(self) -> int:
        """Return the number of live/hybrid production claims still blocked."""

        return self.envelope.field_int("live_hybrid_claim_blocked_count", 0) or 0

    @property
    def streaming_capability_matrix_report_id(self) -> str | None:
        """Return the GAR-0013 streaming capability matrix report identifier."""

        return self.envelope.field("streaming_capability_matrix_report_id")

    @property
    def streaming_capability_rows(self) -> tuple[str, ...]:
        """Return streaming capability matrix row identifiers."""

        return _csv_values(self.envelope.field("streaming_capability_matrix_row_order"))

    @property
    def streaming_capability_blocked_row_count(self) -> int:
        """Return blocked rows in the streaming capability matrix."""

        return self.envelope.field_int("streaming_capability_matrix_blocked_row_count", 0) or 0

    @property
    def streaming_capability_diagnostic_codes(self) -> tuple[str, ...]:
        """Return deterministic diagnostics emitted for blocked streaming matrix rows."""

        return _csv_values(self.envelope.field("streaming_capability_matrix_diagnostic_code_order"))

    @property
    def streaming_capability_no_fallback_no_external_engine(self) -> bool:
        """Whether every streaming matrix row reports no fallback and no external engine."""

        return (
            self.envelope.field_bool(
                "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_schema_version(self) -> str | None:
        """Return the live/hybrid fabric gate schema version."""

        return self.envelope.field("live_hybrid_fabric_gate_schema_version")

    @property
    def live_hybrid_fabric_gate_report_id(self) -> str | None:
        """Return the live/hybrid fabric gate report identifier."""

        return self.envelope.field("live_hybrid_fabric_gate_report_id")

    @property
    def live_hybrid_fabric_gate_rows(self) -> tuple[str, ...]:
        """Return live/hybrid fabric gate row identifiers."""

        return _csv_values(self.envelope.field("live_hybrid_fabric_gate_row_order"))

    @property
    def live_hybrid_fabric_gate_blocked_row_count(self) -> int:
        """Return blocked rows in the live/hybrid fabric gate."""

        return self.envelope.field_int("live_hybrid_fabric_gate_blocked_row_count", 0) or 0

    @property
    def live_hybrid_fabric_gate_report_only_row_count(self) -> int:
        """Return report-only rows in the live/hybrid fabric gate."""

        return (
            self.envelope.field_int(
                "live_hybrid_fabric_gate_report_only_row_count",
                0,
            )
            or 0
        )

    @property
    def live_hybrid_fabric_gate_fixture_smoke_row_count(self) -> int:
        """Return fixture-smoke rows in the live/hybrid fabric gate."""

        return (
            self.envelope.field_int(
                "live_hybrid_fabric_gate_fixture_smoke_row_count",
                0,
            )
            or 0
        )

    @property
    def live_hybrid_fabric_gate_claim_gate_status(self) -> str | None:
        """Return the live/hybrid fabric gate claim-gate status."""

        return self.envelope.field("live_hybrid_fabric_gate_claim_gate_status")

    @property
    def live_hybrid_freshness_claim_allowed(self) -> bool:
        """Whether production live/hybrid freshness claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_freshness_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_exactly_once_claim_allowed(self) -> bool:
        """Whether exactly-once live/hybrid claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_exactly_once_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_production_live_claim_allowed(self) -> bool:
        """Whether production live-engine claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_production_live_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_production_hybrid_claim_allowed(self) -> bool:
        """Whether production hybrid-engine claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_production_hybrid_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_object_store_runtime_supported(self) -> bool:
        """Whether object-store runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_object_store_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_broker_runtime_supported(self) -> bool:
        """Whether broker runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_broker_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_state_store_runtime_supported(self) -> bool:
        """Whether durable state-store runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_state_store_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_baseline_oracle_only(self) -> bool:
        """Whether external systems are baselines/oracles only."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_baseline_oracle_only",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_fallback_attempted(self) -> bool:
        """Whether the live/hybrid fabric gate attempted fallback."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_fallback_attempted",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_external_engine_invoked(self) -> bool:
        """Whether the live/hybrid fabric gate invoked an external engine."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_external_engine_invoked",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_no_fallback_no_external_engine(self) -> bool:
        """Whether the live/hybrid fabric gate preserved no-fallback evidence."""

        return (
            not self.live_hybrid_fabric_gate_fallback_attempted
            and not self.live_hybrid_fabric_gate_external_engine_invoked
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether matrix discovery reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether matrix discovery invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class WorkloadCertificationDossier:
    """Typed view over a workload-scoped certification dossier."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic dossier scenario."""

        return self.envelope.field("scenario")

    @property
    def workload_id(self) -> str:
        """Return the workload identifier represented by this dossier."""

        return _required_field(self.envelope, "workload_id")

    @property
    def overall_status(self) -> str:
        """Return the cross-CG dossier status."""

        return _required_field(self.envelope, "overall_status")

    @property
    def certificate_refs(self) -> tuple[str, ...]:
        """Return available certificate references."""

        return _csv_values(self.envelope.field("certificate_refs"))

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        """Return stable blocker IDs preventing certification claims."""

        return _csv_values(self.envelope.field("blocker_ids"))

    @property
    def missing_evidence(self) -> tuple[str, ...]:
        """Return evidence surfaces not present for the workload."""

        return _csv_values(self.envelope.field("missing_evidence"))

    @property
    def blocked_evidence(self) -> tuple[str, ...]:
        """Return blocked evidence entries."""

        return _csv_values(self.envelope.field("blocked_evidence"))

    @property
    def unsupported_evidence(self) -> tuple[str, ...]:
        """Return unsupported evidence entries."""

        return _csv_values(self.envelope.field("unsupported_evidence"))

    @property
    def suggested_next_action(self) -> str | None:
        """Return the next action suggested by the dossier."""

        return self.envelope.field("suggested_next_action")

    @property
    def no_runtime(self) -> bool:
        """Whether the dossier avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the dossier declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the dossier performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class ClaimGateCloseoutReport:
    """Typed view over the P7 claim-gate and release-readiness closeout report."""

    envelope: OutputEnvelope

    @property
    def claim_gate_status(self) -> str:
        """Return the broad claim gate status."""

        return _required_field(self.envelope, "claim_gate_status")

    @property
    def release_readiness_status(self) -> str:
        """Return the release-readiness status."""

        return _required_field(self.envelope, "release_readiness_status")

    @property
    def p7_closeout_status(self) -> str:
        """Return the Priority 7 closeout status."""

        return _required_field(self.envelope, "p7_closeout_status")

    @property
    def allowed_claims(self) -> tuple[str, ...]:
        """Return report/local claims allowed by the closeout gate."""

        return _csv_values(self.envelope.field("allowed_claims"))

    @property
    def blocked_claims(self) -> tuple[str, ...]:
        """Return claims blocked by missing evidence."""

        return _csv_values(self.envelope.field("blocked_claims"))

    @property
    def out_of_scope_claims(self) -> tuple[str, ...]:
        """Return claims explicitly outside the current release scope."""

        return _csv_values(self.envelope.field("out_of_scope_claims"))

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        """Return stable blocker IDs for blocked broad claims."""

        return _csv_values(self.envelope.field("blocker_ids"))

    @property
    def no_runtime(self) -> bool:
        """Whether the closeout report avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the closeout report declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the closeout report performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class EvidenceAwareOptimizerTraceReport:
    """Typed view over the report-only GAR-PERF-2B optimizer trace."""

    envelope: OutputEnvelope

    @property
    def optimizer_trace_id(self) -> str:
        """Return the stable optimizer trace identifier."""

        return _required_field(self.envelope, "optimizer_trace_id")

    @property
    def optimizer_registry_version(self) -> str:
        """Return the optimizer registry version."""

        return _required_field(self.envelope, "optimizer_registry_version")

    @property
    def optimizer_phase(self) -> str:
        """Return the optimizer phase represented by this report."""

        return _required_field(self.envelope, "optimizer_phase")

    @property
    def rule_order(self) -> tuple[str, ...]:
        """Return optimizer rule IDs in stable order."""

        return _csv_values(self.envelope.field("optimizer_rule_order"))

    @property
    def rule_status_vocabulary(self) -> tuple[str, ...]:
        """Return the stable rule status vocabulary."""

        return _csv_values(self.envelope.field("optimizer_rule_status_vocabulary"))

    @property
    def benchmark_trace_ref(self) -> str:
        """Return the benchmark trace reference for future timing rows."""

        return _required_field(self.envelope, "benchmark_optimizer_trace_ref")

    @property
    def claim_gate_status(self) -> str:
        """Return the optimizer trace claim-gate status."""

        return _required_field(self.envelope, "claim_gate_status")

    @property
    def no_runtime(self) -> bool:
        """Whether this report avoided runtime execution."""

        return self.envelope.field_bool("runtime_execution", True) is False

    @property
    def no_rewrite_applied(self) -> bool:
        """Whether this report applied no optimizer rewrites."""

        return (
            self.envelope.field_bool("optimizer_execution", True) is False
            and self.envelope.field_bool("plan_rewritten", True) is False
            and self.envelope.field("optimizer_rule_applied_count") == "0"
        )

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the trace preserves no fallback and no external engine execution."""
        from ._client_report_views import EvidenceAwareOptimizerTraceReport_no_fallback_no_external_engine as _impl
        return _impl(self)

    def rule_status(self, rule_id: str) -> str:
        """Return one optimizer rule status by ID."""

        normalized = rule_id.strip().lower().replace("-", "_")
        if normalized not in self.rule_order:
            raise KeyError(f"optimizer rule {rule_id!r} is not in the trace")
        return _required_field(self.envelope, f"optimizer_rule_{normalized}_status")

    def rule_applied(self, rule_id: str) -> bool:
        """Return whether one optimizer rule was applied."""

        normalized = rule_id.strip().lower().replace("-", "_")
        if normalized not in self.rule_order:
            raise KeyError(f"optimizer rule {rule_id!r} is not in the trace")
        return self.envelope.field_bool(f"optimizer_rule_{normalized}_applied", True) is True


@dataclass(frozen=True, slots=True)
class SessionCacheSmokeReport:
    """Typed view over the scoped GAR-4L/5I CLI session-cache smoke."""

    envelope: OutputEnvelope

    @property
    def session_id(self) -> str:
        """Return the scoped session identifier."""

        return _required_field(self.envelope, "session_id")

    @property
    def session_runtime_status(self) -> str:
        """Return the session-cache runtime status."""

        return _required_field(self.envelope, "session_runtime_status")

    @property
    def cache_artifact_order(self) -> tuple[str, ...]:
        """Return cache artifact families represented by this smoke."""

        return _csv_values(self.envelope.field("cache_artifact_order"))

    @property
    def invalidation_reason_order(self) -> tuple[str, ...]:
        """Return invalidation reasons exercised by this smoke."""

        return _csv_values(self.envelope.field("invalidation_reason_order"))

    @property
    def cache_hit_count(self) -> int:
        """Return the cache-hit count."""

        return self.envelope.field_int("cache_hit_count", 0) or 0

    @property
    def cache_miss_count(self) -> int:
        """Return the cache-miss count."""

        return self.envelope.field_int("cache_miss_count", 0) or 0

    @property
    def invalidation_count(self) -> int:
        """Return the invalidation count."""

        return self.envelope.field_int("invalidation_count", 0) or 0

    @property
    def buffer_reuse_count(self) -> int:
        """Return the scoped scratch-buffer reuse count."""

        return self.envelope.field_int("buffer_reuse_count", 0) or 0

    @property
    def source_state_id(self) -> str:
        """Return the SourceState id represented by the smoke."""

        return _required_field(self.envelope, "source_state_id")

    @property
    def prepared_state_id(self) -> str:
        """Return the prepared-state id represented by the smoke."""

        return _required_field(self.envelope, "prepared_state_id")

    @property
    def output_plan_id(self) -> str:
        """Return the OutputPlan id represented by the smoke."""

        return _required_field(self.envelope, "output_plan_id")

    @property
    def lifecycle_closed_and_cleaned(self) -> bool:
        """Whether explicit close and cleanup completed."""

        return self.envelope.field_bool("lifecycle_closed_and_cleaned", False) is True

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the smoke preserved no fallback and no external engine execution."""

        return (
            self.envelope.field_bool("fallback_attempted", True) is False
            and self.envelope.field_bool("fallback_execution_allowed", True) is False
            and self.envelope.field_bool("external_engine_invoked", True) is False
            and self.envelope.field_bool("no_fallback_no_external_engine", False) is True
        )

    @property
    def optimizer_trace_id(self) -> str:
        """Return the linked optimizer trace id."""

        return _required_field(self.envelope, "optimizer_trace_id")

    def optimizer_rule_status(self, rule_id: str) -> str:
        """Return one linked optimizer rule status."""

        normalized = rule_id.strip().lower().replace("-", "_")
        return _required_field(self.envelope, f"optimizer_rule_{normalized}_status")


@dataclass(frozen=True, slots=True)
class ComputeCapabilityRow:
    """One row in the report-only compute capability matrix."""

    row_id: str
    surface: str
    family: str
    support_status: str
    engine_mode: str
    execution_mode: str
    provider_kind: str
    semantic_profile: str
    materialization_decode_requirement: str
    memory_spill_requirement: str
    correctness_refs: tuple[str, ...]
    benchmark_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    vortex_native_claim_allowed: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class OperatorFamilyCoverageRow:
    """One operator-family coverage row in the compute matrix."""

    family_id: str
    support_status: str
    next_evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NativeVortexAdmissionLane:
    """One scoped native Vortex provider-admission lane."""

    lane_id: str
    source_surface: str
    operator_surface: str
    sink_surface: str
    admission_status: str
    support_status: str
    execution_mode: str
    provider_kind: str
    provider_api_surface: tuple[str, ...]
    provider_crate: str
    provider_version: str
    feature_gate: str
    shardloom_admission_policy: str
    compute_row_ref: str
    benchmark_ref: str
    correctness_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    materialization_decode_refs: tuple[str, ...]
    policy_refs: tuple[str, ...]
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    residual_executor: str
    vortex_native_claim_allowed: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    object_store_io: bool
    write_io: bool


@dataclass(frozen=True, slots=True)
class PreparedVortexScanPushdownRow:
    """One prepared/native Vortex Scan pushdown capability row."""

    row_id: str
    scenario: str
    pushdown_status: str
    filter_required: bool
    projection_required: bool
    limit_required: bool
    filter_pushed_down: bool
    projection_pushed_down: bool
    limit_pushed_down: bool
    filter_status: str
    projection_status: str
    limit_status: str
    residual_limit_status: str
    residual_limit_executor: str
    filter_columns_read: tuple[str, ...]
    output_columns_read: tuple[str, ...]
    filter_only_columns_read: tuple[str, ...]
    blocker_id: str
    blocker_reason: str
    benchmark_refs: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class NativeUnsupportedCoverageRow:
    """One deterministic unsupported native coverage row in the compute matrix."""

    row_id: str
    category: str
    surface: str
    support_status: str
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    source_refs: tuple[str, ...]
    claim_gate_status: str
    execution_attempted: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class PredicateDtypeCoverageRow:
    """One predicate/DType/null/nested coverage row in the compute matrix."""

    row_id: str
    category: str
    family: str
    surface: str
    support_status: str
    runtime_surface: tuple[str, ...]
    statistics_required: tuple[str, ...]
    fixture_status: str
    correctness_refs: tuple[str, ...]
    benchmark_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    materialization_decode_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    execution_attempted: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class MaterializationPolicyRow:
    """One shared materialization/decode policy row."""

    row_id: str
    operator_execution_class: str
    support_status: str
    data_decoded: bool
    data_materialized: bool
    stayed_encoded: bool
    materialization_boundary_required: bool
    materialization_boundary_emitted: bool
    materialized_temporary_path: bool
    encoded_native_claim_allowed: bool
    materialization_decode_refs: tuple[str, ...]
    policy_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    runtime_execution: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class PublicWorkflowRoute:
    """Typed view over the side-effect-free public workflow route envelope."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the public workflow route schema version."""

        return _required_field(self.envelope, "public_workflow_route_schema_version")

    @property
    def route_id(self) -> str:
        """Return the resolved public route identifier."""

        return _required_field(self.envelope, "route_id")

    @property
    def route_status(self) -> str:
        """Return the route admission status."""

        return _required_field(self.envelope, "route_status")

    @property
    def resolved_internal_command(self) -> str:
        """Return the internal ShardLoom command selected for admitted execution."""

        return _required_field(self.envelope, "resolved_internal_command")

    @property
    def surface(self) -> str:
        """Return the public surface that requested the route."""

        return _required_field(self.envelope, "surface")

    @property
    def start_state(self) -> str:
        """Return the declared route start state."""

        return _required_field(self.envelope, "start_state")

    @property
    def vortex_normalization_point(self) -> str:
        """Return the route's Vortex normalization boundary."""

        return _required_field(self.envelope, "vortex_normalization_point")

    @property
    def execution_mode(self) -> str:
        """Return the internal execution mode selected by the route."""

        return _required_field(self.envelope, "execution_mode")

    @property
    def preparation_included(self) -> bool:
        """Whether compatibility preparation is part of the route."""

        return self.envelope.field_bool("preparation_included", False) is True

    @property
    def query_timing_starts_after_preparation(self) -> bool:
        """Whether query timing starts after preparation for this route."""

        return (
            self.envelope.field_bool("query_timing_starts_after_preparation", False)
            is True
        )

    @property
    def fanout_output_count(self) -> int:
        """Return the number of fanout outputs declared on the route."""

        return self.envelope.field_int("fanout_output_count", 0) or 0

    @property
    def fanout_outputs(self) -> tuple[str, ...]:
        """Return declared fanout targets as ``format=path`` entries."""

        value = self.envelope.field("fanout_outputs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_primitive(self) -> str | None:
        """Return the declared native Vortex primitive payload, if present."""

        value = self.envelope.field("vortex_primitive")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_predicate(self) -> str | None:
        """Return the declared native Vortex predicate payload, if present."""

        value = self.envelope.field("vortex_predicate")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_columns(self) -> tuple[str, ...]:
        """Return declared native Vortex projection columns."""

        value = self.envelope.field("vortex_columns")
        return () if value in {None, "", "none"} else _csv_values(value)

    @property
    def fallback_attempted(self) -> bool:
        """Whether the route inspection attempted fallback execution."""

        return self.envelope.field_bool("fallback_attempted", False) is True

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the route inspection invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def blocker_id(self) -> str | None:
        """Return the deterministic blocker ID when the route is blocked."""

        value = self.envelope.field("blocker_id")
        if value in {None, "", "none"}:
            return None
        return value

    @property
    def activation_summary(self) -> RuntimeActivationSummary:
        """Return compact runtime activation evidence for this route inspection."""

        return self.envelope.activation_summary

    @property
    def side_effect_free(self) -> bool:
        """Whether the route command reports side-effect-free planning."""

        return self.envelope.field_bool("route_side_effect_free", False) is True

    def as_dict(self) -> dict[str, Any]:
        """Return a compact dictionary for notebooks and simple integrations."""
        from ._client_report_views import PublicWorkflowRoute_as_dict as _impl
        return _impl(self)


@dataclass(frozen=True, slots=True)
class PublicWorkflowExecution:
    """Typed view over a public workflow run/prepare envelope with attached route metadata."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the public workflow facade schema version."""

        return _required_field(self.envelope, "public_workflow_facade_schema_version")

    @property
    def facade_command(self) -> str:
        """Return the high-level facade command that emitted the envelope."""

        return _required_field(self.envelope, "public_workflow_facade_command")

    @property
    def route_attached(self) -> bool:
        """Whether a public route envelope was attached to this execution."""

        return self.envelope.field_bool("public_workflow_route_attached", False) is True

    @property
    def route_id(self) -> str:
        """Return the public route identifier attached to execution."""

        return _required_field(self.envelope, "public_workflow_route_id")

    @property
    def route_status(self) -> str:
        """Return the attached route admission status."""

        return _required_field(self.envelope, "public_workflow_route_status")

    @property
    def resolved_internal_command(self) -> str:
        """Return the internal command selected by the route."""

        return _required_field(self.envelope, "public_workflow_resolved_internal_command")

    @property
    def vortex_normalization_point(self) -> str:
        """Return the attached Vortex normalization boundary."""

        return _required_field(self.envelope, "public_workflow_vortex_normalization_point")

    @property
    def execution_mode(self) -> str:
        """Return the attached execution mode."""

        return _required_field(self.envelope, "public_workflow_execution_mode")

    @property
    def runtime_execution(self) -> bool:
        """Whether the emitted envelope reports runtime execution."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def preparation_included(self) -> bool:
        """Whether route preparation is part of this facade request."""

        return (
            self.envelope.field_bool("public_workflow_preparation_included", False)
            is True
        )

    @property
    def fanout_output_count(self) -> int:
        """Return the number of fanout outputs declared on the public workflow."""

        return self.envelope.field_int("public_workflow_fanout_output_count", 0) or 0

    @property
    def fanout_outputs(self) -> tuple[str, ...]:
        """Return declared public workflow fanout targets as ``format=path`` entries."""

        value = self.envelope.field("public_workflow_fanout_outputs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_primitive(self) -> str | None:
        """Return the declared public native Vortex primitive payload."""

        value = self.envelope.field("public_workflow_vortex_primitive")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_predicate(self) -> str | None:
        """Return the declared public native Vortex predicate payload."""

        value = self.envelope.field("public_workflow_vortex_predicate")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_columns(self) -> tuple[str, ...]:
        """Return declared public native Vortex projection columns."""

        value = self.envelope.field("public_workflow_vortex_columns")
        return () if value in {None, "", "none"} else _csv_values(value)

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return self.envelope.field_bool("fallback_attempted", False) is True

    @property
    def external_engine_invoked(self) -> bool:
        """Whether an external engine was invoked."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def public_workflow_fallback_attempted(self) -> bool:
        """Whether the attached public route reports fallback attempted."""

        return (
            self.envelope.field_bool("public_workflow_fallback_attempted", False)
            is True
        )

    @property
    def public_workflow_external_engine_invoked(self) -> bool:
        """Whether the attached public route reports external engine invocation."""

        return (
            self.envelope.field_bool("public_workflow_external_engine_invoked", False)
            is True
        )

    @property
    def blocker_id(self) -> str | None:
        """Return the attached route blocker ID when blocked."""

        value = self.envelope.field("public_workflow_blocker_id")
        if value in {None, "", "none"}:
            return None
        return value

    @property
    def activation_summary(self) -> RuntimeActivationSummary:
        """Return compact runtime activation evidence for this public workflow."""

        return self.envelope.activation_summary

    def as_dict(self) -> dict[str, Any]:
        """Return a compact dictionary for notebooks and simple integrations."""
        from ._client_report_views import PublicWorkflowExecution_as_dict as _impl
        return _impl(self)


@dataclass(frozen=True, slots=True)
class CommandMetadataReport:
    """Typed view over the side-effect-free CLI command registry metadata."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the command registry metadata schema version."""

        return _required_field(self.envelope, "command_registry_schema_version")

    @property
    def registered_command_count(self) -> int:
        """Return the number of commands in the registry."""

        return self.envelope.field_int("registered_command_count", 0) or 0

    @property
    def support_state_vocabulary(self) -> tuple[str, ...]:
        """Return the registry support-state vocabulary."""

        return _csv_values(self.envelope.field("command_registry_support_state_vocabulary"))

    @property
    def user_surface_graduation_posture_vocabulary(self) -> tuple[str, ...]:
        """Return the user-surface graduation posture vocabulary."""

        return _csv_values(
            self.envelope.field(
                "command_registry_user_surface_graduation_posture_vocabulary"
            )
        )

    @property
    def registered_commands(self) -> tuple[str, ...]:
        """Return registered CLI command names in usage order."""

        return _csv_values(self.envelope.field("registered_commands"))

    @property
    def registered_command_families(self) -> Mapping[str, str]:
        """Return command name to command-family mappings."""

        return _csv_key_value_map(self.envelope.field("registered_command_families"))

    @property
    def registered_command_support_states(self) -> Mapping[str, str]:
        """Return command name to support-state mappings."""

        return _csv_key_value_map(self.envelope.field("registered_command_support_states"))

    @property
    def registered_command_user_surface_graduation_postures(self) -> Mapping[str, str]:
        """Return command name to user-surface graduation posture mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_user_surface_graduation_postures")
        )

    @property
    def registered_command_side_effect_levels(self) -> Mapping[str, str]:
        """Return command name to side-effect-level mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_side_effect_levels")
        )

    @property
    def registered_command_feature_gate_statuses(self) -> Mapping[str, str]:
        """Return command name to feature-gate status mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_feature_gate_statuses")
        )

    @property
    def registered_command_input_contracts(self) -> Mapping[str, str]:
        """Return command name to registry input-contract mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_input_contracts")
        )

    @property
    def registered_command_output_contracts(self) -> Mapping[str, str]:
        """Return command name to registry output-contract mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_output_contracts")
        )

    @property
    def registered_command_owning_phase_items(self) -> Mapping[str, str]:
        """Return command name to owning phase or gate mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_owning_phase_items")
        )

    @property
    def selected_command(self) -> str | None:
        """Return the selected command, when the CLI request targeted one command."""

        return self.envelope.field("selected_command")

    @property
    def selected_command_family(self) -> str | None:
        """Return the selected command family, when present."""

        return self.envelope.field("selected_command_family")

    @property
    def selected_command_support_state(self) -> str | None:
        """Return the selected command support state, when present."""

        return self.envelope.field("selected_command_support_state")

    @property
    def selected_command_user_surface_graduation_posture(self) -> str | None:
        """Return the selected command user-surface graduation posture, when present."""

        return self.envelope.field("selected_command_user_surface_graduation_posture")

    @property
    def selected_command_side_effect_level(self) -> str | None:
        """Return the selected command side-effect level, when present."""

        return self.envelope.field("selected_command_side_effect_level")

    @property
    def selected_command_usage_fragment(self) -> str | None:
        """Return the selected command usage fragment, when present."""

        return self.envelope.field("selected_command_usage_fragment")

    @property
    def selected_command_feature_gate_status(self) -> str | None:
        """Return the selected command feature-gate status, when present."""

        return self.envelope.field("selected_command_feature_gate_status")

    @property
    def selected_command_input_contract(self) -> str | None:
        """Return the selected command input contract, when present."""

        return self.envelope.field("selected_command_input_contract")

    @property
    def selected_command_output_contract(self) -> str | None:
        """Return the selected command output contract, when present."""

        return self.envelope.field("selected_command_output_contract")

    @property
    def selected_command_evidence_fields(self) -> tuple[str, ...]:
        """Return the selected command evidence fields, when present."""

        return _csv_values(
            (self.envelope.field("selected_command_evidence_fields") or "").replace(
                "|", ","
            )
        )

    @property
    def selected_command_owning_phase_item(self) -> str | None:
        """Return the selected command owning phase or gate, when present."""

        return self.envelope.field("selected_command_owning_phase_item")

    @property
    def fallback_attempted(self) -> bool:
        """Whether command metadata attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether command metadata invoked an external execution engine."""

        return _envelope_external_engine_invoked(self.envelope)

    def family_for(self, command: str) -> str:
        """Return the registered family for a command."""

        return self.registered_command_families[command]

    def support_state_for(self, command: str) -> str:
        """Return the registered support state for a command."""

        return self.registered_command_support_states[command]

    def user_surface_graduation_posture_for(self, command: str) -> str:
        """Return the registered user-surface graduation posture for a command."""

        return self.registered_command_user_surface_graduation_postures[command]

    def side_effect_level_for(self, command: str) -> str:
        """Return the registered side-effect level for a command."""

        return self.registered_command_side_effect_levels[command]

    def feature_gate_status_for(self, command: str) -> str:
        """Return the registered feature-gate status for a command."""

        return self.registered_command_feature_gate_statuses[command]

    def input_contract_for(self, command: str) -> str:
        """Return the registered input contract for a command."""

        return self.registered_command_input_contracts[command]

    def output_contract_for(self, command: str) -> str:
        """Return the registered output contract for a command."""

        return self.registered_command_output_contracts[command]

    def owning_phase_item_for(self, command: str) -> str:
        """Return the registered owning phase or gate for a command."""

        return self.registered_command_owning_phase_items[command]


@dataclass(frozen=True, slots=True)
class EvidenceSchemaRegistryReport:
    """Typed view over the side-effect-free evidence field schema registry."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the evidence field registry schema version."""

        return _required_field(
            self.envelope, "evidence_schema_registry_schema_version"
        )

    @property
    def surface_count(self) -> int:
        """Return the number of registered evidence surfaces."""

        return (
            self.envelope.field_int("evidence_schema_registry_surface_count", 0)
            or 0
        )

    @property
    def field_count(self) -> int:
        """Return the total registered evidence field count."""

        return (
            self.envelope.field_int("evidence_schema_registry_field_count", 0) or 0
        )

    @property
    def surface_order(self) -> tuple[str, ...]:
        """Return registered evidence surfaces in deterministic order."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_surface_order")
        )

    @property
    def dtype_vocabulary(self) -> tuple[str, ...]:
        """Return the registry dtype vocabulary."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_dtype_vocabulary")
        )

    @property
    def cardinality_vocabulary(self) -> tuple[str, ...]:
        """Return the registry cardinality vocabulary."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_cardinality_vocabulary")
        )

    @property
    def selected_surface(self) -> str | None:
        """Return the selected evidence surface, when one was requested."""

        return self.envelope.field("selected_surface")

    @property
    def selected_surface_field_order(self) -> tuple[str, ...]:
        """Return the selected surface field order, when present."""

        return _csv_values(self.envelope.field("selected_surface_field_order"))

    @property
    def fallback_attempted(self) -> bool:
        """Whether evidence schema rendering attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool(
                "evidence_schema_registry_fallback_attempted", False
            )
            is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether evidence schema rendering invoked an external execution engine."""

        return (
            _envelope_external_engine_invoked(self.envelope)
            or self.envelope.field_bool(
                "evidence_schema_registry_external_engine_invoked", False
            )
            is True
        )

    def field_order_for(self, surface_id: str) -> tuple[str, ...]:
        """Return the declared field order for a surface."""

        return _csv_values(
            self.envelope.field(
                f"evidence_schema_surface_{surface_id}_field_order"
            )
        )

    def python_accessor_mapping_for(self, surface_id: str) -> str:
        """Return the declared Python accessor mapping for a surface."""

        return _required_field(
            self.envelope,
            f"evidence_schema_surface_{surface_id}_python_accessor_mapping",
        )

    def required_no_fallback_fields_for(self, surface_id: str) -> tuple[str, ...]:
        """Return no-fallback fields required by the surface contract."""

        return _csv_values(
            self.envelope.field(
                f"evidence_schema_surface_{surface_id}_required_no_fallback_fields"
            )
        )

    def dtype_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared dtype for one evidence field."""

        return _required_field(
            self.envelope, f"{self._field_prefix(surface_id, field_key)}_dtype"
        )

    def cardinality_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared cardinality for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_cardinality",
        )

    def no_fallback_semantics_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared no-fallback semantics for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_no_fallback_semantics",
        )

    def support_state_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared support state for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_support_state",
        )

    def python_accessor_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared Python accessor mapping for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_python_accessor_mapping",
        )

    @staticmethod
    def _field_prefix(surface_id: str, field_key: str) -> str:
        field_id = field_key.replace("-", "_")
        return f"evidence_schema_field_{surface_id}_{field_id}"


@dataclass(frozen=True, slots=True)
class ProductionUnsupportedDiagnosticRow:
    """One production-family unsupported diagnostic row from `runs-today`."""

    row_id: str
    production_family: str
    user_surface: tuple[str, ...]
    entrypoint_kind: str
    support_status: str
    diagnostic_code: str
    blocker_id: str
    message: str
    next_action: str
    required_evidence: tuple[str, ...]
    claim_gate_status: str
    route_scope: str
    fallback_attempted: bool
    external_engine_invoked: bool
    side_effects_performed: bool


@dataclass(frozen=True, slots=True)
class RunsTodaySupportRow:
    """One current-support row from the `runs-today` matrix."""

    row_id: str
    family: str
    surface: tuple[str, ...]
    support_state: str
    feature_gate: str
    evidence_refs: tuple[str, ...]
    blocker_id: str
    claim_gate_status: str
    claim_boundary: str
    runtime_execution: bool
    data_read: bool
    write_io: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class RunsTodaySupportMatrix:
    """Typed view over the generated current-support matrix."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the generated matrix schema version."""

        return _required_field(self.envelope, "runs_today_schema_version")

    @property
    def matrix_id(self) -> str:
        """Return the generated matrix identifier."""

        return _required_field(self.envelope, "runs_today_matrix_id")

    @property
    def support_state_vocabulary(self) -> tuple[str, ...]:
        """Return the support-state vocabulary used by this matrix."""

        return _csv_values(self.envelope.field("runs_today_support_state_vocabulary"))

    @property
    def families(self) -> tuple[str, ...]:
        """Return support-row families in declared order."""

        return _csv_values(self.envelope.field("runs_today_family_order"))

    @property
    def row_ids(self) -> tuple[str, ...]:
        """Return support rows in declared order."""

        return _csv_values(self.envelope.field("runs_today_row_order"))

    @property
    def rows(self) -> tuple[RunsTodaySupportRow, ...]:
        """Return typed support rows in declared order."""
        from ._client_report_views import RunsTodaySupportMatrix_rows as _impl
        return _impl(self)

    @property
    def executable_row_count(self) -> int:
        """Return executable row count."""

        return self.envelope.field_int("runs_today_executable_row_count", 0) or 0

    @property
    def feature_gated_row_count(self) -> int:
        """Return feature-gated row count."""

        return self.envelope.field_int("runs_today_feature_gated_row_count", 0) or 0

    @property
    def diagnostic_only_row_count(self) -> int:
        """Return diagnostic-only row count."""

        return self.envelope.field_int("runs_today_diagnostic_only_row_count", 0) or 0

    @property
    def report_only_row_count(self) -> int:
        """Return report-only row count."""

        return self.envelope.field_int("runs_today_report_only_row_count", 0) or 0

    @property
    def blocked_row_count(self) -> int:
        """Return blocked row count."""

        return self.envelope.field_int("runs_today_blocked_row_count", 0) or 0

    @property
    def future_row_count(self) -> int:
        """Return future row count."""

        return self.envelope.field_int("runs_today_future_row_count", 0) or 0

    @property
    def all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether all rows report no fallback and no external engine invocation."""

        return (
            self.envelope.field_bool(
                "runs_today_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
        )

    @property
    def performance_claim_allowed(self) -> bool:
        """Whether this matrix authorizes a performance claim."""

        return self.envelope.field_bool("runs_today_performance_claim_allowed", True) is True

    @property
    def package_publication_allowed(self) -> bool:
        """Whether this matrix authorizes package publication."""

        return self.envelope.field_bool("runs_today_package_publication_allowed", True) is True

    @property
    def production_unsupported_diagnostic_schema_version(self) -> str:
        """Return the production-family unsupported diagnostic catalog schema."""

        return _required_field(
            self.envelope,
            "production_unsupported_diagnostic_schema_version",
        )

    @property
    def production_unsupported_diagnostic_row_ids(self) -> tuple[str, ...]:
        """Return production unsupported diagnostic row identifiers."""

        return _csv_values(
            self.envelope.field("production_unsupported_diagnostic_row_order")
        )

    @property
    def production_unsupported_diagnostic_rows(
        self,
    ) -> tuple[ProductionUnsupportedDiagnosticRow, ...]:
        """Return production unsupported diagnostics in declared order."""
        from ._client_report_views import RunsTodaySupportMatrix_production_unsupported_diagnostic_rows as _impl
        return _impl(self)

    @property
    def production_unsupported_diagnostic_all_rows_safe(self) -> bool:
        """Whether every production unsupported diagnostic is no-fallback/no-effect."""
        from ._client_report_views import RunsTodaySupportMatrix_production_unsupported_diagnostic_all_rows_safe as _impl
        return _impl(self)

    def production_unsupported_diagnostic_row(
        self,
        row_id: str,
    ) -> ProductionUnsupportedDiagnosticRow:
        """Return a single production unsupported diagnostic row by id."""

        normalized = row_id.strip().replace("-", "_")
        for row in self.production_unsupported_diagnostic_rows:
            if row.row_id == normalized:
                return row
        raise KeyError(row_id)

    def rows_by_family(self, family: str) -> tuple[RunsTodaySupportRow, ...]:
        """Return rows matching one family."""

        return tuple(row for row in self.rows if row.family == family)

    def rows_by_support_state(self, support_state: str) -> tuple[RunsTodaySupportRow, ...]:
        """Return rows matching one support state."""

        return tuple(row for row in self.rows if row.support_state == support_state)

    def row(self, row_id: str) -> RunsTodaySupportRow:
        """Return a single support row by id."""

        for row in self.rows:
            if row.row_id == row_id:
                return row
        raise KeyError(row_id)


@dataclass(frozen=True, slots=True)
class ComputeCapabilityMatrix:
    """Typed view over the report-only compute capability coverage matrix."""

    envelope: OutputEnvelope

    @property
    def matrix_status(self) -> str:
        """Return the matrix report status."""

        return _required_field(self.envelope, "matrix_status")

    @property
    def claim_grade_status(self) -> str:
        """Return whether the matrix allows claim-grade compute-engine claims."""

        return _required_field(self.envelope, "claim_grade_status")

    @property
    def rows(self) -> tuple[ComputeCapabilityRow, ...]:
        """Return matrix rows in declared order."""
        from ._client_report_views import ComputeCapabilityMatrix_rows as _impl
        return _impl(self)

    @property
    def operator_families(self) -> tuple[OperatorFamilyCoverageRow, ...]:
        """Return operator-family ladder rows in declared order."""
        from ._client_report_views import ComputeCapabilityMatrix_operator_families as _impl
        return _impl(self)

    @property
    def native_vortex_admission_status(self) -> str:
        """Return the scoped native Vortex admission status."""

        return _required_field(self.envelope, "native_vortex_admission_status")

    @property
    def native_vortex_admission_lanes(self) -> tuple[NativeVortexAdmissionLane, ...]:
        """Return scoped native Vortex provider-admission lanes."""
        from ._client_report_views import ComputeCapabilityMatrix_native_vortex_admission_lanes as _impl
        return _impl(self)

    @property
    def prepared_vortex_scan_pushdown_status(self) -> str:
        """Return the prepared/native Vortex Scan pushdown matrix status."""

        return _required_field(self.envelope, "prepared_vortex_scan_pushdown_status")

    @property
    def prepared_vortex_scan_pushdown_rows(self) -> tuple[PreparedVortexScanPushdownRow, ...]:
        """Return prepared/native Vortex Scan pushdown capability rows."""
        from ._client_report_views import ComputeCapabilityMatrix_prepared_vortex_scan_pushdown_rows as _impl
        return _impl(self)

    @property
    def prepared_vortex_scan_pushdown_all_rows_no_fallback(self) -> bool:
        """Whether every scan pushdown row preserves no-fallback evidence."""

        return (
            self.envelope.field_bool(
                "prepared_vortex_scan_pushdown_all_rows_no_fallback",
                False,
            )
            is True
        )

    @property
    def prepared_vortex_scan_pushdown_all_rows_external_engine_free(self) -> bool:
        """Whether every scan pushdown row avoids external-engine execution."""

        return (
            self.envelope.field_bool(
                "prepared_vortex_scan_pushdown_all_rows_external_engine_invoked_false",
                False,
            )
            is True
        )

    @property
    def native_unsupported_coverage_status(self) -> str:
        """Return native unsupported coverage status for the current matrix."""

        return _required_field(self.envelope, "native_unsupported_coverage_status")

    @property
    def native_unsupported_coverage_rows(self) -> tuple[NativeUnsupportedCoverageRow, ...]:
        """Return deterministic unsupported source/sink/operator/workload rows."""
        from ._client_report_views import ComputeCapabilityMatrix_native_unsupported_coverage_rows as _impl
        return _impl(self)

    @property
    def native_unsupported_coverage_complete(self) -> bool:
        """Whether current source/sink/operator/workload coverage is explicit."""

        return (
            self.envelope.field_bool(
                "native_unsupported_coverage_current_matrix_complete",
                False,
            )
            is True
        )

    @property
    def predicate_dtype_coverage_rows(self) -> tuple[PredicateDtypeCoverageRow, ...]:
        """Return predicate, DType, null, nested, and statistics coverage rows."""
        from ._client_report_views import ComputeCapabilityMatrix_predicate_dtype_coverage_rows as _impl
        return _impl(self)

    @property
    def predicate_dtype_coverage_complete(self) -> bool:
        """Whether current predicate/DType/null/nested coverage is explicit."""

        return (
            self.envelope.field_bool("predicate_dtype_coverage_current_matrix_complete", False)
            is True
        )

    @property
    def materialization_policy_report_ref(self) -> str:
        """Return the shared materialization/decode policy report ref."""

        return _required_field(self.envelope, "materialization_policy_report_ref")

    @property
    def materialization_policy_rows(self) -> tuple[MaterializationPolicyRow, ...]:
        """Return shared materialization/decode policy rows."""
        from ._client_report_views import ComputeCapabilityMatrix_materialization_policy_rows as _impl
        return _impl(self)

    @property
    def materialization_policy_all_rows_classified(self) -> bool:
        """Whether every materialization policy row has explicit decode/materialization flags."""

        return (
            self.envelope.field_bool("materialization_policy_all_rows_classified", False) is True
        )

    @property
    def no_runtime(self) -> bool:
        """Whether the matrix command avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the matrix declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and self.envelope.field_bool("all_rows_fallback_attempted_false", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the matrix command performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class SemanticConformanceRow:
    """One semantic dimension row in the ShardLoomNative conformance suite."""

    row_id: str
    dimension: str
    operator_family: str
    fixture_status: str
    current_support: str
    assertion: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    fixture_executed: bool
    passed: bool
    fallback_attempted: bool
    external_oracle_used: bool


@dataclass(frozen=True, slots=True)
class SemanticConformanceSuite:
    """Typed view over the P7.4 semantic conformance suite report."""

    envelope: OutputEnvelope

    @property
    def suite_status(self) -> str:
        """Return the semantic suite status."""

        return _required_field(self.envelope, "suite_status")

    @property
    def semantic_profile(self) -> str:
        """Return the named semantic profile under test."""

        return _required_field(self.envelope, "semantic_profile")

    @property
    def rows(self) -> tuple[SemanticConformanceRow, ...]:
        """Return semantic rows in declared order."""
        from ._client_report_views import SemanticConformanceSuite_rows as _impl
        return _impl(self)

    @property
    def executed_fixture_count(self) -> int:
        """Return the number of in-memory semantic fixtures executed."""

        return int(_required_field(self.envelope, "executed_fixture_count"))

    @property
    def passed_fixture_count(self) -> int:
        """Return the number of semantic fixtures that passed."""

        return int(_required_field(self.envelope, "passed_fixture_count"))

    @property
    def failed_fixture_count(self) -> int:
        """Return the number of semantic fixtures that failed."""

        return int(_required_field(self.envelope, "failed_fixture_count"))

    @property
    def no_runtime(self) -> bool:
        """Whether the suite avoided workload runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the suite declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the suite performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceSlot:
    """One evidence-slot status from a typed execution-result envelope."""

    kind: str
    status: str
    refs: tuple[str, ...]
    detail: str


@dataclass(frozen=True, slots=True)
class ExecutionResultEnvelopeView:
    """Typed view over an artifact-rich top-level execution result envelope."""

    envelope: OutputEnvelope

    @property
    def plan_id(self) -> str:
        """Return the top-level execution plan id."""

        return _required_field(self.envelope, "plan_id")

    @property
    def plan_kind(self) -> str:
        """Return the top-level execution plan kind."""

        return _required_field(self.envelope, "plan_kind")

    @property
    def execution_status(self) -> str:
        """Return the provider-neutral execution status."""

        return _required_field(self.envelope, "execution_status")

    @property
    def provider_api_surface(self) -> str | None:
        """Return the provider API surface when present."""

        value = self.envelope.field("provider_api_surface")
        return None if value in {None, "none"} else value

    @property
    def provider_version(self) -> str | None:
        """Return the provider version when present."""

        value = self.envelope.field("provider_version")
        return None if value in {None, "none"} else value

    @property
    def evidence_completeness_status(self) -> str:
        """Return the overall evidence completeness status."""

        return _required_field(self.envelope, "evidence_completeness_status")

    @property
    def result_refs(self) -> tuple[str, ...]:
        """Return result refs preserved in the execution envelope."""

        return _csv_values(self.envelope.field("result_refs"))

    @property
    def artifact_refs(self) -> tuple[str, ...]:
        """Return provider artifact refs preserved in the execution envelope."""

        return _csv_values(self.envelope.field("artifact_refs"))

    @property
    def inline_artifact_ids(self) -> tuple[str, ...]:
        """Return inline artifact ids preserved in the execution envelope."""

        return _csv_values(self.envelope.field("inline_artifact_ids"))

    @property
    def execution_certificate_refs(self) -> tuple[str, ...]:
        """Return execution certificate refs preserved in the envelope."""

        return _csv_values(self.envelope.field("execution_certificate_refs"))

    @property
    def native_io_certificate_refs(self) -> tuple[str, ...]:
        """Return Native I/O certificate refs preserved in the envelope."""

        return _csv_values(self.envelope.field("native_io_certificate_refs"))

    @property
    def representation_transitions(self) -> tuple[str, ...]:
        """Return representation-transition labels preserved in the envelope."""

        return _csv_values(self.envelope.field("representation_transitions"))

    @property
    def execution_mode_selection_fields(self) -> Mapping[str, str]:
        """Return the typed execution-mode selection report fields when present."""
        from ._client_report_views import ExecutionResultEnvelopeView_execution_mode_selection_fields as _impl
        return _impl(self)

    @property
    def compute_flow_evidence_fields(self) -> Mapping[str, str]:
        """Return typed compute-flow evidence fields when present."""
        from ._client_report_views import ExecutionResultEnvelopeView_compute_flow_evidence_fields as _impl
        return _impl(self)

    @property
    def facade_compatibility_matrix_fields(self) -> Mapping[str, str]:
        """Return typed GAR-0038 facade compatibility matrix fields when present."""
        from ._client_report_views import ExecutionResultEnvelopeView_facade_compatibility_matrix_fields as _impl
        return _impl(self)

    def _execution_mode_field(self, key: str) -> str | None:
        return self.execution_mode_selection_fields.get(key) or self.envelope.field(key)

    def _execution_mode_bool(self, key: str, default: bool = False) -> bool:
        value = self.execution_mode_selection_fields.get(key)
        if value is not None:
            if value.strip().lower() == "evidence_incomplete":
                return default
            return _string_bool_value(value, key)
        return self.envelope.field_bool(key, default) is True

    def _compute_flow_field(self, key: str) -> str | None:
        return self.compute_flow_evidence_fields.get(key) or self.envelope.field(key)

    def _compute_flow_bool(self, key: str, default: bool = False) -> bool:
        value = self.compute_flow_evidence_fields.get(key)
        if value is not None and value != "evidence_incomplete":
            return _string_bool_value(value, key)
        return self.envelope.field_bool(key, default) is True

    def _facade_matrix_field(self, key: str) -> str | None:
        from ._client_report_views import ExecutionResultEnvelopeView__facade_matrix_field as _impl
        return _impl(self, key)

    def _facade_matrix_bool(self, key: str, default: bool = False) -> bool:
        value = self._facade_matrix_field(key)
        if value is None:
            return default
        return _string_bool_value(value, key)

    def _facade_matrix_int(self, key: str) -> int | None:
        value = self._facade_matrix_field(key)
        if value is None:
            return None
        return int(value)

    @property
    def requested_execution_mode(self) -> str | None:
        """Return the requested execution mode when the envelope reports one."""

        return self._execution_mode_field("requested_execution_mode")

    @property
    def selected_execution_mode(self) -> str | None:
        """Return the execution mode selected by ShardLoom."""

        return self._execution_mode_field("selected_execution_mode") or (
            self._execution_mode_field("execution_mode")
        )

    @property
    def mode_selection_reason(self) -> str | None:
        """Return the reported execution-mode selection reason."""

        return self._execution_mode_field("mode_selection_reason")

    @property
    def execution_mode_family(self) -> str | None:
        """Return the execution-mode family."""

        return self._execution_mode_field("execution_mode_family")

    @property
    def mode_supported(self) -> bool:
        """Whether the requested mode was admitted as supported."""

        return self._execution_mode_bool("mode_supported")

    @property
    def support_status(self) -> str | None:
        """Return the execution-mode admission support status."""

        return self._execution_mode_field("support_status")

    @property
    def claim_gate_status(self) -> str | None:
        """Return the workload/row claim-gate status."""

        return self._execution_mode_field("claim_gate_status")

    @property
    def claim_gate_reason(self) -> str | None:
        """Return the reason for the claim-gate status."""

        return self._execution_mode_field("claim_gate_reason")

    @property
    def unsupported_diagnostic_code(self) -> str | None:
        """Return the unsupported diagnostic for blocked mode admission."""

        return self._execution_mode_field("unsupported_diagnostic_code")

    @property
    def blocker_id(self) -> str | None:
        """Return the blocker identifier for unsupported mode admission."""

        return self._execution_mode_field("blocker_id")

    @property
    def required_future_evidence(self) -> str | None:
        """Return required future evidence for unsupported mode admission."""

        return self._execution_mode_field("required_future_evidence")

    @property
    def vortex_native_claim_allowed(self) -> bool:
        """Whether this envelope can satisfy the native Vortex claim gate."""

        return self._execution_mode_bool("vortex_native_claim_allowed")

    @property
    def compatibility_import_included(self) -> bool:
        """Whether compatibility import is included in the reported timing scope."""

        return self._execution_mode_bool("compatibility_import_included")

    @property
    def vortex_prepare_included(self) -> bool:
        """Whether Vortex preparation is included in the reported timing scope."""

        return self._execution_mode_bool("vortex_prepare_included")

    @property
    def vortex_write_reopen_included(self) -> bool:
        """Whether Vortex write/reopen is included in the reported timing scope."""

        return self._execution_mode_bool("vortex_write_reopen_included")

    @property
    def direct_transient_execution(self) -> bool:
        """Whether the execution used the internal local-source smoke compatibility mode."""

        return self._execution_mode_bool("direct_transient_execution")

    @property
    def result_sink_claim_gate_status(self) -> str | None:
        """Return result-sink-specific claim-gate status when present."""

        return self._compute_flow_field("result_sink_claim_gate_status")

    @property
    def result_sink_claim_gate_reason(self) -> str | None:
        """Return result-sink-specific claim-gate reason when present."""

        return self._compute_flow_field("result_sink_claim_gate_reason")

    @property
    def computed_result_sink_replay_verified(self) -> bool:
        """Whether a computed result sink was replay-verified."""

        return self._compute_flow_bool("computed_result_sink_replay_verified")

    @property
    def computed_result_sink_native_io_certificate_status(self) -> str | None:
        """Return the result-sink Native I/O certificate status when present."""

        return self._compute_flow_field("computed_result_sink_native_io_certificate_status")

    @property
    def prepared_native_vortex_lifecycle_status(self) -> str | None:
        """Return the prepared/native Vortex artifact lifecycle status."""

        return self._compute_flow_field("prepared_native_vortex_lifecycle_status")

    @property
    def prepared_native_vortex_lifecycle_output_status(self) -> str | None:
        """Return the lifecycle output/replay status."""

        return self._compute_flow_field("prepared_native_vortex_lifecycle_output_status")

    @property
    def prepared_native_vortex_lifecycle_no_standalone_lane(self) -> bool:
        """Whether lifecycle evidence stayed in the prepared/native route."""

        return self._compute_flow_bool("prepared_native_vortex_lifecycle_no_standalone_lane")

    @property
    def operator_execution_class(self) -> str | None:
        """Return the prepared/native operator execution class when present."""

        return self._compute_flow_field("operator_execution_class")

    @property
    def operator_admission_status(self) -> str | None:
        """Return the prepared/native operator admission status when present."""

        return self._compute_flow_field("operator_admission_status")

    @property
    def operator_blocker_id(self) -> str | None:
        """Return the operator blocker id when prepared/native execution is not encoded-native."""

        return self._compute_flow_field("operator_blocker_id")

    @property
    def operator_blocker_reason(self) -> str | None:
        """Return the operator blocker reason when present."""

        return self._compute_flow_field("operator_blocker_reason")

    @property
    def operator_encoded_native_claim_allowed(self) -> bool:
        """Whether the row may claim encoded-native operator execution."""

        return self._compute_flow_bool("operator_encoded_native_claim_allowed")

    @property
    def operator_temporary_materialization_used(self) -> bool:
        """Whether prepared/native execution used a materialized temporary operator."""

        return self._compute_flow_bool("operator_temporary_materialization_used")

    @property
    def facade_compatibility_matrix_report_id(self) -> str | None:
        """Return the GAR-0038 facade compatibility matrix report id."""

        return self._facade_matrix_field("report_id")

    @property
    def facade_compatibility_matrix_gar_id(self) -> str | None:
        """Return the GAR id for the facade compatibility matrix."""

        return self._facade_matrix_field("gar_id")

    @property
    def facade_compatibility_matrix_support_status(self) -> str | None:
        """Return the facade matrix support status."""

        return self._facade_matrix_field("support_status")

    @property
    def facade_compatibility_matrix_claim_gate_status(self) -> str | None:
        """Return the facade matrix claim-gate status."""

        return self._facade_matrix_field("claim_gate_status")

    @property
    def facade_compatibility_matrix_row_order(self) -> tuple[str, ...]:
        """Return ordered surface names from the facade compatibility matrix."""

        return _csv_values(self._facade_matrix_field("row_order"))

    @property
    def facade_unsupported_surface_count(self) -> int | None:
        """Return the count of unsupported runtime surfaces in the facade matrix."""

        return self._facade_matrix_int("unsupported_surface_count")

    @property
    def facade_legacy_boundary_status(self) -> str | None:
        """Return the legacy facade boundary status."""

        return self._facade_matrix_field("legacy_boundary_status")

    @property
    def facade_all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether all facade matrix rows preserve no-fallback/no-external-engine policy."""

        return self._facade_matrix_bool("all_rows_no_fallback_no_external_engine")

    @property
    def evidence_slots(self) -> tuple[ExecutionEvidenceSlot, ...]:
        """Return explicit present/not-required/evidence-incomplete slot statuses."""
        from ._client_report_views import ExecutionResultEnvelopeView_evidence_slots as _impl
        return _impl(self)

    @property
    def incomplete_evidence_slots(self) -> tuple[ExecutionEvidenceSlot, ...]:
        """Return evidence slots marked incomplete."""

        return tuple(
            slot for slot in self.evidence_slots if slot.status == "evidence_incomplete"
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether execution attempted fallback according to policy/envelope fields."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether execution invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


class _RestApiSurfaceParityMixin:
    """Common REST parity fields shared by contract, lifecycle, and event views."""

    __slots__ = ()

    @property
    def rest_api_surface_parity_schema_version(self) -> str | None:
        """Return the REST surface parity schema version."""

        return self.envelope.field("rest_api_surface_parity_schema_version")

    @property
    def rest_api_surface_parity_surface_id(self) -> str | None:
        """Return the REST surface identifier used by parity checks."""

        return self.envelope.field("rest_api_surface_parity_surface_id")

    @property
    def rest_api_surface_parity_status(self) -> str | None:
        """Return the REST parity status for this surface."""

        return self.envelope.field("rest_api_surface_parity_status")

    @property
    def rest_api_cli_python_field_parity(self) -> bool:
        """Whether the CLI output exposes the common Python-readable REST fields."""

        return self.envelope.field_bool("rest_api_cli_python_field_parity", False) is True

    @property
    def rest_api_runtime_execution(self) -> bool:
        """Whether the REST parity contract reports runtime execution."""

        return self.envelope.field_bool(
            "rest_api_runtime_execution",
            self.envelope.field_bool("runtime_execution", False),
        ) is True

    @property
    def rest_api_runtime_equivalent_api_claim_allowed(self) -> bool:
        """Whether this surface permits a broad runtime-equivalent REST API claim."""

        return (
            self.envelope.field_bool(
                "rest_api_runtime_equivalent_api_claim_allowed",
                False,
            )
            is True
        )

    @property
    def rest_api_policy_fields(self) -> tuple[str, ...]:
        """Return policy fields every REST surface projects for CLI/Python parity."""

        return _csv_values(self.envelope.field("rest_api_policy_fields"))

    @property
    def rest_api_mode_selection_fields(self) -> tuple[str, ...]:
        """Return execution-mode selection fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_mode_selection_fields"))

    @property
    def rest_api_evidence_fields(self) -> tuple[str, ...]:
        """Return evidence fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_evidence_fields"))

    @property
    def rest_api_evidence_refs(self) -> tuple[str, ...]:
        """Return surface-specific evidence references carried by the REST view."""

        return _csv_values(self.envelope.field("rest_api_evidence_refs"))

    @property
    def rest_api_claim_gate_status(self) -> str | None:
        """Return the REST claim-gate status for this surface."""

        return self.envelope.field("rest_api_claim_gate_status")

    @property
    def rest_api_claim_gate_reason(self) -> str | None:
        """Return the reason behind the REST claim-gate status."""

        return self.envelope.field("rest_api_claim_gate_reason")

    @property
    def rest_api_no_fallback_fields(self) -> tuple[str, ...]:
        """Return no-fallback fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_no_fallback_fields"))

    @property
    def rest_api_fallback_attempted(self) -> bool:
        """Whether REST handling attempted fallback execution."""

        return self.envelope.fallback.attempted or (
            self.envelope.field_bool(
                "rest_api_fallback_attempted",
                self.envelope.field_bool("fallback_attempted", False),
            )
            is True
        )

    @property
    def rest_api_external_engine_invoked(self) -> bool:
        """Whether REST handling invoked an external engine."""

        return (
            self.envelope.field_bool("rest_api_external_engine_invoked", False) is True
            or _envelope_external_engine_invoked(self.envelope)
        )

    @property
    def rest_api_execution_delegated(self) -> bool:
        """Whether REST handling delegated execution."""

        return (
            self.envelope.field_bool(
                "rest_api_execution_delegated",
                self.envelope.field_bool("execution_delegated", False),
            )
            is True
        )

    @property
    def rest_api_no_fallback_no_external_engine(self) -> bool:
        """Whether REST parity proves no fallback, no external engine, and no delegation."""

        explicit = self.envelope.field_bool("rest_api_no_fallback_no_external_engine")
        if explicit is not None:
            return explicit is True
        return not (
            self.rest_api_fallback_attempted
            or self.rest_api_external_engine_invoked
            or self.rest_api_execution_delegated
        )


@dataclass(frozen=True, slots=True)
class RestApiContractPlan(_RestApiSurfaceParityMixin):
    """Typed convenience view over the CG-23 REST/OpenAPI contract report."""

    envelope: OutputEnvelope

    @property
    def api_version(self) -> str | None:
        """Return the REST API version."""

        return self.envelope.field("api_version")

    @property
    def openapi_version(self) -> str | None:
        """Return the declared OpenAPI version."""

        return self.envelope.field("openapi_version")

    @property
    def openapi_contract_path(self) -> str | None:
        """Return the checked-in OpenAPI contract path."""

        return self.envelope.field("openapi_contract_path")

    @property
    def represented_resources(self) -> tuple[str, ...]:
        """Return represented REST resource groups."""

        value = self.envelope.field("represented_resources", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def discovery_endpoint_paths(self) -> tuple[str, ...]:
        """Return contract-only discovery endpoint paths."""

        value = self.envelope.field("discovery_endpoint_paths", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def execution_mode_vocabulary(self) -> tuple[str, ...]:
        """Return the shared execution-mode enum declared by the REST contract."""

        value = self.envelope.field("execution_mode_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def execution_mode_selection_schema_version(self) -> str | None:
        """Return the execution-mode selection report schema version."""

        return self.envelope.field("execution_mode_selection_schema_version")

    @property
    def execution_mode_selection_fields(self) -> tuple[str, ...]:
        """Return REST selection-report fields mirrored from CLI/Python."""

        value = self.envelope.field("execution_mode_selection_fields", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def rest_execution_mode_support_status(self) -> str | None:
        """Return the REST execution-mode support posture."""

        return self.envelope.field("rest_execution_mode_support_status")

    @property
    def unsupported_execution_mode_diagnostic_code(self) -> str | None:
        """Return the deterministic diagnostic for unsupported REST mode requests."""

        return self.envelope.field("unsupported_execution_mode_diagnostic_code")

    @property
    def rest_runtime_unsupported_schema_version(self) -> str | None:
        """Return the REST runtime unsupported gate schema version."""

        return self.envelope.field("rest_runtime_unsupported_schema_version")

    @property
    def rest_runtime_unsupported_report_id(self) -> str | None:
        """Return the REST runtime unsupported gate report identifier."""

        return self.envelope.field("rest_runtime_unsupported_report_id")

    @property
    def rest_runtime_unsupported_rows(self) -> tuple[str, ...]:
        """Return REST runtime unsupported gate row identifiers."""

        return _csv_values(self.envelope.field("rest_runtime_unsupported_row_order"))

    @property
    def rest_runtime_unsupported_blocked_row_count(self) -> int:
        """Return blocked rows in the REST runtime unsupported gate."""

        return self.envelope.field_int("rest_runtime_unsupported_blocked_row_count", 0) or 0

    @property
    def rest_runtime_unsupported_report_only_row_count(self) -> int:
        """Return report-only rows in the REST runtime unsupported gate."""

        return self.envelope.field_int("rest_runtime_unsupported_report_only_row_count", 0) or 0

    @property
    def rest_runtime_unsupported_diagnostic_codes(self) -> tuple[str, ...]:
        """Return diagnostics declared by the REST runtime unsupported gate."""

        return _csv_values(self.envelope.field("rest_runtime_unsupported_diagnostic_codes"))

    @property
    def rest_runtime_unsupported_claim_gate_status(self) -> str | None:
        """Return the REST runtime unsupported gate claim status."""

        return self.envelope.field("rest_runtime_unsupported_claim_gate_status")

    @property
    def rest_runtime_http_listener_supported(self) -> bool:
        """Whether the REST runtime gate supports an HTTP listener."""

        return self.envelope.field_bool("rest_runtime_http_listener_supported", False) is True

    @property
    def rest_runtime_remote_execution_supported(self) -> bool:
        """Whether the REST runtime gate supports remote execution."""

        return self.envelope.field_bool("rest_runtime_remote_execution_supported", False) is True

    @property
    def rest_runtime_flight_adbc_transport_supported(self) -> bool:
        """Whether the REST runtime gate supports Flight/ADBC transport."""

        return (
            self.envelope.field_bool(
                "rest_runtime_flight_adbc_transport_supported",
                False,
            )
            is True
        )

    @property
    def rest_runtime_external_broker_supported(self) -> bool:
        """Whether the REST runtime gate supports external brokers."""

        return self.envelope.field_bool("rest_runtime_external_broker_supported", False) is True

    @property
    def rest_runtime_dependency_expansion_allowed(self) -> bool:
        """Whether dependency-expanded server work is currently allowed."""

        return (
            self.envelope.field_bool(
                "rest_runtime_dependency_expansion_allowed",
                False,
            )
            is True
        )

    @property
    def rest_runtime_no_server_no_fallback_no_external_engine(self) -> bool:
        """Whether REST runtime posture preserves no-server/no-fallback boundaries."""

        return (
            self.envelope.field_bool("rest_runtime_server_started", True) is False
            and self.envelope.field_bool("rest_runtime_network_listener_opened", True) is False
            and self.envelope.field_bool("rest_runtime_external_engine_invoked", True) is False
            and self.envelope.field_bool("rest_runtime_fallback_attempted", True) is False
        )

    @property
    def contract_artifact_checked_in(self) -> bool:
        """Whether the checked-in OpenAPI contract artifact is reported present."""

        return self.envelope.field_bool("openapi_contract_artifact_checked_in", False) is True

    @property
    def server_started(self) -> bool:
        """Whether the command started a server."""

        return self.envelope.field_bool("server_started", False) is True

    @property
    def network_listener_opened(self) -> bool:
        """Whether the command opened a network listener."""

        return self.envelope.field_bool("network_listener_opened", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )


@dataclass(frozen=True, slots=True)
class RestApiDiscoveryContract(RestApiContractPlan):
    """Typed view over `serve --mode discovery` contract-only output."""

    @property
    def bind(self) -> str | None:
        """Return the requested bind address."""

        return self.envelope.field("bind")

    @property
    def server_mode(self) -> str | None:
        """Return the requested server mode."""

        return self.envelope.field("server_mode")

    @property
    def contract_only(self) -> bool:
        """Whether `serve` stayed in contract-only mode."""

        return self.envelope.field_bool("serve_command_contract_only", False) is True


@dataclass(frozen=True, slots=True)
class RestApiPlanPreview(_RestApiSurfaceParityMixin):
    """Typed view over the CG-23 plan/explain/validate/certification preview."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic preview fixture scenario."""

        return self.envelope.field("scenario")

    @property
    def preview_status(self) -> str | None:
        """Return the preview status."""

        return self.envelope.field("preview_status")

    @property
    def plan_handle(self) -> str | None:
        """Return the stable plan handle."""

        return self.envelope.field("plan_handle")

    @property
    def operations(self) -> tuple[str, ...]:
        """Return supported plan-preview operations."""

        value = self.envelope.field("preview_operations", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def stage_order(self) -> tuple[str, ...]:
        """Return the preview stage order."""

        value = self.envelope.field("stage_order", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def stage_statuses(self) -> Mapping[str, str]:
        """Return per-stage status values keyed by stage id."""

        statuses: dict[str, str] = {}
        for stage in self.stage_order:
            value = self.envelope.field(f"{stage}_stage_status")
            if value is not None:
                statuses[stage] = value
        return statuses

    @property
    def problem_details_emitted(self) -> bool:
        """Whether the preview emitted problem-details fields."""

        return self.envelope.field_bool("problem_details_emitted", False) is True

    @property
    def problem_details_type(self) -> str | None:
        """Return the problem-details type URI, if any."""

        value = self.envelope.field("problem_details_type")
        return None if value in {None, "none"} else value

    @property
    def problem_details_status(self) -> int | None:
        """Return the problem-details HTTP status, if any."""

        value = self.envelope.field("problem_details_status")
        if value in {None, "none"}:
            return None
        return int(value)

    @property
    def problem_details_diagnostic_code(self) -> str | None:
        """Return the problem-details diagnostic code, if any."""

        value = self.envelope.field("problem_details_diagnostic_code")
        return None if value in {None, "none"} else value

    @property
    def unsupported_reason(self) -> str | None:
        """Return the deterministic unsupported reason, if any."""

        value = self.envelope.field("unsupported_reason")
        return None if value in {None, "none"} else value

    @property
    def server_started(self) -> bool:
        """Whether the preview started a server."""

        return self.envelope.field_bool("server_started", False) is True

    @property
    def network_listener_opened(self) -> bool:
        """Whether the preview opened a listener."""

        return self.envelope.field_bool("network_listener_opened", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether the preview executed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the preview attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether the preview delegated execution to any external engine."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiLocalLifecycle(_RestApiSurfaceParityMixin):
    """Typed view over the CG-23 certified local lifecycle and result delivery bundle."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic lifecycle scenario."""

        return self.envelope.field("scenario")

    @property
    def lifecycle_status(self) -> str | None:
        """Return the lifecycle status."""

        return self.envelope.field("lifecycle_status")

    @property
    def engine_mode(self) -> str | None:
        """Return the engine mode routed through the local lifecycle."""

        return self.envelope.field("engine_mode")

    @property
    def control_plane_invoked(self) -> bool:
        """Whether the local lifecycle control-plane envelope was invoked."""

        return self.envelope.field_bool("control_plane_invoked", False) is True

    @property
    def control_plane_scope(self) -> str | None:
        """Return the local control-plane scope."""

        return self.envelope.field("control_plane_scope")

    @property
    def network_policy(self) -> str | None:
        """Return the lifecycle network policy."""

        return self.envelope.field("network_policy")

    @property
    def checkpoint_state_posture(self) -> str | None:
        """Return the checkpoint/state posture."""

        return self.envelope.field("checkpoint_state_posture")

    @property
    def query_id(self) -> str | None:
        """Return the query handle."""

        return self.envelope.field("query_id")

    @property
    def result_ref(self) -> str | None:
        """Return the result reference, if available."""

        value = self.envelope.field("result_ref")
        return None if value in {None, "none"} else value

    @property
    def lifecycle_operations(self) -> tuple[str, ...]:
        """Return supported lifecycle operations."""

        value = self.envelope.field("lifecycle_operations", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def result_policies(self) -> tuple[str, ...]:
        """Return result policy materialization summaries."""

        value = self.envelope.field("result_policies", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def inline_json_available(self) -> bool:
        """Whether inline JSON result delivery is available."""

        return self.envelope.field_bool("inline_json_available", False) is True

    @property
    def vortex_artifact_available(self) -> bool:
        """Whether a high-fidelity Vortex artifact result is available."""

        return self.envelope.field_bool("vortex_artifact_available", False) is True

    @property
    def arrow_ipc_materialization(self) -> str | None:
        """Return the Arrow IPC materialization classification."""

        return self.envelope.field("arrow_ipc_materialization")

    @property
    def arrow_ipc_certified_native(self) -> bool:
        """Whether Arrow IPC is certified as native full-fidelity output."""

        return self.envelope.field_bool("arrow_ipc_certified_native", False) is True

    @property
    def result_ttl_seconds(self) -> int:
        """Return local result TTL in seconds."""

        return self.envelope.field_int("result_ttl_seconds", 0) or 0

    @property
    def cleanup_required(self) -> bool:
        """Whether lifecycle cleanup is required."""

        return self.envelope.field_bool("cleanup_required", False) is True

    @property
    def non_certified_path_blocked(self) -> bool:
        """Whether an uncertified path was blocked before execution."""

        return self.envelope.field_bool("non_certified_path_blocked", False) is True

    @property
    def cancellation_status(self) -> str | None:
        """Return cancellation state."""

        return self.envelope.field("cancellation_status")

    @property
    def retry_status(self) -> str | None:
        """Return retry state."""

        return self.envelope.field("retry_status")

    @property
    def live_fixture_invoked(self) -> bool:
        """Whether the in-memory live fixture runtime was invoked."""

        return self.envelope.field_bool("live_fixture_invoked", False) is True

    @property
    def hybrid_fixture_invoked(self) -> bool:
        """Whether the in-memory hybrid fixture runtime was invoked."""

        return self.envelope.field_bool("hybrid_fixture_invoked", False) is True

    @property
    def remote_worker_invoked(self) -> bool:
        """Whether any remote worker was invoked."""

        return self.envelope.field_bool("remote_worker_invoked", False) is True

    @property
    def distributed_runtime_status(self) -> str | None:
        """Return distributed runtime status."""

        return self.envelope.field("distributed_runtime_status")

    @property
    def distributed_worker_blocker_id(self) -> str | None:
        """Return the stable distributed worker blocker id."""

        return self.envelope.field("distributed_worker_blocker_id")

    @property
    def distributed_claim_gate_status(self) -> str | None:
        """Return distributed claim-gate status."""

        return self.envelope.field("distributed_claim_gate_status")

    @property
    def small_result_boundary(self) -> str | None:
        """Return the small-result transfer boundary."""

        return self.envelope.field("small_result_boundary")

    @property
    def query_execution(self) -> bool:
        """Whether query execution was performed."""

        return self.envelope.field_bool("query_execution", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether runtime execution was performed."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def local_execution_performed(self) -> bool:
        """Whether the lifecycle used a certified local execution path."""

        return self.envelope.field_bool("local_execution_performed", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether lifecycle handling attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether lifecycle handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiEventStream(_RestApiSurfaceParityMixin):
    """Typed view over the CG-23 live/hybrid event stream contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic event-stream scenario."""

        return self.envelope.field("scenario")

    @property
    def event_stream_status(self) -> str | None:
        """Return the event stream status."""

        return self.envelope.field("event_stream_status")

    @property
    def stream_id(self) -> str | None:
        """Return the stream handle."""

        return self.envelope.field("stream_id")

    @property
    def stream_ref(self) -> str | None:
        """Return the stream reference."""

        value = self.envelope.field("stream_ref")
        return None if value in {None, "none"} else value

    @property
    def engine_mode(self) -> str | None:
        """Return the event stream engine-mode posture."""

        return self.envelope.field("engine_mode")

    @property
    def delivery_protocols(self) -> tuple[str, ...]:
        """Return declared event delivery protocols."""

        value = self.envelope.field("delivery_protocols", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def event_types(self) -> tuple[str, ...]:
        """Return declared event type names."""

        value = self.envelope.field("event_types", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def certificate_refs(self) -> tuple[str, ...]:
        """Return available event evidence certificate refs."""

        value = (
            self.envelope.field("certificate_ref_summary")
            or self.envelope.field("certificate_refs")
            or ""
        )
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def asyncapi_contract_path(self) -> str | None:
        """Return the checked-in AsyncAPI contract path."""

        return self.envelope.field("asyncapi_contract_path")

    @property
    def sse_first(self) -> bool:
        """Whether SSE is the default event delivery protocol."""

        return self.envelope.field_bool("sse_first", False) is True

    @property
    def websocket_required(self) -> bool:
        """Whether WebSocket is required for this contract."""

        return self.envelope.field_bool("websocket_required", False) is True

    @property
    def event_count(self) -> int:
        """Return the deterministic event count for the scenario."""

        return self.envelope.field_int("event_count", 0) or 0

    @property
    def workload_certified(self) -> bool:
        """Whether the referenced workload is certified for event streaming."""

        return self.envelope.field_bool("workload_certified", False) is True

    @property
    def production_claim_allowed(self) -> bool:
        """Whether production event-streaming claims are allowed."""

        return self.envelope.field_bool("production_claim_allowed", False) is True

    @property
    def broker_required(self) -> bool:
        """Whether the contract requires a broker."""

        return self.envelope.field_bool("broker_required", False) is True

    @property
    def broker_io(self) -> bool:
        """Whether broker I/O was performed."""

        return self.envelope.field_bool("broker_io", False) is True

    @property
    def object_store_io(self) -> bool:
        """Whether object-store I/O was performed."""

        return self.envelope.field_bool("object_store_io", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether event-stream handling attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether event-stream handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiSecurityGovernance(_RestApiSurfaceParityMixin):
    """Typed view over the CG-23 security/governance/agent API contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic security/governance scenario."""

        return self.envelope.field("scenario")

    @property
    def governance_status(self) -> str | None:
        """Return the governance status."""

        return self.envelope.field("governance_status")

    @property
    def auth_postures(self) -> tuple[str, ...]:
        """Return declared auth posture entries."""

        value = self.envelope.field("auth_postures", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def api_scopes(self) -> tuple[str, ...]:
        """Return API scope posture entries."""

        value = self.envelope.field("api_scopes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def mcp_tools(self) -> tuple[str, ...]:
        """Return MCP tool posture entries."""

        value = self.envelope.field("mcp_tools", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def evidence_model_signals(self) -> tuple[str, ...]:
        """Return unified evidence-model signal names."""

        value = self.envelope.field("evidence_model_signals", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def credential_references_only(self) -> bool:
        """Whether credentials stay as references."""

        return self.envelope.field_bool("credential_references_only", False) is True

    @property
    def secrets_redacted(self) -> bool:
        """Whether secret material is redacted from output."""

        return self.envelope.field_bool("secrets_redacted", False) is True

    @property
    def raw_secret_values_present(self) -> bool:
        """Whether raw secret values were emitted."""

        return self.envelope.field_bool("raw_secret_values_present", False) is True

    @property
    def destructive_policy_required(self) -> bool:
        """Whether destructive operations require an explicit policy."""

        return self.envelope.field_bool("destructive_policy_required", False) is True

    @property
    def destructive_policy_present(self) -> bool:
        """Whether an explicit destructive-operation policy was present."""

        return self.envelope.field_bool("destructive_policy_present", False) is True

    @property
    def destructive_operations_allowed(self) -> bool:
        """Whether destructive operations are allowed."""

        return self.envelope.field_bool("destructive_operations_allowed", False) is True

    @property
    def mcp_dry_run_default(self) -> bool:
        """Whether MCP tools default to dry-run-safe behavior."""

        return self.envelope.field_bool("mcp_dry_run_default", False) is True

    @property
    def mcp_effectful_tools_allowed(self) -> bool:
        """Whether effectful MCP tools are allowed by default."""

        return self.envelope.field_bool("mcp_effectful_tools_allowed", False) is True

    @property
    def mcp_discovery_side_effect_free(self) -> bool:
        """Whether MCP discovery is side-effect-free."""

        return self.envelope.field_bool("mcp_discovery_side_effect_free", False) is True

    @property
    def opentelemetry_exporter_enabled(self) -> bool:
        """Whether OpenTelemetry export was enabled."""

        return self.envelope.field_bool("opentelemetry_exporter_enabled", False) is True

    @property
    def openlineage_facets_mapped(self) -> bool:
        """Whether OpenLineage facets are represented in the evidence model."""

        return self.envelope.field_bool("openlineage_facets_mapped", False) is True

    @property
    def problem_details_mapped(self) -> bool:
        """Whether problem-details errors are represented in the evidence model."""

        return self.envelope.field_bool("problem_details_mapped", False) is True

    @property
    def cloudevents_mapped(self) -> bool:
        """Whether CloudEvents are represented in the evidence model."""

        return self.envelope.field_bool("cloudevents_mapped", False) is True

    @property
    def certificate_refs_mapped(self) -> bool:
        """Whether certificate refs are represented in the evidence model."""

        return self.envelope.field_bool("certificate_refs_mapped", False) is True

    @property
    def credential_resolution(self) -> bool:
        """Whether credentials were resolved."""

        return self.envelope.field_bool("credential_resolution", False) is True

    @property
    def secret_resolution(self) -> bool:
        """Whether secret material was resolved."""

        return self.envelope.field_bool("secret_resolution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether security/governance handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiDataPlane(_RestApiSurfaceParityMixin):
    """Typed view over the CG-23 data-plane and standards boundary contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic data-plane scenario."""

        return self.envelope.field("scenario")

    @property
    def data_plane_status(self) -> str | None:
        """Return the data-plane status."""

        return self.envelope.field("data_plane_status")

    @property
    def transfer_modes(self) -> tuple[str, ...]:
        """Return result transfer mode entries."""

        value = self.envelope.field("transfer_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def standards_names(self) -> tuple[str, ...]:
        """Return classified standards names."""

        value = self.envelope.field("standards_names", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def preferred_large_payload_modes(self) -> tuple[str, ...]:
        """Return preferred large-payload result policies."""

        value = self.envelope.field("preferred_large_payload_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def large_payload_threshold_bytes(self) -> int:
        """Return the large-payload threshold."""

        return self.envelope.field_int("large_payload_threshold_bytes", 0) or 0

    @property
    def rest_control_plane_sufficient_for_local_use(self) -> bool:
        """Whether REST remains sufficient for local use."""

        return (
            self.envelope.field_bool("rest_control_plane_sufficient_for_local_use", False)
            is True
        )

    @property
    def flight_adbc_required_for_basic_local_use(self) -> bool:
        """Whether Flight/ADBC is required for basic local use."""

        return (
            self.envelope.field_bool("flight_adbc_required_for_basic_local_use", False)
            is True
        )

    @property
    def flight_ticket_requested(self) -> bool:
        """Whether a Flight ticket was requested."""

        return self.envelope.field_bool("flight_ticket_requested", False) is True

    @property
    def flight_ticket_supported(self) -> bool:
        """Whether Flight ticket delivery is currently supported."""

        return self.envelope.field_bool("flight_ticket_supported", False) is True

    @property
    def adbc_endpoint_requested(self) -> bool:
        """Whether an ADBC endpoint was requested."""

        return self.envelope.field_bool("adbc_endpoint_requested", False) is True

    @property
    def adbc_endpoint_supported(self) -> bool:
        """Whether ADBC endpoint delivery is currently supported."""

        return self.envelope.field_bool("adbc_endpoint_supported", False) is True

    @property
    def decoded_columnar_boundary_declared(self) -> bool:
        """Whether decoded-columnar boundaries are explicitly declared."""

        return self.envelope.field_bool("decoded_columnar_boundary_declared", False) is True

    @property
    def materialization_declared(self) -> bool:
        """Whether transfer materialization is declared."""

        return self.envelope.field_bool("materialization_declared", False) is True

    @property
    def result_policy_declared(self) -> bool:
        """Whether result policy is declared."""

        return self.envelope.field_bool("result_policy_declared", False) is True

    @property
    def standards_matrix_count(self) -> int:
        """Return the standards matrix row count."""

        return self.envelope.field_int("standards_matrix_count", 0) or 0

    @property
    def flight_server_started(self) -> bool:
        """Whether a Flight server was started."""

        return self.envelope.field_bool("flight_server_started", False) is True

    @property
    def adbc_endpoint_opened(self) -> bool:
        """Whether an ADBC endpoint was opened."""

        return self.envelope.field_bool("adbc_endpoint_opened", False) is True

    @property
    def broker_io(self) -> bool:
        """Whether broker I/O was performed."""

        return self.envelope.field_bool("broker_io", False) is True

    @property
    def object_store_io(self) -> bool:
        """Whether object-store I/O was performed."""

        return self.envelope.field_bool("object_store_io", False) is True

    @property
    def catalog_probe(self) -> bool:
        """Whether catalog probing was performed."""

        return self.envelope.field_bool("catalog_probe", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether data-plane handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class LiveChangeContractPlan:
    """Typed convenience view over the CG-22 live change contract."""

    envelope: OutputEnvelope

    @property
    def change_record_fields(self) -> tuple[str, ...]:
        """Return required `ChangeRecord` field names in contract order."""

        value = self.envelope.field("change_record_field_order", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def operations(self) -> tuple[str, ...]:
        """Return supported fixture change operations."""

        value = self.envelope.field("change_operation_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def fixture_operators(self) -> tuple[str, ...]:
        """Return supported in-memory live fixture operators."""

        value = self.envelope.field("fixture_operator_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def fallback_attempted(self) -> bool:
        """Whether the contract plan reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether the contract plan executed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True


@dataclass(frozen=True, slots=True)
class LiveFixtureRunReport:
    """Typed convenience view over the CG-22 in-memory live fixture run."""

    envelope: OutputEnvelope

    @property
    def operator(self) -> str | None:
        """Return the executed fixture operator."""

        return self.envelope.field("fixture_operator")

    @property
    def input_change_record_count(self) -> int:
        """Return the number of deterministic fixture change records."""

        return self.envelope.field_int("input_change_record_count", 0) or 0

    @property
    def active_state_key_count(self) -> int:
        """Return the active state key count after applying changes."""

        return self.envelope.field_int("active_state_key_count", 0) or 0

    @property
    def output_row_count(self) -> int:
        """Return the emitted fixture output row count."""

        return self.envelope.field_int("output_row_count", 0) or 0

    @property
    def output_rows(self) -> tuple[str, ...]:
        """Return deterministic output rows from the fixture report."""

        value = self.envelope.field("output_rows", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split("|") if part.strip())

    @property
    def all_certified(self) -> bool:
        """Whether live, execution, and Native I/O certificates are all certified."""
        from ._client_report_views import LiveFixtureRunReport_all_certified as _impl
        return _impl(self)

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def data_read(self) -> bool:
        """Whether the fixture command read external data."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the fixture command wrote output or checkpoints."""

        return self.envelope.field_bool("write_io", False) is True


@dataclass(frozen=True, slots=True)
class HybridOverlayRunReport:
    """Typed convenience view over the CG-22 hybrid overlay fixture run."""

    envelope: OutputEnvelope

    @property
    def operator(self) -> str | None:
        """Return the executed fixture operator."""

        return self.envelope.field("fixture_operator")

    @property
    def base_row_count(self) -> int:
        """Return the declared local Vortex base row count."""

        return self.envelope.field_int("base_row_count", 0) or 0

    @property
    def hot_change_record_count(self) -> int:
        """Return the hot delta change-record count."""

        return self.envelope.field_int("hot_change_record_count", 0) or 0

    @property
    def merged_row_count(self) -> int:
        """Return the merged base-plus-hot row count."""

        return self.envelope.field_int("merged_row_count", 0) or 0

    @property
    def output_rows(self) -> tuple[str, ...]:
        """Return deterministic output rows from the hybrid fixture report."""

        value = self.envelope.field("output_rows", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split("|") if part.strip())

    @property
    def all_certified(self) -> bool:
        """Whether overlay, flush, freshness, execution, and Native I/O evidence is certified."""
        from ._client_report_views import HybridOverlayRunReport_all_certified as _impl
        return _impl(self)

    @property
    def layout_health_status(self) -> str | None:
        """Return the bundled layout-health status."""

        return self.envelope.field("layout_health_bundle_status")

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def data_read(self) -> bool:
        """Whether the fixture command read external data."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the fixture command wrote output or checkpoints."""

        return self.envelope.field_bool("write_io", False) is True


@dataclass(frozen=True, slots=True)
class LiveHybridStateTransitionReport:
    """Typed view over the bounded live/hybrid state-transition fixture."""

    envelope: OutputEnvelope

    @property
    def selected_engine_mode(self) -> str | None:
        """Return the selected fixture engine mode."""

        return self.envelope.field("selected_engine_mode")

    @property
    def transition_kind(self) -> str | None:
        """Return the state-transition fixture kind."""

        return self.envelope.field("transition_kind")

    @property
    def snapshot_epoch(self) -> int:
        """Return the deterministic target snapshot epoch."""

        return self.envelope.field_int("snapshot_epoch", 0) or 0

    @property
    def attempt_count(self) -> int:
        """Return the number of simulated attempts."""

        return self.envelope.field_int("attempt_count", 0) or 0

    @property
    def attempt_outcomes(self) -> tuple[str, ...]:
        """Return attempt outcomes in deterministic order."""

        return _csv_values(self.envelope.field("attempt_outcome_order"))

    @property
    def all_certified(self) -> bool:
        """Whether freshness, state, and transition evidence is certified."""

        return all(
            self.envelope.field(key) == "certified"
            for key in (
                "freshness_certificate_status",
                "state_certificate_status",
                "state_transition_certificate_status",
            )
        )

    @property
    def cleanup_completed(self) -> bool:
        """Whether cooperative cancellation cleanup completed."""

        return self.envelope.field_bool("cancellation_cleanup_completed", False) is True

    @property
    def partial_output_committed(self) -> bool:
        """Whether the cancelled attempt committed partial output."""

        return self.envelope.field_bool("partial_output_committed", False) is True

    @property
    def durable_checkpoint_store_used(self) -> bool:
        """Whether a durable checkpoint store was used."""

        return self.envelope.field_bool("durable_checkpoint_store_used", False) is True

    @property
    def exactly_once_claim_allowed(self) -> bool:
        """Whether the fixture authorizes an exactly-once claim."""

        return self.envelope.field_bool("exactly_once_claim_allowed", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class LiveHybridDurableCheckpointReport:
    """Typed view over the bounded live/hybrid local checkpoint fixture."""

    envelope: OutputEnvelope

    @property
    def selected_engine_mode(self) -> str | None:
        """Return the selected fixture engine mode."""

        return self.envelope.field("selected_engine_mode")

    @property
    def checkpoint_store_kind(self) -> str | None:
        """Return the checkpoint store kind used by the fixture."""

        return self.envelope.field("checkpoint_store_kind")

    @property
    def checkpoint_dir(self) -> str | None:
        """Return the caller-provided checkpoint directory."""

        return self.envelope.field("checkpoint_dir")

    @property
    def checkpoint_path(self) -> str | None:
        """Return the checkpoint file path."""

        return self.envelope.field("checkpoint_path")

    @property
    def changelog_path(self) -> str | None:
        """Return the changelog file path."""

        return self.envelope.field("changelog_path")

    @property
    def state_store_path(self) -> str | None:
        """Return the durable local state-store file path."""

        return self.envelope.field("state_store_path")

    @property
    def micro_segment_path(self) -> str | None:
        """Return the local Vortex microsegment manifest path."""

        return self.envelope.field("micro_segment_path")

    @property
    def cold_vortex_segment_manifest_path(self) -> str | None:
        """Return the local cold-promotion manifest path."""

        return self.envelope.field("cold_vortex_segment_manifest_path")

    @property
    def partial_checkpoint_path(self) -> str | None:
        """Return the local partial-checkpoint recovery artifact path."""

        return self.envelope.field("partial_checkpoint_path")

    @property
    def input_change_record_count(self) -> int:
        """Return the number of fixture change records."""

        return self.envelope.field_int("input_change_record_count", 0) or 0

    @property
    def active_state_key_count(self) -> int:
        """Return the active state key count."""

        return self.envelope.field_int("active_state_key_count", 0) or 0

    @property
    def checkpoint_record_count(self) -> int:
        """Return the checkpoint record count."""

        return self.envelope.field_int("checkpoint_record_count", 0) or 0

    @property
    def restored_active_state_key_count(self) -> int:
        """Return the restored active state key count."""

        return self.envelope.field_int("restored_active_state_key_count", 0) or 0

    @property
    def micro_segment_record_count(self) -> int:
        """Return the local microsegment manifest record count."""

        return self.envelope.field_int("micro_segment_record_count", 0) or 0

    @property
    def micro_segment_delete_vector_entry_count(self) -> int:
        """Return the delete-vector-shaped entry count."""

        return self.envelope.field_int("micro_segment_delete_vector_entry_count", 0) or 0

    @property
    def micro_segment_tombstone_count(self) -> int:
        """Return the tombstone count recorded in the microsegment manifest."""

        return self.envelope.field_int("micro_segment_tombstone_count", 0) or 0

    @property
    def state_match(self) -> bool:
        """Whether checkpoint digest and restored key count matched."""

        return self.envelope.field_bool("state_match", False) is True

    @property
    def durable_checkpoint_store_used(self) -> bool:
        """Whether the local durable checkpoint store was used."""

        return self.envelope.field_bool("durable_checkpoint_store_used", False) is True

    @property
    def durable_checkpoint_write_performed(self) -> bool:
        """Whether the checkpoint write was performed."""

        return self.envelope.field_bool("durable_checkpoint_write_performed", False) is True

    @property
    def durable_checkpoint_restore_performed(self) -> bool:
        """Whether the checkpoint restore/readback was performed."""

        return self.envelope.field_bool("durable_checkpoint_restore_performed", False) is True

    @property
    def durable_changelog_write_performed(self) -> bool:
        """Whether the changelog write was performed."""

        return self.envelope.field_bool("durable_changelog_write_performed", False) is True

    @property
    def durable_state_store_used(self) -> bool:
        """Whether the local durable state store was used."""

        return self.envelope.field_bool("durable_state_store_used", False) is True

    @property
    def durable_state_store_write_performed(self) -> bool:
        """Whether the local durable state-store write was performed."""

        return self.envelope.field_bool("durable_state_store_write_performed", False) is True

    @property
    def durable_state_store_restore_performed(self) -> bool:
        """Whether the local durable state-store readback was performed."""

        return self.envelope.field_bool("durable_state_store_restore_performed", False) is True

    @property
    def micro_segment_persistence_performed(self) -> bool:
        """Whether the local microsegment manifest was persisted."""

        return self.envelope.field_bool("micro_segment_persistence_performed", False) is True

    @property
    def micro_segment_restore_performed(self) -> bool:
        """Whether the local microsegment manifest was read back."""

        return self.envelope.field_bool("micro_segment_restore_performed", False) is True

    @property
    def cold_vortex_segment_promotion_performed(self) -> bool:
        """Whether the cold-promotion manifest was persisted."""

        return self.envelope.field_bool("cold_vortex_segment_promotion_performed", False) is True

    @property
    def cold_vortex_segment_manifest_restore_performed(self) -> bool:
        """Whether the cold-promotion manifest was read back."""

        return (
            self.envelope.field_bool(
                "cold_vortex_segment_manifest_restore_performed", False
            )
            is True
        )

    @property
    def restart_restore_performed(self) -> bool:
        """Whether local restart restore evidence was emitted."""

        return self.envelope.field_bool("restart_restore_performed", False) is True

    @property
    def partial_checkpoint_detected(self) -> bool:
        """Whether a partial checkpoint was detected."""

        return self.envelope.field_bool("partial_checkpoint_detected", False) is True

    @property
    def partial_checkpoint_committed(self) -> bool:
        """Whether the partial checkpoint was committed."""

        return self.envelope.field_bool("partial_checkpoint_committed", False) is True

    @property
    def partial_checkpoint_cleanup_completed(self) -> bool:
        """Whether partial checkpoint cleanup completed."""

        return self.envelope.field_bool("partial_checkpoint_cleanup_completed", False) is True

    @property
    def duplicate_replay_protection_performed(self) -> bool:
        """Whether duplicate replay protection evidence was emitted."""

        return self.envelope.field_bool("duplicate_replay_protection_performed", False) is True

    @property
    def state_restore_status(self) -> str | None:
        """Return the deterministic restore status."""

        return self.envelope.field("state_restore_status")

    @property
    def restart_restore_status(self) -> str | None:
        """Return the deterministic restart-restore status."""

        return self.envelope.field("restart_restore_status")

    @property
    def duplicate_replay_protection_status(self) -> str | None:
        """Return the duplicate replay protection status."""

        return self.envelope.field("duplicate_replay_protection_status")

    @property
    def retry_idempotency_key(self) -> str | None:
        """Return the retry idempotency key."""

        return self.envelope.field("retry_idempotency_key")

    @property
    def vortex_micro_segment_persistence_status(self) -> str | None:
        """Return the local microsegment persistence status."""

        return self.envelope.field("vortex_micro_segment_persistence_status")

    @property
    def cold_vortex_segment_promotion_status(self) -> str | None:
        """Return the local cold-promotion status."""

        return self.envelope.field("cold_vortex_segment_promotion_status")

    @property
    def upstream_vortex_file_write_performed(self) -> bool:
        """Whether an upstream Vortex file write was performed."""

        return self.envelope.field_bool("upstream_vortex_file_write_performed", False) is True

    @property
    def vortex_micro_segment_manifest_only(self) -> bool:
        """Whether the microsegment artifact is a manifest-only fixture."""

        return self.envelope.field_bool("vortex_micro_segment_manifest_only", False) is True

    @property
    def cold_vortex_promotion_manifest_only(self) -> bool:
        """Whether the cold-promotion artifact is a manifest-only fixture."""

        return self.envelope.field_bool("cold_vortex_promotion_manifest_only", False) is True

    @property
    def all_certified(self) -> bool:
        """Whether freshness, state, execution, and Native I/O evidence is certified."""
        from ._client_report_views import LiveHybridDurableCheckpointReport_all_certified as _impl
        return _impl(self)

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def write_io(self) -> bool:
        """Whether this fixture wrote local checkpoint/changelog files."""

        return self.envelope.field_bool("write_io", False) is True

    @property
    def object_store_io(self) -> bool:
        """Whether this fixture touched object storage."""

        return self.envelope.field_bool("object_store_io", False) is True

    @property
    def exactly_once_claim_allowed(self) -> bool:
        """Whether the fixture authorizes an exactly-once claim."""

        return self.envelope.field_bool("exactly_once_claim_allowed", False) is True

    @property
    def broker_replay_supported(self) -> bool:
        """Whether broker replay is supported by this fixture."""

        return self.envelope.field_bool("broker_replay_supported", False) is True

    @property
    def production_claim_allowed(self) -> bool:
        """Whether the fixture authorizes a production live/hybrid claim."""

        return self.envelope.field_bool("production_claim_allowed", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class LocalDistributedFixtureRunReport:
    """Typed view over the scoped local distributed fixture run."""

    envelope: OutputEnvelope

    @property
    def distributed_runtime_status(self) -> str | None:
        """Return the distributed runtime support status."""

        return self.envelope.field("distributed_runtime_status")

    @property
    def distributed_claim_gate_status(self) -> str | None:
        """Return the distributed claim-gate status."""

        return self.envelope.field("distributed_claim_gate_status")

    @property
    def worker_count(self) -> int:
        """Return the requested local worker-slot count."""

        return self.envelope.field_int("worker_count", 0) or 0

    @property
    def split_unit_count(self) -> int:
        """Return the number of capillary split units executed."""

        return self.envelope.field_int("split_unit_count", 0) or 0

    @property
    def task_attempt_count(self) -> int:
        """Return task-attempt evidence count."""

        return self.envelope.field_int("task_attempt_count", 0) or 0

    @property
    def task_attempt_outcomes(self) -> tuple[str, ...]:
        """Return task-attempt outcomes in deterministic order."""

        return _csv_values(self.envelope.field("task_attempt_outcome_order"))

    @property
    def result_fragment_count(self) -> int:
        """Return result-fragment evidence count."""

        return self.envelope.field_int("result_fragment_count", 0) or 0

    @property
    def merged_row_count(self) -> int:
        """Return deterministic merged output row count."""

        return self.envelope.field_int("merged_row_count", 0) or 0

    @property
    def merged_rows(self) -> tuple[str, ...]:
        """Return deterministic merged rows."""

        value = self.envelope.field("merged_rows", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split("|") if part.strip())

    @property
    def merge_digest(self) -> str | None:
        """Return the deterministic merged-output digest."""

        return self.envelope.field("merge_digest")

    @property
    def retry_performed(self) -> bool:
        """Whether the fixture performed a retry."""

        return self.envelope.field_bool("retry_performed", False) is True

    @property
    def duplicate_attempt_rejected(self) -> bool:
        """Whether duplicate task attempts were rejected."""

        return self.envelope.field_bool("duplicate_attempt_rejected", False) is True

    @property
    def stale_lease_rejected(self) -> bool:
        """Whether stale worker leases were rejected."""

        return self.envelope.field_bool("stale_lease_rejected", False) is True

    @property
    def cancellation_cleanup_completed(self) -> bool:
        """Whether cancellation cleanup completed."""

        return self.envelope.field_bool("cancellation_cleanup_completed", False) is True

    @property
    def partial_output_committed(self) -> bool:
        """Whether partial output was committed."""

        return self.envelope.field_bool("partial_output_committed", False) is True

    @property
    def repartition_performed(self) -> bool:
        """Whether scoped local repartition ran."""

        return self.envelope.field_bool("repartition_performed", False) is True

    @property
    def remote_shuffle_performed(self) -> bool:
        """Whether remote shuffle ran."""

        return self.envelope.field_bool("remote_shuffle_performed", False) is True

    @property
    def skew_detected(self) -> bool:
        """Whether fixture skew detection identified a skewed key."""

        return self.envelope.field_bool("skew_detected", False) is True

    @property
    def memory_budget_exceeded(self) -> bool:
        """Whether the scoped memory budget was exceeded."""

        return self.envelope.field_bool("memory_budget_exceeded", False) is True

    @property
    def spill_required(self) -> bool:
        """Whether spill was required."""

        return self.envelope.field_bool("spill_required", False) is True

    @property
    def all_certified(self) -> bool:
        """Whether execution and Native I/O certificates are certified."""

        return all(
            self.envelope.field(key) == "certified"
            for key in ("execution_certificate_status", "native_io_certificate_status")
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether the explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def production_claim_allowed(self) -> bool:
        """Whether the fixture authorizes a production distributed claim."""

        return self.envelope.field_bool("production_claim_allowed", False) is True

    @property
    def distributed_performance_claim_allowed(self) -> bool:
        """Whether the fixture authorizes a distributed performance claim."""

        return self.envelope.field_bool("distributed_performance_claim_allowed", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether an external engine was invoked."""

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class PythonClientSmokeReport:
    """No-dataset Python client smoke-check envelopes."""

    binary_command: tuple[str, ...]
    python_package_version: str
    platform: str
    status: OutputEnvelope
    python_capabilities: OutputEnvelope
    deployment_capabilities: OutputEnvelope
    input_adapters: OutputEnvelope

    @property
    def fallback_attempted(self) -> bool:
        """Whether any smoke-check command reported attempted fallback execution."""

        return (
            self.status.fallback.attempted
            or self.python_capabilities.fallback.attempted
            or self.deployment_capabilities.fallback.attempted
            or self.input_adapters.fallback.attempted
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether any smoke-check command reported external engine execution."""

        return (
            _envelope_external_engine_invoked(self.status)
            or _envelope_external_engine_invoked(self.python_capabilities)
            or _envelope_external_engine_invoked(self.deployment_capabilities)
            or _envelope_external_engine_invoked(self.input_adapters)
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return the commands executed by the smoke check."""

        return (
            self.status.command,
            self.python_capabilities.command,
            self.deployment_capabilities.command,
            self.input_adapters.command,
        )

    @property
    def protocol_version(self) -> str:
        """Return the parsed ShardLoom output protocol version."""

        return self.status.schema_version

    @property
    def cli_version(self) -> str | None:
        """Return the CLI version field when the status surface exposes it."""

        return self.status.field("cli_binary_version") or self.status.field("version")

    @property
    def resolved_cli_path(self) -> str | None:
        """Return the resolved CLI executable path or command head."""

        return self.binary_command[0] if self.binary_command else None

    @property
    def feature_gates(self) -> tuple[str, ...]:
        """Return feature-gate-like runtime fields exposed by smoke envelopes."""
        from ._client_report_views import PythonClientSmokeReport_feature_gates as _impl
        return _impl(self)


class ShardLoomClient:
    """Thin client that invokes the ShardLoom CLI with `--format json`.

    The client does not inspect datasets, probe catalogs, load external engines,
    or provide fallback execution. It only runs explicit CLI commands requested
    by the caller and parses the resulting JSON envelope.
    """

    def __init__(
        self,
        binary: Binary | None = None,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        repo_root: str | os.PathLike[str] | None = None,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        timeout: float | None = None,
        use_persistent_worker: bool | None = None,
        transport: str = "subprocess",
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        resource_limits: ExecutionResourceLimits | None = None,
        float_policy: str = "strict",
        recursion_limit: int = 1000,
    ) -> None:
        from ._client_session import ShardLoomClient___init__ as _impl
        return _impl(self, binary, env=env, cwd=cwd, repo_root=repo_root, profile_order=profile_order, timeout=timeout, use_persistent_worker=use_persistent_worker, transport=transport, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, resource_limits=resource_limits, float_policy=float_policy, recursion_limit=recursion_limit)

    @property
    def resources(self) -> ExecutionResources | None:
        """Return this client's explicit allocation, independent of its transport."""

        return self._resources

    @property
    def resource_limits(self) -> ExecutionResourceLimits | None:
        """Return explicit ceilings, including when no job grant is configured."""

        return self._resource_limits

    @property
    def float_policy(self) -> str:
        """Return this client's immutable floating semantics policy."""

        return self._float_policy

    @property
    def recursion_limit(self) -> int:
        """Return this client's immutable recursive-term evaluation bound."""
        return self._recursion_limit

    @property
    def transport(self) -> str:
        """Return the explicitly selected execution transport."""

        return self._transport

    @property
    def closed(self) -> bool:
        return self._native_closed

    def _ensure_open(self) -> None:
        from .errors import ShardLoomClosedError
        if self._native_closed:
            raise ShardLoomClosedError()

    def _native_backend_for(self, allocation: ExecutionResources | None = None):
        from ._client_session import ShardLoomClient__native_backend_for as _impl
        return _impl(self, allocation)

    @classmethod
    def from_repo(
        cls,
        repo_root: str | os.PathLike[str] | None = None,
        *,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        **kwargs: Any,
    ) -> "ShardLoomClient":
        """Create a client that resolves `target/<profile>/shardloom` lazily.

        This is intended for source-tree development and local ETL testing. It
        does not run commands or probe anything at import time.
        """

        root = Path.cwd() if repo_root is None else Path(repo_root)
        return cls(repo_root=root, profile_order=profile_order, **kwargs)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        profile_order: Sequence[str] | None = None,
        **kwargs: object,
    ) -> "ShardLoomClient":
        """Create a client from ShardLoom Python environment variables.

        Supported variables:
        `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, `SHARDLOOM_PROFILE_ORDER`, and
        `SHARDLOOM_TIMEOUT_SECONDS`. The method only reads configuration; it
        does not run the CLI or inspect datasets.
        """
        from ._client_session import ShardLoomClient_from_env as _impl
        return _impl(cls, env, profile_order=profile_order, **kwargs)

    def status(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI status envelope."""

        return self.run(["status"], check=check)

    def doctor(self, *, check: bool = True) -> OutputEnvelope:
        """Return the side-effect-free v1 doctor envelope."""

        return self.run(["doctor"], check=check)

    def support_bundle(
        self,
        *,
        note: str | None = None,
        include_defaults: bool = True,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a redacted support-bundle envelope without writing files."""

        args: list[CommandPart] = ["support-bundle"]
        if note is not None:
            args.extend(["--note", note])
        if include_defaults:
            args.append("--include-defaults")
        return self.run(args, check=check)

    def runs_today(self, *, check: bool = True) -> RunsTodaySupportMatrix:
        """Return the generated current-support matrix."""

        return RunsTodaySupportMatrix(self.run(["runs-today"], check=check))

    def command_metadata(
        self, command: str | None = None, *, check: bool = True
    ) -> CommandMetadataReport:
        """Return side-effect-free CLI command registry metadata."""

        args = ["command-metadata"]
        if command is not None:
            args.append(command)
        return CommandMetadataReport(self.run(args, check=check))

    def evidence_schema(
        self, surface: str | None = None, *, check: bool = True
    ) -> EvidenceSchemaRegistryReport:
        """Return side-effect-free evidence field schema registry metadata."""

        args = ["evidence-schema"]
        if surface is not None:
            args.append(surface)
        return EvidenceSchemaRegistryReport(self.run(args, check=check))

    def api_compat_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI/API JSON compatibility plan envelope."""

        return self.run(["api-compat-plan"], check=check)

    def rest_api_contract_plan(self, *, check: bool = True) -> RestApiContractPlan:
        """Return the CG-23 REST/OpenAPI contract plan envelope."""

        return RestApiContractPlan(self.run(["rest-api-contract-plan"], check=check))

    def serve_discovery_contract(
        self,
        *,
        bind: str = "127.0.0.1:8787",
        check: bool = True,
    ) -> RestApiDiscoveryContract:
        """Return `serve --mode discovery` contract output without starting a server."""

        return RestApiDiscoveryContract(
            self.run(["serve", "--mode", "discovery", "--bind", bind], check=check)
        )

    def rest_api_plan_preview(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiPlanPreview:
        """Return a CG-23 plan/explain/validate/certification preview envelope."""

        return RestApiPlanPreview(
            self.run(["rest-api-plan-preview", scenario], check=check)
        )

    def rest_api_local_lifecycle(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiLocalLifecycle:
        """Return a CG-23 certified local lifecycle/result delivery envelope."""

        return RestApiLocalLifecycle(
            self.run(["rest-api-local-lifecycle", scenario], check=check)
        )

    def rest_api_event_stream(
        self,
        scenario: str = "certified-live-fixture",
        *,
        check: bool = True,
    ) -> RestApiEventStream:
        """Return a CG-23 live/hybrid event stream contract envelope."""

        return RestApiEventStream(
            self.run(["rest-api-event-stream", scenario], check=check)
        )

    def rest_api_security_governance(
        self,
        scenario: str = "safe-local-default",
        *,
        check: bool = True,
    ) -> RestApiSecurityGovernance:
        """Return a CG-23 security/governance/agent contract envelope."""

        return RestApiSecurityGovernance(
            self.run(["rest-api-security-governance", scenario], check=check)
        )

    def rest_api_data_plane(
        self,
        scenario: str = "artifact-reference-default",
        *,
        check: bool = True,
    ) -> RestApiDataPlane:
        """Return a CG-23 data-plane/standards boundary contract envelope."""

        return RestApiDataPlane(self.run(["rest-api-data-plane", scenario], check=check))

    def python_wrapper_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the Python wrapper foundation plan envelope."""

        return self.run(["python-wrapper-plan"], check=check)

    def capabilities(self, scope: str | None = None, *, check: bool = True) -> OutputEnvelope:
        """Return a capability-discovery envelope for the optional scope."""

        args = ["capabilities"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def public_workflow_route(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        source_schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None = None,
        source_bindings: Mapping[str, Mapping[str, object]] | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "vortex_middle",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        fanout_outputs: FanoutOutputs | None = None,
        native_vortex_operation_family: str | None = None,
        native_vortex_right_input: str | os.PathLike[str] | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        vortex_sample_seed: int | None = None,
        vortex_sample_fraction: float | None = None,
        vortex_sample_replacement: bool = False,
        vortex_sample_weight_column: str | None = None,
        vortex_duplicate_keep: str | None = None,
        vortex_deduplicate_key_columns: str | Sequence[str] | None = None,
        vortex_expression_projection: str | None = None,
        vortex_melt_projection: str | None = None,
        vortex_explode_projection: str | None = None,
        vortex_pivot_projection: str | None = None,
        vortex_rolling_window: str | None = None,
        vortex_sort_rows: str | None = None,
        vortex_aggregate: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> PublicWorkflowRoute:
        """Return the side-effect-free public route envelope for a declared workflow."""
        from ._client_workflow import ShardLoomClient_public_workflow_route as _impl
        return _impl(self, surface, input_uri=input_uri, input_format=input_format, source_schema=source_schema, source_bindings=source_bindings, sql_statement=sql_statement, plan_summary=plan_summary, requested_output=requested_output, output_ref=output_ref, execution_policy=execution_policy, materialization_policy=materialization_policy, evidence_level=evidence_level, bounded=bounded, fanout_outputs=fanout_outputs, native_vortex_operation_family=native_vortex_operation_family, native_vortex_right_input=native_vortex_right_input, vortex_primitive=vortex_primitive, vortex_predicate=vortex_predicate, vortex_columns=vortex_columns, vortex_source_order_limit=vortex_source_order_limit, vortex_sample_seed=vortex_sample_seed, vortex_sample_fraction=vortex_sample_fraction, vortex_sample_replacement=vortex_sample_replacement, vortex_sample_weight_column=vortex_sample_weight_column, vortex_duplicate_keep=vortex_duplicate_keep, vortex_deduplicate_key_columns=vortex_deduplicate_key_columns, vortex_expression_projection=vortex_expression_projection, vortex_melt_projection=vortex_melt_projection, vortex_explode_projection=vortex_explode_projection, vortex_pivot_projection=vortex_pivot_projection, vortex_rolling_window=vortex_rolling_window, vortex_sort_rows=vortex_sort_rows, vortex_aggregate=vortex_aggregate, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def public_workflow_run(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        source_schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None = None,
        source_bindings: Mapping[str, Mapping[str, object]] | None = None,
        input_batches: Mapping[str, object] | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "vortex_middle",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        allow_overwrite: bool = False,
        fanout_outputs: FanoutOutputs | None = None,
        native_vortex_operation_family: str | None = None,
        native_vortex_right_input: str | os.PathLike[str] | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        vortex_sample_seed: int | None = None,
        vortex_sample_fraction: float | None = None,
        vortex_sample_replacement: bool = False,
        vortex_sample_weight_column: str | None = None,
        vortex_duplicate_keep: str | None = None,
        vortex_deduplicate_key_columns: str | Sequence[str] | None = None,
        vortex_expression_projection: str | None = None,
        vortex_melt_projection: str | None = None,
        vortex_explode_projection: str | None = None,
        vortex_pivot_projection: str | None = None,
        vortex_rolling_window: str | None = None,
        vortex_sort_rows: str | None = None,
        vortex_aggregate: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
        _native_plan: object | None = None,
        _native_frame: object | None = None,
    ) -> PublicWorkflowExecution:
        """Run an admitted public workflow through the shared route facade."""
        from ._client_workflow import ShardLoomClient_public_workflow_run as _impl
        return _impl(self, surface, input_uri=input_uri, input_format=input_format, source_schema=source_schema, source_bindings=source_bindings, input_batches=input_batches, sql_statement=sql_statement, plan_summary=plan_summary, requested_output=requested_output, output_ref=output_ref, execution_policy=execution_policy, materialization_policy=materialization_policy, evidence_level=evidence_level, bounded=bounded, allow_overwrite=allow_overwrite, fanout_outputs=fanout_outputs, native_vortex_operation_family=native_vortex_operation_family, native_vortex_right_input=native_vortex_right_input, vortex_primitive=vortex_primitive, vortex_predicate=vortex_predicate, vortex_columns=vortex_columns, vortex_source_order_limit=vortex_source_order_limit, vortex_sample_seed=vortex_sample_seed, vortex_sample_fraction=vortex_sample_fraction, vortex_sample_replacement=vortex_sample_replacement, vortex_sample_weight_column=vortex_sample_weight_column, vortex_duplicate_keep=vortex_duplicate_keep, vortex_deduplicate_key_columns=vortex_deduplicate_key_columns, vortex_expression_projection=vortex_expression_projection, vortex_melt_projection=vortex_melt_projection, vortex_explode_projection=vortex_explode_projection, vortex_pivot_projection=vortex_pivot_projection, vortex_rolling_window=vortex_rolling_window, vortex_sort_rows=vortex_sort_rows, vortex_aggregate=vortex_aggregate, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check, _native_plan=_native_plan, _native_frame=_native_frame)

    def public_workflow_batches(
        self, surface: str, *, batch_rows: int = 2048,
        input_batches: Mapping[str, object] | None = None,
        output_window_bytes: int = 0, **kwargs: Any,
    ):
        """Iterate native result batches; use a context manager for early close."""
        from ._client_workflow import ShardLoomClient_public_workflow_batches as _impl
        return _impl(self, surface, batch_rows=batch_rows, input_batches=input_batches, output_window_bytes=output_window_bytes, **kwargs)

    def public_workflow_prepare(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str],
        output_ref: str | os.PathLike[str],
        input_format: str | None = None,
        source_schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None = None,
        plan_summary: str | None = None,
        evidence_level: str = "runtime_smoke",
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Prepare an admitted public workflow input through the shared route facade."""
        from ._client_workflow import ShardLoomClient_public_workflow_prepare as _impl
        return _impl(self, surface, input_uri=input_uri, output_ref=output_ref, input_format=input_format, source_schema=source_schema, plan_summary=plan_summary, evidence_level=evidence_level, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def _public_workflow_facade_args(
        self,
        command: str,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        source_schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None = None,
        source_bindings: Mapping[str, Mapping[str, object]] | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "vortex_middle",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        allow_overwrite: bool = False,
        fanout_outputs: FanoutOutputs | None = None,
        native_vortex_operation_family: str | None = None,
        native_vortex_right_input: str | os.PathLike[str] | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        vortex_sample_seed: int | None = None,
        vortex_sample_fraction: float | None = None,
        vortex_sample_replacement: bool = False,
        vortex_sample_weight_column: str | None = None,
        vortex_duplicate_keep: str | None = None,
        vortex_deduplicate_key_columns: str | Sequence[str] | None = None,
        vortex_expression_projection: str | None = None,
        vortex_melt_projection: str | None = None,
        vortex_explode_projection: str | None = None,
        vortex_pivot_projection: str | None = None,
        vortex_rolling_window: str | None = None,
        vortex_sort_rows: str | None = None,
        vortex_aggregate: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
    ) -> list[CommandPart]:
        from ._client_workflow import ShardLoomClient__public_workflow_facade_args as _impl
        return _impl(self, command, surface, input_uri=input_uri, input_format=input_format, source_schema=source_schema, source_bindings=source_bindings, sql_statement=sql_statement, plan_summary=plan_summary, requested_output=requested_output, output_ref=output_ref, execution_policy=execution_policy, materialization_policy=materialization_policy, evidence_level=evidence_level, bounded=bounded, allow_overwrite=allow_overwrite, fanout_outputs=fanout_outputs, native_vortex_operation_family=native_vortex_operation_family, native_vortex_right_input=native_vortex_right_input, vortex_primitive=vortex_primitive, vortex_predicate=vortex_predicate, vortex_columns=vortex_columns, vortex_source_order_limit=vortex_source_order_limit, vortex_sample_seed=vortex_sample_seed, vortex_sample_fraction=vortex_sample_fraction, vortex_sample_replacement=vortex_sample_replacement, vortex_sample_weight_column=vortex_sample_weight_column, vortex_duplicate_keep=vortex_duplicate_keep, vortex_deduplicate_key_columns=vortex_deduplicate_key_columns, vortex_expression_projection=vortex_expression_projection, vortex_melt_projection=vortex_melt_projection, vortex_explode_projection=vortex_explode_projection, vortex_pivot_projection=vortex_pivot_projection, vortex_rolling_window=vortex_rolling_window, vortex_sort_rows=vortex_sort_rows, vortex_aggregate=vortex_aggregate, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill)

    def engine_selection_plan(
        self,
        engine: str = "auto",
        *,
        boundedness: str = "snapshot",
        update_mode: str = "snapshot",
        output_mode: str = "snapshot",
        check: bool = True,
    ) -> EngineSelectionPlan:
        """Return the CG-22 report-only engine-selection plan."""
        from ._client_commands import ShardLoomClient_engine_selection_plan as _impl
        return _impl(self, engine, boundedness=boundedness, update_mode=update_mode, output_mode=output_mode, check=check)

    def engine_capability_matrix(self, *, check: bool = True) -> EngineCapabilityMatrix:
        """Return the CG-22 report-only per-engine capability matrix."""

        return EngineCapabilityMatrix(self.run(["engine-capability-matrix"], check=check))

    def workload_certification_dossier(
        self,
        scenario: str = "local-vortex-count",
        *,
        check: bool = True,
    ) -> WorkloadCertificationDossier:
        """Return a workload-scoped cross-CG certification dossier."""

        return WorkloadCertificationDossier(
            self.run(["workload-certification-dossier", scenario], check=check)
        )

    def claim_gate_closeout(self, *, check: bool = True) -> ClaimGateCloseoutReport:
        """Return the P7 claim-gate and release-readiness closeout report."""

        return ClaimGateCloseoutReport(self.run(["claim-gate-closeout"], check=check))

    def compute_capability_matrix(self, *, check: bool = True) -> ComputeCapabilityMatrix:
        """Return the P7.4 report-only compute capability coverage matrix."""

        return ComputeCapabilityMatrix(self.run(["compute-capability-matrix"], check=check))

    def semantic_conformance_suite(self, *, check: bool = True) -> SemanticConformanceSuite:
        """Return the P7.4 ShardLoomNative semantic conformance suite report."""

        return SemanticConformanceSuite(self.run(["semantic-conformance-suite"], check=check))

    def live_change_contract_plan(self, *, check: bool = True) -> LiveChangeContractPlan:
        """Return the CG-22 report-only live change contract."""

        return LiveChangeContractPlan(self.run(["live-change-contract-plan"], check=check))

    def live_fixture_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> LiveFixtureRunReport:
        """Run the explicit CG-22 in-memory live fixture command."""
        from ._client_commands import ShardLoomClient_live_fixture_run as _impl
        return _impl(self, operator, argument, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def hybrid_overlay_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> HybridOverlayRunReport:
        """Run the explicit CG-22 in-memory hybrid overlay fixture command."""
        from ._client_commands import ShardLoomClient_hybrid_overlay_run as _impl
        return _impl(self, operator, argument, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def live_hybrid_state_transition_smoke(
        self,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> LiveHybridStateTransitionReport:
        """Run the bounded CG-22 state-transition retry/cancel/cleanup fixture."""
        from ._client_commands import ShardLoomClient_live_hybrid_state_transition_smoke as _impl
        return _impl(self, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def live_hybrid_durable_checkpoint_smoke(
        self,
        checkpoint_dir: str | os.PathLike[str],
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> LiveHybridDurableCheckpointReport:
        """Run the bounded CG-22 local checkpoint/changelog fixture."""
        from ._client_commands import ShardLoomClient_live_hybrid_durable_checkpoint_smoke as _impl
        return _impl(self, checkpoint_dir, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def distributed_local_fixture_run(
        self,
        worker_count: int = 2,
        fault_mode: str = "none",
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> LocalDistributedFixtureRunReport:
        """Run the scoped local distributed fixture command."""
        from ._client_commands import ShardLoomClient_distributed_local_fixture_run as _impl
        return _impl(self, worker_count, fault_mode, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def explain(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only explain envelope for an operation summary."""

        return self.run(["explain", operation], check=check)

    def optimizer_plan(self, *, check: bool = True) -> EvidenceAwareOptimizerTraceReport:
        """Return the report-only evidence-aware optimizer trace."""

        return EvidenceAwareOptimizerTraceReport(
            self.run(["optimizer-plan"], check=check)
        )

    def session_cache_smoke(self, *, check: bool = True) -> SessionCacheSmokeReport:
        """Run the scoped CLI session-cache lifecycle smoke."""

        return SessionCacheSmokeReport(
            self.run(["session-cache-smoke"], check=check)
        )

    def estimate(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only estimate envelope for an operation summary."""

        return self.run(["estimate", operation], check=check)

    def workflow_unsupported_plan(
        self,
        operation: str,
        workflow_summary: str,
        target_ref: str | os.PathLike[str] | None = None,
        *,
        check: bool = False,
    ) -> OutputEnvelope:
        """Return a report-only unsupported workflow-operation envelope."""

        command = ["workflow-unsupported-plan", operation, workflow_summary]
        if target_ref is not None:
            command.append(str(target_ref))
        return self.run(command, check=check)

    def vortex_prepare(
        self,
        source_path: str | os.PathLike[str],
        target_vortex_path: str | os.PathLike[str],
        *,
        input_format: str | None = None,
        schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        delta_source_path: str | os.PathLike[str] | None = None,
        delta_target_vortex_path: str | os.PathLike[str] | None = None,
        delta_update_mode: str = "append-only",
        check: bool = True,
    ) -> VortexIngestSmokeReport:
        """Prepare local compatibility input as a Vortex artifact."""
        from ._client_commands import ShardLoomClient_vortex_prepare as _impl
        return _impl(self, source_path, target_vortex_path, input_format=input_format, schema=schema, allow_overwrite=allow_overwrite, certification_level=certification_level, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, delta_source_path=delta_source_path, delta_target_vortex_path=delta_target_vortex_path, delta_update_mode=delta_update_mode, check=check)

    def execution_certificate_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only execution certificate planning envelope."""

        return self.run(["execution-certificate-plan"], check=check)

    def native_io_envelope_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only Native I/O envelope planning envelope."""

        return self.run(["native-io-envelope-plan"], check=check)

    def vortex_run(
        self,
        dataset_uri: str | os.PathLike[str],
        primitive: str,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit `vortex-run` CLI command and parse its envelope."""
        from ._client_commands import ShardLoomClient_vortex_run as _impl
        return _impl(self, dataset_uri, primitive, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def vortex_count(
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        execute_local_encoded_count: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-count` with optional explicit local encoded execution."""
        from ._client_commands import ShardLoomClient_vortex_count as _impl
        return _impl(self, dataset_uri, execute_local_encoded_count=execute_local_encoded_count, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def vortex_count_where(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        *,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-count-where` with optional explicit local execution."""
        from ._client_commands import ShardLoomClient_vortex_count_where as _impl
        return _impl(self, dataset_uri, predicate, execute_local_primitive=execute_local_primitive, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def vortex_filter(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        *,
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter` with optional explicit local execution."""
        from ._client_commands import ShardLoomClient_vortex_filter as _impl
        return _impl(self, dataset_uri, predicate, source_order_limit=source_order_limit, execute_local_primitive=execute_local_primitive, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def vortex_project(
        self,
        dataset_uri: str | os.PathLike[str],
        columns: str | Sequence[str],
        *,
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-project` with optional explicit local execution."""
        from ._client_commands import ShardLoomClient_vortex_project as _impl
        return _impl(self, dataset_uri, columns, source_order_limit=source_order_limit, execute_local_primitive=execute_local_primitive, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def vortex_filter_project(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        columns: str | Sequence[str],
        *,
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter-project` with optional explicit local execution."""
        from ._client_commands import ShardLoomClient_vortex_filter_project as _impl
        return _impl(self, dataset_uri, predicate, columns, source_order_limit=source_order_limit, execute_local_primitive=execute_local_primitive, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)

    def local_vortex_primitive_smoke(
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        predicate: str = "gte:value:3",
        columns: str | Sequence[str] = ("metric",),
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> LocalVortexPrimitiveSmokeReport:
        """Run the certified local Vortex primitive workflow through explicit CLI flags."""
        from ._client_commands import ShardLoomClient_local_vortex_primitive_smoke as _impl
        return _impl(self, dataset_uri, predicate=predicate, columns=columns, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, check=check)










    def dynamic_work_shaping_plan(
        self, profile: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the advisory dynamic work-shaping plan for an optional profile."""

        args = ["dynamic-work-shaping-plan"]
        if profile is not None:
            args.append(profile)
        return self.run(args, check=check)

    def sizing_feedback_plan(
        self,
        memory_gb: int,
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return the advisory dynamic sizing feedback plan."""

        if isinstance(signals, str):
            signals_text = signals
        else:
            signals_text = ",".join(signals)
        return self.run(
            ["sizing-feedback-plan", str(memory_gb), signals_text],
            check=check,
        )

    def benchmark_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the benchmark plan for the optional scope."""

        args = ["benchmark-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def benchmark_constitution(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return benchmark constitution validation for the optional scope."""

        args = ["benchmark-constitution"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def benchmark_claim_evidence_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return benchmark claim-evidence planning for the optional scope."""

        args = ["benchmark-claim-evidence-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def world_class_sufficiency_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the current CG-20 world-class sufficiency evidence envelope."""

        return self.run(["world-class-sufficiency-plan"], check=check)

    def translation_plan(
        self, target_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a target translation or compatibility-output planning envelope."""

        return self.run(["translation-plan", str(target_uri)], check=check)

    def plan_import(
        self,
        format_kind: str,
        source_label: str,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a plan import portability envelope for the requested format."""

        return self.run(["plan-import", format_kind, source_label], check=check)

    def plan_export(self, format_kind: str = "native", *, check: bool = True) -> OutputEnvelope:
        """Return a plan export portability envelope for the requested format."""

        return self.run(["plan-export", format_kind], check=check)

    def vortex_output_plan(
        self, target_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a Vortex output target preview envelope."""

        return self.run(["vortex-output-plan", str(target_uri)], check=check)

    def vortex_write_intent_plan(
        self,
        target_uri: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex write-intent readiness without writing output data."""

        return self.run(
            ["vortex-write-intent-plan", str(target_uri), _signals_arg(signals)],
            check=check,
        )

    def vortex_output_payload_plan(
        self,
        target_uri: str | os.PathLike[str],
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex output payload readiness without writing an artifact."""
        from ._client_commands import ShardLoomClient_vortex_output_payload_plan as _impl
        return _impl(self, target_uri, workspace_path, signals, check=check)

    def vortex_staged_manifest_file_plan(
        self,
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return staged manifest file readiness without writing a manifest."""

        return self.run(
            [
                "vortex-staged-manifest-file-plan",
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_commit_marker_plan(
        self,
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return commit marker readiness without writing a marker."""

        return self.run(
            [
                "vortex-commit-marker-plan",
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_commit_intent_plan(
        self,
        target_uri: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex commit-intent readiness without committing manifests."""

        return self.run(
            ["vortex-commit-intent-plan", str(target_uri), _signals_arg(signals)],
            check=check,
        )

    def vortex_commit_protocol_plan(
        self,
        target_uri: str | os.PathLike[str],
        current_state: str,
        transition: str,
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex commit protocol state transition readiness."""
        from ._client_commands import ShardLoomClient_vortex_commit_protocol_plan as _impl
        return _impl(self, target_uri, current_state, transition, signals, check=check)

    def vortex_local_commit_recovery_plan(
        self,
        target_uri: str | os.PathLike[str],
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return local commit recovery readiness without cleanup or rollback effects."""
        from ._client_commands import ShardLoomClient_vortex_local_commit_recovery_plan as _impl
        return _impl(self, target_uri, workspace_path, signals, check=check)

    def table_compat_plan(
        self,
        format_or_mode: str | None = None,
        scenario: str | None = None,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return table-format compatibility planning for a format or table mode."""

        args = ["table-compat-plan"]
        if format_or_mode is not None:
            args.append(format_or_mode)
        if scenario is not None:
            args.append(scenario)
        return self.run(args, check=check)

    def table_intelligence_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return table-intelligence report-only readiness."""

        return self.run(["table-intelligence-plan"], check=check)

    def layout_health_plan(
        self, scenario: str = "healthy", *, check: bool = True
    ) -> OutputEnvelope:
        """Return layout-health planning for the requested fixture scenario."""

        return self.run(["layout-health-plan", scenario], check=check)

    def compaction_plan(
        self, scenario: str = "small-files", *, check: bool = True
    ) -> OutputEnvelope:
        """Return compaction planning for the requested fixture scenario."""

        return self.run(["compaction-plan", scenario], check=check)

    def catalog_metadata_gate(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CG-9 catalog metadata gate report."""

        return self.run(["cg9-catalog-metadata-gate"], check=check)

    def local_table_metadata_read_smoke(
        self,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the scoped local-manifest table metadata read smoke."""
        from ._client_commands import ShardLoomClient_local_table_metadata_read_smoke as _impl
        return _impl(self, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def object_store_runtime_gate(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CG-10 object-store/distributed runtime promotion gate."""

        return self.run(["cg10-object-store-runtime-gate"], check=check)

    def object_store_request_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store request planning without remote IO."""

        return self.run(["object-store-request-plan", scenario], check=check)

    def object_store_range_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store byte-range planning without remote IO."""

        return self.run(["object-store-range-plan", scenario], check=check)

    def object_store_coalesce_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store request coalescing planning without remote IO."""

        return self.run(["object-store-coalesce-plan", scenario], check=check)

    def object_store_schedule_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store scheduling planning without remote IO."""

        return self.run(["object-store-schedule-plan", scenario], check=check)

    def object_store_checkpoint_retry_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store checkpoint/retry planning without remote IO."""

        return self.run(["object-store-checkpoint-retry-plan", scenario], check=check)

    def object_store_commit_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store commit planning without remote IO or writes."""

        return self.run(["object-store-commit-plan", scenario], check=check)

    def object_store_read_smoke(
        self,
        local_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        byte_range: tuple[int, int] | None = None,
        public_fixture_path: str | os.PathLike[str] | None = None,
        fixture_listing: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run an explicit object-store read smoke for an admitted fixture profile."""
        from ._client_commands import ShardLoomClient_object_store_read_smoke as _impl
        return _impl(self, local_object_path, profile=profile, byte_range=byte_range, public_fixture_path=public_fixture_path, fixture_listing=fixture_listing, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def object_store_partition_discovery_smoke(
        self,
        local_partition_root: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        partition_columns: Sequence[str] | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run scoped local-emulator key=value partition discovery."""
        from ._client_commands import ShardLoomClient_object_store_partition_discovery_smoke as _impl
        return _impl(self, local_partition_root, profile=profile, partition_columns=partition_columns, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def object_store_write_smoke(
        self,
        source_path: str | os.PathLike[str],
        target_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit local-emulator staged object-store write smoke."""
        from ._client_commands import ShardLoomClient_object_store_write_smoke as _impl
        return _impl(self, source_path, target_object_path, profile=profile, idempotency_key=idempotency_key, allow_overwrite=allow_overwrite, rollback_after_commit=rollback_after_commit, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def local_table_append_commit_rehearsal_smoke(
        self,
        target_manifest_path: str | os.PathLike[str],
        *,
        profile: str = "local-manifest",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local-manifest table append commit rehearsal smoke."""
        from ._client_commands import ShardLoomClient_local_table_append_commit_rehearsal_smoke as _impl
        return _impl(self, target_manifest_path, profile=profile, idempotency_key=idempotency_key, allow_overwrite=allow_overwrite, rollback_after_commit=rollback_after_commit, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def object_store_write_recovery_smoke(
        self,
        target_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run local-emulator object-store write recovery replay."""
        from ._client_commands import ShardLoomClient_object_store_write_recovery_smoke as _impl
        return _impl(self, target_object_path, profile=profile, idempotency_key=idempotency_key, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def local_table_commit_recovery_smoke(
        self,
        target_manifest_path: str | os.PathLike[str],
        *,
        profile: str = "local-manifest",
        idempotency_key: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local-manifest table commit recovery smoke."""
        from ._client_commands import ShardLoomClient_local_table_commit_recovery_smoke as _impl
        return _impl(self, target_manifest_path, profile=profile, idempotency_key=idempotency_key, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def correctness_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the correctness evidence planning surface."""

        return self.run(["correctness-plan"], check=check)

    def workflow_readiness_smoke(
        self,
        *,
        target_uri: str | os.PathLike[str] = DEFAULT_WORKFLOW_READINESS_TARGET_URI,
        workspace_path: str | os.PathLike[str] = DEFAULT_WORKFLOW_READINESS_WORKSPACE,
        compatibility_target_uri: str | os.PathLike[str] = (
            DEFAULT_WORKFLOW_READINESS_COMPATIBILITY_TARGET_URI
        ),
        remote_sources: Mapping[str, str | os.PathLike[str]] | None = None,
        check: bool = True,
    ) -> WorkflowReadinessSmokeReport:
        """Preview output, remote-data, and evidence readiness without side effects."""
        from ._client_commands import ShardLoomClient_workflow_readiness_smoke as _impl
        return _impl(self, target_uri=target_uri, workspace_path=workspace_path, compatibility_target_uri=compatibility_target_uri, remote_sources=remote_sources, check=check)

    def input_adapters(self, *, check: bool = True) -> OutputEnvelope:
        """Return the universal input adapter registry snapshot."""

        return self.run(["input-adapters"], check=check)

    def extension_registry(
        self,
        *,
        manifest_dir: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return the side-effect-free extension registry snapshot."""

        if manifest_dir is not None:
            return self.run(
                ["extension-registry", "--manifest-dir", manifest_dir],
                check=check,
            )
        return self.run(["extension-registry"], check=check)

    def extension_inspect(
        self,
        extension_id: str | None = None,
        *,
        manifest_path: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Inspect extension manifest metadata without loading extension code."""

        if manifest_path is not None:
            return self.run(["extension-inspect", "--manifest", manifest_path], check=check)
        if extension_id is None:
            raise ValueError("extension_id or manifest_path is required")
        return self.run(["extension-inspect", extension_id], check=check)

    def udf_runtime_plan(
        self,
        runtime: str = "unknown",
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return UDF runtime posture, including the admitted built-in fixture."""

        return self.run(["udf-runtime-plan", runtime], check=check)

    def udf_registry(self, *, check: bool = True) -> OutputEnvelope:
        """Return the typed UDF registry without executing user UDF code."""

        return self.run(["udf-registry"], check=check)

    def udf_local_scalar_fixture_smoke(
        self,
        values: Sequence[int | None] | str,
        *,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the built-in deterministic nullable-int64 scalar UDF fixture."""
        from ._client_commands import ShardLoomClient_udf_local_scalar_fixture_smoke as _impl
        return _impl(self, values, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def embedding_vector_local_fixture_smoke(
        self,
        texts: Sequence[str] | str,
        *,
        query: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the built-in deterministic embedding/vector fixture."""
        from ._client_commands import ShardLoomClient_embedding_vector_local_fixture_smoke as _impl
        return _impl(self, texts, query=query, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def sqlite_local_import_export_smoke(
        self,
        database_path: str | os.PathLike[str],
        *,
        table: str,
        export_jsonl: str | os.PathLike[str],
        roundtrip_db: str | os.PathLike[str],
        order_by: str | None = None,
        allow_overwrite: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local SQLite file import/export fixture smoke."""
        from ._client_commands import ShardLoomClient_sqlite_local_import_export_smoke as _impl
        return _impl(self, database_path, table=table, export_jsonl=export_jsonl, roundtrip_db=roundtrip_db, order_by=order_by, allow_overwrite=allow_overwrite, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def input_plan(
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        source_format: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a side-effect-free universal input plan for a dataset URI."""

        command: list[CommandPart] = ["input-plan", str(dataset_uri)]
        if source_format is not None:
            command.extend(["--source-format", str(source_format)])
        return self.run(command, check=check)

    def compatibility_source_smoke(
        self,
        sources: Mapping[str, str | os.PathLike[str]] | None = None,
        *,
        check: bool = True,
    ) -> CompatibilitySourceSmokeReport:
        """Plan compatibility file sources without reading, writing, or materializing data."""
        from ._client_commands import ShardLoomClient_compatibility_source_smoke as _impl
        return _impl(self, sources, check=check)

    def vortex_input_plan(
        self, dataset_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a side-effect-free native Vortex input planning envelope."""

        return self.run(["vortex-input-plan", str(dataset_uri)], check=check)

    def vortex_read_plan(
        self, dataset_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a side-effect-free native Vortex read planning envelope."""

        return self.run(["vortex-read-plan", str(dataset_uri)], check=check)

    def smoke_check(self, *, check: bool = True) -> PythonClientSmokeReport:
        """Run no-dataset commands that verify the Python client can reach ShardLoom."""
        from ._client_transport import ShardLoomClient_smoke_check as _impl
        return _impl(self, check=check)

    def binary_command(self) -> tuple[str, ...]:
        """Resolve the CLI binary command without running a ShardLoom command."""

        return tuple(self._binary_parts())

    def run(self, args: Sequence[CommandPart], *, check: bool = True) -> OutputEnvelope:
        """Invoke a ShardLoom CLI command with JSON output enabled."""
        from ._client_transport import ShardLoomClient_run as _impl
        return _impl(self, args, check=check)

    def cancel(self) -> None:
        """Cancel current calls; later calls remain usable.

        Native/batch cancellation is cooperative and producer callbacks must
        return. Ordinary CLI processes are terminated: their active call reports
        that output cleanup cannot be confirmed. Completed results remain valid.
        """

        self._stop_active_work(closing=False)

    def close(self) -> None:
        """Close this client's native session or persistent local CLI worker."""

        self._stop_active_work(closing=True)

    def _stop_active_work(self, *, closing: bool) -> None:
        from ._client_transport import ShardLoomClient__stop_active_work as _impl
        return _impl(self, closing=closing)

    def release_prepared(self) -> None:
        """Discard preparation, retaining this client's sources, results and lifetime."""
        with self._native_backend_lock:
            self._ensure_open()
            if self._native_backend is not None:
                self._native_backend.release_prepared()
        if self.transport == "subprocess":
            with self._worker_lock:
                self._close_worker()

    def _register_process(self, process) -> None:
        with self._native_backend_lock:
            self._ensure_open()
            self._active_processes.add(process)

    def _release_process(self, process) -> None:
        with self._native_backend_lock:
            self._active_processes.discard(process)
            self._cancelled_processes.discard(process)

    def _check_process_cancelled(self, process, command) -> None:
        with self._native_backend_lock:
            cancelled = process in self._cancelled_processes
        if cancelled:
            from .errors import _cancelled_error
            raise _cancelled_error(command, forced=True)

    def __del__(self) -> None:
        try:
            self._close_worker()
        except Exception:
            pass

    def _run_subprocess(
        self, args: Sequence[CommandPart], *, check: bool = True
    ) -> OutputEnvelope:
        from ._client_transport import ShardLoomClient__run_subprocess as _impl
        return _impl(self, args, check=check)

    def _run_worker(
        self, args: Sequence[CommandPart], *, check: bool = True
    ) -> OutputEnvelope:
        from ._client_transport import ShardLoomClient__run_worker as _impl
        return _impl(self, args, check=check)

    def _write_worker_request(
        self,
        process: subprocess.Popen[str],
        payload: str,
        redacted_command: Sequence[str],
        deadline: float | None,
    ) -> None:
        if process.stdin is None:
            return
        if deadline is None:
            process.stdin.write(payload)
            process.stdin.flush()
            return
        self._write_worker_request_with_timeout(process, payload, redacted_command, deadline)

    def _write_worker_request_with_timeout(
        self,
        process: subprocess.Popen[str],
        payload: str,
        redacted_command: Sequence[str],
        deadline: float,
    ) -> None:
        from ._client_transport import ShardLoomClient__write_worker_request_with_timeout as _impl
        return _impl(self, process, payload, redacted_command, deadline)

    def _read_worker_response_line(
        self,
        process: subprocess.Popen[str],
        redacted_command: Sequence[str],
        deadline: float | None,
    ) -> str:
        from ._client_transport import ShardLoomClient__read_worker_response_line as _impl
        return _impl(self, process, redacted_command, deadline)

    def _read_worker_response_line_with_timeout(
        self,
        process: subprocess.Popen[str],
        redacted_command: Sequence[str],
        deadline: float,
    ) -> str:
        from ._client_transport import ShardLoomClient__read_worker_response_line_with_timeout as _impl
        return _impl(self, process, redacted_command, deadline)

    def _pop_worker_stdout_buffered_line(self) -> str | None:
        if not self._worker_stdout_buffer:
            return None
        line, separator, remainder = self._worker_stdout_buffer.partition(b"\n")
        if not separator:
            return None
        self._worker_stdout_buffer = remainder
        return self._decode_worker_stdout(line + separator)

    @staticmethod
    def _decode_worker_stdout(chunk: bytes) -> str:
        return chunk.decode("utf-8")

    def _command(self, args: Sequence[CommandPart]) -> list[str]:
        command = self._binary_parts()
        command.extend(self._command_args(args))
        return command

    def _command_args(self, args: Sequence[CommandPart]) -> list[str]:
        from ._client_transport import ShardLoomClient__command_args as _impl
        return _impl(self, args)

    def _worker_enabled(self) -> bool:
        from ._client_transport import ShardLoomClient__worker_enabled as _impl
        return _impl(self)

    def _ensure_worker(self) -> subprocess.Popen[str]:
        from ._client_transport import ShardLoomClient__ensure_worker as _impl
        return _impl(self)

    def _close_worker(self) -> None:
        from ._client_transport import ShardLoomClient__close_worker as _impl
        return _impl(self)

    @staticmethod
    def _worker_stderr_preview(process: subprocess.Popen[str]) -> str:
        if process.poll() is None:
            return ""
        if process.stderr is None:
            return ""
        try:
            return process.stderr.read(2048).strip()
        except OSError:
            return ""

    def _binary_parts(self) -> list[str]:
        binary = self._resolved_binary()
        if isinstance(binary, (str, os.PathLike)):
            return [str(binary)]
        if not binary:
            raise ValueError("ShardLoom binary command cannot be empty")
        return [str(part) for part in binary]

    def _resolved_binary(self) -> Binary:
        from ._client_transport import ShardLoomClient__resolved_binary as _impl
        return _impl(self)

    def _effective_env(self) -> Mapping[str, str]:
        if self._env is None:
            return os.environ
        effective_env = dict(os.environ)
        effective_env.update(self._env)
        return effective_env

    def _resolve_configured_binary(self, value: str, env: Mapping[str, str]) -> str:
        from ._client_transport import ShardLoomClient__resolve_configured_binary as _impl
        return _impl(self, value, env)

    def _repo_binary_candidate(self) -> Path | None:
        suffixes = (".exe", "") if os.name == "nt" else ("",)
        for profile in self._profile_order:
            for suffix in suffixes:
                candidate = self._repo_root / "target" / profile / f"shardloom{suffix}"
                if candidate.is_file():
                    return candidate
        return None

    def _bundled_binary_candidate(self) -> str | None:
        from ._client_transport import ShardLoomClient__bundled_binary_candidate as _impl
        return _impl(self)

    @staticmethod
    def _append_json_format(command: list[str]) -> None:
        from ._client_transport import ShardLoomClient__append_json_format as _impl
        return _impl(command)

    @staticmethod
    def _parse_stdout(stdout: str, command: Sequence[str]) -> OutputEnvelope:
        from ._client_transport import ShardLoomClient__parse_stdout as _impl
        return _impl(stdout, command)


def _redact_command_for_error(command: Sequence[str]) -> tuple[str, ...]:
    return tuple(_redact_command_part_for_error(part) for part in command)


def _redact_command_part_for_error(part: str) -> str:
    if len(part) > _MAX_COMMAND_PART_REDACTION_CHARS:
        return f"<redacted-large-arg:{len(part)}chars>"
    return _URI_WITH_AUTHORITY_RE.sub(_redact_uri_match_for_error, part)


def _redact_uri_match_for_error(match: re.Match[str]) -> str:
    scheme = match.group("scheme")
    body = match.group("body")
    body_without_query = re.split(r"[?#]", body, maxsplit=1)[0]
    authority, separator, path = body_without_query.partition("/")
    redacted_authority = _redact_uri_authority_for_error(scheme, authority)
    return f"{scheme}://{redacted_authority}{separator}{path}"


def _redact_uri_authority_for_error(scheme: str, authority: str) -> str:
    normalized_scheme = scheme.lower()
    if normalized_scheme in {"abfs", "abfss"} and not _adls_authority_has_userinfo(
        authority
    ):
        return authority
    if "@" not in authority:
        return authority
    return "<redacted>@" + authority.rsplit("@", 1)[1]


def _adls_authority_has_userinfo(authority: str) -> bool:
    if authority.count("@") > 1:
        return True
    container, separator, _account = authority.partition("@")
    return bool(separator and ":" in container)


def _required_field(envelope: OutputEnvelope, key: str, *, allow_empty: bool = False) -> str:
    value = envelope.field(key)
    if value is None or (value == "" and not allow_empty):
        raise ShardLoomProtocolError(
            f"ShardLoom command {envelope.command!r} did not emit required field {key!r}"
        )
    return value




def _iter_fanout_outputs(
    fanout_outputs: FanoutOutputs | None,
) -> tuple[tuple[str, str], ...]:
    from ._client_arguments import _iter_fanout_outputs as _impl
    return _impl(fanout_outputs)


def _schema_command_arg(
    schema: Mapping[str, object] | Sequence[tuple[str, object]] | str | None,
) -> str | None:
    from ._client_arguments import _schema_command_arg as _impl
    return _impl(schema)


def _source_bindings_command_arg(
    source_bindings: Mapping[str, Mapping[str, object]] | None,
) -> str | None:
    from ._client_arguments import _source_bindings_command_arg as _impl
    return _impl(source_bindings)


def _jsonl_object_rows(value: str, *, field_name: str) -> tuple[Mapping[str, Any], ...]:
    from ._client_report_values import _jsonl_object_rows as _impl
    return _impl(value, field_name=field_name)


def _csv_values(value: str | None) -> tuple[str, ...]:
    if value is None or value == "" or value == "none":
        return ()
    row = next(csv.reader([value]), [])
    return tuple(part.strip() for part in row if part.strip())


ABSENT_REFERENCE_VALUES = {
    "none",
    "null",
    "not_requested",
    "not_applicable",
    "not_available",
    "not_applicable_inline_result",
}


def _is_absent_csv_value(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in ABSENT_REFERENCE_VALUES


def _csv_present_values(value: str | None) -> tuple[str, ...]:
    return tuple(part for part in _csv_values(value) if not _is_absent_csv_value(part))


def _csv_output_plan_required_columns(value: str | None) -> tuple[str, ...]:
    from ._client_report_values import _csv_output_plan_required_columns as _impl
    return _impl(value)


def _csv_reference_values(value: str | None) -> tuple[str, ...]:
    return _csv_present_values(value)


def _csv_key_value_map(value: str | None) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in _csv_values(value):
        if "=" not in part:
            continue
        key, item_value = part.split("=", 1)
        fields[key] = item_value
    return fields


LOCAL_VORTEX_FALLBACK_ATTEMPTED_FIELDS = (
    "local_count_native_io_fallback_attempted",
    "execution_certificate_fallback_attempted",
    "filtered_count_local_execution_fallback_attempted",
    "filter_local_execution_fallback_attempted",
    "project_local_execution_fallback_attempted",
    "filter_project_local_execution_fallback_attempted",
    "local_primitive_native_io_fallback_attempted",
    "local_primitive_execution_certificate_fallback_attempted",
)

LOCAL_VORTEX_CERTIFIED_FIELDS = (
    "local_primitive_native_io_certified",
    "local_primitive_execution_certificate_correctness_passed",
)

NO_WRITE_FALSE_FIELDS = (
    "fallback_attempted",
    "fallback_execution_allowed",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "runtime_execution",
    "query_execution",
    "external_engine_execution",
    "external_effects_executed",
    "data_read",
    "data_materialized",
    "read_io",
    "write_io",
    "object_store_io",
    "output_data_written",
    "manifest_written",
    "manifest_file_written",
    "draft_file_written",
    "commit_marker_written",
    "manifest_finalized",
    "manifest_committed",
    "commit_performed",
    "upstream_vortex_write_called",
    "recovery_action_executed",
    "rollback_executed",
    "cleanup_performed",
    "write_execution_allowed",
    "commit_execution_allowed",
    "payload_write_allowed",
    "marker_write_allowed",
    "finalization_execution_allowed",
    "snapshot_manifest_metadata_read_allowed",
    "catalog_resolution_allowed",
    "table_metadata_read_allowed",
    "catalog_io_allowed",
    "object_store_io_allowed",
    "data_io_allowed",
    "write_io_allowed",
    "external_table_format_dependency_allowed",
    "credential_resolution_allowed",
    "metadata_cache_runtime_allowed",
    "metadata_integration_claim_allowed",
    "performance_claim_allowed",
    "superiority_claim_allowed",
    "best_default_claim_allowed",
)

REPORT_ONLY_EXECUTION_VALUES = frozenset(
    {
        "not_performed",
        "local_commit_recovery_planning_only",
        "marker_write_or_not_performed",
        "commit_marker_write_or_not_performed",
        "payload_write_or_not_performed",
        "native_count_payload_write_or_not_performed",
    }
)

BLOCKER_FIELDS = (
    "claim_blocked",
    "runtime_promotions_blocked",
    "metadata_integration_claim_allowed",
    "performance_claim_allowed",
    "superiority_claim_allowed",
    "best_default_claim_allowed",
)


def _all_bool_fields(envelope: OutputEnvelope, keys: Sequence[str]) -> bool:
    return all(envelope.field_bool(key, False) is True for key in keys)


def _any_bool_field(envelope: OutputEnvelope, keys: Sequence[str]) -> bool:
    return any(
        envelope.field_bool(key, False) is True
        for key in keys
        if envelope.field(key) is not None
    )


def _envelope_external_engine_invoked(envelope: OutputEnvelope) -> bool:
    typed_policy = _typed_payload_field_bool(envelope.policy, "external_engine_invoked", None)
    if typed_policy is not None:
        return typed_policy is True
    return envelope.field_bool("external_engine_invoked", False) is True


def _artifact_payload_field_map(artifact: Mapping[str, Any]) -> dict[str, str]:
    payload = artifact.get("payload")
    if not isinstance(payload, Mapping):
        return {}
    return _typed_payload_field_map(payload)


def _typed_payload_field_map(payload: Mapping[str, Any]) -> dict[str, str]:
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return {}
    out: dict[str, str] = {}
    for field in fields:
        if isinstance(field, Mapping):
            out[str(field.get("key", ""))] = str(field.get("value", ""))
    return out


def _typed_payload_field_bool(
    payload: Mapping[str, Any],
    key: str,
    default: bool | None = None,
) -> bool | None:
    value = _typed_payload_field_map(payload).get(key)
    if value is None:
        return default
    return _string_bool_value(value, key)


def _string_bool_value(value: str, key: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"typed payload field {key!r} is not a boolean value: {value!r}")


def _compatibility_source_items(
    sources: Mapping[str, str | os.PathLike[str]] | None,
) -> tuple[tuple[str, str | os.PathLike[str]], ...]:
    if sources is None:
        return DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS
    if not sources:
        raise ValueError("sources must not be empty")
    items = tuple((str(name), uri) for name, uri in sources.items())
    if any(name.strip() == "" for name, _ in items):
        raise ValueError("source names must not be empty")
    return items


def _workflow_remote_source_items(
    sources: Mapping[str, str | os.PathLike[str]] | None,
) -> tuple[tuple[str, str | os.PathLike[str]], ...]:
    if sources is None:
        return DEFAULT_WORKFLOW_READINESS_REMOTE_SOURCES
    if not sources:
        raise ValueError("remote_sources must not be empty")
    items = tuple((str(name), uri) for name, uri in sources.items())
    if any(name.strip() == "" for name, _ in items):
        raise ValueError("remote source names must not be empty")
    return items


def _signals_arg(signals: str | Sequence[str]) -> str:
    from ._client_arguments import _signals_arg as _impl
    return _impl(signals)




def _envelope_preserves_no_write(envelope: OutputEnvelope) -> bool:
    if envelope.fallback.attempted or envelope.fallback.allowed:
        return False
    return all(
        envelope.field_bool(key, False) is False
        for key in NO_WRITE_FALSE_FIELDS
        if envelope.field(key) is not None
    )


def _envelope_is_report_only_or_planned(envelope: OutputEnvelope) -> bool:
    execution = envelope.field("execution")
    if execution is not None and execution not in REPORT_ONLY_EXECUTION_VALUES:
        return False
    plan_only = envelope.field("plan_only")
    if plan_only is not None and envelope.field_bool("plan_only") is not True:
        return False
    return True


def _envelope_reports_blocked_or_incomplete(envelope: OutputEnvelope) -> bool:
    from ._client_arguments import _envelope_reports_blocked_or_incomplete as _impl
    return _impl(envelope)


def _columns_arg(columns: str | Sequence[str]) -> str:
    from ._client_arguments import _columns_arg as _impl
    return _impl(columns)


def _append_resource_execution_args(
    args: list[str],
    *,
    option: str,
    enabled: bool,
    memory_gb: int | None,
    max_parallelism: int | None,
    memory_bytes: int | None = None,
    resources: ExecutionResources | None = None,
) -> None:
    from ._client_arguments import _append_resource_execution_args as _impl
    return _impl(args, option=option, enabled=enabled, memory_gb=memory_gb, max_parallelism=max_parallelism, memory_bytes=memory_bytes, resources=resources)


def _append_public_vortex_payload_args(
    args: list[CommandPart],
    *,
    native_vortex_operation_family: str | None,
    native_vortex_right_input: str | os.PathLike[str] | None,
    vortex_primitive: str | None,
    vortex_predicate: str | None,
    vortex_columns: str | Sequence[str] | None,
    vortex_source_order_limit: int | None,
    vortex_sample_seed: int | None,
    vortex_sample_fraction: float | None,
    vortex_sample_replacement: bool,
    vortex_sample_weight_column: str | None,
    vortex_duplicate_keep: str | None,
    vortex_deduplicate_key_columns: str | Sequence[str] | None,
    vortex_expression_projection: str | None,
    vortex_melt_projection: str | None,
    vortex_explode_projection: str | None,
    vortex_pivot_projection: str | None,
    vortex_rolling_window: str | None,
    vortex_sort_rows: str | None,
    vortex_aggregate: str | None,
    resources: ExecutionResources | None,
    spill: Mapping[str, object] | str | None = None,
) -> None:
    from ._client_arguments import _append_public_vortex_payload_args as _impl
    return _impl(args, native_vortex_operation_family=native_vortex_operation_family, native_vortex_right_input=native_vortex_right_input, vortex_primitive=vortex_primitive, vortex_predicate=vortex_predicate, vortex_columns=vortex_columns, vortex_source_order_limit=vortex_source_order_limit, vortex_sample_seed=vortex_sample_seed, vortex_sample_fraction=vortex_sample_fraction, vortex_sample_replacement=vortex_sample_replacement, vortex_sample_weight_column=vortex_sample_weight_column, vortex_duplicate_keep=vortex_duplicate_keep, vortex_deduplicate_key_columns=vortex_deduplicate_key_columns, vortex_expression_projection=vortex_expression_projection, vortex_melt_projection=vortex_melt_projection, vortex_explode_projection=vortex_explode_projection, vortex_pivot_projection=vortex_pivot_projection, vortex_rolling_window=vortex_rolling_window, vortex_sort_rows=vortex_sort_rows, vortex_aggregate=vortex_aggregate, resources=resources, spill=spill)


def _positive_int(name: str, value: int) -> int:
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _non_negative_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def _sample_fraction_arg(value: float) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("vortex_sample_fraction must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0 or parsed > 1:
        raise ValueError("vortex_sample_fraction must be finite and in the range (0, 1]")
    return f"{parsed:.12g}"


def _duplicate_keep_arg(value: str) -> str:
    normalized = str(value).strip().lower().replace("_", "-")
    if normalized in {"first", "last"}:
        return normalized
    if normalized in {"false", "all", "none"}:
        return "false"
    raise ValueError("vortex_duplicate_keep must be 'first', 'last', or False")


def _bundled_cli_platform_tags() -> tuple[str, ...]:
    from ._client_transport import _bundled_cli_platform_tags as _impl
    return _impl()


def _normalized_bundled_cli_system_tag() -> str:
    raw = platform.system().strip().lower()
    if raw == "darwin":
        return "macos"
    if raw.startswith("msys") or raw.startswith("mingw") or raw == "windows":
        return "windows"
    return raw or "unknown"


def _normalized_bundled_cli_arch_tag() -> str:
    raw = platform.machine().strip().lower()
    if raw in {"amd64", "x64"}:
        return "x86_64"
    if raw == "arm64":
        return "aarch64"
    return raw or "unknown"


def _is_executable_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if os.name == "nt":
        return True
    return os.access(path, os.X_OK)


def _looks_like_path(value: str) -> bool:
    path = Path(value)
    separators = [os.sep]
    if os.altsep is not None:
        separators.append(os.altsep)
    return path.is_absolute() or any(separator in value for separator in separators)


def _profile_order_from_env(env: Mapping[str, str]) -> tuple[str, ...]:
    raw = env.get(ENV_PROFILE_ORDER)
    if raw is None or raw.strip() == "":
        return DEFAULT_PROFILE_ORDER
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or DEFAULT_PROFILE_ORDER


def _timeout_from_env(env: Mapping[str, str]) -> float | None:
    raw = env.get(ENV_TIMEOUT_SECONDS)
    if raw is None or raw.strip() == "":
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{ENV_TIMEOUT_SECONDS} must be a number of seconds") from exc
