"""Deferred implementations for client report views.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import client as _api


def CompatibilitySourceSmokeReport_all_plan_only(self):

    return all(
        source.plan.field_bool("plan_only", False) is True
        and source.plan.field_bool("data_read", True) is False
        and source.plan.field_bool("data_materialized", True) is False
        and source.plan.field_bool("write_io", True) is False
        and source.plan.field_bool("native_vortex", True) is False
        and source.plan.field_bool("fallback_execution_allowed", True) is False
        for source in self.sources
    )


def EvidenceAwareOptimizerTraceReport_no_fallback_no_external_engine(self):

    return (
        self.envelope.field_bool("fallback_attempted", True) is False
        and self.envelope.field_bool("fallback_execution_allowed", True) is False
        and self.envelope.field_bool("external_engine_invoked", True) is False
        and (
            self.envelope.field_bool("all_no_fallback_no_external_engine", False)
            is True
        )
    )


def PublicWorkflowRoute_as_dict(self):

    return {
        "schema_version": self.schema_version,
        "route_id": self.route_id,
        "route_status": self.route_status,
        "resolved_internal_command": self.resolved_internal_command,
        "surface": self.surface,
        "start_state": self.start_state,
        "vortex_normalization_point": self.vortex_normalization_point,
        "execution_mode": self.execution_mode,
        "preparation_included": self.preparation_included,
        "query_timing_starts_after_preparation": self.query_timing_starts_after_preparation,
        "fanout_output_count": self.fanout_output_count,
        "fanout_outputs": self.fanout_outputs,
        "vortex_primitive": self.vortex_primitive,
        "vortex_predicate": self.vortex_predicate,
        "vortex_columns": self.vortex_columns,
        "fallback_attempted": self.fallback_attempted,
        "external_engine_invoked": self.external_engine_invoked,
        "blocker_id": self.blocker_id,
        "side_effect_free": self.side_effect_free,
        "activation_summary": self.activation_summary.as_dict(),
    }


def PublicWorkflowExecution_as_dict(self):

    return {
        "schema_version": self.schema_version,
        "facade_command": self.facade_command,
        "route_attached": self.route_attached,
        "route_id": self.route_id,
        "route_status": self.route_status,
        "resolved_internal_command": self.resolved_internal_command,
        "vortex_normalization_point": self.vortex_normalization_point,
        "execution_mode": self.execution_mode,
        "runtime_execution": self.runtime_execution,
        "preparation_included": self.preparation_included,
        "fanout_output_count": self.fanout_output_count,
        "fanout_outputs": self.fanout_outputs,
        "vortex_primitive": self.vortex_primitive,
        "vortex_predicate": self.vortex_predicate,
        "vortex_columns": self.vortex_columns,
        "fallback_attempted": self.fallback_attempted,
        "external_engine_invoked": self.external_engine_invoked,
        "public_workflow_fallback_attempted": self.public_workflow_fallback_attempted,
        "public_workflow_external_engine_invoked": (
            self.public_workflow_external_engine_invoked
        ),
        "blocker_id": self.blocker_id,
        "activation_summary": self.activation_summary.as_dict(),
    }


def RunsTodaySupportMatrix_rows(self):

    rows: list[RunsTodaySupportRow] = []
    for row_id in self.row_ids:
        prefix = f"runs_today_row_{row_id}_"
        rows.append(
            _api.RunsTodaySupportRow(
                row_id=row_id,
                family=_api._required_field(self.envelope, f"{prefix}family"),
                surface=_api._csv_values(self.envelope.field(f"{prefix}surface")),
                support_state=_api._required_field(self.envelope, f"{prefix}support_state"),
                feature_gate=_api._required_field(self.envelope, f"{prefix}feature_gate"),
                evidence_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}evidence_refs")
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                claim_boundary=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_boundary",
                ),
                runtime_execution=self.envelope.field_bool(
                    f"{prefix}runtime_execution",
                    False,
                )
                is True,
                data_read=self.envelope.field_bool(f"{prefix}data_read", False) is True,
                write_io=self.envelope.field_bool(f"{prefix}write_io", False) is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def RunsTodaySupportMatrix_production_unsupported_diagnostic_rows(self):

    rows: list[ProductionUnsupportedDiagnosticRow] = []
    for row_id in self.production_unsupported_diagnostic_row_ids:
        prefix = f"production_unsupported_diagnostic_row_{row_id}_"
        rows.append(
            _api.ProductionUnsupportedDiagnosticRow(
                row_id=row_id,
                production_family=_api._required_field(
                    self.envelope,
                    f"{prefix}production_family",
                ),
                user_surface=_api._csv_values(
                    self.envelope.field(f"{prefix}user_surface")
                ),
                entrypoint_kind=_api._required_field(
                    self.envelope,
                    f"{prefix}entrypoint_kind",
                ),
                support_status=_api._required_field(
                    self.envelope,
                    f"{prefix}support_status",
                ),
                diagnostic_code=_api._required_field(
                    self.envelope,
                    f"{prefix}diagnostic_code",
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                message=_api._required_field(self.envelope, f"{prefix}message"),
                next_action=_api._required_field(self.envelope, f"{prefix}next_action"),
                required_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_evidence")
                ),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                route_scope=_api._required_field(self.envelope, f"{prefix}route_scope"),
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
                side_effects_performed=self.envelope.field_bool(
                    f"{prefix}side_effects_performed",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def RunsTodaySupportMatrix_production_unsupported_diagnostic_all_rows_safe(self):

    return (
        self.envelope.field_bool(
            "production_unsupported_diagnostic_all_rows_fallback_attempted_false",
            False,
        )
        is True
        and self.envelope.field_bool(
            "production_unsupported_diagnostic_all_rows_external_engine_invoked_false",
            False,
        )
        is True
        and self.envelope.field_bool(
            "production_unsupported_diagnostic_all_rows_side_effects_performed_false",
            False,
        )
        is True
    )


def ComputeCapabilityMatrix_rows(self):

    rows: list[ComputeCapabilityRow] = []
    for row_id in _api._csv_values(self.envelope.field("compute_row_order")):
        prefix = f"compute_row_{row_id}_"
        rows.append(
            _api.ComputeCapabilityRow(
                row_id=row_id,
                surface=_api._required_field(self.envelope, f"{prefix}surface"),
                family=_api._required_field(self.envelope, f"{prefix}family"),
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                engine_mode=_api._required_field(self.envelope, f"{prefix}engine_mode"),
                execution_mode=_api._required_field(self.envelope, f"{prefix}execution_mode"),
                provider_kind=_api._required_field(self.envelope, f"{prefix}provider_kind"),
                semantic_profile=_api._required_field(self.envelope, f"{prefix}semantic_profile"),
                materialization_decode_requirement=_api._required_field(
                    self.envelope,
                    f"{prefix}materialization_decode_requirement",
                ),
                memory_spill_requirement=_api._required_field(
                    self.envelope,
                    f"{prefix}memory_spill_requirement",
                ),
                correctness_refs=_api._csv_values(self.envelope.field(f"{prefix}correctness_refs")),
                benchmark_refs=_api._csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                execution_certificate_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}execution_certificate_refs")
                ),
                native_io_refs=_api._csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                unsupported_diagnostic_code=_api._required_field(
                    self.envelope,
                    f"{prefix}unsupported_diagnostic_code",
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                vortex_native_claim_allowed=self.envelope.field_bool(
                    f"{prefix}vortex_native_claim_allowed",
                    False,
                )
                is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_operator_families(self):

    rows: list[OperatorFamilyCoverageRow] = []
    for family_id in _api._csv_values(self.envelope.field("operator_family_order")):
        prefix = f"operator_family_{family_id}_"
        rows.append(
            _api.OperatorFamilyCoverageRow(
                family_id=family_id,
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                next_evidence=_api._csv_values(self.envelope.field(f"{prefix}next_evidence")),
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_native_vortex_admission_lanes(self):

    rows: list[NativeVortexAdmissionLane] = []
    for lane_id in _api._csv_values(self.envelope.field("native_vortex_admission_lane_order")):
        prefix = f"native_vortex_admission_lane_{lane_id}_"
        rows.append(
            _api.NativeVortexAdmissionLane(
                lane_id=lane_id,
                source_surface=_api._required_field(self.envelope, f"{prefix}source_surface"),
                operator_surface=_api._required_field(
                    self.envelope,
                    f"{prefix}operator_surface",
                ),
                sink_surface=_api._required_field(self.envelope, f"{prefix}sink_surface"),
                admission_status=_api._required_field(
                    self.envelope,
                    f"{prefix}admission_status",
                ),
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                execution_mode=_api._required_field(self.envelope, f"{prefix}execution_mode"),
                provider_kind=_api._required_field(self.envelope, f"{prefix}provider_kind"),
                provider_api_surface=_api._csv_values(
                    self.envelope.field(f"{prefix}provider_api_surface")
                ),
                provider_crate=_api._required_field(self.envelope, f"{prefix}provider_crate"),
                provider_version=_api._required_field(self.envelope, f"{prefix}provider_version"),
                feature_gate=_api._required_field(self.envelope, f"{prefix}feature_gate"),
                shardloom_admission_policy=_api._required_field(
                    self.envelope,
                    f"{prefix}shardloom_admission_policy",
                ),
                compute_row_ref=_api._required_field(self.envelope, f"{prefix}compute_row_ref"),
                benchmark_ref=_api._required_field(self.envelope, f"{prefix}benchmark_ref"),
                correctness_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}correctness_refs")
                ),
                execution_certificate_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}execution_certificate_refs")
                ),
                native_io_refs=_api._csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                materialization_decode_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}materialization_decode_refs")
                ),
                policy_refs=_api._csv_values(self.envelope.field(f"{prefix}policy_refs")),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                claim_boundary=_api._required_field(self.envelope, f"{prefix}claim_boundary"),
                residual_executor=_api._required_field(
                    self.envelope,
                    f"{prefix}residual_executor",
                ),
                vortex_native_claim_allowed=self.envelope.field_bool(
                    f"{prefix}vortex_native_claim_allowed",
                    False,
                )
                is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
                object_store_io=self.envelope.field_bool(
                    f"{prefix}object_store_io",
                    True,
                )
                is True,
                write_io=self.envelope.field_bool(f"{prefix}write_io", True) is True,
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_prepared_vortex_scan_pushdown_rows(self):

    rows: list[PreparedVortexScanPushdownRow] = []
    for row_id in _api._csv_values(self.envelope.field("prepared_vortex_scan_pushdown_row_order")):
        prefix = f"prepared_vortex_scan_pushdown_row_{row_id}_"
        rows.append(
            _api.PreparedVortexScanPushdownRow(
                row_id=row_id,
                scenario=_api._required_field(self.envelope, f"{prefix}scenario"),
                pushdown_status=_api._required_field(
                    self.envelope,
                    f"{prefix}pushdown_status",
                ),
                filter_required=self.envelope.field_bool(
                    f"{prefix}filter_required",
                    False,
                )
                is True,
                projection_required=self.envelope.field_bool(
                    f"{prefix}projection_required",
                    False,
                )
                is True,
                limit_required=self.envelope.field_bool(
                    f"{prefix}limit_required",
                    False,
                )
                is True,
                filter_pushed_down=self.envelope.field_bool(
                    f"{prefix}filter_pushed_down",
                    False,
                )
                is True,
                projection_pushed_down=self.envelope.field_bool(
                    f"{prefix}projection_pushed_down",
                    False,
                )
                is True,
                limit_pushed_down=self.envelope.field_bool(
                    f"{prefix}limit_pushed_down",
                    False,
                )
                is True,
                filter_status=_api._required_field(self.envelope, f"{prefix}filter_status"),
                projection_status=_api._required_field(
                    self.envelope,
                    f"{prefix}projection_status",
                ),
                limit_status=_api._required_field(self.envelope, f"{prefix}limit_status"),
                residual_limit_status=_api._required_field(
                    self.envelope,
                    f"{prefix}residual_limit_status",
                ),
                residual_limit_executor=_api._required_field(
                    self.envelope,
                    f"{prefix}residual_limit_executor",
                ),
                filter_columns_read=_api._csv_values(
                    self.envelope.field(f"{prefix}filter_columns_read")
                ),
                output_columns_read=_api._csv_values(
                    self.envelope.field(f"{prefix}output_columns_read")
                ),
                filter_only_columns_read=_api._csv_values(
                    self.envelope.field(f"{prefix}filter_only_columns_read")
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                blocker_reason=_api._required_field(self.envelope, f"{prefix}blocker_reason"),
                benchmark_refs=_api._csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                claim_boundary=_api._required_field(self.envelope, f"{prefix}claim_boundary"),
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_native_unsupported_coverage_rows(self):

    rows: list[NativeUnsupportedCoverageRow] = []
    for row_id in _api._csv_values(self.envelope.field("native_unsupported_coverage_row_order")):
        prefix = f"native_unsupported_coverage_row_{row_id}_"
        rows.append(
            _api.NativeUnsupportedCoverageRow(
                row_id=row_id,
                category=_api._required_field(self.envelope, f"{prefix}category"),
                surface=_api._required_field(self.envelope, f"{prefix}surface"),
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                unsupported_diagnostic_code=_api._required_field(
                    self.envelope,
                    f"{prefix}unsupported_diagnostic_code",
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                source_refs=_api._csv_values(self.envelope.field(f"{prefix}source_refs")),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                execution_attempted=self.envelope.field_bool(
                    f"{prefix}execution_attempted",
                    True,
                )
                is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_predicate_dtype_coverage_rows(self):

    rows: list[PredicateDtypeCoverageRow] = []
    for row_id in _api._csv_values(self.envelope.field("predicate_dtype_coverage_row_order")):
        prefix = f"predicate_dtype_coverage_row_{row_id}_"
        rows.append(
            _api.PredicateDtypeCoverageRow(
                row_id=row_id,
                category=_api._required_field(self.envelope, f"{prefix}category"),
                family=_api._required_field(self.envelope, f"{prefix}family"),
                surface=_api._required_field(self.envelope, f"{prefix}surface"),
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                runtime_surface=_api._csv_values(self.envelope.field(f"{prefix}runtime_surface")),
                statistics_required=_api._csv_values(
                    self.envelope.field(f"{prefix}statistics_required")
                ),
                fixture_status=_api._required_field(self.envelope, f"{prefix}fixture_status"),
                correctness_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}correctness_refs")
                ),
                benchmark_refs=_api._csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                execution_certificate_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}execution_certificate_refs")
                ),
                native_io_refs=_api._csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                materialization_decode_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}materialization_decode_refs")
                ),
                unsupported_diagnostic_code=_api._required_field(
                    self.envelope,
                    f"{prefix}unsupported_diagnostic_code",
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                claim_boundary=_api._required_field(self.envelope, f"{prefix}claim_boundary"),
                execution_attempted=self.envelope.field_bool(
                    f"{prefix}execution_attempted",
                    True,
                )
                is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def ComputeCapabilityMatrix_materialization_policy_rows(self):

    rows: list[MaterializationPolicyRow] = []
    for row_id in _api._csv_values(self.envelope.field("materialization_policy_row_order")):
        prefix = f"materialization_policy_row_{row_id}_"
        rows.append(
            _api.MaterializationPolicyRow(
                row_id=row_id,
                operator_execution_class=_api._required_field(
                    self.envelope,
                    f"{prefix}operator_execution_class",
                ),
                support_status=_api._required_field(self.envelope, f"{prefix}support_status"),
                data_decoded=self.envelope.field_bool(f"{prefix}data_decoded", True) is True,
                data_materialized=self.envelope.field_bool(
                    f"{prefix}data_materialized",
                    True,
                )
                is True,
                stayed_encoded=self.envelope.field_bool(
                    f"{prefix}stayed_encoded",
                    False,
                )
                is True,
                materialization_boundary_required=self.envelope.field_bool(
                    f"{prefix}materialization_boundary_required",
                    False,
                )
                is True,
                materialization_boundary_emitted=self.envelope.field_bool(
                    f"{prefix}materialization_boundary_emitted",
                    False,
                )
                is True,
                materialized_temporary_path=self.envelope.field_bool(
                    f"{prefix}materialized_temporary_path",
                    False,
                )
                is True,
                encoded_native_claim_allowed=self.envelope.field_bool(
                    f"{prefix}encoded_native_claim_allowed",
                    False,
                )
                is True,
                materialization_decode_refs=_api._csv_values(
                    self.envelope.field(f"{prefix}materialization_decode_refs")
                ),
                policy_refs=_api._csv_values(self.envelope.field(f"{prefix}policy_refs")),
                unsupported_diagnostic_code=_api._required_field(
                    self.envelope,
                    f"{prefix}unsupported_diagnostic_code",
                ),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                claim_gate_status=_api._required_field(
                    self.envelope,
                    f"{prefix}claim_gate_status",
                ),
                claim_boundary=_api._required_field(self.envelope, f"{prefix}claim_boundary"),
                runtime_execution=self.envelope.field_bool(
                    f"{prefix}runtime_execution",
                    True,
                )
                is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_engine_invoked=self.envelope.field_bool(
                    f"{prefix}external_engine_invoked",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def SemanticConformanceSuite_rows(self):

    rows: list[SemanticConformanceRow] = []
    for row_id in _api._csv_values(self.envelope.field("row_order")):
        prefix = f"semantic_row_{row_id}_"
        rows.append(
            _api.SemanticConformanceRow(
                row_id=row_id,
                dimension=_api._required_field(self.envelope, f"{prefix}dimension"),
                operator_family=_api._required_field(
                    self.envelope,
                    f"{prefix}operator_family",
                ),
                fixture_status=_api._required_field(self.envelope, f"{prefix}fixture_status"),
                current_support=_api._required_field(self.envelope, f"{prefix}current_support"),
                assertion=_api._required_field(self.envelope, f"{prefix}assertion"),
                blocker_id=_api._required_field(self.envelope, f"{prefix}blocker_id"),
                required_future_evidence=_api._csv_values(
                    self.envelope.field(f"{prefix}required_future_evidence")
                ),
                fixture_executed=self.envelope.field_bool(
                    f"{prefix}fixture_executed",
                    False,
                )
                is True,
                passed=self.envelope.field_bool(f"{prefix}passed", False) is True,
                fallback_attempted=self.envelope.field_bool(
                    f"{prefix}fallback_attempted",
                    True,
                )
                is True,
                external_oracle_used=self.envelope.field_bool(
                    f"{prefix}external_oracle_used",
                    True,
                )
                is True,
            )
        )
    return tuple(rows)


def ExecutionResultEnvelopeView_execution_mode_selection_fields(self):

    artifact = next(
        (
            artifact
            for artifact in self.envelope.artifacts
            if artifact.get("artifact_kind") == "execution_mode_selection_report"
        ),
        None,
    )
    if artifact is not None:
        fields = _api._artifact_payload_field_map(artifact)
        if fields:
            return fields
    keys = (
        "execution_mode_selection_schema_version",
        "requested_execution_mode",
        "selected_execution_mode",
        "execution_mode",
        "mode_selection_reason",
        "execution_mode_family",
        "source_format",
        "workload_constitution_id",
        "compatibility_import_included",
        "vortex_prepare_included",
        "vortex_write_reopen_included",
        "direct_transient_execution",
        "vortex_native_claim_allowed",
        "certification_requested",
        "result_sink_requested",
        "prepared_artifact_available",
        "native_vortex_provider_available",
        "mode_supported",
        "support_status",
        "unsupported_diagnostic_code",
        "blocker_id",
        "required_future_evidence",
        "claim_gate_status",
        "claim_gate_reason",
        "fallback_attempted",
        "external_engine_invoked",
    )
    return {
        key: value
        for key in keys
        if (value := self.envelope.field(key)) is not None
    }


def ExecutionResultEnvelopeView_compute_flow_evidence_fields(self):

    artifact = next(
        (
            artifact
            for artifact in self.envelope.artifacts
            if artifact.get("artifact_kind") == "compute_flow_evidence"
        ),
        None,
    )
    if artifact is not None:
        fields = _api._artifact_payload_field_map(artifact)
        if fields:
            return fields
    return {}


def ExecutionResultEnvelopeView_facade_compatibility_matrix_fields(self):

    artifact = next(
        (
            artifact
            for artifact in self.envelope.artifacts
            if artifact.get("artifact_kind") == "facade_compatibility_matrix"
        ),
        None,
    )
    if artifact is not None:
        fields = _api._artifact_payload_field_map(artifact)
        if fields:
            return fields
    keys = (
        "facade_compatibility_matrix_report_id",
        "facade_compatibility_matrix_gar_id",
        "facade_compatibility_matrix_support_status",
        "facade_compatibility_matrix_claim_gate_status",
        "facade_compatibility_matrix_row_order",
        "facade_executable_surface_count",
        "facade_report_only_surface_count",
        "facade_unsupported_surface_count",
        "facade_legacy_boundary_count",
        "facade_prohibited_surface_count",
        "facade_legacy_boundary_status",
        "facade_all_rows_no_fallback_no_external_engine",
    )
    return {
        key: value
        for key in keys
        if (value := self.envelope.field(key)) is not None
    }


def ExecutionResultEnvelopeView__facade_matrix_field(self, key):
    fields = self.facade_compatibility_matrix_fields
    return (
        fields.get(key)
        or fields.get(f"facade_compatibility_matrix_{key}")
        or fields.get(f"facade_{key}")
        or self.envelope.field(f"facade_compatibility_matrix_{key}")
        or self.envelope.field(f"facade_{key}")
        or self.envelope.field(key)
    )


def ExecutionResultEnvelopeView_evidence_slots(self):

    artifact = next(
        (
            artifact
            for artifact in self.envelope.artifacts
            if artifact.get("artifact_kind") == "execution_evidence_slots"
        ),
        None,
    )
    if artifact is None:
        return ()
    fields = _api._artifact_payload_field_map(artifact)
    slots: list[ExecutionEvidenceSlot] = []
    for kind in _api._csv_values(fields.get("evidence_slot_order")):
        prefix = f"evidence_slot_{kind}_"
        slots.append(
            _api.ExecutionEvidenceSlot(
                kind=kind,
                status=fields.get(f"{prefix}status", ""),
                refs=_api._csv_values(fields.get(f"{prefix}refs")),
                detail=fields.get(f"{prefix}detail", ""),
            )
        )
    return tuple(slots)


def LiveFixtureRunReport_all_certified(self):

    return all(
        self.envelope.field(key) == "certified"
        for key in (
            "freshness_certificate_status",
            "state_certificate_status",
            "continuous_view_certificate_status",
            "execution_certificate_status",
            "native_io_certificate_status",
        )
    )


def HybridOverlayRunReport_all_certified(self):

    return all(
        self.envelope.field(key) == "certified"
        for key in (
            "delta_overlay_certificate_status",
            "micro_segment_flush_evidence_status",
            "freshness_certificate_status",
            "execution_certificate_status",
            "native_io_certificate_status",
        )
    )


def LiveHybridDurableCheckpointReport_all_certified(self):

    return all(
        self.envelope.field(key) == "certified"
        for key in (
            "freshness_certificate_status",
            "state_certificate_status",
            "execution_certificate_status",
            "native_io_certificate_status",
        )
    )


def PythonClientSmokeReport_feature_gates(self):

    values: list[str] = []
    for envelope in (
        self.status,
        self.python_capabilities,
        self.deployment_capabilities,
        self.input_adapters,
    ):
        for key in (
            "feature_gates",
            "enabled_feature_gates",
            "disabled_feature_gates",
            "surface_components",
        ):
            value = envelope.field(key)
            if value:
                values.extend(part.strip() for part in value.split(",") if part.strip())
    return tuple(dict.fromkeys(values))
