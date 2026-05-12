//! Vortex operational alignment facets.
//!
//! These report-only surfaces keep upstream Vortex-adjacent write, IO,
//! telemetry, compression, integrity, Python, runtime, and benchmark concepts
//! visible without widening `ShardLoom` execution support.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingSinkWriterMode {
    Pull,
    Push,
    Streaming,
}

impl StreamingSinkWriterMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pull => "pull",
            Self::Push => "push",
            Self::Streaming => "streaming",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StreamingSinkCertificate {
    pub schema_version: &'static str,
    pub certificate_id: &'static str,
    pub writer_mode: StreamingSinkWriterMode,
    pub flush_policy: &'static str,
    pub buffered_rows: u64,
    pub buffered_bytes: u64,
    pub emitted_micro_segments: u64,
    pub compression_strategy: &'static str,
    pub backpressure_state: &'static str,
    pub sink_commit_status: &'static str,
    pub recovery_status: &'static str,
    pub output_manifest_ref: Option<&'static str>,
    pub streaming_sink_support_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl StreamingSinkCertificate {
    #[must_use]
    pub const fn report_only_future() -> Self {
        Self {
            schema_version: "shardloom.streaming_sink_certificate.v1",
            certificate_id: "cg19.streaming_sink.report_only_future",
            writer_mode: StreamingSinkWriterMode::Streaming,
            flush_policy: "explicit_flush_required_before_support_claims",
            buffered_rows: 0,
            buffered_bytes: 0,
            emitted_micro_segments: 0,
            compression_strategy: "not_selected",
            backpressure_state: "not_executed",
            sink_commit_status: "not_implemented",
            recovery_status: "not_implemented",
            output_manifest_ref: None,
            streaming_sink_support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.streaming_sink_support_claim_allowed
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackendKind {
    LocalFile,
    ObjectStore,
    FoundryS3Dataset,
}

impl IoBackendKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalFile => "local_file",
            Self::ObjectStore => "object_store",
            Self::FoundryS3Dataset => "foundry_s3_dataset",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IoBackendEvidence {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub backend_kind: IoBackendKind,
    pub read_at_count: u64,
    pub object_request_count: u64,
    pub coalesced_request_count: u64,
    pub requested_bytes: u64,
    pub returned_bytes: u64,
    pub useful_bytes: u64,
    pub read_amplification_ratio: Option<&'static str>,
    pub prefetch_registered: u64,
    pub prefetch_resolved: u64,
    pub prefetch_dropped: u64,
    pub segment_cache_hits: u64,
    pub segment_cache_misses: u64,
    pub backend_concurrency: Option<u32>,
    pub coalescing_policy: &'static str,
    pub sub_segment_read_supported: bool,
    pub io_performed: bool,
    pub object_store_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl IoBackendEvidence {
    #[must_use]
    pub const fn object_store_report_only() -> Self {
        Self {
            schema_version: "shardloom.io_backend_evidence.v1",
            report_id: "cg10.cg19.io_backend.object_store_report_only",
            backend_kind: IoBackendKind::ObjectStore,
            read_at_count: 0,
            object_request_count: 0,
            coalesced_request_count: 0,
            requested_bytes: 0,
            returned_bytes: 0,
            useful_bytes: 0,
            read_amplification_ratio: None,
            prefetch_registered: 0,
            prefetch_resolved: 0,
            prefetch_dropped: 0,
            segment_cache_hits: 0,
            segment_cache_misses: 0,
            backend_concurrency: None,
            coalescing_policy: "not_executed",
            sub_segment_read_supported: false,
            io_performed: false,
            object_store_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.object_store_claim_allowed
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionTelemetryFacet {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub trace_id: Option<&'static str>,
    pub span_refs: Vec<&'static str>,
    pub operator_metric_refs: Vec<&'static str>,
    pub io_metric_refs: Vec<&'static str>,
    pub certificate_refs: Vec<&'static str>,
    pub profile_refs: Vec<&'static str>,
    pub perfetto_trace_ref: Option<&'static str>,
    pub telemetry_required_for_performance_claims: bool,
    pub performance_or_cost_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ExecutionTelemetryFacet {
    #[must_use]
    pub fn report_only_empty() -> Self {
        Self {
            schema_version: "shardloom.execution_telemetry_facet.v1",
            report_id: "cg16.cg23.execution_telemetry.report_only",
            trace_id: None,
            span_refs: Vec::new(),
            operator_metric_refs: Vec::new(),
            io_metric_refs: Vec::new(),
            certificate_refs: Vec::new(),
            profile_refs: Vec::new(),
            perfetto_trace_ref: None,
            telemetry_required_for_performance_claims: true,
            performance_or_cost_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.performance_or_cost_claim_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ApproxAnalyticsCertificate {
    pub schema_version: &'static str,
    pub certificate_id: &'static str,
    pub operation: &'static str,
    pub exact_reference_required: bool,
    pub error_bound_required: bool,
    pub confidence_required: bool,
    pub exact_reference_status: &'static str,
    pub error_bound_status: &'static str,
    pub approximate_query_answer_claim_allowed: bool,
    pub used_as_exact_correctness_evidence: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ApproxAnalyticsCertificate {
    #[must_use]
    pub const fn blocked_query_answer() -> Self {
        Self {
            schema_version: "shardloom.approx_analytics_certificate.v1",
            certificate_id: "cg20.approx_analytics.query_answer_blocked",
            operation: "approx_count_distinct",
            exact_reference_required: true,
            error_bound_required: true,
            confidence_required: true,
            exact_reference_status: "missing",
            error_bound_status: "missing",
            approximate_query_answer_claim_allowed: false,
            used_as_exact_correctness_evidence: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.approximate_query_answer_claim_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CompressionAdvisorReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub approximate_cardinality: Option<u64>,
    pub null_count: Option<u64>,
    pub run_count: Option<u64>,
    pub sortedness: Option<&'static str>,
    pub value_width: Option<u32>,
    pub string_length_distribution: Option<&'static str>,
    pub selected_encoding: Option<&'static str>,
    pub rejected_encodings: Vec<&'static str>,
    pub estimated_size: Option<u64>,
    pub observed_size: Option<u64>,
    pub confidence_basis_points: Option<u16>,
    pub encoding_choice_applied: bool,
    pub used_as_exact_correctness_evidence: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl CompressionAdvisorReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.compression_advisor_report.v1",
            report_id: "cg13.cg19.compression_advisor.report_only",
            approximate_cardinality: None,
            null_count: None,
            run_count: None,
            sortedness: None,
            value_width: None,
            string_length_distribution: None,
            selected_encoding: None,
            rejected_encodings: Vec::new(),
            estimated_size: None,
            observed_size: None,
            confidence_basis_points: None,
            encoding_choice_applied: false,
            used_as_exact_correctness_evidence: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn correctness_claim_blocked(&self) -> bool {
        !self.used_as_exact_correctness_evidence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IntegrityAndEncryptionReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub checksum_present: bool,
    pub checksum_verified: bool,
    pub encryption_present: bool,
    pub encryption_supported: bool,
    pub key_policy_ref: Option<&'static str>,
    pub decrypted_boundary: &'static str,
    pub integrity_error_policy: &'static str,
    pub unsupported_encryption_diagnostic: &'static str,
    pub hidden_plaintext_materialization_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl IntegrityAndEncryptionReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: "shardloom.integrity_and_encryption_report.v1",
            report_id: "cg19.cg20.integrity_encryption.report_only",
            checksum_present: false,
            checksum_verified: false,
            encryption_present: false,
            encryption_supported: false,
            key_policy_ref: None,
            decrypted_boundary: "none",
            integrity_error_policy: "deterministic_unsupported_or_failed",
            unsupported_encryption_diagnostic: "encrypted_artifact_requires_policy_and_certificate",
            hidden_plaintext_materialization_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct PythonVortexInteropReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub shardloom_package_version: &'static str,
    pub vortex_data_package_version: Option<&'static str>,
    pub python_version_policy: &'static str,
    pub import_side_effects: bool,
    pub conversion_boundaries: Vec<&'static str>,
    pub materialization_boundaries: Vec<&'static str>,
    pub optional_extras_detected: Vec<&'static str>,
    pub pyvortex_required_for_shardloom_import: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl PythonVortexInteropReport {
    #[must_use]
    pub fn optional_pyvortex() -> Self {
        Self {
            schema_version: "shardloom.python_vortex_interop_report.v1",
            report_id: "cg20.python_vortex_interop.optional_pyvortex",
            shardloom_package_version: "source_or_package_version_reported_by_wrapper",
            vortex_data_package_version: None,
            python_version_policy: "shardloom_import_independent; vortex-data_requires_python_3.11_plus_when_used",
            import_side_effects: false,
            conversion_boundaries: vec!["explicit_source_sink_or_test_reference_boundary"],
            materialization_boundaries: vec!["python_arrow_pandas_conversion_is_explicit"],
            optional_extras_detected: Vec::new(),
            pyvortex_required_for_shardloom_import: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignRuntimeStatus {
    PythonFirst,
    Deferred,
}

impl ForeignRuntimeStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PythonFirst => "python_first",
            Self::Deferred => "deferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ForeignRuntimeSurface {
    pub runtime: &'static str,
    pub status: ForeignRuntimeStatus,
    pub certificate_semantics_required: bool,
    pub no_fallback_required: bool,
    pub runtime_support_claim_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ForeignRuntimePosture {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub surfaces: Vec<ForeignRuntimeSurface>,
    pub python_conda_current_priority: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ForeignRuntimePosture {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.foreign_runtime_posture.v1",
            report_id: "cg20.foreign_runtime_posture",
            surfaces: vec![
                ForeignRuntimeSurface {
                    runtime: "python_conda",
                    status: ForeignRuntimeStatus::PythonFirst,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: true,
                },
                ForeignRuntimeSurface {
                    runtime: "c_ffi",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
                ForeignRuntimeSurface {
                    runtime: "cpp_wrapper",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
                ForeignRuntimeSurface {
                    runtime: "java_jvm",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
                ForeignRuntimeSurface {
                    runtime: "wasm_component",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
                ForeignRuntimeSurface {
                    runtime: "arrow_c_stream_pycapsule",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
                ForeignRuntimeSurface {
                    runtime: "adbc_flight",
                    status: ForeignRuntimeStatus::Deferred,
                    certificate_semantics_required: true,
                    no_fallback_required: true,
                    runtime_support_claim_allowed: false,
                },
            ],
            python_conda_current_priority: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn deferred_runtime_names(&self) -> Vec<&'static str> {
        self.surfaces
            .iter()
            .filter(|surface| surface.status == ForeignRuntimeStatus::Deferred)
            .map(|surface| surface.runtime)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexBenchmarkInteropRow {
    pub scenario_name: &'static str,
    pub input_format: &'static str,
    pub engine: &'static str,
    pub file_format: &'static str,
    pub startup_policy: &'static str,
    pub conversion_policy: &'static str,
    pub result_policy: &'static str,
    pub correctness_oracle: &'static str,
    pub shardloom_native_execution: bool,
    pub vortex_integration_execution: bool,
    pub measured_result_present: bool,
    pub performance_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexBenchmarkInteropRow {
    fn shardloom_native_planned() -> Self {
        Self {
            scenario_name: "vortex_foundation_scenario",
            input_format: "vortex",
            engine: "shardloom",
            file_format: "vortex",
            startup_policy: "recorded_when_benchmark_runs",
            conversion_policy: "none_for_native_vortex_input",
            result_policy: "certificate_linked_result_or_artifact_ref",
            correctness_oracle: "shardloom_correctness_harness_required",
            shardloom_native_execution: true,
            vortex_integration_execution: false,
            measured_result_present: false,
            performance_claim_allowed: false,
            fallback_attempted: false,
        }
    }

    fn vortex_integration_baseline(engine: &'static str) -> Self {
        Self {
            scenario_name: "vortex_foundation_scenario",
            input_format: "vortex",
            engine,
            file_format: "vortex",
            startup_policy: "recorded_when_benchmark_runs",
            conversion_policy: "integration_policy_recorded_as_baseline",
            result_policy: "baseline_result_not_shardloom_execution",
            correctness_oracle: "comparison_only",
            shardloom_native_execution: false,
            vortex_integration_execution: true,
            measured_result_present: false,
            performance_claim_allowed: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexBenchmarkInterop {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<VortexBenchmarkInteropRow>,
    pub upstream_vortex_results_are_shardloom_claims: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexBenchmarkInterop {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_benchmark_interop.v1",
            report_id: "cg6.vortex_benchmark_interop",
            rows: vec![
                VortexBenchmarkInteropRow::shardloom_native_planned(),
                VortexBenchmarkInteropRow::vortex_integration_baseline("vortex_datafusion"),
                VortexBenchmarkInteropRow::vortex_integration_baseline("vortex_duckdb"),
            ],
            upstream_vortex_results_are_shardloom_claims: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn integration_baselines(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.vortex_integration_execution)
            .map(|row| row.engine)
            .collect()
    }

    #[must_use]
    pub fn all_claims_blocked(&self) -> bool {
        !self.upstream_vortex_results_are_shardloom_claims
            && self
                .rows
                .iter()
                .all(|row| !row.performance_claim_allowed && !row.fallback_attempted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexOperationalHardeningReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub streaming_sink_certificate: StreamingSinkCertificate,
    pub io_backend_evidence: IoBackendEvidence,
    pub execution_telemetry_facet: ExecutionTelemetryFacet,
    pub approx_analytics_certificate: ApproxAnalyticsCertificate,
    pub compression_advisor_report: CompressionAdvisorReport,
    pub integrity_and_encryption_report: IntegrityAndEncryptionReport,
    pub python_vortex_interop_report: PythonVortexInteropReport,
    pub foreign_runtime_posture: ForeignRuntimePosture,
    pub vortex_benchmark_interop: VortexBenchmarkInterop,
    pub docs_report_only: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexOperationalHardeningReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_operational_hardening.v1",
            report_id: "priority_2_5.vortex_operational_hardening",
            streaming_sink_certificate: StreamingSinkCertificate::report_only_future(),
            io_backend_evidence: IoBackendEvidence::object_store_report_only(),
            execution_telemetry_facet: ExecutionTelemetryFacet::report_only_empty(),
            approx_analytics_certificate: ApproxAnalyticsCertificate::blocked_query_answer(),
            compression_advisor_report: CompressionAdvisorReport::report_only(),
            integrity_and_encryption_report: IntegrityAndEncryptionReport::report_only(),
            python_vortex_interop_report: PythonVortexInteropReport::optional_pyvortex(),
            foreign_runtime_posture: ForeignRuntimePosture::current(),
            vortex_benchmark_interop: VortexBenchmarkInterop::current(),
            docs_report_only: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn report_only_claims_blocked(&self) -> bool {
        self.docs_report_only
            && self.streaming_sink_certificate.claim_blocked()
            && self.io_backend_evidence.claim_blocked()
            && self.execution_telemetry_facet.claim_blocked()
            && self.approx_analytics_certificate.claim_blocked()
            && self.compression_advisor_report.correctness_claim_blocked()
            && self.vortex_benchmark_interop.all_claims_blocked()
            && !self
                .integrity_and_encryption_report
                .hidden_plaintext_materialization_allowed
    }

    #[must_use]
    pub fn fallback_free(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self.streaming_sink_certificate.fallback_free()
            && self.io_backend_evidence.fallback_free()
            && !self.execution_telemetry_facet.external_engine_invoked
            && !self.execution_telemetry_facet.fallback_attempted
            && !self.approx_analytics_certificate.external_engine_invoked
            && !self.approx_analytics_certificate.fallback_attempted
            && !self.compression_advisor_report.external_engine_invoked
            && !self.compression_advisor_report.fallback_attempted
            && !self.integrity_and_encryption_report.external_engine_invoked
            && !self.integrity_and_encryption_report.fallback_attempted
            && !self.python_vortex_interop_report.external_engine_invoked
            && !self.python_vortex_interop_report.fallback_attempted
            && !self.foreign_runtime_posture.external_engine_invoked
            && !self.foreign_runtime_posture.fallback_attempted
            && !self.vortex_benchmark_interop.external_engine_invoked
            && !self.vortex_benchmark_interop.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "vortex operational hardening\nschema_version: {}\nreport: {}\nsurfaces: 9\nreport only: {}\nclaims blocked: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.docs_report_only,
            self.report_only_claims_blocked(),
        )
    }
}

#[must_use]
pub fn plan_vortex_operational_hardening_report() -> VortexOperationalHardeningReport {
    VortexOperationalHardeningReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_facets_are_report_only_and_fallback_free() {
        let report = plan_vortex_operational_hardening_report();

        assert!(report.docs_report_only);
        assert!(report.report_only_claims_blocked());
        assert!(report.fallback_free());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn streaming_sink_and_object_store_claims_are_blocked_without_evidence() {
        let report = plan_vortex_operational_hardening_report();

        assert_eq!(
            report.streaming_sink_certificate.writer_mode,
            StreamingSinkWriterMode::Streaming
        );
        assert_eq!(
            report.streaming_sink_certificate.sink_commit_status,
            "not_implemented"
        );
        assert!(report.streaming_sink_certificate.claim_blocked());
        assert_eq!(
            report.io_backend_evidence.backend_kind,
            IoBackendKind::ObjectStore
        );
        assert!(!report.io_backend_evidence.io_performed);
        assert!(report.io_backend_evidence.claim_blocked());
    }

    #[test]
    fn approximate_query_answers_stay_separate_from_compression_advice() {
        let report = plan_vortex_operational_hardening_report();

        assert_eq!(
            report.approx_analytics_certificate.operation,
            "approx_count_distinct"
        );
        assert!(report.approx_analytics_certificate.exact_reference_required);
        assert!(report.approx_analytics_certificate.error_bound_required);
        assert!(report.approx_analytics_certificate.claim_blocked());
        assert!(!report.compression_advisor_report.encoding_choice_applied);
        assert!(
            report
                .compression_advisor_report
                .correctness_claim_blocked()
        );
        assert!(
            !report
                .compression_advisor_report
                .used_as_exact_correctness_evidence
        );
    }

    #[test]
    fn pyvortex_and_foreign_runtimes_preserve_python_first_optional_posture() {
        let report = plan_vortex_operational_hardening_report();

        assert!(!report.python_vortex_interop_report.import_side_effects);
        assert!(
            !report
                .python_vortex_interop_report
                .pyvortex_required_for_shardloom_import
        );
        assert!(report.foreign_runtime_posture.python_conda_current_priority);
        assert!(
            report
                .foreign_runtime_posture
                .deferred_runtime_names()
                .contains(&"adbc_flight")
        );
    }

    #[test]
    fn vortex_benchmarks_distinguish_shardloom_rows_from_integration_baselines() {
        let report = plan_vortex_operational_hardening_report();
        let benchmarks = &report.vortex_benchmark_interop;

        assert_eq!(
            benchmarks.integration_baselines(),
            vec!["vortex_datafusion", "vortex_duckdb"]
        );
        assert!(
            benchmarks
                .rows
                .iter()
                .any(|row| row.shardloom_native_execution && row.engine == "shardloom")
        );
        assert!(
            benchmarks
                .rows
                .iter()
                .filter(|row| row.vortex_integration_execution)
                .all(|row| !row.shardloom_native_execution && !row.performance_claim_allowed)
        );
        assert!(benchmarks.all_claims_blocked());
    }
}
