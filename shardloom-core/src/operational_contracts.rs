//! Cross-surface operational contracts.
//!
//! These report-only contracts keep evidence artifacts, policies, lifecycle
//! states, protocol parity, workload constitutions, semantic profiles,
//! standards decisions, benchmark constitutions, cost simulations, and Rust
//! build evidence aligned before runtime/API/client work fans out.

#![allow(clippy::struct_excessive_bools)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceArtifactEnvelope {
    pub artifact_id: String,
    pub artifact_type: &'static str,
    pub schema_version: &'static str,
    pub producer_component: &'static str,
    pub engine_version: &'static str,
    pub protocol_version: &'static str,
    pub created_at: &'static str,
    pub workload_constitution_ref: Option<String>,
    pub plan_id: Option<String>,
    pub query_id: Option<String>,
    pub run_id: Option<String>,
    pub input_refs: Vec<String>,
    pub output_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub policy_refs: Vec<String>,
    pub redaction_policy: &'static str,
    pub retention_policy: &'static str,
    pub invalidation_refs: Vec<String>,
    pub digest: Option<String>,
    pub diagnostics: Vec<String>,
    pub fallback_attempted: bool,
}

impl EvidenceArtifactEnvelope {
    #[must_use]
    pub fn report_only(artifact_id: impl Into<String>, artifact_type: &'static str) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            artifact_type,
            schema_version: "shardloom.evidence_artifact_envelope.v1",
            producer_component: "shardloom-core",
            engine_version: "source_tree",
            protocol_version: "shardloom.protocol.v1",
            created_at: "not_recorded_for_report_only_contract",
            workload_constitution_ref: None,
            plan_id: None,
            query_id: None,
            run_id: None,
            input_refs: Vec::new(),
            output_refs: Vec::new(),
            evidence_refs: Vec::new(),
            policy_refs: Vec::new(),
            redaction_policy: "strict",
            retention_policy: "explicit_required_before_export",
            invalidation_refs: Vec::new(),
            digest: None,
            diagnostics: Vec::new(),
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn has_required_identity(&self) -> bool {
        !self.artifact_id.trim().is_empty()
            && !self.artifact_type.trim().is_empty()
            && !self.schema_version.trim().is_empty()
            && !self.producer_component.trim().is_empty()
            && !self.protocol_version.trim().is_empty()
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceArtifactSafety {
    pub data_classification: &'static str,
    pub contains_user_values: bool,
    pub contains_paths: bool,
    pub contains_credentials: bool,
    pub contains_samples: bool,
    pub contains_query_text: bool,
    pub contains_schema_names: bool,
    pub redaction_policy: &'static str,
    pub retention_policy: &'static str,
    pub export_allowed: bool,
    pub agent_visible: bool,
}

impl EvidenceArtifactSafety {
    #[must_use]
    pub const fn strict_default() -> Self {
        Self {
            data_classification: "unclassified_until_declared",
            contains_user_values: false,
            contains_paths: false,
            contains_credentials: false,
            contains_samples: false,
            contains_query_text: false,
            contains_schema_names: false,
            redaction_policy: "strict",
            retention_policy: "explicit_required_before_export",
            export_allowed: false,
            agent_visible: false,
        }
    }

    #[must_use]
    pub const fn safe_for_agent_default(&self) -> bool {
        !self.contains_credentials
            && !self.contains_user_values
            && !self.contains_samples
            && !self.export_allowed
            && !self.agent_visible
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomExecutionPolicy {
    pub schema_version: &'static str,
    pub policy_id: &'static str,
    pub requested_engine: &'static str,
    pub allowed_engines: Vec<&'static str>,
    pub fallback_policy: &'static str,
    pub materialization_policy: &'static str,
    pub decode_policy: &'static str,
    pub result_delivery_policy: &'static str,
    pub evidence_policy: &'static str,
    pub effect_policy: &'static str,
    pub credential_policy: &'static str,
    pub redaction_policy: &'static str,
    pub retention_policy: &'static str,
    pub memory_policy: &'static str,
    pub spill_policy: &'static str,
    pub network_policy: &'static str,
    pub destructive_operation_policy: &'static str,
    pub benchmark_policy: &'static str,
    pub agent_policy: &'static str,
    pub fallback_attempted: bool,
}

impl ShardLoomExecutionPolicy {
    #[must_use]
    pub fn safe_default() -> Self {
        Self {
            schema_version: "shardloom.execution_policy.v1",
            policy_id: "shardloom.execution_policy.safe_default",
            requested_engine: "batch",
            allowed_engines: vec!["batch"],
            fallback_policy: "deny",
            materialization_policy: "explicit_only",
            decode_policy: "deny_unless_boundary_reported",
            result_delivery_policy: "inline_diagnostics_or_artifact_ref",
            evidence_policy: "certificates_required_for_support_claims",
            effect_policy: "deny",
            credential_policy: "references_only",
            redaction_policy: "strict",
            retention_policy: "explicit_required",
            memory_policy: "bounded_or_blocked",
            spill_policy: "deny_unless_certified",
            network_policy: "deny",
            destructive_operation_policy: "deny",
            benchmark_policy: "comparison_only_until_claim_evidence",
            agent_policy: "read_only_dry_run_default",
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn denies_unsafe_defaults(&self) -> bool {
        self.fallback_policy == "deny"
            && self.effect_policy == "deny"
            && self.network_policy == "deny"
            && self.destructive_operation_policy == "deny"
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLifecycleState {
    Accepted,
    Validating,
    Planned,
    Blocked,
    Queued,
    Running,
    Cancelling,
    Cancelled,
    Failed,
    Succeeded,
    Expired,
}

impl QueryLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Validating => "validating",
            Self::Planned => "planned",
            Self::Blocked => "blocked",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::Succeeded => "succeeded",
            Self::Expired => "expired",
        }
    }

    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Accepted,
            Self::Validating,
            Self::Planned,
            Self::Blocked,
            Self::Queued,
            Self::Running,
            Self::Cancelling,
            Self::Cancelled,
            Self::Failed,
            Self::Succeeded,
            Self::Expired,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLifecycleContract {
    pub schema_version: &'static str,
    pub query_id: String,
    pub state: QueryLifecycleState,
    pub idempotency_key: Option<String>,
    pub plan_id: Option<String>,
    pub selected_engine: &'static str,
    pub policy_ref: String,
    pub cancellation_mode: &'static str,
    pub retry_policy: &'static str,
    pub result_retention: &'static str,
    pub certificate_retention: &'static str,
    pub cleanup_status: &'static str,
    pub side_effect_status: &'static str,
    pub ambiguous_commit_status: &'static str,
    pub fallback_attempted: bool,
}

impl QueryLifecycleContract {
    #[must_use]
    pub fn blocked_unsupported(query_id: impl Into<String>) -> Self {
        Self {
            schema_version: "shardloom.query_lifecycle_contract.v1",
            query_id: query_id.into(),
            state: QueryLifecycleState::Blocked,
            idempotency_key: None,
            plan_id: None,
            selected_engine: "none",
            policy_ref: "shardloom.execution_policy.safe_default".to_string(),
            cancellation_mode: "not_started",
            retry_policy: "disabled_for_unsupported",
            result_retention: "none",
            certificate_retention: "explicit_required",
            cleanup_status: "not_required",
            side_effect_status: "not_attempted",
            ambiguous_commit_status: "none",
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn is_safe_blocked_state(&self) -> bool {
        self.state == QueryLifecycleState::Blocked
            && self.side_effect_status == "not_attempted"
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolSurfaceParityRow {
    pub surface: &'static str,
    pub field_mapping_status: &'static str,
    pub unsupported_field_mappings: Vec<&'static str>,
    pub diagnostics_mapped: bool,
    pub certificate_refs_mapped: bool,
    pub result_policy_mapped: bool,
    pub fallback_field_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolSurfaceParityReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<ProtocolSurfaceParityRow>,
    pub known_surface_gaps: Vec<&'static str>,
    pub diagnostics: Vec<String>,
    pub fallback_attempted: bool,
}

impl ProtocolSurfaceParityReport {
    #[must_use]
    pub fn current_contract() -> Self {
        Self {
            schema_version: "shardloom.protocol_surface_parity_report.v1",
            report_id: "cg23.protocol_surface_parity",
            rows: vec![
                ProtocolSurfaceParityRow {
                    surface: "cli_json",
                    field_mapping_status: "current",
                    unsupported_field_mappings: Vec::new(),
                    diagnostics_mapped: true,
                    certificate_refs_mapped: true,
                    result_policy_mapped: true,
                    fallback_field_visible: true,
                },
                ProtocolSurfaceParityRow {
                    surface: "python_wrapper",
                    field_mapping_status: "partial_current",
                    unsupported_field_mappings: vec!["future_rest_query_lifecycle"],
                    diagnostics_mapped: true,
                    certificate_refs_mapped: true,
                    result_policy_mapped: true,
                    fallback_field_visible: true,
                },
                ProtocolSurfaceParityRow {
                    surface: "rest_openapi",
                    field_mapping_status: "future_contract",
                    unsupported_field_mappings: vec!["server_not_implemented"],
                    diagnostics_mapped: false,
                    certificate_refs_mapped: false,
                    result_policy_mapped: false,
                    fallback_field_visible: true,
                },
                ProtocolSurfaceParityRow {
                    surface: "mcp_resources",
                    field_mapping_status: "future_contract",
                    unsupported_field_mappings: vec!["mcp_server_not_implemented"],
                    diagnostics_mapped: false,
                    certificate_refs_mapped: false,
                    result_policy_mapped: false,
                    fallback_field_visible: true,
                },
                ProtocolSurfaceParityRow {
                    surface: "flight_adbc_metadata",
                    field_mapping_status: "future_contract",
                    unsupported_field_mappings: vec!["data_plane_not_implemented"],
                    diagnostics_mapped: false,
                    certificate_refs_mapped: false,
                    result_policy_mapped: false,
                    fallback_field_visible: true,
                },
            ],
            known_surface_gaps: vec!["rest_openapi", "mcp_resources", "flight_adbc_metadata"],
            diagnostics: Vec::new(),
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn all_fallback_fields_visible(&self) -> bool {
        !self.fallback_attempted && self.rows.iter().all(|row| row.fallback_field_visible)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadConstitutionEntry {
    pub workload_id: &'static str,
    pub required_sources: Vec<&'static str>,
    pub required_sinks: Vec<&'static str>,
    pub required_operators: Vec<&'static str>,
    pub required_functions: Vec<&'static str>,
    pub semantic_profile: &'static str,
    pub allowed_engine_modes: Vec<&'static str>,
    pub allowed_materialization_boundaries: Vec<&'static str>,
    pub required_certificates: Vec<&'static str>,
    pub required_correctness_fixtures: Vec<&'static str>,
    pub required_benchmark_scenarios: Vec<&'static str>,
    pub required_governance_policy: &'static str,
    pub claim_level: &'static str,
    pub disallowed_effects: Vec<&'static str>,
    pub fallback_attempted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadConstitutionCatalog {
    pub schema_version: &'static str,
    pub entries: Vec<WorkloadConstitutionEntry>,
}

impl WorkloadConstitutionCatalog {
    #[must_use]
    pub fn starter() -> Self {
        let entry = |workload_id| WorkloadConstitutionEntry {
            workload_id,
            required_sources: vec!["declared_per_workload"],
            required_sinks: vec!["declared_per_workload"],
            required_operators: vec!["declared_per_workload"],
            required_functions: Vec::new(),
            semantic_profile: "shardloom_native",
            allowed_engine_modes: vec!["batch"],
            allowed_materialization_boundaries: vec!["explicit_only"],
            required_certificates: vec!["execution_certificate", "native_io_certificate"],
            required_correctness_fixtures: vec!["workload_fixture_manifest"],
            required_benchmark_scenarios: vec!["workload_benchmark_constitution"],
            required_governance_policy: "strict_default",
            claim_level: "not_claim_grade",
            disallowed_effects: vec!["fallback_execution", "unreported_materialization"],
            fallback_attempted: false,
        };

        Self {
            schema_version: "shardloom.workload_constitution_catalog.v1",
            entries: vec![
                entry("local_vortex_primitives"),
                entry("local_file_etl"),
                entry("conda_import_smoke"),
                entry("python_dataframe_local_etl"),
                entry("rest_discovery_only"),
                entry("batch_vortex_analytics"),
                entry("hybrid_base_delta_fixture"),
                entry("adapter_vortex_read_write_local"),
                entry("traditional_analytics_benchmark"),
            ],
        }
    }

    #[must_use]
    pub fn workload_ids(&self) -> Vec<&'static str> {
        self.entries.iter().map(|entry| entry.workload_id).collect()
    }

    #[must_use]
    pub fn all_non_claim_grade(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| entry.claim_level == "not_claim_grade" && !entry.fallback_attempted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomNativeSemanticDimension {
    pub dimension: &'static str,
    pub default_rule: &'static str,
    pub implementation_evidence_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomNativeSemanticProfile {
    pub schema_version: &'static str,
    pub profile_name: &'static str,
    pub dimensions: Vec<ShardLoomNativeSemanticDimension>,
    pub production_sql_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl ShardLoomNativeSemanticProfile {
    #[must_use]
    pub fn floor() -> Self {
        let dim = |dimension, default_rule| ShardLoomNativeSemanticDimension {
            dimension,
            default_rule,
            implementation_evidence_required: true,
        };
        Self {
            schema_version: "shardloom.semantic_profile.shardloom_native.v1",
            profile_name: "shardloom_native",
            dimensions: vec![
                dim("null_comparison", "three_valued_logic"),
                dim("null_sort_ordering", "explicit_per_sort_key_required"),
                dim(
                    "nan_equality_ordering",
                    "explicit_numeric_semantics_required",
                ),
                dim("signed_zero", "explicit_numeric_semantics_required"),
                dim("integer_overflow", "checked_or_declared_wrapping_required"),
                dim(
                    "decimal_precision_scale",
                    "declared_precision_scale_required",
                ),
                dim("timestamp_unit_timezone", "declared_unit_timezone_required"),
                dim("date_parsing", "strict_declared_formats"),
                dim(
                    "string_collation",
                    "binary_by_default_until_collation_support",
                ),
                dim("case_sensitivity", "case_sensitive_identifiers_by_default"),
                dim("binary_equality", "bytewise"),
                dim("empty_aggregate_behavior", "sql_profile_explicit"),
                dim(
                    "count_null_behavior",
                    "count_star_vs_count_expr_distinguished",
                ),
                dim("join_null_semantics", "three_valued_join_predicates"),
                dim("window_frame_defaults", "explicit_default_table_required"),
                dim("duplicate_column_behavior", "diagnostic_required"),
                dim("nested_list_equality", "profile_defined_before_execution"),
                dim(
                    "schema_field_identity",
                    "name_and_optional_field_id_policy_required",
                ),
            ],
            production_sql_claim_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn dimension_names(&self) -> Vec<&'static str> {
        self.dimensions
            .iter()
            .map(|dimension| dimension.dimension)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardsDecisionStatus {
    ReferenceOnly,
    SchemaOnly,
    OptionalFeatureCandidate,
    ApprovedDependencyRequired,
    Rejected,
}

impl StandardsDecisionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReferenceOnly => "reference_only",
            Self::SchemaOnly => "schema_only",
            Self::OptionalFeatureCandidate => "optional_feature_candidate",
            Self::ApprovedDependencyRequired => "approved_dependency_required",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardsDependencyDecision {
    pub standard_or_system: &'static str,
    pub status: StandardsDecisionStatus,
    pub dependency_approved: bool,
    pub runtime_use_allowed: bool,
    pub fallback_role_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardsDependencyDecisionReport {
    pub schema_version: &'static str,
    pub rows: Vec<StandardsDependencyDecision>,
}

impl StandardsDependencyDecisionReport {
    #[must_use]
    pub fn current() -> Self {
        let reference = |standard_or_system| StandardsDependencyDecision {
            standard_or_system,
            status: StandardsDecisionStatus::ReferenceOnly,
            dependency_approved: false,
            runtime_use_allowed: false,
            fallback_role_allowed: false,
        };
        Self {
            schema_version: "shardloom.standards_dependency_decision.v1",
            rows: vec![
                StandardsDependencyDecision {
                    standard_or_system: "openapi",
                    status: StandardsDecisionStatus::SchemaOnly,
                    dependency_approved: false,
                    runtime_use_allowed: false,
                    fallback_role_allowed: false,
                },
                StandardsDependencyDecision {
                    standard_or_system: "asyncapi",
                    status: StandardsDecisionStatus::SchemaOnly,
                    dependency_approved: false,
                    runtime_use_allowed: false,
                    fallback_role_allowed: false,
                },
                reference("cloudevents"),
                reference("opentelemetry_otlp"),
                reference("openlineage"),
                reference("arrow_flight_adbc"),
                reference("arrow_c_stream_pycapsule"),
                reference("iceberg_rest_polaris_gravitino"),
                reference("delta_sharing"),
                reference("substrait"),
                reference("wasi_webassembly_components"),
                reference("mcp"),
                reference("kafka_nats_redpanda_paimon_fluss"),
            ],
        }
    }

    #[must_use]
    pub fn all_runtime_blocked_until_approved(&self) -> bool {
        self.rows.iter().all(|row| {
            !row.runtime_use_allowed && !row.dependency_approved && !row.fallback_role_allowed
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkConstitution {
    pub schema_version: &'static str,
    pub workload_constitution_ref: &'static str,
    pub engine_mode: &'static str,
    pub input_format: &'static str,
    pub native_vortex_or_compatibility_import: &'static str,
    pub startup_included: bool,
    pub conversion_included: bool,
    pub result_delivery_included: bool,
    pub cache_policy: &'static str,
    pub object_store_policy: &'static str,
    pub warmup_policy: &'static str,
    pub iterations: Option<u32>,
    pub correctness_oracle: &'static str,
    pub result_materialization_policy: &'static str,
    pub api_transport_policy: &'static str,
    pub resource_limits: &'static str,
    pub cost_assumptions: &'static str,
    pub claim_level: &'static str,
    pub fallback_attempted: bool,
}

impl BenchmarkConstitution {
    #[must_use]
    pub const fn report_only_foundation() -> Self {
        Self {
            schema_version: "shardloom.benchmark_constitution.v1",
            workload_constitution_ref: "traditional_analytics_benchmark",
            engine_mode: "batch",
            input_format: "declared_per_row",
            native_vortex_or_compatibility_import: "declared_per_row",
            startup_included: true,
            conversion_included: true,
            result_delivery_included: true,
            cache_policy: "declared_cold_or_warm",
            object_store_policy: "not_used_until_object_store_evidence",
            warmup_policy: "declared_per_engine",
            iterations: None,
            correctness_oracle: "required_before_claims",
            result_materialization_policy: "declared_per_row",
            api_transport_policy: "cli_or_local_harness_declared",
            resource_limits: "declared_before_claims",
            cost_assumptions: "report_only",
            claim_level: "not_claim_grade",
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostSimulationReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub workload_constitution_ref: &'static str,
    pub resource_model: &'static str,
    pub object_store_request_model: &'static str,
    pub compute_cost_model: &'static str,
    pub egress_cost_model: &'static str,
    pub execution_performed: bool,
    pub cost_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl CostSimulationReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: "shardloom.cost_simulation_report.v1",
            report_id: "cg6.cost_simulation.report_only",
            workload_constitution_ref: "declared_per_workload",
            resource_model: "required_before_cost_claims",
            object_store_request_model: "required_before_object_store_cost_claims",
            compute_cost_model: "required_before_cost_claims",
            egress_cost_model: "required_before_remote_claims",
            execution_performed: false,
            cost_claim_allowed: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustPerformanceProfileEvidence {
    pub schema_version: &'static str,
    pub rustc_version: &'static str,
    pub target_triple: &'static str,
    pub target_cpu: &'static str,
    pub opt_level: &'static str,
    pub lto_mode: &'static str,
    pub codegen_units: &'static str,
    pub panic_strategy: &'static str,
    pub allocator: &'static str,
    pub simd_feature_flags: Vec<&'static str>,
    pub pgo_status: &'static str,
    pub bolt_status: &'static str,
    pub binary_size: Option<u64>,
    pub benchmark_refs: Vec<&'static str>,
    pub correctness_refs: Vec<&'static str>,
    pub performance_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl RustPerformanceProfileEvidence {
    #[must_use]
    pub fn report_only_required() -> Self {
        Self {
            schema_version: "shardloom.rust_performance_profile_evidence.v1",
            rustc_version: "required_before_performance_claims",
            target_triple: "required_before_performance_claims",
            target_cpu: "required_before_performance_claims",
            opt_level: "required_before_performance_claims",
            lto_mode: "required_before_performance_claims",
            codegen_units: "required_before_performance_claims",
            panic_strategy: "required_before_performance_claims",
            allocator: "required_before_performance_claims",
            simd_feature_flags: Vec::new(),
            pgo_status: "not_used",
            bolt_status: "not_used",
            binary_size: None,
            benchmark_refs: Vec::new(),
            correctness_refs: Vec::new(),
            performance_claim_allowed: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationalContractsReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub evidence_artifact_envelope: EvidenceArtifactEnvelope,
    pub evidence_artifact_safety: EvidenceArtifactSafety,
    pub execution_policy: ShardLoomExecutionPolicy,
    pub query_lifecycle_contract: QueryLifecycleContract,
    pub protocol_surface_parity_report: ProtocolSurfaceParityReport,
    pub workload_constitution_catalog: WorkloadConstitutionCatalog,
    pub shardloom_native_semantic_profile: ShardLoomNativeSemanticProfile,
    pub standards_dependency_decision_report: StandardsDependencyDecisionReport,
    pub benchmark_constitution: BenchmarkConstitution,
    pub cost_simulation_report: CostSimulationReport,
    pub rust_performance_profile_evidence: RustPerformanceProfileEvidence,
    pub docs_report_only: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl OperationalContractsReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.operational_contracts_report.v1",
            report_id: "priority_3_7.operational_contracts",
            evidence_artifact_envelope: EvidenceArtifactEnvelope::report_only(
                "report-only.execution-certificate-envelope",
                "execution_certificate",
            ),
            evidence_artifact_safety: EvidenceArtifactSafety::strict_default(),
            execution_policy: ShardLoomExecutionPolicy::safe_default(),
            query_lifecycle_contract: QueryLifecycleContract::blocked_unsupported(
                "query.report_only.blocked",
            ),
            protocol_surface_parity_report: ProtocolSurfaceParityReport::current_contract(),
            workload_constitution_catalog: WorkloadConstitutionCatalog::starter(),
            shardloom_native_semantic_profile: ShardLoomNativeSemanticProfile::floor(),
            standards_dependency_decision_report: StandardsDependencyDecisionReport::current(),
            benchmark_constitution: BenchmarkConstitution::report_only_foundation(),
            cost_simulation_report: CostSimulationReport::report_only(),
            rust_performance_profile_evidence: RustPerformanceProfileEvidence::report_only_required(
            ),
            docs_report_only: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn preserves_report_only_no_fallback(&self) -> bool {
        self.docs_report_only
            && !self.runtime_execution_allowed
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && self.evidence_artifact_envelope.has_required_identity()
            && self.evidence_artifact_safety.safe_for_agent_default()
            && self.execution_policy.denies_unsafe_defaults()
            && self.query_lifecycle_contract.is_safe_blocked_state()
            && self
                .protocol_surface_parity_report
                .all_fallback_fields_visible()
            && self.workload_constitution_catalog.all_non_claim_grade()
            && !self
                .shardloom_native_semantic_profile
                .production_sql_claim_allowed
            && self
                .standards_dependency_decision_report
                .all_runtime_blocked_until_approved()
            && self.benchmark_constitution.claim_level == "not_claim_grade"
            && !self.cost_simulation_report.cost_claim_allowed
            && !self
                .rust_performance_profile_evidence
                .performance_claim_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "operational contracts\nschema_version: {}\nreport: {}\nworkloads: {}\nsemantic dimensions: {}\nreport only: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.workload_constitution_catalog.entries.len(),
            self.shardloom_native_semantic_profile.dimensions.len(),
            self.docs_report_only,
        )
    }
}

#[must_use]
pub fn plan_operational_contracts() -> OperationalContractsReport {
    OperationalContractsReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_contracts_preserve_report_only_no_fallback_defaults() {
        let report = plan_operational_contracts();

        assert!(report.preserves_report_only_no_fallback());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn workload_catalog_contains_starter_entries_in_stable_order() {
        let catalog = WorkloadConstitutionCatalog::starter();

        assert_eq!(
            catalog.workload_ids(),
            vec![
                "local_vortex_primitives",
                "local_file_etl",
                "conda_import_smoke",
                "python_dataframe_local_etl",
                "rest_discovery_only",
                "batch_vortex_analytics",
                "hybrid_base_delta_fixture",
                "adapter_vortex_read_write_local",
                "traditional_analytics_benchmark"
            ]
        );
        assert!(catalog.all_non_claim_grade());
    }

    #[test]
    fn shardloom_native_profile_defines_semantic_floor_without_sql_claims() {
        let profile = ShardLoomNativeSemanticProfile::floor();

        assert!(profile.dimension_names().contains(&"null_comparison"));
        assert!(
            profile
                .dimension_names()
                .contains(&"timestamp_unit_timezone")
        );
        assert!(profile.dimension_names().contains(&"nested_list_equality"));
        assert!(!profile.production_sql_claim_allowed);
        assert!(!profile.fallback_attempted);
    }

    #[test]
    fn lifecycle_and_protocol_parity_keep_blocked_work_safe() {
        let lifecycle = QueryLifecycleContract::blocked_unsupported("query-1");
        let parity = ProtocolSurfaceParityReport::current_contract();

        assert!(lifecycle.is_safe_blocked_state());
        assert_eq!(QueryLifecycleState::all().len(), 11);
        assert!(parity.all_fallback_fields_visible());
        assert!(parity.known_surface_gaps.contains(&"rest_openapi"));
    }

    #[test]
    fn benchmark_cost_rust_and_standards_claims_remain_blocked() {
        let report = plan_operational_contracts();

        assert_eq!(report.benchmark_constitution.claim_level, "not_claim_grade");
        assert!(!report.cost_simulation_report.execution_performed);
        assert!(!report.cost_simulation_report.cost_claim_allowed);
        assert!(
            report
                .standards_dependency_decision_report
                .all_runtime_blocked_until_approved()
        );
        assert!(
            !report
                .rust_performance_profile_evidence
                .performance_claim_allowed
        );
    }
}
