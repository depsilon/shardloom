//! Vortex runtime capability utilization audit.
//!
//! This report answers a narrower question than general compatibility: how much
//! of Vortex's runtime capability stack is `ShardLoom` actually using, wrapping,
//! or deliberately keeping blocked. It is intentionally report-only.

use crate::{
    DeviceResidencyReport, ExecuteStepEvidence, IoBackendEvidence, plan_device_residency_report,
    plan_execute_step_evidence,
};
use shardloom_core::{ShardLoomSessionModelReport, plan_shardloom_session_model};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRuntimeCapabilityArea {
    Arrays,
    DeferredExecution,
    ExecutionLayers,
    ScanSourceSinkSplit,
    ScanFieldMasks,
    ScanPredicateOrdering,
    Layouts,
    LayoutAdvisor,
    IoCoalescingPrefetch,
    SessionRegistries,
    DeviceResidency,
    ExtensionTypes,
    BenchmarkDiscipline,
    VortexIntegrations,
}

impl VortexRuntimeCapabilityArea {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Arrays => "arrays",
            Self::DeferredExecution => "deferred_execution",
            Self::ExecutionLayers => "execution_layers",
            Self::ScanSourceSinkSplit => "scan_source_sink_split",
            Self::ScanFieldMasks => "scan_field_masks",
            Self::ScanPredicateOrdering => "scan_predicate_ordering",
            Self::Layouts => "layouts",
            Self::LayoutAdvisor => "layout_advisor",
            Self::IoCoalescingPrefetch => "io_coalescing_prefetch",
            Self::SessionRegistries => "session_registries",
            Self::DeviceResidency => "device_residency",
            Self::ExtensionTypes => "extension_types",
            Self::BenchmarkDiscipline => "benchmark_discipline",
            Self::VortexIntegrations => "vortex_integrations",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCapabilityUse {
    NotUsed,
    ReportOnlyWrapped,
    PartialRuntimeEvidence,
    PlannedRuntimeProvider,
    BlockedUntilEvidence,
    BaselineOnly,
}

impl VortexCapabilityUse {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotUsed => "not_used",
            Self::ReportOnlyWrapped => "report_only_wrapped",
            Self::PartialRuntimeEvidence => "partial_runtime_evidence",
            Self::PlannedRuntimeProvider => "planned_runtime_provider",
            Self::BlockedUntilEvidence => "blocked_until_evidence",
            Self::BaselineOnly => "baseline_only",
        }
    }

    #[must_use]
    pub const fn claim_ready(self) -> bool {
        matches!(self, Self::PartialRuntimeEvidence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCapabilityUtilizationRow {
    pub area: VortexRuntimeCapabilityArea,
    pub upstream_concept: &'static str,
    pub status: VortexCapabilityUse,
    pub shardloom_surface: &'static str,
    pub required_next_evidence: &'static str,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexCapabilityUtilizationRow {
    #[must_use]
    pub const fn new(
        area: VortexRuntimeCapabilityArea,
        upstream_concept: &'static str,
        status: VortexCapabilityUse,
        shardloom_surface: &'static str,
        required_next_evidence: &'static str,
    ) -> Self {
        Self {
            area,
            upstream_concept,
            status,
            shardloom_surface,
            required_next_evidence,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCapabilityUtilizationReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub vortex_crate_version: &'static str,
    pub file_format_version_assumption: &'static str,
    pub rows: Vec<VortexCapabilityUtilizationRow>,
    pub arrays_used: VortexCapabilityUse,
    pub layouts_used: VortexCapabilityUse,
    pub scan_api_used: VortexCapabilityUse,
    pub source_sink_used: VortexCapabilityUse,
    pub split_execution_used: VortexCapabilityUse,
    pub expression_pushdown_used: VortexCapabilityUse,
    pub field_masks_used: VortexCapabilityUse,
    pub zone_pruning_used: VortexCapabilityUse,
    pub dynamic_predicate_reordering_used: VortexCapabilityUse,
    pub deferred_execution_used: VortexCapabilityUse,
    pub execute_parent_kernels_used: VortexCapabilityUse,
    pub native_provider_kind: &'static str,
    pub materialization_boundary: &'static str,
    pub decode_boundary: &'static str,
    pub arrow_boundary: &'static str,
    pub object_store_io: VortexCapabilityUse,
    pub gpu_device_status: VortexCapabilityUse,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexCapabilityUtilizationReport {
    fn array_execution_rows() -> Vec<VortexCapabilityUtilizationRow> {
        use VortexCapabilityUse as Use;
        use VortexRuntimeCapabilityArea as Area;

        vec![
            VortexCapabilityUtilizationRow::new(
                Area::Arrays,
                "Array tree, encodings, buffers, statistics, vtables",
                Use::PartialRuntimeEvidence,
                "local primitive and reader/prepared encoded evidence",
                "array-tree refs and execution-step traces for each provider path",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::DeferredExecution,
                "FilterArray and ScalarFnArray deferred operation model",
                Use::ReportOnlyWrapped,
                "ExecuteStepEvidence",
                "trace-backed deferred operation and fusion evidence",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::ExecutionLayers,
                "reduce, reduce_parent, execute_parent, execute",
                Use::ReportOnlyWrapped,
                "VortexArrayExecutionCertificate",
                "runtime layer traces from Vortex provider execution",
            ),
        ]
    }

    fn scan_rows() -> Vec<VortexCapabilityUtilizationRow> {
        use VortexCapabilityUse as Use;
        use VortexRuntimeCapabilityArea as Area;

        vec![
            VortexCapabilityUtilizationRow::new(
                Area::ScanSourceSinkSplit,
                "Source, Sink, Scan request, Split",
                Use::PartialRuntimeEvidence,
                "VortexScanExecutionSpineReport",
                "remote/object-store/table split evidence before broad source/sink claims",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::ScanFieldMasks,
                "filter/output field masks",
                Use::BlockedUntilEvidence,
                "VortexFieldMaskEvidence",
                "filter/output/union column evidence from real scan requests",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::ScanPredicateOrdering,
                "dynamic predicate ordering from selectivity sketches",
                Use::BlockedUntilEvidence,
                "VortexPredicateOrderingEvidence",
                "observed selectivity and reorder-decision evidence",
            ),
        ]
    }

    fn layout_io_rows() -> Vec<VortexCapabilityUtilizationRow> {
        use VortexCapabilityUse as Use;
        use VortexRuntimeCapabilityArea as Area;

        vec![
            VortexCapabilityUtilizationRow::new(
                Area::Layouts,
                "Flat, Struct, Chunked, Dictionary, Zoned layouts",
                Use::ReportOnlyWrapped,
                "VortexLayoutAdvisorReport",
                "layout refs, lazy segment fetches, and zone-pruning metrics",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::LayoutAdvisor,
                "layout writer strategy, deterministic placement, and rewrite posture",
                Use::ReportOnlyWrapped,
                "VortexLayoutAdvisorReport",
                "workload constitution, layout refs, write/read tradeoff, and layout-health evidence",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::IoCoalescingPrefetch,
                "read_at, coalescing, prefetch, backend concurrency",
                Use::ReportOnlyWrapped,
                "IoBackendEvidence",
                "request counts, useful bytes, coalescing, prefetch, and backend concurrency",
            ),
        ]
    }

    fn integration_posture_rows() -> Vec<VortexCapabilityUtilizationRow> {
        use VortexCapabilityUse as Use;
        use VortexRuntimeCapabilityArea as Area;

        vec![
            VortexCapabilityUtilizationRow::new(
                Area::SessionRegistries,
                "VortexSession and Registry<T>",
                Use::ReportOnlyWrapped,
                "ShardLoomSessionModelReport",
                "explicit session context and registry admission implementation",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::DeviceResidency,
                "vortex-cuda, CudaSession, device buffers",
                Use::ReportOnlyWrapped,
                "DeviceResidencyReport",
                "device buffer refs, transfer bytes, kernel refs, and output boundary evidence",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::ExtensionTypes,
                "extension DTypes, extension encodings, and extension compute functions",
                Use::ReportOnlyWrapped,
                "ExtensionTypeCapabilityMatrix",
                "dtype recognition, metadata preservation, scan, expression, write, and execution evidence",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::BenchmarkDiscipline,
                "microbenchmark and end-to-end benchmark separation",
                Use::ReportOnlyWrapped,
                "BenchmarkConstitution and benchmark-suite catalog",
                "seeded fixture setup, timed-scope declaration, correctness oracle, and materialization policy",
            ),
            VortexCapabilityUtilizationRow::new(
                Area::VortexIntegrations,
                "Vortex query-engine integrations",
                Use::BaselineOnly,
                "VortexBenchmarkInterop",
                "comparison rows labeled not ShardLoom execution and not fallback",
            ),
        ]
    }

    fn current_rows() -> Vec<VortexCapabilityUtilizationRow> {
        let mut rows = Self::array_execution_rows();
        rows.extend(Self::scan_rows());
        rows.extend(Self::layout_io_rows());
        rows.extend(Self::integration_posture_rows());
        rows
    }

    #[must_use]
    pub fn current() -> Self {
        use VortexCapabilityUse as Use;

        Self {
            schema_version: "shardloom.vortex_capability_utilization_report.v1",
            report_id: "priority_2_6.vortex_runtime_utilization.current",
            vortex_crate_version: crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
            file_format_version_assumption: "recorded_in_vortex_public_api_inventory",
            rows: Self::current_rows(),
            arrays_used: Use::PartialRuntimeEvidence,
            layouts_used: Use::ReportOnlyWrapped,
            scan_api_used: Use::PartialRuntimeEvidence,
            source_sink_used: Use::PartialRuntimeEvidence,
            split_execution_used: Use::PartialRuntimeEvidence,
            expression_pushdown_used: Use::BlockedUntilEvidence,
            field_masks_used: Use::BlockedUntilEvidence,
            zone_pruning_used: Use::BlockedUntilEvidence,
            dynamic_predicate_reordering_used: Use::BlockedUntilEvidence,
            deferred_execution_used: Use::ReportOnlyWrapped,
            execute_parent_kernels_used: Use::BlockedUntilEvidence,
            native_provider_kind: "vortex_native_provider_or_shardloom_kernel_recorded_per_path",
            materialization_boundary: "must_be_recorded_per_provider_path",
            decode_boundary: "must_be_recorded_per_provider_path",
            arrow_boundary: "compatibility_boundary_only_when_explicit",
            object_store_io: Use::BlockedUntilEvidence,
            gpu_device_status: Use::ReportOnlyWrapped,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        self.rows
            .iter()
            .all(VortexCapabilityUtilizationRow::fallback_free)
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn row_status(&self, area: VortexRuntimeCapabilityArea) -> Option<VortexCapabilityUse> {
        self.rows
            .iter()
            .find_map(|row| (row.area == area).then_some(row.status))
    }

    #[must_use]
    pub fn blocked_or_report_only_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    VortexCapabilityUse::ReportOnlyWrapped
                        | VortexCapabilityUse::PlannedRuntimeProvider
                        | VortexCapabilityUse::BlockedUntilEvidence
                )
            })
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexFieldMaskEvidence {
    pub schema_version: &'static str,
    pub filter_columns: Vec<&'static str>,
    pub output_columns: Vec<&'static str>,
    pub union_read_columns: Vec<&'static str>,
    pub filter_only_columns_discarded: Vec<&'static str>,
    pub field_masks_used: bool,
    pub fallback_attempted: bool,
}

impl VortexFieldMaskEvidence {
    #[must_use]
    pub fn report_only_required() -> Self {
        Self {
            schema_version: "shardloom.vortex_field_mask_evidence.v1",
            filter_columns: Vec::new(),
            output_columns: Vec::new(),
            union_read_columns: Vec::new(),
            filter_only_columns_discarded: Vec::new(),
            field_masks_used: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPredicateOrderingEvidence {
    pub schema_version: &'static str,
    pub conjunct_count: usize,
    pub observed_selectivity_refs: Vec<&'static str>,
    pub dynamic_reorder_decisions: Vec<&'static str>,
    pub row_reduction_evidence_refs: Vec<&'static str>,
    pub predicate_ordering_used: bool,
    pub fallback_attempted: bool,
}

impl VortexPredicateOrderingEvidence {
    #[must_use]
    pub fn report_only_required() -> Self {
        Self {
            schema_version: "shardloom.vortex_predicate_ordering_evidence.v1",
            conjunct_count: 0,
            observed_selectivity_refs: Vec::new(),
            dynamic_reorder_decisions: Vec::new(),
            row_reduction_evidence_refs: Vec::new(),
            predicate_ordering_used: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexScanExecutionSpineReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub source_trait_used: bool,
    pub sink_trait_used: bool,
    pub split_task_graph_used: bool,
    pub split_estimates_used: bool,
    pub split_serialization_used: bool,
    pub compressed_ipc_transport_used: bool,
    pub projection_pushdown_recorded: bool,
    pub filter_pushdown_recorded: bool,
    pub limit_pushdown_recorded: bool,
    pub field_mask_evidence: VortexFieldMaskEvidence,
    pub predicate_ordering_evidence: VortexPredicateOrderingEvidence,
    pub residual_executor: &'static str,
    pub native_io_certificate_required: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexScanExecutionSpineReport {
    #[must_use]
    pub fn report_only_required() -> Self {
        Self {
            schema_version: "shardloom.vortex_scan_execution_spine_report.v1",
            report_id: "priority_2_6.vortex_scan_execution_spine.report_only",
            source_trait_used: false,
            sink_trait_used: false,
            split_task_graph_used: false,
            split_estimates_used: false,
            split_serialization_used: false,
            compressed_ipc_transport_used: false,
            projection_pushdown_recorded: false,
            filter_pushdown_recorded: false,
            limit_pushdown_recorded: false,
            field_mask_evidence: VortexFieldMaskEvidence::report_only_required(),
            predicate_ordering_evidence: VortexPredicateOrderingEvidence::report_only_required(),
            residual_executor: "unsupported_blocked_until_provider_evidence",
            native_io_certificate_required: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.runtime_execution_allowed
            || self.native_io_certificate_required
            || self.fallback_attempted
            || self.external_engine_invoked
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLayoutAdvisorReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub target_workloads: Vec<&'static str>,
    pub candidate_layouts: Vec<&'static str>,
    pub chunking_policy: &'static str,
    pub zone_statistics_policy: &'static str,
    pub dictionary_strategy: &'static str,
    pub expected_pruning_benefit: &'static str,
    pub expected_random_access_benefit: &'static str,
    pub write_read_tradeoff: &'static str,
    pub object_store_request_shape: &'static str,
    pub gpu_device_read_friendliness: &'static str,
    pub compaction_recommendation: &'static str,
    pub advisor_runtime_allowed: bool,
    pub layout_support_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexLayoutAdvisorReport {
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            schema_version: "shardloom.vortex_layout_advisor_report.v1",
            report_id: "priority_2_6.vortex_layout_advisor.report_only",
            target_workloads: vec![
                "selective_filters",
                "wide_projection",
                "random_access",
                "object_store_scans",
                "future_device_reads",
            ],
            candidate_layouts: vec![
                "flat_layout",
                "struct_layout",
                "chunked_layout",
                "dictionary_layout",
                "zoned_layout",
            ],
            chunking_policy: "required_before_write_layout_claims",
            zone_statistics_policy: "required_before_pruning_claims",
            dictionary_strategy: "required_before_dictionary_layout_claims",
            expected_pruning_benefit: "not_estimated_without_workload_constitution",
            expected_random_access_benefit: "not_estimated_without_workload_constitution",
            write_read_tradeoff: "must_be_reported_before_advisor_claims",
            object_store_request_shape: "must_link_to_io_backend_evidence",
            gpu_device_read_friendliness: "report_only_until_device_residency_evidence",
            compaction_recommendation: "deferred_until_layout_health_and_write_evidence",
            advisor_runtime_allowed: false,
            layout_support_claim_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.layout_support_claim_allowed
            || !self.advisor_runtime_allowed
            || self.fallback_attempted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundarySupportStatus {
    ReportOnly,
    Unsupported,
    ComparisonOnly,
}

impl VortexBoundarySupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::Unsupported => "unsupported",
            Self::ComparisonOnly => "comparison_only",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLayoutDeviceManagedBoundarySurface {
    LayoutWrite,
    DeviceExecution,
    ObjectStoreIo,
    ManagedPlatformComparison,
}

impl VortexLayoutDeviceManagedBoundarySurface {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LayoutWrite => "layout_write",
            Self::DeviceExecution => "device_execution",
            Self::ObjectStoreIo => "object_store_io",
            Self::ManagedPlatformComparison => "managed_platform_comparison",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLayoutDeviceManagedBoundaryRow {
    pub row_id: &'static str,
    pub surface: VortexLayoutDeviceManagedBoundarySurface,
    pub support_status: VortexBoundarySupportStatus,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub evidence_required: &'static str,
    pub benchmark_ref: &'static str,
    pub release_gate_ref: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub object_store_io: bool,
    pub device_execution: bool,
    pub managed_platform_execution: bool,
    pub managed_platform_dependency_added: bool,
    pub credential_required: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexLayoutDeviceManagedBoundaryRow {
    const BENCHMARK_REF: &'static str =
        "vortex-runtime-utilization-audit://layout_device_managed_boundary.v1";
    const RELEASE_GATE_REF: &'static str =
        "release-readiness://vortex_layout_device_managed_boundary.v1";

    #[must_use]
    pub const fn layout_write() -> Self {
        Self {
            row_id: "layout_write_boundary",
            surface: VortexLayoutDeviceManagedBoundarySurface::LayoutWrite,
            support_status: VortexBoundarySupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_layout_write_claim_without_workload_layout_and_write_evidence",
            evidence_required: "workload_constitution,layout_refs,write_read_tradeoff_metrics,native_io_certificate,execution_certificate",
            benchmark_ref: Self::BENCHMARK_REF,
            release_gate_ref: Self::RELEASE_GATE_REF,
            unsupported_diagnostic_code: "SL_LAYOUT_WRITE_BOUNDARY_NOT_CLAIM_GRADE",
            blocker_id: "gar0042b.layout_write_evidence_boundary",
            runtime_execution: false,
            write_io: false,
            object_store_io: false,
            device_execution: false,
            managed_platform_execution: false,
            managed_platform_dependency_added: false,
            credential_required: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn device_execution() -> Self {
        Self {
            row_id: "device_execution_boundary",
            surface: VortexLayoutDeviceManagedBoundarySurface::DeviceExecution,
            support_status: VortexBoundarySupportStatus::Unsupported,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_gpu_or_device_execution_claim_without_residency_evidence",
            evidence_required: "device_buffer_refs,transfer_bytes,kernel_refs,output_boundary,native_io_certificate,execution_certificate",
            benchmark_ref: Self::BENCHMARK_REF,
            release_gate_ref: Self::RELEASE_GATE_REF,
            unsupported_diagnostic_code: "SL_UNSUPPORTED_DEVICE_EXECUTION_BOUNDARY",
            blocker_id: "gar0042b.device_execution_boundary",
            runtime_execution: false,
            write_io: false,
            object_store_io: false,
            device_execution: false,
            managed_platform_execution: false,
            managed_platform_dependency_added: false,
            credential_required: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn object_store_io() -> Self {
        Self {
            row_id: "object_store_io_boundary",
            surface: VortexLayoutDeviceManagedBoundarySurface::ObjectStoreIo,
            support_status: VortexBoundarySupportStatus::ReportOnly,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_object_store_io_claim_without_request_and_native_io_evidence",
            evidence_required: "range_request_plan,coalescing_metrics,useful_bytes,read_amplification,native_io_certificate,execution_certificate",
            benchmark_ref: Self::BENCHMARK_REF,
            release_gate_ref: Self::RELEASE_GATE_REF,
            unsupported_diagnostic_code: "SL_OBJECT_STORE_IO_BOUNDARY_NOT_CLAIM_GRADE",
            blocker_id: "gar0042b.object_store_io_boundary",
            runtime_execution: false,
            write_io: false,
            object_store_io: false,
            device_execution: false,
            managed_platform_execution: false,
            managed_platform_dependency_added: false,
            credential_required: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn managed_platform_comparison() -> Self {
        Self {
            row_id: "managed_platform_comparison_boundary",
            surface: VortexLayoutDeviceManagedBoundarySurface::ManagedPlatformComparison,
            support_status: VortexBoundarySupportStatus::ComparisonOnly,
            claim_gate_status: "not_claim_grade",
            claim_boundary: "comparison_only_design_reference_not_shardloom_execution",
            evidence_required: "approved_comparison_protocol,platform_version_config,neutral_result_refs,no_fallback_policy",
            benchmark_ref: Self::BENCHMARK_REF,
            release_gate_ref: Self::RELEASE_GATE_REF,
            unsupported_diagnostic_code: "SL_MANAGED_PLATFORM_COMPARISON_ONLY",
            blocker_id: "gar0042b.managed_platform_comparison_boundary",
            runtime_execution: false,
            write_io: false,
            object_store_io: false,
            device_execution: false,
            managed_platform_execution: false,
            managed_platform_dependency_added: false,
            credential_required: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }

    #[must_use]
    pub fn not_claim_grade(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
    }

    #[must_use]
    pub const fn cannot_satisfy_native_claim_without_evidence(&self) -> bool {
        !self.runtime_execution
            && !self.write_io
            && !self.object_store_io
            && !self.device_execution
            && !self.managed_platform_execution
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLayoutDeviceManagedBoundaryMatrix {
    pub schema_version: &'static str,
    pub matrix_id: &'static str,
    pub rows: Vec<VortexLayoutDeviceManagedBoundaryRow>,
    pub claim_gate_status: &'static str,
    pub benchmark_ref: &'static str,
    pub docs_ref: &'static str,
    pub release_gate_ref: &'static str,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexLayoutDeviceManagedBoundaryMatrix {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_layout_device_managed_boundary.v1",
            matrix_id: "gar0042b.layout_device_managed_boundary",
            rows: vec![
                VortexLayoutDeviceManagedBoundaryRow::layout_write(),
                VortexLayoutDeviceManagedBoundaryRow::device_execution(),
                VortexLayoutDeviceManagedBoundaryRow::object_store_io(),
                VortexLayoutDeviceManagedBoundaryRow::managed_platform_comparison(),
            ],
            claim_gate_status: "not_claim_grade",
            benchmark_ref: "vortex-runtime-utilization-audit://layout_device_managed_boundary.v1",
            docs_ref: "docs/architecture/vortex-runtime-utilization-audit.md#layout-device-managed-boundary-matrix",
            release_gate_ref: "release-readiness://vortex_layout_device_managed_boundary.v1",
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .all(VortexLayoutDeviceManagedBoundaryRow::fallback_free)
    }

    #[must_use]
    pub fn all_rows_not_claim_grade(&self) -> bool {
        self.claim_gate_status == "not_claim_grade"
            && self
                .rows
                .iter()
                .all(VortexLayoutDeviceManagedBoundaryRow::not_claim_grade)
    }

    #[must_use]
    pub fn managed_platform_rows_are_comparison_only(&self) -> bool {
        let managed_rows = self.rows.iter().filter(|row| {
            row.surface == VortexLayoutDeviceManagedBoundarySurface::ManagedPlatformComparison
        });
        let mut managed_row_count = 0;
        for row in managed_rows {
            managed_row_count += 1;
            if row.support_status != VortexBoundarySupportStatus::ComparisonOnly
                || row.managed_platform_execution
                || row.managed_platform_dependency_added
                || row.credential_required
            {
                return false;
            }
        }
        managed_row_count > 0
    }

    #[must_use]
    pub fn device_and_object_store_claims_blocked_without_evidence(&self) -> bool {
        let mut device_boundary_present = false;
        let mut object_store_boundary_present = false;
        for row in &self.rows {
            match row.surface {
                VortexLayoutDeviceManagedBoundarySurface::DeviceExecution => {
                    device_boundary_present = true;
                    if !row.cannot_satisfy_native_claim_without_evidence() {
                        return false;
                    }
                }
                VortexLayoutDeviceManagedBoundarySurface::ObjectStoreIo => {
                    object_store_boundary_present = true;
                    if !row.cannot_satisfy_native_claim_without_evidence() {
                        return false;
                    }
                }
                VortexLayoutDeviceManagedBoundarySurface::LayoutWrite
                | VortexLayoutDeviceManagedBoundarySurface::ManagedPlatformComparison => {}
            }
        }
        device_boundary_present && object_store_boundary_present
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexArrayExecutionCertificate {
    pub schema_version: &'static str,
    pub certificate_id: &'static str,
    pub array_tree_before: &'static str,
    pub array_tree_after: &'static str,
    pub reduce_steps: Vec<&'static str>,
    pub reduce_parent_steps: Vec<&'static str>,
    pub execute_parent_kernel_steps: Vec<&'static str>,
    pub execute_steps: Vec<&'static str>,
    pub constant_short_circuit_used: bool,
    pub dict_scalar_function_pushdown_used: bool,
    pub runend_parent_kernel_used: bool,
    pub canonicalization_performed: bool,
    pub materialization_performed: bool,
    pub final_representation: &'static str,
    pub runtime_trace_refs: Vec<&'static str>,
    pub support_claim_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexArrayExecutionCertificate {
    #[must_use]
    pub fn report_only_required() -> Self {
        Self {
            schema_version: "shardloom.vortex_array_execution_certificate.v1",
            certificate_id: "priority_2_6.vortex_array_execution.report_only_required",
            array_tree_before: "required_before_array_execution_claims",
            array_tree_after: "required_before_array_execution_claims",
            reduce_steps: Vec::new(),
            reduce_parent_steps: Vec::new(),
            execute_parent_kernel_steps: Vec::new(),
            execute_steps: Vec::new(),
            constant_short_circuit_used: false,
            dict_scalar_function_pushdown_used: false,
            runend_parent_kernel_used: false,
            canonicalization_performed: false,
            materialization_performed: false,
            final_representation: "not_recorded",
            runtime_trace_refs: Vec::new(),
            support_claim_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn blocks_execution_layer_claims(&self) -> bool {
        !self.support_claim_allowed
            && self.reduce_steps.is_empty()
            && self.reduce_parent_steps.is_empty()
            && self.execute_parent_kernel_steps.is_empty()
            && self.runtime_trace_refs.is_empty()
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexRuntimeUtilizationAuditReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub capability_utilization: VortexCapabilityUtilizationReport,
    pub scan_execution_spine: VortexScanExecutionSpineReport,
    pub layout_advisor: VortexLayoutAdvisorReport,
    pub layout_device_managed_boundary_matrix: VortexLayoutDeviceManagedBoundaryMatrix,
    pub array_execution_certificate: VortexArrayExecutionCertificate,
    pub execute_step_evidence: ExecuteStepEvidence,
    pub device_residency: DeviceResidencyReport,
    pub io_backend_evidence: IoBackendEvidence,
    pub session_model: ShardLoomSessionModelReport,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub runtime_expansion_authorized: bool,
}

impl VortexRuntimeUtilizationAuditReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_runtime_utilization_audit.v1",
            report_id: "priority_2_6.vortex_runtime_utilization_audit",
            capability_utilization: VortexCapabilityUtilizationReport::current(),
            scan_execution_spine: VortexScanExecutionSpineReport::report_only_required(),
            layout_advisor: VortexLayoutAdvisorReport::report_only(),
            layout_device_managed_boundary_matrix: VortexLayoutDeviceManagedBoundaryMatrix::current(
            ),
            array_execution_certificate: VortexArrayExecutionCertificate::report_only_required(),
            execute_step_evidence: plan_execute_step_evidence(),
            device_residency: plan_device_residency_report(),
            io_backend_evidence: IoBackendEvidence::object_store_report_only(),
            session_model: plan_shardloom_session_model(),
            external_engine_invoked: false,
            fallback_attempted: false,
            runtime_expansion_authorized: false,
        }
    }

    #[must_use]
    pub fn preserves_no_fallback_and_no_runtime_expansion(&self) -> bool {
        self.capability_utilization.all_rows_fallback_free()
            && self.scan_execution_spine.claim_blocked()
            && self.layout_advisor.claim_blocked()
            && self
                .layout_device_managed_boundary_matrix
                .all_rows_fallback_free()
            && self
                .layout_device_managed_boundary_matrix
                .all_rows_not_claim_grade()
            && self
                .layout_device_managed_boundary_matrix
                .managed_platform_rows_are_comparison_only()
            && self
                .layout_device_managed_boundary_matrix
                .device_and_object_store_claims_blocked_without_evidence()
            && self
                .array_execution_certificate
                .blocks_execution_layer_claims()
            && self.device_residency.fallback_free()
            && self.io_backend_evidence.fallback_free()
            && self.session_model.preserves_no_runtime_expansion()
            && !self.execute_step_evidence.external_engine_invoked
            && !self.execute_step_evidence.fallback_attempted
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.runtime_expansion_authorized
    }
}

#[must_use]
pub fn plan_vortex_runtime_utilization_audit() -> VortexRuntimeUtilizationAuditReport {
    VortexRuntimeUtilizationAuditReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utilization_report_distinguishes_partial_runtime_from_blocked_vortex_features() {
        let report = VortexCapabilityUtilizationReport::current();

        assert_eq!(
            report.row_status(VortexRuntimeCapabilityArea::Arrays),
            Some(VortexCapabilityUse::PartialRuntimeEvidence)
        );
        assert_eq!(
            report.row_status(VortexRuntimeCapabilityArea::ScanSourceSinkSplit),
            Some(VortexCapabilityUse::PartialRuntimeEvidence)
        );
        assert_eq!(
            report.scan_api_used,
            VortexCapabilityUse::PartialRuntimeEvidence
        );
        assert_eq!(
            report.source_sink_used,
            VortexCapabilityUse::PartialRuntimeEvidence
        );
        assert_eq!(
            report.split_execution_used,
            VortexCapabilityUse::PartialRuntimeEvidence
        );
        assert_eq!(
            report.row_status(VortexRuntimeCapabilityArea::ScanFieldMasks),
            Some(VortexCapabilityUse::BlockedUntilEvidence)
        );
        assert_eq!(
            report.row_status(VortexRuntimeCapabilityArea::VortexIntegrations),
            Some(VortexCapabilityUse::BaselineOnly)
        );
        assert_eq!(
            report.row_status(VortexRuntimeCapabilityArea::BenchmarkDiscipline),
            Some(VortexCapabilityUse::ReportOnlyWrapped)
        );
        assert!(report.all_rows_fallback_free());
        assert!(report.blocked_or_report_only_count() >= 6);
    }

    #[test]
    fn scan_spine_requires_real_vortex_source_split_and_field_mask_evidence() {
        let report = VortexScanExecutionSpineReport::report_only_required();

        assert!(!report.source_trait_used);
        assert!(!report.split_task_graph_used);
        assert!(!report.field_mask_evidence.field_masks_used);
        assert!(!report.predicate_ordering_evidence.predicate_ordering_used);
        assert_eq!(
            report.residual_executor,
            "unsupported_blocked_until_provider_evidence"
        );
        assert!(report.claim_blocked());
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn layout_advisor_claims_stay_blocked_when_fallback_was_attempted() {
        let mut report = VortexLayoutAdvisorReport::report_only();
        report.layout_support_claim_allowed = true;
        report.advisor_runtime_allowed = true;
        report.fallback_attempted = true;

        assert!(report.claim_blocked());
    }

    #[test]
    fn layout_advisor_is_report_only_until_workload_and_vortex_layout_evidence_exists() {
        let report = VortexLayoutAdvisorReport::report_only();

        assert!(report.candidate_layouts.contains(&"zoned_layout"));
        assert!(report.target_workloads.contains(&"object_store_scans"));
        assert_eq!(
            report.zone_statistics_policy,
            "required_before_pruning_claims"
        );
        assert!(report.claim_blocked());
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn layout_device_managed_boundary_matrix_blocks_claims_without_evidence() {
        let matrix = VortexLayoutDeviceManagedBoundaryMatrix::current();

        assert_eq!(
            matrix.schema_version,
            "shardloom.vortex_layout_device_managed_boundary.v1"
        );
        assert_eq!(
            matrix.row_order(),
            vec![
                "layout_write_boundary",
                "device_execution_boundary",
                "object_store_io_boundary",
                "managed_platform_comparison_boundary"
            ]
        );
        assert!(matrix.all_rows_fallback_free());
        assert!(matrix.all_rows_not_claim_grade());
        assert!(matrix.managed_platform_rows_are_comparison_only());
        assert!(matrix.device_and_object_store_claims_blocked_without_evidence());

        let mut missing_device = matrix.clone();
        missing_device
            .rows
            .retain(|row| row.surface != VortexLayoutDeviceManagedBoundarySurface::DeviceExecution);
        assert!(!missing_device.device_and_object_store_claims_blocked_without_evidence());

        let mut bad_managed = matrix;
        let managed = bad_managed
            .rows
            .iter_mut()
            .find(|row| {
                row.surface == VortexLayoutDeviceManagedBoundarySurface::ManagedPlatformComparison
            })
            .expect("managed platform comparison row exists");
        managed.managed_platform_dependency_added = true;
        assert!(!bad_managed.managed_platform_rows_are_comparison_only());
    }

    #[test]
    fn array_execution_certificate_does_not_claim_vortex_execution_layers_without_traces() {
        let certificate = VortexArrayExecutionCertificate::report_only_required();

        assert!(certificate.reduce_steps.is_empty());
        assert!(certificate.reduce_parent_steps.is_empty());
        assert!(certificate.execute_parent_kernel_steps.is_empty());
        assert!(certificate.blocks_execution_layer_claims());
        assert!(!certificate.canonicalization_performed);
        assert!(!certificate.materialization_performed);
        assert!(!certificate.fallback_attempted);
    }

    #[test]
    fn aggregate_audit_preserves_no_runtime_expansion() {
        let report = plan_vortex_runtime_utilization_audit();

        assert!(report.preserves_no_fallback_and_no_runtime_expansion());
        assert!(report.session_model.preserves_no_runtime_expansion());
        assert!(!report.runtime_expansion_authorized);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn aggregate_audit_rejects_execute_step_external_engine_or_fallback_evidence() {
        let mut report = plan_vortex_runtime_utilization_audit();
        report.execute_step_evidence.external_engine_invoked = true;
        assert!(!report.preserves_no_fallback_and_no_runtime_expansion());

        let mut report = plan_vortex_runtime_utilization_audit();
        report.execute_step_evidence.fallback_attempted = true;
        assert!(!report.preserves_no_fallback_and_no_runtime_expansion());
    }
}
