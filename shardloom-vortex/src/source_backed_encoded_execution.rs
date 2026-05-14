use std::{collections::BTreeSet, fmt::Write as _};

use shardloom_core::{
    ColumnRef, DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, ExecutionCertificate,
    NativeIoCertificate, PredicateExpr, Result, SelectionVector, ShardLoomError,
    UniversalInputSource, intersect_selection_vectors,
};

use crate::{
    VortexEncodedValuePredicateBatch, VortexGeneralizedEncodedFilterExecutionReport,
    VortexGeneralizedEncodedFilterExecutionStatus,
    VortexGeneralizedEncodedProjectionExecutionReport,
    VortexGeneralizedEncodedProjectionExecutionStatus, VortexPreparedEncodedProjectionColumn,
    VortexSelectionVectorFilterKernelReport, evaluate_vortex_encoded_value_predicate_batch,
    execute_vortex_generalized_filter_from_encoded_value_batches,
    execute_vortex_generalized_projection_from_encoded_projection_batches,
};

const FILTER_SCHEMA_VERSION: &str = "shardloom.vortex_source_backed_encoded_filter_execution.v1";
const FILTER_REPORT_ID: &str = "vortex.cg2.source-backed-filter.prepared-encoded-values";
const FILTER_EXECUTION_KIND: &str = "vortex.source_backed_prepared_encoded_filter";

const PROJECTION_SCHEMA_VERSION: &str =
    "shardloom.vortex_source_backed_encoded_projection_execution.v1";
const PROJECTION_REPORT_ID: &str = "vortex.cg2.source-backed-projection.prepared-encoded-columns";
const PROJECTION_EXECUTION_KIND: &str = "vortex.source_backed_prepared_encoded_projection";
const READER_FILTER_SCHEMA_VERSION: &str =
    "shardloom.vortex_reader_backed_encoded_filter_execution.v1";
const READER_FILTER_REPORT_ID: &str =
    "vortex.cg2.reader-backed-filter.reader-validated-prepared-encoded-values";
const READER_FILTER_EXECUTION_KIND: &str =
    "vortex.reader_backed_reader_validated_prepared_encoded_filter";
const READER_PROJECTION_SCHEMA_VERSION: &str =
    "shardloom.vortex_reader_backed_encoded_projection_execution.v1";
const READER_PROJECTION_REPORT_ID: &str =
    "vortex.cg2.reader-backed-projection.reader-validated-prepared-encoded-columns";
const READER_PROJECTION_EXECUTION_KIND: &str =
    "vortex.reader_backed_reader_validated_prepared_encoded_projection";
const READER_GENERATED_BATCH_SCHEMA_VERSION: &str =
    "shardloom.vortex_reader_generated_prepared_batch_envelope.v1";
const READER_GENERATED_BATCH_REPORT_ID: &str =
    "vortex.cg2.reader-generated-prepared-batch-envelope";
const READER_GENERATED_BATCH_EXECUTION_KIND: &str =
    "vortex.reader_generated_prepared_chunk_envelope";
const READER_CONJUNCTIVE_FILTER_SCHEMA_VERSION: &str =
    "shardloom.vortex_reader_generated_conjunctive_selection_vector_bridge.v1";
const READER_CONJUNCTIVE_FILTER_REPORT_ID: &str =
    "vortex.cg2.reader-generated-conjunctive-selection-vector-bridge";
const READER_CONJUNCTIVE_FILTER_EXECUTION_KIND: &str =
    "vortex.reader_generated_conjunctive_selection_vector_bridge";
const LOCAL_SCAN_PROVIDER_KIND: &str = "vortex_scan";
const LOCAL_SCAN_PROVIDER_API_SURFACE: &str = "VortexFile::scan.into_array_iter";
const LOCAL_SCAN_PROVIDER_CRATE: &str = "vortex";
const LOCAL_SCAN_PROVIDER_VERSION: &str = "0.70";
const LOCAL_SCAN_FEATURE_GATE: &str = "vortex-local-primitives";
const LOCAL_SCAN_ADMISSION_POLICY: &str = "shardloom.vortex.local_scan_primitive.v1";
const LOCAL_SCAN_CERTIFICATE_REQUIREMENT: &str =
    "cg16_execution_certificate_and_cg19_native_io_certificate";
const REPRESENTATION_VORTEX_READER_CHUNK: &str = "vortex_reader_chunk";
const REPRESENTATION_PREPARED_CHUNK_ENVELOPE: &str = "reader_generated_prepared_chunk_envelope";
const REPRESENTATION_PREPARED_ENCODED_KERNEL_INPUT: &str =
    "reader_generated_prepared_encoded_kernel_input";
const RESIDUAL_EXECUTOR_NONE: &str = "none";
const RESIDUAL_EXECUTOR_SHARDLOOM_NATIVE: &str = "shardloom_native";
const RESIDUAL_EXECUTOR_UNSUPPORTED_BLOCKED: &str = "unsupported_blocked";
const RESIDUAL_EXECUTOR_EXTERNAL_BASELINE_ONLY: &str = "external_baseline_only";
const RESIDUAL_EXECUTOR_PROHIBITED_EXTERNAL_FALLBACK: &str = "prohibited_external_fallback";

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexResidualBoundaryReport {
    pub residual_required: bool,
    pub residual_executor: &'static str,
    pub residual_expression: Option<String>,
    pub accepted_operations: Vec<String>,
    pub rejected_operations: Vec<String>,
    pub shardloom_native_residual_execution: bool,
    pub external_engine_invoked: bool,
    pub prohibited_external_fallback: bool,
    pub fallback_attempted: bool,
}

impl VortexResidualBoundaryReport {
    #[must_use]
    pub fn none(accepted_operation: impl Into<String>) -> Self {
        Self {
            residual_required: false,
            residual_executor: RESIDUAL_EXECUTOR_NONE,
            residual_expression: None,
            accepted_operations: vec![accepted_operation.into()],
            rejected_operations: Vec::new(),
            shardloom_native_residual_execution: false,
            external_engine_invoked: false,
            prohibited_external_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn unsupported_blocked(
        residual_expression: impl Into<String>,
        rejected_operation: impl Into<String>,
    ) -> Self {
        Self {
            residual_required: true,
            residual_executor: RESIDUAL_EXECUTOR_UNSUPPORTED_BLOCKED,
            residual_expression: Some(residual_expression.into()),
            accepted_operations: Vec::new(),
            rejected_operations: vec![rejected_operation.into()],
            shardloom_native_residual_execution: false,
            external_engine_invoked: false,
            prohibited_external_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn shardloom_native(
        residual_expression: impl Into<String>,
        accepted_operation: impl Into<String>,
    ) -> Self {
        Self {
            residual_required: true,
            residual_executor: RESIDUAL_EXECUTOR_SHARDLOOM_NATIVE,
            residual_expression: Some(residual_expression.into()),
            accepted_operations: vec![accepted_operation.into()],
            rejected_operations: Vec::new(),
            shardloom_native_residual_execution: true,
            external_engine_invoked: false,
            prohibited_external_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn external_baseline_only(
        residual_expression: impl Into<String>,
        rejected_operation: impl Into<String>,
    ) -> Self {
        Self {
            residual_required: true,
            residual_executor: RESIDUAL_EXECUTOR_EXTERNAL_BASELINE_ONLY,
            residual_expression: Some(residual_expression.into()),
            accepted_operations: Vec::new(),
            rejected_operations: vec![rejected_operation.into()],
            shardloom_native_residual_execution: false,
            external_engine_invoked: false,
            prohibited_external_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn prohibited_external_fallback(
        residual_expression: impl Into<String>,
        rejected_operation: impl Into<String>,
    ) -> Self {
        Self {
            residual_required: true,
            residual_executor: RESIDUAL_EXECUTOR_PROHIBITED_EXTERNAL_FALLBACK,
            residual_expression: Some(residual_expression.into()),
            accepted_operations: Vec::new(),
            rejected_operations: vec![rejected_operation.into()],
            shardloom_native_residual_execution: false,
            external_engine_invoked: false,
            prohibited_external_fallback: true,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn residual_executor_values() -> &'static [&'static str] {
        &[
            RESIDUAL_EXECUTOR_NONE,
            RESIDUAL_EXECUTOR_SHARDLOOM_NATIVE,
            RESIDUAL_EXECUTOR_UNSUPPORTED_BLOCKED,
            RESIDUAL_EXECUTOR_EXTERNAL_BASELINE_ONLY,
            RESIDUAL_EXECUTOR_PROHIBITED_EXTERNAL_FALLBACK,
        ]
    }

    #[must_use]
    pub const fn external_fallback_blocked(&self) -> bool {
        !self.external_engine_invoked
            && self.prohibited_external_fallback
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSourceBackedExpansionEvidenceReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_report_id: String,
    pub correctness_evidence_present: bool,
    pub correctness_refs: Vec<String>,
    pub benchmark_rows_required: bool,
    pub benchmark_rows_present: bool,
    pub benchmark_refs: Vec<String>,
    pub benchmark_claim_allowed: bool,
    pub execution_certificate_present: bool,
    pub execution_certificate_refs: Vec<String>,
    pub native_io_certificate_present: bool,
    pub native_io_certificate_refs: Vec<String>,
    pub native_io_certificate_path_refs: Vec<String>,
    pub certificate_pair_report_ref: String,
    pub no_fallback_evidence_present: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
}

impl VortexSourceBackedExpansionEvidenceReport {
    fn from_filter(report: &VortexSourceBackedEncodedFilterExecutionReport) -> Self {
        let execution_certificate = &report.prepared_execution.execution_certificate;
        let native_io_certificate = &report.prepared_execution.native_io_certificate;
        let correctness_refs = execution_certificate
            .correctness_fixture_id
            .clone()
            .map_or_else(
                || vec![execution_certificate.certificate_id.clone()],
                |fixture_id| vec![fixture_id],
            );
        Self {
            schema_version: "shardloom.vortex_source_backed_expansion_evidence.v1",
            report_id: format!("{}.evidence", report.report_id),
            execution_kind: report.execution_kind,
            source_report_id: report.report_id.clone(),
            correctness_evidence_present: report.correctness_certified,
            correctness_refs,
            benchmark_rows_required: true,
            benchmark_rows_present: false,
            benchmark_refs: vec![
                "cg6.source_backed_encoded_execution.deferred_no_claim".to_string(),
            ],
            benchmark_claim_allowed: false,
            execution_certificate_present: execution_certificate_present(execution_certificate),
            execution_certificate_refs: vec![execution_certificate.certificate_id.clone()],
            native_io_certificate_present: native_io_certificate_present(native_io_certificate),
            native_io_certificate_refs: vec![native_io_certificate.certificate_id.clone()],
            native_io_certificate_path_refs: vec![native_io_certificate.path_id.clone()],
            certificate_pair_report_ref: format!("{}.certificate-pair", report.report_id),
            no_fallback_evidence_present: !report.fallback_attempted
                && !report.fallback_execution_allowed
                && !report.external_effects_executed,
            external_engine_invoked: false,
            fallback_execution_allowed: report.fallback_execution_allowed,
            fallback_attempted: report.fallback_attempted,
            production_claim_allowed: false,
        }
    }

    fn from_projection(report: &VortexSourceBackedEncodedProjectionExecutionReport) -> Self {
        let execution_certificate = &report.prepared_execution.execution_certificate;
        let native_io_certificate = &report.prepared_execution.native_io_certificate;
        let correctness_refs = execution_certificate
            .correctness_fixture_id
            .clone()
            .map_or_else(
                || vec![execution_certificate.certificate_id.clone()],
                |fixture_id| vec![fixture_id],
            );
        Self {
            schema_version: "shardloom.vortex_source_backed_expansion_evidence.v1",
            report_id: format!("{}.evidence", report.report_id),
            execution_kind: report.execution_kind,
            source_report_id: report.report_id.clone(),
            correctness_evidence_present: report.correctness_certified,
            correctness_refs,
            benchmark_rows_required: true,
            benchmark_rows_present: false,
            benchmark_refs: vec![
                "cg6.source_backed_encoded_execution.deferred_no_claim".to_string(),
            ],
            benchmark_claim_allowed: false,
            execution_certificate_present: execution_certificate_present(execution_certificate),
            execution_certificate_refs: vec![execution_certificate.certificate_id.clone()],
            native_io_certificate_present: native_io_certificate_present(native_io_certificate),
            native_io_certificate_refs: vec![native_io_certificate.certificate_id.clone()],
            native_io_certificate_path_refs: vec![native_io_certificate.path_id.clone()],
            certificate_pair_report_ref: format!("{}.certificate-pair", report.report_id),
            no_fallback_evidence_present: !report.fallback_attempted
                && !report.fallback_execution_allowed
                && !report.external_effects_executed,
            external_engine_invoked: false,
            fallback_execution_allowed: report.fallback_execution_allowed,
            fallback_attempted: report.fallback_attempted,
            production_claim_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocks_claims_without_benchmarks(&self) -> bool {
        self.benchmark_rows_required
            && !self.benchmark_rows_present
            && !self.benchmark_claim_allowed
            && !self.production_claim_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSourceBackedCertificatePairReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_report_id: String,
    pub execution_certificate_id: String,
    pub execution_certificate_status: &'static str,
    pub execution_certificate_present: bool,
    pub native_io_certificate_id: String,
    pub native_io_certificate_path_id: String,
    pub native_io_certificate_status: &'static str,
    pub native_io_certificate_present: bool,
    pub per_path_native_io_certificate: bool,
    pub certificate_pair_complete: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexSourceBackedCertificatePairReport {
    fn from_filter(report: &VortexSourceBackedEncodedFilterExecutionReport) -> Self {
        let execution_certificate = &report.prepared_execution.execution_certificate;
        let native_io_certificate = &report.prepared_execution.native_io_certificate;
        let per_path_native_io_certificate = !native_io_certificate.path_id.trim().is_empty();
        let certificate_pair_complete = execution_certificate.is_certified()
            && native_io_certificate.is_certified()
            && per_path_native_io_certificate
            && !report.fallback_attempted
            && !report.fallback_execution_allowed;
        Self {
            schema_version: "shardloom.vortex_source_backed_certificate_pair.v1",
            report_id: format!("{}.certificate-pair", report.report_id),
            execution_kind: report.execution_kind,
            source_report_id: report.report_id.clone(),
            execution_certificate_id: execution_certificate.certificate_id.clone(),
            execution_certificate_status: execution_certificate.status.as_str(),
            execution_certificate_present: execution_certificate_present(execution_certificate),
            native_io_certificate_id: native_io_certificate.certificate_id.clone(),
            native_io_certificate_path_id: native_io_certificate.path_id.clone(),
            native_io_certificate_status: native_io_certificate.status(),
            native_io_certificate_present: native_io_certificate_present(native_io_certificate),
            per_path_native_io_certificate,
            certificate_pair_complete,
            external_engine_invoked: false,
            fallback_execution_allowed: report.fallback_execution_allowed,
            fallback_attempted: report.fallback_attempted,
        }
    }

    fn from_projection(report: &VortexSourceBackedEncodedProjectionExecutionReport) -> Self {
        let execution_certificate = &report.prepared_execution.execution_certificate;
        let native_io_certificate = &report.prepared_execution.native_io_certificate;
        let per_path_native_io_certificate = !native_io_certificate.path_id.trim().is_empty();
        let certificate_pair_complete = execution_certificate.is_certified()
            && native_io_certificate.is_certified()
            && per_path_native_io_certificate
            && !report.fallback_attempted
            && !report.fallback_execution_allowed;
        Self {
            schema_version: "shardloom.vortex_source_backed_certificate_pair.v1",
            report_id: format!("{}.certificate-pair", report.report_id),
            execution_kind: report.execution_kind,
            source_report_id: report.report_id.clone(),
            execution_certificate_id: execution_certificate.certificate_id.clone(),
            execution_certificate_status: execution_certificate.status.as_str(),
            execution_certificate_present: execution_certificate_present(execution_certificate),
            native_io_certificate_id: native_io_certificate.certificate_id.clone(),
            native_io_certificate_path_id: native_io_certificate.path_id.clone(),
            native_io_certificate_status: native_io_certificate.status(),
            native_io_certificate_present: native_io_certificate_present(native_io_certificate),
            per_path_native_io_certificate,
            certificate_pair_complete,
            external_engine_invoked: false,
            fallback_execution_allowed: report.fallback_execution_allowed,
            fallback_attempted: report.fallback_attempted,
        }
    }

    #[must_use]
    pub const fn claim_ready_before_benchmarks(&self) -> bool {
        false
    }
}

fn execution_certificate_present(certificate: &ExecutionCertificate) -> bool {
    !certificate.schema_version.trim().is_empty() && !certificate.certificate_id.trim().is_empty()
}

fn native_io_certificate_present(certificate: &NativeIoCertificate) -> bool {
    !certificate.schema_version.trim().is_empty() && !certificate.certificate_id.trim().is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSourceBackedEncodedExecutionStatus {
    ExecutedSourceBackedPreparedEncodedBatches,
    BlockedNonNativeSource,
    BlockedMissingSourceUri,
    BlockedSourceBatchMismatch,
    BlockedMissingSplitRef,
    BlockedPreparedExecution,
}

impl VortexSourceBackedEncodedExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutedSourceBackedPreparedEncodedBatches => {
                "executed_source_backed_prepared_encoded_batches"
            }
            Self::BlockedNonNativeSource => "blocked_non_native_source",
            Self::BlockedMissingSourceUri => "blocked_missing_source_uri",
            Self::BlockedSourceBatchMismatch => "blocked_source_batch_mismatch",
            Self::BlockedMissingSplitRef => "blocked_missing_split_ref",
            Self::BlockedPreparedExecution => "blocked_prepared_execution",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ExecutedSourceBackedPreparedEncodedBatches)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReaderBackedEncodedExecutionStatus {
    ExecutedReaderValidatedPreparedEncodedBatches,
    BlockedNonNativeSource,
    BlockedMissingSourceUri,
    BlockedMissingReaderSplitEvidence,
    BlockedReaderSourceMismatch,
    BlockedPreparedBatchSplitMismatch,
    BlockedUnsafeReaderEffects,
    BlockedSourceBackedExecution,
}

impl VortexReaderBackedEncodedExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutedReaderValidatedPreparedEncodedBatches => {
                "executed_reader_validated_prepared_encoded_batches"
            }
            Self::BlockedNonNativeSource => "blocked_non_native_source",
            Self::BlockedMissingSourceUri => "blocked_missing_source_uri",
            Self::BlockedMissingReaderSplitEvidence => "blocked_missing_reader_split_evidence",
            Self::BlockedReaderSourceMismatch => "blocked_reader_source_mismatch",
            Self::BlockedPreparedBatchSplitMismatch => "blocked_prepared_batch_split_mismatch",
            Self::BlockedUnsafeReaderEffects => "blocked_unsafe_reader_effects",
            Self::BlockedSourceBackedExecution => "blocked_source_backed_execution",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ExecutedReaderValidatedPreparedEncodedBatches)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReaderGeneratedPreparedBatchStatus {
    PreparedReaderChunkEnvelopes,
    PreparedEncodedKernelInputs,
    BlockedNonNativeSource,
    BlockedMissingSourceUri,
    BlockedMissingReaderSplitEvidence,
    BlockedReaderSourceMismatch,
    BlockedKernelInputSourceMismatch,
    BlockedKernelInputSplitMismatch,
    BlockedKernelInputRowCountMismatch,
    BlockedKernelInputMappingEvidence,
    BlockedUnsafeReaderEffects,
}

impl VortexReaderGeneratedPreparedBatchStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedReaderChunkEnvelopes => "prepared_reader_chunk_envelopes",
            Self::PreparedEncodedKernelInputs => "prepared_encoded_kernel_inputs",
            Self::BlockedNonNativeSource => "blocked_non_native_source",
            Self::BlockedMissingSourceUri => "blocked_missing_source_uri",
            Self::BlockedMissingReaderSplitEvidence => "blocked_missing_reader_split_evidence",
            Self::BlockedReaderSourceMismatch => "blocked_reader_source_mismatch",
            Self::BlockedKernelInputSourceMismatch => "blocked_kernel_input_source_mismatch",
            Self::BlockedKernelInputSplitMismatch => "blocked_kernel_input_split_mismatch",
            Self::BlockedKernelInputRowCountMismatch => "blocked_kernel_input_row_count_mismatch",
            Self::BlockedKernelInputMappingEvidence => "blocked_kernel_input_mapping_evidence",
            Self::BlockedUnsafeReaderEffects => "blocked_unsafe_reader_effects",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::PreparedReaderChunkEnvelopes | Self::PreparedEncodedKernelInputs
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VortexNativeProviderBoundary {
    pub provider_kind: &'static str,
    pub provider_crate: &'static str,
    pub provider_version: &'static str,
    pub provider_api_surface: &'static str,
    pub feature_gate: &'static str,
    pub admission_policy: &'static str,
    pub certificate_requirement: &'static str,
    pub support_claim_allowed_without_certificate: bool,
    pub external_query_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexNativeProviderBoundary {
    #[must_use]
    pub const fn local_scan() -> Self {
        Self {
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_crate: LOCAL_SCAN_PROVIDER_CRATE,
            provider_version: LOCAL_SCAN_PROVIDER_VERSION,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            feature_gate: LOCAL_SCAN_FEATURE_GATE,
            admission_policy: LOCAL_SCAN_ADMISSION_POLICY,
            certificate_requirement: LOCAL_SCAN_CERTIFICATE_REQUIREMENT,
            support_claim_allowed_without_certificate: false,
            external_query_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn is_policy_admitted(&self) -> bool {
        self.feature_gate == LOCAL_SCAN_FEATURE_GATE
            && self.admission_policy == LOCAL_SCAN_ADMISSION_POLICY
            && self.provider_crate == LOCAL_SCAN_PROVIDER_CRATE
            && self.provider_version == LOCAL_SCAN_PROVIDER_VERSION
            && !self.support_claim_allowed_without_certificate
            && !self.external_query_engine_invoked
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderBackedSplitEvidence {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub row_count: usize,
    pub dtype_summary: String,
    pub encoding_id: String,
    pub child_count: usize,
    pub buffer_count: usize,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexReaderBackedSplitEvidence {
    /// # Errors
    /// Returns an error when `split_ref` is empty or whitespace only.
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        row_count: usize,
        dtype_summary: impl Into<String>,
        encoding_id: impl Into<String>,
        child_count: usize,
        buffer_count: usize,
    ) -> Result<Self> {
        let split_ref = validated_split_ref(split_ref)?;
        Ok(Self {
            source_uri,
            split_ref,
            provider_boundary: VortexNativeProviderBoundary::local_scan(),
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            row_count,
            dtype_summary: dtype_summary.into(),
            encoding_id: encoding_id.into(),
            child_count,
            buffer_count,
            data_read: true,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
        })
    }

    /// # Errors
    /// Returns an error when `chunk_index` does not produce a valid split ref.
    pub fn local_scan_chunk(
        source_uri: DatasetUri,
        chunk_index: usize,
        row_count: usize,
        dtype_summary: impl Into<String>,
        encoding_id: impl Into<String>,
        child_count: usize,
        buffer_count: usize,
    ) -> Result<Self> {
        Self::new(
            source_uri,
            format!("vortex-local-scan-chunk-{chunk_index}"),
            row_count,
            dtype_summary,
            encoding_id,
            child_count,
            buffer_count,
        )
    }

    #[must_use]
    pub const fn has_forbidden_effects(&self) -> bool {
        self.data_decoded
            || self.data_materialized
            || self.row_read
            || self.arrow_converted
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || self.external_effects_executed
            || self.fallback_execution_allowed
            || self.fallback_attempted
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{} rows={} dtype={} encoding={} children={} buffers={}",
            self.split_ref,
            self.row_count,
            self.dtype_summary,
            self.encoding_id,
            self.child_count,
            self.buffer_count
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderGeneratedEncodedKernelInput {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub batch: VortexEncodedValuePredicateBatch,
    pub dtype_mapped_without_decode: bool,
    pub encoding_mapped_without_decode: bool,
    pub values_mapped_without_decode: bool,
    pub row_count_matches_segment: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexReaderGeneratedEncodedKernelInput {
    /// # Errors
    /// Returns an error when `split_ref` is empty or the encoded value row count
    /// cannot be matched to the supplied segment metadata.
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        batch: VortexEncodedValuePredicateBatch,
    ) -> Result<Self> {
        let split_ref = validated_split_ref(split_ref)?;
        let values_row_count = batch.values.row_count().ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "reader-generated encoded kernel input values require a known row count"
                    .to_string(),
            )
        })?;
        let segment_row_count = batch.segment.stats.row_count.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "reader-generated encoded kernel input segment requires row-count metadata"
                    .to_string(),
            )
        })?;
        if values_row_count != segment_row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "reader-generated encoded kernel input row count mismatch: values={values_row_count} segment={segment_row_count}"
            )));
        }
        Ok(Self {
            source_uri,
            split_ref,
            provider_boundary: VortexNativeProviderBoundary::local_scan(),
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            batch,
            dtype_mapped_without_decode: true,
            encoding_mapped_without_decode: true,
            values_mapped_without_decode: true,
            row_count_matches_segment: true,
            data_read: true,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
        })
    }

    #[must_use]
    pub const fn mapping_evidence_complete(&self) -> bool {
        self.dtype_mapped_without_decode
            && self.encoding_mapped_without_decode
            && self.values_mapped_without_decode
            && self.row_count_matches_segment
    }

    #[must_use]
    pub const fn has_forbidden_effects(&self) -> bool {
        self.data_decoded
            || self.data_materialized
            || self.row_read
            || self.arrow_converted
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || self.external_effects_executed
            || self.fallback_execution_allowed
            || self.fallback_attempted
    }

    /// # Errors
    /// Returns an error when the split ref fails source-backed batch validation.
    pub fn to_source_backed_filter_batch(
        &self,
    ) -> Result<VortexSourceBackedEncodedValuePredicateBatch> {
        VortexSourceBackedEncodedValuePredicateBatch::new(
            self.source_uri.clone(),
            self.split_ref.clone(),
            self.batch.clone(),
        )
    }

    /// # Errors
    /// Returns an error when the split ref fails source-backed projection validation.
    pub fn to_source_backed_projection_column(
        &self,
    ) -> Result<VortexSourceBackedEncodedProjectionColumn> {
        VortexSourceBackedEncodedProjectionColumn::new(
            self.source_uri.clone(),
            self.split_ref.clone(),
            VortexPreparedEncodedProjectionColumn::new(
                self.batch.segment.clone(),
                self.batch.values.clone(),
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderGeneratedPreparedBatchEvidence {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub row_count: usize,
    pub dtype_summary: String,
    pub encoding_id: String,
    pub child_count: usize,
    pub buffer_count: usize,
    pub representation_before: &'static str,
    pub representation_after: &'static str,
    pub encoded_value_batch_available: bool,
    pub encoded_projection_batch_available: bool,
    pub encoded_kernel_input_count: usize,
    pub residual_required: bool,
    pub residual_executor: &'static str,
    pub residual_boundary: VortexResidualBoundaryReport,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
}

impl VortexReaderGeneratedPreparedBatchEvidence {
    #[must_use]
    pub fn from_reader_split(
        split: &VortexReaderBackedSplitEvidence,
        encoded_kernel_input_count: usize,
    ) -> Self {
        let encoded_kernel_inputs_available = encoded_kernel_input_count > 0;
        let residual_boundary = if encoded_kernel_inputs_available {
            VortexResidualBoundaryReport::none("reader_generated_encoded_kernel_input")
        } else {
            VortexResidualBoundaryReport::unsupported_blocked(
                "opaque_vortex_reader_chunk",
                "reader_chunk_residual_without_encoded_kernel_input",
            )
        };
        Self {
            source_uri: split.source_uri.clone(),
            split_ref: split.split_ref.clone(),
            provider_boundary: split.provider_boundary,
            provider_kind: split.provider_kind,
            provider_api_surface: split.provider_api_surface,
            row_count: split.row_count,
            dtype_summary: split.dtype_summary.clone(),
            encoding_id: split.encoding_id.clone(),
            child_count: split.child_count,
            buffer_count: split.buffer_count,
            representation_before: REPRESENTATION_VORTEX_READER_CHUNK,
            representation_after: if encoded_kernel_inputs_available {
                REPRESENTATION_PREPARED_ENCODED_KERNEL_INPUT
            } else {
                REPRESENTATION_PREPARED_CHUNK_ENVELOPE
            },
            encoded_value_batch_available: encoded_kernel_inputs_available,
            encoded_projection_batch_available: encoded_kernel_inputs_available,
            encoded_kernel_input_count,
            residual_required: residual_boundary.residual_required,
            residual_executor: residual_boundary.residual_executor,
            residual_boundary,
            data_read: split.data_read,
            data_decoded: split.data_decoded,
            data_materialized: split.data_materialized,
            row_read: split.row_read,
            arrow_converted: split.arrow_converted,
            object_store_io: split.object_store_io,
            write_io: split.write_io,
            spill_io_performed: split.spill_io_performed,
            external_effects_executed: split.external_effects_executed,
            fallback_execution_allowed: split.fallback_execution_allowed,
            fallback_attempted: split.fallback_attempted,
        }
    }

    #[must_use]
    pub const fn has_forbidden_effects(&self) -> bool {
        self.data_decoded
            || self.data_materialized
            || self.row_read
            || self.arrow_converted
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || self.external_effects_executed
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || !self.residual_boundary.external_fallback_blocked()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderGeneratedPreparedBatchReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexReaderGeneratedPreparedBatchStatus,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub reader_split_count: usize,
    pub generated_batch_count: usize,
    pub total_rows: usize,
    pub split_refs: Vec<String>,
    pub encoded_kernel_input_split_refs: Vec<String>,
    pub reader_source_uri_matches_source: bool,
    pub encoded_kernel_inputs_source_uri_matches_source: bool,
    pub encoded_kernel_input_split_refs_covered_by_reader: bool,
    pub encoded_kernel_input_row_counts_match_reader: bool,
    pub encoded_kernel_input_mapping_evidence_complete: bool,
    pub reader_generated_prepared_batches: bool,
    pub reader_chunk_envelopes_available: bool,
    pub encoded_value_batch_available: bool,
    pub encoded_projection_batch_available: bool,
    pub encoded_kernel_input_count: usize,
    pub kernel_input_lowering_blocked: bool,
    pub runtime_execution_allowed: bool,
    pub residual_required: bool,
    pub residual_executor: &'static str,
    pub residual_boundary: VortexResidualBoundaryReport,
    pub representation_before: &'static str,
    pub representation_after: &'static str,
    pub batches: Vec<VortexReaderGeneratedPreparedBatchEvidence>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexReaderGeneratedPreparedBatchReport {
    fn from_validation(
        source: &UniversalInputSource,
        validation: ReaderGeneratedPreparedBatchValidation,
    ) -> Self {
        let chunk_envelopes_available = validation.status
            == VortexReaderGeneratedPreparedBatchStatus::PreparedReaderChunkEnvelopes
            || validation.status
                == VortexReaderGeneratedPreparedBatchStatus::PreparedEncodedKernelInputs;
        let encoded_kernel_inputs_available = validation.status
            == VortexReaderGeneratedPreparedBatchStatus::PreparedEncodedKernelInputs
            && validation.encoded_kernel_input_count > 0;
        let encoded_value_batch_available = encoded_kernel_inputs_available;
        let encoded_projection_batch_available = encoded_kernel_inputs_available;
        let kernel_input_lowering_blocked = (chunk_envelopes_available
            || validation.encoded_kernel_input_count > 0)
            && !encoded_kernel_inputs_available;
        let residual_boundary = if kernel_input_lowering_blocked {
            VortexResidualBoundaryReport::unsupported_blocked(
                "opaque_vortex_reader_chunk",
                "reader_chunk_residual_without_encoded_kernel_input",
            )
        } else {
            VortexResidualBoundaryReport::none("reader_generated_encoded_kernel_inputs")
        };
        Self {
            schema_version: READER_GENERATED_BATCH_SCHEMA_VERSION,
            report_id: READER_GENERATED_BATCH_REPORT_ID.to_string(),
            execution_kind: READER_GENERATED_BATCH_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status: validation.status,
            provider_boundary: VortexNativeProviderBoundary::local_scan(),
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            reader_split_count: validation.reader_split_refs.len(),
            generated_batch_count: validation.batches.len(),
            total_rows: validation.total_rows,
            split_refs: validation.reader_split_refs,
            encoded_kernel_input_split_refs: validation.encoded_kernel_input_split_refs,
            reader_source_uri_matches_source: validation.reader_source_uri_matches_source,
            encoded_kernel_inputs_source_uri_matches_source: validation
                .encoded_kernel_inputs_source_uri_matches_source,
            encoded_kernel_input_split_refs_covered_by_reader: validation
                .encoded_kernel_input_split_refs_covered_by_reader,
            encoded_kernel_input_row_counts_match_reader: validation
                .encoded_kernel_input_row_counts_match_reader,
            encoded_kernel_input_mapping_evidence_complete: validation
                .encoded_kernel_input_mapping_evidence_complete,
            reader_generated_prepared_batches: chunk_envelopes_available,
            reader_chunk_envelopes_available: chunk_envelopes_available,
            encoded_value_batch_available,
            encoded_projection_batch_available,
            encoded_kernel_input_count: validation.encoded_kernel_input_count,
            kernel_input_lowering_blocked,
            runtime_execution_allowed: encoded_kernel_inputs_available,
            residual_required: residual_boundary.residual_required,
            residual_executor: residual_boundary.residual_executor,
            residual_boundary,
            representation_before: REPRESENTATION_VORTEX_READER_CHUNK,
            representation_after: if encoded_kernel_inputs_available {
                REPRESENTATION_PREPARED_ENCODED_KERNEL_INPUT
            } else {
                REPRESENTATION_PREPARED_CHUNK_ENVELOPE
            },
            batches: validation.batches,
            data_read: validation.data_read,
            data_decoded: validation.data_decoded,
            data_materialized: validation.data_materialized,
            row_read: validation.row_read,
            arrow_converted: validation.arrow_converted,
            object_store_io: validation.object_store_io,
            write_io: validation.write_io,
            spill_io_performed: validation.spill_io_performed,
            external_effects_executed: validation.external_effects_executed,
            fallback_execution_allowed: validation.fallback_execution_allowed,
            fallback_attempted: validation.fallback_attempted
                || validation
                    .diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.fallback.attempted),
            diagnostics: validation.diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_forbidden_effects(&self) -> bool {
        !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
            && self.residual_boundary.external_fallback_blocked()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || !self.avoids_forbidden_effects()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex reader-generated prepared batch envelope");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "source: {}", self.source_summary);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "reader splits: {}", self.reader_split_count);
        let _ = writeln!(
            &mut out,
            "generated chunk envelopes: {}",
            self.generated_batch_count
        );
        let _ = writeln!(
            &mut out,
            "encoded value batch available: {}",
            self.encoded_value_batch_available
        );
        let _ = writeln!(
            &mut out,
            "encoded kernel inputs: {}",
            self.encoded_kernel_input_count
        );
        let _ = writeln!(
            &mut out,
            "kernel input lowering blocked: {}",
            self.kernel_input_lowering_blocked
        );
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReaderGeneratedConjunctiveSelectionVectorStatus {
    IntersectedSelectionVectors,
    BlockedPreparedBatchValidation,
    BlockedMissingPredicate,
    BlockedMissingPredicateInput,
    BlockedPredicateEvaluation,
    BlockedSelectionVectorIntersection,
}

impl VortexReaderGeneratedConjunctiveSelectionVectorStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntersectedSelectionVectors => "intersected_selection_vectors",
            Self::BlockedPreparedBatchValidation => "blocked_prepared_batch_validation",
            Self::BlockedMissingPredicate => "blocked_missing_predicate",
            Self::BlockedMissingPredicateInput => "blocked_missing_predicate_input",
            Self::BlockedPredicateEvaluation => "blocked_predicate_evaluation",
            Self::BlockedSelectionVectorIntersection => "blocked_selection_vector_intersection",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::IntersectedSelectionVectors)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexReaderGeneratedConjunctiveSelectionVectorStatus,
    pub predicate_count: usize,
    pub reader_split_count: usize,
    pub encoded_kernel_input_count: usize,
    pub intersection_count: usize,
    pub selected_row_count: Option<u64>,
    pub reader_generated_prepared_batches: bool,
    pub filter_column_batches_consumed: bool,
    pub selection_vector_intersection_certified: bool,
    pub runtime_execution_allowed: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport {
    fn from_parts(
        source: &UniversalInputSource,
        lowering: &VortexReaderGeneratedPreparedBatchReport,
        status: VortexReaderGeneratedConjunctiveSelectionVectorStatus,
        predicate_count: usize,
        intersection_count: usize,
        selected_row_count: Option<u64>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        let has_error_diagnostics = diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        });
        let avoids_forbidden_effects = !lowering.data_decoded
            && !lowering.data_materialized
            && !lowering.row_read
            && !lowering.arrow_converted
            && !lowering.object_store_io
            && !lowering.write_io
            && !lowering.spill_io_performed
            && !lowering.external_effects_executed
            && !lowering.fallback_execution_allowed
            && !lowering.fallback_attempted;
        let runtime_execution_allowed = status
            == VortexReaderGeneratedConjunctiveSelectionVectorStatus::IntersectedSelectionVectors
            && lowering.runtime_execution_allowed
            && avoids_forbidden_effects
            && !has_error_diagnostics;
        Self {
            schema_version: READER_CONJUNCTIVE_FILTER_SCHEMA_VERSION,
            report_id: READER_CONJUNCTIVE_FILTER_REPORT_ID.to_string(),
            execution_kind: READER_CONJUNCTIVE_FILTER_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status,
            predicate_count,
            reader_split_count: lowering.reader_split_count,
            encoded_kernel_input_count: lowering.encoded_kernel_input_count,
            intersection_count,
            selected_row_count,
            reader_generated_prepared_batches: lowering.reader_generated_prepared_batches,
            filter_column_batches_consumed: runtime_execution_allowed,
            selection_vector_intersection_certified: runtime_execution_allowed,
            runtime_execution_allowed,
            correctness_certified: false,
            production_claim_allowed: false,
            data_read: lowering.data_read,
            data_decoded: lowering.data_decoded,
            data_materialized: lowering.data_materialized,
            row_read: lowering.row_read,
            arrow_converted: lowering.arrow_converted,
            object_store_io: lowering.object_store_io,
            write_io: lowering.write_io,
            spill_io_performed: lowering.spill_io_performed,
            external_effects_executed: lowering.external_effects_executed,
            external_engine_invoked: false,
            fallback_execution_allowed: lowering.fallback_execution_allowed,
            fallback_attempted: lowering.fallback_attempted
                || diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.fallback.attempted),
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.data_decoded
            || self.data_materialized
            || self.row_read
            || self.arrow_converted
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || self.external_effects_executed
            || self.external_engine_invoked
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
}

/// Intersects per-column reader-generated predicate selection vectors for one
/// conjunctive filter after the caller supplies explicit encoded kernel inputs.
///
/// This does not infer values from opaque Vortex chunks. The bridge is admitted
/// only when every predicate has a matching reader-generated encoded kernel
/// input for every reader split and all selection vectors intersect safely.
///
/// # Errors
/// Returns an error if predicate evaluation over an admitted encoded kernel
/// input cannot construct report evidence.
#[allow(clippy::too_many_lines)]
pub fn execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
    predicates: &[PredicateExpr],
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    encoded_kernel_inputs: &[VortexReaderGeneratedEncodedKernelInput],
) -> Result<VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport> {
    let lowering = plan_vortex_reader_generated_prepared_batch_kernel_inputs(
        source,
        reader_splits,
        encoded_kernel_inputs,
    );
    let mut diagnostics = lowering.diagnostics.clone();
    if predicates.is_empty() {
        diagnostics.push(Diagnostic::configuration_error(
            "vortex_reader_generated_conjunctive_selection_vector_bridge",
            "conjunctive reader-generated filter bridge requires at least one predicate",
            "Attach one predicate per filter-column encoded kernel input before requesting the bridge.",
        ));
        return Ok(
            VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport::from_parts(
                source,
                &lowering,
                VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedMissingPredicate,
                0,
                0,
                None,
                diagnostics,
            ),
        );
    }
    if !lowering.runtime_execution_allowed {
        return Ok(
            VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport::from_parts(
                source,
                &lowering,
                VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedPreparedBatchValidation,
                predicates.len(),
                0,
                None,
                diagnostics,
            ),
        );
    }

    let mut status =
        VortexReaderGeneratedConjunctiveSelectionVectorStatus::IntersectedSelectionVectors;
    let mut intersection_count = 0_usize;
    let mut selected_row_count = 0_u64;
    for split in reader_splits {
        let mut split_vectors = Vec::<SelectionVector>::new();
        for predicate in predicates {
            let Some(column) = predicate.column() else {
                diagnostics.push(Diagnostic::unsupported(
                    DiagnosticCode::NoFallbackExecution,
                    "vortex_reader_generated_conjunctive_selection_vector_bridge",
                    "conjunctive reader-generated filter bridge requires column predicates",
                    Some(
                        "Use column-bound predicates for reader-generated filter-column batches."
                            .to_string(),
                    ),
                ));
                status =
                    VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedMissingPredicateInput;
                continue;
            };
            let matching_inputs = encoded_kernel_inputs
                .iter()
                .filter(|input| {
                    input.split_ref == split.split_ref && input.batch.segment.column == *column
                })
                .collect::<Vec<_>>();
            let Some(input) = matching_inputs.first().copied() else {
                diagnostics.push(Diagnostic::unsupported(
                    DiagnosticCode::NoFallbackExecution,
                    "vortex_reader_generated_conjunctive_selection_vector_bridge",
                    format!(
                        "missing reader-generated encoded kernel input for column {} on split {}",
                        column.as_str(),
                        split.split_ref
                    ),
                    Some("Provide one admitted encoded kernel input per predicate column and reader split.".to_string()),
                ));
                status =
                    VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedMissingPredicateInput;
                continue;
            };
            if matching_inputs.len() > 1 {
                diagnostics.push(Diagnostic::configuration_error(
                    "vortex_reader_generated_conjunctive_selection_vector_bridge",
                    format!(
                        "ambiguous reader-generated encoded kernel inputs for column {} on split {}",
                        column.as_str(),
                        split.split_ref
                    ),
                    "Provide exactly one encoded kernel input per predicate column and split.",
                ));
                status =
                    VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedMissingPredicateInput;
                continue;
            }
            let predicate_report = evaluate_vortex_encoded_value_predicate_batch(
                predicate,
                &input.batch.segment,
                &input.batch.values,
            );
            diagnostics.extend(predicate_report.diagnostics.clone());
            let Some(selection_vector) = predicate_report
                .segment_reports
                .first()
                .and_then(|report| report.selection_vector.clone())
            else {
                diagnostics.push(Diagnostic::unsupported(
                    DiagnosticCode::NoFallbackExecution,
                    "vortex_reader_generated_conjunctive_selection_vector_bridge",
                    format!(
                        "predicate {} did not emit a selection vector for split {}",
                        predicate.summary(),
                        split.split_ref
                    ),
                    Some("Keep the bridge blocked until every predicate emits encoded selection-vector evidence.".to_string()),
                ));
                status =
                    VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedPredicateEvaluation;
                continue;
            };
            split_vectors.push(selection_vector);
        }
        if split_vectors.len() != predicates.len() {
            continue;
        }
        match intersect_selection_vectors(split_vectors.iter()) {
            Ok(selection_vector) => {
                intersection_count += 1;
                selected_row_count = selected_row_count
                    .checked_add(selection_vector.selected_count())
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "reader-generated conjunctive selected row count overflow".to_string(),
                        )
                    })?;
            }
            Err(reason) => {
                diagnostics.push(Diagnostic::unsupported(
                    DiagnosticCode::NoFallbackExecution,
                    "vortex_reader_generated_conjunctive_selection_vector_bridge",
                    format!(
                        "selection-vector intersection failed for split {}: {reason}",
                        split.split_ref
                    ),
                    Some("Keep conjunctive filter admission blocked until selection-vector boundaries match.".to_string()),
                ));
                status =
                    VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedSelectionVectorIntersection;
            }
        }
    }
    let bridge_complete = status
        == VortexReaderGeneratedConjunctiveSelectionVectorStatus::IntersectedSelectionVectors
        && intersection_count == reader_splits.len();
    let selected_row_count = bridge_complete.then_some(selected_row_count);
    let final_status = if bridge_complete {
        status
    } else if status
        == VortexReaderGeneratedConjunctiveSelectionVectorStatus::IntersectedSelectionVectors
    {
        VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedSelectionVectorIntersection
    } else {
        status
    };
    Ok(
        VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport::from_parts(
            source,
            &lowering,
            final_status,
            predicates.len(),
            intersection_count,
            selected_row_count,
            diagnostics,
        ),
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSourceBackedEncodedValuePredicateBatch {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub batch: VortexEncodedValuePredicateBatch,
}

impl VortexSourceBackedEncodedValuePredicateBatch {
    /// # Errors
    /// Returns an error when `split_ref` is empty or whitespace only.
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        batch: VortexEncodedValuePredicateBatch,
    ) -> Result<Self> {
        let split_ref = validated_split_ref(split_ref)?;
        Ok(Self {
            source_uri,
            split_ref,
            batch,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSourceBackedEncodedProjectionColumn {
    pub source_uri: DatasetUri,
    pub split_ref: String,
    pub column: VortexPreparedEncodedProjectionColumn,
}

impl VortexSourceBackedEncodedProjectionColumn {
    /// # Errors
    /// Returns an error when `split_ref` is empty or whitespace only.
    pub fn new(
        source_uri: DatasetUri,
        split_ref: impl Into<String>,
        column: VortexPreparedEncodedProjectionColumn,
    ) -> Result<Self> {
        let split_ref = validated_split_ref(split_ref)?;
        Ok(Self {
            source_uri,
            split_ref,
            column,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSourceBackedEncodedFilterExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexSourceBackedEncodedExecutionStatus,
    pub split_count: usize,
    pub source_batch_count: usize,
    pub source_uri_matches_batches: bool,
    pub prepared_execution: VortexGeneralizedEncodedFilterExecutionReport,
    pub runtime_execution_allowed: bool,
    pub source_backed_batches_consumed: bool,
    pub selection_vector_guaranteed: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexSourceBackedEncodedFilterExecutionReport {
    fn from_parts(
        source: &UniversalInputSource,
        validation: SourceBackedValidation,
        prepared_execution: VortexGeneralizedEncodedFilterExecutionReport,
    ) -> Self {
        let mut diagnostics = validation.diagnostics;
        diagnostics.extend(prepared_execution.diagnostics.clone());
        let status = validation.status.unwrap_or_else(|| {
            if prepared_execution.status
                == VortexGeneralizedEncodedFilterExecutionStatus::ExecutedPreparedEncodedValues
                && prepared_execution.runtime_execution_allowed
                && !prepared_execution.has_errors()
            {
                VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
            } else {
                VortexSourceBackedEncodedExecutionStatus::BlockedPreparedExecution
            }
        });
        let runtime_execution_allowed = status
            == VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches;
        Self {
            schema_version: FILTER_SCHEMA_VERSION,
            report_id: FILTER_REPORT_ID.to_string(),
            execution_kind: FILTER_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status,
            split_count: validation.split_count,
            source_batch_count: validation.source_batch_count,
            source_uri_matches_batches: validation.source_uri_matches_batches,
            runtime_execution_allowed,
            source_backed_batches_consumed: runtime_execution_allowed,
            selection_vector_guaranteed: runtime_execution_allowed,
            correctness_certified: runtime_execution_allowed
                && prepared_execution.correctness_certified,
            production_claim_allowed: false,
            data_read: prepared_execution.data_read,
            data_decoded: prepared_execution.data_decoded,
            data_materialized: prepared_execution.data_materialized,
            row_read: prepared_execution.row_read,
            arrow_converted: prepared_execution.arrow_converted,
            object_store_io: prepared_execution.object_store_io,
            write_io: prepared_execution.write_io,
            spill_io_performed: prepared_execution.spill_io_performed,
            external_effects_executed: prepared_execution.external_effects_executed,
            fallback_execution_allowed: prepared_execution.fallback_execution_allowed,
            fallback_attempted: prepared_execution.fallback_attempted
                || diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.fallback.attempted),
            prepared_execution,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_unsafe_effects(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.prepared_execution.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex source-backed encoded filter execution");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "source: {}", self.source_summary);
        let _ = writeln!(
            &mut out,
            "source uri: {}",
            self.source_uri.as_ref().map_or("none", DatasetUri::as_str)
        );
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "split count: {}", self.split_count);
        let _ = writeln!(&mut out, "source batches: {}", self.source_batch_count);
        let _ = writeln!(
            &mut out,
            "runtime execution allowed: {}",
            self.runtime_execution_allowed
        );
        let _ = writeln!(
            &mut out,
            "selection vector guaranteed: {}",
            self.selection_vector_guaranteed
        );
        let _ = writeln!(
            &mut out,
            "correctness certified: {}",
            self.correctness_certified
        );
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }

    #[must_use]
    pub fn evidence_gate_report(&self) -> VortexSourceBackedExpansionEvidenceReport {
        VortexSourceBackedExpansionEvidenceReport::from_filter(self)
    }

    #[must_use]
    pub fn certificate_pair_report(&self) -> VortexSourceBackedCertificatePairReport {
        VortexSourceBackedCertificatePairReport::from_filter(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSourceBackedEncodedProjectionExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexSourceBackedEncodedExecutionStatus,
    pub split_count: usize,
    pub source_batch_count: usize,
    pub source_uri_matches_batches: bool,
    pub prepared_execution: VortexGeneralizedEncodedProjectionExecutionReport,
    pub runtime_execution_allowed: bool,
    pub source_backed_batches_consumed: bool,
    pub encoded_projection_guaranteed: bool,
    pub selection_vector_preserved: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexSourceBackedEncodedProjectionExecutionReport {
    fn from_parts(
        source: &UniversalInputSource,
        validation: SourceBackedValidation,
        prepared_execution: VortexGeneralizedEncodedProjectionExecutionReport,
    ) -> Self {
        let mut diagnostics = validation.diagnostics;
        diagnostics.extend(prepared_execution.diagnostics.clone());
        let status = validation.status.unwrap_or_else(|| {
            if prepared_execution.status
                == VortexGeneralizedEncodedProjectionExecutionStatus::ExecutedPreparedEncodedProjection
                && prepared_execution.runtime_execution_allowed
                && !prepared_execution.has_errors()
            {
                VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
            } else {
                VortexSourceBackedEncodedExecutionStatus::BlockedPreparedExecution
            }
        });
        let runtime_execution_allowed = status
            == VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches;
        Self {
            schema_version: PROJECTION_SCHEMA_VERSION,
            report_id: PROJECTION_REPORT_ID.to_string(),
            execution_kind: PROJECTION_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status,
            split_count: validation.split_count,
            source_batch_count: validation.source_batch_count,
            source_uri_matches_batches: validation.source_uri_matches_batches,
            runtime_execution_allowed,
            source_backed_batches_consumed: runtime_execution_allowed,
            encoded_projection_guaranteed: runtime_execution_allowed,
            selection_vector_preserved: runtime_execution_allowed
                && prepared_execution.selection_vector_preserved,
            correctness_certified: runtime_execution_allowed
                && prepared_execution.correctness_certified,
            production_claim_allowed: false,
            data_read: prepared_execution.data_read,
            data_decoded: prepared_execution.data_decoded,
            data_materialized: prepared_execution.data_materialized,
            row_read: prepared_execution.row_read,
            arrow_converted: prepared_execution.arrow_converted,
            object_store_io: prepared_execution.object_store_io,
            write_io: prepared_execution.write_io,
            spill_io_performed: prepared_execution.spill_io_performed,
            external_effects_executed: prepared_execution.external_effects_executed,
            fallback_execution_allowed: prepared_execution.fallback_execution_allowed,
            fallback_attempted: prepared_execution.fallback_attempted
                || diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.fallback.attempted),
            prepared_execution,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_unsafe_effects(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.prepared_execution.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            &mut out,
            "Vortex source-backed encoded projection execution"
        );
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "source: {}", self.source_summary);
        let _ = writeln!(
            &mut out,
            "source uri: {}",
            self.source_uri.as_ref().map_or("none", DatasetUri::as_str)
        );
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "split count: {}", self.split_count);
        let _ = writeln!(&mut out, "source batches: {}", self.source_batch_count);
        let _ = writeln!(
            &mut out,
            "runtime execution allowed: {}",
            self.runtime_execution_allowed
        );
        let _ = writeln!(
            &mut out,
            "encoded projection guaranteed: {}",
            self.encoded_projection_guaranteed
        );
        let _ = writeln!(
            &mut out,
            "correctness certified: {}",
            self.correctness_certified
        );
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }

    #[must_use]
    pub fn evidence_gate_report(&self) -> VortexSourceBackedExpansionEvidenceReport {
        VortexSourceBackedExpansionEvidenceReport::from_projection(self)
    }

    #[must_use]
    pub fn certificate_pair_report(&self) -> VortexSourceBackedCertificatePairReport {
        VortexSourceBackedCertificatePairReport::from_projection(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderBackedEncodedFilterExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexReaderBackedEncodedExecutionStatus,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub reader_split_count: usize,
    pub source_batch_count: usize,
    pub reader_split_refs: Vec<String>,
    pub prepared_batch_split_refs: Vec<String>,
    pub reader_source_uri_matches_source: bool,
    pub prepared_batch_split_refs_covered_by_reader: bool,
    pub source_execution: VortexSourceBackedEncodedFilterExecutionReport,
    pub runtime_execution_allowed: bool,
    pub reader_split_evidence_consumed: bool,
    pub reader_validated_prepared_batches_consumed: bool,
    pub reader_generated_prepared_batches: bool,
    pub selection_vector_guaranteed: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexReaderBackedEncodedFilterExecutionReport {
    fn from_parts(
        source: &UniversalInputSource,
        validation: ReaderBackedValidation,
        source_execution: VortexSourceBackedEncodedFilterExecutionReport,
    ) -> Self {
        let mut diagnostics = validation.diagnostics;
        diagnostics.extend(source_execution.diagnostics.clone());
        let status = validation.status.unwrap_or_else(|| {
            if source_execution.status
                == VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
                && source_execution.runtime_execution_allowed
                && !source_execution.has_errors()
            {
                VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
            } else {
                VortexReaderBackedEncodedExecutionStatus::BlockedSourceBackedExecution
            }
        });
        let runtime_execution_allowed = status
            == VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches;
        let correctness_certified =
            runtime_execution_allowed && source_execution.correctness_certified;
        let data_read = validation.data_read || source_execution.data_read;
        let data_decoded = validation.data_decoded || source_execution.data_decoded;
        let data_materialized = validation.data_materialized || source_execution.data_materialized;
        let row_read = validation.row_read || source_execution.row_read;
        let arrow_converted = validation.arrow_converted || source_execution.arrow_converted;
        let object_store_io = validation.object_store_io || source_execution.object_store_io;
        let write_io = validation.write_io || source_execution.write_io;
        let spill_io_performed =
            validation.spill_io_performed || source_execution.spill_io_performed;
        let external_effects_executed =
            validation.external_effects_executed || source_execution.external_effects_executed;
        let fallback_execution_allowed =
            validation.fallback_execution_allowed || source_execution.fallback_execution_allowed;
        let fallback_attempted = validation.fallback_attempted
            || source_execution.fallback_attempted
            || diagnostics
                .iter()
                .any(|diagnostic| diagnostic.fallback.attempted);
        Self {
            schema_version: READER_FILTER_SCHEMA_VERSION,
            report_id: READER_FILTER_REPORT_ID.to_string(),
            execution_kind: READER_FILTER_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status,
            provider_boundary: VortexNativeProviderBoundary::local_scan(),
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            reader_split_count: validation.reader_split_refs.len(),
            source_batch_count: validation.prepared_batch_split_refs.len(),
            reader_split_refs: validation.reader_split_refs,
            prepared_batch_split_refs: validation.prepared_batch_split_refs,
            reader_source_uri_matches_source: validation.reader_source_uri_matches_source,
            prepared_batch_split_refs_covered_by_reader: validation
                .prepared_batch_split_refs_covered_by_reader,
            source_execution,
            runtime_execution_allowed,
            reader_split_evidence_consumed: runtime_execution_allowed,
            reader_validated_prepared_batches_consumed: runtime_execution_allowed,
            reader_generated_prepared_batches: runtime_execution_allowed
                && validation.reader_generated_prepared_batches,
            selection_vector_guaranteed: runtime_execution_allowed,
            correctness_certified,
            production_claim_allowed: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_forbidden_effects(&self) -> bool {
        !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || !self.avoids_forbidden_effects()
            || self.source_execution.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex reader-backed encoded filter execution");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "source: {}", self.source_summary);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "reader splits: {}", self.reader_split_count);
        let _ = writeln!(&mut out, "source batches: {}", self.source_batch_count);
        let _ = writeln!(
            &mut out,
            "reader-generated prepared batches: {}",
            self.reader_generated_prepared_batches
        );
        let _ = writeln!(&mut out, "data read: {}", self.data_read);
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReaderBackedEncodedProjectionExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub source_summary: String,
    pub source_uri: Option<DatasetUri>,
    pub status: VortexReaderBackedEncodedExecutionStatus,
    pub provider_boundary: VortexNativeProviderBoundary,
    pub provider_kind: &'static str,
    pub provider_api_surface: &'static str,
    pub reader_split_count: usize,
    pub source_batch_count: usize,
    pub reader_split_refs: Vec<String>,
    pub prepared_batch_split_refs: Vec<String>,
    pub reader_source_uri_matches_source: bool,
    pub prepared_batch_split_refs_covered_by_reader: bool,
    pub source_execution: VortexSourceBackedEncodedProjectionExecutionReport,
    pub runtime_execution_allowed: bool,
    pub reader_split_evidence_consumed: bool,
    pub reader_validated_prepared_batches_consumed: bool,
    pub reader_generated_prepared_batches: bool,
    pub encoded_projection_guaranteed: bool,
    pub selection_vector_preserved: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexReaderBackedEncodedProjectionExecutionReport {
    fn from_parts(
        source: &UniversalInputSource,
        validation: ReaderBackedValidation,
        source_execution: VortexSourceBackedEncodedProjectionExecutionReport,
    ) -> Self {
        let mut diagnostics = validation.diagnostics;
        diagnostics.extend(source_execution.diagnostics.clone());
        let status = validation.status.unwrap_or_else(|| {
            if source_execution.status
                == VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
                && source_execution.runtime_execution_allowed
                && !source_execution.has_errors()
            {
                VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
            } else {
                VortexReaderBackedEncodedExecutionStatus::BlockedSourceBackedExecution
            }
        });
        let runtime_execution_allowed = status
            == VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches;
        let selection_vector_preserved =
            runtime_execution_allowed && source_execution.selection_vector_preserved;
        let correctness_certified =
            runtime_execution_allowed && source_execution.correctness_certified;
        let data_read = validation.data_read || source_execution.data_read;
        let data_decoded = validation.data_decoded || source_execution.data_decoded;
        let data_materialized = validation.data_materialized || source_execution.data_materialized;
        let row_read = validation.row_read || source_execution.row_read;
        let arrow_converted = validation.arrow_converted || source_execution.arrow_converted;
        let object_store_io = validation.object_store_io || source_execution.object_store_io;
        let write_io = validation.write_io || source_execution.write_io;
        let spill_io_performed =
            validation.spill_io_performed || source_execution.spill_io_performed;
        let external_effects_executed =
            validation.external_effects_executed || source_execution.external_effects_executed;
        let fallback_execution_allowed =
            validation.fallback_execution_allowed || source_execution.fallback_execution_allowed;
        let fallback_attempted = validation.fallback_attempted
            || source_execution.fallback_attempted
            || diagnostics
                .iter()
                .any(|diagnostic| diagnostic.fallback.attempted);
        Self {
            schema_version: READER_PROJECTION_SCHEMA_VERSION,
            report_id: READER_PROJECTION_REPORT_ID.to_string(),
            execution_kind: READER_PROJECTION_EXECUTION_KIND,
            source_summary: source.summary(),
            source_uri: source.uri.clone(),
            status,
            provider_boundary: VortexNativeProviderBoundary::local_scan(),
            provider_kind: LOCAL_SCAN_PROVIDER_KIND,
            provider_api_surface: LOCAL_SCAN_PROVIDER_API_SURFACE,
            reader_split_count: validation.reader_split_refs.len(),
            source_batch_count: validation.prepared_batch_split_refs.len(),
            reader_split_refs: validation.reader_split_refs,
            prepared_batch_split_refs: validation.prepared_batch_split_refs,
            reader_source_uri_matches_source: validation.reader_source_uri_matches_source,
            prepared_batch_split_refs_covered_by_reader: validation
                .prepared_batch_split_refs_covered_by_reader,
            source_execution,
            runtime_execution_allowed,
            reader_split_evidence_consumed: runtime_execution_allowed,
            reader_validated_prepared_batches_consumed: runtime_execution_allowed,
            reader_generated_prepared_batches: runtime_execution_allowed
                && validation.reader_generated_prepared_batches,
            encoded_projection_guaranteed: runtime_execution_allowed,
            selection_vector_preserved,
            correctness_certified,
            production_claim_allowed: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_forbidden_effects(&self) -> bool {
        !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || !self.avoids_forbidden_effects()
            || self.source_execution.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            &mut out,
            "Vortex reader-backed encoded projection execution"
        );
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "source: {}", self.source_summary);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "reader splits: {}", self.reader_split_count);
        let _ = writeln!(&mut out, "source batches: {}", self.source_batch_count);
        let _ = writeln!(
            &mut out,
            "reader-generated prepared batches: {}",
            self.reader_generated_prepared_batches
        );
        let _ = writeln!(&mut out, "data read: {}", self.data_read);
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

/// Executes generalized encoded filter evidence over source-bound prepared
/// encoded-value batches.
///
/// This is not broad reader wiring: callers must provide the encoded batches
/// and source/split refs explicitly. The function verifies the native Vortex
/// source boundary before delegating to the prepared encoded filter execution
/// path.
///
/// # Errors
/// Returns an error when the prepared execution path fails to build its
/// certificate evidence.
pub fn execute_vortex_source_backed_filter_from_encoded_value_batches(
    predicate: &PredicateExpr,
    source: &UniversalInputSource,
    batches: &[VortexSourceBackedEncodedValuePredicateBatch],
) -> Result<VortexSourceBackedEncodedFilterExecutionReport> {
    let validation = validate_source_backed_refs(
        source,
        batches
            .iter()
            .map(|batch| (&batch.source_uri, batch.split_ref.as_str())),
    );
    let prepared_execution = if validation.status.is_some() {
        execute_vortex_generalized_filter_from_encoded_value_batches(predicate, &[])?
    } else {
        let prepared_batches = batches
            .iter()
            .map(|batch| batch.batch.clone())
            .collect::<Vec<_>>();
        execute_vortex_generalized_filter_from_encoded_value_batches(predicate, &prepared_batches)?
    };
    Ok(VortexSourceBackedEncodedFilterExecutionReport::from_parts(
        source,
        validation,
        prepared_execution,
    ))
}

/// Executes generalized encoded projection evidence over source-bound prepared
/// encoded projection batches.
///
/// This verifies the source/split envelope and then delegates to the prepared
/// encoded projection execution path. It does not open files, read object
/// stores, decode rows, materialize values, write outputs, or invoke fallback
/// execution.
///
/// # Errors
/// Returns an error when the prepared execution path fails to build its
/// certificate evidence.
pub fn execute_vortex_source_backed_projection_from_encoded_projection_batches(
    requested_columns: &[ColumnRef],
    source: &UniversalInputSource,
    batches: &[VortexSourceBackedEncodedProjectionColumn],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
) -> Result<VortexSourceBackedEncodedProjectionExecutionReport> {
    let validation = validate_source_backed_refs(
        source,
        batches
            .iter()
            .map(|batch| (&batch.source_uri, batch.split_ref.as_str())),
    );
    let prepared_execution = if validation.status.is_some() {
        execute_vortex_generalized_projection_from_encoded_projection_batches(
            requested_columns,
            &[],
            filter_kernel,
        )?
    } else {
        let prepared_batches = batches
            .iter()
            .map(|batch| batch.column.clone())
            .collect::<Vec<_>>();
        execute_vortex_generalized_projection_from_encoded_projection_batches(
            requested_columns,
            &prepared_batches,
            filter_kernel,
        )?
    };
    Ok(
        VortexSourceBackedEncodedProjectionExecutionReport::from_parts(
            source,
            validation,
            prepared_execution,
        ),
    )
}

/// Executes source-backed prepared encoded filter evidence only after it has
/// been matched against split evidence emitted by a real Vortex reader/scan
/// path.
///
/// This wires reader/source/split evidence into the prepared-batch surface. It
/// still does not claim that the reader generated `ShardLoom` prepared encoded
/// batches; that remains a separate runtime expansion.
///
/// # Errors
/// Returns an error when the underlying source-backed prepared execution path
/// cannot construct its report.
pub fn execute_vortex_reader_backed_filter_from_encoded_value_batches(
    predicate: &PredicateExpr,
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    batches: &[VortexSourceBackedEncodedValuePredicateBatch],
) -> Result<VortexReaderBackedEncodedFilterExecutionReport> {
    execute_vortex_reader_backed_filter_inner(predicate, source, reader_splits, batches, false)
}

fn execute_vortex_reader_backed_filter_inner(
    predicate: &PredicateExpr,
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    batches: &[VortexSourceBackedEncodedValuePredicateBatch],
    reader_generated_prepared_batches: bool,
) -> Result<VortexReaderBackedEncodedFilterExecutionReport> {
    let validation = validate_reader_backed_refs(
        source,
        reader_splits,
        batches
            .iter()
            .map(|batch| (&batch.source_uri, batch.split_ref.as_str())),
        reader_generated_prepared_batches,
    );
    let source_execution =
        execute_vortex_source_backed_filter_from_encoded_value_batches(predicate, source, batches)?;
    Ok(VortexReaderBackedEncodedFilterExecutionReport::from_parts(
        source,
        validation,
        source_execution,
    ))
}

/// Executes source-backed prepared encoded projection evidence only after it
/// has been matched against split evidence emitted by a real Vortex reader/scan
/// path.
///
/// The reader split evidence may come from `VortexFile::scan` chunk metadata.
/// Prepared encoded batch extraction from those chunks is deliberately not
/// claimed here.
///
/// # Errors
/// Returns an error when the underlying source-backed prepared execution path
/// cannot construct its report.
pub fn execute_vortex_reader_backed_projection_from_encoded_projection_batches(
    requested_columns: &[ColumnRef],
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    batches: &[VortexSourceBackedEncodedProjectionColumn],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
) -> Result<VortexReaderBackedEncodedProjectionExecutionReport> {
    execute_vortex_reader_backed_projection_inner(
        requested_columns,
        source,
        reader_splits,
        batches,
        filter_kernel,
        false,
    )
}

fn execute_vortex_reader_backed_projection_inner(
    requested_columns: &[ColumnRef],
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    batches: &[VortexSourceBackedEncodedProjectionColumn],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
    reader_generated_prepared_batches: bool,
) -> Result<VortexReaderBackedEncodedProjectionExecutionReport> {
    let validation = validate_reader_backed_refs(
        source,
        reader_splits,
        batches
            .iter()
            .map(|batch| (&batch.source_uri, batch.split_ref.as_str())),
        reader_generated_prepared_batches,
    );
    let source_execution = execute_vortex_source_backed_projection_from_encoded_projection_batches(
        requested_columns,
        source,
        batches,
        filter_kernel,
    )?;
    Ok(
        VortexReaderBackedEncodedProjectionExecutionReport::from_parts(
            source,
            validation,
            source_execution,
        ),
    )
}

/// Builds report-only prepared chunk envelopes from split evidence emitted by
/// an approved Vortex reader/scan path.
///
/// This records that a real reader produced encoded chunk boundaries and
/// provider refs. It deliberately does not lower opaque Vortex chunks into
/// `EncodedValueBatch` or projection kernel inputs, because doing so without
/// dtype/encoding-specific evidence would overstate the representation.
#[must_use]
pub fn plan_vortex_reader_generated_prepared_batch_envelopes(
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
) -> VortexReaderGeneratedPreparedBatchReport {
    let validation = validate_reader_generated_prepared_batches(source, reader_splits, &[]);
    VortexReaderGeneratedPreparedBatchReport::from_validation(source, validation)
}

/// Builds reader-generated prepared-batch evidence and admits encoded kernel
/// inputs only when explicit dtype, encoding, value, row-count, source, split,
/// and no-effect evidence is present.
///
/// The supplied kernel inputs are already mapped encoded values. This function
/// validates that they are covered by the reader split refs; it does not decode
/// opaque Vortex chunks or infer values from metadata alone.
#[must_use]
pub fn plan_vortex_reader_generated_prepared_batch_kernel_inputs(
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    encoded_kernel_inputs: &[VortexReaderGeneratedEncodedKernelInput],
) -> VortexReaderGeneratedPreparedBatchReport {
    let validation =
        validate_reader_generated_prepared_batches(source, reader_splits, encoded_kernel_inputs);
    VortexReaderGeneratedPreparedBatchReport::from_validation(source, validation)
}

/// Executes reader-generated encoded filter evidence only when reader split
/// refs and explicit encoded kernel inputs can be matched without decode,
/// materialization, row reads, Arrow conversion, or fallback execution.
///
/// # Errors
/// Returns an error when source-backed batch construction or the underlying
/// prepared encoded execution path fails to construct report evidence.
pub fn execute_vortex_reader_generated_filter_from_encoded_kernel_inputs(
    predicate: &PredicateExpr,
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    encoded_kernel_inputs: &[VortexReaderGeneratedEncodedKernelInput],
) -> Result<VortexReaderBackedEncodedFilterExecutionReport> {
    let lowering = plan_vortex_reader_generated_prepared_batch_kernel_inputs(
        source,
        reader_splits,
        encoded_kernel_inputs,
    );
    if !lowering.runtime_execution_allowed {
        let mut report = execute_vortex_reader_backed_filter_inner(
            predicate,
            source,
            reader_splits,
            &[],
            false,
        )?;
        report.diagnostics.extend(lowering.diagnostics.clone());
        return Ok(report);
    }
    let batches = encoded_kernel_inputs
        .iter()
        .map(VortexReaderGeneratedEncodedKernelInput::to_source_backed_filter_batch)
        .collect::<Result<Vec<_>>>()?;
    let mut report = execute_vortex_reader_backed_filter_inner(
        predicate,
        source,
        reader_splits,
        &batches,
        lowering.runtime_execution_allowed,
    )?;
    report.diagnostics.extend(lowering.diagnostics.clone());
    Ok(report)
}

/// Executes reader-generated encoded projection or filter-project evidence
/// only when reader split refs and explicit encoded kernel inputs can be
/// matched without decode, materialization, row reads, Arrow conversion, or
/// fallback execution.
///
/// Passing a safe `filter_kernel` preserves the filter-project selection-vector
/// evidence through the projection path.
///
/// # Errors
/// Returns an error when source-backed projection construction or the
/// underlying prepared encoded execution path fails to construct report
/// evidence.
pub fn execute_vortex_reader_generated_projection_from_encoded_kernel_inputs(
    requested_columns: &[ColumnRef],
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    encoded_kernel_inputs: &[VortexReaderGeneratedEncodedKernelInput],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
) -> Result<VortexReaderBackedEncodedProjectionExecutionReport> {
    let lowering = plan_vortex_reader_generated_prepared_batch_kernel_inputs(
        source,
        reader_splits,
        encoded_kernel_inputs,
    );
    if !lowering.runtime_execution_allowed {
        let mut report = execute_vortex_reader_backed_projection_inner(
            requested_columns,
            source,
            reader_splits,
            &[],
            filter_kernel,
            false,
        )?;
        report.diagnostics.extend(lowering.diagnostics.clone());
        return Ok(report);
    }
    let batches = encoded_kernel_inputs
        .iter()
        .map(VortexReaderGeneratedEncodedKernelInput::to_source_backed_projection_column)
        .collect::<Result<Vec<_>>>()?;
    let mut report = execute_vortex_reader_backed_projection_inner(
        requested_columns,
        source,
        reader_splits,
        &batches,
        filter_kernel,
        lowering.runtime_execution_allowed,
    )?;
    report.diagnostics.extend(lowering.diagnostics.clone());
    Ok(report)
}

#[derive(Debug, Clone)]
struct SourceBackedValidation {
    status: Option<VortexSourceBackedEncodedExecutionStatus>,
    source_batch_count: usize,
    split_count: usize,
    source_uri_matches_batches: bool,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct ReaderBackedValidation {
    status: Option<VortexReaderBackedEncodedExecutionStatus>,
    reader_split_refs: Vec<String>,
    prepared_batch_split_refs: Vec<String>,
    reader_source_uri_matches_source: bool,
    prepared_batch_split_refs_covered_by_reader: bool,
    data_read: bool,
    data_decoded: bool,
    data_materialized: bool,
    row_read: bool,
    arrow_converted: bool,
    object_store_io: bool,
    write_io: bool,
    spill_io_performed: bool,
    external_effects_executed: bool,
    fallback_execution_allowed: bool,
    fallback_attempted: bool,
    reader_generated_prepared_batches: bool,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct ReaderGeneratedPreparedBatchValidation {
    status: VortexReaderGeneratedPreparedBatchStatus,
    reader_split_refs: Vec<String>,
    encoded_kernel_input_split_refs: Vec<String>,
    reader_source_uri_matches_source: bool,
    encoded_kernel_inputs_source_uri_matches_source: bool,
    encoded_kernel_input_split_refs_covered_by_reader: bool,
    encoded_kernel_input_row_counts_match_reader: bool,
    encoded_kernel_input_mapping_evidence_complete: bool,
    encoded_kernel_input_count: usize,
    batches: Vec<VortexReaderGeneratedPreparedBatchEvidence>,
    total_rows: usize,
    data_read: bool,
    data_decoded: bool,
    data_materialized: bool,
    row_read: bool,
    arrow_converted: bool,
    object_store_io: bool,
    write_io: bool,
    spill_io_performed: bool,
    external_effects_executed: bool,
    fallback_execution_allowed: bool,
    fallback_attempted: bool,
    diagnostics: Vec<Diagnostic>,
}

fn validate_source_backed_refs<'a>(
    source: &UniversalInputSource,
    refs: impl IntoIterator<Item = (&'a DatasetUri, &'a str)>,
) -> SourceBackedValidation {
    let refs = refs.into_iter().collect::<Vec<_>>();
    let mut diagnostics = Vec::new();
    let mut split_refs = BTreeSet::new();
    for (_, split_ref) in &refs {
        split_refs.insert((*split_ref).to_string());
    }
    if !source.is_native_vortex() {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_source_backed_encoded_execution",
            "source-backed encoded execution requires a native Vortex source",
            Some("Use a Vortex input source; compatibility inputs must pass through explicit compatibility import boundaries before encoded execution.".to_string()),
        ));
        return SourceBackedValidation {
            status: Some(VortexSourceBackedEncodedExecutionStatus::BlockedNonNativeSource),
            source_batch_count: refs.len(),
            split_count: split_refs.len(),
            source_uri_matches_batches: false,
            diagnostics,
        };
    }
    let Some(source_uri) = &source.uri else {
        diagnostics.push(Diagnostic::configuration_error(
            "vortex_source_backed_encoded_execution",
            "native Vortex source-backed encoded execution requires a source URI",
            "Provide a DatasetUri for the source before binding prepared encoded batches.",
        ));
        return SourceBackedValidation {
            status: Some(VortexSourceBackedEncodedExecutionStatus::BlockedMissingSourceUri),
            source_batch_count: refs.len(),
            split_count: split_refs.len(),
            source_uri_matches_batches: false,
            diagnostics,
        };
    };
    if refs
        .iter()
        .any(|(_, split_ref)| split_ref.trim().is_empty())
    {
        diagnostics.push(Diagnostic::configuration_error(
            "vortex_source_backed_encoded_execution",
            "source-backed encoded execution requires non-empty split refs",
            "Attach a stable split ref to every prepared encoded batch.",
        ));
        return SourceBackedValidation {
            status: Some(VortexSourceBackedEncodedExecutionStatus::BlockedMissingSplitRef),
            source_batch_count: refs.len(),
            split_count: split_refs.len(),
            source_uri_matches_batches: false,
            diagnostics,
        };
    }
    let source_uri_matches_batches = refs.iter().all(|(batch_uri, _)| *batch_uri == source_uri);
    if !source_uri_matches_batches {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_source_backed_encoded_execution",
            "prepared encoded batches must all be bound to the declared source URI",
            Some("Reject mixed-source encoded batches; execute separate source-backed plans or construct a certified multi-source plan later.".to_string()),
        ));
        return SourceBackedValidation {
            status: Some(VortexSourceBackedEncodedExecutionStatus::BlockedSourceBatchMismatch),
            source_batch_count: refs.len(),
            split_count: split_refs.len(),
            source_uri_matches_batches,
            diagnostics,
        };
    }
    SourceBackedValidation {
        status: None,
        source_batch_count: refs.len(),
        split_count: split_refs.len(),
        source_uri_matches_batches,
        diagnostics,
    }
}

#[allow(clippy::too_many_lines)]
fn validate_reader_generated_prepared_batches(
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    encoded_kernel_inputs: &[VortexReaderGeneratedEncodedKernelInput],
) -> ReaderGeneratedPreparedBatchValidation {
    let mut reader_split_refs = reader_splits
        .iter()
        .map(|split| split.split_ref.clone())
        .collect::<Vec<_>>();
    reader_split_refs.sort();
    reader_split_refs.dedup();
    let mut encoded_kernel_input_split_refs = encoded_kernel_inputs
        .iter()
        .map(|input| input.split_ref.clone())
        .collect::<Vec<_>>();
    encoded_kernel_input_split_refs.sort();
    encoded_kernel_input_split_refs.dedup();
    let reader_split_ref_set = reader_split_refs.iter().cloned().collect::<BTreeSet<_>>();
    let batches = reader_splits
        .iter()
        .map(|split| {
            let encoded_kernel_input_count = encoded_kernel_inputs
                .iter()
                .filter(|input| input.split_ref == split.split_ref)
                .count();
            VortexReaderGeneratedPreparedBatchEvidence::from_reader_split(
                split,
                encoded_kernel_input_count,
            )
        })
        .collect::<Vec<_>>();
    let total_rows = reader_splits
        .iter()
        .map(|split| split.row_count)
        .try_fold(0usize, usize::checked_add)
        .unwrap_or(usize::MAX);
    let data_read = reader_splits.iter().any(|split| split.data_read);
    let data_decoded = reader_splits.iter().any(|split| split.data_decoded);
    let data_materialized = reader_splits.iter().any(|split| split.data_materialized);
    let row_read = reader_splits.iter().any(|split| split.row_read);
    let arrow_converted = reader_splits.iter().any(|split| split.arrow_converted);
    let object_store_io = reader_splits.iter().any(|split| split.object_store_io);
    let write_io = reader_splits.iter().any(|split| split.write_io);
    let spill_io_performed = reader_splits.iter().any(|split| split.spill_io_performed);
    let external_effects_executed = reader_splits
        .iter()
        .any(|split| split.external_effects_executed);
    let fallback_execution_allowed = reader_splits
        .iter()
        .any(|split| split.fallback_execution_allowed);
    let fallback_attempted = reader_splits.iter().any(|split| split.fallback_attempted);
    let encoded_kernel_inputs_source_uri_matches_source = source.uri.as_ref().is_some_and(|uri| {
        encoded_kernel_inputs
            .iter()
            .all(|input| &input.source_uri == uri)
    });
    let encoded_kernel_input_split_refs_covered_by_reader = !encoded_kernel_inputs.is_empty()
        && encoded_kernel_input_split_refs
            .iter()
            .all(|split_ref| reader_split_ref_set.contains(split_ref));
    let encoded_kernel_input_mapping_evidence_complete = !encoded_kernel_inputs.is_empty()
        && encoded_kernel_inputs
            .iter()
            .all(VortexReaderGeneratedEncodedKernelInput::mapping_evidence_complete);
    let encoded_kernel_input_row_counts_match_reader = !encoded_kernel_inputs.is_empty()
        && encoded_kernel_inputs.iter().all(|input| {
            reader_splits
                .iter()
                .find(|split| split.split_ref == input.split_ref)
                .is_some_and(|split| {
                    input.batch.values.row_count()
                        == Some(u64::try_from(split.row_count).unwrap_or(u64::MAX))
                })
        });

    let mut validation = ReaderGeneratedPreparedBatchValidation {
        status: if encoded_kernel_inputs.is_empty() {
            VortexReaderGeneratedPreparedBatchStatus::PreparedReaderChunkEnvelopes
        } else {
            VortexReaderGeneratedPreparedBatchStatus::PreparedEncodedKernelInputs
        },
        reader_split_refs,
        encoded_kernel_input_split_refs,
        reader_source_uri_matches_source: true,
        encoded_kernel_inputs_source_uri_matches_source,
        encoded_kernel_input_split_refs_covered_by_reader,
        encoded_kernel_input_row_counts_match_reader,
        encoded_kernel_input_mapping_evidence_complete,
        encoded_kernel_input_count: encoded_kernel_inputs.len(),
        batches,
        total_rows,
        data_read,
        data_decoded,
        data_materialized,
        row_read,
        arrow_converted,
        object_store_io,
        write_io,
        spill_io_performed,
        external_effects_executed,
        fallback_execution_allowed,
        fallback_attempted,
        diagnostics: Vec::new(),
    };

    if !source.is_native_vortex() {
        validation.status = VortexReaderGeneratedPreparedBatchStatus::BlockedNonNativeSource;
        validation.reader_source_uri_matches_source = false;
        validation.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_generated_prepared_batch_envelope",
            "reader-generated prepared chunk envelopes require a native Vortex source",
            Some("Use a Vortex source or pass compatibility data through an explicit compatibility import boundary before reader-generated encoded evidence.".to_string()),
        ));
        return validation;
    }
    let Some(source_uri) = &source.uri else {
        validation.status = VortexReaderGeneratedPreparedBatchStatus::BlockedMissingSourceUri;
        validation.reader_source_uri_matches_source = false;
        validation.diagnostics.push(Diagnostic::configuration_error(
            "vortex_reader_generated_prepared_batch_envelope",
            "reader-generated prepared chunk envelopes require a source URI",
            "Attach a DatasetUri before producing reader-generated chunk evidence.",
        ));
        return validation;
    };
    if reader_splits.is_empty() {
        validation.status =
            VortexReaderGeneratedPreparedBatchStatus::BlockedMissingReaderSplitEvidence;
        validation.reader_source_uri_matches_source = false;
        validation.diagnostics.push(Diagnostic::configuration_error(
            "vortex_reader_generated_prepared_batch_envelope",
            "reader-generated prepared chunk envelopes require reader split evidence",
            "Run an approved Vortex reader/scan path before producing reader-generated chunk envelopes.",
        ));
        return validation;
    }
    validation.reader_source_uri_matches_source = reader_splits
        .iter()
        .all(|split| &split.source_uri == source_uri);
    if !validation.reader_source_uri_matches_source {
        validation.status = VortexReaderGeneratedPreparedBatchStatus::BlockedReaderSourceMismatch;
        validation.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_generated_prepared_batch_envelope",
            "reader-generated prepared chunk envelopes must all belong to the declared source URI",
            Some("Reject mixed-source reader split evidence; execute separate source-backed plans or construct a certified multi-source plan later.".to_string()),
        ));
        return validation;
    }
    if !encoded_kernel_inputs.is_empty() {
        if !validation.encoded_kernel_inputs_source_uri_matches_source {
            validation.status =
                VortexReaderGeneratedPreparedBatchStatus::BlockedKernelInputSourceMismatch;
            validation.diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NoFallbackExecution,
                "vortex_reader_generated_prepared_batch_kernel_input",
                "reader-generated encoded kernel inputs must all belong to the declared source URI",
                Some("Reject mixed-source reader-generated encoded kernel inputs; construct separate source-backed plans or a certified multi-source plan later.".to_string()),
            ));
            return validation;
        }
        if !validation.encoded_kernel_input_split_refs_covered_by_reader {
            validation.status =
                VortexReaderGeneratedPreparedBatchStatus::BlockedKernelInputSplitMismatch;
            validation.diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NoFallbackExecution,
                "vortex_reader_generated_prepared_batch_kernel_input",
                "reader-generated encoded kernel inputs must be covered by reader split refs",
                Some("Only lower encoded kernel inputs whose split refs were emitted by the approved Vortex reader path.".to_string()),
            ));
            return validation;
        }
        if !validation.encoded_kernel_input_row_counts_match_reader {
            validation.status =
                VortexReaderGeneratedPreparedBatchStatus::BlockedKernelInputRowCountMismatch;
            validation.diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NoFallbackExecution,
                "vortex_reader_generated_prepared_batch_kernel_input",
                "reader-generated encoded kernel input row counts must match reader split row counts",
                Some("Reject encoded inputs when their value row count cannot be matched to the reader chunk boundary.".to_string()),
            ));
            return validation;
        }
        if !validation.encoded_kernel_input_mapping_evidence_complete {
            validation.status =
                VortexReaderGeneratedPreparedBatchStatus::BlockedKernelInputMappingEvidence;
            validation.diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NoFallbackExecution,
                "vortex_reader_generated_prepared_batch_kernel_input",
                "reader-generated encoded kernel inputs require dtype, encoding, value, and row-count mapping evidence without decode",
                Some("Keep opaque Vortex chunks as prepared chunk envelopes until the dtype and encoding can be mapped into ShardLoom encoded kernel inputs without decode or materialization.".to_string()),
            ));
            return validation;
        }
    }
    if validation
        .batches
        .iter()
        .any(VortexReaderGeneratedPreparedBatchEvidence::has_forbidden_effects)
        || encoded_kernel_inputs
            .iter()
            .any(VortexReaderGeneratedEncodedKernelInput::has_forbidden_effects)
    {
        validation.status = VortexReaderGeneratedPreparedBatchStatus::BlockedUnsafeReaderEffects;
        validation.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_generated_prepared_batch_envelope",
            "reader-generated chunk envelope evidence includes decode, materialization, row, Arrow, object-store, write, spill, external-effect, or fallback evidence",
            Some("Only local Vortex scan chunk evidence with data-read and no decode/materialization/fallback effects may produce reader-generated prepared chunk envelopes.".to_string()),
        ));
    }
    validation
}

#[allow(clippy::too_many_lines)]
fn validate_reader_backed_refs<'a>(
    source: &UniversalInputSource,
    reader_splits: &[VortexReaderBackedSplitEvidence],
    batch_refs: impl IntoIterator<Item = (&'a DatasetUri, &'a str)>,
    reader_generated_prepared_batches: bool,
) -> ReaderBackedValidation {
    let batch_refs = batch_refs.into_iter().collect::<Vec<_>>();
    let mut diagnostics = Vec::new();
    let mut reader_split_refs = reader_splits
        .iter()
        .map(|split| split.split_ref.clone())
        .collect::<Vec<_>>();
    reader_split_refs.sort();
    reader_split_refs.dedup();
    let mut prepared_batch_split_refs = batch_refs
        .iter()
        .map(|(_, split_ref)| (*split_ref).to_string())
        .collect::<Vec<_>>();
    prepared_batch_split_refs.sort();
    prepared_batch_split_refs.dedup();
    let data_read = reader_splits.iter().any(|split| split.data_read);
    let data_decoded = reader_splits.iter().any(|split| split.data_decoded);
    let data_materialized = reader_splits.iter().any(|split| split.data_materialized);
    let row_read = reader_splits.iter().any(|split| split.row_read);
    let arrow_converted = reader_splits.iter().any(|split| split.arrow_converted);
    let object_store_io = reader_splits.iter().any(|split| split.object_store_io);
    let write_io = reader_splits.iter().any(|split| split.write_io);
    let spill_io_performed = reader_splits.iter().any(|split| split.spill_io_performed);
    let external_effects_executed = reader_splits
        .iter()
        .any(|split| split.external_effects_executed);
    let fallback_execution_allowed = reader_splits
        .iter()
        .any(|split| split.fallback_execution_allowed);
    let fallback_attempted = reader_splits.iter().any(|split| split.fallback_attempted);
    if !source.is_native_vortex() {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_backed_encoded_execution",
            "reader-backed encoded execution requires a native Vortex source",
            Some("Use a Vortex input source; compatibility inputs must pass through explicit compatibility import boundaries before encoded execution.".to_string()),
        ));
        return ReaderBackedValidation {
            status: Some(VortexReaderBackedEncodedExecutionStatus::BlockedNonNativeSource),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source: false,
            prepared_batch_split_refs_covered_by_reader: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    }
    let Some(source_uri) = &source.uri else {
        diagnostics.push(Diagnostic::configuration_error(
            "vortex_reader_backed_encoded_execution",
            "reader-backed encoded execution requires a source URI",
            "Provide a DatasetUri for the source before binding reader split evidence.",
        ));
        return ReaderBackedValidation {
            status: Some(VortexReaderBackedEncodedExecutionStatus::BlockedMissingSourceUri),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source: false,
            prepared_batch_split_refs_covered_by_reader: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    };
    if reader_splits.is_empty() {
        diagnostics.push(Diagnostic::configuration_error(
            "vortex_reader_backed_encoded_execution",
            "reader-backed encoded execution requires reader split evidence",
            "Run an approved Vortex reader/scan path and attach its split refs before binding prepared encoded batches.",
        ));
        return ReaderBackedValidation {
            status: Some(
                VortexReaderBackedEncodedExecutionStatus::BlockedMissingReaderSplitEvidence,
            ),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source: false,
            prepared_batch_split_refs_covered_by_reader: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    }
    let reader_source_uri_matches_source = reader_splits
        .iter()
        .all(|split| &split.source_uri == source_uri);
    if !reader_source_uri_matches_source {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_backed_encoded_execution",
            "reader split evidence must all belong to the declared source URI",
            Some("Reject mixed-source reader split evidence; execute separate source-backed plans or construct a certified multi-source plan later.".to_string()),
        ));
        return ReaderBackedValidation {
            status: Some(VortexReaderBackedEncodedExecutionStatus::BlockedReaderSourceMismatch),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source,
            prepared_batch_split_refs_covered_by_reader: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    }
    if reader_splits
        .iter()
        .any(VortexReaderBackedSplitEvidence::has_forbidden_effects)
    {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_backed_encoded_execution",
            "reader split evidence includes decode, materialization, row, Arrow, object-store, write, spill, external-effect, or fallback evidence",
            Some("Reader-backed prepared batch binding only permits local Vortex scan data-read evidence with no decode/materialization/fallback effects.".to_string()),
        ));
        return ReaderBackedValidation {
            status: Some(VortexReaderBackedEncodedExecutionStatus::BlockedUnsafeReaderEffects),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source,
            prepared_batch_split_refs_covered_by_reader: false,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    }
    let reader_split_set = reader_split_refs.iter().collect::<BTreeSet<_>>();
    let prepared_batch_split_refs_covered_by_reader = prepared_batch_split_refs
        .iter()
        .all(|split_ref| reader_split_set.contains(split_ref));
    if !prepared_batch_split_refs_covered_by_reader {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_reader_backed_encoded_execution",
            "prepared encoded batch split refs must be present in reader split evidence",
            Some("Reject prepared encoded batches whose split refs cannot be tied to the approved reader/source path.".to_string()),
        ));
        return ReaderBackedValidation {
            status: Some(
                VortexReaderBackedEncodedExecutionStatus::BlockedPreparedBatchSplitMismatch,
            ),
            reader_split_refs,
            prepared_batch_split_refs,
            reader_source_uri_matches_source,
            prepared_batch_split_refs_covered_by_reader,
            data_read,
            data_decoded,
            data_materialized,
            row_read,
            arrow_converted,
            object_store_io,
            write_io,
            spill_io_performed,
            external_effects_executed,
            fallback_execution_allowed,
            fallback_attempted,
            reader_generated_prepared_batches,
            diagnostics,
        };
    }
    ReaderBackedValidation {
        status: None,
        reader_split_refs,
        prepared_batch_split_refs,
        reader_source_uri_matches_source,
        prepared_batch_split_refs_covered_by_reader,
        data_read,
        data_decoded,
        data_materialized,
        row_read,
        arrow_converted,
        object_store_io,
        write_io,
        spill_io_performed,
        external_effects_executed,
        fallback_execution_allowed,
        fallback_attempted,
        reader_generated_prepared_batches,
        diagnostics,
    }
}

fn validated_split_ref(split_ref: impl Into<String>) -> Result<String> {
    let split_ref = split_ref.into();
    if split_ref.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "source-backed encoded batch split ref must not be empty".to_string(),
        ));
    }
    Ok(split_ref)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ComparisonOp, EncodedSegment, EncodedValueBatch, EncodedValueRun, EncodingKind, LayoutKind,
        LogicalDType, Nullability, SegmentId, SegmentLayout, SegmentStats, StatValue,
    };

    fn source(uri: &str) -> UniversalInputSource {
        UniversalInputSource::from_dataset_uri(DatasetUri::new(uri).expect("uri")).expect("source")
    }

    fn column_ref(name: &str) -> ColumnRef {
        ColumnRef::new(name).expect("column")
    }

    fn segment(column: &str, id: &str, row_count: u64, encoding: EncodingKind) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            column_ref(column),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(encoding, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    fn filter_batches(
        source_uri: &DatasetUri,
    ) -> Vec<VortexSourceBackedEncodedValuePredicateBatch> {
        vec![
            VortexSourceBackedEncodedValuePredicateBatch::new(
                source_uri.clone(),
                "split-1",
                VortexEncodedValuePredicateBatch::new(
                    segment("metric", "segment-1.metric", 5, EncodingKind::Dictionary),
                    EncodedValueBatch::Dictionary {
                        dictionary: vec![
                            Some(StatValue::Int64(1)),
                            Some(StatValue::Int64(5)),
                            None,
                        ],
                        codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                    },
                ),
            )
            .expect("batch"),
            VortexSourceBackedEncodedValuePredicateBatch::new(
                source_uri.clone(),
                "split-2",
                VortexEncodedValuePredicateBatch::new(
                    segment("metric", "segment-2.metric", 3, EncodingKind::RunLength),
                    EncodedValueBatch::RunLength {
                        runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
                    },
                ),
            )
            .expect("batch"),
        ]
    }

    fn reader_splits(source_uri: &DatasetUri) -> Vec<VortexReaderBackedSplitEvidence> {
        vec![
            VortexReaderBackedSplitEvidence::new(
                source_uri.clone(),
                "split-1",
                5,
                "struct(value=int64)",
                "vortex.dict",
                1,
                1,
            )
            .expect("reader split"),
            VortexReaderBackedSplitEvidence::new(
                source_uri.clone(),
                "split-2",
                3,
                "struct(value=int64)",
                "vortex.run_end",
                1,
                1,
            )
            .expect("reader split"),
        ]
    }

    fn projection_column(
        source_uri: &DatasetUri,
        split_ref: &str,
        column: &str,
        id: &str,
        values: EncodedValueBatch,
    ) -> VortexSourceBackedEncodedProjectionColumn {
        VortexSourceBackedEncodedProjectionColumn::new(
            source_uri.clone(),
            split_ref,
            VortexPreparedEncodedProjectionColumn::new(
                segment(
                    column,
                    id,
                    values.row_count().expect("row count"),
                    values.encoding_kind(),
                ),
                values,
            ),
        )
        .expect("projection column")
    }

    fn reader_generated_constant_kernel_input(
        source_uri: &DatasetUri,
        split_ref: &str,
        column: &str,
        id: &str,
        value: i64,
        row_count: u64,
    ) -> VortexReaderGeneratedEncodedKernelInput {
        VortexReaderGeneratedEncodedKernelInput::new(
            source_uri.clone(),
            split_ref,
            VortexEncodedValuePredicateBatch::new(
                segment(column, id, row_count, EncodingKind::Constant),
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(value)),
                    row_count,
                },
            ),
        )
        .expect("reader-generated kernel input")
    }

    fn reader_generated_kernel_input(
        source_uri: &DatasetUri,
        split_ref: &str,
        column: &str,
        id: &str,
        values: EncodedValueBatch,
    ) -> VortexReaderGeneratedEncodedKernelInput {
        let row_count = values.row_count().expect("row count");
        let encoding = values.encoding_kind();
        VortexReaderGeneratedEncodedKernelInput::new(
            source_uri.clone(),
            split_ref,
            VortexEncodedValuePredicateBatch::new(segment(column, id, row_count, encoding), values),
        )
        .expect("reader-generated kernel input")
    }

    #[test]
    fn source_backed_batch_constructor_rejects_blank_split_ref() {
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let error = VortexSourceBackedEncodedValuePredicateBatch::new(
            source_uri,
            " ",
            VortexEncodedValuePredicateBatch::new(
                segment("metric", "segment-1.metric", 1, EncodingKind::Constant),
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(1)),
                    row_count: 1,
                },
            ),
        )
        .expect_err("blank split ref should be rejected");

        assert!(error.to_string().contains("split ref must not be empty"));
    }

    #[test]
    fn source_backed_filter_accepts_native_source_bound_batches() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };

        let report = execute_vortex_source_backed_filter_from_encoded_value_batches(
            &predicate,
            &source,
            &filter_batches(&source_uri),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
        );
        assert_eq!(report.split_count, 2);
        assert_eq!(report.source_batch_count, 2);
        assert!(report.source_uri_matches_batches);
        assert!(report.runtime_execution_allowed);
        assert!(report.source_backed_batches_consumed);
        assert!(report.selection_vector_guaranteed);
        assert!(report.correctness_certified);
        assert!(report.prepared_execution.runtime_execution_allowed);
        let evidence = report.evidence_gate_report();
        assert!(evidence.correctness_evidence_present);
        assert!(evidence.execution_certificate_present);
        assert!(evidence.native_io_certificate_present);
        assert_eq!(
            evidence.native_io_certificate_path_refs,
            vec!["prepared_vortex_encoded_batches_to_selection_vector_filter_result".to_string()]
        );
        assert!(evidence.no_fallback_evidence_present);
        assert!(evidence.blocks_claims_without_benchmarks());
        assert!(!evidence.external_engine_invoked);
        let certificate_pair = report.certificate_pair_report();
        assert_eq!(
            certificate_pair.report_id,
            evidence.certificate_pair_report_ref
        );
        assert!(certificate_pair.execution_certificate_present);
        assert_eq!(certificate_pair.execution_certificate_status, "certified");
        assert!(certificate_pair.native_io_certificate_present);
        assert_eq!(certificate_pair.native_io_certificate_status, "certified");
        assert_eq!(
            certificate_pair.native_io_certificate_path_id,
            "prepared_vortex_encoded_batches_to_selection_vector_filter_result"
        );
        assert!(certificate_pair.per_path_native_io_certificate);
        assert!(certificate_pair.certificate_pair_complete);
        assert!(!certificate_pair.claim_ready_before_benchmarks());
        assert!(!certificate_pair.external_engine_invoked);
        assert!(!certificate_pair.fallback_attempted);
        assert!(report.avoids_unsafe_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn source_backed_filter_rejects_mixed_source_batches_without_fallback() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let mut batches = filter_batches(&source_uri);
        batches[1].source_uri = DatasetUri::new("file:///tmp/other.vortex").expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };

        let report = execute_vortex_source_backed_filter_from_encoded_value_batches(
            &predicate, &source, &batches,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexSourceBackedEncodedExecutionStatus::BlockedSourceBatchMismatch
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.source_uri_matches_batches);
        assert!(!report.prepared_execution.runtime_execution_allowed);
        assert!(!report.prepared_execution.prepared_encoded_values_consumed);
        let certificate_pair = report.certificate_pair_report();
        assert!(certificate_pair.execution_certificate_present);
        assert_ne!(certificate_pair.execution_certificate_status, "certified");
        assert!(certificate_pair.native_io_certificate_present);
        assert_ne!(certificate_pair.native_io_certificate_status, "certified");
        assert!(!certificate_pair.certificate_pair_complete);
        assert!(report.avoids_unsafe_effects());
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn source_backed_projection_accepts_native_source_bound_columns() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let batches = vec![
            projection_column(
                &source_uri,
                "split-1",
                "metric",
                "segment-1.metric",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(10)), Some(StatValue::Int64(20))],
                    codes: vec![Some(0), Some(1), Some(0)],
                },
            ),
            projection_column(
                &source_uri,
                "split-1",
                "other",
                "segment-1.other",
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(1)),
                    row_count: 3,
                },
            ),
        ];

        let report = execute_vortex_source_backed_projection_from_encoded_projection_batches(
            &[column_ref("metric")],
            &source,
            &batches,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexSourceBackedEncodedExecutionStatus::ExecutedSourceBackedPreparedEncodedBatches
        );
        assert_eq!(report.split_count, 1);
        assert_eq!(report.source_batch_count, 2);
        assert!(report.source_uri_matches_batches);
        assert!(report.runtime_execution_allowed);
        assert!(report.source_backed_batches_consumed);
        assert!(report.encoded_projection_guaranteed);
        assert!(!report.selection_vector_preserved);
        assert!(report.correctness_certified);
        let evidence = report.evidence_gate_report();
        assert!(evidence.correctness_evidence_present);
        assert!(evidence.execution_certificate_present);
        assert!(evidence.native_io_certificate_present);
        assert_eq!(
            evidence.native_io_certificate_path_refs,
            vec!["prepared_vortex_encoded_batches_to_encoded_projection_result".to_string()]
        );
        assert!(evidence.no_fallback_evidence_present);
        assert!(evidence.blocks_claims_without_benchmarks());
        assert!(!evidence.external_engine_invoked);
        let certificate_pair = report.certificate_pair_report();
        assert_eq!(
            certificate_pair.report_id,
            evidence.certificate_pair_report_ref
        );
        assert!(certificate_pair.execution_certificate_present);
        assert_eq!(certificate_pair.execution_certificate_status, "certified");
        assert!(certificate_pair.native_io_certificate_present);
        assert_eq!(certificate_pair.native_io_certificate_status, "certified");
        assert_eq!(
            certificate_pair.native_io_certificate_path_id,
            "prepared_vortex_encoded_batches_to_encoded_projection_result"
        );
        assert!(certificate_pair.per_path_native_io_certificate);
        assert!(certificate_pair.certificate_pair_complete);
        assert!(!certificate_pair.claim_ready_before_benchmarks());
        assert!(!certificate_pair.external_engine_invoked);
        assert!(!certificate_pair.fallback_attempted);
        assert!(report.avoids_unsafe_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn source_backed_projection_rejects_non_native_source_without_fallback() {
        let source = source("file:///tmp/orders.csv");
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let batches = vec![projection_column(
            &source_uri,
            "split-1",
            "metric",
            "segment-1.metric",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 1,
            },
        )];

        let report = execute_vortex_source_backed_projection_from_encoded_projection_batches(
            &[column_ref("metric")],
            &source,
            &batches,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexSourceBackedEncodedExecutionStatus::BlockedNonNativeSource
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.prepared_execution.runtime_execution_allowed);
        assert!(!report.prepared_execution.prepared_encoded_columns_consumed);
        let certificate_pair = report.certificate_pair_report();
        assert!(certificate_pair.execution_certificate_present);
        assert_ne!(certificate_pair.execution_certificate_status, "certified");
        assert!(certificate_pair.native_io_certificate_present);
        assert_ne!(certificate_pair.native_io_certificate_status, "certified");
        assert!(!certificate_pair.certificate_pair_complete);
        assert!(report.avoids_unsafe_effects());
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_split_constructor_records_allowed_local_scan_effects() {
        let source_uri = DatasetUri::new("file:///tmp/orders.vortex").expect("uri");
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri,
            7,
            11,
            "struct(value=int64)",
            "vortex.chunked",
            2,
            3,
        )
        .expect("split");

        assert_eq!(split.split_ref, "vortex-local-scan-chunk-7");
        assert_eq!(split.provider_kind, "vortex_scan");
        assert_eq!(split.provider_boundary.provider_crate, "vortex");
        assert_eq!(split.provider_boundary.provider_version, "0.70");
        assert_eq!(
            split.provider_boundary.feature_gate,
            "vortex-local-primitives"
        );
        assert_eq!(
            split.provider_boundary.admission_policy,
            "shardloom.vortex.local_scan_primitive.v1"
        );
        assert_eq!(
            split.provider_boundary.certificate_requirement,
            "cg16_execution_certificate_and_cg19_native_io_certificate"
        );
        assert!(
            !split
                .provider_boundary
                .support_claim_allowed_without_certificate
        );
        assert!(split.provider_boundary.is_policy_admitted());
        assert!(split.data_read);
        assert!(!split.has_forbidden_effects());
        assert!(split.summary().contains("rows=11"));
    }

    #[test]
    fn reader_generated_prepared_batch_envelopes_preserve_reader_chunks() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");

        let report = plan_vortex_reader_generated_prepared_batch_envelopes(
            &source,
            &reader_splits(&source_uri),
        );

        assert_eq!(
            report.status,
            VortexReaderGeneratedPreparedBatchStatus::PreparedReaderChunkEnvelopes
        );
        assert_eq!(report.reader_split_count, 2);
        assert_eq!(report.generated_batch_count, 2);
        assert_eq!(report.total_rows, 8);
        assert!(report.reader_generated_prepared_batches);
        assert!(report.reader_chunk_envelopes_available);
        assert!(report.provider_boundary.is_policy_admitted());
        assert!(
            report
                .batches
                .iter()
                .all(|batch| batch.provider_boundary.is_policy_admitted())
        );
        assert!(!report.encoded_value_batch_available);
        assert!(!report.encoded_projection_batch_available);
        assert!(report.kernel_input_lowering_blocked);
        assert!(!report.runtime_execution_allowed);
        assert!(report.residual_required);
        assert_eq!(report.residual_executor, "unsupported_blocked");
        assert_eq!(
            report.residual_boundary.residual_expression.as_deref(),
            Some("opaque_vortex_reader_chunk")
        );
        assert!(!report.residual_boundary.external_engine_invoked);
        assert!(report.residual_boundary.prohibited_external_fallback);
        assert!(report.residual_boundary.external_fallback_blocked());
        assert!(
            report
                .batches
                .iter()
                .all(|batch| batch.residual_boundary.external_fallback_blocked())
        );
        assert_eq!(report.representation_before, "vortex_reader_chunk");
        assert_eq!(
            report.representation_after,
            "reader_generated_prepared_chunk_envelope"
        );
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn residual_boundary_reports_all_allowed_executor_values_without_fallback() {
        assert_eq!(
            VortexResidualBoundaryReport::residual_executor_values(),
            &[
                "none",
                "shardloom_native",
                "unsupported_blocked",
                "external_baseline_only",
                "prohibited_external_fallback"
            ]
        );

        let native = VortexResidualBoundaryReport::shardloom_native(
            "residual_filter",
            "shardloom_native_residual_filter",
        );
        assert_eq!(native.residual_executor, "shardloom_native");
        assert!(native.shardloom_native_residual_execution);
        assert!(native.external_fallback_blocked());

        let baseline =
            VortexResidualBoundaryReport::external_baseline_only("residual_filter", "datafusion");
        assert_eq!(baseline.residual_executor, "external_baseline_only");
        assert!(!baseline.external_engine_invoked);
        assert!(baseline.external_fallback_blocked());

        let prohibited = VortexResidualBoundaryReport::prohibited_external_fallback(
            "residual_filter",
            "datafusion_runtime_fallback",
        );
        assert_eq!(prohibited.residual_executor, "prohibited_external_fallback");
        assert!(prohibited.prohibited_external_fallback);
        assert!(prohibited.external_fallback_blocked());
    }

    #[test]
    fn reader_generated_prepared_batch_envelopes_reject_decode_effects() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let mut splits = reader_splits(&source_uri);
        splits[0].data_decoded = true;

        let report = plan_vortex_reader_generated_prepared_batch_envelopes(&source, &splits);

        assert_eq!(
            report.status,
            VortexReaderGeneratedPreparedBatchStatus::BlockedUnsafeReaderEffects
        );
        assert!(!report.reader_generated_prepared_batches);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(!report.avoids_forbidden_effects());
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_generated_prepared_batch_kernel_inputs_admit_mapped_encoded_values() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let inputs = vec![
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-1",
                "metric",
                "reader-split-1.metric",
                5,
                5,
            ),
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-2",
                "metric",
                "reader-split-2.metric",
                9,
                3,
            ),
        ];

        let report = plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits(&source_uri),
            &inputs,
        );

        assert_eq!(
            report.status,
            VortexReaderGeneratedPreparedBatchStatus::PreparedEncodedKernelInputs
        );
        assert_eq!(report.encoded_kernel_input_count, 2);
        assert!(report.reader_generated_prepared_batches);
        assert!(report.reader_chunk_envelopes_available);
        assert!(report.encoded_value_batch_available);
        assert!(report.encoded_projection_batch_available);
        assert!(!report.kernel_input_lowering_blocked);
        assert!(report.runtime_execution_allowed);
        assert!(report.provider_boundary.is_policy_admitted());
        assert!(
            inputs
                .iter()
                .all(|input| input.provider_boundary.is_policy_admitted())
        );
        assert!(!report.residual_required);
        assert_eq!(report.residual_executor, "none");
        assert!(!report.residual_boundary.residual_required);
        assert!(!report.residual_boundary.external_engine_invoked);
        assert!(report.residual_boundary.prohibited_external_fallback);
        assert!(report.residual_boundary.external_fallback_blocked());
        assert!(report.encoded_kernel_inputs_source_uri_matches_source);
        assert!(report.encoded_kernel_input_split_refs_covered_by_reader);
        assert!(report.encoded_kernel_input_row_counts_match_reader);
        assert!(report.encoded_kernel_input_mapping_evidence_complete);
        assert_eq!(
            report.representation_after,
            "reader_generated_prepared_encoded_kernel_input"
        );
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn reader_generated_prepared_batch_kernel_inputs_reject_split_mismatch() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let inputs = vec![reader_generated_constant_kernel_input(
            &source_uri,
            "missing-reader-split",
            "metric",
            "reader-split-1.metric",
            5,
            5,
        )];

        let report = plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits(&source_uri),
            &inputs,
        );

        assert_eq!(
            report.status,
            VortexReaderGeneratedPreparedBatchStatus::BlockedKernelInputSplitMismatch
        );
        assert!(!report.runtime_execution_allowed);
        assert!(report.kernel_input_lowering_blocked);
        assert!(!report.encoded_kernel_input_split_refs_covered_by_reader);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_generated_filter_blocks_unadmitted_kernel_inputs_without_execution() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let mut input = reader_generated_constant_kernel_input(
            &source_uri,
            "split-1",
            "metric",
            "reader-split-1.metric",
            5,
            5,
        );
        input.dtype_mapped_without_decode = false;
        let inputs = vec![input];

        let report = execute_vortex_reader_generated_filter_from_encoded_kernel_inputs(
            &predicate,
            &source,
            &reader_splits(&source_uri),
            &inputs,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::BlockedSourceBackedExecution
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.reader_generated_prepared_batches);
        assert_eq!(report.source_batch_count, 0);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .message
                .contains("require dtype, encoding, value, and row-count mapping evidence")
        }));
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_generated_filter_executes_lowered_kernel_inputs() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let inputs = vec![
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-1",
                "metric",
                "reader-split-1.metric",
                5,
                5,
            ),
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-2",
                "metric",
                "reader-split-2.metric",
                9,
                3,
            ),
        ];

        let report = execute_vortex_reader_generated_filter_from_encoded_kernel_inputs(
            &predicate,
            &source,
            &reader_splits(&source_uri),
            &inputs,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
        );
        assert!(report.runtime_execution_allowed);
        assert!(report.reader_generated_prepared_batches);
        assert!(report.reader_split_evidence_consumed);
        assert!(report.reader_validated_prepared_batches_consumed);
        assert!(report.selection_vector_guaranteed);
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn reader_generated_conjunctive_filter_intersects_selection_vectors() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicates = vec![
            PredicateExpr::Compare {
                column: column_ref("flag"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(1),
            },
            PredicateExpr::Compare {
                column: column_ref("value"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(5),
            },
        ];
        let inputs = vec![
            reader_generated_kernel_input(
                &source_uri,
                "split-1",
                "flag",
                "reader-split-1.flag",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(0))],
                    codes: vec![Some(0), Some(1), Some(0), Some(0), Some(1)],
                },
            ),
            reader_generated_kernel_input(
                &source_uri,
                "split-1",
                "value",
                "reader-split-1.value",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![
                        Some(StatValue::Int64(1)),
                        Some(StatValue::Int64(5)),
                        Some(StatValue::Int64(7)),
                    ],
                    codes: vec![Some(0), Some(1), Some(1), Some(2), Some(2)],
                },
            ),
            reader_generated_kernel_input(
                &source_uri,
                "split-2",
                "flag",
                "reader-split-2.flag",
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(1)), 3)],
                },
            ),
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-2",
                "value",
                "reader-split-2.value",
                9,
                3,
            ),
        ];

        let report = execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
            &predicates,
            &source,
            &reader_splits(&source_uri),
            &inputs,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderGeneratedConjunctiveSelectionVectorStatus::IntersectedSelectionVectors
        );
        assert_eq!(report.predicate_count, 2);
        assert_eq!(report.reader_split_count, 2);
        assert_eq!(report.encoded_kernel_input_count, 4);
        assert_eq!(report.intersection_count, 2);
        assert_eq!(report.selected_row_count, Some(5));
        assert!(report.runtime_execution_allowed);
        assert!(report.reader_generated_prepared_batches);
        assert!(report.filter_column_batches_consumed);
        assert!(report.selection_vector_intersection_certified);
        assert!(report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.correctness_certified);
        assert!(!report.production_claim_allowed);
        assert!(!report.has_errors());
    }

    #[test]
    fn reader_generated_conjunctive_filter_blocks_missing_filter_column_input() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicates = vec![
            PredicateExpr::Compare {
                column: column_ref("flag"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(1),
            },
            PredicateExpr::Compare {
                column: column_ref("value"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(5),
            },
        ];
        let inputs = vec![
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-1",
                "flag",
                "reader-split-1.flag",
                1,
                5,
            ),
            reader_generated_constant_kernel_input(
                &source_uri,
                "split-2",
                "flag",
                "reader-split-2.flag",
                1,
                3,
            ),
        ];

        let report = execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
            &predicates,
            &source,
            &reader_splits(&source_uri),
            &inputs,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderGeneratedConjunctiveSelectionVectorStatus::BlockedMissingPredicateInput
        );
        assert_eq!(report.predicate_count, 2);
        assert_eq!(report.reader_split_count, 2);
        assert_eq!(report.encoded_kernel_input_count, 2);
        assert_eq!(report.intersection_count, 0);
        assert_eq!(report.selected_row_count, None);
        assert!(!report.runtime_execution_allowed);
        assert!(report.reader_generated_prepared_batches);
        assert!(!report.filter_column_batches_consumed);
        assert!(!report.selection_vector_intersection_certified);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.message.contains(
                "missing reader-generated encoded kernel input for column value on split split-1",
            )
        }));
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn reader_generated_projection_blocks_unadmitted_kernel_inputs_without_execution() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let mut input = reader_generated_constant_kernel_input(
            &source_uri,
            "split-1",
            "metric",
            "reader-split-1.metric",
            5,
            5,
        );
        input.values_mapped_without_decode = false;
        let inputs = vec![input];

        let report = execute_vortex_reader_generated_projection_from_encoded_kernel_inputs(
            &[column_ref("metric")],
            &source,
            &reader_splits(&source_uri),
            &inputs,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::BlockedSourceBackedExecution
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.reader_generated_prepared_batches);
        assert_eq!(report.source_batch_count, 0);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .message
                .contains("require dtype, encoding, value, and row-count mapping evidence")
        }));
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_generated_projection_executes_lowered_kernel_inputs() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let inputs = vec![reader_generated_constant_kernel_input(
            &source_uri,
            "split-1",
            "metric",
            "reader-split-1.metric",
            5,
            5,
        )];

        let report = execute_vortex_reader_generated_projection_from_encoded_kernel_inputs(
            &[column_ref("metric")],
            &source,
            &reader_splits(&source_uri),
            &inputs,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
        );
        assert!(report.runtime_execution_allowed);
        assert!(report.reader_generated_prepared_batches);
        assert!(report.encoded_projection_guaranteed);
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn reader_backed_filter_accepts_reader_split_bound_batches() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };

        let report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
            &predicate,
            &source,
            &reader_splits(&source_uri),
            &filter_batches(&source_uri),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
        );
        assert_eq!(report.reader_split_count, 2);
        assert_eq!(report.source_batch_count, 2);
        assert!(report.runtime_execution_allowed);
        assert!(report.reader_split_evidence_consumed);
        assert!(report.reader_validated_prepared_batches_consumed);
        assert!(!report.reader_generated_prepared_batches);
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
    }

    #[test]
    fn reader_backed_filter_rejects_batch_split_not_seen_by_reader() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let mut batches = filter_batches(&source_uri);
        batches[1].split_ref = "missing-reader-split".to_string();

        let report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
            &predicate,
            &source,
            &reader_splits(&source_uri),
            &batches,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::BlockedPreparedBatchSplitMismatch
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.prepared_batch_split_refs_covered_by_reader);
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn reader_backed_projection_accepts_reader_split_bound_columns() {
        let source = source("file:///tmp/orders.vortex");
        let source_uri = source.uri.clone().expect("uri");
        let batches = vec![projection_column(
            &source_uri,
            "split-1",
            "metric",
            "segment-1.metric",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 3,
            },
        )];

        let report = execute_vortex_reader_backed_projection_from_encoded_projection_batches(
            &[column_ref("metric")],
            &source,
            &reader_splits(&source_uri),
            &batches,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
        );
        assert_eq!(report.reader_split_count, 2);
        assert_eq!(report.source_batch_count, 1);
        assert!(report.runtime_execution_allowed);
        assert!(report.encoded_projection_guaranteed);
        assert!(!report.reader_generated_prepared_batches);
        assert!(report.data_read);
        assert!(report.avoids_forbidden_effects());
        assert!(!report.has_errors());
    }
}
