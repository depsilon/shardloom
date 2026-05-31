//! Source-backed correctness and benchmark matrix for current encoded paths.
//!
//! The matrix is populated as a claim gate: it names the rows and required
//! evidence before source-backed performance claims, but it does not execute
//! benchmarks or invoke external engines.

use std::collections::BTreeSet;
use std::time::Instant;

use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, EncodedSegment, EncodedValueBatch, EncodedValueRun,
    EncodingKind, ExecutionCertificate, LayoutKind, LogicalDType, NativeIoCertificate, Nullability,
    PredicateExpr, Result, SegmentId, SegmentLayout, SegmentStats, ShardLoomError, StatValue,
    UniversalInputSource,
};

use crate::{
    VortexEncodedValuePredicateBatch, VortexGeneralizedEncodedFilterExecutionReport,
    VortexGeneralizedEncodedProjectionExecutionReport, VortexPreparedEncodedProjectionColumn,
    VortexReaderBackedEncodedFilterExecutionReport,
    VortexReaderBackedEncodedProjectionExecutionReport, VortexReaderBackedSplitEvidence,
    VortexReaderGeneratedEncodedKernelInput, VortexSourceBackedEncodedFilterExecutionReport,
    VortexSourceBackedEncodedProjectionColumn, VortexSourceBackedEncodedProjectionExecutionReport,
    VortexSourceBackedEncodedValuePredicateBatch,
    execute_vortex_generalized_filter_from_encoded_value_batches,
    execute_vortex_generalized_projection_from_encoded_projection_batches,
    execute_vortex_reader_generated_filter_from_encoded_kernel_inputs,
    execute_vortex_reader_generated_projection_from_encoded_kernel_inputs,
    execute_vortex_source_backed_filter_from_encoded_value_batches,
    execute_vortex_source_backed_projection_from_encoded_projection_batches,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBackedBenchmarkLane {
    PreparedBatchOnly,
    SourceBoundEncoded,
    ReaderBackedConstant,
    ReaderBackedDictionary,
    ReaderBackedRunEnd,
    BlockedSparseNullable,
    BlockedDeviceBuffer,
    BlockedNested,
    BlockedExtensionDType,
}

impl SourceBackedBenchmarkLane {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedBatchOnly => "prepared_batch_only",
            Self::SourceBoundEncoded => "source_bound_encoded",
            Self::ReaderBackedConstant => "reader_backed_constant",
            Self::ReaderBackedDictionary => "reader_backed_dictionary",
            Self::ReaderBackedRunEnd => "reader_backed_run_end",
            Self::BlockedSparseNullable => "blocked_sparse_nullable",
            Self::BlockedDeviceBuffer => "blocked_device_buffer",
            Self::BlockedNested => "blocked_nested",
            Self::BlockedExtensionDType => "blocked_extension_dtype",
        }
    }

    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(
            self,
            Self::BlockedSparseNullable
                | Self::BlockedDeviceBuffer
                | Self::BlockedNested
                | Self::BlockedExtensionDType
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBackedBenchmarkOperation {
    Filter,
    Projection,
    FilterProject,
}

impl SourceBackedBenchmarkOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::FilterProject => "filter_project",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBackedBenchmarkRowStatus {
    EvidenceRequired,
    BenchmarkRowMissing,
    MeasuredFixture,
    BlockedUnsupported,
}

impl SourceBackedBenchmarkRowStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceRequired => "evidence_required",
            Self::BenchmarkRowMissing => "benchmark_row_missing",
            Self::MeasuredFixture => "measured_fixture",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }

    #[must_use]
    pub const fn claim_ready(self) -> bool {
        false
    }

    #[must_use]
    pub const fn is_measured(self) -> bool {
        matches!(self, Self::MeasuredFixture)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBackedBenchmarkMeasuredRow {
    pub row_id: &'static str,
    pub lane: SourceBackedBenchmarkLane,
    pub operation: SourceBackedBenchmarkOperation,
    pub benchmark_row_ref: String,
    pub elapsed_nanos: u128,
    pub row_count: Option<u64>,
    pub selected_or_projected_count: Option<u64>,
    pub provider_kind: String,
    pub provider_api_surface: String,
    pub provider_version: String,
    pub source_refs: Vec<String>,
    pub split_refs: Vec<String>,
    pub representation_transitions: Vec<String>,
    pub residual_executor: String,
    pub execution_certificate_refs: Vec<String>,
    pub native_io_certificate_refs: Vec<String>,
    pub native_io_certificate_path_refs: Vec<String>,
    pub correctness_refs: Vec<String>,
    pub benchmark_constitution: String,
    pub reproducibility_ref: String,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub performance_claim_allowed: bool,
}

impl SourceBackedBenchmarkMeasuredRow {
    #[must_use]
    pub fn has_claim_grade_measurement_fields(&self) -> bool {
        self.elapsed_nanos > 0
            && self.row_count.is_some()
            && self.selected_or_projected_count.is_some()
            && !self.provider_kind.is_empty()
            && !self.provider_api_surface.is_empty()
            && !self.provider_version.is_empty()
            && !self.source_refs.is_empty()
            && !self.split_refs.is_empty()
            && !self.representation_transitions.is_empty()
            && self.residual_executor == "none"
            && !self.execution_certificate_refs.is_empty()
            && !self.native_io_certificate_refs.is_empty()
            && !self.native_io_certificate_path_refs.is_empty()
            && !self.correctness_refs.is_empty()
            && !self.benchmark_constitution.is_empty()
            && !self.reproducibility_ref.is_empty()
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && !self.performance_claim_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SourceBackedBenchmarkMatrixRow {
    pub row_id: &'static str,
    pub lane: SourceBackedBenchmarkLane,
    pub operation: SourceBackedBenchmarkOperation,
    pub status: SourceBackedBenchmarkRowStatus,
    pub source_uri_required: bool,
    pub split_refs_required: bool,
    pub provider_kind_required: bool,
    pub provider_api_surface_required: bool,
    pub vortex_version_required: bool,
    pub row_count_required: bool,
    pub selected_or_projected_count_required: bool,
    pub representation_transitions_required: bool,
    pub residual_executor: &'static str,
    pub execution_certificate_ref_required: bool,
    pub native_io_certificate_ref_required: bool,
    pub correctness_fixture_ref_required: bool,
    pub benchmark_row_ref_required: bool,
    pub rust_performance_profile_required: bool,
    pub deterministic_blocker: Option<&'static str>,
    pub benchmark_row_present: bool,
    pub correctness_ref_present: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub performance_claim_allowed: bool,
}

impl SourceBackedBenchmarkMatrixRow {
    #[must_use]
    pub const fn executable(
        row_id: &'static str,
        lane: SourceBackedBenchmarkLane,
        operation: SourceBackedBenchmarkOperation,
    ) -> Self {
        Self {
            row_id,
            lane,
            operation,
            status: SourceBackedBenchmarkRowStatus::BenchmarkRowMissing,
            source_uri_required: true,
            split_refs_required: true,
            provider_kind_required: true,
            provider_api_surface_required: true,
            vortex_version_required: true,
            row_count_required: true,
            selected_or_projected_count_required: true,
            representation_transitions_required: true,
            residual_executor: "none_or_shardloom_native_required",
            execution_certificate_ref_required: true,
            native_io_certificate_ref_required: true,
            correctness_fixture_ref_required: true,
            benchmark_row_ref_required: true,
            rust_performance_profile_required: true,
            deterministic_blocker: None,
            benchmark_row_present: false,
            correctness_ref_present: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            performance_claim_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocked(
        row_id: &'static str,
        lane: SourceBackedBenchmarkLane,
        operation: SourceBackedBenchmarkOperation,
        deterministic_blocker: &'static str,
    ) -> Self {
        Self {
            row_id,
            lane,
            operation,
            status: SourceBackedBenchmarkRowStatus::BlockedUnsupported,
            source_uri_required: false,
            split_refs_required: false,
            provider_kind_required: true,
            provider_api_surface_required: true,
            vortex_version_required: true,
            row_count_required: false,
            selected_or_projected_count_required: false,
            representation_transitions_required: true,
            residual_executor: "unsupported_blocked",
            execution_certificate_ref_required: false,
            native_io_certificate_ref_required: false,
            correctness_fixture_ref_required: false,
            benchmark_row_ref_required: false,
            rust_performance_profile_required: false,
            deterministic_blocker: Some(deterministic_blocker),
            benchmark_row_present: false,
            correctness_ref_present: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            performance_claim_allowed: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }

    #[must_use]
    pub const fn requires_claim_grade_evidence(&self) -> bool {
        !self.lane.is_blocked()
            && self.source_uri_required
            && self.split_refs_required
            && self.provider_kind_required
            && self.provider_api_surface_required
            && self.vortex_version_required
            && self.row_count_required
            && self.selected_or_projected_count_required
            && self.representation_transitions_required
            && self.execution_certificate_ref_required
            && self.native_io_certificate_ref_required
            && self.correctness_fixture_ref_required
            && self.benchmark_row_ref_required
            && self.rust_performance_profile_required
            && !self.performance_claim_allowed
    }

    fn mark_measured_fixture(&mut self) {
        self.status = SourceBackedBenchmarkRowStatus::MeasuredFixture;
        self.benchmark_row_present = true;
        self.correctness_ref_present = true;
        self.residual_executor = "none";
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SourceBackedBenchmarkMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<SourceBackedBenchmarkMatrixRow>,
    pub measured_rows: Vec<SourceBackedBenchmarkMeasuredRow>,
    pub executable_row_count: usize,
    pub blocked_row_count: usize,
    pub measured_row_count: usize,
    pub benchmark_rows_required: bool,
    pub measured_benchmark_rows_present: bool,
    pub correctness_refs_required: bool,
    pub rust_performance_profile_required: bool,
    pub reproducibility_manifest_ref: Option<&'static str>,
    pub source_backed_claim_closeout_allowed: bool,
    pub benchmark_execution_performed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl SourceBackedBenchmarkMatrixReport {
    #[must_use]
    pub fn current() -> Self {
        let rows = source_backed_matrix_rows();
        Self::from_rows(rows, Vec::new(), None, false)
    }

    #[must_use]
    fn from_rows(
        rows: Vec<SourceBackedBenchmarkMatrixRow>,
        measured_rows: Vec<SourceBackedBenchmarkMeasuredRow>,
        reproducibility_manifest_ref: Option<&'static str>,
        benchmark_execution_performed: bool,
    ) -> Self {
        let executable_row_count = rows.iter().filter(|row| !row.lane.is_blocked()).count();
        let blocked_row_count = rows.len() - executable_row_count;
        let measured_row_count = measured_rows.len();
        let measured_benchmark_rows_present = measured_row_count > 0;
        Self {
            schema_version: "shardloom.source_backed_benchmark_matrix.v1",
            report_id: "priority_2_7.source_backed_correctness_benchmark_matrix",
            rows,
            measured_rows,
            executable_row_count,
            blocked_row_count,
            measured_row_count,
            benchmark_rows_required: true,
            measured_benchmark_rows_present,
            correctness_refs_required: true,
            rust_performance_profile_required: true,
            reproducibility_manifest_ref,
            source_backed_claim_closeout_allowed: false,
            benchmark_execution_performed,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn covers_required_plan_matrix(&self) -> bool {
        self.has_lane_operation(SourceBackedBenchmarkLane::PreparedBatchOnly)
            && self.has_lane_operation(SourceBackedBenchmarkLane::SourceBoundEncoded)
            && self.has_lane_operation(SourceBackedBenchmarkLane::ReaderBackedConstant)
            && self.has_lane_operation(SourceBackedBenchmarkLane::ReaderBackedDictionary)
            && self.has_lane_operation(SourceBackedBenchmarkLane::ReaderBackedRunEnd)
            && self.has_blocked_lane(SourceBackedBenchmarkLane::BlockedSparseNullable)
            && self.has_blocked_lane(SourceBackedBenchmarkLane::BlockedDeviceBuffer)
            && self.has_blocked_lane(SourceBackedBenchmarkLane::BlockedNested)
            && self.has_blocked_lane(SourceBackedBenchmarkLane::BlockedExtensionDType)
    }

    #[must_use]
    pub fn executable_rows_require_all_claim_grade_evidence(&self) -> bool {
        self.rows
            .iter()
            .filter(|row| !row.lane.is_blocked())
            .all(SourceBackedBenchmarkMatrixRow::requires_claim_grade_evidence)
    }

    #[must_use]
    pub fn blocked_rows_have_deterministic_no_fallback_blockers(&self) -> bool {
        self.rows
            .iter()
            .filter(|row| row.lane.is_blocked())
            .all(|row| {
                row.deterministic_blocker.is_some()
                    && row.residual_executor == "unsupported_blocked"
                    && row.fallback_free()
            })
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        self.rows
            .iter()
            .all(SourceBackedBenchmarkMatrixRow::fallback_free)
            && self
                .measured_rows
                .iter()
                .all(|row| !row.external_engine_invoked && !row.fallback_attempted)
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn measured_rows_have_claim_grade_fields(&self) -> bool {
        self.measured_rows
            .iter()
            .all(SourceBackedBenchmarkMeasuredRow::has_claim_grade_measurement_fields)
    }

    #[must_use]
    pub fn measured_rows_cover_executable_matrix(&self) -> bool {
        let measured = self
            .measured_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<BTreeSet<_>>();
        self.rows
            .iter()
            .filter(|row| !row.lane.is_blocked())
            .all(|row| measured.contains(row.row_id))
    }

    #[must_use]
    pub fn measured_rows_remain_fixture_evidence_only(&self) -> bool {
        self.measured_benchmark_rows_present
            && self.benchmark_execution_performed
            && !self.source_backed_claim_closeout_allowed
            && self
                .measured_rows
                .iter()
                .all(|row| !row.performance_claim_allowed)
    }

    fn has_lane_operation(&self, lane: SourceBackedBenchmarkLane) -> bool {
        [
            SourceBackedBenchmarkOperation::Filter,
            SourceBackedBenchmarkOperation::Projection,
            SourceBackedBenchmarkOperation::FilterProject,
        ]
        .into_iter()
        .all(|operation| {
            self.rows.iter().any(|row| {
                row.lane == lane
                    && !row.lane.is_blocked()
                    && row.operation == operation
                    && row.status != SourceBackedBenchmarkRowStatus::BlockedUnsupported
            })
        })
    }

    fn has_blocked_lane(&self, lane: SourceBackedBenchmarkLane) -> bool {
        self.rows.iter().any(|row| {
            row.lane == lane && row.status == SourceBackedBenchmarkRowStatus::BlockedUnsupported
        })
    }
}

#[must_use]
pub fn plan_source_backed_benchmark_matrix() -> SourceBackedBenchmarkMatrixReport {
    SourceBackedBenchmarkMatrixReport::current()
}

/// Measures the current eligible source-backed benchmark matrix rows against
/// deterministic in-memory Vortex-encoded fixture batches.
///
/// This is fixture evidence for the benchmark taxonomy and source-backed row
/// population gate. It does not read files, invoke external engines, authorize
/// source-backed claim closeout, or permit performance claims.
///
/// # Errors
/// Returns an error when any encoded fixture or admitted execution report cannot
/// be constructed.
pub fn measure_source_backed_benchmark_matrix_smoke() -> Result<SourceBackedBenchmarkMatrixReport> {
    let measured_rows = measure_source_backed_rows()?;
    let measured_row_ids = measured_rows
        .iter()
        .map(|row| row.row_id)
        .collect::<BTreeSet<_>>();
    let mut rows = source_backed_matrix_rows();
    for row in &mut rows {
        if measured_row_ids.contains(row.row_id) {
            row.mark_measured_fixture();
        }
    }
    Ok(SourceBackedBenchmarkMatrixReport::from_rows(
        rows,
        measured_rows,
        Some("benchmarks/source_backed/source_backed_benchmark_matrix_smoke.v1"),
        true,
    ))
}

fn source_backed_matrix_rows() -> Vec<SourceBackedBenchmarkMatrixRow> {
    let mut rows = Vec::new();
    for lane in [
        SourceBackedBenchmarkLane::PreparedBatchOnly,
        SourceBackedBenchmarkLane::SourceBoundEncoded,
        SourceBackedBenchmarkLane::ReaderBackedConstant,
        SourceBackedBenchmarkLane::ReaderBackedDictionary,
        SourceBackedBenchmarkLane::ReaderBackedRunEnd,
    ] {
        rows.push(SourceBackedBenchmarkMatrixRow::executable(
            row_id(lane, SourceBackedBenchmarkOperation::Filter),
            lane,
            SourceBackedBenchmarkOperation::Filter,
        ));
        rows.push(SourceBackedBenchmarkMatrixRow::executable(
            row_id(lane, SourceBackedBenchmarkOperation::Projection),
            lane,
            SourceBackedBenchmarkOperation::Projection,
        ));
        rows.push(SourceBackedBenchmarkMatrixRow::executable(
            row_id(lane, SourceBackedBenchmarkOperation::FilterProject),
            lane,
            SourceBackedBenchmarkOperation::FilterProject,
        ));
    }
    rows.extend(blocked_rows());
    rows
}

const fn row_id(
    lane: SourceBackedBenchmarkLane,
    operation: SourceBackedBenchmarkOperation,
) -> &'static str {
    match (lane, operation) {
        (SourceBackedBenchmarkLane::PreparedBatchOnly, SourceBackedBenchmarkOperation::Filter) => {
            "prepared_batch_only.filter"
        }
        (
            SourceBackedBenchmarkLane::PreparedBatchOnly,
            SourceBackedBenchmarkOperation::Projection,
        ) => "prepared_batch_only.projection",
        (
            SourceBackedBenchmarkLane::PreparedBatchOnly,
            SourceBackedBenchmarkOperation::FilterProject,
        ) => "prepared_batch_only.filter_project",
        (SourceBackedBenchmarkLane::SourceBoundEncoded, SourceBackedBenchmarkOperation::Filter) => {
            "source_bound_encoded.filter"
        }
        (
            SourceBackedBenchmarkLane::SourceBoundEncoded,
            SourceBackedBenchmarkOperation::Projection,
        ) => "source_bound_encoded.projection",
        (
            SourceBackedBenchmarkLane::SourceBoundEncoded,
            SourceBackedBenchmarkOperation::FilterProject,
        ) => "source_bound_encoded.filter_project",
        (
            SourceBackedBenchmarkLane::ReaderBackedConstant,
            SourceBackedBenchmarkOperation::Filter,
        ) => "reader_backed_constant.filter",
        (
            SourceBackedBenchmarkLane::ReaderBackedConstant,
            SourceBackedBenchmarkOperation::Projection,
        ) => "reader_backed_constant.projection",
        (
            SourceBackedBenchmarkLane::ReaderBackedConstant,
            SourceBackedBenchmarkOperation::FilterProject,
        ) => "reader_backed_constant.filter_project",
        (
            SourceBackedBenchmarkLane::ReaderBackedDictionary,
            SourceBackedBenchmarkOperation::Filter,
        ) => "reader_backed_dictionary.filter",
        (
            SourceBackedBenchmarkLane::ReaderBackedDictionary,
            SourceBackedBenchmarkOperation::Projection,
        ) => "reader_backed_dictionary.projection",
        (
            SourceBackedBenchmarkLane::ReaderBackedDictionary,
            SourceBackedBenchmarkOperation::FilterProject,
        ) => "reader_backed_dictionary.filter_project",
        (SourceBackedBenchmarkLane::ReaderBackedRunEnd, SourceBackedBenchmarkOperation::Filter) => {
            "reader_backed_run_end.filter"
        }
        (
            SourceBackedBenchmarkLane::ReaderBackedRunEnd,
            SourceBackedBenchmarkOperation::Projection,
        ) => "reader_backed_run_end.projection",
        (
            SourceBackedBenchmarkLane::ReaderBackedRunEnd,
            SourceBackedBenchmarkOperation::FilterProject,
        ) => "reader_backed_run_end.filter_project",
        _ => "blocked.unspecified",
    }
}

fn blocked_rows() -> Vec<SourceBackedBenchmarkMatrixRow> {
    vec![
        SourceBackedBenchmarkMatrixRow::blocked(
            "blocked_sparse_nullable.filter_project",
            SourceBackedBenchmarkLane::BlockedSparseNullable,
            SourceBackedBenchmarkOperation::FilterProject,
            "sparse_or_nullable_dictionary_rle_path_requires_validity_and_selection_vector_evidence",
        ),
        SourceBackedBenchmarkMatrixRow::blocked(
            "blocked_device_buffer.filter_project",
            SourceBackedBenchmarkLane::BlockedDeviceBuffer,
            SourceBackedBenchmarkOperation::FilterProject,
            "device_buffer_path_requires_device_residency_and_host_device_transfer_evidence",
        ),
        SourceBackedBenchmarkMatrixRow::blocked(
            "blocked_nested.filter_project",
            SourceBackedBenchmarkLane::BlockedNested,
            SourceBackedBenchmarkOperation::FilterProject,
            "nested_parent_child_execution_requires_parent_child_array_execution_certificate",
        ),
        SourceBackedBenchmarkMatrixRow::blocked(
            "blocked_extension_dtype.filter_project",
            SourceBackedBenchmarkLane::BlockedExtensionDType,
            SourceBackedBenchmarkOperation::FilterProject,
            "extension_dtype_execution_requires_extension_capability_and_expression_evidence",
        ),
    ]
}

const BENCHMARK_CONSTITUTION: &str = "local_vortex_source_backed_encoded_smoke_v1";
const REPRODUCIBILITY_REF: &str =
    "benchmarks/source_backed/source_backed_benchmark_matrix_smoke.v1";
const FIXTURE_SOURCE_URI: &str = "file:///tmp/shardloom/source-backed-benchmark-smoke.vortex";

#[allow(clippy::too_many_lines)]
fn measure_source_backed_rows() -> Result<Vec<SourceBackedBenchmarkMeasuredRow>> {
    let source = benchmark_source()?;
    let source_uri = benchmark_source_uri()?;
    let predicate = metric_threshold_predicate()?;
    let requested_columns = vec![column_ref("metric")?];
    let mut rows = Vec::new();

    let prepared_filter_batches = prepared_predicate_batches()?;
    let prepared_split_refs = prepared_split_refs();
    let started = Instant::now();
    let prepared_filter_report = execute_vortex_generalized_filter_from_encoded_value_batches(
        &predicate,
        &prepared_filter_batches,
    )?;
    ensure_measurement_report_ok(
        "prepared_batch_only.filter",
        prepared_filter_report.has_errors(),
    )?;
    rows.push(measured_prepared_filter_row(
        SourceBackedBenchmarkLane::PreparedBatchOnly,
        &source_uri,
        prepared_split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_predicate_batches(&prepared_filter_batches),
        &prepared_filter_report,
    ));

    let prepared_projection_fixture_columns = prepared_projection_columns()?;
    let started = Instant::now();
    let prepared_projection_report =
        execute_vortex_generalized_projection_from_encoded_projection_batches(
            &requested_columns,
            &prepared_projection_fixture_columns,
            None,
        )?;
    ensure_measurement_report_ok(
        "prepared_batch_only.projection",
        prepared_projection_report.has_errors(),
    )?;
    rows.push(measured_prepared_projection_row(
        SourceBackedBenchmarkLane::PreparedBatchOnly,
        &source_uri,
        prepared_split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_projection_columns(&prepared_projection_fixture_columns),
        &prepared_projection_report,
    ));

    let filter_project_filter_batches = prepared_predicate_batches()?;
    let filter_project_columns = prepared_projection_columns()?;
    let started = Instant::now();
    let filter_project_filter = execute_vortex_generalized_filter_from_encoded_value_batches(
        &predicate,
        &filter_project_filter_batches,
    )?;
    let filter_project_projection =
        execute_vortex_generalized_projection_from_encoded_projection_batches(
            &requested_columns,
            &filter_project_columns,
            Some(&filter_project_filter.filter_kernel),
        )?;
    ensure_measurement_report_ok(
        "prepared_batch_only.filter_project",
        filter_project_filter.has_errors() || filter_project_projection.has_errors(),
    )?;
    rows.push(measured_prepared_filter_project_row(
        SourceBackedBenchmarkLane::PreparedBatchOnly,
        &source_uri,
        prepared_split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_predicate_batches(&filter_project_filter_batches),
        &filter_project_filter,
        &filter_project_projection,
    ));

    let source_filter_batches = source_bound_filter_batches(&source_uri)?;
    let source_split_refs = source_bound_split_refs(&source_filter_batches);
    let started = Instant::now();
    let source_filter_report = execute_vortex_source_backed_filter_from_encoded_value_batches(
        &predicate,
        &source,
        &source_filter_batches,
    )?;
    ensure_measurement_report_ok(
        "source_bound_encoded.filter",
        source_filter_report.has_errors(),
    )?;
    rows.push(measured_source_filter_row(
        &source_uri,
        source_split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_source_filter_batches(&source_filter_batches),
        &source_filter_report,
    ));

    let source_projection_columns = source_bound_projection_columns(&source_uri)?;
    let source_projection_split_refs =
        source_bound_projection_split_refs(&source_projection_columns);
    let started = Instant::now();
    let source_projection_report =
        execute_vortex_source_backed_projection_from_encoded_projection_batches(
            &requested_columns,
            &source,
            &source_projection_columns,
            None,
        )?;
    ensure_measurement_report_ok(
        "source_bound_encoded.projection",
        source_projection_report.has_errors(),
    )?;
    rows.push(measured_source_projection_row(
        &source_uri,
        source_projection_split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_source_projection_columns(&source_projection_columns),
        &source_projection_report,
    ));

    let source_filter_project_batches = source_bound_filter_batches(&source_uri)?;
    let source_filter_project_columns = source_bound_projection_columns(&source_uri)?;
    let started = Instant::now();
    let source_filter_project_filter =
        execute_vortex_source_backed_filter_from_encoded_value_batches(
            &predicate,
            &source,
            &source_filter_project_batches,
        )?;
    let source_filter_project_projection =
        execute_vortex_source_backed_projection_from_encoded_projection_batches(
            &requested_columns,
            &source,
            &source_filter_project_columns,
            Some(
                &source_filter_project_filter
                    .prepared_execution
                    .filter_kernel,
            ),
        )?;
    ensure_measurement_report_ok(
        "source_bound_encoded.filter_project",
        source_filter_project_filter.has_errors() || source_filter_project_projection.has_errors(),
    )?;
    rows.push(measured_source_filter_project_row(
        &source_uri,
        source_bound_split_refs(&source_filter_project_batches),
        elapsed_nanos(started),
        row_count_from_source_filter_batches(&source_filter_project_batches),
        &source_filter_project_filter,
        &source_filter_project_projection,
    ));

    for lane in [
        SourceBackedBenchmarkLane::ReaderBackedConstant,
        SourceBackedBenchmarkLane::ReaderBackedDictionary,
        SourceBackedBenchmarkLane::ReaderBackedRunEnd,
    ] {
        measure_reader_backed_lane(
            lane,
            &source,
            &source_uri,
            &predicate,
            &requested_columns,
            &mut rows,
        )?;
    }

    Ok(rows)
}

fn measure_reader_backed_lane(
    lane: SourceBackedBenchmarkLane,
    source: &UniversalInputSource,
    source_uri: &DatasetUri,
    predicate: &PredicateExpr,
    requested_columns: &[ColumnRef],
    rows: &mut Vec<SourceBackedBenchmarkMeasuredRow>,
) -> Result<()> {
    let reader_splits = reader_splits_for_lane(source_uri, lane)?;
    let split_refs = reader_splits
        .iter()
        .map(|split| split.split_ref.clone())
        .collect::<Vec<_>>();

    let filter_inputs = reader_kernel_inputs_for_lane(source_uri, lane)?;
    let started = Instant::now();
    let filter_report = execute_vortex_reader_generated_filter_from_encoded_kernel_inputs(
        predicate,
        source,
        &reader_splits,
        &filter_inputs,
    )?;
    ensure_measurement_report_ok(
        row_id(lane, SourceBackedBenchmarkOperation::Filter),
        filter_report.has_errors(),
    )?;
    rows.push(measured_reader_filter_row(
        lane,
        source_uri,
        split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_reader_inputs(&filter_inputs),
        &filter_report,
    ));

    let projection_inputs = reader_kernel_inputs_for_lane(source_uri, lane)?;
    let started = Instant::now();
    let projection_report = execute_vortex_reader_generated_projection_from_encoded_kernel_inputs(
        requested_columns,
        source,
        &reader_splits,
        &projection_inputs,
        None,
    )?;
    ensure_measurement_report_ok(
        row_id(lane, SourceBackedBenchmarkOperation::Projection),
        projection_report.has_errors(),
    )?;
    rows.push(measured_reader_projection_row(
        lane,
        source_uri,
        split_refs.clone(),
        elapsed_nanos(started),
        row_count_from_reader_inputs(&projection_inputs),
        &projection_report,
    ));

    let filter_project_filter_inputs = reader_kernel_inputs_for_lane(source_uri, lane)?;
    let filter_project_projection_inputs = reader_kernel_inputs_for_lane(source_uri, lane)?;
    let started = Instant::now();
    let filter_project_filter = execute_vortex_reader_generated_filter_from_encoded_kernel_inputs(
        predicate,
        source,
        &reader_splits,
        &filter_project_filter_inputs,
    )?;
    let filter_project_projection =
        execute_vortex_reader_generated_projection_from_encoded_kernel_inputs(
            requested_columns,
            source,
            &reader_splits,
            &filter_project_projection_inputs,
            Some(
                &filter_project_filter
                    .source_execution
                    .prepared_execution
                    .filter_kernel,
            ),
        )?;
    ensure_measurement_report_ok(
        row_id(lane, SourceBackedBenchmarkOperation::FilterProject),
        filter_project_filter.has_errors() || filter_project_projection.has_errors(),
    )?;
    rows.push(measured_reader_filter_project_row(
        lane,
        source_uri,
        split_refs,
        elapsed_nanos(started),
        row_count_from_reader_inputs(&filter_project_filter_inputs),
        &filter_project_filter,
        &filter_project_projection,
    ));
    Ok(())
}

fn benchmark_source() -> Result<UniversalInputSource> {
    UniversalInputSource::from_dataset_uri(benchmark_source_uri()?)
}

fn benchmark_source_uri() -> Result<DatasetUri> {
    DatasetUri::new(FIXTURE_SOURCE_URI)
}

fn metric_threshold_predicate() -> Result<PredicateExpr> {
    Ok(PredicateExpr::Compare {
        column: column_ref("metric")?,
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(5),
    })
}

fn column_ref(name: &str) -> Result<ColumnRef> {
    ColumnRef::new(name)
}

fn segment(
    column: &str,
    id: &str,
    row_count: u64,
    encoding: EncodingKind,
) -> Result<EncodedSegment> {
    Ok(EncodedSegment::new(
        SegmentId::new(id)?,
        column_ref(column)?,
        LogicalDType::Int64,
        Nullability::Nullable,
        SegmentLayout::new(encoding, LayoutKind::Flat),
        SegmentStats::with_row_count(row_count),
    ))
}

fn prepared_predicate_batches() -> Result<Vec<VortexEncodedValuePredicateBatch>> {
    Ok(vec![
        VortexEncodedValuePredicateBatch::new(
            segment(
                "metric",
                "matrix-smoke.segment-1.metric",
                5,
                EncodingKind::Dictionary,
            )?,
            EncodedValueBatch::Dictionary {
                dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
                codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
            },
        ),
        VortexEncodedValuePredicateBatch::new(
            segment(
                "metric",
                "matrix-smoke.segment-2.metric",
                3,
                EncodingKind::RunLength,
            )?,
            EncodedValueBatch::RunLength {
                runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
            },
        ),
    ])
}

fn source_bound_filter_batches(
    source_uri: &DatasetUri,
) -> Result<Vec<VortexSourceBackedEncodedValuePredicateBatch>> {
    prepared_predicate_batches()?
        .into_iter()
        .enumerate()
        .map(|(index, batch)| {
            VortexSourceBackedEncodedValuePredicateBatch::new(
                source_uri.clone(),
                format!("split-{}", index + 1),
                batch,
            )
        })
        .collect()
}

fn prepared_projection_columns() -> Result<Vec<VortexPreparedEncodedProjectionColumn>> {
    Ok(vec![
        VortexPreparedEncodedProjectionColumn::new(
            segment(
                "metric",
                "matrix-smoke.segment-1.metric",
                5,
                EncodingKind::Dictionary,
            )?,
            EncodedValueBatch::Dictionary {
                dictionary: vec![Some(StatValue::Int64(10)), Some(StatValue::Int64(20))],
                codes: vec![Some(0), Some(1), Some(0), Some(1), Some(0)],
            },
        ),
        VortexPreparedEncodedProjectionColumn::new(
            segment(
                "metric",
                "matrix-smoke.segment-2.metric",
                3,
                EncodingKind::RunLength,
            )?,
            EncodedValueBatch::RunLength {
                runs: vec![EncodedValueRun::new(Some(StatValue::Int64(30)), 3)],
            },
        ),
    ])
}

fn source_bound_projection_columns(
    source_uri: &DatasetUri,
) -> Result<Vec<VortexSourceBackedEncodedProjectionColumn>> {
    prepared_projection_columns()?
        .into_iter()
        .enumerate()
        .map(|(index, column)| {
            VortexSourceBackedEncodedProjectionColumn::new(
                source_uri.clone(),
                format!("split-{}", index + 1),
                column,
            )
        })
        .collect()
}

fn reader_splits_for_lane(
    source_uri: &DatasetUri,
    lane: SourceBackedBenchmarkLane,
) -> Result<Vec<VortexReaderBackedSplitEvidence>> {
    let (encoding_label, row_counts) = match lane {
        SourceBackedBenchmarkLane::ReaderBackedConstant => ("vortex.constant", [4, 4]),
        SourceBackedBenchmarkLane::ReaderBackedDictionary => ("vortex.dict", [5, 3]),
        SourceBackedBenchmarkLane::ReaderBackedRunEnd => ("vortex.run_end", [3, 5]),
        _ => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "source-backed benchmark reader lane '{}' is not executable",
                lane.as_str()
            )));
        }
    };
    row_counts
        .iter()
        .enumerate()
        .map(|(index, row_count)| {
            VortexReaderBackedSplitEvidence::new(
                source_uri.clone(),
                format!("split-{}", index + 1),
                *row_count,
                "struct(metric=int64)",
                encoding_label,
                1,
                1,
            )
        })
        .collect()
}

fn reader_kernel_inputs_for_lane(
    source_uri: &DatasetUri,
    lane: SourceBackedBenchmarkLane,
) -> Result<Vec<VortexReaderGeneratedEncodedKernelInput>> {
    match lane {
        SourceBackedBenchmarkLane::ReaderBackedConstant => Ok(vec![
            reader_kernel_input(
                source_uri,
                "split-1",
                "matrix-smoke.reader.constant-1.metric",
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(7)),
                    row_count: 4,
                },
            )?,
            reader_kernel_input(
                source_uri,
                "split-2",
                "matrix-smoke.reader.constant-2.metric",
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(9)),
                    row_count: 4,
                },
            )?,
        ]),
        SourceBackedBenchmarkLane::ReaderBackedDictionary => Ok(vec![
            reader_kernel_input(
                source_uri,
                "split-1",
                "matrix-smoke.reader.dictionary-1.metric",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(7)), None],
                    codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                },
            )?,
            reader_kernel_input(
                source_uri,
                "split-2",
                "matrix-smoke.reader.dictionary-2.metric",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(9)), Some(StatValue::Int64(1))],
                    codes: vec![Some(0), Some(1), Some(0)],
                },
            )?,
        ]),
        SourceBackedBenchmarkLane::ReaderBackedRunEnd => Ok(vec![
            reader_kernel_input(
                source_uri,
                "split-1",
                "matrix-smoke.reader.run-end-1.metric",
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
                },
            )?,
            reader_kernel_input(
                source_uri,
                "split-2",
                "matrix-smoke.reader.run-end-2.metric",
                EncodedValueBatch::RunLength {
                    runs: vec![
                        EncodedValueRun::new(Some(StatValue::Int64(1)), 2),
                        EncodedValueRun::new(Some(StatValue::Int64(9)), 3),
                    ],
                },
            )?,
        ]),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "source-backed benchmark reader lane '{}' is not executable",
            lane.as_str()
        ))),
    }
}

fn reader_kernel_input(
    source_uri: &DatasetUri,
    split_ref: &str,
    segment_id: &str,
    values: EncodedValueBatch,
) -> Result<VortexReaderGeneratedEncodedKernelInput> {
    let row_count = values.row_count().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "reader benchmark fixture values require a row count".to_string(),
        )
    })?;
    VortexReaderGeneratedEncodedKernelInput::new(
        source_uri.clone(),
        split_ref,
        VortexEncodedValuePredicateBatch::new(
            segment("metric", segment_id, row_count, values.encoding_kind())?,
            values,
        ),
    )
}

fn prepared_split_refs() -> Vec<String> {
    vec![
        "prepared-batch-only.split-1".to_string(),
        "prepared-batch-only.split-2".to_string(),
    ]
}

fn source_bound_split_refs(
    batches: &[VortexSourceBackedEncodedValuePredicateBatch],
) -> Vec<String> {
    unique_strings(batches.iter().map(|batch| batch.split_ref.clone()))
}

fn source_bound_projection_split_refs(
    columns: &[VortexSourceBackedEncodedProjectionColumn],
) -> Vec<String> {
    unique_strings(columns.iter().map(|column| column.split_ref.clone()))
}

fn row_count_from_predicate_batches(batches: &[VortexEncodedValuePredicateBatch]) -> Option<u64> {
    sum_optional_counts(batches.iter().map(|batch| batch.values.row_count()))
}

fn row_count_from_projection_columns(
    columns: &[VortexPreparedEncodedProjectionColumn],
) -> Option<u64> {
    sum_optional_counts(columns.iter().map(|column| column.values.row_count()))
}

fn row_count_from_source_filter_batches(
    batches: &[VortexSourceBackedEncodedValuePredicateBatch],
) -> Option<u64> {
    sum_optional_counts(batches.iter().map(|batch| batch.batch.values.row_count()))
}

fn row_count_from_source_projection_columns(
    columns: &[VortexSourceBackedEncodedProjectionColumn],
) -> Option<u64> {
    sum_optional_counts(
        columns
            .iter()
            .map(|column| column.column.values.row_count()),
    )
}

fn row_count_from_reader_inputs(inputs: &[VortexReaderGeneratedEncodedKernelInput]) -> Option<u64> {
    sum_optional_counts(inputs.iter().map(|input| input.batch.values.row_count()))
}

fn sum_optional_counts(counts: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    counts
        .into_iter()
        .try_fold(0_u64, |total, count| total.checked_add(count?))
}

fn elapsed_nanos(started: Instant) -> u128 {
    started.elapsed().as_nanos().max(1)
}

fn ensure_measurement_report_ok(row_id: &str, has_errors: bool) -> Result<()> {
    if has_errors {
        return Err(ShardLoomError::InvalidOperation(format!(
            "source-backed benchmark measurement row '{row_id}' failed to produce certified fixture evidence"
        )));
    }
    Ok(())
}

fn measured_prepared_filter_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexGeneralizedEncodedFilterExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::Filter,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report.selected_row_count,
        &[&report.execution_certificate],
        &[&report.native_io_certificate],
        None,
    )
}

fn measured_prepared_projection_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::Projection,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report.projected_row_count,
        &[&report.execution_certificate],
        &[&report.native_io_certificate],
        None,
    )
}

fn measured_prepared_filter_project_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    filter: &VortexGeneralizedEncodedFilterExecutionReport,
    projection: &VortexGeneralizedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::FilterProject,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        projection.projected_row_count.or(filter.selected_row_count),
        &[
            &filter.execution_certificate,
            &projection.execution_certificate,
        ],
        &[
            &filter.native_io_certificate,
            &projection.native_io_certificate,
        ],
        None,
    )
}

fn measured_source_filter_row(
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexSourceBackedEncodedFilterExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        SourceBackedBenchmarkLane::SourceBoundEncoded,
        SourceBackedBenchmarkOperation::Filter,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report.prepared_execution.selected_row_count,
        &[&report.prepared_execution.execution_certificate],
        &[&report.prepared_execution.native_io_certificate],
        None,
    )
}

fn measured_source_projection_row(
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexSourceBackedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        SourceBackedBenchmarkLane::SourceBoundEncoded,
        SourceBackedBenchmarkOperation::Projection,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report.prepared_execution.projected_row_count,
        &[&report.prepared_execution.execution_certificate],
        &[&report.prepared_execution.native_io_certificate],
        None,
    )
}

fn measured_source_filter_project_row(
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    filter: &VortexSourceBackedEncodedFilterExecutionReport,
    projection: &VortexSourceBackedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        SourceBackedBenchmarkLane::SourceBoundEncoded,
        SourceBackedBenchmarkOperation::FilterProject,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        projection
            .prepared_execution
            .projected_row_count
            .or(filter.prepared_execution.selected_row_count),
        &[
            &filter.prepared_execution.execution_certificate,
            &projection.prepared_execution.execution_certificate,
        ],
        &[
            &filter.prepared_execution.native_io_certificate,
            &projection.prepared_execution.native_io_certificate,
        ],
        None,
    )
}

fn measured_reader_filter_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexReaderBackedEncodedFilterExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::Filter,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report
            .source_execution
            .prepared_execution
            .selected_row_count,
        &[&report
            .source_execution
            .prepared_execution
            .execution_certificate],
        &[&report
            .source_execution
            .prepared_execution
            .native_io_certificate],
        Some(ProviderOverride {
            kind: report.provider_kind,
            api_surface: report.provider_api_surface,
            version: report.provider_boundary.provider_version,
        }),
    )
}

fn measured_reader_projection_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    report: &VortexReaderBackedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::Projection,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        report
            .source_execution
            .prepared_execution
            .projected_row_count,
        &[&report
            .source_execution
            .prepared_execution
            .execution_certificate],
        &[&report
            .source_execution
            .prepared_execution
            .native_io_certificate],
        Some(ProviderOverride {
            kind: report.provider_kind,
            api_surface: report.provider_api_surface,
            version: report.provider_boundary.provider_version,
        }),
    )
}

fn measured_reader_filter_project_row(
    lane: SourceBackedBenchmarkLane,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    filter: &VortexReaderBackedEncodedFilterExecutionReport,
    projection: &VortexReaderBackedEncodedProjectionExecutionReport,
) -> SourceBackedBenchmarkMeasuredRow {
    measured_row_from_parts(
        lane,
        SourceBackedBenchmarkOperation::FilterProject,
        source_uri,
        split_refs,
        elapsed_nanos,
        row_count,
        projection
            .source_execution
            .prepared_execution
            .projected_row_count
            .or(filter
                .source_execution
                .prepared_execution
                .selected_row_count),
        &[
            &filter
                .source_execution
                .prepared_execution
                .execution_certificate,
            &projection
                .source_execution
                .prepared_execution
                .execution_certificate,
        ],
        &[
            &filter
                .source_execution
                .prepared_execution
                .native_io_certificate,
            &projection
                .source_execution
                .prepared_execution
                .native_io_certificate,
        ],
        Some(ProviderOverride {
            kind: projection.provider_kind,
            api_surface: projection.provider_api_surface,
            version: projection.provider_boundary.provider_version,
        }),
    )
}

#[derive(Debug, Clone, Copy)]
struct ProviderOverride {
    kind: &'static str,
    api_surface: &'static str,
    version: &'static str,
}

#[allow(clippy::too_many_arguments)]
fn measured_row_from_parts(
    lane: SourceBackedBenchmarkLane,
    operation: SourceBackedBenchmarkOperation,
    source_uri: &DatasetUri,
    split_refs: Vec<String>,
    elapsed_nanos: u128,
    row_count: Option<u64>,
    selected_or_projected_count: Option<u64>,
    execution_certificates: &[&ExecutionCertificate],
    native_io_certificates: &[&NativeIoCertificate],
    provider_override: Option<ProviderOverride>,
) -> SourceBackedBenchmarkMeasuredRow {
    let primary_certificate = execution_certificates
        .first()
        .expect("source-backed benchmark measurement requires execution certificate");
    let provider_kind = provider_override.map_or_else(
        || {
            primary_certificate
                .execution_provider_kind
                .as_str()
                .to_string()
        },
        |provider| provider.kind.to_string(),
    );
    let provider_api_surface = provider_override.map_or_else(
        || {
            primary_certificate
                .provider_api_surface
                .clone()
                .unwrap_or_else(|| "unknown".to_string())
        },
        |provider| provider.api_surface.to_string(),
    );
    let provider_version = provider_override.map_or_else(
        || {
            primary_certificate
                .provider_version
                .clone()
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
        },
        |provider| provider.version.to_string(),
    );
    let row_id = row_id(lane, operation);
    SourceBackedBenchmarkMeasuredRow {
        row_id,
        lane,
        operation,
        benchmark_row_ref: format!("benchmarks/source_backed/matrix_smoke/{row_id}"),
        elapsed_nanos,
        row_count,
        selected_or_projected_count,
        provider_kind,
        provider_api_surface,
        provider_version,
        source_refs: vec![source_uri.as_str().to_string()],
        split_refs,
        representation_transitions: native_io_transition_refs(native_io_certificates),
        residual_executor: "none".to_string(),
        execution_certificate_refs: execution_certificate_refs(execution_certificates),
        native_io_certificate_refs: native_io_certificate_refs(native_io_certificates),
        native_io_certificate_path_refs: native_io_certificate_path_refs(native_io_certificates),
        correctness_refs: correctness_refs(execution_certificates),
        benchmark_constitution: BENCHMARK_CONSTITUTION.to_string(),
        reproducibility_ref: REPRODUCIBILITY_REF.to_string(),
        external_engine_invoked: false,
        fallback_attempted: false,
        performance_claim_allowed: false,
    }
}

fn execution_certificate_refs(certificates: &[&ExecutionCertificate]) -> Vec<String> {
    unique_strings(
        certificates
            .iter()
            .map(|certificate| certificate.certificate_id.clone()),
    )
}

fn native_io_certificate_refs(certificates: &[&NativeIoCertificate]) -> Vec<String> {
    unique_strings(
        certificates
            .iter()
            .map(|certificate| certificate.certificate_id.clone()),
    )
}

fn native_io_certificate_path_refs(certificates: &[&NativeIoCertificate]) -> Vec<String> {
    unique_strings(
        certificates
            .iter()
            .map(|certificate| certificate.path_id.clone()),
    )
}

fn native_io_transition_refs(certificates: &[&NativeIoCertificate]) -> Vec<String> {
    unique_strings(
        certificates
            .iter()
            .flat_map(|certificate| certificate.representation_transitions.iter())
            .map(shardloom_core::NativeIoRepresentationTransition::transition_label),
    )
}

fn correctness_refs(certificates: &[&ExecutionCertificate]) -> Vec<String> {
    unique_strings(certificates.iter().map(|certificate| {
        certificate
            .correctness_fixture_id
            .clone()
            .unwrap_or_else(|| certificate.certificate_id.clone())
    }))
}

fn unique_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_backed_matrix_covers_required_reader_and_blocked_rows() {
        let report = plan_source_backed_benchmark_matrix();

        assert!(report.covers_required_plan_matrix());
        assert_eq!(report.executable_row_count, 15);
        assert_eq!(report.blocked_row_count, 4);
        assert!(report.benchmark_rows_required);
    }

    #[test]
    fn required_matrix_coverage_rejects_blocked_required_lane_operations() {
        let mut report = plan_source_backed_benchmark_matrix();
        let row = report
            .rows
            .iter_mut()
            .find(|row| {
                row.lane == SourceBackedBenchmarkLane::SourceBoundEncoded
                    && row.operation == SourceBackedBenchmarkOperation::FilterProject
            })
            .expect("required lane operation");
        row.status = SourceBackedBenchmarkRowStatus::BlockedUnsupported;

        assert!(!report.covers_required_plan_matrix());
    }

    #[test]
    fn executable_rows_require_claim_grade_evidence_before_claims() {
        let report = plan_source_backed_benchmark_matrix();

        assert!(report.executable_rows_require_all_claim_grade_evidence());
        assert_eq!(report.measured_row_count, 0);
        assert!(report.measured_rows.is_empty());
        assert!(!report.measured_benchmark_rows_present);
        assert!(!report.source_backed_claim_closeout_allowed);
        assert!(!report.benchmark_execution_performed);
    }

    #[test]
    fn blocked_rows_have_deterministic_no_fallback_blockers() {
        let report = plan_source_backed_benchmark_matrix();

        assert!(report.blocked_rows_have_deterministic_no_fallback_blockers());
        assert!(report.all_rows_fallback_free());
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn measured_source_backed_smoke_populates_all_executable_rows() {
        let report = measure_source_backed_benchmark_matrix_smoke().expect("measured report");

        assert_eq!(report.executable_row_count, 15);
        assert_eq!(report.blocked_row_count, 4);
        assert_eq!(report.measured_row_count, 15);
        assert!(report.measured_benchmark_rows_present);
        assert!(report.benchmark_execution_performed);
        assert_eq!(
            report.reproducibility_manifest_ref,
            Some("benchmarks/source_backed/source_backed_benchmark_matrix_smoke.v1")
        );
        assert!(report.measured_rows_cover_executable_matrix());
        assert!(report.measured_rows_have_claim_grade_fields());
        assert!(report.measured_rows_remain_fixture_evidence_only());
        assert!(report.all_rows_fallback_free());
        assert!(!report.source_backed_claim_closeout_allowed);
        assert!(
            report
                .rows
                .iter()
                .filter(|row| !row.lane.is_blocked())
                .all(
                    |row| row.status == SourceBackedBenchmarkRowStatus::MeasuredFixture
                        && row.status.is_measured()
                        && row.benchmark_row_present
                        && row.correctness_ref_present
                        && row.residual_executor == "none"
                        && !row.performance_claim_allowed
                )
        );
        assert!(
            report
                .rows
                .iter()
                .filter(|row| row.lane.is_blocked())
                .all(|row| row.status == SourceBackedBenchmarkRowStatus::BlockedUnsupported)
        );
    }

    #[test]
    fn measured_source_backed_rows_preserve_provider_and_certificate_refs() {
        let report = measure_source_backed_benchmark_matrix_smoke().expect("measured report");

        for expected in [
            "prepared_batch_only.filter",
            "source_bound_encoded.filter_project",
            "reader_backed_dictionary.projection",
            "reader_backed_run_end.filter_project",
        ] {
            let row = report
                .measured_rows
                .iter()
                .find(|row| row.row_id == expected)
                .unwrap_or_else(|| panic!("missing measured row {expected}"));
            assert!(row.elapsed_nanos > 0);
            assert_eq!(row.benchmark_constitution, BENCHMARK_CONSTITUTION);
            assert_eq!(row.reproducibility_ref, REPRODUCIBILITY_REF);
            assert!(row.benchmark_row_ref.ends_with(expected));
            assert!(row.row_count.unwrap_or_default() > 0);
            assert!(row.selected_or_projected_count.unwrap_or_default() > 0);
            assert!(!row.execution_certificate_refs.is_empty());
            assert!(!row.native_io_certificate_refs.is_empty());
            assert!(!row.native_io_certificate_path_refs.is_empty());
            assert!(!row.correctness_refs.is_empty());
            assert!(!row.representation_transitions.is_empty());
            assert!(!row.external_engine_invoked);
            assert!(!row.fallback_attempted);
            assert!(!row.performance_claim_allowed);
        }

        let reader_row = report
            .measured_rows
            .iter()
            .find(|row| row.row_id == "reader_backed_dictionary.projection")
            .expect("reader row");
        assert_eq!(reader_row.provider_kind, "vortex_scan");
        assert_eq!(
            reader_row.provider_api_surface,
            "VortexFile::scan.into_array_iter"
        );
        assert_eq!(reader_row.provider_version, "0.73");
        assert_eq!(
            reader_row.split_refs,
            vec!["split-1".to_string(), "split-2".to_string()]
        );
    }
}
