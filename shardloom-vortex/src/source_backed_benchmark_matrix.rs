//! Source-backed correctness and benchmark matrix for current encoded paths.
//!
//! The matrix is populated as a claim gate: it names the rows and required
//! evidence before source-backed performance claims, but it does not execute
//! benchmarks or invoke external engines.

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
    BlockedUnsupported,
}

impl SourceBackedBenchmarkRowStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceRequired => "evidence_required",
            Self::BenchmarkRowMissing => "benchmark_row_missing",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }

    #[must_use]
    pub const fn claim_ready(self) -> bool {
        false
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SourceBackedBenchmarkMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<SourceBackedBenchmarkMatrixRow>,
    pub executable_row_count: usize,
    pub blocked_row_count: usize,
    pub benchmark_rows_required: bool,
    pub measured_benchmark_rows_present: bool,
    pub correctness_refs_required: bool,
    pub rust_performance_profile_required: bool,
    pub source_backed_claim_closeout_allowed: bool,
    pub benchmark_execution_performed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl SourceBackedBenchmarkMatrixReport {
    #[must_use]
    pub fn current() -> Self {
        let rows = source_backed_matrix_rows();
        let executable_row_count = rows.iter().filter(|row| !row.lane.is_blocked()).count();
        let blocked_row_count = rows.len() - executable_row_count;
        Self {
            schema_version: "shardloom.source_backed_benchmark_matrix.v1",
            report_id: "priority_2_7.source_backed_correctness_benchmark_matrix",
            rows,
            executable_row_count,
            blocked_row_count,
            benchmark_rows_required: true,
            measured_benchmark_rows_present: false,
            correctness_refs_required: true,
            rust_performance_profile_required: true,
            source_backed_claim_closeout_allowed: false,
            benchmark_execution_performed: false,
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
            && !self.external_engine_invoked
            && !self.fallback_attempted
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
}
