//! CG-15 CPU operator specialization report contracts.
//!
//! This module records specialization candidates and evidence gates only. It
//! does not detect host CPU features, dispatch SIMD kernels, run operators, or
//! invoke external fallback engines.

use crate::{
    BenchmarkEvidenceState, Diagnostic, DiagnosticSeverity, KernelKind, PhysicalOperatorKind,
};

const CPU_SPECIALIZATION_SCHEMA_VERSION: &str = "shardloom.cpu_operator_specialization.v1";
const CPU_SPECIALIZATION_REPORT_ID: &str = "cg15.cpu-operator-specialization";

/// Report-level status for CPU operator specialization planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuSpecializationStatus {
    /// The report defines planned specialization gates without enabling them.
    ReportOnlyPlanned,
    /// Correctness evidence is required before native specialization can run.
    BlockedByMissingCorrectnessEvidence,
    /// Benchmark evidence is required before performance claims can be emitted.
    BlockedByMissingBenchmarkEvidence,
    /// The specialization target is not supported.
    Unsupported,
}

impl CpuSpecializationStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::BlockedByMissingCorrectnessEvidence => "blocked_by_missing_correctness_evidence",
            Self::BlockedByMissingBenchmarkEvidence => "blocked_by_missing_benchmark_evidence",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether this status should fail a report command.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// Instruction or layout class targeted by a future native CPU specialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuInstructionClass {
    /// Portable scalar/native code path used as the deterministic baseline.
    ScalarPortable,
    /// Portable vectorized loop shape independent of a specific CPU extension.
    SimdPortable,
    /// AVX2 candidate, guarded by future CPU feature checks.
    Avx2Candidate,
    /// AVX-512 candidate, guarded by future CPU feature checks.
    Avx512Candidate,
    /// ARM NEON candidate, guarded by future CPU feature checks.
    NeonCandidate,
    /// Cache-tiled or cache-local access pattern.
    CacheTiled,
    /// Branch-reduced control flow candidate.
    BranchReduced,
    /// Dictionary-encoded layout candidate.
    DictionaryAware,
    /// Run-length or run-end encoded layout candidate.
    RunAware,
    /// Bit-packed layout candidate.
    BitPacked,
    /// Selection-vector-preserving candidate.
    SelectionVectorAware,
}

impl CpuInstructionClass {
    /// Stable machine-readable instruction class label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ScalarPortable => "scalar_portable",
            Self::SimdPortable => "simd_portable",
            Self::Avx2Candidate => "avx2_candidate",
            Self::Avx512Candidate => "avx512_candidate",
            Self::NeonCandidate => "neon_candidate",
            Self::CacheTiled => "cache_tiled",
            Self::BranchReduced => "branch_reduced",
            Self::DictionaryAware => "dictionary_aware",
            Self::RunAware => "run_aware",
            Self::BitPacked => "bit_packed",
            Self::SelectionVectorAware => "selection_vector_aware",
        }
    }

    /// Returns whether the class is a SIMD-family candidate.
    #[must_use]
    pub const fn is_simd_candidate(&self) -> bool {
        matches!(
            self,
            Self::SimdPortable | Self::Avx2Candidate | Self::Avx512Candidate | Self::NeonCandidate
        )
    }

    /// Returns whether the class is cache-aware.
    #[must_use]
    pub const fn is_cache_aware(&self) -> bool {
        matches!(self, Self::CacheTiled)
    }

    /// Returns whether the class depends on encoded layout properties.
    #[must_use]
    pub const fn is_encoded_layout_aware(&self) -> bool {
        matches!(
            self,
            Self::DictionaryAware | Self::RunAware | Self::BitPacked | Self::SelectionVectorAware
        )
    }
}

/// A report-only CPU specialization candidate for one operator/kernel pair.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuOperatorSpecializationEntry {
    /// Operator targeted by the specialization candidate.
    pub operator_kind: PhysicalOperatorKind,
    /// Kernel kind the candidate would specialize.
    pub kernel_kind: KernelKind,
    /// Candidate instruction/layout classes for future implementation.
    pub instruction_classes: Vec<CpuInstructionClass>,
    /// Whether this entry is a specialization candidate.
    pub specialization_candidate: bool,
    /// Correctness evidence state for the candidate.
    pub correctness_evidence: BenchmarkEvidenceState,
    /// Benchmark evidence state for the candidate.
    pub benchmark_evidence: BenchmarkEvidenceState,
    /// CPU feature guards must exist before architecture-specific dispatch.
    pub requires_cpu_feature_guard: bool,
    /// A portable native baseline must remain available for deterministic use.
    pub portable_native_baseline_required: bool,
    /// Dispatch decisions must be deterministic and diagnosable.
    pub deterministic_dispatch_required: bool,
}

impl CpuOperatorSpecializationEntry {
    /// Creates a planned specialization candidate with missing evidence gates.
    #[must_use]
    pub fn planned(
        operator_kind: PhysicalOperatorKind,
        kernel_kind: KernelKind,
        instruction_classes: Vec<CpuInstructionClass>,
    ) -> Self {
        Self {
            operator_kind,
            kernel_kind,
            instruction_classes,
            specialization_candidate: true,
            correctness_evidence: BenchmarkEvidenceState::Missing,
            benchmark_evidence: BenchmarkEvidenceState::Missing,
            requires_cpu_feature_guard: true,
            portable_native_baseline_required: true,
            deterministic_dispatch_required: true,
        }
    }

    /// Stable comma-separated class labels for CLI reporting.
    #[must_use]
    pub fn instruction_class_labels(&self) -> String {
        self.instruction_classes
            .iter()
            .map(CpuInstructionClass::as_str)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether the candidate has a SIMD-family class.
    #[must_use]
    pub fn has_simd_candidate(&self) -> bool {
        self.instruction_classes
            .iter()
            .any(CpuInstructionClass::is_simd_candidate)
    }

    /// Returns whether the candidate has a cache-aware class.
    #[must_use]
    pub fn has_cache_aware_candidate(&self) -> bool {
        self.instruction_classes
            .iter()
            .any(CpuInstructionClass::is_cache_aware)
    }

    /// Returns whether the candidate is aware of encoded layout properties.
    #[must_use]
    pub fn has_encoded_layout_aware_candidate(&self) -> bool {
        self.instruction_classes
            .iter()
            .any(CpuInstructionClass::is_encoded_layout_aware)
    }
}

/// CG-15 report-only CPU operator specialization plan.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuOperatorSpecializationReport {
    /// Stable schema version for machine-readable report consumers.
    pub schema_version: &'static str,
    /// Stable report identifier.
    pub report_id: String,
    /// Report status.
    pub status: CpuSpecializationStatus,
    /// Operator/kernel specialization candidates.
    pub entries: Vec<CpuOperatorSpecializationEntry>,
    /// Correctness evidence is required before specialized execution.
    pub correctness_evidence_required: bool,
    /// Benchmark evidence is required before performance or superiority claims.
    pub benchmark_evidence_required: bool,
    /// Architecture-specific paths require CPU feature guards.
    pub cpu_feature_guard_required: bool,
    /// Portable native baseline remains required for deterministic execution.
    pub portable_native_baseline_required: bool,
    /// Dispatch decisions must be stable and explainable.
    pub deterministic_dispatch_required: bool,
    /// This report does not inspect the host CPU.
    pub host_cpu_probe: bool,
    /// This report does not implement runtime dispatch.
    pub runtime_dispatch_implemented: bool,
    /// This report does not require unsafe code.
    pub unsafe_code_required: bool,
    /// GPU acceleration is not required for CG-15.
    pub gpu_required: bool,
    /// FPGA acceleration is not required for CG-15.
    pub fpga_required: bool,
    /// No runtime operator execution occurs.
    pub runtime_execution: bool,
    /// No data reads occur.
    pub data_read: bool,
    /// No decode occurs.
    pub data_decoded: bool,
    /// No materialization occurs.
    pub data_materialized: bool,
    /// No row reads occur.
    pub row_read: bool,
    /// No Arrow conversion occurs.
    pub arrow_converted: bool,
    /// No object-store IO occurs.
    pub object_store_io: bool,
    /// No write IO occurs.
    pub write_io: bool,
    /// No spill IO occurs.
    pub spill_io_performed: bool,
    /// No external engine executes.
    pub external_engine_execution: bool,
    /// Fallback execution remains disabled.
    pub fallback_execution_allowed: bool,
    /// Fallback execution was not attempted.
    pub fallback_attempted: bool,
    /// Production/performance claims remain disabled.
    pub production_claim_allowed: bool,
    /// Report diagnostics.
    pub diagnostics: Vec<Diagnostic>,
}

impl CpuOperatorSpecializationReport {
    /// Creates the CG-15 report-only foundation for CPU specialization.
    #[must_use]
    pub fn cg15_foundation() -> Self {
        Self {
            schema_version: CPU_SPECIALIZATION_SCHEMA_VERSION,
            report_id: CPU_SPECIALIZATION_REPORT_ID.to_string(),
            status: CpuSpecializationStatus::ReportOnlyPlanned,
            entries: vec![
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::Filter,
                    KernelKind::Encoded,
                    vec![
                        CpuInstructionClass::SimdPortable,
                        CpuInstructionClass::Avx2Candidate,
                        CpuInstructionClass::BranchReduced,
                        CpuInstructionClass::DictionaryAware,
                        CpuInstructionClass::SelectionVectorAware,
                    ],
                ),
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::Project,
                    KernelKind::Encoded,
                    vec![
                        CpuInstructionClass::SimdPortable,
                        CpuInstructionClass::CacheTiled,
                        CpuInstructionClass::SelectionVectorAware,
                        CpuInstructionClass::BitPacked,
                    ],
                ),
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::CountAggregate,
                    KernelKind::Encoded,
                    vec![
                        CpuInstructionClass::SimdPortable,
                        CpuInstructionClass::Avx2Candidate,
                        CpuInstructionClass::RunAware,
                        CpuInstructionClass::BitPacked,
                    ],
                ),
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::Aggregate,
                    KernelKind::PartialDecode,
                    vec![
                        CpuInstructionClass::ScalarPortable,
                        CpuInstructionClass::SimdPortable,
                        CpuInstructionClass::CacheTiled,
                    ],
                ),
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::Sort,
                    KernelKind::PartialDecode,
                    vec![
                        CpuInstructionClass::ScalarPortable,
                        CpuInstructionClass::CacheTiled,
                        CpuInstructionClass::BranchReduced,
                    ],
                ),
                CpuOperatorSpecializationEntry::planned(
                    PhysicalOperatorKind::Join,
                    KernelKind::PartialDecode,
                    vec![
                        CpuInstructionClass::ScalarPortable,
                        CpuInstructionClass::CacheTiled,
                        CpuInstructionClass::BranchReduced,
                    ],
                ),
            ],
            correctness_evidence_required: true,
            benchmark_evidence_required: true,
            cpu_feature_guard_required: true,
            portable_native_baseline_required: true,
            deterministic_dispatch_required: true,
            host_cpu_probe: false,
            runtime_dispatch_implemented: false,
            unsafe_code_required: false,
            gpu_required: false,
            fpga_required: false,
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of specialization entries in the report.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of entries marked as specialization candidates.
    #[must_use]
    pub fn specialization_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.specialization_candidate)
            .count()
    }

    /// Number of entries with SIMD-family candidates.
    #[must_use]
    pub fn simd_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.has_simd_candidate())
            .count()
    }

    /// Number of entries with cache-aware candidates.
    #[must_use]
    pub fn cache_aware_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.has_cache_aware_candidate())
            .count()
    }

    /// Number of entries with encoded-layout-aware candidates.
    #[must_use]
    pub fn encoded_layout_aware_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.has_encoded_layout_aware_candidate())
            .count()
    }

    /// Stable comma-separated operator order for CLI reporting.
    #[must_use]
    pub fn operator_order(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.operator_kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated kernel-kind order for CLI reporting.
    #[must_use]
    pub fn kernel_kind_order(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.kernel_kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether this report avoids all side effects and execution.
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.host_cpu_probe
            && !self.runtime_dispatch_implemented
            && !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    /// Returns whether the report contains errors.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    /// Human-readable report summary.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "cpu operator specialization plan\nschema_version: {}\nreport: {}\nstatus: {}\noperators: {}\nsimd candidates: {}\ncache-aware candidates: {}\nencoded-layout-aware candidates: {}\nhost CPU probe: disabled\nruntime dispatch: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.entry_count(),
            self.simd_candidate_count(),
            self.cache_aware_candidate_count(),
            self.encoded_layout_aware_candidate_count(),
        )
    }
}

/// Produces the CG-15 CPU operator specialization report.
#[must_use]
pub fn plan_cpu_operator_specialization() -> CpuOperatorSpecializationReport {
    CpuOperatorSpecializationReport::cg15_foundation()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_specialization_foundation_is_report_only() {
        let report = CpuOperatorSpecializationReport::cg15_foundation();

        assert_eq!(report.status, CpuSpecializationStatus::ReportOnlyPlanned);
        assert_eq!(report.entry_count(), 6);
        assert_eq!(report.specialization_candidate_count(), 6);
        assert_eq!(report.simd_candidate_count(), 4);
        assert_eq!(report.cache_aware_candidate_count(), 4);
        assert_eq!(report.encoded_layout_aware_candidate_count(), 3);
        assert_eq!(
            report.operator_order(),
            "filter,project,count_aggregate,aggregate,sort,join"
        );
        assert_eq!(
            report.kernel_kind_order(),
            "encoded,encoded,encoded,partial_decode,partial_decode,partial_decode"
        );
        assert!(report.correctness_evidence_required);
        assert!(report.benchmark_evidence_required);
        assert!(report.cpu_feature_guard_required);
        assert!(report.portable_native_baseline_required);
        assert!(report.deterministic_dispatch_required);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.host_cpu_probe);
        assert!(!report.runtime_dispatch_implemented);
        assert!(!report.unsafe_code_required);
        assert!(!report.gpu_required);
        assert!(!report.fpga_required);
        assert!(!report.runtime_execution);
        assert!(!report.external_engine_execution);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }

    #[test]
    fn instruction_class_groups_are_stable() {
        assert!(CpuInstructionClass::Avx2Candidate.is_simd_candidate());
        assert!(CpuInstructionClass::CacheTiled.is_cache_aware());
        assert!(CpuInstructionClass::DictionaryAware.is_encoded_layout_aware());
        assert!(!CpuInstructionClass::ScalarPortable.is_simd_candidate());
    }
}
