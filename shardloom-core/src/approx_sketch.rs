//! Approximate aggregate and sketch function certification contracts.
//!
//! This module is report-only. It names the CG-20 approximate aggregate lane,
//! sketch-state prerequisites, encoded-aware update strategies, and evidence
//! gates that must exist before `ShardLoom` can claim native approximate
//! aggregate support.

use std::fmt::Write as _;

use crate::{Diagnostic, DiagnosticSeverity};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproxSketchFunctionSurface {
    FunctionCoverageMatrixEntry,
    RfcSequencingContract,
    CanonicalApproxCountDistinct,
    AliasApproxDistinct,
    AliasApproxNUnique,
    UngroupedApproxDistinctExecution,
    GroupedApproxDistinctExecution,
    PartialSketchConstruction,
    AssociativeSketchMerge,
    SketchSerialization,
    SketchDeserialization,
    SketchVersionHashSeedMetadata,
    ErrorBoundsConfidenceModel,
    NullStringTemporalValueSemantics,
    DictionaryEncodedStrategy,
    RunLengthEncodedStrategy,
    SelectionVectorValidityStrategy,
    PartialDecodeMaterializationBoundary,
    ExactReferenceFixtureComparison,
    BenchmarkCertificateNativeIoCloseout,
}

impl ApproxSketchFunctionSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FunctionCoverageMatrixEntry => "function_coverage_matrix_entry",
            Self::RfcSequencingContract => "rfc_sequencing_contract",
            Self::CanonicalApproxCountDistinct => "canonical_approx_count_distinct",
            Self::AliasApproxDistinct => "alias_approx_distinct",
            Self::AliasApproxNUnique => "alias_approx_n_unique",
            Self::UngroupedApproxDistinctExecution => "ungrouped_approx_distinct_execution",
            Self::GroupedApproxDistinctExecution => "grouped_approx_distinct_execution",
            Self::PartialSketchConstruction => "partial_sketch_construction",
            Self::AssociativeSketchMerge => "associative_sketch_merge",
            Self::SketchSerialization => "sketch_serialization",
            Self::SketchDeserialization => "sketch_deserialization",
            Self::SketchVersionHashSeedMetadata => "sketch_version_hash_seed_metadata",
            Self::ErrorBoundsConfidenceModel => "error_bounds_confidence_model",
            Self::NullStringTemporalValueSemantics => "null_string_temporal_value_semantics",
            Self::DictionaryEncodedStrategy => "dictionary_encoded_strategy",
            Self::RunLengthEncodedStrategy => "run_length_encoded_strategy",
            Self::SelectionVectorValidityStrategy => "selection_vector_validity_strategy",
            Self::PartialDecodeMaterializationBoundary => "partial_decode_materialization_boundary",
            Self::ExactReferenceFixtureComparison => "exact_reference_fixture_comparison",
            Self::BenchmarkCertificateNativeIoCloseout => {
                "benchmark_certificate_native_io_closeout"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproxSketchFunctionStatus {
    ExistingContractEvidence,
    BlockedUntilCertified,
}

impl ApproxSketchFunctionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingContractEvidence => "existing_contract_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }

    #[must_use]
    pub const fn is_existing_evidence(&self) -> bool {
        matches!(self, Self::ExistingContractEvidence)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ApproxSketchFunctionGateEntry {
    pub surface: ApproxSketchFunctionSurface,
    pub status: ApproxSketchFunctionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub requires_function_registry: bool,
    pub requires_aggregate_state: bool,
    pub requires_sketch_serialization: bool,
    pub requires_stable_hash_seed_policy: bool,
    pub requires_error_model: bool,
    pub requires_exact_reference: bool,
    pub requires_encoded_strategy: bool,
    pub requires_execution_certificate: bool,
    pub requires_native_io_certificate: bool,
    pub requires_benchmark_evidence: bool,
    pub runtime_allowed: bool,
    pub external_dependency_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
}

impl ApproxSketchFunctionGateEntry {
    #[must_use]
    pub const fn existing(
        surface: ApproxSketchFunctionSurface,
        existing_report_ref: &'static str,
    ) -> Self {
        Self {
            surface,
            status: ApproxSketchFunctionStatus::ExistingContractEvidence,
            existing_report_ref: Some(existing_report_ref),
            requires_function_registry: false,
            requires_aggregate_state: false,
            requires_sketch_serialization: false,
            requires_stable_hash_seed_policy: false,
            requires_error_model: false,
            requires_exact_reference: false,
            requires_encoded_strategy: false,
            requires_execution_certificate: false,
            requires_native_io_certificate: false,
            requires_benchmark_evidence: false,
            runtime_allowed: false,
            external_dependency_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn blocked(surface: ApproxSketchFunctionSurface) -> Self {
        Self {
            surface,
            status: ApproxSketchFunctionStatus::BlockedUntilCertified,
            existing_report_ref: None,
            requires_function_registry: true,
            requires_aggregate_state: true,
            requires_sketch_serialization: true,
            requires_stable_hash_seed_policy: true,
            requires_error_model: true,
            requires_exact_reference: true,
            requires_encoded_strategy: true,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_benchmark_evidence: true,
            runtime_allowed: false,
            external_dependency_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_allowed
            && !self.external_dependency_allowed
            && !self.external_engine_invoked
            && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ApproxSketchFunctionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub canonical_function_name: &'static str,
    pub alias_names: Vec<&'static str>,
    pub value_handling_contracts: Vec<&'static str>,
    pub entries: Vec<ApproxSketchFunctionGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_function_coverage_matrix_entry_present: bool,
    pub existing_rfc_sequencing_contract_present: bool,
    pub function_registry_entry_allowed: bool,
    pub sketch_state_runtime_allowed: bool,
    pub sketch_merge_runtime_allowed: bool,
    pub sketch_serialization_runtime_allowed: bool,
    pub grouped_aggregate_runtime_allowed: bool,
    pub encoded_dictionary_strategy_allowed: bool,
    pub encoded_run_length_strategy_allowed: bool,
    pub selection_vector_strategy_allowed: bool,
    pub partial_decode_execution_allowed: bool,
    pub materialization_without_report_allowed: bool,
    pub generic_sketch_dependency_allowed: bool,
    pub exact_claim_allowed: bool,
    pub approximate_function_claim_allowed: bool,
    pub function_registry_required: bool,
    pub aggregate_state_required: bool,
    pub sketch_serialization_required: bool,
    pub stable_hash_seed_policy_required: bool,
    pub error_bounds_required: bool,
    pub confidence_model_required: bool,
    pub exact_reference_fixtures_required: bool,
    pub encoded_dictionary_strategy_required: bool,
    pub encoded_run_length_strategy_required: bool,
    pub selection_vector_strategy_required: bool,
    pub partial_decode_materialization_boundary_required: bool,
    pub correctness_evidence_required: bool,
    pub benchmark_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl ApproxSketchFunctionGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.approx_sketch_function_gate.v1",
            report_id: "cg20.approx_sketch_function_gate",
            canonical_function_name: "approx_count_distinct",
            alias_names: vec!["approx_distinct", "approx_n_unique"],
            value_handling_contracts: approx_sketch_value_handling_contracts(),
            entries: approx_sketch_function_entries(),
            existing_report_refs: approx_sketch_existing_report_refs(),
            existing_function_coverage_matrix_entry_present: true,
            existing_rfc_sequencing_contract_present: true,
            function_registry_entry_allowed: false,
            sketch_state_runtime_allowed: false,
            sketch_merge_runtime_allowed: false,
            sketch_serialization_runtime_allowed: false,
            grouped_aggregate_runtime_allowed: false,
            encoded_dictionary_strategy_allowed: false,
            encoded_run_length_strategy_allowed: false,
            selection_vector_strategy_allowed: false,
            partial_decode_execution_allowed: false,
            materialization_without_report_allowed: false,
            generic_sketch_dependency_allowed: false,
            exact_claim_allowed: false,
            approximate_function_claim_allowed: false,
            function_registry_required: true,
            aggregate_state_required: true,
            sketch_serialization_required: true,
            stable_hash_seed_policy_required: true,
            error_bounds_required: true,
            confidence_model_required: true,
            exact_reference_fixtures_required: true,
            encoded_dictionary_strategy_required: true,
            encoded_run_length_strategy_required: true,
            selection_vector_strategy_required: true,
            partial_decode_materialization_boundary_required: true,
            correctness_evidence_required: true,
            benchmark_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn existing_evidence_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_existing_evidence())
            .count()
    }

    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.status,
                    ApproxSketchFunctionStatus::BlockedUntilCertified
                )
            })
            .count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn value_handling_order(&self) -> String {
        self.value_handling_contracts.join(",")
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.function_registry_entry_allowed
            && !self.sketch_state_runtime_allowed
            && !self.sketch_merge_runtime_allowed
            && !self.sketch_serialization_runtime_allowed
            && !self.grouped_aggregate_runtime_allowed
            && !self.encoded_dictionary_strategy_allowed
            && !self.encoded_run_length_strategy_allowed
            && !self.selection_vector_strategy_allowed
            && !self.partial_decode_execution_allowed
            && !self.generic_sketch_dependency_allowed
            && !self.external_engine_invoked
            && self
                .entries
                .iter()
                .all(|entry| !entry.runtime_allowed && !entry.external_engine_invoked)
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.exact_claim_allowed && !self.approximate_function_claim_allowed
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self
                .entries
                .iter()
                .all(ApproxSketchFunctionGateEntry::side_effect_free)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
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
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "canonical_function: {}", self.canonical_function_name);
        let _ = writeln!(out, "aliases: {}", self.alias_names.join(","));
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} runtime_allowed={} requires_function_registry={} requires_aggregate_state={} requires_sketch_serialization={} requires_exact_reference={} requires_encoded_strategy={} requires_execution_certificate={} requires_native_io_certificate={} fallback_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.runtime_allowed,
                entry.requires_function_registry,
                entry.requires_aggregate_state,
                entry.requires_sketch_serialization,
                entry.requires_exact_reference,
                entry.requires_encoded_strategy,
                entry.requires_execution_certificate,
                entry.requires_native_io_certificate,
                entry.fallback_execution_allowed
            );
        }
        out
    }
}

fn approx_sketch_function_entries() -> Vec<ApproxSketchFunctionGateEntry> {
    vec![
        ApproxSketchFunctionGateEntry::existing(
            ApproxSketchFunctionSurface::FunctionCoverageMatrixEntry,
            "FunctionCoverageGroup::ApproximateAggregates",
        ),
        ApproxSketchFunctionGateEntry::existing(
            ApproxSketchFunctionSurface::RfcSequencingContract,
            "RFC0032.approximate_aggregate_and_sketch_function_lane",
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::CanonicalApproxCountDistinct,
        ),
        ApproxSketchFunctionGateEntry::blocked(ApproxSketchFunctionSurface::AliasApproxDistinct),
        ApproxSketchFunctionGateEntry::blocked(ApproxSketchFunctionSurface::AliasApproxNUnique),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::UngroupedApproxDistinctExecution,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::GroupedApproxDistinctExecution,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::PartialSketchConstruction,
        ),
        ApproxSketchFunctionGateEntry::blocked(ApproxSketchFunctionSurface::AssociativeSketchMerge),
        ApproxSketchFunctionGateEntry::blocked(ApproxSketchFunctionSurface::SketchSerialization),
        ApproxSketchFunctionGateEntry::blocked(ApproxSketchFunctionSurface::SketchDeserialization),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::SketchVersionHashSeedMetadata,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::ErrorBoundsConfidenceModel,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::NullStringTemporalValueSemantics,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::DictionaryEncodedStrategy,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::RunLengthEncodedStrategy,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::SelectionVectorValidityStrategy,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::PartialDecodeMaterializationBoundary,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::ExactReferenceFixtureComparison,
        ),
        ApproxSketchFunctionGateEntry::blocked(
            ApproxSketchFunctionSurface::BenchmarkCertificateNativeIoCloseout,
        ),
    ]
}

fn approx_sketch_existing_report_refs() -> Vec<&'static str> {
    vec![
        "shardloom.function_coverage.v1",
        "RFC0032.approximate_aggregate_and_sketch_function_lane",
        "capability-certification-sequencing.R5.4.4a",
        "canonical-terminology.approximate_aggregate_sketch",
        "canonical-terminology.encoded_sketch_strategy",
    ]
}

fn approx_sketch_value_handling_contracts() -> Vec<&'static str> {
    vec![
        "null_policy",
        "string_values",
        "binary_values",
        "temporal_values",
        "dictionary_encoded_values",
        "run_length_encoded_values",
        "validity_and_selection_vectors",
        "nested_values_rejected_or_certified",
    ]
}

#[must_use]
pub fn plan_approx_sketch_function_gate() -> ApproxSketchFunctionGateReport {
    ApproxSketchFunctionGateReport::planning_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approx_sketch_function_gate_names_required_surfaces() {
        let report = plan_approx_sketch_function_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.approx_sketch_function_gate.v1"
        );
        assert_eq!(report.report_id, "cg20.approx_sketch_function_gate");
        assert_eq!(report.canonical_function_name, "approx_count_distinct");
        assert_eq!(
            report.alias_names,
            vec!["approx_distinct", "approx_n_unique"]
        );
        assert_eq!(report.surface_count(), 20);
        assert_eq!(report.existing_evidence_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 18);
        assert!(
            report
                .surface_order()
                .contains(&"grouped_approx_distinct_execution")
        );
        assert!(report.surface_order().contains(&"associative_sketch_merge"));
        assert!(
            report
                .surface_order()
                .contains(&"dictionary_encoded_strategy")
        );
        assert!(
            report
                .surface_order()
                .contains(&"benchmark_certificate_native_io_closeout")
        );
    }

    #[test]
    fn approx_sketch_function_gate_blocks_runtime_dependencies_and_claims() {
        let report = plan_approx_sketch_function_gate();
        assert!(report.existing_function_coverage_matrix_entry_present);
        assert!(report.existing_rfc_sequencing_contract_present);
        assert!(!report.function_registry_entry_allowed);
        assert!(!report.sketch_state_runtime_allowed);
        assert!(!report.sketch_merge_runtime_allowed);
        assert!(!report.sketch_serialization_runtime_allowed);
        assert!(!report.grouped_aggregate_runtime_allowed);
        assert!(!report.encoded_dictionary_strategy_allowed);
        assert!(!report.encoded_run_length_strategy_allowed);
        assert!(!report.selection_vector_strategy_allowed);
        assert!(!report.partial_decode_execution_allowed);
        assert!(!report.generic_sketch_dependency_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
        assert!(!report.approximate_function_claim_allowed);
        assert!(report.function_registry_required);
        assert!(report.aggregate_state_required);
        assert!(report.sketch_serialization_required);
        assert!(report.stable_hash_seed_policy_required);
        assert!(report.error_bounds_required);
        assert!(report.confidence_model_required);
        assert!(report.exact_reference_fixtures_required);
        assert!(report.encoded_dictionary_strategy_required);
        assert!(report.encoded_run_length_strategy_required);
        assert!(report.selection_vector_strategy_required);
        assert!(report.partial_decode_materialization_boundary_required);
        assert!(report.correctness_evidence_required);
        assert!(report.benchmark_evidence_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
    }
}
