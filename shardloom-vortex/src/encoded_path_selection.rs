use shardloom_core::{
    Diagnostic, KernelKind, PhysicalOperatorExecutionLevel, PhysicalOperatorExecutionProfile,
    PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorKind,
};

use crate::{
    VortexEncodedCountPhysicalKernelDiscoveryReport,
    VortexEncodedPredicateEvaluationDiscoveryReport,
    VortexSelectionVectorFilterKernelDiscoveryReport,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_encoded_path_selection.v1";
const REPORT_ID: &str = "vortex.cg13.encoded-path-selection";
const PROJECT_EVIDENCE_SOURCE: &str =
    "vortex.query_primitive.project_columns.encoded_project.kernel-admission";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedExecutionPathSelectionStatus {
    ReportOnlyPlanned,
    BlockedMissingProfile,
    Unsupported,
}

impl VortexEncodedExecutionPathSelectionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::BlockedMissingProfile => "blocked_missing_profile",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::ReportOnlyPlanned)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedExecutionPathSelectionEntry {
    pub operator_kind: PhysicalOperatorKind,
    pub profile_id: String,
    pub selected_level: PhysicalOperatorExecutionLevel,
    pub required_kernel_kinds: Vec<KernelKind>,
    pub evidence_sources: Vec<String>,
    pub encoding_aware: bool,
    pub metadata_only_candidate: bool,
    pub encoded_native_candidate: bool,
    pub hybrid_native_candidate: bool,
    pub native_decoded_candidate: bool,
    pub decode_avoided: bool,
    pub materialization_avoided: bool,
    pub selection_vector_preserved: bool,
    pub requires_correctness_evidence: bool,
    pub requires_memory_safety_evidence: bool,
    pub requires_benchmark_for_production: bool,
    pub data_read: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl VortexEncodedExecutionPathSelectionEntry {
    #[must_use]
    fn from_profile(
        profile: &PhysicalOperatorExecutionProfile,
        evidence_sources: Vec<String>,
        selection_vector_preserved: bool,
    ) -> Self {
        let selected_level = profile.preferred_level;
        Self {
            operator_kind: profile.operator_kind,
            profile_id: profile.profile_id.clone(),
            selected_level,
            required_kernel_kinds: profile.required_kernel_kinds_for_level(selected_level),
            evidence_sources,
            encoding_aware: true,
            metadata_only_candidate: profile
                .allows_level(PhysicalOperatorExecutionLevel::MetadataOnly),
            encoded_native_candidate: profile
                .allows_level(PhysicalOperatorExecutionLevel::EncodedNative),
            hybrid_native_candidate: profile
                .allows_level(PhysicalOperatorExecutionLevel::HybridNative),
            native_decoded_candidate: profile
                .allows_level(PhysicalOperatorExecutionLevel::NativeDecoded),
            decode_avoided: matches!(
                selected_level,
                PhysicalOperatorExecutionLevel::MetadataOnly
                    | PhysicalOperatorExecutionLevel::EncodedNative
            ),
            materialization_avoided: !profile.row_materialization_allowed,
            selection_vector_preserved,
            requires_correctness_evidence: true,
            requires_memory_safety_evidence: true,
            requires_benchmark_for_production: true,
            data_read: false,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub fn required_kernel_kind_names(&self) -> Vec<&'static str> {
        self.required_kernel_kinds
            .iter()
            .map(KernelKind::as_str)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedExecutionPathSelectionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub profile_matrix_id: String,
    pub status: VortexEncodedExecutionPathSelectionStatus,
    pub entries: Vec<VortexEncodedExecutionPathSelectionEntry>,
    pub encoded_count_discovery_present: bool,
    pub encoded_predicate_discovery_present: bool,
    pub selection_vector_filter_discovery_present: bool,
    pub encoded_projection_evidence_present: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_execution: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexEncodedExecutionPathSelectionReport {
    #[must_use]
    pub fn cg13_foundation() -> Self {
        let profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
        Self::from_profiles(&profiles)
    }

    #[must_use]
    pub fn from_profiles(profiles: &PhysicalOperatorExecutionProfileMatrix) -> Self {
        let count_discovery = VortexEncodedCountPhysicalKernelDiscoveryReport::report_only();
        let predicate_discovery = VortexEncodedPredicateEvaluationDiscoveryReport::report_only();
        let filter_discovery = VortexSelectionVectorFilterKernelDiscoveryReport::report_only();

        let requested = [
            (
                PhysicalOperatorKind::CountAggregate,
                vec![count_discovery.kernel_report_id.to_string()],
                false,
            ),
            (
                PhysicalOperatorKind::Filter,
                vec![
                    predicate_discovery.report_id.to_string(),
                    filter_discovery.kernel_report_id.to_string(),
                ],
                true,
            ),
            (
                PhysicalOperatorKind::Project,
                vec![PROJECT_EVIDENCE_SOURCE.to_string()],
                false,
            ),
        ];

        let mut entries = Vec::new();
        let mut diagnostics = Vec::new();
        for (operator_kind, evidence_sources, selection_vector_preserved) in requested {
            if let Some(profile) = profiles.profile_for(operator_kind) {
                entries.push(VortexEncodedExecutionPathSelectionEntry::from_profile(
                    profile,
                    evidence_sources,
                    selection_vector_preserved,
                ));
            } else {
                diagnostics.push(Diagnostic::not_implemented(
                    format!("cg13.encoded_path_selection.{}", operator_kind.as_str()),
                    "CG-13 encoded path selection requires an execution profile for this operator.",
                    "Add the operator to PhysicalOperatorExecutionProfileMatrix before selecting encoded paths.",
                ));
            }
        }

        let status = if diagnostics.is_empty() {
            VortexEncodedExecutionPathSelectionStatus::ReportOnlyPlanned
        } else {
            VortexEncodedExecutionPathSelectionStatus::BlockedMissingProfile
        };

        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            profile_matrix_id: profiles.matrix_id.clone(),
            status,
            entries,
            encoded_count_discovery_present: count_discovery.is_side_effect_free(),
            encoded_predicate_discovery_present: predicate_discovery.is_side_effect_free(),
            selection_vector_filter_discovery_present: filter_discovery.is_side_effect_free(),
            encoded_projection_evidence_present: true,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            runtime_execution_allowed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn encoded_native_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.encoded_native_candidate)
            .count()
    }

    #[must_use]
    pub fn metadata_only_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.metadata_only_candidate)
            .count()
    }

    #[must_use]
    pub fn hybrid_native_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.hybrid_native_candidate)
            .count()
    }

    #[must_use]
    pub fn native_decoded_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.native_decoded_candidate)
            .count()
    }

    #[must_use]
    pub fn decode_avoided_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.decode_avoided)
            .count()
    }

    #[must_use]
    pub fn materialization_avoided_candidate_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.materialization_avoided)
            .count()
    }

    #[must_use]
    pub fn selection_vector_preserved_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.selection_vector_preserved)
            .count()
    }

    #[must_use]
    pub fn has_operator(&self, operator_kind: PhysicalOperatorKind) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.operator_kind == operator_kind)
    }

    #[must_use]
    pub fn operator_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.operator_kind.as_str())
            .collect()
    }

    #[must_use]
    pub fn selected_execution_levels(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.selected_level.as_str())
            .collect()
    }

    #[must_use]
    pub fn evidence_sources(&self) -> Vec<&str> {
        self.entries
            .iter()
            .flat_map(|entry| entry.evidence_sources.iter().map(String::as_str))
            .collect()
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.runtime_execution_allowed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || !self.diagnostics.is_empty()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "Vortex encoded path selection plan\nschema_version: {}\nreport: {}\nstatus: {}\nprofile matrix: {}\nentries: {}\noperators: {}\nselected levels: {}\ndecode avoided candidates: {}\nmaterialization avoided candidates: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.profile_matrix_id,
            self.entry_count(),
            self.operator_order().join(","),
            self.selected_execution_levels().join(","),
            self.decode_avoided_candidate_count(),
            self.materialization_avoided_candidate_count(),
        )
    }
}

#[must_use]
pub fn plan_vortex_encoded_execution_path_selection() -> VortexEncodedExecutionPathSelectionReport {
    VortexEncodedExecutionPathSelectionReport::cg13_foundation()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cg13_foundation_selects_count_filter_project_encoded_native_paths() {
        let report = VortexEncodedExecutionPathSelectionReport::cg13_foundation();

        assert_eq!(
            report.status,
            VortexEncodedExecutionPathSelectionStatus::ReportOnlyPlanned
        );
        assert_eq!(report.entry_count(), 3);
        assert_eq!(
            report.operator_order(),
            vec!["count_aggregate", "filter", "project"]
        );
        assert_eq!(
            report.selected_execution_levels(),
            vec!["encoded_native", "encoded_native", "encoded_native"]
        );
        assert!(report.has_operator(PhysicalOperatorKind::CountAggregate));
        assert!(report.has_operator(PhysicalOperatorKind::Filter));
        assert!(report.has_operator(PhysicalOperatorKind::Project));
        assert_eq!(report.encoded_native_candidate_count(), 3);
    }

    #[test]
    fn cg13_foundation_records_decode_and_materialization_avoidance() {
        let report = VortexEncodedExecutionPathSelectionReport::cg13_foundation();

        assert_eq!(report.decode_avoided_candidate_count(), 3);
        assert_eq!(report.materialization_avoided_candidate_count(), 3);
        assert_eq!(report.selection_vector_preserved_count(), 1);
        assert!(
            report
                .entries
                .iter()
                .filter(|entry| entry.operator_kind == PhysicalOperatorKind::Filter)
                .all(|entry| entry.selection_vector_preserved)
        );
    }

    #[test]
    fn cg13_foundation_is_side_effect_free_and_no_fallback() {
        let report = VortexEncodedExecutionPathSelectionReport::cg13_foundation();

        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.runtime_execution_allowed);
        assert!(!report.external_engine_execution);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }
}
