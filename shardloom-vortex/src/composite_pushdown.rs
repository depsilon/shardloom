//! Composite pushdown capability matrix.
//!
//! This report keeps combinations such as filter+projection+limit distinct from
//! individual operator availability. It is evidence inventory only and never
//! delegates unsupported combinations to an external query engine.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositePushdownStatus {
    Certified,
    EvidenceIncomplete,
    ReportOnly,
    Deferred,
    UnsupportedBlocked,
}

impl CompositePushdownStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::ReportOnly => "report_only",
            Self::Deferred => "deferred",
            Self::UnsupportedBlocked => "unsupported_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CompositePushdownCapabilityRow {
    pub combination: &'static str,
    pub status: CompositePushdownStatus,
    pub evidence_refs: Vec<&'static str>,
    pub deterministic_unsupported_diagnostic: bool,
    pub correctness_required: bool,
    pub benchmark_required: bool,
    pub certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl CompositePushdownCapabilityRow {
    fn new(
        combination: &'static str,
        status: CompositePushdownStatus,
        evidence_refs: Vec<&'static str>,
    ) -> Self {
        Self {
            combination,
            status,
            evidence_refs,
            deterministic_unsupported_diagnostic: matches!(
                status,
                CompositePushdownStatus::Deferred | CompositePushdownStatus::UnsupportedBlocked
            ),
            correctness_required: true,
            benchmark_required: true,
            certificate_required: true,
            native_io_certificate_required: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CompositePushdownCapabilityMatrix {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<CompositePushdownCapabilityRow>,
    pub primitive_support_inflates_composite_support: bool,
    pub external_query_engine_residual_allowed: bool,
    pub fallback_attempted: bool,
}

impl CompositePushdownCapabilityMatrix {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.composite_pushdown_capability_matrix.v1",
            report_id: "cg2.cg13.composite_pushdown_capability_matrix",
            rows: vec![
                CompositePushdownCapabilityRow::new(
                    "filter_projection",
                    CompositePushdownStatus::Certified,
                    vec![
                        "local FilterAndProject scan-pushdown evidence",
                        "prepared encoded filter-project evidence",
                    ],
                ),
                CompositePushdownCapabilityRow::new(
                    "filter_limit",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "projection_limit",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "filter_projection_limit",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "ordered_limit",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "reverse_scan",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "top_n",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "range_predicate_projection",
                    CompositePushdownStatus::EvidenceIncomplete,
                    vec![
                        "predicate/projection primitive evidence without full range pushdown claim",
                    ],
                ),
                CompositePushdownCapabilityRow::new(
                    "zone_pruned_filter_residual",
                    CompositePushdownStatus::Deferred,
                    Vec::new(),
                ),
                CompositePushdownCapabilityRow::new(
                    "filter_only_columns_discarded_after_mask",
                    CompositePushdownStatus::ReportOnly,
                    vec!["VortexScanCompatibilityReport field-mask posture"],
                ),
                CompositePushdownCapabilityRow::new(
                    "external_residual_evaluation",
                    CompositePushdownStatus::UnsupportedBlocked,
                    Vec::new(),
                ),
            ],
            primitive_support_inflates_composite_support: false,
            external_query_engine_residual_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn row(&self, combination: &str) -> Option<&CompositePushdownCapabilityRow> {
        self.rows.iter().find(|row| row.combination == combination)
    }

    #[must_use]
    pub fn combination_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.combination).collect()
    }

    #[must_use]
    pub fn certified_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.status == CompositePushdownStatus::Certified)
            .count()
    }

    #[must_use]
    pub fn unsupported_or_deferred_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    CompositePushdownStatus::Deferred | CompositePushdownStatus::UnsupportedBlocked
                )
            })
            .count()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_query_engine_residual_allowed
            && self
                .rows
                .iter()
                .all(|row| !row.external_engine_invoked && !row.fallback_attempted)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "composite pushdown capability matrix\nschema_version: {}\nreport: {}\ncombinations: {}\ncertified: {}\nunsupported_or_deferred: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.rows.len(),
            self.certified_count(),
            self.unsupported_or_deferred_count(),
        )
    }
}

#[must_use]
pub fn plan_composite_pushdown_capability_matrix() -> CompositePushdownCapabilityMatrix {
    CompositePushdownCapabilityMatrix::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_tracks_composite_combinations_separately() {
        let matrix = plan_composite_pushdown_capability_matrix();

        assert_eq!(
            matrix.combination_order(),
            vec![
                "filter_projection",
                "filter_limit",
                "projection_limit",
                "filter_projection_limit",
                "ordered_limit",
                "reverse_scan",
                "top_n",
                "range_predicate_projection",
                "zone_pruned_filter_residual",
                "filter_only_columns_discarded_after_mask",
                "external_residual_evaluation"
            ]
        );
        assert!(!matrix.primitive_support_inflates_composite_support);
    }

    #[test]
    fn matrix_marks_only_currently_certified_composite_path() {
        let matrix = plan_composite_pushdown_capability_matrix();

        assert_eq!(
            matrix.row("filter_projection").map(|row| row.status),
            Some(CompositePushdownStatus::Certified)
        );
        assert_eq!(
            matrix.row("filter_projection_limit").map(|row| row.status),
            Some(CompositePushdownStatus::Deferred)
        );
        assert_eq!(
            matrix
                .row("external_residual_evaluation")
                .map(|row| row.status),
            Some(CompositePushdownStatus::UnsupportedBlocked)
        );
        assert_eq!(matrix.certified_count(), 1);
    }

    #[test]
    fn matrix_preserves_no_external_residual_fallback() {
        let matrix = plan_composite_pushdown_capability_matrix();
        let residual = matrix
            .row("external_residual_evaluation")
            .expect("external residual row");

        assert!(residual.deterministic_unsupported_diagnostic);
        assert!(!matrix.external_query_engine_residual_allowed);
        assert!(matrix.all_rows_fallback_free());
        assert!(
            matrix
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
