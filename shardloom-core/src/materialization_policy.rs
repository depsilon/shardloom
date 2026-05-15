//! Shared materialization/decode policy reporting.
//!
//! This is a report-only contract surface. It classifies operator paths by
//! representation behavior without executing operators, reading data, decoding
//! values, materializing rows, invoking external engines, or attempting
//! fallback execution.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationPolicyOperatorClass {
    EncodedNative,
    ResidualNative,
    MaterializedTemporary,
    Unsupported,
}

impl MaterializationPolicyOperatorClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EncodedNative => "encoded_native",
            Self::ResidualNative => "residual_native",
            Self::MaterializedTemporary => "materialized_temporary",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct MaterializationPolicyRow {
    pub row_id: &'static str,
    pub operator_execution_class: MaterializationPolicyOperatorClass,
    pub support_status: &'static str,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub stayed_encoded: bool,
    pub materialization_boundary_required: bool,
    pub materialization_boundary_emitted: bool,
    pub materialized_temporary_path: bool,
    pub encoded_native_claim_allowed: bool,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl MaterializationPolicyRow {
    #[must_use]
    pub const fn encoded_native() -> Self {
        Self {
            row_id: "encoded_native_operator_path",
            operator_execution_class: MaterializationPolicyOperatorClass::EncodedNative,
            support_status: "report_only_contract",
            data_decoded: false,
            data_materialized: false,
            stayed_encoded: true,
            materialization_boundary_required: true,
            materialization_boundary_emitted: true,
            materialized_temporary_path: false,
            encoded_native_claim_allowed: true,
            materialization_decode_refs: "metadata_or_encoded_values_no_row_materialization",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "none",
            blocker_id: "none",
            required_future_evidence: "execution_certificate,native_io_certificate,operator_correctness_fixture,benchmark_row",
            claim_gate_status: "fixture_or_claim_gate_dependent",
            claim_boundary: "encoded_native_claim_requires_operator_and_workload_scoped_evidence",
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn residual_native() -> Self {
        Self {
            row_id: "residual_native_operator_path",
            operator_execution_class: MaterializationPolicyOperatorClass::ResidualNative,
            support_status: "report_only_contract",
            data_decoded: false,
            data_materialized: false,
            stayed_encoded: false,
            materialization_boundary_required: true,
            materialization_boundary_emitted: true,
            materialized_temporary_path: false,
            encoded_native_claim_allowed: false,
            materialization_decode_refs: "residual_native_boundary_no_external_engine",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "none",
            blocker_id: "gar0003b.residual_native_not_encoded_native",
            required_future_evidence: "residual_executor_certificate,semantic_fixture,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "residual_native_paths_are_shardloom_native_but_not_encoded_native_claims",
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn materialized_temporary() -> Self {
        Self {
            row_id: "materialized_temporary_operator_path",
            operator_execution_class: MaterializationPolicyOperatorClass::MaterializedTemporary,
            support_status: "supported_with_boundary",
            data_decoded: true,
            data_materialized: true,
            stayed_encoded: false,
            materialization_boundary_required: true,
            materialization_boundary_emitted: true,
            materialized_temporary_path: true,
            encoded_native_claim_allowed: false,
            materialization_decode_refs: "materialization_boundary_report_required",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "none",
            blocker_id: "gar-flow-2b.materialized_temporary_operator_not_encoded_native",
            required_future_evidence: "encoded_native_operator_evidence_before_encoded_claim",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "materialized_temporary_paths_cannot_satisfy_encoded_native_claims",
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            row_id: "unsupported_operator_path",
            operator_execution_class: MaterializationPolicyOperatorClass::Unsupported,
            support_status: "unsupported",
            data_decoded: false,
            data_materialized: false,
            stayed_encoded: false,
            materialization_boundary_required: false,
            materialization_boundary_emitted: false,
            materialized_temporary_path: false,
            encoded_native_claim_allowed: false,
            materialization_decode_refs: "unsupported_no_decode_or_materialization",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_OPERATOR_MATERIALIZATION_POLICY",
            blocker_id: "gar0003b.unsupported_operator_materialization_policy",
            required_future_evidence: "operator_capability_row,deterministic_diagnostic,materialization_policy_row",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "unsupported_paths_do_not_decode_materialize_or_execute",
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.external_engine_invoked
    }

    #[must_use]
    pub const fn classified(&self) -> bool {
        match self.operator_execution_class {
            MaterializationPolicyOperatorClass::EncodedNative => {
                self.stayed_encoded && !self.data_decoded && !self.data_materialized
            }
            MaterializationPolicyOperatorClass::ResidualNative
            | MaterializationPolicyOperatorClass::Unsupported => {
                !self.stayed_encoded && !self.data_decoded && !self.data_materialized
            }
            MaterializationPolicyOperatorClass::MaterializedTemporary => {
                !self.stayed_encoded && self.data_decoded && self.data_materialized
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct MaterializationPolicyReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub report_ref: &'static str,
    pub docs_ref: &'static str,
    pub support_status_vocabulary: &'static str,
    pub operator_execution_class_vocabulary: &'static str,
    pub claim_gate_status: &'static str,
    pub rows: Vec<MaterializationPolicyRow>,
    pub runtime_execution: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl MaterializationPolicyReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.materialization_policy.v1",
            report_id: "gar0003b.materialization_policy",
            report_ref: "compute-capability-matrix://materialization_policy.v1",
            docs_ref: "docs/architecture/compute-engine-flow-reference.md#materialization-and-decode-flow",
            support_status_vocabulary: "report_only_contract,supported_with_boundary,unsupported",
            operator_execution_class_vocabulary: "encoded_native,residual_native,materialized_temporary,unsupported",
            claim_gate_status: "not_claim_grade",
            rows: vec![
                MaterializationPolicyRow::encoded_native(),
                MaterializationPolicyRow::residual_native(),
                MaterializationPolicyRow::materialized_temporary(),
                MaterializationPolicyRow::unsupported(),
            ],
            runtime_execution: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn row_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.row_id).collect()
    }

    #[must_use]
    pub fn operator_execution_classes(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.operator_execution_class.as_str())
            .collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.blocker_id)
            .filter(|blocker| *blocker != "none")
            .collect()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_engine_invoked
            && self
                .rows
                .iter()
                .all(MaterializationPolicyRow::fallback_free)
    }

    #[must_use]
    pub fn all_rows_external_engine_free(&self) -> bool {
        !self.external_engine_invoked && self.rows.iter().all(|row| !row.external_engine_invoked)
    }

    #[must_use]
    pub fn all_rows_classified(&self) -> bool {
        self.rows.iter().all(MaterializationPolicyRow::classified)
    }

    #[must_use]
    pub fn materialized_temporary_encoded_native_claim_allowed(&self) -> bool {
        self.rows
            .iter()
            .find(|row| {
                matches!(
                    row.operator_execution_class,
                    MaterializationPolicyOperatorClass::MaterializedTemporary
                )
            })
            .is_some_and(|row| row.encoded_native_claim_allowed)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "materialization policy report\nschema_version: {}\nreport: {}\nrows: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.row_order().join(","),
            self.claim_gate_status,
        )
    }
}

#[must_use]
pub fn plan_materialization_policy_report() -> MaterializationPolicyReport {
    MaterializationPolicyReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialization_policy_classifies_all_operator_paths() {
        let report = plan_materialization_policy_report();

        assert_eq!(report.schema_version, "shardloom.materialization_policy.v1");
        assert_eq!(report.rows.len(), 4);
        assert_eq!(
            report.operator_execution_classes(),
            vec![
                "encoded_native",
                "residual_native",
                "materialized_temporary",
                "unsupported"
            ]
        );
        assert!(report.all_rows_classified());
        assert!(report.all_rows_fallback_free());
        assert!(report.all_rows_external_engine_free());
        assert!(!report.materialized_temporary_encoded_native_claim_allowed());
        assert!(
            report
                .blocker_ids()
                .contains(&"gar-flow-2b.materialized_temporary_operator_not_encoded_native")
        );
    }

    #[test]
    fn materialization_policy_keeps_unsupported_paths_non_executing() {
        let report = plan_materialization_policy_report();
        let unsupported = report
            .rows
            .iter()
            .find(|row| {
                matches!(
                    row.operator_execution_class,
                    MaterializationPolicyOperatorClass::Unsupported
                )
            })
            .expect("unsupported row");

        assert_eq!(
            unsupported.unsupported_diagnostic_code,
            "SL_UNSUPPORTED_OPERATOR_MATERIALIZATION_POLICY"
        );
        assert!(!unsupported.runtime_execution);
        assert!(!unsupported.data_decoded);
        assert!(!unsupported.data_materialized);
        assert!(!unsupported.fallback_attempted);
        assert!(!unsupported.external_engine_invoked);
    }
}
