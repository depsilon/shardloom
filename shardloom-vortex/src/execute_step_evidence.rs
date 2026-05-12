//! Execute-step evidence for Vortex-native provider paths.
//!
//! This report records representation transitions and execution-stage evidence
//! without assuming upstream deferred/iterative execution occurred. Empty stage
//! lists are intentional evidence: no fusion, reduction, canonicalization, or
//! materialization claim is made until traces/certificates exist.

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecuteStepEvidence {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub initial_representation: &'static str,
    pub deferred_operations: Vec<&'static str>,
    pub executed_operations: Vec<&'static str>,
    pub fused_operations: Vec<&'static str>,
    pub reduce_steps: Vec<&'static str>,
    pub canonicalization_steps: Vec<&'static str>,
    pub materialization_steps: Vec<&'static str>,
    pub execution_context_id: Option<&'static str>,
    pub trace_span_refs: Vec<&'static str>,
    pub final_representation: &'static str,
    pub deferred_execution_claimed: bool,
    pub fusion_claimed: bool,
    pub canonicalization_claimed: bool,
    pub materialization_claimed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ExecuteStepEvidence {
    #[must_use]
    pub fn current_reader_chunk_path() -> Self {
        Self {
            schema_version: "shardloom.execute_step_evidence.v1",
            report_id: "cg16.execute_step_evidence.reader_chunk_path",
            initial_representation: "vortex_reader_chunk",
            deferred_operations: vec!["reader_chunk_envelope_planning"],
            executed_operations: vec![
                "reader_generated_kernel_input_admission",
                "prepared_encoded_filter_or_projection",
            ],
            fused_operations: Vec::new(),
            reduce_steps: Vec::new(),
            canonicalization_steps: Vec::new(),
            materialization_steps: Vec::new(),
            execution_context_id: None,
            trace_span_refs: Vec::new(),
            final_representation: "selection_vector_or_encoded_projection",
            deferred_execution_claimed: false,
            fusion_claimed: false,
            canonicalization_claimed: false,
            materialization_claimed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn blocks_claims_without_trace_evidence(&self) -> bool {
        self.trace_span_refs.is_empty()
            && !self.deferred_execution_claimed
            && !self.fusion_claimed
            && !self.canonicalization_claimed
            && !self.materialization_claimed
    }

    #[must_use]
    pub fn preserves_encoded_execution_boundary(&self) -> bool {
        self.canonicalization_steps.is_empty()
            && self.materialization_steps.is_empty()
            && !self.canonicalization_claimed
            && !self.materialization_claimed
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "execute-step evidence\nschema_version: {}\nreport: {}\ninitial: {}\nexecuted: {}\nfinal: {}\ncanonicalization: none\nmaterialization: none\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.initial_representation,
            self.executed_operations.join(","),
            self.final_representation,
        )
    }
}

#[must_use]
pub fn plan_execute_step_evidence() -> ExecuteStepEvidence {
    ExecuteStepEvidence::current_reader_chunk_path()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_step_evidence_records_current_reader_chunk_path() {
        let evidence = plan_execute_step_evidence();

        assert_eq!(
            evidence.schema_version,
            "shardloom.execute_step_evidence.v1"
        );
        assert_eq!(evidence.initial_representation, "vortex_reader_chunk");
        assert_eq!(
            evidence.deferred_operations,
            vec!["reader_chunk_envelope_planning"]
        );
        assert_eq!(
            evidence.executed_operations,
            vec![
                "reader_generated_kernel_input_admission",
                "prepared_encoded_filter_or_projection"
            ]
        );
        assert_eq!(
            evidence.final_representation,
            "selection_vector_or_encoded_projection"
        );
    }

    #[test]
    fn execute_step_evidence_does_not_overclaim_deferred_or_fused_execution() {
        let evidence = plan_execute_step_evidence();

        assert!(!evidence.deferred_execution_claimed);
        assert!(!evidence.fusion_claimed);
        assert!(evidence.fused_operations.is_empty());
        assert!(evidence.reduce_steps.is_empty());
        assert!(evidence.trace_span_refs.is_empty());
        assert!(evidence.blocks_claims_without_trace_evidence());
    }

    #[test]
    fn execute_step_evidence_blocks_canonicalization_materialization_and_fallback() {
        let evidence = plan_execute_step_evidence();

        assert!(evidence.canonicalization_steps.is_empty());
        assert!(evidence.materialization_steps.is_empty());
        assert!(evidence.preserves_encoded_execution_boundary());
        assert!(!evidence.external_engine_invoked);
        assert!(!evidence.fallback_attempted);
        assert!(evidence.to_human_text().contains("materialization: none"));
    }
}
