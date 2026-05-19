//! Shared GAR-0029-A evidence-expansion field surface.
//!
//! This is report-only metadata. It does not run correctness fixtures,
//! benchmarks, stateful reuse, cache reads/writes, external engines, or
//! fallback execution.

use shardloom_core::{
    CorrectnessBenchmarkReuseEvidenceExpansionReport,
    CorrectnessBenchmarkReuseEvidenceExpansionRow,
    plan_correctness_benchmark_reuse_evidence_expansion,
};

pub(crate) fn gar_0029_evidence_expansion_fields() -> Vec<(String, String)> {
    let report = plan_correctness_benchmark_reuse_evidence_expansion();
    let mut fields = Vec::new();
    append_gar_0029_evidence_expansion_summary_fields(&mut fields, &report);
    append_gar_0029_evidence_expansion_row_fields(&mut fields, &report);
    fields
}

fn append_gar_0029_evidence_expansion_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &CorrectnessBenchmarkReuseEvidenceExpansionReport,
) {
    push_field(
        fields,
        "gar_0029_evidence_expansion_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "gar_0029_evidence_expansion_report_id",
        report.report_id,
    );
    push_field(
        fields,
        "gar_0029_evidence_expansion_docs_ref",
        report.docs_ref,
    );
    push_field(
        fields,
        "gar_0029_evidence_expansion_support_status",
        report.support_status,
    );
    push_field(
        fields,
        "gar_0029_evidence_expansion_claim_gate_status",
        report.claim_gate_status,
    );
    push_count_field(
        fields,
        "gar_0029_evidence_expansion_row_count",
        report.rows.len(),
    );
    push_count_field(
        fields,
        "gar_0029_evidence_expansion_blocking_row_count",
        report.blocking_row_count(),
    );
    push_field(
        fields,
        "gar_0029_evidence_expansion_row_ids",
        report.row_ids().join(","),
    );
    append_gar_0029_evidence_expansion_boolean_fields(fields, report);
}

fn append_gar_0029_evidence_expansion_boolean_fields(
    fields: &mut Vec<(String, String)>,
    report: &CorrectnessBenchmarkReuseEvidenceExpansionReport,
) {
    for (field, value) in [
        (
            "gar_0029_evidence_expansion_correctness_evidence_attached",
            report.correctness_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_benchmark_evidence_attached",
            report.benchmark_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_execution_certificate_evidence_attached",
            report.execution_certificate_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_native_io_evidence_attached",
            report.native_io_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_stateful_reuse_evidence_attached",
            report.stateful_reuse_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_reuse_benchmark_evidence_attached",
            report.reuse_benchmark_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_selected_workload_evidence_attached",
            report.selected_workload_evidence_attached,
        ),
        (
            "gar_0029_evidence_expansion_deterministic_blocker_report",
            report.deterministic_blocker_report,
        ),
        (
            "gar_0029_evidence_expansion_stateful_reuse_runtime_supported",
            report.stateful_reuse_runtime_supported,
        ),
        (
            "gar_0029_evidence_expansion_cache_read_allowed",
            report.cache_read_allowed,
        ),
        (
            "gar_0029_evidence_expansion_cache_write_allowed",
            report.cache_write_allowed,
        ),
        (
            "gar_0029_evidence_expansion_cache_replay_allowed",
            report.cache_replay_allowed,
        ),
        (
            "gar_0029_evidence_expansion_incremental_execution_allowed",
            report.incremental_execution_allowed,
        ),
        (
            "gar_0029_evidence_expansion_performance_claim_allowed",
            report.performance_claim_allowed,
        ),
        (
            "gar_0029_evidence_expansion_superiority_claim_allowed",
            report.superiority_claim_allowed,
        ),
        (
            "gar_0029_evidence_expansion_production_reuse_claim_allowed",
            report.production_reuse_claim_allowed,
        ),
        (
            "gar_0029_evidence_expansion_claim_grade_closeout_allowed",
            report.claim_grade_closeout_allowed,
        ),
        (
            "gar_0029_evidence_expansion_benchmark_rerun_performed",
            report.benchmark_rerun_performed,
        ),
        (
            "gar_0029_evidence_expansion_runtime_execution_performed",
            report.runtime_execution_performed,
        ),
        (
            "gar_0029_evidence_expansion_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "gar_0029_evidence_expansion_external_engine_invoked",
            report.external_engine_invoked,
        ),
        (
            "gar_0029_evidence_expansion_all_claims_blocked",
            report.all_claims_blocked(),
        ),
        (
            "gar_0029_evidence_expansion_side_effect_free",
            report.side_effect_free(),
        ),
    ] {
        push_bool_field(fields, field, value);
    }
}

fn append_gar_0029_evidence_expansion_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &CorrectnessBenchmarkReuseEvidenceExpansionReport,
) {
    for row in &report.rows {
        let prefix = format!("gar_0029_evidence_expansion_row_{}", row.row_id);
        append_gar_0029_evidence_expansion_row(fields, &prefix, row);
    }
}

fn append_gar_0029_evidence_expansion_row(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &CorrectnessBenchmarkReuseEvidenceExpansionRow,
) {
    push_field(fields, &format!("{prefix}_family"), row.evidence_family);
    push_field(fields, &format!("{prefix}_surface_ref"), row.surface_ref);
    push_field(
        fields,
        &format!("{prefix}_required_evidence"),
        row.required_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_current_state"),
        row.current_state,
    );
    push_field(fields, &format!("{prefix}_blocker"), row.blocker);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_evidence_attached"),
        row.evidence_attached,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_supported"),
        row.runtime_supported,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_public_claim_allowed"),
        row.public_claim_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        row.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        row.external_engine_invoked,
    );
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: impl Into<String>) {
    fields.push((key.to_string(), value.into()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, value.to_string());
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, value.to_string());
}
