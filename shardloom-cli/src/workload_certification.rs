//! Workload-scoped certification dossier planning.
//!
//! The dossier is a report-only evidence index. It combines existing CG evidence
//! surfaces without executing workloads, reading datasets, probing services, or
//! publishing claims.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{CommandStatus, Diagnostic, DiagnosticCode, OutputFormat, ShardLoomError};

use crate::cli_output::{emit, emit_error};

const COMMAND: &str = "workload-certification-dossier";
const USAGE: &str = "usage: shardloom workload-certification-dossier [local-vortex-count|planned-live-hybrid|blocked-remote-api|unsupported-sql]";

pub(crate) fn handle_workload_certification_dossier(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = DossierScenario::parse(&token) else {
                return emit_error(
                    COMMAND,
                    format,
                    "workload certification dossier failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown workload certification dossier scenario: {token}; {USAGE}"
                    )),
                );
            };
            parsed
        }
        None => DossierScenario::LocalVortexCount,
    };
    if let Some(extra) = args.next() {
        return emit_error(
            COMMAND,
            format,
            "workload certification dossier failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected workload certification dossier argument: {extra}; {USAGE}"
            )),
        );
    }

    let dossier = WorkloadCertificationDossier::for_scenario(scenario);
    emit(
        COMMAND,
        format,
        dossier.command_status(),
        "workload certification dossier".to_string(),
        dossier.human_text(),
        dossier.diagnostics(),
        dossier.fields(),
    );
    dossier.exit_code()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DossierScenario {
    LocalVortexCount,
    PlannedLiveHybrid,
    BlockedRemoteApi,
    UnsupportedSql,
}

impl DossierScenario {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "local-vortex-count" | "local-count" | "vortex-count" => Some(Self::LocalVortexCount),
            "planned-live-hybrid" | "live-hybrid" | "planned-hybrid" => {
                Some(Self::PlannedLiveHybrid)
            }
            "blocked-remote-api" | "remote-api" | "blocked-remote" => Some(Self::BlockedRemoteApi),
            "unsupported-sql" | "sql" | "unsupported-operator" => Some(Self::UnsupportedSql),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::LocalVortexCount => "local-vortex-count",
            Self::PlannedLiveHybrid => "planned-live-hybrid",
            Self::BlockedRemoteApi => "blocked-remote-api",
            Self::UnsupportedSql => "unsupported-sql",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DossierStatus {
    Partial,
    Planned,
    Blocked,
    Unsupported,
}

impl DossierStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Planned => "planned",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
        }
    }
}

struct WorkloadCertificationDossier {
    scenario: DossierScenario,
    workload_id: &'static str,
    workload_scope: &'static str,
    workload_summary: &'static str,
    overall_status: DossierStatus,
    correctness_status: &'static str,
    benchmark_status: &'static str,
    execution_certificate_status: &'static str,
    native_io_certificate_status: &'static str,
    capability_evidence_status: &'static str,
    workflow_evidence_status: &'static str,
    engine_evidence_status: &'static str,
    api_evidence_status: &'static str,
    certificate_refs: &'static str,
    missing_evidence: &'static str,
    blocked_evidence: &'static str,
    unsupported_evidence: &'static str,
    blocker_ids: &'static str,
    suggested_next_action: &'static str,
}

impl WorkloadCertificationDossier {
    fn for_scenario(scenario: DossierScenario) -> Self {
        match scenario {
            DossierScenario::LocalVortexCount => Self {
                scenario,
                workload_id: "workload://cg7/local-vortex-count",
                workload_scope: "local_vortex_count",
                workload_summary: "local Vortex count fixture dossier",
                overall_status: DossierStatus::Partial,
                correctness_status: "certified",
                benchmark_status: "blocked",
                execution_certificate_status: "certified",
                native_io_certificate_status: "certified",
                capability_evidence_status: "report_only",
                workflow_evidence_status: "report_only",
                engine_evidence_status: "partial",
                api_evidence_status: "planned",
                certificate_refs: "certificates/cg16/local-vortex-count/execution.json,certificates/cg19/local-vortex-count/native-io.json",
                missing_evidence: "claim_grade_benchmark_results,api_contract_workload_mapping",
                blocked_evidence: "cg6.benchmark.claim_grade_results_missing",
                unsupported_evidence: "none",
                blocker_ids: "cg6.benchmark.claim_grade_results_missing,cg23.api.workload_mapping_planned",
                suggested_next_action: "Run benchmark-claim-evidence-plan and rest-api-contract-plan before publishing this workload as certified.",
            },
            DossierScenario::PlannedLiveHybrid => Self {
                scenario,
                workload_id: "workload://cg22/planned-live-hybrid",
                workload_scope: "live_hybrid_fixture",
                workload_summary: "planned live/hybrid workload dossier",
                overall_status: DossierStatus::Planned,
                correctness_status: "planned",
                benchmark_status: "planned",
                execution_certificate_status: "planned",
                native_io_certificate_status: "planned",
                capability_evidence_status: "report_only",
                workflow_evidence_status: "report_only",
                engine_evidence_status: "partial",
                api_evidence_status: "planned",
                certificate_refs: "none",
                missing_evidence: "state_certificate,durable_checkpoint_store,benchmark_evidence,api_event_stream_certificate",
                blocked_evidence: "none",
                unsupported_evidence: "none",
                blocker_ids: "cg22.engine.live.durable_checkpoint_store,cg22.engine.hybrid.object_store_commit_protocol",
                suggested_next_action: "Use live-change-contract-plan, engine-capability-matrix, and rest-api-event-stream before promoting live/hybrid claims.",
            },
            DossierScenario::BlockedRemoteApi => Self {
                scenario,
                workload_id: "workload://cg23/blocked-remote-api",
                workload_scope: "remote_api_object_store",
                workload_summary: "blocked remote API workload dossier",
                overall_status: DossierStatus::Blocked,
                correctness_status: "blocked",
                benchmark_status: "blocked",
                execution_certificate_status: "blocked",
                native_io_certificate_status: "blocked",
                capability_evidence_status: "report_only",
                workflow_evidence_status: "blocked",
                engine_evidence_status: "blocked",
                api_evidence_status: "blocked",
                certificate_refs: "none",
                missing_evidence: "object_store_certificate,remote_execution_policy,native_io_certificate,execution_certificate",
                blocked_evidence: "cg23.remote_api.remote_object_store.unsupported,cg19.native_io.remote_object_store_certificate_missing",
                unsupported_evidence: "remote_object_store_execution",
                blocker_ids: "cg23.remote_api.remote_object_store.unsupported,cg19.native_io.remote_object_store_certificate_missing",
                suggested_next_action: "Use rest-api-plan-preview blocked-remote-object-store and object-store planning reports before requesting remote execution.",
            },
            DossierScenario::UnsupportedSql => Self {
                scenario,
                workload_id: "workload://cg21/unsupported-sql",
                workload_scope: "sql_frontend",
                workload_summary: "unsupported SQL workload dossier",
                overall_status: DossierStatus::Unsupported,
                correctness_status: "unsupported",
                benchmark_status: "unsupported",
                execution_certificate_status: "blocked",
                native_io_certificate_status: "blocked",
                capability_evidence_status: "report_only",
                workflow_evidence_status: "unsupported",
                engine_evidence_status: "blocked",
                api_evidence_status: "unsupported",
                certificate_refs: "none",
                missing_evidence: "sql_parser,binder,semantic_profile,operator_capability_matrix,execution_certificate,native_io_certificate",
                blocked_evidence: "cg21.workflow.sql.frontend_unsupported",
                unsupported_evidence: "sql_frontend",
                blocker_ids: "cg21.workflow.sql.frontend_unsupported,cg23.remote_api.plan_preview.unsupported_operator",
                suggested_next_action: "Use workflow-unsupported-plan sql and capabilities cross-cg to inspect unsupported SQL blockers.",
            },
        }
    }

    const fn command_status(&self) -> CommandStatus {
        match self.overall_status {
            DossierStatus::Partial | DossierStatus::Planned => CommandStatus::Success,
            DossierStatus::Blocked | DossierStatus::Unsupported => CommandStatus::Unsupported,
        }
    }

    fn exit_code(&self) -> ExitCode {
        match self.overall_status {
            DossierStatus::Partial | DossierStatus::Planned => ExitCode::SUCCESS,
            DossierStatus::Blocked | DossierStatus::Unsupported => ExitCode::from(1),
        }
    }

    fn diagnostics(&self) -> Vec<Diagnostic> {
        match self.overall_status {
            DossierStatus::Blocked => vec![Diagnostic::unsupported(
                DiagnosticCode::ObjectStoreUnsupported,
                "workload_certification_dossier",
                "Workload certification dossier is blocked before execution.",
                Some(self.suggested_next_action.to_string()),
            )],
            DossierStatus::Unsupported => vec![Diagnostic::unsupported(
                DiagnosticCode::UnsupportedSql,
                "workload_certification_dossier",
                "Workload certification dossier contains unsupported SQL evidence.",
                Some(self.suggested_next_action.to_string()),
            )],
            DossierStatus::Partial | DossierStatus::Planned => vec![],
        }
    }

    fn human_text(&self) -> String {
        format!(
            "workload certification dossier\nworkload: {}\nstatus: {}\nblockers: {}\nfallback execution: disabled\nruntime execution: false\nside effects: none",
            self.workload_id,
            self.overall_status.as_str(),
            self.blocker_ids
        )
    }

    fn fields(&self) -> Vec<(String, String)> {
        let mut fields = vec![];
        push_field(&mut fields, "mode", "workload_certification_dossier");
        push_field(
            &mut fields,
            "schema_version",
            "shardloom.workload_certification_dossier.v1",
        );
        push_field(
            &mut fields,
            "report_id",
            "cg21_cg22_cg23.workload_certification_dossier",
        );
        push_field(&mut fields, "scenario", self.scenario.as_str());
        push_field(&mut fields, "workload_id", self.workload_id);
        push_field(&mut fields, "workload_scope", self.workload_scope);
        push_field(&mut fields, "workload_summary", self.workload_summary);
        push_field(&mut fields, "overall_status", self.overall_status.as_str());
        push_field(
            &mut fields,
            "status_vocabulary",
            "certified,partial,planned,report_only,blocked,unsupported",
        );
        push_bool_field(&mut fields, "claim_allowed", false);
        push_bool_field(&mut fields, "production_claim_allowed", false);
        push_field(
            &mut fields,
            "cg5_correctness_status",
            self.correctness_status,
        );
        push_field(&mut fields, "cg6_benchmark_status", self.benchmark_status);
        push_field(
            &mut fields,
            "cg16_execution_certificate_status",
            self.execution_certificate_status,
        );
        push_field(
            &mut fields,
            "cg19_native_io_certificate_status",
            self.native_io_certificate_status,
        );
        push_field(
            &mut fields,
            "cg20_capability_evidence_status",
            self.capability_evidence_status,
        );
        push_field(
            &mut fields,
            "cg21_workflow_evidence_status",
            self.workflow_evidence_status,
        );
        push_field(
            &mut fields,
            "cg22_engine_evidence_status",
            self.engine_evidence_status,
        );
        push_field(
            &mut fields,
            "cg23_api_evidence_status",
            self.api_evidence_status,
        );
        push_field(&mut fields, "certificate_refs", self.certificate_refs);
        push_field(&mut fields, "missing_evidence", self.missing_evidence);
        push_field(&mut fields, "blocked_evidence", self.blocked_evidence);
        push_field(
            &mut fields,
            "unsupported_evidence",
            self.unsupported_evidence,
        );
        push_field(&mut fields, "blocker_ids", self.blocker_ids);
        push_field(
            &mut fields,
            "suggested_next_action",
            self.suggested_next_action,
        );
        push_field(
            &mut fields,
            "source_evidence_surfaces",
            "correctness-plan,benchmark-claim-evidence-plan,execution-certificate-plan,native-io-envelope-plan,capabilities cross-cg,workflow-unsupported-plan,engine-capability-matrix,rest-api-plan-preview",
        );
        push_bool_field(&mut fields, "plan_only", true);
        push_bool_field(&mut fields, "runtime_execution", false);
        push_bool_field(&mut fields, "query_execution", false);
        push_bool_field(&mut fields, "data_read", false);
        push_bool_field(&mut fields, "data_materialized", false);
        push_bool_field(&mut fields, "write_io", false);
        push_bool_field(&mut fields, "object_store_io", false);
        push_bool_field(&mut fields, "network_probe", false);
        push_bool_field(&mut fields, "catalog_probe", false);
        push_bool_field(&mut fields, "external_engine_invoked", false);
        push_bool_field(&mut fields, "external_effects_executed", false);
        push_bool_field(&mut fields, "fallback_execution_allowed", false);
        push_bool_field(&mut fields, "fallback_attempted", false);
        push_bool_field(&mut fields, "no_runtime", true);
        push_bool_field(&mut fields, "no_fallback", true);
        push_bool_field(&mut fields, "no_effects", true);
        fields
    }
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}
