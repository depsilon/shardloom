//! Fixture-scoped table/lakehouse runtime smoke handlers.
//!
//! This module deliberately admits only a local-manifest append commit rehearsal
//! over a ShardLoom-owned in-memory fixture. It writes a staged manifest JSON
//! and sidecar commit record to a caller-provided local path, optionally rolls
//! them back for cleanup proof, and keeps catalog, object-store, external
//! table-format dependencies, and production lakehouse claims blocked.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use shardloom_core::{
    CommandStatus, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, OutputFormat, ShardLoomError,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const COMMAND: &str = "local-table-append-commit-rehearsal-smoke";
const SCHEMA_VERSION: &str = "shardloom.local_table_append_commit_rehearsal_smoke.v1";
const DEFAULT_PROFILE: &str = "local-manifest";
const FIXTURE_ID: &str = "gar-runtime-impl-4o-local-table-append-commit";
const REPORT_ID: &str = "gar-runtime-impl-4o.local_table_append_commit_rehearsal_smoke";
const GAR_ID: &str = "GAR-RUNTIME-IMPL-4O";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalTableAppendCommitStatus {
    Committed,
    RolledBack,
    BlockedUnsupportedProfile,
    BlockedRemoteProvider,
    BlockedInvalidTarget,
    BlockedTargetExists,
    BlockedWriteError,
}

impl LocalTableAppendCommitStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedInvalidTarget => "blocked_invalid_target",
            Self::BlockedTargetExists => "blocked_target_exists",
            Self::BlockedWriteError => "blocked_write_error",
        }
    }

    const fn is_error(self) -> bool {
        !matches!(self, Self::Committed | Self::RolledBack)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalTableAppendCommitReport {
    status: LocalTableAppendCommitStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    target_uri: String,
    target_path: Option<PathBuf>,
    staging_path: Option<PathBuf>,
    commit_record_path: Option<PathBuf>,
    idempotency_key: String,
    idempotency_status: &'static str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    base_manifest_id: &'static str,
    append_manifest_id: &'static str,
    committed_manifest_id: &'static str,
    base_snapshot_id: &'static str,
    append_snapshot_id: &'static str,
    committed_snapshot_id: &'static str,
    schema_id: &'static str,
    base_row_count: usize,
    append_row_count: usize,
    effective_row_count: usize,
    base_manifest_file_count: usize,
    append_manifest_file_count: usize,
    committed_manifest_file_count: usize,
    base_manifest_segment_count: usize,
    append_manifest_segment_count: usize,
    committed_manifest_segment_count: usize,
    cleanup_deleted_count: usize,
    manifest_bytes: usize,
    written_bytes: usize,
    manifest_payload_digest: String,
    committed_manifest_digest: String,
    commit_record_digest: String,
    correctness_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalTableAppendCommitOutcome {
    status: LocalTableAppendCommitStatus,
    cleanup_deleted_count: usize,
    written_bytes: usize,
    committed_manifest_digest: String,
    commit_record_digest: String,
}

impl LocalTableAppendCommitReport {
    fn blocked(
        status: LocalTableAppendCommitStatus,
        provider_profile: impl Into<String>,
        target_uri: impl Into<String>,
        allow_overwrite: bool,
        rollback_after_commit: bool,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: provider_profile.into(),
            target_uri: target_uri.into(),
            target_path: None,
            staging_path: None,
            commit_record_path: None,
            idempotency_key: "not_emitted_blocked".to_string(),
            idempotency_status: "not_emitted_blocked",
            allow_overwrite,
            rollback_after_commit,
            base_manifest_id: "gar-runtime-4o-base-manifest-v1",
            append_manifest_id: "gar-runtime-4o-append-manifest-v1",
            committed_manifest_id: "not_emitted_no_manifest_write",
            base_snapshot_id: "gar-runtime-4o-base-snapshot-0001",
            append_snapshot_id: "gar-runtime-4o-append-snapshot-0002",
            committed_snapshot_id: "not_emitted_no_commit",
            schema_id: "gar-runtime-4o-orders-schema",
            base_row_count: 0,
            append_row_count: 0,
            effective_row_count: 0,
            base_manifest_file_count: 0,
            append_manifest_file_count: 0,
            committed_manifest_file_count: 0,
            base_manifest_segment_count: 0,
            append_manifest_segment_count: 0,
            committed_manifest_segment_count: 0,
            cleanup_deleted_count: 0,
            manifest_bytes: 0,
            written_bytes: 0,
            manifest_payload_digest: "not_emitted_no_manifest_write".to_string(),
            committed_manifest_digest: "not_emitted_no_manifest_write".to_string(),
            commit_record_digest: "not_emitted_no_commit_record".to_string(),
            correctness_digest: "not_emitted_no_commit_rehearsal".to_string(),
        }
    }

    fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    fn to_human_text(&self) -> String {
        format!(
            "local_table_append_commit_rehearsal(status={}, profile={}, manifest_bytes={}, rollback_status={}, table_metadata_write_performed={}, object_store_io=false, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.written_bytes,
            rollback_status(self.status, self.rollback_after_commit),
            !self.has_errors(),
            claim_gate_status(self.has_errors())
        )
    }
}

pub(crate) fn handle_local_table_append_commit_rehearsal_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target) = args.next() else {
        return emit_error(
            COMMAND,
            format,
            "local table append commit rehearsal failed",
            &ShardLoomError::InvalidOperation(
                "local-table-append-commit-rehearsal-smoke requires <local-committed-manifest-path>"
                    .to_string(),
            ),
        );
    };

    let mut profile = DEFAULT_PROFILE.to_string();
    let mut idempotency_key: Option<String> = None;
    let mut allow_overwrite = false;
    let mut rollback_after_commit = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        COMMAND,
                        format,
                        "local table append commit rehearsal failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --profile".to_string(),
                        ),
                    );
                };
                profile = value;
            }
            "--idempotency-key" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        COMMAND,
                        format,
                        "local table append commit rehearsal failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --idempotency-key".to_string(),
                        ),
                    );
                };
                let value = value.trim().to_string();
                if value.is_empty() {
                    return emit_error(
                        COMMAND,
                        format,
                        "local table append commit rehearsal failed",
                        &ShardLoomError::InvalidOperation(
                            "idempotency key must not be empty".to_string(),
                        ),
                    );
                }
                idempotency_key = Some(value);
            }
            "--allow-overwrite" => allow_overwrite = true,
            "--rollback-after-commit" => rollback_after_commit = true,
            value => {
                return emit_error(
                    COMMAND,
                    format,
                    "local table append commit rehearsal failed",
                    &cli_unknown_arg_error(COMMAND, value),
                );
            }
        }
    }

    let report = execute_local_table_append_commit_rehearsal(
        &target,
        &profile,
        idempotency_key.as_deref(),
        allow_overwrite,
        rollback_after_commit,
    );
    emit_local_table_append_commit_rehearsal_report(format, &report)
}

#[allow(clippy::too_many_lines)]
fn execute_local_table_append_commit_rehearsal(
    target: &str,
    profile: &str,
    idempotency_key: Option<&str>,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> LocalTableAppendCommitReport {
    if profile != DEFAULT_PROFILE {
        return LocalTableAppendCommitReport::blocked(
            LocalTableAppendCommitStatus::BlockedUnsupportedProfile,
            profile,
            target,
            allow_overwrite,
            rollback_after_commit,
            Diagnostic::new(
                DiagnosticCode::InvalidInput,
                DiagnosticSeverity::Error,
                DiagnosticCategory::InvalidInput,
                "Local table append commit rehearsal profile is not admitted.",
                Some("local_table_append_commit_profile".to_string()),
                Some(format!(
                    "profile {profile} is not admitted for fixture table commit rehearsal"
                )),
                Some("Use --profile local-manifest with a local manifest target path.".to_string()),
                FallbackStatus::disabled_by_policy(),
            ),
        );
    }
    if is_remote_uri(target) {
        return LocalTableAppendCommitReport::blocked(
            LocalTableAppendCommitStatus::BlockedRemoteProvider,
            profile,
            target,
            allow_overwrite,
            rollback_after_commit,
            Diagnostic::object_store_blocked(
                "local_table_append_commit_remote_target",
                "remote table/object-store manifest targets remain blocked; no credential, catalog, provider, or network probe was performed",
                "Use a local manifest target path for this fixture rehearsal.",
            ),
        );
    }

    let target_path = match normalize_local_path(target) {
        Ok(path) => path,
        Err(error) => {
            return local_target_blocker(
                LocalTableAppendCommitStatus::BlockedInvalidTarget,
                target,
                profile,
                allow_overwrite,
                rollback_after_commit,
                error.to_string(),
            );
        }
    };
    if let Err((status, message)) = validate_local_manifest_target(&target_path, allow_overwrite) {
        return local_target_blocker(
            status,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            message,
        );
    }

    let manifest_payload = build_committed_manifest_payload();
    let manifest_payload_digest = fnv64_digest(&manifest_payload);
    let (resolved_key, idempotency_status) =
        resolved_idempotency_key(idempotency_key, target, &manifest_payload_digest);
    let staging_path = staging_manifest_path(&target_path, &resolved_key);
    let commit_record_path = commit_record_sidecar_path(&target_path);
    let correctness_digest = fixture_correctness_digest();

    let outcome = match perform_local_manifest_append_commit(
        target,
        &target_path,
        &staging_path,
        &commit_record_path,
        &resolved_key,
        &manifest_payload,
        &manifest_payload_digest,
        &correctness_digest,
        allow_overwrite,
        rollback_after_commit,
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            return write_error_blocker(
                target,
                profile,
                allow_overwrite,
                rollback_after_commit,
                &error,
            );
        }
    };

    LocalTableAppendCommitReport {
        status: outcome.status,
        diagnostics: vec![],
        provider_profile: profile.to_string(),
        target_uri: target.to_string(),
        target_path: Some(target_path),
        staging_path: Some(staging_path),
        commit_record_path: Some(commit_record_path),
        idempotency_key: resolved_key,
        idempotency_status,
        allow_overwrite,
        rollback_after_commit,
        base_manifest_id: "gar-runtime-4o-base-manifest-v1",
        append_manifest_id: "gar-runtime-4o-append-manifest-v1",
        committed_manifest_id: "gar-runtime-4o-committed-manifest-v2",
        base_snapshot_id: "gar-runtime-4o-base-snapshot-0001",
        append_snapshot_id: "gar-runtime-4o-append-snapshot-0002",
        committed_snapshot_id: "gar-runtime-4o-committed-snapshot-0002",
        schema_id: "gar-runtime-4o-orders-schema",
        base_row_count: 3,
        append_row_count: 2,
        effective_row_count: 5,
        base_manifest_file_count: 1,
        append_manifest_file_count: 1,
        committed_manifest_file_count: 2,
        base_manifest_segment_count: 1,
        append_manifest_segment_count: 1,
        committed_manifest_segment_count: 2,
        cleanup_deleted_count: outcome.cleanup_deleted_count,
        manifest_bytes: manifest_payload.len(),
        written_bytes: outcome.written_bytes,
        manifest_payload_digest,
        committed_manifest_digest: outcome.committed_manifest_digest,
        commit_record_digest: outcome.commit_record_digest,
        correctness_digest,
    }
}

fn emit_local_table_append_commit_rehearsal_report(
    format: OutputFormat,
    report: &LocalTableAppendCommitReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    emit(
        COMMAND,
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "local table append commit rehearsal smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        local_table_append_commit_rehearsal_fields(report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn local_table_append_commit_rehearsal_fields(
    report: &LocalTableAppendCommitReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_identity_fields(&mut fields, report);
    push_snapshot_manifest_fields(&mut fields, report);
    push_commit_fields(&mut fields, report);
    push_policy_boundary_fields(&mut fields, report);
    fields
}

fn push_identity_fields(fields: &mut Vec<(String, String)>, report: &LocalTableAppendCommitReport) {
    push_field(fields, "schema_version", SCHEMA_VERSION);
    push_field(fields, "mode", "local_table_append_commit_rehearsal_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_table_append_commit_rehearsal",
    );
    push_field(fields, "report_id", REPORT_ID);
    push_field(fields, "gar_id", GAR_ID);
    push_field(fields, "fixture_id", FIXTURE_ID);
    push_field(
        fields,
        "support_status",
        support_status(report.has_errors()),
    );
    push_field(
        fields,
        "claim_gate_status",
        claim_gate_status(report.has_errors()),
    );
    push_field(
        fields,
        "claim_boundary",
        "local-manifest fixture table append commit rehearsal only; no Iceberg/Delta/Hudi production runtime, catalog service, object-store table commit, merge/update/delete, distributed, performance, or Spark-replacement claim",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "table_metadata_adapter", "local_manifest_fixture");
    push_field(fields, "table_format", "shardloom_local_manifest");
    push_field(fields, "catalog_kind", "local_manifest");
    push_field(fields, "target_uri", &report.target_uri);
    push_field(
        fields,
        "local_manifest_target_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(fields, "table_append_commit_status", report.status.as_str());
}

fn push_snapshot_manifest_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableAppendCommitReport,
) {
    push_field(fields, "base_manifest_id", report.base_manifest_id);
    push_field(fields, "append_manifest_id", report.append_manifest_id);
    push_field(
        fields,
        "committed_manifest_id",
        report.committed_manifest_id,
    );
    push_field(fields, "base_snapshot_id", report.base_snapshot_id);
    push_field(fields, "append_snapshot_id", report.append_snapshot_id);
    push_field(
        fields,
        "committed_snapshot_id",
        report.committed_snapshot_id,
    );
    push_field(fields, "snapshot_id", report.committed_snapshot_id);
    push_field(fields, "schema_id", report.schema_id);
    push_count_field(fields, "base_row_count", report.base_row_count);
    push_count_field(fields, "append_row_count", report.append_row_count);
    push_count_field(fields, "effective_row_count", report.effective_row_count);
    push_count_field(
        fields,
        "base_manifest_file_count",
        report.base_manifest_file_count,
    );
    push_count_field(
        fields,
        "append_manifest_file_count",
        report.append_manifest_file_count,
    );
    push_count_field(
        fields,
        "committed_manifest_file_count",
        report.committed_manifest_file_count,
    );
    push_count_field(
        fields,
        "manifest_file_count",
        report.committed_manifest_file_count,
    );
    push_count_field(
        fields,
        "base_manifest_segment_count",
        report.base_manifest_segment_count,
    );
    push_count_field(
        fields,
        "append_manifest_segment_count",
        report.append_manifest_segment_count,
    );
    push_count_field(
        fields,
        "committed_manifest_segment_count",
        report.committed_manifest_segment_count,
    );
    push_count_field(
        fields,
        "manifest_segment_count",
        report.committed_manifest_segment_count,
    );
    push_field(
        fields,
        "append_operation_status",
        append_operation_status(report.status),
    );
    push_field(
        fields,
        "snapshot_reader_status",
        snapshot_reader_status(report.status),
    );
    push_field(
        fields,
        "manifest_payload_digest",
        &report.manifest_payload_digest,
    );
    push_field(
        fields,
        "committed_manifest_digest",
        &report.committed_manifest_digest,
    );
    push_field(fields, "correctness_digest", &report.correctness_digest);
}

fn push_commit_fields(fields: &mut Vec<(String, String)>, report: &LocalTableAppendCommitReport) {
    push_field(
        fields,
        "write_mode",
        "local_manifest_staged_append_commit_rehearsal",
    );
    push_field(
        fields,
        "write_staging_status",
        write_staging_status(report.status),
    );
    push_field(
        fields,
        "commit_protocol",
        "local_manifest_sidecar_commit_record",
    );
    push_field(
        fields,
        "commit_protocol_status",
        commit_protocol_status(report.status),
    );
    push_field(fields, "commit_status", commit_status(report.status));
    push_field(
        fields,
        "table_commit_rehearsal_status",
        table_commit_rehearsal_status(report.status),
    );
    push_field(
        fields,
        "rollback_status",
        rollback_status(report.status, report.rollback_after_commit),
    );
    push_field(
        fields,
        "cleanup_status",
        cleanup_status(report.status, report.rollback_after_commit),
    );
    push_count_field(
        fields,
        "cleanup_deleted_count",
        report.cleanup_deleted_count,
    );
    push_field(fields, "idempotency_key", &report.idempotency_key);
    push_field(fields, "idempotency_status", report.idempotency_status);
    push_bool_field(fields, "allow_overwrite", report.allow_overwrite);
    push_bool_field(fields, "rollback_requested", report.rollback_after_commit);
    push_field(
        fields,
        "staged_manifest_path",
        &path_field(report.staging_path.as_deref()),
    );
    push_field(
        fields,
        "committed_manifest_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(
        fields,
        "commit_record_path",
        &path_field(report.commit_record_path.as_deref()),
    );
    push_bool_field(fields, "manifest_written", !report.has_errors());
    push_bool_field(
        fields,
        "committed_manifest_present",
        matches!(report.status, LocalTableAppendCommitStatus::Committed),
    );
    push_bool_field(
        fields,
        "commit_record_present",
        matches!(report.status, LocalTableAppendCommitStatus::Committed),
    );
    push_count_field(fields, "manifest_bytes", report.manifest_bytes);
    push_count_field(fields, "written_bytes", report.written_bytes);
    push_field(fields, "commit_record_digest", &report.commit_record_digest);
}

fn push_policy_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableAppendCommitReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "credential_policy_status",
        "not_required_local_manifest_fixture",
    );
    push_field(
        fields,
        "network_effect_status",
        "not_required_local_manifest_fixture",
    );
    push_bool_field(fields, "credential_resolution_allowed", false);
    push_bool_field(fields, "credential_resolution_performed", false);
    push_bool_field(fields, "network_probe_allowed", false);
    push_bool_field(fields, "network_probe_performed", false);
    push_bool_field(fields, "provider_probe_allowed", false);
    push_bool_field(fields, "provider_probe_performed", false);
    push_bool_field(fields, "external_catalog_resolution_performed", false);
    push_bool_field(fields, "catalog_io_performed", false);
    push_bool_field(fields, "table_metadata_read_performed", !has_errors);
    push_bool_field(
        fields,
        "local_manifest_metadata_read_performed",
        !has_errors,
    );
    push_bool_field(fields, "table_metadata_write_performed", !has_errors);
    push_bool_field(fields, "manifest_write_performed", !has_errors);
    push_bool_field(fields, "transaction_execution_performed", false);
    push_bool_field(fields, "commit_rehearsal_performed", !has_errors);
    push_bool_field(fields, "commit_execution_performed", false);
    push_bool_field(fields, "table_catalog_commit_performed", false);
    push_bool_field(fields, "object_store_io_performed", false);
    push_bool_field(fields, "object_store_io", false);
    push_bool_field(fields, "object_store_write_io", false);
    push_bool_field(fields, "data_file_read_performed", false);
    push_bool_field(fields, "source_io_performed", false);
    push_bool_field(fields, "write_io_performed", !has_errors);
    push_bool_field(fields, "write_io", !has_errors);
    push_bool_field(fields, "local_manifest_write_io", !has_errors);
    push_bool_field(fields, "external_table_format_dependency_invoked", false);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-4o.local_table_append_commit_rehearsal.native_io",
    );
    push_field(
        fields,
        "native_io_certificate_status",
        if has_errors {
            "blocked"
        } else {
            "fixture_smoke_only"
        },
    );
    push_bool_field(fields, "table_runtime_supported", !has_errors);
    push_bool_field(
        fields,
        "table_append_commit_rehearsal_supported",
        !has_errors,
    );
    push_bool_field(fields, "table_commit_allowed", false);
    push_bool_field(fields, "production_table_catalog_claim_allowed", false);
    push_bool_field(fields, "lakehouse_claim_allowed", false);
    push_bool_field(fields, "performance_claim_allowed", false);
    push_field(
        fields,
        "execution",
        if has_errors { "blocked" } else { "performed" },
    );
    push_bool_field(fields, "plan_only", false);
}

#[allow(clippy::too_many_arguments)]
fn perform_local_manifest_append_commit(
    target_uri: &str,
    target_path: &Path,
    staging_path: &Path,
    commit_record_path: &Path,
    idempotency_key: &str,
    manifest_payload: &str,
    manifest_payload_digest: &str,
    correctness_digest: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> std::io::Result<LocalTableAppendCommitOutcome> {
    if let Some(staging_parent) = staging_path.parent() {
        fs::create_dir_all(staging_parent)?;
    }
    remove_file_if_exists(staging_path)?;
    fs::write(staging_path, manifest_payload)?;
    if allow_overwrite {
        remove_file_if_exists(target_path)?;
        remove_file_if_exists(commit_record_path)?;
    }
    if target_path.exists() {
        remove_file_if_exists(staging_path)?;
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "local table manifest target already exists",
        ));
    }
    fs::rename(staging_path, target_path)?;
    let committed_manifest = match fs::read_to_string(target_path) {
        Ok(content) => content,
        Err(error) => {
            let _ = remove_file_if_exists(target_path);
            return Err(error);
        }
    };
    let committed_manifest_digest = fnv64_digest(&committed_manifest);
    let commit_record = build_commit_record(
        target_uri,
        target_path,
        idempotency_key,
        manifest_payload.len(),
        manifest_payload_digest,
        &committed_manifest_digest,
        correctness_digest,
    );
    let commit_record_digest = fnv64_digest(&commit_record);
    if let Err(error) = fs::write(commit_record_path, commit_record) {
        let _ = remove_file_if_exists(target_path);
        return Err(error);
    }
    let mut cleanup_deleted_count = 0;
    let status = if rollback_after_commit {
        cleanup_deleted_count += usize::from(remove_file_if_exists(target_path)?);
        cleanup_deleted_count += usize::from(remove_file_if_exists(commit_record_path)?);
        LocalTableAppendCommitStatus::RolledBack
    } else {
        LocalTableAppendCommitStatus::Committed
    };
    Ok(LocalTableAppendCommitOutcome {
        status,
        cleanup_deleted_count,
        written_bytes: manifest_payload.len(),
        committed_manifest_digest,
        commit_record_digest,
    })
}

fn validate_local_manifest_target(
    target_path: &Path,
    allow_overwrite: bool,
) -> Result<(), (LocalTableAppendCommitStatus, String)> {
    let Some(parent) = target_path.parent() else {
        return Err((
            LocalTableAppendCommitStatus::BlockedInvalidTarget,
            "local manifest target must have a parent directory".to_string(),
        ));
    };
    if !parent.as_os_str().is_empty() && !parent.is_dir() {
        return Err((
            LocalTableAppendCommitStatus::BlockedInvalidTarget,
            format!(
                "local manifest target parent does not exist or is not a directory: {}",
                parent.to_string_lossy()
            ),
        ));
    }
    if target_path.is_dir() {
        return Err((
            LocalTableAppendCommitStatus::BlockedInvalidTarget,
            "local manifest target is a directory, not a manifest file path".to_string(),
        ));
    }
    if target_path.exists() && !allow_overwrite {
        return Err((
            LocalTableAppendCommitStatus::BlockedTargetExists,
            "local manifest target already exists".to_string(),
        ));
    }
    Ok(())
}

fn local_target_blocker(
    status: LocalTableAppendCommitStatus,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    message: impl Into<String>,
) -> LocalTableAppendCommitReport {
    LocalTableAppendCommitReport::blocked(
        status,
        profile,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::invalid_input(
            "local_table_append_commit_target_path",
            message.into(),
            "Use a local manifest target path whose parent directory exists; pass --allow-overwrite to replace an existing rehearsal artifact.",
        ),
    )
}

fn write_error_blocker(
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    error: &std::io::Error,
) -> LocalTableAppendCommitReport {
    LocalTableAppendCommitReport::blocked(
        LocalTableAppendCommitStatus::BlockedWriteError,
        profile,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::new(
            DiagnosticCode::CommitNotAtomic,
            DiagnosticSeverity::Error,
            DiagnosticCategory::Execution,
            "Local table append commit rehearsal failed.",
            Some("local_table_append_commit_rehearsal".to_string()),
            Some(error.to_string()),
            Some(
                "Retry with a writable local manifest target path and no remote provider."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn build_committed_manifest_payload() -> String {
    concat!(
        "{\n",
        "  \"schema_version\": \"shardloom.local_table_committed_manifest.v1\",\n",
        "  \"table_format\": \"shardloom_local_manifest\",\n",
        "  \"fixture_id\": \"gar-runtime-impl-4o-local-table-append-commit\",\n",
        "  \"base_manifest_id\": \"gar-runtime-4o-base-manifest-v1\",\n",
        "  \"append_manifest_id\": \"gar-runtime-4o-append-manifest-v1\",\n",
        "  \"committed_manifest_id\": \"gar-runtime-4o-committed-manifest-v2\",\n",
        "  \"base_snapshot_id\": \"gar-runtime-4o-base-snapshot-0001\",\n",
        "  \"append_snapshot_id\": \"gar-runtime-4o-append-snapshot-0002\",\n",
        "  \"committed_snapshot_id\": \"gar-runtime-4o-committed-snapshot-0002\",\n",
        "  \"schema_id\": \"gar-runtime-4o-orders-schema\",\n",
        "  \"operation\": \"append_only_commit_rehearsal\",\n",
        "  \"files\": [\n",
        "    {\"uri\": \"file://fixtures/gar-runtime-4o/orders/base-part.vortex\", \"snapshot_id\": \"gar-runtime-4o-base-snapshot-0001\", \"row_count\": 3},\n",
        "    {\"uri\": \"file://fixtures/gar-runtime-4o/orders/append-part.vortex\", \"snapshot_id\": \"gar-runtime-4o-append-snapshot-0002\", \"row_count\": 2}\n",
        "  ],\n",
        "  \"segments\": [\n",
        "    {\"segment_id\": \"gar-runtime-4o-segment-base\", \"snapshot_id\": \"gar-runtime-4o-base-snapshot-0001\", \"row_count\": 3},\n",
        "    {\"segment_id\": \"gar-runtime-4o-segment-append\", \"snapshot_id\": \"gar-runtime-4o-append-snapshot-0002\", \"row_count\": 2}\n",
        "  ],\n",
        "  \"base_row_count\": 3,\n",
        "  \"append_row_count\": 2,\n",
        "  \"effective_row_count\": 5,\n",
        "  \"fallback_attempted\": false,\n",
        "  \"external_engine_invoked\": false,\n",
        "  \"object_store_io_performed\": false,\n",
        "  \"table_catalog_commit_performed\": false\n",
        "}\n"
    )
    .to_string()
}

#[allow(clippy::too_many_arguments)]
fn build_commit_record(
    target_uri: &str,
    target_path: &Path,
    idempotency_key: &str,
    manifest_bytes: usize,
    manifest_payload_digest: &str,
    committed_manifest_digest: &str,
    correctness_digest: &str,
) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"schema_version\": \"shardloom.local_table_commit_rehearsal_record.v1\",\n",
            "  \"commit_protocol\": \"local_manifest_sidecar_commit_record\",\n",
            "  \"operation\": \"append_only_commit_rehearsal\",\n",
            "  \"target_uri\": \"{}\",\n",
            "  \"local_manifest_path\": \"{}\",\n",
            "  \"idempotency_key\": \"{}\",\n",
            "  \"manifest_bytes\": {},\n",
            "  \"manifest_payload_digest\": \"{}\",\n",
            "  \"committed_manifest_digest\": \"{}\",\n",
            "  \"correctness_digest\": \"{}\",\n",
            "  \"fallback_attempted\": false,\n",
            "  \"external_engine_invoked\": false,\n",
            "  \"object_store_io_performed\": false,\n",
            "  \"table_catalog_commit_performed\": false\n",
            "}}\n"
        ),
        escape_json(target_uri),
        escape_json(&target_path.to_string_lossy()),
        escape_json(idempotency_key),
        manifest_bytes,
        escape_json(manifest_payload_digest),
        escape_json(committed_manifest_digest),
        escape_json(correctness_digest),
    )
}

fn fixture_correctness_digest() -> String {
    fnv64_digest(
        "fixture_id=gar-runtime-impl-4o-local-table-append-commit base_rows=3 append_rows=2 effective_rows=5 files=2 segments=2 operation=append_only_commit_rehearsal fallback_attempted=false external_engine_invoked=false",
    )
}

fn resolved_idempotency_key(
    idempotency_key: Option<&str>,
    target: &str,
    manifest_payload_digest: &str,
) -> (String, &'static str) {
    if let Some(key) = idempotency_key {
        return (key.to_string(), "caller_supplied");
    }
    (
        fnv64_digest(&format!("{target}|{manifest_payload_digest}")),
        "derived_from_manifest_digest",
    )
}

fn staging_manifest_path(target_path: &Path, idempotency_key: &str) -> PathBuf {
    let parent = target_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = target_path
        .file_name()
        .map_or_else(|| "table-manifest".into(), |name| name.to_string_lossy());
    parent.join(".shardloom-table-staging").join(format!(
        "{}.{}.tmp",
        file_name,
        sanitize_idempotency_key(idempotency_key)
    ))
}

fn commit_record_sidecar_path(target_path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.shardloom-table-commit.json",
        target_path.to_string_lossy()
    ))
}

fn remove_file_if_exists(path: &Path) -> std::io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn is_remote_uri(source: &str) -> bool {
    let Some((scheme, _)) = source.split_once("://") else {
        return false;
    };
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "s3" | "gs" | "gcs" | "abfs" | "abfss"
    )
}

fn normalize_local_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local manifest target path must not be empty".to_string(),
        ));
    }
    if let Some((scheme, rest)) = raw.split_once("://") {
        if !scheme.eq_ignore_ascii_case("file") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "unsupported URI scheme for local table manifest path: {raw}"
            )));
        }
        if rest.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "file:// path must include a local path".to_string(),
            ));
        }
        return Ok(local_file_uri_path(rest));
    }
    Ok(PathBuf::from(raw))
}

fn local_file_uri_path(rest: &str) -> PathBuf {
    let local_rest = if let Some(path) = rest.strip_prefix("localhost/") {
        format!("/{path}")
    } else {
        rest.to_string()
    };
    if local_rest.len() >= 3 && local_rest.as_bytes()[0] == b'/' && local_rest.as_bytes()[2] == b':'
    {
        PathBuf::from(&local_rest[1..])
    } else {
        PathBuf::from(local_rest)
    }
}

fn fnv64_digest(value: &str) -> String {
    fnv64_digest_bytes(value.as_bytes())
}

fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            value if value.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", value as u32);
            }
            value => escaped.push(value),
        }
    }
    escaped
}

fn sanitize_idempotency_key(idempotency_key: &str) -> String {
    let sanitized: String = idempotency_key
        .chars()
        .take(64)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "idempotency".to_string()
    } else {
        sanitized
    }
}

fn support_status(has_errors: bool) -> &'static str {
    if has_errors {
        "unsupported"
    } else {
        "fixture_smoke_only"
    }
}

fn claim_gate_status(has_errors: bool) -> &'static str {
    if has_errors {
        "not_claim_grade"
    } else {
        "scoped_local_table_append_commit_rehearsal_only"
    }
}

fn append_operation_status(status: LocalTableAppendCommitStatus) -> &'static str {
    if status.is_error() {
        "blocked"
    } else {
        "append_delta_rehearsed"
    }
}

fn snapshot_reader_status(status: LocalTableAppendCommitStatus) -> &'static str {
    if status.is_error() {
        "blocked"
    } else {
        "performed_in_memory_fixture"
    }
}

fn write_staging_status(status: LocalTableAppendCommitStatus) -> &'static str {
    if status.is_error() {
        "blocked"
    } else {
        "performed_local_manifest"
    }
}

fn commit_protocol_status(status: LocalTableAppendCommitStatus) -> &'static str {
    match status {
        LocalTableAppendCommitStatus::Committed => "committed",
        LocalTableAppendCommitStatus::RolledBack => "rolled_back",
        _ => "blocked",
    }
}

fn commit_status(status: LocalTableAppendCommitStatus) -> &'static str {
    match status {
        LocalTableAppendCommitStatus::Committed => "committed_local_manifest",
        LocalTableAppendCommitStatus::RolledBack => "committed_then_rolled_back",
        _ => "blocked",
    }
}

fn table_commit_rehearsal_status(status: LocalTableAppendCommitStatus) -> &'static str {
    match status {
        LocalTableAppendCommitStatus::Committed => "rehearsed_local_manifest_commit",
        LocalTableAppendCommitStatus::RolledBack => "rehearsed_then_rolled_back",
        _ => "blocked",
    }
}

fn rollback_status(
    status: LocalTableAppendCommitStatus,
    rollback_after_commit: bool,
) -> &'static str {
    match (status, rollback_after_commit) {
        (LocalTableAppendCommitStatus::RolledBack, true) => "performed_local_manifest_cleanup",
        (_, true) if status.is_error() => "blocked",
        _ => "not_requested",
    }
}

fn cleanup_status(
    status: LocalTableAppendCommitStatus,
    rollback_after_commit: bool,
) -> &'static str {
    match (status, rollback_after_commit) {
        (LocalTableAppendCommitStatus::Committed, _) => "staging_manifest_removed",
        (LocalTableAppendCommitStatus::RolledBack, true) => "rollback_cleanup_performed",
        _ => "not_performed_blocked",
    }
}

fn path_field(path: Option<&Path>) -> String {
    path.map_or_else(
        || "not_applicable".to_string(),
        |path| path.to_string_lossy().into_owned(),
    )
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_append_commit_rehearsal_payload_digest_is_stable() {
        let payload = build_committed_manifest_payload();
        assert!(payload.contains("\"operation\": \"append_only_commit_rehearsal\""));
        assert!(payload.contains("\"effective_row_count\": 5"));
        assert_eq!(
            fnv64_digest(&payload),
            execute_digest_without_file_io_for_test()
        );
    }

    #[test]
    fn remote_uri_detection_is_scheme_case_insensitive() {
        assert!(is_remote_uri("S3://bucket/table/manifest.json"));
        assert!(is_remote_uri(
            "ABFSS://container@account.dfs.core.windows.net/table"
        ));
        assert!(!is_remote_uri("file://localhost/tmp/table.json"));
    }

    #[test]
    fn file_uri_normalization_strips_localhost_authority() {
        assert_eq!(
            normalize_local_path("file://localhost/tmp/table-manifest.json").unwrap(),
            PathBuf::from("/tmp/table-manifest.json")
        );
        assert_eq!(
            normalize_local_path("FILE:///C:/tmp/table-manifest.json").unwrap(),
            PathBuf::from("C:/tmp/table-manifest.json")
        );
    }

    fn execute_digest_without_file_io_for_test() -> String {
        fnv64_digest(&build_committed_manifest_payload())
    }
}
