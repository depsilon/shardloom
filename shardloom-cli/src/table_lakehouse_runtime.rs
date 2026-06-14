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
const RECOVERY_COMMAND: &str = "local-table-commit-recovery-smoke";
const RECOVERY_SCHEMA_VERSION: &str = "shardloom.local_table_commit_recovery_smoke.v1";
const DEFAULT_PROFILE: &str = "local-manifest";
const FIXTURE_ID: &str = "gar-runtime-impl-4o-local-table-append-commit";
const REPORT_ID: &str = "gar-runtime-impl-4o.local_table_append_commit_rehearsal_smoke";
const RECOVERY_REPORT_ID: &str = "gar-runtime-impl-6d.local_table_commit_recovery_smoke";
const GAR_ID: &str = "GAR-RUNTIME-IMPL-4O";
const RECOVERY_GAR_ID: &str = "GAR-RUNTIME-IMPL-6D";

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
    commit_record_bytes: usize,
    manifest_payload_digest: String,
    committed_manifest_digest: String,
    commit_record_digest: String,
    correctness_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalTableAppendCommitOutcome {
    status: LocalTableAppendCommitStatus,
    staging_path: PathBuf,
    cleanup_deleted_count: usize,
    written_bytes: usize,
    commit_record_bytes: usize,
    committed_manifest_digest: String,
    commit_record_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalTableCommitRecoveryStatus {
    Recovered,
    BlockedUnsupportedProfile,
    BlockedRemoteProvider,
    BlockedInvalidTarget,
    BlockedMissingManifest,
    BlockedMissingCommitRecord,
    BlockedReadError,
    BlockedRecoveryMismatch,
}

impl LocalTableCommitRecoveryStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Recovered => "recovered",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedInvalidTarget => "blocked_invalid_target",
            Self::BlockedMissingManifest => "blocked_missing_manifest",
            Self::BlockedMissingCommitRecord => "blocked_missing_commit_record",
            Self::BlockedReadError => "blocked_read_error",
            Self::BlockedRecoveryMismatch => "blocked_recovery_mismatch",
        }
    }

    const fn is_error(self) -> bool {
        !matches!(self, Self::Recovered)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalTableCommitRecoveryReport {
    status: LocalTableCommitRecoveryStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    target_uri: String,
    target_path: Option<PathBuf>,
    commit_record_path: Option<PathBuf>,
    expected_idempotency_key: Option<String>,
    recovered_idempotency_key: String,
    idempotency_status: &'static str,
    manifest_bytes: usize,
    commit_record_bytes: usize,
    manifest_digest: String,
    commit_record_digest: String,
    expected_manifest_digest: String,
    recorded_manifest_digest: String,
    expected_correctness_digest: String,
    recorded_correctness_digest: String,
    recorded_target_uri: String,
    recorded_local_manifest_path: String,
    manifest_digest_matched: bool,
    correctness_digest_matched: bool,
    commit_record_scope_matched: LocalTableCommitRecordScopeMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalTableCommitRecordScopeMatch {
    target_uri: bool,
    local_manifest_path: bool,
}

impl LocalTableCommitRecoveryReport {
    fn blocked(
        status: LocalTableCommitRecoveryStatus,
        provider_profile: impl Into<String>,
        target_uri: impl Into<String>,
        expected_idempotency_key: Option<String>,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: provider_profile.into(),
            target_uri: target_uri.into(),
            target_path: None,
            commit_record_path: None,
            expected_idempotency_key,
            recovered_idempotency_key: "not_emitted_blocked".to_string(),
            idempotency_status: "not_emitted_blocked",
            manifest_bytes: 0,
            commit_record_bytes: 0,
            manifest_digest: "not_emitted_no_manifest_replay".to_string(),
            commit_record_digest: "not_emitted_no_commit_record_replay".to_string(),
            expected_manifest_digest: "not_emitted_no_manifest_replay".to_string(),
            recorded_manifest_digest: "not_emitted_no_commit_record_replay".to_string(),
            expected_correctness_digest: fixture_correctness_digest(),
            recorded_correctness_digest: "not_emitted_no_commit_record_replay".to_string(),
            recorded_target_uri: "not_emitted_no_commit_record_replay".to_string(),
            recorded_local_manifest_path: "not_emitted_no_commit_record_replay".to_string(),
            manifest_digest_matched: false,
            correctness_digest_matched: false,
            commit_record_scope_matched: LocalTableCommitRecordScopeMatch {
                target_uri: false,
                local_manifest_path: false,
            },
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
            "local_table_commit_recovery(status={}, profile={}, manifest_digest_matched={}, correctness_digest_matched={}, table_metadata_read_performed={}, object_store_io=false, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.manifest_digest_matched,
            self.correctness_digest_matched,
            !self.has_errors(),
            recovery_claim_gate_status(self.has_errors())
        )
    }
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
            commit_record_bytes: 0,
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

pub(crate) fn handle_local_table_commit_recovery_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target) = args.next() else {
        return emit_error(
            RECOVERY_COMMAND,
            format,
            "local table commit recovery failed",
            &ShardLoomError::InvalidOperation(
                "local-table-commit-recovery-smoke requires <local-committed-manifest-path>"
                    .to_string(),
            ),
        );
    };

    let mut profile = DEFAULT_PROFILE.to_string();
    let mut expected_idempotency_key: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        RECOVERY_COMMAND,
                        format,
                        "local table commit recovery failed",
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
                        RECOVERY_COMMAND,
                        format,
                        "local table commit recovery failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --idempotency-key".to_string(),
                        ),
                    );
                };
                let value = value.trim().to_string();
                if value.is_empty() {
                    return emit_error(
                        RECOVERY_COMMAND,
                        format,
                        "local table commit recovery failed",
                        &ShardLoomError::InvalidOperation(
                            "idempotency key must not be empty".to_string(),
                        ),
                    );
                }
                expected_idempotency_key = Some(value);
            }
            value => {
                return emit_error(
                    RECOVERY_COMMAND,
                    format,
                    "local table commit recovery failed",
                    &cli_unknown_arg_error(RECOVERY_COMMAND, value),
                );
            }
        }
    }

    let report = execute_local_table_commit_recovery_smoke(
        &target,
        &profile,
        expected_idempotency_key.as_deref(),
    );
    emit_local_table_commit_recovery_report(format, &report)
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
    let commit_record_path = commit_record_sidecar_path(&target_path);
    let correctness_digest = fixture_correctness_digest();

    let outcome = match perform_local_manifest_append_commit(
        target,
        &target_path,
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
        staging_path: Some(outcome.staging_path),
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
        commit_record_bytes: outcome.commit_record_bytes,
        manifest_payload_digest,
        committed_manifest_digest: outcome.committed_manifest_digest,
        commit_record_digest: outcome.commit_record_digest,
        correctness_digest,
    }
}

#[allow(clippy::too_many_lines)]
fn execute_local_table_commit_recovery_smoke(
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<&str>,
) -> LocalTableCommitRecoveryReport {
    let expected_idempotency_key = expected_idempotency_key.map(str::to_string);
    if profile != DEFAULT_PROFILE {
        return LocalTableCommitRecoveryReport::blocked(
            LocalTableCommitRecoveryStatus::BlockedUnsupportedProfile,
            profile,
            target,
            expected_idempotency_key,
            Diagnostic::new(
                DiagnosticCode::InvalidInput,
                DiagnosticSeverity::Error,
                DiagnosticCategory::InvalidInput,
                "Local table commit recovery profile is not admitted.",
                Some("local_table_commit_recovery_profile".to_string()),
                Some(format!(
                    "profile {profile} is not admitted for fixture table commit recovery"
                )),
                Some(
                    "Use --profile local-manifest with a local committed manifest path."
                        .to_string(),
                ),
                FallbackStatus::disabled_by_policy(),
            ),
        );
    }
    if is_remote_uri(target) {
        return LocalTableCommitRecoveryReport::blocked(
            LocalTableCommitRecoveryStatus::BlockedRemoteProvider,
            profile,
            target,
            expected_idempotency_key,
            Diagnostic::object_store_blocked(
                "local_table_commit_recovery_remote_target",
                "remote table/object-store recovery remains blocked; no credential, catalog, provider, or network probe was performed",
                "Use a local committed manifest and sidecar commit record for this recovery smoke.",
            ),
        );
    }

    let target_path = match normalize_local_path(target) {
        Ok(path) => path,
        Err(error) => {
            return recovery_target_blocker(
                LocalTableCommitRecoveryStatus::BlockedInvalidTarget,
                target,
                profile,
                expected_idempotency_key,
                error.to_string(),
            );
        }
    };
    if target_path.is_dir() {
        return recovery_target_blocker(
            LocalTableCommitRecoveryStatus::BlockedInvalidTarget,
            target,
            profile,
            expected_idempotency_key,
            "local table recovery target is a directory, not a committed manifest file",
        );
    }
    let commit_record_path = commit_record_sidecar_path(&target_path);
    let manifest_payload = match fs::read_to_string(&target_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return LocalTableCommitRecoveryReport::blocked(
                LocalTableCommitRecoveryStatus::BlockedMissingManifest,
                profile,
                target,
                expected_idempotency_key,
                Diagnostic::new(
                    DiagnosticCode::InvalidInput,
                    DiagnosticSeverity::Error,
                    DiagnosticCategory::InvalidInput,
                    "Local table commit recovery manifest is missing.",
                    Some("local_table_commit_recovery_manifest".to_string()),
                    Some("committed local manifest could not be found".to_string()),
                    Some(
                        "Run local-table-append-commit-rehearsal-smoke without rollback first."
                            .to_string(),
                    ),
                    FallbackStatus::disabled_by_policy(),
                ),
            );
        }
        Err(error) => {
            return recovery_read_error_blocker(
                target,
                profile,
                expected_idempotency_key,
                "local_table_commit_recovery_manifest",
                &error,
            );
        }
    };
    let commit_record = match fs::read_to_string(&commit_record_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return LocalTableCommitRecoveryReport::blocked(
                LocalTableCommitRecoveryStatus::BlockedMissingCommitRecord,
                profile,
                target,
                expected_idempotency_key,
                Diagnostic::new(
                    DiagnosticCode::CommitNotAtomic,
                    DiagnosticSeverity::Error,
                    DiagnosticCategory::Execution,
                    "Local table commit recovery sidecar record is missing.",
                    Some("local_table_commit_recovery_commit_record".to_string()),
                    Some("sidecar commit record could not be found".to_string()),
                    Some("Recover only from a committed local-manifest rehearsal with its sidecar record."
                        .to_string()),
                    FallbackStatus::disabled_by_policy(),
                ),
            );
        }
        Err(error) => {
            return recovery_read_error_blocker(
                target,
                profile,
                expected_idempotency_key,
                "local_table_commit_recovery_commit_record",
                &error,
            );
        }
    };
    let commit_record_bytes = commit_record.len();

    let expected_manifest_payload = build_committed_manifest_payload();
    let expected_manifest_digest = fnv64_digest(&expected_manifest_payload);
    let manifest_digest = fnv64_digest(&manifest_payload);
    let commit_record_digest = fnv64_digest(&commit_record);
    let expected_correctness_digest = fixture_correctness_digest();
    let recorded_manifest_digest =
        extract_json_string_field(&commit_record, "committed_manifest_digest")
            .unwrap_or_else(|| "missing_committed_manifest_digest".to_string());
    let recorded_correctness_digest =
        extract_json_string_field(&commit_record, "correctness_digest")
            .unwrap_or_else(|| "missing_correctness_digest".to_string());
    let recorded_target_uri = extract_json_string_field(&commit_record, "target_uri")
        .unwrap_or_else(|| "missing_target_uri".to_string());
    let recorded_local_manifest_path =
        extract_json_string_field(&commit_record, "local_manifest_path")
            .unwrap_or_else(|| "missing_local_manifest_path".to_string());
    let recovered_idempotency_key = extract_json_string_field(&commit_record, "idempotency_key")
        .unwrap_or_else(|| "missing_idempotency_key".to_string());
    let manifest_bytes = extract_json_usize_field(&commit_record, "manifest_bytes").unwrap_or(0);
    let manifest_digest_matched =
        manifest_digest == expected_manifest_digest && recorded_manifest_digest == manifest_digest;
    let correctness_digest_matched = recorded_correctness_digest == expected_correctness_digest;
    let idempotency_matched = expected_idempotency_key
        .as_deref()
        .is_none_or(|expected| expected == recovered_idempotency_key);
    let target_uri_matched = recorded_target_uri == target;
    let local_manifest_path_matched =
        recorded_local_manifest_path == target_path.to_string_lossy().as_ref();
    let commit_record_scope_matched = LocalTableCommitRecordScopeMatch {
        target_uri: target_uri_matched,
        local_manifest_path: local_manifest_path_matched,
    };
    let manifest_bytes_matched = manifest_bytes == manifest_payload.len();
    let manifest_shape_matched = manifest_payload == expected_manifest_payload
        && manifest_payload.contains("\"fallback_attempted\": false")
        && manifest_payload.contains("\"external_engine_invoked\": false")
        && commit_record.contains("\"fallback_attempted\": false")
        && commit_record.contains("\"external_engine_invoked\": false");

    if !manifest_digest_matched
        || !correctness_digest_matched
        || !idempotency_matched
        || !target_uri_matched
        || !local_manifest_path_matched
        || !manifest_bytes_matched
        || !manifest_shape_matched
    {
        let mut detail = vec![];
        if !manifest_digest_matched {
            detail.push("manifest_digest_mismatch");
        }
        if !correctness_digest_matched {
            detail.push("correctness_digest_mismatch");
        }
        if !idempotency_matched {
            detail.push("idempotency_key_mismatch");
        }
        if !target_uri_matched {
            detail.push("target_uri_mismatch");
        }
        if !local_manifest_path_matched {
            detail.push("local_manifest_path_mismatch");
        }
        if !manifest_bytes_matched {
            detail.push("manifest_bytes_mismatch");
        }
        if !manifest_shape_matched {
            detail.push("manifest_or_commit_record_shape_mismatch");
        }
        return LocalTableCommitRecoveryReport {
            status: LocalTableCommitRecoveryStatus::BlockedRecoveryMismatch,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::CommitNotAtomic,
                DiagnosticSeverity::Error,
                DiagnosticCategory::Execution,
                "Local table commit recovery evidence did not match the committed fixture.",
                Some("local_table_commit_recovery_evidence".to_string()),
                Some(detail.join(",")),
                Some("Recover only from the sidecar record emitted by local-table-append-commit-rehearsal-smoke for the same manifest and idempotency key."
                    .to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
            provider_profile: profile.to_string(),
            target_uri: target.to_string(),
            target_path: Some(target_path),
            commit_record_path: Some(commit_record_path),
            expected_idempotency_key,
            recovered_idempotency_key,
            idempotency_status: "recovered_mismatch",
            manifest_bytes: manifest_payload.len(),
            commit_record_bytes,
            manifest_digest,
            commit_record_digest,
            expected_manifest_digest,
            recorded_manifest_digest,
            expected_correctness_digest,
            recorded_correctness_digest,
            recorded_target_uri,
            recorded_local_manifest_path,
            manifest_digest_matched,
            correctness_digest_matched,
            commit_record_scope_matched,
        };
    }

    LocalTableCommitRecoveryReport {
        status: LocalTableCommitRecoveryStatus::Recovered,
        diagnostics: vec![],
        provider_profile: profile.to_string(),
        target_uri: target.to_string(),
        target_path: Some(target_path),
        commit_record_path: Some(commit_record_path),
        expected_idempotency_key,
        recovered_idempotency_key,
        idempotency_status: "recovered_from_commit_record",
        manifest_bytes,
        commit_record_bytes,
        manifest_digest,
        commit_record_digest,
        expected_manifest_digest,
        recorded_manifest_digest,
        expected_correctness_digest,
        recorded_correctness_digest,
        recorded_target_uri,
        recorded_local_manifest_path,
        manifest_digest_matched,
        correctness_digest_matched,
        commit_record_scope_matched,
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

fn emit_local_table_commit_recovery_report(
    format: OutputFormat,
    report: &LocalTableCommitRecoveryReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    emit(
        RECOVERY_COMMAND,
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "local table commit recovery smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        local_table_commit_recovery_fields(report),
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

fn local_table_commit_recovery_fields(
    report: &LocalTableCommitRecoveryReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_recovery_identity_fields(&mut fields, report);
    push_recovery_replay_fields(&mut fields, report);
    push_recovery_digest_fields(&mut fields, report);
    push_recovery_count_fields(&mut fields, report);
    push_table_recovery_native_io_evidence_fields(&mut fields, report);
    push_recovery_policy_boundary_fields(&mut fields, report);
    fields
}

fn push_recovery_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_field(fields, "schema_version", RECOVERY_SCHEMA_VERSION);
    push_field(fields, "mode", "local_table_commit_recovery_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_table_commit_recovery_replay",
    );
    push_field(fields, "report_id", RECOVERY_REPORT_ID);
    push_field(fields, "gar_id", RECOVERY_GAR_ID);
    push_field(fields, "fixture_id", FIXTURE_ID);
    push_field(
        fields,
        "support_status",
        recovery_support_status(has_errors),
    );
    push_field(
        fields,
        "claim_gate_status",
        recovery_claim_gate_status(has_errors),
    );
    push_field(
        fields,
        "claim_boundary",
        "local-manifest fixture commit recovery proof only; no Iceberg/Delta/Hudi production runtime, catalog service, object-store table commit, exactly-once recovery, distributed, performance, or Spark-replacement claim",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "table_metadata_adapter", "local_manifest_fixture");
    push_field(fields, "table_format", "shardloom_local_manifest");
    push_field(fields, "catalog_kind", "local_manifest");
    push_field(fields, "target_uri", &report.target_uri);
    push_field(
        fields,
        "committed_manifest_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(fields, "recorded_target_uri", &report.recorded_target_uri);
    push_field(
        fields,
        "recorded_local_manifest_path",
        &report.recorded_local_manifest_path,
    );
    push_field(
        fields,
        "commit_record_path",
        &path_field(report.commit_record_path.as_deref()),
    );
    push_field(
        fields,
        "table_commit_recovery_status",
        report.status.as_str(),
    );
}

fn push_recovery_replay_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "manifest_replay_status",
        recovery_replay_status(has_errors),
    );
    push_field(
        fields,
        "commit_record_replay_status",
        recovery_replay_status(has_errors),
    );
    push_bool_field(fields, "recovery_replay_performed", !has_errors);
    push_bool_field(fields, "commit_record_read_performed", !has_errors);
    push_bool_field(fields, "table_metadata_read_performed", !has_errors);
    push_bool_field(
        fields,
        "local_manifest_metadata_read_performed",
        !has_errors,
    );
}

fn push_recovery_digest_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_bool_field(
        fields,
        "manifest_digest_matched",
        report.manifest_digest_matched,
    );
    push_bool_field(
        fields,
        "correctness_digest_matched",
        report.correctness_digest_matched,
    );
    push_field(
        fields,
        "manifest_digest_status",
        if report.manifest_digest_matched {
            "matched"
        } else {
            "blocked_or_mismatched"
        },
    );
    push_field(
        fields,
        "correctness_digest_status",
        if report.correctness_digest_matched {
            "matched"
        } else {
            "blocked_or_mismatched"
        },
    );
    push_bool_field(
        fields,
        "commit_record_target_uri_matched",
        report.commit_record_scope_matched.target_uri,
    );
    push_bool_field(
        fields,
        "commit_record_local_manifest_path_matched",
        report.commit_record_scope_matched.local_manifest_path,
    );
    push_field(
        fields,
        "expected_idempotency_key",
        report
            .expected_idempotency_key
            .as_deref()
            .unwrap_or("not_requested"),
    );
    push_field(
        fields,
        "recovered_idempotency_key",
        &report.recovered_idempotency_key,
    );
    push_field(fields, "idempotency_status", report.idempotency_status);
    push_count_field(fields, "manifest_bytes", report.manifest_bytes);
    push_count_field(fields, "commit_record_bytes", report.commit_record_bytes);
    push_bool_field(
        fields,
        "manifest_bytes_matched",
        !has_errors && report.manifest_bytes > 0,
    );
    push_field(fields, "manifest_digest", &report.manifest_digest);
    push_field(
        fields,
        "expected_manifest_digest",
        &report.expected_manifest_digest,
    );
    push_field(
        fields,
        "recorded_manifest_digest",
        &report.recorded_manifest_digest,
    );
    push_field(fields, "commit_record_digest", &report.commit_record_digest);
    push_field(
        fields,
        "expected_correctness_digest",
        &report.expected_correctness_digest,
    );
    push_field(
        fields,
        "recorded_correctness_digest",
        &report.recorded_correctness_digest,
    );
}

fn push_table_recovery_native_io_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
) {
    push_count_field(
        fields,
        "local_table_recovery_read_request_count",
        local_table_recovery_read_request_count(report),
    );
    push_count_field(
        fields,
        "local_table_recovery_manifest_bytes_read",
        report.manifest_bytes,
    );
    push_count_field(
        fields,
        "local_table_recovery_commit_record_bytes_read",
        report.commit_record_bytes,
    );
    push_count_field(
        fields,
        "local_table_recovery_total_bytes_read",
        local_table_recovery_total_bytes_read(report),
    );
    push_field(
        fields,
        "local_table_recovery_retry_policy_status",
        local_table_recovery_retry_policy_status(report),
    );
    push_count_field(fields, "local_table_recovery_retry_attempt_count", 0);
    push_field(
        fields,
        "local_table_recovery_rate_limit_policy_status",
        local_table_recovery_rate_limit_policy_status(report),
    );
    push_field(
        fields,
        "local_table_recovery_ambiguous_commit_status",
        local_table_recovery_ambiguous_commit_status(report),
    );
    push_field(
        fields,
        "table_translation_report_status",
        "not_required_shardloom_local_manifest_native_fixture",
    );
    push_field(
        fields,
        "table_metadata_loss_status",
        "not_applicable_native_local_manifest_fixture",
    );
}

fn push_recovery_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_count_field(fields, "base_row_count", if has_errors { 0 } else { 3 });
    push_count_field(fields, "append_row_count", if has_errors { 0 } else { 2 });
    push_count_field(
        fields,
        "effective_row_count",
        if has_errors { 0 } else { 5 },
    );
    push_count_field(
        fields,
        "manifest_file_count",
        if has_errors { 0 } else { 2 },
    );
    push_count_field(
        fields,
        "manifest_segment_count",
        if has_errors { 0 } else { 2 },
    );
}

fn push_recovery_policy_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableCommitRecoveryReport,
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
    push_bool_field(fields, "object_store_io_performed", false);
    push_bool_field(fields, "object_store_io", false);
    push_bool_field(fields, "object_store_write_io", false);
    push_bool_field(fields, "data_file_read_performed", false);
    push_bool_field(fields, "write_io_performed", false);
    push_bool_field(fields, "write_io", false);
    push_bool_field(fields, "manifest_write_performed", false);
    push_bool_field(fields, "transaction_execution_performed", false);
    push_bool_field(fields, "commit_execution_performed", false);
    push_bool_field(fields, "table_catalog_commit_performed", false);
    push_bool_field(fields, "external_table_format_dependency_invoked", false);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-6d.local_table_commit_recovery.native_io",
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
    push_bool_field(fields, "table_commit_recovery_supported", !has_errors);
    push_bool_field(fields, "table_commit_allowed", false);
    push_bool_field(fields, "recovery_claim_allowed", false);
    push_bool_field(fields, "exactly_once_claim_allowed", false);
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
    push_count_field(fields, "commit_record_bytes", report.commit_record_bytes);
    push_field(fields, "commit_record_digest", &report.commit_record_digest);
    push_table_append_native_io_evidence_fields(fields, report);
}

fn push_table_append_native_io_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableAppendCommitReport,
) {
    push_count_field(
        fields,
        "local_table_manifest_write_request_count",
        local_table_manifest_write_request_count(report),
    );
    push_count_field(
        fields,
        "local_table_commit_record_write_request_count",
        local_table_commit_record_write_request_count(report),
    );
    push_count_field(
        fields,
        "local_table_manifest_bytes_written",
        report.written_bytes,
    );
    push_count_field(
        fields,
        "local_table_commit_record_bytes_written",
        report.commit_record_bytes,
    );
    push_count_field(
        fields,
        "local_table_total_bytes_written",
        local_table_total_bytes_written(report),
    );
    push_field(
        fields,
        "local_table_commit_bounded_status",
        local_table_commit_bounded_status(report),
    );
    push_field(
        fields,
        "local_table_commit_retry_policy_status",
        local_table_commit_retry_policy_status(report),
    );
    push_count_field(fields, "local_table_commit_retry_attempt_count", 0);
    push_field(
        fields,
        "local_table_commit_rate_limit_policy_status",
        local_table_commit_rate_limit_policy_status(report),
    );
    push_count_field(
        fields,
        "local_table_commit_rollback_cleanup_request_count",
        report.cleanup_deleted_count,
    );
    push_field(
        fields,
        "local_table_commit_ambiguous_status",
        local_table_commit_ambiguous_status(report),
    );
    push_field(
        fields,
        "local_table_commit_idempotency_scope",
        local_table_commit_idempotency_scope(report),
    );
    push_field(
        fields,
        "table_translation_report_status",
        "not_required_shardloom_local_manifest_native_fixture",
    );
    push_field(
        fields,
        "table_metadata_loss_status",
        "not_applicable_native_local_manifest_fixture",
    );
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
    commit_record_path: &Path,
    idempotency_key: &str,
    manifest_payload: &str,
    manifest_payload_digest: &str,
    correctness_digest: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> std::io::Result<LocalTableAppendCommitOutcome> {
    let workspace_root = workspace_root_for_local_output(target_path)?;
    validate_workspace_safe_output(&workspace_root, target_path, allow_overwrite)?;
    validate_workspace_safe_output(&workspace_root, commit_record_path, true)?;

    let manifest_write_report = write_workspace_safe_local_bytes(
        &workspace_root,
        target_path,
        allow_overwrite,
        "local table append commit manifest",
        manifest_payload.as_bytes(),
    )?;
    let committed_manifest = match fs::read_to_string(target_path) {
        Ok(content) => content,
        Err(error) => {
            let _ = remove_workspace_safe_file_if_exists(&workspace_root, target_path);
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
    let commit_record_bytes = commit_record.len();
    if let Err(error) = write_workspace_safe_local_bytes(
        &workspace_root,
        commit_record_path,
        true,
        "local table append commit sidecar record",
        commit_record.as_bytes(),
    ) {
        let _ = remove_workspace_safe_file_if_exists(&workspace_root, target_path);
        return Err(error);
    }
    let mut cleanup_deleted_count = 0;
    let status = if rollback_after_commit {
        cleanup_deleted_count += usize::from(remove_workspace_safe_file_if_exists(
            &workspace_root,
            target_path,
        )?);
        cleanup_deleted_count += usize::from(remove_workspace_safe_file_if_exists(
            &workspace_root,
            commit_record_path,
        )?);
        LocalTableAppendCommitStatus::RolledBack
    } else {
        LocalTableAppendCommitStatus::Committed
    };
    Ok(LocalTableAppendCommitOutcome {
        status,
        staging_path: manifest_write_report.staging_path,
        cleanup_deleted_count,
        written_bytes: manifest_payload.len(),
        commit_record_bytes,
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

fn recovery_target_blocker(
    status: LocalTableCommitRecoveryStatus,
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<String>,
    message: impl Into<String>,
) -> LocalTableCommitRecoveryReport {
    LocalTableCommitRecoveryReport::blocked(
        status,
        profile,
        target,
        expected_idempotency_key,
        Diagnostic::invalid_input(
            "local_table_commit_recovery_target_path",
            message.into(),
            "Use a local committed manifest path emitted by local-table-append-commit-rehearsal-smoke.",
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

fn recovery_read_error_blocker(
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<String>,
    feature: &str,
    error: &std::io::Error,
) -> LocalTableCommitRecoveryReport {
    LocalTableCommitRecoveryReport::blocked(
        LocalTableCommitRecoveryStatus::BlockedReadError,
        profile,
        target,
        expected_idempotency_key,
        Diagnostic::new(
            DiagnosticCode::CommitNotAtomic,
            DiagnosticSeverity::Error,
            DiagnosticCategory::Execution,
            "Local table commit recovery read failed.",
            Some(feature.to_string()),
            Some(error.to_string()),
            Some("Retry with readable local manifest and sidecar commit record files.".to_string()),
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

fn extract_json_string_field(source: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\": \"");
    let value_start = source.find(&marker)? + marker.len();
    let mut value = String::new();
    let mut escaped = false;
    for character in source[value_start..].chars() {
        if escaped {
            value.push(match character {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Some(value);
        } else {
            value.push(character);
        }
    }
    None
}

fn extract_json_usize_field(source: &str, field: &str) -> Option<usize> {
    let marker = format!("\"{field}\": ");
    let value_start = source.find(&marker)? + marker.len();
    let digits: String = source[value_start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
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

fn workspace_root_for_local_output(path: &Path) -> std::io::Result<PathBuf> {
    shardloom_core::infer_local_output_workspace_root(path).map_err(shardloom_error_to_io_error)
}

fn validate_workspace_safe_output(
    workspace_root: &Path,
    path: &Path,
    allow_overwrite: bool,
) -> std::io::Result<()> {
    shardloom_core::plan_workspace_safe_local_output(workspace_root, path, allow_overwrite)
        .map(|_| ())
        .map_err(shardloom_error_to_io_error)
}

fn write_workspace_safe_local_bytes(
    workspace_root: &Path,
    path: &Path,
    allow_overwrite: bool,
    operation_label: &str,
    content: &[u8],
) -> std::io::Result<shardloom_core::WorkspaceSafeLocalWriteReport> {
    shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        allow_overwrite,
        operation_label,
        content,
    )
    .map_err(shardloom_error_to_io_error)
}

fn remove_workspace_safe_file_if_exists(
    workspace_root: &Path,
    path: &Path,
) -> std::io::Result<bool> {
    validate_workspace_safe_output(workspace_root, path, true)?;
    remove_file_if_exists(path)
}

fn shardloom_error_to_io_error(error: ShardLoomError) -> std::io::Error {
    std::io::Error::other(error)
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

fn recovery_support_status(has_errors: bool) -> &'static str {
    if has_errors {
        "unsupported"
    } else {
        "fixture_smoke_only"
    }
}

fn recovery_claim_gate_status(has_errors: bool) -> &'static str {
    if has_errors {
        "not_claim_grade"
    } else {
        "scoped_local_table_commit_recovery_only"
    }
}

fn recovery_replay_status(has_errors: bool) -> &'static str {
    if has_errors {
        "blocked"
    } else {
        "verified_local_manifest_sidecar"
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

fn local_table_manifest_write_request_count(report: &LocalTableAppendCommitReport) -> usize {
    usize::from(!report.has_errors())
}

fn local_table_commit_record_write_request_count(report: &LocalTableAppendCommitReport) -> usize {
    usize::from(!report.has_errors())
}

fn local_table_total_bytes_written(report: &LocalTableAppendCommitReport) -> usize {
    report
        .written_bytes
        .saturating_add(report.commit_record_bytes)
}

fn local_table_commit_bounded_status(report: &LocalTableAppendCommitReport) -> &'static str {
    if report.has_errors() {
        "not_performed_blocked"
    } else {
        "bounded_manifest_and_commit_record_under_fixture_budget"
    }
}

fn local_table_commit_retry_policy_status(report: &LocalTableAppendCommitReport) -> &'static str {
    if report.has_errors() {
        "blocked_before_retry"
    } else {
        "not_required_single_attempt_local_manifest_commit"
    }
}

fn local_table_commit_rate_limit_policy_status(
    report: &LocalTableAppendCommitReport,
) -> &'static str {
    if report.has_errors() {
        "blocked_before_rate_limit_policy"
    } else {
        "not_required_local_manifest_no_network"
    }
}

fn local_table_commit_ambiguous_status(report: &LocalTableAppendCommitReport) -> &'static str {
    match report.status {
        LocalTableAppendCommitStatus::Committed => "not_observed_local_commit_record_written",
        LocalTableAppendCommitStatus::RolledBack => "rollback_cleanup_completed",
        _ => "blocked_before_commit_claim",
    }
}

fn local_table_commit_idempotency_scope(report: &LocalTableAppendCommitReport) -> &'static str {
    if report.has_errors() {
        "not_emitted_blocked"
    } else {
        "local_manifest_target_manifest_digest"
    }
}

fn local_table_recovery_read_request_count(report: &LocalTableCommitRecoveryReport) -> usize {
    if report.manifest_bytes > 0 || report.commit_record_bytes > 0 {
        2
    } else {
        0
    }
}

fn local_table_recovery_total_bytes_read(report: &LocalTableCommitRecoveryReport) -> usize {
    report
        .manifest_bytes
        .saturating_add(report.commit_record_bytes)
}

fn local_table_recovery_retry_policy_status(
    report: &LocalTableCommitRecoveryReport,
) -> &'static str {
    match report.status {
        LocalTableCommitRecoveryStatus::Recovered => {
            "not_required_single_attempt_local_manifest_recovery"
        }
        LocalTableCommitRecoveryStatus::BlockedRecoveryMismatch => {
            "not_retried_recovery_evidence_mismatch"
        }
        _ => "blocked_before_retry",
    }
}

fn local_table_recovery_rate_limit_policy_status(
    report: &LocalTableCommitRecoveryReport,
) -> &'static str {
    match report.status {
        LocalTableCommitRecoveryStatus::Recovered
        | LocalTableCommitRecoveryStatus::BlockedRecoveryMismatch => {
            "not_required_local_manifest_no_network"
        }
        _ => "blocked_before_rate_limit_policy",
    }
}

fn local_table_recovery_ambiguous_commit_status(
    report: &LocalTableCommitRecoveryReport,
) -> &'static str {
    match report.status {
        LocalTableCommitRecoveryStatus::Recovered => "replay_matched_sidecar_commit_record",
        LocalTableCommitRecoveryStatus::BlockedRecoveryMismatch => "blocked_recovery_mismatch",
        _ => "blocked_before_recovery_replay",
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
