//! Provider/profile-scoped object-store runtime smoke handlers.
//!
//! The runtime path in this module is deliberately narrow: it admits only an
//! explicit local-emulator profile backed by a local file path. Real S3, GCS,
//! ADLS, credentials, network probes, table commits, and external query engines
//! remain blocked. Local-emulator write support is a fixture-scoped staged
//! object write plus sidecar commit-manifest smoke, not production object-store
//! or lakehouse commit support.

use std::{
    fmt::Write as _,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::ExitCode,
    time::UNIX_EPOCH,
};

use shardloom_core::{
    CommandStatus, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, OutputFormat, ShardLoomError,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const OBJECT_STORE_READ_SMOKE_COMMAND: &str = "object-store-read-smoke";
const OBJECT_STORE_READ_SMOKE_SCHEMA_VERSION: &str = "shardloom.object_store_read_smoke.v1";
const OBJECT_STORE_WRITE_SMOKE_COMMAND: &str = "object-store-write-smoke";
const OBJECT_STORE_WRITE_SMOKE_SCHEMA_VERSION: &str = "shardloom.object_store_write_smoke.v1";
const DEFAULT_PROFILE: &str = "local-emulator";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStoreReadSmokeStatus {
    Succeeded,
    BlockedRemoteProvider,
    BlockedUnsupportedProfile,
    BlockedMissingObject,
    BlockedInvalidRange,
    BlockedReadError,
}

impl ObjectStoreReadSmokeStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedMissingObject => "blocked_missing_object",
            Self::BlockedInvalidRange => "blocked_invalid_range",
            Self::BlockedReadError => "blocked_read_error",
        }
    }

    const fn is_error(self) -> bool {
        !matches!(self, Self::Succeeded)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadMode {
    FullFile,
    ByteRange,
}

impl ReadMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FullFile => "full_file",
            Self::ByteRange => "byte_range",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestedRange {
    offset: u64,
    length: u64,
}

#[derive(Debug)]
enum LocalEmulatorMetadataError {
    NotRegularFile,
    StatFailed(std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectStoreReadSmokeReport {
    status: ObjectStoreReadSmokeStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    requested_uri: String,
    local_path: Option<PathBuf>,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    object_size_bytes: u64,
    object_mtime_millis: Option<u128>,
    bytes_read: usize,
    read_digest: String,
    source_state_id: String,
    source_state_digest: String,
    source_content_digest: String,
    source_fingerprint_kind: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStoreWriteSmokeStatus {
    Committed,
    RolledBack,
    BlockedRemoteProvider,
    BlockedUnsupportedProfile,
    BlockedMissingSource,
    BlockedInvalidTarget,
    BlockedTargetExists,
    BlockedWriteError,
}

impl ObjectStoreWriteSmokeStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedMissingSource => "blocked_missing_source",
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
struct ObjectStoreWriteSmokeReport {
    status: ObjectStoreWriteSmokeStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    source_uri: String,
    target_uri: String,
    source_path: Option<PathBuf>,
    target_path: Option<PathBuf>,
    staging_path: Option<PathBuf>,
    commit_manifest_path: Option<PathBuf>,
    idempotency_key: String,
    idempotency_status: &'static str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    payload_bytes: usize,
    written_bytes: usize,
    cleanup_deleted_count: usize,
    payload_digest: String,
    target_content_digest: String,
    commit_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalEmulatorWriteCommit {
    status: ObjectStoreWriteSmokeStatus,
    written_bytes: usize,
    cleanup_deleted_count: usize,
    target_content_digest: String,
    commit_manifest_digest: String,
}

impl ObjectStoreWriteSmokeReport {
    fn blocked(
        status: ObjectStoreWriteSmokeStatus,
        provider_profile: impl Into<String>,
        source_uri: impl Into<String>,
        target_uri: impl Into<String>,
        allow_overwrite: bool,
        rollback_after_commit: bool,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: provider_profile.into(),
            source_uri: source_uri.into(),
            target_uri: target_uri.into(),
            source_path: None,
            target_path: None,
            staging_path: None,
            commit_manifest_path: None,
            idempotency_key: "not_emitted_blocked".to_string(),
            idempotency_status: "not_emitted_blocked",
            allow_overwrite,
            rollback_after_commit,
            payload_bytes: 0,
            written_bytes: 0,
            cleanup_deleted_count: 0,
            payload_digest: "not_emitted_no_object_write".to_string(),
            target_content_digest: "not_emitted_no_object_write".to_string(),
            commit_manifest_digest: "not_emitted_no_commit_manifest".to_string(),
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
        let claim_gate_status = if self.has_errors() {
            "not_claim_grade"
        } else {
            "fixture_smoke_only"
        };
        format!(
            "object_store_write_smoke(status={}, profile={}, bytes_written={}, rollback_status={}, object_store_io={}, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.written_bytes,
            rollback_status(self.status, self.rollback_after_commit),
            !self.has_errors(),
            claim_gate_status
        )
    }
}

impl ObjectStoreReadSmokeReport {
    fn blocked(
        status: ObjectStoreReadSmokeStatus,
        provider_profile: impl Into<String>,
        requested_uri: impl Into<String>,
        read_mode: ReadMode,
        requested_range: Option<RequestedRange>,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: provider_profile.into(),
            requested_uri: requested_uri.into(),
            local_path: None,
            read_mode,
            requested_range,
            object_size_bytes: 0,
            object_mtime_millis: None,
            bytes_read: 0,
            read_digest: "not_emitted_no_object_read".to_string(),
            source_state_id: "not_emitted_no_object_read".to_string(),
            source_state_digest: "not_emitted_no_object_read".to_string(),
            source_content_digest: "not_emitted_no_object_read".to_string(),
            source_fingerprint_kind: "not_emitted_no_object_read",
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
        let claim_gate_status = if self.has_errors() {
            "not_claim_grade"
        } else {
            "fixture_smoke_only"
        };
        format!(
            "object_store_read_smoke(status={}, profile={}, read_mode={}, bytes_read={}, object_store_io={}, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.read_mode.as_str(),
            self.bytes_read,
            !self.has_errors(),
            claim_gate_status
        )
    }
}

pub(crate) fn handle_object_store_read_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source) = args.next() else {
        return emit_error(
            OBJECT_STORE_READ_SMOKE_COMMAND,
            format,
            "object-store read smoke failed",
            &ShardLoomError::InvalidOperation(
                "object-store-read-smoke requires <local-object-path>".to_string(),
            ),
        );
    };

    let mut profile = DEFAULT_PROFILE.to_string();
    let mut requested_range = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_READ_SMOKE_COMMAND,
                        format,
                        "object-store read smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --profile".to_string(),
                        ),
                    );
                };
                profile = value;
            }
            "--range" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_READ_SMOKE_COMMAND,
                        format,
                        "object-store read smoke failed",
                        &ShardLoomError::InvalidOperation("missing value for --range".to_string()),
                    );
                };
                requested_range = match parse_requested_range(&value) {
                    Ok(range) => Some(range),
                    Err(error) => {
                        return emit_blocked_range_parse(format, &source, &profile, &error);
                    }
                };
            }
            value => {
                return emit_error(
                    OBJECT_STORE_READ_SMOKE_COMMAND,
                    format,
                    "object-store read smoke failed",
                    &cli_unknown_arg_error(OBJECT_STORE_READ_SMOKE_COMMAND, value),
                );
            }
        }
    }

    let report = execute_object_store_read_smoke(&source, &profile, requested_range);
    emit_object_store_read_smoke_report(format, &report)
}

pub(crate) fn handle_object_store_write_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(source) = args.next() else {
        return emit_error(
            OBJECT_STORE_WRITE_SMOKE_COMMAND,
            format,
            "object-store write smoke failed",
            &ShardLoomError::InvalidOperation(
                "object-store-write-smoke requires <source-local-path> <target-local-object-path>"
                    .to_string(),
            ),
        );
    };
    let Some(target) = args.next() else {
        return emit_error(
            OBJECT_STORE_WRITE_SMOKE_COMMAND,
            format,
            "object-store write smoke failed",
            &ShardLoomError::InvalidOperation(
                "object-store-write-smoke requires <source-local-path> <target-local-object-path>"
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
                        OBJECT_STORE_WRITE_SMOKE_COMMAND,
                        format,
                        "object-store write smoke failed",
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
                        OBJECT_STORE_WRITE_SMOKE_COMMAND,
                        format,
                        "object-store write smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --idempotency-key".to_string(),
                        ),
                    );
                };
                let value = value.trim().to_string();
                if value.is_empty() {
                    return emit_error(
                        OBJECT_STORE_WRITE_SMOKE_COMMAND,
                        format,
                        "object-store write smoke failed",
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
                    OBJECT_STORE_WRITE_SMOKE_COMMAND,
                    format,
                    "object-store write smoke failed",
                    &cli_unknown_arg_error(OBJECT_STORE_WRITE_SMOKE_COMMAND, value),
                );
            }
        }
    }

    let report = execute_object_store_write_smoke(
        &source,
        &target,
        &profile,
        idempotency_key.as_deref(),
        allow_overwrite,
        rollback_after_commit,
    );
    emit_object_store_write_smoke_report(format, &report)
}

fn emit_blocked_range_parse(
    format: OutputFormat,
    source: &str,
    profile: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let report = ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedInvalidRange,
        profile,
        source,
        ReadMode::ByteRange,
        None,
        Diagnostic::invalid_input(
            "object_store_read_range",
            error.to_string(),
            "Use --range offset:length with a positive length.",
        ),
    );
    emit_object_store_read_smoke_report(format, &report)
}

fn emit_object_store_read_smoke_report(
    format: OutputFormat,
    report: &ObjectStoreReadSmokeReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    let status = if has_errors {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        OBJECT_STORE_READ_SMOKE_COMMAND,
        format,
        status,
        "object-store local-emulator read smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_read_smoke_fields(report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn emit_object_store_write_smoke_report(
    format: OutputFormat,
    report: &ObjectStoreWriteSmokeReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    let status = if has_errors {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        OBJECT_STORE_WRITE_SMOKE_COMMAND,
        format,
        status,
        "object-store local-emulator write smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_write_smoke_fields(report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn execute_object_store_read_smoke(
    source: &str,
    profile: &str,
    requested_range: Option<RequestedRange>,
) -> ObjectStoreReadSmokeReport {
    let read_mode = read_mode_for(requested_range);
    if let Some(report) = early_profile_blocker(source, profile, read_mode, requested_range) {
        return report;
    }

    let local_path = match normalize_local_emulator_path(source) {
        Ok(path) => path,
        Err(error) => {
            return local_path_blocker(source, profile, read_mode, requested_range, &error);
        }
    };
    let metadata = match local_emulator_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return local_metadata_blocker(source, profile, read_mode, requested_range, &error);
        }
    };
    if let Some(report) = range_blocker(source, profile, read_mode, requested_range, metadata.len())
    {
        return report;
    }

    let bytes = match read_local_emulator_bytes(&local_path, requested_range) {
        Ok(bytes) => bytes,
        Err(error) => {
            return read_error_blocker(source, profile, read_mode, requested_range, &error);
        }
    };
    successful_object_store_read_report(
        source,
        profile,
        local_path,
        read_mode,
        &metadata,
        requested_range,
        &bytes,
    )
}

fn execute_object_store_write_smoke(
    source: &str,
    target: &str,
    profile: &str,
    idempotency_key: Option<&str>,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> ObjectStoreWriteSmokeReport {
    if let Some(report) = early_write_profile_blocker(
        source,
        target,
        profile,
        allow_overwrite,
        rollback_after_commit,
    ) {
        return report;
    }

    let (source_path, target_path, payload) = match prepare_local_emulator_write_inputs(
        source,
        target,
        profile,
        allow_overwrite,
        rollback_after_commit,
    ) {
        Ok(inputs) => inputs,
        Err(report) => return *report,
    };
    let payload_digest = fnv64_digest_bytes(&payload);
    let (idempotency_key, idempotency_status) =
        resolved_idempotency_key(idempotency_key, source, target, &payload_digest);
    let staging_path = staging_object_path(&target_path, &idempotency_key);
    let commit_manifest_path = commit_manifest_sidecar_path(&target_path);
    let commit_result = perform_local_emulator_write_commit(
        source,
        target,
        &source_path,
        &target_path,
        &staging_path,
        &commit_manifest_path,
        &idempotency_key,
        &payload_digest,
        &payload,
        allow_overwrite,
        rollback_after_commit,
    );

    match commit_result {
        Ok(commit) => ObjectStoreWriteSmokeReport {
            status: commit.status,
            diagnostics: vec![],
            provider_profile: profile.to_string(),
            source_uri: source.to_string(),
            target_uri: target.to_string(),
            source_path: Some(source_path),
            target_path: Some(target_path),
            staging_path: Some(staging_path),
            commit_manifest_path: Some(commit_manifest_path),
            idempotency_key,
            idempotency_status,
            allow_overwrite,
            rollback_after_commit,
            payload_bytes: payload.len(),
            written_bytes: commit.written_bytes,
            cleanup_deleted_count: commit.cleanup_deleted_count,
            payload_digest,
            target_content_digest: commit.target_content_digest,
            commit_manifest_digest: commit.commit_manifest_digest,
        },
        Err(error) => write_error_blocker(
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            &error,
        ),
    }
}

fn prepare_local_emulator_write_inputs(
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> Result<(PathBuf, PathBuf, Vec<u8>), Box<ObjectStoreWriteSmokeReport>> {
    let source_path = normalize_local_emulator_path(source).map_err(|error| {
        Box::new(write_source_path_blocker(
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            &error,
        ))
    })?;
    let target_path = normalize_local_emulator_path(target).map_err(|error| {
        Box::new(write_target_path_blocker(
            ObjectStoreWriteSmokeStatus::BlockedInvalidTarget,
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            error.to_string(),
        ))
    })?;
    if let Err((status, message)) =
        validate_local_emulator_write_target(&target_path, allow_overwrite)
    {
        return Err(Box::new(write_target_path_blocker(
            status,
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            message,
        )));
    }
    if let Err(error) = local_emulator_metadata(&source_path) {
        return Err(Box::new(write_source_metadata_blocker(
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            &error,
        )));
    }
    let payload = fs::read(&source_path).map_err(|error| {
        Box::new(write_error_blocker(
            source,
            target,
            profile,
            allow_overwrite,
            rollback_after_commit,
            &error,
        ))
    })?;
    Ok((source_path, target_path, payload))
}

fn read_mode_for(requested_range: Option<RequestedRange>) -> ReadMode {
    if requested_range.is_some() {
        ReadMode::ByteRange
    } else {
        ReadMode::FullFile
    }
}

fn early_profile_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
) -> Option<ObjectStoreReadSmokeReport> {
    if profile != DEFAULT_PROFILE {
        return Some(ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedUnsupportedProfile,
            profile,
            source,
            read_mode,
            requested_range,
            Diagnostic::object_store_blocked(
                "object_store_read_profile",
                format!("profile {profile} is not admitted for object-store read runtime"),
                "Use --profile local-emulator with a local fixture path.",
            ),
        ));
    }
    if is_remote_object_store_uri(source) {
        return Some(ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedRemoteProvider,
            profile,
            source,
            read_mode,
            requested_range,
            Diagnostic::object_store_blocked(
                "object_store_remote_read",
                "real S3/GCS/ADLS providers remain blocked; no credential or network probe was performed",
                "Use a local-emulator fixture path for this smoke proof.",
            ),
        ));
    }
    None
}

fn local_path_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    error: &ShardLoomError,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedReadError,
        profile,
        source,
        read_mode,
        requested_range,
        Diagnostic::invalid_input(
            "object_store_local_emulator_path",
            error.to_string(),
            "Use a local path or file:// path with no remote provider.",
        ),
    )
}

fn local_emulator_metadata(local_path: &Path) -> Result<fs::Metadata, LocalEmulatorMetadataError> {
    match fs::metadata(local_path) {
        Ok(metadata) if metadata.is_file() => Ok(metadata),
        Ok(_) => Err(LocalEmulatorMetadataError::NotRegularFile),
        Err(error) => Err(LocalEmulatorMetadataError::StatFailed(error)),
    }
}

fn local_metadata_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    error: &LocalEmulatorMetadataError,
) -> ObjectStoreReadSmokeReport {
    match error {
        LocalEmulatorMetadataError::NotRegularFile => ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedMissingObject,
            profile,
            source,
            read_mode,
            requested_range,
            Diagnostic::object_store_blocked(
                "object_store_local_emulator_object",
                "local-emulator source is not a regular file",
                "Use a regular local fixture file.",
            ),
        ),
        LocalEmulatorMetadataError::StatFailed(error) => ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedMissingObject,
            profile,
            source,
            read_mode,
            requested_range,
            Diagnostic::object_store_blocked(
                "object_store_local_emulator_object",
                format!("local-emulator fixture could not be statted: {error}"),
                "Create the local fixture file and retry.",
            ),
        ),
    }
}

fn range_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    object_size_bytes: u64,
) -> Option<ObjectStoreReadSmokeReport> {
    let range = requested_range?;
    if range.length > 0 && range.offset.saturating_add(range.length) <= object_size_bytes {
        return None;
    }
    Some(ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedInvalidRange,
        profile,
        source,
        read_mode,
        requested_range,
        Diagnostic::invalid_input(
            "object_store_read_range",
            format!(
                "range {}:{} is outside object size {}",
                range.offset, range.length, object_size_bytes
            ),
            "Choose a byte range inside the local-emulator object.",
        ),
    ))
}

fn read_error_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    error: &std::io::Error,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedReadError,
        profile,
        source,
        read_mode,
        requested_range,
        Diagnostic::new(
            DiagnosticCode::ObjectStoreUnsupported,
            DiagnosticSeverity::Error,
            DiagnosticCategory::ObjectStore,
            "Object-store local-emulator read failed.",
            Some("object_store_local_emulator_read".to_string()),
            Some(error.to_string()),
            Some("Retry with a readable local fixture file.".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn early_write_profile_blocker(
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> Option<ObjectStoreWriteSmokeReport> {
    if profile != DEFAULT_PROFILE {
        return Some(ObjectStoreWriteSmokeReport::blocked(
            ObjectStoreWriteSmokeStatus::BlockedUnsupportedProfile,
            profile,
            source,
            target,
            allow_overwrite,
            rollback_after_commit,
            Diagnostic::object_store_blocked(
                "object_store_write_profile",
                format!("profile {profile} is not admitted for object-store write runtime"),
                "Use --profile local-emulator with local fixture paths.",
            ),
        ));
    }
    if is_remote_object_store_uri(source) || is_remote_object_store_uri(target) {
        return Some(ObjectStoreWriteSmokeReport::blocked(
            ObjectStoreWriteSmokeStatus::BlockedRemoteProvider,
            profile,
            source,
            target,
            allow_overwrite,
            rollback_after_commit,
            Diagnostic::object_store_blocked(
                "object_store_remote_write",
                "real S3/GCS/ADLS providers remain blocked; no credential, network, or provider probe was performed",
                "Use local-emulator fixture paths for this staged write smoke.",
            ),
        ));
    }
    None
}

fn write_source_path_blocker(
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    error: &ShardLoomError,
) -> ObjectStoreWriteSmokeReport {
    ObjectStoreWriteSmokeReport::blocked(
        ObjectStoreWriteSmokeStatus::BlockedMissingSource,
        profile,
        source,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::invalid_input(
            "object_store_write_source_path",
            error.to_string(),
            "Use a readable regular local source file or file:// path.",
        ),
    )
}

fn write_target_path_blocker(
    status: ObjectStoreWriteSmokeStatus,
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    message: impl Into<String>,
) -> ObjectStoreWriteSmokeReport {
    ObjectStoreWriteSmokeReport::blocked(
        status,
        profile,
        source,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::invalid_input(
            "object_store_write_target_path",
            message.into(),
            "Use a local target path whose parent directory exists; pass --allow-overwrite to replace an existing object.",
        ),
    )
}

fn write_source_metadata_blocker(
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    error: &LocalEmulatorMetadataError,
) -> ObjectStoreWriteSmokeReport {
    let detail = match error {
        LocalEmulatorMetadataError::NotRegularFile => {
            "local-emulator write source is not a regular file".to_string()
        }
        LocalEmulatorMetadataError::StatFailed(error) => {
            format!("local-emulator write source could not be statted: {error}")
        }
    };
    ObjectStoreWriteSmokeReport::blocked(
        ObjectStoreWriteSmokeStatus::BlockedMissingSource,
        profile,
        source,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::object_store_blocked(
            "object_store_write_source",
            detail,
            "Create a readable regular local source file and retry.",
        ),
    )
}

fn write_error_blocker(
    source: &str,
    target: &str,
    profile: &str,
    allow_overwrite: bool,
    rollback_after_commit: bool,
    error: &std::io::Error,
) -> ObjectStoreWriteSmokeReport {
    ObjectStoreWriteSmokeReport::blocked(
        ObjectStoreWriteSmokeStatus::BlockedWriteError,
        profile,
        source,
        target,
        allow_overwrite,
        rollback_after_commit,
        Diagnostic::new(
            DiagnosticCode::ObjectStoreUnsupported,
            DiagnosticSeverity::Error,
            DiagnosticCategory::ObjectStore,
            "Object-store local-emulator write failed.",
            Some("object_store_local_emulator_write".to_string()),
            Some(error.to_string()),
            Some("Retry with writable local fixture paths and no remote provider.".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn validate_local_emulator_write_target(
    target_path: &Path,
    allow_overwrite: bool,
) -> Result<(), (ObjectStoreWriteSmokeStatus, String)> {
    let Some(parent) = target_path.parent() else {
        return Err((
            ObjectStoreWriteSmokeStatus::BlockedInvalidTarget,
            "local-emulator target must have a parent directory".to_string(),
        ));
    };
    if !parent.as_os_str().is_empty() && !parent.is_dir() {
        return Err((
            ObjectStoreWriteSmokeStatus::BlockedInvalidTarget,
            format!(
                "local-emulator target parent does not exist or is not a directory: {}",
                parent.to_string_lossy()
            ),
        ));
    }
    if target_path.is_dir() {
        return Err((
            ObjectStoreWriteSmokeStatus::BlockedInvalidTarget,
            "local-emulator target is a directory, not an object path".to_string(),
        ));
    }
    if target_path.exists() && !allow_overwrite {
        return Err((
            ObjectStoreWriteSmokeStatus::BlockedTargetExists,
            "local-emulator target already exists".to_string(),
        ));
    }
    Ok(())
}

fn resolved_idempotency_key(
    idempotency_key: Option<&str>,
    source: &str,
    target: &str,
    payload_digest: &str,
) -> (String, &'static str) {
    if let Some(key) = idempotency_key {
        return (key.to_string(), "caller_supplied");
    }
    (
        fnv64_digest(&format!("{source}|{target}|{payload_digest}")),
        "derived_from_payload_digest",
    )
}

fn staging_object_path(target_path: &Path, idempotency_key: &str) -> PathBuf {
    let parent = target_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = target_path
        .file_name()
        .map_or_else(|| "object".into(), |name| name.to_string_lossy());
    parent.join(".shardloom-object-store-staging").join(format!(
        "{}.{}.tmp",
        file_name,
        sanitize_idempotency_key(idempotency_key)
    ))
}

fn commit_manifest_sidecar_path(target_path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.shardloom-commit.json",
        target_path.to_string_lossy()
    ))
}

#[allow(clippy::too_many_arguments)]
fn perform_local_emulator_write_commit(
    source_uri: &str,
    target_uri: &str,
    source_path: &Path,
    target_path: &Path,
    staging_path: &Path,
    commit_manifest_path: &Path,
    idempotency_key: &str,
    payload_digest: &str,
    payload: &[u8],
    allow_overwrite: bool,
    rollback_after_commit: bool,
) -> std::io::Result<LocalEmulatorWriteCommit> {
    if let Some(staging_parent) = staging_path.parent() {
        fs::create_dir_all(staging_parent)?;
    }
    remove_file_if_exists(staging_path)?;
    fs::write(staging_path, payload)?;
    if allow_overwrite {
        remove_file_if_exists(target_path)?;
        remove_file_if_exists(commit_manifest_path)?;
    }
    if target_path.exists() {
        remove_file_if_exists(staging_path)?;
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "local-emulator target already exists",
        ));
    }

    fs::rename(staging_path, target_path)?;
    let target_bytes = match fs::read(target_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = remove_file_if_exists(target_path);
            return Err(error);
        }
    };
    let target_content_digest = fnv64_digest_bytes(&target_bytes);
    let manifest = build_commit_manifest(
        source_uri,
        target_uri,
        source_path,
        target_path,
        idempotency_key,
        payload.len(),
        payload_digest,
        &target_content_digest,
    );
    let commit_manifest_digest = fnv64_digest(&manifest);
    if let Err(error) = fs::write(commit_manifest_path, manifest) {
        let _ = remove_file_if_exists(target_path);
        return Err(error);
    }

    let mut cleanup_deleted_count = 0;
    let status = if rollback_after_commit {
        cleanup_deleted_count += usize::from(remove_file_if_exists(target_path)?);
        cleanup_deleted_count += usize::from(remove_file_if_exists(commit_manifest_path)?);
        ObjectStoreWriteSmokeStatus::RolledBack
    } else {
        ObjectStoreWriteSmokeStatus::Committed
    };

    Ok(LocalEmulatorWriteCommit {
        status,
        written_bytes: payload.len(),
        cleanup_deleted_count,
        target_content_digest,
        commit_manifest_digest,
    })
}

fn remove_file_if_exists(path: &Path) -> std::io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_commit_manifest(
    source_uri: &str,
    target_uri: &str,
    source_path: &Path,
    target_path: &Path,
    idempotency_key: &str,
    payload_bytes: usize,
    payload_digest: &str,
    target_content_digest: &str,
) -> String {
    format!(
        concat!(
            "{{\n",
            "  \"schema_version\": \"shardloom.object_store_write_commit_manifest.v1\",\n",
            "  \"commit_protocol\": \"local_emulator_sidecar_manifest\",\n",
            "  \"write_mode\": \"single_object_staged_commit\",\n",
            "  \"source_uri\": \"{}\",\n",
            "  \"target_uri\": \"{}\",\n",
            "  \"local_source_path\": \"{}\",\n",
            "  \"local_target_path\": \"{}\",\n",
            "  \"idempotency_key\": \"{}\",\n",
            "  \"payload_bytes\": {},\n",
            "  \"payload_digest\": \"{}\",\n",
            "  \"target_content_digest\": \"{}\",\n",
            "  \"fallback_attempted\": false,\n",
            "  \"external_engine_invoked\": false\n",
            "}}\n"
        ),
        escape_json(source_uri),
        escape_json(target_uri),
        escape_json(&source_path.to_string_lossy()),
        escape_json(&target_path.to_string_lossy()),
        escape_json(idempotency_key),
        payload_bytes,
        escape_json(payload_digest),
        escape_json(target_content_digest),
    )
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

fn successful_object_store_read_report(
    source: &str,
    profile: &str,
    local_path: PathBuf,
    read_mode: ReadMode,
    metadata: &fs::Metadata,
    requested_range: Option<RequestedRange>,
    bytes: &[u8],
) -> ObjectStoreReadSmokeReport {
    let read_digest = fnv64_digest_bytes(bytes);
    let mtime_millis = metadata.modified().ok().and_then(|mtime| {
        mtime
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis())
    });
    let source_content_digest = read_digest.clone();
    let fingerprint_material = format!(
        "{}|{}|{}|{}|{}|{}",
        source,
        metadata.len(),
        mtime_millis.unwrap_or_default(),
        read_mode.as_str(),
        requested_range.map_or(0, |range| range.offset),
        read_digest
    );
    let source_state_digest = fnv64_digest(&fingerprint_material);
    let source_state_id = format!(
        "object-store-local-emulator-{}",
        source_state_digest.replace(':', "-")
    );

    ObjectStoreReadSmokeReport {
        status: ObjectStoreReadSmokeStatus::Succeeded,
        diagnostics: vec![],
        provider_profile: profile.to_string(),
        requested_uri: source.to_string(),
        local_path: Some(local_path),
        read_mode,
        requested_range,
        object_size_bytes: metadata.len(),
        object_mtime_millis: mtime_millis,
        bytes_read: bytes.len(),
        read_digest: source_content_digest.clone(),
        source_state_id,
        source_state_digest,
        source_content_digest,
        source_fingerprint_kind: if requested_range.is_some() {
            "local_emulator_metadata_plus_range_digest"
        } else {
            "local_emulator_content_digest"
        },
    }
}

fn object_store_read_smoke_fields(report: &ObjectStoreReadSmokeReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_object_store_identity_fields(&mut fields, report);
    push_object_store_read_fields(&mut fields, report);
    push_object_store_source_state_fields(&mut fields, report);
    push_object_store_policy_fields(&mut fields);
    push_object_store_claim_fields(&mut fields, report);
    fields
}

fn push_object_store_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let local_emulator_path = report.local_path.as_ref().map_or_else(
        || "not_applicable".to_string(),
        |path| path.to_string_lossy().into_owned(),
    );
    push_field(
        fields,
        "schema_version",
        OBJECT_STORE_READ_SMOKE_SCHEMA_VERSION,
    );
    push_field(fields, "mode", "object_store_read_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_emulator_object_store_read",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "object_store_provider", "local_emulator");
    push_field(fields, "requested_uri", &report.requested_uri);
    push_field(fields, "local_emulator_path", &local_emulator_path);
    push_field(fields, "object_store_read_status", report.status.as_str());
}

fn push_object_store_read_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(fields, "read_mode", report.read_mode.as_str());
    push_field(
        fields,
        "byte_range_read_status",
        byte_range_read_status(report.read_mode, has_errors),
    );
    push_field(
        fields,
        "full_file_read_status",
        full_file_read_status(report.read_mode, has_errors),
    );
    push_field(
        fields,
        "streaming_read_status",
        streaming_read_status(report.read_mode, has_errors),
    );
    push_u64_field(
        fields,
        "read_range_offset",
        report.requested_range.map_or(0, |range| range.offset),
    );
    push_u64_field(
        fields,
        "read_range_length",
        report.requested_range.map_or(0, |range| range.length),
    );
    push_count_field(fields, "bytes_read", report.bytes_read);
    push_u64_field(fields, "object_size_bytes", report.object_size_bytes);
    push_field(
        fields,
        "object_mtime_millis",
        &report
            .object_mtime_millis
            .map_or_else(|| "not_available".to_string(), |value| value.to_string()),
    );
    push_field(fields, "object_etag", "not_applicable_local_emulator");
    push_field(fields, "object_version", "not_applicable_local_emulator");
    push_field(fields, "read_digest", &report.read_digest);
}

fn push_object_store_source_state_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(fields, "source_state_id", &report.source_state_id);
    push_field(fields, "source_state_digest", &report.source_state_digest);
    push_field(fields, "source_format", "object_store_object");
    push_field(fields, "source_location", &report.requested_uri);
    push_field(
        fields,
        "source_fingerprint_kind",
        report.source_fingerprint_kind,
    );
    push_field(
        fields,
        "source_content_digest",
        &report.source_content_digest,
    );
    push_bool_field(fields, "row_count_known", false);
    push_count_field(fields, "file_count", usize::from(!has_errors));
    push_u64_field(fields, "byte_size", report.object_size_bytes);
    push_field(fields, "partition_columns", "");
    push_field(fields, "compression", "unknown");
}

fn push_object_store_policy_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "credential_policy_status",
        "not_required_local_emulator",
    );
    push_field(
        fields,
        "network_effect_status",
        "not_required_local_emulator",
    );
    push_bool_field(fields, "credential_resolution_allowed", false);
    push_bool_field(fields, "credential_resolution_performed", false);
    push_bool_field(fields, "network_probe_allowed", false);
    push_bool_field(fields, "network_probe_performed", false);
    push_bool_field(fields, "provider_probe_allowed", false);
    push_bool_field(fields, "provider_probe_performed", false);
    push_bool_field(fields, "listing_allowed", false);
    push_field(
        fields,
        "listing_status",
        "not_performed_local_emulator_single_object",
    );
    push_bool_field(fields, "cache_write_allowed", false);
    push_field(fields, "local_cache_status", "not_performed");
}

fn push_object_store_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-4n.local_emulator_object_store_read.native_io",
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
    push_field(
        fields,
        "claim_gate_status",
        if has_errors {
            "not_claim_grade"
        } else {
            "fixture_smoke_only"
        },
    );
    push_field(
        fields,
        "claim_boundary",
        "local-emulator object-store read smoke only; no S3/GCS/ADLS, credential, network, production, table, commit, distributed, performance, or Spark-replacement claim",
    );
    push_bool_field(fields, "object_store_runtime_supported", !has_errors);
    push_bool_field(fields, "public_object_store_claim_allowed", false);
    push_bool_field(fields, "production_object_store_claim_allowed", false);
    push_bool_field(fields, "object_store_io", !has_errors);
    push_bool_field(fields, "object_store_read_io", !has_errors);
    push_bool_field(fields, "object_store_write_io", false);
    push_bool_field(fields, "write_io", false);
    push_bool_field(fields, "data_read", !has_errors);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_field(
        fields,
        "execution",
        if has_errors { "blocked" } else { "performed" },
    );
    push_bool_field(fields, "plan_only", false);
}

fn object_store_write_smoke_fields(report: &ObjectStoreWriteSmokeReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_object_store_write_identity_fields(&mut fields, report);
    push_object_store_write_commit_fields(&mut fields, report);
    push_object_store_write_policy_fields(&mut fields, report);
    push_object_store_write_claim_fields(&mut fields, report);
    fields
}

fn push_object_store_write_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteSmokeReport,
) {
    push_field(
        fields,
        "schema_version",
        OBJECT_STORE_WRITE_SMOKE_SCHEMA_VERSION,
    );
    push_field(fields, "mode", "object_store_write_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_emulator_object_store_write_commit",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "object_store_provider", "local_emulator");
    push_field(fields, "source_uri", &report.source_uri);
    push_field(fields, "target_uri", &report.target_uri);
    push_field(
        fields,
        "local_source_path",
        &path_field(report.source_path.as_deref()),
    );
    push_field(
        fields,
        "local_emulator_target_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(fields, "object_store_write_status", report.status.as_str());
}

fn push_object_store_write_commit_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(fields, "write_mode", "single_object_staged_commit");
    push_field(
        fields,
        "write_staging_status",
        write_staging_status(report.status),
    );
    push_field(fields, "commit_protocol", "local_emulator_sidecar_manifest");
    push_field(
        fields,
        "commit_protocol_status",
        commit_protocol_status(report.status),
    );
    push_field(fields, "commit_status", commit_status(report.status));
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
        "staged_object_path",
        &path_field(report.staging_path.as_deref()),
    );
    push_field(
        fields,
        "committed_object_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(
        fields,
        "committed_manifest_path",
        &path_field(report.commit_manifest_path.as_deref()),
    );
    push_bool_field(fields, "commit_manifest_written", !has_errors);
    push_bool_field(
        fields,
        "commit_manifest_present",
        matches!(report.status, ObjectStoreWriteSmokeStatus::Committed),
    );
    push_bool_field(
        fields,
        "target_exists_after_commit",
        matches!(report.status, ObjectStoreWriteSmokeStatus::Committed),
    );
    push_count_field(fields, "payload_bytes", report.payload_bytes);
    push_count_field(fields, "written_bytes", report.written_bytes);
    push_field(fields, "payload_digest", &report.payload_digest);
    push_field(
        fields,
        "target_content_digest",
        &report.target_content_digest,
    );
    push_field(
        fields,
        "commit_manifest_digest",
        &report.commit_manifest_digest,
    );
}

fn push_object_store_write_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteSmokeReport,
) {
    let has_errors = report.has_errors();
    push_object_store_policy_fields(fields);
    push_bool_field(fields, "remote_write_allowed", false);
    push_bool_field(fields, "local_emulator_write_allowed", !has_errors);
    push_bool_field(fields, "write_staging_allowed", !has_errors);
}

fn push_object_store_write_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-4o.local_emulator_object_store_write.native_io",
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
    push_field(
        fields,
        "claim_gate_status",
        if has_errors {
            "not_claim_grade"
        } else {
            "fixture_smoke_only"
        },
    );
    push_field(
        fields,
        "claim_boundary",
        "local-emulator staged object write/commit smoke only; no S3/GCS/ADLS, credential, network, production, table/lakehouse commit, distributed, performance, or Spark-replacement claim",
    );
    push_bool_field(fields, "object_store_runtime_supported", !has_errors);
    push_bool_field(fields, "object_store_write_runtime_supported", !has_errors);
    push_bool_field(fields, "public_object_store_claim_allowed", false);
    push_bool_field(fields, "production_object_store_claim_allowed", false);
    push_bool_field(fields, "table_runtime_supported", false);
    push_bool_field(fields, "table_commit_allowed", false);
    push_bool_field(fields, "object_store_io", !has_errors);
    push_bool_field(fields, "object_store_read_io", false);
    push_bool_field(fields, "object_store_write_io", !has_errors);
    push_bool_field(fields, "write_io", !has_errors);
    push_bool_field(fields, "data_read", !has_errors);
    push_bool_field(fields, "source_io_performed", !has_errors);
    push_bool_field(fields, "table_metadata_read_performed", false);
    push_bool_field(fields, "table_commit_performed", false);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_field(
        fields,
        "execution",
        if has_errors { "blocked" } else { "performed" },
    );
    push_bool_field(fields, "plan_only", false);
}

fn write_staging_status(status: ObjectStoreWriteSmokeStatus) -> &'static str {
    if status.is_error() {
        "blocked"
    } else {
        "performed_local_emulator"
    }
}

fn commit_protocol_status(status: ObjectStoreWriteSmokeStatus) -> &'static str {
    match status {
        ObjectStoreWriteSmokeStatus::Committed => "committed",
        ObjectStoreWriteSmokeStatus::RolledBack => "rolled_back",
        _ => "blocked",
    }
}

fn commit_status(status: ObjectStoreWriteSmokeStatus) -> &'static str {
    match status {
        ObjectStoreWriteSmokeStatus::Committed => "committed_local_emulator_object",
        ObjectStoreWriteSmokeStatus::RolledBack => "committed_then_rolled_back",
        _ => "blocked",
    }
}

fn rollback_status(
    status: ObjectStoreWriteSmokeStatus,
    rollback_after_commit: bool,
) -> &'static str {
    match (status, rollback_after_commit) {
        (ObjectStoreWriteSmokeStatus::RolledBack, true) => "performed_local_emulator_cleanup",
        (_, true) if status.is_error() => "blocked",
        _ => "not_requested",
    }
}

fn cleanup_status(
    status: ObjectStoreWriteSmokeStatus,
    rollback_after_commit: bool,
) -> &'static str {
    match (status, rollback_after_commit) {
        (ObjectStoreWriteSmokeStatus::Committed, _) => "staging_object_removed",
        (ObjectStoreWriteSmokeStatus::RolledBack, true) => "rollback_cleanup_performed",
        _ => "not_performed_blocked",
    }
}

fn path_field(path: Option<&Path>) -> String {
    path.map_or_else(
        || "not_applicable".to_string(),
        |path| path.to_string_lossy().into_owned(),
    )
}

fn byte_range_read_status(read_mode: ReadMode, has_errors: bool) -> &'static str {
    if read_mode == ReadMode::ByteRange && !has_errors {
        "performed_local_emulator"
    } else if read_mode == ReadMode::ByteRange {
        "blocked"
    } else {
        "not_requested"
    }
}

fn full_file_read_status(read_mode: ReadMode, has_errors: bool) -> &'static str {
    if read_mode == ReadMode::FullFile && !has_errors {
        "performed_local_emulator"
    } else if read_mode == ReadMode::FullFile {
        "blocked"
    } else {
        "not_requested"
    }
}

fn streaming_read_status(read_mode: ReadMode, has_errors: bool) -> &'static str {
    if read_mode == ReadMode::FullFile && !has_errors {
        "performed_local_emulator_full_file_stream"
    } else {
        "not_performed"
    }
}

fn read_local_emulator_bytes(
    local_path: &Path,
    requested_range: Option<RequestedRange>,
) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(local_path)?;
    let mut bytes = Vec::new();
    if let Some(range) = requested_range {
        file.seek(SeekFrom::Start(range.offset))?;
        let mut limited = file.take(range.length);
        limited.read_to_end(&mut bytes)?;
    } else {
        file.read_to_end(&mut bytes)?;
    }
    Ok(bytes)
}

fn parse_requested_range(raw: &str) -> Result<RequestedRange, ShardLoomError> {
    let Some((offset, length)) = raw.split_once(':') else {
        return Err(ShardLoomError::InvalidOperation(
            "range must use offset:length syntax".to_string(),
        ));
    };
    let offset = offset.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation("range offset must be an unsigned integer".to_string())
    })?;
    let length = length.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation("range length must be an unsigned integer".to_string())
    })?;
    if length == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "range length must be greater than zero".to_string(),
        ));
    }
    Ok(RequestedRange { offset, length })
}

fn is_remote_object_store_uri(source: &str) -> bool {
    source.starts_with("s3://")
        || source.starts_with("gs://")
        || source.starts_with("gcs://")
        || source.starts_with("abfs://")
        || source.starts_with("abfss://")
}

fn normalize_local_emulator_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local-emulator object path must not be empty".to_string(),
        ));
    }
    if let Some(rest) = raw.strip_prefix("file://") {
        if rest.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "file:// path must include a local path".to_string(),
            ));
        }
        let path = if rest.len() >= 3 && rest.as_bytes()[0] == b'/' && rest.as_bytes()[2] == b':' {
            &rest[1..]
        } else {
            rest
        };
        return Ok(PathBuf::from(path));
    }
    if raw.contains("://") {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unsupported URI scheme for local-emulator object-store path: {raw}"
        )));
    }
    Ok(PathBuf::from(raw))
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

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_emulator_range_read_emits_source_state_and_no_fallback_fields() {
        let fixture = std::env::temp_dir().join(format!(
            "shardloom-object-store-read-smoke-{}.bin",
            std::process::id()
        ));
        fs::write(&fixture, b"abcdef").expect("fixture write");

        let report = execute_object_store_read_smoke(
            fixture.to_string_lossy().as_ref(),
            DEFAULT_PROFILE,
            Some(RequestedRange {
                offset: 1,
                length: 3,
            }),
        );
        let fields = object_store_read_smoke_fields(&report);
        fs::remove_file(&fixture).expect("fixture cleanup");

        assert!(!report.has_errors());
        assert_eq!(report.bytes_read, 3);
        assert_eq!(
            output_field(&fields, "byte_range_read_status"),
            "performed_local_emulator"
        );
        assert_eq!(
            output_field(&fields, "credential_resolution_performed"),
            "false"
        );
        assert_eq!(output_field(&fields, "network_probe_performed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "external_engine_invoked"), "false");
        assert_eq!(
            output_field(&fields, "claim_gate_status"),
            "fixture_smoke_only"
        );
        assert!(
            output_field(&fields, "source_state_id").starts_with("object-store-local-emulator-")
        );
    }

    #[test]
    fn remote_provider_is_blocked_without_probe_or_io() {
        let report =
            execute_object_store_read_smoke("s3://bucket/object.vortex", DEFAULT_PROFILE, None);
        let fields = object_store_read_smoke_fields(&report);

        assert!(report.has_errors());
        assert_eq!(
            report.status,
            ObjectStoreReadSmokeStatus::BlockedRemoteProvider
        );
        assert_eq!(output_field(&fields, "object_store_io"), "false");
        assert_eq!(
            output_field(&fields, "credential_resolution_performed"),
            "false"
        );
        assert_eq!(output_field(&fields, "network_probe_performed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(
            report.diagnostics[0].code,
            DiagnosticCode::ObjectStoreUnsupported
        );
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map_or("", |(_, value)| value.as_str())
    }
}
