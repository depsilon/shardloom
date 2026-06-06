//! Provider/profile-scoped object-store runtime smoke handlers.
//!
//! The runtime path in this module is deliberately narrow: it admits explicit
//! local-emulator reads/writes and a public no-credential fixture read profile.
//! The public fixture profile parses S3/GCS/ADLS URIs and reads caller-supplied
//! local fixture bytes only; it never resolves credentials, probes providers,
//! opens a network connection, writes cache entries, commits tables, or invokes
//! external query engines.

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write as _},
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
const OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND: &str = "object-store-write-recovery-smoke";
const OBJECT_STORE_WRITE_RECOVERY_SMOKE_SCHEMA_VERSION: &str =
    "shardloom.object_store_write_recovery_smoke.v1";
const OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND: &str =
    "object-store-partition-discovery-smoke";
const OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_SCHEMA_VERSION: &str =
    "shardloom.object_store_partition_discovery_smoke.v1";
const LOCAL_EMULATOR_PROFILE: &str = "local-emulator";
const PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE: &str = "public-no-credential-fixture";
const DEFAULT_PROFILE: &str = LOCAL_EMULATOR_PROFILE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStoreReadSmokeStatus {
    Succeeded,
    BlockedRemoteProvider,
    BlockedUnsupportedProfile,
    BlockedInvalidUri,
    BlockedMissingFixturePath,
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
            Self::BlockedInvalidUri => "blocked_invalid_uri",
            Self::BlockedMissingFixturePath => "blocked_missing_fixture_path",
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
    object_store_provider: String,
    object_store_bucket: String,
    object_store_key: String,
    requested_uri: String,
    local_path: Option<PathBuf>,
    fixture_listing_requested: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectStoreReadBlockerContext {
    provider_profile: String,
    requested_uri: String,
    object_store_provider: String,
    object_store_bucket: String,
    object_store_key: String,
    fixture_listing_requested: bool,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
}

impl ObjectStoreReadBlockerContext {
    fn new(
        source: &str,
        profile: &str,
        read_mode: ReadMode,
        requested_range: Option<RequestedRange>,
        fixture_listing_requested: bool,
    ) -> Self {
        Self {
            provider_profile: profile.to_string(),
            requested_uri: source.to_string(),
            object_store_provider: provider_for_source_or_profile(source, profile).to_string(),
            object_store_bucket: "not_available".to_string(),
            object_store_key: "not_available".to_string(),
            fixture_listing_requested,
            read_mode,
            requested_range,
        }
    }

    fn with_redacted_uri(mut self, source: &str) -> Self {
        self.requested_uri = redact_object_store_uri(source);
        self
    }
}

#[derive(Debug)]
struct ObjectStoreReadSuccessInput<'a> {
    source: &'a str,
    profile: &'a str,
    object_store_provider: String,
    object_store_bucket: String,
    object_store_key: String,
    local_path: PathBuf,
    fixture_listing_requested: bool,
    read_mode: ReadMode,
    metadata: &'a fs::Metadata,
    requested_range: Option<RequestedRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObjectStoreDisabledEffectPolicy<'a> {
    credential_policy_status: &'a str,
    network_effect_status: &'a str,
    listing_allowed: bool,
    listing_status: &'a str,
    listing_object_count: usize,
    local_cache_status: &'a str,
    public_no_credential_fixture_profile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedObjectStoreUri {
    provider: &'static str,
    bucket: String,
    key: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStoreWriteRecoveryStatus {
    Recovered,
    BlockedRemoteProvider,
    BlockedUnsupportedProfile,
    BlockedInvalidTarget,
    BlockedMissingObject,
    BlockedMissingCommitManifest,
    BlockedReadError,
    BlockedRecoveryMismatch,
}

impl ObjectStoreWriteRecoveryStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Recovered => "recovered",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedInvalidTarget => "blocked_invalid_target",
            Self::BlockedMissingObject => "blocked_missing_object",
            Self::BlockedMissingCommitManifest => "blocked_missing_commit_manifest",
            Self::BlockedReadError => "blocked_read_error",
            Self::BlockedRecoveryMismatch => "blocked_recovery_mismatch",
        }
    }

    const fn is_error(self) -> bool {
        !matches!(self, Self::Recovered)
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
#[allow(clippy::struct_excessive_bools)]
struct ObjectStoreWriteRecoveryReport {
    status: ObjectStoreWriteRecoveryStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    target_uri: String,
    target_path: Option<PathBuf>,
    commit_manifest_path: Option<PathBuf>,
    expected_idempotency_key: Option<String>,
    recovered_idempotency_key: String,
    idempotency_status: &'static str,
    object_bytes: usize,
    recorded_payload_bytes: usize,
    object_digest: String,
    recorded_payload_digest: String,
    recorded_target_content_digest: String,
    commit_manifest_digest: String,
    target_digest_matched: bool,
    payload_digest_matched: bool,
    payload_bytes_matched: bool,
    target_path_matched: bool,
    manifest_shape_matched: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectStorePartitionDiscoveryStatus {
    Succeeded,
    BlockedRemoteProvider,
    BlockedUnsupportedProfile,
    BlockedMissingRoot,
    BlockedInvalidRoot,
    BlockedListingError,
    BlockedNoPartitions,
}

impl ObjectStorePartitionDiscoveryStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::BlockedRemoteProvider => "blocked_remote_provider",
            Self::BlockedUnsupportedProfile => "blocked_unsupported_profile",
            Self::BlockedMissingRoot => "blocked_missing_root",
            Self::BlockedInvalidRoot => "blocked_invalid_root",
            Self::BlockedListingError => "blocked_listing_error",
            Self::BlockedNoPartitions => "blocked_no_partitions",
        }
    }

    const fn is_error(self) -> bool {
        !matches!(self, Self::Succeeded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectStorePartitionDiscoveryReport {
    status: ObjectStorePartitionDiscoveryStatus,
    diagnostics: Vec<Diagnostic>,
    provider_profile: String,
    requested_uri: String,
    root_path: Option<PathBuf>,
    requested_partition_columns: Vec<String>,
    discovered_partition_columns: Vec<String>,
    discovered_partition_values: Vec<String>,
    partition_directory_count: usize,
    listing_directory_count: usize,
    max_partition_depth: usize,
}

impl ObjectStorePartitionDiscoveryReport {
    fn blocked(
        status: ObjectStorePartitionDiscoveryStatus,
        profile: impl Into<String>,
        requested_uri: impl Into<String>,
        requested_partition_columns: Vec<String>,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: profile.into(),
            requested_uri: requested_uri.into(),
            root_path: None,
            requested_partition_columns,
            discovered_partition_columns: vec![],
            discovered_partition_values: vec![],
            partition_directory_count: 0,
            listing_directory_count: 0,
            max_partition_depth: 0,
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
            "object_store_partition_discovery_smoke(status={}, profile={}, partition_directories={}, discovered_columns={}, object_store_listing_io={}, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.partition_directory_count,
            self.discovered_partition_columns.len(),
            !self.has_errors(),
            claim_gate_status
        )
    }
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

impl ObjectStoreWriteRecoveryReport {
    fn blocked(
        status: ObjectStoreWriteRecoveryStatus,
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
            commit_manifest_path: None,
            expected_idempotency_key,
            recovered_idempotency_key: "not_emitted_blocked".to_string(),
            idempotency_status: "not_emitted_blocked",
            object_bytes: 0,
            recorded_payload_bytes: 0,
            object_digest: "not_emitted_no_object_recovery".to_string(),
            recorded_payload_digest: "not_emitted_no_commit_manifest".to_string(),
            recorded_target_content_digest: "not_emitted_no_commit_manifest".to_string(),
            commit_manifest_digest: "not_emitted_no_commit_manifest".to_string(),
            target_digest_matched: false,
            payload_digest_matched: false,
            payload_bytes_matched: false,
            target_path_matched: false,
            manifest_shape_matched: false,
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
            "object_store_write_recovery_smoke(status={}, profile={}, object_bytes={}, target_digest_matched={}, payload_digest_matched={}, object_store_io={}, fallback_attempted=false, external_engine_invoked=false, claim_gate_status={})",
            self.status.as_str(),
            self.provider_profile,
            self.object_bytes,
            self.target_digest_matched,
            self.payload_digest_matched,
            !self.has_errors(),
            claim_gate_status
        )
    }
}

impl ObjectStoreReadSmokeReport {
    fn blocked(
        status: ObjectStoreReadSmokeStatus,
        context: ObjectStoreReadBlockerContext,
        diagnostic: Diagnostic,
    ) -> Self {
        Self {
            status,
            diagnostics: vec![diagnostic],
            provider_profile: context.provider_profile,
            requested_uri: context.requested_uri,
            object_store_provider: context.object_store_provider,
            object_store_bucket: context.object_store_bucket,
            object_store_key: context.object_store_key,
            local_path: None,
            fixture_listing_requested: context.fixture_listing_requested,
            read_mode: context.read_mode,
            requested_range: context.requested_range,
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
        let claim_gate_status = read_claim_gate_status(self);
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
    let mut public_fixture_path: Option<String> = None;
    let mut fixture_listing_requested = false;
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
            "--public-fixture-path" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_READ_SMOKE_COMMAND,
                        format,
                        "object-store read smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --public-fixture-path".to_string(),
                        ),
                    );
                };
                public_fixture_path = Some(value);
            }
            "--fixture-listing" => {
                fixture_listing_requested = true;
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

    let report = execute_object_store_read_smoke(
        &source,
        &profile,
        requested_range,
        public_fixture_path.as_deref(),
        fixture_listing_requested,
    );
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

pub(crate) fn handle_object_store_write_recovery_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target) = args.next() else {
        return emit_error(
            OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
            format,
            "object-store write recovery smoke failed",
            &ShardLoomError::InvalidOperation(
                "object-store-write-recovery-smoke requires <target-local-object-path>".to_string(),
            ),
        );
    };

    let mut profile = DEFAULT_PROFILE.to_string();
    let mut idempotency_key: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
                        format,
                        "object-store write recovery smoke failed",
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
                        OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
                        format,
                        "object-store write recovery smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --idempotency-key".to_string(),
                        ),
                    );
                };
                let value = value.trim().to_string();
                if value.is_empty() {
                    return emit_error(
                        OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
                        format,
                        "object-store write recovery smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "idempotency key must not be empty".to_string(),
                        ),
                    );
                }
                idempotency_key = Some(value);
            }
            value => {
                return emit_error(
                    OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
                    format,
                    "object-store write recovery smoke failed",
                    &cli_unknown_arg_error(OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND, value),
                );
            }
        }
    }

    let report =
        execute_object_store_write_recovery_smoke(&target, &profile, idempotency_key.as_deref());
    emit_object_store_write_recovery_smoke_report(format, &report)
}

pub(crate) fn handle_object_store_partition_discovery_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(root) = args.next() else {
        return emit_error(
            OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
            format,
            "object-store partition discovery smoke failed",
            &ShardLoomError::InvalidOperation(
                "object-store-partition-discovery-smoke requires <local-partition-root>"
                    .to_string(),
            ),
        );
    };

    let mut profile = DEFAULT_PROFILE.to_string();
    let mut partition_columns: Vec<String> = vec![];
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
                        format,
                        "object-store partition discovery smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --profile".to_string(),
                        ),
                    );
                };
                profile = value;
            }
            "--partition-columns" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
                        format,
                        "object-store partition discovery smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --partition-columns".to_string(),
                        ),
                    );
                };
                partition_columns = parse_partition_columns(&value);
                if partition_columns.is_empty() {
                    return emit_error(
                        OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
                        format,
                        "object-store partition discovery smoke failed",
                        &ShardLoomError::InvalidOperation(
                            "partition columns must not be empty".to_string(),
                        ),
                    );
                }
            }
            value => {
                return emit_error(
                    OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
                    format,
                    "object-store partition discovery smoke failed",
                    &cli_unknown_arg_error(OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND, value),
                );
            }
        }
    }

    let report = execute_object_store_partition_discovery_smoke(&root, &profile, partition_columns);
    emit_object_store_partition_discovery_smoke_report(format, &report)
}

fn emit_blocked_range_parse(
    format: OutputFormat,
    source: &str,
    profile: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let report = ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedInvalidRange,
        ObjectStoreReadBlockerContext::new(source, profile, ReadMode::ByteRange, None, false),
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
        object_store_read_summary(&report.provider_profile).to_string(),
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

fn emit_object_store_write_recovery_smoke_report(
    format: OutputFormat,
    report: &ObjectStoreWriteRecoveryReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    let status = if has_errors {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        OBJECT_STORE_WRITE_RECOVERY_SMOKE_COMMAND,
        format,
        status,
        "object-store local-emulator write recovery smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_write_recovery_smoke_fields(report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn emit_object_store_partition_discovery_smoke_report(
    format: OutputFormat,
    report: &ObjectStorePartitionDiscoveryReport,
) -> ExitCode {
    let has_errors = report.has_errors();
    let status = if has_errors {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_COMMAND,
        format,
        status,
        "object-store local-emulator partition discovery smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_partition_discovery_smoke_fields(report),
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
    public_fixture_path: Option<&str>,
    fixture_listing_requested: bool,
) -> ObjectStoreReadSmokeReport {
    let read_mode = read_mode_for(requested_range);
    if let Some(report) = early_profile_blocker(
        source,
        profile,
        read_mode,
        requested_range,
        public_fixture_path,
        fixture_listing_requested,
    ) {
        return report;
    }
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        return execute_public_fixture_read_smoke(
            source,
            profile,
            requested_range,
            public_fixture_path.expect("checked by early_profile_blocker"),
            fixture_listing_requested,
        );
    }

    let local_path = match normalize_local_emulator_path(source) {
        Ok(path) => path,
        Err(error) => {
            return local_path_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    let metadata = match local_emulator_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return local_metadata_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    if let Some(report) = range_blocker(
        source,
        profile,
        read_mode,
        requested_range,
        metadata.len(),
        fixture_listing_requested,
    ) {
        return report;
    }

    let bytes = match read_local_emulator_bytes(&local_path, requested_range) {
        Ok(bytes) => bytes,
        Err(error) => {
            return read_error_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    successful_object_store_read_report(
        ObjectStoreReadSuccessInput {
            source,
            profile,
            object_store_provider: "local_emulator".to_string(),
            object_store_bucket: "not_applicable".to_string(),
            object_store_key: "not_applicable".to_string(),
            local_path,
            fixture_listing_requested: false,
            read_mode,
            metadata: &metadata,
            requested_range,
        },
        &bytes,
    )
}

fn execute_public_fixture_read_smoke(
    source: &str,
    profile: &str,
    requested_range: Option<RequestedRange>,
    public_fixture_path: &str,
    fixture_listing_requested: bool,
) -> ObjectStoreReadSmokeReport {
    let read_mode = read_mode_for(requested_range);
    let parsed = match parse_object_store_uri(source) {
        Ok(parsed) => parsed,
        Err(error) => {
            return public_fixture_uri_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    let local_path = match normalize_local_emulator_path(public_fixture_path) {
        Ok(path) => path,
        Err(error) => {
            return public_fixture_path_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    let metadata = match local_emulator_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return local_metadata_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    if let Some(report) = range_blocker(
        source,
        profile,
        read_mode,
        requested_range,
        metadata.len(),
        fixture_listing_requested,
    ) {
        return report;
    }
    let bytes = match read_local_emulator_bytes(&local_path, requested_range) {
        Ok(bytes) => bytes,
        Err(error) => {
            return read_error_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            );
        }
    };
    successful_object_store_read_report(
        ObjectStoreReadSuccessInput {
            source,
            profile,
            object_store_provider: parsed.provider.to_string(),
            object_store_bucket: parsed.bucket,
            object_store_key: parsed.key,
            local_path,
            fixture_listing_requested,
            read_mode,
            metadata: &metadata,
            requested_range,
        },
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

#[allow(clippy::too_many_lines)]
fn execute_object_store_write_recovery_smoke(
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<&str>,
) -> ObjectStoreWriteRecoveryReport {
    let expected_idempotency_key = expected_idempotency_key.map(str::to_string);
    if profile != DEFAULT_PROFILE {
        return ObjectStoreWriteRecoveryReport::blocked(
            ObjectStoreWriteRecoveryStatus::BlockedUnsupportedProfile,
            profile,
            target,
            expected_idempotency_key,
            Diagnostic::object_store_blocked(
                "object_store_write_recovery_profile",
                format!("profile {profile} is not admitted for object-store write recovery"),
                "Use --profile local-emulator with a committed local object path.",
            ),
        );
    }
    if is_remote_object_store_uri(target) {
        return ObjectStoreWriteRecoveryReport::blocked(
            ObjectStoreWriteRecoveryStatus::BlockedRemoteProvider,
            profile,
            target,
            expected_idempotency_key,
            Diagnostic::object_store_blocked(
                "object_store_write_recovery_remote_target",
                "real S3/GCS/ADLS recovery remains blocked; no credential, provider, or network probe was performed",
                "Use a committed local-emulator object and sidecar commit manifest for this recovery smoke.",
            ),
        );
    }

    let target_path = match normalize_local_emulator_path(target) {
        Ok(path) => path,
        Err(error) => {
            return write_recovery_target_blocker(
                ObjectStoreWriteRecoveryStatus::BlockedInvalidTarget,
                target,
                profile,
                expected_idempotency_key,
                error.to_string(),
            );
        }
    };
    if target_path.is_dir() {
        return write_recovery_target_blocker(
            ObjectStoreWriteRecoveryStatus::BlockedInvalidTarget,
            target,
            profile,
            expected_idempotency_key,
            "local-emulator recovery target is a directory, not an object path",
        );
    }
    let commit_manifest_path = commit_manifest_sidecar_path(&target_path);
    let object_bytes = match fs::read(&target_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ObjectStoreWriteRecoveryReport::blocked(
                ObjectStoreWriteRecoveryStatus::BlockedMissingObject,
                profile,
                target,
                expected_idempotency_key,
                Diagnostic::new(
                    DiagnosticCode::InvalidInput,
                    DiagnosticSeverity::Error,
                    DiagnosticCategory::InvalidInput,
                    "Object-store write recovery target object is missing.",
                    Some("object_store_write_recovery_object".to_string()),
                    Some("committed local-emulator object could not be found".to_string()),
                    Some(
                        "Run object-store-write-smoke without rollback before recovery."
                            .to_string(),
                    ),
                    FallbackStatus::disabled_by_policy(),
                ),
            );
        }
        Err(error) => {
            return write_recovery_read_error_blocker(
                target,
                profile,
                expected_idempotency_key,
                "object_store_write_recovery_object",
                &error,
            );
        }
    };
    let commit_manifest = match fs::read_to_string(&commit_manifest_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ObjectStoreWriteRecoveryReport::blocked(
                ObjectStoreWriteRecoveryStatus::BlockedMissingCommitManifest,
                profile,
                target,
                expected_idempotency_key,
                Diagnostic::new(
                    DiagnosticCode::CommitNotAtomic,
                    DiagnosticSeverity::Error,
                    DiagnosticCategory::Execution,
                    "Object-store write recovery sidecar manifest is missing.",
                    Some("object_store_write_recovery_commit_manifest".to_string()),
                    Some("sidecar commit manifest could not be found".to_string()),
                    Some("Recover only from an object-store-write-smoke commit with its sidecar manifest."
                        .to_string()),
                    FallbackStatus::disabled_by_policy(),
                ),
            );
        }
        Err(error) => {
            return write_recovery_read_error_blocker(
                target,
                profile,
                expected_idempotency_key,
                "object_store_write_recovery_commit_manifest",
                &error,
            );
        }
    };

    let object_digest = fnv64_digest_bytes(&object_bytes);
    let commit_manifest_digest = fnv64_digest(&commit_manifest);
    let recorded_payload_digest = extract_json_string_field(&commit_manifest, "payload_digest")
        .unwrap_or_else(|| "missing_payload_digest".to_string());
    let recorded_target_content_digest =
        extract_json_string_field(&commit_manifest, "target_content_digest")
            .unwrap_or_else(|| "missing_target_content_digest".to_string());
    let recovered_idempotency_key = extract_json_string_field(&commit_manifest, "idempotency_key")
        .unwrap_or_else(|| "missing_idempotency_key".to_string());
    let recorded_target_path = extract_json_string_field(&commit_manifest, "local_target_path")
        .unwrap_or_else(|| "missing_local_target_path".to_string());
    let recorded_payload_bytes =
        extract_json_usize_field(&commit_manifest, "payload_bytes").unwrap_or(0);
    let target_digest_matched = recorded_target_content_digest == object_digest;
    let payload_digest_matched = recorded_payload_digest == object_digest;
    let payload_bytes_matched = recorded_payload_bytes == object_bytes.len();
    let target_path_matched = recorded_target_path == target_path.to_string_lossy().as_ref();
    let idempotency_matched = expected_idempotency_key
        .as_deref()
        .is_none_or(|expected| expected == recovered_idempotency_key);
    let manifest_shape_matched = commit_manifest
        .contains("\"schema_version\": \"shardloom.object_store_write_commit_manifest.v1\"")
        && commit_manifest.contains("\"commit_protocol\": \"local_emulator_sidecar_manifest\"")
        && commit_manifest.contains("\"write_mode\": \"single_object_staged_commit\"")
        && commit_manifest.contains("\"fallback_attempted\": false")
        && commit_manifest.contains("\"external_engine_invoked\": false");

    if !target_digest_matched
        || !payload_digest_matched
        || !payload_bytes_matched
        || !target_path_matched
        || !idempotency_matched
        || !manifest_shape_matched
    {
        let mut detail = vec![];
        if !target_digest_matched {
            detail.push("target_digest_mismatch");
        }
        if !payload_digest_matched {
            detail.push("payload_digest_mismatch");
        }
        if !payload_bytes_matched {
            detail.push("payload_bytes_mismatch");
        }
        if !target_path_matched {
            detail.push("target_path_mismatch");
        }
        if !idempotency_matched {
            detail.push("idempotency_key_mismatch");
        }
        if !manifest_shape_matched {
            detail.push("commit_manifest_shape_mismatch");
        }
        return ObjectStoreWriteRecoveryReport {
            status: ObjectStoreWriteRecoveryStatus::BlockedRecoveryMismatch,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::CommitNotAtomic,
                DiagnosticSeverity::Error,
                DiagnosticCategory::Execution,
                "Object-store write recovery evidence did not match the committed local object.",
                Some("object_store_write_recovery_evidence".to_string()),
                Some(detail.join(",")),
                Some("Recover only from the sidecar manifest emitted by object-store-write-smoke for the same object and idempotency key."
                    .to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
            provider_profile: profile.to_string(),
            target_uri: target.to_string(),
            target_path: Some(target_path),
            commit_manifest_path: Some(commit_manifest_path),
            expected_idempotency_key,
            recovered_idempotency_key,
            idempotency_status: "recovered_mismatch",
            object_bytes: object_bytes.len(),
            recorded_payload_bytes,
            object_digest,
            recorded_payload_digest,
            recorded_target_content_digest,
            commit_manifest_digest,
            target_digest_matched,
            payload_digest_matched,
            payload_bytes_matched,
            target_path_matched,
            manifest_shape_matched,
        };
    }

    ObjectStoreWriteRecoveryReport {
        status: ObjectStoreWriteRecoveryStatus::Recovered,
        diagnostics: vec![],
        provider_profile: profile.to_string(),
        target_uri: target.to_string(),
        target_path: Some(target_path),
        commit_manifest_path: Some(commit_manifest_path),
        expected_idempotency_key,
        recovered_idempotency_key,
        idempotency_status: "recovered_from_commit_manifest",
        object_bytes: object_bytes.len(),
        recorded_payload_bytes,
        object_digest,
        recorded_payload_digest,
        recorded_target_content_digest,
        commit_manifest_digest,
        target_digest_matched,
        payload_digest_matched,
        payload_bytes_matched,
        target_path_matched,
        manifest_shape_matched,
    }
}

#[allow(clippy::too_many_lines)]
fn execute_object_store_partition_discovery_smoke(
    root: &str,
    profile: &str,
    requested_partition_columns: Vec<String>,
) -> ObjectStorePartitionDiscoveryReport {
    if profile != DEFAULT_PROFILE {
        return ObjectStorePartitionDiscoveryReport::blocked(
            if is_remote_object_store_uri(root) {
                ObjectStorePartitionDiscoveryStatus::BlockedRemoteProvider
            } else {
                ObjectStorePartitionDiscoveryStatus::BlockedUnsupportedProfile
            },
            profile,
            redact_object_store_uri(root),
            requested_partition_columns,
            Diagnostic::object_store_blocked(
                "object_store_partition_discovery_profile",
                format!("profile {profile} is not admitted for partition discovery runtime"),
                "Use --profile local-emulator with a caller-owned local partition directory.",
            ),
        );
    }
    if is_remote_object_store_uri(root) {
        return ObjectStorePartitionDiscoveryReport::blocked(
            ObjectStorePartitionDiscoveryStatus::BlockedRemoteProvider,
            profile,
            redact_object_store_uri(root),
            requested_partition_columns,
            Diagnostic::object_store_blocked(
                "object_store_partition_discovery_remote_provider",
                "live S3/GCS/ADLS partition listing remains blocked; no credential or network probe was performed",
                "Use a local-emulator partition directory for fixture proof, or keep live provider discovery gated.",
            ),
        );
    }

    let root_path = match normalize_local_emulator_path(root) {
        Ok(path) => path,
        Err(error) => {
            return ObjectStorePartitionDiscoveryReport::blocked(
                ObjectStorePartitionDiscoveryStatus::BlockedMissingRoot,
                profile,
                root,
                requested_partition_columns,
                Diagnostic::invalid_input(
                    "object_store_partition_discovery_root",
                    error.to_string(),
                    "Use a local path or file:// path with no remote provider.",
                ),
            );
        }
    };
    let metadata = match fs::metadata(&root_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return ObjectStorePartitionDiscoveryReport::blocked(
                ObjectStorePartitionDiscoveryStatus::BlockedMissingRoot,
                profile,
                root,
                requested_partition_columns,
                Diagnostic::object_store_blocked(
                    "object_store_partition_discovery_root",
                    format!("partition discovery root could not be statted: {error}"),
                    "Create the local-emulator partition root and retry.",
                ),
            );
        }
    };
    if !metadata.is_dir() {
        return ObjectStorePartitionDiscoveryReport::blocked(
            ObjectStorePartitionDiscoveryStatus::BlockedInvalidRoot,
            profile,
            root,
            requested_partition_columns,
            Diagnostic::invalid_input(
                "object_store_partition_discovery_root",
                "partition discovery root is not a directory",
                "Use a directory containing key=value partition folders.",
            ),
        );
    }

    match discover_local_partition_directories(&root_path) {
        Ok(discovery) if discovery.partition_directory_count > 0 => {
            ObjectStorePartitionDiscoveryReport {
                status: ObjectStorePartitionDiscoveryStatus::Succeeded,
                diagnostics: vec![],
                provider_profile: profile.to_string(),
                requested_uri: root.to_string(),
                root_path: Some(root_path),
                requested_partition_columns,
                discovered_partition_columns: discovery.partition_columns,
                discovered_partition_values: discovery.partition_values,
                partition_directory_count: discovery.partition_directory_count,
                listing_directory_count: discovery.listing_directory_count,
                max_partition_depth: discovery.max_partition_depth,
            }
        }
        Ok(_) => ObjectStorePartitionDiscoveryReport::blocked(
            ObjectStorePartitionDiscoveryStatus::BlockedNoPartitions,
            profile,
            root,
            requested_partition_columns,
            Diagnostic::object_store_blocked(
                "object_store_partition_discovery_empty",
                "no key=value partition directories were discovered",
                "Create at least one key=value directory under the local-emulator root.",
            ),
        ),
        Err(error) => ObjectStorePartitionDiscoveryReport::blocked(
            ObjectStorePartitionDiscoveryStatus::BlockedListingError,
            profile,
            root,
            requested_partition_columns,
            Diagnostic::object_store_blocked(
                "object_store_partition_discovery_listing",
                format!("partition discovery listing failed: {error}"),
                "Ensure the local-emulator partition tree can be listed without elevated permissions.",
            ),
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
    public_fixture_path: Option<&str>,
    fixture_listing_requested: bool,
) -> Option<ObjectStoreReadSmokeReport> {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        if let Err(error) = parse_object_store_uri(source) {
            return Some(public_fixture_uri_blocker(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
                &error,
            ));
        }
        if public_fixture_path.map(str::trim).is_none_or(str::is_empty) {
            return Some(ObjectStoreReadSmokeReport::blocked(
                ObjectStoreReadSmokeStatus::BlockedMissingFixturePath,
                ObjectStoreReadBlockerContext::new(
                    source,
                    profile,
                    read_mode,
                    requested_range,
                    fixture_listing_requested,
                ),
                Diagnostic::invalid_input(
                    "object_store_public_fixture_path",
                    "public-no-credential-fixture profile requires --public-fixture-path",
                    "Provide an explicit local fixture file; ShardLoom will not probe the provider or network.",
                ),
            ));
        }
        return None;
    }
    if profile != DEFAULT_PROFILE {
        return Some(ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedUnsupportedProfile,
            ObjectStoreReadBlockerContext::new(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
            ),
            Diagnostic::object_store_blocked(
                "object_store_read_profile",
                format!("profile {profile} is not admitted for object-store read runtime"),
                "Use --profile local-emulator with a local fixture path, or --profile public-no-credential-fixture with a supported object URI and --public-fixture-path.",
            ),
        ));
    }
    if is_remote_object_store_uri(source) {
        return Some(ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedRemoteProvider,
            ObjectStoreReadBlockerContext::new(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
            ),
            Diagnostic::object_store_blocked(
                "object_store_remote_read",
                "real S3/GCS/ADLS providers remain blocked; no credential or network probe was performed",
                "Use --profile public-no-credential-fixture with --public-fixture-path for the approved public fixture read proof, or a local-emulator fixture path.",
            ),
        ));
    }
    None
}

fn public_fixture_uri_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    fixture_listing_requested: bool,
    error: &ShardLoomError,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedInvalidUri,
        ObjectStoreReadBlockerContext::new(
            source,
            profile,
            read_mode,
            requested_range,
            fixture_listing_requested,
        )
        .with_redacted_uri(source),
        Diagnostic::invalid_input(
            "object_store_public_fixture_uri",
            error.to_string(),
            "Use a supported s3://, gs://, gcs://, abfs://, or abfss:// object URI with a bucket/container and object key.",
        ),
    )
}

fn public_fixture_path_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    fixture_listing_requested: bool,
    error: &ShardLoomError,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedReadError,
        ObjectStoreReadBlockerContext::new(
            source,
            profile,
            read_mode,
            requested_range,
            fixture_listing_requested,
        )
        .with_redacted_uri(source),
        Diagnostic::invalid_input(
            "object_store_public_fixture_path",
            error.to_string(),
            "Use a local path or file:// path for the public fixture bytes; remote fixture paths are not admitted.",
        ),
    )
}

fn local_path_blocker(
    source: &str,
    profile: &str,
    read_mode: ReadMode,
    requested_range: Option<RequestedRange>,
    fixture_listing_requested: bool,
    error: &ShardLoomError,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedReadError,
        ObjectStoreReadBlockerContext::new(
            source,
            profile,
            read_mode,
            requested_range,
            fixture_listing_requested,
        )
        .with_redacted_uri(source),
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
    fixture_listing_requested: bool,
    error: &LocalEmulatorMetadataError,
) -> ObjectStoreReadSmokeReport {
    match error {
        LocalEmulatorMetadataError::NotRegularFile => ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedMissingObject,
            ObjectStoreReadBlockerContext::new(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
            )
            .with_redacted_uri(source),
            Diagnostic::object_store_blocked(
                object_store_read_object_feature(profile),
                "object-store read fixture source is not a regular file",
                "Use a regular local fixture file for the admitted profile.",
            ),
        ),
        LocalEmulatorMetadataError::StatFailed(error) => ObjectStoreReadSmokeReport::blocked(
            ObjectStoreReadSmokeStatus::BlockedMissingObject,
            ObjectStoreReadBlockerContext::new(
                source,
                profile,
                read_mode,
                requested_range,
                fixture_listing_requested,
            )
            .with_redacted_uri(source),
            Diagnostic::object_store_blocked(
                object_store_read_object_feature(profile),
                format!("object-store read fixture could not be statted: {error}"),
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
    fixture_listing_requested: bool,
) -> Option<ObjectStoreReadSmokeReport> {
    let range = requested_range?;
    if range.length > 0 && range.offset.saturating_add(range.length) <= object_size_bytes {
        return None;
    }
    Some(ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedInvalidRange,
        ObjectStoreReadBlockerContext::new(
            source,
            profile,
            read_mode,
            requested_range,
            fixture_listing_requested,
        )
        .with_redacted_uri(source),
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
    fixture_listing_requested: bool,
    error: &std::io::Error,
) -> ObjectStoreReadSmokeReport {
    ObjectStoreReadSmokeReport::blocked(
        ObjectStoreReadSmokeStatus::BlockedReadError,
        ObjectStoreReadBlockerContext::new(
            source,
            profile,
            read_mode,
            requested_range,
            fixture_listing_requested,
        )
        .with_redacted_uri(source),
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

fn write_recovery_target_blocker(
    status: ObjectStoreWriteRecoveryStatus,
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<String>,
    message: impl Into<String>,
) -> ObjectStoreWriteRecoveryReport {
    ObjectStoreWriteRecoveryReport::blocked(
        status,
        profile,
        target,
        expected_idempotency_key,
        Diagnostic::invalid_input(
            "object_store_write_recovery_target_path",
            message.into(),
            "Use a committed local object path emitted by object-store-write-smoke.",
        ),
    )
}

fn write_recovery_read_error_blocker(
    target: &str,
    profile: &str,
    expected_idempotency_key: Option<String>,
    feature: &str,
    error: &std::io::Error,
) -> ObjectStoreWriteRecoveryReport {
    ObjectStoreWriteRecoveryReport::blocked(
        ObjectStoreWriteRecoveryStatus::BlockedReadError,
        profile,
        target,
        expected_idempotency_key,
        Diagnostic::new(
            DiagnosticCode::ObjectStoreUnsupported,
            DiagnosticSeverity::Error,
            DiagnosticCategory::ObjectStore,
            "Object-store local-emulator write recovery failed.",
            Some(feature.to_string()),
            Some(error.to_string()),
            Some(
                "Retry with readable local-emulator object and sidecar manifest paths.".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
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

fn backup_object_path(target_path: &Path, idempotency_key: &str) -> PathBuf {
    let parent = target_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = target_path
        .file_name()
        .map_or_else(|| "object".into(), |name| name.to_string_lossy());
    parent.join(".shardloom-object-store-staging").join(format!(
        "{}.{}.backup",
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

struct LocalEmulatorWriteBackups {
    target_backup_path: PathBuf,
    manifest_backup_path: PathBuf,
    target_taken: bool,
    manifest_taken: bool,
}

impl LocalEmulatorWriteBackups {
    fn new(target_path: &Path, commit_manifest_path: &Path, idempotency_key: &str) -> Self {
        Self {
            target_backup_path: backup_object_path(target_path, idempotency_key),
            manifest_backup_path: backup_object_path(commit_manifest_path, idempotency_key),
            target_taken: false,
            manifest_taken: false,
        }
    }

    fn restore(&self, target_path: &Path, commit_manifest_path: &Path) {
        if self.target_taken {
            let _ = fs::rename(&self.target_backup_path, target_path);
        }
        if self.manifest_taken {
            let _ = fs::rename(&self.manifest_backup_path, commit_manifest_path);
        }
    }

    fn restore_required(
        &self,
        target_path: &Path,
        commit_manifest_path: &Path,
    ) -> std::io::Result<()> {
        if self.target_taken {
            fs::rename(&self.target_backup_path, target_path)?;
        }
        if self.manifest_taken {
            fs::rename(&self.manifest_backup_path, commit_manifest_path)?;
        }
        Ok(())
    }

    fn discard(self) -> std::io::Result<()> {
        if self.target_taken {
            remove_file_if_exists(&self.target_backup_path)?;
        }
        if self.manifest_taken {
            remove_file_if_exists(&self.manifest_backup_path)?;
        }
        Ok(())
    }
}

fn prepare_local_emulator_write_backups(
    target_path: &Path,
    commit_manifest_path: &Path,
    idempotency_key: &str,
    allow_overwrite: bool,
) -> std::io::Result<LocalEmulatorWriteBackups> {
    let mut backups =
        LocalEmulatorWriteBackups::new(target_path, commit_manifest_path, idempotency_key);
    if !allow_overwrite {
        return Ok(backups);
    }
    remove_file_if_exists(&backups.target_backup_path)?;
    remove_file_if_exists(&backups.manifest_backup_path)?;
    if target_path.exists() {
        fs::rename(target_path, &backups.target_backup_path)?;
        backups.target_taken = true;
    }
    if commit_manifest_path.exists() {
        match fs::rename(commit_manifest_path, &backups.manifest_backup_path) {
            Ok(()) => backups.manifest_taken = true,
            Err(error) => {
                backups.restore(target_path, commit_manifest_path);
                return Err(error);
            }
        }
    }
    Ok(backups)
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
    let backups = prepare_local_emulator_write_backups(
        target_path,
        commit_manifest_path,
        idempotency_key,
        allow_overwrite,
    )?;
    if allow_overwrite {
        if let Err(error) = fs::rename(staging_path, target_path) {
            let _ = remove_file_if_exists(staging_path);
            backups.restore(target_path, commit_manifest_path);
            return Err(error);
        }
    } else {
        create_target_exclusively_from_staging(staging_path, target_path, payload)?;
    }
    let target_bytes = match fs::read(target_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = remove_file_if_exists(target_path);
            backups.restore(target_path, commit_manifest_path);
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
        backups.restore(target_path, commit_manifest_path);
        return Err(error);
    }

    let mut cleanup_deleted_count = 0;
    let status = if rollback_after_commit {
        cleanup_deleted_count += usize::from(remove_file_if_exists(target_path)?);
        cleanup_deleted_count += usize::from(remove_file_if_exists(commit_manifest_path)?);
        backups.restore_required(target_path, commit_manifest_path)?;
        ObjectStoreWriteSmokeStatus::RolledBack
    } else {
        backups.discard()?;
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

fn create_target_exclusively_from_staging(
    staging_path: &Path,
    target_path: &Path,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut target = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target_path)
    {
        Ok(target) => target,
        Err(error) => {
            let _ = remove_file_if_exists(staging_path);
            return Err(error);
        }
    };
    if let Err(error) = target.write_all(payload) {
        let _ = remove_file_if_exists(target_path);
        let _ = remove_file_if_exists(staging_path);
        return Err(error);
    }
    if let Err(error) = target.sync_all() {
        let _ = remove_file_if_exists(target_path);
        let _ = remove_file_if_exists(staging_path);
        return Err(error);
    }
    remove_file_if_exists(staging_path)?;
    Ok(())
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
    input: ObjectStoreReadSuccessInput<'_>,
    bytes: &[u8],
) -> ObjectStoreReadSmokeReport {
    let read_digest = fnv64_digest_bytes(bytes);
    let mtime_millis = input.metadata.modified().ok().and_then(|mtime| {
        mtime
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis())
    });
    let source_content_digest = read_digest.clone();
    let fingerprint_material = format!(
        "{}|{}|{}|{}|{}|{}",
        input.source,
        input.metadata.len(),
        mtime_millis.unwrap_or_default(),
        input.read_mode.as_str(),
        input.requested_range.map_or(0, |range| range.offset),
        read_digest
    );
    let source_state_digest = fnv64_digest(&fingerprint_material);
    let source_state_prefix = if input.profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "object-store-public-fixture"
    } else {
        "object-store-local-emulator"
    };
    let source_state_id = format!(
        "{}-{}",
        source_state_prefix,
        source_state_digest.replace(':', "-")
    );

    ObjectStoreReadSmokeReport {
        status: ObjectStoreReadSmokeStatus::Succeeded,
        diagnostics: vec![],
        provider_profile: input.profile.to_string(),
        object_store_provider: input.object_store_provider,
        object_store_bucket: input.object_store_bucket,
        object_store_key: input.object_store_key,
        requested_uri: input.source.to_string(),
        local_path: Some(input.local_path),
        fixture_listing_requested: input.fixture_listing_requested,
        read_mode: input.read_mode,
        requested_range: input.requested_range,
        object_size_bytes: input.metadata.len(),
        object_mtime_millis: mtime_millis,
        bytes_read: bytes.len(),
        read_digest: source_content_digest.clone(),
        source_state_id,
        source_state_digest,
        source_content_digest,
        source_fingerprint_kind: if input.profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
            "public_no_credential_fixture_uri_metadata_range_digest"
        } else if input.requested_range.is_some() {
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
    push_object_store_policy_fields(&mut fields, report);
    push_object_store_claim_fields(&mut fields, report);
    fields
}

fn push_object_store_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let local_emulator_path = report.local_path.as_ref().map_or_else(
        || "not_applicable".to_string(),
        |path| {
            if report.provider_profile == LOCAL_EMULATOR_PROFILE {
                path.to_string_lossy().into_owned()
            } else {
                "not_applicable".to_string()
            }
        },
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
        read_runtime_enablement(&report.provider_profile),
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(
        fields,
        "object_store_provider",
        &report.object_store_provider,
    );
    push_field(fields, "object_store_bucket", &report.object_store_bucket);
    push_field(fields, "object_store_key", &report.object_store_key);
    push_field(
        fields,
        "object_store_uri_parse_status",
        object_store_uri_parse_status(report),
    );
    push_field(
        fields,
        "requested_uri",
        &redact_object_store_uri(&report.requested_uri),
    );
    push_field(
        fields,
        "requested_uri_redaction_status",
        requested_uri_redaction_status(&report.requested_uri),
    );
    push_field(fields, "local_emulator_path", &local_emulator_path);
    push_field(
        fields,
        "public_fixture_path",
        &public_fixture_path_field(report),
    );
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
        byte_range_read_status(report, has_errors),
    );
    push_field(
        fields,
        "full_file_read_status",
        full_file_read_status(report, has_errors),
    );
    push_field(
        fields,
        "streaming_read_status",
        streaming_read_status(report, has_errors),
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
    push_field(fields, "object_etag", &object_etag(report));
    push_field(fields, "object_version", &object_version(report));
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

fn push_object_store_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    push_object_store_disabled_effect_policy_fields(
        fields,
        &ObjectStoreDisabledEffectPolicy {
            credential_policy_status: credential_policy_status(
                &report.provider_profile,
                report.has_errors(),
            ),
            network_effect_status: network_effect_status(&report.provider_profile),
            listing_allowed: report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE
                && report.fixture_listing_requested
                && !report.has_errors(),
            listing_status: listing_status(report),
            listing_object_count: listing_object_count(report),
            local_cache_status: local_cache_status(report),
            public_no_credential_fixture_profile: report.provider_profile
                == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE,
        },
    );
}

fn push_object_store_disabled_effect_policy_fields(
    fields: &mut Vec<(String, String)>,
    policy: &ObjectStoreDisabledEffectPolicy<'_>,
) {
    push_field(
        fields,
        "credential_policy_status",
        policy.credential_policy_status,
    );
    push_field(
        fields,
        "network_effect_status",
        policy.network_effect_status,
    );
    push_bool_field(fields, "credential_resolution_allowed", false);
    push_bool_field(fields, "credential_resolution_performed", false);
    push_bool_field(fields, "network_probe_allowed", false);
    push_bool_field(fields, "network_probe_performed", false);
    push_bool_field(fields, "provider_probe_allowed", false);
    push_bool_field(fields, "provider_probe_performed", false);
    push_bool_field(fields, "listing_allowed", policy.listing_allowed);
    push_field(fields, "listing_status", policy.listing_status);
    push_count_field(fields, "listing_object_count", policy.listing_object_count);
    push_bool_field(fields, "cache_write_allowed", false);
    push_field(fields, "local_cache_status", policy.local_cache_status);
    push_bool_field(
        fields,
        "public_no_credential_fixture_profile",
        policy.public_no_credential_fixture_profile,
    );
    push_bool_field(fields, "live_provider_network_read_allowed", false);
}

fn push_object_store_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreReadSmokeReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "native_io_certificate_id",
        native_io_certificate_id(&report.provider_profile),
    );
    push_field(
        fields,
        "native_io_certificate_status",
        read_certificate_status(report),
    );
    push_field(fields, "claim_gate_status", read_claim_gate_status(report));
    push_field(
        fields,
        "object_store_read_claim_gate_status",
        read_claim_gate_status(report),
    );
    push_field(
        fields,
        "claim_boundary",
        read_claim_boundary(&report.provider_profile),
    );
    push_bool_field(fields, "object_store_runtime_supported", !has_errors);
    push_bool_field(
        fields,
        "public_no_credential_fixture_claim_allowed",
        !has_errors && report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE,
    );
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

fn object_store_write_recovery_smoke_fields(
    report: &ObjectStoreWriteRecoveryReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_object_store_write_recovery_identity_fields(&mut fields, report);
    push_object_store_write_recovery_replay_fields(&mut fields, report);
    push_object_store_write_recovery_digest_fields(&mut fields, report);
    push_object_store_write_recovery_policy_fields(&mut fields, report);
    push_object_store_write_recovery_claim_fields(&mut fields, report);
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
    push_object_store_disabled_effect_policy_fields(
        fields,
        &ObjectStoreDisabledEffectPolicy {
            credential_policy_status: "not_required_local_emulator",
            network_effect_status: "not_required_local_emulator",
            listing_allowed: false,
            listing_status: "not_performed_local_emulator_single_object",
            listing_object_count: 0,
            local_cache_status: "not_performed",
            public_no_credential_fixture_profile: false,
        },
    );
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

fn push_object_store_write_recovery_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteRecoveryReport,
) {
    push_field(
        fields,
        "schema_version",
        OBJECT_STORE_WRITE_RECOVERY_SMOKE_SCHEMA_VERSION,
    );
    push_field(fields, "mode", "object_store_write_recovery_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_emulator_object_store_write_recovery_replay",
    );
    push_field(
        fields,
        "report_id",
        "gar-runtime-impl-6d.object_store_write_recovery_smoke",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "object_store_provider", "local_emulator");
    push_field(fields, "target_uri", &report.target_uri);
    push_field(
        fields,
        "local_emulator_target_path",
        &path_field(report.target_path.as_deref()),
    );
    push_field(
        fields,
        "committed_manifest_path",
        &path_field(report.commit_manifest_path.as_deref()),
    );
    push_field(
        fields,
        "object_store_write_recovery_status",
        report.status.as_str(),
    );
}

fn push_object_store_write_recovery_replay_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "recovery_replay_status",
        if has_errors {
            "blocked"
        } else {
            "recovered_local_emulator_sidecar"
        },
    );
    push_field(
        fields,
        "commit_manifest_replay_status",
        if has_errors {
            "blocked"
        } else {
            "recovered_local_emulator_sidecar"
        },
    );
    push_bool_field(fields, "recovery_replay_performed", !has_errors);
    push_bool_field(fields, "committed_object_read_performed", !has_errors);
    push_bool_field(fields, "commit_manifest_read_performed", !has_errors);
}

fn push_object_store_write_recovery_digest_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteRecoveryReport,
) {
    push_bool_field(
        fields,
        "target_digest_matched",
        report.target_digest_matched,
    );
    push_bool_field(
        fields,
        "payload_digest_matched",
        report.payload_digest_matched,
    );
    push_bool_field(
        fields,
        "payload_bytes_matched",
        report.payload_bytes_matched,
    );
    push_bool_field(fields, "target_path_matched", report.target_path_matched);
    push_bool_field(
        fields,
        "commit_manifest_shape_matched",
        report.manifest_shape_matched,
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
    push_count_field(fields, "object_bytes", report.object_bytes);
    push_count_field(
        fields,
        "recorded_payload_bytes",
        report.recorded_payload_bytes,
    );
    push_field(fields, "object_digest", &report.object_digest);
    push_field(
        fields,
        "recorded_payload_digest",
        &report.recorded_payload_digest,
    );
    push_field(
        fields,
        "recorded_target_content_digest",
        &report.recorded_target_content_digest,
    );
    push_field(
        fields,
        "commit_manifest_digest",
        &report.commit_manifest_digest,
    );
}

fn push_object_store_write_recovery_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_object_store_disabled_effect_policy_fields(
        fields,
        &ObjectStoreDisabledEffectPolicy {
            credential_policy_status: "not_required_local_emulator",
            network_effect_status: "not_required_local_emulator",
            listing_allowed: false,
            listing_status: "not_performed_local_emulator_single_object_recovery",
            listing_object_count: 0,
            local_cache_status: "not_performed",
            public_no_credential_fixture_profile: false,
        },
    );
    push_bool_field(fields, "remote_recovery_allowed", false);
    push_bool_field(fields, "local_emulator_recovery_allowed", !has_errors);
    push_bool_field(fields, "write_staging_allowed", false);
}

fn push_object_store_write_recovery_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreWriteRecoveryReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-6d.local_emulator_object_store_write_recovery.native_io",
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
        "local-emulator staged object write recovery proof only; no S3/GCS/ADLS, credential, network, production, table/lakehouse commit, remote result delivery, distributed, performance, or Spark-replacement claim",
    );
    push_bool_field(fields, "object_store_runtime_supported", !has_errors);
    push_bool_field(
        fields,
        "object_store_write_recovery_runtime_supported",
        !has_errors,
    );
    push_bool_field(
        fields,
        "local_emulator_write_recovery_runtime_supported",
        !has_errors,
    );
    push_bool_field(
        fields,
        "live_provider_write_recovery_runtime_supported",
        false,
    );
    push_bool_field(fields, "remote_result_delivery_supported", false);
    push_bool_field(fields, "public_object_store_claim_allowed", false);
    push_bool_field(fields, "production_object_store_claim_allowed", false);
    push_bool_field(fields, "table_runtime_supported", false);
    push_bool_field(fields, "table_commit_allowed", false);
    push_bool_field(fields, "object_store_io", !has_errors);
    push_bool_field(fields, "object_store_read_io", !has_errors);
    push_bool_field(fields, "object_store_write_io", false);
    push_bool_field(fields, "write_io", false);
    push_bool_field(fields, "data_read", !has_errors);
    push_bool_field(fields, "source_io_performed", false);
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

fn object_store_partition_discovery_smoke_fields(
    report: &ObjectStorePartitionDiscoveryReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_partition_discovery_identity_fields(&mut fields, report);
    push_partition_discovery_listing_fields(&mut fields, report);
    push_partition_discovery_policy_fields(&mut fields, report);
    push_partition_discovery_claim_fields(&mut fields, report);
    fields
}

fn push_partition_discovery_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStorePartitionDiscoveryReport,
) {
    let has_errors = report.has_errors();
    push_field(
        fields,
        "schema_version",
        OBJECT_STORE_PARTITION_DISCOVERY_SMOKE_SCHEMA_VERSION,
    );
    push_field(fields, "mode", "object_store_partition_discovery_smoke");
    push_field(
        fields,
        "runtime_enablement",
        "local_emulator_partition_discovery",
    );
    push_field(fields, "provider_profile", &report.provider_profile);
    push_field(fields, "object_store_provider", "local_emulator");
    push_field(
        fields,
        "requested_uri",
        &redact_object_store_uri(&report.requested_uri),
    );
    push_field(
        fields,
        "local_partition_root_path",
        &path_field(report.root_path.as_deref()),
    );
    push_field(fields, "partition_discovery_status", report.status.as_str());
    push_field(
        fields,
        "partition_listing_status",
        if has_errors {
            "blocked"
        } else {
            "performed_local_emulator"
        },
    );
}

fn push_partition_discovery_listing_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStorePartitionDiscoveryReport,
) {
    push_count_field(
        fields,
        "partition_directory_count",
        report.partition_directory_count,
    );
    push_count_field(
        fields,
        "listing_directory_count",
        report.listing_directory_count,
    );
    push_count_field(
        fields,
        "partition_key_count",
        report.discovered_partition_columns.len(),
    );
    push_count_field(
        fields,
        "partition_value_count",
        report.discovered_partition_values.len(),
    );
    push_count_field(fields, "max_partition_depth", report.max_partition_depth);
    push_field(
        fields,
        "requested_partition_columns",
        &joined_or_none(&report.requested_partition_columns),
    );
    push_field(
        fields,
        "discovered_partition_columns",
        &joined_or_none(&report.discovered_partition_columns),
    );
    push_field(
        fields,
        "discovered_partition_values",
        &joined_or_none(&report.discovered_partition_values),
    );
}

fn push_partition_discovery_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStorePartitionDiscoveryReport,
) {
    let has_errors = report.has_errors();
    push_object_store_disabled_effect_policy_fields(
        fields,
        &ObjectStoreDisabledEffectPolicy {
            credential_policy_status: "not_required_local_emulator",
            network_effect_status: "not_required_local_emulator",
            listing_allowed: !has_errors,
            listing_status: if has_errors {
                "blocked"
            } else {
                "performed_local_emulator_partition_listing"
            },
            listing_object_count: report.partition_directory_count,
            local_cache_status: "not_performed",
            public_no_credential_fixture_profile: false,
        },
    );
    push_field(
        fields,
        "native_io_certificate_id",
        "gar-runtime-impl-6d.local_emulator_partition_discovery.native_io",
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
}

fn push_partition_discovery_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStorePartitionDiscoveryReport,
) {
    let has_errors = report.has_errors();
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
        "local-emulator partition discovery over caller-owned key=value directories only; no live S3/GCS/ADLS listing, credential resolution, catalog integration, table commit, remote result delivery, production, performance, or Spark-replacement claim",
    );
    push_bool_field(fields, "object_store_runtime_supported", !has_errors);
    push_bool_field(fields, "partition_discovery_runtime_supported", !has_errors);
    push_bool_field(fields, "live_provider_partition_discovery_supported", false);
    push_bool_field(fields, "catalog_integration_supported", false);
    push_bool_field(fields, "remote_result_delivery_supported", false);
    push_bool_field(fields, "object_store_io", !has_errors);
    push_bool_field(fields, "object_store_listing_io", !has_errors);
    push_bool_field(fields, "object_store_read_io", false);
    push_bool_field(fields, "object_store_write_io", false);
    push_bool_field(fields, "write_io", false);
    push_bool_field(fields, "data_read", false);
    push_bool_field(fields, "source_io_performed", false);
    push_bool_field(fields, "table_metadata_read_performed", false);
    push_bool_field(fields, "table_commit_performed", false);
    push_bool_field(fields, "credential_resolution_performed", false);
    push_bool_field(fields, "network_probe_performed", false);
    push_bool_field(fields, "provider_probe_performed", false);
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

fn byte_range_read_status(report: &ObjectStoreReadSmokeReport, has_errors: bool) -> &'static str {
    if report.read_mode == ReadMode::ByteRange && !has_errors {
        if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
            "performed_public_no_credential_fixture"
        } else {
            "performed_local_emulator"
        }
    } else if report.read_mode == ReadMode::ByteRange {
        "blocked"
    } else {
        "not_requested"
    }
}

fn full_file_read_status(report: &ObjectStoreReadSmokeReport, has_errors: bool) -> &'static str {
    if report.read_mode == ReadMode::FullFile && !has_errors {
        if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
            "performed_public_no_credential_fixture"
        } else {
            "performed_local_emulator"
        }
    } else if report.read_mode == ReadMode::FullFile {
        "blocked"
    } else {
        "not_requested"
    }
}

fn streaming_read_status(report: &ObjectStoreReadSmokeReport, has_errors: bool) -> &'static str {
    if report.read_mode == ReadMode::FullFile && !has_errors {
        if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
            "performed_public_fixture_full_file_stream"
        } else {
            "performed_local_emulator_full_file_stream"
        }
    } else {
        "not_performed"
    }
}

fn object_store_read_summary(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "object-store public fixture read smoke"
    } else {
        "object-store local-emulator read smoke"
    }
}

fn read_runtime_enablement(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "public_no_credential_fixture_object_store_read"
    } else {
        "local_emulator_object_store_read"
    }
}

fn object_store_uri_parse_status(report: &ObjectStoreReadSmokeReport) -> &'static str {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE && !report.has_errors() {
        "parsed_public_no_credential_fixture_uri"
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "blocked_public_no_credential_fixture_uri"
    } else {
        "not_requested_local_emulator"
    }
}

fn credential_policy_status(profile: &str, has_errors: bool) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE && !has_errors {
        "public_no_credential_fixture_admitted"
    } else if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "public_no_credential_fixture_blocked"
    } else {
        "not_required_local_emulator"
    }
}

fn network_effect_status(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "disabled_public_fixture"
    } else {
        "not_required_local_emulator"
    }
}

fn listing_status(report: &ObjectStoreReadSmokeReport) -> &'static str {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE
        && report.fixture_listing_requested
        && !report.has_errors()
    {
        "performed_public_fixture_single_object"
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "not_requested_public_fixture"
    } else {
        "not_performed_local_emulator_single_object"
    }
}

fn listing_object_count(report: &ObjectStoreReadSmokeReport) -> usize {
    usize::from(
        report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE
            && report.fixture_listing_requested
            && !report.has_errors(),
    )
}

fn local_cache_status(report: &ObjectStoreReadSmokeReport) -> &'static str {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "not_performed_public_fixture_read_through"
    } else {
        "not_performed"
    }
}

fn native_io_certificate_id(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "gar-runtime-impl-5k.public_no_credential_fixture_read.native_io"
    } else {
        "gar-runtime-impl-4n.local_emulator_object_store_read.native_io"
    }
}

fn read_certificate_status(report: &ObjectStoreReadSmokeReport) -> &'static str {
    if report.has_errors() {
        "blocked"
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "public_fixture_smoke_only"
    } else {
        "fixture_smoke_only"
    }
}

fn read_claim_gate_status(report: &ObjectStoreReadSmokeReport) -> &'static str {
    if report.has_errors() {
        "not_claim_grade"
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "public_fixture_smoke_only"
    } else {
        "fixture_smoke_only"
    }
}

fn read_claim_boundary(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "public no-credential object-store fixture read smoke only; URI parsing, SourceState, byte-range/full-file fixture bytes, optional fixture listing, Native I/O evidence, and no-fallback fields are admitted without credentials or network probes; live S3/GCS/ADLS provider reads, authenticated reads, cache writes, cloud writes, table commits, distributed runtime, production use, performance, and Spark-replacement claims remain blocked"
    } else {
        "local-emulator object-store read smoke only; no S3/GCS/ADLS, credential, network, production, table, commit, distributed, performance, or Spark-replacement claim"
    }
}

fn public_fixture_path_field(report: &ObjectStoreReadSmokeReport) -> String {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        path_field(report.local_path.as_deref())
    } else {
        "not_applicable".to_string()
    }
}

fn object_etag(report: &ObjectStoreReadSmokeReport) -> String {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE && !report.has_errors() {
        format!("public-fixture-{}", report.read_digest.replace(':', "-"))
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "not_emitted_no_public_fixture_read".to_string()
    } else {
        "not_applicable_local_emulator".to_string()
    }
}

fn object_version(report: &ObjectStoreReadSmokeReport) -> String {
    if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE && !report.has_errors() {
        format!(
            "public-fixture-mtime-{}",
            report.object_mtime_millis.unwrap_or_default()
        )
    } else if report.provider_profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "not_emitted_no_public_fixture_read".to_string()
    } else {
        "not_applicable_local_emulator".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalPartitionDiscovery {
    partition_columns: Vec<String>,
    partition_values: Vec<String>,
    partition_directory_count: usize,
    listing_directory_count: usize,
    max_partition_depth: usize,
}

fn discover_local_partition_directories(
    root: &Path,
) -> Result<LocalPartitionDiscovery, std::io::Error> {
    let mut columns = BTreeSet::new();
    let mut values = BTreeSet::new();
    let mut partition_directory_count = 0usize;
    let mut listing_directory_count = 0usize;
    let mut max_partition_depth = 0usize;
    discover_local_partition_directories_inner(
        root,
        0,
        &mut columns,
        &mut values,
        &mut partition_directory_count,
        &mut listing_directory_count,
        &mut max_partition_depth,
    )?;
    Ok(LocalPartitionDiscovery {
        partition_columns: columns.into_iter().collect(),
        partition_values: values.into_iter().collect(),
        partition_directory_count,
        listing_directory_count,
        max_partition_depth,
    })
}

fn discover_local_partition_directories_inner(
    path: &Path,
    depth: usize,
    columns: &mut BTreeSet<String>,
    values: &mut BTreeSet<String>,
    partition_directory_count: &mut usize,
    listing_directory_count: &mut usize,
    max_partition_depth: &mut usize,
) -> Result<(), std::io::Error> {
    *listing_directory_count += 1;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let child_depth = depth + 1;
        if let Some((key, value)) = parse_partition_directory_name(&name) {
            columns.insert(key.to_string());
            values.insert(format!("{key}={value}"));
            *partition_directory_count += 1;
            *max_partition_depth = (*max_partition_depth).max(child_depth);
        }
        discover_local_partition_directories_inner(
            &entry.path(),
            child_depth,
            columns,
            values,
            partition_directory_count,
            listing_directory_count,
            max_partition_depth,
        )?;
    }
    Ok(())
}

fn parse_partition_directory_name(name: &str) -> Option<(&str, &str)> {
    let (key, value) = name.split_once('=')?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        None
    } else {
        Some((key, value))
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

fn parse_partition_columns(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn is_remote_object_store_uri(source: &str) -> bool {
    let Some((scheme, _)) = source.split_once("://") else {
        return false;
    };
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "s3" | "gs" | "gcs" | "abfs" | "abfss"
    )
}

fn parse_object_store_uri(source: &str) -> Result<ParsedObjectStoreUri, ShardLoomError> {
    let (scheme, rest) = source.split_once("://").ok_or_else(|| {
        ShardLoomError::InvalidOperation("object-store URI must include a scheme".to_string())
    })?;
    let scheme = scheme.to_ascii_lowercase();
    let provider = match scheme.as_str() {
        "s3" => "s3",
        "gs" | "gcs" => "gcs",
        "abfs" | "abfss" => "adls",
        _ => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "unsupported object-store URI scheme: {scheme}"
            )));
        }
    };
    if rest.contains('?') || rest.contains('#') {
        return Err(ShardLoomError::InvalidOperation(
            "public fixture object URI must not include query strings or fragments".to_string(),
        ));
    }
    let Some((bucket, key)) = rest.split_once('/') else {
        return Err(ShardLoomError::InvalidOperation(
            "object-store URI must include bucket/container and object key".to_string(),
        ));
    };
    if bucket.trim().is_empty() || key.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "object-store URI bucket/container and object key must be non-empty".to_string(),
        ));
    }
    validate_object_store_authority(scheme.as_str(), bucket, key)?;
    Ok(ParsedObjectStoreUri {
        provider,
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

fn validate_object_store_authority(
    scheme: &str,
    authority: &str,
    key: &str,
) -> Result<(), ShardLoomError> {
    if key.contains('@') {
        return Err(ShardLoomError::InvalidOperation(
            "object-store URI object key must not include credentials or userinfo".to_string(),
        ));
    }
    if matches!(scheme, "abfs" | "abfss") {
        if authority.matches('@').count() > 1 {
            return Err(ShardLoomError::InvalidOperation(
                "ADLS object-store URI authority must contain at most one container/account separator".to_string(),
            ));
        }
        if let Some((container, account)) = authority.split_once('@') {
            if container.trim().is_empty() || account.trim().is_empty() {
                return Err(ShardLoomError::InvalidOperation(
                    "ADLS object-store URI container and account authority must be non-empty"
                        .to_string(),
                ));
            }
            if container.contains(':') {
                return Err(ShardLoomError::InvalidOperation(
                    "object-store URI must not include credentials or userinfo".to_string(),
                ));
            }
        }
        return Ok(());
    }
    if authority.contains('@') {
        return Err(ShardLoomError::InvalidOperation(
            "object-store URI must not include credentials or userinfo".to_string(),
        ));
    }
    Ok(())
}

fn provider_for_source_or_profile(source: &str, profile: &str) -> &'static str {
    if profile == LOCAL_EMULATOR_PROFILE {
        return "local_emulator";
    }
    parse_object_store_uri(source).map_or("unknown", |parsed| parsed.provider)
}

fn object_store_read_object_feature(profile: &str) -> &'static str {
    if profile == PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE {
        "object_store_public_fixture_object"
    } else {
        "object_store_local_emulator_object"
    }
}

fn redact_object_store_uri(source: &str) -> String {
    let without_query = source.split(['?', '#']).next().unwrap_or(source);
    let Some((scheme, rest)) = without_query.split_once("://") else {
        return without_query.to_string();
    };
    if object_store_uri_has_userinfo(scheme, rest) {
        let tail = rest.rsplit_once('@').map_or(rest, |(_, tail)| tail);
        format!("{scheme}://<redacted>@{tail}")
    } else {
        without_query.to_string()
    }
}

fn requested_uri_redaction_status(source: &str) -> &'static str {
    if source.contains('?') || source.contains('#') {
        return "redacted";
    }
    let Some((scheme, rest)) = source.split_once("://") else {
        return "not_required";
    };
    if object_store_uri_has_userinfo(scheme, rest) {
        "redacted"
    } else {
        "not_required"
    }
}

fn object_store_uri_has_userinfo(scheme: &str, rest: &str) -> bool {
    let authority = rest.split('/').next().unwrap_or(rest);
    let scheme = scheme.to_ascii_lowercase();
    if matches!(scheme.as_str(), "abfs" | "abfss") {
        authority.matches('@').count() > 1
            || authority
                .split_once('@')
                .is_some_and(|(container, _)| container.contains(':'))
    } else {
        authority.contains('@')
    }
}

fn normalize_local_emulator_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    if raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local-emulator object path must not be empty".to_string(),
        ));
    }
    if let Some((scheme, rest)) = raw.split_once("://") {
        if !scheme.eq_ignore_ascii_case("file") {
            return Err(ShardLoomError::InvalidOperation(format!(
                "unsupported URI scheme for local-emulator object-store path: {raw}"
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

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn joined_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(",")
    }
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
            None,
            false,
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
        let report = execute_object_store_read_smoke(
            "s3://bucket/object.vortex",
            DEFAULT_PROFILE,
            None,
            None,
            false,
        );
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

    #[test]
    fn public_no_credential_fixture_reads_remote_uri_shape_without_network() {
        let fixture = std::env::temp_dir().join(format!(
            "shardloom-object-store-public-fixture-{}.bin",
            std::process::id()
        ));
        fs::write(&fixture, b"abcdef").expect("fixture write");

        let report = execute_object_store_read_smoke(
            "s3://public-bucket/orders.vortex",
            PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE,
            Some(RequestedRange {
                offset: 2,
                length: 2,
            }),
            Some(fixture.to_string_lossy().as_ref()),
            true,
        );
        let fields = object_store_read_smoke_fields(&report);
        fs::remove_file(&fixture).expect("fixture cleanup");

        assert!(!report.has_errors());
        assert_eq!(report.object_store_provider, "s3");
        assert_eq!(report.object_store_bucket, "public-bucket");
        assert_eq!(report.object_store_key, "orders.vortex");
        assert_eq!(
            output_field(&fields, "object_store_uri_parse_status"),
            "parsed_public_no_credential_fixture_uri"
        );
        assert_eq!(
            output_field(&fields, "byte_range_read_status"),
            "performed_public_no_credential_fixture"
        );
        assert_eq!(
            output_field(&fields, "credential_policy_status"),
            "public_no_credential_fixture_admitted"
        );
        assert_eq!(output_field(&fields, "network_probe_performed"), "false");
        assert_eq!(output_field(&fields, "provider_probe_performed"), "false");
        assert_eq!(
            output_field(&fields, "listing_status"),
            "performed_public_fixture_single_object"
        );
        assert_eq!(
            output_field(&fields, "native_io_certificate_status"),
            "public_fixture_smoke_only"
        );
        assert_eq!(
            output_field(&fields, "claim_gate_status"),
            "public_fixture_smoke_only"
        );
        assert_eq!(
            output_field(&fields, "public_no_credential_fixture_claim_allowed"),
            "true"
        );
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "external_engine_invoked"), "false");
        assert!(
            output_field(&fields, "source_state_id").starts_with("object-store-public-fixture-")
        );
    }

    #[test]
    fn public_fixture_accepts_adls_container_account_authority() {
        let fixture = std::env::temp_dir().join(format!(
            "shardloom-object-store-public-adls-fixture-{}.bin",
            std::process::id()
        ));
        fs::write(&fixture, b"abcdef").expect("fixture write");

        let report = execute_object_store_read_smoke(
            "abfss://public-container@storageacct.dfs.core.windows.net/orders.vortex",
            PUBLIC_NO_CREDENTIAL_FIXTURE_PROFILE,
            None,
            Some(fixture.to_string_lossy().as_ref()),
            false,
        );
        let fields = object_store_read_smoke_fields(&report);
        fs::remove_file(&fixture).expect("fixture cleanup");

        assert!(!report.has_errors());
        assert_eq!(report.object_store_provider, "adls");
        assert_eq!(
            report.object_store_bucket,
            "public-container@storageacct.dfs.core.windows.net"
        );
        assert_eq!(report.object_store_key, "orders.vortex");
        assert_eq!(
            output_field(&fields, "requested_uri_redaction_status"),
            "not_required"
        );
    }

    #[test]
    fn uri_redaction_keeps_valid_adls_authority_visible() {
        assert_eq!(
            redact_object_store_uri("abfss://container@account.dfs.core.windows.net/path.vortex"),
            "abfss://container@account.dfs.core.windows.net/path.vortex"
        );
        assert_eq!(
            requested_uri_redaction_status(
                "abfss://container@account.dfs.core.windows.net/path.vortex"
            ),
            "not_required"
        );
        assert_eq!(
            redact_object_store_uri("s3://user@bucket/path.vortex?token=secret"),
            "s3://<redacted>@bucket/path.vortex"
        );
        assert_eq!(
            requested_uri_redaction_status("s3://user@bucket/path.vortex"),
            "redacted"
        );
    }

    #[test]
    fn local_file_uri_normalization_strips_localhost_authority() {
        assert_eq!(
            normalize_local_emulator_path("file://localhost/tmp/orders.vortex").unwrap(),
            PathBuf::from("/tmp/orders.vortex")
        );
        assert_eq!(
            normalize_local_emulator_path("FILE:///C:/tmp/orders.vortex").unwrap(),
            PathBuf::from("C:/tmp/orders.vortex")
        );
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map_or("", |(_, value)| value.as_str())
    }
}
