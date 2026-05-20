//! Provider/profile-scoped object-store runtime smoke handlers.
//!
//! The runtime path in this module is deliberately narrow: it admits only an
//! explicit local-emulator profile backed by a local file path. Real S3, GCS,
//! ADLS, credentials, network probes, writes, table commits, and external query
//! engines remain blocked.

use std::{
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
            "unsupported URI scheme for local-emulator object-store read: {raw}"
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
