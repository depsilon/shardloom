//! Extension and UDF planning CLI handlers.
//!
//! These handlers emit metadata-only extension and UDF reports. They do not
//! dynamically load extension code, execute UDFs, write data, invoke external
//! services, or provide fallback execution.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use serde_json::Value;
use shardloom_core::{
    CommandStatus, DeterministicEmbeddingVectorFixtureReport, DeterministicScalarUdfFixtureReport,
    EffectLevel, EffectfulOperationAdmissionMatrix, EffectfulOperationAdmissionRow,
    ExtensionAuditContract, ExtensionCapability, ExtensionCapabilityStatus, ExtensionCategory,
    ExtensionDeterminismContract, ExtensionEffectDeclaration, ExtensionExecutionContract,
    ExtensionId, ExtensionIdempotencyContract, ExtensionInspectionReport,
    ExtensionInspectionStatus, ExtensionLicenseKind, ExtensionLifecycleState, ExtensionManifest,
    ExtensionManifestEffectCapabilityMatrix, ExtensionManifestEffectCapabilityRow,
    ExtensionMaterializationContract, ExtensionNullBehaviorContract, ExtensionPermission,
    ExtensionProvenance, ExtensionRegistrySnapshot, ExtensionRetryContract, ExtensionVersion,
    ExternalEffectKind, OutputFormat, PermissionKind, PluginAbiRequirement, PluginAbiStatus,
    PluginAbiUdfSandboxBlockerReport, PluginAbiUdfSandboxBlockerRow, SandboxPolicy,
    SandboxPolicyKind, ShardLoomError, TypedUdfRegistryEntry, TypedUdfRegistryReport,
    UdfRuntimeKind, plan_plugin_abi_udf_sandbox_blocker,
    run_deterministic_embedding_vector_fixture, run_deterministic_scalar_udf_fixture,
    typed_udf_registry_report,
};

use crate::cli_output::{emit, emit_error};

const EXTENSION_MANIFEST_SCHEMA_VERSION: &str = "shardloom.extension_manifest.v1";
const EXTENSION_MANIFEST_INSPECTION_SCHEMA_VERSION: &str =
    "shardloom.extension_manifest_inspection.v1";
const EXTENSION_REGISTRY_SNAPSHOT_SCHEMA_VERSION: &str = "shardloom.extension_registry_snapshot.v1";
const MAX_EXTENSION_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_EXTENSION_REGISTRY_MANIFESTS: usize = 64;
const MAX_EXTENSION_REGISTRY_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn handle_extension_registry(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let (snapshot, input) = match args.next().as_deref() {
        None => (
            ExtensionRegistrySnapshot::empty(),
            ExtensionRegistryInput::EmptyRegistry,
        ),
        Some("--manifest-dir") => {
            let Some(path_raw) = args.next() else {
                return emit_error(
                    "extension-registry",
                    format,
                    "extension registry discovery failed",
                    &ShardLoomError::InvalidOperation("missing --manifest-dir path".to_string()),
                );
            };
            if let Some(extra) = args.next() {
                return emit_error(
                    "extension-registry",
                    format,
                    "extension registry discovery failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unexpected extension-registry argument after --manifest-dir path: {extra}"
                    )),
                );
            }
            match discover_extension_manifest_directory(&path_raw) {
                Ok(value) => value,
                Err(error) => {
                    return emit_error(
                        "extension-registry",
                        format,
                        "extension registry discovery failed",
                        &error,
                    );
                }
            }
        }
        Some(other) => {
            return emit_error(
                "extension-registry",
                format,
                "extension registry discovery failed",
                &ShardLoomError::InvalidOperation(format!(
                    "unknown extension-registry argument: {other}"
                )),
            );
        }
    };
    let status = if snapshot.has_errors() {
        CommandStatus::Error
    } else if snapshot.requires_review_count() > 0 {
        CommandStatus::Warning
    } else {
        CommandStatus::Success
    };
    emit(
        "extension-registry",
        format,
        status,
        "extension registry metadata-only snapshot".to_string(),
        snapshot.to_human_text(),
        snapshot.diagnostics.clone(),
        extension_registry_fields(&snapshot, &input),
    );
    ExitCode::SUCCESS
}

#[derive(Debug, Clone)]
enum ExtensionRegistryInput {
    EmptyRegistry,
    LocalManifestDirectory {
        path: PathBuf,
        entry_count: usize,
        manifest_file_count: usize,
        file_read_request_count: usize,
        bytes_read: usize,
    },
}

pub(crate) fn handle_extension_inspect(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(first_arg) = args.next() else {
        return emit_error(
            "extension-inspect",
            format,
            "extension inspect failed",
            &ShardLoomError::InvalidOperation(
                "missing extension_id or --manifest <local-manifest.json>".to_string(),
            ),
        );
    };
    if first_arg == "--manifest" {
        let Some(path_raw) = args.next() else {
            return emit_error(
                "extension-inspect",
                format,
                "extension inspect failed",
                &ShardLoomError::InvalidOperation("missing --manifest path".to_string()),
            );
        };
        if let Some(extra) = args.next() {
            return emit_error(
                "extension-inspect",
                format,
                "extension inspect failed",
                &ShardLoomError::InvalidOperation(format!(
                    "unexpected extension-inspect argument after --manifest path: {extra}"
                )),
            );
        }
        return handle_extension_manifest_inspect(&path_raw, format);
    }
    if let Some(extra) = args.next() {
        return emit_error(
            "extension-inspect",
            format,
            "extension inspect failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected extension-inspect argument after extension_id: {extra}"
            )),
        );
    }
    let extension_id = first_arg;
    let id = match ExtensionId::new(extension_id.clone()) {
        Ok(v) => v,
        Err(e) => return emit_error("extension-inspect", format, "extension inspect failed", &e),
    };
    let manifest = match ExtensionManifest::new(
        id,
        extension_id,
        ExtensionVersion::new(0, 1, 0),
        shardloom_core::ExtensionCategory::Unknown,
        ExtensionProvenance::new(ExtensionLicenseKind::Unknown),
    ) {
        Ok(v) => v,
        Err(e) => return emit_error("extension-inspect", format, "extension inspect failed", &e),
    };
    let report = ExtensionInspectionReport::requires_review(
        manifest,
        "Extension inspection is metadata-only and requires provenance review.",
    );
    let status = if report.has_errors() {
        CommandStatus::Warning
    } else {
        CommandStatus::Success
    };
    emit(
        "extension-inspect",
        format,
        status,
        "extension inspection metadata-only report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        extension_inspection_fields(&report, ExtensionManifestInput::SyntheticId),
    );
    ExitCode::SUCCESS
}

#[derive(Debug, Clone)]
enum ExtensionManifestInput {
    SyntheticId,
    LocalManifestFile {
        path: PathBuf,
        bytes_read: usize,
        manifest_schema_version: String,
    },
}

#[derive(Debug, Clone)]
struct ParsedLocalExtensionManifest {
    manifest: ExtensionManifest,
    bytes_read: usize,
    manifest_schema_version: String,
}

fn handle_extension_manifest_inspect(path_raw: &str, format: OutputFormat) -> ExitCode {
    let path = match normalize_local_extension_manifest_path(path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "extension-inspect",
                format,
                "extension inspect failed",
                &error,
            );
        }
    };
    let parsed = match parse_local_extension_manifest_file(&path) {
        Ok(parsed) => parsed,
        Err(error) => {
            return emit_error(
                "extension-inspect",
                format,
                "extension inspect failed",
                &error,
            );
        }
    };
    let report = extension_inspection_report_from_manifest(parsed.manifest);
    let status = if report.has_errors() {
        CommandStatus::Error
    } else if report.status == ExtensionInspectionStatus::RequiresReview {
        CommandStatus::Warning
    } else {
        CommandStatus::Success
    };
    emit(
        "extension-inspect",
        format,
        status,
        "extension manifest inspection report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        extension_inspection_fields(
            &report,
            ExtensionManifestInput::LocalManifestFile {
                path,
                bytes_read: parsed.bytes_read,
                manifest_schema_version: parsed.manifest_schema_version,
            },
        ),
    );
    ExitCode::SUCCESS
}

fn extension_inspection_report_from_manifest(
    manifest: ExtensionManifest,
) -> ExtensionInspectionReport {
    let mut reasons = Vec::new();
    if manifest.provenance.requires_review() {
        reasons.push("license_or_provenance_review_required");
    }
    if manifest
        .permissions
        .iter()
        .any(ExtensionPermission::is_effectful)
    {
        reasons.push("effectful_permission_declared");
    }
    if manifest
        .effects
        .iter()
        .any(ExtensionEffectDeclaration::is_effectful)
    {
        reasons.push("effectful_operation_declared");
    }
    if manifest
        .capabilities
        .iter()
        .any(ExtensionCapability::is_usable)
    {
        reasons.push("supported_capability_claim_declared");
    }
    if !manifest.execution_contract.production_contract_complete() {
        reasons.push("execution_contract_incomplete");
    }
    if reasons.is_empty() {
        let mut report = ExtensionInspectionReport::metadata_only(manifest);
        report.status = ExtensionInspectionStatus::Validated;
        return report;
    }
    ExtensionInspectionReport::requires_review(
        manifest,
        format!(
            "Manifest is parseable and code-free but needs manual review before enablement: {}",
            reasons.join(",")
        ),
    )
}

fn normalize_local_extension_manifest_path(raw: &str) -> Result<PathBuf, ShardLoomError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "extension manifest path must not be empty".to_string(),
        ));
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("file://") {
        let rest = &trimmed["file://".len()..];
        if rest.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "file:// extension manifest path must include a local path".to_string(),
            ));
        }
        return local_file_uri_path(rest);
    }
    if trimmed.contains("://") {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest inspection admits local files only, not remote URI: {trimmed}"
        )));
    }
    Ok(PathBuf::from(trimmed))
}

fn local_file_uri_path(rest: &str) -> Result<PathBuf, ShardLoomError> {
    if let Some(path) = rest.strip_prefix("localhost/") {
        return Ok(PathBuf::from(format!("/{path}")));
    }
    if rest.starts_with('/') {
        return Ok(PathBuf::from(rest));
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "file:// extension manifest path must use empty or localhost authority, got: file://{rest}"
    )))
}

fn discover_extension_manifest_directory(
    path_raw: &str,
) -> Result<(ExtensionRegistrySnapshot, ExtensionRegistryInput), ShardLoomError> {
    let path = normalize_local_extension_manifest_path(path_raw)?;
    let metadata = fs::symlink_metadata(&path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect extension manifest directory '{}': {error}; no extension code was loaded",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest directory '{}' is a symlink; inspect an approved regular local directory",
            path.display()
        )));
    }
    if !metadata.is_dir() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest directory '{}' is not a regular local directory",
            path.display()
        )));
    }

    let mut entry_count = 0usize;
    let mut manifest_paths = Vec::new();
    for entry in fs::read_dir(&path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read extension manifest directory '{}': {error}; no extension code was loaded",
            path.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to inspect extension manifest directory entry in '{}': {error}; no extension code was loaded",
                path.display()
            ))
        })?;
        entry_count += 1;
        let entry_path = entry.path();
        if entry_path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            manifest_paths.push(entry_path);
        }
    }
    manifest_paths.sort();
    if manifest_paths.len() > MAX_EXTENSION_REGISTRY_MANIFESTS {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest directory '{}' has {} manifest files, over the {} manifest discovery limit",
            path.display(),
            manifest_paths.len(),
            MAX_EXTENSION_REGISTRY_MANIFESTS
        )));
    }

    let mut snapshot = ExtensionRegistrySnapshot::empty();
    let mut seen_ids = HashSet::new();
    let mut bytes_read = 0usize;
    for manifest_path in &manifest_paths {
        let parsed = parse_local_extension_manifest_file(manifest_path)?;
        bytes_read = bytes_read.checked_add(parsed.bytes_read).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "extension manifest directory byte count overflowed".to_string(),
            )
        })?;
        if bytes_read > MAX_EXTENSION_REGISTRY_BYTES {
            return Err(ShardLoomError::InvalidOperation(format!(
                "extension manifest directory '{}' read {} bytes, over the {} byte registry discovery limit",
                path.display(),
                bytes_read,
                MAX_EXTENSION_REGISTRY_BYTES
            )));
        }
        let manifest_id = parsed.manifest.id.as_str().to_string();
        if !seen_ids.insert(manifest_id.clone()) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "duplicate extension manifest id {manifest_id:?} in approved manifest directory '{}'",
                path.display()
            )));
        }
        snapshot.add_manifest(parsed.manifest);
    }

    Ok((
        snapshot,
        ExtensionRegistryInput::LocalManifestDirectory {
            path,
            entry_count,
            manifest_file_count: manifest_paths.len(),
            file_read_request_count: manifest_paths.len(),
            bytes_read,
        },
    ))
}

fn read_local_extension_manifest_bytes(path: &Path) -> Result<Vec<u8>, ShardLoomError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to inspect extension manifest '{}': {error}; no extension code was loaded",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest path '{}' is a symlink; inspect a regular local manifest file",
            path.display()
        )));
    }
    if !metadata.is_file() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest path '{}' is not a regular file",
            path.display()
        )));
    }
    if metadata.len() > MAX_EXTENSION_MANIFEST_BYTES {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension manifest '{}' is {} bytes, over the {} byte inspection limit",
            path.display(),
            metadata.len(),
            MAX_EXTENSION_MANIFEST_BYTES
        )));
    }
    fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read extension manifest '{}': {error}; no extension code was loaded",
            path.display()
        ))
    })
}

fn parse_local_extension_manifest_file(
    path: &Path,
) -> Result<ParsedLocalExtensionManifest, ShardLoomError> {
    let bytes = read_local_extension_manifest_bytes(path)?;
    let bytes_read = bytes.len();
    let manifest_text = std::str::from_utf8(&bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "extension manifest must be valid UTF-8 JSON: {error}; no extension code was loaded"
        ))
    })?;
    let json = serde_json::from_str::<Value>(manifest_text).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "extension manifest JSON parse failed: {error}; no extension code was loaded"
        ))
    })?;
    let manifest_schema_version = json_string(&json, "schema_version")?.to_string();
    let manifest = parse_extension_manifest_json(&json)?;
    Ok(ParsedLocalExtensionManifest {
        manifest,
        bytes_read,
        manifest_schema_version,
    })
}

fn parse_extension_manifest_json(value: &Value) -> Result<ExtensionManifest, ShardLoomError> {
    let schema_version = json_string(value, "schema_version")?;
    if schema_version != EXTENSION_MANIFEST_SCHEMA_VERSION {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unsupported extension manifest schema_version {schema_version:?}; expected {EXTENSION_MANIFEST_SCHEMA_VERSION}"
        )));
    }
    let extension_id = json_string(value, "extension_id")
        .or_else(|_| json_string(value, "id"))?
        .to_string();
    let name = json_string(value, "name")?.to_string();
    let version = parse_extension_version(json_string(value, "version")?)?;
    let category = parse_extension_category(json_string(value, "category")?)?;
    let mut provenance =
        ExtensionProvenance::new(parse_extension_license(json_string(value, "license")?)?);
    if let Some(source) = optional_json_string(value, "source") {
        provenance = provenance.with_source(source);
    }
    if let Some(homepage) = optional_json_string(value, "homepage") {
        provenance = provenance.with_homepage(homepage);
    }
    if let Some(notes) = optional_json_string(value, "provenance_notes") {
        provenance = provenance.with_notes(notes);
    }
    let mut manifest = ExtensionManifest::new(
        ExtensionId::new(extension_id)?,
        name,
        version,
        category,
        provenance,
    )?;
    if let Some(provider) = optional_json_string(value, "provider") {
        manifest = manifest.with_provider(provider);
    }
    if let Some(lifecycle) = optional_json_string(value, "lifecycle") {
        manifest = manifest.with_lifecycle(parse_extension_lifecycle(&lifecycle)?);
    }
    if let Some(runtime) = optional_json_string(value, "runtime") {
        manifest = manifest.with_runtime(parse_udf_runtime_kind(&runtime)?);
    }
    if let Some(abi) = value.get("abi") {
        manifest = manifest.with_abi(parse_plugin_abi_requirement(abi)?);
    }
    if let Some(sandbox) = value.get("sandbox") {
        manifest = manifest.with_sandbox(parse_sandbox_policy(sandbox)?);
    }
    if let Some(capabilities) = value.get("capabilities") {
        for capability in json_array(capabilities, "capabilities")? {
            manifest.add_capability(parse_extension_capability(capability)?);
        }
    }
    if let Some(permissions) = value.get("permissions") {
        for permission in json_array(permissions, "permissions")? {
            manifest.add_permission(parse_extension_permission(permission)?);
        }
    }
    if let Some(effects) = value.get("effects") {
        for effect in json_array(effects, "effects")? {
            manifest.add_effect(parse_extension_effect(effect)?);
        }
    }
    if let Some(contract) = value.get("execution_contract") {
        manifest = manifest.with_execution_contract(parse_extension_execution_contract(contract)?);
    }
    Ok(manifest)
}

fn parse_extension_capability(value: &Value) -> Result<ExtensionCapability, ShardLoomError> {
    let name = json_string(value, "name")?;
    let status = parse_extension_capability_status(json_string(value, "status")?)?;
    let capability = ExtensionCapability::new(name.to_string(), status)?;
    Ok(if let Some(notes) = optional_json_string(value, "notes") {
        capability.with_notes(notes)
    } else {
        capability
    })
}

fn parse_extension_permission(value: &Value) -> Result<ExtensionPermission, ShardLoomError> {
    let permission = parse_permission_kind(json_string(value, "permission")?)?;
    let reason = json_string(value, "reason")?.to_string();
    let required = optional_json_bool(value, "required").unwrap_or(true);
    Ok(if required {
        ExtensionPermission::required(permission, reason)
    } else {
        ExtensionPermission::optional(permission, reason)
    })
}

fn parse_extension_effect(value: &Value) -> Result<ExtensionEffectDeclaration, ShardLoomError> {
    let effect = parse_external_effect_kind(json_string(value, "effect")?)?;
    let level = parse_effect_level(json_string(value, "level")?)?;
    let mut declaration = ExtensionEffectDeclaration::new(effect, level);
    if let Some(requires_approval) = optional_json_bool(value, "requires_approval") {
        declaration = declaration.requires_approval(requires_approval);
    }
    if let Some(dry_run_safe) = optional_json_bool(value, "dry_run_safe") {
        declaration = declaration.dry_run_safe(dry_run_safe);
    }
    if let Some(idempotency_required) = optional_json_bool(value, "idempotency_required") {
        declaration = declaration.idempotency_required(idempotency_required);
    }
    Ok(declaration)
}

fn parse_extension_execution_contract(
    value: &Value,
) -> Result<ExtensionExecutionContract, ShardLoomError> {
    let mut contract = ExtensionExecutionContract::undeclared();
    if let Some(determinism) = optional_json_string(value, "determinism") {
        contract = contract.with_determinism(parse_extension_determinism_contract(&determinism)?);
    }
    if let Some(materialization) = optional_json_string(value, "materialization") {
        contract = contract
            .with_materialization(parse_extension_materialization_contract(&materialization)?);
    }
    if let Some(null_behavior) = optional_json_string(value, "null_behavior") {
        contract =
            contract.with_null_behavior(parse_extension_null_behavior_contract(&null_behavior)?);
    }
    if let Some(input_dtypes) = value.get("input_dtypes") {
        let dtypes = json_string_vec(input_dtypes, "input_dtypes")?;
        let output_dtype = optional_json_string(value, "output_dtype").unwrap_or_default();
        contract = contract.with_dtypes(dtypes, output_dtype)?;
    } else if let Some(output_dtype) = optional_json_string(value, "output_dtype") {
        contract = contract.with_dtypes(Vec::new(), output_dtype)?;
    }
    if let Some(timeout_millis) = optional_json_u64(value, "timeout_millis") {
        contract = contract.with_timeout_millis(timeout_millis);
    }
    if let Some(max_memory_bytes) = optional_json_u64(value, "max_memory_bytes") {
        contract = contract.with_max_memory_bytes(max_memory_bytes);
    }
    if let Some(max_cpu_millis) = optional_json_u64(value, "max_cpu_millis") {
        contract = contract.with_max_cpu_millis(max_cpu_millis);
    }
    if let Some(retry) = optional_json_string(value, "retry") {
        contract = contract.with_retry(parse_extension_retry_contract(&retry)?);
    }
    if let Some(idempotency) = optional_json_string(value, "idempotency") {
        contract = contract.with_idempotency(parse_extension_idempotency_contract(&idempotency)?);
    }
    if let Some(audit) = optional_json_string(value, "audit") {
        contract = contract.with_audit(parse_extension_audit_contract(&audit)?);
    }
    Ok(contract)
}

fn parse_plugin_abi_requirement(value: &Value) -> Result<PluginAbiRequirement, ShardLoomError> {
    let api_name = json_string(value, "api_name")?;
    let required_version = parse_extension_version(json_string(value, "required_version")?)?;
    let status = optional_json_string(value, "status")
        .map_or(Ok(PluginAbiStatus::NotChecked), |value| {
            parse_plugin_abi_status(&value)
        })?;
    Ok(PluginAbiRequirement::new(api_name.to_string(), required_version)?.with_status(status))
}

fn parse_sandbox_policy(value: &Value) -> Result<SandboxPolicy, ShardLoomError> {
    let kind = optional_json_string(value, "kind")
        .map_or(Ok(SandboxPolicyKind::MetadataOnly), |value| {
            parse_sandbox_policy_kind(&value)
        })?;
    let mut sandbox = if kind == SandboxPolicyKind::FullSandboxRequired {
        SandboxPolicy::full_sandbox_required()
    } else {
        let mut sandbox = SandboxPolicy::metadata_only();
        sandbox.kind = kind;
        sandbox
    };
    if let Some(allow_filesystem) = optional_json_bool(value, "allow_filesystem") {
        sandbox = sandbox.allow_filesystem(allow_filesystem);
    }
    if let Some(allow_network) = optional_json_bool(value, "allow_network") {
        sandbox = sandbox.allow_network(allow_network);
    }
    if let Some(allow_environment) = optional_json_bool(value, "allow_environment") {
        sandbox = sandbox.allow_environment(allow_environment);
    }
    if let Some(allow_secret_access) = optional_json_bool(value, "allow_secret_access") {
        sandbox = sandbox.allow_secret_access(allow_secret_access);
    }
    if let Some(max_memory_bytes) = optional_json_u64(value, "max_memory_bytes") {
        sandbox = sandbox.with_max_memory_bytes(max_memory_bytes);
    }
    if let Some(max_runtime_millis) = optional_json_u64(value, "max_runtime_millis") {
        sandbox = sandbox.with_max_runtime_millis(max_runtime_millis);
    }
    Ok(sandbox)
}

fn parse_extension_version(raw: &str) -> Result<ExtensionVersion, ShardLoomError> {
    let (version_part, pre_release) = raw
        .split_once('-')
        .map_or((raw, None), |(version, pre)| (version, Some(pre)));
    let parts = version_part.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "extension version must use major.minor.patch, got {raw:?}"
        )));
    }
    let major = parse_u32(parts[0], "version major")?;
    let minor = parse_u32(parts[1], "version minor")?;
    let patch = parse_u32(parts[2], "version patch")?;
    let version = ExtensionVersion::new(major, minor, patch);
    pre_release.map_or(Ok(version.clone()), |pre| {
        version.with_pre_release(pre.to_string())
    })
}

fn parse_u32(raw: &str, label: &str) -> Result<u32, ShardLoomError> {
    raw.parse::<u32>().map_err(|error| {
        ShardLoomError::InvalidOperation(format!("invalid {label} {raw:?}: {error}"))
    })
}

fn parse_extension_category(raw: &str) -> Result<ExtensionCategory, ShardLoomError> {
    match raw {
        "frontend" => Ok(ExtensionCategory::Frontend),
        "function" => Ok(ExtensionCategory::Function),
        "scalar_udf" => Ok(ExtensionCategory::ScalarUdf),
        "aggregate_udf" => Ok(ExtensionCategory::AggregateUdf),
        "table_function" => Ok(ExtensionCategory::TableFunction),
        "encoded_kernel" => Ok(ExtensionCategory::EncodedKernel),
        "translation_sink" => Ok(ExtensionCategory::TranslationSink),
        "connector" => Ok(ExtensionCategory::Connector),
        "catalog_provider" => Ok(ExtensionCategory::CatalogProvider),
        "object_store_provider" => Ok(ExtensionCategory::ObjectStoreProvider),
        "effect_provider" => Ok(ExtensionCategory::EffectProvider),
        "llm_provider" => Ok(ExtensionCategory::LlmProvider),
        "embedding_provider" => Ok(ExtensionCategory::EmbeddingProvider),
        "vector_index_provider" => Ok(ExtensionCategory::VectorIndexProvider),
        "observability_exporter" => Ok(ExtensionCategory::ObservabilityExporter),
        "benchmark_provider" => Ok(ExtensionCategory::BenchmarkProvider),
        "unknown" => Ok(ExtensionCategory::Unknown),
        other => Err(unsupported_manifest_value("category", other)),
    }
}

fn parse_extension_lifecycle(raw: &str) -> Result<ExtensionLifecycleState, ShardLoomError> {
    match raw {
        "discovered" => Ok(ExtensionLifecycleState::Discovered),
        "loaded" => Ok(ExtensionLifecycleState::Loaded),
        "validated" => Ok(ExtensionLifecycleState::Validated),
        "enabled" => Ok(ExtensionLifecycleState::Enabled),
        "disabled" => Ok(ExtensionLifecycleState::Disabled),
        "failed" => Ok(ExtensionLifecycleState::Failed),
        "quarantined" => Ok(ExtensionLifecycleState::Quarantined),
        "deprecated" => Ok(ExtensionLifecycleState::Deprecated),
        "removed" => Ok(ExtensionLifecycleState::Removed),
        "unsupported" => Ok(ExtensionLifecycleState::Unsupported),
        other => Err(unsupported_manifest_value("lifecycle", other)),
    }
}

fn parse_extension_capability_status(
    raw: &str,
) -> Result<ExtensionCapabilityStatus, ShardLoomError> {
    match raw {
        "supported" => Ok(ExtensionCapabilityStatus::Supported),
        "partially_supported" => Ok(ExtensionCapabilityStatus::PartiallySupported),
        "planned" => Ok(ExtensionCapabilityStatus::Planned),
        "disabled" => Ok(ExtensionCapabilityStatus::Disabled),
        "requires_configuration" => Ok(ExtensionCapabilityStatus::RequiresConfiguration),
        "requires_explicit_enablement" => Ok(ExtensionCapabilityStatus::RequiresExplicitEnablement),
        "unsupported" => Ok(ExtensionCapabilityStatus::Unsupported),
        other => Err(unsupported_manifest_value("capability status", other)),
    }
}

fn parse_extension_license(raw: &str) -> Result<ExtensionLicenseKind, ShardLoomError> {
    match raw {
        "Apache-2.0" | "apache-2.0" => Ok(ExtensionLicenseKind::Apache2),
        "MIT" | "mit" => Ok(ExtensionLicenseKind::Mit),
        "BSD" | "bsd" => Ok(ExtensionLicenseKind::Bsd),
        "ISC" | "isc" => Ok(ExtensionLicenseKind::Isc),
        "Zlib" | "zlib" => Ok(ExtensionLicenseKind::Zlib),
        "MPL-2.0" | "mpl-2.0" => Ok(ExtensionLicenseKind::Mpl2),
        "Unknown" | "unknown" => Ok(ExtensionLicenseKind::Unknown),
        "Incompatible" | "incompatible" => Ok(ExtensionLicenseKind::Incompatible),
        other => Err(unsupported_manifest_value("license", other)),
    }
}

fn parse_permission_kind(raw: &str) -> Result<PermissionKind, ShardLoomError> {
    match raw {
        "read_metadata" => Ok(PermissionKind::ReadMetadata),
        "read_data" => Ok(PermissionKind::ReadData),
        "write_temporary_output" => Ok(PermissionKind::WriteTemporaryOutput),
        "commit_output" => Ok(PermissionKind::CommitOutput),
        "delete_temporary_files" => Ok(PermissionKind::DeleteTemporaryFiles),
        "access_network" => Ok(PermissionKind::AccessNetwork),
        "access_filesystem" => Ok(PermissionKind::AccessFilesystem),
        "access_secret" => Ok(PermissionKind::AccessSecret),
        "call_llm" => Ok(PermissionKind::CallLlm),
        "call_api" => Ok(PermissionKind::CallApi),
        "generate_embeddings" => Ok(PermissionKind::GenerateEmbeddings),
        "external_write" => Ok(PermissionKind::ExternalWrite),
        "execute_udf" => Ok(PermissionKind::ExecuteUdf),
        "execute_plugin" => Ok(PermissionKind::ExecutePlugin),
        "export_compatibility_output" => Ok(PermissionKind::ExportCompatibilityOutput),
        "unsupported" => Ok(PermissionKind::Unsupported),
        other => Err(unsupported_manifest_value("permission", other)),
    }
}

fn parse_external_effect_kind(raw: &str) -> Result<ExternalEffectKind, ShardLoomError> {
    match raw {
        "none" => Ok(ExternalEffectKind::None),
        "object_store_write" => Ok(ExternalEffectKind::ObjectStoreWrite),
        "local_file_write" => Ok(ExternalEffectKind::LocalFileWrite),
        "api_read" => Ok(ExternalEffectKind::ApiRead),
        "api_write" => Ok(ExternalEffectKind::ApiWrite),
        "llm_call" => Ok(ExternalEffectKind::LlmCall),
        "embedding_generation" => Ok(ExternalEffectKind::EmbeddingGeneration),
        "vector_search" => Ok(ExternalEffectKind::VectorSearch),
        "catalog_read" => Ok(ExternalEffectKind::CatalogRead),
        "catalog_write" => Ok(ExternalEffectKind::CatalogWrite),
        "external_workflow_trigger" => Ok(ExternalEffectKind::ExternalWorkflowTrigger),
        "udf_execution" => Ok(ExternalEffectKind::UdfExecution),
        "plugin_execution" => Ok(ExternalEffectKind::PluginExecution),
        "unknown" => Ok(ExternalEffectKind::Unknown),
        other => Err(unsupported_manifest_value("effect", other)),
    }
}

fn parse_effect_level(raw: &str) -> Result<EffectLevel, ShardLoomError> {
    match raw {
        "pure_deterministic" => Ok(EffectLevel::PureDeterministic),
        "pure_nondeterministic" => Ok(EffectLevel::PureNondeterministic),
        "external_read" => Ok(EffectLevel::ExternalRead),
        "external_write" => Ok(EffectLevel::ExternalWrite),
        "model_call" => Ok(EffectLevel::ModelCall),
        "embedding_call" => Ok(EffectLevel::EmbeddingCall),
        "vector_search" => Ok(EffectLevel::VectorSearch),
        "unknown" => Ok(EffectLevel::Unknown),
        other => Err(unsupported_manifest_value("effect level", other)),
    }
}

fn parse_extension_determinism_contract(
    raw: &str,
) -> Result<ExtensionDeterminismContract, ShardLoomError> {
    match raw {
        "pure_deterministic" => Ok(ExtensionDeterminismContract::PureDeterministic),
        "pure_nondeterministic" => Ok(ExtensionDeterminismContract::PureNondeterministic),
        "external_effect_bound" => Ok(ExtensionDeterminismContract::ExternalEffectBound),
        "unknown" => Ok(ExtensionDeterminismContract::Unknown),
        "unsupported" => Ok(ExtensionDeterminismContract::Unsupported),
        other => Err(unsupported_manifest_value("determinism", other)),
    }
}

fn parse_extension_materialization_contract(
    raw: &str,
) -> Result<ExtensionMaterializationContract, ShardLoomError> {
    match raw {
        "metadata_only" => Ok(ExtensionMaterializationContract::MetadataOnly),
        "encoded_native" => Ok(ExtensionMaterializationContract::EncodedNative),
        "late_materialized" => Ok(ExtensionMaterializationContract::LateMaterialized),
        "materialization_required" => Ok(ExtensionMaterializationContract::MaterializationRequired),
        "unsupported" => Ok(ExtensionMaterializationContract::Unsupported),
        other => Err(unsupported_manifest_value("materialization", other)),
    }
}

fn parse_extension_null_behavior_contract(
    raw: &str,
) -> Result<ExtensionNullBehaviorContract, ShardLoomError> {
    match raw {
        "null_propagating" => Ok(ExtensionNullBehaviorContract::NullPropagating),
        "null_skipping" => Ok(ExtensionNullBehaviorContract::NullSkipping),
        "null_aware" => Ok(ExtensionNullBehaviorContract::NullAware),
        "null_error" => Ok(ExtensionNullBehaviorContract::NullError),
        "unknown" => Ok(ExtensionNullBehaviorContract::Unknown),
        "unsupported" => Ok(ExtensionNullBehaviorContract::Unsupported),
        other => Err(unsupported_manifest_value("null_behavior", other)),
    }
}

fn parse_extension_retry_contract(raw: &str) -> Result<ExtensionRetryContract, ShardLoomError> {
    match raw {
        "none" => Ok(ExtensionRetryContract::None),
        "idempotent_retry" => Ok(ExtensionRetryContract::IdempotentRetry),
        "at_most_once" => Ok(ExtensionRetryContract::AtMostOnce),
        "manual_replay_required" => Ok(ExtensionRetryContract::ManualReplayRequired),
        "unsupported" => Ok(ExtensionRetryContract::Unsupported),
        other => Err(unsupported_manifest_value("retry", other)),
    }
}

fn parse_extension_idempotency_contract(
    raw: &str,
) -> Result<ExtensionIdempotencyContract, ShardLoomError> {
    match raw {
        "not_required" => Ok(ExtensionIdempotencyContract::NotRequired),
        "required" => Ok(ExtensionIdempotencyContract::Required),
        "key_required" => Ok(ExtensionIdempotencyContract::KeyRequired),
        "unsupported" => Ok(ExtensionIdempotencyContract::Unsupported),
        other => Err(unsupported_manifest_value("idempotency", other)),
    }
}

fn parse_extension_audit_contract(raw: &str) -> Result<ExtensionAuditContract, ShardLoomError> {
    match raw {
        "manifest_only" => Ok(ExtensionAuditContract::ManifestOnly),
        "execution_certificate_required" => {
            Ok(ExtensionAuditContract::ExecutionCertificateRequired)
        }
        "full_audit_required" => Ok(ExtensionAuditContract::FullAuditRequired),
        "unsupported" => Ok(ExtensionAuditContract::Unsupported),
        other => Err(unsupported_manifest_value("audit", other)),
    }
}

fn parse_sandbox_policy_kind(raw: &str) -> Result<SandboxPolicyKind, ShardLoomError> {
    match raw {
        "none" => Ok(SandboxPolicyKind::None),
        "metadata_only" => Ok(SandboxPolicyKind::MetadataOnly),
        "no_network" => Ok(SandboxPolicyKind::NoNetwork),
        "no_filesystem" => Ok(SandboxPolicyKind::NoFilesystem),
        "bounded_resources" => Ok(SandboxPolicyKind::BoundedResources),
        "full_sandbox_required" => Ok(SandboxPolicyKind::FullSandboxRequired),
        "unsupported" => Ok(SandboxPolicyKind::Unsupported),
        other => Err(unsupported_manifest_value("sandbox policy kind", other)),
    }
}

fn parse_plugin_abi_status(raw: &str) -> Result<PluginAbiStatus, ShardLoomError> {
    match raw {
        "internal_only" => Ok(PluginAbiStatus::InternalOnly),
        "experimental" => Ok(PluginAbiStatus::Experimental),
        "compatible" => Ok(PluginAbiStatus::Compatible),
        "incompatible" => Ok(PluginAbiStatus::Incompatible),
        "not_checked" => Ok(PluginAbiStatus::NotChecked),
        "unsupported" => Ok(PluginAbiStatus::Unsupported),
        other => Err(unsupported_manifest_value("plugin ABI status", other)),
    }
}

fn parse_udf_runtime_kind(raw: &str) -> Result<UdfRuntimeKind, ShardLoomError> {
    match raw {
        "builtin_deterministic_fixture" => Ok(UdfRuntimeKind::BuiltinDeterministicFixture),
        "rust_native" => Ok(UdfRuntimeKind::RustNative),
        "wasm" => Ok(UdfRuntimeKind::Wasm),
        "python" => Ok(UdfRuntimeKind::Python),
        "sql_defined" => Ok(UdfRuntimeKind::SqlDefined),
        "external_service" => Ok(UdfRuntimeKind::ExternalService),
        "unknown" => Ok(UdfRuntimeKind::Unknown),
        other => Err(unsupported_manifest_value("UDF runtime", other)),
    }
}

fn unsupported_manifest_value(field: &str, value: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "unsupported extension manifest {field} value {value:?}; no extension code was loaded"
    ))
}

fn json_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ShardLoomError> {
    value.get(key).and_then(Value::as_str).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "extension manifest field {key:?} must be a string"
        ))
    })
}

fn optional_json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn optional_json_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn optional_json_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn json_array<'a>(value: &'a Value, label: &str) -> Result<&'a Vec<Value>, ShardLoomError> {
    value.as_array().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "extension manifest field {label:?} must be an array"
        ))
    })
}

fn json_string_vec(value: &Value, label: &str) -> Result<Vec<String>, ShardLoomError> {
    let values = json_array(value, label)?;
    values
        .iter()
        .map(|value| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "extension manifest field {label:?} entries must be strings"
                ))
            })
        })
        .collect()
}

pub(crate) fn handle_udf_registry(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "udf-registry",
            format,
            "typed UDF registry failed",
            &ShardLoomError::InvalidOperation(format!("unknown udf-registry argument: {extra}")),
        );
    }
    let report = typed_udf_registry_report();
    emit(
        "udf-registry",
        format,
        CommandStatus::Success,
        "typed UDF registry".to_string(),
        report.to_human_text(),
        vec![],
        udf_registry_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_udf_runtime_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let runtime = match args.next().as_deref() {
        Some("fixture" | "builtin-fixture" | "builtin-deterministic") => {
            UdfRuntimeKind::BuiltinDeterministicFixture
        }
        Some("rust") => UdfRuntimeKind::RustNative,
        Some("wasm") => UdfRuntimeKind::Wasm,
        Some("python") => UdfRuntimeKind::Python,
        Some("sql") => UdfRuntimeKind::SqlDefined,
        Some("external") => UdfRuntimeKind::ExternalService,
        Some(_) | None => UdfRuntimeKind::Unknown,
    };
    let text = format!(
        "udf runtime={} available_initially={} sandboxing_required={} execution=not_performed fallback_execution=disabled",
        runtime.as_str(),
        runtime.is_available_initially(),
        runtime.requires_sandboxing()
    );
    emit(
        "udf-runtime-plan",
        format,
        CommandStatus::Success,
        "udf runtime availability skeleton".to_string(),
        text,
        vec![],
        udf_runtime_plan_fields(runtime),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_udf_local_scalar_fixture_smoke(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(values_raw) = args.next() else {
        return emit_error(
            "udf-local-scalar-fixture-smoke",
            format,
            "udf local scalar fixture failed",
            &ShardLoomError::InvalidOperation(
                "missing comma-separated int64/null values".to_string(),
            ),
        );
    };
    let values = match parse_nullable_i64_values(&values_raw) {
        Ok(values) => values,
        Err(error) => {
            return emit_error(
                "udf-local-scalar-fixture-smoke",
                format,
                "udf local scalar fixture failed",
                &error,
            );
        }
    };
    let report = match run_deterministic_scalar_udf_fixture(&values) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "udf-local-scalar-fixture-smoke",
                format,
                "udf local scalar fixture failed",
                &error,
            );
        }
    };
    emit(
        "udf-local-scalar-fixture-smoke",
        format,
        CommandStatus::Success,
        "deterministic scalar UDF fixture smoke".to_string(),
        report.to_human_text(),
        vec![],
        udf_local_scalar_fixture_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_embedding_vector_local_fixture_smoke(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(texts_raw) = args.next() else {
        return emit_error(
            "embedding-vector-local-fixture-smoke",
            format,
            "embedding/vector local fixture failed",
            &ShardLoomError::InvalidOperation(
                "missing semicolon-separated text values".to_string(),
            ),
        );
    };
    let mut query_text = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--query" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        "embedding-vector-local-fixture-smoke",
                        format,
                        "embedding/vector local fixture failed",
                        &ShardLoomError::InvalidOperation("missing --query value".to_string()),
                    );
                };
                query_text = Some(value);
            }
            other => {
                return emit_error(
                    "embedding-vector-local-fixture-smoke",
                    format,
                    "embedding/vector local fixture failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown embedding/vector fixture argument: {other}"
                    )),
                );
            }
        }
    }
    let texts = match parse_semicolon_text_values(&texts_raw) {
        Ok(texts) => texts,
        Err(error) => {
            return emit_error(
                "embedding-vector-local-fixture-smoke",
                format,
                "embedding/vector local fixture failed",
                &error,
            );
        }
    };
    let query = query_text.unwrap_or_else(|| texts[0].clone());
    let report = match run_deterministic_embedding_vector_fixture(&texts, &query) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "embedding-vector-local-fixture-smoke",
                format,
                "embedding/vector local fixture failed",
                &error,
            );
        }
    };
    emit(
        "embedding-vector-local-fixture-smoke",
        format,
        CommandStatus::Success,
        "deterministic embedding/vector fixture smoke".to_string(),
        report.to_human_text(),
        vec![],
        embedding_vector_local_fixture_fields(&report),
    );
    ExitCode::SUCCESS
}

fn parse_nullable_i64_values(values_raw: &str) -> Result<Vec<Option<i64>>, ShardLoomError> {
    if values_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "comma-separated values must not be empty".to_string(),
        ));
    }
    let mut values = Vec::new();
    for token in values_raw.split(',') {
        let token = token.trim();
        if token.eq_ignore_ascii_case("null") {
            values.push(None);
        } else if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "comma-separated values must not contain empty entries".to_string(),
            ));
        } else {
            let value = token.parse::<i64>().map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "invalid int64 UDF fixture value {token:?}: {error}"
                ))
            })?;
            values.push(Some(value));
        }
    }
    Ok(values)
}

fn parse_semicolon_text_values(values_raw: &str) -> Result<Vec<String>, ShardLoomError> {
    if values_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "semicolon-separated text values must not be empty".to_string(),
        ));
    }
    let mut values = Vec::new();
    for token in values_raw.split(';') {
        let text = token.trim();
        if text.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "semicolon-separated text values must not contain empty entries".to_string(),
            ));
        }
        values.push(text.to_string());
    }
    Ok(values)
}

fn udf_runtime_plan_fields(runtime: UdfRuntimeKind) -> Vec<(String, String)> {
    let mut fields = extension_report_only_fields("udf_runtime_plan");
    fields.extend([
        ("udf_runtime_kind".to_string(), runtime.as_str().to_string()),
        (
            "udf_runtime_available_initially".to_string(),
            runtime.is_available_initially().to_string(),
        ),
        (
            "udf_runtime_sandboxing_required".to_string(),
            runtime.requires_sandboxing().to_string(),
        ),
        (
            "udf_runtime_fixture_command".to_string(),
            if runtime.is_available_initially() {
                "udf-local-scalar-fixture-smoke"
            } else {
                "none"
            }
            .to_string(),
        ),
    ]);
    append_typed_udf_registry_fields(&mut fields);
    fields
}

fn udf_local_scalar_fixture_fields(
    report: &DeterministicScalarUdfFixtureReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "udf_local_scalar_fixture_smoke".to_string(),
        ),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("udf_id".to_string(), report.udf_id.to_string()),
        ("udf_version".to_string(), report.udf_version.to_string()),
        (
            "udf_runtime_kind".to_string(),
            report.runtime_kind.as_str().to_string(),
        ),
        ("udf_type".to_string(), "scalar".to_string()),
        ("input_dtype".to_string(), report.input_dtype.to_string()),
        ("output_dtype".to_string(), report.output_dtype.to_string()),
        ("determinism".to_string(), report.determinism.to_string()),
        ("null_policy".to_string(), report.null_policy.to_string()),
        (
            "input_row_count".to_string(),
            report.input_row_count.to_string(),
        ),
        (
            "output_row_count".to_string(),
            report.output_row_count.to_string(),
        ),
        ("input_digest".to_string(), report.input_digest.clone()),
        ("output_digest".to_string(), report.output_digest.clone()),
        ("output_values".to_string(), report.output_values_summary()),
        (
            "overflow_policy_enforced".to_string(),
            report.overflow_policy_enforced.to_string(),
        ),
        (
            "overflow_blocked".to_string(),
            report.overflow_blocked.to_string(),
        ),
        (
            "sandbox_required".to_string(),
            report.sandbox_required.to_string(),
        ),
        (
            "network_allowed".to_string(),
            report.network_allowed.to_string(),
        ),
        (
            "credential_resolution_performed".to_string(),
            report.credential_resolution_performed.to_string(),
        ),
        (
            "dynamic_loading_performed".to_string(),
            report.dynamic_loading_performed.to_string(),
        ),
        (
            "extension_code_executed".to_string(),
            report.extension_code_executed.to_string(),
        ),
        (
            "external_effect_executed".to_string(),
            report.external_effect_executed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "no_fallback_invariant_holds".to_string(),
            report.no_fallback_invariant_holds().to_string(),
        ),
        (
            "claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
    ];
    append_effectful_operation_admission_matrix_fields(&mut fields);
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    append_typed_udf_registry_fields(&mut fields);
    fields
}

fn udf_registry_fields(report: &TypedUdfRegistryReport) -> Vec<(String, String)> {
    let mut fields = extension_report_only_fields("typed_udf_registry");
    append_typed_udf_registry_fields_from_report(&mut fields, report);
    fields
}

fn append_typed_udf_registry_fields(fields: &mut Vec<(String, String)>) {
    let report = typed_udf_registry_report();
    append_typed_udf_registry_fields_from_report(fields, &report);
}

fn append_typed_udf_registry_fields_from_report(
    fields: &mut Vec<(String, String)>,
    report: &TypedUdfRegistryReport,
) {
    let row_order = report.row_order().join(",");
    let blocker_ids = report.blocker_ids().join(",");
    let required_evidence = report.required_evidence().join("|");
    for (key, value) in [
        ("typed_udf_registry_schema_version", report.schema_version),
        ("typed_udf_registry_id", report.registry_id),
        ("typed_udf_registry_docs_ref", report.docs_ref),
        ("typed_udf_registry_support_status", report.support_status),
        (
            "typed_udf_registry_claim_gate_status",
            report.claim_gate_status,
        ),
        ("typed_udf_registry_row_order", row_order.as_str()),
        ("typed_udf_registry_blocker_ids", blocker_ids.as_str()),
        (
            "typed_udf_registry_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(fields, "typed_udf_registry_row_count", report.entries.len());
    push_count_field(
        fields,
        "typed_udf_registry_admitted_local_fixture_count",
        report.admitted_local_fixture_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_blocked_count",
        report.blocked_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_scalar_count",
        report.scalar_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_aggregate_count",
        report.aggregate_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_table_function_count",
        report.table_function_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_encoded_native_candidate_count",
        report.encoded_native_candidate_count(),
    );
    push_count_field(
        fields,
        "typed_udf_registry_materialization_required_count",
        report.materialization_required_count(),
    );
    append_typed_udf_registry_bool_fields(fields, report);
    for entry in &report.entries {
        append_typed_udf_registry_entry_fields(fields, entry);
    }
}

fn append_typed_udf_registry_bool_fields(
    fields: &mut Vec<(String, String)>,
    report: &TypedUdfRegistryReport,
) {
    for (key, value) in [
        (
            "typed_udf_registry_side_effect_free",
            report.side_effect_free(),
        ),
        (
            "typed_udf_registry_local_fixture_execution_bridge_available",
            report.local_fixture_execution_bridge_available,
        ),
        (
            "typed_udf_registry_arbitrary_runtime_bridge_available",
            report.arbitrary_runtime_bridge_available,
        ),
        (
            "typed_udf_registry_sandbox_policy_declared",
            report.sandbox_policy_declared,
        ),
        (
            "typed_udf_registry_filesystem_access_allowed",
            report.filesystem_access_allowed,
        ),
        (
            "typed_udf_registry_network_access_allowed",
            report.network_access_allowed,
        ),
        (
            "typed_udf_registry_secret_access_allowed",
            report.secret_access_allowed,
        ),
        (
            "typed_udf_registry_dynamic_loading_allowed",
            report.dynamic_loading_allowed,
        ),
        (
            "typed_udf_registry_runtime_execution_performed",
            report.runtime_execution_performed,
        ),
        (
            "typed_udf_registry_extension_code_executed",
            report.extension_code_executed,
        ),
        (
            "typed_udf_registry_external_effect_executed",
            report.external_effect_executed,
        ),
        (
            "typed_udf_registry_credential_resolution_performed",
            report.credential_resolution_performed,
        ),
        (
            "typed_udf_registry_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "typed_udf_registry_external_engine_invoked",
            report.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_typed_udf_registry_entry_fields(
    fields: &mut Vec<(String, String)>,
    entry: &TypedUdfRegistryEntry,
) {
    let prefix = format!("typed_udf_registry_row_{}", entry.udf_id);
    let input_dtypes = entry.input_dtype_summary();
    for (suffix, value) in [
        ("display_name", entry.display_name),
        ("udf_version", entry.udf_version),
        ("kind", entry.kind.as_str()),
        ("runtime_kind", entry.runtime_kind.as_str()),
        ("support_status", entry.support_status.as_str()),
        ("encoded_capability", entry.encoded_capability.as_str()),
        ("determinism", entry.determinism.as_str()),
        ("null_behavior", entry.null_behavior.as_str()),
        ("materialization", entry.materialization.as_str()),
        ("input_dtypes", input_dtypes.as_str()),
        ("output_dtype", entry.output_dtype),
        ("sandbox_policy", entry.sandbox_policy.as_str()),
        ("permission_contract", entry.permission_contract),
        ("effect_level", entry.effect_level.as_str()),
        (
            "runtime_fixture_command",
            entry.runtime_fixture_command.unwrap_or("none"),
        ),
        ("blocker_id", entry.blocker_id),
        ("diagnostic_code", entry.diagnostic_code),
        ("required_evidence", entry.required_evidence),
        ("claim_boundary", entry.claim_boundary),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    for (suffix, value) in [
        (
            "registry_execution_allowed",
            entry.registry_execution_allowed,
        ),
        ("runtime_fixture_available", entry.runtime_fixture_available),
        ("materialization_required", entry.materialization_required()),
        ("sandbox_required", entry.sandbox_required),
        ("filesystem_access_allowed", entry.filesystem_access_allowed),
        ("network_access_allowed", entry.network_access_allowed),
        ("secret_access_allowed", entry.secret_access_allowed),
        (
            "credential_resolution_required",
            entry.credential_resolution_required,
        ),
        ("dynamic_loading_allowed", entry.dynamic_loading_allowed),
        (
            "runtime_execution_performed",
            entry.runtime_execution_performed,
        ),
        ("extension_code_executed", entry.extension_code_executed),
        ("external_effect_executed", entry.external_effect_executed),
        ("fallback_attempted", entry.fallback_attempted),
        ("external_engine_invoked", entry.external_engine_invoked),
        (
            "no_fallback_invariant_holds",
            entry.no_fallback_invariant_holds(),
        ),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

#[allow(clippy::too_many_lines)]
fn embedding_vector_local_fixture_fields(
    report: &DeterministicEmbeddingVectorFixtureReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "embedding_vector_local_fixture_smoke".to_string(),
        ),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("fixture_id".to_string(), report.fixture_id.to_string()),
        (
            "fixture_version".to_string(),
            report.fixture_version.to_string(),
        ),
        ("input_dtype".to_string(), report.input_dtype.to_string()),
        ("output_dtype".to_string(), report.output_dtype.to_string()),
        ("determinism".to_string(), report.determinism.to_string()),
        (
            "embedding_model_id".to_string(),
            report.embedding_model_id.to_string(),
        ),
        (
            "vector_index_kind".to_string(),
            report.vector_index_kind.to_string(),
        ),
        ("vector_metric".to_string(), report.metric.to_string()),
        ("vector_dimension".to_string(), report.dimension.to_string()),
        (
            "input_row_count".to_string(),
            report.input_row_count.to_string(),
        ),
        (
            "vector_row_count".to_string(),
            report.vector_row_count.to_string(),
        ),
        ("query_text".to_string(), report.query_text.clone()),
        ("query_vector".to_string(), report.query_vector_summary()),
        (
            "nearest_index".to_string(),
            report.nearest_index.to_string(),
        ),
        ("nearest_text".to_string(), report.nearest_text.clone()),
        (
            "nearest_distance_squared".to_string(),
            report.nearest_distance_squared.to_string(),
        ),
        ("nearest_summary".to_string(), report.nearest_summary()),
        ("input_digest".to_string(), report.input_digest.clone()),
        ("vector_digest".to_string(), report.vector_digest.clone()),
        (
            "model_call_performed".to_string(),
            report.model_call_performed.to_string(),
        ),
        (
            "credential_resolution_performed".to_string(),
            report.credential_resolution_performed.to_string(),
        ),
        (
            "network_probe_performed".to_string(),
            report.network_probe_performed.to_string(),
        ),
        (
            "dynamic_loading_performed".to_string(),
            report.dynamic_loading_performed.to_string(),
        ),
        (
            "extension_code_executed".to_string(),
            report.extension_code_executed.to_string(),
        ),
        (
            "external_effect_executed".to_string(),
            report.external_effect_executed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "external_engine_invoked".to_string(),
            report.external_engine_invoked.to_string(),
        ),
        (
            "no_fallback_invariant_holds".to_string(),
            report.no_fallback_invariant_holds().to_string(),
        ),
        (
            "claim_gate_status".to_string(),
            report.claim_gate_status.to_string(),
        ),
        (
            "claim_boundary".to_string(),
            report.claim_boundary.to_string(),
        ),
    ];
    append_effectful_operation_admission_matrix_fields(&mut fields);
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    fields
}

fn extension_registry_fields(
    snapshot: &ExtensionRegistrySnapshot,
    input: &ExtensionRegistryInput,
) -> Vec<(String, String)> {
    let mut fields = extension_report_only_fields("extension_registry");
    push_field(
        &mut fields,
        "extension_registry_snapshot_schema_version",
        EXTENSION_REGISTRY_SNAPSHOT_SCHEMA_VERSION,
    );
    append_extension_registry_input_fields(&mut fields, input);
    append_extension_registry_summary_fields(&mut fields, snapshot);
    append_extension_registry_no_runtime_fields(&mut fields);
    fields
}

fn append_extension_registry_input_fields(
    fields: &mut Vec<(String, String)>,
    input: &ExtensionRegistryInput,
) {
    match input {
        ExtensionRegistryInput::EmptyRegistry => {
            push_field(fields, "extension_registry_input_kind", "empty_registry");
            push_field(
                fields,
                "extension_registry_manifest_dir_path",
                "not_applicable_empty_registry",
            );
            push_bool_field(fields, "extension_registry_directory_read_performed", false);
            push_count_field(fields, "extension_registry_directory_entry_count", 0);
            push_count_field(fields, "extension_registry_manifest_file_count", 0);
            push_count_field(
                fields,
                "extension_registry_manifest_file_read_request_count",
                0,
            );
            push_count_field(fields, "extension_registry_manifest_bytes_read", 0);
        }
        ExtensionRegistryInput::LocalManifestDirectory {
            path,
            entry_count,
            manifest_file_count,
            file_read_request_count,
            bytes_read,
        } => {
            push_field(
                fields,
                "extension_registry_input_kind",
                "approved_local_manifest_directory",
            );
            push_field(
                fields,
                "extension_registry_manifest_dir_path",
                &path.display().to_string(),
            );
            push_bool_field(fields, "extension_registry_directory_read_performed", true);
            push_count_field(
                fields,
                "extension_registry_directory_entry_count",
                *entry_count,
            );
            push_count_field(
                fields,
                "extension_registry_manifest_file_count",
                *manifest_file_count,
            );
            push_count_field(
                fields,
                "extension_registry_manifest_file_read_request_count",
                *file_read_request_count,
            );
            push_count_field(
                fields,
                "extension_registry_manifest_bytes_read",
                *bytes_read,
            );
        }
    }
}

fn append_extension_registry_summary_fields(
    fields: &mut Vec<(String, String)>,
    snapshot: &ExtensionRegistrySnapshot,
) {
    push_count_field(
        fields,
        "extension_registry_manifest_count",
        snapshot.extension_count(),
    );
    push_count_field(
        fields,
        "extension_registry_requires_review_count",
        snapshot.requires_review_count(),
    );
    push_count_field(
        fields,
        "extension_registry_validated_metadata_count",
        snapshot
            .manifests
            .iter()
            .filter(|manifest| !manifest.requires_review() && !manifest.has_errors())
            .count(),
    );
    push_count_field(
        fields,
        "extension_registry_usable_count",
        snapshot.usable_count(),
    );
    push_count_field(
        fields,
        "extension_registry_contract_complete_count",
        snapshot
            .manifests
            .iter()
            .filter(|manifest| manifest.execution_contract.production_contract_complete())
            .count(),
    );
    push_count_field(
        fields,
        "extension_registry_contract_incomplete_count",
        snapshot
            .manifests
            .iter()
            .filter(|manifest| !manifest.execution_contract.production_contract_complete())
            .count(),
    );
    push_field(
        fields,
        "extension_registry_manifest_ids",
        &extension_registry_manifest_ids(snapshot),
    );
    push_field(
        fields,
        "extension_registry_manifest_categories",
        &extension_registry_manifest_categories(snapshot),
    );
    push_field(
        fields,
        "extension_registry_contract_materialization_modes",
        &extension_registry_materialization_modes(snapshot),
    );
}

fn append_extension_registry_no_runtime_fields(fields: &mut Vec<(String, String)>) {
    for (key, value) in [
        ("extension_registry_runtime_execution", false),
        ("extension_registry_dynamic_loading_performed", false),
        ("extension_registry_extension_code_executed", false),
        ("extension_registry_external_effect_executed", false),
        ("extension_registry_network_probe_performed", false),
        ("extension_registry_credential_resolution_performed", false),
        ("extension_registry_dependency_expansion_allowed", false),
        ("extension_registry_fallback_attempted", false),
        ("extension_registry_external_engine_invoked", false),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn extension_inspection_fields(
    report: &ExtensionInspectionReport,
    input: ExtensionManifestInput,
) -> Vec<(String, String)> {
    let mut fields = extension_report_only_fields("extension_inspect");
    append_extension_manifest_inspection_input_fields(&mut fields, report, input);
    let manifest = &report.manifest;
    append_extension_manifest_identity_fields(&mut fields, manifest);
    append_extension_manifest_capability_fields(&mut fields, manifest);
    append_extension_manifest_permission_effect_fields(&mut fields, manifest, report.status);
    append_extension_manifest_sandbox_runtime_fields(&mut fields, manifest);
    append_extension_manifest_contract_fields(&mut fields, manifest);
    append_extension_manifest_no_runtime_fields(&mut fields, report);
    fields
}

fn append_extension_manifest_inspection_input_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExtensionInspectionReport,
    input: ExtensionManifestInput,
) {
    push_field(
        fields,
        "extension_manifest_inspection_schema_version",
        EXTENSION_MANIFEST_INSPECTION_SCHEMA_VERSION,
    );
    push_field(
        fields,
        "extension_manifest_inspection_status",
        report.status.as_str(),
    );
    push_bool_field(fields, "extension_manifest_inspection_only", true);
    push_bool_field(
        fields,
        "extension_manifest_file_read_performed",
        matches!(input, ExtensionManifestInput::LocalManifestFile { .. }),
    );
    match input {
        ExtensionManifestInput::SyntheticId => {
            push_field(fields, "extension_manifest_input_kind", "synthetic_id");
            push_field(
                fields,
                "extension_manifest_path",
                "not_applicable_synthetic_id",
            );
            push_count_field(fields, "extension_manifest_file_read_request_count", 0);
            push_count_field(fields, "extension_manifest_bytes_read", 0);
            push_field(
                fields,
                "extension_manifest_schema_version",
                "not_applicable_synthetic_id",
            );
            push_field(
                fields,
                "extension_manifest_json_parse_status",
                "not_applicable_synthetic_id",
            );
        }
        ExtensionManifestInput::LocalManifestFile {
            path,
            bytes_read,
            manifest_schema_version,
        } => {
            push_field(
                fields,
                "extension_manifest_input_kind",
                "local_manifest_file",
            );
            push_field(
                fields,
                "extension_manifest_path",
                &path.display().to_string(),
            );
            push_count_field(fields, "extension_manifest_file_read_request_count", 1);
            push_count_field(fields, "extension_manifest_bytes_read", bytes_read);
            push_field(
                fields,
                "extension_manifest_schema_version",
                &manifest_schema_version,
            );
            push_field(
                fields,
                "extension_manifest_json_parse_status",
                "passed_no_code_loaded",
            );
        }
    }
}

fn append_extension_manifest_identity_fields(
    fields: &mut Vec<(String, String)>,
    manifest: &ExtensionManifest,
) {
    push_field(fields, "extension_manifest_id", manifest.id.as_str());
    push_field(fields, "extension_manifest_name", &manifest.name);
    push_field(
        fields,
        "extension_manifest_version",
        &manifest.version.summary(),
    );
    push_field(
        fields,
        "extension_manifest_provider",
        manifest.provider.as_deref().unwrap_or("not_declared"),
    );
    push_field(
        fields,
        "extension_manifest_category",
        manifest.category.as_str(),
    );
    push_field(
        fields,
        "extension_manifest_lifecycle",
        manifest.lifecycle.as_str(),
    );
}

fn append_extension_manifest_capability_fields(
    fields: &mut Vec<(String, String)>,
    manifest: &ExtensionManifest,
) {
    push_count_field(
        fields,
        "extension_manifest_capability_count",
        manifest.capabilities.len(),
    );
    push_count_field(
        fields,
        "extension_manifest_supported_capability_claim_count",
        manifest
            .capabilities
            .iter()
            .filter(|capability| capability.is_usable())
            .count(),
    );
    push_field(
        fields,
        "extension_manifest_capability_summaries",
        &extension_capability_summaries(manifest),
    );
}

fn append_extension_manifest_permission_effect_fields(
    fields: &mut Vec<(String, String)>,
    manifest: &ExtensionManifest,
    status: ExtensionInspectionStatus,
) {
    push_count_field(
        fields,
        "extension_manifest_permission_count",
        manifest.permissions.len(),
    );
    push_field(
        fields,
        "extension_manifest_permission_names",
        &extension_permission_names(manifest),
    );
    push_count_field(
        fields,
        "extension_manifest_effect_count",
        manifest.effects.len(),
    );
    push_field(
        fields,
        "extension_manifest_effect_kinds",
        &extension_effect_kinds(manifest),
    );
    push_field(
        fields,
        "extension_manifest_effect_levels",
        &extension_effect_levels(manifest),
    );
    push_field(
        fields,
        "extension_manifest_license",
        manifest.provenance.license.as_str(),
    );
    push_bool_field(
        fields,
        "extension_manifest_provenance_requires_review",
        manifest.provenance.requires_review(),
    );
    push_bool_field(
        fields,
        "extension_manifest_effects_declared",
        manifest.has_effects(),
    );
    push_bool_field(
        fields,
        "extension_manifest_review_required",
        status == ExtensionInspectionStatus::RequiresReview,
    );
    push_bool_field(
        fields,
        "extension_manifest_usable",
        manifest.is_usable() && status == ExtensionInspectionStatus::Validated,
    );
}

fn append_extension_manifest_sandbox_runtime_fields(
    fields: &mut Vec<(String, String)>,
    manifest: &ExtensionManifest,
) {
    push_field(
        fields,
        "extension_manifest_sandbox_kind",
        manifest.sandbox.kind.as_str(),
    );
    push_bool_field(
        fields,
        "extension_manifest_sandbox_safe_default",
        manifest.sandbox.is_safe_default(),
    );
    push_bool_field(
        fields,
        "extension_manifest_sandbox_allow_filesystem",
        manifest.sandbox.allow_filesystem,
    );
    push_bool_field(
        fields,
        "extension_manifest_sandbox_allow_network",
        manifest.sandbox.allow_network,
    );
    push_bool_field(
        fields,
        "extension_manifest_sandbox_allow_environment",
        manifest.sandbox.allow_environment,
    );
    push_bool_field(
        fields,
        "extension_manifest_sandbox_allow_secret_access",
        manifest.sandbox.allow_secret_access,
    );
    push_field(
        fields,
        "extension_manifest_abi_status",
        manifest
            .abi
            .as_ref()
            .map_or("not_declared", |abi| abi.status.as_str()),
    );
    push_field(
        fields,
        "extension_manifest_runtime_kind",
        manifest
            .runtime
            .map_or("not_declared", |runtime| runtime.as_str()),
    );
}

fn append_extension_manifest_contract_fields(
    fields: &mut Vec<(String, String)>,
    manifest: &ExtensionManifest,
) {
    let contract = &manifest.execution_contract;
    push_bool_field(
        fields,
        "extension_manifest_execution_contract_complete",
        contract.production_contract_complete(),
    );
    push_field(
        fields,
        "extension_manifest_determinism",
        contract.determinism.as_str(),
    );
    push_field(
        fields,
        "extension_manifest_materialization",
        contract.materialization.as_str(),
    );
    push_bool_field(
        fields,
        "extension_manifest_materialization_required",
        contract.materialization.requires_materialization(),
    );
    push_field(
        fields,
        "extension_manifest_null_behavior",
        contract.null_behavior.as_str(),
    );
    push_field(
        fields,
        "extension_manifest_input_dtypes",
        &contract.input_dtype_summary(),
    );
    push_field(
        fields,
        "extension_manifest_output_dtype",
        contract.output_dtype_summary(),
    );
    push_bool_field(
        fields,
        "extension_manifest_dtype_contract_declared",
        contract.dtype_contract_declared(),
    );
    push_field(
        fields,
        "extension_manifest_resource_contract",
        &contract.resource_summary(),
    );
    push_count_field(
        fields,
        "extension_manifest_timeout_millis",
        optional_u64_as_count(contract.timeout_millis),
    );
    push_count_field(
        fields,
        "extension_manifest_max_memory_bytes",
        optional_u64_as_count(contract.max_memory_bytes),
    );
    push_count_field(
        fields,
        "extension_manifest_max_cpu_millis",
        optional_u64_as_count(contract.max_cpu_millis),
    );
    push_bool_field(
        fields,
        "extension_manifest_resource_contract_declared",
        contract.resource_contract_declared(),
    );
    push_field(
        fields,
        "extension_manifest_retry_policy",
        contract.retry.as_str(),
    );
    push_field(
        fields,
        "extension_manifest_idempotency_policy",
        contract.idempotency.as_str(),
    );
    push_field(
        fields,
        "extension_manifest_audit_policy",
        contract.audit.as_str(),
    );
}

fn append_extension_manifest_no_runtime_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExtensionInspectionReport,
) {
    for (key, value) in [
        ("extension_manifest_runtime_execution", false),
        ("extension_manifest_dynamic_loading_performed", false),
        (
            "extension_manifest_extension_code_executed",
            report.code_executed,
        ),
        ("extension_manifest_udf_execution_performed", false),
        ("extension_manifest_external_effect_executed", false),
        ("extension_manifest_credential_resolution_performed", false),
        ("extension_manifest_network_probe_performed", false),
        ("extension_manifest_dependency_expansion_allowed", false),
        ("extension_manifest_fallback_attempted", false),
        ("extension_manifest_external_engine_invoked", false),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn extension_capability_summaries(manifest: &ExtensionManifest) -> String {
    if manifest.capabilities.is_empty() {
        return "none".to_string();
    }
    manifest
        .capabilities
        .iter()
        .map(ExtensionCapability::summary)
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_permission_names(manifest: &ExtensionManifest) -> String {
    if manifest.permissions.is_empty() {
        return "none".to_string();
    }
    manifest
        .permissions
        .iter()
        .map(|permission| permission.permission.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_effect_kinds(manifest: &ExtensionManifest) -> String {
    if manifest.effects.is_empty() {
        return "none".to_string();
    }
    manifest
        .effects
        .iter()
        .map(|effect| effect.effect.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_effect_levels(manifest: &ExtensionManifest) -> String {
    if manifest.effects.is_empty() {
        return "none".to_string();
    }
    manifest
        .effects
        .iter()
        .map(|effect| effect.level.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_registry_manifest_ids(snapshot: &ExtensionRegistrySnapshot) -> String {
    if snapshot.manifests.is_empty() {
        return "none".to_string();
    }
    snapshot
        .manifests
        .iter()
        .map(|manifest| manifest.id.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_registry_manifest_categories(snapshot: &ExtensionRegistrySnapshot) -> String {
    if snapshot.manifests.is_empty() {
        return "none".to_string();
    }
    snapshot
        .manifests
        .iter()
        .map(|manifest| manifest.category.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_registry_materialization_modes(snapshot: &ExtensionRegistrySnapshot) -> String {
    if snapshot.manifests.is_empty() {
        return "none".to_string();
    }
    snapshot
        .manifests
        .iter()
        .map(|manifest| manifest.execution_contract.materialization.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extension_report_only_fields(mode: &str) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), mode.to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        ("extension_code_executed".to_string(), "false".to_string()),
        ("dynamic_loading".to_string(), "false".to_string()),
    ];
    append_extension_manifest_effect_capability_matrix_fields(&mut fields);
    append_plugin_abi_udf_sandbox_blocker_fields(&mut fields);
    append_effectful_operation_admission_matrix_fields(&mut fields);
    fields
}

pub(crate) fn append_extension_manifest_effect_capability_matrix_fields(
    fields: &mut Vec<(String, String)>,
) {
    let matrix = ExtensionManifestEffectCapabilityMatrix::report_only();
    let row_order = matrix.row_order().join(",");
    let blocker_ids = matrix.blocker_ids().join(",");
    let required_evidence = matrix.required_evidence().join("|");
    for (key, value) in [
        (
            "extension_manifest_effect_matrix_schema_version",
            matrix.schema_version,
        ),
        ("extension_manifest_effect_matrix_id", matrix.matrix_id),
        ("extension_manifest_effect_docs_ref", matrix.docs_ref),
        (
            "extension_manifest_effect_support_status_vocabulary",
            "report_only,blocked,unsupported",
        ),
        (
            "extension_manifest_effect_claim_gate_status",
            matrix.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "extension_manifest_effect_row_count",
        matrix.rows.len(),
    );
    for (key, value) in [
        ("extension_manifest_effect_row_order", row_order.as_str()),
        (
            "extension_manifest_effect_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "extension_manifest_effect_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_extension_manifest_effect_matrix_bool_fields(fields, &matrix);
    for row in &matrix.rows {
        append_extension_manifest_effect_capability_row_fields(fields, row);
    }
}

fn append_extension_manifest_effect_matrix_bool_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &ExtensionManifestEffectCapabilityMatrix,
) {
    for (key, value) in [
        (
            "extension_manifest_effect_all_runtime_blocked",
            matrix.all_runtime_blocked(),
        ),
        (
            "extension_manifest_effect_all_external_effects_blocked",
            matrix.all_external_effects_blocked(),
        ),
        (
            "extension_manifest_effect_runtime_execution",
            matrix.runtime_execution,
        ),
        (
            "extension_manifest_effect_extension_code_executed",
            matrix.extension_code_executed,
        ),
        (
            "extension_manifest_effect_dynamic_loading",
            matrix.dynamic_loading,
        ),
        (
            "extension_manifest_effect_udf_execution",
            matrix.udf_execution,
        ),
        (
            "extension_manifest_effect_external_effect_executed",
            matrix.external_effect_executed,
        ),
        (
            "extension_manifest_effect_credential_resolution_performed",
            matrix.credential_resolution_performed,
        ),
        (
            "extension_manifest_effect_network_probe_performed",
            matrix.network_probe_performed,
        ),
        (
            "extension_manifest_effect_dependency_expansion_allowed",
            matrix.dependency_expansion_allowed,
        ),
        (
            "extension_manifest_effect_fallback_attempted",
            matrix.fallback_attempted,
        ),
        (
            "extension_manifest_effect_external_engine_invoked",
            matrix.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_extension_manifest_effect_capability_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &ExtensionManifestEffectCapabilityRow,
) {
    let prefix = format!("extension_manifest_effect_row_{}", row.row_id);
    for (suffix, value) in [
        ("extension_type", row.extension_type),
        ("support_status", row.support_status),
        ("manifest_status", row.manifest_status),
        ("required_permissions", row.required_permissions),
        ("sandbox_policy", row.sandbox_policy),
        ("effect_metadata", row.effect_metadata),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_boundary_required"),
        row.materialization_boundary_required,
    );
    for (suffix, value) in [
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    append_extension_manifest_effect_row_bool_fields(fields, row, &prefix);
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_extension_manifest_effect_row_bool_fields(
    fields: &mut Vec<(String, String)>,
    row: &ExtensionManifestEffectCapabilityRow,
    prefix: &str,
) {
    for (suffix, value) in [
        ("runtime_execution", row.runtime_execution),
        ("extension_code_executed", row.extension_code_executed),
        ("dynamic_loading", row.dynamic_loading),
        ("udf_execution", row.udf_execution),
        ("external_effect_executed", row.external_effect_executed),
        (
            "credential_resolution_performed",
            row.credential_resolution_performed,
        ),
        ("network_probe_performed", row.network_probe_performed),
        (
            "dependency_expansion_allowed",
            row.dependency_expansion_allowed,
        ),
        ("fallback_attempted", row.fallback_attempted),
        ("external_engine_invoked", row.external_engine_invoked),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

pub(crate) fn append_plugin_abi_udf_sandbox_blocker_fields(fields: &mut Vec<(String, String)>) {
    let report = plan_plugin_abi_udf_sandbox_blocker();
    let row_order = report.row_order().join(",");
    let blocker_ids = report.blocker_ids().join(",");
    let required_evidence = report.required_evidence().join("|");
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_schema_version",
            report.schema_version,
        ),
        ("plugin_abi_udf_sandbox_blocker_id", report.blocker_id),
        ("plugin_abi_udf_sandbox_blocker_docs_ref", report.docs_ref),
        (
            "plugin_abi_udf_sandbox_blocker_support_status",
            report.support_status,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_claim_gate_status",
            report.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "plugin_abi_udf_sandbox_blocker_row_count",
        report.rows.len(),
    );
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_row_order",
            row_order.as_str(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_plugin_abi_udf_sandbox_blocker_bool_fields(fields, &report);
    for row in &report.rows {
        append_plugin_abi_udf_sandbox_blocker_row_fields(fields, row);
    }
}

pub(crate) fn append_effectful_operation_admission_matrix_fields(
    fields: &mut Vec<(String, String)>,
) {
    let matrix = EffectfulOperationAdmissionMatrix::current();
    let row_order = matrix.row_order().join(",");
    let blocker_ids = matrix.blocker_ids().join(",");
    let required_evidence = matrix.required_evidence().join("|");
    for (key, value) in [
        (
            "effectful_operation_admission_matrix_schema_version",
            matrix.schema_version,
        ),
        ("effectful_operation_admission_matrix_id", matrix.matrix_id),
        ("effectful_operation_admission_docs_ref", matrix.docs_ref),
        (
            "effectful_operation_admission_support_status_vocabulary",
            "fixture_smoke_supported,metadata_only_supported,blocked",
        ),
        (
            "effectful_operation_admission_claim_gate_status",
            matrix.claim_gate_status,
        ),
    ] {
        push_field(fields, key, value);
    }
    push_count_field(
        fields,
        "effectful_operation_admission_row_count",
        matrix.rows.len(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_admitted_local_fixture_count",
        matrix.admitted_local_fixture_count(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_metadata_only_count",
        matrix.metadata_only_count(),
    );
    push_count_field(
        fields,
        "effectful_operation_admission_blocked_count",
        matrix.blocked_count(),
    );
    for (key, value) in [
        (
            "effectful_operation_admission_row_order",
            row_order.as_str(),
        ),
        (
            "effectful_operation_admission_blocker_ids",
            blocker_ids.as_str(),
        ),
        (
            "effectful_operation_admission_required_evidence",
            required_evidence.as_str(),
        ),
    ] {
        push_field(fields, key, value);
    }
    append_effectful_operation_admission_matrix_bool_fields(fields, &matrix);
    for row in &matrix.rows {
        append_effectful_operation_admission_row_fields(fields, row);
    }
}

fn append_effectful_operation_admission_matrix_bool_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &EffectfulOperationAdmissionMatrix,
) {
    for (key, value) in [
        (
            "effectful_operation_admission_all_external_and_sandboxed_paths_blocked",
            matrix.all_external_and_sandboxed_paths_blocked(),
        ),
        (
            "effectful_operation_admission_credential_resolution_performed",
            matrix.credential_resolution_performed,
        ),
        (
            "effectful_operation_admission_network_probe_performed",
            matrix.network_probe_performed,
        ),
        (
            "effectful_operation_admission_dynamic_loading_performed",
            matrix.dynamic_loading_performed,
        ),
        (
            "effectful_operation_admission_extension_code_executed",
            matrix.extension_code_executed,
        ),
        (
            "effectful_operation_admission_external_effect_executed",
            matrix.external_effect_executed,
        ),
        (
            "effectful_operation_admission_dependency_expansion_allowed",
            matrix.dependency_expansion_allowed,
        ),
        (
            "effectful_operation_admission_fallback_attempted",
            matrix.fallback_attempted,
        ),
        (
            "effectful_operation_admission_external_engine_invoked",
            matrix.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_effectful_operation_admission_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &EffectfulOperationAdmissionRow,
) {
    let prefix = format!("effectful_operation_admission_row_{}", row.row_id);
    for (suffix, value) in [
        ("family", row.family),
        ("operation", row.operation),
        ("support_status", row.support_status),
        ("admission_scope", row.admission_scope),
        ("permission_status", row.permission_status),
        ("effect_status", row.effect_status),
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    for (suffix, value) in [
        ("credential_required", row.credential_required),
        ("network_required", row.network_required),
        ("sandbox_required", row.sandbox_required),
        (
            "local_filesystem_io_allowed",
            row.local_filesystem_io_allowed,
        ),
        ("runtime_fixture_available", row.runtime_fixture_available),
        ("extension_code_executed", row.extension_code_executed),
        ("dynamic_loading_performed", row.dynamic_loading_performed),
        ("external_effect_executed", row.external_effect_executed),
        ("fallback_attempted", row.fallback_attempted),
        ("external_engine_invoked", row.external_engine_invoked),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_plugin_abi_udf_sandbox_blocker_bool_fields(
    fields: &mut Vec<(String, String)>,
    report: &PluginAbiUdfSandboxBlockerReport,
) {
    for (key, value) in [
        (
            "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
            report.all_plugin_runtime_blocked(),
        ),
        (
            "plugin_abi_udf_sandbox_blocker_abi_loading_supported",
            report.abi_loading_supported,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_dynamic_loading_performed",
            report.dynamic_loading_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_extension_code_executed",
            report.extension_code_executed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_udf_execution_performed",
            report.udf_execution_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_sandbox_evidence_required",
            report.sandbox_evidence_required,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_sandbox_enforced",
            report.sandbox_enforced,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_permission_policy_enforced",
            report.permission_policy_enforced,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_runtime_execution",
            report.runtime_execution,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_external_effect_executed",
            report.external_effect_executed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_credential_resolution_performed",
            report.credential_resolution_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_network_probe_performed",
            report.network_probe_performed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_dependency_expansion_allowed",
            report.dependency_expansion_allowed,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_fallback_attempted",
            report.fallback_attempted,
        ),
        (
            "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
            report.external_engine_invoked,
        ),
    ] {
        push_bool_field(fields, key, value);
    }
}

fn append_plugin_abi_udf_sandbox_blocker_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &PluginAbiUdfSandboxBlockerRow,
) {
    let prefix = format!("plugin_abi_udf_sandbox_blocker_row_{}", row.row_id);
    for (suffix, value) in [
        ("plugin_surface", row.plugin_surface),
        ("support_status", row.support_status),
        ("abi_status", row.abi_status),
        ("sandbox_requirement", row.sandbox_requirement),
        ("blocker_id", row.blocker_id),
        ("diagnostic_code", row.diagnostic_code),
        ("required_evidence", row.required_evidence),
        ("user_visible_surface", row.user_visible_surface),
    ] {
        push_field(fields, &format!("{prefix}_{suffix}"), value);
    }
    append_plugin_abi_udf_sandbox_blocker_row_bool_fields(fields, row, &prefix);
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
}

fn append_plugin_abi_udf_sandbox_blocker_row_bool_fields(
    fields: &mut Vec<(String, String)>,
    row: &PluginAbiUdfSandboxBlockerRow,
    prefix: &str,
) {
    for (suffix, value) in [
        ("dynamic_loading_performed", row.dynamic_loading_performed),
        ("extension_code_executed", row.extension_code_executed),
        ("udf_execution_performed", row.udf_execution_performed),
        ("sandbox_enforced", row.sandbox_enforced),
        ("permission_policy_enforced", row.permission_policy_enforced),
        ("runtime_execution", row.runtime_execution),
        ("external_effect_executed", row.external_effect_executed),
        (
            "credential_resolution_performed",
            row.credential_resolution_performed,
        ),
        ("network_probe_performed", row.network_probe_performed),
        (
            "dependency_expansion_allowed",
            row.dependency_expansion_allowed,
        ),
        ("fallback_attempted", row.fallback_attempted),
        ("external_engine_invoked", row.external_engine_invoked),
    ] {
        push_bool_field(fields, &format!("{prefix}_{suffix}"), value);
    }
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn optional_u64_as_count(value: Option<u64>) -> usize {
    value
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0)
}
