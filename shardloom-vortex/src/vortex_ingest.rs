//! Feature-gated local `vortex_ingest` lifecycle helpers.
//!
//! This module intentionally exposes a narrow local prepare-once path for flat
//! scalar rows. It writes a local Vortex artifact, reopens/scans the artifact
//! for row-count proof, and returns evidence fields that callers can surface as
//! a `VortexPreparedState`. It is not a broad Vortex writer, object-store sink,
//! table commit path, or query-engine integration.

use std::{
    collections::BTreeMap,
    fs,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

#[cfg(feature = "vortex-write")]
use std::{collections::BTreeSet, time::Instant};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use arrow_array::{
    Array, ArrayRef as ArrowArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use shardloom_core::{
    LogicalDType, Result, ScalarValue, ShardLoomError, WorkspaceSafeLocalWriteReport,
};
use shardloom_exec::{
    OperatorMemoryClass, PulseWeaveInput, PulseWeaveReport, PulseWeaveTaskShape, plan_pulseweave,
};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use crate::universal_format_io::FlatLocalColumnarSource;

/// Evidence schema emitted by the local Vortex preparation spine.
pub const VORTEX_PREPARATION_SPINE_SCHEMA_VERSION: &str = "shardloom.vortex_preparation_spine.v1";
/// Evidence schema emitted by scoped local differential Vortex preparation overlays.
pub const VORTEX_DIFFERENTIAL_PREPARATION_SCHEMA_VERSION: &str =
    "shardloom.vortex_differential_preparation.v1";
/// Evidence schema emitted by automatic append-only differential refinement manifests.
pub const VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION: &str =
    "shardloom.vortex_differential_refinement_manifest.v1";
/// Admission policy for automatic local append-only prepared-state refinement.
pub const VORTEX_DIFFERENTIAL_REFINEMENT_POLICY: &str =
    "artifact_adjacent_append_only_prepared_state_refinement.v1";
/// Evidence schema emitted by scoped local capillary cold-preparation task control.
pub const VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION: &str =
    "shardloom.vortex_capillary_preparation.v1";
const VORTEX_CAPILLARY_ACTIVATION_POLICY: &str = "dynamic_size_complexity_gate.v1";
const VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;
const VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROWS: u64 = 1_000_000;
const VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_SPLITS: usize = 8;
const VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_COLUMNS: usize = 64;
const VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROW_COLUMN_PRODUCT: u64 = 32_000_000;
const VORTEX_CAPILLARY_ACTIVATION_MEMORY_PRESSURE_DENOMINATOR: u64 = 4;
/// Evidence schema emitted by scoped local scout ingress and triage.
pub const VORTEX_SCOUT_INGRESS_SCHEMA_VERSION: &str = "shardloom.vortex_scout_ingress.v1";
/// Evidence schema emitted by scoped local layout/write advisor checks.
pub const VORTEX_LAYOUT_WRITE_ADVISOR_SCHEMA_VERSION: &str =
    "shardloom.vortex_layout_write_advisor.v1";
/// Evidence schema emitted by scoped local copy-budget and buffer-lifecycle checks.
pub const VORTEX_COPY_BUDGET_SCHEMA_VERSION: &str = "shardloom.vortex_copy_budget.v1";
/// Evidence schema emitted by scoped local prepared-state reuse manifests.
pub const VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION: &str =
    "shardloom.vortex_prepared_state_reuse_manifest.v1";
/// Admission policy for artifact-adjacent prepared-state reuse manifests.
pub const VORTEX_PREPARED_STATE_REUSE_POLICY: &str =
    "artifact_adjacent_local_prepared_state_reuse.v1";
/// Pinned upstream Vortex crate line used by the scoped local preparation spine.
pub const VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION: &str = "0.73";

/// Request used to decide whether an existing local Vortex prepared artifact can
/// be reused without re-running compatibility preparation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPreparedStateReuseRequest {
    pub source_path: PathBuf,
    pub manifest_path: PathBuf,
    pub prepared_artifact_path: PathBuf,
    pub source_format: String,
    pub source_content_digest: String,
    pub source_size_bytes: u64,
    pub source_mtime_ns: String,
    pub source_schema_digest: Option<String>,
    pub parse_decode_plan_digest: String,
    pub selected_columns: String,
    pub output_policy: String,
    pub provider_version: String,
    pub feature_gates: String,
    pub certification_level: String,
}

/// Evidence attached when writing a prepared-state reuse manifest after a
/// successful local Vortex preparation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPreparedStateReuseWriteEvidence {
    pub source_state_id: String,
    pub source_state_digest: String,
    pub source_schema_digest: String,
    pub source_row_count: u64,
    pub source_column_family_summary: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub prepared_artifact_digest: String,
    pub certificate_refs: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Result of a prepared-state reuse lookup or manifest write.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexPreparedStateReuseReport {
    pub schema_version: &'static str,
    pub status: String,
    pub scope: String,
    pub manifest_path: PathBuf,
    pub policy: &'static str,
    pub hit: bool,
    pub reason: String,
    pub manifest_digest: String,
    pub invalidation_reason: String,
    pub manifest_written: bool,
    pub source_path: PathBuf,
    pub source_format: String,
    pub source_content_digest: String,
    pub source_size_bytes: u64,
    pub source_mtime_ns: String,
    pub source_schema_digest: String,
    pub source_row_count: u64,
    pub source_column_family_summary: String,
    pub parse_decode_plan_digest: String,
    pub selected_columns: String,
    pub output_policy: String,
    pub prepared_artifact_ref: PathBuf,
    pub prepared_artifact_digest: String,
    pub prepared_artifact_size_bytes: u64,
    pub provider_version: String,
    pub feature_gates: String,
    pub certification_level: String,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub certificate_refs: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexPreparedStateReuseRequest {
    /// Create a local prepared-state reuse request and fingerprint the source
    /// path. The source content digest is the primary fail-closed invalidation
    /// key; mtime is reported as evidence but does not invalidate by itself.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the local source cannot
    /// be fingerprinted.
    #[allow(clippy::too_many_arguments)]
    pub fn new_local(
        source_path: impl AsRef<Path>,
        prepared_artifact_path: impl AsRef<Path>,
        manifest_path: impl Into<PathBuf>,
        source_format: impl Into<String>,
        source_schema_digest: Option<String>,
        parse_decode_plan_digest: impl Into<String>,
        selected_columns: impl Into<String>,
        output_policy: impl Into<String>,
        provider_version: impl Into<String>,
        feature_gates: impl Into<String>,
        certification_level: impl Into<String>,
    ) -> Result<Self> {
        let source_fingerprint = LocalReuseFileFingerprint::from_path(
            source_path.as_ref(),
            "prepared-state reuse source",
        )?;
        Ok(Self {
            source_path: source_fingerprint.path,
            manifest_path: absolute_local_path(manifest_path.into())?,
            prepared_artifact_path: absolute_local_path(prepared_artifact_path.as_ref())?,
            source_format: source_format.into(),
            source_content_digest: source_fingerprint.content_digest,
            source_size_bytes: source_fingerprint.size_bytes,
            source_mtime_ns: source_fingerprint.mtime_ns,
            source_schema_digest,
            parse_decode_plan_digest: parse_decode_plan_digest.into(),
            selected_columns: selected_columns.into(),
            output_policy: output_policy.into(),
            provider_version: provider_version.into(),
            feature_gates: feature_gates.into(),
            certification_level: certification_level.into(),
        })
    }

    /// Create a local prepared-state reuse request for a deterministic
    /// generated/source-free input. The generated source has no physical source
    /// file to fingerprint, so callers must provide the stable source digest
    /// and generated-source byte count that will fail-closed on source kind,
    /// schema, row, plan, or output-policy drift.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the generated source
    /// reference is empty or local artifact paths cannot be resolved.
    #[allow(clippy::too_many_arguments)]
    pub fn new_generated_local(
        generated_source_ref: impl Into<String>,
        prepared_artifact_path: impl AsRef<Path>,
        manifest_path: impl Into<PathBuf>,
        source_format: impl Into<String>,
        source_content_digest: impl Into<String>,
        source_size_bytes: u64,
        source_schema_digest: impl Into<String>,
        parse_decode_plan_digest: impl Into<String>,
        selected_columns: impl Into<String>,
        output_policy: impl Into<String>,
        provider_version: impl Into<String>,
        feature_gates: impl Into<String>,
        certification_level: impl Into<String>,
    ) -> Result<Self> {
        let generated_source_ref = generated_source_ref.into();
        if generated_source_ref.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "generated prepared-state reuse source reference must not be empty; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        Ok(Self {
            source_path: PathBuf::from(generated_source_ref),
            manifest_path: absolute_local_path(manifest_path.into())?,
            prepared_artifact_path: absolute_local_path(prepared_artifact_path.as_ref())?,
            source_format: source_format.into(),
            source_content_digest: source_content_digest.into(),
            source_size_bytes,
            source_mtime_ns: "not_applicable_generated_source".to_string(),
            source_schema_digest: Some(source_schema_digest.into()),
            parse_decode_plan_digest: parse_decode_plan_digest.into(),
            selected_columns: selected_columns.into(),
            output_policy: output_policy.into(),
            provider_version: provider_version.into(),
            feature_gates: feature_gates.into(),
            certification_level: certification_level.into(),
        })
    }
}

impl VortexPreparedStateReuseReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "vortex_prepared_state_reuse_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_status".to_string(),
                self.status.clone(),
            ),
            (
                "vortex_prepared_state_reuse_scope".to_string(),
                self.scope.clone(),
            ),
            (
                "vortex_prepared_state_reuse_manifest_path".to_string(),
                self.manifest_path.display().to_string(),
            ),
            (
                "vortex_prepared_state_reuse_policy".to_string(),
                self.policy.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_hit".to_string(),
                self.hit.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_reason".to_string(),
                self.reason.clone(),
            ),
            (
                "vortex_prepared_state_reuse_manifest_digest".to_string(),
                self.manifest_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_invalidation_reason".to_string(),
                self.invalidation_reason.clone(),
            ),
            (
                "vortex_prepared_state_reuse_manifest_written".to_string(),
                self.manifest_written.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_source_path".to_string(),
                self.source_path.display().to_string(),
            ),
            (
                "vortex_prepared_state_reuse_source_format".to_string(),
                self.source_format.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_content_digest".to_string(),
                self.source_content_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_size_bytes".to_string(),
                self.source_size_bytes.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_source_mtime_ns".to_string(),
                self.source_mtime_ns.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_schema_digest".to_string(),
                self.source_schema_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_row_count".to_string(),
                self.source_row_count.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_source_column_family_summary".to_string(),
                self.source_column_family_summary.clone(),
            ),
            (
                "vortex_prepared_state_reuse_parse_decode_plan_digest".to_string(),
                self.parse_decode_plan_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_selected_columns".to_string(),
                self.selected_columns.clone(),
            ),
            (
                "vortex_prepared_state_reuse_output_policy".to_string(),
                self.output_policy.clone(),
            ),
            (
                "vortex_prepared_state_reuse_prepared_artifact_ref".to_string(),
                self.prepared_artifact_ref.display().to_string(),
            ),
            (
                "vortex_prepared_state_reuse_prepared_artifact_digest".to_string(),
                self.prepared_artifact_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_prepared_artifact_size_bytes".to_string(),
                self.prepared_artifact_size_bytes.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_provider_version".to_string(),
                self.provider_version.clone(),
            ),
            (
                "vortex_prepared_state_reuse_feature_gates".to_string(),
                self.feature_gates.clone(),
            ),
            (
                "vortex_prepared_state_reuse_certification_level".to_string(),
                self.certification_level.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_state_id".to_string(),
                self.source_state_id.clone(),
            ),
            (
                "vortex_prepared_state_reuse_source_state_digest".to_string(),
                self.source_state_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_prepared_state_id".to_string(),
                self.prepared_state_id.clone(),
            ),
            (
                "vortex_prepared_state_reuse_prepared_state_digest".to_string(),
                self.prepared_state_digest.clone(),
            ),
            (
                "vortex_prepared_state_reuse_certificate_refs".to_string(),
                self.certificate_refs.clone(),
            ),
            (
                "vortex_prepared_state_reuse_fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "vortex_prepared_state_reuse_external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
        ]
    }
}

/// Return the artifact-adjacent reuse manifest path for a local Vortex artifact.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the artifact path has no
/// file name.
pub fn vortex_prepared_state_reuse_manifest_path(
    prepared_artifact_path: impl AsRef<Path>,
) -> Result<PathBuf> {
    let path = absolute_local_path(prepared_artifact_path.as_ref())?;
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "prepared-state reuse manifest requires a local artifact file name for '{}'; no fallback execution was attempted",
            path.display()
        )));
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent
        .join(".shardloom")
        .join(format!("{file_name}.prepared-state-reuse.manifest")))
}

/// Evaluate whether a local prepared artifact can be reused.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the manifest exists but
/// cannot be read or parsed deterministically.
#[allow(clippy::too_many_lines)]
pub fn evaluate_vortex_prepared_state_reuse(
    request: &VortexPreparedStateReuseRequest,
) -> Result<VortexPreparedStateReuseReport> {
    if !request.manifest_path.exists() {
        return Ok(prepared_state_reuse_miss(
            request,
            "no_reuse_manifest",
            "none",
        ));
    }
    let fields = read_reuse_manifest_fields(&request.manifest_path)?;
    let manifest_digest = fields
        .get("manifest_digest")
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    let expected_digest = reuse_manifest_digest(&fields);
    if manifest_digest != expected_digest {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "reuse_manifest_digest_mismatch",
            &manifest_digest,
        ));
    }
    if fields.get("schema_version").map(String::as_str)
        != Some(VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION)
    {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "reuse_manifest_schema_mismatch",
            &manifest_digest,
        ));
    }
    if let Some(reason) = reuse_manifest_request_mismatch_reason(request, &fields) {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            &reason,
            &manifest_digest,
        ));
    }
    if fields.get("fallback_attempted").map(String::as_str) != Some("false") {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "reuse_manifest_fallback_attempted",
            &manifest_digest,
        ));
    }
    if fields.get("external_engine_invoked").map(String::as_str) != Some("false") {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "reuse_manifest_external_engine_invoked",
            &manifest_digest,
        ));
    }
    let artifact_fingerprint = LocalReuseFileFingerprint::from_path(
        &request.prepared_artifact_path,
        "prepared-state reuse artifact",
    )?;
    if fields
        .get("prepared_artifact_size_bytes")
        .map(String::as_str)
        != Some(artifact_fingerprint.size_bytes.to_string().as_str())
    {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "prepared_artifact_size_changed",
            &manifest_digest,
        ));
    }
    if fields.get("prepared_artifact_digest").map(String::as_str)
        != Some(artifact_fingerprint.content_digest.as_str())
    {
        return Ok(prepared_state_reuse_miss_with_digest(
            request,
            "prepared_artifact_digest_changed",
            &manifest_digest,
        ));
    }
    for required in [
        "source_state_id",
        "source_state_digest",
        "prepared_state_id",
        "prepared_state_digest",
    ] {
        if fields.get(required).is_none_or(String::is_empty) {
            return Ok(prepared_state_reuse_miss_with_digest(
                request,
                &format!("reuse_manifest_missing_{required}"),
                &manifest_digest,
            ));
        }
    }
    Ok(VortexPreparedStateReuseReport {
        schema_version: VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION,
        status: "prepared_state_reuse_hit".to_string(),
        scope: "artifact_adjacent_manifest_local_vortex_artifacts".to_string(),
        manifest_path: request.manifest_path.clone(),
        policy: VORTEX_PREPARED_STATE_REUSE_POLICY,
        hit: true,
        reason: "manifest_fingerprints_match".to_string(),
        manifest_digest,
        invalidation_reason: "none".to_string(),
        manifest_written: false,
        source_path: request.source_path.clone(),
        source_format: request.source_format.clone(),
        source_content_digest: request.source_content_digest.clone(),
        source_size_bytes: request.source_size_bytes,
        source_mtime_ns: request.source_mtime_ns.clone(),
        source_schema_digest: fields
            .get("source_schema_digest")
            .cloned()
            .unwrap_or_else(|| request.source_schema_digest.clone().unwrap_or_default()),
        source_row_count: fields
            .get("source_row_count")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        source_column_family_summary: fields
            .get("source_column_family_summary")
            .cloned()
            .unwrap_or_default(),
        parse_decode_plan_digest: request.parse_decode_plan_digest.clone(),
        selected_columns: request.selected_columns.clone(),
        output_policy: request.output_policy.clone(),
        prepared_artifact_ref: request.prepared_artifact_path.clone(),
        prepared_artifact_digest: artifact_fingerprint.content_digest,
        prepared_artifact_size_bytes: artifact_fingerprint.size_bytes,
        provider_version: request.provider_version.clone(),
        feature_gates: request.feature_gates.clone(),
        certification_level: request.certification_level.clone(),
        source_state_id: fields.get("source_state_id").cloned().unwrap_or_default(),
        source_state_digest: fields
            .get("source_state_digest")
            .cloned()
            .unwrap_or_default(),
        prepared_state_id: fields.get("prepared_state_id").cloned().unwrap_or_default(),
        prepared_state_digest: fields
            .get("prepared_state_digest")
            .cloned()
            .unwrap_or_default(),
        certificate_refs: fields
            .get("certificate_refs")
            .cloned()
            .unwrap_or_else(|| "manifest_certificate_refs_missing".to_string()),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
}

/// Write a fail-closed prepared-state reuse manifest after a successful local
/// Vortex preparation.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the prepared artifact
/// cannot be fingerprinted or the manifest cannot be written atomically.
#[allow(clippy::too_many_lines)]
pub fn write_vortex_prepared_state_reuse_manifest(
    request: &VortexPreparedStateReuseRequest,
    previous_decision: &VortexPreparedStateReuseReport,
    evidence: VortexPreparedStateReuseWriteEvidence,
) -> Result<VortexPreparedStateReuseReport> {
    let artifact_fingerprint = LocalReuseFileFingerprint::from_path(
        &request.prepared_artifact_path,
        "prepared-state reuse artifact",
    )?;
    if artifact_fingerprint.content_digest != evidence.prepared_artifact_digest {
        return Err(ShardLoomError::InvalidOperation(format!(
            "prepared-state reuse manifest artifact digest mismatch for '{}': writer reported {}, file fingerprint {}; no fallback execution was attempted",
            request.prepared_artifact_path.display(),
            evidence.prepared_artifact_digest,
            artifact_fingerprint.content_digest
        )));
    }
    let mut fields = BTreeMap::new();
    fields.insert(
        "schema_version".to_string(),
        VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION.to_string(),
    );
    fields.insert(
        "policy".to_string(),
        VORTEX_PREPARED_STATE_REUSE_POLICY.to_string(),
    );
    fields.insert(
        "source_path".to_string(),
        request.source_path.display().to_string(),
    );
    fields.insert("source_format".to_string(), request.source_format.clone());
    fields.insert(
        "source_content_digest".to_string(),
        request.source_content_digest.clone(),
    );
    fields.insert(
        "source_size_bytes".to_string(),
        request.source_size_bytes.to_string(),
    );
    fields.insert(
        "source_mtime_ns".to_string(),
        request.source_mtime_ns.clone(),
    );
    fields.insert(
        "source_schema_digest".to_string(),
        evidence.source_schema_digest.clone(),
    );
    fields.insert(
        "source_row_count".to_string(),
        evidence.source_row_count.to_string(),
    );
    fields.insert(
        "source_column_family_summary".to_string(),
        evidence.source_column_family_summary.clone(),
    );
    fields.insert(
        "parse_decode_plan_digest".to_string(),
        request.parse_decode_plan_digest.clone(),
    );
    fields.insert(
        "selected_columns".to_string(),
        request.selected_columns.clone(),
    );
    fields.insert("output_policy".to_string(), request.output_policy.clone());
    fields.insert(
        "prepared_artifact_path".to_string(),
        request.prepared_artifact_path.display().to_string(),
    );
    fields.insert(
        "prepared_artifact_size_bytes".to_string(),
        artifact_fingerprint.size_bytes.to_string(),
    );
    fields.insert(
        "prepared_artifact_digest".to_string(),
        evidence.prepared_artifact_digest.clone(),
    );
    fields.insert(
        "provider_version".to_string(),
        request.provider_version.clone(),
    );
    fields.insert("feature_gates".to_string(), request.feature_gates.clone());
    fields.insert(
        "certification_level".to_string(),
        request.certification_level.clone(),
    );
    fields.insert(
        "source_state_id".to_string(),
        evidence.source_state_id.clone(),
    );
    fields.insert(
        "source_state_digest".to_string(),
        evidence.source_state_digest.clone(),
    );
    fields.insert(
        "prepared_state_id".to_string(),
        evidence.prepared_state_id.clone(),
    );
    fields.insert(
        "prepared_state_digest".to_string(),
        evidence.prepared_state_digest.clone(),
    );
    fields.insert(
        "certificate_refs".to_string(),
        evidence.certificate_refs.clone(),
    );
    fields.insert(
        "fallback_attempted".to_string(),
        evidence.fallback_attempted.to_string(),
    );
    fields.insert(
        "external_engine_invoked".to_string(),
        evidence.external_engine_invoked.to_string(),
    );
    let manifest_digest = reuse_manifest_digest(&fields);
    fields.insert("manifest_digest".to_string(), manifest_digest.clone());
    write_reuse_manifest_fields(&request.manifest_path, &fields)?;

    Ok(VortexPreparedStateReuseReport {
        schema_version: VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION,
        status: "prepared_state_created_manifest_written".to_string(),
        scope: "artifact_adjacent_manifest_local_vortex_artifacts".to_string(),
        manifest_path: request.manifest_path.clone(),
        policy: VORTEX_PREPARED_STATE_REUSE_POLICY,
        hit: false,
        reason: format!("prepared_state_created_after_{}", previous_decision.reason),
        manifest_digest,
        invalidation_reason: previous_decision.invalidation_reason.clone(),
        manifest_written: true,
        source_path: request.source_path.clone(),
        source_format: request.source_format.clone(),
        source_content_digest: request.source_content_digest.clone(),
        source_size_bytes: request.source_size_bytes,
        source_mtime_ns: request.source_mtime_ns.clone(),
        source_schema_digest: evidence.source_schema_digest,
        source_row_count: evidence.source_row_count,
        source_column_family_summary: evidence.source_column_family_summary,
        parse_decode_plan_digest: request.parse_decode_plan_digest.clone(),
        selected_columns: request.selected_columns.clone(),
        output_policy: request.output_policy.clone(),
        prepared_artifact_ref: request.prepared_artifact_path.clone(),
        prepared_artifact_digest: artifact_fingerprint.content_digest,
        prepared_artifact_size_bytes: artifact_fingerprint.size_bytes,
        provider_version: request.provider_version.clone(),
        feature_gates: request.feature_gates.clone(),
        certification_level: request.certification_level.clone(),
        source_state_id: evidence.source_state_id,
        source_state_digest: evidence.source_state_digest,
        prepared_state_id: evidence.prepared_state_id,
        prepared_state_digest: evidence.prepared_state_digest,
        certificate_refs: evidence.certificate_refs,
        fallback_attempted: evidence.fallback_attempted,
        external_engine_invoked: evidence.external_engine_invoked,
    })
}

/// Decision for automatic append-only refinement of an existing prepared state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexPreparedStateAppendOnlyRefinementDecision {
    pub schema_version: &'static str,
    pub status: String,
    pub policy: &'static str,
    pub reason: String,
    pub blocker_id: String,
    pub automatic_detection_status: String,
    pub manifest_path: PathBuf,
    pub reuse_manifest_digest: String,
    pub source_path: PathBuf,
    pub source_format: String,
    pub base_source_content_digest: String,
    pub current_source_content_digest: String,
    pub base_source_size_bytes: u64,
    pub current_source_size_bytes: u64,
    pub delta_byte_start: u64,
    pub delta_byte_end: u64,
    pub changed_byte_range_refs: String,
    pub source_prefix_verification_status: String,
    pub base_source_state_id: String,
    pub base_source_state_digest: String,
    pub base_source_row_count: u64,
    pub base_source_schema_digest: String,
    pub base_source_column_family_summary: String,
    pub base_prepared_state_id: String,
    pub base_prepared_state_digest: String,
    pub prepared_artifact_ref: PathBuf,
    pub prepared_artifact_digest: String,
    pub prepared_artifact_size_bytes: u64,
    pub certificate_refs: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexPreparedStateAppendOnlyRefinementDecision {
    /// Whether this decision admits automatic append-only refinement.
    #[must_use]
    pub fn is_admitted(&self) -> bool {
        self.status == "admitted_append_only_refinement"
    }

    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "vortex_differential_preparation_refinement_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "vortex_differential_preparation_refinement_mode".to_string(),
                "automatic_append_only_delta".to_string(),
            ),
            (
                "vortex_differential_preparation_refinement_status".to_string(),
                self.status.clone(),
            ),
            (
                "vortex_differential_preparation_refinement_policy".to_string(),
                self.policy.to_string(),
            ),
            (
                "vortex_differential_preparation_automatic_detection_status".to_string(),
                self.automatic_detection_status.clone(),
            ),
            (
                "vortex_differential_preparation_automatic_detection_reason".to_string(),
                self.reason.clone(),
            ),
            (
                "vortex_differential_preparation_blocker_id".to_string(),
                self.blocker_id.clone(),
            ),
            (
                "vortex_differential_preparation_base_source_content_digest".to_string(),
                self.base_source_content_digest.clone(),
            ),
            (
                "vortex_differential_preparation_current_source_content_digest".to_string(),
                self.current_source_content_digest.clone(),
            ),
            (
                "vortex_differential_preparation_base_source_size_bytes".to_string(),
                self.base_source_size_bytes.to_string(),
            ),
            (
                "vortex_differential_preparation_current_source_size_bytes".to_string(),
                self.current_source_size_bytes.to_string(),
            ),
            (
                "vortex_differential_preparation_delta_byte_start".to_string(),
                self.delta_byte_start.to_string(),
            ),
            (
                "vortex_differential_preparation_delta_byte_end".to_string(),
                self.delta_byte_end.to_string(),
            ),
            (
                "vortex_differential_preparation_source_prefix_verification_status".to_string(),
                self.source_prefix_verification_status.clone(),
            ),
            (
                "vortex_differential_preparation_reuse_manifest_path".to_string(),
                self.manifest_path.display().to_string(),
            ),
            (
                "vortex_differential_preparation_reuse_manifest_digest".to_string(),
                self.reuse_manifest_digest.clone(),
            ),
        ]
    }
}

/// Manifest write result for a scoped automatic differential refinement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexDifferentialRefinementManifestReport {
    pub schema_version: &'static str,
    pub manifest_path: PathBuf,
    pub manifest_digest: String,
    pub manifest_written: bool,
    pub refined_prepared_state_id: String,
    pub refined_prepared_state_digest: String,
    pub overlay_consumer_family: String,
    pub overlay_consumer_status: String,
    pub overlay_consumer_correctness_digest: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexDifferentialRefinementManifestReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "vortex_differential_preparation_refinement_manifest_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "vortex_differential_preparation_refinement_manifest_path".to_string(),
                self.manifest_path.display().to_string(),
            ),
            (
                "vortex_differential_preparation_refinement_manifest_digest".to_string(),
                self.manifest_digest.clone(),
            ),
            (
                "vortex_differential_preparation_refinement_manifest_written".to_string(),
                self.manifest_written.to_string(),
            ),
            (
                "vortex_differential_preparation_refined_prepared_state_id".to_string(),
                self.refined_prepared_state_id.clone(),
            ),
            (
                "vortex_differential_preparation_refined_prepared_state_digest".to_string(),
                self.refined_prepared_state_digest.clone(),
            ),
            (
                "vortex_differential_preparation_overlay_consumer_family".to_string(),
                self.overlay_consumer_family.clone(),
            ),
            (
                "vortex_differential_preparation_overlay_consumer_status".to_string(),
                self.overlay_consumer_status.clone(),
            ),
            (
                "vortex_differential_preparation_overlay_consumer_correctness_digest".to_string(),
                self.overlay_consumer_correctness_digest.clone(),
            ),
            (
                "vortex_differential_preparation_refinement_fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "vortex_differential_preparation_refinement_external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
        ]
    }
}

/// Return the artifact-adjacent differential refinement manifest path.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the artifact path has no
/// local file name.
pub fn vortex_differential_refinement_manifest_path(
    prepared_artifact_path: impl AsRef<Path>,
) -> Result<PathBuf> {
    let path = absolute_local_path(prepared_artifact_path.as_ref())?;
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "differential refinement manifest requires a local artifact file name for '{}'; no fallback execution was attempted",
            path.display()
        )));
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent
        .join(".shardloom")
        .join(format!("{file_name}.differential-refinement.manifest")))
}

/// Evaluate whether a prepared-state reuse miss can become an automatic
/// append-only refinement instead of a base reprepare.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the manifest exists but
/// cannot be read or parsed deterministically.
#[allow(clippy::too_many_lines)]
pub fn evaluate_vortex_prepared_state_append_only_refinement(
    request: &VortexPreparedStateReuseRequest,
) -> Result<VortexPreparedStateAppendOnlyRefinementDecision> {
    if !request.manifest_path.exists() {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_missing_base_manifest",
            "missing_base_manifest",
            "none",
            &BTreeMap::new(),
        ));
    }
    let fields = read_reuse_manifest_fields(&request.manifest_path)?;
    let manifest_digest = fields
        .get("manifest_digest")
        .cloned()
        .unwrap_or_else(|| "none".to_string());
    let expected_digest = reuse_manifest_digest(&fields);
    if manifest_digest != expected_digest {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_reuse_manifest_digest_mismatch",
            "reuse_manifest_digest_mismatch",
            &manifest_digest,
            &fields,
        ));
    }
    if fields.get("schema_version").map(String::as_str)
        != Some(VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION)
    {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_reuse_manifest_schema_mismatch",
            "reuse_manifest_schema_mismatch",
            &manifest_digest,
            &fields,
        ));
    }
    if fields.get("fallback_attempted").map(String::as_str) != Some("false") {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_reuse_manifest_fallback_attempted",
            "reuse_manifest_fallback_attempted",
            &manifest_digest,
            &fields,
        ));
    }
    if fields.get("external_engine_invoked").map(String::as_str) != Some("false") {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_reuse_manifest_external_engine_invoked",
            "reuse_manifest_external_engine_invoked",
            &manifest_digest,
            &fields,
        ));
    }
    if let Some(reason) = append_only_refinement_static_mismatch_reason(request, &fields) {
        return Ok(append_only_refinement_blocked(
            request,
            &format!("blocked_{reason}"),
            &reason,
            &manifest_digest,
            &fields,
        ));
    }
    if !matches!(request.source_format.as_str(), "csv" | "jsonl") {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_changed_compression_or_format_posture",
            "changed_compression_or_format_posture",
            &manifest_digest,
            &fields,
        ));
    }

    let Some(base_source_digest) = fields.get("source_content_digest").cloned() else {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_missing_base_source_content_digest",
            "missing_base_source_content_digest",
            &manifest_digest,
            &fields,
        ));
    };
    let Some(base_source_size_bytes) = fields
        .get("source_size_bytes")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_missing_base_source_size_bytes",
            "missing_base_source_size_bytes",
            &manifest_digest,
            &fields,
        ));
    };
    if request.source_size_bytes <= base_source_size_bytes {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_current_source_not_larger",
            "current_source_not_larger",
            &manifest_digest,
            &fields,
        ));
    }
    if request.source_content_digest == base_source_digest {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_no_source_change",
            "no_source_change",
            &manifest_digest,
            &fields,
        ));
    }
    let prefix_digest = fnv64_file_prefix_digest(
        &request.source_path,
        base_source_size_bytes,
        "append-only refinement source prefix",
    )?;
    if prefix_digest != base_source_digest {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_source_prefix_digest_mismatch",
            "source_prefix_digest_mismatch",
            &manifest_digest,
            &fields,
        ));
    }
    let previous_terminal_byte = byte_at_offset(
        &request.source_path,
        base_source_size_bytes.saturating_sub(1),
        "append-only refinement source boundary",
    )?;
    if !matches!(previous_terminal_byte, b'\n' | b'\r') {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_append_boundary_not_line_aligned",
            "append_boundary_not_line_aligned",
            &manifest_digest,
            &fields,
        ));
    }
    if missing_refinement_manifest_field(&fields, "source_row_count")
        || missing_refinement_manifest_field(&fields, "source_column_family_summary")
        || missing_refinement_manifest_field(&fields, "source_schema_digest")
    {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_missing_base_refinement_manifest_fields",
            "missing_base_refinement_manifest_fields",
            &manifest_digest,
            &fields,
        ));
    }
    let Ok(artifact_fingerprint) = LocalReuseFileFingerprint::from_path(
        &request.prepared_artifact_path,
        "base prepared-state refinement artifact",
    ) else {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_missing_base_prepared_artifact",
            "missing_base_prepared_artifact",
            &manifest_digest,
            &fields,
        ));
    };
    if fields
        .get("prepared_artifact_size_bytes")
        .map(String::as_str)
        != Some(artifact_fingerprint.size_bytes.to_string().as_str())
    {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_base_prepared_artifact_size_changed",
            "base_prepared_artifact_size_changed",
            &manifest_digest,
            &fields,
        ));
    }
    if fields.get("prepared_artifact_digest").map(String::as_str)
        != Some(artifact_fingerprint.content_digest.as_str())
    {
        return Ok(append_only_refinement_blocked(
            request,
            "blocked_base_prepared_artifact_digest_changed",
            "base_prepared_artifact_digest_changed",
            &manifest_digest,
            &fields,
        ));
    }

    let delta_byte_start = base_source_size_bytes;
    let delta_byte_end = request.source_size_bytes;
    let changed_byte_range_refs = format!(
        "{}#bytes={}..{}",
        request.source_path.display(),
        delta_byte_start,
        delta_byte_end
    );
    Ok(VortexPreparedStateAppendOnlyRefinementDecision {
        schema_version: VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION,
        status: "admitted_append_only_refinement".to_string(),
        policy: VORTEX_DIFFERENTIAL_REFINEMENT_POLICY,
        reason: "source_prefix_verified_and_delta_bytes_detected".to_string(),
        blocker_id: "none".to_string(),
        automatic_detection_status: "append_only_delta_detected".to_string(),
        manifest_path: request.manifest_path.clone(),
        reuse_manifest_digest: manifest_digest,
        source_path: request.source_path.clone(),
        source_format: request.source_format.clone(),
        base_source_content_digest: base_source_digest,
        current_source_content_digest: request.source_content_digest.clone(),
        base_source_size_bytes,
        current_source_size_bytes: request.source_size_bytes,
        delta_byte_start,
        delta_byte_end,
        changed_byte_range_refs,
        source_prefix_verification_status: "verified_old_source_bytes_are_current_prefix"
            .to_string(),
        base_source_state_id: fields.get("source_state_id").cloned().unwrap_or_default(),
        base_source_state_digest: fields
            .get("source_state_digest")
            .cloned()
            .unwrap_or_default(),
        base_source_row_count: fields
            .get("source_row_count")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        base_source_schema_digest: fields
            .get("source_schema_digest")
            .cloned()
            .unwrap_or_default(),
        base_source_column_family_summary: fields
            .get("source_column_family_summary")
            .cloned()
            .unwrap_or_default(),
        base_prepared_state_id: fields.get("prepared_state_id").cloned().unwrap_or_default(),
        base_prepared_state_digest: fields
            .get("prepared_state_digest")
            .cloned()
            .unwrap_or_default(),
        prepared_artifact_ref: request.prepared_artifact_path.clone(),
        prepared_artifact_digest: artifact_fingerprint.content_digest,
        prepared_artifact_size_bytes: artifact_fingerprint.size_bytes,
        certificate_refs: fields
            .get("certificate_refs")
            .cloned()
            .unwrap_or_else(|| "manifest_certificate_refs_missing".to_string()),
        fallback_attempted: false,
        external_engine_invoked: false,
    })
}

/// Write a digest-backed automatic differential refinement manifest.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the manifest path cannot
/// be written atomically.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn write_vortex_differential_refinement_manifest(
    manifest_path: impl AsRef<Path>,
    decision: &VortexPreparedStateAppendOnlyRefinementDecision,
    differential: &VortexDifferentialPreparationReport,
    refined_prepared_state_id: impl Into<String>,
    refined_prepared_state_digest: impl Into<String>,
    overlay_consumer_family: impl Into<String>,
    overlay_consumer_status: impl Into<String>,
    overlay_consumer_correctness_digest: impl Into<String>,
) -> Result<VortexDifferentialRefinementManifestReport> {
    let manifest_path = absolute_local_path(manifest_path.as_ref())?;
    let refined_prepared_state_id = refined_prepared_state_id.into();
    let refined_prepared_state_digest = refined_prepared_state_digest.into();
    let overlay_consumer_family = overlay_consumer_family.into();
    let overlay_consumer_status = overlay_consumer_status.into();
    let overlay_consumer_correctness_digest = overlay_consumer_correctness_digest.into();
    let mut fields = BTreeMap::new();
    fields.insert(
        "schema_version".to_string(),
        VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION.to_string(),
    );
    fields.insert(
        "policy".to_string(),
        VORTEX_DIFFERENTIAL_REFINEMENT_POLICY.to_string(),
    );
    fields.insert("status".to_string(), differential.status.clone());
    fields.insert(
        "automatic_detection_status".to_string(),
        decision.automatic_detection_status.clone(),
    );
    fields.insert(
        "base_source_state_id".to_string(),
        differential.base_source_state_id.clone(),
    );
    fields.insert(
        "base_source_state_digest".to_string(),
        differential.base_source_state_digest.clone(),
    );
    fields.insert(
        "base_prepared_state_id".to_string(),
        differential.base_prepared_state_id.clone(),
    );
    fields.insert(
        "base_prepared_state_digest".to_string(),
        differential.base_prepared_state_digest.clone(),
    );
    fields.insert(
        "delta_source_state_id".to_string(),
        differential.delta_source_state_id.clone(),
    );
    fields.insert(
        "delta_source_state_digest".to_string(),
        differential.delta_source_state_digest.clone(),
    );
    fields.insert(
        "base_row_count".to_string(),
        differential.base_row_count.to_string(),
    );
    fields.insert(
        "delta_row_count".to_string(),
        differential.delta_row_count.to_string(),
    );
    fields.insert(
        "delta_manifest_digest".to_string(),
        differential.delta_manifest_digest.clone(),
    );
    fields.insert(
        "overlay_manifest_digest".to_string(),
        differential.overlay_manifest_digest.clone(),
    );
    fields.insert(
        "changed_byte_range_refs".to_string(),
        differential.changed_byte_range_refs.clone(),
    );
    fields.insert(
        "changed_row_range_refs".to_string(),
        differential.changed_row_range_refs.clone(),
    );
    fields.insert(
        "changed_segment_refs".to_string(),
        differential.changed_segment_refs.clone(),
    );
    fields.insert(
        "delta_artifact_ref".to_string(),
        differential.delta_artifact_ref.clone(),
    );
    fields.insert(
        "delta_artifact_digest".to_string(),
        differential.delta_artifact_digest.clone(),
    );
    fields.insert(
        "refined_prepared_state_id".to_string(),
        refined_prepared_state_id.clone(),
    );
    fields.insert(
        "refined_prepared_state_digest".to_string(),
        refined_prepared_state_digest.clone(),
    );
    fields.insert(
        "overlay_consumer_family".to_string(),
        overlay_consumer_family.clone(),
    );
    fields.insert(
        "overlay_consumer_status".to_string(),
        overlay_consumer_status.clone(),
    );
    fields.insert(
        "overlay_consumer_correctness_digest".to_string(),
        overlay_consumer_correctness_digest.clone(),
    );
    fields.insert(
        "fallback_attempted".to_string(),
        differential.fallback_attempted.to_string(),
    );
    fields.insert(
        "external_engine_invoked".to_string(),
        differential.external_engine_invoked.to_string(),
    );
    let manifest_digest = reuse_manifest_digest(&fields);
    fields.insert("manifest_digest".to_string(), manifest_digest.clone());
    write_key_value_manifest_fields(
        &manifest_path,
        &fields,
        "ShardLoom differential refinement manifest",
        "differential refinement manifest",
    )?;
    Ok(VortexDifferentialRefinementManifestReport {
        schema_version: VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION,
        manifest_path,
        manifest_digest,
        manifest_written: true,
        refined_prepared_state_id,
        refined_prepared_state_digest,
        overlay_consumer_family,
        overlay_consumer_status,
        overlay_consumer_correctness_digest,
        fallback_attempted: differential.fallback_attempted,
        external_engine_invoked: differential.external_engine_invoked,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalReuseFileFingerprint {
    path: PathBuf,
    size_bytes: u64,
    mtime_ns: String,
    content_digest: String,
}

impl LocalReuseFileFingerprint {
    fn from_path(path: &Path, label: &str) -> Result<Self> {
        let path = absolute_local_path(path)?;
        let metadata = fs::metadata(&path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to stat {label} '{}': {error}; no fallback execution was attempted",
                path.display()
            ))
        })?;
        if !metadata.is_file() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{label} '{}' must be a local file for prepared-state reuse; no fallback execution was attempted",
                path.display()
            )));
        }
        let modified = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map_or_else(
                || "unknown".to_string(),
                |value| value.as_nanos().to_string(),
            );
        Ok(Self {
            content_digest: fnv64_file_digest(&path, label)?,
            path,
            size_bytes: metadata.len(),
            mtime_ns: modified,
        })
    }
}

fn absolute_local_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "prepared-state reuse local path must not be empty; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to resolve prepared-state reuse path '{}': {error}; no fallback execution was attempted",
                    path.display()
                ))
            })
    }
}

fn fnv64_file_digest(path: &Path, label: &str) -> Result<String> {
    let mut file = fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open {label} '{}' for prepared-state reuse digest: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {label} '{}' for prepared-state reuse digest: {error}; no fallback execution was attempted",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        for byte in &buffer[..read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    Ok(format!("fnv64:{hash:016x}"))
}

fn fnv64_file_prefix_digest(path: &Path, prefix_len: u64, label: &str) -> Result<String> {
    let mut file = fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open {label} '{}' for digest: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut buffer = [0_u8; 8192];
    let mut remaining = prefix_len;
    while remaining > 0 {
        let to_read = usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| {
            ShardLoomError::InvalidOperation(
                "append-only refinement prefix length does not fit usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let read = file.read(&mut buffer[..to_read]).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read {label} '{}' for digest: {error}; no fallback execution was attempted",
                path.display()
            ))
        })?;
        if read == 0 {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{label} '{}' ended before expected prefix length {prefix_len}; no fallback execution was attempted",
                path.display()
            )));
        }
        remaining -= u64::try_from(read).map_err(|_| {
            ShardLoomError::InvalidOperation(
                "append-only refinement read length does not fit u64; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        for byte in &buffer[..read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    Ok(format!("fnv64:{hash:016x}"))
}

fn byte_at_offset(path: &Path, offset: u64, label: &str) -> Result<u8> {
    use std::io::{Seek as _, SeekFrom};

    let mut file = fs::File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open {label} '{}' for boundary check: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to seek {label} '{}' to byte {offset}: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read {label} '{}' at byte {offset}: {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    Ok(byte[0])
}

fn append_only_refinement_static_mismatch_reason(
    request: &VortexPreparedStateReuseRequest,
    fields: &BTreeMap<String, String>,
) -> Option<String> {
    let source_path = request.source_path.display().to_string();
    let prepared_artifact_path = request.prepared_artifact_path.display().to_string();
    for (key, expected) in [
        ("policy", VORTEX_PREPARED_STATE_REUSE_POLICY),
        ("source_path", source_path.as_str()),
        ("source_format", request.source_format.as_str()),
        (
            "parse_decode_plan_digest",
            request.parse_decode_plan_digest.as_str(),
        ),
        ("selected_columns", request.selected_columns.as_str()),
        ("output_policy", request.output_policy.as_str()),
        ("prepared_artifact_path", prepared_artifact_path.as_str()),
        ("provider_version", request.provider_version.as_str()),
        ("feature_gates", request.feature_gates.as_str()),
        ("certification_level", request.certification_level.as_str()),
    ] {
        if fields.get(key).map(String::as_str) != Some(expected) {
            return Some(format!("{key}_changed"));
        }
    }
    if let Some(schema_digest) = request.source_schema_digest.as_deref() {
        if fields.get("source_schema_digest").map(String::as_str) != Some(schema_digest) {
            return Some("source_schema_digest_changed".to_string());
        }
    }
    None
}

fn missing_refinement_manifest_field(fields: &BTreeMap<String, String>, key: &str) -> bool {
    fields.get(key).is_none_or(|value| value.trim().is_empty())
}

fn append_only_refinement_blocked(
    request: &VortexPreparedStateReuseRequest,
    status: &str,
    reason: &str,
    manifest_digest: &str,
    fields: &BTreeMap<String, String>,
) -> VortexPreparedStateAppendOnlyRefinementDecision {
    let base_source_size_bytes = fields
        .get("source_size_bytes")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    VortexPreparedStateAppendOnlyRefinementDecision {
        schema_version: VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION,
        status: status.to_string(),
        policy: VORTEX_DIFFERENTIAL_REFINEMENT_POLICY,
        reason: reason.to_string(),
        blocker_id: reason.to_string(),
        automatic_detection_status: "blocked_before_append_only_delta_detection".to_string(),
        manifest_path: request.manifest_path.clone(),
        reuse_manifest_digest: manifest_digest.to_string(),
        source_path: request.source_path.clone(),
        source_format: request.source_format.clone(),
        base_source_content_digest: fields
            .get("source_content_digest")
            .cloned()
            .unwrap_or_else(|| "none".to_string()),
        current_source_content_digest: request.source_content_digest.clone(),
        base_source_size_bytes,
        current_source_size_bytes: request.source_size_bytes,
        delta_byte_start: base_source_size_bytes,
        delta_byte_end: request.source_size_bytes,
        changed_byte_range_refs: "none".to_string(),
        source_prefix_verification_status: "not_verified".to_string(),
        base_source_state_id: fields.get("source_state_id").cloned().unwrap_or_default(),
        base_source_state_digest: fields
            .get("source_state_digest")
            .cloned()
            .unwrap_or_default(),
        base_source_row_count: fields
            .get("source_row_count")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        base_source_schema_digest: fields
            .get("source_schema_digest")
            .cloned()
            .unwrap_or_default(),
        base_source_column_family_summary: fields
            .get("source_column_family_summary")
            .cloned()
            .unwrap_or_default(),
        base_prepared_state_id: fields.get("prepared_state_id").cloned().unwrap_or_default(),
        base_prepared_state_digest: fields
            .get("prepared_state_digest")
            .cloned()
            .unwrap_or_default(),
        prepared_artifact_ref: request.prepared_artifact_path.clone(),
        prepared_artifact_digest: fields
            .get("prepared_artifact_digest")
            .cloned()
            .unwrap_or_else(|| "none".to_string()),
        prepared_artifact_size_bytes: fields
            .get("prepared_artifact_size_bytes")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0),
        certificate_refs: fields.get("certificate_refs").cloned().unwrap_or_default(),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn prepared_state_reuse_miss(
    request: &VortexPreparedStateReuseRequest,
    reason: &str,
    manifest_digest: &str,
) -> VortexPreparedStateReuseReport {
    prepared_state_reuse_miss_with_digest(request, reason, manifest_digest)
}

fn prepared_state_reuse_miss_with_digest(
    request: &VortexPreparedStateReuseRequest,
    reason: &str,
    manifest_digest: &str,
) -> VortexPreparedStateReuseReport {
    VortexPreparedStateReuseReport {
        schema_version: VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION,
        status: "prepared_state_reuse_miss".to_string(),
        scope: "artifact_adjacent_manifest_local_vortex_artifacts".to_string(),
        manifest_path: request.manifest_path.clone(),
        policy: VORTEX_PREPARED_STATE_REUSE_POLICY,
        hit: false,
        reason: reason.to_string(),
        manifest_digest: manifest_digest.to_string(),
        invalidation_reason: reason.to_string(),
        manifest_written: false,
        source_path: request.source_path.clone(),
        source_format: request.source_format.clone(),
        source_content_digest: request.source_content_digest.clone(),
        source_size_bytes: request.source_size_bytes,
        source_mtime_ns: request.source_mtime_ns.clone(),
        source_schema_digest: request.source_schema_digest.clone().unwrap_or_default(),
        source_row_count: 0,
        source_column_family_summary: String::new(),
        parse_decode_plan_digest: request.parse_decode_plan_digest.clone(),
        selected_columns: request.selected_columns.clone(),
        output_policy: request.output_policy.clone(),
        prepared_artifact_ref: request.prepared_artifact_path.clone(),
        prepared_artifact_digest: "none".to_string(),
        prepared_artifact_size_bytes: 0,
        provider_version: request.provider_version.clone(),
        feature_gates: request.feature_gates.clone(),
        certification_level: request.certification_level.clone(),
        source_state_id: String::new(),
        source_state_digest: String::new(),
        prepared_state_id: String::new(),
        prepared_state_digest: String::new(),
        certificate_refs: String::new(),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

fn reuse_manifest_request_mismatch_reason(
    request: &VortexPreparedStateReuseRequest,
    fields: &BTreeMap<String, String>,
) -> Option<String> {
    let source_size_bytes = request.source_size_bytes.to_string();
    let source_path = request.source_path.display().to_string();
    let prepared_artifact_path = request.prepared_artifact_path.display().to_string();
    for (key, expected) in [
        ("policy", VORTEX_PREPARED_STATE_REUSE_POLICY),
        ("source_path", source_path.as_str()),
        ("source_format", request.source_format.as_str()),
        (
            "source_content_digest",
            request.source_content_digest.as_str(),
        ),
        ("source_size_bytes", source_size_bytes.as_str()),
        (
            "parse_decode_plan_digest",
            request.parse_decode_plan_digest.as_str(),
        ),
        ("selected_columns", request.selected_columns.as_str()),
        ("output_policy", request.output_policy.as_str()),
        ("prepared_artifact_path", prepared_artifact_path.as_str()),
        ("provider_version", request.provider_version.as_str()),
        ("feature_gates", request.feature_gates.as_str()),
        ("certification_level", request.certification_level.as_str()),
    ] {
        if fields.get(key).map(String::as_str) != Some(expected) {
            return Some(format!("{key}_changed"));
        }
    }
    if let Some(schema_digest) = request.source_schema_digest.as_deref() {
        if fields.get("source_schema_digest").map(String::as_str) != Some(schema_digest) {
            return Some("source_schema_digest_changed".to_string());
        }
    }
    None
}

fn read_reuse_manifest_fields(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = fs::read_to_string(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read prepared-state reuse manifest '{}': {error}; no fallback execution was attempted",
            path.display()
        ))
    })?;
    let mut fields = BTreeMap::new();
    for (line_index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "prepared-state reuse manifest '{}' line {} is not key=value; no fallback execution was attempted",
                path.display(),
                line_index + 1
            )));
        };
        if key.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "prepared-state reuse manifest '{}' line {} has an empty key; no fallback execution was attempted",
                path.display(),
                line_index + 1
            )));
        }
        fields.insert(
            key.trim().to_string(),
            unescape_manifest_value(value.trim())?,
        );
    }
    Ok(fields)
}

fn write_reuse_manifest_fields(path: &Path, fields: &BTreeMap<String, String>) -> Result<()> {
    write_key_value_manifest_fields(
        path,
        fields,
        "ShardLoom prepared-state reuse manifest",
        "prepared-state reuse manifest",
    )
}

fn write_key_value_manifest_fields(
    path: &Path,
    fields: &BTreeMap<String, String>,
    title: &str,
    error_label: &str,
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "{error_label} path '{}' has no parent directory; no fallback execution was attempted",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create {error_label} directory '{}': {error}; no fallback execution was attempted",
            parent.display()
        ))
    })?;
    let tmp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create {error_label} temp file '{}': {error}; no fallback execution was attempted",
            tmp_path.display()
        ))
    })?;
    writeln!(file, "# {title}").map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write {error_label} '{}': {error}; no fallback execution was attempted",
            tmp_path.display()
        ))
    })?;
    for (key, value) in fields {
        writeln!(file, "{key}={}", escape_manifest_value(value)).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to write {error_label} '{}': {error}; no fallback execution was attempted",
                tmp_path.display()
            ))
        })?;
    }
    file.sync_all().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to sync {error_label} '{}': {error}; no fallback execution was attempted",
            tmp_path.display()
        ))
    })?;
    drop(file);
    fs::rename(&tmp_path, path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to publish {error_label} '{}' to '{}': {error}; no fallback execution was attempted",
            tmp_path.display(),
            path.display()
        ))
    })
}

fn reuse_manifest_digest(fields: &BTreeMap<String, String>) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (key, value) in fields
        .iter()
        .filter(|(key, _)| key.as_str() != "manifest_digest")
    {
        for byte in key
            .as_bytes()
            .iter()
            .chain(b"=")
            .chain(value.as_bytes())
            .chain(b"\0")
        {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("fnv64:{hash:016x}")
}

fn escape_manifest_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '%' => escaped.push_str("%25"),
            '\n' => escaped.push_str("%0A"),
            '\r' => escaped.push_str("%0D"),
            '=' => escaped.push_str("%3D"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn unescape_manifest_value(value: &str) -> Result<String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            output.push(ch);
            continue;
        }
        let hi = chars.next().ok_or_else(invalid_manifest_escape)?;
        let lo = chars.next().ok_or_else(invalid_manifest_escape)?;
        match (hi, lo) {
            ('2', '5') => output.push('%'),
            ('0', 'A' | 'a') => output.push('\n'),
            ('0', 'D' | 'd') => output.push('\r'),
            ('3', 'D' | 'd') => output.push('='),
            _ => return Err(invalid_manifest_escape()),
        }
    }
    Ok(output)
}

fn invalid_manifest_escape() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "prepared-state reuse manifest contains an invalid percent escape; no fallback execution was attempted"
            .to_string(),
    )
}

/// Request to write one flat scalar local source into a local Vortex artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateWriteRequest {
    pub target_path: PathBuf,
    pub columns: Vec<String>,
    pub column_dtypes: Vec<Option<LogicalDType>>,
    pub rows: Vec<Vec<(String, ScalarValue)>>,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
    pub layout_write_advisor: Option<VortexLayoutWriteAdvisorReport>,
    pub capillary_prewrite_input: Option<VortexCapillaryPreparationInput>,
}

/// Request to write one flat columnar local source into a Vortex artifact.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateColumnarWriteRequest {
    pub target_path: PathBuf,
    pub source: FlatLocalColumnarSource,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
    pub layout_write_advisor: Option<VortexLayoutWriteAdvisorReport>,
    pub capillary_prewrite_input: Option<VortexCapillaryPreparationInput>,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl VortexPreparedStateColumnarWriteRequest {
    /// Create a request for a columnar local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(target_path: impl Into<PathBuf>, source: FlatLocalColumnarSource) -> Self {
        Self {
            target_path: target_path.into(),
            source,
            allow_overwrite: false,
            certification_level: VortexIngestCertificationLevel::IngestCertified,
            layout_write_advisor: None,
            capillary_prewrite_input: None,
        }
    }

    /// Allow overwriting an existing local target artifact.
    #[must_use]
    pub const fn allow_overwrite(mut self, allow_overwrite: bool) -> Self {
        self.allow_overwrite = allow_overwrite;
        self
    }

    /// Set the requested ingest certification depth.
    #[must_use]
    pub const fn certification_level(
        mut self,
        certification_level: VortexIngestCertificationLevel,
    ) -> Self {
        self.certification_level = certification_level;
        self
    }

    /// Attach pre-write capillary control input for the local cold-preparation route.
    #[must_use]
    pub fn capillary_prewrite_input(mut self, input: VortexCapillaryPreparationInput) -> Self {
        self.capillary_prewrite_input = Some(input);
        self
    }

    /// Attach the caller's layout/write advisor decision so the writer can
    /// fail closed before applying unsupported write behavior.
    #[must_use]
    pub fn layout_write_advisor(mut self, report: VortexLayoutWriteAdvisorReport) -> Self {
        self.layout_write_advisor = Some(report);
        self
    }
}

impl VortexPreparedStateWriteRequest {
    /// Create a request for a local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(
        target_path: impl Into<PathBuf>,
        columns: Vec<String>,
        rows: Vec<Vec<(String, ScalarValue)>>,
    ) -> Self {
        let column_dtypes = vec![None; columns.len()];
        Self {
            target_path: target_path.into(),
            columns,
            column_dtypes,
            rows,
            allow_overwrite: false,
            certification_level: VortexIngestCertificationLevel::IngestCertified,
            layout_write_advisor: None,
            capillary_prewrite_input: None,
        }
    }

    /// Attach logical dtype hints for flat scalar output columns.
    #[must_use]
    pub fn column_dtypes(mut self, column_dtypes: Vec<Option<LogicalDType>>) -> Self {
        self.column_dtypes = column_dtypes;
        self
    }

    /// Allow overwriting an existing local target artifact.
    #[must_use]
    pub const fn allow_overwrite(mut self, allow_overwrite: bool) -> Self {
        self.allow_overwrite = allow_overwrite;
        self
    }

    /// Set the requested ingest certification depth.
    #[must_use]
    pub const fn certification_level(
        mut self,
        certification_level: VortexIngestCertificationLevel,
    ) -> Self {
        self.certification_level = certification_level;
        self
    }

    /// Attach pre-write capillary control input for the local cold-preparation route.
    #[must_use]
    pub fn capillary_prewrite_input(mut self, input: VortexCapillaryPreparationInput) -> Self {
        self.capillary_prewrite_input = Some(input);
        self
    }

    /// Attach the caller's layout/write advisor decision so the writer can
    /// fail closed before applying unsupported write behavior.
    #[must_use]
    pub fn layout_write_advisor(mut self, report: VortexLayoutWriteAdvisorReport) -> Self {
        self.layout_write_advisor = Some(report);
        self
    }
}

/// Certification depth for the scoped local `vortex_ingest` lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexIngestCertificationLevel {
    /// Write a local artifact and report bytes/digest/writer evidence only.
    IngestMinimal,
    /// Write and reopen/scan the artifact for row-count proof.
    IngestCertified,
    /// Requires downstream result replay/output evidence, so this prepare-only helper blocks it.
    IngestFullReplay,
}

impl VortexIngestCertificationLevel {
    /// Parse a command/API certification-depth token.
    ///
    /// # Errors
    /// Returns an error when the value is not one of the admitted certification
    /// depth tokens.
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "ingest_minimal" => Ok(Self::IngestMinimal),
            "ingest_certified" => Ok(Self::IngestCertified),
            "ingest_full_replay" => Ok(Self::IngestFullReplay),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown vortex_ingest certification level '{other}'; expected ingest_minimal, ingest_certified, or ingest_full_replay; no fallback execution was attempted"
            ))),
        }
    }

    /// Return the canonical evidence token for this certification depth.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IngestMinimal => "ingest_minimal",
            Self::IngestCertified => "ingest_certified",
            Self::IngestFullReplay => "ingest_full_replay",
        }
    }
}

/// Source/sink/split evidence for the scoped local Vortex preparation spine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPreparationSpineReport {
    pub schema_version: &'static str,
    pub status: String,
    pub vortex_first_decision: String,
    pub provider_kind: String,
    pub provider_crate: String,
    pub provider_version: String,
    pub feature_gate: String,
    pub provider_api_surface: String,
    pub shardloom_admission_policy: String,
    pub source_surface: String,
    pub sink_surface: String,
    pub split_surface: String,
    pub split_ref_status: String,
    pub split_count: usize,
    pub projection_mask_status: String,
    pub filter_mask_status: String,
    pub write_provider_surface: String,
    pub reopen_provider_surface: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub native_io_certificate_status: String,
    pub native_io_certificate_refs: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexPreparationSpineReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(26);
        self.append_provider_evidence_fields(&mut fields);
        self.append_surface_evidence_fields(&mut fields);
        self.append_boundary_evidence_fields(&mut fields);
        fields.push((
            "vortex_preparation_spine_fallback_attempted".to_string(),
            self.fallback_attempted.to_string(),
        ));
        fields.push((
            "vortex_preparation_spine_external_engine_invoked".to_string(),
            self.external_engine_invoked.to_string(),
        ));
        fields
    }

    fn push_evidence_field(
        fields: &mut Vec<(String, String)>,
        key: &'static str,
        value: impl Into<String>,
    ) {
        fields.push((key.to_string(), value.into()));
    }

    fn append_provider_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_schema_version",
            self.schema_version,
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_status",
            self.status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_vortex_first_decision",
            self.vortex_first_decision.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_provider_kind",
            self.provider_kind.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_provider_crate",
            self.provider_crate.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_provider_version",
            self.provider_version.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_feature_gate",
            self.feature_gate.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_provider_api_surface",
            self.provider_api_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_shardloom_admission_policy",
            self.shardloom_admission_policy.as_str(),
        );
    }

    fn append_surface_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_source_surface",
            self.source_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_sink_surface",
            self.sink_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_split_surface",
            self.split_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_split_ref_status",
            self.split_ref_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_split_count",
            self.split_count.to_string(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_projection_mask_status",
            self.projection_mask_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_filter_mask_status",
            self.filter_mask_status.as_str(),
        );
    }

    fn append_boundary_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_write_provider_surface",
            self.write_provider_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_reopen_provider_surface",
            self.reopen_provider_surface.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_materialization_boundary_status",
            self.materialization_boundary_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_decode_boundary_status",
            self.decode_boundary_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_native_io_certificate_status",
            self.native_io_certificate_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_native_io_certificate_refs",
            self.native_io_certificate_refs.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_claim_gate_status",
            self.claim_gate_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_preparation_spine_claim_boundary",
            self.claim_boundary.as_str(),
        );
    }
}

/// Delta update modes accepted or blocked by scoped differential preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexDifferentialUpdateMode {
    /// Append-only delta rows may be overlaid when base and delta schemas match.
    AppendOnly,
    /// In-place updates remain blocked until row identity and rewrite semantics are certified.
    Update,
    /// Deletes remain blocked until tombstone semantics and replay are certified.
    Delete,
    /// Mixed insert/update semantics remain blocked until conflict handling is certified.
    Upsert,
}

impl VortexDifferentialUpdateMode {
    /// Parse a differential-preparation update-mode token.
    ///
    /// # Errors
    /// Returns an error when the token is not one of the deterministic update-mode values.
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "append-only" | "append_only" => Ok(Self::AppendOnly),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            "upsert" => Ok(Self::Upsert),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown vortex_ingest differential update mode '{other}'; expected append-only, update, delete, or upsert; no fallback execution was attempted"
            ))),
        }
    }

    /// Return the canonical evidence token for this update mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AppendOnly => "append_only",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Upsert => "upsert",
        }
    }

    const fn is_append_only(self) -> bool {
        matches!(self, Self::AppendOnly)
    }
}

/// Inputs used to validate a scoped local differential Vortex preparation overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexDifferentialPreparationInput {
    pub update_mode: VortexDifferentialUpdateMode,
    pub base_source_state_id: String,
    pub base_source_state_digest: String,
    pub base_prepared_state_id: String,
    pub base_prepared_state_digest: String,
    pub base_row_count: u64,
    pub base_schema_digest: String,
    pub base_column_family_summary: String,
    pub delta_source_state_id: String,
    pub delta_source_state_digest: String,
    pub delta_row_count: u64,
    pub delta_schema_digest: String,
    pub delta_column_family_summary: String,
    pub delta_manifest_digest: String,
    pub changed_byte_range_refs: String,
    pub changed_row_range_refs: String,
    pub changed_segment_refs: String,
    pub delta_artifact_ref: String,
    pub delta_artifact_digest: String,
    pub native_io_certificate_refs: String,
}

/// Evidence for a scoped local differential Vortex preparation overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexDifferentialPreparationReport {
    pub schema_version: &'static str,
    pub status: String,
    pub update_mode: VortexDifferentialUpdateMode,
    pub base_source_state_id: String,
    pub base_source_state_digest: String,
    pub base_prepared_state_id: String,
    pub base_prepared_state_digest: String,
    pub base_row_count: u64,
    pub delta_source_state_id: String,
    pub delta_source_state_digest: String,
    pub delta_row_count: u64,
    pub delta_manifest_digest: String,
    pub overlay_manifest_digest: String,
    pub changed_byte_range_refs: String,
    pub changed_row_range_refs: String,
    pub changed_segment_refs: String,
    pub schema_compatibility_status: String,
    pub update_mode_policy: String,
    pub tombstone_policy: String,
    pub delete_policy: String,
    pub update_policy: String,
    pub prepared_state_reuse_status: String,
    pub base_reprepare_performed: bool,
    pub delta_artifact_written: bool,
    pub delta_artifact_ref: String,
    pub delta_artifact_digest: String,
    pub overlay_applied: bool,
    pub replay_verification_status: String,
    pub correctness_digest: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub native_io_certificate_status: String,
    pub native_io_certificate_refs: String,
    pub no_standalone_lane_status: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexDifferentialPreparationReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(38);
        self.append_identity_evidence_fields(&mut fields);
        self.append_delta_manifest_evidence_fields(&mut fields);
        self.append_policy_evidence_fields(&mut fields);
        self.append_boundary_evidence_fields(&mut fields);
        Self::push_evidence_field(
            &mut fields,
            "vortex_differential_preparation_fallback_attempted",
            self.fallback_attempted.to_string(),
        );
        Self::push_evidence_field(
            &mut fields,
            "vortex_differential_preparation_external_engine_invoked",
            self.external_engine_invoked.to_string(),
        );
        fields
    }

    /// Whether this report admitted and applied the delta overlay.
    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        self.overlay_applied
    }

    fn push_evidence_field(
        fields: &mut Vec<(String, String)>,
        key: &'static str,
        value: impl Into<String>,
    ) {
        fields.push((key.to_string(), value.into()));
    }

    fn append_identity_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_schema_version",
            self.schema_version,
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_status",
            self.status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_update_mode",
            self.update_mode.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_source_state_id",
            self.base_source_state_id.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_source_state_digest",
            self.base_source_state_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_prepared_state_id",
            self.base_prepared_state_id.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_prepared_state_digest",
            self.base_prepared_state_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_row_count",
            self.base_row_count.to_string(),
        );
    }

    fn append_delta_manifest_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_source_state_id",
            self.delta_source_state_id.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_source_state_digest",
            self.delta_source_state_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_row_count",
            self.delta_row_count.to_string(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_manifest_digest",
            self.delta_manifest_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_overlay_manifest_digest",
            self.overlay_manifest_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_changed_byte_range_refs",
            self.changed_byte_range_refs.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_changed_row_range_refs",
            self.changed_row_range_refs.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_changed_segment_refs",
            self.changed_segment_refs.as_str(),
        );
    }

    fn append_policy_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_schema_compatibility_status",
            self.schema_compatibility_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_update_mode_policy",
            self.update_mode_policy.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_tombstone_policy",
            self.tombstone_policy.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delete_policy",
            self.delete_policy.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_update_policy",
            self.update_policy.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_prepared_state_reuse_status",
            self.prepared_state_reuse_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_base_reprepare_performed",
            self.base_reprepare_performed.to_string(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_artifact_written",
            self.delta_artifact_written.to_string(),
        );
    }

    fn append_boundary_evidence_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_artifact_ref",
            self.delta_artifact_ref.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_delta_artifact_digest",
            self.delta_artifact_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_overlay_applied",
            self.overlay_applied.to_string(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_replay_verification_status",
            self.replay_verification_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_correctness_digest",
            self.correctness_digest.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_materialization_boundary_status",
            self.materialization_boundary_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_decode_boundary_status",
            self.decode_boundary_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_native_io_certificate_status",
            self.native_io_certificate_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_native_io_certificate_refs",
            self.native_io_certificate_refs.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_no_standalone_lane_status",
            self.no_standalone_lane_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_claim_gate_status",
            self.claim_gate_status.as_str(),
        );
        Self::push_evidence_field(
            fields,
            "vortex_differential_preparation_claim_boundary",
            self.claim_boundary.as_str(),
        );
    }
}

struct DifferentialPreparationEvaluation {
    status: &'static str,
    schema_compatibility_status: &'static str,
    update_mode_policy: &'static str,
    prepared_state_reuse_status: &'static str,
    replay_verification_status: &'static str,
    native_io_certificate_status: &'static str,
    delta_artifact_written: bool,
    overlay_applied: bool,
}

fn evaluate_differential_preparation_state(
    input: &VortexDifferentialPreparationInput,
) -> DifferentialPreparationEvaluation {
    let base_identity_complete = !input.base_source_state_id.is_empty()
        && !input.base_source_state_digest.is_empty()
        && !input.base_prepared_state_id.is_empty()
        && !input.base_prepared_state_digest.is_empty();
    let schema_compatible = input.base_schema_digest == input.delta_schema_digest
        && input.base_column_family_summary == input.delta_column_family_summary;
    let delta_artifact_written =
        input.delta_row_count > 0 && input.delta_artifact_digest.starts_with("fnv64:");
    let overlay_applied = base_identity_complete
        && input.update_mode.is_append_only()
        && schema_compatible
        && delta_artifact_written;
    let status = if !base_identity_complete {
        "blocked_missing_base_identity"
    } else if !input.update_mode.is_append_only() {
        "blocked_update_mode_policy"
    } else if !schema_compatible {
        "blocked_schema_mismatch"
    } else if !delta_artifact_written {
        "blocked_empty_delta_manifest"
    } else {
        "admitted_append_only_delta_overlay"
    };

    DifferentialPreparationEvaluation {
        status,
        schema_compatibility_status: if schema_compatible {
            "compatible_source_schema_and_column_families"
        } else {
            "blocked_source_schema_or_column_family_mismatch"
        },
        update_mode_policy: if input.update_mode.is_append_only() {
            "append_only_overlay_admitted_when_fingerprints_match"
        } else {
            "update_delete_upsert_blocked_until_row_identity_and_rewrite_semantics_are_certified"
        },
        prepared_state_reuse_status: if overlay_applied {
            "base_prepared_state_reused_for_delta_overlay"
        } else {
            "blocked_before_prepared_state_reuse"
        },
        replay_verification_status: if overlay_applied {
            "delta_writer_and_reopen_row_count_verified"
        } else {
            "blocked_before_overlay_replay"
        },
        native_io_certificate_status: if overlay_applied {
            "certified_local_vortex_differential_preparation_overlay"
        } else {
            "blocked_before_differential_native_io_certificate"
        },
        delta_artifact_written,
        overlay_applied,
    }
}

fn differential_overlay_manifest_digest(
    input: &VortexDifferentialPreparationInput,
    status: &str,
) -> String {
    fnv64_digest_text(&format!(
        "{}|{}|{}|{}|{}|{}",
        input.base_prepared_state_digest,
        input.delta_manifest_digest,
        input.delta_artifact_digest,
        input.changed_row_range_refs,
        input.changed_segment_refs,
        status
    ))
}

fn differential_correctness_digest(
    input: &VortexDifferentialPreparationInput,
    overlay_manifest_digest: &str,
    replay_verification_status: &str,
) -> String {
    fnv64_digest_text(&format!(
        "{}|{}|{}|{}|{}|{}",
        input.base_source_state_digest,
        input.base_prepared_state_digest,
        input.delta_source_state_digest,
        input.delta_manifest_digest,
        overlay_manifest_digest,
        replay_verification_status
    ))
}

/// Validate and report a scoped local differential Vortex preparation overlay.
#[must_use]
pub fn evaluate_vortex_differential_preparation(
    input: VortexDifferentialPreparationInput,
) -> VortexDifferentialPreparationReport {
    let evaluation = evaluate_differential_preparation_state(&input);
    let overlay_manifest_digest = differential_overlay_manifest_digest(&input, evaluation.status);
    let correctness_digest = differential_correctness_digest(
        &input,
        &overlay_manifest_digest,
        evaluation.replay_verification_status,
    );

    VortexDifferentialPreparationReport {
        schema_version: VORTEX_DIFFERENTIAL_PREPARATION_SCHEMA_VERSION,
        status: evaluation.status.to_string(),
        update_mode: input.update_mode,
        base_source_state_id: input.base_source_state_id,
        base_source_state_digest: input.base_source_state_digest,
        base_prepared_state_id: input.base_prepared_state_id,
        base_prepared_state_digest: input.base_prepared_state_digest,
        base_row_count: input.base_row_count,
        delta_source_state_id: input.delta_source_state_id,
        delta_source_state_digest: input.delta_source_state_digest,
        delta_row_count: input.delta_row_count,
        delta_manifest_digest: input.delta_manifest_digest,
        overlay_manifest_digest,
        changed_byte_range_refs: input.changed_byte_range_refs,
        changed_row_range_refs: input.changed_row_range_refs,
        changed_segment_refs: input.changed_segment_refs,
        schema_compatibility_status: evaluation.schema_compatibility_status.to_string(),
        update_mode_policy: evaluation.update_mode_policy.to_string(),
        tombstone_policy: "tombstones_blocked_for_scoped_append_only_overlay".to_string(),
        delete_policy: "deletes_blocked_for_scoped_append_only_overlay".to_string(),
        update_policy: "updates_blocked_for_scoped_append_only_overlay".to_string(),
        prepared_state_reuse_status: evaluation.prepared_state_reuse_status.to_string(),
        base_reprepare_performed: false,
        delta_artifact_written: evaluation.delta_artifact_written,
        delta_artifact_ref: input.delta_artifact_ref,
        delta_artifact_digest: input.delta_artifact_digest,
        overlay_applied: evaluation.overlay_applied,
        replay_verification_status: evaluation.replay_verification_status.to_string(),
        correctness_digest,
        materialization_boundary_status: "delta_only_artifact_overlay_no_base_reprepare"
            .to_string(),
        decode_boundary_status: "delta_source_uses_existing_vortex_ingest_decode_boundary"
            .to_string(),
        native_io_certificate_status: evaluation.native_io_certificate_status.to_string(),
        native_io_certificate_refs: input.native_io_certificate_refs,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_prepared_state_delta_overlay"
                .to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: format!(
            "Scoped local differential preparation overlay only: status={}; append-only deltas may overlay an existing VortexPreparedState when source/prepared fingerprints and schemas match; no broad CDC, table transaction, object-store, update/delete/upsert, performance, production, or Spark-replacement claim",
            evaluation.status
        ),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

/// Inputs used to expose scout ingress and source triage evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexScoutIngressInput {
    pub source_state_id: String,
    pub source_state_digest: String,
    pub source_format: String,
    pub source_path: String,
    pub source_schema_digest: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub read_plan: String,
    pub metadata_range_refs: String,
    pub sampled_row_range_refs: String,
    pub anomaly_count: u64,
    pub anomaly_families: String,
    pub malformed_row_refs: String,
    pub schema_drift_status: String,
    pub unsupported_shape_status: String,
    pub nullability_status: String,
    pub small_file_pathology_status: String,
    pub quarantine_required: bool,
    pub quarantine_output_plan_status: String,
    pub quarantine_output_ref: String,
    pub quarantine_output_digest: String,
    pub redaction_status: String,
    pub unsupported_diagnostic_code: String,
    pub correctness_policy: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Evidence for the scoped local scout ingress and triage pass.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexScoutIngressReport {
    pub schema_version: &'static str,
    pub status: String,
    pub route: String,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub source_format: String,
    pub source_path: String,
    pub source_schema_digest_before: String,
    pub source_schema_digest_after: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub read_plan: String,
    pub metadata_range_refs: String,
    pub sampled_row_range_refs: String,
    pub anomaly_count: u64,
    pub anomaly_families: String,
    pub malformed_row_refs: String,
    pub schema_drift_status: String,
    pub unsupported_shape_status: String,
    pub nullability_status: String,
    pub small_file_pathology_status: String,
    pub quarantine_required: bool,
    pub quarantine_output_plan_status: String,
    pub quarantine_output_ref: String,
    pub quarantine_output_digest: String,
    pub redaction_status: String,
    pub unsupported_diagnostic_code: String,
    pub correctness_policy: String,
    pub no_standalone_lane_status: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexScoutIngressReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(36);
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_schema_version",
            self.schema_version,
        );
        Self::push_field(&mut fields, "vortex_scout_ingress_status", &self.status);
        Self::push_field(&mut fields, "vortex_scout_ingress_route", &self.route);
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_state_id",
            &self.source_state_id,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_state_digest",
            &self.source_state_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_format",
            &self.source_format,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_path",
            &self.source_path,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_schema_digest_before",
            &self.source_schema_digest_before,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_schema_digest_after",
            &self.source_schema_digest_after,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_row_count",
            self.row_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_source_byte_count",
            self.source_byte_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_column_count",
            self.column_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_read_plan",
            &self.read_plan,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_metadata_range_refs",
            &self.metadata_range_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_sampled_row_range_refs",
            &self.sampled_row_range_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_anomaly_count",
            self.anomaly_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_anomaly_families",
            &self.anomaly_families,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_malformed_row_refs",
            &self.malformed_row_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_schema_drift_status",
            &self.schema_drift_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_unsupported_shape_status",
            &self.unsupported_shape_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_nullability_status",
            &self.nullability_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_small_file_pathology_status",
            &self.small_file_pathology_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_quarantine_required",
            self.quarantine_required.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_quarantine_output_plan_status",
            &self.quarantine_output_plan_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_quarantine_output_ref",
            &self.quarantine_output_ref,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_quarantine_output_digest",
            &self.quarantine_output_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_redaction_status",
            &self.redaction_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_unsupported_diagnostic_code",
            &self.unsupported_diagnostic_code,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_correctness_policy",
            &self.correctness_policy,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_no_standalone_lane_status",
            &self.no_standalone_lane_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_claim_gate_status",
            &self.claim_gate_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_claim_boundary",
            &self.claim_boundary,
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_fallback_attempted",
            self.fallback_attempted.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_scout_ingress_external_engine_invoked",
            self.external_engine_invoked.to_string(),
        );
        fields
    }

    fn push_field(fields: &mut Vec<(String, String)>, key: &'static str, value: impl Into<String>) {
        fields.push((key.to_string(), value.into()));
    }
}

/// Evaluate scout ingress and source triage evidence for a local preparation route.
#[must_use]
pub fn evaluate_vortex_scout_ingress(input: VortexScoutIngressInput) -> VortexScoutIngressReport {
    let status = scout_ingress_status(&input);
    VortexScoutIngressReport {
        schema_version: VORTEX_SCOUT_INGRESS_SCHEMA_VERSION,
        status: status.to_string(),
        route: "vortex_ingest_source_state_scout_triage".to_string(),
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        source_format: input.source_format,
        source_path: input.source_path,
        source_schema_digest_before: input.source_schema_digest.clone(),
        source_schema_digest_after: input.source_schema_digest,
        row_count: input.row_count,
        source_byte_count: input.source_byte_count,
        column_count: input.column_count,
        read_plan: input.read_plan,
        metadata_range_refs: input.metadata_range_refs,
        sampled_row_range_refs: input.sampled_row_range_refs,
        anomaly_count: input.anomaly_count,
        anomaly_families: if input.anomaly_families.trim().is_empty() {
            "none".to_string()
        } else {
            input.anomaly_families
        },
        malformed_row_refs: if input.malformed_row_refs.trim().is_empty() {
            "none".to_string()
        } else {
            input.malformed_row_refs
        },
        schema_drift_status: input.schema_drift_status,
        unsupported_shape_status: input.unsupported_shape_status,
        nullability_status: input.nullability_status,
        small_file_pathology_status: input.small_file_pathology_status,
        quarantine_required: input.quarantine_required,
        quarantine_output_plan_status: input.quarantine_output_plan_status,
        quarantine_output_ref: input.quarantine_output_ref,
        quarantine_output_digest: input.quarantine_output_digest,
        redaction_status: input.redaction_status,
        unsupported_diagnostic_code: input.unsupported_diagnostic_code,
        correctness_policy: input.correctness_policy,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "Scoped local scout ingress and triage evidence only: malformed input, schema drift, unsupported shapes, nullability risk, small-file pathology, and quarantine planning are visible before vortex_ingest preparation; no data-quality product, automatic repair, production, broad SQL/DataFrame, performance, object-store, or Spark-replacement claim".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    }
}

fn scout_ingress_status(input: &VortexScoutIngressInput) -> &'static str {
    if input.unsupported_diagnostic_code == "vortex_ingest.requires_vortex_write_feature" {
        "blocked_feature_gate"
    } else if input.unsupported_shape_status != "not_detected" {
        "blocked_unsupported_nested_shape"
    } else if input.malformed_row_refs != "none" || input.anomaly_families.contains("malformed") {
        "blocked_malformed_source"
    } else if input.schema_drift_status.starts_with("blocked") {
        "blocked_schema_drift"
    } else if input.quarantine_required {
        "quarantine_planned"
    } else {
        "admitted_scout_ingress_clean"
    }
}

/// Inputs used to expose cold-lane Vortex layout/write advisor evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLayoutWriteAdvisorInput {
    pub source_state_id: String,
    pub source_state_digest: String,
    pub source_format: String,
    pub source_schema_digest: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub workload_constitution: String,
    pub source_statistics_status: String,
    pub requested_pushdown_requirements: String,
    pub sink_requirements: String,
    pub layout_strategy: String,
    pub chunking_strategy: String,
    pub segmentation_strategy: String,
    pub dictionary_strategy: String,
    pub statistics_policy: String,
    pub writer_provider_kind: String,
    pub writer_provider_surface: String,
    pub writer_admission_policy: String,
    pub write_reopen_verification_depth: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub expected_read_tradeoff: String,
    pub expected_write_tradeoff: String,
    pub strategy_admitted: bool,
    pub unsupported_diagnostic_code: String,
    pub correctness_refs: String,
    pub benchmark_refs: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Evidence for scoped local Vortex layout/write advisor checks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLayoutWriteAdvisorReport {
    pub schema_version: &'static str,
    pub status: String,
    pub route: String,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub source_format: String,
    pub source_schema_digest: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub workload_constitution: String,
    pub source_statistics_status: String,
    pub requested_pushdown_requirements: String,
    pub sink_requirements: String,
    pub layout_strategy: String,
    pub chunking_strategy: String,
    pub segmentation_strategy: String,
    pub dictionary_strategy: String,
    pub statistics_policy: String,
    pub writer_provider_kind: String,
    pub writer_provider_version: &'static str,
    pub writer_provider_surface: String,
    pub writer_admission_policy: String,
    pub write_reopen_verification_depth: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub expected_read_tradeoff: String,
    pub expected_write_tradeoff: String,
    pub strategy_admitted: bool,
    pub runtime_decision_applied: bool,
    pub selected_strategy: String,
    pub strategy_decision_digest: String,
    pub provider_admitted: bool,
    pub blocker: String,
    pub unsupported_diagnostic_code: String,
    pub correctness_refs: String,
    pub benchmark_refs: String,
    pub no_standalone_lane_status: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Runtime decision returned by the local Vortex writer after it validates a
/// scoped layout/write advisor strategy and applies it to the writer path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLayoutWriteRuntimeDecision {
    pub runtime_decision_applied: bool,
    pub selected_strategy: String,
    pub strategy_decision_digest: String,
    pub provider_admitted: bool,
    pub blocker: String,
}

#[cfg(feature = "vortex-write")]
impl VortexLayoutWriteRuntimeDecision {
    fn not_requested() -> Self {
        let blocker = "layout_write_advisor_not_attached_to_writer".to_string();
        Self {
            runtime_decision_applied: false,
            selected_strategy: "not_requested".to_string(),
            strategy_decision_digest: fnv64_digest_text(&format!(
                "layout_write_runtime_decision|not_requested|{blocker}"
            )),
            provider_admitted: false,
            blocker,
        }
    }

    fn applied(
        advisor: &VortexLayoutWriteAdvisorReport,
        target_path: &Path,
        expected_provider_kind: &str,
        expected_provider_surface: &str,
        certification_level: VortexIngestCertificationLevel,
    ) -> Self {
        let selected_strategy = advisor.layout_strategy.clone();
        let strategy_decision_digest = fnv64_digest_text(&format!(
            "layout_write_runtime_decision|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            advisor.schema_version,
            advisor.source_state_digest,
            advisor.source_schema_digest,
            selected_strategy,
            expected_provider_kind,
            expected_provider_surface,
            advisor.writer_admission_policy,
            certification_level.as_str(),
            target_path.display()
        ));
        Self {
            runtime_decision_applied: true,
            selected_strategy,
            strategy_decision_digest,
            provider_admitted: true,
            blocker: "none".to_string(),
        }
    }
}

impl VortexLayoutWriteAdvisorReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(43);
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_schema_version",
            self.schema_version,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_status",
            &self.status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_route",
            &self.route,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_state_id",
            &self.source_state_id,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_state_digest",
            &self.source_state_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_format",
            &self.source_format,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_schema_digest",
            &self.source_schema_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_row_count",
            self.row_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_byte_count",
            self.source_byte_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_column_count",
            self.column_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_workload_constitution",
            &self.workload_constitution,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_source_statistics_status",
            &self.source_statistics_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_requested_pushdown_requirements",
            &self.requested_pushdown_requirements,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_sink_requirements",
            &self.sink_requirements,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_layout_strategy",
            &self.layout_strategy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_chunking_strategy",
            &self.chunking_strategy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_segmentation_strategy",
            &self.segmentation_strategy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_dictionary_strategy",
            &self.dictionary_strategy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_statistics_policy",
            &self.statistics_policy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_writer_provider_kind",
            &self.writer_provider_kind,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_writer_provider_version",
            self.writer_provider_version,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_writer_provider_surface",
            &self.writer_provider_surface,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_writer_admission_policy",
            &self.writer_admission_policy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_write_reopen_verification_depth",
            &self.write_reopen_verification_depth,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_materialization_boundary_status",
            &self.materialization_boundary_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_decode_boundary_status",
            &self.decode_boundary_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_expected_read_tradeoff",
            &self.expected_read_tradeoff,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_expected_write_tradeoff",
            &self.expected_write_tradeoff,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_strategy_admitted",
            self.strategy_admitted.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_runtime_decision_applied",
            self.runtime_decision_applied.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_selected_strategy",
            &self.selected_strategy,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_strategy_decision_digest",
            &self.strategy_decision_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_provider_admitted",
            self.provider_admitted.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_blocker",
            &self.blocker,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_unsupported_diagnostic_code",
            &self.unsupported_diagnostic_code,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_correctness_refs",
            &self.correctness_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_benchmark_refs",
            &self.benchmark_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_no_standalone_lane_status",
            &self.no_standalone_lane_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_claim_gate_status",
            &self.claim_gate_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_claim_boundary",
            &self.claim_boundary,
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_fallback_attempted",
            self.fallback_attempted.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_layout_write_advisor_external_engine_invoked",
            self.external_engine_invoked.to_string(),
        );
        fields
    }

    fn push_field(fields: &mut Vec<(String, String)>, key: &'static str, value: impl Into<String>) {
        fields.push((key.to_string(), value.into()));
    }

    /// Attach a writer-validated runtime decision to the public advisor
    /// evidence after the writer path has actually applied or blocked it.
    #[must_use]
    pub fn with_runtime_decision(mut self, decision: &VortexLayoutWriteRuntimeDecision) -> Self {
        self.runtime_decision_applied = decision.runtime_decision_applied;
        self.selected_strategy
            .clone_from(&decision.selected_strategy);
        self.strategy_decision_digest
            .clone_from(&decision.strategy_decision_digest);
        self.provider_admitted = decision.provider_admitted;
        self.blocker.clone_from(&decision.blocker);
        self
    }
}

/// Evaluate scoped local Vortex layout/write advisor evidence.
#[must_use]
pub fn evaluate_vortex_layout_write_advisor(
    input: VortexLayoutWriteAdvisorInput,
) -> VortexLayoutWriteAdvisorReport {
    let status = layout_write_advisor_status(&input);
    let provider_admitted = status == "admitted_local_layout_write_strategy";
    let selected_strategy = if provider_admitted {
        input.layout_strategy.clone()
    } else {
        "not_admitted".to_string()
    };
    let blocker = if provider_admitted {
        "pending_runtime_write_decision".to_string()
    } else if input.unsupported_diagnostic_code == "none" {
        status.to_string()
    } else {
        input.unsupported_diagnostic_code.clone()
    };
    let strategy_decision_digest = fnv64_digest_text(&format!(
        "layout_write_advisor_evaluation|{}|{}|{}|{}|{}|{}",
        input.source_state_digest,
        input.source_schema_digest,
        selected_strategy,
        input.writer_provider_kind,
        input.writer_admission_policy,
        blocker
    ));
    VortexLayoutWriteAdvisorReport {
        schema_version: VORTEX_LAYOUT_WRITE_ADVISOR_SCHEMA_VERSION,
        status: status.to_string(),
        route: "vortex_ingest_layout_write_advisor".to_string(),
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        source_format: input.source_format,
        source_schema_digest: input.source_schema_digest,
        row_count: input.row_count,
        source_byte_count: input.source_byte_count,
        column_count: input.column_count,
        workload_constitution: input.workload_constitution,
        source_statistics_status: input.source_statistics_status,
        requested_pushdown_requirements: input.requested_pushdown_requirements,
        sink_requirements: input.sink_requirements,
        layout_strategy: input.layout_strategy,
        chunking_strategy: input.chunking_strategy,
        segmentation_strategy: input.segmentation_strategy,
        dictionary_strategy: input.dictionary_strategy,
        statistics_policy: input.statistics_policy,
        writer_provider_kind: input.writer_provider_kind,
        writer_provider_version: VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION,
        writer_provider_surface: input.writer_provider_surface,
        writer_admission_policy: input.writer_admission_policy,
        write_reopen_verification_depth: input.write_reopen_verification_depth,
        materialization_boundary_status: input.materialization_boundary_status,
        decode_boundary_status: input.decode_boundary_status,
        expected_read_tradeoff: input.expected_read_tradeoff,
        expected_write_tradeoff: input.expected_write_tradeoff,
        strategy_admitted: input.strategy_admitted,
        runtime_decision_applied: false,
        selected_strategy,
        strategy_decision_digest,
        provider_admitted,
        blocker,
        unsupported_diagnostic_code: input.unsupported_diagnostic_code,
        correctness_refs: input.correctness_refs,
        benchmark_refs: input.benchmark_refs,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "VortexLayoutWriteAdvisor evidence is scoped local cold-lane layout/write admission only: it records source statistics posture, pushdown/sink requirements, writer strategy, provider boundary, tradeoffs, verification depth, correctness refs, and benchmark-ref posture without proving performance, object-store/table layout, production, SQL/DataFrame, or Spark-replacement readiness".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    }
}

fn layout_write_advisor_status(input: &VortexLayoutWriteAdvisorInput) -> &'static str {
    if input.unsupported_diagnostic_code == "vortex_ingest.requires_vortex_write_feature" {
        "blocked_feature_gate"
    } else if !input.strategy_admitted || input.unsupported_diagnostic_code != "none" {
        "blocked_layout_write_strategy"
    } else {
        "admitted_local_layout_write_strategy"
    }
}

#[cfg(feature = "vortex-write")]
fn admit_layout_write_runtime_decision(
    advisor: Option<&VortexLayoutWriteAdvisorReport>,
    expected_provider_kind: &str,
    expected_provider_surface: &str,
    target_path: &Path,
    certification_level: VortexIngestCertificationLevel,
) -> Result<VortexLayoutWriteRuntimeDecision> {
    let Some(advisor) = advisor else {
        return Ok(VortexLayoutWriteRuntimeDecision::not_requested());
    };
    let blocker =
        layout_write_runtime_blocker(advisor, expected_provider_kind, expected_provider_surface);
    if blocker != "none" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest layout/write advisor blocked writer strategy '{}': {}; no fallback execution was attempted",
            advisor.layout_strategy, blocker
        )));
    }
    Ok(VortexLayoutWriteRuntimeDecision::applied(
        advisor,
        target_path,
        expected_provider_kind,
        expected_provider_surface,
        certification_level,
    ))
}

#[cfg(feature = "vortex-write")]
fn layout_write_runtime_blocker(
    advisor: &VortexLayoutWriteAdvisorReport,
    expected_provider_kind: &str,
    expected_provider_surface: &str,
) -> String {
    if advisor.status != "admitted_local_layout_write_strategy" {
        return format!(
            "vortex_layout_write_advisor.status_not_admitted:{}",
            advisor.status
        );
    }
    if !advisor.strategy_admitted {
        return "vortex_layout_write_advisor.strategy_not_admitted".to_string();
    }
    if advisor.unsupported_diagnostic_code != "none" {
        return advisor.unsupported_diagnostic_code.clone();
    }
    if advisor.layout_strategy != "single_local_vortex_artifact" {
        return "vortex_layout_write_advisor.unsupported_layout_strategy".to_string();
    }
    if advisor.sink_requirements != "workspace_safe_local_vortex_file_sink" {
        return "vortex_layout_write_advisor.unsupported_sink_requirements".to_string();
    }
    if advisor.writer_admission_policy != "scoped_local_vortex_ingest_prepare_once" {
        return "vortex_layout_write_advisor.unsupported_writer_admission_policy".to_string();
    }
    if !matches!(
        advisor.chunking_strategy.as_str(),
        "single_chunk_for_scoped_local_fixture"
            | "single_chunk_for_scoped_fixture"
            | "writer_default_chunking_no_performance_claim"
    ) {
        return "vortex_layout_write_advisor.unsupported_chunking_strategy".to_string();
    }
    if !matches!(
        advisor.segmentation_strategy.as_str(),
        "single_segment_local_fixture" | "single_segment_fixture"
    ) {
        return "vortex_layout_write_advisor.unsupported_segmentation_strategy".to_string();
    }
    if advisor.dictionary_strategy != "writer_default_no_dictionary_claim" {
        return "vortex_layout_write_advisor.unsupported_dictionary_strategy".to_string();
    }
    if advisor.statistics_policy != "writer_default_statistics_no_pruning_claim" {
        return "vortex_layout_write_advisor.unsupported_statistics_policy".to_string();
    }
    if advisor.writer_provider_kind != expected_provider_kind {
        return format!(
            "vortex_layout_write_advisor.provider_kind_mismatch:expected={expected_provider_kind};actual={}",
            advisor.writer_provider_kind
        );
    }
    if !advisor
        .writer_provider_surface
        .contains(expected_provider_surface)
    {
        return format!(
            "vortex_layout_write_advisor.provider_surface_mismatch:expected={expected_provider_surface}"
        );
    }
    if !advisor
        .writer_provider_surface
        .contains("VortexSession::write_options().write(ArrayStream)")
    {
        return "vortex_layout_write_advisor.missing_vortex_writer_surface".to_string();
    }
    "none".to_string()
}

/// Inputs used to expose cold-lane copy-budget and buffer-lifecycle evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCopyBudgetInput {
    pub source_state_id: String,
    pub source_state_digest: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub source_format: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub allocation_scope: String,
    pub copy_scope: String,
    pub measurement_status: String,
    pub source_read_copy_bytes: String,
    pub parse_normalization_copy_bytes: String,
    pub columnar_handoff_copy_bytes: String,
    pub vortex_array_build_copy_bytes: String,
    pub writer_buffer_bytes: String,
    pub reopen_verify_copy_bytes: String,
    pub evidence_render_copy_bytes: String,
    pub total_measured_copy_bytes: String,
    pub buffer_family: String,
    pub ownership_policy: String,
    pub writer_buffering_status: String,
    pub buffer_reuse_status: String,
    pub buffer_reuse_count: u64,
    pub unsafe_lifetime_shortcut_status: String,
    pub correctness_parity_refs: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub unsupported_diagnostic_code: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

/// Evidence for scoped local copy-budget and buffer-lifecycle checks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCopyBudgetReport {
    pub schema_version: &'static str,
    pub status: String,
    pub route: String,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub source_format: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub allocation_scope: String,
    pub copy_scope: String,
    pub measurement_status: String,
    pub source_read_copy_bytes: String,
    pub parse_normalization_copy_bytes: String,
    pub columnar_handoff_copy_bytes: String,
    pub vortex_array_build_copy_bytes: String,
    pub writer_buffer_bytes: String,
    pub reopen_verify_copy_bytes: String,
    pub evidence_render_copy_bytes: String,
    pub total_measured_copy_bytes: String,
    pub buffer_family: String,
    pub ownership_policy: String,
    pub writer_buffering_status: String,
    pub buffer_reuse_status: String,
    pub buffer_reuse_count: u64,
    pub unsafe_lifetime_shortcut_status: String,
    pub correctness_parity_refs: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub unsupported_diagnostic_code: String,
    pub no_standalone_lane_status: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexCopyBudgetReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(38);
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_schema_version",
            self.schema_version,
        );
        Self::push_field(&mut fields, "vortex_copy_budget_status", &self.status);
        Self::push_field(&mut fields, "vortex_copy_budget_route", &self.route);
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_source_state_id",
            &self.source_state_id,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_source_state_digest",
            &self.source_state_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_prepared_state_id",
            &self.prepared_state_id,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_prepared_state_digest",
            &self.prepared_state_digest,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_source_format",
            &self.source_format,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_row_count",
            self.row_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_source_byte_count",
            self.source_byte_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_column_count",
            self.column_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_allocation_scope",
            &self.allocation_scope,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_copy_scope",
            &self.copy_scope,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_measurement_status",
            &self.measurement_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_source_read_copy_bytes",
            &self.source_read_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_parse_normalization_copy_bytes",
            &self.parse_normalization_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_columnar_handoff_copy_bytes",
            &self.columnar_handoff_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_vortex_array_build_copy_bytes",
            &self.vortex_array_build_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_writer_buffer_bytes",
            &self.writer_buffer_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_reopen_verify_copy_bytes",
            &self.reopen_verify_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_evidence_render_copy_bytes",
            &self.evidence_render_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_total_measured_copy_bytes",
            &self.total_measured_copy_bytes,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_buffer_family",
            &self.buffer_family,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_ownership_policy",
            &self.ownership_policy,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_writer_buffering_status",
            &self.writer_buffering_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_buffer_reuse_status",
            &self.buffer_reuse_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_buffer_reuse_count",
            self.buffer_reuse_count.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_unsafe_lifetime_shortcut_status",
            &self.unsafe_lifetime_shortcut_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_correctness_parity_refs",
            &self.correctness_parity_refs,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_materialization_boundary_status",
            &self.materialization_boundary_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_decode_boundary_status",
            &self.decode_boundary_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_unsupported_diagnostic_code",
            &self.unsupported_diagnostic_code,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_no_standalone_lane_status",
            &self.no_standalone_lane_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_claim_gate_status",
            &self.claim_gate_status,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_claim_boundary",
            &self.claim_boundary,
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_fallback_attempted",
            self.fallback_attempted.to_string(),
        );
        Self::push_field(
            &mut fields,
            "vortex_copy_budget_external_engine_invoked",
            self.external_engine_invoked.to_string(),
        );
        fields
    }

    fn push_field(fields: &mut Vec<(String, String)>, key: &'static str, value: impl Into<String>) {
        fields.push((key.to_string(), value.into()));
    }
}

/// Evaluate scoped local copy-budget and buffer-lifecycle evidence.
#[must_use]
pub fn evaluate_vortex_copy_budget(input: VortexCopyBudgetInput) -> VortexCopyBudgetReport {
    let status = copy_budget_status(&input);
    VortexCopyBudgetReport {
        schema_version: VORTEX_COPY_BUDGET_SCHEMA_VERSION,
        status: status.to_string(),
        route: "vortex_ingest_copy_budget_buffer_lifecycle".to_string(),
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        prepared_state_id: input.prepared_state_id,
        prepared_state_digest: input.prepared_state_digest,
        source_format: input.source_format,
        row_count: input.row_count,
        source_byte_count: input.source_byte_count,
        column_count: input.column_count,
        allocation_scope: input.allocation_scope,
        copy_scope: input.copy_scope,
        measurement_status: input.measurement_status,
        source_read_copy_bytes: input.source_read_copy_bytes,
        parse_normalization_copy_bytes: input.parse_normalization_copy_bytes,
        columnar_handoff_copy_bytes: input.columnar_handoff_copy_bytes,
        vortex_array_build_copy_bytes: input.vortex_array_build_copy_bytes,
        writer_buffer_bytes: input.writer_buffer_bytes,
        reopen_verify_copy_bytes: input.reopen_verify_copy_bytes,
        evidence_render_copy_bytes: input.evidence_render_copy_bytes,
        total_measured_copy_bytes: input.total_measured_copy_bytes,
        buffer_family: input.buffer_family,
        ownership_policy: input.ownership_policy,
        writer_buffering_status: input.writer_buffering_status,
        buffer_reuse_status: input.buffer_reuse_status,
        buffer_reuse_count: input.buffer_reuse_count,
        unsafe_lifetime_shortcut_status: input.unsafe_lifetime_shortcut_status,
        correctness_parity_refs: input.correctness_parity_refs,
        materialization_boundary_status: input.materialization_boundary_status,
        decode_boundary_status: input.decode_boundary_status,
        unsupported_diagnostic_code: input.unsupported_diagnostic_code,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "VortexCopyBudget evidence is scoped local cold-lane allocation/copy and buffer-lifecycle visibility only: it records measured or not-measured copy scopes, writer buffering, ownership policy, reuse blockers, unsafe-lifetime posture, correctness parity refs, and materialization/decode boundaries without proving memory efficiency, global buffer-pool behavior, performance, production, SQL/DataFrame, or Spark-replacement readiness".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    }
}

fn copy_budget_status(input: &VortexCopyBudgetInput) -> &'static str {
    if input.unsupported_diagnostic_code == "vortex_ingest.requires_vortex_write_feature" {
        "blocked_feature_gate"
    } else if input.unsupported_diagnostic_code != "none" {
        "blocked_copy_budget"
    } else if input.unsafe_lifetime_shortcut_status != "blocked_no_unsafe_lifetime_shortcuts" {
        "blocked_unsafe_lifetime_shortcut"
    } else if input.buffer_reuse_status.starts_with("admitted") {
        "admitted_scoped_buffer_reuse"
    } else if input.measurement_status.contains("not_measured") {
        "reported_copy_budget_with_unmeasured_segments"
    } else {
        "reported_copy_budget"
    }
}

/// Inputs used to expose capillary cold-preparation task evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCapillaryPreparationInput {
    pub source_state_id: String,
    pub source_state_digest: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub source_surface: String,
    pub sink_surface: String,
    pub format_family: String,
    pub operation_class: String,
    pub certification_depth: String,
    pub row_count: u64,
    pub source_byte_count: u64,
    pub column_count: usize,
    pub source_split_refs: String,
    pub source_byte_range_refs: String,
    pub source_row_range_refs: String,
    pub projection_mask: String,
    pub filter_mask_status: String,
    pub prepared_artifact_ref: String,
    pub prepared_artifact_digest: String,
    pub prepared_artifact_segment_refs: String,
    pub writer_sink_refs: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub native_io_certificate_status: String,
    pub native_io_certificate_refs: String,
    pub correctness_digest: String,
    pub memory_budget_bytes: u64,
    pub max_parallelism: usize,
    pub result_sink_requested: bool,
    pub result_sink_replay_verified: bool,
    pub capillary_claim_evidence_requested: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VortexCapillaryActivation {
    pub activated: bool,
    pub reason: &'static str,
    pub observed_split_count: usize,
    pub estimated_peak_memory_bytes: u64,
}

struct VortexCapillaryActivatedEvidence {
    task_manifest_digest: String,
    execution_certificate_id: String,
    execution_certificate_status: &'static str,
    task_count: usize,
    task_roles: String,
    task_ids: String,
    task_byte_range_refs: String,
    task_row_range_refs: String,
    vortex_segment_refs: String,
    execution_window_count: usize,
    execution_window_size: usize,
    execution_window_ids: String,
    execution_window_task_counts: String,
    execution_window_task_ids: String,
    execution_window_digests: String,
    scheduler_applied: bool,
    scheduler_application_reason: &'static str,
    peak_memory_bytes: u64,
    pulseweave_report: PulseWeaveReport,
    status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VortexCapillaryPreparationTask {
    task_id: String,
    role: &'static str,
    operator_memory_class: OperatorMemoryClass,
    byte_range_ref: String,
    row_range_ref: String,
    vortex_segment_ref: String,
    writer_sink_ref: String,
    estimated_memory_bytes: u64,
}

impl VortexCapillaryPreparationTask {
    fn to_pulseweave_shape(&self) -> Result<PulseWeaveTaskShape> {
        PulseWeaveTaskShape::new(
            self.task_id.as_str(),
            self.role,
            self.operator_memory_class,
            self.estimated_memory_bytes,
        )
        .map(|shape| {
            shape
                .with_estimated_rows(1)
                .with_refs(
                    self.writer_sink_ref.clone(),
                    format!("capillary-task://{}", self.task_id),
                )
                .with_shape_permissions(true, true, true)
                .with_materialization_and_sink(
                    matches!(self.operator_memory_class, OperatorMemoryClass::Translation),
                    if matches!(self.operator_memory_class, OperatorMemoryClass::Sink) {
                        "workspace_safe_local_vortex_file_sink"
                    } else {
                        "none"
                    },
                )
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VortexCapillaryExecutionWindow {
    window_id: String,
    task_ids: Vec<String>,
    task_count: usize,
    window_digest: String,
}

/// Evidence for capillary cold-preparation task control.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCapillaryPreparationReport {
    pub schema_version: &'static str,
    pub status: String,
    pub route: String,
    pub activation_policy: String,
    pub activation_result: String,
    pub activation_reason: String,
    pub activation_threshold_bytes: u64,
    pub activation_threshold_rows: u64,
    pub activation_threshold_splits: usize,
    pub activation_threshold_columns: usize,
    pub activation_observed_bytes: u64,
    pub activation_observed_rows: u64,
    pub activation_observed_columns: usize,
    pub activation_observed_split_count: usize,
    pub activation_estimated_peak_memory_bytes: u64,
    pub activation_format_family: String,
    pub activation_operation_class: String,
    pub activation_certification_depth: String,
    pub activation_result_sink_replay_requested: bool,
    pub source_surface: String,
    pub sink_surface: String,
    pub source_state_id: String,
    pub source_state_digest: String,
    pub prepared_state_id: String,
    pub prepared_state_digest: String,
    pub task_manifest_id: String,
    pub task_manifest_digest: String,
    pub task_count: usize,
    pub task_roles: String,
    pub task_ids: String,
    pub execution_window_count: usize,
    pub execution_window_size: usize,
    pub execution_window_ids: String,
    pub execution_window_task_counts: String,
    pub execution_window_task_ids: String,
    pub execution_window_digests: String,
    pub scheduler_applied: bool,
    pub scheduler_application_reason: String,
    pub source_split_refs: String,
    pub read_chunk_byte_range_refs: String,
    pub row_range_refs: String,
    pub projection_mask: String,
    pub filter_mask_status: String,
    pub vortex_segment_refs: String,
    pub writer_sink_refs: String,
    pub memory_budget_bytes: u64,
    pub max_parallelism: usize,
    pub peak_memory_bytes: u64,
    pub memory_pressure_status: String,
    pub sink_pressure_status: String,
    pub retry_idempotency_status: String,
    pub materialization_boundary_status: String,
    pub decode_boundary_status: String,
    pub native_io_certificate_status: String,
    pub native_io_certificate_refs: String,
    pub execution_certificate_id: String,
    pub execution_certificate_status: String,
    pub pulseweave_report: PulseWeaveReport,
    pub correctness_refs: String,
    pub no_standalone_lane_status: String,
    pub claim_gate_status: String,
    pub claim_boundary: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl VortexCapillaryPreparationReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        let mut fields = Vec::with_capacity(104);
        self.append_identity_fields(&mut fields);
        self.append_activation_fields(&mut fields);
        self.append_source_sink_fields(&mut fields);
        self.append_task_fields(&mut fields);
        self.append_policy_fields(&mut fields);
        fields.extend(
            self.pulseweave_report
                .fields()
                .into_iter()
                .map(|(key, value)| (format!("vortex_capillary_preparation_{key}"), value)),
        );
        fields
    }

    fn push_field(fields: &mut Vec<(String, String)>, key: &'static str, value: impl Into<String>) {
        fields.push((key.to_string(), value.into()));
    }

    fn append_identity_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_field(
            fields,
            "vortex_capillary_preparation_schema_version",
            self.schema_version,
        );
        Self::push_field(fields, "vortex_capillary_preparation_status", &self.status);
        Self::push_field(fields, "vortex_capillary_preparation_route", &self.route);
    }

    fn append_activation_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_policy",
            &self.activation_policy,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_result",
            &self.activation_result,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_reason",
            &self.activation_reason,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_threshold_bytes",
            self.activation_threshold_bytes.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_threshold_rows",
            self.activation_threshold_rows.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_threshold_splits",
            self.activation_threshold_splits.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_threshold_columns",
            self.activation_threshold_columns.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_observed_bytes",
            self.activation_observed_bytes.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_observed_rows",
            self.activation_observed_rows.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_observed_columns",
            self.activation_observed_columns.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_observed_split_count",
            self.activation_observed_split_count.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_estimated_peak_memory_bytes",
            self.activation_estimated_peak_memory_bytes.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_format_family",
            &self.activation_format_family,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_operation_class",
            &self.activation_operation_class,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_certification_depth",
            &self.activation_certification_depth,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_activation_result_sink_replay_requested",
            self.activation_result_sink_replay_requested.to_string(),
        );
    }

    fn append_source_sink_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_field(
            fields,
            "vortex_capillary_preparation_source_surface",
            &self.source_surface,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_sink_surface",
            &self.sink_surface,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_source_state_id",
            &self.source_state_id,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_source_state_digest",
            &self.source_state_digest,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_prepared_state_id",
            &self.prepared_state_id,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_prepared_state_digest",
            &self.prepared_state_digest,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_task_manifest_id",
            &self.task_manifest_id,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_task_manifest_digest",
            &self.task_manifest_digest,
        );
    }

    fn append_task_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_field(
            fields,
            "vortex_capillary_preparation_task_count",
            self.task_count.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_task_roles",
            &self.task_roles,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_task_ids",
            &self.task_ids,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_count",
            self.execution_window_count.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_size",
            self.execution_window_size.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_ids",
            &self.execution_window_ids,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_task_counts",
            &self.execution_window_task_counts,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_task_ids",
            &self.execution_window_task_ids,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_window_digests",
            &self.execution_window_digests,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_scheduler_applied",
            self.scheduler_applied.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_scheduler_application_reason",
            &self.scheduler_application_reason,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_source_split_refs",
            &self.source_split_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_read_chunk_byte_range_refs",
            &self.read_chunk_byte_range_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_row_range_refs",
            &self.row_range_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_projection_mask",
            &self.projection_mask,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_filter_mask_status",
            &self.filter_mask_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_vortex_segment_refs",
            &self.vortex_segment_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_writer_sink_refs",
            &self.writer_sink_refs,
        );
    }

    fn append_policy_fields(&self, fields: &mut Vec<(String, String)>) {
        Self::push_field(
            fields,
            "vortex_capillary_preparation_memory_budget_bytes",
            self.memory_budget_bytes.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_max_parallelism",
            self.max_parallelism.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_peak_memory_bytes",
            self.peak_memory_bytes.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_memory_pressure_status",
            &self.memory_pressure_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_sink_pressure_status",
            &self.sink_pressure_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_retry_idempotency_status",
            &self.retry_idempotency_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_materialization_boundary_status",
            &self.materialization_boundary_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_decode_boundary_status",
            &self.decode_boundary_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_native_io_certificate_status",
            &self.native_io_certificate_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_native_io_certificate_refs",
            &self.native_io_certificate_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_certificate_id",
            &self.execution_certificate_id,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_execution_certificate_status",
            &self.execution_certificate_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_correctness_refs",
            &self.correctness_refs,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_no_standalone_lane_status",
            &self.no_standalone_lane_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_claim_gate_status",
            &self.claim_gate_status,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_claim_boundary",
            &self.claim_boundary,
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_fallback_attempted",
            self.fallback_attempted.to_string(),
        );
        Self::push_field(
            fields,
            "vortex_capillary_preparation_external_engine_invoked",
            self.external_engine_invoked.to_string(),
        );
    }
}

/// Pre-write capillary control evidence attached to local Vortex preparation.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCapillaryPreWriteControlReport {
    pub schema_version: &'static str,
    pub status: String,
    pub source_report_status: String,
    pub activation_result: String,
    pub activation_reason: String,
    pub scheduler_applied: bool,
    pub execution_window_count: usize,
    pub execution_window_size: usize,
    pub execution_window_ids: String,
    pub execution_window_task_ids: String,
    pub execution_window_digests: String,
    pub controlled_task_roles: String,
    pub plan_digest: String,
    pub source_split_discovery_gate_status: String,
    pub read_chunk_gate_status: String,
    pub array_build_gate_status: String,
    pub write_gate_status: String,
    pub reopen_gate_status: String,
    pub sink_evidence_gate_status: String,
    pub flow_inventory_wip_limit: usize,
    pub scarcity_ledger_selected_action: String,
    pub endopulse_adjustment_applied: bool,
    pub proofbound_pre_application_status: String,
    pub proofbound_post_application_status: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

#[cfg(feature = "vortex-write")]
impl VortexCapillaryPreWriteControlReport {
    fn not_requested(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self {
            schema_version: VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION,
            status: "not_requested".to_string(),
            source_report_status: "not_requested".to_string(),
            activation_result: "not_requested".to_string(),
            activation_reason: reason.clone(),
            scheduler_applied: false,
            execution_window_count: 0,
            execution_window_size: 0,
            execution_window_ids: "none".to_string(),
            execution_window_task_ids: "none".to_string(),
            execution_window_digests: "none".to_string(),
            controlled_task_roles: "none".to_string(),
            plan_digest: "none".to_string(),
            source_split_discovery_gate_status: "not_requested".to_string(),
            read_chunk_gate_status: "not_requested".to_string(),
            array_build_gate_status: "not_requested".to_string(),
            write_gate_status: "not_requested".to_string(),
            reopen_gate_status: "not_requested".to_string(),
            sink_evidence_gate_status: "not_requested".to_string(),
            flow_inventory_wip_limit: 0,
            scarcity_ledger_selected_action: "not_requested".to_string(),
            endopulse_adjustment_applied: false,
            proofbound_pre_application_status: "not_requested".to_string(),
            proofbound_post_application_status: "not_requested".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn from_preparation_report(report: &VortexCapillaryPreparationReport) -> Self {
        let status = if report.scheduler_applied {
            "applied_before_array_build"
        } else if report.activation_result == "skipped" {
            "not_requested_below_threshold"
        } else if report.status.starts_with("blocked")
            || report.status.starts_with("report_only_blocked")
        {
            "report_only_blocked_before_array_build"
        } else {
            "report_only_not_applied_before_array_build"
        };
        let plan_digest = fnv64_digest_text(&format!(
            "{}|{}|{}|{}|{}|{}",
            report.source_state_digest,
            report.task_manifest_digest,
            report.execution_window_count,
            report.execution_window_size,
            report.execution_window_ids,
            report.execution_window_task_ids
        ));
        Self {
            schema_version: VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION,
            status: status.to_string(),
            source_report_status: report.status.clone(),
            activation_result: report.activation_result.clone(),
            activation_reason: report.activation_reason.clone(),
            scheduler_applied: report.scheduler_applied,
            execution_window_count: report.execution_window_count,
            execution_window_size: report.execution_window_size,
            execution_window_ids: report.execution_window_ids.clone(),
            execution_window_task_ids: report.execution_window_task_ids.clone(),
            execution_window_digests: report.execution_window_digests.clone(),
            controlled_task_roles: report.task_roles.clone(),
            plan_digest,
            source_split_discovery_gate_status: "pending".to_string(),
            read_chunk_gate_status: "pending".to_string(),
            array_build_gate_status: "pending".to_string(),
            write_gate_status: "pending".to_string(),
            reopen_gate_status: "pending".to_string(),
            sink_evidence_gate_status: "pending".to_string(),
            flow_inventory_wip_limit: report.pulseweave_report.flow_inventory.wip_limit,
            scarcity_ledger_selected_action: report
                .pulseweave_report
                .scarcity_ledger
                .selected_action
                .as_str()
                .to_string(),
            endopulse_adjustment_applied: report.pulseweave_report.endopulse.adjustment_applied,
            proofbound_pre_application_status: report
                .pulseweave_report
                .proofbound
                .pre_application_status
                .clone(),
            proofbound_post_application_status: report
                .pulseweave_report
                .proofbound
                .post_application_status
                .clone(),
            fallback_attempted: report.fallback_attempted,
            external_engine_invoked: report.external_engine_invoked,
        }
    }

    fn apply_task_role_gate(&mut self, role: &str, gate: &str) -> Result<()> {
        let status = if self.scheduler_applied {
            if capillary_role_list_contains(&self.controlled_task_roles, role) {
                "applied_prewrite_window"
            } else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest capillary pre-write scheduler applied, but task role '{role}' was missing before {gate}; no fallback execution was attempted"
                )));
            }
        } else {
            "not_applicable_scheduler_not_applied"
        };
        self.set_gate_status(gate, status);
        Ok(())
    }

    fn mark_task_role_not_performed(&mut self, role: &str, gate: &str) {
        let status = if self.scheduler_applied
            && capillary_role_list_contains(&self.controlled_task_roles, role)
        {
            "not_performed_by_certification_depth"
        } else {
            "not_applicable_scheduler_not_applied"
        };
        self.set_gate_status(gate, status);
    }

    fn set_gate_status(&mut self, gate: &str, status: &str) {
        match gate {
            "source_split_discovery" => {
                self.source_split_discovery_gate_status = status.to_string();
            }
            "read_chunk" => self.read_chunk_gate_status = status.to_string(),
            "array_build" => self.array_build_gate_status = status.to_string(),
            "write" => self.write_gate_status = status.to_string(),
            "reopen" => self.reopen_gate_status = status.to_string(),
            "sink_evidence" => self.sink_evidence_gate_status = status.to_string(),
            _ => {}
        }
    }
}

impl VortexCapillaryPreWriteControlReport {
    /// Return stable evidence fields for CLI/API surfaces.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "vortex_capillary_preparation_prewrite_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_status".to_string(),
                self.status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_source_report_status".to_string(),
                self.source_report_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_activation_result".to_string(),
                self.activation_result.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_activation_reason".to_string(),
                self.activation_reason.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_scheduler_applied".to_string(),
                self.scheduler_applied.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_execution_window_count".to_string(),
                self.execution_window_count.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_execution_window_size".to_string(),
                self.execution_window_size.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_execution_window_ids".to_string(),
                self.execution_window_ids.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_execution_window_task_ids".to_string(),
                self.execution_window_task_ids.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_execution_window_digests".to_string(),
                self.execution_window_digests.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_controlled_task_roles".to_string(),
                self.controlled_task_roles.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_plan_digest".to_string(),
                self.plan_digest.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_source_split_discovery_gate_status"
                    .to_string(),
                self.source_split_discovery_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_read_chunk_gate_status".to_string(),
                self.read_chunk_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_array_build_gate_status".to_string(),
                self.array_build_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_write_gate_status".to_string(),
                self.write_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_reopen_gate_status".to_string(),
                self.reopen_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_sink_evidence_gate_status".to_string(),
                self.sink_evidence_gate_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_flow_inventory_wip_limit".to_string(),
                self.flow_inventory_wip_limit.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_scarcity_ledger_selected_action".to_string(),
                self.scarcity_ledger_selected_action.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_endopulse_adjustment_applied".to_string(),
                self.endopulse_adjustment_applied.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_proofbound_pre_application_status"
                    .to_string(),
                self.proofbound_pre_application_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_proofbound_post_application_status"
                    .to_string(),
                self.proofbound_post_application_status.clone(),
            ),
            (
                "vortex_capillary_preparation_prewrite_fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "vortex_capillary_preparation_prewrite_external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
        ]
    }
}

/// Plan capillary cold-preparation task evidence and `PulseWeave` control.
///
/// # Errors
/// Returns an error when the `PulseWeave` task input is structurally invalid.
pub fn evaluate_vortex_capillary_preparation(
    input: VortexCapillaryPreparationInput,
) -> Result<VortexCapillaryPreparationReport> {
    let activation = should_activate_capillary(&input);
    if !activation.activated {
        return Ok(skipped_vortex_capillary_preparation_report(
            input, activation,
        ));
    }
    let tasks = capillary_preparation_tasks(&input);
    let task_roles = join_task_values(&tasks, |task| task.role.to_string());
    let task_ids = join_task_values(&tasks, |task| task.task_id.clone());
    let vortex_segment_refs = join_task_values(&tasks, |task| task.vortex_segment_ref.clone());
    let task_byte_range_refs = join_task_values(&tasks, |task| {
        format!("{}={}", task.role, task.byte_range_ref)
    });
    let task_row_range_refs = join_task_values(&tasks, |task| {
        format!("{}={}", task.role, task.row_range_ref)
    });
    let peak_memory_bytes = tasks
        .iter()
        .map(|task| task.estimated_memory_bytes)
        .sum::<u64>()
        .min(input.memory_budget_bytes);
    let execution_certificate_status = if missing_capillary_task_manifest(&input.source_split_refs)
    {
        "missing_capillary_task_manifest"
    } else {
        "certified"
    };
    let task_manifest_digest = fnv64_digest_text(&format!(
        "{}|{}|{}|{}|{}|{}",
        input.source_state_digest,
        input.prepared_state_digest,
        task_ids,
        task_roles,
        input.prepared_artifact_digest,
        input.correctness_digest
    ));
    let execution_certificate_id = format!(
        "vortex-capillary-preparation-{}",
        task_manifest_digest.replace(':', "-")
    );
    let pulseweave_report = capillary_pulseweave_report(
        &input,
        &tasks,
        peak_memory_bytes,
        &execution_certificate_id,
        execution_certificate_status,
    )?;
    let status = capillary_status(&input, &tasks, &pulseweave_report);
    let scheduler_applied = capillary_scheduler_applied(&input, &tasks, &pulseweave_report);
    let scheduler_application_reason =
        capillary_scheduler_application_reason(&input, &tasks, &pulseweave_report);
    let execution_window_size = if scheduler_applied {
        pulseweave_report.batch_window_size(input.max_parallelism)
    } else {
        0
    };
    let execution_windows =
        capillary_execution_windows(&tasks, scheduler_applied, execution_window_size);

    let evidence = VortexCapillaryActivatedEvidence {
        task_manifest_digest,
        execution_certificate_id,
        execution_certificate_status,
        task_count: tasks.len(),
        task_roles,
        task_ids,
        task_byte_range_refs,
        task_row_range_refs,
        vortex_segment_refs,
        execution_window_count: execution_windows.len(),
        execution_window_size,
        execution_window_ids: join_window_values(&execution_windows, |window| {
            window.window_id.clone()
        }),
        execution_window_task_counts: join_window_values(&execution_windows, |window| {
            format!("{}={}", window.window_id, window.task_count)
        }),
        execution_window_task_ids: join_window_values(&execution_windows, |window| {
            format!("{}={}", window.window_id, window.task_ids.join("|"))
        }),
        execution_window_digests: join_window_values(&execution_windows, |window| {
            format!("{}={}", window.window_id, window.window_digest)
        }),
        scheduler_applied,
        scheduler_application_reason,
        peak_memory_bytes,
        pulseweave_report,
        status,
    };
    Ok(activated_vortex_capillary_preparation_report(
        input, activation, evidence,
    ))
}

#[cfg(feature = "vortex-write")]
fn plan_capillary_prewrite_control(
    input: Option<&VortexCapillaryPreparationInput>,
) -> Result<VortexCapillaryPreWriteControlReport> {
    let Some(input) = input else {
        return Ok(VortexCapillaryPreWriteControlReport::not_requested(
            "no_capillary_prewrite_input",
        ));
    };
    if input.certification_depth != VortexIngestCertificationLevel::IngestCertified.as_str() {
        return Ok(VortexCapillaryPreWriteControlReport::not_requested(
            "certification_depth_does_not_request_reopen_verified_prewrite_control",
        ));
    }
    evaluate_vortex_capillary_preparation(input.clone())
        .map(|report| VortexCapillaryPreWriteControlReport::from_preparation_report(&report))
}

fn activated_vortex_capillary_preparation_report(
    input: VortexCapillaryPreparationInput,
    activation: VortexCapillaryActivation,
    evidence: VortexCapillaryActivatedEvidence,
) -> VortexCapillaryPreparationReport {
    VortexCapillaryPreparationReport {
        schema_version: VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION,
        status: evidence.status.to_string(),
        route: "vortex_ingest_source_state_to_prepared_state".to_string(),
        activation_policy: VORTEX_CAPILLARY_ACTIVATION_POLICY.to_string(),
        activation_result: "activated".to_string(),
        activation_reason: activation.reason.to_string(),
        activation_threshold_bytes: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES,
        activation_threshold_rows: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROWS,
        activation_threshold_splits: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_SPLITS,
        activation_threshold_columns: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_COLUMNS,
        activation_observed_bytes: input.source_byte_count,
        activation_observed_rows: input.row_count,
        activation_observed_columns: input.column_count,
        activation_observed_split_count: activation.observed_split_count,
        activation_estimated_peak_memory_bytes: activation.estimated_peak_memory_bytes,
        activation_format_family: input.format_family,
        activation_operation_class: input.operation_class,
        activation_certification_depth: input.certification_depth,
        activation_result_sink_replay_requested: input.result_sink_requested
            || input.result_sink_replay_verified,
        source_surface: input.source_surface,
        sink_surface: input.sink_surface,
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        prepared_state_id: input.prepared_state_id,
        prepared_state_digest: input.prepared_state_digest,
        task_manifest_id: format!(
            "vortex-capillary-task-manifest-{}",
            evidence.task_manifest_digest.replace(':', "-")
        ),
        task_manifest_digest: evidence.task_manifest_digest,
        task_count: evidence.task_count,
        task_roles: evidence.task_roles,
        task_ids: evidence.task_ids,
        execution_window_count: evidence.execution_window_count,
        execution_window_size: evidence.execution_window_size,
        execution_window_ids: evidence.execution_window_ids,
        execution_window_task_counts: evidence.execution_window_task_counts,
        execution_window_task_ids: evidence.execution_window_task_ids,
        execution_window_digests: evidence.execution_window_digests,
        scheduler_applied: evidence.scheduler_applied,
        scheduler_application_reason: evidence.scheduler_application_reason.to_string(),
        source_split_refs: input.source_split_refs,
        read_chunk_byte_range_refs: evidence.task_byte_range_refs,
        row_range_refs: evidence.task_row_range_refs,
        projection_mask: input.projection_mask,
        filter_mask_status: input.filter_mask_status,
        vortex_segment_refs: evidence.vortex_segment_refs,
        writer_sink_refs: input.writer_sink_refs,
        memory_budget_bytes: input.memory_budget_bytes,
        max_parallelism: input.max_parallelism,
        peak_memory_bytes: evidence.peak_memory_bytes,
        memory_pressure_status: "bounded_by_local_cold_preparation_memory_budget".to_string(),
        sink_pressure_status: if input.result_sink_requested {
            "bounded_by_result_sink_replay_requirement".to_string()
        } else {
            "workspace_safe_local_vortex_sink_only".to_string()
        },
        retry_idempotency_status: "task_ids_and_manifest_digest_are_replay_stable".to_string(),
        materialization_boundary_status: input.materialization_boundary_status,
        decode_boundary_status: input.decode_boundary_status,
        native_io_certificate_status: input.native_io_certificate_status,
        native_io_certificate_refs: input.native_io_certificate_refs,
        execution_certificate_id: evidence.execution_certificate_id,
        execution_certificate_status: evidence.execution_certificate_status.to_string(),
        pulseweave_report: evidence.pulseweave_report,
        correctness_refs: input.correctness_digest,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "Scoped local capillary cold-preparation evidence only: task boundaries and PulseWeave control are tied to vortex_ingest writer/reopen certificates; no object-store, distributed, broad parallel performance, production, SQL/DataFrame, or Spark-replacement claim".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    }
}

#[must_use]
pub fn should_activate_capillary(
    input: &VortexCapillaryPreparationInput,
) -> VortexCapillaryActivation {
    let observed_split_count = capillary_source_split_count(&input.source_split_refs);
    let row_column_product = input.row_count.saturating_mul(input.column_count as u64);
    let estimated_peak_memory_bytes = input
        .source_byte_count
        .saturating_add(row_column_product.saturating_mul(8))
        .max(1);
    let memory_pressure_threshold = input
        .memory_budget_bytes
        .saturating_div(VORTEX_CAPILLARY_ACTIVATION_MEMORY_PRESSURE_DENOMINATOR)
        .max(1);
    let reason = if input.capillary_claim_evidence_requested {
        "capillary_claim_evidence_requested"
    } else if input.result_sink_requested || input.result_sink_replay_verified {
        "result_sink_replay_requested"
    } else if input.source_byte_count >= VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES {
        "source_bytes_above_threshold"
    } else if input.row_count >= VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROWS {
        "row_count_above_threshold"
    } else if observed_split_count >= VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_SPLITS {
        "split_count_above_threshold"
    } else if input.column_count >= VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_COLUMNS {
        "wide_source_above_threshold"
    } else if row_column_product >= VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROW_COLUMN_PRODUCT {
        "row_column_product_above_threshold"
    } else if estimated_peak_memory_bytes >= memory_pressure_threshold {
        "memory_pressure_above_threshold"
    } else if capillary_complex_operation_class(&input.operation_class) {
        "operation_class_above_threshold"
    } else {
        "below_threshold_small_local_fixture"
    };
    VortexCapillaryActivation {
        activated: reason != "below_threshold_small_local_fixture",
        reason,
        observed_split_count,
        estimated_peak_memory_bytes,
    }
}

fn skipped_vortex_capillary_preparation_report(
    input: VortexCapillaryPreparationInput,
    activation: VortexCapillaryActivation,
) -> VortexCapillaryPreparationReport {
    VortexCapillaryPreparationReport {
        schema_version: VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION,
        status: "not_requested_below_threshold".to_string(),
        route: "vortex_ingest_source_state_to_prepared_state".to_string(),
        activation_policy: VORTEX_CAPILLARY_ACTIVATION_POLICY.to_string(),
        activation_result: "skipped".to_string(),
        activation_reason: activation.reason.to_string(),
        activation_threshold_bytes: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES,
        activation_threshold_rows: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_ROWS,
        activation_threshold_splits: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_SPLITS,
        activation_threshold_columns: VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_COLUMNS,
        activation_observed_bytes: input.source_byte_count,
        activation_observed_rows: input.row_count,
        activation_observed_columns: input.column_count,
        activation_observed_split_count: activation.observed_split_count,
        activation_estimated_peak_memory_bytes: activation.estimated_peak_memory_bytes,
        activation_format_family: input.format_family,
        activation_operation_class: input.operation_class,
        activation_certification_depth: input.certification_depth,
        activation_result_sink_replay_requested: input.result_sink_requested
            || input.result_sink_replay_verified,
        source_surface: input.source_surface,
        sink_surface: input.sink_surface,
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        prepared_state_id: input.prepared_state_id,
        prepared_state_digest: input.prepared_state_digest,
        task_manifest_id: "none".to_string(),
        task_manifest_digest: "none".to_string(),
        task_count: 0,
        task_roles: "none".to_string(),
        task_ids: "none".to_string(),
        execution_window_count: 0,
        execution_window_size: 0,
        execution_window_ids: "none".to_string(),
        execution_window_task_counts: "none".to_string(),
        execution_window_task_ids: "none".to_string(),
        execution_window_digests: "none".to_string(),
        scheduler_applied: false,
        scheduler_application_reason: "not_requested_below_threshold".to_string(),
        source_split_refs: input.source_split_refs,
        read_chunk_byte_range_refs: "none".to_string(),
        row_range_refs: "none".to_string(),
        projection_mask: input.projection_mask,
        filter_mask_status: input.filter_mask_status,
        vortex_segment_refs: "none".to_string(),
        writer_sink_refs: input.writer_sink_refs,
        memory_budget_bytes: input.memory_budget_bytes,
        max_parallelism: input.max_parallelism,
        peak_memory_bytes: 0,
        memory_pressure_status: "not_requested_below_threshold".to_string(),
        sink_pressure_status: "not_requested_below_threshold".to_string(),
        retry_idempotency_status: "not_requested".to_string(),
        materialization_boundary_status: input.materialization_boundary_status,
        decode_boundary_status: input.decode_boundary_status,
        native_io_certificate_status: input.native_io_certificate_status,
        native_io_certificate_refs: input.native_io_certificate_refs,
        execution_certificate_id: "none".to_string(),
        execution_certificate_status: "not_requested".to_string(),
        pulseweave_report: PulseWeaveReport::not_requested(
            "vortex_cold_preparation_local_capillary_io",
            activation.reason,
            input.fallback_attempted,
            input.external_engine_invoked,
        ),
        correctness_refs: input.correctness_digest,
        no_standalone_lane_status:
            "not_requested_below_threshold_no_standalone_lane".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "Scoped local capillary cold-preparation evidence was not required by the dynamic size/complexity gate; no PulseWeave planning, object-store, distributed, broad parallel performance, production, SQL/DataFrame, or Spark-replacement claim".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    }
}

fn capillary_preparation_tasks(
    input: &VortexCapillaryPreparationInput,
) -> Vec<VortexCapillaryPreparationTask> {
    let source_bytes = input.source_byte_count.max(1);
    let write_bytes = input.prepared_artifact_digest.len() as u64 + source_bytes.min(64 * 1024);
    vec![
        capillary_task(
            "source-split-discovery",
            "source_split_discovery",
            OperatorMemoryClass::Scan,
            4 * 1024,
            input,
        ),
        capillary_task(
            "read-chunk",
            "read_chunk",
            OperatorMemoryClass::Scan,
            source_bytes,
            input,
        ),
        capillary_task(
            "columnarize-encode",
            "columnarize_encode",
            OperatorMemoryClass::Translation,
            source_bytes.saturating_add(input.row_count.saturating_mul(8)),
            input,
        ),
        capillary_task(
            "vortex-segment-write",
            "vortex_segment_write",
            OperatorMemoryClass::Sink,
            write_bytes,
            input,
        ),
        capillary_task(
            "reopen-verify",
            "reopen_verify",
            OperatorMemoryClass::Scan,
            write_bytes.saturating_div(2).max(1),
            input,
        ),
        capillary_task(
            "sink-evidence",
            "sink_evidence",
            OperatorMemoryClass::Sink,
            4 * 1024,
            input,
        ),
    ]
}

fn capillary_task(
    suffix: &str,
    role: &'static str,
    operator_memory_class: OperatorMemoryClass,
    estimated_memory_bytes: u64,
    input: &VortexCapillaryPreparationInput,
) -> VortexCapillaryPreparationTask {
    VortexCapillaryPreparationTask {
        task_id: format!("vortex-capillary-{suffix}"),
        role,
        operator_memory_class,
        byte_range_ref: input.source_byte_range_refs.clone(),
        row_range_ref: input.source_row_range_refs.clone(),
        vortex_segment_ref: input.prepared_artifact_segment_refs.clone(),
        writer_sink_ref: input.writer_sink_refs.clone(),
        estimated_memory_bytes: estimated_memory_bytes.max(1),
    }
}

fn capillary_pulseweave_report(
    input: &VortexCapillaryPreparationInput,
    tasks: &[VortexCapillaryPreparationTask],
    peak_memory_bytes: u64,
    execution_certificate_id: &str,
    execution_certificate_status: &str,
) -> Result<PulseWeaveReport> {
    let task_shapes = tasks
        .iter()
        .map(VortexCapillaryPreparationTask::to_pulseweave_shape)
        .collect::<Result<Vec<_>>>()?;
    let target_task_bytes = input.source_byte_count.max(
        input
            .row_count
            .saturating_mul(input.column_count as u64)
            .max(1),
    );
    let pulseweave_input = PulseWeaveInput::new(
        "vortex_ingest_cold_preparation",
        "vortex_cold_preparation_local_capillary_io",
        format!("vortex_ingest:{}", input.source_state_digest),
        task_shapes,
        input.memory_budget_bytes.max(1),
        input.max_parallelism.max(1),
        target_task_bytes,
    )?
    .with_task_byte_limits(4 * 1024, target_task_bytes.saturating_mul(4).max(4 * 1024))
    .with_boundaries(true, true)
    .with_result_sink(
        input.result_sink_requested,
        input.result_sink_replay_verified,
    )
    .with_correctness_and_output(&input.correctness_digest, &input.prepared_artifact_digest)
    .with_execution_certificate(execution_certificate_id, execution_certificate_status)
    .with_native_io_certificate_status(input.native_io_certificate_status.clone())
    .with_memory_observations(tasks.len(), tasks.len(), 0, peak_memory_bytes)
    .with_spill(false, false)
    .with_no_fallback_policy(
        false,
        input.fallback_attempted,
        input.external_engine_invoked,
    );
    plan_pulseweave(pulseweave_input)
}

fn capillary_status(
    input: &VortexCapillaryPreparationInput,
    tasks: &[VortexCapillaryPreparationTask],
    pulseweave_report: &PulseWeaveReport,
) -> &'static str {
    if tasks.is_empty() || missing_capillary_task_manifest(&input.source_split_refs) {
        "blocked_missing_capillary_task_manifest"
    } else if input.native_io_certificate_status != "certified" {
        "report_only_blocked_missing_native_io_certificate"
    } else if pulseweave_report.runtime_decision_applied {
        "applied_capillary_pulseweave_control"
    } else {
        "report_only_blocked_pulseweave_control"
    }
}

fn capillary_scheduler_applied(
    input: &VortexCapillaryPreparationInput,
    tasks: &[VortexCapillaryPreparationTask],
    pulseweave_report: &PulseWeaveReport,
) -> bool {
    !tasks.is_empty()
        && !missing_capillary_task_manifest(&input.source_split_refs)
        && input.native_io_certificate_status == "certified"
        && pulseweave_report.runtime_decision_applied
}

fn capillary_scheduler_application_reason(
    input: &VortexCapillaryPreparationInput,
    tasks: &[VortexCapillaryPreparationTask],
    pulseweave_report: &PulseWeaveReport,
) -> &'static str {
    if tasks.is_empty() {
        "no_capillary_tasks"
    } else if missing_capillary_task_manifest(&input.source_split_refs) {
        "blocked_missing_capillary_task_manifest"
    } else if input.native_io_certificate_status != "certified" {
        "blocked_missing_native_io_certificate"
    } else if pulseweave_report.runtime_decision_applied {
        "pulseweave_batch_window_applied_to_capillary_manifest"
    } else {
        "proofbound_or_resource_policy_blocked"
    }
}

fn capillary_execution_windows(
    tasks: &[VortexCapillaryPreparationTask],
    scheduler_applied: bool,
    execution_window_size: usize,
) -> Vec<VortexCapillaryExecutionWindow> {
    if !scheduler_applied || tasks.is_empty() || execution_window_size == 0 {
        return Vec::new();
    }

    tasks
        .chunks(execution_window_size.max(1))
        .enumerate()
        .map(|(index, window_tasks)| {
            let window_id = format!("vortex-capillary-window-{index:04}");
            let task_ids = window_tasks
                .iter()
                .map(|task| task.task_id.clone())
                .collect::<Vec<_>>();
            let window_digest = fnv64_digest_text(&format!(
                "{}|{}|{}",
                window_id,
                execution_window_size,
                task_ids.join("|")
            ));
            VortexCapillaryExecutionWindow {
                window_id,
                task_count: task_ids.len(),
                task_ids,
                window_digest,
            }
        })
        .collect()
}

fn missing_capillary_task_manifest(source_split_refs: &str) -> bool {
    let source_split_refs = source_split_refs.trim();
    source_split_refs.is_empty() || source_split_refs.eq_ignore_ascii_case("none")
}

fn capillary_source_split_count(source_split_refs: &str) -> usize {
    if missing_capillary_task_manifest(source_split_refs) {
        return 0;
    }
    source_split_refs
        .split(';')
        .filter(|value| {
            let value = value.trim();
            !value.is_empty() && !value.eq_ignore_ascii_case("none")
        })
        .count()
        .max(1)
}

fn capillary_complex_operation_class(operation_class: &str) -> bool {
    let operation_class = operation_class.to_ascii_lowercase();
    [
        "prepare-batch",
        "prepare_batch",
        "many-small-files",
        "many_small_files",
        "stress",
        "cdc",
        "shuffle",
        "join",
        "group",
        "window",
        "wide",
        "null-heavy",
        "null_heavy",
        "high-cardinality",
        "high_cardinality",
    ]
    .iter()
    .any(|needle| operation_class.contains(needle))
}

fn join_task_values(
    tasks: &[VortexCapillaryPreparationTask],
    mut value: impl FnMut(&VortexCapillaryPreparationTask) -> String,
) -> String {
    tasks.iter().map(&mut value).collect::<Vec<_>>().join(",")
}

fn join_window_values(
    windows: &[VortexCapillaryExecutionWindow],
    mut value: impl FnMut(&VortexCapillaryExecutionWindow) -> String,
) -> String {
    if windows.is_empty() {
        "none".to_string()
    } else {
        windows.iter().map(&mut value).collect::<Vec<_>>().join(";")
    }
}

#[cfg(feature = "vortex-write")]
fn capillary_role_list_contains(roles: &str, role: &str) -> bool {
    roles
        .split(',')
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(role))
}

/// Evidence returned by the scoped local `vortex_ingest` helper.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexPreparedStateWriteReport {
    pub target_path: PathBuf,
    pub row_count: u64,
    pub column_count: usize,
    pub column_families: Vec<(String, String)>,
    pub bytes_written: u64,
    pub artifact_digest: String,
    pub digest_micros: u128,
    pub writer_row_count: u64,
    pub reopen_row_count: u64,
    pub array_build_micros: u128,
    pub write_micros: u128,
    pub writer_context_open_micros: u128,
    pub writer_context_reuse_status: String,
    pub vortex_segment_write_micros: u128,
    pub workspace_stage_micros: u128,
    pub reopen_scan_micros: u128,
    pub reopen_verification_status: String,
    pub timing_scope: String,
    pub certification_level: String,
    pub preparation_included: bool,
    pub query_timing_starts_after_preparation: bool,
    pub upstream_vortex_write_called: bool,
    pub upstream_vortex_scan_called: bool,
    pub array_build_provider_kind: String,
    pub array_build_provider_surface: String,
    pub array_build_strategy: String,
    pub array_build_input_layout: String,
    pub array_build_record_batch_count: usize,
    pub manual_scalar_copy_avoided: bool,
    pub preparation_spine: VortexPreparationSpineReport,
    pub layout_write_decision: VortexLayoutWriteRuntimeDecision,
    pub capillary_prewrite_control: VortexCapillaryPreWriteControlReport,
    pub workspace_write_report: WorkspaceSafeLocalWriteReport,
}

impl VortexPreparedStateWriteReport {
    /// Return a stable comma-separated column family summary.
    #[must_use]
    pub fn column_family_summary(&self) -> String {
        self.column_families
            .iter()
            .map(|(column, family)| format!("{column}:{family}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Return a compact schema/layout summary for evidence fields.
    #[must_use]
    pub fn layout_summary(&self) -> String {
        format!(
            "local_flat_struct;columns={};rows={}",
            self.column_count, self.row_count
        )
    }

    /// Return a compact encoding summary for evidence fields.
    #[must_use]
    pub fn encoding_summary(&self) -> String {
        format!(
            "upstream_vortex_writer_default;{}",
            self.column_family_summary()
        )
    }

    /// Return a compact statistics summary for evidence fields.
    #[must_use]
    pub fn statistics_summary(&self) -> String {
        format!(
            "writer_row_count={};reopen_row_count={};reopen_verification_status={};bytes_written={}",
            self.writer_row_count,
            self.reopen_row_count,
            self.reopen_verification_status,
            self.bytes_written
        )
    }
}

/// Whether local Vortex artifact writing is compiled into this crate.
#[must_use]
pub const fn vortex_ingest_write_feature_enabled() -> bool {
    cfg!(feature = "vortex-write")
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the feature gate is not
/// enabled, row shape/family support is outside the scoped contract, the target
/// already exists without overwrite permission, or upstream Vortex write/reopen
/// APIs fail.
#[cfg(not(feature = "vortex-write"))]
pub fn write_flat_scalar_vortex_prepared_state(
    _request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    Err(ShardLoomError::InvalidOperation(
        "local vortex_ingest runtime requires building shardloom-cli with --features vortex-write; default builds expose vortex_ingest as a deterministic blocked prepare-once route"
            .to_string(),
    ))
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when row shape/family support is
/// outside the scoped contract, the target already exists without overwrite
/// permission, or upstream Vortex write/reopen APIs fail.
#[cfg(feature = "vortex-write")]
pub fn write_flat_scalar_vortex_prepared_state(
    request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    if request.certification_level == VortexIngestCertificationLevel::IngestFullReplay {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest ingest_full_replay requires downstream result replay/output evidence; use ingest_certified for prepare-once proof or run an output/replay workflow; no fallback execution was attempted"
                .to_string(),
        ));
    }

    let row_count = validate_flat_rows(&request.columns, &request.rows)?;
    let column_families =
        scalar_column_families(&request.columns, &request.column_dtypes, &request.rows)?;
    let layout_write_decision = admit_layout_write_runtime_decision(
        request.layout_write_advisor.as_ref(),
        "shardloom_kernel",
        "shardloom_scalar_rows_to_vortex_struct",
        &request.target_path,
        request.certification_level,
    )?;
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
    let mut capillary_prewrite_control =
        plan_capillary_prewrite_control(request.capillary_prewrite_input.as_ref())?;
    capillary_prewrite_control
        .apply_task_role_gate("source_split_discovery", "source_split_discovery")?;
    capillary_prewrite_control.apply_task_role_gate("read_chunk", "read_chunk")?;
    capillary_prewrite_control.apply_task_role_gate("columnarize_encode", "array_build")?;
    let array_build_start = Instant::now();
    let array = flat_rows_to_vortex_struct(&request.columns, &request.rows, &column_families)?;
    let array_build_micros = array_build_start.elapsed().as_micros();
    finalize_vortex_prepared_state_write(VortexPreparedStateFinalizeInput {
        target_path: request.target_path,
        column_count: request.columns.len(),
        column_families,
        row_count,
        array: &array,
        array_build_micros,
        certification_level: request.certification_level,
        allow_overwrite: request.allow_overwrite,
        array_build_provider_kind: "shardloom_kernel",
        array_build_provider_surface: "shardloom_scalar_rows_to_vortex_struct",
        array_build_strategy: "scalar_rows_to_vortex_struct",
        array_build_input_layout: "materialized_rows",
        array_build_record_batch_count: 0,
        manual_scalar_copy_avoided: false,
        capillary_prewrite_control,
        layout_write_decision,
        preparation_spine: VortexPreparationSpineFinalizeInput {
            vortex_first_decision: "implement_shardloom_kernel",
            feature_gate: "vortex-write",
            source_surface: "local_text_source_state_scalar_rows",
            split_surface: "single_materialized_scalar_row_split",
            split_count: usize::from(row_count > 0),
            projection_mask_status: "full_projection_materialized",
            filter_mask_status: "not_requested",
            materialization_boundary_status: "materialized_scalar_rows_before_vortex_write",
            decode_boundary_status: "text_source_decoded_by_shardloom_adapter",
        },
    })
}

/// Write flat columnar Arrow batches into a local Vortex artifact and
/// reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when columnar support is outside
/// the scoped contract, the target already exists without overwrite
/// permission, or upstream Vortex write/reopen APIs fail.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub fn write_flat_columnar_vortex_prepared_state(
    request: VortexPreparedStateColumnarWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    if request.certification_level == VortexIngestCertificationLevel::IngestFullReplay {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest ingest_full_replay requires downstream result replay/output evidence; use ingest_certified for prepare-once proof or run an output/replay workflow; no fallback execution was attempted"
                .to_string(),
        ));
    }

    let source_shape = validate_flat_columnar_source_shape(&request.source)?;
    let (expected_provider_kind, expected_provider_surface) = if request.source.batches.is_empty() {
        (
            "shardloom_kernel",
            "shardloom_empty_columnar_struct_builder",
        )
    } else {
        ("vortex_array_kernel", "ArrayRef::from_arrow(RecordBatch)")
    };
    let layout_write_decision = admit_layout_write_runtime_decision(
        request.layout_write_advisor.as_ref(),
        expected_provider_kind,
        expected_provider_surface,
        &request.target_path,
        request.certification_level,
    )?;
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
    let mut capillary_prewrite_control =
        plan_capillary_prewrite_control(request.capillary_prewrite_input.as_ref())?;
    capillary_prewrite_control
        .apply_task_role_gate("source_split_discovery", "source_split_discovery")?;
    capillary_prewrite_control.apply_task_role_gate("read_chunk", "read_chunk")?;
    capillary_prewrite_control.apply_task_role_gate("columnarize_encode", "array_build")?;
    let array_build_start = Instant::now();
    let array_build = flat_columnar_source_to_vortex_struct(&request.source, &source_shape)?;
    let array_build_micros = array_build_start.elapsed().as_micros();
    let row_count = usize_to_u64(request.source.row_count)?;
    let projection_mask_status =
        if request.source.materialized_columns.len() < request.source.header.len() {
            "columnar_projection_mask_applied"
        } else {
            "full_projection"
        };
    let (vortex_first_decision, materialization_boundary_status, decode_boundary_status) =
        if array_build.manual_scalar_copy_avoided {
            (
                "use_vortex_native_provider",
                "columnar_source_state_preserved_to_vortex_array_provider",
                "no_scalar_row_decode_for_non_empty_batches",
            )
        } else {
            (
                "implement_shardloom_kernel",
                "empty_columnar_schema_materialized_by_shardloom_kernel",
                "empty_source_no_data_decode",
            )
        };
    finalize_vortex_prepared_state_write(VortexPreparedStateFinalizeInput {
        target_path: request.target_path,
        column_count: request.source.materialized_columns.len(),
        column_families: array_build.column_families,
        row_count,
        array: &array_build.array,
        array_build_micros,
        certification_level: request.certification_level,
        allow_overwrite: request.allow_overwrite,
        array_build_provider_kind: array_build.provider_kind,
        array_build_provider_surface: array_build.provider_surface,
        array_build_strategy: array_build.strategy,
        array_build_input_layout: "arrow_record_batch_columnar_source_state",
        array_build_record_batch_count: request.source.batches.len(),
        manual_scalar_copy_avoided: array_build.manual_scalar_copy_avoided,
        capillary_prewrite_control,
        layout_write_decision,
        preparation_spine: VortexPreparationSpineFinalizeInput {
            vortex_first_decision,
            feature_gate: "vortex-write,universal-format-io",
            source_surface: "local_columnar_source_state_arrow_record_batches",
            split_surface: "arrow_record_batch_source_splits",
            split_count: request.source.batches.len(),
            projection_mask_status,
            filter_mask_status: "not_requested",
            materialization_boundary_status,
            decode_boundary_status,
        },
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnarProjectedColumn {
    column: String,
    reader_index: usize,
    dtype_hint: Option<LogicalDType>,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FlatColumnarSourceShape {
    projected_columns: Vec<ColumnarProjectedColumn>,
}

#[cfg(feature = "vortex-write")]
fn prepare_vortex_target(target_path: &Path, allow_overwrite: bool) -> Result<()> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(target_path)?;
    shardloom_core::plan_workspace_safe_local_output(workspace_root, target_path, allow_overwrite)?;
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn validate_flat_columnar_source_shape(
    source: &FlatLocalColumnarSource,
) -> Result<FlatColumnarSourceShape> {
    validate_flat_columns(&source.materialized_columns)?;
    validate_flat_columns(&source.reader_projection_columns)?;
    if source.column_dtypes.len() != source.header.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest columnar SourceState dtype hint count is {}, expected {}; no fallback execution was attempted",
            source.column_dtypes.len(),
            source.header.len()
        )));
    }

    let reader_positions = source
        .reader_projection_columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let projected_columns = source
        .materialized_columns
        .iter()
        .map(|column| {
            let reader_index = reader_positions.get(column.as_str()).ok_or_else(|| {
                ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest columnar SourceState is missing projected column '{column}'; no fallback execution was attempted"
                ))
            })?;
            let dtype_hint = source
                .header
                .iter()
                .position(|candidate| candidate == column)
                .and_then(|index| source.column_dtypes.get(index))
                .and_then(Clone::clone);
            Ok(ColumnarProjectedColumn {
                column: column.clone(),
                reader_index: *reader_index,
                dtype_hint,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut row_count = 0usize;
    for (batch_index, batch) in source.batches.iter().enumerate() {
        let expected_column_count = source.reader_projection_columns.len();
        if batch.num_columns() != expected_column_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest columnar SourceState batch {} has {} projected columns, expected {}; no fallback execution was attempted",
                batch_index + 1,
                batch.num_columns(),
                expected_column_count
            )));
        }
        let schema = batch.schema();
        if schema.fields().len() != expected_column_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest columnar SourceState batch {} schema has {} projected fields, expected {}; no fallback execution was attempted",
                batch_index + 1,
                schema.fields().len(),
                expected_column_count
            )));
        }
        for (column_index, expected_column) in source.reader_projection_columns.iter().enumerate() {
            let actual_column = schema.field(column_index).name();
            if actual_column != expected_column {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest columnar SourceState batch {} projected field {} is '{}', expected '{}'; no fallback execution was attempted",
                    batch_index + 1,
                    column_index + 1,
                    actual_column,
                    expected_column
                )));
            }
        }
        row_count = row_count.checked_add(batch.num_rows()).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local vortex_ingest columnar SourceState row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
    }

    if row_count != source.row_count {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest columnar SourceState row count is {}, expected {}; no fallback execution was attempted",
            row_count, source.row_count
        )));
    }

    Ok(FlatColumnarSourceShape { projected_columns })
}

#[cfg(feature = "vortex-write")]
struct VortexPreparedStateFinalizeInput<'a> {
    target_path: PathBuf,
    column_count: usize,
    column_families: Vec<(String, String)>,
    row_count: u64,
    array: &'a vortex::array::ArrayRef,
    array_build_micros: u128,
    certification_level: VortexIngestCertificationLevel,
    allow_overwrite: bool,
    array_build_provider_kind: &'static str,
    array_build_provider_surface: &'static str,
    array_build_strategy: &'static str,
    array_build_input_layout: &'static str,
    array_build_record_batch_count: usize,
    manual_scalar_copy_avoided: bool,
    capillary_prewrite_control: VortexCapillaryPreWriteControlReport,
    layout_write_decision: VortexLayoutWriteRuntimeDecision,
    preparation_spine: VortexPreparationSpineFinalizeInput,
}

#[cfg(feature = "vortex-write")]
#[derive(Debug, Clone, Copy)]
struct VortexPreparationSpineFinalizeInput {
    vortex_first_decision: &'static str,
    feature_gate: &'static str,
    source_surface: &'static str,
    split_surface: &'static str,
    split_count: usize,
    projection_mask_status: &'static str,
    filter_mask_status: &'static str,
    materialization_boundary_status: &'static str,
    decode_boundary_status: &'static str,
}

#[cfg(feature = "vortex-write")]
fn finalize_vortex_prepared_state_write(
    input: VortexPreparedStateFinalizeInput<'_>,
) -> Result<VortexPreparedStateWriteReport> {
    let mut capillary_prewrite_control = input.capillary_prewrite_control.clone();
    capillary_prewrite_control.apply_task_role_gate("vortex_segment_write", "write")?;
    let write_result = write_vortex_array(&input.target_path, input.array, input.allow_overwrite)?;

    let (
        reopen_row_count,
        reopen_scan_micros,
        reopen_verification_status,
        upstream_vortex_scan_called,
    ) = if input.certification_level == VortexIngestCertificationLevel::IngestCertified {
        capillary_prewrite_control.apply_task_role_gate("reopen_verify", "reopen")?;
        let reopen_start = Instant::now();
        let reopen_row_count = reopen_vortex_row_count(&input.target_path)?;
        let reopen_scan_micros = reopen_start.elapsed().as_micros();
        if write_result.writer_row_count != input.row_count || reopen_row_count != input.row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest row-count proof mismatch: source={} writer={} reopen={reopen_row_count}",
                input.row_count, write_result.writer_row_count
            )));
        }
        (
            reopen_row_count,
            reopen_scan_micros,
            "reopen_row_count_verified".to_string(),
            true,
        )
    } else {
        capillary_prewrite_control.mark_task_role_not_performed("reopen_verify", "reopen");
        if write_result.writer_row_count != input.row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest writer row count mismatch: source={} writer={}; no fallback execution was attempted",
                input.row_count, write_result.writer_row_count
            )));
        }
        (0, 0, "not_performed_ingest_minimal".to_string(), false)
    };
    capillary_prewrite_control.apply_task_role_gate("sink_evidence", "sink_evidence")?;
    let preparation_spine = preparation_spine_report(
        &input,
        upstream_vortex_scan_called,
        &reopen_verification_status,
    );

    Ok(VortexPreparedStateWriteReport {
        target_path: input.target_path,
        row_count: input.row_count,
        column_count: input.column_count,
        column_families: input.column_families,
        bytes_written: write_result.bytes_written,
        artifact_digest: write_result.artifact_digest,
        digest_micros: write_result.digest_micros,
        writer_row_count: write_result.writer_row_count,
        reopen_row_count,
        array_build_micros: input.array_build_micros,
        write_micros: write_result.write_micros,
        writer_context_open_micros: write_result.writer_context_open_micros,
        writer_context_reuse_status: write_result.writer_context_reuse_status,
        vortex_segment_write_micros: write_result.vortex_segment_write_micros,
        workspace_stage_micros: write_result.workspace_stage_micros,
        reopen_scan_micros,
        reopen_verification_status,
        timing_scope: "vortex_ingest_prepare_once".to_string(),
        certification_level: input.certification_level.as_str().to_string(),
        preparation_included: true,
        query_timing_starts_after_preparation: false,
        upstream_vortex_write_called: true,
        upstream_vortex_scan_called,
        array_build_provider_kind: input.array_build_provider_kind.to_string(),
        array_build_provider_surface: input.array_build_provider_surface.to_string(),
        array_build_strategy: input.array_build_strategy.to_string(),
        array_build_input_layout: input.array_build_input_layout.to_string(),
        array_build_record_batch_count: input.array_build_record_batch_count,
        manual_scalar_copy_avoided: input.manual_scalar_copy_avoided,
        preparation_spine,
        layout_write_decision: input.layout_write_decision,
        capillary_prewrite_control,
        workspace_write_report: write_result.workspace_write_report,
    })
}

#[cfg(feature = "vortex-write")]
fn preparation_spine_report(
    input: &VortexPreparedStateFinalizeInput<'_>,
    upstream_vortex_scan_called: bool,
    reopen_verification_status: &str,
) -> VortexPreparationSpineReport {
    let provider_is_vortex = input.array_build_provider_kind == "vortex_array_kernel";
    let provider_crate = if provider_is_vortex {
        "vortex".to_string()
    } else {
        "shardloom-vortex,vortex".to_string()
    };
    let provider_version = if provider_is_vortex {
        VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION.to_string()
    } else {
        format!(
            "shardloom-vortex={};vortex={}",
            env!("CARGO_PKG_VERSION"),
            VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION
        )
    };
    let reopen_provider_surface = if upstream_vortex_scan_called {
        "VortexSession::open_options().open_buffer(...).scan().into_array_stream().read_all()"
    } else {
        "not_invoked_ingest_minimal"
    };
    let native_io_certificate_status = if upstream_vortex_scan_called {
        "certified_local_vortex_preparation_spine"
    } else {
        "minimal_local_vortex_preparation_spine_digest_only"
    };
    let native_io_certificate_refs = if upstream_vortex_scan_called {
        "source_split_refs,prepared_artifact_ref,reopen_row_count_scan"
    } else {
        "source_split_refs,prepared_artifact_ref,artifact_digest"
    };

    VortexPreparationSpineReport {
        schema_version: VORTEX_PREPARATION_SPINE_SCHEMA_VERSION,
        status: "admitted_local_preparation_spine".to_string(),
        vortex_first_decision: input.preparation_spine.vortex_first_decision.to_string(),
        provider_kind: input.array_build_provider_kind.to_string(),
        provider_crate,
        provider_version,
        feature_gate: input.preparation_spine.feature_gate.to_string(),
        provider_api_surface: format!(
            "{};{};{}",
            input.array_build_provider_surface,
            "VortexSession::write_options().write(ArrayStream)",
            reopen_provider_surface
        ),
        shardloom_admission_policy: "scoped_local_vortex_ingest_source_sink_split_prepare_once"
            .to_string(),
        source_surface: input.preparation_spine.source_surface.to_string(),
        sink_surface: "workspace_safe_local_vortex_file_sink".to_string(),
        split_surface: input.preparation_spine.split_surface.to_string(),
        split_ref_status: "reported_by_cli_source_state_boundary".to_string(),
        split_count: input.preparation_spine.split_count,
        projection_mask_status: input.preparation_spine.projection_mask_status.to_string(),
        filter_mask_status: input.preparation_spine.filter_mask_status.to_string(),
        write_provider_surface: "VortexSession::write_options().write(ArrayStream)".to_string(),
        reopen_provider_surface: reopen_provider_surface.to_string(),
        materialization_boundary_status: input
            .preparation_spine
            .materialization_boundary_status
            .to_string(),
        decode_boundary_status: input.preparation_spine.decode_boundary_status.to_string(),
        native_io_certificate_status: native_io_certificate_status.to_string(),
        native_io_certificate_refs: native_io_certificate_refs.to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: format!(
            "Scoped local Vortex preparation spine only: provider/admission/split/write/reopen evidence for flat local SourceState to VortexPreparedState; reopen_verification_status={reopen_verification_status}; no object-store, table, distributed, broad writer, encoded-operator, performance, production, or Spark-replacement claim"
        ),
        fallback_attempted: false,
        external_engine_invoked: false,
    }
}

#[cfg(feature = "vortex-write")]
fn validate_flat_rows(columns: &[String], rows: &[Vec<(String, ScalarValue)>]) -> Result<u64> {
    validate_flat_columns(columns)?;
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest row {} has {} columns, expected {}; no fallback execution was attempted",
                row_index + 1,
                row.len(),
                columns.len()
            )));
        }
        for (column_index, (name, _value)) in row.iter().enumerate() {
            let expected = &columns[column_index];
            if name != expected {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest row {} column {} is '{}', expected '{}'; no fallback execution was attempted",
                    row_index + 1,
                    column_index + 1,
                    name,
                    expected
                )));
            }
        }
    }
    usize_to_u64(rows.len())
}

#[cfg(feature = "vortex-write")]
fn validate_scalar_column_dtype_hints(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
) -> Result<()> {
    if column_dtypes.len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest declared {} column dtype hints for {} columns; no fallback execution was attempted",
            column_dtypes.len(),
            columns.len()
        )));
    }
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn validate_flat_columns(columns: &[String]) -> Result<()> {
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest requires at least one column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local vortex_ingest column names must not be empty; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        if !seen.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest contains duplicate column '{column}'; no fallback execution was attempted"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn scalar_column_families(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<(String, String)>> {
    validate_scalar_column_dtype_hints(columns, column_dtypes)?;
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let mut family = column_dtypes
                .get(column_index)
                .and_then(Option::as_ref)
                .map(|dtype| scalar_family_from_dtype_hint(column, dtype))
                .transpose()?
                .flatten();
            for row in rows {
                let value = &row[column_index].1;
                let Some(candidate) = scalar_family(value) else {
                    if matches!(value, ScalarValue::Null) && family.is_some() {
                        continue;
                    }
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains unsupported value {}; scoped Vortex ingest admits nullable boolean, int64, uint64, float64, utf8, binary, decimal128, date32, and timestamp_micros columns only when the column family is known; no fallback execution was attempted",
                        value.summary()
                    )));
                };
                if let Some(existing) = family {
                    if existing != candidate {
                        let existing = existing.label();
                        let candidate = candidate.label();
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{column}' mixes scalar families {existing} and {candidate}; no fallback execution was attempted"
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((
                column.clone(),
                family.unwrap_or(ScalarFamily::Utf8).label(),
            ))
        })
        .collect()
}

#[cfg(feature = "vortex-write")]
fn scalar_family_from_dtype_hint(
    column: &str,
    dtype: &LogicalDType,
) -> Result<Option<ScalarFamily>> {
    match dtype {
        LogicalDType::Boolean => Ok(Some(ScalarFamily::Boolean)),
        LogicalDType::Int64 => Ok(Some(ScalarFamily::Int64)),
        LogicalDType::UInt64 => Ok(Some(ScalarFamily::UInt64)),
        LogicalDType::Float64 => Ok(Some(ScalarFamily::Float64)),
        LogicalDType::Utf8 => Ok(Some(ScalarFamily::Utf8)),
        LogicalDType::Binary => Ok(Some(ScalarFamily::Binary)),
        LogicalDType::Date32 => Ok(Some(ScalarFamily::Date32)),
        LogicalDType::TimestampMicros => Ok(Some(ScalarFamily::TimestampMicros)),
        LogicalDType::Extension(value) if value.starts_with("decimal128") => {
            let (precision, scale) =
                scalar_decimal128_dtype_precision_scale(column, value)?.ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' has invalid decimal128 dtype hint {value:?}; decimal hints must use decimal128(precision,scale) with 1 <= precision <= 38 and scale <= precision; no fallback execution was attempted"
                    ))
                })?;
            Ok(Some(ScalarFamily::Decimal128 { precision, scale }))
        }
        _ => Ok(None),
    }
}

#[cfg(feature = "vortex-write")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScalarFamily {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Binary,
    Decimal128 { precision: u8, scale: u8 },
    Date32,
    TimestampMicros,
}

#[cfg(feature = "vortex-write")]
impl ScalarFamily {
    fn label(self) -> String {
        match self {
            Self::Boolean => "boolean".to_string(),
            Self::Int64 => "int64".to_string(),
            Self::UInt64 => "uint64".to_string(),
            Self::Float64 => "float64".to_string(),
            Self::Utf8 => "utf8".to_string(),
            Self::Binary => "binary".to_string(),
            Self::Decimal128 { precision, scale } => format!("decimal128({precision},{scale})"),
            Self::Date32 => "date32".to_string(),
            Self::TimestampMicros => "timestamp_micros".to_string(),
        }
    }
}

#[cfg(feature = "vortex-write")]
fn scalar_family(value: &ScalarValue) -> Option<ScalarFamily> {
    match value {
        ScalarValue::Boolean(_) => Some(ScalarFamily::Boolean),
        ScalarValue::Int64(_) => Some(ScalarFamily::Int64),
        ScalarValue::UInt64(_) => Some(ScalarFamily::UInt64),
        ScalarValue::Float64(value) if value.is_finite() => Some(ScalarFamily::Float64),
        ScalarValue::Utf8(_) => Some(ScalarFamily::Utf8),
        ScalarValue::Binary(_) => Some(ScalarFamily::Binary),
        ScalarValue::Decimal128 {
            precision, scale, ..
        } if (1..=38).contains(precision) && scale <= precision => Some(ScalarFamily::Decimal128 {
            precision: *precision,
            scale: *scale,
        }),
        ScalarValue::Date32(_) => Some(ScalarFamily::Date32),
        ScalarValue::TimestampMicros(_) => Some(ScalarFamily::TimestampMicros),
        ScalarValue::Null
        | ScalarValue::Decimal128 { .. }
        | ScalarValue::Float64(_)
        | ScalarValue::List(_)
        | ScalarValue::Struct(_) => None,
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
struct ColumnarVortexArrayBuild {
    array: vortex::array::ArrayRef,
    column_families: Vec<(String, String)>,
    provider_kind: &'static str,
    provider_surface: &'static str,
    strategy: &'static str,
    manual_scalar_copy_avoided: bool,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn flat_columnar_source_to_vortex_struct(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<ColumnarVortexArrayBuild> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let column_families = columnar_column_families(source, source_shape)?;
    if !source.batches.is_empty() {
        let array = flat_columnar_source_to_vortex_from_arrow_provider(source, source_shape)?;
        return Ok(ColumnarVortexArrayBuild {
            array,
            column_families,
            provider_kind: "vortex_array_kernel",
            provider_surface: "ArrayRef::from_arrow(RecordBatch)",
            strategy: "vortex_from_arrow_record_batch",
            manual_scalar_copy_avoided: true,
        });
    }

    let fields = column_families
        .iter()
        .zip(&source_shape.projected_columns)
        .map(|((column, family), projected_column)| {
            let arrays = source
                .batches
                .iter()
                .map(|batch| batch.column(projected_column.reader_index).clone())
                .collect::<Vec<_>>();
            columnar_column_to_vortex_array(column, family, &arrays)
        })
        .collect::<Result<Vec<_>>>()?;
    let array = StructArray::try_new(
        FieldNames::from(
            source
                .materialized_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        ),
        fields,
        source.row_count,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(ColumnarVortexArrayBuild {
        array,
        column_families,
        provider_kind: "shardloom_kernel",
        provider_surface: "shardloom_empty_columnar_struct_builder",
        strategy: "empty_columnar_schema_to_vortex_struct",
        manual_scalar_copy_avoided: false,
    })
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn flat_columnar_source_to_vortex_from_arrow_provider(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::ChunkedArray;
    use vortex::array::arrow::FromArrowArray as _;

    let projection_indices = source_shape
        .projected_columns
        .iter()
        .map(|column| column.reader_index)
        .collect::<Vec<_>>();
    let chunks = source
        .batches
        .iter()
        .map(|batch| {
            let projected = batch.project(&projection_indices).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest Arrow RecordBatch projection failed: {error}; no fallback execution was attempted"
                ))
            })?;
            vortex::array::ArrayRef::from_arrow(projected, false).map_err(vortex_error)
        })
        .collect::<Result<Vec<_>>>()?;
    match chunks.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest columnar SourceState contained no RecordBatch chunks; no fallback execution was attempted"
                .to_string(),
        )),
        _ => {
            let dtype = chunks[0].dtype().clone();
            Ok(ChunkedArray::try_new(chunks, dtype)
                .map_err(vortex_error)?
                .into_array())
        }
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_column_families(
    source: &FlatLocalColumnarSource,
    source_shape: &FlatColumnarSourceShape,
) -> Result<Vec<(String, String)>> {
    source_shape
        .projected_columns
        .iter()
        .map(|projected_column| {
            let mut family = None;
            for batch in &source.batches {
                let array = batch.column(projected_column.reader_index);
                reject_columnar_nulls(&projected_column.column, array.as_ref())?;
                let candidate = arrow_column_family(&projected_column.column, array.as_ref())?;
                if candidate == "float64" {
                    reject_columnar_non_finite_floats(&projected_column.column, array.as_ref())?;
                }
                if let Some(existing) = family {
                    if existing != candidate {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{}' mixes columnar families {existing} and {candidate}; no fallback execution was attempted",
                            projected_column.column
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((
                projected_column.column.clone(),
                match family {
                    Some(family) => family,
                    None => columnar_family_from_dtype_hint(
                        &projected_column.column,
                        projected_column.dtype_hint.as_ref(),
                    )?,
                }
                .to_string(),
            ))
        })
        .collect()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_family_from_dtype_hint(
    column: &str,
    dtype_hint: Option<&LogicalDType>,
) -> Result<&'static str> {
    match dtype_hint {
        Some(LogicalDType::Boolean) => Ok("boolean"),
        Some(LogicalDType::Int64) => Ok("int64"),
        Some(LogicalDType::UInt64) => Ok("uint64"),
        Some(LogicalDType::Float64) => Ok("float64"),
        Some(LogicalDType::Utf8) | None => Ok("utf8"),
        Some(LogicalDType::Binary) => Ok("binary"),
        Some(LogicalDType::Date32) => Ok("date32"),
        Some(LogicalDType::TimestampMicros) => Ok("timestamp_micros"),
        Some(dtype) => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported dtype hint {}; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, binary, date32, and timestamp_micros only; no fallback execution was attempted",
            dtype.as_str()
        ))),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn reject_columnar_nulls(column: &str, array: &dyn Array) -> Result<()> {
    if array.null_count() > 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' contains nulls; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, binary, date32, and timestamp_micros only; no fallback execution was attempted"
        )));
    }
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn reject_columnar_non_finite_floats(column: &str, array: &dyn Array) -> Result<()> {
    if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        for index in 0..array.len() {
            if !array.value(index).is_finite() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest column '{column}' contains non-finite float32; no fallback execution was attempted"
                )));
            }
        }
    } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        for index in 0..array.len() {
            if !array.value(index).is_finite() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest column '{column}' contains non-finite float64; no fallback execution was attempted"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn arrow_column_family(column: &str, array: &dyn Array) -> Result<&'static str> {
    if array.as_any().is::<BooleanArray>() {
        return Ok("boolean");
    }
    if array.as_any().is::<Int8Array>()
        || array.as_any().is::<Int16Array>()
        || array.as_any().is::<Int32Array>()
        || array.as_any().is::<Int64Array>()
    {
        return Ok("int64");
    }
    if array.as_any().is::<UInt8Array>()
        || array.as_any().is::<UInt16Array>()
        || array.as_any().is::<UInt32Array>()
        || array.as_any().is::<UInt64Array>()
    {
        return Ok("uint64");
    }
    if array.as_any().is::<Float32Array>() || array.as_any().is::<Float64Array>() {
        return Ok("float64");
    }
    if array.as_any().is::<StringArray>()
        || array.as_any().is::<LargeStringArray>()
        || array.as_any().is::<StringViewArray>()
    {
        return Ok("utf8");
    }
    if array.as_any().is::<BinaryArray>()
        || array.as_any().is::<LargeBinaryArray>()
        || array.as_any().is::<BinaryViewArray>()
    {
        return Ok("binary");
    }
    if array.as_any().is::<Date32Array>() {
        return Ok("date32");
    }
    if array.as_any().is::<TimestampMicrosecondArray>() {
        return Ok("timestamp_micros");
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' has unsupported Arrow type {:?}; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, binary, date32, and timestamp_micros only; no fallback execution was attempted",
        array.data_type()
    )))
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_column_to_vortex_array(
    column: &str,
    family: &str,
    arrays: &[ArrowArrayRef],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{BoolArray, PrimitiveArray, VarBinViewArray};
    use vortex::array::validity::Validity;
    use vortex::buffer::BitBuffer;

    match family {
        "boolean" => Ok(BoolArray::new(
            BitBuffer::from(columnar_boolean_values(column, arrays)?),
            Validity::NonNullable,
        )
        .into_array()),
        "int64" => Ok(columnar_int64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "uint64" => Ok(columnar_uint64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "float64" => Ok(columnar_float64_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "utf8" => {
            let values = columnar_utf8_values(column, arrays)?;
            Ok(VarBinViewArray::from_iter_str(values.iter().map(String::as_str)).into_array())
        }
        "binary" => {
            let values = columnar_binary_values(column, arrays)?;
            Ok(VarBinViewArray::from_iter_bin(values.iter().map(Vec::as_slice)).into_array())
        }
        "date32" => Ok(columnar_date32_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "timestamp_micros" => Ok(columnar_timestamp_micros_values(column, arrays)?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported columnar family {other}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_boolean_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<bool>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "boolean", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_int64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Int8Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int16Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
            values.extend((0..array.len()).map(|index| i64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "int64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_uint64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<u64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<UInt8Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt16Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
            values.extend((0..array.len()).map(|index| u64::from(array.value(index))));
        } else if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "uint64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_float64_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<f64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
            for index in 0..array.len() {
                let value = array.value(index);
                if !value.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains non-finite float32; no fallback execution was attempted"
                    )));
                }
                values.push(f64::from(value));
            }
        } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
            for index in 0..array.len() {
                let value = array.value(index);
                if !value.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains non-finite float64; no fallback execution was attempted"
                    )));
                }
                values.push(value);
            }
        } else {
            return Err(unexpected_columnar_array(column, "float64", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_utf8_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<String>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_string()));
        } else {
            return Err(unexpected_columnar_array(column, "utf8", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_binary_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<Vec<u8>>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_vec()));
        } else if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_vec()));
        } else if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
            values.extend((0..array.len()).map(|index| array.value(index).to_vec()));
        } else {
            return Err(unexpected_columnar_array(column, "binary", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_date32_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i32>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(column, "date32", array.as_ref()));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_timestamp_micros_values(column: &str, arrays: &[ArrowArrayRef]) -> Result<Vec<i64>> {
    let mut values = Vec::with_capacity(columnar_array_len(arrays));
    for array in arrays {
        if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            values.extend((0..array.len()).map(|index| array.value(index)));
        } else {
            return Err(unexpected_columnar_array(
                column,
                "timestamp_micros",
                array.as_ref(),
            ));
        }
    }
    Ok(values)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn columnar_array_len(arrays: &[ArrowArrayRef]) -> usize {
    arrays.iter().map(arrow_array::Array::len).sum()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn unexpected_columnar_array(column: &str, family: &str, array: &dyn Array) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' expected {family}, found Arrow type {:?}; no fallback execution was attempted",
        array.data_type()
    ))
}

#[cfg(feature = "vortex-write")]
fn flat_rows_to_vortex_struct(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    column_families: &[(String, String)],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let fields = column_families
        .iter()
        .enumerate()
        .map(|(column_index, (_column, family))| {
            column_to_vortex_array(&columns[column_index], column_index, family, rows)
        })
        .collect::<Result<Vec<_>>>()?;

    let array = StructArray::try_new(
        FieldNames::from(columns.iter().map(String::as_str).collect::<Vec<_>>()),
        fields,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(array)
}

#[cfg(feature = "vortex-write")]
fn column_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    match family {
        "boolean" => scalar_boolean_to_vortex_array(column, column_index, family, rows),
        "int64" => scalar_primitive_to_vortex_array(column_index, rows, |value| match value {
            ScalarValue::Int64(value) => Ok(*value),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        }),
        "uint64" => scalar_primitive_to_vortex_array(column_index, rows, |value| match value {
            ScalarValue::UInt64(value) => Ok(*value),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        }),
        "float64" => scalar_primitive_to_vortex_array(column_index, rows, |value| match value {
            ScalarValue::Float64(value) if value.is_finite() => Ok(*value),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        }),
        "utf8" => scalar_utf8_to_vortex_array(column, column_index, family, rows),
        "binary" => scalar_binary_to_vortex_array(column, column_index, family, rows),
        "date32" => scalar_primitive_to_vortex_array(column_index, rows, |value| match value {
            ScalarValue::Date32(value) => Ok(*value),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        }),
        "timestamp_micros" => {
            scalar_primitive_to_vortex_array(column_index, rows, |value| match value {
                ScalarValue::TimestampMicros(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
        }
        family if family.starts_with("decimal128(") => {
            scalar_decimal128_to_vortex_array(column, column_index, family, rows)
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported scalar family {other}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-write")]
fn scalar_boolean_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::BoolArray;

    if rows
        .iter()
        .any(|row| matches!(row[column_index].1, ScalarValue::Null))
    {
        let values = rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Boolean(value) => Ok(Some(*value)),
                ScalarValue::Null => Ok(None),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(BoolArray::from_iter(values).into_array());
    }

    let values = rows
        .iter()
        .map(|row| match &row[column_index].1 {
            ScalarValue::Boolean(value) => Ok(*value),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoolArray::from_iter(values).into_array())
}

#[cfg(feature = "vortex-write")]
fn scalar_primitive_to_vortex_array<T>(
    column_index: usize,
    rows: &[Vec<(String, ScalarValue)>],
    value_from_scalar: impl Fn(&ScalarValue) -> Result<T>,
) -> Result<vortex::array::ArrayRef>
where
    T: vortex::array::dtype::NativePType,
{
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::PrimitiveArray;

    if rows
        .iter()
        .any(|row| matches!(row[column_index].1, ScalarValue::Null))
    {
        let values = rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Null => Ok(None),
                value => value_from_scalar(value).map(Some),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(PrimitiveArray::from_option_iter(values).into_array());
    }

    Ok(rows
        .iter()
        .map(|row| value_from_scalar(&row[column_index].1))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect::<PrimitiveArray>()
        .into_array())
}

#[cfg(feature = "vortex-write")]
fn scalar_utf8_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::VarBinViewArray;

    if rows
        .iter()
        .any(|row| matches!(row[column_index].1, ScalarValue::Null))
    {
        let values = rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Utf8(value) => Ok(Some(value.as_str())),
                ScalarValue::Null => Ok(None),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(VarBinViewArray::from_iter_nullable_str(values).into_array());
    }

    let values = rows
        .iter()
        .map(|row| match &row[column_index].1 {
            ScalarValue::Utf8(value) => Ok(value.as_str()),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(VarBinViewArray::from_iter_str(values).into_array())
}

#[cfg(feature = "vortex-write")]
fn scalar_binary_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::VarBinViewArray;

    if rows
        .iter()
        .any(|row| matches!(row[column_index].1, ScalarValue::Null))
    {
        let values = rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Binary(value) => Ok(Some(value.as_slice())),
                ScalarValue::Null => Ok(None),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(VarBinViewArray::from_iter_nullable_bin(values).into_array());
    }

    let values = rows
        .iter()
        .map(|row| match &row[column_index].1 {
            ScalarValue::Binary(value) => Ok(value.as_slice()),
            value => Err(unexpected_vortex_ingest_value(column, family, value)),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(VarBinViewArray::from_iter_bin(values).into_array())
}

#[cfg(feature = "vortex-write")]
fn scalar_decimal128_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::builders::ArrayBuilder as _;
    use vortex::array::dtype::Nullability;

    let (precision, scale_u8) = scalar_decimal128_family_precision_scale(family)
        .ok_or_else(|| unexpected_vortex_ingest_family(column, family))?;
    let scale_i8 = i8::try_from(scale_u8).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported decimal128 scale {scale_u8}; no fallback execution was attempted"
        ))
    })?;
    let nullability = if rows
        .iter()
        .any(|row| matches!(row[column_index].1, ScalarValue::Null))
    {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    let mut builder = vortex::array::builders::DecimalBuilder::with_capacity::<i128>(
        rows.len(),
        vortex::array::dtype::DecimalDType::new(precision, scale_i8),
        nullability,
    );
    for row in rows {
        match &row[column_index].1 {
            ScalarValue::Decimal128 {
                value,
                precision: value_precision,
                scale: value_scale,
            } if *value_precision == precision && *value_scale == scale_u8 => {
                builder.append_value(*value);
            }
            ScalarValue::Null => builder.append_null(),
            value => return Err(unexpected_vortex_ingest_value(column, family, value)),
        }
    }
    Ok(builder.finish_into_decimal().into_array())
}

#[cfg(feature = "vortex-write")]
fn scalar_decimal128_dtype_precision_scale(column: &str, value: &str) -> Result<Option<(u8, u8)>> {
    if !value.starts_with("decimal128") {
        return Ok(None);
    }
    let invalid_decimal_hint = || {
        ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has invalid decimal128 dtype hint {value:?}; decimal hints must use decimal128(precision,scale) with 1 <= precision <= 38 and scale <= precision; no fallback execution was attempted"
        ))
    };
    let args = value
        .strip_prefix("decimal128(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(invalid_decimal_hint)?;
    let (precision_raw, scale_raw) = args.split_once(',').ok_or_else(invalid_decimal_hint)?;
    let precision = precision_raw
        .trim()
        .parse::<u8>()
        .map_err(|_| invalid_decimal_hint())?;
    let scale = scale_raw
        .trim()
        .parse::<u8>()
        .map_err(|_| invalid_decimal_hint())?;
    if precision == 0 || precision > 38 || scale > precision {
        return Err(invalid_decimal_hint());
    }
    Ok(Some((precision, scale)))
}

#[cfg(feature = "vortex-write")]
fn scalar_decimal128_family_precision_scale(family: &str) -> Option<(u8, u8)> {
    let args = family
        .strip_prefix("decimal128(")
        .and_then(|value| value.strip_suffix(')'))?;
    let (precision, scale) = args.split_once(',')?;
    let precision = precision.parse::<u8>().ok()?;
    let scale = scale.parse::<u8>().ok()?;
    if !(1..=38).contains(&precision) || scale > precision {
        return None;
    }
    Some((precision, scale))
}

#[cfg(feature = "vortex-write")]
fn unexpected_vortex_ingest_family(column: &str, family: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' has unsupported scalar family {family}; no fallback execution was attempted"
    ))
}

#[cfg(feature = "vortex-write")]
fn unexpected_vortex_ingest_value(
    column: &str,
    family: &str,
    value: &ScalarValue,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' expected {family}, found {}; no fallback execution was attempted",
        value.summary()
    ))
}

#[cfg(feature = "vortex-write")]
struct LocalVortexWriteResult {
    writer_row_count: u64,
    bytes_written: u64,
    artifact_digest: String,
    digest_micros: u128,
    write_micros: u128,
    writer_context_open_micros: u128,
    writer_context_reuse_status: String,
    vortex_segment_write_micros: u128,
    workspace_stage_micros: u128,
    workspace_write_report: WorkspaceSafeLocalWriteReport,
}

#[cfg(feature = "vortex-write")]
struct LocalVortexWriteContext {
    runtime: vortex::io::runtime::single::SingleThreadRuntime,
    session: vortex::session::VortexSession,
    open_micros: u128,
}

#[cfg(feature = "vortex-write")]
impl LocalVortexWriteContext {
    fn open() -> Self {
        use vortex::VortexSessionDefault as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let open_start = Instant::now();
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        Self {
            runtime,
            session,
            open_micros: open_start.elapsed().as_micros(),
        }
    }

    fn write_array(
        &self,
        path: &Path,
        array: &vortex::array::ArrayRef,
        allow_overwrite: bool,
        writer_context_reuse_status: impl Into<String>,
    ) -> Result<LocalVortexWriteResult> {
        use vortex::file::WriteOptionsSessionExt as _;

        let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
        let write_start = Instant::now();
        let expected_rows = usize_to_u64(array.len())?;
        let mut vortex_segment_write_micros = 0;
        let (summary, workspace_write_report) =
            shardloom_core::write_workspace_safe_bytes_with_validated_producer(
                workspace_root,
                path,
                allow_overwrite,
                "local vortex_ingest artifact",
                |writer| {
                    let segment_write_start = Instant::now();
                    let result = self
                        .session
                        .write_options()
                        .blocking(&self.runtime)
                        .write(writer, array.to_array_iterator())
                        .map_err(vortex_error);
                    vortex_segment_write_micros = segment_write_start.elapsed().as_micros();
                    result
                },
                |summary| {
                    if summary.row_count() != expected_rows {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest writer row count mismatch: wrote {}, expected {}; staging cleanup attempted; no fallback execution was attempted",
                            summary.row_count(),
                            expected_rows
                        )));
                    }
                    Ok(())
                },
            )?;
        let artifact_digest = workspace_write_report.output_digest.clone();
        let digest_micros = 0;
        let bytes_written = workspace_write_report.bytes_written;
        let write_micros = write_start.elapsed().as_micros();
        let workspace_stage_micros = write_micros.saturating_sub(vortex_segment_write_micros);
        Ok(LocalVortexWriteResult {
            writer_row_count: summary.row_count(),
            bytes_written,
            artifact_digest,
            digest_micros,
            write_micros,
            writer_context_open_micros: self.open_micros,
            writer_context_reuse_status: writer_context_reuse_status.into(),
            vortex_segment_write_micros,
            workspace_stage_micros,
            workspace_write_report,
        })
    }
}

#[cfg(feature = "vortex-write")]
fn write_vortex_array(
    path: &Path,
    array: &vortex::array::ArrayRef,
    allow_overwrite: bool,
) -> Result<LocalVortexWriteResult> {
    let context = LocalVortexWriteContext::open();
    context.write_array(
        path,
        array,
        allow_overwrite,
        "single_write_context_opened_for_artifact",
    )
}

#[cfg(feature = "vortex-write")]
fn reopen_vortex_row_count(path: &Path) -> Result<u64> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to reopen local vortex_ingest artifact '{}': {error}",
            path.display()
        ))
    })?;
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session
        .open_options()
        .open_buffer(bytes)
        .map_err(vortex_error)?;
    let array = runtime
        .block_on(
            file.scan()
                .map_err(vortex_error)?
                .into_array_stream()
                .map_err(vortex_error)?
                .read_all(),
        )
        .map_err(vortex_error)?;
    usize_to_u64(array.len())
}

#[cfg(feature = "vortex-write")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest upstream Vortex API failed: {error}; no fallback execution was attempted"
    ))
}

fn fnv64_digest_text(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

#[cfg(feature = "vortex-write")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "local vortex_ingest value does not fit in u64".to_string(),
        )
    })
}

#[cfg(all(test, feature = "vortex-write"))]
mod tests {
    use super::*;

    #[cfg(feature = "universal-format-io")]
    fn reopen_vortex_artifact_as_arrow_struct(
        path: &Path,
        schema: &arrow_schema::Schema,
    ) -> arrow_array::ArrayRef {
        use vortex::VortexSessionDefault as _;
        use vortex::array::VortexSessionExecute as _;
        use vortex::array::arrow::ArrowSessionExt as _;
        use vortex::array::stream::ArrayStreamExt as _;
        use vortex::file::OpenOptionsSessionExt as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let bytes = std::fs::read(path).expect("read vortex artifact");
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let file = session
            .open_options()
            .open_buffer(bytes)
            .expect("open vortex artifact");
        let array = runtime
            .block_on(
                file.scan()
                    .expect("scan vortex artifact")
                    .into_array_stream()
                    .expect("array stream")
                    .read_all(),
            )
            .expect("read vortex array");
        let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
        let target = arrow_schema::Field::new(
            "",
            arrow_schema::DataType::Struct(schema.fields.clone()),
            false,
        );
        vortex::array::LEGACY_SESSION
            .arrow()
            .execute_arrow(array, Some(&target), &mut ctx)
            .expect("execute vortex artifact to arrow struct")
    }

    fn temp_test_root(name: &str) -> PathBuf {
        let mut root = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        root.push(format!(
            "shardloom-vortex-ingest-{name}-{}-{nanos}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp root");
        root
    }

    fn reuse_request_for_test(
        source: &Path,
        target: &Path,
        manifest_path: &Path,
    ) -> VortexPreparedStateReuseRequest {
        VortexPreparedStateReuseRequest::new_local(
            source,
            target,
            manifest_path.to_path_buf(),
            "csv",
            None,
            "fnv64:parse-plan",
            "all_columns",
            "caller_owned_local_vortex_artifact",
            "test-provider",
            "vortex-write",
            "ingest_certified",
        )
        .expect("reuse request")
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn local_flat_scalar_rows_write_and_reopen_vortex_artifact() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec![
                "id".to_string(),
                "label".to_string(),
                "metric".to_string(),
                "active".to_string(),
            ],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(1.5)),
                    ("active".to_string(), ScalarValue::Boolean(true)),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(2.5)),
                    ("active".to_string(), ScalarValue::Boolean(false)),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.reopen_verification_status,
            "reopen_row_count_verified"
        );
        assert!(report.artifact_digest.starts_with("fnv64:"));
        assert_eq!(report.digest_micros, 0);
        assert_eq!(
            report.workspace_write_report.bytes_written,
            report.bytes_written
        );
        assert_eq!(
            report.workspace_write_report.output_digest,
            report.artifact_digest
        );
        assert_eq!(report.workspace_write_report.commit_status, "committed");
        assert_eq!(
            report.workspace_write_report.commit_mode,
            "atomic_rename_same_directory"
        );
        assert_eq!(
            report.writer_context_reuse_status,
            "single_write_context_opened_for_artifact"
        );
        assert!(report.write_micros >= report.vortex_segment_write_micros);
        assert_eq!(
            report.workspace_stage_micros,
            report
                .write_micros
                .saturating_sub(report.vortex_segment_write_micros)
        );
        assert!(!report.workspace_write_report.staging_path.exists());
        assert!(report.workspace_write_report.no_fallback_invariant_holds());
        assert_eq!(report.timing_scope, "vortex_ingest_prepare_once");
        assert_eq!(report.certification_level, "ingest_certified");
        assert_eq!(
            report.column_family_summary(),
            "id:int64,label:utf8,metric:float64,active:boolean"
        );
        assert!(report.preparation_included);
        assert!(!report.query_timing_starts_after_preparation);
        assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "shardloom_scalar_rows_to_vortex_struct"
        );
        assert_eq!(report.array_build_strategy, "scalar_rows_to_vortex_struct");
        assert_eq!(report.array_build_input_layout, "materialized_rows");
        assert_eq!(report.array_build_record_batch_count, 0);
        assert!(!report.manual_scalar_copy_avoided);
        assert!(!report.layout_write_decision.runtime_decision_applied);
        assert_eq!(
            report.layout_write_decision.blocker,
            "layout_write_advisor_not_attached_to_writer"
        );
        assert_eq!(
            report.preparation_spine.schema_version,
            VORTEX_PREPARATION_SPINE_SCHEMA_VERSION
        );
        assert_eq!(
            report.preparation_spine.status,
            "admitted_local_preparation_spine"
        );
        assert_eq!(
            report.preparation_spine.vortex_first_decision,
            "implement_shardloom_kernel"
        );
        assert_eq!(report.preparation_spine.provider_kind, "shardloom_kernel");
        assert_eq!(
            report.preparation_spine.source_surface,
            "local_text_source_state_scalar_rows"
        );
        assert_eq!(report.preparation_spine.split_count, 1);
        assert_eq!(
            report.preparation_spine.native_io_certificate_status,
            "certified_local_vortex_preparation_spine"
        );
        assert!(!report.preparation_spine.fallback_attempted);
        assert!(!report.preparation_spine.external_engine_invoked);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_scalar_binary_rows_write_reopens_exact_bytes() {
        use arrow_array::{BinaryArray, StructArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-binary-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "payload".to_string(), "label".to_string()];
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            columns,
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    (
                        "payload".to_string(),
                        ScalarValue::Binary(vec![0x00, 0xff, 0x10]),
                    ),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("payload".to_string(), ScalarValue::Binary(Vec::new())),
                    ("label".to_string(), ScalarValue::Utf8("empty".to_string())),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(3)),
                    ("payload".to_string(), ScalarValue::Binary(b"raw".to_vec())),
                    ("label".to_string(), ScalarValue::Utf8("omega".to_string())),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 3);
        assert_eq!(report.reopen_row_count, 3);
        assert_eq!(
            report.column_family_summary(),
            "id:int64,payload:binary,label:utf8"
        );
        assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "shardloom_scalar_rows_to_vortex_struct"
        );
        assert!(report.upstream_vortex_write_called);
        assert!(report.upstream_vortex_scan_called);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let payload = struct_array
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary payload column");
        assert_eq!(payload.value(0), &[0x00, 0xff, 0x10]);
        assert_eq!(payload.value(1), &[] as &[u8]);
        assert_eq!(payload.value(2), b"raw");
        assert_eq!(payload.null_count(), 0);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_scalar_all_null_binary_rows_write_reopens_binary() {
        use arrow_array::{Array as _, BinaryArray, StructArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-binary-null-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["payload".to_string()],
            vec![
                vec![("payload".to_string(), ScalarValue::Null)],
                vec![("payload".to_string(), ScalarValue::Null)],
            ],
        )
        .column_dtypes(vec![Some(LogicalDType::Binary)]);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(report.column_family_summary(), "payload:binary");
        let schema = Schema::new(vec![Field::new("payload", DataType::Binary, true)]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let payload = struct_array
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary payload column");
        assert_eq!(payload.len(), 2);
        assert!(payload.is_null(0));
        assert!(payload.is_null(1));
        assert_eq!(payload.null_count(), 2);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn local_flat_scalar_all_null_typed_rows_write_reopens_nullable_scalars() {
        use arrow_array::{
            Array as _, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
            StructArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow_schema::{DataType, Field, Schema, TimeUnit};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-scalar-null-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec![
            "active".to_string(),
            "id".to_string(),
            "count".to_string(),
            "metric".to_string(),
            "label".to_string(),
            "event_day".to_string(),
            "event_ts".to_string(),
        ];
        let null_row = || {
            vec![
                ("active".to_string(), ScalarValue::Null),
                ("id".to_string(), ScalarValue::Null),
                ("count".to_string(), ScalarValue::Null),
                ("metric".to_string(), ScalarValue::Null),
                ("label".to_string(), ScalarValue::Null),
                ("event_day".to_string(), ScalarValue::Null),
                ("event_ts".to_string(), ScalarValue::Null),
            ]
        };
        let request =
            VortexPreparedStateWriteRequest::new(&path, columns, vec![null_row(), null_row()])
                .column_dtypes(vec![
                    Some(LogicalDType::Boolean),
                    Some(LogicalDType::Int64),
                    Some(LogicalDType::UInt64),
                    Some(LogicalDType::Float64),
                    Some(LogicalDType::Utf8),
                    Some(LogicalDType::Date32),
                    Some(LogicalDType::TimestampMicros),
                ]);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.column_family_summary(),
            "active:boolean,id:int64,count:uint64,metric:float64,label:utf8,event_day:date32,event_ts:timestamp_micros"
        );
        let schema = Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("id", DataType::Int64, true),
            Field::new("count", DataType::UInt64, true),
            Field::new("metric", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("event_day", DataType::Date32, true),
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");

        assert_eq!(
            struct_array
                .column_by_name("active")
                .expect("active column")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("count")
                .expect("count column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("metric")
                .expect("metric column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("label")
                .expect("label column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("event_day")
                .expect("event_day column")
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date32 column")
                .null_count(),
            2
        );
        assert_eq!(
            struct_array
                .column_by_name("event_ts")
                .expect("event_ts column")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp_micros column")
                .null_count(),
            2
        );

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn local_flat_scalar_mixed_null_rows_write_reopens_nullable_scalars() {
        use arrow_array::{
            Array as _, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
            StructArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow_schema::{DataType, Field, Schema, TimeUnit};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-scalar-mixed-null-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec![
                "active".to_string(),
                "id".to_string(),
                "count".to_string(),
                "metric".to_string(),
                "label".to_string(),
                "event_day".to_string(),
                "event_ts".to_string(),
            ],
            vec![
                vec![
                    ("active".to_string(), ScalarValue::Boolean(true)),
                    ("id".to_string(), ScalarValue::Int64(-10)),
                    ("count".to_string(), ScalarValue::UInt64(7)),
                    ("metric".to_string(), ScalarValue::Float64(1.5)),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                    ("event_day".to_string(), ScalarValue::Date32(20_000)),
                    (
                        "event_ts".to_string(),
                        ScalarValue::TimestampMicros(1_700_000_000_000_000),
                    ),
                ],
                vec![
                    ("active".to_string(), ScalarValue::Null),
                    ("id".to_string(), ScalarValue::Null),
                    ("count".to_string(), ScalarValue::Null),
                    ("metric".to_string(), ScalarValue::Null),
                    ("label".to_string(), ScalarValue::Null),
                    ("event_day".to_string(), ScalarValue::Null),
                    ("event_ts".to_string(), ScalarValue::Null),
                ],
                vec![
                    ("active".to_string(), ScalarValue::Boolean(false)),
                    ("id".to_string(), ScalarValue::Int64(20)),
                    ("count".to_string(), ScalarValue::UInt64(8)),
                    ("metric".to_string(), ScalarValue::Float64(2.5)),
                    ("label".to_string(), ScalarValue::Utf8("omega".to_string())),
                    ("event_day".to_string(), ScalarValue::Date32(20_001)),
                    (
                        "event_ts".to_string(),
                        ScalarValue::TimestampMicros(1_700_000_000_001_000),
                    ),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 3);
        assert_eq!(report.reopen_row_count, 3);
        assert_eq!(
            report.column_family_summary(),
            "active:boolean,id:int64,count:uint64,metric:float64,label:utf8,event_day:date32,event_ts:timestamp_micros"
        );
        let schema = Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("id", DataType::Int64, true),
            Field::new("count", DataType::UInt64, true),
            Field::new("metric", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("event_day", DataType::Date32, true),
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let active = struct_array
            .column_by_name("active")
            .expect("active column")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column");
        assert!(active.value(0));
        assert!(active.is_null(1));
        assert!(!active.value(2));
        assert_eq!(active.null_count(), 1);
        let id = struct_array
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(id.value(0), -10);
        assert!(id.is_null(1));
        assert_eq!(id.value(2), 20);
        let count = struct_array
            .column_by_name("count")
            .expect("count column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 column");
        assert_eq!(count.value(0), 7);
        assert!(count.is_null(1));
        assert_eq!(count.value(2), 8);
        let metric = struct_array
            .column_by_name("metric")
            .expect("metric column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 column");
        assert!((metric.value(0) - 1.5).abs() < f64::EPSILON);
        assert!(metric.is_null(1));
        assert!((metric.value(2) - 2.5).abs() < f64::EPSILON);
        let label = struct_array
            .column_by_name("label")
            .expect("label column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        assert_eq!(label.value(0), "alpha");
        assert!(label.is_null(1));
        assert_eq!(label.value(2), "omega");
        let event_day = struct_array
            .column_by_name("event_day")
            .expect("event_day column")
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 column");
        assert_eq!(event_day.value(0), 20_000);
        assert!(event_day.is_null(1));
        assert_eq!(event_day.value(2), 20_001);
        let event_ts = struct_array
            .column_by_name("event_ts")
            .expect("event_ts column")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp_micros column");
        assert_eq!(event_ts.value(0), 1_700_000_000_000_000);
        assert!(event_ts.is_null(1));
        assert_eq!(event_ts.value(2), 1_700_000_000_001_000);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_scalar_decimal_rows_write_reopens_decimal128() {
        use arrow_array::{Decimal128Array, StructArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-decimal-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "amount".to_string()],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    (
                        "amount".to_string(),
                        ScalarValue::Decimal128 {
                            value: 1234,
                            precision: 10,
                            scale: 2,
                        },
                    ),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    (
                        "amount".to_string(),
                        ScalarValue::Decimal128 {
                            value: -800,
                            precision: 10,
                            scale: 2,
                        },
                    ),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.column_family_summary(),
            "id:int64,amount:decimal128(10,2)"
        );
        assert_eq!(report.array_build_provider_kind, "shardloom_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "shardloom_scalar_rows_to_vortex_struct"
        );
        assert!(report.upstream_vortex_write_called);
        assert!(report.upstream_vortex_scan_called);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(10, 2), false),
        ]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let amount = struct_array
            .column_by_name("amount")
            .expect("amount column")
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal amount column");
        assert_eq!(amount.value(0), 1234);
        assert_eq!(amount.value(1), -800);
        assert_eq!(amount.null_count(), 0);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_scalar_all_null_decimal_rows_write_reopens_decimal128() {
        use arrow_array::{Array as _, Decimal128Array, StructArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-decimal-null-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["amount".to_string()],
            vec![
                vec![("amount".to_string(), ScalarValue::Null)],
                vec![("amount".to_string(), ScalarValue::Null)],
            ],
        )
        .column_dtypes(vec![Some(LogicalDType::Extension(
            "decimal128(10,2)".to_string(),
        ))]);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(report.column_family_summary(), "amount:decimal128(10,2)");
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]);
        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, &schema);
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let amount = struct_array
            .column_by_name("amount")
            .expect("amount column")
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal amount column");
        assert_eq!(amount.len(), 2);
        assert!(amount.is_null(0));
        assert!(amount.is_null(1));
        assert_eq!(amount.null_count(), 2);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[test]
    fn local_flat_scalar_decimal_rows_reject_mixed_precision_scale() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-decimal-mixed-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["amount".to_string()],
            vec![
                vec![(
                    "amount".to_string(),
                    ScalarValue::Decimal128 {
                        value: 1234,
                        precision: 10,
                        scale: 2,
                    },
                )],
                vec![(
                    "amount".to_string(),
                    ScalarValue::Decimal128 {
                        value: 1234,
                        precision: 12,
                        scale: 2,
                    },
                )],
            ],
        );

        let error = write_flat_scalar_vortex_prepared_state(request)
            .expect_err("mixed decimal precision/scale should block");

        assert!(
            error.to_string().contains(
                "local vortex_ingest column 'amount' mixes scalar families decimal128(10,2) and decimal128(12,2)"
            ),
            "{error}"
        );
        assert!(!path.exists());
    }

    #[test]
    fn local_flat_scalar_rows_apply_layout_write_advisor_before_write() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-layout-advisor-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let advisor = evaluate_vortex_layout_write_advisor(layout_advisor_input(true, "none"));
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .layout_write_advisor(advisor);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert!(report.layout_write_decision.runtime_decision_applied);
        assert_eq!(
            report.layout_write_decision.selected_strategy,
            "single_local_vortex_artifact"
        );
        assert!(
            report
                .layout_write_decision
                .strategy_decision_digest
                .starts_with("fnv64:")
        );
        assert!(report.layout_write_decision.provider_admitted);
        assert_eq!(report.layout_write_decision.blocker, "none");
        assert_eq!(report.reopen_row_count, 1);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[test]
    fn local_flat_scalar_rows_block_unsupported_layout_strategy_before_write() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-layout-advisor-blocked-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let advisor = evaluate_vortex_layout_write_advisor(layout_advisor_input(
            false,
            "vortex_layout_write_advisor.unsupported_layout_strategy",
        ));
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .layout_write_advisor(advisor);

        let error = write_flat_scalar_vortex_prepared_state(request)
            .expect_err("unsupported layout strategy must block");

        assert!(error.to_string().contains(
            "vortex_layout_write_advisor.status_not_admitted:blocked_layout_write_strategy"
        ));
        assert!(
            !path.exists(),
            "blocked layout/write advisor must not create {}",
            path.display()
        );
    }

    #[test]
    fn local_flat_scalar_minimal_ingest_skips_reopen_scan() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-minimal-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .certification_level(VortexIngestCertificationLevel::IngestMinimal);

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 1);
        assert_eq!(report.writer_row_count, 1);
        assert_eq!(report.reopen_row_count, 0);
        assert_eq!(
            report.reopen_verification_status,
            "not_performed_ingest_minimal"
        );
        assert_eq!(report.certification_level, "ingest_minimal");
        assert!(report.upstream_vortex_write_called);
        assert!(!report.upstream_vortex_scan_called);
        assert_eq!(
            report.preparation_spine.native_io_certificate_status,
            "minimal_local_vortex_preparation_spine_digest_only"
        );
        assert_eq!(
            report.preparation_spine.reopen_provider_surface,
            "not_invoked_ingest_minimal"
        );
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[test]
    fn local_flat_scalar_rows_apply_capillary_prewrite_control_before_write() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-capillary-prewrite-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                ],
            ],
        )
        .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 2));

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert!(report.capillary_prewrite_control.scheduler_applied);
        assert_eq!(
            report.capillary_prewrite_control.status,
            "applied_before_array_build"
        );
        assert_eq!(
            report.capillary_prewrite_control.array_build_gate_status,
            "applied_prewrite_window"
        );
        assert_eq!(
            report.capillary_prewrite_control.write_gate_status,
            "applied_prewrite_window"
        );
        assert_eq!(
            report.capillary_prewrite_control.reopen_gate_status,
            "applied_prewrite_window"
        );
        assert_eq!(
            report.capillary_prewrite_control.sink_evidence_gate_status,
            "applied_prewrite_window"
        );
        assert!(report.capillary_prewrite_control.execution_window_count > 0);
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn local_flat_columnar_source_writes_and_reopens_vortex_artifact() {
        use std::sync::Arc;

        use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec![
            "id".to_string(),
            "label".to_string(),
            "metric".to_string(),
            "active".to_string(),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("metric", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            column_dtypes: vec![None; columns.len()],
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source)
            .capillary_prewrite_input(capillary_prewrite_test_input(&path, 2, 4));

        let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert_eq!(
            report.column_family_summary(),
            "id:int64,label:utf8,metric:float64,active:boolean"
        );
        assert_eq!(
            report.reopen_verification_status,
            "reopen_row_count_verified"
        );
        assert!(report.upstream_vortex_write_called);
        assert!(report.upstream_vortex_scan_called);
        assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "ArrayRef::from_arrow(RecordBatch)"
        );
        assert_eq!(
            report.array_build_strategy,
            "vortex_from_arrow_record_batch"
        );
        assert_eq!(
            report.array_build_input_layout,
            "arrow_record_batch_columnar_source_state"
        );
        assert_eq!(report.array_build_record_batch_count, 1);
        assert!(report.manual_scalar_copy_avoided);
        assert_eq!(
            report.preparation_spine.vortex_first_decision,
            "use_vortex_native_provider"
        );
        assert_eq!(
            report.preparation_spine.provider_kind,
            "vortex_array_kernel"
        );
        assert_eq!(report.preparation_spine.provider_crate, "vortex");
        assert_eq!(
            report.preparation_spine.provider_version,
            VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION
        );
        assert_eq!(
            report.preparation_spine.source_surface,
            "local_columnar_source_state_arrow_record_batches"
        );
        assert_eq!(report.preparation_spine.split_count, 1);
        assert_eq!(
            report.preparation_spine.materialization_boundary_status,
            "columnar_source_state_preserved_to_vortex_array_provider"
        );
        assert_eq!(
            report.preparation_spine.decode_boundary_status,
            "no_scalar_row_decode_for_non_empty_batches"
        );
        assert!(report.capillary_prewrite_control.scheduler_applied);
        assert_eq!(
            report.capillary_prewrite_control.array_build_gate_status,
            "applied_prewrite_window"
        );
        assert_eq!(
            report.capillary_prewrite_control.write_gate_status,
            "applied_prewrite_window"
        );
        assert_eq!(
            report.capillary_prewrite_control.reopen_gate_status,
            "applied_prewrite_window"
        );
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_binary_source_writes_reopens_exact_bytes() {
        use std::sync::Arc;

        use arrow_array::{BinaryArray, Int64Array, RecordBatch, StructArray};
        use arrow_schema::{DataType, Field, Schema};
        use shardloom_core::LogicalDType;

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-binary-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "payload".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0x00, 0xff, 0x10][..]),
                    Some(&[][..]),
                    Some(&b"raw"[..]),
                ])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            column_dtypes: vec![None, Some(LogicalDType::Binary)],
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 3,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let report = write_flat_columnar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 3);
        assert_eq!(report.reopen_row_count, 3);
        assert_eq!(report.column_family_summary(), "id:int64,payload:binary");
        assert_eq!(report.array_build_provider_kind, "vortex_array_kernel");
        assert_eq!(
            report.array_build_provider_surface,
            "ArrayRef::from_arrow(RecordBatch)"
        );
        assert_eq!(
            report.preparation_spine.materialization_boundary_status,
            "columnar_source_state_preserved_to_vortex_array_provider"
        );
        assert_eq!(
            report.preparation_spine.decode_boundary_status,
            "no_scalar_row_decode_for_non_empty_batches"
        );
        assert!(report.manual_scalar_copy_avoided);

        let arrow = reopen_vortex_artifact_as_arrow_struct(&path, schema.as_ref());
        let struct_array = arrow
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("arrow struct");
        let payload = struct_array
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary payload column");
        assert_eq!(payload.value(0), &[0x00, 0xff, 0x10]);
        assert_eq!(payload.value(1), &[] as &[u8]);
        assert_eq!(payload.value(2), b"raw");
        assert_eq!(payload.null_count(), 0);

        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_record_batch_rejects_non_finite_float_before_provider_path() {
        use std::sync::Arc;

        use arrow_array::{Float64Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-non-finite-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "metric".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("metric", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            column_dtypes: vec![None; columns.len()],
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let error = write_flat_columnar_vortex_prepared_state(request)
            .expect_err("non-finite float should be rejected");

        assert!(error.to_string().contains("non-finite float64"));
        assert!(!path.exists());
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_source_rejects_short_batches_before_column_access() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-short-batch-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .expect("record batch");
        let columns = vec!["id".to_string(), "label".to_string(), "metric".to_string()];
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            column_dtypes: vec![None; columns.len()],
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let error = write_flat_columnar_vortex_prepared_state(request)
            .expect_err("short batch must be rejected before column access");

        assert!(
            error
                .to_string()
                .contains("columnar SourceState batch 1 has 2 projected columns, expected 3")
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
        assert!(!path.exists());
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn local_flat_columnar_source_rejects_row_count_mismatch() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-columnar-row-count-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let columns = vec!["id".to_string(), "label".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .expect("record batch");
        let source = FlatLocalColumnarSource {
            header: columns.clone(),
            column_dtypes: vec![None; columns.len()],
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 3,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

        let error = write_flat_columnar_vortex_prepared_state(request)
            .expect_err("row count mismatch must be rejected");

        assert!(
            error
                .to_string()
                .contains("columnar SourceState row count is 2, expected 3")
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
        assert!(!path.exists());
    }

    #[test]
    fn local_flat_scalar_full_replay_is_blocked_without_output_replay() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-full-replay-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string()],
            vec![vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ]],
        )
        .certification_level(VortexIngestCertificationLevel::IngestFullReplay);

        let error = write_flat_scalar_vortex_prepared_state(request)
            .expect_err("full replay requires downstream output evidence");

        assert!(
            error
                .to_string()
                .contains("ingest_full_replay requires downstream result replay/output evidence")
        );
        assert!(!path.exists());
    }

    #[test]
    fn differential_preparation_admits_append_only_delta_overlay() {
        let input = differential_input(VortexDifferentialUpdateMode::AppendOnly);

        let report = evaluate_vortex_differential_preparation(input);
        let fields = report.evidence_fields();

        assert!(report.is_admitted());
        assert_eq!(report.status, "admitted_append_only_delta_overlay");
        assert_eq!(
            report.schema_compatibility_status,
            "compatible_source_schema_and_column_families"
        );
        assert_eq!(
            report.prepared_state_reuse_status,
            "base_prepared_state_reused_for_delta_overlay"
        );
        assert_eq!(
            report.native_io_certificate_status,
            "certified_local_vortex_differential_preparation_overlay"
        );
        assert!(report.overlay_manifest_digest.starts_with("fnv64:"));
        assert!(report.correctness_digest.starts_with("fnv64:"));
        assert!(fields.contains(&(
            "vortex_differential_preparation_overlay_applied".to_string(),
            "true".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_differential_preparation_fallback_attempted".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn capillary_preparation_applies_pulseweave_when_certified() {
        let input = capillary_input("certified");

        let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");
        let fields = report.evidence_fields();

        assert_eq!(report.status, "applied_capillary_pulseweave_control");
        assert_eq!(report.task_count, 6);
        assert!(report.task_manifest_digest.starts_with("fnv64:"));
        assert_eq!(report.native_io_certificate_status, "certified");
        assert_eq!(report.pulseweave_report.status, "applied");
        assert!(report.pulseweave_report.runtime_decision_applied);
        assert!(report.scheduler_applied);
        assert_eq!(
            report.scheduler_application_reason,
            "pulseweave_batch_window_applied_to_capillary_manifest"
        );
        assert!(report.execution_window_size >= 1);
        assert_eq!(
            report.execution_window_count,
            report.task_count.div_ceil(report.execution_window_size)
        );
        assert_ne!(report.execution_window_ids, "none");
        assert_ne!(report.execution_window_task_ids, "none");
        assert_ne!(report.execution_window_digests, "none");
        assert_eq!(
            report.pulseweave_report.application_scope,
            "vortex_cold_preparation_local_capillary_io"
        );
        assert!(fields.contains(&(
            "vortex_capillary_preparation_task_roles".to_string(),
            "source_split_discovery,read_chunk,columnarize_encode,vortex_segment_write,reopen_verify,sink_evidence".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_pulseweave_runtime_decision_applied".to_string(),
            "true".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_scheduler_applied".to_string(),
            "true".to_string()
        )));
        assert!(fields.iter().any(|(key, value)| {
            key == "vortex_capillary_preparation_execution_window_count" && value != "0"
        }));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_fallback_attempted".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn capillary_preparation_skips_small_input_below_threshold() {
        let mut input = capillary_input("certified");
        input.capillary_claim_evidence_requested = false;

        let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");
        let fields = report.evidence_fields();

        assert_eq!(report.status, "not_requested_below_threshold");
        assert_eq!(report.activation_result, "skipped");
        assert_eq!(
            report.activation_reason,
            "below_threshold_small_local_fixture"
        );
        assert_eq!(report.task_count, 0);
        assert_eq!(report.task_manifest_id, "none");
        assert_eq!(report.execution_window_count, 0);
        assert_eq!(report.execution_window_size, 0);
        assert_eq!(report.execution_window_ids, "none");
        assert!(!report.scheduler_applied);
        assert_eq!(
            report.scheduler_application_reason,
            "not_requested_below_threshold"
        );
        assert_eq!(report.pulseweave_report.status, "not_requested");
        assert!(!report.pulseweave_report.runtime_decision_applied);
        assert_eq!(report.execution_certificate_status, "not_requested");
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(fields.contains(&(
            "vortex_capillary_preparation_activation_policy".to_string(),
            "dynamic_size_complexity_gate.v1".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_activation_result".to_string(),
            "skipped".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_task_count".to_string(),
            "0".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_pulseweave_status".to_string(),
            "not_requested".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_scheduler_applied".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_capillary_preparation_execution_window_count".to_string(),
            "0".to_string()
        )));
    }

    #[test]
    fn capillary_preparation_activates_large_input_by_threshold() {
        let mut input = capillary_input("certified");
        input.capillary_claim_evidence_requested = false;
        input.source_byte_count = VORTEX_CAPILLARY_ACTIVATION_THRESHOLD_BYTES;

        let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

        assert_eq!(report.status, "applied_capillary_pulseweave_control");
        assert_eq!(report.activation_result, "activated");
        assert_eq!(report.activation_reason, "source_bytes_above_threshold");
        assert_eq!(report.task_count, 6);
        assert!(report.scheduler_applied);
        assert!(report.execution_window_count > 0);
        assert_eq!(report.pulseweave_report.status, "applied");
    }

    #[test]
    fn capillary_preparation_blocks_none_source_split_refs_as_missing_manifest() {
        let mut input = capillary_input("certified");
        input.source_split_refs = "none".to_string();

        let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

        assert_eq!(report.status, "blocked_missing_capillary_task_manifest");
        assert_eq!(
            report.execution_certificate_status,
            "missing_capillary_task_manifest"
        );
        assert_eq!(report.pulseweave_report.status, "blocked");
        assert!(!report.pulseweave_report.runtime_decision_applied);
        assert!(!report.scheduler_applied);
        assert_eq!(
            report.scheduler_application_reason,
            "blocked_missing_capillary_task_manifest"
        );
        assert_eq!(report.execution_window_count, 0);
        assert!(
            report
                .pulseweave_report
                .blocker
                .contains("execution_certificate")
        );
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn scout_ingress_admits_clean_local_source() {
        let input = scout_input(0, "none", "not_detected", false);

        let report = evaluate_vortex_scout_ingress(input);
        let fields = report.evidence_fields();

        assert_eq!(report.status, "admitted_scout_ingress_clean");
        assert_eq!(
            report.no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
        );
        assert!(!report.quarantine_required);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(fields.contains(&(
            "vortex_scout_ingress_anomaly_count".to_string(),
            "0".to_string()
        )));
        assert!(fields.contains(&(
            "vortex_scout_ingress_fallback_attempted".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn scout_ingress_blocks_nested_shapes_with_quarantine_plan() {
        let input = scout_input(
            1,
            "unsupported_nested_shape",
            "blocked_unsupported_nested_shape",
            true,
        );

        let report = evaluate_vortex_scout_ingress(input);

        assert_eq!(report.status, "blocked_unsupported_nested_shape");
        assert_eq!(report.anomaly_families, "unsupported_nested_shape");
        assert_eq!(
            report.unsupported_shape_status,
            "blocked_unsupported_nested_shape"
        );
        assert!(report.quarantine_required);
        assert_eq!(
            report.quarantine_output_plan_status,
            "planned_not_emitted_no_quarantine_sink_requested"
        );
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn layout_write_advisor_admits_scoped_local_strategy() {
        let report = evaluate_vortex_layout_write_advisor(layout_advisor_input(true, "none"));

        assert_eq!(report.status, "admitted_local_layout_write_strategy");
        assert!(report.strategy_admitted);
        assert!(!report.runtime_decision_applied);
        assert_eq!(report.selected_strategy, "single_local_vortex_artifact");
        assert!(report.strategy_decision_digest.starts_with("fnv64:"));
        assert!(report.provider_admitted);
        assert_eq!(report.blocker, "pending_runtime_write_decision");
        assert_eq!(
            report.no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
        );
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn layout_write_advisor_blocks_unsupported_strategy() {
        let report = evaluate_vortex_layout_write_advisor(layout_advisor_input(
            false,
            "vortex_layout_write_advisor.unsupported_layout_strategy",
        ));

        assert_eq!(report.status, "blocked_layout_write_strategy");
        assert!(!report.strategy_admitted);
        assert!(!report.runtime_decision_applied);
        assert_eq!(report.selected_strategy, "not_admitted");
        assert!(!report.provider_admitted);
        assert_eq!(
            report.blocker,
            "vortex_layout_write_advisor.unsupported_layout_strategy"
        );
        assert_eq!(
            report.unsupported_diagnostic_code,
            "vortex_layout_write_advisor.unsupported_layout_strategy"
        );
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn copy_budget_reports_unmeasured_segments_and_blocks_unsafe_reuse() {
        let report = evaluate_vortex_copy_budget(copy_budget_input(
            "reported_with_not_measured_segments",
            "blocked_no_unsafe_lifetime_shortcuts",
            "blocked_until_correctness_parity",
        ));

        assert_eq!(
            report.status,
            "reported_copy_budget_with_unmeasured_segments"
        );
        assert_eq!(report.buffer_reuse_count, 0);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.fallback_attempted);

        let blocked = evaluate_vortex_copy_budget(copy_budget_input(
            "reported_with_not_measured_segments",
            "unsafe_lifetime_shortcut_requested",
            "blocked_unsafe_lifetime_shortcut",
        ));
        assert_eq!(blocked.status, "blocked_unsafe_lifetime_shortcut");
    }

    #[test]
    fn capillary_preparation_blocks_pulseweave_without_native_io_certificate() {
        let input = capillary_input("missing");

        let report = evaluate_vortex_capillary_preparation(input).expect("capillary report");

        assert_eq!(
            report.status,
            "report_only_blocked_missing_native_io_certificate"
        );
        assert_eq!(report.execution_certificate_status, "certified");
        assert_eq!(report.pulseweave_report.status, "blocked");
        assert!(!report.pulseweave_report.runtime_decision_applied);
        assert!(!report.scheduler_applied);
        assert_eq!(
            report.scheduler_application_reason,
            "blocked_missing_native_io_certificate"
        );
        assert_eq!(report.execution_window_count, 0);
        assert!(
            report
                .pulseweave_report
                .blocker
                .contains("native_io_certificate")
        );
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn differential_preparation_blocks_update_mode_without_overlay() {
        let input = differential_input(VortexDifferentialUpdateMode::Update);

        let report = evaluate_vortex_differential_preparation(input);

        assert!(!report.is_admitted());
        assert_eq!(report.status, "blocked_update_mode_policy");
        assert_eq!(
            report.update_mode_policy,
            "update_delete_upsert_blocked_until_row_identity_and_rewrite_semantics_are_certified"
        );
        assert_eq!(
            report.replay_verification_status,
            "blocked_before_overlay_replay"
        );
        assert!(!report.base_reprepare_performed);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn differential_preparation_blocks_schema_mismatch_without_overlay() {
        let mut input = differential_input(VortexDifferentialUpdateMode::AppendOnly);
        input.delta_schema_digest = "fnv64:other".to_string();

        let report = evaluate_vortex_differential_preparation(input);

        assert!(!report.is_admitted());
        assert_eq!(report.status, "blocked_schema_mismatch");
        assert_eq!(
            report.schema_compatibility_status,
            "blocked_source_schema_or_column_family_mismatch"
        );
        assert_eq!(
            report.prepared_state_reuse_status,
            "blocked_before_prepared_state_reuse"
        );
        assert!(report.delta_artifact_written);
        assert!(!report.overlay_applied);
    }

    #[test]
    fn append_only_refinement_admits_verified_source_prefix_delta() {
        let root = temp_test_root("append-only-refinement-admit");
        let source = root.join("input.csv");
        let target = root.join("prepared.vortex");
        std::fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write base source");
        std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
        let manifest_path =
            vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
        let base_request = reuse_request_for_test(&source, &target, &manifest_path);
        let base_decision =
            evaluate_vortex_prepared_state_reuse(&base_request).expect("base reuse decision");
        let artifact_digest = fnv64_file_digest(&target, "test artifact").expect("artifact digest");
        write_vortex_prepared_state_reuse_manifest(
            &base_request,
            &base_decision,
            VortexPreparedStateReuseWriteEvidence {
                source_state_id: "source-state-base".to_string(),
                source_state_digest: "fnv64:source-base".to_string(),
                source_schema_digest: "fnv64:schema".to_string(),
                source_row_count: 2,
                source_column_family_summary: "id:int64,label:utf8".to_string(),
                prepared_state_id: "prepared-base".to_string(),
                prepared_state_digest: "fnv64:prepared-base".to_string(),
                prepared_artifact_digest: artifact_digest,
                certificate_refs: "reopen_row_count_scan".to_string(),
                fallback_attempted: false,
                external_engine_invoked: false,
            },
        )
        .expect("write reuse manifest");

        std::fs::write(&source, "id,label\n1,alpha\n2,beta\n3,gamma\n").expect("append source");
        let current_request = reuse_request_for_test(&source, &target, &manifest_path);
        let decision = evaluate_vortex_prepared_state_append_only_refinement(&current_request)
            .expect("refinement decision");

        assert!(decision.is_admitted());
        assert_eq!(decision.status, "admitted_append_only_refinement");
        assert_eq!(
            decision.automatic_detection_status,
            "append_only_delta_detected"
        );
        assert_eq!(
            decision.source_prefix_verification_status,
            "verified_old_source_bytes_are_current_prefix"
        );
        assert_eq!(decision.base_source_row_count, 2);
        assert_eq!(
            decision.base_source_column_family_summary,
            "id:int64,label:utf8"
        );
        assert!(decision.changed_byte_range_refs.contains("#bytes="));
        assert!(!decision.fallback_attempted);
        assert!(!decision.external_engine_invoked);

        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn append_only_refinement_blocks_prefix_mismatch() {
        let root = temp_test_root("append-only-refinement-prefix");
        let source = root.join("input.csv");
        let target = root.join("prepared.vortex");
        std::fs::write(&source, "id,label\n1,alpha\n2,beta\n").expect("write base source");
        std::fs::write(&target, b"prepared-vortex-artifact").expect("write prepared artifact");
        let manifest_path =
            vortex_prepared_state_reuse_manifest_path(&target).expect("manifest path");
        let base_request = reuse_request_for_test(&source, &target, &manifest_path);
        let base_decision =
            evaluate_vortex_prepared_state_reuse(&base_request).expect("base reuse decision");
        let artifact_digest = fnv64_file_digest(&target, "test artifact").expect("artifact digest");
        write_vortex_prepared_state_reuse_manifest(
            &base_request,
            &base_decision,
            VortexPreparedStateReuseWriteEvidence {
                source_state_id: "source-state-base".to_string(),
                source_state_digest: "fnv64:source-base".to_string(),
                source_schema_digest: "fnv64:schema".to_string(),
                source_row_count: 2,
                source_column_family_summary: "id:int64,label:utf8".to_string(),
                prepared_state_id: "prepared-base".to_string(),
                prepared_state_digest: "fnv64:prepared-base".to_string(),
                prepared_artifact_digest: artifact_digest,
                certificate_refs: "reopen_row_count_scan".to_string(),
                fallback_attempted: false,
                external_engine_invoked: false,
            },
        )
        .expect("write reuse manifest");

        std::fs::write(&source, "id,label\n1,ALPHA\n2,beta\n3,gamma\n")
            .expect("rewrite and append source");
        let current_request = reuse_request_for_test(&source, &target, &manifest_path);
        let decision = evaluate_vortex_prepared_state_append_only_refinement(&current_request)
            .expect("refinement decision");

        assert!(!decision.is_admitted());
        assert_eq!(decision.status, "blocked_source_prefix_digest_mismatch");
        assert_eq!(decision.blocker_id, "source_prefix_digest_mismatch");
        assert!(!decision.fallback_attempted);
        assert!(!decision.external_engine_invoked);

        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn differential_refinement_manifest_is_digest_backed() {
        let root = temp_test_root("append-only-refinement-manifest");
        let manifest_path = root.join("refinement.manifest");
        let decision = VortexPreparedStateAppendOnlyRefinementDecision {
            schema_version: VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION,
            status: "admitted_append_only_refinement".to_string(),
            policy: VORTEX_DIFFERENTIAL_REFINEMENT_POLICY,
            reason: "source_prefix_verified_and_delta_bytes_detected".to_string(),
            blocker_id: "none".to_string(),
            automatic_detection_status: "append_only_delta_detected".to_string(),
            manifest_path: root.join("reuse.manifest"),
            reuse_manifest_digest: "fnv64:reuse".to_string(),
            source_path: root.join("input.csv"),
            source_format: "csv".to_string(),
            base_source_content_digest: "fnv64:base-source".to_string(),
            current_source_content_digest: "fnv64:current-source".to_string(),
            base_source_size_bytes: 10,
            current_source_size_bytes: 20,
            delta_byte_start: 10,
            delta_byte_end: 20,
            changed_byte_range_refs: "input.csv#bytes=10..20".to_string(),
            source_prefix_verification_status: "verified_old_source_bytes_are_current_prefix"
                .to_string(),
            base_source_state_id: "source-state-base".to_string(),
            base_source_state_digest: "fnv64:source-base".to_string(),
            base_source_row_count: 2,
            base_source_schema_digest: "fnv64:schema".to_string(),
            base_source_column_family_summary: "id:int64,label:utf8".to_string(),
            base_prepared_state_id: "prepared-base".to_string(),
            base_prepared_state_digest: "fnv64:prepared-base".to_string(),
            prepared_artifact_ref: root.join("prepared.vortex"),
            prepared_artifact_digest: "fnv64:artifact".to_string(),
            prepared_artifact_size_bytes: 64,
            certificate_refs: "reopen_row_count_scan".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        };
        let differential = evaluate_vortex_differential_preparation(differential_input(
            VortexDifferentialUpdateMode::AppendOnly,
        ));

        let report = write_vortex_differential_refinement_manifest(
            &manifest_path,
            &decision,
            &differential,
            "refined-prepared",
            "fnv64:refined",
            "count",
            "admitted_base_manifest_plus_delta_reopen_row_count",
            "fnv64:consumer",
        )
        .expect("write refinement manifest");

        assert!(report.manifest_written);
        assert!(report.manifest_digest.starts_with("fnv64:"));
        let manifest = std::fs::read_to_string(&manifest_path).expect("read manifest");
        assert!(
            manifest
                .contains("schema_version=shardloom.vortex_differential_refinement_manifest.v1")
        );
        assert!(manifest.contains("overlay_consumer_family=count"));
        assert!(manifest.contains("manifest_digest=fnv64:"));

        std::fs::remove_dir_all(root).expect("remove temp root");
    }

    fn differential_input(
        update_mode: VortexDifferentialUpdateMode,
    ) -> VortexDifferentialPreparationInput {
        VortexDifferentialPreparationInput {
            update_mode,
            base_source_state_id: "local-csv-base".to_string(),
            base_source_state_digest: "fnv64:base-source".to_string(),
            base_prepared_state_id: "vortex-prepared-state-base".to_string(),
            base_prepared_state_digest: "fnv64:base-prepared".to_string(),
            base_row_count: 2,
            base_schema_digest: "fnv64:schema".to_string(),
            base_column_family_summary: "id:int64,label:utf8".to_string(),
            delta_source_state_id: "local-csv-delta".to_string(),
            delta_source_state_digest: "fnv64:delta-source".to_string(),
            delta_row_count: 1,
            delta_schema_digest: "fnv64:schema".to_string(),
            delta_column_family_summary: "id:int64,label:utf8".to_string(),
            delta_manifest_digest: "fnv64:delta-manifest".to_string(),
            changed_byte_range_refs: "local-csv-delta:split=1:bytes=0..32".to_string(),
            changed_row_range_refs: "local-csv-delta:split=1:rows=0..1".to_string(),
            changed_segment_refs: "vortex-prepared-state-delta:rows=0..1:digest=fnv64:delta"
                .to_string(),
            delta_artifact_ref: "target/delta.vortex".to_string(),
            delta_artifact_digest: "fnv64:delta-artifact".to_string(),
            native_io_certificate_refs: "base_prepared_state,delta_artifact,reopen_row_count_scan"
                .to_string(),
        }
    }

    fn scout_input(
        anomaly_count: u64,
        anomaly_families: &str,
        unsupported_shape_status: &str,
        quarantine_required: bool,
    ) -> VortexScoutIngressInput {
        VortexScoutIngressInput {
            source_state_id: "local-jsonl-source".to_string(),
            source_state_digest: "fnv64:source".to_string(),
            source_format: "jsonl".to_string(),
            source_path: "target/source.jsonl".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            row_count: 2,
            source_byte_count: 128,
            column_count: 2,
            read_plan: "full_columns".to_string(),
            metadata_range_refs: "local-jsonl-source:split=1:bytes=0..128".to_string(),
            sampled_row_range_refs: "local-jsonl-source:split=1:rows=0..2".to_string(),
            anomaly_count,
            anomaly_families: anomaly_families.to_string(),
            malformed_row_refs: "none".to_string(),
            schema_drift_status: "not_detected_no_prior_schema_baseline".to_string(),
            unsupported_shape_status: unsupported_shape_status.to_string(),
            nullability_status: "nullable_fields_admitted_as_scalar_nulls".to_string(),
            small_file_pathology_status: "observed_tiny_local_fixture_not_blocking".to_string(),
            quarantine_required,
            quarantine_output_plan_status: "planned_not_emitted_no_quarantine_sink_requested"
                .to_string(),
            quarantine_output_ref: "not_emitted".to_string(),
            quarantine_output_digest: "not_emitted".to_string(),
            redaction_status: "malformed_row_refs_are_row_numbers_only".to_string(),
            unsupported_diagnostic_code: "none".to_string(),
            correctness_policy: "fail_closed_no_silent_repair_or_row_drop".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn layout_advisor_input(
        strategy_admitted: bool,
        unsupported_diagnostic_code: &str,
    ) -> VortexLayoutWriteAdvisorInput {
        VortexLayoutWriteAdvisorInput {
            source_state_id: "local-csv-source".to_string(),
            source_state_digest: "fnv64:source".to_string(),
            source_format: "csv".to_string(),
            source_schema_digest: "fnv64:schema".to_string(),
            row_count: 2,
            source_byte_count: 128,
            column_count: 2,
            workload_constitution: "prepare_once_local_fixture".to_string(),
            source_statistics_status: "local_source_file_stats_only".to_string(),
            requested_pushdown_requirements: "none_prepare_once".to_string(),
            sink_requirements: "workspace_safe_local_vortex_file_sink".to_string(),
            layout_strategy: "single_local_vortex_artifact".to_string(),
            chunking_strategy: "single_chunk_for_scoped_fixture".to_string(),
            segmentation_strategy: "single_segment_fixture".to_string(),
            dictionary_strategy: "writer_default_no_dictionary_claim".to_string(),
            statistics_policy: "writer_default_statistics_no_pruning_claim".to_string(),
            writer_provider_kind: "shardloom_kernel".to_string(),
            writer_provider_surface:
                "shardloom_scalar_rows_to_vortex_struct;VortexSession::write_options().write(ArrayStream)"
                    .to_string(),
            writer_admission_policy: "scoped_local_vortex_ingest_prepare_once".to_string(),
            write_reopen_verification_depth: "writer_and_reopen_row_count".to_string(),
            materialization_boundary_status: "materialized_scalar_rows_before_write".to_string(),
            decode_boundary_status: "compatibility_parse_to_scalar_values".to_string(),
            expected_read_tradeoff: "not_claimed_fixture_layout".to_string(),
            expected_write_tradeoff: "not_claimed_fixture_layout".to_string(),
            strategy_admitted,
            unsupported_diagnostic_code: unsupported_diagnostic_code.to_string(),
            correctness_refs: "writer_reopen_row_count".to_string(),
            benchmark_refs: "not_claim_grade_no_benchmark_refresh".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn copy_budget_input(
        measurement_status: &str,
        unsafe_lifetime_shortcut_status: &str,
        buffer_reuse_status: &str,
    ) -> VortexCopyBudgetInput {
        VortexCopyBudgetInput {
            source_state_id: "local-csv-source".to_string(),
            source_state_digest: "fnv64:source".to_string(),
            prepared_state_id: "vortex-prepared-state-fnv64-prepared".to_string(),
            prepared_state_digest: "fnv64:prepared".to_string(),
            source_format: "csv".to_string(),
            row_count: 2,
            source_byte_count: 128,
            column_count: 2,
            allocation_scope: "vortex_ingest_local_prepare_once".to_string(),
            copy_scope: "source_read,parse_normalization,vortex_array_build,writer,reopen,evidence"
                .to_string(),
            measurement_status: measurement_status.to_string(),
            source_read_copy_bytes: "128".to_string(),
            parse_normalization_copy_bytes: "not_measured".to_string(),
            columnar_handoff_copy_bytes: "not_applicable_scalar_rows".to_string(),
            vortex_array_build_copy_bytes: "not_measured".to_string(),
            writer_buffer_bytes: "256".to_string(),
            reopen_verify_copy_bytes: "not_measured".to_string(),
            evidence_render_copy_bytes: "not_measured".to_string(),
            total_measured_copy_bytes: "384".to_string(),
            buffer_family: "source_bytes,scalar_rows,vortex_writer_buffer".to_string(),
            ownership_policy: "owned_buffers_no_borrowed_lifetime_reuse".to_string(),
            writer_buffering_status: "writer_buffer_bytes_reported".to_string(),
            buffer_reuse_status: buffer_reuse_status.to_string(),
            buffer_reuse_count: 0,
            unsafe_lifetime_shortcut_status: unsafe_lifetime_shortcut_status.to_string(),
            correctness_parity_refs: "writer_reopen_row_count".to_string(),
            materialization_boundary_status: "materialized_scalar_rows_before_write".to_string(),
            decode_boundary_status: "compatibility_parse_to_scalar_values".to_string(),
            unsupported_diagnostic_code: "none".to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn capillary_input(native_io_certificate_status: &str) -> VortexCapillaryPreparationInput {
        VortexCapillaryPreparationInput {
            source_state_id: "local-csv-source".to_string(),
            source_state_digest: "fnv64:source".to_string(),
            prepared_state_id: "vortex-prepared-state-source".to_string(),
            prepared_state_digest: "fnv64:prepared".to_string(),
            source_surface: "local_text_source_state_scalar_rows".to_string(),
            sink_surface: "workspace_safe_local_vortex_file_sink".to_string(),
            format_family: "csv".to_string(),
            operation_class: "vortex_ingest_prepare_once".to_string(),
            certification_depth: "ingest_certified".to_string(),
            row_count: 4,
            source_byte_count: 128,
            column_count: 2,
            source_split_refs: "local-csv-source:split=0".to_string(),
            source_byte_range_refs: "local-csv-source:split=0:bytes=0..128".to_string(),
            source_row_range_refs: "local-csv-source:split=0:rows=0..4".to_string(),
            projection_mask: "id,label".to_string(),
            filter_mask_status: "none".to_string(),
            prepared_artifact_ref: "target/source.vortex".to_string(),
            prepared_artifact_digest: "fnv64:artifact".to_string(),
            prepared_artifact_segment_refs:
                "vortex-prepared-state-source:rows=0..4:digest=fnv64:artifact".to_string(),
            writer_sink_refs: "target/source.vortex".to_string(),
            materialization_boundary_status: "scalar_rows_materialized_before_vortex_array_build"
                .to_string(),
            decode_boundary_status: "local_text_parse_to_scalar_values".to_string(),
            native_io_certificate_status: native_io_certificate_status.to_string(),
            native_io_certificate_refs:
                "source_split_refs,prepared_artifact_ref,reopen_row_count_scan".to_string(),
            correctness_digest: "fnv64:correct".to_string(),
            memory_budget_bytes: 16 * 1024 * 1024,
            max_parallelism: 2,
            result_sink_requested: false,
            result_sink_replay_verified: false,
            capillary_claim_evidence_requested: true,
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }

    fn capillary_prewrite_test_input(
        target_path: &Path,
        row_count: u64,
        column_count: usize,
    ) -> VortexCapillaryPreparationInput {
        let mut input = capillary_input("certified");
        input.prepared_artifact_ref = target_path.display().to_string();
        input.prepared_artifact_digest =
            "prewrite_pending_until_vortex_writer_certificate".to_string();
        input.writer_sink_refs = target_path.display().to_string();
        input.row_count = row_count;
        input.column_count = column_count;
        input.certification_depth = VortexIngestCertificationLevel::IngestCertified
            .as_str()
            .to_string();
        input.capillary_claim_evidence_requested = true;
        input
    }
}
