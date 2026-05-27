//! Feature-gated local `vortex_ingest` lifecycle helpers.
//!
//! This module intentionally exposes a narrow local prepare-once path for flat
//! scalar rows. It writes a local Vortex artifact, reopens/scans the artifact
//! for row-count proof, and returns evidence fields that callers can surface as
//! a `VortexPreparedState`. It is not a broad Vortex writer, object-store sink,
//! table commit path, or query-engine integration.

use std::path::PathBuf;

#[cfg(feature = "vortex-write")]
use std::{collections::BTreeSet, path::Path, time::Instant};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use std::collections::BTreeMap;

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use arrow_array::{
    Array, ArrayRef as ArrowArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use shardloom_core::{Result, ScalarValue, ShardLoomError, WorkspaceSafeLocalWriteReport};
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
/// Evidence schema emitted by scoped local capillary cold-preparation task control.
pub const VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION: &str =
    "shardloom.vortex_capillary_preparation.v1";
/// Pinned upstream Vortex crate line used by the scoped local preparation spine.
pub const VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION: &str = "0.72";

/// Request to write one flat scalar local source into a local Vortex artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateWriteRequest {
    pub target_path: PathBuf,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, ScalarValue)>>,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
}

/// Request to write one flat columnar local source into a Vortex artifact.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateColumnarWriteRequest {
    pub target_path: PathBuf,
    pub source: FlatLocalColumnarSource,
    pub allow_overwrite: bool,
    pub certification_level: VortexIngestCertificationLevel,
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
}

impl VortexPreparedStateWriteRequest {
    /// Create a request for a local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(
        target_path: impl Into<PathBuf>,
        columns: Vec<String>,
        rows: Vec<Vec<(String, ScalarValue)>>,
    ) -> Self {
        Self {
            target_path: target_path.into(),
            columns,
            rows,
            allow_overwrite: false,
            certification_level: VortexIngestCertificationLevel::IngestCertified,
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
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
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

/// Evidence for capillary cold-preparation task control.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCapillaryPreparationReport {
    pub schema_version: &'static str,
    pub status: String,
    pub route: String,
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
        let mut fields = Vec::with_capacity(84);
        self.append_identity_fields(&mut fields);
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

/// Plan capillary cold-preparation task evidence and `PulseWeave` control.
///
/// # Errors
/// Returns an error when the `PulseWeave` task input is structurally invalid.
pub fn evaluate_vortex_capillary_preparation(
    input: VortexCapillaryPreparationInput,
) -> Result<VortexCapillaryPreparationReport> {
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
    let execution_certificate_status = if input.source_split_refs.trim().is_empty() {
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

    Ok(VortexCapillaryPreparationReport {
        schema_version: VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION,
        status: status.to_string(),
        route: "vortex_ingest_source_state_to_prepared_state".to_string(),
        source_surface: input.source_surface,
        sink_surface: input.sink_surface,
        source_state_id: input.source_state_id,
        source_state_digest: input.source_state_digest,
        prepared_state_id: input.prepared_state_id,
        prepared_state_digest: input.prepared_state_digest,
        task_manifest_id: format!(
            "vortex-capillary-task-manifest-{}",
            task_manifest_digest.replace(':', "-")
        ),
        task_manifest_digest,
        task_count: tasks.len(),
        task_roles,
        task_ids,
        source_split_refs: input.source_split_refs,
        read_chunk_byte_range_refs: task_byte_range_refs,
        row_range_refs: task_row_range_refs,
        projection_mask: input.projection_mask,
        filter_mask_status: input.filter_mask_status,
        vortex_segment_refs,
        writer_sink_refs: input.writer_sink_refs,
        memory_budget_bytes: input.memory_budget_bytes,
        max_parallelism: input.max_parallelism,
        peak_memory_bytes,
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
        execution_certificate_id,
        execution_certificate_status: execution_certificate_status.to_string(),
        pulseweave_report,
        correctness_refs: input.correctness_digest,
        no_standalone_lane_status:
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state".to_string(),
        claim_gate_status: "not_claim_grade".to_string(),
        claim_boundary: "Scoped local capillary cold-preparation evidence only: task boundaries and PulseWeave control are tied to vortex_ingest writer/reopen certificates; no object-store, distributed, broad parallel performance, production, SQL/DataFrame, or Spark-replacement claim".to_string(),
        fallback_attempted: input.fallback_attempted,
        external_engine_invoked: input.external_engine_invoked,
    })
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
    if tasks.is_empty() || input.source_split_refs.trim().is_empty() {
        "blocked_missing_capillary_task_manifest"
    } else if input.native_io_certificate_status != "certified" {
        "report_only_blocked_missing_native_io_certificate"
    } else if pulseweave_report.runtime_decision_applied {
        "applied_capillary_pulseweave_control"
    } else {
        "report_only_blocked_pulseweave_control"
    }
}

fn join_task_values(
    tasks: &[VortexCapillaryPreparationTask],
    mut value: impl FnMut(&VortexCapillaryPreparationTask) -> String,
) -> String {
    tasks.iter().map(&mut value).collect::<Vec<_>>().join(",")
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
    let column_families = scalar_column_families(&request.columns, &request.rows)?;
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
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
    prepare_vortex_target(&request.target_path, request.allow_overwrite)?;
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
            Ok(ColumnarProjectedColumn {
                column: column.clone(),
                reader_index: *reader_index,
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
    let write_result = write_vortex_array(&input.target_path, input.array, input.allow_overwrite)?;

    let (
        reopen_row_count,
        reopen_scan_micros,
        reopen_verification_status,
        upstream_vortex_scan_called,
    ) = if input.certification_level == VortexIngestCertificationLevel::IngestCertified {
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
        if write_result.writer_row_count != input.row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest writer row count mismatch: source={} writer={}; no fallback execution was attempted",
                input.row_count, write_result.writer_row_count
            )));
        }
        (0, 0, "not_performed_ingest_minimal".to_string(), false)
    };
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
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<(String, String)>> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let mut family: Option<&'static str> = None;
            for row in rows {
                let value = &row[column_index].1;
                let candidate = scalar_family(value).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains unsupported value {}; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted",
                        value.summary()
                    ))
                })?;
                if let Some(existing) = family {
                    if existing != candidate {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{column}' mixes scalar families {existing} and {candidate}; no fallback execution was attempted"
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((column.clone(), family.unwrap_or("utf8").to_string()))
        })
        .collect()
}

#[cfg(feature = "vortex-write")]
fn scalar_family(value: &ScalarValue) -> Option<&'static str> {
    match value {
        ScalarValue::Boolean(_) => Some("boolean"),
        ScalarValue::Int64(_) => Some("int64"),
        ScalarValue::UInt64(_) => Some("uint64"),
        ScalarValue::Float64(value) if value.is_finite() => Some("float64"),
        ScalarValue::Utf8(_) => Some("utf8"),
        ScalarValue::Date32(_) => Some("date32"),
        ScalarValue::TimestampMicros(_) => Some("timestamp_micros"),
        ScalarValue::Null | ScalarValue::Binary(_) | ScalarValue::Float64(_) => None,
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
                family.unwrap_or("utf8").to_string(),
            ))
        })
        .collect()
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn reject_columnar_nulls(column: &str, array: &dyn Array) -> Result<()> {
    if array.null_count() > 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' contains nulls; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted"
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
    if array.as_any().is::<Date32Array>() {
        return Ok("date32");
    }
    if array.as_any().is::<TimestampMicrosecondArray>() {
        return Ok("timestamp_micros");
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' has unsupported Arrow type {:?}; scoped Vortex ingest admits non-null boolean, int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted",
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
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{BoolArray, PrimitiveArray, VarBinViewArray};
    use vortex::array::validity::Validity;
    use vortex::buffer::BitBuffer;

    match family {
        "boolean" => Ok(BoolArray::new(
            BitBuffer::from(
                rows.iter()
                    .map(|row| match &row[column_index].1 {
                        ScalarValue::Boolean(value) => Ok(*value),
                        value => Err(unexpected_vortex_ingest_value(column, family, value)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            Validity::NonNullable,
        )
        .into_array()),
        "int64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Int64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "uint64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::UInt64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "float64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Float64(value) if value.is_finite() => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "utf8" => {
            let values = rows
                .iter()
                .map(|row| match &row[column_index].1 {
                    ScalarValue::Utf8(value) => Ok(value.as_str()),
                    value => Err(unexpected_vortex_ingest_value(column, family, value)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(VarBinViewArray::from_iter_str(values).into_array())
        }
        "date32" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Date32(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "timestamp_micros" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::TimestampMicros(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported scalar family {other}; no fallback execution was attempted"
        ))),
    }
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
    workspace_write_report: WorkspaceSafeLocalWriteReport,
}

#[cfg(feature = "vortex-write")]
fn write_vortex_array(
    path: &Path,
    array: &vortex::array::ArrayRef,
    allow_overwrite: bool,
) -> Result<LocalVortexWriteResult> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let write_start = Instant::now();
    let summary = runtime
        .block_on(
            session
                .write_options()
                .write(&mut bytes, array.to_array_stream()),
        )
        .map_err(vortex_error)?;
    let expected_rows = usize_to_u64(array.len())?;
    if summary.row_count() != expected_rows {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest writer row count mismatch: wrote {}, expected {}; no fallback execution was attempted",
            summary.row_count(),
            expected_rows
        )));
    }
    let digest_start = Instant::now();
    let artifact_digest = fnv64_digest_bytes(&bytes);
    let digest_micros = digest_start.elapsed().as_micros();
    let bytes_written = usize_to_u64(bytes.len())?;
    let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
    let workspace_write_report = shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        path,
        allow_overwrite,
        "local vortex_ingest artifact",
        &bytes,
    )?;
    Ok(LocalVortexWriteResult {
        writer_row_count: summary.row_count(),
        bytes_written,
        artifact_digest,
        digest_micros,
        write_micros: write_start.elapsed().as_micros(),
        workspace_write_report,
    })
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
fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
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

    #[test]
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

    #[cfg(feature = "universal-format-io")]
    #[test]
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
            materialized_columns: columns.clone(),
            reader_projection_columns: columns,
            batches: vec![batch],
            row_count: 2,
        };
        let request = VortexPreparedStateColumnarWriteRequest::new(&path, source);

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
            "vortex_capillary_preparation_fallback_attempted".to_string(),
            "false".to_string()
        )));
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

    fn capillary_input(native_io_certificate_status: &str) -> VortexCapillaryPreparationInput {
        VortexCapillaryPreparationInput {
            source_state_id: "local-csv-source".to_string(),
            source_state_digest: "fnv64:source".to_string(),
            prepared_state_id: "vortex-prepared-state-source".to_string(),
            prepared_state_digest: "fnv64:prepared".to_string(),
            source_surface: "local_text_source_state_scalar_rows".to_string(),
            sink_surface: "workspace_safe_local_vortex_file_sink".to_string(),
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
            fallback_attempted: false,
            external_engine_invoked: false,
        }
    }
}
