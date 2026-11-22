//! Durable prepared-source, attempt-isolation, atomic commit fencing, and
//! retained-owner lease store for short-lived portable native workers (`RFC 0047`).

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use shardloom_core::{FloatPolicy, RecursionLimit, Result, ShardLoomError};
use std::{
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::Read as _,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub const DURABLE_PREPARED_SOURCE_SCHEMA_VERSION: &str = "shardloom.durable_prepared_source.v1";
pub const DURABLE_RESULT_MANIFEST_SCHEMA_VERSION: &str = "shardloom.durable_result_manifest.v1";
pub const PROVIDER_VORTEX_VERSION: &str = "0.93.0";
pub const PROVIDER_ARROW_VERSION: &str = "59.3.2";
pub const RUNTIME_VERSION: &str = env!("CARGO_PKG_VERSION");

const STREAM_CHUNK_BYTES: usize = 64 * 1024;
static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

fn store_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "portable durable store: {reason}; no fallback execution was attempted"
    ))
}

/// Validate a portable identifier used as a path segment or manifest key.
/// Rejects empty strings, lengths above 128 bytes, path separators, `..`,
/// and leading dots so persisted manifests can never escape their root.
///
/// # Errors
/// Returns an explicit store validation error if `id` violates naming rules.
pub fn validate_safe_id(label: &str, id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 128 {
        return Err(store_error(&format!("{label} must be 1..=128 ASCII bytes")));
    }
    if id.starts_with('.') || id.contains("..") {
        return Err(store_error(&format!(
            "{label} contains forbidden dot segments"
        )));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(store_error(&format!(
            "{label} may contain only ASCII [A-Za-z0-9._-]"
        )));
    }
    Ok(())
}

/// Reject secret-looking content from job metadata and scope identifiers so
/// raw credentials never enter persisted manifests or worker evidence.
///
/// # Errors
/// Returns an explicit store error when secret markers or inline credentials
/// are detected.
pub fn reject_secret_material(label: &str, value: &str) -> Result<()> {
    let lower = value.to_ascii_lowercase();
    for marker in [
        "password=",
        "secret=",
        "token=",
        "api_key=",
        "apikey=",
        "authorization:",
        "bearer ",
        "-----begin ",
        "aws_secret_access_key",
        "aws_session_token",
    ] {
        if lower.contains(marker) {
            return Err(store_error(&format!(
                "{label} appears to embed credential/secret material ({marker}); pass scoped authorization reference ids only"
            )));
        }
    }
    Ok(())
}

/// Compute lowercase hex SHA-256 and exact byte count by streaming in bounded
/// 64 KiB chunks.
///
/// # Errors
/// Fails if `path` cannot be opened as a regular file or read cleanly.
pub fn sha256_file(path: &Path) -> Result<(String, u64)> {
    let meta = fs::metadata(path).map_err(|err| {
        store_error(&format!(
            "cannot stat input/artifact '{}': {err}",
            path.display()
        ))
    })?;
    if !meta.is_file() {
        return Err(store_error(&format!(
            "expected regular file at '{}'",
            path.display()
        )));
    }
    let mut file = File::open(path)
        .map_err(|err| store_error(&format!("cannot open '{}': {err}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; STREAM_CHUNK_BYTES];
    let mut total_bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buf)
            .map_err(|err| store_error(&format!("read error on '{}': {err}", path.display())))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        let chunk = u64::try_from(read).map_err(|_| store_error("file byte count overflow"))?;
        total_bytes = total_bytes
            .checked_add(chunk)
            .ok_or_else(|| store_error("file size overflow"))?;
    }
    Ok((hex_digest(hasher.finalize().as_slice()), total_bytes))
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Full identity binding source bytes, format, semantics, runtime/provider
/// versions, and caller authorization scope for reusable durable preparation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePreparedSourceIdentity {
    pub source_sha256: String,
    pub source_size_bytes: u64,
    pub source_format: String,
    pub float_policy: String,
    pub recursion_limit: u64,
    pub runtime_version: String,
    pub provider_vortex_version: String,
    pub provider_arrow_version: String,
    pub authorization_scope_id: String,
}

impl DurablePreparedSourceIdentity {
    /// Construct a pinned identity using current workspace runtime and provider versions.
    ///
    /// # Errors
    /// Validates `authorization_scope_id` for safe filename/metadata use and secret absence.
    pub fn current(
        source_sha256: String,
        source_size_bytes: u64,
        source_format: String,
        float_policy: FloatPolicy,
        recursion_limit: RecursionLimit,
        authorization_scope_id: String,
    ) -> Result<Self> {
        validate_safe_id("authorization_scope_id", &authorization_scope_id)?;
        reject_secret_material("authorization_scope_id", &authorization_scope_id)?;
        Ok(Self {
            source_sha256,
            source_size_bytes,
            source_format,
            float_policy: match float_policy {
                FloatPolicy::Strict => "strict".to_string(),
                FloatPolicy::Extended => "extended".to_string(),
            },
            recursion_limit: recursion_limit.get(),
            runtime_version: RUNTIME_VERSION.to_string(),
            provider_vortex_version: PROVIDER_VORTEX_VERSION.to_string(),
            provider_arrow_version: PROVIDER_ARROW_VERSION.to_string(),
            authorization_scope_id,
        })
    }

    /// Deterministic SHA-256 content+policy key used for store lookup.
    ///
    /// # Errors
    /// Fails if canonical serialization fails.
    pub fn artifact_id(&self) -> Result<String> {
        let canonical = serde_json::to_vec(self)
            .map_err(|err| store_error(&format!("identity encode failed: {err}")))?;
        let digest = Sha256::digest(&canonical);
        Ok(hex_digest(digest.as_slice()))
    }
}

/// Persisted metadata binding a committed `.vortex` artifact to its exact
/// source generation and execution semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePreparedSourceManifest {
    pub schema_version: String,
    pub artifact_id: String,
    pub identity: DurablePreparedSourceIdentity,
    pub artifact_rel_path: String,
    pub artifact_sha256: String,
    pub artifact_bytes: u64,
    pub rows: u64,
}

/// Persisted manifest created atomically for the winning attempt of a request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableResultManifest {
    pub schema_version: String,
    pub request_id: String,
    pub winning_attempt_id: String,
    pub output_format: String,
    pub output_rel_path: String,
    pub output_sha256: String,
    pub output_bytes: u64,
    pub rows: u64,
    pub route_selected: String,
    pub prepared_artifact_ids: Vec<String>,
    pub runtime_version: String,
    pub provider_vortex_version: String,
    pub provider_arrow_version: String,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: String,
}

/// Validated open handle to a reusable durable prepared `.vortex` artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedPreparedArtifact {
    pub manifest: DurablePreparedSourceManifest,
    pub vortex_path: PathBuf,
}

/// Outcome of an atomic host commit fence attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommitOutcome {
    Committed {
        manifest: DurableResultManifest,
    },
    FencedAlreadyCommitted {
        winning_attempt_id: String,
        manifest: DurableResultManifest,
    },
}

/// Artifact namespace protected against premature expiration while owners hold leases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactKind {
    Prepared,
    Committed,
}

impl ArtifactKind {
    const fn dir_name(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Committed => "committed",
        }
    }
}

/// Outcome of requesting storage reclamation on a durable artifact or request result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimOutcome {
    Reclaimed,
    Absent,
    ProtectedByRetainedOwners { owner_count: usize },
}

/// Active RAII scratch/spill workspace scoped strictly to one attempt.
/// Dropping never removes committed request outputs or prepared artifacts.
pub struct AttemptWorkspaceGuard {
    attempt_dir: PathBuf,
    spill_dir: PathBuf,
}

impl AttemptWorkspaceGuard {
    /// Directory dedicated to this attempt's uncommitted intermediate files.
    #[must_use]
    pub fn attempt_dir(&self) -> &Path {
        &self.attempt_dir
    }

    /// Spill directory inside the attempt workspace.
    #[must_use]
    pub fn spill_dir(&self) -> &Path {
        &self.spill_dir
    }
}

impl Drop for AttemptWorkspaceGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.attempt_dir);
    }
}

/// Retained owner lease file protecting a prepared or committed artifact from
/// background reclamation until dropped.
pub struct DurableArtifactLease {
    lease_path: PathBuf,
}

impl Drop for DurableArtifactLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lease_path);
    }
}

/// Host-managed local durable store root for portable serverless workers.
#[derive(Clone, Debug)]
pub struct DurableArtifactStore {
    root: PathBuf,
}

impl DurableArtifactStore {
    /// Construct a handle for a local durable store root directory.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Resolve a safe path relative to store root while rejecting traversal segments.
    ///
    /// # Errors
    /// Rejects non-relative or parent-traversing relative paths.
    pub fn resolve_rel_path(&self, rel: &str) -> Result<PathBuf> {
        let candidate = Path::new(rel);
        if candidate.is_absolute()
            || candidate
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(store_error(&format!(
                "manifest relative path '{rel}' escapes store root"
            )));
        }
        Ok(self.root.join(candidate))
    }

    /// Create an isolated uncommitted scratch/spill directory for one `(request_id, attempt_id)`.
    ///
    /// # Errors
    /// Rejects unsafe identifiers or duplicate active attempt directories.
    pub fn begin_attempt(
        &self,
        request_id: &str,
        attempt_id: &str,
    ) -> Result<AttemptWorkspaceGuard> {
        validate_safe_id("request_id", request_id)?;
        validate_safe_id("attempt_id", attempt_id)?;
        let req_attempts_dir = self.root.join("attempts").join(request_id);
        fs::create_dir_all(&req_attempts_dir)
            .map_err(|err| store_error(&format!("cannot create attempt root: {err}")))?;
        let attempt_dir = req_attempts_dir.join(attempt_id);
        fs::create_dir(&attempt_dir).map_err(|err| {
            store_error(&format!(
                "cannot isolate attempt workspace '{}': {err}",
                attempt_dir.display()
            ))
        })?;
        let spill_dir = attempt_dir.join("spill");
        fs::create_dir(&spill_dir).map_err(|err| {
            let _ = fs::remove_dir_all(&attempt_dir);
            store_error(&format!("cannot create attempt spill directory: {err}"))
        })?;
        Ok(AttemptWorkspaceGuard {
            attempt_dir,
            spill_dir,
        })
    }

    /// Look up and strictly validate a durable prepared `.vortex` artifact.
    /// Returns `Ok(None)` when absent; fails deterministically if a present
    /// manifest or artifact is stale, corrupted, incompatible, or wrong-scope.
    ///
    /// # Errors
    /// Fails closed on any identity/version/scope mismatch or checksum corruption.
    pub fn open_validated_prepared(
        &self,
        expected: &DurablePreparedSourceIdentity,
    ) -> Result<Option<ValidatedPreparedArtifact>> {
        let artifact_id = expected.artifact_id()?;
        self.open_validated_prepared_by_id(&artifact_id, expected)
    }

    /// Validate a concrete `artifact_id` entry against `expected` identity rules.
    /// Useful both on direct lookup and when testing mismatched artifact IDs or scopes.
    ///
    /// # Errors
    /// Fails closed on corrupt manifest JSON, incompatible runtime/provider version,
    /// wrong authorization scope, stale source hash, or artifact checksum mismatch.
    pub fn open_validated_prepared_by_id(
        &self,
        artifact_id: &str,
        expected: &DurablePreparedSourceIdentity,
    ) -> Result<Option<ValidatedPreparedArtifact>> {
        validate_safe_id("artifact_id", artifact_id)?;
        let entry_dir = self.root.join("prepared").join(artifact_id);
        let manifest_path = entry_dir.join("manifest.json");
        if !manifest_path.exists() {
            return Ok(None);
        }
        let manifest = read_and_validate_prepared_manifest(&manifest_path, artifact_id, expected)?;
        let vortex_path = self.resolve_rel_path(&manifest.artifact_rel_path)?;
        let (actual_sha, actual_bytes) = sha256_file(&vortex_path).map_err(|err| {
            store_error(&format!(
                "prepared artifact '{artifact_id}' missing or unreadable: {err}"
            ))
        })?;
        if actual_bytes != manifest.artifact_bytes || actual_sha != manifest.artifact_sha256 {
            return Err(store_error(&format!(
                "corrupted prepared artifact '{artifact_id}': expected sha={}/bytes={}, got sha={actual_sha}/bytes={actual_bytes}",
                manifest.artifact_sha256, manifest.artifact_bytes
            )));
        }
        Ok(Some(ValidatedPreparedArtifact {
            manifest,
            vortex_path,
        }))
    }

    /// Publish a staged `.vortex` preparation into the durable store atomically.
    ///
    /// # Errors
    /// Propagates file staging, checksum, serialization, or existing-manifest mismatch errors.
    pub fn publish_prepared_vortex(
        &self,
        staged_vortex_path: &Path,
        identity: &DurablePreparedSourceIdentity,
        rows: u64,
    ) -> Result<DurablePreparedSourceManifest> {
        let artifact_id = identity.artifact_id()?;
        validate_safe_id("artifact_id", &artifact_id)?;
        let entry_dir = self.root.join("prepared").join(&artifact_id);
        fs::create_dir_all(&entry_dir)
            .map_err(|err| store_error(&format!("cannot create prepared dir: {err}")))?;
        let (artifact_sha256, artifact_bytes) = sha256_file(staged_vortex_path)?;
        let rel_path = format!("prepared/{artifact_id}/data.vortex");
        let final_vortex = self.root.join(&rel_path);
        if !final_vortex.exists() {
            stage_copy_atomic(staged_vortex_path, &final_vortex)?;
        }
        let manifest = DurablePreparedSourceManifest {
            schema_version: DURABLE_PREPARED_SOURCE_SCHEMA_VERSION.to_string(),
            artifact_id: artifact_id.clone(),
            identity: identity.clone(),
            artifact_rel_path: rel_path,
            artifact_sha256,
            artifact_bytes,
            rows,
        };
        let manifest_path = entry_dir.join("manifest.json");
        if write_manifest_exclusive(&manifest_path, &manifest)? {
            Ok(manifest)
        } else {
            read_and_validate_prepared_manifest(&manifest_path, &artifact_id, identity)
        }
    }

    /// Atomically commit an attempt's staged output artifact and fenced request
    /// manifest. A losing attempt cleans up its own committed artifact copy and
    /// returns the authoritative winning manifest without mutating it.
    ///
    /// # Errors
    /// Fails if artifact staging, checksum calculation, or manifest read/write fails.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_attempt_output(
        &self,
        request_id: &str,
        attempt_id: &str,
        staged_output_path: &Path,
        output_format: &str,
        rows: u64,
        route_selected: String,
        prepared_artifact_ids: Vec<String>,
    ) -> Result<CommitOutcome> {
        validate_safe_id("request_id", request_id)?;
        validate_safe_id("attempt_id", attempt_id)?;
        validate_safe_id("output_format", output_format)?;
        let commit_dir = self.root.join("committed").join(request_id);
        let artifacts_dir = commit_dir.join("artifacts");
        fs::create_dir_all(&artifacts_dir)
            .map_err(|err| store_error(&format!("cannot create committed dir: {err}")))?;
        let (output_sha256, output_bytes) = sha256_file(staged_output_path)?;
        let output_rel_path =
            format!("committed/{request_id}/artifacts/{attempt_id}.{output_format}");
        let committed_artifact = self.root.join(&output_rel_path);
        stage_copy_atomic(staged_output_path, &committed_artifact)?;
        let manifest = DurableResultManifest {
            schema_version: DURABLE_RESULT_MANIFEST_SCHEMA_VERSION.to_string(),
            request_id: request_id.to_string(),
            winning_attempt_id: attempt_id.to_string(),
            output_format: output_format.to_string(),
            output_rel_path,
            output_sha256,
            output_bytes,
            rows,
            route_selected,
            prepared_artifact_ids,
            runtime_version: RUNTIME_VERSION.to_string(),
            provider_vortex_version: PROVIDER_VORTEX_VERSION.to_string(),
            provider_arrow_version: PROVIDER_ARROW_VERSION.to_string(),
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade".to_string(),
        };
        let manifest_path = commit_dir.join("manifest.json");
        if write_manifest_exclusive(&manifest_path, &manifest)? {
            return Ok(CommitOutcome::Committed { manifest });
        }
        let _ = fs::remove_file(&committed_artifact);
        let winning = read_and_validate_result_manifest(&manifest_path, request_id)?;
        Ok(CommitOutcome::FencedAlreadyCommitted {
            winning_attempt_id: winning.winning_attempt_id.clone(),
            manifest: winning,
        })
    }

    /// Read an already-committed request manifest and verify its output artifact checksum.
    ///
    /// # Errors
    /// Fails if manifest is missing, malformed, or output artifact bytes are corrupted.
    pub fn read_committed_manifest(&self, request_id: &str) -> Result<DurableResultManifest> {
        validate_safe_id("request_id", request_id)?;
        let manifest_path = self
            .root
            .join("committed")
            .join(request_id)
            .join("manifest.json");
        let manifest = read_and_validate_result_manifest(&manifest_path, request_id)?;
        let output_path = self.resolve_rel_path(&manifest.output_rel_path)?;
        let (sha, bytes) = sha256_file(&output_path)?;
        if bytes != manifest.output_bytes || sha != manifest.output_sha256 {
            return Err(store_error(&format!(
                "corrupted committed output for request '{request_id}'"
            )));
        }
        Ok(manifest)
    }

    /// Pin an artifact with an owner lease so reclamation refuses deletion while
    /// a consumer handle or downstream reader remains live.
    ///
    /// # Errors
    /// Rejects unsafe IDs or duplicate active lease for the same `owner_id`.
    pub fn acquire_lease(
        &self,
        kind: ArtifactKind,
        artifact_or_request_id: &str,
        owner_id: &str,
    ) -> Result<DurableArtifactLease> {
        validate_safe_id("artifact_or_request_id", artifact_or_request_id)?;
        validate_safe_id("owner_id", owner_id)?;
        let dir = self
            .root
            .join("owners")
            .join(kind.dir_name())
            .join(artifact_or_request_id);
        fs::create_dir_all(&dir)
            .map_err(|err| store_error(&format!("cannot create owner lease dir: {err}")))?;
        let lease_path = dir.join(format!("{owner_id}.lease"));
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lease_path)
            .map_err(|err| store_error(&format!("cannot acquire owner lease: {err}")))?;
        Ok(DurableArtifactLease { lease_path })
    }

    /// Reclaim a prepared source artifact unless active owner leases protect it.
    ///
    /// # Errors
    /// Fails on unsafe IDs or filesystem removal errors.
    pub fn reclaim_prepared(&self, artifact_id: &str) -> Result<ReclaimOutcome> {
        self.reclaim_entry(ArtifactKind::Prepared, artifact_id)
    }

    /// Reclaim a committed request directory unless active owner leases protect it.
    ///
    /// # Errors
    /// Fails on unsafe IDs or filesystem removal errors.
    pub fn reclaim_committed(&self, request_id: &str) -> Result<ReclaimOutcome> {
        self.reclaim_entry(ArtifactKind::Committed, request_id)
    }

    fn reclaim_entry(&self, kind: ArtifactKind, id: &str) -> Result<ReclaimOutcome> {
        validate_safe_id("id", id)?;
        let owners_dir = self.root.join("owners").join(kind.dir_name()).join(id);
        let owner_count = count_active_leases(&owners_dir)?;
        if owner_count > 0 {
            return Ok(ReclaimOutcome::ProtectedByRetainedOwners { owner_count });
        }
        let target = self.root.join(kind.dir_name()).join(id);
        if !target.exists() {
            return Ok(ReclaimOutcome::Absent);
        }
        fs::remove_dir_all(&target)
            .map_err(|err| store_error(&format!("cannot reclaim '{}': {err}", target.display())))?;
        let _ = fs::remove_dir(&owners_dir);
        Ok(ReclaimOutcome::Reclaimed)
    }
}

fn count_active_leases(owners_dir: &Path) -> Result<usize> {
    if !owners_dir.exists() {
        return Ok(0);
    }
    let mut count = 0_usize;
    let entries = fs::read_dir(owners_dir)
        .map_err(|err| store_error(&format!("cannot inspect owner leases: {err}")))?;
    for entry in entries {
        let entry = entry.map_err(|err| store_error(&format!("lease entry error: {err}")))?;
        if entry.path().extension().is_some_and(|ext| ext == "lease") {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn stage_copy_atomic(src: &Path, dst: &Path) -> Result<()> {
    let parent = dst
        .parent()
        .ok_or_else(|| store_error("destination has no parent directory"))?;
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".tmp-{}-{nonce}", std::process::id()));
    let copy_res = (|| -> Result<()> {
        fs::copy(src, &tmp).map_err(|err| {
            store_error(&format!(
                "cannot stage artifact bytes from '{}' to '{}': {err}",
                src.display(),
                tmp.display()
            ))
        })?;
        // Use hard_link when dst does not yet exist so concurrent writers never overwrite dst.
        match fs::hard_link(&tmp, dst) {
            Ok(()) => {
                let _ = fs::remove_file(&tmp);
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&tmp);
                Ok(())
            }
            Err(_) => fs::rename(&tmp, dst).map_err(|rename_err| {
                store_error(&format!(
                    "cannot promote staged artifact '{}': {rename_err}",
                    dst.display()
                ))
            }),
        }
    })();
    if copy_res.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    copy_res
}

fn write_manifest_exclusive<T: Serialize>(path: &Path, value: &T) -> Result<bool> {
    use std::io::Write as _;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|err| store_error(&format!("manifest serialize failed: {err}")))?;
    let parent = path
        .parent()
        .ok_or_else(|| store_error("manifest has no parent directory"))?;
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".tmp-manifest-{}-{nonce}", std::process::id()));
    let write_res = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|err| {
                store_error(&format!("cannot stage manifest '{}': {err}", tmp.display()))
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|err| {
                store_error(&format!(
                    "cannot write manifest '{}': {err}",
                    path.display()
                ))
            })
    })();
    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }
    let linked = match fs::hard_link(&tmp, path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(store_error(&format!(
            "cannot publish manifest '{}': {err}",
            path.display()
        ))),
    };
    let _ = fs::remove_file(&tmp);
    linked
}

fn read_and_validate_prepared_manifest(
    manifest_path: &Path,
    expected_id: &str,
    expected: &DurablePreparedSourceIdentity,
) -> Result<DurablePreparedSourceManifest> {
    let raw = fs::read(manifest_path).map_err(|err| {
        store_error(&format!(
            "cannot read prepared manifest '{}': {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: DurablePreparedSourceManifest = serde_json::from_slice(&raw)
        .map_err(|err| store_error(&format!("malformed prepared manifest: {err}")))?;
    if manifest.schema_version != DURABLE_PREPARED_SOURCE_SCHEMA_VERSION {
        return Err(store_error(&format!(
            "incompatible prepared manifest schema '{}'",
            manifest.schema_version
        )));
    }
    if manifest.artifact_id != expected_id || manifest.identity != *expected {
        return Err(store_error(&format!(
            "stale, incompatible, or unauthorized prepared artifact '{expected_id}'"
        )));
    }
    Ok(manifest)
}

fn read_and_validate_result_manifest(
    manifest_path: &Path,
    expected_request_id: &str,
) -> Result<DurableResultManifest> {
    let raw = fs::read(manifest_path).map_err(|err| {
        store_error(&format!(
            "cannot read committed result manifest '{}': {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: DurableResultManifest = serde_json::from_slice(&raw)
        .map_err(|err| store_error(&format!("malformed committed manifest: {err}")))?;
    if manifest.schema_version != DURABLE_RESULT_MANIFEST_SCHEMA_VERSION
        || manifest.request_id != expected_request_id
        || manifest.runtime_version != RUNTIME_VERSION
        || manifest.provider_vortex_version != PROVIDER_VORTEX_VERSION
        || manifest.provider_arrow_version != PROVIDER_ARROW_VERSION
        || manifest.fallback_attempted
        || manifest.external_engine_invoked
    {
        return Err(store_error(&format!(
            "incompatible or invalid committed result manifest for '{expected_request_id}'"
        )));
    }
    Ok(manifest)
}
