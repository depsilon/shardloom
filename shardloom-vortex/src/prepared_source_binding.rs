//! Embedded preparation provenance for the local immutable-artifact cache.
//! Generation checks are local change detection, not cryptographic authentication.

use crate::source_identity::SourceIdentity;
use sha2::{Digest as _, Sha256};
use shardloom_core::{Result, ShardLoomError};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub(crate) const KEY: &str = "shardloom.prepared-source.v1";
const MAX_BYTES: usize = 64 * 1024;

fn error(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "prepared source binding: {message}; no fallback execution was attempted"
    ))
}

fn source_paths(source: &Path, format: &str) -> Result<Vec<PathBuf>> {
    if !source.is_dir() {
        return Ok(vec![source.to_owned()]);
    }
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(source).map_err(error)? {
        let entry = entry.map_err(error)?;
        let path = entry.path();
        if !entry.metadata().map_err(error)?.is_file() {
            continue;
        }
        let extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let matches = match format {
            "arrow_ipc" => matches!(extension.as_str(), "arrow" | "ipc" | "feather"),
            "jsonl" => matches!(extension.as_str(), "jsonl" | "ndjson"),
            other => extension == other,
        };
        if matches {
            paths.push(path);
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(error("source directory has no admitted files"));
    }
    Ok(paths)
}

/// Capture a versioned local source-generation binding for embedded preparation metadata.
/// No input rows are read; optional content fingerprints belong to the caller's scout.
/// # Errors
/// Rejects missing, nonregular, changing, or oversized local source inventories.
pub fn local_preparation_binding(source: &Path, format: &str, fingerprint: &str) -> Result<String> {
    let source = std::path::absolute(source).map_err(error)?;
    let files = source_paths(&source, format)?;
    // Bound the embedded metadata independently of partition count. Hash each
    // sorted generation with an explicit length delimiter; this fingerprints
    // the inventory, not file contents or artifact authenticity.
    let mut generations = Sha256::new();
    for path in &files {
        let generation = SourceIdentity::capture(path)?
            .preparation_binding()?
            .to_string();
        generations.update(
            u64::try_from(generation.len())
                .map_err(error)?
                .to_le_bytes(),
        );
        generations.update(generation.as_bytes());
    }
    let mut generations_sha256 = String::with_capacity(64);
    for byte in generations.finalize() {
        write!(generations_sha256, "{byte:02x}").map_err(error)?;
    }
    let binding = serde_json::json!({"version":1,"engine":env!("CARGO_PKG_VERSION"),
        "provider":crate::UPSTREAM_VORTEX_PROVIDER_VERSION,"source":source,"format":format,
        "fingerprint":fingerprint,"file_count":files.len(),"generations_sha256":generations_sha256})
    .to_string();
    if binding.len() > MAX_BYTES {
        return Err(error("metadata exceeds 64 KiB"));
    }
    Ok(binding)
}

pub(crate) fn validate(binding: &str) -> Result<()> {
    if binding.len() > MAX_BYTES {
        return Err(error("metadata exceeds 64 KiB"));
    }
    let fields: serde_json::Value = serde_json::from_str(binding).map_err(error)?;
    let source = fields["source"]
        .as_str()
        .ok_or_else(|| error("source missing"))?;
    let format = fields["format"]
        .as_str()
        .ok_or_else(|| error("format missing"))?;
    let fingerprint = fields["fingerprint"]
        .as_str()
        .ok_or_else(|| error("fingerprint missing"))?;
    if local_preparation_binding(Path::new(source), format, fingerprint)? != binding {
        return Err(error("source generation changed during preparation"));
    }
    Ok(())
}

/// Read and compare an existing artifact's embedded source binding before reuse.
/// Missing or mismatched metadata rejects reuse and never permits an overwrite.
/// # Errors
/// Rejects stale source generations, invalid Vortex files, and unbound artifacts.
pub fn reuse_local_preparation(path: &Path, expected: &str) -> Result<u64> {
    Ok(local_preparation_identity(path, expected)?.row_count)
}

/// Public preparation identities bound to the validated source and artifact generations.
/// These digests identify local generations, not cryptographically authenticated contents.
#[derive(Debug)]
pub struct LocalPreparationIdentity {
    pub row_count: u64,
    pub source_digest: String,
    pub prepared_digest: String,
    artifact: SourceIdentity,
    source_binding: String,
}

impl LocalPreparationIdentity {
    /// Check the held artifact and its source inventory before execution and
    /// before publishing execution evidence. Keep this identity alive between
    /// checks so replacement cannot silently rebind the preparation certificate.
    /// # Errors
    /// Rejects changed or invalidated local source and artifact generations.
    pub fn validate_generation(&self) -> Result<()> {
        self.artifact.validate()?;
        validate(&self.source_binding)?;
        self.artifact.validate()
    }
}

fn identity_digest(bytes: &[u8]) -> Result<String> {
    let mut encoded = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(encoded, "{byte:02x}").map_err(error)?;
    }
    Ok(encoded)
}

/// Resolve the same public identity after creation and on subsequent reuse.
/// Only the footer and bounded source-binding metadata are read.
/// # Errors
/// Rejects missing bindings, changed source/artifact generations and invalid Vortex files.
pub fn local_preparation_identity(path: &Path, expected: &str) -> Result<LocalPreparationIdentity> {
    use vortex::{
        VortexSessionDefault as _,
        file::OpenOptionsSessionExt as _,
        io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    };
    validate(expected)?;
    let identity = SourceIdentity::capture(path)?;
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(error)?;
    // OpenOptions intentionally omits user metadata by default. Resolve only
    // our bounded segment through the provider footer instead of loading every
    // arbitrary user metadata segment with include_metadata().
    let segment = file.footer().metadata_segment(KEY).ok_or_else(|| {
        error(
            "existing target has no source binding; preserve it or explicitly prepare a new target",
        )
    })?;
    if usize::try_from(segment.length).map_err(error)? > MAX_BYTES {
        return Err(error("existing source binding exceeds 64 KiB"));
    }
    let mut bytes = vec![0; usize::try_from(segment.length).map_err(error)?];
    std::os::unix::fs::FileExt::read_exact_at(&identity.file, &mut bytes, segment.offset)
        .map_err(error)?;
    if bytes != expected.as_bytes() {
        return Err(error(
            "existing target has no matching source binding; preserve it or explicitly prepare a new target",
        ));
    }
    identity.validate()?;
    validate(expected)?;
    let source_digest = identity_digest(expected.as_bytes())?;
    let generation = identity.preparation_binding()?;
    let prepared_binding = serde_json::json!({
        "source": source_digest,
        "artifact": generation,
        "rows": file.row_count(),
        "dtype": file.dtype().to_string(),
    });
    Ok(LocalPreparationIdentity {
        row_count: file.row_count(),
        source_digest,
        prepared_digest: identity_digest(prepared_binding.to_string().as_bytes())?,
        artifact: identity,
        source_binding: expected.to_owned(),
    })
}
