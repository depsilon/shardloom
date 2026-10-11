//! Local source expansion owns its strings, listing state and parsing workspace
//! inside the complete operation's existing grant. No input-count limit is used.

use std::{fs, path::Path};

use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};

use crate::fixture_io::{copy_text, read_utf8, reserve_additional};

#[cfg(test)]
#[path = "native_vortex_input_binding_tests.rs"]
mod tests;

#[derive(Debug)]
pub(super) struct NativeVortexInputBinding {
    pub(super) mode: &'static str,
    pub(super) sources: Vec<String>,
    // Storage precedes its reservation so buffers are dropped before credits.
    lease: MemoryLease,
}

impl NativeVortexInputBinding {
    pub(super) fn single(source: &str, pool: &LiveMemoryPool) -> Result<Self> {
        let mut binding = Self::empty("single_file", pool)?;
        binding.push(source)?;
        Ok(binding)
    }

    fn empty(mode: &'static str, pool: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            mode,
            sources: Vec::new(),
            lease: pool.reserve(0)?,
        })
    }

    fn push(&mut self, source: &str) -> Result<()> {
        reserve_additional(&mut self.sources, 1, &mut self.lease)?;
        self.sources.push(copy_text(source, &mut self.lease)?);
        Ok(())
    }

    pub(super) fn is_partitioned(&self) -> bool {
        self.sources.len() > 1
    }

    pub(super) fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "native_vortex_input_binding_schema_version".into(),
                "shardloom.native_vortex_input_binding.v1".into(),
            ),
            ("native_vortex_input_binding_mode".into(), self.mode.into()),
            (
                "native_vortex_input_binding_count".into(),
                self.sources.len().to_string(),
            ),
            (
                "native_vortex_input_binding_strategy".into(),
                if self.is_partitioned() {
                    "sequential_capillary_parts"
                } else {
                    "single_source"
                }
                .into(),
            ),
            (
                "native_vortex_partitioned_input_binding".into(),
                self.is_partitioned().to_string(),
            ),
            (
                "native_vortex_input_binding_sources".into(),
                self.sources.join(","),
            ),
        ]
    }
}

/// Charge control/path workspace before even probing the filesystem. The
/// workspace covers path copies and filesystem iterator metadata, not process RSS.
fn inspect_workspace(pool: &LiveMemoryPool, path: &str) -> Result<MemoryLease> {
    let bytes = path
        .len()
        .checked_mul(8)
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(size_error)?;
    pool.reserve(bytes)
}

fn local_path(input: &str) -> &Path {
    Path::new(input.strip_prefix("file://").unwrap_or(input))
}

#[cfg(any(test, all(feature = "vortex-local-primitives", unix)))]
pub(super) fn is_collection(
    input: &str,
    pool: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<bool> {
    cancellation.check()?;
    let _inspection = inspect_workspace(pool, input)?;
    let path = local_path(input);
    let collection = path.is_dir() || (manifest_candidate(path) && path.is_file());
    cancellation.check()?;
    Ok(collection)
}

pub(super) fn bind(
    input: &str,
    pool: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<NativeVortexInputBinding> {
    cancellation.check()?;
    let _inspection = inspect_workspace(pool, input)?;
    let path = local_path(input);
    let binding = if path.is_dir() {
        directory_binding(path, pool, cancellation)?
    } else if manifest_candidate(path) && path.is_file() {
        manifest_binding(path, pool, cancellation)?
    } else {
        NativeVortexInputBinding::single(input, pool)?
    };
    cancellation.check()?;
    Ok(binding)
}

fn manifest_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            ["json", "manifest", "vortex-manifest"]
                .iter()
                .any(|extension| value.eq_ignore_ascii_case(extension))
        })
}

fn vortex_file(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            value.eq_ignore_ascii_case("vortex") || value.eq_ignore_ascii_case("vtx")
        })
}

fn directory_binding(
    directory: &Path,
    pool: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<NativeVortexInputBinding> {
    let mut binding = NativeVortexInputBinding::empty("local_directory", pool)?;
    // Explicit stack retains a reservation per open directory and never grows
    // the Rust call stack with nesting. Ancestor identities reject link cycles.
    let mut stack_lease = pool.reserve(0)?;
    let mut frames = Vec::new();
    push_directory(directory, pool, &mut frames, &mut stack_lease)?;
    while let Some(frame) = frames.last_mut() {
        cancellation.check()?;
        let Some(entry) = frame.entries.next() else {
            frames.pop();
            continue;
        };
        let entry = entry.map_err(|error| io_error("directory entry", &error))?;
        // The OS returns one entry at a time. Charge any joined path before
        // allocating it; the directory frame covers the entry's own metadata.
        let path_bytes = frame
            .path_bytes
            .checked_add(entry.file_name().len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(size_error)?;
        let _path_lease = pool.reserve(
            u64::try_from(path_bytes.checked_mul(8).ok_or_else(size_error)?)
                .map_err(|_| size_error())?,
        )?;
        let path = entry.path();
        cancellation.check()?;
        if path.is_dir() {
            push_directory(&path, pool, &mut frames, &mut stack_lease)?;
        } else if vortex_file(&path) {
            binding.push(&path.to_string_lossy())?;
        }
    }
    binding.sources.sort_unstable();
    binding.sources.dedup();
    if binding.sources.is_empty() {
        return Err(failed(
            "native Vortex input directory contains no .vortex or .vtx files",
        ));
    }
    Ok(binding)
}

struct DirectoryFrame {
    entries: fs::ReadDir,
    identity: std::path::PathBuf,
    path_bytes: usize,
    _lease: MemoryLease,
}

fn push_directory(
    path: &Path,
    pool: &LiveMemoryPool,
    frames: &mut Vec<DirectoryFrame>,
    stack_lease: &mut MemoryLease,
) -> Result<()> {
    reserve_additional(frames, 1, stack_lease)?;
    let lease = inspect_workspace(pool, &path.to_string_lossy())?;
    let identity = path
        .canonicalize()
        .map_err(|error| io_error("directory identity", &error))?;
    if frames.iter().any(|frame| frame.identity == identity) {
        return Err(failed(
            "native Vortex input directory contains a symbolic-link cycle",
        ));
    }
    let entries = fs::read_dir(path).map_err(|error| io_error("directory", &error))?;
    frames.push(DirectoryFrame {
        entries,
        identity,
        path_bytes: path.as_os_str().len(),
        _lease: lease,
    });
    Ok(())
}

fn manifest_binding(
    path: &Path,
    pool: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<NativeVortexInputBinding> {
    cancellation.check()?;
    let raw = read_utf8(
        path,
        pool.snapshot().limit_bytes,
        "native Vortex input manifest",
        pool,
    )
    .map_err(|error| io_error("manifest", &error))?;
    cancellation.check()?;
    // serde_json's Value tree, maps, strings, parser scratch and replacement
    // capacity coexist with the input. This conservative workspace scales with
    // bytes actually read; it is neither an input limit nor a separate grant.
    let parser_bytes = raw
        .value()
        .len()
        .checked_mul(128)
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(size_error)?;
    let _parser = pool.reserve(parser_bytes)?;
    let payload: serde_json::Value = serde_json::from_str(raw.value()).map_err(|error| {
        failed(&format!(
            "native Vortex input manifest is not valid JSON: {error}"
        ))
    })?;
    let array = payload
        .get("inputs")
        .or_else(|| payload.get("paths"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            failed("native Vortex input manifest must contain an inputs or paths array")
        })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut binding = NativeVortexInputBinding::empty("manifest", pool)?;
    for value in array {
        cancellation.check()?;
        let raw_path = value
            .as_str()
            .ok_or_else(|| failed("native Vortex input manifest entries must be strings"))?;
        let _path_workspace = inspect_workspace(pool, raw_path)?;
        let _base_workspace = inspect_workspace(pool, &base.to_string_lossy())?;
        let entry = Path::new(raw_path);
        let resolved = if entry.is_absolute() {
            entry.to_path_buf()
        } else {
            base.join(entry)
        };
        if !resolved.is_file() {
            return Err(failed(&format!(
                "native Vortex input manifest entry is not an existing file: {}",
                resolved.display()
            )));
        }
        if !vortex_file(&resolved) {
            return Err(failed(&format!(
                "native Vortex input manifest entry is not a .vortex or .vtx file: {}",
                resolved.display()
            )));
        }
        binding.push(&resolved.to_string_lossy())?;
    }
    // Check duplicates without copying source text or changing manifest order.
    let mut order_lease = pool.reserve(0)?;
    let mut ordered = Vec::new();
    reserve_additional(&mut ordered, binding.sources.len(), &mut order_lease)?;
    ordered.extend(binding.sources.iter().map(String::as_str));
    ordered.sort_unstable();
    if let Some([duplicate, _]) = ordered.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(failed(&format!(
            "native Vortex input manifest contains duplicate entry: {duplicate}"
        )));
    }
    if binding.sources.is_empty() {
        return Err(failed(
            "native Vortex input manifest contains no .vortex or .vtx files",
        ));
    }
    cancellation.check()?;
    Ok(binding)
}

fn size_error() -> ShardLoomError {
    failed("native Vortex input binding size overflow")
}

fn io_error(context: &str, error: &std::io::Error) -> ShardLoomError {
    failed(&format!(
        "failed to read native Vortex input {context}: {error}"
    ))
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}
