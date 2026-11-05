//! Embedded preparation provenance for the local immutable-artifact cache.
//! Generation checks are local change detection, not cryptographic authentication.

use crate::source_identity::SourceIdentity;
use crate::universal_format_io::local_dataset::LocalDataset;
use sha2::{Digest as _, Sha256};
use shardloom_core::{FloatPolicy, Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::fmt::Write as _;
use std::path::Path;

pub(crate) const KEY: &str = "shardloom.prepared-source.v1";
const MAX_BYTES: usize = 64 * 1024;

/// Fingerprint every selected file in a local collection under one grant.
/// The returned FNV-64 digest is change-detection evidence, not authentication.
/// # Errors
/// Rejects changes, cancellation, invalid collections and denied read buffers.
pub fn fingerprint_local_dataset(
    source: &Path,
    format: &str,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<(u64, String)> {
    use std::os::unix::fs::FileExt as _;
    let collection = LocalDataset::admit(source, format, memory, cancellation)?;
    let _scratch = memory.reserve(1 << 20)?;
    let mut buffer = vec![0_u8; 1 << 20];
    let mut total = 0_u64;
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut update = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for entry in &collection.files {
        cancellation.check()?;
        let identity = SourceIdentity::capture(&entry.path)?;
        let len = identity.generation.len;
        if collection.files.len() > 1 || source.is_dir() || collection.manifest_identity().is_some()
        {
            update(&(entry.path.as_os_str().len() as u64).to_le_bytes());
            update(entry.path.as_os_str().as_encoded_bytes());
            update(&len.to_le_bytes());
        }
        let mut position = 0_u64;
        while position < len {
            cancellation.check()?;
            identity.validate()?;
            let count =
                usize::try_from((len - position).min(buffer.len() as u64)).map_err(error)?;
            identity
                .file
                .read_exact_at(&mut buffer[..count], position)
                .map_err(error)?;
            update(&buffer[..count]);
            position += count as u64;
        }
        identity.validate()?;
        total = total
            .checked_add(len)
            .ok_or_else(|| error("source byte count overflow"))?;
    }
    collection.validate(cancellation)?;
    Ok((total, format!("fnv64:{hash:016x}")))
}

fn error(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "prepared source binding: {message}; no fallback execution was attempted"
    ))
}

/// Capture a versioned local source-generation binding for embedded preparation metadata.
/// No input rows are read; optional content fingerprints belong to the caller's scout.
/// # Errors
/// Rejects missing, nonregular, changing, or oversized local source inventories.
pub fn local_preparation_binding(source: &Path, format: &str, fingerprint: &str) -> Result<String> {
    local_preparation_binding_with_schema(source, format, fingerprint, None)
}

/// Bind a declared input schema to the prepared artifact's source generation.
/// Undeclared schemas preserve the existing binding representation.
/// # Errors
/// Rejects missing, changing or oversized source inventories and schema metadata.
pub fn local_preparation_binding_with_schema(
    source: &Path,
    format: &str,
    fingerprint: &str,
    source_schema: Option<&str>,
) -> Result<String> {
    local_preparation_binding_with_schema_controlled(
        source,
        format,
        fingerprint,
        source_schema,
        &CancellationToken::default(),
    )
}

/// Capture preparation provenance with cooperative cancellation between local files.
/// # Errors
/// Rejects cancellation and the same invalid inventories as the uncontrolled entry point.
pub fn local_preparation_binding_with_schema_controlled(
    source: &Path,
    format: &str,
    fingerprint: &str,
    source_schema: Option<&str>,
    cancellation: &CancellationToken,
) -> Result<String> {
    local_preparation_binding_with_float_policy(
        source,
        format,
        fingerprint,
        source_schema,
        cancellation,
        FloatPolicy::Strict,
    )
}

/// Bind the nondefault input policy as well as the immutable source generation.
/// Strict preparation retains its existing metadata representation.
/// # Errors
/// Rejects the same invalid inventories and metadata as schema-bound preparation.
pub fn local_preparation_binding_with_float_policy(
    source: &Path,
    format: &str,
    fingerprint: &str,
    source_schema: Option<&str>,
    cancellation: &CancellationToken,
    float_policy: FloatPolicy,
) -> Result<String> {
    // Preserve the legacy metadata API with a finite standalone envelope.
    // Product callers use the operation's shared pool via the explicit variant.
    let memory = LiveMemoryPool::new(64 << 20)?;
    local_preparation_binding_in_pool(
        source,
        format,
        fingerprint,
        source_schema,
        cancellation,
        float_policy,
        &memory,
    )
}

/// Bind a complete local inventory using the operation's metadata grant.
/// # Errors
/// Rejects invalid collections, changes, cancellation and denied reservations.
#[allow(clippy::too_many_arguments)]
pub fn local_preparation_binding_in_pool(
    source: &Path,
    format: &str,
    fingerprint: &str,
    source_schema: Option<&str>,
    cancellation: &CancellationToken,
    float_policy: FloatPolicy,
    memory: &LiveMemoryPool,
) -> Result<String> {
    cancellation.check()?;
    if source_schema.is_some_and(|schema| schema.len() > MAX_BYTES) {
        return Err(error("schema metadata exceeds 64 KiB"));
    }
    let source = std::path::absolute(source).map_err(error)?;
    let collection = LocalDataset::admit(&source, format, memory, cancellation)?;
    // Bound the embedded metadata independently of partition count. Hash each
    // sorted generation with an explicit length delimiter; this fingerprints
    // the inventory, not file contents or artifact authenticity.
    let mut generations = Sha256::new();
    if let Some(manifest) = collection.manifest_identity() {
        let generation = manifest.preparation_binding()?.to_string();
        generations.update((generation.len() as u64).to_le_bytes());
        generations.update(generation.as_bytes());
    }
    for file in &collection.files {
        cancellation.check()?;
        let generation = SourceIdentity::capture(&file.path)?
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
    let mut binding = serde_json::json!({"version":1,"engine":env!("CARGO_PKG_VERSION"),
        "provider":crate::UPSTREAM_VORTEX_PROVIDER_VERSION,"source":source,"format":format,
        "fingerprint":fingerprint,"file_count":collection.files.len(),"generations_sha256":generations_sha256});
    if matches!(format, "csv" | "tsv" | "json" | "jsonl" | "ndjson") {
        binding["text_codec_version"] = 2.into();
    }
    if let Some(schema) = source_schema {
        binding["source_schema"] = schema.into();
    }
    if float_policy != FloatPolicy::Strict {
        binding["float_policy"] = float_policy.as_str().into();
    }
    let binding = binding.to_string();
    if binding.len() > MAX_BYTES {
        return Err(error("metadata exceeds 64 KiB"));
    }
    cancellation.check()?;
    Ok(binding)
}

pub(crate) fn validate_controlled(binding: &str, cancellation: &CancellationToken) -> Result<()> {
    let memory = LiveMemoryPool::new(64 << 20)?;
    validate_in_pool(binding, cancellation, &memory)
}

pub(crate) fn validate_in_pool(
    binding: &str,
    cancellation: &CancellationToken,
    memory: &LiveMemoryPool,
) -> Result<()> {
    cancellation.check()?;
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
    let source_schema = fields
        .get("source_schema")
        .map(|value| value.as_str().ok_or_else(|| error("invalid source schema")))
        .transpose()?;
    let float_policy = fields
        .get("float_policy")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| error("invalid floating policy"))
        })
        .transpose()?
        .map(FloatPolicy::parse)
        .transpose()?
        .unwrap_or_default();
    if local_preparation_binding_in_pool(
        Path::new(source),
        format,
        fingerprint,
        source_schema,
        cancellation,
        float_policy,
        memory,
    )? != binding
    {
        return Err(error("source generation changed during preparation"));
    }
    Ok(())
}

/// Read and compare an existing artifact's embedded source binding before reuse.
/// Missing or mismatched metadata rejects reuse and never permits an overwrite.
/// # Errors
/// Rejects stale source generations, invalid Vortex files, and unbound artifacts.
pub fn reuse_local_preparation(
    path: &Path,
    expected: &str,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
) -> Result<u64> {
    Ok(local_preparation_identity(path, expected, memory)?.row_count)
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
    memory: LiveMemoryPool,
    _metadata: shardloom_exec::live_memory::MemoryLease,
}

impl LocalPreparationIdentity {
    /// Check the held artifact and its source inventory before execution and
    /// before publishing execution evidence. Keep this identity alive between
    /// checks so replacement cannot silently rebind the preparation certificate.
    /// # Errors
    /// Rejects changed or invalidated local source and artifact generations.
    pub fn validate_generation(&self) -> Result<()> {
        self.validate_generation_controlled(&CancellationToken::default())
    }

    /// Revalidate source and artifact generations using the operation's cancellation owner.
    /// # Errors
    /// Rejects cancellation, changed or invalidated local generations.
    pub fn validate_generation_controlled(&self, cancellation: &CancellationToken) -> Result<()> {
        cancellation.check()?;
        self.artifact.validate()?;
        validate_in_pool(&self.source_binding, cancellation, &self.memory)?;
        self.artifact.validate()?;
        cancellation.check()
    }

    /// Reject output aliases of the prepared artifact or any compatibility input.
    /// An output inside an input directory would also change that input inventory.
    /// # Errors
    /// Rejects changed generations, unreadable identities and conflicting outputs.
    pub fn validate_destination(&self, path: &Path) -> Result<()> {
        self.validate_destination_controlled(path, &CancellationToken::default())
    }

    /// Check output aliases with the operation's cancellation owner.
    /// # Errors
    /// Rejects cancellation, changed generations and conflicting outputs.
    pub fn validate_destination_controlled(
        &self,
        path: &Path,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        self.validate_generation_controlled(cancellation)?;
        let fields: serde_json::Value =
            serde_json::from_str(&self.source_binding).map_err(error)?;
        let source = Path::new(
            fields["source"]
                .as_str()
                .ok_or_else(|| error("source missing"))?,
        );
        let format = fields["format"]
            .as_str()
            .ok_or_else(|| error("format missing"))?;
        let collection = LocalDataset::admit(source, format, &self.memory, cancellation)?;
        collection.validate_destination(path)?;
        let target = match std::fs::metadata(path) {
            Ok(target) => target,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return cancellation.check();
            }
            Err(failure) => return Err(error(failure)),
        };
        if (target.dev(), target.ino())
            == (
                self.artifact.generation.device,
                self.artifact.generation.inode,
            )
        {
            return Err(error("source and output must be different files"));
        }
        for source in &collection.files {
            cancellation.check()?;
            let metadata = std::fs::metadata(&source.path).map_err(error)?;
            if (target.dev(), target.ino()) == (metadata.dev(), metadata.ino()) {
                return Err(error("source and output must be different files"));
            }
        }
        cancellation.check()
    }
}

fn identity_digest(bytes: &[u8]) -> Result<String> {
    let mut encoded = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(encoded, "{byte:02x}").map_err(error)?;
    }
    Ok(encoded)
}

#[test]
fn preparation_identity_reserves_before_binding_validation_or_source_access() {
    let memory = shardloom_exec::live_memory::LiveMemoryPool::new(1).unwrap();
    let error =
        local_preparation_identity(Path::new("must-not-be-opened"), "{}", &memory).unwrap_err();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn controlled_preparation_binding_and_identity_refuse_before_source_access() {
    let memory = shardloom_exec::live_memory::LiveMemoryPool::new(4096).unwrap();
    let retained = memory.reserve(17).unwrap();
    let parent = CancellationToken::default();
    for token in [
        {
            let token = CancellationToken::default();
            token.cancel();
            token
        },
        parent.with_deadline(std::time::Instant::now()),
    ] {
        let source = Path::new("must-not-be-opened");
        let error = local_preparation_binding_with_schema_controlled(
            source,
            "csv",
            "metadata_only",
            None,
            &token,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("cancel") || error.to_string().contains("deadline"),
            "{error}"
        );
        let error =
            local_preparation_identity_controlled(source, "invalid binding", &memory, &token)
                .unwrap_err();
        assert!(
            error.to_string().contains("cancel") || error.to_string().contains("deadline"),
            "{error}"
        );
        assert_eq!(memory.snapshot().reserved_bytes, 17);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 17);
        parent.check().unwrap();
    }
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

/// Resolve the same public identity after creation and on subsequent reuse.
/// Only the footer and bounded source-binding metadata are read.
/// # Errors
/// Rejects missing bindings, changed source/artifact generations and invalid Vortex files.
pub fn local_preparation_identity(
    path: &Path,
    expected: &str,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
) -> Result<LocalPreparationIdentity> {
    local_preparation_identity_controlled(path, expected, memory, &CancellationToken::default())
}

/// Read bounded preparation metadata using the operation's cancellation owner.
/// # Errors
/// Rejects cancellation, changed generations, missing bindings and invalid Vortex files.
pub fn local_preparation_identity_controlled(
    path: &Path,
    expected: &str,
    memory: &shardloom_exec::live_memory::LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<LocalPreparationIdentity> {
    use vortex::{
        VortexSessionDefault as _,
        array::memory::HostMemorySessionExt as _,
        file::OpenOptionsSessionExt as _,
        io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    };
    cancellation.check()?;
    if expected.len() > MAX_BYTES {
        return Err(error("metadata exceeds 64 KiB"));
    }
    let metadata = memory.reserve(
        u64::try_from(expected.len())
            .map_err(error)?
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| error("metadata reservation overflow"))?,
    )?;
    validate_in_pool(expected, cancellation, memory)?;
    let identity = SourceIdentity::capture(path)?;
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default()
        .with_handle(runtime.handle())
        .with_host_allocator(std::sync::Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        ));
    crate::native_extensions::register(&session);
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(error)?;
    cancellation.check()?;
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
    let _binding_scratch = memory.reserve(u64::from(segment.length))?;
    let mut bytes = vec![0; usize::try_from(segment.length).map_err(error)?];
    std::os::unix::fs::FileExt::read_exact_at(&identity.file, &mut bytes, segment.offset)
        .map_err(error)?;
    cancellation.check()?;
    if bytes != expected.as_bytes() {
        return Err(error(
            "existing target has no matching source binding; preserve it or explicitly prepare a new target",
        ));
    }
    identity.validate()?;
    validate_in_pool(expected, cancellation, memory)?;
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
        memory: memory.clone(),
        _metadata: metadata,
    })
}
