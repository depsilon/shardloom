//! Explicit local binary-object ownership and checked lazy byte access.
//!
//! Inline Binary columns remain ordinary values. Object columns carry a
//! versioned descriptor; their payload lives in this session's retained arena
//! or in the raw segments of a persisted native Vortex artifact.

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    any::Any,
    fmt,
    ops::Range,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use vortex::session::{SessionExt as _, SessionVar, VortexSession};

use crate::{
    resident_session::{LocalReadMetrics, NativeExecutionContext, ResidentVortexSession},
    source_identity::SourceIdentity,
};

#[path = "binary_object_catalog.rs"]
pub(crate) mod catalog;
#[path = "binary_object_descriptor.rs"]
mod descriptor;
#[path = "binary_object_layout.rs"]
pub(crate) mod layout;
#[path = "binary_object_store.rs"]
mod store;
#[path = "binary_object_transfer.rs"]
pub(crate) mod transfer;
#[path = "binary_object_writer.rs"]
pub(crate) mod writer;
pub use store::{BinaryObjectStore, BinaryObjectStoreSnapshot};

pub(crate) type ObjectId = [u8; 32];
pub(crate) const CHUNK_BYTES: usize = 64 * 1024;

/// Observed object-payload work. Table/footer reads retain their existing
/// native I/O evidence; these counters count only explicit object access.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BinaryObjectSnapshot {
    /// Distinct managed identities retained by this session.
    pub managed_objects: usize,
    /// Logical managed bytes retained; shared content is counted once.
    pub managed_bytes: u64,
    /// Actual local payload read requests.
    pub payload_read_requests: u64,
    /// Actual bytes read, including whole integrity chunks for selected ranges.
    pub payload_read_bytes: u64,
    /// Bytes delivered by successful explicit object reads.
    pub delivered_bytes: u64,
}

/// A byte object whose owner survives its source, query, and caller session.
/// Clones share ownership. Merely inspecting this handle does not read bytes.
#[derive(Clone)]
pub struct BinaryObject {
    payload: Arc<Payload>,
    descriptor: Arc<Budgeted<Vec<u8>>>,
    session: ResidentVortexSession,
}

impl fmt::Debug for BinaryObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryObject")
            .field("managed", &self.is_managed())
            .field("byte_length", &self.len())
            .finish_non_exhaustive()
    }
}

impl BinaryObject {
    /// Check that an input adapter is carrying an actual handle admitted by
    /// this owner, rather than a descriptor borrowed from another session.
    /// # Errors
    /// Rejects a different session or an invalidated backing generation.
    pub fn validate_owner(&self, session: &ResidentVortexSession) -> Result<()> {
        if !self.session.same_owner(session) {
            return Err(error(
                "OWNERSHIP",
                "binary object belongs to another session; import its owner explicitly",
            ));
        }
        self.payload.validate()
    }

    /// Number of logical bytes; zero is a valid object.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.payload.length
    }

    /// Whether the object has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Managed bytes belong to the native owner; external ranges do not.
    #[must_use]
    pub fn is_managed(&self) -> bool {
        self.payload.id.is_some()
    }

    /// Versioned descriptor for a binary-object column. This is not its payload.
    #[must_use]
    pub fn descriptor(&self) -> &[u8] {
        self.descriptor.value()
    }

    /// Retrieve all bytes, subject to the session's live memory budget.
    /// # Errors
    /// Rejects unavailable/changed content, denied memory or cancellation.
    pub fn read(&self, cancellation: &CancellationToken) -> Result<Bytes> {
        self.read_range(0..self.len(), cancellation)
    }

    /// Retrieve exactly this range. Only touched integrity chunks are read.
    /// Empty ranges still validate the held source. Buffers retain memory
    /// credits until their last view drops.
    /// # Errors
    /// Rejects invalid bounds, unavailable/changed/corrupt content, denied
    /// memory, or cancellation. A short read is an error, never partial success.
    pub fn read_range(&self, range: Range<u64>, cancellation: &CancellationToken) -> Result<Bytes> {
        let arena = self.session.binary_object_arena()?;
        let bytes = self
            .session
            .with_native_execution_context(cancellation, |context| {
                let bytes = self.payload.read_range(range, context, &arena.0.metrics)?;
                Ok(bytes)
            })?;
        arena
            .0
            .delivered
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

/// Immutable segment description. Offsets belong to its held local backing,
/// not to another file or a caller-supplied arbitrary URI.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Chunk {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) digest: [u8; 32],
}

pub(crate) struct Payload {
    pub(crate) id: Option<ObjectId>,
    pub(crate) length: u64,
    pub(crate) chunks: Budgeted<Vec<Chunk>>,
    pub(crate) backing: Backing,
    // Release data and metadata before refunding their owner overhead.
    _metadata: MemoryLease,
}

pub(crate) enum Backing {
    Memory(Arc<Budgeted<Vec<u8>>>),
    File {
        identity: Arc<SourceIdentity>,
        offset: u64,
        // A managed private file cannot be reclaimed while its payload is live.
        _retention: Option<Arc<store::TemporaryObject>>,
    },
}

impl Payload {
    fn validate(&self) -> Result<()> {
        match &self.backing {
            Backing::Memory(_) => Ok(()),
            Backing::File { identity, .. } => identity.validate(),
        }
    }

    pub(crate) fn read_range(
        &self,
        range: Range<u64>,
        context: &NativeExecutionContext<'_>,
        metrics: &Arc<LocalReadMetrics>,
    ) -> Result<Bytes> {
        context.check_cancelled()?;
        if range.start > range.end || range.end > self.length {
            return Err(error(
                "RANGE",
                "requested range is outside the binary object",
            ));
        }
        self.validate()?;
        let length = usize::try_from(range.end - range.start)
            .map_err(|_| error("RANGE", "requested object range exceeds addressable memory"))?;
        if let Backing::Memory(data) = &self.backing {
            // The actual imported allocation, including an empty allocation's
            // owner, stays charged across every escaping view.
            return Ok(Bytes::from_owner(MemoryView {
                data: Arc::clone(data),
                range: usize::try_from(range.start)
                    .map_err(|_| error("RANGE", "object offset exceeds addressable memory"))?
                    ..usize::try_from(range.end)
                        .map_err(|_| error("RANGE", "object offset exceeds addressable memory"))?,
            }));
        }
        let Backing::File {
            identity, offset, ..
        } = &self.backing
        else {
            unreachable!("memory backing returned above");
        };
        let reader = context.local_source_reader(Arc::clone(identity), Arc::clone(metrics))?;
        if self.id.is_none() {
            let start = offset
                .checked_add(range.start)
                .ok_or_else(|| error("RANGE", "external object offset overflows"))?;
            let bytes = reader.bytes(start, length)?;
            // Even zero-length reads retain a generation check after admission.
            self.validate()?;
            context.check_cancelled()?;
            return Ok(bytes);
        }
        let (mut output, credit) = reserved_vec::<u8>(context.memory(), length)?;
        output.resize(length, 0);
        let first = range.start / CHUNK_BYTES as u64;
        let end = range.end.div_ceil(CHUNK_BYTES as u64);
        if !range.is_empty() {
            for index in first..end {
                context.check_cancelled()?;
                let chunk = self
                    .chunks
                    .value()
                    .get(
                        usize::try_from(index)
                            .map_err(|_| error("RANGE", "object chunk index overflows"))?,
                    )
                    .ok_or_else(|| error("DIRECTORY", "object is missing a required chunk"))?;
                let bytes = reader.bytes(chunk.offset, chunk.length as usize)?;
                if digest(&bytes) != chunk.digest {
                    return Err(error(
                        "INTEGRITY",
                        "managed object chunk checksum does not match",
                    ));
                }
                let origin = index * CHUNK_BYTES as u64;
                let start = range.start.max(origin);
                let stop = range.end.min(origin + u64::from(chunk.length));
                let target_start = usize::try_from(start - range.start)
                    .map_err(|_| error("RANGE", "object output offset overflows"))?;
                let source_start = usize::try_from(start - origin)
                    .map_err(|_| error("RANGE", "object chunk offset overflows"))?;
                let count = usize::try_from(stop - start)
                    .map_err(|_| error("RANGE", "object chunk length overflows"))?;
                output[target_start..target_start + count]
                    .copy_from_slice(&bytes[source_start..source_start + count]);
            }
        }
        self.validate()?;
        context.check_cancelled()?;
        Ok(Bytes::from_owner(OwnedBytes(Budgeted::new(output, credit))))
    }
}

struct MemoryView {
    data: Arc<Budgeted<Vec<u8>>>,
    range: Range<usize>,
}
impl AsRef<[u8]> for MemoryView {
    fn as_ref(&self) -> &[u8] {
        &self.data.value()[self.range.clone()]
    }
}
struct OwnedBytes(Budgeted<Vec<u8>>);
impl AsRef<[u8]> for OwnedBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.value()
    }
}

#[derive(Clone)]
pub(crate) struct Arena(Arc<ArenaOwner>);
struct ArenaOwner {
    memory: LiveMemoryPool,
    objects: Mutex<Budgeted<Vec<Arc<Payload>>>>,
    metrics: Arc<LocalReadMetrics>,
    delivered: AtomicU64,
}

impl fmt::Debug for Arena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryObjectArena").finish_non_exhaustive()
    }
}
impl SessionVar for Arena {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Arena {
    pub(crate) fn install(session: &VortexSession, memory: LiveMemoryPool) -> Result<()> {
        let (values, credit) = reserved_vec(&memory, 0)?;
        session.register(Self(Arc::new(ArenaOwner {
            memory,
            objects: Mutex::new(Budgeted::new(values, credit)),
            metrics: Arc::new(LocalReadMetrics::default()),
            delivered: AtomicU64::new(0),
        })));
        Ok(())
    }

    pub(crate) fn get(session: &VortexSession) -> Result<Self> {
        session
            .get_opt::<Self>()
            .map(|arena| (*arena).clone())
            .ok_or_else(|| error("OWNER", "binary objects require a resident native session"))
    }

    pub(crate) fn insert(
        &self,
        value: Arc<Payload>,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Payload>> {
        let id = value
            .id
            .ok_or_else(|| error("OWNER", "external content cannot enter managed storage"))?;
        let mut objects = self
            .0
            .objects
            .lock()
            .map_err(|_| error("OWNER", "binary object arena is poisoned"))?;
        cancellation.check()?;
        match objects
            .value()
            .binary_search_by_key(&id, |object| object.id.expect("managed arena"))
        {
            Ok(index) => {
                let current = &objects.value()[index];
                if current.length != value.length {
                    return Err(error(
                        "INTEGRITY",
                        "object identity has inconsistent length",
                    ));
                }
                if current.validate().is_ok() {
                    return Ok(Arc::clone(current));
                }
                // A newly admitted immutable copy can replace an unavailable
                // locator for identical content; existing handles keep their
                // own generation and still reject its invalidation.
                let count = objects.value().len();
                let (mut replacement, credit) = reserved_vec(&self.0.memory, count)?;
                replacement.extend(objects.value().iter().cloned());
                replacement[index] = Arc::clone(&value);
                cancellation.check()?;
                *objects = Budgeted::new(replacement, credit);
            }
            Err(index) => {
                let count = objects
                    .value()
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| error("RESOURCE", "object count overflows"))?;
                // Reallocate under a complete second credit, then release the
                // prior container. No allocator-dependent uncharged growth.
                let (mut replacement, credit) = reserved_vec(&self.0.memory, count)?;
                replacement.extend(objects.value()[..index].iter().cloned());
                replacement.push(Arc::clone(&value));
                replacement.extend(objects.value()[index..].iter().cloned());
                cancellation.check()?;
                *objects = Budgeted::new(replacement, credit);
            }
        }
        Ok(value)
    }

    pub(crate) fn lookup(&self, id: &ObjectId, length: u64) -> Result<Arc<Payload>> {
        let objects = self
            .0
            .objects
            .lock()
            .map_err(|_| error("OWNER", "binary object arena is poisoned"))?;
        let index = objects
            .value()
            .binary_search_by_key(id, |object| object.id.expect("managed arena"))
            .map_err(|_| {
                error(
                    "MISSING",
                    "managed object bytes are not bound to this native owner",
                )
            })?;
        let object = &objects.value()[index];
        if object.length != length {
            return Err(error(
                "DESCRIPTOR",
                "managed object descriptor length does not match its owner",
            ));
        }
        Ok(Arc::clone(object))
    }

    pub(crate) fn insert_batch(
        &self,
        batch: &[Arc<Payload>],
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if batch.is_empty() {
            return cancellation.check();
        }
        let mut objects = self
            .0
            .objects
            .lock()
            .map_err(|_| error("OWNER", "binary object arena is poisoned"))?;
        cancellation.check()?;
        let count = objects
            .value()
            .len()
            .checked_add(batch.len())
            .ok_or_else(|| error("RESOURCE", "object count overflows"))?;
        let (mut replacement, credit) = reserved_vec(&self.0.memory, count)?;
        replacement.extend(objects.value().iter().cloned());
        for object in batch {
            cancellation.check()?;
            let id = object
                .id
                .ok_or_else(|| error("OWNER", "external content cannot enter managed storage"))?;
            match replacement.binary_search_by_key(&id, |value| value.id.expect("managed arena")) {
                Ok(index) => {
                    if replacement[index].length != object.length {
                        return Err(error(
                            "INTEGRITY",
                            "object identity has inconsistent length",
                        ));
                    }
                    if replacement[index].validate().is_err() {
                        replacement[index] = Arc::clone(object);
                    }
                }
                Err(index) => replacement.insert(index, Arc::clone(object)),
            }
        }
        cancellation.check()?;
        // Exactly one publication point: admission or validation failure leaves
        // the existing arena unchanged, including when a later object fails.
        *objects = Budgeted::new(replacement, credit);
        Ok(())
    }

    fn snapshot(&self) -> Result<BinaryObjectSnapshot> {
        let objects = self
            .0
            .objects
            .lock()
            .map_err(|_| error("OWNER", "binary object arena is poisoned"))?;
        let managed_bytes = objects.value().iter().try_fold(0_u64, |sum, object| {
            sum.checked_add(object.length)
                .ok_or_else(|| error("RESOURCE", "retained object-byte count overflows"))
        })?;
        Ok(BinaryObjectSnapshot {
            managed_objects: objects.value().len(),
            managed_bytes,
            payload_read_requests: self.0.metrics.requests.load(Ordering::Relaxed),
            payload_read_bytes: self.0.metrics.bytes.load(Ordering::Relaxed),
            delivered_bytes: self.0.delivered.load(Ordering::Relaxed),
        })
    }
}

impl ResidentVortexSession {
    /// Resolve a checked object descriptor under this session's retained owners.
    /// Managed bytes must already be owned or admitted from a complete native
    /// file. External descriptors retain their declared local generation.
    /// # Errors
    /// Rejects malformed, unbound, changed or inaccessible objects and denied
    /// memory. This metadata-only lookup is safe while a producer awaits its
    /// consumer; it never enters the compute runtime or reads object payloads.
    pub fn binary_object_from_descriptor(
        &self,
        encoded: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        cancellation.check()?;
        let payload = match descriptor::parse(encoded)? {
            descriptor::Descriptor::Managed { id, length } => {
                let payload = self.binary_object_arena()?.lookup(&id, length)?;
                payload.validate()?;
                payload
            }
            descriptor::Descriptor::External {
                path,
                range,
                generation,
            } => {
                let metadata = self
                    .memory()
                    .reserve(4096_u64.saturating_add(path.len() as u64))?;
                let identity = Arc::new(SourceIdentity::capture(Path::new(path))?);
                if identity.generation_fingerprint() != generation
                    || range.end > identity.generation.len
                {
                    return Err(error(
                        "SOURCE",
                        "external object no longer has its declared generation or range",
                    ));
                }
                let (chunks, credit) = reserved_vec(self.memory(), 0)?;
                Arc::new(Payload {
                    id: None,
                    length: range.end - range.start,
                    chunks: Budgeted::new(chunks, credit),
                    backing: Backing::File {
                        identity,
                        offset: range.start,
                        _retention: None,
                    },
                    _metadata: metadata,
                })
            }
        };
        let handle = self.object_handle(payload, encoded)?;
        cancellation.check()?;
        Ok(handle)
    }

    /// Copy caller bytes into one admitted immutable managed object.
    /// # Errors
    /// Rejects denied memory, arithmetic overflow or cancellation.
    pub fn binary_object(
        &self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        let handle = self.with_native_execution_context(cancellation, |context| {
            let (mut copied, credit) = reserved_vec(context.memory(), bytes.len())?;
            let count = bytes.len().div_ceil(CHUNK_BYTES);
            let (mut chunks, chunk_credit) = reserved_vec(context.memory(), count)?;
            for (index, bytes) in bytes.chunks(CHUNK_BYTES).enumerate() {
                context.check_cancelled()?;
                copied.extend_from_slice(bytes);
                chunks.push(Chunk {
                    offset: (index * CHUNK_BYTES) as u64,
                    length: u32::try_from(bytes.len())
                        .map_err(|_| error("RESOURCE", "object chunk length overflows"))?,
                    digest: digest(bytes),
                });
            }
            let length = bytes.len() as u64;
            let id = content_id(length, &chunks);
            let metadata = context.memory().reserve(512)?;
            let payload = Arc::new(Payload {
                id: Some(id),
                length,
                chunks: Budgeted::new(chunks, chunk_credit),
                backing: Backing::Memory(Arc::new(Budgeted::new(copied, credit))),
                _metadata: metadata,
            });
            let handle = self.object_handle(payload, &descriptor::managed(&id, length))?;
            context.check_cancelled()?;
            Ok(handle)
        })?;
        self.publish_binary_object(handle, cancellation)
    }

    fn publish_binary_object(
        &self,
        mut handle: BinaryObject,
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        // Reads, final generation checks and I/O drain completed before this
        // final publication point. No later fallible step can retain an object
        // after returning failed intake.
        handle.payload = self
            .binary_object_arena()?
            .insert(Arc::clone(&handle.payload), cancellation)?;
        Ok(handle)
    }

    /// Reference an explicitly externally owned local byte range without
    /// copying or claiming ownership of the file.
    /// # Errors
    /// Rejects nonregular/missing files, non-UTF-8 paths, invalid ranges,
    /// changed generations, denied memory or cancellation.
    pub fn external_binary_object(
        &self,
        path: &Path,
        range: Range<u64>,
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        self.with_native_execution_context(cancellation, |context| {
            let path = std::fs::canonicalize(path).map_err(|cause| error("SOURCE", cause))?;
            let path_text = path
                .to_str()
                .ok_or_else(|| error("DESCRIPTOR", "external object path must be UTF-8"))?;
            let metadata = context
                .memory()
                .reserve(4096_u64.saturating_add(path_text.len() as u64))?;
            let identity = Arc::new(SourceIdentity::capture(&path)?);
            if range.start > range.end || range.end > identity.generation.len {
                return Err(error("RANGE", "external object range exceeds its source"));
            }
            let count = 58_usize
                .checked_add(path_text.len())
                .ok_or_else(|| error("DESCRIPTOR", "external descriptor size overflows"))?;
            let (mut encoded, credit) = reserved_vec(context.memory(), count)?;
            descriptor::external(
                path_text,
                range.clone(),
                &identity.generation_fingerprint(),
                &mut encoded,
            )?;
            let (chunks, chunk_credit) = reserved_vec(context.memory(), 0)?;
            let payload = Arc::new(Payload {
                id: None,
                length: range.end - range.start,
                chunks: Budgeted::new(chunks, chunk_credit),
                backing: Backing::File {
                    identity,
                    offset: range.start,
                    _retention: None,
                },
                _metadata: metadata,
            });
            context.check_cancelled()?;
            payload.validate()?;
            Ok(BinaryObject {
                payload,
                descriptor: Arc::new(Budgeted::new(encoded, credit)),
                session: self.clone(),
            })
        })
    }

    fn object_handle(&self, payload: Arc<Payload>, encoded: &[u8]) -> Result<BinaryObject> {
        let (mut bytes, credit) = reserved_vec(self.memory(), encoded.len())?;
        bytes.extend_from_slice(encoded);
        Ok(BinaryObject {
            payload,
            descriptor: Arc::new(Budgeted::new(bytes, credit)),
            session: self.clone(),
        })
    }

    /// Observe payload work and retained managed content without fetching bytes.
    /// # Errors
    /// Rejects poisoned ownership state or overflowing counters.
    pub fn binary_object_snapshot(&self) -> Result<BinaryObjectSnapshot> {
        self.with_native_execution_context(&CancellationToken::default(), |context| {
            Arena::get(context.native_session())?.snapshot()
        })
    }
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn content_id(length: u64, chunks: &[Chunk]) -> ObjectId {
    let mut hash = Sha256::new();
    hash.update(b"shardloom.binary-object.chunks.v1\0");
    hash.update((CHUNK_BYTES as u64).to_le_bytes());
    hash.update(length.to_le_bytes());
    hash.update((chunks.len() as u64).to_le_bytes());
    for chunk in chunks {
        hash.update(chunk.length.to_le_bytes());
        hash.update(chunk.digest);
    }
    hash.finalize().into()
}

pub(crate) fn reserved_vec<T>(
    memory: &LiveMemoryPool,
    count: usize,
) -> Result<(Vec<T>, MemoryLease)> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| error("RESOURCE", "object allocation size overflows"))?;
    let credit = memory.reserve(u64::try_from(bytes).map_err(|_| {
        error(
            "RESOURCE",
            "object allocation size exceeds the budget width",
        )
    })?)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|cause| error("RESOURCE", cause))?;
    if values.capacity() > count && std::mem::size_of::<T>() != 0 {
        return Err(error(
            "RESOURCE",
            "object allocation exceeded its admission",
        ));
    }
    Ok((values, credit))
}

pub(crate) fn error(code: &str, message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "SL-BINARY-OBJECT-{code}: {message}; no fallback execution was attempted"
    ))
}

pub(crate) fn validate_descriptors(
    array: &vortex::array::ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    use vortex::array::VortexSessionExecute as _;
    if !crate::native_binary_type::contains(array.dtype())
        .map_err(|cause| error("SCHEMA", cause))?
    {
        return Ok(());
    }
    let arena = Arena::get(context.native_session())?;
    writer::visit(
        array,
        &mut context.native_session().create_execution_ctx(),
        context.cancellation(),
        &mut |raw| {
            if let descriptor::Descriptor::Managed { id, length } = descriptor::parse(raw)? {
                arena.lookup(&id, length)?.validate()?;
            }
            Ok(())
        },
    )
}

#[cfg(test)]
#[path = "binary_object_tests.rs"]
mod tests;
