//! Bounded private local intake; published native files have separate ownership.
//!
//! Cleanup is cooperative exact-path ownership, not a filesystem compare-and-
//! swap against hostile same-user replacement between validation and unlink.

use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::Write as _,
    os::unix::fs::{
        DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
    },
    path::PathBuf,
};

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(0);

/// Explicit temporary storage for large managed input objects.
/// The ceiling covers this store's actual live payloads, including failed
/// cleanup. Store/result/reader clones retain the same owners and ceiling.
#[derive(Clone)]
pub struct BinaryObjectStore {
    session: ResidentVortexSession,
    directory: Arc<Namespace>,
    quota: Arc<Quota>,
}

/// Local storage accounting, separate from memory and query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryObjectStoreSnapshot {
    pub limit_bytes: u64,
    pub retained_bytes: u64,
    /// Files that could not be safely removed remain charged and untouched.
    pub cleanup_failures: u64,
}

struct Quota {
    limit: u64,
    bytes: AtomicU64,
    cleanup_failures: AtomicU64,
}

struct DiskCredit {
    quota: Arc<Quota>,
    bytes: u64,
    refund: bool,
}
impl Drop for DiskCredit {
    fn drop(&mut self) {
        if self.refund {
            self.quota.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}
impl Quota {
    fn reserve(self: &Arc<Self>, bytes: u64) -> Result<DiskCredit> {
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .filter(|sum| *sum <= self.limit)
                .ok_or_else(|| error("STORAGE", "managed object store byte ceiling exceeded"))?;
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(DiskCredit {
            quota: Arc::clone(self),
            bytes,
            refund: true,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct DirectoryId(u64, u64);
fn directory_id(path: &Path, held: &File) -> Result<DirectoryId> {
    let current = fs::symlink_metadata(path).map_err(|cause| error("STORAGE", cause))?;
    let original = held.metadata().map_err(|cause| error("STORAGE", cause))?;
    if !current.is_dir()
        || current.file_type().is_symlink()
        || !original.is_dir()
        || current.dev() != original.dev()
        || current.ino() != original.ino()
    {
        return Err(error("STORAGE", "managed object namespace changed"));
    }
    Ok(DirectoryId(current.dev(), current.ino()))
}

struct Namespace {
    parent: PathBuf,
    parent_handle: File,
    parent_id: DirectoryId,
    path: PathBuf,
    handle: File,
    id: DirectoryId,
    next: AtomicU64,
    _metadata: MemoryLease,
}
impl Namespace {
    fn new(parent: &Path, memory: &LiveMemoryPool) -> Result<Arc<Self>> {
        let path_bytes = parent
            .as_os_str()
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| error("RESOURCE", "managed namespace path size overflows"))?;
        let metadata = memory.reserve(path_bytes as u64)?;
        let parent = fs::canonicalize(parent).map_err(|cause| error("STORAGE", cause))?;
        let parent_handle = File::open(&parent).map_err(|cause| error("STORAGE", cause))?;
        let parent_id = directory_id(&parent, &parent_handle)?;
        let mut created = None;
        for _ in 0..16 {
            let nonce = NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!("shardloom-binary-{}-{nonce}", std::process::id()));
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => {
                    created = Some(path);
                    break;
                }
                Err(cause) if cause.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(cause) => return Err(error("STORAGE", cause)),
            }
        }
        let path =
            created.ok_or_else(|| error("STORAGE", "cannot create a unique managed namespace"))?;
        let setup = (|| {
            let handle = File::open(&path).map_err(|cause| error("STORAGE", cause))?;
            let id = directory_id(&path, &handle)?;
            if directory_id(&parent, &parent_handle)? != parent_id {
                return Err(error("STORAGE", "managed namespace parent changed"));
            }
            Ok((handle, id))
        })();
        // If capture failed, no reliable namespace identity exists. Leave the
        // empty/substituted path alone instead of guessing ownership to delete.
        let (handle, id) = setup?;
        Ok(Arc::new(Self {
            parent,
            parent_handle,
            parent_id,
            path,
            handle,
            id,
            next: AtomicU64::new(0),
            _metadata: metadata,
        }))
    }

    fn validate(&self) -> Result<()> {
        if directory_id(&self.parent, &self.parent_handle)? != self.parent_id
            || directory_id(&self.path, &self.handle)? != self.id
        {
            return Err(error(
                "STORAGE",
                "managed object namespace identity changed",
            ));
        }
        Ok(())
    }

    fn create(
        self: &Arc<Self>,
        credit: DiskCredit,
        metadata: MemoryLease,
    ) -> Result<TemporaryObject> {
        self.validate()?;
        for _ in 0..16 {
            let number = self.next.fetch_add(1, Ordering::Relaxed);
            let path = self.path.join(format!("{number}.bytes"));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(TemporaryObject {
                        directory: Arc::clone(self),
                        path,
                        file,
                        generation: None,
                        credit,
                        _metadata: metadata,
                    });
                }
                Err(cause) if cause.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(cause) => return Err(error("STORAGE", cause)),
            }
        }
        Err(error(
            "STORAGE",
            "cannot create a unique managed object file",
        ))
    }
}
impl Drop for Namespace {
    fn drop(&mut self) {
        if self.validate().is_ok() {
            // Unknown entries keep the directory present; none are traversed.
            let _ = fs::remove_dir(&self.path);
        }
    }
}

pub(crate) struct TemporaryObject {
    directory: Arc<Namespace>,
    path: PathBuf,
    file: File,
    generation: Option<Arc<SourceIdentity>>,
    credit: DiskCredit,
    _metadata: MemoryLease,
}
impl TemporaryObject {
    fn cleanup(&self) -> Result<()> {
        self.directory.validate()?;
        let held = self
            .file
            .metadata()
            .map_err(|cause| error("CLEANUP", cause))?;
        let current = match fs::symlink_metadata(&self.path) {
            Ok(value) => value,
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => {
                // A renamed object still occupies storage. Only an unlinked
                // inode releases its last held bytes when this owner drops.
                return if held.nlink() == 0 {
                    Ok(())
                } else {
                    Err(error(
                        "CLEANUP",
                        "managed object was moved; storage credit retained",
                    ))
                };
            }
            Err(cause) => return Err(error("CLEANUP", cause)),
        };
        if !current.is_file()
            || current.file_type().is_symlink()
            || current.dev() != held.dev()
            || current.ino() != held.ino()
        {
            return Err(error(
                "CLEANUP",
                "managed object path was substituted; file retained",
            ));
        }
        if let Some(generation) = &self.generation {
            generation.validate()?;
        }
        fs::remove_file(&self.path).map_err(|cause| error("CLEANUP", cause))
    }
}
impl Drop for TemporaryObject {
    fn drop(&mut self) {
        if self.cleanup().is_err() {
            // A still-live store must not reuse credit for bytes that remain.
            self.credit.refund = false;
            self.credit
                .quota
                .cleanup_failures
                .fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl ResidentVortexSession {
    /// Select an existing local directory and a live-byte ceiling for managed
    /// large-object intake. A fresh private child namespace is created.
    /// # Errors
    /// Rejects inaccessible/non-directory storage, denied metadata memory or
    /// invalid namespace identity. No caller file is adopted or deleted.
    pub fn binary_object_store(
        &self,
        directory: &Path,
        max_bytes: u64,
    ) -> Result<BinaryObjectStore> {
        self.with_native_execution_context(&CancellationToken::default(), |context| {
            let directory = Namespace::new(directory, context.memory())?;
            Ok(BinaryObjectStore {
                session: self.clone(),
                directory,
                quota: Arc::new(Quota {
                    limit: max_bytes,
                    bytes: AtomicU64::new(0),
                    cleanup_failures: AtomicU64::new(0),
                }),
            })
        })
    }
}

impl BinaryObjectStore {
    /// Observe actual retained private payload bytes, without reading content.
    #[must_use]
    pub fn snapshot(&self) -> BinaryObjectStoreSnapshot {
        BinaryObjectStoreSnapshot {
            limit_bytes: self.quota.limit,
            retained_bytes: self.quota.bytes.load(Ordering::Acquire),
            cleanup_failures: self.quota.cleanup_failures.load(Ordering::Acquire),
        }
    }

    /// Copy a regular local file into immutable managed storage in bounded
    /// chunks. The caller's original file remains externally owned.
    /// # Errors
    /// Rejects storage/memory denial, source mutation, short reads, write
    /// failures and cancellation. An incomplete object is never registered.
    pub fn import_file(
        &self,
        path: &Path,
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        let handle = self
            .session
            .with_native_execution_context(cancellation, |context| {
                let source = Arc::new(SourceIdentity::capture(path)?);
                let reader = context.local_source_reader(
                    Arc::clone(&source),
                    Arc::new(LocalReadMetrics::default()),
                )?;
                let handle = self.copy(source.generation.len, context, |start, bytes| {
                    let mut offset = 0;
                    while offset < bytes.len() {
                        let count = reader.read_at(&mut bytes[offset..], start + offset as u64)?;
                        if count == 0 {
                            return Err(error(
                                "SOURCE",
                                "managed intake ended before its declared length",
                            ));
                        }
                        offset += count;
                    }
                    Ok(())
                })?;
                source.validate()?;
                Ok(handle)
            })?;
        self.session.publish_binary_object(handle, cancellation)
    }

    /// Copy caller bytes to this store without keeping their full size in
    /// engine memory.
    /// # Errors
    /// Rejects storage/memory denial, write failures or cancellation.
    pub fn put_bytes(
        &self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<BinaryObject> {
        let handle = self
            .session
            .with_native_execution_context(cancellation, |context| {
                self.copy(bytes.len() as u64, context, |start, target| {
                    let start = usize::try_from(start)
                        .map_err(|_| error("RANGE", "input offset overflows"))?;
                    target.copy_from_slice(&bytes[start..start + target.len()]);
                    Ok(())
                })
            })?;
        self.session.publish_binary_object(handle, cancellation)
    }

    fn copy(
        &self,
        length: u64,
        context: &NativeExecutionContext<'_>,
        mut read: impl FnMut(u64, &mut [u8]) -> Result<()>,
    ) -> Result<BinaryObject> {
        context.check_cancelled()?;
        let disk = self.quota.reserve(length)?;
        let metadata = context.memory().reserve(8192)?;
        let mut temporary = self.directory.create(disk, metadata)?;
        let count = usize::try_from(length.div_ceil(CHUNK_BYTES as u64)).map_err(|_| {
            error(
                "RESOURCE",
                "managed object directory exceeds addressable memory",
            )
        })?;
        let (mut chunks, chunk_credit) = reserved_vec(context.memory(), count)?;
        let capacity = usize::try_from(length.min(CHUNK_BYTES as u64))
            .map_err(|_| error("RESOURCE", "managed input chunk exceeds addressable memory"))?;
        let (mut scratch, _scratch_credit) = reserved_vec(context.memory(), capacity)?;
        scratch.resize(capacity, 0);
        let mut offset = 0;
        while offset < length {
            context.check_cancelled()?;
            let count = usize::try_from((length - offset).min(CHUNK_BYTES as u64))
                .map_err(|_| error("RANGE", "managed input chunk size overflows"))?;
            let bytes = &mut scratch[..count];
            read(offset, bytes)?;
            context.check_cancelled()?;
            temporary
                .file
                .write_all(bytes)
                .map_err(|cause| error("STORAGE", cause))?;
            chunks.push(Chunk {
                offset,
                length: u32::try_from(count)
                    .map_err(|_| error("RESOURCE", "managed input chunk length overflows"))?,
                digest: digest(bytes),
            });
            offset += count as u64;
        }
        temporary
            .file
            .flush()
            .map_err(|cause| error("STORAGE", cause))?;
        temporary
            .file
            .set_permissions(fs::Permissions::from_mode(0o400))
            .map_err(|cause| error("STORAGE", cause))?;
        self.directory.validate()?;
        let identity = Arc::new(SourceIdentity::capture(&temporary.path)?);
        if identity.generation.len != length {
            return Err(error(
                "SOURCE",
                "managed object length changed before publication",
            ));
        }
        temporary.generation = Some(Arc::clone(&identity));
        let id = content_id(length, &chunks);
        let metadata = context.memory().reserve(512)?;
        let payload = Arc::new(Payload {
            id: Some(id),
            length,
            chunks: Budgeted::new(chunks, chunk_credit),
            backing: Backing::File {
                identity,
                offset: 0,
                _retention: Some(Arc::new(temporary)),
            },
            _metadata: metadata,
        });
        let handle = self
            .session
            .object_handle(payload, &descriptor::managed(&id, length))?;
        context.check_cancelled()?;
        handle.payload.validate()?;
        Ok(handle)
    }
}

#[cfg(test)]
#[path = "binary_object_store_tests.rs"]
mod tests;
