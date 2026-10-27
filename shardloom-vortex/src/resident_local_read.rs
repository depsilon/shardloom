//! Positional compatibility reads under the resident operation's native owner.

use super::{
    SourceIdentity,
    io_ownership::{IoScope, ReaderOwner},
    resident_error,
};
use bytes::Bytes;
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    os::unix::fs::FileExt as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct LocalReadMetrics {
    pub(crate) requests: AtomicU64,
    pub(crate) bytes: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct OwnedLocalRead(Arc<Reader>);

struct Reader {
    identity: Arc<SourceIdentity>,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    metrics: Arc<LocalReadMetrics>,
    scope: Option<Arc<IoScope>>,
    _owner: Option<Arc<ReaderOwner>>,
    _metadata: MemoryLease,
}

impl OwnedLocalRead {
    pub(crate) fn metadata(
        identity: Arc<SourceIdentity>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        Self::new(
            identity,
            memory,
            cancellation,
            Arc::new(LocalReadMetrics::default()),
            None,
        )
    }

    pub(super) fn new(
        identity: Arc<SourceIdentity>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        metrics: Arc<LocalReadMetrics>,
        scope: Option<Arc<IoScope>>,
    ) -> Result<Self> {
        cancellation.check()?;
        identity.validate()?;
        let metadata = memory.reserve(4096)?;
        let owner = scope
            .as_ref()
            .map(|scope| scope.retain_reader(memory))
            .transpose()?;
        Ok(Self(Arc::new(Reader {
            identity,
            memory: memory.clone(),
            cancellation: cancellation.clone(),
            metrics,
            scope,
            _owner: owner,
            _metadata: metadata,
        })))
    }

    pub(crate) fn len(&self) -> u64 {
        self.0.identity.generation.len
    }

    pub(crate) fn metrics(&self) -> &Arc<LocalReadMetrics> {
        &self.0.metrics
    }

    pub(crate) fn read_at(&self, buffer: &mut [u8], start: u64) -> Result<usize> {
        self.0.cancellation.check()?;
        self.0.identity.validate()?;
        if start > self.len() {
            return Err(resident_error("source read begins beyond its held length"));
        }
        let length = buffer
            .len()
            .min(usize::try_from(self.len() - start).unwrap_or(usize::MAX));
        let _job = self
            .0
            .scope
            .as_ref()
            .map(|scope| scope.admit(length))
            .transpose()?;
        let count = self
            .0
            .identity
            .file
            .read_at(&mut buffer[..length], start)
            .map_err(|error| resident_error(&error.to_string()))?;
        self.0.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.0
            .metrics
            .bytes
            .fetch_add(count as u64, Ordering::Relaxed);
        self.0.cancellation.check()?;
        self.0.identity.validate()?;
        Ok(count)
    }

    pub(crate) fn bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.len())
        {
            return Err(resident_error("source range exceeds its held length"));
        }
        self.0.cancellation.check()?;
        let credit = self.0.memory.reserve(length as u64)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|error| resident_error(&error.to_string()))?;
        if bytes.capacity() > length {
            return Err(resident_error("source range allocation exceeds admission"));
        }
        bytes.resize(length, 0);
        let mut offset = 0;
        while offset < length {
            let read = self.read_at(&mut bytes[offset..], start + offset as u64)?;
            if read == 0 {
                return Err(resident_error("source range ended before its held length"));
            }
            offset += read;
        }
        Ok(Bytes::from_owner(OwnedBytes(Budgeted::new(bytes, credit))))
    }
}

struct OwnedBytes(Budgeted<Vec<u8>>);
impl AsRef<[u8]> for OwnedBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.value().as_slice()
    }
}
