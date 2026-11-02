//! The compatibility decoder uses the operation's existing memory grant.

use super::{Result, failed};
use arrow_ipc::reader::{DecoderMemory, DecoderMemoryReservation};
use arrow_schema::ArrowError;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    mem::size_of,
    sync::{Arc, Mutex},
};

pub(super) fn new(
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<dyn DecoderMemory>> {
    cancellation.check()?;
    let credit = memory
        .reserve(u64::try_from(size_of::<IpcMemory>() + 2 * size_of::<usize>()).map_err(failed)?)?;
    Ok(Arc::new(IpcMemory {
        zstd: Mutex::default(),
        memory: memory.clone(),
        cancellation: cancellation.clone(),
        _credit: credit,
    }))
}

#[derive(Debug)]
struct IpcMemory {
    zstd: Mutex<vortex_zstd::DecoderWorkspace>,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    _credit: MemoryLease,
}

#[derive(Debug)]
struct Credit {
    _lease: MemoryLease,
}
impl DecoderMemoryReservation for Credit {}

fn external(error: impl std::error::Error + Send + Sync + 'static) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}

impl DecoderMemory for IpcMemory {
    fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<Box<dyn DecoderMemoryReservation>, ArrowError> {
        self.cancellation.check().map_err(external)?;
        // The callback's boxed lease can be moved into an Arc by the provider.
        // Admit both records during that conversion, before constructing either.
        let bytes = bytes
            .checked_add(2 * size_of::<Credit>() + 2 * size_of::<usize>())
            .ok_or_else(|| external(failed("IPC reservation metadata overflow")))?;
        let credit = self
            .memory
            .reserve(u64::try_from(bytes).map_err(external)?)
            .map_err(external)?;
        Ok(Box::new(Credit { _lease: credit }))
    }

    fn decompress_zstd(
        &self,
        input: &[u8],
        output: &mut [u8],
    ) -> std::result::Result<usize, ArrowError> {
        self.cancellation.check().map_err(external)?;
        let written = self
            .zstd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .decompress_frame_into(
                input,
                output,
                &crate::owned_buffers::ReservedHostAllocator::new(self.memory.clone()),
            )
            .map_err(external)?;
        self.cancellation.check().map_err(external)?;
        Ok(written)
    }
}
