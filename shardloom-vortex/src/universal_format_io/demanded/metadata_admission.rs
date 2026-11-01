//! Connect provider footer parsing to the source's existing shared credit owner.

use super::{CancellationToken, LiveMemoryPool, MemoryLease, Result, failed};
use parquet::{errors::ParquetError, file::metadata::MetadataMemoryReservation};
use shardloom_core::ShardLoomError;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(in super::super) struct Admission {
    credit: Mutex<MemoryLease>,
    cancellation: CancellationToken,
}

impl Admission {
    pub(in super::super) fn new(
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            credit: Mutex::new(memory.reserve(0)?),
            cancellation: cancellation.clone(),
        }))
    }

    pub(in super::super) fn finish(self: Arc<Self>) -> Result<MemoryLease> {
        self.cancellation.check()?;
        let this = Arc::try_unwrap(self)
            .map_err(|_| failed("metadata parser retained its admission callback"))?;
        Ok(this
            .credit
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

impl MetadataMemoryReservation for Admission {
    fn resize(&self, bytes: usize) -> parquet::errors::Result<()> {
        self.cancellation
            .check()
            .map_err(|error| ParquetError::External(Box::new(error)))?;
        let bytes =
            u64::try_from(bytes).map_err(|error| ParquetError::External(Box::new(error)))?;
        self.credit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resize(bytes)
            .map_err(|error| ParquetError::External(Box::new(error)))
    }
}

pub(in super::super) fn source_error(error: ParquetError) -> ShardLoomError {
    match error {
        ParquetError::External(error) => match error.downcast::<ShardLoomError>() {
            Ok(error) => *error,
            Err(error) => failed(error),
        },
        error => failed(error),
    }
}
