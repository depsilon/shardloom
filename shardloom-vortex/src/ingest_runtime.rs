//! One native executor for an artifact's source, conversion and writer work.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use shardloom_core::{Result, ShardLoomError};
use vortex::io::runtime::{BlockingRuntime as _, JoinOutcome, Task, current::CurrentThreadRuntime};

/// Shared native ingest executor carried by an admitted columnar source.
///
/// Cloning this handle creates no threads. The artifact writer owns and joins
/// the background drivers; a standalone source reader progresses on its caller.
/// Source tasks and native provider tasks use the same executor and CPU grant.
#[derive(Clone)]
pub struct IngestRuntime(Arc<IngestState>);

struct IngestState {
    runtime: CurrentThreadRuntime,
    parallelism: usize,
    drivers_active: AtomicBool,
}

impl IngestRuntime {
    // Bound scheduling metadata even when a caller supplies an unusually large
    // CPU grant. Shared conversion/source queues remain separately byte-admitted.
    pub(crate) const MAX_CONVERSION_TASKS: usize = 32;

    pub(crate) fn new(parallelism: usize) -> Self {
        Self(Arc::new(IngestState {
            runtime: CurrentThreadRuntime::new(),
            parallelism: parallelism.max(1),
            drivers_active: AtomicBool::new(false),
        }))
    }

    pub(crate) fn parallelism(&self) -> usize {
        self.0.parallelism
    }

    pub(crate) fn runtime(&self) -> &CurrentThreadRuntime {
        &self.0.runtime
    }

    pub(crate) fn start_drivers(&self) -> Result<IngestDrivers> {
        self.0.drivers_active.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ShardLoomError::InvalidOperation(
                "native ingest runtime already has an active writer; a cloned source handle cannot multiply its CPU grant; no fallback execution was attempted".to_string(),
            ))?;
        let drivers = crate::resident_worker_group::ResidentWorkerGroup::new(
            self.runtime(),
            self.parallelism() - 1,
        )
        .map_err(|error| {
            self.0.drivers_active.store(false, Ordering::Release);
            ShardLoomError::InvalidOperation(format!(
                "failed to start shared native ingest CPU drivers: {error}; all started drivers joined; no fallback execution was attempted"
            ))
        })?;
        Ok(IngestDrivers {
            drivers: Some(drivers),
            runtime: self.clone(),
        })
    }

    pub(crate) fn spawn<F, T>(&self, work: F) -> IngestTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        IngestTask {
            task: self.runtime().handle().spawn(work),
            runtime: self.runtime().clone(),
        }
    }
}

/// The admission remains held until every background driver has been joined.
pub(crate) struct IngestDrivers {
    drivers: Option<crate::resident_worker_group::ResidentWorkerGroup>,
    runtime: IngestRuntime,
}

impl Drop for IngestDrivers {
    fn drop(&mut self) {
        drop(self.drivers.take());
        self.runtime
            .0
            .drivers_active
            .store(false, Ordering::Release);
    }
}

pub(crate) struct IngestTask<T> {
    task: Task<T>,
    runtime: CurrentThreadRuntime,
}

impl<T> IngestTask<T> {
    /// Join while executing other ready native work. Callers close their queues
    /// before draining, so no producer waits for a receiver held by this join.
    pub(crate) fn join(mut self) -> Result<T> {
        match self
            .runtime
            .block_on(futures::future::poll_fn(|cx| self.task.poll_join(cx)))
        {
            JoinOutcome::Completed(value) => Ok(value),
            JoinOutcome::Panicked(_) => Err(ShardLoomError::InvalidOperation(
                "native ingest task panicked; no fallback execution was attempted".to_string(),
            )),
            JoinOutcome::Aborted => Err(ShardLoomError::InvalidOperation(
                "native ingest task stopped before completion; no fallback execution was attempted"
                    .to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_runtime_cannot_admit_overlapping_driver_groups() {
        for grant in [1, 2, 4, 6, 8] {
            let runtime = IngestRuntime::new(grant);
            let clone = runtime.clone();
            let first = runtime.start_drivers().unwrap();
            let error = clone
                .start_drivers()
                .err()
                .expect("duplicate writer must fail");
            assert!(error.to_string().contains("cannot multiply its CPU grant"));
            assert_eq!(runtime.spawn(async { 42 }).join().unwrap(), 42);
            drop(first);
            let next = clone.start_drivers().unwrap();
            assert_eq!(runtime.spawn(async { 43 }).join().unwrap(), 43);
            drop(next);
        }
    }
}
