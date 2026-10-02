//! Conversion dispatch without reserving a separate CPU worker in shared mode.

use super::*;
use crate::ingest_runtime::{IngestRuntime, IngestTask};
use vortex::io::runtime::BlockingRuntime as _;

pub(super) enum ConversionExecutor {
    Dedicated(ComputePool),
    Shared {
        runtime: IngestRuntime,
        memory: LiveMemoryPool,
    },
}

pub(super) enum ConversionTask {
    Dedicated(ComputeTask<PrefetchedVortexArray>),
    Shared(IngestTask<Result<Budgeted<PrefetchedVortexArray>>>),
}

impl ConversionTask {
    pub(super) fn join(self) -> Result<Budgeted<PrefetchedVortexArray>> {
        match self {
            Self::Dedicated(task) => task.join(),
            Self::Shared(task) => task.join()?,
        }
    }
}

impl ConversionExecutor {
    pub(super) fn memory(&self) -> &LiveMemoryPool {
        match self {
            Self::Dedicated(pool) => pool.memory(),
            Self::Shared { memory, .. } => memory,
        }
    }

    #[cfg(test)]
    pub(super) fn dedicated(&self) -> &ComputePool {
        match self {
            Self::Dedicated(pool) => pool,
            Self::Shared { .. } => panic!("fixture requires a dedicated legacy conversion pool"),
        }
    }

    pub(super) fn submit_read(
        &self,
        context: Arc<StreamingColumnarVortexArrayWorker>,
        lease: MemoryLease,
        cancellation: CancellationToken,
    ) -> Result<ConversionTask> {
        cancellation.check()?;
        match self {
            Self::Dedicated(pool) => pool
                .submit(
                    Budgeted::new(
                        move |worker: &WorkerContext, lease: &mut MemoryLease| {
                            context.read_convert(worker, lease)
                        },
                        lease,
                    ),
                    cancellation,
                )
                .map(ConversionTask::Dedicated),
            Self::Shared { runtime, memory } => {
                if !memory.owns(&lease) {
                    return Err(ShardLoomError::InvalidOperation(
                        "native ingest conversion reservation belongs to another budget; no fallback execution was attempted".to_string(),
                    ));
                }
                let handle = runtime.runtime().handle();
                Ok(ConversionTask::Shared(runtime.spawn(async move {
                    let lock_started = Instant::now();
                    let mut reader = Arc::clone(&context.reader).lock_owned().await;
                    context.stream_timing.stages.record(
                        Stage::ReaderLockWait,
                        lock_started.elapsed(),
                        0,
                        0,
                        0,
                    );
                    let task_cancellation = cancellation.clone();
                    let result = handle
                        .spawn_cpu(move || {
                            let mut lease = lease;
                            task_cancellation.check()?;
                            let batch = context.read_batch(&mut reader)?;
                            drop(reader);
                            task_cancellation.check()?;
                            let value = match batch {
                                Some((index, batch)) => context
                                    .convert_batch(index, &batch, &mut lease)
                                    .inspect_err(|error| context.remember_failure(error))?,
                                None => None,
                            };
                            task_cancellation.check()?;
                            Ok(Budgeted::new(value, lease))
                        })
                        .await;
                    if result.is_err() {
                        cancellation.cancel();
                    }
                    result
                })))
            }
        }
    }
}
