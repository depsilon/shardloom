//! Ordered bounded source tasks that yield to native conversion and writer work.

use std::{collections::VecDeque, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use futures::{SinkExt as _, StreamExt as _, channel::mpsc};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::io::runtime::BlockingRuntime as _;

use crate::ingest_runtime::{IngestRuntime, IngestTask};

type Batch = std::result::Result<RecordBatch, ArrowError>;
pub(crate) type ReaderFactory =
    Box<dyn FnOnce() -> std::result::Result<Box<dyn RecordBatchReader + Send>, ArrowError> + Send>;

struct SourceTask {
    receiver: mpsc::Receiver<Batch>,
    completion: IngestTask<()>,
}

/// Each source task retains at most two queued batches and one active batch.
/// The ordered task window includes later tasks blocked behind an early task.
pub(crate) struct IngestSourceReader {
    schema: SchemaRef,
    runtime: IngestRuntime,
    factories: VecDeque<ReaderFactory>,
    tasks: VecDeque<SourceTask>,
    window: usize,
    cancellation: CancellationToken,
}

impl IngestSourceReader {
    pub(crate) const QUEUED_BATCHES: usize = 2;

    pub(crate) fn new(
        schema: SchemaRef,
        runtime: IngestRuntime,
        factories: Vec<ReaderFactory>,
        window: usize,
    ) -> Self {
        let window = window.max(1).min(runtime.parallelism());
        let mut reader = Self {
            schema,
            runtime,
            factories: factories.into(),
            tasks: VecDeque::new(),
            window,
            cancellation: CancellationToken::default(),
        };
        reader.fill_window();
        reader
    }

    fn fill_window(&mut self) {
        while self.tasks.len() < self.window && !self.cancellation.is_cancelled() {
            let Some(factory) = self.factories.pop_front() else {
                break;
            };
            // futures mpsc grants each sender one extra slot. There is exactly
            // one sender here: buffer + sender slot equals QUEUED_BATCHES.
            let (mut sender, receiver) = mpsc::channel(Self::QUEUED_BATCHES - 1);
            let cancellation = self.cancellation.clone();
            let handle = self.runtime.runtime().handle();
            let completion = self.runtime.spawn(async move {
                if cancellation.is_cancelled() {
                    return;
                }
                let mut reader = match handle.spawn_cpu(factory).await {
                    Ok(reader) => reader,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                while !cancellation.is_cancelled() {
                    let (returned_reader, batch) = handle
                        .spawn_cpu(move || {
                            let batch = reader.next();
                            (reader, batch)
                        })
                        .await;
                    reader = returned_reader;
                    let Some(batch) = batch else {
                        break;
                    };
                    let failed = batch.is_err();
                    // A full queue yields this driver to any ready source,
                    // converter or native codec. It never parks a CPU thread.
                    if sender.send(batch).await.is_err() || failed {
                        break;
                    }
                }
            });
            self.tasks.push_back(SourceTask {
                receiver,
                completion,
            });
        }
    }

    fn close_and_drain(&mut self) {
        self.cancellation.cancel();
        self.factories.clear();
        // Disconnect every result queue before joining any producer, including
        // later tasks whose full output is waiting behind a failed early task.
        let completions: Vec<_> = self
            .tasks
            .drain(..)
            .map(|task| {
                drop(task.receiver);
                task.completion
            })
            .collect();
        for task in completions {
            let _ = task.join();
        }
    }
}

impl Iterator for IngestSourceReader {
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let task = self.tasks.front_mut()?;
            match self.runtime.runtime().block_on(task.receiver.next()) {
                Some(Ok(batch)) => return Some(Ok(batch)),
                Some(Err(error)) => {
                    self.close_and_drain();
                    return Some(Err(error));
                }
                None => {
                    let task = self.tasks.pop_front()?;
                    drop(task.receiver);
                    if let Err(error) = task.completion.join() {
                        self.close_and_drain();
                        return Some(Err(ArrowError::ExternalError(Box::new(error))));
                    }
                    self.fill_window();
                }
            }
        }
    }
}

impl RecordBatchReader for IngestSourceReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for IngestSourceReader {
    fn drop(&mut self) {
        self.close_and_drain();
    }
}

#[cfg(test)]
#[path = "ingest_source_tests.rs"]
mod tests;
