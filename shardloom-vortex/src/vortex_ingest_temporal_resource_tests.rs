//! Shared-grant admission, failed publication and reuse for native timestamps.

use super::*;
use shardloom_exec::live_memory::MemoryLease;
use std::{
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll},
};
use vortex::{array::iter::ArrayIterator, error::VortexResult};

fn source() -> ArrayRef {
    timestamp(
        &(0..4096)
            .map(|row| match row % 3 {
                0 => Some(i64::MIN + i64::from(row)),
                1 => Some(i64::MAX - i64::from(row)),
                _ => None,
            })
            .collect::<Vec<_>>(),
    )
}

fn assert_reopened(context: &LocalVortexWriteContext, path: &Path, source: &ArrayRef) {
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(path))
        .unwrap();
    assert_eq!(file.dtype(), source.dtype());
    let mut execution = context.session.create_execution_ctx();
    let expected = values(source, &mut execution);
    let mut actual = Vec::new();
    for batch in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        actual.extend(values(&batch.unwrap(), &mut execution));
    }
    assert_eq!(actual, expected);
    verify_statistics(&file, source, &mut execution);
}

fn assert_only_output(directory: &Directory, output: &Path) {
    let files = fs::read_dir(&directory.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(files, [output]);
}

#[test]
fn full_domain_timestamp_writer_shares_retained_grants_preserves_outputs_and_reuses_credit() {
    let array = source();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for composition in [
            Composition::Default,
            Composition::FastLoad,
            Composition::Balanced,
            Composition::SourceText,
        ] {
            let directory = Directory::new();
            let existing = directory.0.join("existing.vortex");
            let absent = directory.0.join("new-parent").join("new.vortex");
            fs::write(&existing, b"previous independently owned output").unwrap();
            let memory = LiveMemoryPool::new(16 << 20).unwrap();
            let retained = memory.reserve((16 << 20) - 128).unwrap();
            for path in [&existing, &absent] {
                for pool in [&LiveMemoryPool::new(1).unwrap(), &memory] {
                    let result = context.write_array(
                        path,
                        &array,
                        true,
                        &decision(composition, path),
                        "test",
                        pool,
                    );
                    let Err(error) = result else {
                        panic!("insufficient shared grant must fail");
                    };
                    assert!(
                        error.to_string().contains("memory reservation denied"),
                        "{error}"
                    );
                    assert_eq!(
                        fs::read(&existing).unwrap(),
                        b"previous independently owned output"
                    );
                    assert!(!absent.parent().unwrap().exists());
                    assert_only_output(&directory, &existing);
                }
                assert_eq!(memory.snapshot().reserved_bytes, retained.bytes());
            }
            drop(retained);
            context
                .write_array(
                    &existing,
                    &array,
                    true,
                    &decision(composition, &existing),
                    "test",
                    &memory,
                )
                .unwrap();
            assert_reopened(&context, &existing, &array);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
            assert_only_output(&directory, &existing);
        }
    });
}

#[derive(Clone, Copy)]
enum Action {
    None,
    Cancel,
    Exhaust,
}

struct NativeIterator {
    dtype: DType,
    source: Option<ArrayRef>,
    memory: LiveMemoryPool,
    pressure: Option<MemoryLease>,
    cancellation: CancellationToken,
    action: Action,
    pulls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for NativeIterator {
    type Item = VortexResult<ArrayRef>;
    fn next(&mut self) -> Option<Self::Item> {
        let array = self.source.take()?;
        self.pulls.fetch_add(1, Ordering::SeqCst);
        match self.action {
            Action::None => {}
            Action::Cancel => self.cancellation.cancel(),
            Action::Exhaust => {
                // This fixture uses one CPU lane, so no other task races the
                // snapshot. Hold the remaining shared grant during compression.
                let snapshot = self.memory.snapshot();
                self.pressure = Some(
                    self.memory
                        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
                        .unwrap(),
                );
            }
        }
        Some(Ok(array))
    }
}

impl ArrayIterator for NativeIterator {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl futures::Stream for NativeIterator {
    type Item = VortexResult<ArrayRef>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next())
    }
}

impl Drop for NativeIterator {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Failure/reuse observations share the same real owner and output.
fn full_domain_timestamp_stream_failures_release_owners_and_preserve_previous_output() {
    let array = source();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for composition in [
            Composition::Default,
            Composition::FastLoad,
            Composition::Balanced,
            Composition::SourceText,
        ] {
            let directory = Directory::new();
            let output = directory.0.join("existing.vortex");
            fs::write(&output, b"previous independently owned output").unwrap();
            let memory = NativeIngestMemory::new(16 << 20).unwrap();
            let mut decision = decision(composition, &output);
            decision.writer_runtime_requested_parallelism = 1;
            decision.writer_runtime_applied_parallelism = 1;
            decision.writer_runtime_background_workers = 0;
            for (case, action, precancel, expected_rows, message) in [
                (
                    "before-pull",
                    Action::None,
                    true,
                    4096,
                    "execution cancelled",
                ),
                (
                    "after-pull",
                    Action::Cancel,
                    false,
                    4096,
                    "execution cancelled",
                ),
                (
                    "after-entry-pressure",
                    Action::Exhaust,
                    false,
                    4096,
                    "memory reservation denied",
                ),
                (
                    "validation",
                    Action::None,
                    false,
                    4097,
                    "writer row count mismatch",
                ),
                ("reuse", Action::None, false, 4096, ""),
            ] {
                let cancellation = CancellationToken::default();
                if precancel {
                    cancellation.cancel();
                }
                let pulls = Arc::new(AtomicUsize::new(0));
                let dropped = Arc::new(AtomicUsize::new(0));
                let iterator = NativeIterator {
                    dtype: array.dtype().clone(),
                    source: Some(array.clone()),
                    memory: memory.pool.clone(),
                    pressure: None,
                    cancellation: cancellation.clone(),
                    action,
                    pulls: Arc::clone(&pulls),
                    dropped: Arc::clone(&dropped),
                };
                let result = context.write_array_iterator(
                    &output,
                    iterator,
                    true,
                    &decision,
                    Some(expected_rows),
                    Some(&memory),
                    &[],
                    None,
                    None,
                    &cancellation,
                );
                assert_eq!(
                    pulls.load(Ordering::SeqCst),
                    usize::from(!precancel),
                    "{case}"
                );
                assert_eq!(dropped.load(Ordering::SeqCst), 1, "{case}");
                assert_eq!(memory.pool.snapshot().reserved_bytes, 0, "{case}");
                assert!(
                    memory.pool.snapshot().peak_reserved_bytes
                        <= memory.pool.snapshot().limit_bytes
                );
                if message.is_empty() {
                    assert_eq!(result.unwrap().writer_row_count, 4096);
                    assert_reopened(&context, &output, &array);
                } else {
                    let Err(error) = result else {
                        panic!("{case} must fail");
                    };
                    assert!(error.to_string().contains(message), "{case}: {error}");
                    assert_eq!(
                        fs::read(&output).unwrap(),
                        b"previous independently owned output"
                    );
                }
                assert_only_output(&directory, &output);
            }
            assert!(memory.pool.snapshot().denied_reservations > 0);
        }
    });
}
