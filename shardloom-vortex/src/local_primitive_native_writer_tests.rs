use super::*;
use crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cell::Cell;
use vortex::{
    VortexSessionDefault as _,
    array::{IntoArray as _, arrays::PrimitiveArray, dtype::PType, validity::Validity},
    buffer::Buffer,
    io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _},
};

#[derive(Clone, Copy, Debug)]
enum Failure {
    Write,
    Zero,
    Flush,
    Interrupted,
    None,
}

#[derive(Default)]
struct SinkState {
    bytes: Cell<usize>,
    failed: Cell<bool>,
    retried_interrupt: Cell<bool>,
}

struct Sink {
    state: Rc<SinkState>,
    failure: Failure,
    after: usize,
}

impl io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(!self.state.failed.get(), "failed sink was accessed again");
        if matches!(self.failure, Failure::Interrupted)
            && !self.state.retried_interrupt.replace(true)
        {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        if self.state.bytes.get() >= self.after {
            match self.failure {
                Failure::Write => {
                    self.state.failed.set(true);
                    return Err(io::Error::other("injected native sink failure"));
                }
                Failure::Zero => {
                    self.state.failed.set(true);
                    return Ok(0);
                }
                _ => {}
            }
        }
        // Deliberately short writes prove real accepted bytes are kept while
        // only the pending remainder is discarded after the first error.
        let count = bytes.len().min(7);
        self.state.bytes.set(self.state.bytes.get() + count);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        assert!(!self.state.failed.get(), "failed sink was flushed again");
        if matches!(self.failure, Failure::Flush) {
            self.state.failed.set(true);
            Err(io::Error::other("injected native flush failure"))
        } else {
            Ok(())
        }
    }
}

fn array(memory: &LiveMemoryPool) -> ArrayRef {
    let credit = memory.reserve(8192).unwrap();
    let buffer = Buffer::<i32>::from_iter(0..2048).into_byte_buffer();
    PrimitiveArray::from_byte_buffer(
        crate::owned_buffers::retain_credit(buffer, credit),
        PType::I32,
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn native_writer_sink_failures_join_pending_payloads_and_stop_producer_demand() {
    for workers in [0, 1] {
        let runtime = CurrentThreadRuntime::new();
        let pool = runtime.new_pool();
        pool.set_workers(workers);
        let session = VortexSession::default().with_handle(runtime.handle());
        for failure in [Failure::Write, Failure::Zero, Failure::Flush] {
            for after in [0, 9, 16_413] {
                let memory = LiveMemoryPool::new(1 << 20).unwrap();
                let state = Rc::new(SinkState::default());
                let dtype = array(&memory).dtype().clone();
                let mut writer = NativeWriter::new(
                    &session,
                    &runtime,
                    Sink {
                        state: Rc::clone(&state),
                        failure,
                        after,
                    },
                    dtype,
                    SequentialNativeFlatLayout::strategy(8),
                );
                let mut result = Ok(());
                for _ in 0..8 {
                    assert!(!state.failed.get(), "producer demanded after sink failure");
                    result = writer.push(array(&memory));
                    if result.is_err() {
                        // A failed push has already drained its accepted prefix.
                        assert_eq!(memory.snapshot().reserved_bytes, 0);
                        break;
                    }
                }
                let error = if let Err(error) = result {
                    assert!(
                        writer
                            .push(array(&memory))
                            .unwrap_err()
                            .to_string()
                            .contains("closed")
                    );
                    drop(writer);
                    error
                } else {
                    writer.finish().err().unwrap()
                };
                match failure {
                    Failure::Write => {
                        assert!(error.to_string().contains("injected native sink failure"));
                    }
                    Failure::Flush => {
                        assert!(error.to_string().contains("injected native flush failure"));
                    }
                    Failure::Zero => {
                        assert!(
                            error
                                .to_string()
                                .contains(&io::Error::from(io::ErrorKind::WriteZero).to_string()),
                            "{error}"
                        );
                    }
                    _ => unreachable!(),
                }
                assert!(state.failed.get());
                assert_eq!(
                    memory.snapshot().reserved_bytes,
                    0,
                    "{failure:?} {after} {workers}"
                );
            }
        }
    }
}

#[test]
fn native_writer_producer_drop_layout_denial_and_interrupted_writes_release_owners() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for (failure, chunks, abandon) in [
        (Failure::Interrupted, 8, false),
        (Failure::None, 1, false),
        (Failure::Write, 8, true),
        (Failure::None, 8, true),
    ] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let state = Rc::new(SinkState::default());
        let dtype = array(&memory).dtype().clone();
        let mut writer = NativeWriter::new(
            &session,
            &runtime,
            Sink {
                state: Rc::clone(&state),
                failure,
                after: 9,
            },
            dtype,
            SequentialNativeFlatLayout::strategy(chunks),
        );
        writer.push(array(&memory)).unwrap();
        if abandon {
            drop(writer);
        } else {
            let result = (0..3).try_for_each(|_| writer.push(array(&memory)));
            let result = result.and_then(|()| writer.finish().map(|summary| summary.row_count()));
            if chunks == 1 {
                assert!(result.unwrap_err().to_string().contains("chunk count"));
            } else {
                assert_eq!(result.unwrap(), 8192);
                assert!(state.retried_interrupt.get());
            }
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
