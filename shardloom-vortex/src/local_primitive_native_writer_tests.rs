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

#[test]
fn native_file_statistics_never_publish_partial_sum_after_chunk_overflow() {
    use vortex::array::expr::stats::{Precision, Stat};

    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let first = PrimitiveArray::from_iter([i64::MAX, i64::MAX]).into_array();
    let second = PrimitiveArray::from_iter([-i64::MAX]).into_array();
    let mut bytes = Vec::new();
    let mut writer = session
        .write_options()
        .with_strategy(SequentialNativeFlatLayout::strategy(2))
        .blocking(&runtime)
        .writer(&mut bytes, first.dtype().clone());
    writer.push(first).unwrap();
    writer.push(second).unwrap();
    let summary = writer.finish().unwrap();
    let statistics = summary.footer().statistics().unwrap();
    assert_eq!(summary.row_count(), 3);
    assert!(
        matches!(statistics.stats_sets()[0].get(Stat::Sum), Precision::Absent),
        "an overflowed batch must not disappear into a partial file sum: {:?}",
        statistics.stats_sets()[0].get(Stat::Sum)
    );
}

#[test]
fn native_file_count_statistic_merges_reject_overflow() {
    use vortex::array::{
        expr::stats::{Precision, Stat},
        stats::StatsSet,
    };
    let dtype = DType::Primitive(PType::F64, vortex::array::dtype::Nullability::Nullable);
    for stat in [
        Stat::NullCount,
        Stat::NaNCount,
        Stat::UncompressedSizeInBytes,
    ] {
        let merged = StatsSet::of(stat, Precision::exact(u64::MAX))
            .merge_ordered(&StatsSet::of(stat, Precision::exact(1_u64)), &dtype);
        assert!(merged.get(stat).is_absent());
        let merged = StatsSet::of(stat, Precision::exact(u64::MAX - 3))
            .merge_ordered(&StatsSet::of(stat, Precision::exact(3_u64)), &dtype);
        assert_eq!(
            merged.get(stat).as_exact(),
            vortex::array::scalar::Scalar::from(u64::MAX).into_value()
        );
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn persisted_writer_retains_only_statistics_credit_until_footer_release() {
    use vortex::array::{arrays::VarBinArray, dtype::Nullability, expr::stats::Stat};
    use vortex::buffer::ByteBuffer;

    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let dtype = DType::Utf8(Nullability::NonNullable);
    let statistics = FileStatisticsReservation::reserve(&dtype, &memory).unwrap();
    let statistics_bytes = statistics.bytes();
    let mut output = Vec::new();
    let mut writer = NativeWriter::persisted(
        &session,
        &runtime,
        &mut output,
        dtype.clone(),
        SequentialNativeFlatLayout::strategy(40),
        statistics,
    );
    for byte in b'a'..=b'z' {
        let bytes = crate::owned_buffers::retain_credit(
            ByteBuffer::from(vec![byte; 65_536]),
            memory.reserve(65_536).unwrap(),
        );
        writer
            .push(
                VarBinArray::new(
                    PrimitiveArray::from_iter([0_u32, 65_536]).into_array(),
                    bytes,
                    dtype.clone(),
                    Validity::NonNullable,
                )
                .into_array(),
            )
            .unwrap();
    }
    let summary = writer.finish().unwrap();
    assert_eq!(summary.row_count(), 26);
    assert_eq!(memory.snapshot().reserved_bytes, statistics_bytes);
    let sets = summary.footer().statistics().unwrap().stats_sets();
    assert!(matches!(
        sets[0].get(Stat::Min),
        vortex::array::expr::stats::Precision::Inexact(_)
    ));
    assert!(matches!(
        sets[0].get(Stat::Max),
        vortex::array::expr::stats::Precision::Inexact(_)
    ));
    drop(summary);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn persisted_writer_failures_and_abandonment_release_statistics_credit() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for (failure, abandon) in [
        (Failure::Write, false),
        (Failure::Zero, false),
        (Failure::Flush, false),
        (Failure::None, true),
    ] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let dtype = array(&memory).dtype().clone();
        let statistics = FileStatisticsReservation::reserve(&dtype, &memory).unwrap();
        let mut writer = NativeWriter::persisted(
            &session,
            &runtime,
            Sink {
                state: Rc::new(SinkState::default()),
                failure,
                after: 9,
            },
            dtype,
            SequentialNativeFlatLayout::strategy(8),
            statistics,
        );
        let pushed = (0..4).try_for_each(|_| writer.push(array(&memory)));
        if abandon {
            drop(writer);
        } else if pushed.is_err() {
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            drop(writer);
        } else {
            assert!(writer.finish().is_err());
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{failure:?}");
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn persisted_statistics_admission_refuses_insufficient_credit() {
    let dtype = DType::Primitive(PType::I64, vortex::array::dtype::Nullability::NonNullable);
    let memory = LiveMemoryPool::new(64 * 1024).unwrap();
    let error = FileStatisticsReservation::reserve(&dtype, &memory)
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn persisted_nullable_root_keeps_schema_and_omits_unmasked_statistics() {
    use vortex::array::{VortexSessionExecute as _, arrays::StructArray, scalar::Scalar};
    use vortex::file::OpenOptionsSessionExt as _;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let array = StructArray::new(
        ["value"].into(),
        vec![PrimitiveArray::from_iter([99_i64, 1, 7]).into_array()],
        3,
        Validity::from_iter([false, true, true]),
    )
    .into_array();
    let dtype = array.dtype().clone();
    let statistics = FileStatisticsReservation::reserve(&dtype, &memory).unwrap();
    let mut bytes = Vec::new();
    let mut writer = NativeWriter::persisted(
        &session,
        &runtime,
        &mut bytes,
        dtype.clone(),
        SequentialNativeFlatLayout::strategy(1),
        statistics,
    );
    writer.push(array).unwrap();
    let summary = writer.finish().unwrap();
    assert_eq!(summary.row_count(), 3);
    let sets = summary.footer().statistics().unwrap().stats_sets();
    assert_eq!(sets.len(), 1);
    assert!(sets[0].is_empty());
    let file = session
        .open_options()
        .open_buffer(vortex::buffer::ByteBuffer::from(bytes))
        .unwrap();
    assert_eq!(file.dtype(), &dtype);
    assert_eq!(file.row_count(), 3);
    let mut execution = session.create_execution_ctx();
    let actual = file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
        .flat_map(|chunk| {
            let chunk = chunk.unwrap();
            (0..chunk.len())
                .map(|index| chunk.execute_scalar(index, &mut execution).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            Scalar::null(dtype.clone()),
            Scalar::struct_(dtype.clone(), [Scalar::from(1_i64)]),
            Scalar::struct_(dtype, [Scalar::from(7_i64)]),
        ]
    );
}
