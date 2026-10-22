//! Admission and final-owner retention of chunked layout row-offset storage.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{StreamExt as _, future::BoxFuture};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, EmptyMetadata, IntoArray as _,
        arrays::PrimitiveArray,
        dtype::{DType, Nullability, PType},
        memory::MemorySessionExt as _,
        stream::ArrayStreamAdapter,
    },
    buffer::ByteBuffer,
    error::{VortexResult, vortex_err},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::{
        LayoutChildType, LayoutChildren, LayoutDeserializeArgs, LayoutParts, LayoutRef,
        LayoutStrategy, LayoutWriterContext, VTable,
        layouts::{
            chunked::{Chunked, ChunkedData, ChunkedLayout, writer::ChunkedLayoutStrategy},
            flat::FlatLayout,
        },
        segments::{SegmentId, SegmentSink, SegmentSinkRef},
        sequence::{
            SendableSequentialStream, SequenceId, SequencePointer, SequentialArrayStreamExt as _,
        },
    },
    session::{VortexSession, registry::ReadContext},
};

fn dtype() -> DType {
    DType::Primitive(PType::I64, Nullability::Nullable)
}

fn session(memory: &LiveMemoryPool) -> VortexSession {
    VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ))
}

#[derive(Clone)]
struct MetadataOnlyChildren {
    rows: Arc<[u64]>,
    inspected: Arc<AtomicUsize>,
}

impl MetadataOnlyChildren {
    fn new(rows: Vec<u64>) -> Self {
        Self {
            rows: rows.into(),
            inspected: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl LayoutChildren for MetadataOnlyChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, _index: usize, _dtype: &DType) -> vortex::error::VortexResult<LayoutRef> {
        panic!("offset metadata must not materialize children")
    }

    fn child_row_count(&self, index: usize) -> u64 {
        self.inspected.fetch_add(1, Ordering::Relaxed);
        self.rows[index]
    }

    fn nchildren(&self) -> usize {
        self.rows.len()
    }
}

fn deserialize(
    children: &dyn LayoutChildren,
    rows: u64,
    consumer: &VortexSession,
) -> vortex::error::VortexResult<ChunkedData> {
    Chunked.deserialize(
        &LayoutDeserializeArgs {
            session: consumer,
            array_read_ctx: &ReadContext::new([]),
            dtype: &dtype(),
            row_count: rows,
            segment_ids: Vec::new(),
            children,
        },
        &EmptyMetadata,
    )
}

#[test]
fn native_chunked_offsets_requires_admission_before_row_counts() {
    let children = MetadataOnlyChildren::new(vec![3; 128]);
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = deserialize(&children, 384, &session(&memory))
        .expect_err("offset storage must be admitted before construction");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(children.inspected.load(Ordering::Relaxed), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_chunked_offsets_clones_keep_one_credit_after_layout_and_session_drop() {
    let children = MetadataOnlyChildren::new(vec![3; 128]);
    let required = u64::try_from(129 * std::mem::size_of::<u64>()).unwrap();
    let memory = LiveMemoryPool::new(required).unwrap();
    let consumer = session(&memory);
    let data = deserialize(&children, 384, &consumer).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, required);
    let layout =
        LayoutParts::new(Chunked, dtype(), 384, Vec::new(), children.to_arc(), data).into_typed();
    let retained_data = layout.data().clone();
    let alias = retained_data.clone();
    drop((layout, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, required);
    assert_eq!(memory.snapshot().peak_reserved_bytes, required);
    let rebuilt = LayoutParts::new(
        Chunked,
        dtype(),
        384,
        Vec::new(),
        children.to_arc(),
        retained_data,
    )
    .into_typed();
    for index in 0..128 {
        assert_eq!(
            Chunked::child_type(&rebuilt, index),
            LayoutChildType::Chunk((index, index as u64 * 3))
        );
    }
    drop(rebuilt);
    assert_eq!(memory.snapshot().reserved_bytes, required);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_chunked_offsets_exact_short_and_empty_grants_preserve_boundaries() {
    for width in [0, 1, 2, 7, 8, 128] {
        let children = MetadataOnlyChildren::new((0..width).map(|n| n % 3).collect());
        let rows = children.rows.iter().sum();
        let required = if width == 0 { 0 } else { (width + 1) * 8 };
        let memory = LiveMemoryPool::new(required.max(1)).unwrap();
        let data = deserialize(&children, rows, &session(&memory)).unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, required);
        assert_eq!(memory.snapshot().peak_reserved_bytes, required);
        let layout = LayoutParts::new(Chunked, dtype(), rows, Vec::new(), children.to_arc(), data)
            .into_typed();
        let mut offset = 0;
        for (index, child_rows) in children.rows.iter().enumerate() {
            assert_eq!(
                Chunked::child_type(&layout, index),
                LayoutChildType::Chunk((index, offset))
            );
            offset += child_rows;
        }
        assert_eq!(offset, rows);
        drop(layout);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        if required > 0 {
            let short = LiveMemoryPool::new(required - 1).unwrap();
            let error = deserialize(&children, rows, &session(&short)).unwrap_err();
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(short.snapshot().peak_reserved_bytes, 0);
            assert_eq!(short.snapshot().reserved_bytes, 0);
            assert_eq!(short.snapshot().denied_reservations, 1);
        }
    }
}

#[derive(Clone)]
struct OversizedChildren(usize);

impl LayoutChildren for OversizedChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        panic!("invalid count must fail before child ownership is requested")
    }

    fn child(&self, _index: usize, _dtype: &DType) -> vortex::error::VortexResult<LayoutRef> {
        panic!("invalid count must fail before child materialization")
    }

    fn child_row_count(&self, _index: usize) -> u64 {
        panic!("invalid count must fail before row counts are inspected")
    }

    fn nchildren(&self) -> usize {
        self.0
    }
}

#[test]
fn native_chunked_offsets_count_overflow_precedes_admission() {
    for count in [usize::MAX, usize::MAX / 8] {
        let memory = LiveMemoryPool::new(1).unwrap();
        let error = deserialize(&OversizedChildren(count), 0, &session(&memory)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Chunk offset table size overflow")
        );
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn native_chunked_offsets_malformed_rows_release_admitted_storage() {
    for (counts, rows, message) in [
        (
            vec![u64::MAX, 1],
            u64::MAX,
            "Chunked child row counts overflow",
        ),
        (
            vec![2, 3],
            6,
            "Chunked child row counts do not add up to parent row count",
        ),
    ] {
        let children = MetadataOnlyChildren::new(counts);
        let required = u64::try_from((children.rows.len() + 1) * 8).unwrap();
        let memory = LiveMemoryPool::new(required).unwrap();
        let error = deserialize(&children, rows, &session(&memory)).unwrap_err();
        assert!(error.to_string().contains(message));
        assert_eq!(memory.snapshot().peak_reserved_bytes, required);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn native_chunked_offsets_competing_owner_denial_preserves_owner_and_can_retry() {
    let children = MetadataOnlyChildren::new(vec![3; 8]);
    let required = 9 * 8;
    let memory = LiveMemoryPool::new(required * 2).unwrap();
    let consumer = session(&memory);
    let first = deserialize(&children, 24, &consumer).unwrap();
    let competitor = memory.reserve(required).unwrap();
    let error = deserialize(&children, 24, &consumer).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, required * 2);
    drop(competitor);
    let second = deserialize(&children, 24, &consumer).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, required * 2);
    drop((first, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, required);
    drop(second);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[derive(Debug)]
struct BufferOnlyAllocator;

impl vortex::array::memory::HostAllocator for BufferOnlyAllocator {
    fn allocate(
        &self,
        _bytes: usize,
        _alignment: vortex::buffer::Alignment,
    ) -> VortexResult<vortex::array::memory::WritableHostBuffer> {
        panic!("offset admission must not request a byte-buffer allocation")
    }
}

#[test]
fn native_chunked_offsets_custom_allocator_and_empty_writer_constructor() {
    let consumer = VortexSession::default().with_allocator(Arc::new(BufferOnlyAllocator));
    let empty = MetadataOnlyChildren::new(Vec::new());
    let layout = ChunkedLayout::try_new(0, dtype(), empty.to_arc(), &consumer).unwrap();
    assert_eq!(layout.nchildren(), 0);
    assert_eq!(layout.row_count(), 0);
    assert!(deserialize(&empty, 0, &consumer).is_ok());
    let nonempty = MetadataOnlyChildren::new(vec![3]);
    for error in [
        ChunkedLayout::try_new(3, dtype(), nonempty.to_arc(), &consumer).unwrap_err(),
        deserialize(&nonempty, 3, &consumer).unwrap_err(),
    ] {
        assert!(
            error
                .to_string()
                .contains("does not support external memory reservations")
        );
    }
    assert_eq!(nonempty.inspected.load(Ordering::Relaxed), 0);
}

struct MetadataChild(Arc<AtomicUsize>);

impl LayoutStrategy for MetadataChild {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        _ctx: LayoutWriterContext,
        _sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        _eof: SequencePointer,
        _session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let dtype = input.dtype().clone();
            let mut rows = 0;
            while let Some(item) = input.next().await {
                rows += item?.1.len() as u64;
            }
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(FlatLayout::new(rows, dtype, 0_u32.into(), ReadContext::new([])).into_layout())
        })
    }
}

struct NoSegmentWrites;

impl SegmentSink for NoSegmentWrites {
    fn write<'a, 'future>(
        &'a self,
        _sequence: SequenceId,
        _buffers: Vec<ByteBuffer>,
    ) -> BoxFuture<'future, VortexResult<SegmentId>>
    where
        'a: 'future,
        Self: 'future,
    {
        Box::pin(async { Err(vortex_err!("offset-only fixture forbids segment writes")) })
    }
}

#[test]
fn native_chunked_offsets_real_writer_admits_and_retains_final_table() {
    for budget in [23, 24] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let runtime = CurrentThreadRuntime::new();
        let consumer = session(&memory).with_handle(runtime.handle());
        let calls = Arc::new(AtomicUsize::new(0));
        let strategy = ChunkedLayoutStrategy::new(MetadataChild(Arc::clone(&calls)));
        // Arrays and child layout handles belong to the fixture. Only the real
        // chunked writer's final offset-table allocation uses this exact grant.
        let array = PrimitiveArray::from_option_iter([Some(11_i64), None, Some(13)]).into_array();
        let input = ArrayStreamAdapter::new(
            array.dtype().clone(),
            futures::stream::iter([Ok(array.clone()), Ok(array)]),
        );
        let (pointer, eof) = SequenceId::root().split();
        let result = runtime.block_on(strategy.write_stream(
            LayoutWriterContext::new(ArrayContext::empty()),
            Arc::new(NoSegmentWrites),
            input.sequenced(pointer),
            eof,
            &consumer,
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        if budget == 23 {
            let error = result.unwrap_err();
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 1);
        } else {
            let layout = result.unwrap();
            assert_eq!(layout.row_count(), 6);
            assert_eq!(layout.nchildren(), 2);
            let typed = layout.as_::<Chunked>();
            assert_eq!(
                Chunked::child_type(typed, 1),
                LayoutChildType::Chunk((1, 3))
            );
            let data = typed.data().clone();
            drop((layout, consumer, runtime));
            assert_eq!(memory.snapshot().reserved_bytes, 24);
            drop(data);
            assert_eq!(memory.snapshot().peak_reserved_bytes, 24);
            assert_eq!(memory.snapshot().denied_reservations, 0);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
