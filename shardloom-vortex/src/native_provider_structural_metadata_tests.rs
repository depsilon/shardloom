//! Independent ownership of native layout and reader containers.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        dtype::{DType, FieldMask, Nullability, PType},
        memory::MemorySessionExt as _,
    },
    flatbuffers::{FlatBuffer, WriteFlatBufferExt as _},
    layout::{
        LayoutChildren, LayoutContext, LayoutReaderContext, LayoutReaderRef, LayoutRef,
        LazyReaderChildren, layout_children, layout_from_flatbuffer,
        layouts::{chunked::Chunked, chunked::ChunkedLayout, flat::FlatLayout},
        scan::split_by::SplitBy,
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
    session::{VortexSession, registry::ReadContext},
};

#[derive(Debug)]
struct BufferOnlyAllocator;

impl vortex::array::memory::HostAllocator for BufferOnlyAllocator {
    fn allocate(
        &self,
        _bytes: usize,
        _alignment: vortex::buffer::Alignment,
    ) -> vortex::error::VortexResult<vortex::array::memory::WritableHostBuffer> {
        panic!("structural admission must not fall through to a buffer allocation")
    }
}

#[test]
fn native_structural_metadata_custom_allocator_refuses_unadmitted_nonempty_storage() {
    let consumer = VortexSession::default().with_allocator(Arc::new(BufferOnlyAllocator));
    let fixture = chunked((0..8).map(flat).collect());
    let (buffer, context) = serialized(&fixture);
    let error = reopen(buffer, &context, &consumer).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
    let error = fixture
        .new_reader(
            "custom-allocator".into(),
            Arc::new(NoSegmentReads),
            &consumer,
            &LayoutReaderContext::new(),
        )
        .err()
        .expect("a native reader cannot silently use an unadmitted cache");
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
    let (buffer, context) = serialized(&flat(0));
    assert_eq!(reopen(buffer, &context, &consumer).unwrap().nchildren(), 0);
    let empty = LazyReaderChildren::try_new_uniform(
        Arc::new(UnmaterializedChildren(0)),
        dtype(),
        Arc::from("empty"),
        Arc::new(NoSegmentReads),
        consumer,
        LayoutReaderContext::new(),
    )
    .unwrap();
    assert!(empty.get(0).is_err());
}

#[test]
fn native_structural_metadata_concurrent_aliases_share_one_nested_cache_owner() {
    let nested = chunked((0..64).map(flat).collect());
    let (nested_buffer, nested_context) = serialized(&nested);
    let measured = LiveMemoryPool::new(1 << 20).unwrap();
    let one = reopen(nested_buffer, &nested_context, &session(&measured)).unwrap();
    let nested_bytes = measured.snapshot().reserved_bytes;
    assert!(nested_bytes > 0);
    drop(one);
    assert_eq!(measured.snapshot().reserved_bytes, 0);

    let fixture = chunked(vec![nested, flat(64)]);
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory);
    let layout = reopen(buffer, &context, &consumer).unwrap();
    let parent_bytes = memory.snapshot().reserved_bytes;
    let barrier = std::sync::Barrier::new(4);
    let children = std::thread::scope(|scope| {
        let jobs = (0..4)
            .map(|_| {
                let layout = Arc::clone(&layout);
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    layout.slot(0).unwrap().unwrap()
                })
            })
            .collect::<Vec<_>>();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        children
            .iter()
            .all(|child| Arc::ptr_eq(child, &children[0]))
    );
    assert_eq!(
        memory.snapshot().reserved_bytes,
        parent_bytes + nested_bytes
    );
    assert_eq!(
        memory.snapshot().peak_reserved_bytes,
        parent_bytes + nested_bytes
    );
    assert_eq!(memory.snapshot().denied_reservations, 0);
    drop((layout, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, nested_bytes);
    assert_eq!(children[0].slot(63).unwrap().unwrap().row_count(), 3);
    drop(children);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn dtype() -> DType {
    DType::Primitive(PType::I64, Nullability::Nullable)
}

fn session(memory: &LiveMemoryPool) -> VortexSession {
    VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ))
}

#[derive(Debug)]
struct RecordingAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    requests: Arc<Mutex<Vec<usize>>>,
}

impl vortex::array::memory::HostAllocator for RecordingAllocator {
    fn allocate(
        &self,
        _bytes: usize,
        _alignment: vortex::buffer::Alignment,
    ) -> vortex::error::VortexResult<vortex::array::memory::WritableHostBuffer> {
        panic!("caller-owned serialized bytes must not be copied by this fixture")
    }

    fn reserve_external(
        &self,
        bytes: usize,
    ) -> vortex::error::VortexResult<vortex::array::memory::HostMemoryReservation> {
        self.requests.lock().unwrap().push(bytes);
        vortex::array::memory::HostAllocator::reserve_external(&self.inner, bytes)
    }
}

fn recording_session(memory: &LiveMemoryPool) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let consumer = VortexSession::default().with_allocator(Arc::new(RecordingAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        requests: Arc::clone(&requests),
    }));
    (consumer, requests)
}

fn layout_storage_charges(requests: &Mutex<Vec<usize>>, width: usize) -> (u64, u64) {
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "child cache and offset table are distinct owners"
    );
    assert!(requests[0] > width * std::mem::size_of::<LayoutRef>());
    assert_eq!(requests[1], (width + 1) * std::mem::size_of::<u64>());
    (requests[0] as u64, requests[1] as u64)
}

fn flat(index: usize) -> LayoutRef {
    FlatLayout::new(
        3,
        dtype(),
        SegmentId::try_from(index).unwrap(),
        ReadContext::new([]),
    )
    .into_layout()
}

fn chunked(children: Vec<LayoutRef>) -> LayoutRef {
    let rows = children.iter().map(|child| child.row_count()).sum();
    ChunkedLayout::new(rows, dtype(), layout_children(children)).into_layout()
}

fn serialized(layout: &LayoutRef) -> (FlatBuffer, ReadContext) {
    let context = LayoutContext::empty();
    let buffer = layout
        .flatbuffer_writer(&context)
        .write_flatbuffer_bytes()
        .unwrap();
    (buffer, ReadContext::new(context.to_ids()))
}

fn reopen(
    buffer: FlatBuffer,
    context: &ReadContext,
    session: &VortexSession,
) -> vortex::error::VortexResult<LayoutRef> {
    layout_from_flatbuffer(buffer, &dtype(), context, &ReadContext::new([]), session)
}

struct NoSegmentReads;

impl SegmentSource for NoSegmentReads {
    fn request(&self, _id: SegmentId) -> SegmentFuture {
        panic!("metadata construction must not read any segment payload")
    }
}

#[test]
fn native_structural_metadata_layout_cache_requires_admission_before_construction() {
    let fixture = chunked((0..128).map(flat).collect());
    let (buffer, context) = serialized(&fixture);
    // The serialized input and logical schema belong to the caller. This
    // isolates new structural storage from already-covered file byte copies.
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = reopen(buffer, &context, &session(&memory))
        .expect_err("a one-byte grant cannot admit 128 layout cache cells");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_structural_metadata_layout_cache_shares_credit_until_last_alias() {
    let fixture = chunked((0..128).map(flat).collect());
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (consumer, requests) = recording_session(&memory);
    let layout = reopen(buffer, &context, &consumer).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let (cache_bytes, offset_bytes) = layout_storage_charges(&requests, 128);
    assert_eq!(retained, cache_bytes + offset_bytes);
    assert!(retained > 0, "layout cache cells need their own owner");
    let children = Arc::clone(layout.as_::<Chunked>().children());
    let alias = children.to_arc();
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let child = alias.child(127, &dtype()).unwrap();
    assert_eq!(child.row_count(), 3);
    assert_eq!(child.dtype(), &dtype());
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop((layout, consumer, children));
    // The child adapter keeps its cache, but does not own the parent layout's
    // independently admitted offset table. Its release must be observable.
    assert_eq!(memory.snapshot().reserved_bytes, cache_bytes);
    assert_eq!(alias.nchildren(), 128);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    // An independently retained leaf remains usable after its parent's cache
    // storage is freed. Its caller-owned schema is not charged as cache bytes.
    assert_eq!(child.row_count(), 3);
    assert_eq!(child.segment_ids(), vec![SegmentId::from(127_u32)]);
}

#[test]
fn native_structural_metadata_nested_cache_denial_preserves_parent_and_can_retry() {
    let nested = chunked((0..64).map(flat).collect());
    let fixture = chunked(vec![nested, flat(64)]);
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory);
    let layout = reopen(buffer, &context, &consumer).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0);
    let competitor = memory
        .reserve(memory.snapshot().limit_bytes - retained)
        .unwrap();
    let error = layout
        .slot(0)
        .expect_err("a new nested cache cannot allocate against a full grant");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(
        memory.snapshot().reserved_bytes,
        memory.snapshot().limit_bytes
    );
    assert_eq!(layout.row_count(), 195);
    assert_eq!(layout.slot(1).unwrap().unwrap().row_count(), 3);
    drop(competitor);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let child = layout.slot(0).unwrap().unwrap();
    assert_eq!(child.row_count(), 192);
    assert_eq!(child.nchildren(), 64);
    assert!(memory.snapshot().reserved_bytes > retained);
    drop((layout, consumer));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(child.slot(63).unwrap().unwrap().row_count(), 3);
    drop(child);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_reader_cache_requires_its_own_admission() {
    // The in-memory layout is caller-owned and bypasses serialized-layout
    // construction, isolating the subsequent reader-cell allocation.
    let layout = chunked((0..128).map(flat).collect());
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = layout
        .new_reader(
            "structural-test".into(),
            Arc::new(NoSegmentReads),
            &session(&memory),
            &LayoutReaderContext::new(),
        )
        .err()
        .expect("a one-byte grant cannot admit 128 reader cache cells");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_structural_metadata_reader_cache_survives_session_and_layout_drop() {
    let layout = chunked((0..128).map(flat).collect());
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory);
    let reader = layout
        .new_reader(
            "structural-test".into(),
            Arc::new(NoSegmentReads),
            &consumer,
            &LayoutReaderContext::new(),
        )
        .unwrap();
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0, "reader cache cells need their own owner");
    let alias = Arc::clone(&reader);
    drop((layout, consumer, reader));
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(alias.row_count(), 384);
    assert_eq!(alias.dtype(), &dtype());
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_empty_child_tables_do_not_spend_payload_credit() {
    let fixture = flat(0);
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1).unwrap();
    let layout = reopen(buffer, &context, &session(&memory)).unwrap();
    assert_eq!(layout.nchildren(), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_structural_metadata_invalid_child_preserves_parent_and_releases_credit() {
    let fixture = chunked((0..64).map(flat).collect());
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    // A failed lookup leaves the already-admitted parent and its valid child
    // accesses usable; dropping it must still release the container credit.
    let layout = reopen(buffer, &context, &session(&memory)).unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    let children = Arc::clone(layout.as_::<Chunked>().children());
    let error = children
        .child(64, &dtype())
        .expect_err("the out-of-range child must fail");
    assert!(error.to_string().contains("Child index out of bounds"));
    assert_eq!(layout.slot(63).unwrap().unwrap().row_count(), 3);
    drop((children, layout));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_layout_cache_exact_and_short_grants() {
    for width in [1, 2, 7, 8, 128] {
        let fixture = chunked((0..width).map(flat).collect());
        let (buffer, context) = serialized(&fixture);
        let observed = LiveMemoryPool::new(1 << 20).unwrap();
        let (consumer, requests) = recording_session(&observed);
        let layout = reopen(buffer.clone(), &context, &consumer).unwrap();
        let (cache_bytes, offset_bytes) = layout_storage_charges(&requests, width);
        let required = observed.snapshot().reserved_bytes;
        assert_eq!(required, cache_bytes + offset_bytes);
        assert!(required > 1);
        drop(layout);
        assert_eq!(observed.snapshot().reserved_bytes, 0);
        for budget in [cache_bytes - 1, required - 1, required] {
            let memory = LiveMemoryPool::new(budget).unwrap();
            let result = reopen(buffer.clone(), &context, &session(&memory));
            if budget < required {
                let error = result.expect_err("one-byte-short structural grant");
                assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
                assert_eq!(
                    memory.snapshot().peak_reserved_bytes,
                    if budget < cache_bytes { 0 } else { cache_bytes }
                );
                assert_eq!(memory.snapshot().denied_reservations, 1);
            } else {
                let layout = result.unwrap();
                assert_eq!(layout.nchildren(), width);
                assert_eq!(memory.snapshot().reserved_bytes, required);
                drop(layout);
                assert_eq!(memory.snapshot().peak_reserved_bytes, required);
            }
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_structural_metadata_reader_cache_exact_and_short_grants() {
    let layout = chunked((0..128).map(flat).collect());
    let open = |memory: &LiveMemoryPool| {
        layout.new_reader(
            "reader-exact".into(),
            Arc::new(NoSegmentReads),
            &session(memory),
            &LayoutReaderContext::new(),
        )
    };
    let observed = LiveMemoryPool::new(1 << 20).unwrap();
    let reader = open(&observed).unwrap();
    let required = observed.snapshot().reserved_bytes;
    assert!(required > 1);
    drop(reader);
    assert_eq!(observed.snapshot().reserved_bytes, 0);
    for budget in [required - 1, required] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let result = open(&memory);
        if budget < required {
            let error = result.err().expect("one-byte-short reader grant");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 1);
        } else {
            let reader = result.unwrap();
            assert_eq!(reader.row_count(), 384);
            assert_eq!(memory.snapshot().reserved_bytes, required);
            drop(reader);
            assert_eq!(memory.snapshot().peak_reserved_bytes, required);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[derive(Clone)]
struct UnmaterializedChildren(usize);

impl LayoutChildren for UnmaterializedChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, _index: usize, _dtype: &DType) -> vortex::error::VortexResult<LayoutRef> {
        panic!("shape checks must precede child materialization")
    }

    fn child_row_count(&self, _index: usize) -> u64 {
        panic!("cache shape checks do not require row counts")
    }

    fn nchildren(&self) -> usize {
        self.0
    }
}

#[test]
fn native_structural_metadata_reader_shape_errors_precede_admission() {
    for (types, names) in [(1, 2), (2, 1), (3, 2), (2, 3)] {
        let memory = LiveMemoryPool::new(1).unwrap();
        let error = LazyReaderChildren::try_new(
            Arc::new(UnmaterializedChildren(2)),
            vec![dtype(); types],
            vec![Arc::from("child"); names],
            Arc::new(NoSegmentReads),
            session(&memory),
            LayoutReaderContext::new(),
        )
        .err()
        .expect("mismatched child metadata must fail");
        assert!(error.to_string().contains("Child metadata lengths"));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn native_structural_metadata_reader_size_overflow_precedes_admission() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = LazyReaderChildren::try_new_uniform(
        Arc::new(UnmaterializedChildren(usize::MAX)),
        dtype(),
        Arc::from("child"),
        Arc::new(NoSegmentReads),
        session(&memory),
        LayoutReaderContext::new(),
    )
    .err()
    .expect("overflow must fail before allocation or child access");
    assert!(
        error
            .to_string()
            .contains("Reader child cache size overflow")
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_structural_metadata_empty_reader_cache_spends_no_credit() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let reader = LazyReaderChildren::try_new_uniform(
        Arc::new(UnmaterializedChildren(0)),
        dtype(),
        Arc::from("empty"),
        Arc::new(NoSegmentReads),
        session(&memory),
        LayoutReaderContext::new(),
    )
    .unwrap();
    assert!(reader.get(0).is_err());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[derive(Clone)]
struct ClassifiedChildren {
    layouts: Arc<[LayoutRef]>,
    flags: Arc<[bool]>,
    opened: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}

impl LayoutChildren for ClassifiedChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, index: usize, dtype: &DType) -> vortex::error::VortexResult<LayoutRef> {
        self.opened.fetch_add(1, Ordering::Relaxed);
        if self.fail.load(Ordering::Relaxed) {
            return Err(vortex::error::vortex_err!(
                "injected child construction failure"
            ));
        }
        assert_eq!(self.layouts[index].dtype(), dtype);
        Ok(Arc::clone(&self.layouts[index]))
    }

    fn child_row_count(&self, index: usize) -> u64 {
        self.layouts[index].row_count()
    }

    fn nchildren(&self) -> usize {
        self.layouts.len()
    }

    fn child_is_indivisible(&self, index: usize) -> bool {
        self.flags[index]
    }
}

fn classified_reader(
    flags: &[bool],
    memory: &LiveMemoryPool,
) -> (LayoutReaderRef, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let opened = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    // False is conservative for flat children, exercising the ordinary reader
    // path without introducing an unrelated nested cache allocation.
    let children = ClassifiedChildren {
        layouts: (0..flags.len()).map(flat).collect(),
        flags: flags.into(),
        opened: Arc::clone(&opened),
        fail: Arc::clone(&fail),
    };
    let layout = ChunkedLayout::new(
        u64::try_from(flags.len()).unwrap() * 3,
        dtype(),
        Arc::new(children),
    );
    let reader = layout
        .new_reader(
            "classification".into(),
            Arc::new(NoSegmentReads),
            &session(memory),
            &LayoutReaderContext::new(),
        )
        .unwrap();
    (reader, opened, fail)
}

fn registered_splits(reader: &LayoutReaderRef) -> vortex::error::VortexResult<Vec<u64>> {
    SplitBy::Layout.splits(reader.as_ref(), &(0..reader.row_count()), &[])
}

#[test]
fn native_structural_metadata_uniform_classification_needs_no_flag_array() {
    for indivisible in [true, false] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let (reader, opened, _) = classified_reader(&[indivisible; 4], &memory);
        let retained = memory.snapshot().reserved_bytes;
        assert!(retained > 0);
        let competitor = memory
            .reserve(memory.snapshot().limit_bytes - retained)
            .unwrap();
        assert_eq!(registered_splits(&reader).unwrap(), vec![0, 3, 6, 9, 12]);
        assert_eq!(
            opened.load(Ordering::Relaxed),
            if indivisible { 0 } else { 4 }
        );
        assert_eq!(memory.snapshot().denied_reservations, 0);
        drop(competitor);
        assert_eq!(memory.snapshot().reserved_bytes, retained);
        drop(reader);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_structural_metadata_mixed_classification_denial_can_retry() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (reader, opened, _) = classified_reader(&[true, false, true, false], &memory);
    let retained = memory.snapshot().reserved_bytes;
    let competitor = memory
        .reserve(memory.snapshot().limit_bytes - retained)
        .unwrap();
    let error = registered_splits(&reader).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(competitor);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(registered_splits(&reader).unwrap(), vec![0, 3, 6, 9, 12]);
    assert_eq!(opened.load(Ordering::Relaxed), 2);
    assert_eq!(memory.snapshot().reserved_bytes, retained + 4);
    assert_eq!(registered_splits(&reader).unwrap(), vec![0, 3, 6, 9, 12]);
    assert_eq!(opened.load(Ordering::Relaxed), 2);
    drop(reader);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_child_failure_keeps_classification_owned_and_retryable() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (reader, opened, fail) = classified_reader(&[true, false, true, false], &memory);
    let retained = memory.snapshot().reserved_bytes;
    fail.store(true, Ordering::Relaxed);
    let error = registered_splits(&reader).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected child construction failure")
    );
    assert_eq!(opened.load(Ordering::Relaxed), 1);
    assert_eq!(memory.snapshot().reserved_bytes, retained + 4);
    fail.store(false, Ordering::Relaxed);
    assert_eq!(registered_splits(&reader).unwrap(), vec![0, 3, 6, 9, 12]);
    assert_eq!(opened.load(Ordering::Relaxed), 3);
    assert_eq!(memory.snapshot().reserved_bytes, retained + 4);
    drop(reader);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_traversal_propagates_denial_and_can_restart() {
    let fixture = chunked(vec![chunked((0..64).map(flat).collect()), flat(64)]);
    let (buffer, context) = serialized(&fixture);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory);
    let layout = reopen(buffer, &context, &consumer).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let competitor = memory
        .reserve(memory.snapshot().limit_bytes - retained)
        .unwrap();
    let mut traversal = layout.depth_first_traversal();
    let error = traversal
        .next()
        .expect("the root is pending")
        .expect_err("denied children must not become an apparently complete traversal");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(traversal.next().is_none(), "a failed traversal terminates");
    drop(traversal);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(competitor);
    assert_eq!(memory.snapshot().reserved_bytes, retained);

    let nodes = layout
        .depth_first_traversal()
        .collect::<vortex::error::VortexResult<Vec<_>>>()
        .unwrap();
    assert_eq!(nodes.len(), 67);
    assert_eq!(nodes[0].row_count(), 195);
    assert_eq!(nodes[1].row_count(), 192);
    assert!(nodes[2..].iter().all(|node| node.row_count() == 3));
    assert_eq!(
        nodes
            .iter()
            .flat_map(|node| node.segment_ids())
            .collect::<Vec<_>>(),
        (0_u32..65).map(SegmentId::from).collect::<Vec<_>>()
    );
    drop((layout, consumer));
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(nodes);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn reader_cell_bytes(width: usize) -> u64 {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let cache = LazyReaderChildren::try_new_uniform(
        Arc::new(UnmaterializedChildren(width)),
        dtype(),
        Arc::from("caller-owned-name"),
        Arc::new(NoSegmentReads),
        session(&memory),
        LayoutReaderContext::new(),
    )
    .unwrap();
    let bytes = memory.snapshot().reserved_bytes;
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    bytes
}

fn assert_native_reader_metadata_storage(layout: &LayoutRef, has_per_child_metadata: bool) {
    let width = layout.nchildren();
    let metadata_bytes = if has_per_child_metadata {
        u64::try_from(width * (size_of::<DType>() + size_of::<Arc<str>>())).unwrap()
    } else {
        0
    };
    let expected = reader_cell_bytes(width) + metadata_bytes;
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (consumer, requests) = recording_session(&memory);
    let reader = layout
        .new_reader(
            "metadata-storage".into(),
            Arc::new(NoSegmentReads),
            &consumer,
            &LayoutReaderContext::new(),
        )
        .unwrap();
    assert_eq!(
        *requests.lock().unwrap(),
        if expected == 0 {
            Vec::new()
        } else {
            vec![usize::try_from(expected).unwrap()]
        },
        "native reader must admit both metadata vectors before allocating them"
    );
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let alias = Arc::clone(&reader);
    drop((reader, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(alias.row_count(), layout.row_count());
    assert_eq!(alias.dtype(), layout.dtype());
    if matches!(layout.dtype(), DType::Struct(..)) {
        // Materialize the validity and every field reader without reading payload.
        // Owned children check each supplied dtype, including nullable slot offsets.
        assert_eq!(
            SplitBy::Layout
                .splits(alias.as_ref(), &(0..layout.row_count()), &[FieldMask::All])
                .unwrap(),
            vec![0, layout.row_count()]
        );
        assert_eq!(memory.snapshot().reserved_bytes, expected);
    }
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    if expected > 0 {
        for budget in [expected - 1, expected] {
            let memory = LiveMemoryPool::new(budget.max(1)).unwrap();
            let consumer = session(&memory);
            let result = layout.new_reader(
                "exact-or-short".into(),
                Arc::new(NoSegmentReads),
                &consumer,
                &LayoutReaderContext::new(),
            );
            if budget == expected {
                let reader = result.unwrap();
                assert_eq!(memory.snapshot().reserved_bytes, expected);
                drop(consumer);
                assert_eq!(memory.snapshot().reserved_bytes, expected);
                drop(reader);
                assert_eq!(memory.snapshot().denied_reservations, 0);
                assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
            } else {
                let error = result.err().expect("short metadata grant must be denied");
                assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
                assert_eq!(memory.snapshot().denied_reservations, 1);
                assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            }
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_structural_metadata_native_struct_reader_admits_metadata_vectors() {
    use vortex::array::dtype::StructFields;
    use vortex::layout::layouts::struct_::StructLayout;

    for width in [0, 1, 2, 7, 8, 80, 81, 128] {
        for nullable in [false, true] {
            let fields = StructFields::from_iter(
                (0..width).map(|index| (format!("field_{index}"), dtype())),
            );
            let mut children = Vec::new();
            if nullable {
                children.push(
                    FlatLayout::new(
                        3,
                        DType::Bool(Nullability::NonNullable),
                        SegmentId::from(0_u32),
                        ReadContext::new([]),
                    )
                    .into_layout(),
                );
            }
            children.extend((0..width).map(flat));
            let layout = StructLayout::new(
                3,
                DType::Struct(
                    fields,
                    if nullable {
                        Nullability::Nullable
                    } else {
                        Nullability::NonNullable
                    },
                ),
                children,
            )
            .into_layout();
            assert_native_reader_metadata_storage(&layout, true);
        }
    }
}

#[test]
fn native_structural_metadata_native_zoned_reader_admits_metadata_vectors() {
    use std::num::NonZeroUsize;
    use vortex::array::dtype::StructFields;
    use vortex::layout::layouts::zoned::ZonedLayout;

    let zones = FlatLayout::new(
        1,
        DType::Struct(
            StructFields::from_iter(std::iter::empty::<(String, DType)>()),
            Nullability::NonNullable,
        ),
        SegmentId::from(1_u32),
        ReadContext::new([]),
    )
    .into_layout();
    let layout = ZonedLayout::try_new(flat(0), zones, NonZeroUsize::new(3).unwrap(), Arc::new([]))
        .unwrap()
        .into_layout();
    assert_native_reader_metadata_storage(&layout, true);
}

#[test]
fn native_structural_metadata_native_chunked_reader_admits_debug_metadata_vectors() {
    for width in [0, 1, 2, 7, 8, 128] {
        assert_native_reader_metadata_storage(
            &chunked((0..width).map(flat).collect()),
            cfg!(debug_assertions),
        );
    }
}

fn reader_metadata_bytes(width: usize) -> u64 {
    reader_cell_bytes(width)
        + u64::try_from(width * (size_of::<DType>() + size_of::<Arc<str>>())).unwrap()
}

#[test]
fn native_structural_metadata_factory_denial_precedes_callback_and_preserves_peer() {
    let width = 8;
    let expected = reader_metadata_bytes(width);
    let memory = LiveMemoryPool::new(expected).unwrap();
    let consumer = session(&memory);
    let produced = AtomicUsize::new(0);
    let name: Arc<str> = Arc::from("caller-owned");
    let make = || {
        LazyReaderChildren::try_new_with_metadata(
            Arc::new(UnmaterializedChildren(width)),
            width,
            |_| {
                produced.fetch_add(1, Ordering::Relaxed);
                Ok((dtype(), Arc::clone(&name)))
            },
            Arc::new(NoSegmentReads),
            consumer.clone(),
            LayoutReaderContext::new(),
        )
    };
    let first = make().unwrap();
    assert_eq!(produced.load(Ordering::Relaxed), width);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let error = make().err().expect("a retained peer holds the whole grant");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(produced.load(Ordering::Relaxed), width);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(first);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let retry = make().unwrap();
    assert_eq!(produced.load(Ordering::Relaxed), 2 * width);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    drop(retry);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_factory_partial_failure_releases_vectors_and_credit() {
    let width = 8;
    let expected = reader_metadata_bytes(width);
    let memory = LiveMemoryPool::new(expected).unwrap();
    let produced = AtomicUsize::new(0);
    let name: Arc<str> = Arc::from("caller-owned");
    let error = LazyReaderChildren::try_new_with_metadata(
        Arc::new(UnmaterializedChildren(width)),
        width,
        |index| {
            produced.fetch_add(1, Ordering::Relaxed);
            if index == 2 {
                return Err(vortex::error::vortex_err!("injected metadata failure"));
            }
            Ok((dtype(), Arc::clone(&name)))
        },
        Arc::new(NoSegmentReads),
        session(&memory),
        LayoutReaderContext::new(),
    )
    .err()
    .expect("partial metadata must not produce a reader");
    assert!(error.to_string().contains("injected metadata failure"));
    assert_eq!(produced.load(Ordering::Relaxed), 3);
    assert_eq!(Arc::strong_count(&name), 1);
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    assert_eq!(memory.snapshot().denied_reservations, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_structural_metadata_factory_shape_and_size_errors_precede_admission() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let produced = AtomicUsize::new(0);
    for metadata_count in [2, 4] {
        let error = LazyReaderChildren::try_new_with_metadata(
            Arc::new(UnmaterializedChildren(3)),
            metadata_count,
            |_| {
                produced.fetch_add(1, Ordering::Relaxed);
                Ok((dtype(), Arc::from("unreachable")))
            },
            Arc::new(NoSegmentReads),
            session(&memory),
            LayoutReaderContext::new(),
        )
        .err()
        .expect("metadata count must match before allocation");
        assert!(error.to_string().contains("Child metadata length"));
    }
    let largest_element = size_of::<DType>()
        .max(size_of::<Arc<str>>())
        .max(usize::try_from(reader_cell_bytes(1)).unwrap());
    for width in [
        usize::MAX,
        usize::try_from(isize::MAX).unwrap() / largest_element,
    ] {
        let error = LazyReaderChildren::try_new_with_metadata(
            Arc::new(UnmaterializedChildren(width)),
            width,
            |_| {
                produced.fetch_add(1, Ordering::Relaxed);
                Ok((dtype(), Arc::from("unreachable")))
            },
            Arc::new(NoSegmentReads),
            session(&memory),
            LayoutReaderContext::new(),
        )
        .err()
        .expect("each allocation and their sum must be checked");
        assert!(error.to_string().contains("overflow"));
    }
    assert_eq!(produced.load(Ordering::Relaxed), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_structural_metadata_factory_empty_and_custom_allocator_boundaries() {
    let consumer = VortexSession::default().with_allocator(Arc::new(BufferOnlyAllocator));
    let empty = LazyReaderChildren::try_new_with_metadata(
        Arc::new(UnmaterializedChildren(0)),
        0,
        |_| panic!("empty metadata must not be produced"),
        Arc::new(NoSegmentReads),
        consumer.clone(),
        LayoutReaderContext::new(),
    )
    .unwrap();
    assert!(empty.get(0).is_err());
    let error = LazyReaderChildren::try_new_with_metadata(
        Arc::new(UnmaterializedChildren(1)),
        1,
        |_| panic!("unsupported admission must precede metadata production"),
        Arc::new(NoSegmentReads),
        consumer,
        LayoutReaderContext::new(),
    )
    .err()
    .expect("nonempty vectors require external allocation admission");
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
}

#[test]
fn native_structural_metadata_factory_keeps_child_types_names_and_alias_lifetimes() {
    let types = [
        dtype(),
        DType::Bool(Nullability::Nullable),
        DType::Utf8(Nullability::NonNullable),
    ];
    let names: [Arc<str>; 3] = ["integer".into(), "boolean".into(), "text".into()];
    let layouts = types
        .iter()
        .enumerate()
        .map(|(index, dtype)| {
            FlatLayout::new(
                3,
                dtype.clone(),
                SegmentId::try_from(index).unwrap(),
                ReadContext::new([]),
            )
            .into_layout()
        })
        .collect();
    let opened = Arc::new(AtomicUsize::new(0));
    let children = ClassifiedChildren {
        layouts,
        flags: [true; 3].into(),
        opened: Arc::clone(&opened),
        fail: Arc::new(AtomicBool::new(false)),
    };
    let expected = reader_metadata_bytes(types.len());
    let memory = LiveMemoryPool::new(expected).unwrap();
    let consumer = session(&memory);
    let mut produced = Vec::new();
    let cache = Arc::new(
        LazyReaderChildren::try_new_with_metadata(
            Arc::new(children),
            types.len(),
            |index| {
                produced.push(index);
                Ok((types[index].clone(), Arc::clone(&names[index])))
            },
            Arc::new(NoSegmentReads),
            consumer.clone(),
            LayoutReaderContext::new(),
        )
        .unwrap(),
    );
    assert_eq!(produced, vec![0, 1, 2]);
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    assert!(cache.get(3).is_err());
    let alias = Arc::clone(&cache);
    drop((cache, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let retained_children = (0..3)
        .map(|index| {
            let child = Arc::clone(alias.get(index).unwrap());
            assert_eq!(child.dtype(), &types[index]);
            assert_eq!(child.name(), &names[index]);
            assert!(Arc::ptr_eq(&child, alias.get(index).unwrap()));
            child
        })
        .collect::<Vec<_>>();
    assert_eq!(opened.load(Ordering::Relaxed), 3);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for (index, child) in retained_children.iter().enumerate() {
        assert_eq!(child.dtype(), &types[index]);
        assert_eq!(child.name(), &names[index]);
        assert_eq!(child.row_count(), 3);
    }
    drop(retained_children);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
