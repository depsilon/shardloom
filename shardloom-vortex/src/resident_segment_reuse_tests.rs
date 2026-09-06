use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use vortex::io::runtime::{BlockingRuntime as _, current::CurrentThreadRuntime};

struct TestSource {
    calls: AtomicUsize,
    read: Box<dyn Fn(SegmentId) -> SegmentFuture + Send + Sync>,
}

impl SegmentSource for TestSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.read)(id)
    }
}

fn source(bytes: usize) -> Arc<TestSource> {
    Arc::new(TestSource {
        calls: AtomicUsize::new(0),
        read: Box::new(move |id| {
            async move {
                Ok(BufferHandle::new_host(ByteBuffer::from(vec![
                    (*id)
                        .to_le_bytes(
                        )[0];
                    bytes
                ])))
            }
            .boxed()
        }),
    })
}

fn policy(bytes: u64, entries: usize) -> SegmentReusePolicy {
    SegmentReusePolicy {
        max_retained_bytes: bytes,
        max_segment_bytes: 4096,
        max_entries: entries,
    }
}

fn cache(
    source: Arc<dyn SegmentSource>,
    memory: &LiveMemoryPool,
    policy: SegmentReusePolicy,
) -> ScanSegmentReuse {
    ScanSegmentReuse::new(source, memory.clone(), policy, || Ok(())).unwrap()
}

#[test]
fn sequential_hits_do_not_register_downstream_and_slices_own_full_capacity_after_close() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let source = source(128);
    let cache = cache(source.clone(), &memory, policy(1024, 2));
    let table = cache.snapshot().unwrap().table_reserved_bytes;
    let first = runtime.block_on(cache.request(7.into())).unwrap();
    let second = runtime.block_on(cache.request(7.into())).unwrap();
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.as_host().as_slice(), &[7; 128]);
    assert_eq!(first.as_host().as_ptr(), second.as_host().as_ptr());
    let slice = first.as_host().slice(8..16);
    drop(first);
    drop(second);
    let closed = cache.close().unwrap();
    assert_eq!(closed.counters.hits, 1);
    assert_eq!(closed.counters.copied_bytes, 128);
    assert_eq!(closed.retained_entries, 0);
    assert_eq!(closed.retention.reserved_bytes, 384);
    assert_eq!(memory.snapshot().reserved_bytes, table + 384);
    assert!(runtime.block_on(cache.request(7.into())).is_err());
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 384);
    assert_eq!(slice.as_slice(), &[7; 8]);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn retention_pressure_evicts_but_never_refunds_a_live_consumer_slice() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let source = source(128);
    let cache = cache(source.clone(), &memory, policy(384, 2));
    let first = runtime.block_on(cache.request(1.into())).unwrap();
    let slice = first.as_host().slice(0..1);
    drop(first);
    // Eviction cannot reclaim the borrowed first segment. The second read must
    // remain uncached instead of oversubscribing or waiting for that consumer.
    let second = runtime.block_on(cache.request(2.into())).unwrap();
    assert_eq!(second.as_host().as_slice(), &[2; 128]);
    let pressured = cache.snapshot().unwrap();
    assert_eq!(pressured.counters.evictions, 1);
    assert_eq!(pressured.counters.pressure_bypasses, 1);
    assert_eq!(pressured.retention.reserved_bytes, 384);
    assert_eq!(pressured.retained_entries, 0);
    drop(second);
    drop(slice);
    let second = runtime.block_on(cache.request(2.into())).unwrap();
    assert_eq!(cache.snapshot().unwrap().retained_entries, 1);
    assert_eq!(source.calls.load(Ordering::SeqCst), 3);
    drop(second);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn entry_pressure_evicts_least_recently_used_completed_segment() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let source = source(128);
    let cache = cache(source.clone(), &memory, policy(4096, 2));
    for id in [1, 2, 1, 3, 1, 2] {
        assert_eq!(
            runtime
                .block_on(cache.request(id.into()))
                .unwrap()
                .as_host()
                .as_slice(),
            &[u8::try_from(id).unwrap(); 128]
        );
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 4);
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.counters.hits, 2);
    assert_eq!(snapshot.counters.evictions, 2);
    assert_eq!(snapshot.counters.peak_entries, 2);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn concurrent_consumers_share_errors_and_later_retry_reads_again() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let (send, receive) = futures::channel::oneshot::channel::<VortexResult<BufferHandle>>();
    let pending = Mutex::new(Some(receive));
    let source = Arc::new(TestSource {
        calls: AtomicUsize::new(0),
        read: Box::new(move |_| {
            if let Some(receive) = pending.lock().unwrap().take() {
                async move { receive.await.unwrap() }.boxed()
            } else {
                async { Ok(BufferHandle::new_host(ByteBuffer::from(vec![19; 128]))) }.boxed()
            }
        }),
    });
    let cache = cache(source.clone(), &memory, policy(1024, 2));
    runtime.block_on(async {
        let mut first = cache.request(0.into());
        let mut second = cache.request(0.into());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        send.send(Err(vortex_err!("injected exact-read failure")))
            .unwrap();
        let (first, second) = futures::join!(first, second);
        assert!(
            first
                .unwrap_err()
                .to_string()
                .contains("injected exact-read failure")
        );
        assert!(
            second
                .unwrap_err()
                .to_string()
                .contains("injected exact-read failure")
        );
    });
    assert_eq!(cache.snapshot().unwrap().counters.shared_requests, 1);
    assert_eq!(cache.snapshot().unwrap().counters.failed_requests, 1);
    assert_eq!(cache.snapshot().unwrap().in_flight, 0);
    assert_eq!(
        runtime
            .block_on(cache.request(0.into()))
            .unwrap()
            .as_host()
            .as_slice(),
        &[19; 128]
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cancelling_one_consumer_keeps_the_read_and_cancelling_all_refunds_the_flight() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let (send, receive) = futures::channel::oneshot::channel::<BufferHandle>();
    let pending = Mutex::new(Some(receive));
    let source = Arc::new(TestSource {
        calls: AtomicUsize::new(0),
        read: Box::new(move |_| {
            if let Some(receive) = pending.lock().unwrap().take() {
                async move { Ok(receive.await.unwrap()) }.boxed()
            } else {
                futures::future::pending().boxed()
            }
        }),
    });
    let cache = cache(source.clone(), &memory, policy(1024, 1));
    runtime.block_on(async {
        let mut first = cache.request(0.into());
        let mut second = cache.request(0.into());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(first);
        assert_eq!(cache.snapshot().unwrap().in_flight, 1);
        send.send(BufferHandle::new_host(ByteBuffer::from(vec![43; 128])))
            .unwrap();
        assert_eq!(second.await.unwrap().as_host().as_slice(), &[43; 128]);
        let mut abandoned = cache.request(1.into());
        assert!(futures::poll!(&mut abandoned).is_pending());
        drop(abandoned);
    });
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(snapshot.counters.cancelled_requests, 1);
    assert_eq!(snapshot.retention.reserved_bytes, 0);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn full_in_flight_table_bypasses_without_expanding_and_close_rejects_late_delivery() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let (send, receive) = futures::channel::oneshot::channel::<BufferHandle>();
    let pending = Mutex::new(Some(receive));
    let source = Arc::new(TestSource {
        calls: AtomicUsize::new(0),
        read: Box::new(move |id| {
            if *id == 0 {
                let receive = pending.lock().unwrap().take().unwrap();
                async move { Ok(receive.await.unwrap()) }.boxed()
            } else {
                async { Ok(BufferHandle::new_host(ByteBuffer::from(vec![1; 128]))) }.boxed()
            }
        }),
    });
    let cache = cache(source, &memory, policy(1024, 1));
    runtime.block_on(async {
        let mut first = cache.request(0.into());
        assert!(futures::poll!(&mut first).is_pending());
        assert_eq!(
            cache.request(1.into()).await.unwrap().as_host().as_slice(),
            &[1; 128]
        );
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.counters.entry_bypasses, 1);
        assert_eq!(snapshot.counters.peak_entries, 1);
        assert_eq!(snapshot.in_flight, 1);
        assert_eq!(cache.close().unwrap().in_flight, 1);
        send.send(BufferHandle::new_host(ByteBuffer::from(vec![0; 128])))
            .unwrap();
        assert!(first.await.unwrap_err().to_string().contains("closed"));
    });
    assert_eq!(cache.snapshot().unwrap().in_flight, 0);
    assert_eq!(cache.snapshot().unwrap().retention.reserved_bytes, 0);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn invalidation_prevents_even_completed_hits_and_clears_owned_retention() {
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let source = source(128);
    let valid = Arc::new(AtomicBool::new(true));
    let check = Arc::clone(&valid);
    let cache = ScanSegmentReuse::new(source.clone(), memory.clone(), policy(1024, 1), move || {
        if check.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(vortex_err!("generation changed"))
        }
    })
    .unwrap();
    drop(runtime.block_on(cache.request(0.into())).unwrap());
    valid.store(false, Ordering::SeqCst);
    assert!(
        runtime
            .block_on(cache.request(0.into()))
            .unwrap_err()
            .to_string()
            .contains("generation changed")
    );
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.retention.reserved_bytes, 0);
    assert!(snapshot.closed);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    // Even if a test validator stops detecting the mutation, close stays latched.
    valid.store(true, Ordering::SeqCst);
    assert!(runtime.block_on(cache.request(0.into())).is_err());
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn empty_oversized_and_unavailable_shared_budget_bypass_without_owned_copy() {
    let runtime = CurrentThreadRuntime::new();
    for bytes in [0, 8192, 128] {
        let memory = LiveMemoryPool::new(16384).unwrap();
        let source = source(bytes);
        let cache = cache(source.clone(), &memory, policy(1024, 1));
        let table = memory.snapshot().reserved_bytes;
        let competing = memory.reserve(16384 - table).unwrap();
        for _ in 0..2 {
            assert_eq!(
                runtime
                    .block_on(cache.request(0.into()))
                    .unwrap()
                    .as_host()
                    .len(),
                bytes
            );
        }
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.snapshot().unwrap().retention.reserved_bytes, 0);
        assert_eq!(cache.snapshot().unwrap().counters.copied_bytes, 0);
        drop(competing);
        drop(cache);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn copying_a_slice_does_not_retain_the_unknown_large_backing_owner() {
    struct OpaqueOwner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for OpaqueOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for OpaqueOwner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let bytes = bytes::Bytes::from_owner(OpaqueOwner {
        bytes: vec![91; 65536],
        dropped: Arc::clone(&dropped),
    });
    let buffer = ByteBuffer::from(bytes).slice(128..256);
    let pending = Mutex::new(Some(buffer));
    let source = Arc::new(TestSource {
        calls: AtomicUsize::new(0),
        read: Box::new(move |_| {
            let buffer = pending.lock().unwrap().take().unwrap();
            async move { Ok(BufferHandle::new_host(buffer)) }.boxed()
        }),
    });
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(8192).unwrap();
    let cache = cache(source, &memory, policy(1024, 1));
    let result = runtime.block_on(cache.request(0.into())).unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(result.as_host().as_slice(), &[91; 128]);
    assert_eq!(cache.snapshot().unwrap().retention.reserved_bytes, 384);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 384);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn admission_root(fields: vortex::array::dtype::StructFields) -> vortex::layout::LayoutRef {
    use vortex::{
        array::dtype::Nullability,
        layout::{LayoutChildren, LayoutParts, LayoutRef},
    };
    #[derive(Clone)]
    struct UnvisitedChildren(usize);
    impl LayoutChildren for UnvisitedChildren {
        fn to_arc(&self) -> Arc<dyn LayoutChildren> {
            Arc::new(self.clone())
        }
        fn child(&self, _: usize, _: &DType) -> VortexResult<LayoutRef> {
            panic!("reuse admission must not realize any child layout")
        }
        fn child_row_count(&self, _: usize) -> u64 {
            1
        }
        fn nchildren(&self) -> usize {
            self.0
        }
    }
    let width = fields.nfields();
    LayoutParts::new(
        Struct,
        DType::Struct(fields, Nullability::NonNullable),
        1,
        Vec::new(),
        Arc::new(UnvisitedChildren(width)),
        (),
    )
    .into_layout()
}

#[test]
fn automatic_admission_requires_filter_only_scalar_fields_and_bounded_root_metadata() {
    use vortex::array::dtype::{Nullability, StructFields};
    fn leaf(name: &str) -> PredicateExpr {
        PredicateExpr::IsNotNull {
            column: ColumnRef::new(name).unwrap(),
        }
    }
    let fields = StructFields::from_iter(
        ["renamed", "left", "right"].map(|name| (name, DType::Utf8(Nullability::Nullable))),
    );
    let layout = admission_root(fields);
    let projected = [ColumnRef::new("right").unwrap()];
    let admit = |predicate: &PredicateExpr, memory| {
        SegmentReusePolicy::for_scan(predicate, &projected, layout.as_ref(), memory)
    };
    let single = leaf("renamed");
    assert!(admit(&single, 1 << 30).is_none());
    let independent = PredicateExpr::And(vec![leaf("left"), leaf("right")]);
    assert!(admit(&independent, 1 << 30).is_none());
    let duplicate = PredicateExpr::And(vec![
        leaf("renamed"),
        PredicateExpr::And(vec![leaf("renamed")]),
    ]);
    assert!(admit(&duplicate, (16 << 20) - 1).is_none());
    let admitted = admit(&duplicate, 1 << 30).unwrap();
    assert_eq!(admitted.max_retained_bytes, 64 << 20);
    assert_eq!(admitted.max_segment_bytes, 16 << 20);
    assert_eq!(admitted.max_entries, 128);
    let wide = PredicateExpr::And((0..65).map(|index| leaf(&format!("key-{index}"))).collect());
    assert!(admit(&wide, 1 << 30).is_none());
    // A repeated prefix cannot bypass the complete bounded traversal.
    let too_many_nodes = PredicateExpr::And(vec![
        duplicate.clone(),
        PredicateExpr::And(vec![leaf("renamed"); 128]),
    ]);
    assert!(admit(&too_many_nodes, 1 << 30).is_none());
    let mut deep = duplicate.clone();
    for _ in 0..129 {
        deep = PredicateExpr::And(vec![deep]);
    }
    assert!(admit(&deep, 1 << 30).is_none());
    let included = [ColumnRef::new("renamed").unwrap()];
    assert!(
        SegmentReusePolicy::for_scan(&duplicate, &included, layout.as_ref(), 1 << 30).is_none()
    );
    let unknown = PredicateExpr::And(vec![leaf("missing"), leaf("missing")]);
    assert!(admit(&unknown, 1 << 30).is_none());
    let invalid_projection = [ColumnRef::new("missing").unwrap()];
    assert!(
        SegmentReusePolicy::for_scan(&duplicate, &invalid_projection, layout.as_ref(), 1 << 30)
            .is_none()
    );
    let wide_projection = vec![projected[0].clone(); 65];
    assert!(
        SegmentReusePolicy::for_scan(&duplicate, &wide_projection, layout.as_ref(), 1 << 30)
            .is_none()
    );
}

#[test]
fn automatic_admission_rejects_flat_chunked_nested_and_oversized_schema_without_traversal() {
    use vortex::{
        array::dtype::{Nullability, StructFields},
        layout::{
            layout_children,
            layouts::{chunked::ChunkedLayout, flat::FlatLayout},
        },
        session::registry::ReadContext,
    };
    let column = ColumnRef::new("renamed").unwrap();
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::IsNotNull {
            column: column.clone(),
        },
        PredicateExpr::IsNull { column },
    ]);
    let fields = StructFields::from_iter([("renamed", DType::Utf8(Nullability::Nullable))]);
    let dtype = DType::Struct(fields.clone(), Nullability::NonNullable);
    let flat = FlatLayout::new(1, dtype.clone(), 0.into(), ReadContext::new([])).into_layout();
    let chunked = ChunkedLayout::new(1, dtype, layout_children(vec![flat.clone()])).into_layout();
    let nested = admission_root(StructFields::from_iter([(
        "renamed",
        DType::Struct(fields, Nullability::NonNullable),
    )]));
    let wide =
        admission_root(StructFields::from_iter((0..257).map(|index| {
            (format!("key-{index}"), DType::Utf8(Nullability::Nullable))
        })));
    for root in [flat, chunked, nested, wide] {
        assert!(SegmentReusePolicy::for_scan(&predicate, &[], root.as_ref(), 1 << 30).is_none());
    }
}

#[test]
fn optional_table_admission_pressure_skips_without_registering_a_source_read() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let held = memory.reserve(1024).unwrap();
    let source = source(128);
    let cache =
        ScanSegmentReuse::try_new(source.clone(), memory.clone(), policy(1024, 2), || Ok(()))
            .unwrap();
    assert!(cache.is_none());
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 1024);
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "vortex-write")]
#[path = "resident_segment_reuse_io_tests.rs"]
mod io_tests;
