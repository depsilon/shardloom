use super::*;
use std::sync::atomic::AtomicUsize;
use vortex::buffer::ByteBufferMut;
use vortex::layout::segments::SegmentFuture;

struct CountingFallback {
    requests: Arc<AtomicUsize>,
    payload: Vec<u8>,
}

impl SegmentSource for CountingFallback {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let requests = Arc::clone(&self.requests);
        let bytes = self.payload.clone();
        async move {
            requests.fetch_add(1, Ordering::SeqCst);
            let buf = ByteBufferMut::copy_from_aligned(&bytes, Alignment::new(1)).freeze();
            let _ = id;
            Ok(BufferHandle::new_host(buf))
        }
        .boxed()
    }
}

fn span(id: u32, offset: u64, length: u32) -> SegmentByteSpan {
    SegmentByteSpan {
        id: SegmentId::from(id),
        offset,
        length,
        alignment: Alignment::new(1),
    }
}

#[test]
fn coalescing_merges_within_gap_range_and_four_x_amplification_caps() {
    let policy = SegmentPrefetchPolicy {
        gap_limit_bytes: 128,
        coalesced_range_limit_bytes: 1024,
        read_amplification_numerator: 4,
        read_amplification_denominator: 1,
        prefetch_buffer_limit_bytes: 4096,
    };

    // Adjacent/nearby segments within 128B gap and <= 4x amplification coalesce.
    let ranges = plan_coalesced_byte_ranges(
        &[span(2, 200, 100), span(0, 0, 100), span(0, 0, 100)],
        &policy,
    );
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].start_offset, 0);
    assert_eq!(ranges[0].end_offset, 300);
    assert_eq!(ranges[0].useful_bytes, 200);
    assert_eq!(ranges[0].segments.len(), 2);

    // Gap > max_gap_bytes splits ranges.
    let split_gap = plan_coalesced_byte_ranges(&[span(0, 0, 100), span(1, 300, 100)], &policy);
    assert_eq!(split_gap.len(), 2);

    // Amplification > 4x splits even when gap <= max_gap_bytes.
    // Seg 0: [0..10] (useful 10). Seg 1: [120..130] -> gap=110 <= 128, span=130, useful=20 -> 130/20 = 6.5x > 4x!
    let split_amp = plan_coalesced_byte_ranges(&[span(0, 0, 10), span(1, 120, 10)], &policy);
    assert_eq!(split_amp.len(), 2);

    // Total range > max_coalesced_range_bytes splits ranges.
    let split_size = plan_coalesced_byte_ranges(&[span(0, 0, 600), span(1, 610, 600)], &policy);
    assert_eq!(split_size.len(), 2);
}

#[test]
fn bounded_prefetch_respects_tight_memory_cancellation_and_zero_close_leak() {
    let temp = std::env::temp_dir().join(format!(
        "shardloom-prefetch-unit-{}.bin",
        std::process::id()
    ));
    let mut data = vec![0_u8; 512];
    data[0..64].fill(0xAA);
    data[96..160].fill(0xBB);
    std::fs::write(&temp, &data).unwrap();

    let segment_map = vec![
        SegmentSpec {
            offset: 0,
            length: 64,
            alignment: Alignment::new(1),
        },
        SegmentSpec {
            offset: 96,
            length: 64,
            alignment: Alignment::new(1),
        },
    ];
    let fallback_calls = Arc::new(AtomicUsize::new(0));
    let fallback: Arc<dyn SegmentSource> = Arc::new(CountingFallback {
        requests: Arc::clone(&fallback_calls),
        payload: vec![0xCC; 64],
    });

    // 1. Normal bounded prefetch coalesces both segments into one read, hits prefetch on request, and frees memory on close().
    let pool = LiveMemoryPool::new(4096).unwrap();
    let token = CancellationToken::default();
    let source = BoundedPrefetchSegmentSource::new(
        Arc::clone(&fallback),
        temp.clone(),
        &segment_map,
        SegmentPrefetchPolicy::default(),
        Some(pool.clone()),
        token.clone(),
        #[cfg(unix)]
        None,
    );
    let guard = PrefetchScanGuard::new(Arc::clone(&source));
    let stats = source
        .prefetch_segments(&[SegmentId::from(0), SegmentId::from(1)])
        .unwrap();
    assert_eq!(stats.coalesced_reads, 1);
    assert_eq!(stats.prefetched_segments, 2);
    assert!(pool.snapshot().reserved_bytes >= 128);

    let rt = futures::executor::block_on(async {
        let b0 = source.request(SegmentId::from(0)).await.unwrap();
        let b1 = source.request(SegmentId::from(1)).await.unwrap();
        (b0, b1)
    });
    assert_eq!(rt.0.as_host().as_slice(), &[0xAA; 64]);
    assert_eq!(rt.1.as_host().as_slice(), &[0xBB; 64]);
    assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    assert_eq!(source.stats().prefetch_hits, 2);

    drop(guard);
    assert_eq!(pool.snapshot().reserved_bytes, 0);

    // 2. Tight memory falls back cleanly without OOM.
    let tight_pool = LiveMemoryPool::new(32).unwrap();
    let tight_source = BoundedPrefetchSegmentSource::new(
        Arc::clone(&fallback),
        temp.clone(),
        &segment_map,
        SegmentPrefetchPolicy::default(),
        Some(tight_pool.clone()),
        token,
        #[cfg(unix)]
        None,
    );
    let tight_stats = tight_source
        .prefetch_segments(&[SegmentId::from(0), SegmentId::from(1)])
        .unwrap();
    assert_eq!(tight_stats.prefetched_segments, 0);
    futures::executor::block_on(tight_source.request(SegmentId::from(0))).unwrap();
    assert_eq!(fallback_calls.load(Ordering::SeqCst), 1);
    tight_source.close();
    assert_eq!(tight_pool.snapshot().reserved_bytes, 0);

    // 3. Cancellation rejects prefetch and request deterministically.
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let cancel_source = BoundedPrefetchSegmentSource::new(
        fallback,
        temp.clone(),
        &segment_map,
        SegmentPrefetchPolicy::default(),
        Some(pool),
        cancelled,
        #[cfg(unix)]
        None,
    );
    assert!(
        cancel_source
            .prefetch_segments(&[SegmentId::from(0)])
            .is_err()
    );
    assert!(futures::executor::block_on(cancel_source.request(SegmentId::from(0))).is_err());

    let _ = std::fs::remove_file(&temp);
}

#[cfg(all(feature = "vortex-write", unix))]
fn write_wide_sort_prefetch_fixture(path: &std::path::Path) {
    use vortex::VortexSessionDefault as _;
    use vortex::array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        validity::Validity,
    };
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::{BlockingRuntime as _, single::SingleThreadRuntime};
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex::session::VortexSession;

    let _ = std::fs::remove_file(path);
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let rows_i64 = 32_i64;
    let rows = usize::try_from(rows_i64).unwrap();
    let array = StructArray::try_new(
        ["key", "c0", "c1", "c2", "c3", "c4", "c5", "c6"].into(),
        vec![
            PrimitiveArray::from_iter((0..rows_i64).rev()).into_array(),
            VarBinViewArray::from_iter_str((0..rows).map(|i| format!("payload-{i:03}")))
                .into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
            PrimitiveArray::from_iter(0..rows_i64).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut f = std::fs::File::create(path).unwrap();
    session
        .write_options()
        .with_strategy(Arc::new(FlatLayoutStrategy::default()))
        .blocking(&runtime)
        .write(&mut f, array.to_array_iterator())
        .unwrap();
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn wide_sort_late_materialization_uses_coalesced_prefetch_and_preserves_evidence() {
    use crate::{
        VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest, VortexSortRowsRequest,
        local_primitives::{
            execute_vortex_local_primitive_with_policy,
            materialize_local_vortex_sort_output_rows_by_source_ordinals,
        },
        resident_session::ResidentVortexSession,
    };
    use shardloom_core::{DatasetUri, StatValue};
    use shardloom_plan::ProjectionRequest;

    let temp = std::env::temp_dir().join(format!(
        "shardloom-prefetch-sort-{}.vortex",
        std::process::id()
    ));
    write_wide_sort_prefetch_fixture(&temp);

    let uri = DatasetUri::new(temp.display().to_string()).unwrap();
    let request = VortexQueryPrimitiveRequest::sort_rows(
        uri,
        ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", false)]),
        3,
    );
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 2).unwrap();
    let resident = ResidentVortexSession::new(32 << 20, 2).unwrap();
    let columns = vec!["key".to_string(), "c0".to_string(), "c6".to_string()];
    let materialized = resident
        .with_native_execution_context(&CancellationToken::default(), |context| {
            materialize_local_vortex_sort_output_rows_by_source_ordinals(
                &temp,
                &request,
                &columns,
                &[31, 30, 29],
                None,
                policy,
                context.native_session(),
                context.runtime(),
                Some(context),
            )
        })
        .unwrap();
    assert!(materialized.selected_row_materialization_used);
    assert_eq!(
        materialized.rows,
        vec![
            vec![
                StatValue::Int64(0),
                StatValue::Utf8("payload-031".into()),
                StatValue::Int64(31)
            ],
            vec![
                StatValue::Int64(1),
                StatValue::Utf8("payload-030".into()),
                StatValue::Int64(30)
            ],
            vec![
                StatValue::Int64(2),
                StatValue::Utf8("payload-029".into()),
                StatValue::Int64(29)
            ],
        ]
    );
    drop(materialized);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);

    let report = execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
    let summary = report.result_summary.as_deref().unwrap();
    let (_, json_str) = summary.split_once(" values=").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed["values"][0]["key"], serde_json::json!(0));
    assert_eq!(parsed["values"][1]["key"], serde_json::json!(1));
    assert_eq!(parsed["values"][2]["key"], serde_json::json!(2));
    let _ = std::fs::remove_file(&temp);
}
