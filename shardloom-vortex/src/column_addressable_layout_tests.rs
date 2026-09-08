use super::*;
use futures::FutureExt as _;
use std::{fs, path::PathBuf, sync::atomic::AtomicUsize};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            DictArray, PrimitiveArray, StructArray as NativeStructArray, VarBinViewArray,
            struct_::StructArrayExt as _,
        },
        dtype::{FieldNames, Nullability},
        iter::{ArrayIteratorAdapter, ArrayIteratorExt as _},
        scalar::Scalar,
        validity::Validity,
    },
    buffer::ByteBuffer,
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::{
        layouts::{
            flat::{FlatLayout, writer::FlatLayoutStrategy},
            struct_::{StructLayout, StructStrategy},
        },
        segments::{SegmentFuture, SegmentId, SegmentSink, SegmentSource},
        sequence::{SequenceId, SequentialArrayStreamExt as _},
    },
    session::registry::ReadContext,
};

use crate::vortex_ingest::bounded_ingest_layout as retained_baseline;

static NEXT: AtomicUsize = AtomicUsize::new(0);
const BASE: i64 = 1_i64 << 60;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-column-layout-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn encoded_batch(group: usize) -> ArrayRef {
    let domain = if group.is_multiple_of(2) {
        [Some("a-東京"), None, Some("z-λ")]
    } else {
        [Some("z-λ"), Some("a-東京"), None]
    };
    let text = DictArray::try_new(
        PrimitiveArray::new(vec![0_u8, 1, 0, 2], Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_nullable_str(domain).into_array(),
    )
    .unwrap()
    .into_array();
    let ids = PrimitiveArray::new(
        (0..4)
            .map(|row| BASE + i64::try_from(group * 4 + row).unwrap())
            .collect::<Vec<_>>(),
        Validity::NonNullable,
    )
    .into_array();
    NativeStructArray::try_new(
        FieldNames::from(["exact_identifier", "renamed_text"]),
        vec![ids, text],
        4,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn child_strategy() -> Arc<dyn LayoutStrategy> {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(
        FlatLayoutStrategy::default().with_max_variable_length_statistics_size(usize::MAX),
    );
    Arc::new(StructStrategy::new(Arc::clone(&flat), flat))
}

fn expected_text(row: usize) -> Option<&'static str> {
    match (row / 4 % 2, row % 4) {
        (0, 0 | 2) | (1, 1) => Some("a-東京"),
        (0, 3) | (1, 0 | 2) => Some("z-λ"),
        _ => None,
    }
}

fn assert_values(
    file: &vortex::file::VortexFile,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
) {
    let mut context = session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            let scalar = array.execute_scalar(row, &mut context).unwrap();
            let fields = scalar.as_struct();
            assert_eq!(
                fields.field("exact_identifier"),
                Some(Scalar::from(BASE + i64::try_from(seen).unwrap()))
            );
            let text = expected_text(seen).map_or_else(
                || Scalar::null(DType::Utf8(Nullability::Nullable)),
                |text| Scalar::utf8(text, Nullability::Nullable),
            );
            assert_eq!(fields.field("renamed_text"), Some(text));
            seen += 1;
        }
    }
    assert_eq!(seen, 12);
}

struct RequestedSegments {
    inner: Arc<dyn SegmentSource>,
    ids: Mutex<Vec<SegmentId>>,
}
impl SegmentSource for RequestedSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.ids.lock().unwrap().push(id);
        self.inner.request(id)
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the paired payload/footer/value proof together.
fn real_paired_files_keep_segment_geometry_payloads_dictionary_epochs_and_full_values() {
    let directory = Fixture::new();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut physical = Vec::new();
    for candidate in [false, true] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let candidate_strategy = candidate.then(|| {
            ColumnAddressableLayout::new(
                child_strategy(),
                memory.clone(),
                ColumnLayoutBounds::default(),
            )
            .unwrap()
        });
        let counters = candidate_strategy
            .as_ref()
            .map(ColumnAddressableLayout::counters);
        let strategy: Arc<dyn LayoutStrategy> = candidate_strategy.map_or_else(
            || {
                Arc::new(retained_baseline::BoundedIngestLayout::new(
                    child_strategy(),
                    0,
                    memory.reserve(0).unwrap(),
                )) as Arc<dyn LayoutStrategy>
            },
            |strategy| Arc::new(strategy) as Arc<dyn LayoutStrategy>,
        );
        let mut batches = (0..3).map(encoded_batch).collect::<Vec<_>>();
        batches.insert(1, batches[0].slice(0..0).unwrap());
        let dtype = batches[0].dtype().clone();
        let path = directory.0.join(format!("candidate-{candidate}.vortex"));
        let summary = session
            .write_options()
            .with_file_statistics(Vec::new())
            .with_strategy(strategy)
            .blocking(&runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(dtype.clone(), batches.into_iter().map(Ok)),
            )
            .unwrap();
        assert_eq!(summary.row_count(), 12);
        assert!(
            memory.snapshot().reserved_bytes > 0,
            "returned footer owns references after strategy drop"
        );
        fs::File::open(&path).unwrap().sync_all().unwrap();
        let bytes = fs::read(&path).unwrap();
        let file = runtime
            .block_on(session.open_options().open_path(&path))
            .unwrap();
        assert_eq!(file.dtype(), &dtype);
        assert_values(&file, &session, &runtime);
        let segments = file
            .footer()
            .segment_map()
            .iter()
            .map(|spec| {
                let start = usize::try_from(spec.offset).unwrap();
                (
                    spec.offset,
                    spec.length,
                    *spec.alignment,
                    bytes[start..start + spec.length as usize].to_vec(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(segments.len(), 6);
        physical.push(segments);
        if candidate {
            assert!(file.footer().layout().is::<Struct>());
            let identifiers = file.footer().layout().slot(1).unwrap().unwrap();
            assert_eq!(identifiers.nchildren(), 3);
            let requested_id = identifiers.slot(1).unwrap().unwrap().segment_ids()[0];
            let requested = Arc::new(RequestedSegments {
                inner: file.segment_source(),
                ids: Mutex::new(Vec::new()),
            });
            let projected = file.clone().with_segment_source(requested.clone());
            let projection = vortex::expr::select(["exact_identifier"], vortex::expr::root())
                .bind(&dtype)
                .unwrap();
            let mut context = session.create_execution_ctx();
            let mut seen = 0;
            for array in projected
                .scan()
                .unwrap()
                .with_projection(projection)
                .with_row_range(4..8)
                .with_ordered(true)
                .into_array_iter(&runtime)
                .unwrap()
            {
                let array = array.unwrap();
                for row in 0..array.len() {
                    assert_eq!(
                        array
                            .execute_scalar(row, &mut context)
                            .unwrap()
                            .as_struct()
                            .field("exact_identifier"),
                        Some(Scalar::from(BASE + 4 + seen))
                    );
                    seen += 1;
                }
            }
            assert_eq!(seen, 4);
            let ids = requested.ids.lock().unwrap();
            assert!(!ids.is_empty());
            assert!(ids.iter().all(|id| *id == requested_id));
            let snapshot = counters.unwrap().snapshot();
            assert_eq!(snapshot.input_groups, 3);
            // Native file writer filters empty chunks before strategy dispatch.
            // The direct strategy test below separately exercises empty intake.
            assert_eq!(snapshot.empty_groups, 0);
            assert_eq!(snapshot.child_writer_calls, 3);
            assert_eq!(snapshot.transposed_references, 6);
            assert!(snapshot.peak_reference_bytes > 0);
        } else {
            assert!(
                file.footer()
                    .layout()
                    .is::<vortex::layout::layouts::chunked::Chunked>()
            );
        }
        drop(summary);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(
        physical[0], physical[1],
        "only the footer hierarchy may differ; segment offsets/alignments/bytes must match"
    );
}

#[test]
fn default_file_statistics_preserve_exact_global_bounds_nulls_and_schema_after_reopen() {
    use vortex::expr::stats::Stat;
    let directory = Fixture::new();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut compared = Vec::new();
    for candidate in [false, true] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let strategy: Arc<dyn LayoutStrategy> = if candidate {
            Arc::new(
                ColumnAddressableLayout::new(
                    child_strategy(),
                    memory.clone(),
                    ColumnLayoutBounds::default(),
                )
                .unwrap(),
            )
        } else {
            Arc::new(retained_baseline::BoundedIngestLayout::new(
                child_strategy(),
                0,
                memory.reserve(0).unwrap(),
            ))
        };
        // Fresh arrays for each writer: neither reuses the other's computed stats.
        let batches = (0..3).map(encoded_batch).collect::<Vec<_>>();
        let dtype = batches[0].dtype().clone();
        let path = directory
            .0
            .join(format!("default-stats-{candidate}.vortex"));
        let summary = session
            .write_options()
            .with_strategy(strategy)
            .blocking(&runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(dtype.clone(), batches.into_iter().map(Ok)),
            )
            .unwrap();
        let file = runtime
            .block_on(session.open_options().open_path(&path))
            .unwrap();
        assert_eq!(file.dtype(), &dtype);
        assert_eq!(file.row_count(), 12);
        assert_values(&file, &session, &runtime);
        let statistics = file.footer().statistics().unwrap();
        assert_eq!(statistics.stats_sets().len(), 2);
        let expected = [
            (Scalar::from(BASE), Scalar::from(BASE + 11), 0_u64),
            (Scalar::from("a-東京"), Scalar::from("z-λ"), 3_u64),
        ];
        for (index, (minimum, maximum, nulls)) in expected.into_iter().enumerate() {
            let (stats, field_dtype) = statistics.get(index);
            assert_eq!(
                field_dtype,
                &dtype
                    .as_struct_fields_opt()
                    .unwrap()
                    .fields()
                    .nth(index)
                    .unwrap()
            );
            assert_eq!(stats.get(Stat::Min).as_exact(), minimum.into_value());
            assert_eq!(stats.get(Stat::Max).as_exact(), maximum.into_value());
            assert_eq!(
                stats.get(Stat::NullCount).as_exact(),
                Scalar::from(nulls).into_value()
            );
        }
        compared.push((
            statistics.dtypes().to_vec(),
            statistics
                .stats_sets()
                .iter()
                .map(|stats| stats.iter().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        ));
        drop(summary);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
    assert_eq!(
        compared[0], compared[1],
        "all default statistics and precision must agree"
    );
}

#[test]
fn zero_rows_preserve_schema_without_child_calls_or_data_segments() {
    let directory = Fixture::new();
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for include_empty in [false, true] {
        let memory = LiveMemoryPool::new(65536).unwrap();
        let strategy = ColumnAddressableLayout::new(
            child_strategy(),
            memory.clone(),
            ColumnLayoutBounds::default(),
        )
        .unwrap();
        let counters = strategy.counters();
        let array = encoded_batch(0).slice(0..0).unwrap();
        let dtype = array.dtype().clone();
        let arrays = if include_empty {
            vec![Ok(array)]
        } else {
            Vec::new()
        };
        let path = directory.0.join(format!("empty-{include_empty}.vortex"));
        let summary = session
            .write_options()
            .with_file_statistics(Vec::new())
            .with_strategy(Arc::new(strategy))
            .blocking(&runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(dtype.clone(), arrays.into_iter()),
            )
            .unwrap();
        assert_eq!(summary.row_count(), 0);
        let file = runtime
            .block_on(session.open_options().open_path(&path))
            .unwrap();
        assert_eq!(file.dtype(), &dtype);
        assert_eq!(file.row_count(), 0);
        assert!(file.footer().segment_map().is_empty());
        assert_eq!(
            file.scan()
                .unwrap()
                .into_array_iter(&runtime)
                .unwrap()
                .map(|array| array.unwrap().len())
                .sum::<usize>(),
            0
        );
        assert_eq!(counters.snapshot().child_writer_calls, 0);
        drop(summary);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

struct NoWriteSink;
impl SegmentSink for NoWriteSink {
    fn write<'a, 'future>(
        &'a self,
        _sequence: SequenceId,
        _buffers: Vec<ByteBuffer>,
    ) -> BoxFuture<'future, VortexResult<SegmentId>>
    where
        'a: 'future,
        Self: 'future,
    {
        async { Err(vortex_err!("test forbids segment writes")) }.boxed()
    }
}

struct ControlledChild {
    calls: AtomicUsize,
    pending_second: bool,
    invalid_second: bool,
}
impl LayoutStrategy for ControlledChild {
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
            let (_, array) = input.next().await.unwrap()?;
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if call == 1 && self.pending_second {
                return futures::future::pending().await;
            }
            if call == 1 && self.invalid_second {
                return Ok(FlatLayout::new(
                    array.len() as u64,
                    array.dtype().clone(),
                    0.into(),
                    ReadContext::new([]),
                )
                .into_layout());
            }
            let fields = array
                .dtype()
                .as_struct_fields_opt()
                .unwrap()
                .fields()
                .enumerate()
                .map(|(index, dtype)| {
                    FlatLayout::new(
                        array.len() as u64,
                        dtype,
                        SegmentId::try_from(index).unwrap(),
                        ReadContext::new([]),
                    )
                    .into_layout()
                })
                .collect();
            Ok(StructLayout::new(array.len() as u64, array.dtype().clone(), fields).into_layout())
        })
    }
}

fn controlled(pending: bool, invalid: bool) -> Arc<ControlledChild> {
    Arc::new(ControlledChild {
        calls: AtomicUsize::new(0),
        pending_second: pending,
        invalid_second: invalid,
    })
}

#[test]
fn cancellation_and_incompatible_second_child_release_all_reference_owners() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    for pending in [false, true] {
        let memory = LiveMemoryPool::new(65536).unwrap();
        let child = controlled(pending, !pending);
        let strategy = ColumnAddressableLayout::new(
            child.clone(),
            memory.clone(),
            ColumnLayoutBounds::default(),
        )
        .unwrap();
        let (pointer, eof) = SequenceId::root().split();
        let arrays = vec![encoded_batch(0), encoded_batch(1)];
        let dtype = arrays[0].dtype().clone();
        let mut future = strategy.write_stream(
            LayoutWriterContext::new(ArrayContext::new(Vec::new())),
            Arc::new(NoWriteSink),
            ArrayIteratorAdapter::new(dtype, arrays.into_iter().map(Ok))
                .into_array_stream()
                .sequenced(pointer),
            eof,
            &session,
        );
        if pending {
            runtime.block_on(async {
                assert!(futures::poll!(&mut future).is_pending());
            });
            assert_eq!(child.calls.load(Ordering::Relaxed), 2);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(future);
        } else {
            let error = runtime.block_on(future).unwrap_err();
            assert!(error.to_string().contains("incompatible Struct root"));
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn admission_and_reference_pressure_fail_before_child_side_effects() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let array = encoded_batch(0);
    let nullable = NativeStructArray::try_new(
        FieldNames::from(["exact_identifier", "renamed_text"]),
        array
            .as_opt::<StructArray>()
            .unwrap()
            .iter_unmasked_fields()
            .cloned()
            .collect::<Vec<_>>(),
        4,
        Validity::AllValid,
    )
    .unwrap()
    .into_array();
    for (array, bytes, bounds) in [
        (nullable, 65536, ColumnLayoutBounds::default()),
        (array.clone(), 1, ColumnLayoutBounds::default()),
        (
            array,
            65536,
            ColumnLayoutBounds {
                max_columns: 2,
                max_row_groups: 2,
                max_rows_per_group: 3,
            },
        ),
    ] {
        let memory = LiveMemoryPool::new(bytes).unwrap();
        let child = controlled(false, false);
        let strategy = ColumnAddressableLayout::new(child.clone(), memory.clone(), bounds).unwrap();
        let (pointer, eof) = SequenceId::root().split();
        assert!(
            runtime
                .block_on(strategy.write_stream(
                    LayoutWriterContext::new(ArrayContext::new(Vec::new())),
                    Arc::new(NoWriteSink),
                    array.to_array_stream().sequenced(pointer),
                    eof,
                    &session
                ))
                .is_err()
        );
        assert_eq!(child.calls.load(Ordering::Relaxed), 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn actual_nonempty_group_limit_and_child_field_mismatches_are_explicit() {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let memory = LiveMemoryPool::new(65536).unwrap();
    let child = controlled(false, false);
    let strategy = ColumnAddressableLayout::new(
        child.clone(),
        memory.clone(),
        ColumnLayoutBounds {
            max_row_groups: 2,
            ..ColumnLayoutBounds::default()
        },
    )
    .unwrap();
    let counters = strategy.counters();
    let arrays = vec![
        encoded_batch(0),
        encoded_batch(0).slice(0..0).unwrap(),
        encoded_batch(1),
        encoded_batch(2),
    ];
    let dtype = arrays[0].dtype().clone();
    let (pointer, eof) = SequenceId::root().split();
    let error = runtime
        .block_on(
            strategy.write_stream(
                LayoutWriterContext::new(ArrayContext::new(Vec::new())),
                Arc::new(NoWriteSink),
                ArrayIteratorAdapter::new(dtype.clone(), arrays.into_iter().map(Ok))
                    .into_array_stream()
                    .sequenced(pointer),
                eof,
                &session,
            ),
        )
        .unwrap_err();
    assert!(error.to_string().contains("actual row-group/row admission"));
    assert_eq!(child.calls.load(Ordering::Relaxed), 2);
    assert_eq!(counters.snapshot().empty_groups, 1);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for wrong_dtype in [false, true] {
        let mut matrix =
            ReferenceMatrix::new(2, &memory, Arc::new(ColumnLayoutCounters::default())).unwrap();
        matrix.admit_next_group().unwrap();
        let fields = dtype.as_struct_fields_opt().unwrap();
        let first = FlatLayout::new(
            4,
            fields.field_by_index(0).unwrap(),
            0.into(),
            ReadContext::new([]),
        )
        .into_layout();
        let second_dtype = if wrong_dtype {
            DType::Bool(Nullability::Nullable)
        } else {
            fields.field_by_index(1).unwrap()
        };
        let second = FlatLayout::new(
            if wrong_dtype { 4 } else { 5 },
            second_dtype,
            1.into(),
            ReadContext::new([]),
        )
        .into_layout();
        let invalid = StructLayout::new(4, dtype.clone(), vec![first, second]).into_layout();
        let error = matrix.accept(invalid, &dtype, 4).unwrap_err().to_string();
        // Native OwnedLayoutChildren rejects a wrong dtype during slot access;
        // our wrapper checks the admitted row count after that successful access.
        let expected = if wrong_dtype {
            "Child dtype mismatch"
        } else {
            "column-addressable child field dtype/row count mismatch"
        };
        assert!(error.contains(expected), "unexpected diagnostic: {error}");
        assert_eq!(matrix.counters.snapshot().transposed_references, 0);
        drop(matrix);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn root_children_clones_share_vectors_and_hold_credits_through_footer_lifetime() {
    let memory = LiveMemoryPool::new(65536).unwrap();
    let counters = Arc::new(ColumnLayoutCounters::default());
    let mut matrix = ReferenceMatrix::new(2, &memory, counters).unwrap();
    matrix.admit_next_group().unwrap();
    let array = encoded_batch(0);
    let dtype = array.dtype().clone();
    let children = dtype
        .as_struct_fields_opt()
        .unwrap()
        .fields()
        .enumerate()
        .map(|(index, dtype)| {
            FlatLayout::new(
                4,
                dtype,
                SegmentId::try_from(index).unwrap(),
                ReadContext::new([]),
            )
            .into_layout()
        })
        .collect();
    matrix
        .accept(
            StructLayout::new(4, dtype.clone(), children).into_layout(),
            &dtype,
            4,
        )
        .unwrap();
    let root = matrix.into_layout(dtype, 4);
    let children = root.as_opt::<Struct>().unwrap().children().clone();
    let cloned = children.as_ref().to_arc();
    let first = children
        .child(
            0,
            &root
                .dtype()
                .as_struct_fields_opt()
                .unwrap()
                .field_by_index(0)
                .unwrap(),
        )
        .unwrap();
    let count = Arc::strong_count(&first);
    let again = cloned.as_ref().to_arc();
    assert_eq!(
        Arc::strong_count(&first),
        count,
        "cloning child containers must not clone the reference vector"
    );
    drop(root);
    drop(children);
    drop(cloned);
    drop(first);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(again);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
