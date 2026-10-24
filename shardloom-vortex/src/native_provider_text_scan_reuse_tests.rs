//! Count real provider allocations at the file filter/projection boundary.

use std::sync::{Arc, Mutex};

use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, StatValue};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            Dict, DictArray, PrimitiveArray, StructArray, VarBinViewArray,
            struct_::StructArrayExt as _, varbinview::BinaryView,
        },
        dtype::Nullability,
        memory::{HostAllocator, HostMemoryReservation, MemorySessionExt as _, WritableHostBuffer},
        validity::Validity,
    },
    buffer::{Alignment, ByteBuffer},
    encodings::zstd::Zstd,
    error::VortexResult,
    expr::{col, lit, not_eq, root, select},
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

#[derive(Debug)]
struct Recorder {
    inner: crate::owned_buffers::ReservedHostAllocator,
    allocations: Mutex<Vec<(usize, Alignment)>>,
}

impl HostAllocator for Recorder {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.allocations.lock().unwrap().push((len, alignment));
        self.inner.allocate(len, alignment)
    }

    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(len)
    }
}

fn fixture() -> (ByteBuffer, Vec<String>) {
    let values = (0..128)
        .map(|row| format!("{row:04}:{}:λ", "native-text".repeat(32)))
        .collect::<Vec<_>>();
    let plain = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let encoded = Zstd::from_var_bin_view_without_dict(
        &plain,
        0,
        128,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let array = StructArray::new(
        ["text"].into(),
        vec![encoded],
        values.len(),
        Validity::NonNullable,
    )
    .into_array();
    (write_array(&array), values)
}

fn write_array(array: &ArrayRef) -> ByteBuffer {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    runtime.block_on(session.write_options()
        .with_strategy(crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(1))
        .with_file_statistics(Vec::new())
        .write(&mut bytes, array.to_array_stream())).unwrap();
    ByteBuffer::from(bytes)
}

fn reuse_scan(
    file: &vortex::file::VortexFile,
    memory: &LiveMemoryPool,
    predicate: &PredicateExpr,
    cancellation: Option<&shardloom_exec::compute_pool::CancellationToken>,
) -> (
    vortex::layout::scan::scan_builder::ScanBuilder<vortex::array::ArrayRef>,
    Arc<super::Stats>,
) {
    reuse_columns(file, memory, predicate, &["text"], cancellation)
}

fn reuse_columns(
    file: &vortex::file::VortexFile,
    memory: &LiveMemoryPool,
    predicate: &PredicateExpr,
    columns: &[&str],
    cancellation: Option<&shardloom_exec::compute_pool::CancellationToken>,
) -> (
    vortex::layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    Arc<super::Stats>,
) {
    let kind = crate::VortexQueryPrimitiveKind::SimpleAggregate;
    let mut plan = super::super::projection_scan_plan(
        file.dtype(),
        &shardloom_plan::ProjectionRequest::columns(
            columns
                .iter()
                .map(|name| ColumnRef::new(*name).unwrap())
                .collect(),
        ),
        kind,
    )
    .unwrap();
    plan.filter =
        Some(super::super::predicate_to_vortex_expr(predicate, file.dtype(), kind).unwrap());
    let (scan, stats) = super::admit(file, &plan, Some(predicate), Some(memory), cancellation)
        .unwrap()
        .unwrap();
    (
        scan.with_filter(
            plan.filter
                .as_ref()
                .unwrap()
                .optimize_recursive(file.dtype())
                .unwrap()
                .bind(file.dtype())
                .unwrap(),
        )
        .with_projection(
            plan.projection
                .as_ref()
                .unwrap()
                .optimize_recursive(file.dtype())
                .unwrap()
                .bind(file.dtype())
                .unwrap(),
        ),
        stats,
    )
}

fn predicate() -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("text").unwrap(),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8(String::new()),
    }
}

fn consume(
    bytes: &ByteBuffer,
    expected: &[String],
    filtered: bool,
    reused: bool,
) -> Vec<(usize, Alignment)> {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let recorder = Arc::new(Recorder {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        allocations: Mutex::new(Vec::new()),
    });
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default()
        .with_allocator(recorder.clone())
        .with_handle(runtime.handle());
    crate::native_provider_memory::install(&session, memory.clone());
    let file = session.open_options().open_buffer(bytes.clone()).unwrap();
    let (mut scan, stats) = if reused {
        let (scan, stats) = reuse_scan(&file, &memory, &predicate(), None);
        (scan, Some(stats))
    } else {
        (file.scan().unwrap(), None)
    };
    scan = scan.with_projection(
        select(["text"], root())
            .optimize_recursive(file.dtype())
            .unwrap()
            .bind(file.dtype())
            .unwrap(),
    );
    if filtered {
        scan = scan.with_filter(
            not_eq(col("text"), lit(""))
                .optimize_recursive(file.dtype())
                .unwrap()
                .bind(file.dtype())
                .unwrap(),
        );
    }
    recorder.allocations.lock().unwrap().clear();
    let mut rows = 0;
    for array in scan.with_concurrency(1).into_array_iter(&runtime).unwrap() {
        let fields = array
            .unwrap()
            .execute::<StructArray>(&mut session.create_execution_ctx())
            .unwrap();
        let text = fields
            .unmasked_field_by_name("text")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
            .unwrap();
        assert_eq!(
            text.dtype(),
            &vortex::array::dtype::DType::Utf8(Nullability::NonNullable)
        );
        for row in 0..text.len() {
            assert_eq!(text.bytes_at(row).as_slice(), expected[rows].as_bytes());
            rows += 1;
        }
    }
    assert_eq!(rows, expected.len());
    if let Some(stats) = stats {
        assert_eq!(
            stats
                .decoded_chunks
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            stats
                .projected_reuses
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    recorder.allocations.lock().unwrap().clone()
}

#[test]
fn native_text_scan_baseline_decodes_same_file_range_for_filter_and_projection() {
    let (bytes, values) = fixture();
    let views = (
        values.len() * size_of::<BinaryView>(),
        Alignment::of::<BinaryView>(),
    );
    let unfiltered = consume(&bytes, &values, false, false);
    let filtered = consume(&bytes, &values, true, false);
    assert_eq!(
        unfiltered.iter().filter(|entry| **entry == views).count(),
        1,
        "{unfiltered:?}"
    );
    assert_eq!(
        filtered.iter().filter(|entry| **entry == views).count(),
        2,
        "{filtered:?}"
    );
}

#[test]
fn native_text_scan_reuse_decodes_once_and_returns_all_projected_values() {
    let (bytes, values) = fixture();
    let allocations = consume(&bytes, &values, true, true);
    let views = (
        values.len() * size_of::<BinaryView>(),
        Alignment::of::<BinaryView>(),
    );
    assert_eq!(
        allocations.iter().filter(|entry| **entry == views).count(),
        1,
        "{allocations:?}"
    );
}

fn session(memory: &LiveMemoryPool, runtime: &CurrentThreadRuntime) -> VortexSession {
    let session = VortexSession::default()
        .with_handle(runtime.handle())
        .with_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
            memory.clone(),
        )));
    crate::native_provider_memory::install(&session, memory.clone());
    session
}

#[test]
fn native_text_scan_reuse_preserves_nulls_unicode_slices_and_declared_column_order() {
    let values = [
        None,
        Some(""),
        Some("tiny"),
        Some("東京の長い値"),
        Some("nul\0value"),
        Some(""),
        Some("more than twelve bytes here"),
        None,
    ];
    let plain = VarBinViewArray::from_iter_nullable_str(values);
    let encoded = Zstd::from_var_bin_view_without_dict(
        &plain,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let bytes = write_array(
        &StructArray::new(
            ["metric", "text"].into(),
            vec![PrimitiveArray::from_iter(0_u64..8).into_array(), encoded],
            8,
            Validity::NonNullable,
        )
        .into_array(),
    );
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    for (op, literal) in [
        (ComparisonOp::Eq, ""),
        (ComparisonOp::NotEq, ""),
        (ComparisonOp::Lt, "tiny"),
        (ComparisonOp::GtEq, "東京"),
        (ComparisonOp::Eq, "missing"),
    ] {
        let predicate = PredicateExpr::Compare {
            column: ColumnRef::new("text").unwrap(),
            op,
            value: StatValue::Utf8(literal.into()),
        };
        let (scan, _) = reuse_columns(&file, &memory, &predicate, &["text", "metric"], None);
        let mut actual = Vec::new();
        for array in scan
            .with_row_range(1..7)
            .with_concurrency(2)
            .into_array_iter(&runtime)
            .unwrap()
        {
            let array = array.unwrap();
            assert_eq!(
                array.dtype().as_struct_fields_opt().unwrap().names(),
                &vortex::array::dtype::FieldNames::from(["text", "metric"])
            );
            let fields = array
                .execute::<StructArray>(&mut session.create_execution_ctx())
                .unwrap();
            let text = fields
                .unmasked_field(0)
                .clone()
                .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
                .unwrap();
            let metric = fields
                .unmasked_field(1)
                .clone()
                .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
                .unwrap();
            for (row, index) in metric.as_slice::<u64>().iter().copied().enumerate() {
                actual.push((
                    index,
                    String::from_utf8(text.bytes_at(row).to_vec()).unwrap(),
                ));
            }
        }
        let expected = (1..7)
            .filter_map(|index| values[index].map(|value| (index, value)))
            .filter(|(_, value)| match op {
                ComparisonOp::Eq => *value == literal,
                ComparisonOp::NotEq => *value != literal,
                ComparisonOp::Lt => *value < literal,
                ComparisonOp::GtEq => *value >= literal,
                _ => unreachable!(),
            })
            .map(|(index, value)| (u64::try_from(index).unwrap(), value.to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_text_scan_reuse_preserves_dictionary_encoding_and_unpolled_cleanup() {
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([0_u8, 1, 0, 1]).into_array(),
        VarBinViewArray::from_iter_str(["alpha", "東京の長い値"]).into_array(),
    )
    .unwrap()
    .into_array();
    let bytes = write_array(
        &StructArray::new(["text"].into(), vec![dictionary], 4, Validity::NonNullable).into_array(),
    );
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    let before = memory.snapshot().reserved_bytes;
    let (scan, _) = reuse_scan(&file, &memory, &predicate(), None);
    let prepared = scan.prepare().unwrap();
    let tasks = prepared.execute(None).unwrap();
    assert!(memory.snapshot().reserved_bytes > before);
    drop(tasks);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, before);
    let (scan, stats) = reuse_scan(&file, &memory, &predicate(), None);
    let mut rows = 0;
    for array in scan.into_array_iter(&runtime).unwrap() {
        let array = array.unwrap();
        assert!(array.depth_first_traversal().any(|node| node.is::<Dict>()));
        rows += array.len();
    }
    assert_eq!(rows, 4);
    assert_eq!(
        stats
            .decoded_chunks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats
            .projected_reuses
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct FaultSource {
    inner: Arc<dyn vortex::layout::segments::SegmentSource>,
    cancellation: Option<shardloom_exec::compute_pool::CancellationToken>,
}

impl vortex::layout::segments::SegmentSource for FaultSource {
    fn request(
        &self,
        id: vortex::layout::segments::SegmentId,
    ) -> vortex::layout::segments::SegmentFuture {
        if let Some(token) = &self.cancellation {
            token.cancel();
            self.inner.request(id)
        } else {
            Box::pin(async {
                Err(vortex::error::vortex_err!(
                    "injected text source corruption"
                ))
            })
        }
    }
}

#[test]
fn native_text_scan_reuse_releases_entries_on_source_error_and_cancellation() {
    let (bytes, _) = fixture();
    for (cancel, compound) in [(false, false), (true, false), (false, true), (true, true)] {
        let runtime = CurrentThreadRuntime::new();
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let session = session(&memory, &runtime);
        let token = shardloom_exec::compute_pool::CancellationToken::default();
        let file = session.open_options().open_buffer(bytes.clone()).unwrap();
        let source = Arc::new(FaultSource {
            inner: file.segment_source(),
            cancellation: cancel.then(|| token.clone()),
        });
        let file = file.with_segment_source(source);
        let predicate = if compound {
            PredicateExpr::And(vec![
                PredicateExpr::StringContains {
                    column: ColumnRef::new("text").unwrap(),
                    needle: "native-text".into(),
                    negated: false,
                },
                predicate(),
            ])
        } else {
            predicate()
        };
        let (scan, _) = reuse_scan(&file, &memory, &predicate, Some(&token));
        let error = scan
            .into_array_iter(&runtime)
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();
        if cancel {
            assert!(error.to_string().contains("cancel"), "{error}");
        } else {
            assert!(
                error
                    .to_string()
                    .contains("injected text source corruption"),
                "{error}"
            );
        }
        drop(file);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_text_scan_reuse_registry_denial_duplicate_and_last_owner_release() {
    let memory =
        LiveMemoryPool::new(super::REGISTRY_METADATA_BYTES + super::ENTRY_METADATA_BYTES).unwrap();
    let registry = Arc::new(super::Registry {
        entries: Mutex::new(std::collections::BTreeMap::new()),
        memory: memory.clone(),
        _credit: crate::owned_buffers::reserve(&memory, super::REGISTRY_METADATA_BYTES).unwrap(),
    });
    let entry = registry.insert(&(0..4)).unwrap();
    assert!(registry.insert(&(0..4)).is_err());
    let error = registry.insert(&(4..8)).err().unwrap();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    let second_owner = registry.get(&(0..4)).unwrap().unwrap();
    drop(entry);
    assert_eq!(registry.entries.lock().unwrap().len(), 1);
    drop(second_owner);
    assert!(registry.entries.lock().unwrap().is_empty());
    assert_eq!(
        memory.snapshot().reserved_bytes,
        super::REGISTRY_METADATA_BYTES
    );
    drop(registry);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_text_scan_reuse_partial_input_mask_keeps_native_selection() {
    let (bytes, values) = fixture();
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    let (scan, stats) = reuse_scan(&file, &memory, &predicate(), None);
    let excluded = vortex::scan::strict_sorted_buffer::StrictSortedBuffer::try_new(
        vortex::buffer::Buffer::from_iter([3_u64, 5]),
    )
    .unwrap();
    let scan = scan.with_selection(vortex::scan::selection::Selection::ExcludeByIndex(excluded));
    let mut actual = Vec::new();
    for array in scan.into_array_iter(&runtime).unwrap() {
        let fields = array
            .unwrap()
            .execute::<StructArray>(&mut session.create_execution_ctx())
            .unwrap();
        let text = fields
            .unmasked_field(0)
            .clone()
            .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
            .unwrap();
        for row in 0..text.len() {
            actual.push(String::from_utf8(text.bytes_at(row).to_vec()).unwrap());
        }
    }
    assert_eq!(
        actual,
        values
            .into_iter()
            .enumerate()
            .filter(|(row, _)| ![3, 5].contains(row))
            .map(|(_, value)| value)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        stats
            .decoded_chunks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats
            .projected_reuses
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats
            .sparse_masks
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn text_and_metric(
    scan: vortex::layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
) -> Vec<(u64, String)> {
    let mut rows = Vec::new();
    for array in scan.with_concurrency(1).into_array_iter(runtime).unwrap() {
        let fields = array
            .unwrap()
            .execute::<StructArray>(&mut session.create_execution_ctx())
            .unwrap();
        let text = fields
            .unmasked_field_by_name("text")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
            .unwrap();
        let metric = fields
            .unmasked_field_by_name("metric")
            .unwrap()
            .clone()
            .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
            .unwrap();
        for (row, index) in metric.as_slice::<u64>().iter().copied().enumerate() {
            rows.push((
                index,
                String::from_utf8(text.bytes_at(row).to_vec()).unwrap(),
            ));
        }
    }
    rows
}

#[test]
fn native_text_scan_conjunction_reuses_decode_after_later_mask_narrowing() {
    let values = (0..128)
        .map(|row| format!("{row:04}:{}:東京", "native-text".repeat(32)))
        .collect::<Vec<_>>();
    let plain = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let encoded = Zstd::from_var_bin_view_without_dict(
        &plain,
        0,
        128,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let bytes = write_array(
        &StructArray::new(
            ["metric", "text"].into(),
            vec![PrimitiveArray::from_iter(0_u64..128).into_array(), encoded],
            128,
            Validity::NonNullable,
        )
        .into_array(),
    );
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::StringContains {
            column: ColumnRef::new("text").unwrap(),
            needle: "native-text".into(),
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("metric").unwrap(),
            op: ComparisonOp::Lt,
            value: StatValue::UInt64(64),
        },
    ]);
    for reused in [false, true] {
        let runtime = CurrentThreadRuntime::new();
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let recorder = Arc::new(Recorder {
            inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
            allocations: Mutex::new(Vec::new()),
        });
        let session = VortexSession::default()
            .with_allocator(recorder.clone())
            .with_handle(runtime.handle());
        crate::native_provider_memory::install(&session, memory.clone());
        let file = session.open_options().open_buffer(bytes.clone()).unwrap();
        let (scan, stats) = if reused {
            let (scan, stats) =
                reuse_columns(&file, &memory, &predicate, &["text", "metric"], None);
            (scan, Some(stats))
        } else {
            let filter = super::super::predicate_to_vortex_expr(
                &predicate,
                file.dtype(),
                crate::VortexQueryPrimitiveKind::SimpleAggregate,
            )
            .unwrap()
            .optimize_recursive(file.dtype())
            .unwrap()
            .bind(file.dtype())
            .unwrap();
            (
                file.scan().unwrap().with_filter(filter).with_projection(
                    select(["text", "metric"], root())
                        .bind(file.dtype())
                        .unwrap(),
                ),
                None,
            )
        };
        recorder.allocations.lock().unwrap().clear();
        assert_eq!(
            text_and_metric(scan, &session, &runtime),
            values
                .iter()
                .take(64)
                .enumerate()
                .map(|(row, value)| (row as u64, value.clone()))
                .collect::<Vec<_>>()
        );
        let views = (128 * size_of::<BinaryView>(), Alignment::of::<BinaryView>());
        assert_eq!(
            recorder
                .allocations
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| **entry == views)
                .count(),
            if reused { 1 } else { 2 }
        );
        if let Some(stats) = stats {
            assert_eq!(
                stats
                    .decoded_chunks
                    .load(std::sync::atomic::Ordering::Relaxed),
                1
            );
            assert_eq!(
                stats
                    .projected_reuses
                    .load(std::sync::atomic::Ordering::Relaxed),
                1
            );
        }
        drop(file);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_text_scan_conjunction_preserves_nullable_contains_order_and_empty_results() {
    let values = [
        None,
        Some(""),
        Some("Google東京の長い値"),
        Some("λ_google"),
        Some("Google nul\0value"),
        Some("tiny"),
        None,
        Some("Google_%.\\ long value"),
    ];
    let plain = VarBinViewArray::from_iter_nullable_str(values);
    let encoded = Zstd::from_var_bin_view_without_dict(
        &plain,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let bytes = write_array(
        &StructArray::new(
            ["text", "metric", "other"].into(),
            vec![
                encoded,
                PrimitiveArray::from_iter(0_u64..8).into_array(),
                VarBinViewArray::from_iter_str([
                    "ok", "ok", "ok", "blocked", "ok", "ok", "ok", "ok",
                ])
                .into_array(),
            ],
            8,
            Validity::NonNullable,
        )
        .into_array(),
    );
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    for needle in ["Google", "λ", "_%.\\", "", "absent"] {
        for negated in [false, true] {
            for reversed in [false, true] {
                for minimum in [2, 20] {
                    let text = PredicateExpr::StringContains {
                        column: ColumnRef::new("text").unwrap(),
                        needle: needle.into(),
                        negated,
                    };
                    let metric = PredicateExpr::Compare {
                        column: ColumnRef::new("metric").unwrap(),
                        op: ComparisonOp::GtEq,
                        value: StatValue::UInt64(minimum),
                    };
                    let mut children = vec![text.clone(), metric];
                    if reversed {
                        children.reverse();
                    }
                    children.push(PredicateExpr::And(vec![
                        text,
                        PredicateExpr::StringContains {
                            column: ColumnRef::new("other").unwrap(),
                            needle: "blocked".into(),
                            negated: true,
                        },
                    ]));
                    let (scan, stats) = reuse_columns(
                        &file,
                        &memory,
                        &PredicateExpr::And(children),
                        &["text", "metric"],
                        None,
                    );
                    let actual = text_and_metric(scan, &session, &runtime);
                    let expected = values
                        .iter()
                        .enumerate()
                        .filter_map(|(row, value)| {
                            value
                                .filter(|value| {
                                    row as u64 >= minimum
                                        && row != 3
                                        && value.contains(needle) != negated
                                })
                                .map(|value| (row as u64, value.to_owned()))
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "needle={needle:?}, negated={negated}, reversed={reversed}, minimum={minimum}"
                    );
                    if reversed {
                        assert_eq!(
                            stats
                                .decoded_chunks
                                .load(std::sync::atomic::Ordering::Relaxed),
                            0
                        );
                    }
                }
            }
        }
    }
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_text_scan_conjunction_false_filter_preserves_native_source_error_order() {
    let (bytes, _) = fixture();
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    let source = Arc::new(FaultSource {
        inner: file.segment_source(),
        cancellation: None,
    });
    let file = file.with_segment_source(source);
    let predicate = PredicateExpr::And(vec![PredicateExpr::AlwaysFalse, predicate()]);
    // Native layout metadata or array reads may fail before a constant false
    // expression is evaluated. Reuse must preserve that provider ordering.
    let filter = super::super::predicate_to_vortex_expr(
        &predicate,
        file.dtype(),
        crate::VortexQueryPrimitiveKind::SimpleAggregate,
    )
    .unwrap()
    .optimize_recursive(file.dtype())
    .unwrap()
    .bind(file.dtype())
    .unwrap();
    let native_error = file
        .scan()
        .unwrap()
        .with_filter(filter)
        .with_projection(select(["text"], root()).bind(file.dtype()).unwrap())
        .into_array_iter(&runtime)
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
    assert!(
        native_error
            .to_string()
            .contains("injected text source corruption")
    );
    let (scan, stats) = reuse_scan(&file, &memory, &predicate, None);
    let reused_error = scan
        .into_array_iter(&runtime)
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
    assert_eq!(reused_error.to_string(), native_error.to_string());
    assert_eq!(
        stats
            .decoded_chunks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_text_scan_conjunction_releases_owners_after_allocation_denial() {
    let (bytes, _) = fixture();
    let runtime = CurrentThreadRuntime::new();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory, &runtime);
    let file = session.open_options().open_buffer(bytes).unwrap();
    let (scan, _) = reuse_scan(
        &file,
        &memory,
        &PredicateExpr::And(vec![predicate(), predicate()]),
        None,
    );
    let before = memory.snapshot();
    let pressure = memory
        .reserve(before.limit_bytes - before.reserved_bytes - super::ENTRY_METADATA_BYTES)
        .unwrap();
    let error = scan
        .into_array_iter(&runtime)
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
    assert!(
        crate::owned_buffers::is_owned_reservation_denial(&error),
        "{error}"
    );
    drop(pressure);
    drop(file);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_text_scan_conjunction_all_null_text_is_empty_with_or_without_zstd() {
    for compressed in [false, true] {
        let plain = VarBinViewArray::from_iter_nullable_str([None::<&str>; 8]);
        let text = if compressed {
            Zstd::from_var_bin_view_without_dict(
                &plain,
                0,
                8,
                &mut VortexSession::default().create_execution_ctx(),
            )
            .unwrap()
            .into_array()
        } else {
            plain.into_array()
        };
        let bytes = write_array(
            &StructArray::new(["text"].into(), vec![text], 8, Validity::NonNullable).into_array(),
        );
        let runtime = CurrentThreadRuntime::new();
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let session = session(&memory, &runtime);
        let file = session.open_options().open_buffer(bytes).unwrap();
        let (scan, _) = reuse_scan(
            &file,
            &memory,
            &PredicateExpr::And(vec![predicate(), predicate()]),
            None,
        );
        assert!(scan.into_array_iter(&runtime).unwrap().next().is_none());
        drop(file);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_text_scan_projection_rejects_widened_masks_including_sliced_bit_tails() {
    use vortex::{buffer::BitBuffer, mask::Mask};
    for len in [0, 1, 63, 64, 65, 127, 128, 129] {
        let masks = [
            Mask::new_false(len),
            Mask::new_true(len),
            Mask::from_buffer(BitBuffer::from_iter((0..len + 7).map(|i| i % 3 == 0)))
                .slice(7..len + 7),
            Mask::from_buffer(BitBuffer::from_iter((0..len + 5).map(|i| i % 5 != 0)))
                .slice(5..len + 5),
            Mask::from_indices(len, (0..len).filter(|i| i % 2 == 0)),
        ];
        for selected in &masks {
            for previous in &masks {
                let expected = selected.iter().zip(previous.iter()).all(|(s, p)| !s || p);
                assert_eq!(super::mask_is_subset(selected, previous), expected);
            }
            assert!(!super::mask_is_subset(selected, &Mask::new_true(len + 1)));
        }
    }
}
