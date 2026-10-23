//! Native layout-directory bounds and pruning before payload registration.

use super::*;
use crate::resident_session::ResidentVortexSession;
use std::{num::NonZeroUsize, sync::Mutex};
use vortex::{
    array::{ExecutionCtx, arrays::DictArray},
    file::Footer,
    layout::{
        LayoutStrategy, layout_children,
        layouts::{
            chunked::ChunkedLayout,
            dict::writer::{DictLayoutOptions, DictStrategy},
            flat::{FlatLayout, writer::FlatLayoutStrategy},
            list::ListLayout,
            struct_::StructStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
    session::registry::ReadContext,
};

struct NoReads;
impl SegmentSource for NoReads {
    fn request(&self, _: SegmentId) -> SegmentFuture {
        panic!("directory admission must not request native payloads")
    }
}

fn metadata_file(layout: LayoutRef) -> VortexFile {
    VortexFile::new(
        Footer::new(layout, Arc::from([]), None, ReadContext::new([])),
        Arc::new(NoReads),
        VortexSession::default(),
    )
}

fn flat(rows: u64, dtype: DType) -> LayoutRef {
    FlatLayout::new(rows, dtype, SegmentId::from(0), ReadContext::new([])).into_layout()
}

#[test]
fn scalar_region_directory_rejects_unknown_layout_and_excess_depth_before_payloads() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let dtype = DType::Primitive(PType::I16, Nullability::NonNullable);
    for layers in [MAX_DEPTH - 1, MAX_DEPTH] {
        let mut layout = flat(MIN_ROWS, dtype.clone());
        for _ in 0..layers {
            layout = ChunkedLayout::new(MIN_ROWS, dtype.clone(), layout_children(vec![layout]))
                .into_layout();
        }
        let file = metadata_file(layout);
        let nodes = layout_node_bound(&file, &memory).unwrap();
        assert_eq!(nodes, (layers < MAX_DEPTH).then_some(MAX_DEPTH));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let list = ListLayout::new(
        DType::List(Arc::new(dtype.clone()), Nullability::NonNullable),
        flat(MIN_ROWS, dtype),
        flat(
            MIN_ROWS + 1,
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        None,
    )
    .into_layout();
    assert!(
        layout_node_bound(&metadata_file(list), &memory)
            .unwrap()
            .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(directory_bound(usize::MAX, u64::MAX).is_err());
}

struct RecordedSource {
    inner: Arc<dyn SegmentSource>,
    requested: Arc<Mutex<Vec<SegmentId>>>,
}
impl SegmentSource for RecordedSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requested.lock().unwrap().push(id);
        self.inner.request(id)
    }
}

fn segment_ids(layout: &LayoutRef) -> Vec<SegmentId> {
    let mut ids = layout.segment_ids();
    for index in 0..layout.nslots() {
        if let Some(child) = layout.slot(index).unwrap() {
            ids.extend(segment_ids(&child));
        }
    }
    ids
}

fn strategy(kind: &str) -> Arc<dyn LayoutStrategy> {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let chunks = super::super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(2);
    let leaf: Arc<dyn LayoutStrategy> = match kind {
        "dict" => Arc::new(DictStrategy::new(
            Arc::clone(&chunks),
            Arc::clone(&flat),
            chunks,
            DictLayoutOptions::default(),
            // This exact fixture has a four-value domain. Supply its encoded
            // probe while leaving the writer its required primitive input.
            Arc::new(|chunk: &ArrayRef, _: &mut ExecutionCtx| {
                Ok(DictArray::try_new(
                    PrimitiveArray::new(
                        (0..chunk.len())
                            .map(|row| u8::try_from(row % 4).unwrap())
                            .collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                    PrimitiveArray::from_option_iter([Some(-3_i16), None, Some(5), Some(0)])
                        .into_array(),
                )?
                .into_array())
            }),
        )),
        "zoned" => Arc::new(ZonedStrategy::new(
            chunks,
            Arc::clone(&flat),
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(65_536).unwrap(),
                concurrency: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
        )),
        _ => chunks,
    };
    Arc::new(StructStrategy::new(flat, leaf))
}

#[test]
fn scalar_region_struct_dictionary_and_zoned_layouts_keep_original_boundaries_and_values() {
    for kind in ["struct", "dict", "zoned"] {
        let source = write_with_strategy(
            &[65_536; 2],
            |_, rows| {
                let x = PrimitiveArray::from_option_iter(
                    (0..rows).map(|row| [Some(-3_i16), None, Some(5), Some(0)][row % 4]),
                )
                .into_array();
                StructArray::try_new(
                    FieldNames::from(["x"]),
                    vec![x],
                    rows,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
            },
            strategy(kind),
        );
        let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
        let memory = resident.memory().clone();
        let prepared = resident.prepare_file(&source.0).unwrap();
        crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
        let retained = memory.snapshot().reserved_bytes;
        prepared
            .with_native_execution(|file, session, runtime| {
                let child = file
                    .footer()
                    .layout()
                    .slot(1)
                    .map_err(vortex_error)?
                    .unwrap();
                assert_eq!(
                    child.encoding_id().as_ref(),
                    match kind {
                        "dict" => "vortex.dict",
                        "zoned" => "vortex.zoned",
                        _ => "vortex.chunked",
                    }
                );
                let nodes = layout_node_bound(file, &memory)?.unwrap();
                let (_, capacity) = directory_bound(nodes, file.row_count())?;
                let original = file
                    .scan()
                    .map_err(vortex_error)?
                    .full_file_splits()
                    .map_err(vortex_error)?;
                assert_eq!(original, vec![0, 65_536, 131_072]);
                assert!(original.capacity() as u64 <= 2 * capacity);
                for prune in [false, true] {
                    if prune && kind != "zoned" {
                        continue;
                    }
                    let requested = Arc::new(Mutex::new(Vec::new()));
                    let file = file.clone().with_segment_source(Arc::new(RecordedSource {
                        inner: file.segment_source(),
                        requested: Arc::clone(&requested),
                    }));
                    let mut request = request(&source, false);
                    if prune {
                        request.predicate = Some(PredicateExpr::Compare {
                            column: ColumnRef::new("x").unwrap(),
                            op: ComparisonOp::Gt,
                            value: StatValue::Int64(10),
                        });
                    }
                    let (regions, mut states) = super::lifecycle::admitted(
                        &file, &request, session, runtime, &memory, 4, None,
                    )?;
                    assert_eq!(&*regions.boundaries, original);
                    let report = super::lifecycle::run(regions, &mut states)?;
                    assert_eq!(report.completed, 2);
                    assert_eq!(report.source_rows, 131_072);
                    assert_eq!(
                        payload(&states.result_summary(&[])?)["values"],
                        expected(if prune { 0 } else { 131_072 }, false, false)
                    );
                    if prune {
                        assert_eq!(report.pruned, 2);
                        assert_eq!(report.arrays, 0);
                        let data = child.slot(0).map_err(vortex_error)?.unwrap();
                        let data_ids = segment_ids(&data);
                        let requests = requested.lock().unwrap();
                        assert!(!requests.is_empty(), "zone-map data must actually be read");
                        assert_ne!(data_ids, []);
                        assert!(
                            requests.iter().all(|id| !data_ids.contains(id)),
                            "pruning must precede data-payload registration"
                        );
                    }
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, retained);
        drop(prepared);
        drop(resident);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
