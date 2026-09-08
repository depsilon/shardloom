use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Mutex;
use vortex::{
    array::VortexSessionExecute as _,
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::{
        LayoutChildType, LayoutRef,
        layouts::{flat::Flat, zoned::Zoned},
        segments::{SegmentFuture, SegmentId, SegmentSource},
    },
};

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn text_zone_writer_reopens_exact_nullable_values_and_filtered_zone_boundaries() {
    let path = std::env::temp_dir().join(format!(
        "shardloom-text-zones-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    let directory = FixtureDirectory(path);
    for workers in [1, 3] {
        for all_null in [false, true] {
            verify_fixture(&directory.0, workers, all_null);
        }
    }
}

fn verify_fixture(root: &Path, workers: usize, all_null: bool) {
    let labels = (0..95)
        .map(|row| {
            if all_null || row % 7 == 0 {
                None
            } else {
                Some(match row % 4 {
                    0 => "",
                    1 => "a repeated category",
                    2 => "z repeated category",
                    _ => "港-λ quoted \" and newline\n",
                })
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("renamed_text", DataType::Utf8, true),
            Field::new("exact_identifier", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(labels.clone())),
            Arc::new(Int64Array::from_iter_values(
                (0..95_i64).map(|row| (1_i64 << 60) + row),
            )),
        ],
    )
    .unwrap();
    let expected = arrow_record_batch_to_vortex_array(batch).unwrap();
    let context = LocalVortexWriteContext::open();
    let _drivers =
        crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, workers - 1)
            .unwrap();
    let timing = VortexWriterStageTiming::default();
    let strategy = zoned_source_text_vortex_write_strategy(
        8,
        1024,
        workers,
        workers,
        &["renamed_text".into()],
        &timing,
        &context.session,
    );
    let path = root.join(format!("workers-{workers}-null-{all_null}.vortex"));
    context
        .session
        .write_options()
        .with_strategy(strategy)
        .blocking(&context.runtime)
        .write(
            fs::File::create(&path).unwrap(),
            expected.to_array_iterator(),
        )
        .unwrap();
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(&path))
        .unwrap();
    assert!(has_zoned_text_child(file.footer().layout().as_ref()));
    for filtered in [false, true] {
        let expected_rows = labels
            .iter()
            .enumerate()
            .filter(|(_, value)| !filtered || value.is_some_and(|text| text >= "m"))
            .map(|(row, _)| row)
            .collect::<Vec<_>>();
        let mut scan = file.scan().unwrap().with_ordered(true);
        if filtered {
            scan = scan.with_filter(
                vortex::expr::gt_eq(
                    vortex::expr::get_item("renamed_text", vortex::expr::root()),
                    vortex::expr::lit("m"),
                )
                .bind(file.dtype())
                .unwrap(),
            );
        }
        let mut seen = 0;
        let mut execution = context.session.create_execution_ctx();
        for array in scan.into_array_iter(&context.runtime).unwrap() {
            let array = array.unwrap();
            for row in 0..array.len() {
                assert_eq!(
                    array.execute_scalar(row, &mut execution).unwrap(),
                    expected
                        .execute_scalar(expected_rows[seen], &mut execution)
                        .unwrap()
                );
                seen += 1;
            }
        }
        assert_eq!(seen, expected_rows.len());
    }
}

fn has_zoned_text_child(layout: &dyn vortex::layout::DynLayout) -> bool {
    for (name, child) in layout.child_names().zip(layout.children().unwrap()) {
        if name.as_ref() == "renamed_text" {
            return vortex_layout_encoding_inventory(child.as_ref())
                .1
                .contains("zoned");
        }
        if has_zoned_text_child(child.as_ref()) {
            return true;
        }
    }
    false
}

struct RequestedSegments {
    source: Arc<dyn SegmentSource>,
    ids: Mutex<BTreeSet<u32>>,
}

impl SegmentSource for RequestedSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.ids.lock().unwrap().insert(*id);
        self.source.request(id)
    }
}

fn text_field_layout(layout: &dyn vortex::layout::DynLayout) -> Option<LayoutRef> {
    for slot in 0..layout.nslots() {
        let Some(child) = layout.slot(slot).unwrap() else {
            continue;
        };
        if matches!(layout.slot_type(slot), Some(LayoutChildType::Field(name)) if name.as_ref() == "renamed_text")
        {
            return Some(child);
        }
        if let Some(field) = text_field_layout(child.as_ref()) {
            return Some(field);
        }
    }
    None
}

fn flat_segment_ids(layout: &dyn vortex::layout::DynLayout) -> Vec<u32> {
    if let Some(flat) = layout.as_opt::<Flat>() {
        return vec![*flat.segment_id()];
    }
    layout
        .children()
        .unwrap()
        .iter()
        .flat_map(|child| flat_segment_ids(child.as_ref()))
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One artifact verifies zone metadata, exact values and skipped requests.
fn text_filter_pruning_skips_unprojected_disjoint_and_null_only_payloads_with_conservative_utf8_bounds()
 {
    let directory = FixtureDirectory(std::env::temp_dir().join(format!(
        "shardloom-text-pruning-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
    )));
    fs::create_dir(&directory.0).unwrap();
    // 63 ASCII bytes put the first multibyte character across the provider's
    // 64-byte bounded-extrema cutoff. The last zone must remain conservative.
    let long_prefix = format!("{}港", "m".repeat(63));
    let pivot = format!("{long_prefix}m");
    let labels = (0..40)
        .map(|row| match row / 8 {
            0 if row % 4 == 0 => None,
            0 => Some(format!("{}λ低", "a".repeat(80))),
            1 => None,
            2 => Some(format!("{}λ中", "n".repeat(80))),
            3 => Some(format!("{}λ高", "z".repeat(80))),
            _ => Some(format!(
                "{long_prefix}{}",
                if row % 2 == 0 { "a" } else { "z" }
            )),
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("renamed_text", DataType::Utf8, true),
            Field::new("exact_identifier", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                labels.iter().map(Option::as_deref).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..40_i64).map(|row| (1_i64 << 60) + row),
            )),
        ],
    )
    .unwrap();
    let input = arrow_record_batch_to_vortex_array(batch).unwrap();
    for workers in [1, 3] {
        let context = LocalVortexWriteContext::open();
        let _drivers =
            crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, workers - 1)
                .unwrap();
        let timing = VortexWriterStageTiming::default();
        let strategy = zoned_source_text_vortex_write_strategy(
            8,
            1024,
            workers,
            workers,
            &["renamed_text".into()],
            &timing,
            &context.session,
        );
        let path = directory.0.join(format!("pruning-{workers}.vortex"));
        context
            .session
            .write_options()
            .with_strategy(strategy)
            .blocking(&context.runtime)
            .write(fs::File::create(&path).unwrap(), input.to_array_iterator())
            .unwrap();
        let file = context
            .runtime
            .block_on(context.session.open_options().open_path(&path))
            .unwrap();
        let text = text_field_layout(file.footer().layout().as_ref()).unwrap();
        let zoned = text
            .as_opt::<Zoned>()
            .expect("selected text leaf must retain zoning");
        assert_eq!(zoned.zone_len(), 8);
        assert_eq!(zoned.nzones(), 5);
        for aggregate in ["bounded_min", "bounded_max", "null_count"] {
            assert!(
                zoned
                    .present_aggregates()
                    .iter()
                    .any(|name| name.contains(aggregate))
            );
        }
        let data = text.slot(0).unwrap().unwrap();
        let payloads = flat_segment_ids(data.as_ref());
        assert_eq!(payloads.len(), 5);
        assert_eq!(data.nslots(), 5);
        for (slot, payload) in payloads.iter().enumerate() {
            assert!(
                matches!(data.slot_type(slot), Some(LayoutChildType::Chunk((index, offset)))
                    if index == slot && offset == u64::try_from(slot * 8).unwrap())
            );
            let child = data.slot(slot).unwrap().unwrap();
            assert_eq!(child.row_count(), 8);
            assert_eq!(*child.as_opt::<Flat>().unwrap().segment_id(), *payload);
        }
        let stats_ids = flat_segment_ids(text.slot(1).unwrap().unwrap().as_ref());
        assert!(payloads.iter().all(|id| !stats_ids.contains(id)));
        for (threshold, project_text, where_guard) in [
            (None, true, false),
            (Some("m"), true, false),
            (Some(pivot.as_str()), true, false),
            (Some("m"), false, false),
            (Some(pivot.as_str()), false, false),
            (Some("m"), false, true),
            (Some(pivot.as_str()), false, true),
        ] {
            // A fresh reader tree prevents earlier scans' reader caches from
            // substituting for the segment-request pruning proof.
            let requests = Arc::new(RequestedSegments {
                source: file.segment_source(),
                ids: Mutex::new(BTreeSet::new()),
            });
            let observed = file.clone().with_segment_source(requests.clone());
            let mut scan = observed.scan().unwrap().with_ordered(true);
            if !project_text {
                // Pinned split_exec constructs projection futures before it
                // awaits pruning. Projecting text therefore registers its
                // payload eagerly, even for a subsequently pruned split.
                // Filter-only text must avoid those disjoint payload requests;
                // exact full-row projection remains covered above as well.
                scan = scan.with_projection(
                    vortex::expr::select(["exact_identifier"], vortex::expr::root())
                        .bind(observed.dtype())
                        .unwrap(),
                );
            }
            if let Some(threshold) = threshold {
                let field = vortex::expr::get_item("renamed_text", vortex::expr::root());
                let comparison = vortex::expr::gt_eq(field.clone(), vortex::expr::lit(threshold));
                let predicate = if where_guard {
                    // WHERE treats NULL as unselected. For this positive,
                    // infallible field/non-null-literal comparison, an explicit
                    // null guard preserves selected rows and lets native null
                    // statistics prove false. This is not a generic binary
                    // falsifier or a rewrite under NOT/projected Boolean values.
                    vortex::expr::and(vortex::expr::is_not_null(field), comparison)
                } else {
                    comparison
                };
                scan = scan.with_filter(predicate.bind(observed.dtype()).unwrap());
            }
            let expected_rows = labels
                .iter()
                .enumerate()
                .filter(|(_, label)| {
                    threshold.is_none_or(|threshold| {
                        label.as_deref().is_some_and(|label| label >= threshold)
                    })
                })
                .map(|(row, _)| row)
                .collect::<Vec<_>>();
            let mut seen = 0;
            let mut execution = context.session.create_execution_ctx();
            for array in scan.into_array_iter(&context.runtime).unwrap() {
                let array = array.unwrap();
                for row in 0..array.len() {
                    let expected_row = expected_rows[seen];
                    let scalar = array.execute_scalar(row, &mut execution).unwrap();
                    let fields = scalar.as_struct();
                    assert_eq!(
                        fields.field("exact_identifier").unwrap(),
                        ((1_i64 << 60) + i64::try_from(expected_row).unwrap()).into()
                    );
                    if project_text {
                        let actual_text = fields.field("renamed_text").unwrap();
                        if let Some(label) = &labels[expected_row] {
                            assert_eq!(actual_text, label.as_str().into());
                        } else {
                            assert!(actual_text.is_null());
                        }
                    }
                    seen += 1;
                }
            }
            assert_eq!(seen, expected_rows.len());
            let requested = requests.ids.lock().unwrap();
            if threshold.is_some() && !project_text {
                assert!(
                    !requested.contains(&payloads[0]),
                    "disjoint filter-only text payload was requested"
                );
                if where_guard {
                    assert!(
                        !requested.contains(&payloads[1]),
                        "WHERE-guarded null-only filter text payload was requested"
                    );
                } else {
                    // Native Gte falsifies with max < literal. An all-null max
                    // yields NULL (inconclusive), so this raw comparison cannot
                    // use the null-count proof and requests its data payload.
                    assert!(
                        requested.contains(&payloads[1]),
                        "raw comparison no longer exhibits the pinned null-zone limitation"
                    );
                }
                for id in &payloads[2..] {
                    assert!(requested.contains(id), "selected payload was not requested");
                }
            } else {
                // Projection requests are eager, including filtered full-row
                // scans. Registration is not proof of physical I/O: the provider
                // may cancel or coalesce it. It may synthesize all-null metadata.
                for index in [0, 2, 3, 4] {
                    assert!(
                        requested.contains(&payloads[index]),
                        "full scan missed a non-null payload"
                    );
                }
            }
        }
    }
}
