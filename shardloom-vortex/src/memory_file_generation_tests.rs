use super::{MemoryFileGeneration, MemoryFileGenerationBounds, generation_error, hex_digest};
use std::{
    fs,
    io::{Seek as _, SeekFrom, Write as _},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use vortex::{
    VortexSessionDefault as _,
    array::VortexSessionExecute as _,
    expr::{get_item, gt_eq, lit, root},
    layout::segments::{SegmentId, SegmentSource as _},
    session::VortexSession,
};

use crate::{
    local_primitives::collect::render_owned_json,
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
    resident_session::ResidentVortexSession,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-memory-file-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        Self(directory)
    }

    fn target(&self) -> PathBuf {
        self.0.join("generation.vortex")
    }

    fn entries(&self) -> usize {
        fs::read_dir(&self.0).unwrap().count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

const COLUMNS: [&str; 4] = ["renamed_label", "admitted", "measurement", "identifier"];

fn source(session: &ResidentVortexSession) -> ResidentMemorySource {
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "identifier",
                values: MemoryColumnValues::Int64(&[Some(i64::MAX), None, Some(i64::MIN), Some(7)]),
            },
            MemoryColumn {
                name: "measurement",
                values: MemoryColumnValues::Float64(&[Some(1.25), None, Some(-0.0), Some(1.5)]),
            },
            MemoryColumn {
                name: "admitted",
                values: MemoryColumnValues::Bool(&[Some(true), None, Some(false), Some(false)]),
            },
            MemoryColumn {
                name: "renamed_label",
                values: MemoryColumnValues::Utf8(&[
                    Some("λ\"\n東京"),
                    None,
                    Some("kept"),
                    Some(""),
                ]),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap()
}

fn generation(session: &ResidentVortexSession) -> MemoryFileGeneration {
    source(session)
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap()
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the addressable query and publication proof together.
fn column_row_group_ranges_request_only_addressed_native_segments_and_publish_exactly() {
    use super::MemoryFileGenerationLayout;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 1).unwrap();
    let ids = (0..12_i64).collect::<Vec<_>>();
    let strings = (0..12)
        .map(|row| format!("row{row}:{}", "é".repeat(1024)))
        .collect::<Vec<_>>();
    let text = strings
        .iter()
        .enumerate()
        .map(|(row, value)| {
            if (4..8).contains(&row) {
                None
            } else {
                Some(value.as_str())
            }
        })
        .collect::<Vec<_>>();
    let source = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn {
                name: "exact_id",
                values: MemoryColumnValues::Int64NonNullable(&ids),
            },
            MemoryColumn {
                name: "renamed_text",
                values: MemoryColumnValues::Utf8(&text),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let generation = source
        .file_generation_with_layout(
            MemoryFileGenerationBounds::default(),
            MemoryFileGenerationLayout {
                row_group_rows: 4,
                max_segments: 6,
            },
            None,
        )
        .unwrap();
    assert_eq!(generation.dtype(), source.dtype());
    assert_eq!(generation.evidence().array_serializer_calls, 6);
    assert_eq!(generation.evidence().row_groups, 3);
    assert!(generation.evidence().row_group_offset_bytes_built > 0);
    let before = generation.segment_evidence();
    assert_eq!(before.len(), 6);
    assert!(before.iter().all(|segment| segment.requests == 0));
    // A UTF8 row-group does not serialize the unrelated whole-column backing.
    assert!(
        before
            .iter()
            .filter(|segment| segment.column_index == 1)
            .all(|segment| segment.serialized_bytes < 12 * 1024)
    );
    let operation = generation
        .prepare_projection_range(&["exact_id"], None, 4..8, 4, 64 * 1024)
        .unwrap();
    let arrays = operation.execute().unwrap();
    let names = vec!["exact_id".to_string()];
    let json = render_owned_json(&arrays, &names, session.memory(), 64 * 1024).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(json.value()).unwrap(),
        json!([{"exact_id":4},{"exact_id":5},{"exact_id":6},{"exact_id":7}])
    );
    let after = generation.segment_evidence();
    assert!(after[1].requests > 0);
    assert!(
        after
            .iter()
            .enumerate()
            .all(|(id, segment)| id == 1 || segment.requests == 0)
    );
    drop(json);
    drop(arrays);
    drop(operation);
    let operation = generation
        .prepare_projection_range(
            &["renamed_text", "exact_id"],
            Some(gt_eq(get_item("exact_id", root()), lit(6_i64))),
            4..8,
            4,
            64 * 1024,
        )
        .unwrap();
    let arrays = operation.execute().unwrap();
    let names = vec!["renamed_text".to_string(), "exact_id".to_string()];
    let json = render_owned_json(&arrays, &names, session.memory(), 64 * 1024).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(json.value()).unwrap(),
        json!([{"exact_id":6,"renamed_text":null},{"exact_id":7,"renamed_text":null}])
    );
    assert!(
        generation
            .segment_evidence()
            .iter()
            .enumerate()
            .all(|(id, segment)| id == 1 || id == 4 || segment.requests == 0)
    );
    let publication = generation.publish(&fixture.target()).unwrap();
    assert!(publication.durable);
    assert_eq!(publication.array_serializer_calls, 0);
    assert_eq!(generation.evidence().array_serializer_calls, 6);
    assert_eq!(
        publication.independent_readback_bytes,
        publication.file_bytes_written
    );
    let reopened = session.prepare_file(fixture.target()).unwrap();
    assert_eq!(reopened.dtype(), source.dtype());
    let all = reopened
        .prepare_projection(&["exact_id", "renamed_text"], 12, 128 * 1024)
        .unwrap()
        .execute()
        .unwrap();
    let names = vec!["exact_id".to_string(), "renamed_text".to_string()];
    let all_json = render_owned_json(&all, &names, session.memory(), 128 * 1024).unwrap();
    let expected = ids
        .iter()
        .enumerate()
        .map(|(row, id)| json!({"exact_id":id,"renamed_text":text[row]}))
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::from_str::<Value>(all_json.value()).unwrap(),
        json!(expected)
    );
    assert!(
        generation
            .prepare_projection_range(&["exact_id"], None, 12..13, 1, 4096)
            .is_err()
    );
    let empty = generation
        .prepare_projection_range(&["exact_id"], None, 5..5, 1, 4096)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(empty.row_count(), 0);
}

#[test]
fn generation_preserves_exact_long_text_stats_without_reusing_whole_column_bounds_on_slices() {
    use crate::resident_memory_source::OwnedMemoryColumn;
    use vortex::array::{
        IntoArray as _, arrays::StructArray, buffer::BufferHandle, scalar::Scalar,
        serde::SerializedArray, validity::Validity,
    };
    use vortex::expr::stats::{Precision, Stat, StatsProvider as _};
    use vortex::layout::layouts::flat::Flat;
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let minimum = format!("a{}", "λ".repeat(100));
    let maximum = format!("z{}", "猫".repeat(100));
    let bytes = format!("{minimum}{maximum}").into_bytes();
    let column = OwnedMemoryColumn::utf8(
        &session,
        "long_text",
        vec![0, minimum.len() as u64, bytes.len() as u64],
        bytes,
        None,
    )
    .unwrap();
    let min_scalar = Scalar::from(minimum.as_str());
    let max_scalar = Scalar::from(maximum.as_str());
    column.array().statistics().set(
        Stat::Min,
        Precision::Exact(min_scalar.clone().into_value().unwrap()),
    );
    column.array().statistics().set(
        Stat::Max,
        Precision::Exact(max_scalar.clone().into_value().unwrap()),
    );
    column
        .array()
        .statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
    let (slice, _) =
        super::slice_generation_column(column.array(), 1..2, &session.native_allocator()).unwrap();
    assert!(slice.statistics().get(Stat::Min).as_exact().is_none());
    assert!(slice.statistics().get(Stat::Max).as_exact().is_none());
    assert_eq!(
        slice.statistics().get(Stat::IsSorted),
        Precision::Exact(Scalar::from(true))
    );
    let array = StructArray::try_new(
        ["long_text"].into(),
        vec![column.array().clone()],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let generation = MemoryFileGeneration::build(
        &session,
        &array,
        1024,
        0,
        MemoryFileGenerationBounds::default(),
    )
    .unwrap();
    let evidence = generation.evidence();
    assert_eq!(evidence.construction_footer_serializer_calls, 1);
    assert!(evidence.construction_footer_bytes > 0);
    let column_layout = generation
        .0
        .source
        .file()
        .footer()
        .layout()
        .children()
        .unwrap()
        .remove(0);
    let leaf = column_layout.children().unwrap().remove(0);
    let flat = leaf.as_opt::<Flat>().unwrap();
    let buffer = BufferHandle::new_host(generation.0.segments.segments[0].buffer.clone());
    let serialized = if let Some(tree) = flat.array_tree() {
        SerializedArray::from_flatbuffer_and_segment(tree.clone(), buffer).unwrap()
    } else {
        SerializedArray::try_from(buffer).unwrap()
    };
    session
        .with_native_session(|native, _| {
            let restored = serialized
                .decode(column.array().dtype(), 2, flat.array_ctx(), native)
                .unwrap();
            assert_eq!(
                restored.statistics().get(Stat::Min),
                Precision::Exact(min_scalar)
            );
            assert_eq!(
                restored.statistics().get(Stat::Max),
                Precision::Exact(max_scalar)
            );
            assert_eq!(
                restored.statistics().get(Stat::IsSorted),
                Precision::Exact(Scalar::from(true))
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn partial_generation_cancellation_releases_segments_and_preserves_owned_intake() {
    use super::{GenerationBuildControl, MemoryFileGenerationLayout};
    use crate::resident_memory_source::OwnedMemoryColumn;
    use vortex::array::{IntoArray as _, arrays::StructArray, validity::Validity};
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let memory = session.memory().clone();
    let input = OwnedMemoryColumn::int64(&session, "key", (0..32_i64).collect(), None).unwrap();
    let array = StructArray::try_new(
        ["key"].into(),
        vec![input.array().clone()],
        32,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let before = memory.snapshot().reserved_bytes;
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    let after_leaf = |completed| {
        assert_eq!(completed, 1);
        cancelled.store(true, Ordering::Release);
    };
    let result = MemoryFileGeneration::build_controlled(
        &session,
        &array,
        256,
        0,
        MemoryFileGenerationBounds::default(),
        MemoryFileGenerationLayout {
            row_group_rows: 8,
            max_segments: 4,
        },
        GenerationBuildControl {
            cancelled: Some(&cancelled),
            after_leaf: Some(&after_leaf),
        },
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("construction cancelled")
    );
    assert_eq!(memory.snapshot().reserved_bytes, before);
    assert!(
        MemoryFileGeneration::build_with_layout(
            &session,
            &array,
            256,
            0,
            MemoryFileGenerationBounds::default(),
            MemoryFileGenerationLayout {
                row_group_rows: 8,
                max_segments: 4
            },
            Some(&cancelled)
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, before);
    drop(array);
    drop(input);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn generation_geometry_rejection_precedes_serialization_and_zero_rows_need_no_segments() {
    use super::MemoryFileGenerationLayout;
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let input = source(&session);
    let before = session.snapshot().memory.reserved_bytes;
    for layout in [
        MemoryFileGenerationLayout {
            row_group_rows: 0,
            max_segments: 4,
        },
        MemoryFileGenerationLayout {
            row_group_rows: 1,
            max_segments: 15,
        },
        MemoryFileGenerationLayout {
            row_group_rows: 1,
            max_segments: 4097,
        },
    ] {
        assert!(
            input
                .file_generation_with_layout(MemoryFileGenerationBounds::default(), layout, None)
                .is_err()
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, before);
    }
    let empty = ResidentMemorySource::from_owned_columns(
        &session,
        vec![
            crate::resident_memory_source::OwnedMemoryColumn::int64(
                &session,
                "empty",
                vec![],
                None,
            )
            .unwrap(),
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let generation = empty
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap();
    assert!(generation.segment_evidence().is_empty());
    assert_eq!(generation.evidence().array_serializer_calls, 0);
    assert_eq!(generation.evidence().row_groups, 0);
}

// Independent decoded Rust values, including nullable values, extreme integers,
// Unicode, escaping and signed zero. No second native query supplies the oracle.
fn expected() -> Value {
    json!([
        {"identifier": i64::MAX, "measurement": 1.25, "admitted": true, "renamed_label": "λ\"\n東京"},
        {"identifier": null, "measurement": null, "admitted": null, "renamed_label": null},
        {"identifier": i64::MIN, "measurement": -0.0, "admitted": false, "renamed_label": "kept"},
        {"identifier": 7, "measurement": 1.5, "admitted": false, "renamed_label": ""}
    ])
}

fn collected(generation: &MemoryFileGeneration) -> Value {
    let result = generation.collect(&COLUMNS, None, 100, 64 * 1024).unwrap();
    assert!(!result.native_io_certificate.side_effects.fallback_attempted);
    assert!(!result.native_io_certificate.side_effects.arrow_converted);
    assert!(!result.native_io_certificate.side_effects.write_io);
    assert_eq!(
        result
            .native_io_certificate
            .source_capability_report
            .source_kind,
        "immutable_vortex_file_segments"
    );
    serde_json::from_str(result.values_json.value()).unwrap()
}

#[test]
fn real_native_file_queries_preserve_full_values_filter_nulls_and_ordered_limit() {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 2).unwrap();
    let input = source(&session);
    let generation = input
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap();
    assert_eq!(generation.dtype(), input.dtype());
    assert_eq!(generation.row_count(), 4);
    let before = generation.evidence();
    assert_eq!(before.array_serializer_calls, 4);
    assert_eq!(before.columns, 4);
    assert_eq!(before.row_groups, 1);
    assert_eq!(before.dictionary_build_calls, 0);
    assert_eq!(before.memory_file_constructions, 1);
    assert_eq!(before.source_file_opens, 0);
    assert_eq!(before.memory_segment_requests, 0);
    assert_eq!(
        before.intake_payload_bytes_copied,
        64 + "λ\"\n東京".len() as u64 + 4
    );
    assert!(before.segment_assembly_bytes_copied > 0);
    drop(input);
    for _ in 0..3 {
        assert_eq!(collected(&generation), expected());
    }
    let result = generation
        .collect(
            &COLUMNS,
            Some(gt_eq(get_item("identifier", root()), lit(0_i64))),
            1,
            64 * 1024,
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(result.values_json.value()).unwrap(),
        json!([expected()[0]])
    );
    assert_eq!(result.rows, 1);
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    assert_eq!(session.snapshot().completed_executions, 4);
    let after = generation.evidence();
    assert_eq!(after.array_serializer_calls, before.array_serializer_calls);
    assert_eq!(
        after.segment_assembly_bytes_copied,
        before.segment_assembly_bytes_copied
    );
    assert!(after.memory_segment_requests > 0);
}

#[test]
fn durable_publication_reuses_identical_segments_and_reopens_exact_values_and_dtype() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 * 1024 * 1024, 2).unwrap();
    let generation = generation(&session);
    let old_projection = generation
        .prepare_projection(&COLUMNS, None, 100, 64 * 1024)
        .unwrap();
    assert_eq!(fixture.entries(), 0);
    let before = generation.evidence();
    let publication = generation.publish(&fixture.target()).unwrap();
    assert!(publication.durable);
    assert!(!publication.fallback_attempted);
    assert_eq!(publication.array_serializer_calls, 0);
    assert_eq!(publication.dictionary_build_calls, 0);
    assert_eq!(publication.footer_serializer_calls, 1);
    assert_eq!(publication.validation_file_opens, 1);
    assert_eq!(publication.rows, 4);
    let bytes = fs::read(fixture.target()).unwrap();
    assert_eq!(
        publication.output_sha256,
        hex_digest(Sha256::digest(&bytes).into())
    );
    assert_eq!(publication.file_bytes_written, bytes.len() as u64);
    assert_eq!(publication.independent_readback_bytes, bytes.len() as u64);
    for segment in &generation.0.segments.segments {
        let start = usize::try_from(segment.spec.offset).unwrap();
        let end = start + segment.spec.length as usize;
        assert_eq!(&bytes[start..end], segment.buffer.as_ref());
    }
    assert_eq!(generation.evidence(), before);
    let reopened = session.prepare_file(fixture.target()).unwrap();
    assert_eq!(reopened.dtype(), generation.dtype());
    assert_eq!(reopened.prepare_count().execute().unwrap(), 4);
    let arrays = reopened
        .prepare_projection(&COLUMNS, 100, 64 * 1024)
        .unwrap()
        .execute()
        .unwrap();
    let names = COLUMNS.map(str::to_owned);
    let values = render_owned_json(&arrays, &names, session.memory(), 64 * 1024).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(values.value()).unwrap(),
        expected()
    );
    let old = old_projection.execute().unwrap();
    let old_values = render_owned_json(&old, &names, session.memory(), 64 * 1024).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(old_values.value()).unwrap(),
        expected()
    );
    assert_eq!(fixture.entries(), 1);
}

#[test]
fn prepared_memory_reader_and_result_buffer_credits_outlive_generation_and_session() {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let memory = session.memory().clone();
    let generation = generation(&session);
    let prepared = generation
        .prepare_projection(&["identifier"], None, 100, 64 * 1024)
        .unwrap();
    drop(generation);
    drop(session);
    let result = prepared.execute().unwrap();
    let arrays = result.arrays().to_vec();
    drop(prepared);
    drop(result);
    assert!(memory.snapshot().reserved_bytes > 0);
    let provider = VortexSession::default();
    let mut context = provider.create_execution_ctx();
    let projection = get_item("identifier", root())
        .bind(arrays[0].dtype())
        .unwrap();
    let field = arrays[0].clone().apply_bound(&projection).unwrap();
    assert_eq!(
        field.execute_scalar(0, &mut context).unwrap(),
        i64::MAX.into()
    );
    assert!(field.execute_scalar(1, &mut context).unwrap().is_null());
    assert_eq!(
        field.execute_scalar(2, &mut context).unwrap(),
        i64::MIN.into()
    );
    assert_eq!(field.execute_scalar(3, &mut context).unwrap(), 7_i64.into());
    drop(field);
    drop(context);
    drop(arrays);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn empty_typed_generation_has_real_footer_and_exact_native_reopen() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let source = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn {
                name: "empty_alias",
                values: MemoryColumnValues::Int64(&[]),
            },
            MemoryColumn {
                name: "nullable_text",
                values: MemoryColumnValues::Utf8(&[]),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let generation = source
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap();
    let result = generation
        .collect(&["nullable_text", "empty_alias"], None, 1, 4096)
        .unwrap();
    assert_eq!(result.rows, 0);
    assert_eq!(result.values_json.value(), "[]");
    assert_eq!(generation.publish(&fixture.target()).unwrap().rows, 0);
    let reopened = session.prepare_file(fixture.target()).unwrap();
    assert_eq!(reopened.dtype(), source.dtype());
    assert_eq!(reopened.prepare_count().execute().unwrap(), 0);
    assert_eq!(
        reopened
            .prepare_projection(&["nullable_text", "empty_alias"], 1, 4096)
            .unwrap()
            .execute()
            .unwrap()
            .row_count(),
        0
    );
}

#[test]
fn failed_generation_and_failed_queries_release_admitted_credits() {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let memory = session.memory().clone();
    let source = source(&session);
    let intake_bytes = memory.snapshot().reserved_bytes;
    for bounds in [
        MemoryFileGenerationBounds {
            max_serialized_bytes: 0,
            ..MemoryFileGenerationBounds::default()
        },
        MemoryFileGenerationBounds {
            max_serialized_bytes: 16,
            ..MemoryFileGenerationBounds::default()
        },
        MemoryFileGenerationBounds {
            max_metadata_bytes: 0,
            ..MemoryFileGenerationBounds::default()
        },
        MemoryFileGenerationBounds {
            max_metadata_bytes: 4 * 1024 * 1024,
            ..MemoryFileGenerationBounds::default()
        },
    ] {
        assert!(source.file_generation(bounds).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, intake_bytes);
    }
    let generation = source
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap();
    assert!(
        generation
            .prepare_projection(&["missing"], None, 1, 4096)
            .is_err()
    );
    assert!(
        generation
            .prepare_projection(&["identifier", "identifier"], None, 1, 4096)
            .is_err()
    );
    assert!(
        generation
            .prepare_projection(&["identifier"], Some(lit(1_i64)), 1, 4096)
            .is_err()
    );
    assert!(
        generation
            .prepare_projection(&["identifier"], None, 0, 4096)
            .is_err()
    );
    assert!(
        generation
            .prepare_projection(&["identifier"], None, 65_537, 4096)
            .is_err()
    );
    assert!(generation.collect(&COLUMNS, None, 100, 1).is_err());
    assert!(
        futures::executor::block_on(generation.0.segments.request(SegmentId::from(99))).is_err()
    );
    drop(generation);
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn corrupt_or_cancelled_publication_removes_owned_staging_and_preserves_generation() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let generation = generation(&session);
    let corrupt = generation.publish_with_validation(&fixture.target(), |file| {
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"bad!").unwrap();
        Ok(())
    });
    assert!(
        corrupt
            .unwrap_err()
            .to_string()
            .contains("independent readback differs")
    );
    assert_eq!(fixture.entries(), 0);
    let cancelled = generation.publish_with_validation(&fixture.target(), |_| {
        Err(generation_error("injected cancellation"))
    });
    assert!(
        cancelled
            .unwrap_err()
            .to_string()
            .contains("injected cancellation")
    );
    assert_eq!(fixture.entries(), 0);
    assert_eq!(collected(&generation), expected());
    assert!(generation.publish(&fixture.target()).unwrap().durable);
    assert_eq!(fixture.entries(), 1);
}

#[test]
fn publication_never_overwrites_existing_or_symlink_targets() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let generation = generation(&session);
    fs::write(fixture.target(), b"foreign owner").unwrap();
    assert!(generation.publish(&fixture.target()).is_err());
    assert_eq!(fs::read(fixture.target()).unwrap(), b"foreign owner");
    let link = fixture.0.join("link.vortex");
    std::os::unix::fs::symlink(fixture.target(), &link).unwrap();
    assert!(generation.publish(&link).is_err());
    assert_eq!(fs::read(fixture.target()).unwrap(), b"foreign owner");
    assert!(fs::symlink_metadata(link).unwrap().file_type().is_symlink());
    assert_eq!(fixture.entries(), 2);
}

#[test]
fn publication_requires_existing_real_parent_without_creating_missing_ancestors() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let generation = generation(&session);
    let missing = fixture
        .0
        .join("missing")
        .join("nested")
        .join("generation.vortex");
    let error = generation.publish(&missing).unwrap_err().to_string();
    assert!(error.contains("existing real parent directory"));
    assert!(!fixture.0.join("missing").exists());
    assert_eq!(fixture.entries(), 0);

    let real = fixture.0.join("real");
    fs::create_dir(&real).unwrap();
    let alias = fixture.0.join("alias");
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    assert!(
        generation
            .publish(&alias.join("generation.vortex"))
            .is_err()
    );
    assert_eq!(fs::read_dir(&real).unwrap().count(), 0);
    assert!(
        generation
            .publish(&real.join("generation.vortex"))
            .unwrap()
            .durable
    );
}

#[test]
fn publication_parent_replacement_preserves_foreign_directory_and_rejects_durable_claim() {
    for after_publication in [false, true] {
        let fixture = Fixture::new();
        let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
        let generation = generation(&session);
        let parent = fixture.0.join("parent");
        let moved = fixture.0.join("moved-parent");
        fs::create_dir(&parent).unwrap();
        let target = parent.join("generation.vortex");
        let replace_parent = || -> shardloom_core::Result<()> {
            fs::rename(&parent, &moved).unwrap();
            fs::create_dir(&parent).unwrap();
            fs::write(parent.join("foreign-owner"), b"preserve this directory").unwrap();
            Ok(())
        };
        let result = if after_publication {
            generation.publish_with_hooks(&target, |_| Ok(()), replace_parent)
        } else {
            generation.publish_with_validation(&target, |_| replace_parent())
        };
        let error = result.unwrap_err().to_string();
        assert!(error.contains("parent directory identity changed"));
        assert_eq!(error.contains("output was published"), after_publication);
        assert_eq!(
            fs::read(parent.join("foreign-owner")).unwrap(),
            b"preserve this directory"
        );
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);
        assert!(!target.exists());
        assert_eq!(moved.join("generation.vortex").exists(), after_publication);
        assert_eq!(collected(&generation), expected());
    }
}

#[test]
fn separately_built_generations_do_not_change_existing_prepared_answers() {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
    let first = generation(&session);
    let prepared = first
        .prepare_projection(&["identifier"], None, 100, 4096)
        .unwrap();
    let second = ResidentMemorySource::from_columns(
        &session,
        &[MemoryColumn {
            name: "identifier",
            values: MemoryColumnValues::Int64(&[Some(10)]),
        }],
        MemorySourceBounds::default(),
    )
    .unwrap()
    .file_generation(MemoryFileGenerationBounds::default())
    .unwrap();
    assert_eq!(
        second
            .collect(&["identifier"], None, 1, 4096)
            .unwrap()
            .values_json
            .value(),
        "[{\"identifier\":10}]"
    );
    drop(first);
    let result = prepared.execute().unwrap();
    assert_eq!(result.row_count(), 4);
    let values =
        render_owned_json(&result, &["identifier".into()], session.memory(), 4096).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(values.value()).unwrap(),
        json!([
            {"identifier": i64::MAX}, {"identifier": null}, {"identifier": i64::MIN}, {"identifier": 7}
        ])
    );
}

#[test]
fn publication_target_replacement_unlink_and_in_place_changes_reject_durable_claim() {
    for mutation in ["replace", "unlink", "in_place"] {
        let fixture = Fixture::new();
        let session = ResidentVortexSession::new(4 * 1024 * 1024, 1).unwrap();
        let generation = generation(&session);
        let target = fixture.target();
        let result = generation.publish_with_hooks(
            &target,
            |_| Ok(()),
            || {
                match mutation {
                    "replace" => {
                        fs::remove_file(&target).unwrap();
                        fs::write(&target, b"foreign generation").unwrap();
                    }
                    "unlink" => fs::remove_file(&target).unwrap(),
                    "in_place" => {
                        let mut file = fs::OpenOptions::new().write(true).open(&target).unwrap();
                        let metadata = file.metadata().unwrap();
                        file.write_all(b"foreign generation").unwrap();
                        // Preserve size and inode, with a deterministic mtime
                        // change even on filesystems with coarse timestamps.
                        file.set_modified(
                            metadata.modified().unwrap() + std::time::Duration::from_secs(1),
                        )
                        .unwrap();
                        assert_eq!(file.metadata().unwrap().len(), metadata.len());
                    }
                    _ => unreachable!(),
                }
                Ok(())
            },
        );
        let error = result.unwrap_err().to_string();
        assert!(error.contains("output was published but durability is unconfirmed"));
        if mutation == "unlink" {
            assert!(!target.exists());
            assert_eq!(fixture.entries(), 0);
        } else if mutation == "replace" {
            assert_eq!(fs::read(&target).unwrap(), b"foreign generation");
            assert_eq!(fixture.entries(), 1);
        } else {
            assert!(
                fs::read(&target)
                    .unwrap()
                    .starts_with(b"foreign generation")
            );
            assert_eq!(fixture.entries(), 1);
        }
        assert_eq!(collected(&generation), expected());
    }
}
