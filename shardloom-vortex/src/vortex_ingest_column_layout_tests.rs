use super::column_layout::{StreamFooterLayout, stream_options};
use super::*;
#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "vortex_ingest_column_layout_bench.rs"]
mod benchmark;
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::{DType, FieldNames, Nullability},
        iter::ArrayIteratorAdapter,
        scalar::Scalar,
        validity::Validity,
    },
    expr::stats::Stat,
    file::OpenOptionsSessionExt as _,
    io::runtime::BlockingRuntime as _,
    layout::{
        LayoutRef,
        layouts::{chunked::Chunked, struct_::Struct},
    },
};

const ROWS: usize = 1033;
const GROUPS: usize = 3;
const BASE: i64 = 1_i64 << 60;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shardloom-column-compositions-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone, Copy)]
enum Composition {
    Default,
    FastLoad,
    Balanced,
    SourceText,
}

fn decision(composition: Composition, path: &Path) -> VortexLayoutWriteRuntimeDecision {
    let mut input = tests::layout_advisor_input(true, "none");
    input.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
    input.source_byte_count = 1_073_741_824;
    input.writer_parallelism_budget = 2;
    input.workload_constitution =
        "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=counter_olap;layout_family=counter_typed_stats_layout;text_domain=false;time_bucket=false;counter=true;key_profile=high_cardinality_numeric_keys;dictionary=columnar_dictionary_status_provider_dependent".into();
    if matches!(composition, Composition::SourceText) {
        input.workload_constitution =
            "product_vortex_prepare_once;format=parquet;scale=large_olap;adapter=streaming_columnar_source_state;profile=url_time_counter_olap;layout_family=url_time_counter_dictionary_stats_layout;text_domain=true;time_bucket=true;counter=true;key_profile=high_cardinality_numeric_text_time_keys;dictionary=source_dictionary_or_derived_dictionary_evidence".into();
        input.writer_compression_candidate_fields = vec!["renamed_payload".into()];
    }
    let advisor = evaluate_vortex_layout_write_advisor(input);
    let mut decision = VortexLayoutWriteRuntimeDecision::applied(
        &advisor,
        path,
        "vortex_array_kernel",
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator",
        VortexIngestCertificationLevel::IngestCertified,
    );
    // Exercise the native default and existing balanced composition directly,
    // without pretending this small fixture passed ordinary large-source admission.
    match composition {
        Composition::Default => decision.runtime_decision_applied = false,
        Composition::Balanced => {
            decision.writer_compression_policy =
                VORTEX_PREPARED_OLAP_WRITER_BALANCED_LARGE_SOURCE_COMPRESSION_POLICY.into();
            decision.writer_compression_concurrency =
                VORTEX_PREPARED_OLAP_WRITER_DEFAULT_COMPRESSION_CONCURRENCY;
            assert!(vortex_writer_uses_large_source_balanced(&decision));
        }
        Composition::FastLoad => assert!(vortex_writer_uses_large_source_fast_load(&decision)),
        Composition::SourceText => assert!(vortex_writer_uses_large_source_text(&decision)),
    }
    decision
}

fn label(group: usize, local: usize) -> Option<&'static str> {
    match (group % 2, local % 3) {
        (0, 0) | (1, 1) => Some("a-東京"),
        (0, 2) | (1, 0) => Some("z-λ"),
        _ => None,
    }
}

fn payload(row: usize) -> Option<String> {
    (!row.is_multiple_of(11)).then(|| {
        format!(
            "ordinary renamed text {} / 港-λ / {}",
            row % 17,
            "x".repeat(row % 53)
        )
    })
}

fn batch(group: usize) -> ArrayRef {
    let domain = if group.is_multiple_of(2) {
        [Some("a-東京"), None, Some("z-λ")]
    } else {
        [Some("z-λ"), Some("a-東京"), None]
    };
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(
            (0..ROWS)
                .map(|row| u8::try_from(row % 3).unwrap())
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        VarBinViewArray::from_iter_nullable_str(domain).into_array(),
    )
    .unwrap()
    .into_array();
    StructArray::try_new(
        FieldNames::from([
            "exact_identifier",
            "narrow_nullable",
            "renamed_category",
            "renamed_payload",
        ]),
        vec![
            PrimitiveArray::new(
                (0..ROWS)
                    .map(|row| BASE + i64::try_from(group * ROWS + row).unwrap())
                    .collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::from_option_iter((0..ROWS).map(|local| {
                let row = group * ROWS + local;
                (!row.is_multiple_of(7)).then(|| i16::try_from(row % 67).unwrap() - 33)
            }))
            .into_array(),
            dictionary,
            VarBinViewArray::from_iter_nullable_str(
                (0..ROWS).map(|row| payload(group * ROWS + row)),
            )
            .into_array(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn assert_row(scalar: &Scalar, row: usize) {
    let fields = scalar.as_struct();
    assert_eq!(
        fields.field("exact_identifier"),
        Some(Scalar::from(BASE + i64::try_from(row).unwrap()))
    );
    let narrow = if row.is_multiple_of(7) {
        Scalar::null(DType::Primitive(
            vortex::array::dtype::PType::I16,
            Nullability::Nullable,
        ))
    } else {
        Scalar::primitive(i16::try_from(row % 67).unwrap() - 33, Nullability::Nullable)
    };
    assert_eq!(fields.field("narrow_nullable"), Some(narrow));
    for (name, expected) in [
        (
            "renamed_category",
            label(row / ROWS, row % ROWS).map(str::to_string),
        ),
        ("renamed_payload", payload(row)),
    ] {
        let expected = expected.map_or_else(
            || Scalar::null(DType::Utf8(Nullability::Nullable)),
            |value| Scalar::utf8(value, Nullability::Nullable),
        );
        assert_eq!(fields.field(name), Some(expected));
    }
}

fn assert_values(file: &vortex::file::VortexFile, context: &LocalVortexWriteContext) {
    let mut execution = context.session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for local in 0..array.len() {
            assert_row(&array.execute_scalar(local, &mut execution).unwrap(), seen);
            seen += 1;
        }
    }
    assert_eq!(seen, ROWS * GROUPS);
}

fn subtree(layout: &LayoutRef) -> String {
    let children = (0..layout.nslots())
        .map(|slot| layout.slot(slot).unwrap().as_ref().map(subtree))
        .collect::<Vec<_>>();
    format!(
        "{:?}/{:?}/{}/{:?}/{:?}/{children:?}",
        layout.encoding_id(),
        layout.dtype(),
        layout.row_count(),
        layout.metadata(),
        layout.segment_ids()
    )
}

fn artifact_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = sha2::Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(hex, "{byte:02x}").unwrap();
    }
    hex
}

#[derive(Debug, PartialEq, Eq)]
struct ArtifactProof {
    payload_prefix: Vec<u8>,
    geometry: Vec<(u64, u64, String)>,
    field_subtrees: Vec<Vec<String>>,
    statistics: String,
}

fn artifact_proof(file: &vortex::file::VortexFile, bytes: &[u8], candidate: bool) -> ArtifactProof {
    let geometry = file
        .footer()
        .segment_map()
        .iter()
        .map(|spec| {
            (
                spec.offset,
                u64::from(spec.length),
                format!("{:?}", spec.alignment),
            )
        })
        .collect::<Vec<_>>();
    assert!(!geometry.is_empty());
    let end = geometry
        .iter()
        .map(|(offset, length, _)| offset + length)
        .max()
        .unwrap();
    let root = file.footer().layout();
    let mut fields = vec![Vec::new(); 4];
    if candidate {
        assert!(root.is::<Struct>());
        for (field, target) in fields.iter_mut().enumerate() {
            let column = root.slot(field + 1).unwrap().unwrap();
            assert!(column.is::<Chunked>());
            assert_eq!(column.nchildren(), GROUPS);
            for group in 0..GROUPS {
                target.push(subtree(&column.slot(group).unwrap().unwrap()));
            }
        }
    } else {
        assert!(root.is::<Chunked>());
        assert_eq!(root.nchildren(), GROUPS);
        for group in 0..GROUPS {
            let batch = root.slot(group).unwrap().unwrap();
            assert!(batch.is::<Struct>());
            for (field, target) in fields.iter_mut().enumerate() {
                target.push(subtree(&batch.slot(field + 1).unwrap().unwrap()));
            }
        }
    }
    ArtifactProof {
        payload_prefix: bytes[..usize::try_from(end).unwrap()].to_vec(),
        geometry,
        field_subtrees: fields,
        statistics: statistics_proof(file),
    }
}

fn statistics_proof(file: &vortex::file::VortexFile) -> String {
    let statistics = file.footer().statistics().unwrap();
    assert_eq!(statistics.stats_sets().len(), 4);
    let (identifiers, _) = statistics.get(0);
    assert_eq!(
        identifiers.get(Stat::Min).as_exact(),
        Scalar::from(BASE).into_value()
    );
    assert_eq!(
        identifiers.get(Stat::Max).as_exact(),
        Scalar::from(BASE + i64::try_from(ROWS * GROUPS - 1).unwrap()).into_value()
    );
    for (field, nulls) in [
        (
            1,
            (0..ROWS * GROUPS)
                .filter(|row| row.is_multiple_of(7))
                .count(),
        ),
        (
            2,
            (0..ROWS * GROUPS)
                .filter(|row| label(row / ROWS, row % ROWS).is_none())
                .count(),
        ),
        (
            3,
            (0..ROWS * GROUPS)
                .filter(|row| row.is_multiple_of(11))
                .count(),
        ),
    ] {
        assert_eq!(
            statistics.get(field).0.get(Stat::NullCount).as_exact(),
            Scalar::from(u64::try_from(nulls).unwrap()).into_value()
        );
    }
    format!(
        "{:?}/{:?}",
        statistics.dtypes(),
        statistics
            .stats_sets()
            .iter()
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>()
    )
}

fn write_composition(
    context: &LocalVortexWriteContext,
    path: &Path,
    composition: Composition,
    choice: StreamFooterLayout,
) -> ArtifactProof {
    let memory = NativeIngestMemory::new(32 << 20).unwrap();
    let decision = decision(composition, path);
    let _drivers =
        crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
    let timing = VortexWriterStageTiming::default();
    // Fresh arrays prevent either writer inheriting cached statistics from its pair.
    let batches = (0..GROUPS).map(batch).collect::<Vec<_>>();
    let dtype = batches[0].dtype().clone();
    let (options, evidence) =
        stream_options(context, &decision, &timing, Some(&memory), &dtype, choice).unwrap();
    let summary = options
        .blocking(&context.runtime)
        .write(
            fs::File::create(path).unwrap(),
            ArrayIteratorAdapter::new(dtype.clone(), batches.into_iter().map(Ok)),
        )
        .unwrap();
    assert_eq!(summary.row_count(), u64::try_from(ROWS * GROUPS).unwrap());
    assert!(
        memory.pool.snapshot().reserved_bytes > 0,
        "footer references outlive the strategy"
    );
    fs::File::open(path).unwrap().sync_all().unwrap();
    let bytes = fs::read(path).unwrap();
    assert!(bytes.len() < 4 << 20);
    let digest = artifact_digest(&bytes);
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(path))
        .unwrap();
    assert_eq!(file.dtype(), &dtype);
    assert_eq!(file.row_count(), summary.row_count());
    assert_values(&file, context);
    let candidate = choice == StreamFooterLayout::ColumnAddressable;
    let proof = artifact_proof(&file, &bytes, candidate);
    let mut applied = String::new();
    evidence.append_to(&mut applied);
    if candidate {
        let snapshot = evidence.counters.as_ref().unwrap().snapshot();
        assert_eq!(snapshot.input_groups, 3);
        assert_eq!(snapshot.child_writer_calls, 3);
        assert_eq!(snapshot.transposed_references, 12);
        assert!(snapshot.peak_reference_bytes > 0);
        assert!(applied.contains(
            "footer_layout=native_struct_column_chunked_preserved_subtrees;actual_nonempty_groups=3"
        ));
    } else {
        assert!(applied.is_empty());
    }
    drop(file);
    drop(summary);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    assert_eq!(memory.pool.snapshot().denied_reservations, 0);
    eprintln!(
        "COLUMN_LAYOUT_COMPOSITION_PROOF {}",
        serde_json::json!({
            "composition": format!("{composition:?}"), "footer_layout": format!("{choice:?}"),
            "rows": ROWS * GROUPS, "bytes": bytes.len(), "artifact_sha256": digest,
            "verification": "full_independent_values_payload_bytes_subtrees_geometry_default_file_stats",
            "scope": "forced_small_fixture_writer_composition_not_large_source_admission_or_timing",
            "writer_background_drivers": 1, "caller": 1,
        })
    );
    proof
}

#[test]
fn column_footer_actual_writer_compositions_keep_payloads_subtrees_stats_and_values() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for composition in [
            Composition::Default,
            Composition::FastLoad,
            Composition::Balanced,
            Composition::SourceText,
        ] {
            let retained = write_composition(
                &context,
                &directory.0.join(format!("{composition:?}-retained.vortex")),
                composition,
                StreamFooterLayout::RetainedRows,
            );
            let candidate = write_composition(
                &context,
                &directory.0.join(format!("{composition:?}-columns.vortex")),
                composition,
                StreamFooterLayout::ColumnAddressable,
            );
            assert_eq!(
                retained, candidate,
                "composition {composition:?}: only the outer footer hierarchy may differ"
            );
        }
    });
}

#[test]
fn column_footer_private_seam_keeps_default_and_inadmissible_schema_on_retained_writer() {
    assert_eq!(
        column_layout::DEFAULT_STREAM_FOOTER_LAYOUT,
        StreamFooterLayout::RetainedRows
    );
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let memory = NativeIngestMemory::new(8 << 20).unwrap();
        let timing = VortexWriterStageTiming::default();
        let path = directory.0.join("nested-root.vortex");
        let decision = decision(Composition::Default, &path);
        // Nested fields decline the candidate, but are valid for the retained
        // writer. Its default file-statistics provider cannot write nullable
        // top-level structs, so do not use that unsupported shape as the oracle.
        let input = StructArray::try_new(
            FieldNames::from(["nested"]),
            vec![batch(0)],
            ROWS,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let dtype = input.dtype().clone();
        let (options, evidence) = stream_options(
            &context,
            &decision,
            &timing,
            Some(&memory),
            &dtype,
            StreamFooterLayout::ColumnAddressable,
        )
        .unwrap();
        assert_eq!(
            evidence.status,
            "retained_source_batch_rows_candidate_schema_not_admitted"
        );
        assert!(evidence.counters.is_none());
        let summary = options
            .blocking(&context.runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(dtype.clone(), [Ok(input)].into_iter()),
            )
            .unwrap();
        let file = context
            .runtime
            .block_on(context.session.open_options().open_path(&path))
            .unwrap();
        assert_eq!(file.dtype(), &dtype);
        assert!(file.footer().layout().is::<Chunked>());
        let mut execution = context.session.create_execution_ctx();
        let mut count = 0_usize;
        for array in file
            .scan()
            .unwrap()
            .with_ordered(true)
            .into_array_iter(&context.runtime)
            .unwrap()
        {
            let array = array.unwrap();
            for row in 0..array.len() {
                let actual = array.execute_scalar(row, &mut execution).unwrap();
                assert_row(&actual.as_struct().field("nested").unwrap(), count);
                count += 1;
            }
        }
        assert_eq!(count, ROWS);
        drop(file);
        drop(execution);
        drop(summary);
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
        let (options, evidence) = stream_options(
            &context,
            &decision,
            &timing,
            None,
            &dtype,
            StreamFooterLayout::ColumnAddressable,
        )
        .unwrap();
        assert_eq!(
            evidence.status,
            "retained_writer_candidate_requires_shared_memory"
        );
        drop(options);
    });
}
