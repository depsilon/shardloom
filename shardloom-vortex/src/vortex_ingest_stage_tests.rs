use super::*;
use arrow_array::{Float64Array, Int64Array, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("number", DataType::Int64, true),
                Field::new("renamed_text", DataType::Utf8, true),
            ],
            [("application".to_string(), "preserve metadata".to_string())].into(),
        )),
        vec![
            Arc::new(Int64Array::from(vec![Some(3), None, Some(-8)])),
            Arc::new(StringArray::from(vec![
                Some("茶 and an outlined Unicode string"),
                Some(""),
                None,
            ])),
        ],
    )
    .unwrap()
}

fn shape(batch: &RecordBatch, indices: &[usize]) -> FlatColumnarSourceShape {
    FlatColumnarSourceShape {
        projected_columns: indices
            .iter()
            .map(|&reader_index| ColumnarProjectedColumn {
                column: batch.schema().field(reader_index).name().clone(),
                reader_index,
                dtype_hint: None,
                arrow_dtype_hint: Some(batch.column(reader_index).data_type().clone()),
            })
            .collect(),
    }
}

#[test]
fn identity_projection_preserves_metadata_and_shares_schema_without_rebuilding() {
    for batch in [batch(), batch().slice(0, 0)] {
        for indices in [vec![0, 1], vec![1, 0], vec![1], vec![0, 0]] {
            let expected = batch.project(&indices).unwrap();
            let (actual, identity) =
                project_record_batch_for_vortex(&batch, &shape(&batch, &indices)).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(identity, indices == [0, 1]);
            if identity {
                assert!(Arc::ptr_eq(&actual.schema(), &batch.schema()));
            }
            for (column, &index) in actual.columns().iter().zip(&indices) {
                assert!(Arc::ptr_eq(column, batch.column(index)));
            }
        }
    }
}

#[test]
fn projected_native_artifact_bytes_match_the_original_projection_path() {
    let batch = batch();
    let root = std::env::temp_dir().join(format!(
        "shardloom-projection-proof-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    for (index, indices) in [vec![0, 1], vec![1, 0], vec![1]].iter().enumerate() {
        let original = arrow_record_batch_to_vortex_array(batch.project(indices).unwrap()).unwrap();
        let timing = IngestStageTimings::default();
        let actual = record_batch_to_vortex_from_arrow_provider_profiled(
            &batch,
            &shape(&batch, indices),
            &timing,
        )
        .unwrap();
        assert_eq!(actual.dtype(), original.dtype());
        let expected_path = root.join(format!("original-{index}.vortex"));
        let actual_path = root.join(format!("candidate-{index}.vortex"));
        let decision = admit_layout_write_runtime_decision_for_source(
            None,
            "vortex_array_kernel",
            "ArrayRef::from_arrow(RecordBatch)",
            &actual_path,
            VortexIngestCertificationLevel::IngestCertified,
            VortexWriterPhysicalDesignSourceInput::buffered_columnar(1),
        )
        .unwrap();
        let original_report =
            write_vortex_array(&expected_path, &original, false, &decision).unwrap();
        let actual_report = write_vortex_array(&actual_path, &actual, false, &decision).unwrap();
        assert_eq!(
            actual_report.artifact_digest,
            original_report.artifact_digest
        );
        assert_eq!(
            fs::read(actual_path).unwrap(),
            fs::read(expected_path).unwrap()
        );
        let fields: BTreeMap<_, _> = timing.snapshot().evidence_fields().into_iter().collect();
        assert_eq!(fields["vortex_ingest_stream_arrow_conversion_rows"], "3");
        assert_eq!(
            fields["vortex_ingest_identity_projection_batches"],
            if index == 0 { "1" } else { "0" }
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn identity_projection_does_not_skip_stream_validation_before_conversion() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "renamed_metric",
        DataType::Float64,
        true,
    )]));
    let good = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Float64Array::from(vec![1.0]))],
    )
    .unwrap();
    let bad = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Float64Array::from(vec![f64::NAN]))],
    )
    .unwrap();
    for window in [0, 2] {
        let source_shape = shape(&good, &[0]);
        let first = record_batch_to_vortex_from_arrow_provider(&good, &source_shape).unwrap();
        let timing = VortexStreamingIngestTiming::default();
        let mut stream = StreamingColumnarVortexArrayIterator::new(
            first.dtype().clone(),
            first,
            Box::new(RecordBatchIterator::new(
                vec![Ok(bad.clone())],
                Arc::clone(&schema),
            )),
            vec!["renamed_metric".into()],
            source_shape,
            Arc::new(AtomicUsize::new(1)),
            timing.clone(),
            2,
            window,
            window,
            1_048_576,
            None,
        )
        .unwrap();
        assert!(stream.next().unwrap().is_ok());
        assert!(
            stream
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("non-finite")
        );
        assert!(stream.next().is_none());
        let fields: BTreeMap<_, _> = timing
            .stages
            .snapshot()
            .evidence_fields()
            .into_iter()
            .collect();
        assert_eq!(fields["vortex_ingest_stream_validation_calls"], "1");
        assert_eq!(fields["vortex_ingest_stream_arrow_conversion_calls"], "0");
        assert_eq!(fields["vortex_ingest_identity_projection_batches"], "0");
    }
}
