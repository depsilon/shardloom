use super::*;
use arrow_array::{Array, Int64Array};

#[test]
fn columnar_identity_projection_reuses_the_admitted_schema_without_new_capacity() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let held = memory.reserve(1).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let (projected, credit) = project(&schema, &[0], &memory).unwrap();
    assert!(Arc::ptr_eq(&schema, &projected));
    assert!(credit.is_none());
    drop((held, projected));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_projection_admits_metadata_copies_and_preserves_field_order_and_sharing() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ],
        [("large".into(), "x".repeat(64 << 10))].into(),
    ));
    let denied = LiveMemoryPool::new(32 << 10).unwrap();
    let error = project(&schema, &[1, 0], &denied).unwrap_err();
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    assert_eq!(denied.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (projected, credit) = project(&schema, &[1, 0, 1], &memory).unwrap();
    assert_eq!(projected.metadata(), schema.metadata());
    assert!(Arc::ptr_eq(&projected.fields()[0], &schema.fields()[1]));
    assert!(Arc::ptr_eq(&projected.fields()[1], &schema.fields()[0]));
    assert!(Arc::ptr_eq(&projected.fields()[2], &schema.fields()[1]));
    assert!(memory.snapshot().reserved_bytes > 64 << 10);
    drop((projected, credit));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(project(&schema, &[2], &memory).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn owned_input(memory: &LiveMemoryPool) -> OwnedBatch {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..4096))],
    )
    .unwrap();
    let credit = memory
        .reserve(batch.get_array_memory_size() as u64)
        .unwrap();
    crate::universal_format_io::text_stream::credited_batch(batch, credit).unwrap()
}

#[test]
fn columnar_adaptive_alignment_keeps_existing_credit_without_charging_payload_twice() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let input = owned_input(&memory);
    let before = memory.snapshot().reserved_bytes;
    let held_column = input.column(0).clone();
    let schema = input.schema();
    let output = align(input, schema, &[], &memory, true).unwrap();
    let added = memory.snapshot().reserved_bytes - before;
    assert!(
        added < 4096,
        "alignment charged {added} bytes for already owned buffers"
    );
    drop(held_column);
    let values = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &(0..4096).collect::<Vec<i64>>());
    let child = values.values().inner().clone();
    drop(output);
    assert!(memory.snapshot().reserved_bytes >= before);
    drop(child);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_adaptive_alignment_credits_new_null_and_partition_buffers_and_retains_owners() {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let input = owned_input(&memory);
    let before = memory.snapshot().reserved_bytes;
    let target = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
        Field::new("missing", DataType::Int64, true),
    ]));
    let output = align(
        input,
        target,
        &[("part".into(), Some("東京/λ".into()))],
        &memory,
        true,
    )
    .unwrap();
    assert!(memory.snapshot().reserved_bytes > before);
    let partition = output
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(partition.iter().all(|value| value == Some("東京/λ")));
    assert_eq!(output.column(2).null_count(), 4096);
    let retained = output
        .map(|batch| Ok::<_, arrow_schema::ArrowError>(batch.slice(4094, 2)))
        .unwrap();
    assert_eq!(
        retained
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[4094, 4095]
    );
    assert!(memory.snapshot().reserved_bytes > before);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn columnar_adaptive_alignment_does_not_spend_existing_credit_on_a_new_partition_allocation() {
    let memory = LiveMemoryPool::new(128 << 10).unwrap();
    let input = owned_input(&memory);
    let target = Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)]));
    let error = align(
        input,
        target,
        &[("part".into(), Some("x".repeat(1024)))],
        &memory,
        true,
    )
    .unwrap_err();
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn align_owned_parquet_accepts_dictionary_utf8_for_utf8_and_utf8_view_targets() {
    use arrow_array::{DictionaryArray, Int32Array, types::Int32Type};

    let dict_values = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
    let dict_keys = Int32Array::from(vec![Some(0), Some(1), None, Some(0)]);
    let dictionary =
        Arc::new(DictionaryArray::<Int32Type>::try_new(dict_keys, dict_values).unwrap())
            as ArrayRef;
    let dict_dtype = dictionary.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", dict_dtype.clone(), true)])),
        vec![dictionary],
    )
    .unwrap();

    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    for target_type in [DataType::Utf8, DataType::Utf8View] {
        let target = Arc::new(Schema::new(vec![Field::new("s", target_type, true)]));
        let aligned =
            align_owned_parquet(OwnedBatch::from(batch.clone()), target, &[], &memory).unwrap();
        assert_eq!(aligned.schema().field(0).data_type(), &dict_dtype);
        assert_eq!(aligned.num_rows(), 4);
    }

    let wrong_utf8_target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::LargeUtf8,
        true,
    )]));
    let large_err = align_owned_parquet(
        OwnedBatch::from(batch.clone()),
        wrong_utf8_target,
        &[],
        &memory,
    )
    .unwrap_err();
    assert!(
        large_err
            .to_string()
            .contains("decoded field type changed after source schema admission"),
        "{large_err}"
    );

    let view_target = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
    let non_owned_err =
        align(OwnedBatch::from(batch), view_target, &[], &memory, false).unwrap_err();
    assert!(
        non_owned_err
            .to_string()
            .contains("decoded field type changed after source schema admission"),
        "{non_owned_err}"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
