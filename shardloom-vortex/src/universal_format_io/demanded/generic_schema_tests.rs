use super::*;
use arrow_array::{Array, Int64Array};

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
