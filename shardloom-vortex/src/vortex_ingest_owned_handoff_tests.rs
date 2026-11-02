//! Real validated-copy conversion must overlap its independent input owner.

use super::*;
use arrow_schema::{DataType, Field, Schema};

fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..128))],
    )
    .unwrap()
}

fn shape() -> FlatColumnarSourceShape {
    FlatColumnarSourceShape {
        projected_columns: vec![ColumnarProjectedColumn {
            column: "n".into(),
            reader_index: 0,
            dtype_hint: None,
            arrow_dtype_hint: Some(DataType::Int64),
        }],
    }
}

#[test]
fn conversion_keeps_source_structure_credit_until_its_owned_copy_finishes() {
    let memory = NativeIngestMemory::new(1 << 20).unwrap();
    let mut input_lease = memory.reserve_input(1).unwrap();
    let owner = memory
        .pool
        .reserve(memory.pool.snapshot().limit_bytes - input_lease.bytes())
        .unwrap();
    // The payload itself has no credited buffers. Only the explicit handoff
    // owner represents the retained source allocation, so releasing it at
    // projection would incorrectly make room for this otherwise denied copy.
    let batch = OwnedBatch::with_owner(input(), Arc::new(owner));
    let result = owned_record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
        batch,
        &shape(),
        &IngestStageTimings::default(),
        Some((&memory, &mut input_lease)),
    );
    assert!(result.is_err());
    assert!(memory.pool.snapshot().denied_reservations > 0);
    assert_eq!(memory.pool.snapshot().reserved_bytes, input_lease.bytes());
    drop(input_lease);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);

    // Same conversion with actual free capacity succeeds and its result owns
    // the copied buffers after the conversion admission lease is released.
    let mut input_lease = memory.reserve_input(1).unwrap();
    let array = owned_record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
        input(),
        &shape(),
        &IngestStageTimings::default(),
        Some((&memory, &mut input_lease)),
    )
    .unwrap();
    assert_eq!(array.len(), 128);
    drop(input_lease);
    assert!(memory.pool.snapshot().reserved_bytes > 0);
    drop(array);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
}
