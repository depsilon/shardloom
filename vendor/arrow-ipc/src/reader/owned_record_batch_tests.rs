// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::reader::{
    RecordBatchDecoder, controlled_array_tests::shapes, decoder_memory_tests::Memory,
};
use arrow_array::{types::Int8Type, *};
use arrow_schema::{DataType, Field, Schema};

fn decode(
    batch: &RecordBatch,
    memory: &Memory,
    projection: Option<&[usize]>,
) -> Result<OwnedRecordBatch, ArrowError> {
    let mut tracker = crate::writer::DictionaryTracker::new(false);
    let encoded_schema = crate::writer::IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            &batch.schema(),
            &mut tracker,
            &Default::default(),
        );
    let schema_message = crate::root_as_message(&encoded_schema.ipc_message).unwrap();
    let schema = Arc::new(crate::convert::fb_to_schema(
        schema_message.header_as_schema().unwrap(),
    ));
    let (_, encoded) = crate::writer::IpcDataGenerator::default().encode(
        batch,
        &mut tracker,
        &Default::default(),
        &mut Default::default(),
    )?;
    let message = crate::root_as_message(&encoded.ipc_message).unwrap();
    RecordBatchDecoder::try_new(
        &Buffer::from_vec(encoded.arrow_data),
        message.header_as_record_batch().unwrap(),
        schema,
        &Default::default(),
        &message.version(),
    )?
    .with_projection(projection)
    .with_decoder_memory(Arc::new(memory.clone()))
    .read_owned_record_batch()
}

#[test]
fn owned_structures_cover_bufferless_batches_and_last_metadata_alias() {
    let batches = [
        RecordBatch::try_from_iter([("empty-values", Arc::new(NullArray::new(7)) as ArrayRef)])
            .unwrap(),
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap(),
        RecordBatch::try_from_iter([(
            "empty-record",
            Arc::new(StructArray::new_empty_fields(7, None)) as ArrayRef,
        )])
        .unwrap(),
    ];
    for batch in batches {
        let memory = Memory::new(1 << 20);
        let owned = decode(&batch, &memory, None).unwrap();
        assert_eq!(owned.batch(), &batch);
        let (decoded, credit) = owned.into_parts();
        let slice = decoded.slice(2, 3);
        drop(decoded);
        assert_eq!(slice.num_rows(), 3);
        assert!(memory.current() > 0);
        let last = credit.clone();
        drop(credit);
        drop(slice);
        assert!(memory.current() > 0);
        drop(last);
        assert_eq!(memory.current(), 0);
    }
}

#[test]
fn owned_structures_preserve_nested_values_and_duplicate_projection_order() {
    for array in shapes() {
        let rows = array.len();
        let batch = RecordBatch::try_from_iter([
            ("value", array),
            ("none", Arc::new(NullArray::new(rows)) as ArrayRef),
        ])
        .unwrap();
        let memory = Memory::new(4 << 20);
        let owned = decode(&batch, &memory, Some(&[1, 0, 1, 0])).unwrap();
        assert_eq!(owned.batch(), &batch.project(&[1, 0, 1, 0]).unwrap());
        assert!(memory.current() > 0);
        assert!(
            memory.peak() > memory.current(),
            "construction scratch must be released"
        );
        drop(owned);
        assert_eq!(memory.current(), 0);
    }
}

#[test]
fn owned_structures_deny_and_cancel_before_construction_and_refund_scratch() {
    let batch = RecordBatch::try_from_iter([(
        "values",
        Arc::new(Int32Array::from(vec![11, 13])) as ArrayRef,
    )])
    .unwrap();
    let generous = Memory::new(1 << 20);
    let owned = decode(&batch, &generous, None).unwrap();
    let retained = generous.current();
    let peak = generous.peak();
    drop(owned);
    assert!(peak > retained);
    for limit in [0, retained, peak - 1] {
        let memory = Memory::new(limit);
        assert!(
            decode(&batch, &memory, None)
                .unwrap_err()
                .to_string()
                .contains("denied")
        );
        assert_eq!(memory.current(), 0);
    }
    let memory = Memory::new(peak);
    memory.cancel();
    assert!(
        decode(&batch, &memory, None)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!(memory.current(), 0);
    let retry = Memory::new(peak);
    assert_eq!(decode(&batch, &retry, None).unwrap().batch(), &batch);
    assert_eq!(retry.current(), 0);
}

#[test]
fn omitted_all_null_dictionary_owns_its_synthesized_values() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![None, None, None]),
        Arc::new(StringArray::from(Vec::<String>::new())),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter([("dict", Arc::new(dictionary) as ArrayRef)]).unwrap();
    let memory = Memory::new(1 << 20);
    // decode intentionally does not supply the encoder's dictionary message.
    let owned = decode(&batch, &memory, None).unwrap();
    assert_eq!(owned.batch(), &batch);
    let (decoded, credit) = owned.into_parts();
    assert!(memory.current() > 0);
    drop(decoded);
    drop(credit);
    assert_eq!(memory.current(), 0);
}

#[test]
fn structure_inspection_accounts_for_sparse_union_ids_and_logical_null_scratch() {
    use crate::reader::array_structure;
    use arrow_schema::{UnionFields, UnionMode};
    let narrow = DataType::Union(
        UnionFields::try_new([0], [Field::new("n", DataType::Null, true)]).unwrap(),
        UnionMode::Sparse,
    );
    let wide = DataType::Union(
        UnionFields::try_new([127], [Field::new("n", DataType::Null, true)]).unwrap(),
        UnionMode::Sparse,
    );
    let a = array_structure::node(&narrow, 1, 0, 1).unwrap();
    let b = array_structure::node(&wide, 1, 0, 1).unwrap();
    assert!(
        b.retained > a.retained,
        "union slots follow max id, not field count"
    );
    let long = array_structure::node(&wide, 1_000_001, 0, 1).unwrap();
    assert_eq!(
        long.retained, b.retained,
        "row validation masks are transient"
    );
    assert!(long.temporary > b.temporary + 125_000);
    assert!(array_structure::node(&wide, usize::MAX, usize::MAX, 129).is_err());
}

#[test]
fn nonnullable_nested_validation_keeps_null_and_expanded_list_masks_admitted() {
    let rows = 4097;
    let structure = StructArray::try_new_with_length(
        vec![Field::new("missing", DataType::Null, false)].into(),
        vec![Arc::new(NullArray::new(rows))],
        Some(NullBuffer::new_null(rows)),
        rows,
    )
    .unwrap();
    let fixed = FixedSizeListArray::try_new(
        Arc::new(Field::new("value", DataType::Int32, false)),
        3,
        Arc::new(Int32Array::new_null(rows * 3)),
        Some(NullBuffer::new_null(rows)),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter([
        ("struct", Arc::new(structure) as ArrayRef),
        ("fixed", Arc::new(fixed) as ArrayRef),
    ])
    .unwrap();
    let memory = Memory::new(1 << 20);
    let decoded = decode(&batch, &memory, None).unwrap();
    assert_eq!(decoded.batch(), &batch);
    assert!(memory.peak() > memory.current() + (rows * 3).div_ceil(8));
    drop(decoded);
    assert_eq!(memory.current(), 0);
}
