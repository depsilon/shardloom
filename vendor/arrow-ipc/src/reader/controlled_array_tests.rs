// SPDX-License-Identifier: Apache-2.0

use super::{RecordBatchDecoder, decoder_memory_tests::Memory};
use arrow_array::{builder::*, cast::AsArray, types::*, *};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, UnionFields};
use std::sync::Arc;

fn roundtrip(
    batch: &RecordBatch,
    shift: usize,
    memory: &Memory,
    strict: bool,
) -> Result<RecordBatch, ArrowError> {
    let mut tracker = crate::writer::DictionaryTracker::new(false);
    let (_, encoded) = crate::writer::IpcDataGenerator::default().encode(
        batch,
        &mut tracker,
        &Default::default(),
        &mut Default::default(),
    )?;
    let message = crate::root_as_message(&encoded.ipc_message).unwrap();
    let mut body = MutableBuffer::new(encoded.arrow_data.len() + shift);
    body.resize(shift, 0);
    body.extend_from_slice(&encoded.arrow_data);
    let body = Buffer::from(body).slice(shift);
    RecordBatchDecoder::try_new(
        &body,
        message.header_as_record_batch().unwrap(),
        batch.schema(),
        &Default::default(),
        &message.version(),
    )?
    .with_decoder_memory(Arc::new(memory.clone()))
    .with_require_alignment(strict)
    .read_record_batch()
}

pub(super) fn shapes() -> Vec<ArrayRef> {
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(11), None, Some(13)]));
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(11), None]),
        None,
        Some(vec![Some(13)]),
    ]);
    let large_list = LargeListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(11), None]),
        None,
        Some(vec![Some(13)]),
    ]);
    let view = ListViewArray::new(
        field.clone(),
        vec![1, 0].into(),
        vec![2, 2].into(),
        values.clone(),
        None,
    );
    let large_view =
        LargeListViewArray::new(field, vec![1, 0].into(), vec![2, 2].into(), values, None);
    let fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("i", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ],
    )
    .unwrap();
    let union = UnionArray::try_new(
        fields,
        vec![0_i8, 1, 0].into(),
        Some(vec![0, 0, 1].into()),
        vec![
            Arc::new(Int32Array::from(vec![17, 19])),
            Arc::new(StringArray::from(vec!["u"])),
        ],
    )
    .unwrap();
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map.keys().append_value("key");
    map.values().append_value(7);
    map.append(true).unwrap();
    map.append(false).unwrap();
    vec![
        Arc::new(Int64Array::from(vec![Some(-3), None, Some(i64::MAX)])),
        Arc::new(
            Decimal128Array::from(vec![Some(12345), None])
                .with_precision_and_scale(18, 3)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![
            Some("alpha"),
            None,
            Some("long variable text"),
        ])),
        Arc::new(LargeStringArray::from(vec![Some("large offsets"), None])),
        Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("external view bytes require their own buffer"),
            None,
        ])),
        Arc::new(list),
        Arc::new(large_list),
        Arc::new(view),
        Arc::new(large_view),
        Arc::new(union),
        Arc::new(map.finish()),
    ]
}

#[test]
fn controlled_alignment_preserves_all_values_and_owned_retained_arrays() {
    for array in shapes() {
        let batch = RecordBatch::try_from_iter([("value", array)]).unwrap();
        let memory = Memory::new(1024 * 1024);
        let aligned = roundtrip(&batch, 0, &memory, false).unwrap();
        assert_eq!(aligned, batch);
        assert_eq!(memory.current(), 0, "aligned payload should stay shared");
        drop(aligned);
        let decoded = roundtrip(&batch, 1, &memory, false).unwrap();
        assert_eq!(decoded, batch);
        assert!(memory.current() > 0, "{}", batch.schema());
        let retained = decoded.column(0).slice(0, decoded.num_rows());
        drop(decoded);
        assert_eq!(retained.as_ref(), batch.column(0).as_ref());
        assert!(memory.current() > 0);
        drop(retained);
        assert_eq!(memory.current(), 0);
        assert!(roundtrip(&batch, 1, &memory, true).is_err());
        assert_eq!(memory.current(), 0);
    }
}

#[test]
fn controlled_alignment_denies_before_copy_and_preserves_source() {
    let batch = RecordBatch::try_from_iter([(
        "values",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap();
    let memory = Memory::new(0);
    assert!(
        roundtrip(&batch, 1, &memory, false)
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert_eq!(memory.current(), 0);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .as_ref(),
        &[1, 2, 3]
    );
    assert_eq!(roundtrip(&batch, 0, &memory, false).unwrap(), batch);
}

fn malformed(
    schema: SchemaRef,
    length: i64,
    nodes: &[crate::FieldNode],
    buffers: &[crate::Buffer],
    variadic: &[i64],
    body: &[u8],
    projection: Option<&[usize]>,
) -> Result<RecordBatch, ArrowError> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let nodes = fbb.create_vector(nodes);
    let buffers = fbb.create_vector(buffers);
    let variadic = fbb.create_vector(variadic);
    let batch = crate::RecordBatch::create(
        &mut fbb,
        &crate::RecordBatchArgs {
            length,
            nodes: Some(nodes),
            buffers: Some(buffers),
            variadicBufferCounts: Some(variadic),
            ..Default::default()
        },
    );
    fbb.finish_minimal(batch);
    let batch = flatbuffers::root::<crate::RecordBatch>(fbb.finished_data()).unwrap();
    RecordBatchDecoder::try_new(
        &Buffer::from_slice_ref(body),
        batch,
        schema,
        &Default::default(),
        &crate::MetadataVersion::V5,
    )?
    .with_projection(projection)
    .with_decoder_memory(Arc::new(Memory::new(1024)))
    .read_record_batch()
}

#[test]
fn malformed_fixed_list_extent_returns_error_without_panicking() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Null, true)), 3),
        true,
    )]));
    let outcome = std::panic::catch_unwind(|| {
        malformed(
            schema,
            i64::MAX,
            &[
                crate::FieldNode::new(i64::MAX, 0),
                crate::FieldNode::new(0, 0),
            ],
            &[crate::Buffer::new(0, 0)],
            &[],
            &[],
            None,
        )
    });
    assert!(outcome.is_ok(), "untrusted list extent must not panic");
    assert!(outcome.unwrap().is_err());
}

#[test]
fn malformed_struct_length_cannot_be_replaced_by_its_child_length() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "record",
        DataType::Struct(vec![Field::new("value", DataType::Int32, true)].into()),
        true,
    )]));
    let outcome = malformed(
        schema,
        2,
        &[crate::FieldNode::new(3, 0), crate::FieldNode::new(2, 0)],
        &[
            crate::Buffer::new(0, 0),
            crate::Buffer::new(0, 0),
            crate::Buffer::new(0, 8),
        ],
        &[],
        &[0; 8],
        None,
    );
    assert!(outcome.is_err(), "declared parent length must be validated");
}

#[test]
fn malformed_extents_counts_and_projected_missing_buffers_return_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
    for invalid in [
        crate::Buffer::new(-1, 4),
        crate::Buffer::new(0, -1),
        crate::Buffer::new(2, 4),
        crate::Buffer::new(i64::MAX, i64::MAX),
    ] {
        assert!(
            malformed(
                schema.clone(),
                1,
                &[crate::FieldNode::new(1, 0)],
                &[crate::Buffer::new(0, 0), invalid],
                &[],
                &[0; 4],
                None
            )
            .is_err()
        );
    }
    for node in [
        crate::FieldNode::new(-1, 0),
        crate::FieldNode::new(1, -1),
        crate::FieldNode::new(1, 2),
    ] {
        assert!(
            malformed(
                schema.clone(),
                1,
                &[node],
                &[crate::Buffer::new(0, 0), crate::Buffer::new(0, 4)],
                &[],
                &[0; 4],
                None
            )
            .is_err()
        );
    }
    assert!(malformed(schema.clone(), -1, &[], &[], &[], &[], None).is_err());
    assert!(
        malformed(
            schema.clone(),
            1,
            &[crate::FieldNode::new(1, 0)],
            &[crate::Buffer::new(0, 0)],
            &[],
            &[],
            Some(&[])
        )
        .is_err()
    );
    assert!(
        malformed(
            schema,
            1,
            &[crate::FieldNode::new(1, 0)],
            &[crate::Buffer::new(0, 0), crate::Buffer::new(0, 4)],
            &[0],
            &[0; 4],
            None
        )
        .is_err()
    );
    for count in [-1, i64::MAX] {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8View, true)]));
        assert!(
            malformed(
                schema,
                0,
                &[crate::FieldNode::new(0, 0)],
                &[crate::Buffer::new(0, 0); 2],
                &[count],
                &[],
                None
            )
            .is_err()
        );
    }
}

#[test]
fn malformed_struct_validity_and_union_offsets_return_errors() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Default::default()),
        true,
    )]));
    assert!(
        malformed(
            schema,
            1,
            &[crate::FieldNode::new(1, 1)],
            &[crate::Buffer::new(0, 0)],
            &[],
            &[],
            None
        )
        .is_err()
    );
    let fields = UnionFields::try_new([0], [Field::new("i", DataType::Int32, false)]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(fields, arrow_schema::UnionMode::Dense),
        false,
    )]));
    assert!(
        malformed(
            schema,
            1,
            &[crate::FieldNode::new(1, 0), crate::FieldNode::new(1, 0)],
            &[
                crate::Buffer::new(0, 1),
                crate::Buffer::new(0, 0),
                crate::Buffer::new(0, 0),
                crate::Buffer::new(0, 4)
            ],
            &[],
            &[0; 4],
            None
        )
        .is_err()
    );
}

#[test]
fn file_decoder_checks_message_and_block_extents_before_slicing() {
    let batch =
        RecordBatch::try_from_iter([("n", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef)])
            .unwrap();
    let (_, encoded) = crate::writer::IpcDataGenerator::default()
        .encode(
            &batch,
            &mut crate::writer::DictionaryTracker::new(false),
            &Default::default(),
            &mut Default::default(),
        )
        .unwrap();
    let mut framed = crate::CONTINUATION_MARKER.to_vec();
    framed.extend_from_slice(&(encoded.ipc_message.len() as i32).to_le_bytes());
    framed.extend_from_slice(&encoded.ipc_message);
    let metadata_length = framed.len() as i32;
    framed.extend_from_slice(&encoded.arrow_data);
    let framed = Buffer::from_vec(framed);
    let block = crate::Block::new(0, metadata_length, encoded.arrow_data.len() as i64);
    let decoder = super::FileDecoder::new(batch.schema(), crate::MetadataVersion::V5);
    assert_eq!(
        decoder.read_record_batch(&block, &framed).unwrap().unwrap(),
        batch
    );
    for length in 0..framed.len() {
        assert!(
            decoder
                .read_record_batch(&block, &framed.slice_with_length(0, length))
                .is_err(),
            "truncation {length}"
        );
    }
    for bad in [
        crate::Block::new(0, -1, block.bodyLength()),
        crate::Block::new(0, block.metaDataLength(), -1),
        crate::Block::new(0, block.metaDataLength() + 1, block.bodyLength() - 1),
        crate::Block::new(0, block.metaDataLength(), block.bodyLength() + 1),
    ] {
        assert!(decoder.read_record_batch(&bad, &framed).is_err());
    }
    let mut negative = crate::CONTINUATION_MARKER.to_vec();
    negative.extend_from_slice(&(-1_i32).to_le_bytes());
    assert!(super::parse_message(&negative).is_err());
    assert!(super::read_block(std::io::Cursor::new(&[]), &crate::Block::new(-1, 0, 0)).is_err());
}
