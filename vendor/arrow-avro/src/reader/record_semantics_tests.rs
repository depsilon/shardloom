// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom controls for record continuation and output semantics.

use super::*;

#[test]
fn dense_union_offsets_restart_for_each_retained_batch() {
    let fields = UnionFields::try_new(
        [2, 7],
        [
            ArrowField::new("number", DataType::Int32, false),
            ArrowField::new("text", DataType::Utf8, false),
        ],
    )
    .unwrap();
    let children = [Codec::Int32, Codec::Utf8]
        .into_iter()
        .map(|codec| AvroDataType::new(codec, Default::default(), None))
        .collect::<Vec<_>>();
    let dtype = AvroDataType::new(
        Codec::Union(children.into(), fields, UnionMode::Dense),
        Default::default(),
        None,
    );
    let mut decoder = Decoder::try_new(&dtype).unwrap();
    let mut held = Vec::new();
    for number in [7_u8, 11, 19] {
        for bytes in [&[0, number * 2][..], &[2, 2, b'x'][..], &[0, 1][..]] {
            decoder.decode(&mut AvroCursor::new(bytes)).unwrap();
        }
        held.push(decoder.flush(None).unwrap());
    }
    for (array, number) in held.iter().zip([7, 11, 19]) {
        let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[2, 7, 2]);
        assert_eq!(union.offsets().unwrap().as_ref(), &[0, 0, 1]);
        let ints = union
            .child(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ints.values().as_ref(), &[number, -1]);
        let strings = union
            .child(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.iter().collect::<Vec<_>>(), vec![Some("x")]);
    }
}

#[test]
fn string_views_preserve_nulls_and_complete_values_across_batches() {
    let dtype = AvroDataType::new(
        Codec::Utf8View,
        Default::default(),
        Some(Nullability::NullFirst),
    );
    let mut decoder = Decoder::try_new(&dtype).unwrap();
    let mut held = Vec::new();
    for text in ["short", "a longer 東京 value for an external view", ""] {
        decoder.decode(&mut AvroCursor::new(&[0])).unwrap();
        let mut encoded = vec![2, u8::try_from(text.len() * 2).unwrap()];
        encoded.extend_from_slice(text.as_bytes());
        decoder.decode(&mut AvroCursor::new(&encoded)).unwrap();
        decoder.decode(&mut AvroCursor::new(&[0])).unwrap();
        held.push((decoder.flush(None).unwrap(), text));
    }
    for (array, expected) in held {
        let views = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(
            views.iter().collect::<Vec<_>>(),
            vec![None, Some(expected), None]
        );
    }
}

#[test]
fn nested_empty_records_keep_their_row_count_across_batches() {
    let dtype = AvroDataType::new(Codec::Struct(Vec::new().into()), Default::default(), None);
    let mut decoder = Decoder::try_new(&dtype).unwrap();
    let mut held = Vec::new();
    for rows in [3, 1, 7] {
        for _ in 0..rows {
            decoder.decode(&mut AvroCursor::new(&[])).unwrap();
        }
        held.push((decoder.flush(None).unwrap(), rows));
    }
    for (array, rows) in held {
        let record = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(record.len(), rows);
        assert_eq!(record.num_columns(), 0);
        assert_eq!(record.null_count(), 0);
    }
}
