// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::array_decoder::{
    ownership_tests::{budget, Literal},
    Int64ArrayDecoder,
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::DataType,
};

fn decoder(lengths: Vec<i64>, memory: Memory) -> MapArrayDecoder {
    MapArrayDecoder {
        keys: Box::new(Int64ArrayDecoder::new(
            Box::new(Literal::new(vec![1, 2, 3])),
            None,
            memory.clone(),
        )),
        values: Box::new(Int64ArrayDecoder::new(
            Box::new(Literal::new(vec![10, 20, 30])),
            None,
            memory.clone(),
        )),
        present: None,
        lengths: Box::new(Literal::new(lengths)),
        fields: vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]
        .into(),
        memory,
    }
}

#[test]
fn map_values_offsets_and_nulls_survive_slicing_and_reader_drop() {
    let (memory, counters) = budget(256);
    let mut decoder = decoder(vec![2, 1], memory);
    let parent = NullBuffer::from(vec![true, false, true]);
    let array = decoder.next_batch(3, Some(&parent)).unwrap();
    let map = array.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2, 2, 3]);
    assert!(map.is_null(1));
    assert_eq!(
        map.keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2, 3]
    );
    assert_eq!(
        map.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[10, 20, 30]
    );
    let slice = array.slice(1, 2);
    drop(array);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 64);
    drop(slice);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn map_skip_and_invalid_lengths_do_not_consume_unrelated_entries() {
    let (memory, counters) = budget(256);
    let mut selected = decoder(vec![2, 1], memory.clone());
    selected.skip_values(1, None).unwrap();
    let array = selected.next_batch(1, None).unwrap();
    let map = array.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(
        map.keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        map.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        30
    );
    drop(array);
    drop(selected);
    for lengths in [vec![-1], vec![i64::MAX], vec![i32::MAX as i64, 1]] {
        let count = lengths.len();
        let mut invalid = decoder(lengths, memory.clone());
        assert!(invalid.next_batch(count, None).is_err());
        let key = invalid.keys.next_batch(1, None).unwrap();
        assert_eq!(
            key.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            1
        );
        drop(key);
        drop(invalid);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}
