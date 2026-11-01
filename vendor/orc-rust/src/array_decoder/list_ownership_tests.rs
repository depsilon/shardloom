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

fn decoder(lengths: Vec<i64>, values: Vec<i64>, memory: Memory) -> ListArrayDecoder {
    ListArrayDecoder {
        inner: Box::new(Int64ArrayDecoder::new(
            Box::new(Literal::new(values)),
            None,
            memory.clone(),
        )),
        present: None,
        lengths: Box::new(Literal::new(lengths)),
        field: Arc::new(Field::new("item", DataType::Int64, true)),
        memory,
    }
}

#[test]
fn list_offsets_children_and_slices_share_credited_owners() {
    let (memory, counters) = budget(256);
    let mut decoder = decoder(vec![2, 1], vec![11, 12, 13], memory);
    let parent = NullBuffer::from(vec![true, false, true]);
    let array = decoder.next_batch(3, Some(&parent)).unwrap();
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.value_offsets(), &[0, 2, 2, 3]);
    assert!(lists.is_null(1));
    assert_eq!(
        lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[11, 12, 13]
    );
    assert_eq!(counters.lock().unwrap().live, 40);
    let slice = array.slice(1, 2);
    drop(array);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 40);
    drop(slice);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn list_skip_and_malformed_lengths_preserve_child_boundary() {
    let (memory, counters) = budget(256);
    let mut selected = decoder(vec![2, 1, 2], vec![10, 11, 20, 30, 31], memory.clone());
    selected.skip_values(2, None).unwrap();
    let array = selected.next_batch(1, None).unwrap();
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[30, 31]
    );
    drop(array);
    drop(selected);
    for lengths in [vec![-1], vec![i64::MAX], vec![i32::MAX as i64, 1]] {
        let count = lengths.len();
        let mut invalid = decoder(lengths, vec![17], memory.clone());
        assert!(invalid.next_batch(count, None).is_err());
        let child = invalid.inner.next_batch(1, None).unwrap();
        assert_eq!(
            child
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            17
        );
        drop(child);
        drop(invalid);
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}
