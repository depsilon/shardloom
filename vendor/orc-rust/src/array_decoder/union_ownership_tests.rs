// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::array_decoder::{
    ownership_tests::{budget, Literal},
    Int64ArrayDecoder,
};
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field},
};

#[test]
fn skipped_union_rows_consume_only_the_matching_child_stream() {
    let fields = UnionFields::try_new(
        [0, 1],
        [
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ],
    )
    .unwrap();
    let (memory, counters) = budget(128);
    let mut decoder = UnionArrayDecoder {
        fields,
        variants: vec![
            Box::new(Int64ArrayDecoder::new(
                Box::new(Literal::new(vec![10, 11])),
                None,
                memory.clone(),
            )),
            Box::new(Int64ArrayDecoder::new(
                Box::new(Literal::new(vec![20, 21])),
                None,
                memory.clone(),
            )),
        ],
        tags: Box::new(Literal::new(vec![0, 1, 0, 1])),
        present: None,
        memory,
    };
    decoder.skip_values(3, None).unwrap();
    let array = decoder.next_batch(1, None).unwrap();
    let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_id(0), 1);
    let value = union.value(0);
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        21
    );
    drop(value);
    drop(array);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 0);
}
