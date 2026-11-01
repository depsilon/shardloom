// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::reader::{controlled_array_tests::shapes, decoder_memory_tests::Memory};
use arrow_array::{types::*, *};
use arrow_schema::{Field, UnionFields};

fn all_shapes() -> Vec<ArrayRef> {
    let mut arrays = shapes();
    arrays.extend([
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef,
        Arc::new(NullArray::new(3)),
        Arc::new(StructArray::new_empty_fields(3, None)),
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"ab", b"cd"].into_iter()).unwrap()),
        Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            [
                Some(vec![Some(1), None]),
                None,
                Some(vec![Some(3), Some(4)]),
            ],
            2,
        )),
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(vec![2, 5]),
                &StringArray::from(vec![Some("run value"), None]),
            )
            .unwrap(),
        ),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(1), None, Some(0)]),
                Arc::new(StringArray::from(vec!["first", "second"])),
            )
            .unwrap(),
        ),
    ]);
    let nested = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1]),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1, 0]),
                Arc::new(StringArray::from(vec!["inside-a", "inside-b"])),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    arrays.push(Arc::new(nested));
    arrays
}

#[test]
fn owned_delta_preserves_complete_nested_values_and_refunds_last_alias() {
    for input in all_shapes() {
        let left = input.slice(0, input.len());
        let right = input.slice(1.min(input.len()), input.len().saturating_sub(1));
        let memory = Memory::new(16 << 20);
        let (combined, credit) = concatenate(&left, &right, &memory).unwrap();
        assert_eq!(combined.len(), left.len() + right.len());
        assert_eq!(
            combined.slice(0, left.len()).as_ref(),
            left.as_ref(),
            "{:?}",
            input.data_type()
        );
        assert_eq!(
            combined.slice(left.len(), right.len()).as_ref(),
            right.as_ref(),
            "{:?}",
            input.data_type()
        );
        assert!(memory.current() > 0);
        assert!(memory.peak() > memory.current());
        let kept = combined.slice(0, combined.len());
        drop(combined);
        drop(left);
        drop(right);
        drop(input);
        drop(credit);
        // Payload-bearing outputs retain the reservation through buffer aliases;
        // bufferless output metadata instead requires the explicit envelope.
        if kept.get_buffer_memory_size() > 0 {
            assert!(memory.current() > 0);
        }
        drop(kept);
        assert_eq!(memory.current(), 0);
    }
}

#[test]
fn dense_union_delta_admits_repeated_variable_values_and_empty_child_capacity() {
    let text = "repeated wide union value ".repeat(200);
    let union = UnionArray::try_new(
        UnionFields::try_new(
            [0, 127],
            [
                Field::new("text", DataType::Utf8, true),
                Field::new("unused", DataType::Int64, true),
            ],
        )
        .unwrap(),
        vec![0_i8; 257].into(),
        Some(vec![0_i32; 257].into()),
        vec![
            Arc::new(StringArray::from(vec![text.as_str()])),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ],
    )
    .unwrap();
    let input: ArrayRef = Arc::new(union);
    let memory = Memory::new(32 << 20);
    let (output, credit) = concatenate(&input, &input, &memory).unwrap();
    assert_eq!(output.len(), 514);
    let output = output.as_any().downcast_ref::<UnionArray>().unwrap();
    let text_values = output
        .child(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text_values.len(), 514);
    assert!(text_values.iter().all(|value| value == Some(text.as_str())));
    assert!(memory.current() >= text.len() * 514);
    let requested = memory.peak();
    drop(credit);
    // A matched small grant cannot accept that repeated decoded payload merely
    // because the two source dictionaries each store the text once.
    let denied = Memory::new(text.len() * 2 + 64 * 1024);
    assert!(
        concatenate(&input, &input, &denied)
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert_eq!(denied.current(), 0);
    assert!(requested > text.len() * 514);
}

#[test]
fn delta_view_payloads_remain_shared_without_retaining_every_generation() {
    let text = "view payload ".repeat(20_000);
    let input: ArrayRef = Arc::new(StringViewArray::from(vec![text.as_str()]));
    let memory = Memory::new(1 << 20);
    let (mut held, mut owner) = concatenate(&input, &input, &memory).unwrap();
    assert!(
        memory.current() < text.len(),
        "shared view payload is not a copied allocation"
    );
    for _ in 0..12 {
        let old = memory.current();
        let (next, next_owner) = concatenate(&held, &input, &memory).unwrap();
        let new_capacity = memory.current() - old;
        drop(held);
        drop(owner);
        assert_eq!(
            memory.current(),
            new_capacity,
            "dead dictionary generation must refund"
        );
        held = next;
        owner = next_owner;
    }
    assert_eq!(held.len(), 14);
    assert!(
        held.as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .all(|v| v == Some(text.as_str()))
    );
    drop(held);
    drop(owner);
    assert_eq!(memory.current(), 0);
}

#[test]
fn delta_denial_cancellation_and_key_overflow_preserve_inputs_and_refund() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(11)]));
    for cancelled in [false, true] {
        let memory = Memory::new(if cancelled { 1 << 20 } else { 0 });
        if cancelled {
            memory.cancel();
        }
        let error = concatenate(&input, &input, &memory).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancelled { "cancelled" } else { "denied" })
        );
        assert_eq!(memory.current(), 0);
        assert_eq!(
            input
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(3), None, Some(11)]
        );
    }
    // Non-mergeable value domains previously reach an infallible constructor
    // with a key overflow. Controlled concatenation rejects before that call.
    let dictionary = |value| -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    (0..100).map(|_| Some(vec![Some(value)])),
                )),
            )
            .unwrap(),
        )
    };
    let memory = Memory::new(1 << 20);
    assert!(matches!(
        concatenate(&dictionary(1), &dictionary(2), &memory),
        Err(ArrowError::DictionaryKeyOverflowError)
    ));
    assert_eq!(memory.current(), 0);
}
