//! Complete validation shares copy semantics without retaining payload buffers.

use super::*;
use crate::local_primitives::native_relational_sort::{Spec, StreamingPrefix};
use crate::relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey};
use crate::resident_session::ResidentVortexSession;
use shardloom_core::ColumnRef;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        arrays::{DecimalArray, DictArray, VarBinViewArray},
        dtype::{DecimalDType, FieldNames},
        extension::datetime::{Time, TimeUnit},
    },
    buffer::Buffer,
};

fn record(fields: Vec<(&str, ArrayRef)>, validity: Validity) -> ArrayRef {
    let rows = fields[0].1.len();
    StructArray::try_new(
        FieldNames::from(fields.iter().map(|(name, _)| *name).collect::<Vec<_>>()),
        fields.into_iter().map(|(_, value)| value),
        rows,
        validity,
    )
    .unwrap()
    .into_array()
}

fn fields(array: &ArrayRef) -> Vec<(String, DType)> {
    let fields = array.dtype().as_struct_fields();
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect()
}

fn list(values: ArrayRef, offsets: &[u64], validity: Validity) -> ArrayRef {
    ListArray::try_new(
        values,
        PrimitiveArray::from_iter(offsets.iter().copied()).into_array(),
        validity,
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}

fn decimal(values: &[i128]) -> ArrayRef {
    DecimalArray::try_new(
        Buffer::from(values.to_vec()),
        DecimalDType::new(2, 0),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn check_copy_agreement(array: &ArrayRef, expected_error: Option<&str>) {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let baseline = context.memory().snapshot().reserved_bytes;
            let copied = detach(array, context).map(drop).map_err(|e| e.to_string());
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            let validated =
                validate_record(array, &fields(array), context).map_err(|e| e.to_string());
            assert_eq!(validated, copied);
            if let Some(expected) = expected_error {
                assert!(validated.unwrap_err().contains(expected));
            } else {
                validated.unwrap();
            }
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_payload_validation_preserves_parent_masks_and_unreferenced_decimal_values() {
    let malformed = decimal(&[999, 7]);
    for codes in [vec![1_u8, 1], vec![1_u8, 0]] {
        let referenced_invalid = codes[1] == 0;
        let dictionary = DictArray::try_new(
            PrimitiveArray::from_iter(codes).into_array(),
            malformed.clone(),
        )
        .unwrap()
        .into_array();
        let chunks = ChunkedArray::try_new(
            vec![
                dictionary.slice(0..1).unwrap(),
                dictionary.slice(1..2).unwrap(),
            ],
            dictionary.dtype().clone(),
        )
        .unwrap()
        .into_array();
        let array = record(vec![("value", chunks)], Validity::NonNullable);
        check_copy_agreement(&array, referenced_invalid.then_some("precision"));
    }
    for fixed in [false, true] {
        let values = decimal(&[999, -999, 7, -8]);
        let child = if fixed {
            FixedSizeListArray::try_new(values, 2, Validity::from_iter([false, true]), 2)
                .unwrap()
                .into_array()
        } else {
            list(values, &[0, 2, 4], Validity::from_iter([false, true]))
        };
        check_copy_agreement(&record(vec![("items", child)], Validity::NonNullable), None);
    }
    let invalid_float = PrimitiveArray::from_iter([f64::NAN, 1.0]).into_array();
    let parent = record(
        vec![("value", invalid_float)],
        Validity::from_iter([false, true]),
    );
    check_copy_agreement(&parent, None);
    check_copy_agreement(
        &record(vec![("child", parent)], Validity::NonNullable),
        None,
    );
}

#[test]
fn native_payload_validation_preserves_field_order_and_discarded_temporal_errors() {
    let values = list(
        PrimitiveArray::from_iter([1.0_f64, f64::NAN]).into_array(),
        &[0, 1, 2],
        Validity::NonNullable,
    );
    let amounts = decimal(&[999, 7]);
    check_copy_agreement(
        &record(
            vec![("items", values.clone()), ("amount", amounts.clone())],
            Validity::NonNullable,
        ),
        Some("finite declared dtype"),
    );
    check_copy_agreement(
        &record(
            vec![("amount", amounts), ("items", values)],
            Validity::NonNullable,
        ),
        Some("precision"),
    );
    let time = vortex::array::arrays::ExtensionArray::try_new(
        Time::new(TimeUnit::Milliseconds, Nullability::NonNullable).erased(),
        PrimitiveArray::from_iter([0_i32, 86_400_000]).into_array(),
    )
    .unwrap()
    .into_array();
    check_copy_agreement(
        &record(
            vec![("items", list(time, &[0, 1, 2], Validity::NonNullable))],
            Validity::NonNullable,
        ),
        Some("time"),
    );
}

#[test]
fn native_payload_validation_keeps_large_nested_text_out_of_the_retained_grant() {
    let text = "λ".repeat(64 * 1024);
    let source = record(
        vec![(
            "items",
            list(
                VarBinViewArray::from_iter_str(std::iter::repeat_n(text.as_str(), 64)).into_array(),
                &(0..=64_u64).collect::<Vec<_>>(),
                Validity::NonNullable,
            ),
        )],
        Validity::NonNullable,
    );
    assert!(source.nbytes() > 8 << 20);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let error = session
        .with_native_execution_context(&token, |context| {
            let baseline = context.memory().snapshot().reserved_bytes;
            validate_record(&source, &fields(&source), context)?;
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            assert!(context.memory().snapshot().peak_reserved_bytes < 128 << 10);
            assert!(
                detach(&source, context)
                    .unwrap_err()
                    .to_string()
                    .contains("reservation denied")
            );
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            let held = context.memory().reserve((1 << 20) - baseline)?;
            assert!(validate_record(&source, &fields(&source), context).is_err());
            drop(held);
            validate_record(&source, &fields(&source), context)?;
            token.cancel();
            assert!(
                validate_record(&source, &fields(&source), context)
                    .unwrap_err()
                    .to_string()
                    .contains("cancel")
            );
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancel"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn order_spec(array: &ArrayRef, nulls: Option<VortexRelationalNullOrder>) -> Spec {
    Spec {
        fields: fields(array),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("key").unwrap(),
            descending: false,
            nulls,
        }],
        names: vec!["key".into()],
        copy_policy: CopyPolicy::ValidateValues,
    }
}

#[test]
fn streamed_rich_prefix_validates_discarded_payload_before_any_key_or_output() {
    for cap in [0, 1] {
        for nested in [false, true] {
            let payload = if nested {
                list(
                    PrimitiveArray::from_iter([1.0_f64, 2.0, 3.0, 4.0, f64::NAN]).into_array(),
                    &[0, 1, 2, 3, 4, 5],
                    Validity::NonNullable,
                )
            } else {
                decimal(&[1, 2, 3, 4, 999])
            };
            let array = record(
                vec![
                    ("payload", payload),
                    (
                        "key",
                        PrimitiveArray::from_option_iter([
                            None,
                            Some(100_i64),
                            Some(100),
                            Some(100),
                            Some(100),
                        ])
                        .into_array(),
                    ),
                ],
                Validity::NonNullable,
            );
            let spec = order_spec(&array, None);
            let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let baseline = context.memory().snapshot().reserved_bytes;
                    let mut sort = StreamingPrefix::try_new(&spec, Some(cap), context)?.unwrap();
                    let error = sort.build(array, context).err().unwrap().to_string();
                    assert!(
                        error.contains(if nested {
                            "finite declared dtype"
                        } else {
                            "precision"
                        }),
                        "{error}"
                    );
                    drop(sort);
                    assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
                    Ok(())
                })
                .unwrap();
        }
    }
}

#[test]
fn streamed_rich_prefix_byte_counts_include_nested_offsets_validity_and_repeated_copy() {
    let text = "λ".repeat(16);
    let array = record(
        vec![
            (
                "key",
                PrimitiveArray::from_iter((0..129_i64).rev()).into_array(),
            ),
            ("amount", decimal(&vec![7; 129])),
            (
                "items",
                list(
                    VarBinViewArray::from_iter(
                        (0..258).map(|row| (row % 2 == 0).then_some(text.as_str())),
                        DType::Utf8(Nullability::Nullable),
                    )
                    .into_array(),
                    &(0..=129_u64).map(|row| row * 2).collect::<Vec<_>>(),
                    Validity::AllValid,
                ),
            ),
        ],
        Validity::NonNullable,
    );
    let spec = order_spec(&array, Some(VortexRelationalNullOrder::Last));
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = StreamingPrefix::try_new(&spec, Some(2), context)?.unwrap();
            // Two i64 keys, two i128 decimals, one list validity byte,
            // three u64 list offsets, one string validity byte, five u64 text
            // offsets and two 32-byte UTF-8 values. Final delivery is a separate copy.
            let expected = 16 + 32 + 1 + 24 + 1 + 40 + 64;
            for _ in 0..2 {
                let copied = sort.build(array.clone(), context)?;
                assert_eq!(copied.rows, 2);
                assert_eq!(copied.buffer_bytes, expected);
                assert_eq!(copied.retained_buffer_bytes, expected);
            }
            let mut delivered = 0;
            sort.finish(context, 256, &mut |output| {
                delivered += output.len();
                assert_eq!(output.nbytes(), expected);
                Ok(())
            })?;
            assert_eq!(delivered, 2);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
