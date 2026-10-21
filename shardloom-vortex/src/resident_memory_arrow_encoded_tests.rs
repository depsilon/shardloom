//! Prerequisites for fair encoded Arrow transport controls. No transport timing.

use super::*;
use arrow_array::{Array as _, ArrayRef, DictionaryArray, Int16Array, RunArray, StringArray};
use arrow_schema::{Field, Schema};

fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "renamed_value",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}

fn dictionary() -> ArrayRef {
    Arc::new(
        DictionaryArray::<arrow_array::types::Int16Type>::try_new(
            Int16Array::from(vec![Some(0), None, Some(2), Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("λ\0猫"), None, Some("")])),
        )
        .unwrap(),
    )
}

fn runs() -> ArrayRef {
    Arc::new(
        RunArray::<arrow_array::types::Int16Type>::try_new(
            &Int16Array::from(vec![2, 4, 7]),
            &StringArray::from(vec![Some("λ\0猫"), None, Some("")]),
        )
        .unwrap(),
    )
}

#[cfg(not(feature = "native-encoded-exchange-experiment"))]
#[test]
fn encoded_arrow_control_remains_opt_in() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    for array in [dictionary(), runs()] {
        let error = ResidentMemorySource::from_arrow_record_batch(
            &session,
            batch(array),
            &CancellationToken::default(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("native payload schema"));
        assert!(error.to_string().contains("no fallback execution"));
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[cfg(feature = "native-encoded-exchange-experiment")]
mod admitted {
    use super::*;
    use arrow_array::{
        Int8Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        types::{
            Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
            UInt64Type,
        },
    };
    use arrow_schema::DataType;
    use serde_json::{Value, json};
    use vortex::{
        array::{
            VortexSessionExecute as _,
            arrays::{Struct, struct_::StructArraySlotsExt as _},
        },
        buffer::ByteBuffer,
    };

    fn values(source: &ResidentMemorySource) -> Value {
        let result = source
            .prepare_projection(&["renamed_value"], None, None)
            .unwrap()
            .execute()
            .unwrap();
        serde_json::from_str(result.values_json.value()).unwrap()
    }

    fn expected(values: &[Value]) -> Value {
        Value::Array(
            values
                .iter()
                .map(|value| json!({"renamed_value": value}))
                .collect(),
        )
    }

    fn check(array: &dyn arrow_array::Array, reference: &[Value], encoding: &str) {
        for (offset, length) in [(0, array.len()), (1, array.len() - 2), (1, 0)] {
            let input = batch(array.slice(offset, length));
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let source = ResidentMemorySource::from_arrow_record_batch(
                &session,
                input,
                &CancellationToken::default(),
            )
            .unwrap();
            assert_eq!(source.row_count(), length);
            assert_eq!(
                source.0.array.as_::<Struct>().fields()[0]
                    .encoding_id()
                    .as_str(),
                encoding
            );
            assert_eq!(
                source.dtype().as_struct_fields().field_by_index(0),
                Some(vortex::array::dtype::DType::Utf8(
                    vortex::array::dtype::Nullability::Nullable
                ))
            );
            assert_eq!(
                values(&source),
                expected(&reference[offset..offset + length])
            );
            assert_eq!(
                values(&source),
                expected(&reference[offset..offset + length])
            );
            drop(source);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }

    #[test]
    fn encoded_arrow_control_all_dictionary_keys_keep_values_nulls_and_slices() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("λ\0猫"), None, Some("")]));
        let reference = [
            json!("λ\0猫"),
            Value::Null,
            json!(""),
            Value::Null,
            json!("λ\0猫"),
        ];
        macro_rules! check_keys {
            ($key:ty, $array:ty) => {
                check(
                    &DictionaryArray::<$key>::try_new(
                        <$array>::from(vec![Some(0), None, Some(2), Some(1), Some(0)]),
                        Arc::clone(&values),
                    )
                    .unwrap(),
                    &reference,
                    "vortex.dict",
                );
            };
        }
        check_keys!(Int8Type, Int8Array);
        check_keys!(Int16Type, Int16Array);
        check_keys!(Int32Type, Int32Array);
        check_keys!(Int64Type, Int64Array);
        check_keys!(UInt8Type, UInt8Array);
        check_keys!(UInt16Type, UInt16Array);
        check_keys!(UInt32Type, UInt32Array);
        check_keys!(UInt64Type, UInt64Array);
    }

    #[test]
    fn encoded_arrow_control_all_run_ends_keep_values_nulls_and_slices() {
        let values = StringArray::from(vec![Some("λ\0猫"), None, Some("")]);
        let reference = [
            json!("λ\0猫"),
            json!("λ\0猫"),
            Value::Null,
            Value::Null,
            json!(""),
            json!(""),
            json!(""),
        ];
        macro_rules! check_ends {
            ($end:ty, $array:ty) => {
                check(
                    &RunArray::<$end>::try_new(&<$array>::from(vec![2, 4, 7]), &values).unwrap(),
                    &reference,
                    "vortex.runend",
                );
            };
        }
        check_ends!(Int16Type, Int16Array);
        check_ends!(Int32Type, Int32Array);
        check_ends!(Int64Type, Int64Array);
    }

    fn leaf_cases() -> Vec<(ArrayRef, Value, vortex::array::dtype::DType)> {
        use arrow_array::{
            BinaryArray, BinaryViewArray, BooleanArray, LargeBinaryArray, LargeStringArray,
            StringViewArray,
        };
        use vortex::array::dtype::{DType, Nullability, PType};
        let nullable = Nullability::Nullable;
        vec![
            (
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                json!(true),
                DType::Bool(nullable),
            ),
            (
                Arc::new(Int8Array::from(vec![Some(i8::MIN), None])),
                json!(i8::MIN),
                DType::Primitive(PType::I8, nullable),
            ),
            (
                Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
                json!(i16::MIN),
                DType::Primitive(PType::I16, nullable),
            ),
            (
                Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
                json!(i32::MIN),
                DType::Primitive(PType::I32, nullable),
            ),
            (
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
                json!(i64::MIN),
                DType::Primitive(PType::I64, nullable),
            ),
            (
                Arc::new(UInt8Array::from(vec![Some(u8::MAX), None])),
                json!(u8::MAX),
                DType::Primitive(PType::U8, nullable),
            ),
            (
                Arc::new(UInt16Array::from(vec![Some(u16::MAX), None])),
                json!(u16::MAX),
                DType::Primitive(PType::U16, nullable),
            ),
            (
                Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
                json!(u32::MAX),
                DType::Primitive(PType::U32, nullable),
            ),
            (
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
                json!(u64::MAX),
                DType::Primitive(PType::U64, nullable),
            ),
            (
                Arc::new(LargeStringArray::from(vec![Some("λ\0猫"), None])),
                json!("λ\0猫"),
                DType::Utf8(nullable),
            ),
            (
                Arc::new(StringViewArray::from(vec![Some("λ\0猫"), None])),
                json!("λ\0猫"),
                DType::Utf8(nullable),
            ),
            (
                Arc::new(BinaryArray::from(vec![Some(&b"\0\xffA"[..]), None])),
                json!("00ff41"),
                DType::Binary(nullable),
            ),
            (
                Arc::new(LargeBinaryArray::from(vec![Some(&b"\0\xffA"[..]), None])),
                json!("00ff41"),
                DType::Binary(nullable),
            ),
            (
                Arc::new(BinaryViewArray::from(vec![Some(&b"\0\xffA"[..]), None])),
                json!("00ff41"),
                DType::Binary(nullable),
            ),
        ]
    }

    #[test]
    fn encoded_arrow_control_leaf_types_keep_exact_values_and_logical_types() {
        for (leaf, first, dtype) in leaf_cases() {
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int16Type>::try_new(
                    Int16Array::from(vec![Some(0), Some(1), None]),
                    Arc::clone(&leaf),
                )
                .unwrap(),
            );
            let run_end: ArrayRef = Arc::new(
                RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 3]), leaf.as_ref())
                    .unwrap(),
            );
            for array in [dictionary, run_end] {
                let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
                let source = ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch(array),
                    &CancellationToken::default(),
                )
                .unwrap();
                assert_eq!(
                    source.dtype().as_struct_fields().field_by_index(0),
                    Some(dtype.clone())
                );
                assert_eq!(
                    values(&source),
                    expected(&[first.clone(), Value::Null, Value::Null])
                );
                drop(source);
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
        }
    }

    #[test]
    fn encoded_arrow_control_empty_dictionary_and_all_null_values_remain_valid() {
        let dictionaries: Vec<ArrayRef> = vec![
            Arc::new(
                DictionaryArray::<Int16Type>::try_new(
                    Int16Array::from(vec![None, None, None]),
                    Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                )
                .unwrap(),
            ),
            Arc::new(
                DictionaryArray::<Int16Type>::try_new(
                    Int16Array::from(vec![Some(0), Some(0), Some(0)]),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                )
                .unwrap(),
            ),
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![3]),
                    &StringArray::from(vec![None::<&str>]),
                )
                .unwrap(),
            ),
        ];
        for array in dictionaries {
            for length in [0, 3] {
                let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
                let source = ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch(array.slice(0, length)),
                    &CancellationToken::default(),
                )
                .unwrap();
                assert_eq!(values(&source), expected(&vec![Value::Null; length]));
                drop(source);
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
        }
    }

    #[test]
    fn encoded_arrow_control_nested_shared_children_and_domains_stay_exact() {
        let dictionary = dictionary();
        let structure = arrow_array::StructArray::new(
            vec![
                Field::new("first", dictionary.data_type().clone(), true),
                Field::new("shared", dictionary.data_type().clone(), true),
            ]
            .into(),
            vec![Arc::clone(&dictionary), dictionary],
            None,
        );
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let source = ResidentMemorySource::from_arrow_record_batch(
            &session,
            batch(Arc::new(structure)),
            &CancellationToken::default(),
        )
        .unwrap();
        let reference = [
            json!("λ\0猫"),
            Value::Null,
            json!(""),
            Value::Null,
            json!("λ\0猫"),
        ];
        let reference: Vec<_> = reference
            .iter()
            .map(|value| json!({"first":value,"shared":value}))
            .collect();
        assert_eq!(values(&source), expected(&reference));
        let alternate = DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![Some(0), None, Some(2), Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![
                Some("different"),
                None,
                Some("domain"),
            ])),
        )
        .unwrap();
        let other = ResidentMemorySource::from_arrow_record_batch(
            &session,
            batch(Arc::new(alternate)),
            &CancellationToken::default(),
        )
        .unwrap();
        assert_eq!(
            values(&other),
            expected(&[
                json!("different"),
                Value::Null,
                json!("domain"),
                Value::Null,
                json!("different")
            ])
        );
        assert_eq!(values(&source), expected(&reference));
        drop((other, source));
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }

    fn nested_lists(child: &ArrayRef) -> Vec<ArrayRef> {
        use arrow_array::{FixedSizeListArray, LargeListArray, ListArray};
        let rows = [
            Some(vec![Some(0), Some(0)]),
            None,
            Some(vec![]),
            Some(vec![Some(0), Some(0), Some(0)]),
        ];
        let (_, offsets, _, nulls) =
            ListArray::from_iter_primitive::<Int16Type, _, _>(rows.clone()).into_parts();
        let (_, large_offsets, _, large_nulls) =
            LargeListArray::from_iter_primitive::<Int16Type, _, _>(rows).into_parts();
        let field = Arc::new(Field::new("entry", child.data_type().clone(), true));
        vec![
            Arc::new(ListArray::new(
                Arc::clone(&field),
                offsets,
                Arc::clone(child),
                nulls,
            )),
            Arc::new(LargeListArray::new(
                Arc::clone(&field),
                large_offsets,
                Arc::clone(child),
                large_nulls,
            )),
            Arc::new(FixedSizeListArray::new(
                field,
                2,
                child.slice(0, 4),
                Some(vec![true, false].into()),
            )),
        ]
    }

    fn contains_encoding(array: &vortex::array::ArrayRef, encoding: &str) -> bool {
        array.encoding_id().as_str() == encoding
            || array
                .children_iter()
                .any(|child| contains_encoding(child, encoding))
    }

    #[test]
    fn encoded_arrow_control_nested_lists_keep_null_parents_and_sliced_children() {
        for (child, reference, encoding) in [
            (
                dictionary(),
                vec![
                    json!("λ\0猫"),
                    Value::Null,
                    json!(""),
                    Value::Null,
                    json!("λ\0猫"),
                ],
                "vortex.dict",
            ),
            (
                runs().slice(0, 5),
                vec![
                    json!("λ\0猫"),
                    json!("λ\0猫"),
                    Value::Null,
                    Value::Null,
                    json!(""),
                ],
                "vortex.runend",
            ),
        ] {
            for array in nested_lists(&child) {
                let references = if array.len() == 2 {
                    vec![json!(&reference[..2]), Value::Null]
                } else {
                    vec![
                        json!(&reference[..2]),
                        Value::Null,
                        json!([]),
                        json!(&reference[2..]),
                    ]
                };
                for (offset, length) in [(0, array.len()), (1, array.len() - 1), (1, 0)] {
                    let input = batch(array.slice(offset, length));
                    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
                    let dtype = session
                        .with_native_session(|native, _| {
                            native
                                .arrow()
                                .from_arrow_schema(input.schema().as_ref())
                                .map_err(native_error)
                        })
                        .unwrap();
                    let source = ResidentMemorySource::from_arrow_record_batch(
                        &session,
                        input,
                        &CancellationToken::default(),
                    )
                    .unwrap();
                    assert_eq!(source.dtype(), &dtype);
                    assert!(contains_encoding(&source.0.array, encoding));
                    assert_eq!(
                        values(&source),
                        expected(&references[offset..offset + length])
                    );
                    drop(source);
                    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
                }
            }
        }
    }

    #[test]
    fn encoded_arrow_control_nonnullable_schema_preserves_values_or_refuses_nulls() {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        for has_null in [false, true] {
            let leaf: ArrayRef = Arc::new(StringArray::from(vec![
                Some("first"),
                if has_null { None } else { Some("second") },
            ]));
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(
                    DictionaryArray::<Int16Type>::try_new(
                        Int16Array::from(vec![0, 1]),
                        Arc::clone(&leaf),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 2]), leaf.as_ref())
                        .unwrap(),
                ),
            ];
            for array in arrays {
                // RecordBatch checks physical nulls, which are zero here even
                // when the encoded values contain a logical NULL.
                assert_eq!(array.null_count(), 0);
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "renamed_value",
                    array.data_type().clone(),
                    false,
                )]));
                let input = RecordBatch::try_new(schema, vec![array]).unwrap();
                let result = ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    input,
                    &CancellationToken::default(),
                );
                if has_null {
                    let error = result.err().unwrap().to_string();
                    assert!(error.contains("null"), "{error}");
                    assert!(error.contains("no fallback execution"), "{error}");
                } else {
                    let source = result.unwrap();
                    assert_eq!(
                        source.dtype().as_struct_fields().field_by_index(0),
                        Some(vortex::array::dtype::DType::Utf8(
                            vortex::array::dtype::Nullability::NonNullable,
                        ))
                    );
                    assert_eq!(
                        values(&source),
                        expected(&[json!("first"), json!("second")])
                    );
                    drop(source);
                }
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
        }
    }

    fn retained_buffer(array: &vortex::array::ArrayRef) -> ByteBuffer {
        if let Some(buffer) = array
            .buffers()
            .into_iter()
            .find(|buffer| !buffer.is_empty())
        {
            return buffer;
        }
        array
            .children_iter()
            .find_map(|child| {
                (!child.buffers().is_empty() || child.children_iter().next().is_some())
                    .then(|| retained_buffer(child))
            })
            .expect("fixture has an owned buffer")
    }

    #[test]
    fn encoded_arrow_control_keeps_credits_until_last_extracted_buffer_drops() {
        for array in [dictionary(), runs()] {
            let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
            let memory = session.shared_memory_pool();
            let source = ResidentMemorySource::from_arrow_record_batch(
                &session,
                batch(array),
                &CancellationToken::default(),
            )
            .unwrap();
            let field = source.0.array.as_::<Struct>().fields()[0].clone();
            let buffer = retained_buffer(&field);
            let metadata = Arc::downgrade(source.0.metadata.as_ref().unwrap());
            drop((field, source, session));
            assert!(metadata.upgrade().is_some());
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(buffer);
            assert!(metadata.upgrade().is_none());
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn encoded_arrow_control_denial_cancel_and_retry_release_every_credit() {
        for array in [dictionary(), runs()] {
            let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
            let cancelled = CancellationToken::default();
            cancelled.cancel();
            assert!(
                ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch(Arc::clone(&array)),
                    &cancelled
                )
                .is_err()
            );
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            let occupied = session.shared_memory_pool().reserve(1 << 20).unwrap();
            assert!(
                ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    batch(Arc::clone(&array)),
                    &CancellationToken::default()
                )
                .is_err()
            );
            drop(occupied);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            let source = ResidentMemorySource::from_arrow_record_batch(
                &session,
                batch(array),
                &CancellationToken::default(),
            )
            .unwrap();
            assert!(source.row_count() > 0);
            drop(source);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }

    #[test]
    fn encoded_arrow_control_rejects_unadmitted_types_annotations_and_depth() {
        let metadata = crate::native_payload_schema::arrow_metadata_bytes;
        for value in [
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        ] {
            assert!(
                metadata(&DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(value.clone())
                ))
                .is_err()
            );
            assert!(
                metadata(&DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", value, true)),
                ))
                .is_err()
            );
        }
        assert!(
            metadata(&DataType::Dictionary(
                Box::new(DataType::Float32),
                Box::new(DataType::Utf8)
            ))
            .is_err()
        );
        for ends in [
            Field::new("run_ends", DataType::Int32, true),
            Field::new("run_ends", DataType::UInt32, false),
        ] {
            assert!(
                metadata(&DataType::RunEndEncoded(
                    Arc::new(ends),
                    Arc::new(Field::new("values", DataType::Utf8, true))
                ))
                .is_err()
            );
        }
        for extension_at_end in [false, true] {
            let mut ends = Field::new("run_ends", DataType::Int32, false);
            let mut values = Field::new("values", DataType::Utf8, true);
            let field = if extension_at_end {
                &mut ends
            } else {
                &mut values
            };
            *field = field
                .clone()
                .with_metadata([("ARROW:extension:name".into(), "untrusted".into())].into());
            assert!(metadata(&DataType::RunEndEncoded(Arc::new(ends), Arc::new(values))).is_err());
        }
        let mut nested = dictionary().data_type().clone();
        for _ in 0..24 {
            nested = DataType::List(Arc::new(Field::new("item", nested, true)));
        }
        assert!(
            metadata(&nested)
                .unwrap_err()
                .to_string()
                .contains("depth 24")
        );
    }

    #[test]
    fn encoded_arrow_control_provider_and_all_ipc_codecs_preserve_representation() {
        use arrow_ipc::{
            CompressionType,
            reader::StreamReader,
            writer::{IpcWriteOptions, StreamWriter},
        };
        use std::io::Cursor;
        for array in [dictionary(), runs()] {
            for (offset, length) in [(0, array.len()), (1, array.len() - 2), (1, 0)] {
                let input = batch(array.slice(offset, length));
                let schema = input.schema();
                let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
                let source = ResidentMemorySource::from_arrow_record_batch(
                    &session,
                    input,
                    &CancellationToken::default(),
                )
                .unwrap();
                let reference = values(&source);
                let exported = session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut execution = context.native_session().create_execution_ctx();
                        let target =
                            Field::new("", DataType::Struct(schema.fields().clone()), false);
                        let array = context
                            .native_session()
                            .arrow()
                            .execute_arrow(source.0.array.clone(), Some(&target), &mut execution)
                            .map_err(native_error)?;
                        array.to_data().validate_full().map_err(native_error)?;
                        let structure = array
                            .as_any()
                            .downcast_ref::<arrow_array::StructArray>()
                            .unwrap();
                        RecordBatch::try_new(Arc::clone(&schema), structure.columns().to_vec())
                            .map_err(native_error)
                    })
                    .unwrap();
                for compression in [
                    None,
                    Some(CompressionType::LZ4_FRAME),
                    Some(CompressionType::ZSTD),
                ] {
                    let options = IpcWriteOptions::default()
                        .try_with_compression(compression)
                        .unwrap();
                    let mut bytes = Vec::new();
                    {
                        let mut writer = StreamWriter::try_new_with_options(
                            &mut bytes,
                            schema.as_ref(),
                            options,
                        )
                        .unwrap();
                        writer.write(&exported).unwrap();
                        writer.finish().unwrap();
                    }
                    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
                    let received = reader.next().unwrap().unwrap();
                    assert!(reader.next().is_none());
                    assert_eq!(received.schema(), schema);
                    let restored = ResidentMemorySource::from_arrow_record_batch(
                        &session,
                        received,
                        &CancellationToken::default(),
                    )
                    .unwrap();
                    assert_eq!(values(&restored), reference);
                    assert_eq!(
                        restored.0.array.as_::<Struct>().fields()[0].encoding_id(),
                        source.0.array.as_::<Struct>().fields()[0].encoding_id()
                    );
                    drop(restored);
                }
                drop((exported, source));
                assert_eq!(session.snapshot().memory.reserved_bytes, 0);
            }
        }
    }
}
