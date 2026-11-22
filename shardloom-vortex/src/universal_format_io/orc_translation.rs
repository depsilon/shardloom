//! Explicit ORC compatibility mappings. The codec owns serialization, not query
//! evaluation. Width/list/date/zone changes are reported by the sink.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, FixedSizeListArray, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use shardloom_core::Result;

use super::orc_conversion_error as error;

pub(crate) fn schema(source: &SchemaRef) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(
        source
            .fields()
            .iter()
            .map(|field| {
                Ok(Field::new(
                    field.name(),
                    dtype(field.data_type())?,
                    field.is_nullable(),
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )))
}

enum DTypeFrame<'a> {
    List {
        name: &'a str,
        nullable: bool,
    },
    Struct {
        pending: std::slice::Iter<'a, Arc<Field>>,
        current_field: &'a Field,
        built: Vec<Field>,
    },
}

fn dtype(root: &DataType) -> Result<DataType> {
    let mut stack = Vec::<DTypeFrame<'_>>::new();
    let mut current = root;
    loop {
        let mut completed = match current {
            DataType::UInt8 => DataType::Int16,
            DataType::UInt16 => DataType::Int32,
            DataType::UInt32 | DataType::UInt64 => DataType::Int64,
            DataType::Date64 => DataType::Date32,
            DataType::LargeUtf8 => DataType::Utf8,
            DataType::LargeBinary => DataType::Binary,
            DataType::Timestamp(unit, zone) => {
                DataType::Timestamp(*unit, zone.as_ref().map(|_| "UTC".into()))
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => {
                stack.push(DTypeFrame::List {
                    name: child.name(),
                    nullable: child.is_nullable(),
                });
                current = child.data_type();
                continue;
            }
            DataType::Struct(children) => {
                let mut pending = children.iter();
                if let Some(first) = pending.next() {
                    stack.push(DTypeFrame::Struct {
                        pending,
                        current_field: first,
                        built: Vec::with_capacity(children.len()),
                    });
                    current = first.data_type();
                    continue;
                }
                DataType::Struct(Vec::<Field>::new().into())
            }
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision)
                    && *scale >= 0
                    && i16::from(*scale) <= i16::from(*precision) =>
            {
                current.clone()
            }
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32 => current.clone(),
            _ => {
                return Err(error(&format!(
                    "unsupported writer dtype {current:?}; ORC has no admitted exact mapping for this type"
                )));
            }
        };
        loop {
            let Some(frame) = stack.pop() else {
                return Ok(completed);
            };
            completed = match frame {
                DTypeFrame::List { name, nullable } => {
                    DataType::List(Arc::new(Field::new(name, completed, nullable)))
                }
                DTypeFrame::Struct {
                    mut pending,
                    current_field,
                    mut built,
                } => {
                    built.push(Field::new(
                        current_field.name(),
                        completed,
                        current_field.is_nullable(),
                    ));
                    if let Some(next_field) = pending.next() {
                        stack.push(DTypeFrame::Struct {
                            pending,
                            current_field: next_field,
                            built,
                        });
                        current = next_field.data_type();
                        break;
                    }
                    DataType::Struct(built.into())
                }
            };
        }
    }
}

pub(super) fn batch(source: &RecordBatch) -> Result<RecordBatch> {
    let target = schema(&source.schema())?;
    let arrays = source
        .columns()
        .iter()
        .map(column)
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(target, arrays).map_err(|value| error(&value.to_string()))
}

fn typed<T: 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| error("array storage differs from declared dtype"))
}

enum ColumnFrame {
    Struct {
        fields: arrow_schema::Fields,
        nulls: Option<arrow_buffer::NullBuffer>,
        pending: std::vec::IntoIter<ArrayRef>,
        built: Vec<ArrayRef>,
    },
    List {
        field: arrow_schema::FieldRef,
        offsets: OffsetBuffer<i32>,
        nulls: Option<arrow_buffer::NullBuffer>,
    },
}

fn translate_leaf_column(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::UInt8 => Ok(Arc::new(Int16Array::from_iter(
            typed::<UInt8Array>(array)?.iter().map(|v| v.map(i16::from)),
        ))),
        DataType::UInt16 => Ok(Arc::new(Int32Array::from_iter(
            typed::<UInt16Array>(array)?
                .iter()
                .map(|v| v.map(i32::from)),
        ))),
        DataType::UInt32 => Ok(Arc::new(Int64Array::from_iter(
            typed::<UInt32Array>(array)?
                .iter()
                .map(|v| v.map(i64::from)),
        ))),
        DataType::UInt64 => Ok(Arc::new(Int64Array::from(
            typed::<UInt64Array>(array)?
                .iter()
                .map(|v| {
                    v.map(i64::try_from)
                        .transpose()
                        .map_err(|_| error("ORC cannot represent uint64 above i64::MAX"))
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Date64 => Ok(Arc::new(Date32Array::from(
            typed::<Date64Array>(array)?
                .iter()
                .map(|v| {
                    v.map(|millis| {
                        if millis % 86_400_000 != 0 {
                            return Err(error("ORC Date64 conversion requires exact whole days"));
                        }
                        i32::try_from(millis / 86_400_000)
                            .map_err(|_| error("ORC date exceeds Date32 day range"))
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Timestamp(unit, Some(_)) => Ok(match unit {
            TimeUnit::Second => Arc::new(
                typed::<TimestampSecondArray>(array)?
                    .clone()
                    .with_timezone("UTC"),
            ),
            TimeUnit::Millisecond => Arc::new(
                typed::<TimestampMillisecondArray>(array)?
                    .clone()
                    .with_timezone("UTC"),
            ),
            TimeUnit::Microsecond => Arc::new(
                typed::<TimestampMicrosecondArray>(array)?
                    .clone()
                    .with_timezone("UTC"),
            ),
            TimeUnit::Nanosecond => Arc::new(
                typed::<TimestampNanosecondArray>(array)?
                    .clone()
                    .with_timezone("UTC"),
            ),
        }),
        DataType::LargeUtf8 => {
            let values = typed::<LargeStringArray>(array)?;
            let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
            Ok(Arc::new(
                StringArray::try_new(
                    offsets,
                    values.values().slice_with_length(start, length),
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            ))
        }
        DataType::LargeBinary => {
            let values = typed::<LargeBinaryArray>(array)?;
            let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
            Ok(Arc::new(
                BinaryArray::try_new(
                    offsets,
                    values.values().slice_with_length(start, length),
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            ))
        }
        _ => Ok(Arc::clone(array)),
    }
}

fn pop_column_frames(
    stack: &mut Vec<ColumnFrame>,
    mut completed: ArrayRef,
) -> Result<std::ops::ControlFlow<ArrayRef, ArrayRef>> {
    loop {
        let Some(frame) = stack.pop() else {
            return Ok(std::ops::ControlFlow::Break(completed));
        };
        match frame {
            ColumnFrame::List {
                field,
                offsets,
                nulls,
            } => {
                completed = Arc::new(
                    ListArray::try_new(field, offsets, completed, nulls)
                        .map_err(|value| error(&value.to_string()))?,
                );
            }
            ColumnFrame::Struct {
                fields,
                nulls,
                mut pending,
                mut built,
            } => {
                built.push(completed);
                if let Some(next_col) = pending.next() {
                    stack.push(ColumnFrame::Struct {
                        fields,
                        nulls,
                        pending,
                        built,
                    });
                    return Ok(std::ops::ControlFlow::Continue(next_col));
                }
                completed = Arc::new(
                    StructArray::try_new(fields, built, nulls)
                        .map_err(|value| error(&value.to_string()))?,
                );
            }
        }
    }
}

fn column(root: &ArrayRef) -> Result<ArrayRef> {
    let mut stack = Vec::<ColumnFrame>::new();
    let mut current = Arc::clone(root);
    loop {
        let target = dtype(current.data_type())?;
        // Reuse compatible primitive buffers. Iterative frames still visit their
        // children because an unsigned or fixed-size child may need translation.
        let completed: ArrayRef = match current.data_type() {
            DataType::Struct(_) => {
                let values = typed::<StructArray>(&current)?;
                let DataType::Struct(ref fields) = target else {
                    return Err(error("invalid struct translation"));
                };
                let mut pending = values.columns().to_vec().into_iter();
                if let Some(first) = pending.next() {
                    stack.push(ColumnFrame::Struct {
                        fields: fields.clone(),
                        nulls: values.nulls().cloned(),
                        pending,
                        built: Vec::with_capacity(values.columns().len()),
                    });
                    current = first;
                    continue;
                }
                Arc::new(
                    StructArray::try_new(fields.clone(), Vec::new(), values.nulls().cloned())
                        .map_err(|value| error(&value.to_string()))?,
                )
            }
            DataType::List(_) => {
                let values = typed::<ListArray>(&current)?;
                let DataType::List(ref field) = target else {
                    return Err(error("invalid list translation"));
                };
                stack.push(ColumnFrame::List {
                    field: Arc::clone(field),
                    offsets: values.offsets().clone(),
                    nulls: values.nulls().cloned(),
                });
                current = Arc::clone(values.values());
                continue;
            }
            DataType::LargeList(_) => {
                let values = typed::<LargeListArray>(&current)?;
                let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
                let DataType::List(ref field) = target else {
                    return Err(error("invalid large-list translation"));
                };
                stack.push(ColumnFrame::List {
                    field: Arc::clone(field),
                    offsets,
                    nulls: values.nulls().cloned(),
                });
                current = values.values().slice(start, length);
                continue;
            }
            DataType::FixedSizeList(_, size) => {
                let values = typed::<FixedSizeListArray>(&current)?;
                let size = usize::try_from(*size).map_err(|value| error(&value.to_string()))?;
                let offsets = (0..=values.len())
                    .map(|row| {
                        row.checked_mul(size)
                            .and_then(|n| i32::try_from(n).ok())
                            .ok_or_else(|| {
                                error(
                                    "ORC fixed-list conversion exceeds 32-bit reader offset range",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let DataType::List(ref field) = target else {
                    return Err(error("invalid fixed-list translation"));
                };
                stack.push(ColumnFrame::List {
                    field: Arc::clone(field),
                    offsets: OffsetBuffer::new(offsets.into()),
                    nulls: values.nulls().cloned(),
                });
                current = Arc::clone(values.values());
                continue;
            }
            _ => translate_leaf_column(&current)?,
        };
        match pop_column_frames(&mut stack, completed)? {
            std::ops::ControlFlow::Break(done) => return Ok(done),
            std::ops::ControlFlow::Continue(next) => current = next,
        }
    }
}

fn narrow_offsets(values: &[i64]) -> Result<(OffsetBuffer<i32>, usize, usize)> {
    let first = values
        .first()
        .copied()
        .ok_or_else(|| error("missing ORC conversion offset"))?;
    let offsets = values
        .iter()
        .map(|offset| {
            offset
                .checked_sub(first)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| error("ORC conversion exceeds 32-bit reader offset range"))
        })
        .collect::<Result<Vec<_>>>()?;
    let start = usize::try_from(first).map_err(|value| error(&value.to_string()))?;
    let length = usize::try_from(*offsets.last().unwrap_or(&0))
        .map_err(|value| error(&value.to_string()))?;
    Ok((OffsetBuffer::new(offsets.into()), start, length))
}

/// ORC stores neither Arrow nullability nor list element names/offset width nor
/// timestamp units. Check the physical schema before supplying exact units to
/// the reader; a supplied schema alone is not a file-schema validation.
pub(crate) fn wire_schema_matches(actual: &Schema, expected: &Schema) -> bool {
    fn compatible(actual: &DataType, expected: &DataType) -> bool {
        let mut stack = vec![(actual, expected)];
        while let Some((left, right)) = stack.pop() {
            match (left, right) {
                (DataType::Timestamp(_, left_tz), DataType::Timestamp(_, right_tz)) => {
                    if left_tz.is_some() != right_tz.is_some() {
                        return false;
                    }
                }
                (DataType::Utf8, DataType::LargeUtf8)
                | (DataType::Binary, DataType::LargeBinary) => {}
                (DataType::List(left_child), DataType::List(right_child)) => {
                    stack.push((left_child.data_type(), right_child.data_type()));
                }
                (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
                    if left_fields.len() != right_fields.len() {
                        return false;
                    }
                    for (left_child, right_child) in left_fields.iter().zip(right_fields).rev() {
                        if left_child.name() != right_child.name() {
                            return false;
                        }
                        stack.push((left_child.data_type(), right_child.data_type()));
                    }
                }
                _ => {
                    if left != right {
                        return false;
                    }
                }
            }
        }
        true
    }
    actual.fields().len() == expected.fields().len()
        && actual
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(left, right)| {
                left.name() == right.name() && compatible(left.data_type(), right.data_type())
            })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_date64_and_zone_mapping_rejects_fractional_days() {
        let days: ArrayRef = Arc::new(Date64Array::from(vec![Some(-86_400_000), None, Some(0)]));
        let instants: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(12), None, Some(-1_000_000)])
                .with_timezone("America/Chicago"),
        );
        let input = RecordBatch::try_from_iter_with_nullable([
            ("day", days, true),
            ("time", instants, true),
        ])
        .unwrap();
        let output = batch(&input).unwrap();
        assert_eq!(
            typed::<Date32Array>(output.column(0))
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(-1), None, Some(0)]
        );
        assert_eq!(
            output.column(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            typed::<TimestampMicrosecondArray>(output.column(1))
                .unwrap()
                .values()
                .as_ref(),
            &[12, 0, -1_000_000]
        );
        let invalid = Arc::new(Date64Array::from(vec![Some(1)])) as ArrayRef;
        assert!(
            column(&invalid)
                .unwrap_err()
                .to_string()
                .contains("whole days")
        );
    }

    #[test]
    fn nested_unsigned_and_sliced_fixed_lists_translate_exactly() {
        let values = Arc::new(UInt32Array::from(vec![
            Some(91),
            Some(92),
            Some(1),
            None,
            Some(u32::MAX),
            Some(2),
        ])) as ArrayRef;
        let fixed = FixedSizeListArray::new(
            Arc::new(Field::new("entry", DataType::UInt32, true)),
            2,
            values,
            None,
        );
        let sliced = Arc::new(fixed.slice(1, 2)) as ArrayRef;
        let converted = column(&sliced).unwrap();
        let list = typed::<ListArray>(&converted).unwrap();
        assert_eq!(list.value_offsets(), &[0, 2, 4]);
        assert_eq!(
            typed::<Int64Array>(list.values())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some(1), None, Some(i64::from(u32::MAX)), Some(2)]
        );
        let invalid = Arc::new(UInt64Array::from(vec![Some(u64::MAX)])) as ArrayRef;
        let record = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("id", DataType::UInt64, true))].into(),
            vec![invalid],
            None,
        )) as ArrayRef;
        assert!(
            column(&record)
                .unwrap_err()
                .to_string()
                .contains("above i64::MAX")
        );
    }

    #[test]
    fn large_string_offsets_narrow_without_rebuilding_bytes() {
        let source = LargeStringArray::from(vec![Some("discard"), Some("東京"), None, Some("λ")])
            .slice(1, 3);
        let first = usize::try_from(source.value_offsets()[0]).unwrap();
        let payload = source.values().as_ptr().wrapping_add(first);
        let source = Arc::new(source) as ArrayRef;
        let output = column(&source).unwrap();
        let output = typed::<StringArray>(&output).unwrap();
        assert_eq!(
            output.iter().collect::<Vec<_>>(),
            [Some("東京"), None, Some("λ")]
        );
        assert_eq!(output.values().as_ptr(), payload);
        assert!(narrow_offsets(&[0, i64::from(i32::MAX) + 1]).is_err());
    }

    #[test]
    fn unsupported_nested_temporal_schema_fails_before_any_array() {
        for dtype in [
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Decimal128(12, -1),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", dtype, true))),
                true,
            )]));
            assert!(
                super::schema(&schema)
                    .unwrap_err()
                    .to_string()
                    .contains("unsupported writer dtype")
            );
        }
    }

    #[test]
    fn deep_nested_orc_translation_on_small_stack() {
        let handle = std::thread::Builder::new()
            .name("orc_translation::small_stack".into())
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut current: ArrayRef = Arc::new(UInt32Array::from(vec![Some(7), None]));
                for level in 0..2_048 {
                    let child = Arc::new(Field::new("c", current.data_type().clone(), true));
                    current = if level % 2 == 0 {
                        Arc::new(ListArray::new(
                            child,
                            OffsetBuffer::new(vec![0_i32, 1, 2].into()),
                            current,
                            None,
                        ))
                    } else {
                        Arc::new(StructArray::new(vec![child].into(), vec![current], None))
                    };
                }
                let input = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "deep",
                        current.data_type().clone(),
                        true,
                    )])),
                    vec![current],
                )
                .unwrap();
                let converted = batch(&input).unwrap();
                let expected = super::schema(&input.schema()).unwrap();
                assert!(wire_schema_matches(
                    converted.schema().as_ref(),
                    expected.as_ref()
                ));
                assert!(!wire_schema_matches(
                    converted.schema().as_ref(),
                    input.schema().as_ref()
                ));
            })
            .unwrap();
        handle.join().unwrap();
    }
}
