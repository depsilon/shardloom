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

fn dtype(source: &DataType) -> Result<DataType> {
    Ok(match source {
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 | DataType::UInt64 => DataType::Int64,
        DataType::Date64 => DataType::Date32,
        DataType::LargeUtf8 => DataType::Utf8,
        DataType::LargeBinary => DataType::Binary,
        DataType::Timestamp(unit, zone) => {
            DataType::Timestamp(*unit, zone.as_ref().map(|_| "UTC".into()))
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            DataType::List(Arc::new(Field::new(
                child.name(),
                dtype(child.data_type())?,
                child.is_nullable(),
            )))
        }
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|child| {
                    Ok(Field::new(
                        child.name(),
                        dtype(child.data_type())?,
                        child.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision)
                && *scale >= 0
                && i16::from(*scale) <= i16::from(*precision) =>
        {
            source.clone()
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
        | DataType::Date32 => source.clone(),
        _ => {
            return Err(error(&format!(
                "unsupported writer dtype {source:?}; ORC has no admitted exact mapping for this type"
            )));
        }
    })
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

#[allow(clippy::too_many_lines)] // Exhaustive recursive wire-type translation with exact value checks.
fn column(array: &ArrayRef) -> Result<ArrayRef> {
    let target = dtype(array.data_type())?;
    // Reuse compatible primitive buffers. Recursive owners still visit their
    // children because an unsigned or fixed-size child may need translation.
    let result: ArrayRef = match array.data_type() {
        DataType::UInt8 => Arc::new(Int16Array::from_iter(
            typed::<UInt8Array>(array)?.iter().map(|v| v.map(i16::from)),
        )),
        DataType::UInt16 => Arc::new(Int32Array::from_iter(
            typed::<UInt16Array>(array)?
                .iter()
                .map(|v| v.map(i32::from)),
        )),
        DataType::UInt32 => Arc::new(Int64Array::from_iter(
            typed::<UInt32Array>(array)?
                .iter()
                .map(|v| v.map(i64::from)),
        )),
        DataType::UInt64 => Arc::new(Int64Array::from(
            typed::<UInt64Array>(array)?
                .iter()
                .map(|v| {
                    v.map(i64::try_from)
                        .transpose()
                        .map_err(|_| error("ORC cannot represent uint64 above i64::MAX"))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Date64 => Arc::new(Date32Array::from(
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
        )),
        DataType::Timestamp(unit, Some(_)) => match unit {
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
        },
        DataType::LargeUtf8 => {
            let values = typed::<LargeStringArray>(array)?;
            let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
            Arc::new(
                StringArray::try_new(
                    offsets,
                    values.values().slice_with_length(start, length),
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        DataType::LargeBinary => {
            let values = typed::<LargeBinaryArray>(array)?;
            let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
            Arc::new(
                BinaryArray::try_new(
                    offsets,
                    values.values().slice_with_length(start, length),
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        DataType::Struct(_) => {
            let values = typed::<StructArray>(array)?;
            let DataType::Struct(fields) = target else {
                return Err(error("invalid struct translation"));
            };
            Arc::new(
                StructArray::try_new(
                    fields,
                    values
                        .columns()
                        .iter()
                        .map(column)
                        .collect::<Result<Vec<_>>>()?,
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        DataType::List(_) => {
            let values = typed::<ListArray>(array)?;
            let DataType::List(field) = target else {
                return Err(error("invalid list translation"));
            };
            Arc::new(
                ListArray::try_new(
                    field,
                    values.offsets().clone(),
                    column(values.values())?,
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        DataType::LargeList(_) => {
            let values = typed::<LargeListArray>(array)?;
            let (offsets, start, length) = narrow_offsets(values.value_offsets())?;
            let DataType::List(field) = target else {
                return Err(error("invalid large-list translation"));
            };
            Arc::new(
                ListArray::try_new(
                    field,
                    offsets,
                    column(&values.values().slice(start, length))?,
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        DataType::FixedSizeList(_, size) => {
            let values = typed::<FixedSizeListArray>(array)?;
            let size = usize::try_from(*size).map_err(|value| error(&value.to_string()))?;
            let offsets = (0..=values.len())
                .map(|row| {
                    row.checked_mul(size)
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or_else(|| {
                            error("ORC fixed-list conversion exceeds 32-bit reader offset range")
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let DataType::List(field) = target else {
                return Err(error("invalid fixed-list translation"));
            };
            Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    column(values.values())?,
                    values.nulls().cloned(),
                )
                .map_err(|value| error(&value.to_string()))?,
            )
        }
        _ => Arc::clone(array),
    };
    Ok(result)
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
        match (actual, expected) {
            (DataType::Timestamp(_, left), DataType::Timestamp(_, right)) => {
                left.is_some() == right.is_some()
            }
            (DataType::Utf8, DataType::LargeUtf8) | (DataType::Binary, DataType::LargeBinary) => {
                true
            }
            (DataType::List(left), DataType::List(right)) => {
                compatible(left.data_type(), right.data_type())
            }
            (DataType::Struct(left), DataType::Struct(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right).all(|(left, right)| {
                        left.name() == right.name()
                            && compatible(left.data_type(), right.data_type())
                    })
            }
            _ => actual == expected,
        }
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
}
