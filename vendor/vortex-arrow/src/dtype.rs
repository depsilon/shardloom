// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Convert between Vortex [`vortex_array::dtype::DType`] and Apache Arrow [`arrow_schema::DataType`].
//!
//! Apache Arrow's type system includes physical information, which could lead to ambiguities as
//! Vortex treats encodings as separate from logical types.
//!
//! The conversions in this module are "naive": every logical type is encoded in its simplest
//! corresponding Arrow type, and Arrow extension types (other than the builtin temporal types and
//! Parquet Variant) are not understood. The authoritative, plugin-aware conversion entry point is
//! [`ArrowSession`](crate::ArrowSession), through its `to_arrow_schema` / `to_arrow_datatype` /
//! `from_arrow_schema` / `from_arrow_field` / `from_arrow_datatype` methods.
//!
//! For this reason, it's recommended to do as much computation as possible within Vortex, and then
//! materialize an Arrow ArrayRef at the very end of the processing chain.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::TimeUnit as ArrowTimeUnit;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::StructFields;
use vortex_array::extension::datetime::AnyTemporal;
use vortex_array::extension::datetime::Date;
use vortex_array::extension::datetime::TemporalMetadata;
use vortex_array::extension::datetime::Time;
use vortex_array::extension::datetime::TimeUnit;
use vortex_array::extension::datetime::Timestamp;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;

/// Convert an Arrow [`ArrowTimeUnit`] to a Vortex [`TimeUnit`].
pub(crate) fn from_arrow_time_unit(value: ArrowTimeUnit) -> TimeUnit {
    match value {
        ArrowTimeUnit::Second => TimeUnit::Seconds,
        ArrowTimeUnit::Millisecond => TimeUnit::Milliseconds,
        ArrowTimeUnit::Microsecond => TimeUnit::Microseconds,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanoseconds,
    }
}

/// Convert a Vortex [`TimeUnit`] to an Arrow [`ArrowTimeUnit`].
///
/// # Errors
///
/// Returns an error for units with no Arrow equivalent (e.g. [`TimeUnit::Days`]).
pub(crate) fn to_arrow_time_unit(value: TimeUnit) -> VortexResult<ArrowTimeUnit> {
    Ok(match value {
        TimeUnit::Seconds => ArrowTimeUnit::Second,
        TimeUnit::Milliseconds => ArrowTimeUnit::Millisecond,
        TimeUnit::Microseconds => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanoseconds => ArrowTimeUnit::Nanosecond,
        _ => vortex_bail!("Cannot convert {value} to Arrow TimeUnit"),
    })
}

/// Naive conversion of an Arrow [`DataType`] to the Vortex [`PType`] it stores.
pub(crate) fn ptype_from_arrow(value: &DataType) -> VortexResult<PType> {
    match value {
        DataType::Int8 => Ok(PType::I8),
        DataType::Int16 => Ok(PType::I16),
        DataType::Int32 => Ok(PType::I32),
        DataType::Int64 => Ok(PType::I64),
        DataType::UInt8 => Ok(PType::U8),
        DataType::UInt16 => Ok(PType::U16),
        DataType::UInt32 => Ok(PType::U32),
        DataType::UInt64 => Ok(PType::U64),
        DataType::Float16 => Ok(PType::F16),
        DataType::Float32 => Ok(PType::F32),
        DataType::Float64 => Ok(PType::F64),
        _ => Err(vortex_err!(
            "Arrow datatype {:?} cannot be converted to ptype",
            value
        )),
    }
}

enum FromArrowTask<'a> {
    DataType(&'a DataType, Nullability),
    Field(&'a Field),
}

enum FromArrowFrame<'a> {
    List(Nullability),
    FixedSizeList(u32, Nullability),
    MapNeedValue {
        value_field: &'a Field,
        keys_sorted: bool,
        nullability: Nullability,
    },
    MapHaveValue {
        key_dtype: DType,
        keys_sorted: bool,
        nullability: Nullability,
    },
    Struct {
        fields: &'a Fields,
        next_idx: usize,
        built: Vec<DType>,
        nullability: Nullability,
    },
}

enum ToArrowFrame {
    List {
        nullable: bool,
    },
    FixedSizeList {
        size: i32,
        nullable: bool,
    },
    MapNeedValue {
        value_dtype: DType,
        value_nullable: bool,
        keys_sorted: bool,
    },
    MapHaveValue {
        key_type: DataType,
        value_nullable: bool,
        keys_sorted: bool,
    },
    Struct {
        fields: StructFields,
        next_idx: usize,
        built: Vec<FieldRef>,
    },
}

fn from_arrow_leaf_data_type(data_type: &DataType, nullability: Nullability) -> VortexResult<DType> {
    if data_type.is_integer() || data_type.is_floating() {
        return Ok(DType::Primitive(ptype_from_arrow(data_type)?, nullability));
    }

    Ok(match data_type {
        DataType::Null => DType::Null,
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            DType::Decimal(DecimalDType::try_new(*precision, *scale)?, nullability)
        }
        DataType::Boolean => DType::Bool(nullability),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DType::Utf8(nullability),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            DType::Binary(nullability)
        }
        DataType::Date32 => DType::Extension(Date::new(TimeUnit::Days, nullability).erased()),
        DataType::Date64 => {
            DType::Extension(Date::new(TimeUnit::Milliseconds, nullability).erased())
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            DType::Extension(Time::new(from_arrow_time_unit(*unit), nullability).erased())
        }
        DataType::Timestamp(unit, tz) => DType::Extension(
            Timestamp::try_new_with_tz(from_arrow_time_unit(*unit), tz.clone(), nullability)?
                .erased(),
        ),
        _ => vortex_bail!("Arrow data type not supported: {data_type:?}"),
    })
}

fn validate_map_entry_fields(entries: &Field) -> VortexResult<(&Field, &Field)> {
    vortex_ensure!(
        !entries.is_nullable(),
        "Arrow map entries field must be non-nullable"
    );
    let DataType::Struct(fields) = entries.data_type() else {
        vortex_bail!(
            "Arrow map entries field must have Struct type, got {:?}",
            entries.data_type()
        );
    };
    vortex_ensure_eq!(
        fields.len(),
        2,
        "Arrow map entries struct must contain exactly two fields"
    );
    let key = &fields[0];
    let value = &fields[1];
    vortex_ensure!(
        !key.is_nullable(),
        "Arrow map key field must be non-nullable"
    );
    Ok((key.as_ref(), value.as_ref()))
}

fn run_from_arrow_naive(mut task: FromArrowTask<'_>) -> VortexResult<DType> {
    let mut stack = Vec::<FromArrowFrame<'_>>::new();
    loop {
        let (data_type, nullability) = match task {
            FromArrowTask::Field(field) => {
                if field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("arrow.parquet.variant")
                {
                    let completed = DType::Variant(field.is_nullable().into());
                    match pop_from_arrow_naive(&mut stack, completed)? {
                        ControlFlow::Continue(next_task) => {
                            task = next_task;
                            continue;
                        }
                        ControlFlow::Break(result) => return Ok(result),
                    }
                }
                (field.data_type(), Nullability::from(field.is_nullable()))
            }
            FromArrowTask::DataType(data_type, nullability) => (data_type, nullability),
        };

        let completed = match data_type {
            DataType::Dictionary(_, value_type) => {
                task = FromArrowTask::DataType(value_type.as_ref(), nullability);
                continue;
            }
            DataType::RunEndEncoded(_, value_type) => {
                task = FromArrowTask::DataType(value_type.data_type(), nullability);
                continue;
            }
            DataType::List(elem)
            | DataType::LargeList(elem)
            | DataType::ListView(elem)
            | DataType::LargeListView(elem) => {
                stack.push(FromArrowFrame::List(nullability));
                task = FromArrowTask::Field(elem.as_ref());
                continue;
            }
            DataType::FixedSizeList(elem, size) => {
                stack.push(FromArrowFrame::FixedSizeList(*size as u32, nullability));
                task = FromArrowTask::Field(elem.as_ref());
                continue;
            }
            DataType::Map(entries, keys_sorted) => {
                let (key, value) = validate_map_entry_fields(entries.as_ref())?;
                stack.push(FromArrowFrame::MapNeedValue {
                    value_field: value,
                    keys_sorted: *keys_sorted,
                    nullability,
                });
                task = FromArrowTask::Field(key);
                continue;
            }
            DataType::Struct(fields) => {
                if let Some(first) = fields.first() {
                    stack.push(FromArrowFrame::Struct {
                        fields,
                        next_idx: 0,
                        built: Vec::with_capacity(fields.len()),
                        nullability,
                    });
                    task = FromArrowTask::Field(first.as_ref());
                    continue;
                }
                DType::Struct(StructFields::empty(), nullability)
            }
            _ => from_arrow_leaf_data_type(data_type, nullability)?,
        };

        match pop_from_arrow_naive(&mut stack, completed)? {
            ControlFlow::Continue(next_task) => task = next_task,
            ControlFlow::Break(result) => return Ok(result),
        }
    }
}

fn pop_from_arrow_naive<'a>(
    stack: &mut Vec<FromArrowFrame<'a>>,
    mut completed: DType,
) -> VortexResult<ControlFlow<DType, FromArrowTask<'a>>> {
    while let Some(frame) = stack.pop() {
        match frame {
            FromArrowFrame::List(nullability) => {
                completed = DType::List(Arc::new(completed), nullability);
            }
            FromArrowFrame::FixedSizeList(size, nullability) => {
                completed = DType::FixedSizeList(Arc::new(completed), size, nullability);
            }
            FromArrowFrame::MapNeedValue {
                value_field,
                keys_sorted,
                nullability,
            } => {
                stack.push(FromArrowFrame::MapHaveValue {
                    key_dtype: completed,
                    keys_sorted,
                    nullability,
                });
                return Ok(ControlFlow::Continue(FromArrowTask::Field(value_field)));
            }
            FromArrowFrame::MapHaveValue {
                key_dtype,
                keys_sorted,
                nullability,
            } => {
                completed = DType::map(key_dtype, completed, keys_sorted, nullability)?;
            }
            FromArrowFrame::Struct {
                fields,
                next_idx,
                mut built,
                nullability,
            } => {
                built.push(completed);
                let next = next_idx + 1;
                if let Some(next_field) = fields.get(next) {
                    stack.push(FromArrowFrame::Struct {
                        fields,
                        next_idx: next,
                        built,
                        nullability,
                    });
                    return Ok(ControlFlow::Continue(FromArrowTask::Field(
                        next_field.as_ref(),
                    )));
                }
                let names = fields
                    .iter()
                    .map(|field| FieldName::from(field.name().as_str()))
                    .collect();
                completed = DType::Struct(StructFields::new(names, built), nullability);
            }
        }
    }
    Ok(ControlFlow::Break(completed))
}

/// Naive conversion of an Arrow [`DataType`] to a Vortex [`DType`], with no awareness of Arrow
/// extension plugins.
pub(crate) fn from_arrow_data_type(
    data_type: &DataType,
    nullability: Nullability,
) -> VortexResult<DType> {
    run_from_arrow_naive(FromArrowTask::DataType(data_type, nullability))
}

/// Naive conversion of an Arrow [`Field`] to a Vortex [`DType`]. Only the builtin
/// `arrow.parquet.variant` extension metadata is recognized.
#[cfg(test)]
pub(crate) fn from_arrow_field_naive(field: &Field) -> VortexResult<DType> {
    run_from_arrow_naive(FromArrowTask::Field(field))
}

/// Naive conversion of Arrow [`Fields`] to Vortex [`StructFields`].
#[cfg(test)]
pub(crate) fn from_arrow_fields_naive(fields: &Fields) -> VortexResult<StructFields> {
    let mut dtypes = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        dtypes.push(from_arrow_field_naive(field.as_ref())?);
    }
    let names = fields
        .iter()
        .map(|field| FieldName::from(field.name().as_str()))
        .collect();
    Ok(StructFields::new(names, dtypes))
}

fn to_data_type_naive_leaf(dtype: &DType) -> VortexResult<DataType> {
    Ok(match dtype {
        DType::Null => DataType::Null,
        DType::Bool(_) => DataType::Boolean,
        DType::Primitive(ptype, _) => match ptype {
            PType::U8 => DataType::UInt8,
            PType::U16 => DataType::UInt16,
            PType::U32 => DataType::UInt32,
            PType::U64 => DataType::UInt64,
            PType::I8 => DataType::Int8,
            PType::I16 => DataType::Int16,
            PType::I32 => DataType::Int32,
            PType::I64 => DataType::Int64,
            PType::F16 => DataType::Float16,
            PType::F32 => DataType::Float32,
            PType::F64 => DataType::Float64,
        },
        DType::Decimal(dt, _) => {
            let precision = dt.precision();
            let scale = dt.scale();

            match precision {
                // This code is commented out until DataFusion improves its support for smaller decimals.
                // // DECIMAL32_MAX_PRECISION
                // 0..=9 => DataType::Decimal32(precision, scale),
                // // DECIMAL64_MAX_PRECISION
                // 10..=18 => DataType::Decimal64(precision, scale),
                // DECIMAL128_MAX_PRECISION
                0..=38 => DataType::Decimal128(precision, scale),
                // DECIMAL256_MAX_PRECISION
                39.. => DataType::Decimal256(precision, scale),
            }
        }
        DType::Utf8(_) => DataType::Utf8View,
        DType::Binary(_) => DataType::BinaryView,
        DType::Union(..) => vortex_bail!("TODO(connor)[Union]: unimplemented"),
        DType::Variant(_) => vortex_bail!(
            "DType::Variant requires Arrow Field metadata; use to_arrow_schema or a Field helper"
        ),
        DType::Extension(ext_dtype) => {
            // NOTE: Temporal are the only builtin and default-loaded extension types, and they map
            // directly onto non-extension Arrow physical encodings. For this reason, we
            // choose to special-case them as part of this function rather than implementing them
            // as an import/export VTable.
            if let Some(temporal) = ext_dtype.metadata_opt::<AnyTemporal>() {
                return Ok(match temporal {
                    TemporalMetadata::Timestamp(unit, tz) => {
                        DataType::Timestamp(to_arrow_time_unit(*unit)?, tz.clone())
                    }
                    TemporalMetadata::Date(unit) => match unit {
                        TimeUnit::Days => DataType::Date32,
                        TimeUnit::Milliseconds => DataType::Date64,
                        TimeUnit::Nanoseconds | TimeUnit::Microseconds | TimeUnit::Seconds => {
                            vortex_panic!("Invalid TimeUnit {} for {}", unit, ext_dtype.id())
                        }
                    },
                    TemporalMetadata::Time(unit) => match unit {
                        TimeUnit::Seconds => DataType::Time32(ArrowTimeUnit::Second),
                        TimeUnit::Milliseconds => DataType::Time32(ArrowTimeUnit::Millisecond),
                        TimeUnit::Microseconds => DataType::Time64(ArrowTimeUnit::Microsecond),
                        TimeUnit::Nanoseconds => DataType::Time64(ArrowTimeUnit::Nanosecond),
                        TimeUnit::Days => {
                            vortex_panic!("Invalid TimeUnit {} for {}", unit, ext_dtype.id())
                        }
                    },
                });
            }

            vortex_bail!("Unsupported extension type \"{}\"", ext_dtype.id())
        }
        DType::List(..) | DType::FixedSizeList(..) | DType::Map(..) | DType::Struct(..) => {
            unreachable!("container dtypes are handled iteratively")
        }
    })
}

/// Naive conversion from a Vortex `DType` to the nearest Arrow physical data type.
pub(crate) fn to_data_type_naive(dtype: &DType) -> VortexResult<DataType> {
    let mut stack = Vec::<ToArrowFrame>::new();
    let mut current = dtype.clone();
    loop {
        let mut completed = match &current {
            DType::List(elem_dtype, _) => {
                let nullable = elem_dtype.nullability().into();
                let next = elem_dtype.as_ref().clone();
                stack.push(ToArrowFrame::List { nullable });
                current = next;
                continue;
            }
            DType::FixedSizeList(elem_dtype, size, _) => {
                let nullable = elem_dtype.nullability().into();
                let size = *size as i32;
                let next = elem_dtype.as_ref().clone();
                stack.push(ToArrowFrame::FixedSizeList { size, nullable });
                current = next;
                continue;
            }
            DType::Map(map_dtype, _) => {
                let value_dtype = map_dtype.value_dtype();
                let value_nullable = value_dtype.is_nullable();
                let keys_sorted = map_dtype.keys_sorted();
                let next = map_dtype.key_dtype();
                stack.push(ToArrowFrame::MapNeedValue {
                    value_dtype,
                    value_nullable,
                    keys_sorted,
                });
                current = next;
                continue;
            }
            DType::Struct(struct_dtype, _) => {
                if let Some(first_dtype) = struct_dtype.field_by_index(0) {
                    let count = struct_dtype.nfields();
                    stack.push(ToArrowFrame::Struct {
                        fields: struct_dtype.clone(),
                        next_idx: 0,
                        built: Vec::with_capacity(count),
                    });
                    current = first_dtype;
                    continue;
                }
                DataType::Struct(Fields::empty())
            }
            _ => to_data_type_naive_leaf(&current)?,
        };

        loop {
            let Some(frame) = stack.pop() else {
                return Ok(completed);
            };
            match frame {
                ToArrowFrame::List { nullable } => {
                    completed = DataType::List(FieldRef::new(Field::new_list_field(
                        completed, nullable,
                    )));
                }
                ToArrowFrame::FixedSizeList { size, nullable } => {
                    completed = DataType::FixedSizeList(
                        FieldRef::new(Field::new_list_field(completed, nullable)),
                        size,
                    );
                }
                ToArrowFrame::MapNeedValue {
                    value_dtype,
                    value_nullable,
                    keys_sorted,
                } => {
                    stack.push(ToArrowFrame::MapHaveValue {
                        key_type: completed,
                        value_nullable,
                        keys_sorted,
                    });
                    current = value_dtype;
                    break;
                }
                ToArrowFrame::MapHaveValue {
                    key_type,
                    value_nullable,
                    keys_sorted,
                } => {
                    let key = Field::new("key", key_type, false);
                    let value = Field::new("value", completed, value_nullable);
                    let entries =
                        Field::new_struct("entries", Fields::from(vec![key, value]), false);
                    completed = DataType::Map(FieldRef::new(entries), keys_sorted);
                }
                ToArrowFrame::Struct {
                    fields,
                    next_idx,
                    mut built,
                } => {
                    let name = fields.field_name(next_idx).map_or("", FieldName::as_ref);
                    let nullable = fields
                        .field_by_index(next_idx)
                        .is_some_and(|dt| dt.is_nullable());
                    built.push(FieldRef::from(Field::new(name, completed, nullable)));
                    let next = next_idx + 1;
                    if let Some(next_dtype) = fields.field_by_index(next) {
                        stack.push(ToArrowFrame::Struct {
                            fields,
                            next_idx: next,
                            built,
                        });
                        current = next_dtype;
                        break;
                    }
                    completed = DataType::Struct(Fields::from(built));
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::FieldRef;
    use arrow_schema::Fields;
    use arrow_schema::Schema;
    use rstest::fixture;
    use rstest::rstest;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::FieldName;
    use vortex_array::dtype::FieldNames;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::StructFields;

    use super::*;
    use crate::ArrowSession;

    #[test]
    fn test_dtype_conversion_success() {
        assert_eq!(to_data_type_naive(&DType::Null).unwrap(), DataType::Null);

        assert_eq!(
            to_data_type_naive(&DType::Bool(Nullability::NonNullable)).unwrap(),
            DataType::Boolean
        );

        assert_eq!(
            to_data_type_naive(&DType::Primitive(PType::U64, Nullability::NonNullable)).unwrap(),
            DataType::UInt64
        );

        assert_eq!(
            to_data_type_naive(&DType::Utf8(Nullability::NonNullable)).unwrap(),
            DataType::Utf8View
        );

        assert_eq!(
            to_data_type_naive(&DType::Binary(Nullability::NonNullable)).unwrap(),
            DataType::BinaryView
        );

        assert_eq!(
            to_data_type_naive(&DType::struct_(
                [
                    ("field_a", DType::Bool(false.into())),
                    ("field_b", DType::Utf8(true.into()))
                ],
                Nullability::NonNullable,
            ))
            .unwrap(),
            DataType::Struct(Fields::from(vec![
                FieldRef::from(Field::new("field_a", DataType::Boolean, false)),
                FieldRef::from(Field::new("field_b", DataType::Utf8View, true)),
            ]))
        );
    }

    #[test]
    fn test_reject_timestamp_invalid_timezone() {
        let dtype = DataType::Timestamp(ArrowTimeUnit::Second, Some("Not/A/Timezone".into()));
        assert!(from_arrow_data_type(&dtype, Nullability::Nullable).is_err());
    }

    #[rstest]
    #[case(1, DataType::Decimal128(1, 0))]
    #[case(38, DataType::Decimal128(38, 0))]
    #[case(39, DataType::Decimal256(39, 0))]
    #[case(76, DataType::Decimal256(76, 0))]
    fn test_decimal_dtype_to_arrow(#[case] precision: u8, #[case] expected: DataType) {
        use vortex_array::dtype::DecimalDType;

        let dtype = DType::Decimal(DecimalDType::new(precision, 0), Nullability::NonNullable);
        assert_eq!(to_data_type_naive(&dtype).unwrap(), expected);
    }

    #[rstest]
    #[case::decimal32(DataType::Decimal32(1, 2))]
    #[case::decimal64(DataType::Decimal64(1, 2))]
    #[case::decimal128(DataType::Decimal128(1, 2))]
    #[case::decimal256(DataType::Decimal256(1, 2))]
    #[case::zero_precision(DataType::Decimal128(0, 0))]
    #[case::excessive_precision(DataType::Decimal256(77, 0))]
    fn test_malformed_decimal_dtype_from_arrow(#[case] data_type: DataType) {
        assert!(from_arrow_data_type(&data_type, Nullability::Nullable).is_err());
    }

    #[test]
    fn test_variant_dtype_to_arrow_dtype_errors() {
        let err = to_data_type_naive(&DType::Variant(Nullability::NonNullable))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Variant"));
    }

    #[test]
    fn infer_nullable_list_element() {
        let list_non_nullable = DType::List(
            Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
            Nullability::Nullable,
        );

        let arrow_list_non_nullable = to_data_type_naive(&list_non_nullable).unwrap();

        let list_nullable = DType::List(
            Arc::new(DType::Primitive(PType::I64, Nullability::Nullable)),
            Nullability::Nullable,
        );
        let arrow_list_nullable = to_data_type_naive(&list_nullable).unwrap();

        assert_ne!(arrow_list_non_nullable, arrow_list_nullable);
        assert_eq!(
            arrow_list_nullable,
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
        );
        assert_eq!(
            arrow_list_non_nullable,
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false))),
        );
    }

    #[fixture]
    fn the_struct() -> StructFields {
        StructFields::new(
            FieldNames::from([
                FieldName::from("field_a"),
                FieldName::from("field_b"),
                FieldName::from("field_c"),
            ]),
            vec![
                DType::Bool(Nullability::NonNullable),
                DType::Utf8(Nullability::NonNullable),
                DType::Primitive(PType::I32, Nullability::Nullable),
            ],
        )
    }

    #[rstest]
    fn test_schema_conversion(the_struct: StructFields) {
        let schema_nonnull = DType::Struct(the_struct, Nullability::NonNullable);

        assert_eq!(
            ArrowSession::default()
                .to_arrow_schema(&schema_nonnull)
                .unwrap(),
            Schema::new(Fields::from(vec![
                Field::new("field_a", DataType::Boolean, false),
                Field::new("field_b", DataType::Utf8View, false),
                Field::new("field_c", DataType::Int32, true),
            ]))
        );
    }

    #[test]
    fn test_unicode_field_names_roundtrip() {
        // Regression test for https://github.com/vortex-data/vortex/issues/5979.

        // Unicode characters in field names should survive an Arrow roundtrip without
        // double-escaping.
        let unicode_field_name = "\u{5}=A";
        let original_dtype = DType::struct_(
            [(
                unicode_field_name,
                DType::Primitive(PType::I8, Nullability::Nullable),
            )],
            Nullability::NonNullable,
        );

        let arrow_dtype = to_data_type_naive(&original_dtype).unwrap();
        assert_eq!(
            arrow_dtype,
            DataType::Struct(Fields::from(vec![Field::new(
                "\u{5}=A",
                DataType::Int8,
                true
            )]))
        );
        let roundtripped_dtype =
            from_arrow_data_type(&arrow_dtype, Nullability::NonNullable).unwrap();

        assert_eq!(original_dtype, roundtripped_dtype);
    }

    #[test]
    fn test_unicode_field_names_nested_roundtrip() {
        // Regression test for https://github.com/vortex-data/vortex/issues/5979.

        // Nested structs with unicode field names should also survive an Arrow roundtrip.
        let inner_struct = DType::struct_(
            [(
                "\u{6}=inner",
                DType::Primitive(PType::I32, Nullability::Nullable),
            )],
            Nullability::Nullable,
        );
        let original_dtype =
            DType::struct_([("\u{7}=outer", inner_struct)], Nullability::NonNullable);

        let arrow_dtype = to_data_type_naive(&original_dtype).unwrap();
        assert_eq!(
            arrow_dtype,
            DataType::Struct(Fields::from(vec![Field::new(
                "\u{7}=outer",
                DataType::Struct(Fields::from(vec![Field::new(
                    "\u{6}=inner",
                    DataType::Int32,
                    true
                )])),
                true
            )]))
        );
        let roundtripped_dtype =
            from_arrow_data_type(&arrow_dtype, Nullability::NonNullable).unwrap();

        assert_eq!(original_dtype, roundtripped_dtype);
    }

    #[test]
    fn map_dtype_roundtrip_uses_conventional_export_names_and_positional_import() -> VortexResult<()>
    {
        let dtype = DType::map(
            DType::Primitive(PType::I32, Nullability::NonNullable),
            DType::Utf8(Nullability::Nullable),
            true,
            Nullability::Nullable,
        )?;

        let arrow = to_data_type_naive(&dtype)?;
        let DataType::Map(entries, keys_sorted) = &arrow else {
            panic!("expected Map, got {arrow:?}");
        };
        assert!(*keys_sorted);
        assert_eq!(entries.name(), "entries");
        assert!(!entries.is_nullable());
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries to be a struct");
        };
        assert_eq!(fields[0].name(), "key");
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "value");
        assert!(fields[1].is_nullable());
        assert_eq!(from_arrow_data_type(&arrow, Nullability::Nullable)?, dtype);

        let positional = DataType::Map(
            Arc::new(Field::new_struct(
                "anything",
                Fields::from(vec![
                    Field::new("first", DataType::Int32, false),
                    Field::new("second", DataType::Utf8, true),
                ]),
                false,
            )),
            false,
        );
        assert_eq!(
            from_arrow_data_type(&positional, Nullability::NonNullable)?,
            DType::map(
                DType::Primitive(PType::I32, Nullability::NonNullable),
                DType::Utf8(Nullability::Nullable),
                false,
                Nullability::NonNullable,
            )?
        );

        Ok(())
    }

    #[test]
    fn map_dtype_import_rejects_invalid_arrow_shape() {
        let invalid_entries = [
            Field::new_struct(
                "entries",
                Fields::from(vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, true),
                ]),
                true,
            ),
            Field::new("entries", DataType::Int32, false),
            Field::new_struct(
                "entries",
                Fields::from(vec![Field::new("key", DataType::Int32, false)]),
                false,
            ),
            Field::new_struct(
                "entries",
                Fields::from(vec![
                    Field::new("key", DataType::Int32, true),
                    Field::new("value", DataType::Utf8, true),
                ]),
                false,
            ),
        ];

        for entries in invalid_entries {
            let data_type = DataType::Map(Arc::new(entries), false);
            assert!(from_arrow_data_type(&data_type, Nullability::NonNullable).is_err());
        }
    }

    // Regression test for https://github.com/vortex-data/vortex/issues/8346: unsupported Arrow
    // types must return an error instead of panicking with `unimplemented!`.
    #[rstest]
    #[case::duration(DataType::Duration(ArrowTimeUnit::Microsecond))]
    #[case::interval(DataType::Interval(arrow_schema::IntervalUnit::DayTime))]
    #[case::fixed_size_binary(DataType::FixedSizeBinary(3))]
    fn test_try_from_arrow_unsupported_type_errors(#[case] data_type: DataType) {
        let err = from_arrow_data_type(&data_type, Nullability::NonNullable)
            .expect_err("unsupported Arrow type should not convert")
            .to_string();
        assert!(err.contains("not supported"), "unexpected error: {err}");

        // The same unsupported type nested in a field or schema must also error cleanly.
        let field = Field::new("c0", data_type, true);
        assert!(from_arrow_field_naive(&field).is_err());
        let schema = Schema::new(vec![field]);
        assert!(ArrowSession::default().from_arrow_schema(&schema).is_err());
    }

    #[test]
    fn test_try_from_arrow_supported_type_succeeds() -> VortexResult<()> {
        let dtype = from_arrow_data_type(&DataType::Int32, Nullability::Nullable)?;
        assert_eq!(dtype, DType::Primitive(PType::I32, Nullability::Nullable));
        Ok(())
    }

    mod deep_tests {
        use std::sync::Arc;
        use std::thread;

        use arrow_schema::DataType;
        use arrow_schema::Field;
        use arrow_schema::FieldRef;
        use arrow_schema::Fields;
        use vortex_array::dtype::DType;
        use vortex_array::dtype::Nullability;
        use vortex_array::dtype::PType;
        use vortex_error::VortexResult;
        use vortex_error::vortex_err;

        use super::super::from_arrow_data_type;
        use super::super::from_arrow_field_naive;
        use super::super::from_arrow_fields_naive;
        use super::super::to_data_type_naive;

        fn build_deep_arrow_field(depth: usize, leaf: DataType, leaf_nullable: bool) -> Field {
            let mut field = Field::new("leaf", leaf, leaf_nullable);
            for level in 0..depth {
                let data_type = match level % 5 {
                    0 => DataType::List(Arc::new(field)),
                    1 => DataType::LargeList(Arc::new(field)),
                    2 => DataType::FixedSizeList(Arc::new(field), 1),
                    3 => DataType::Struct(Fields::from(vec![Arc::new(field)])),
                    _ => {
                        let entries = Field::new_struct(
                            "entries",
                            Fields::from(vec![
                                Field::new("key", DataType::Int32, false),
                                field,
                            ]),
                            false,
                        );
                        DataType::Map(Arc::new(entries), false)
                    }
                };
                field = Field::new(format!("f_{level}"), data_type, true);
            }
            field
        }

        #[test]
        fn deep_naive_dtype_conversions_on_small_stack() -> VortexResult<()> {
            let depth = 2048;
            let deep_field = build_deep_arrow_field(depth, DataType::Int32, true);
            let bad_field = build_deep_arrow_field(depth, DataType::FixedSizeBinary(3), true);

            let handle = thread::Builder::new()
                .stack_size(128 * 1024)
                .spawn(move || -> VortexResult<(DType, DataType)> {
                    let from_field = from_arrow_field_naive(&deep_field)?;
                    let from_dt = from_arrow_data_type(
                        deep_field.data_type(),
                        Nullability::from(deep_field.is_nullable()),
                    )?;
                    assert_eq!(from_field, from_dt);

                    let fields = Fields::from(vec![FieldRef::new(deep_field.clone())]);
                    let from_fields = from_arrow_fields_naive(&fields)?;
                    assert_eq!(from_fields.field_by_index(0), Some(from_field.clone()));

                    let back_dt = to_data_type_naive(&from_field)?;
                    let roundtrip = from_arrow_data_type(&back_dt, Nullability::Nullable)?;
                    assert_eq!(roundtrip, from_field);

                    assert!(from_arrow_field_naive(&bad_field).is_err());
                    Ok((from_field, back_dt))
                })
                .map_err(|err| vortex_err!("spawn 128 KiB thread: {err}"))?;

            let (vx_dtype, arrow_dt) = handle
                .join()
                .map_err(|err| vortex_err!("128 KiB thread panicked: {err:?}"))??;
            assert!(matches!(vx_dtype, DType::FixedSizeList(..) | DType::List(..) | DType::Map(..) | DType::Struct(..)));
            assert!(matches!(arrow_dt, DataType::FixedSizeList(..) | DataType::List(..) | DataType::Map(..) | DataType::Struct(..)));
            Ok(())
        }

        #[test]
        fn shallow_parity_covers_dict_and_ree_and_variant() -> VortexResult<()> {
            let inner = Field::new("v", DataType::Int32, false);
            let wrapped = Field::new(
                "d",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::RunEndEncoded(
                        Arc::new(Field::new("run_ends", DataType::Int32, false)),
                        Arc::new(inner),
                    )),
                ),
                true,
            );
            let outer = DataType::Struct(Fields::from(vec![wrapped]));
            let vx = from_arrow_data_type(&outer, Nullability::NonNullable)?;
            let DType::Struct(sf, Nullability::NonNullable) = &vx else {
                panic!("expected Struct, got {vx:?}");
            };
            assert_eq!(
                sf.field_by_index(0),
                Some(DType::Primitive(PType::I32, Nullability::Nullable))
            );
            let exported = to_data_type_naive(&vx)?;
            assert!(matches!(exported, DataType::Struct(_)));

            let variant_field = Field::new("var", DataType::Null, true).with_metadata(
                [(
                    "ARROW:extension:name".to_string(),
                    "arrow.parquet.variant".to_string(),
                )]
                .into(),
            );
            assert_eq!(
                from_arrow_field_naive(&variant_field)?,
                DType::Variant(Nullability::Nullable)
            );
            assert!(to_data_type_naive(&DType::Variant(Nullability::Nullable)).is_err());
            Ok(())
        }
    }
}
