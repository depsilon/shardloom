// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom provider extension implementing Apache ORC v1 type trees.

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use snafu::ensure;

use crate::error::{Result, UnexpectedSnafu};
use crate::proto;

/// Build IDs and children in the same preorder used by the stripe encoders.
pub(crate) fn serialize(schema: &SchemaRef) -> Result<Vec<proto::Type>> {
    let mut types = Vec::new();
    append(&DataType::Struct(schema.fields().clone()), &mut types, 0)?;
    Ok(types)
}

fn append(dtype: &DataType, types: &mut Vec<proto::Type>, depth: usize) -> Result<u32> {
    use proto::r#type::Kind;
    ensure!(
        depth <= 64,
        UnexpectedSnafu {
            msg: "ORC schema nesting exceeds 64 levels"
        }
    );
    let id = u32::try_from(types.len()).map_err(|_| {
        UnexpectedSnafu {
            msg: "ORC schema exceeds column ID range",
        }
        .build()
    })?;
    let mut node = proto::Type::default();
    let kind = match dtype {
        DataType::Float32 => Kind::Float,
        DataType::Float64 => Kind::Double,
        DataType::Int8 => Kind::Byte,
        DataType::Int16 => Kind::Short,
        DataType::Int32 => Kind::Int,
        DataType::Int64 => Kind::Long,
        DataType::Boolean => Kind::Boolean,
        DataType::Utf8 | DataType::LargeUtf8 => Kind::String,
        DataType::Binary | DataType::LargeBinary => Kind::Binary,
        DataType::Date32 => Kind::Date,
        DataType::Timestamp(_, None) => Kind::Timestamp,
        DataType::Timestamp(_, Some(zone)) if zone.as_ref() == "UTC" => Kind::TimestampInstant,
        DataType::Decimal128(precision, scale) => {
            ensure!(
                (1..=38).contains(precision) && *scale >= 0 && *scale <= *precision as i8,
                UnexpectedSnafu {
                    msg: "ORC decimal requires precision 1..38 and scale 0..precision"
                }
            );
            node.precision = Some(u32::from(*precision));
            node.scale = Some(u32::try_from(*scale).map_err(|_| {
                UnexpectedSnafu {
                    msg: "ORC decimal scale must be nonnegative",
                }
                .build()
            })?);
            Kind::Decimal
        }
        DataType::List(_) | DataType::LargeList(_) => Kind::List,
        DataType::Struct(_) => Kind::Struct,
        _ => {
            return UnexpectedSnafu {
                msg: format!("unsupported ORC writer datatype: {dtype}"),
            }
            .fail()
        }
    };
    node.kind = Some(kind.into());
    if let DataType::Timestamp(unit, _) = dtype {
        node.attributes.push(proto::StringPair {
            key: Some(crate::schema::ARROW_TIMESTAMP_UNIT_ATTRIBUTE.to_owned()),
            value: Some(
                match unit {
                    TimeUnit::Second => "s",
                    TimeUnit::Millisecond => "ms",
                    TimeUnit::Microsecond => "us",
                    TimeUnit::Nanosecond => "ns",
                }
                .to_owned(),
            ),
        });
    }
    types.push(node);
    match dtype {
        DataType::Struct(fields) => {
            let mut subtypes = Vec::with_capacity(fields.len());
            let mut names = Vec::with_capacity(fields.len());
            for field in fields {
                subtypes.push(append(field.data_type(), types, depth + 1)?);
                names.push(field.name().clone());
            }
            types[id as usize].subtypes = subtypes;
            types[id as usize].field_names = names;
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let child = append(field.data_type(), types, depth + 1)?;
            types[id as usize].subtypes.push(child);
        }
        _ => {}
    }
    Ok(id)
}

pub(crate) fn has_naive_timestamp(dtype: &DataType) -> bool {
    match dtype {
        DataType::Timestamp(_, None) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_naive_timestamp(field.data_type())),
        DataType::List(field) | DataType::LargeList(field) => {
            has_naive_timestamp(field.data_type())
        }
        _ => false,
    }
}
