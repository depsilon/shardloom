//! Iceberg identity values retain their field IDs and manifest storage types.

use super::{Result, ScalarValue, Snapshot, failed, metadata, schema};
use arrow_schema::DataType;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(super) struct IdentityValue {
    dtype: DataType,
    value: ScalarValue,
}

pub(super) fn identities(
    snapshot: &mut Snapshot,
    spec: &[Value],
    shape: &[DataType],
    values: &Map<String, Value>,
) -> Result<BTreeMap<i32, IdentityValue>> {
    let mut result = BTreeMap::new();
    for (field, dtype) in spec.iter().zip(shape) {
        if metadata::string(field, "transform")? != "identity" {
            continue;
        }
        let id = i32::try_from(metadata::integer(field, "source-id")?).map_err(failed)?;
        let source = snapshot.historical_fields.get(&id).ok_or_else(|| {
            failed("Iceberg identity partition source is absent from retained schemas")
        })?;
        if !schema::compatible(dtype, source.field.data_type(), true) {
            return Err(failed(
                "Iceberg identity partition has an incompatible source type",
            ));
        }
        let value = values
            .get(metadata::string(field, "name")?)
            .ok_or_else(|| failed("Iceberg identity partition value is absent"))?;
        snapshot
            .metadata
            .retain(value.to_string().len() as u64 * 32 + 4096)?;
        let value = scalar(dtype, value)?;
        if result
            .insert(
                id,
                IdentityValue {
                    dtype: dtype.clone(),
                    value,
                },
            )
            .is_some()
        {
            return Err(failed("duplicate Iceberg identity partition source"));
        }
    }
    Ok(result)
}

impl IdentityValue {
    pub(super) fn bytes(&self) -> u64 {
        metadata::scalar_bytes(&self.value)
    }

    pub(super) fn scalar(&self, target: &DataType) -> Result<ScalarValue> {
        if !schema::compatible(&self.dtype, target, true) {
            return Err(failed(
                "Iceberg identity partition is incompatible with the requested field",
            ));
        }
        let mut value = self.value.clone();
        if let (ScalarValue::Decimal128 { precision, .. }, DataType::Decimal128(target, _)) =
            (&mut value, target)
        {
            *precision = *target;
        }
        Ok(value)
    }
}

fn scalar(dtype: &DataType, value: &Value) -> Result<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }
    let invalid = || failed("invalid typed Iceberg identity partition value");
    Ok(match dtype {
        DataType::Boolean => ScalarValue::Boolean(value.as_bool().ok_or_else(invalid)?),
        DataType::Int32 => ScalarValue::Int64(i64::from(
            i32::try_from(value.as_i64().ok_or_else(invalid)?).map_err(failed)?,
        )),
        DataType::Int64 => ScalarValue::Int64(value.as_i64().ok_or_else(invalid)?),
        DataType::Float32 | DataType::Float64 => {
            let number = if let Some(bits) = value.get("f32").and_then(Value::as_u64) {
                f64::from(f32::from_bits(u32::try_from(bits).map_err(failed)?))
            } else if dtype == &DataType::Float64 {
                f64::from_bits(
                    value
                        .get("f64")
                        .and_then(Value::as_u64)
                        .ok_or_else(invalid)?,
                )
            } else {
                return Err(invalid());
            };
            ScalarValue::Float64(number)
        }
        DataType::Utf8 => ScalarValue::Utf8(value.as_str().ok_or_else(invalid)?.into()),
        DataType::Binary => ScalarValue::Binary(
            value
                .as_array()
                .ok_or_else(invalid)?
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|v| u8::try_from(v).ok())
                        .ok_or_else(invalid)
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        DataType::Decimal128(precision, scale) => {
            let number = value
                .as_str()
                .ok_or_else(invalid)?
                .parse::<i128>()
                .map_err(failed)?;
            if number.unsigned_abs() >= 10_u128.pow(u32::from(*precision)) {
                return Err(failed(
                    "Iceberg identity partition exceeds decimal precision",
                ));
            }
            ScalarValue::Decimal128 {
                value: number,
                precision: *precision,
                scale: u8::try_from(*scale).map_err(failed)?,
            }
        }
        DataType::Date32 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
            ScalarValue::Temporal(shardloom_core::temporal::TemporalValue::new(
                value.as_i64().ok_or_else(invalid)?,
                crate::temporal_arrow::dtype(dtype).ok_or_else(invalid)?,
            )?)
        }
        _ => return Err(failed("unsupported Iceberg identity partition type")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;
    use serde_json::json;

    #[test]
    fn identity_values_preserve_exact_logical_types_and_admitted_widening() {
        let cases = [
            (DataType::Boolean, json!(true), ScalarValue::Boolean(true)),
            (DataType::Int32, json!(-17), ScalarValue::Int64(-17)),
            (
                DataType::Int64,
                json!(i64::MAX),
                ScalarValue::Int64(i64::MAX),
            ),
            (DataType::Utf8, json!(""), ScalarValue::Utf8(String::new())),
            (
                DataType::Binary,
                json!([0, 128, 255]),
                ScalarValue::Binary(vec![0, 128, 255]),
            ),
            (
                DataType::Decimal128(10, 2),
                json!("-1234"),
                ScalarValue::Decimal128 {
                    value: -1234,
                    precision: 10,
                    scale: 2,
                },
            ),
        ];
        for (dtype, wire, expected) in cases {
            assert_eq!(scalar(&dtype, &wire).unwrap(), expected);
            assert_eq!(scalar(&dtype, &Value::Null).unwrap(), ScalarValue::Null);
        }
        for dtype in [
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            let ScalarValue::Temporal(value) = scalar(&dtype, &json!(42)).unwrap() else {
                panic!("temporal value lost its metadata");
            };
            assert_eq!(value.value(), 42);
            assert_eq!(crate::temporal_arrow::data_type(value.dtype()), dtype);
        }
        let value = IdentityValue {
            dtype: DataType::Decimal128(10, 2),
            value: scalar(&DataType::Decimal128(10, 2), &json!("-1234")).unwrap(),
        };
        assert_eq!(
            value.scalar(&DataType::Decimal128(15, 2)).unwrap(),
            ScalarValue::Decimal128 {
                value: -1234,
                precision: 15,
                scale: 2
            }
        );
        assert!(value.scalar(&DataType::Decimal128(15, 3)).is_err());
        for (dtype, wire) in [
            (DataType::Int32, json!(i64::MAX)),
            (DataType::Binary, json!([256])),
            (DataType::Decimal128(2, 0), json!("100")),
            (DataType::Time64(TimeUnit::Microsecond), json!(-1)),
        ] {
            assert!(scalar(&dtype, &wire).is_err());
        }
        for (dtype, wire) in [
            (DataType::Float32, json!({"f32": (-0.0_f32).to_bits()})),
            (DataType::Float64, json!({"f64": (-0.0_f64).to_bits()})),
        ] {
            let ScalarValue::Float64(value) = scalar(&dtype, &wire).unwrap() else {
                panic!("float value lost its representation");
            };
            assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
        }
    }
}
