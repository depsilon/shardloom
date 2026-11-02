//! Version-specific primitive logical schemas and typed partition constants.

use super::{ColumnMapping, Result, ScalarValue, TableField, failed, metadata};
use arrow_schema::{DataType, Field, TimeUnit};
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

pub(super) fn iceberg(value: &Value) -> Result<Vec<TableField>> {
    let mut fields = Vec::new();
    for field in metadata::array(value, "fields")? {
        let id = i32::try_from(metadata::integer(field, "id")?).map_err(failed)?;
        if id <= 0 {
            return Err(failed("Iceberg field IDs must be positive"));
        }
        let name = metadata::string(field, "name")?;
        let required = field
            .get("required")
            .and_then(Value::as_bool)
            .ok_or_else(|| failed("Iceberg field requires boolean required"))?;
        if ["initial-default", "write-default"]
            .iter()
            .any(|key| field.get(key).is_some_and(|v| !v.is_null()))
        {
            return Err(failed(
                "Iceberg non-null field defaults are outside v1/v2 admission",
            ));
        }
        let kind = field
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| failed("nested Iceberg schema evolution is not admitted"))?;
        let dtype = match kind {
            "boolean" => DataType::Boolean,
            "int" => DataType::Int32,
            "long" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "date" => DataType::Date32,
            "time" => DataType::Time64(TimeUnit::Microsecond),
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "string" => DataType::Utf8,
            "binary" => DataType::Binary,
            kind if kind.starts_with("decimal(") => decimal_type(kind)?,
            _ => return Err(failed(format!("Iceberg field type {kind} is not admitted"))),
        };
        fields.push(TableField {
            field: Arc::new(
                Field::new(name, dtype, !required).with_metadata(HashMap::from([(
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.into(),
                    id.to_string(),
                )])),
            ),
            id: Some(id),
            physical_name: name.into(),
        });
    }
    validate(&fields, false)?;
    Ok(fields)
}

pub(super) fn delta(
    value: &Value,
    mode: ColumnMapping,
    timestamp_ntz: bool,
) -> Result<Vec<TableField>> {
    if metadata::string(value, "type")? != "struct" {
        return Err(failed("Delta schema root must be a struct"));
    }
    let mut fields = Vec::new();
    for field in metadata::array(value, "fields")? {
        let name = metadata::string(field, "name")?;
        let nullable = field
            .get("nullable")
            .and_then(Value::as_bool)
            .ok_or_else(|| failed("Delta field requires boolean nullable"))?;
        let kind = field
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| failed("nested Delta schema evolution is not admitted"))?;
        let dtype = match kind {
            "boolean" => DataType::Boolean,
            "byte" => DataType::Int8,
            "short" => DataType::Int16,
            "integer" => DataType::Int32,
            "long" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "date" => DataType::Date32,
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp_ntz" if timestamp_ntz => DataType::Timestamp(TimeUnit::Microsecond, None),
            "string" => DataType::Utf8,
            "binary" => DataType::Binary,
            "void" => DataType::Null,
            kind if kind.starts_with("decimal(") => decimal_type(kind)?,
            _ => {
                return Err(failed(format!(
                    "Delta field type/feature {kind} is not admitted"
                )));
            }
        };
        let info = field
            .get("metadata")
            .ok_or_else(|| failed("Delta field metadata is absent"))?;
        metadata::object(info)?;
        if info
            .get("delta.typeChanges")
            .is_some_and(|value| value.as_array().is_none_or(|v| !v.is_empty()))
        {
            return Err(failed("Delta type-widening history is not admitted"));
        }
        let (id, physical_name) = match mode {
            ColumnMapping::Name => (None, name.into()),
            ColumnMapping::FieldId | ColumnMapping::PhysicalName => {
                let id = i32::try_from(metadata::integer(info, "delta.columnMapping.id")?)
                    .map_err(failed)?;
                if id <= 0 {
                    return Err(failed("Delta mapping IDs must be positive"));
                }
                (
                    Some(id),
                    metadata::string(info, "delta.columnMapping.physicalName")?.into(),
                )
            }
        };
        fields.push(TableField {
            field: Arc::new(Field::new(name, dtype, nullable)),
            id,
            physical_name,
        });
    }
    validate(&fields, true)?;
    Ok(fields)
}

pub(super) fn hudi(value: &Value) -> Result<Vec<TableField>> {
    if metadata::string(value, "type")? != "record" {
        return Err(failed("Hudi authoritative schema must be an Avro record"));
    }
    let mut fields = Vec::new();
    for field in metadata::array(value, "fields")? {
        let name = metadata::string(field, "name")?;
        let mut kind = field
            .get("type")
            .ok_or_else(|| failed("Hudi field type is absent"))?;
        let mut nullable = false;
        if let Some(union) = kind.as_array() {
            if union.len() != 2 || !union.iter().any(|v| v.as_str() == Some("null")) {
                return Err(failed(
                    "Hudi Avro unions require one null and one primitive branch",
                ));
            }
            nullable = true;
            kind = union
                .iter()
                .find(|v| v.as_str() != Some("null"))
                .expect("validated nullable union");
        }
        if field.get("default").is_some_and(|value| !value.is_null()) {
            return Err(failed("Hudi non-null schema defaults are not admitted"));
        }
        let base = kind
            .as_str()
            .or_else(|| kind.get("type").and_then(Value::as_str))
            .ok_or_else(|| failed("nested Hudi field evolution is not admitted"))?;
        let logical = kind.get("logicalType").and_then(Value::as_str);
        let dtype = match (base, logical) {
            ("boolean", None) => DataType::Boolean,
            ("int", None) => DataType::Int32,
            ("long", None) => DataType::Int64,
            ("float", None) => DataType::Float32,
            ("double", None) => DataType::Float64,
            ("string", None) => DataType::Utf8,
            ("bytes", None) => DataType::Binary,
            ("int", Some("date")) => DataType::Date32,
            ("long", Some("timestamp-micros")) => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            ("long", Some("local-timestamp-micros")) => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            ("bytes" | "fixed", Some("decimal")) => {
                let precision = metadata::unsigned(kind, "precision")?;
                let scale = kind.get("scale").map_or(Ok(0), |value| {
                    value
                        .as_u64()
                        .ok_or_else(|| failed("invalid Avro decimal scale"))
                })?;
                decimal_type(&format!("decimal({precision},{scale})"))?
            }
            _ => {
                return Err(failed(format!(
                    "Hudi field type {base}/{logical:?} is not admitted"
                )));
            }
        };
        fields.push(TableField {
            field: Arc::new(Field::new(name, dtype, nullable)),
            id: None,
            physical_name: name.into(),
        });
    }
    validate(&fields, false)?;
    Ok(fields)
}

fn validate(fields: &[TableField], insensitive: bool) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut physical = BTreeSet::new();
    for field in fields {
        let name = if insensitive {
            field.field.name().to_lowercase()
        } else {
            field.field.name().clone()
        };
        if name.is_empty()
            || !names.insert(name)
            || field.physical_name.is_empty()
            || !physical.insert(field.physical_name.as_str())
            || field.id.is_some_and(|id| !ids.insert(id))
        {
            return Err(failed(
                "table schema has duplicate/empty names, physical names or field IDs",
            ));
        }
    }
    Ok(())
}

fn decimal_type(kind: &str) -> Result<DataType> {
    let (precision, scale) = kind
        .strip_prefix("decimal(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.split_once(','))
        .ok_or_else(|| failed("invalid decimal type"))?;
    let precision: u8 = precision.trim().parse().map_err(failed)?;
    let scale: i8 = scale.trim().parse().map_err(failed)?;
    if precision == 0 || precision > 38 || scale < 0 || scale.cast_unsigned() > precision {
        return Err(failed(
            "table decimal requires 1..38 precision and 0..precision scale",
        ));
    }
    Ok(DataType::Decimal128(precision, scale))
}

pub(super) fn compatible(source: &DataType, target: &DataType, widen: bool) -> bool {
    source == target
        || (widen
            && matches!(
                (source, target),
                (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64)
            ))
        || (widen
            && matches!((source, target), (DataType::Decimal128(p, s), DataType::Decimal128(q, t)) if s == t && p <= q))
}

pub(super) fn partition(dtype: &DataType, value: Option<&str>) -> Result<ScalarValue> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(ScalarValue::Null);
    };
    macro_rules! int {
        ($ty:ty) => {
            ScalarValue::Int64(i64::from(value.parse::<$ty>().map_err(failed)?))
        };
    }
    Ok(match dtype {
        DataType::Utf8 => ScalarValue::Utf8(value.into()),
        DataType::Boolean => ScalarValue::Boolean(value.parse().map_err(failed)?),
        DataType::Int8 => int!(i8),
        DataType::Int16 => int!(i16),
        DataType::Int32 => int!(i32),
        DataType::Int64 => ScalarValue::Int64(value.parse().map_err(failed)?),
        DataType::Float32 => ScalarValue::Float64(f64::from(value.parse::<f32>().map_err(failed)?)),
        DataType::Float64 => ScalarValue::Float64(value.parse().map_err(failed)?),
        DataType::Decimal128(precision, scale) => {
            let scale = u8::try_from(*scale).map_err(failed)?;
            let number = decimal_partition(value, *precision, scale)?;
            ScalarValue::Decimal128 {
                value: number,
                precision: *precision,
                scale,
            }
        }
        dtype if crate::temporal_arrow::dtype(dtype).is_some() => {
            // Delta's wire grammar uses a space for civil timestamps. Keep
            // the shared ISO codec strict and normalize only this boundary.
            let normalized;
            let value = if matches!(dtype, DataType::Timestamp(_, _)) && value.contains(' ') {
                if !value.is_ascii() || value.len() > 128 {
                    return Err(failed(
                        "Delta timestamp partition requires bounded ASCII syntax",
                    ));
                }
                normalized = value.replacen(' ', "T", 1);
                normalized.as_str()
            } else {
                value
            };
            if let DataType::Timestamp(_, zone) = dtype {
                let (_, offset) = shardloom_core::temporal::civil::parse_timestamp(value)?;
                if zone.is_some() && offset.is_none() {
                    return Err(failed(
                        "Delta timestamp partition requires an explicit offset; the writer's timezone is unknown",
                    ));
                }
                if zone.is_none() && offset.is_some() {
                    return Err(failed(
                        "Delta timestamp_ntz partition must not contain a timezone offset",
                    ));
                }
            }
            let dtype = crate::temporal_arrow::dtype(dtype).expect("temporal type checked");
            shardloom_core::temporal::text::TemporalTextCodec::new(dtype)?.parse_scalar(value)?
        }
        _ => return Err(failed(format!("partition type {dtype:?} is not admitted"))),
    })
}

fn decimal_partition(value: &str, precision: u8, scale: u8) -> Result<i128> {
    let negative = value.starts_with('-');
    let value = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|n| n.is_ascii_digit())
        || fraction.len() > usize::from(scale)
    {
        return Err(failed("invalid exact decimal partition value"));
    }
    let padded = format!(
        "{whole}{fraction}{}",
        "0".repeat(usize::from(scale) - fraction.len())
    );
    let number = padded.parse::<i128>().map_err(failed)?;
    if number >= 10_i128.pow(u32::from(precision)) {
        return Err(failed("decimal partition exceeds declared precision"));
    }
    Ok(if negative { -number } else { number })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_partition_timezones_are_explicit_and_nulls_follow_wire_semantics() {
        let zoned = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let local = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert!(partition(&zoned, Some("1970-01-01 00:00:00")).is_err());
        assert!(partition(&zoned, Some("1970-01-01T00:00:00Z")).is_ok());
        assert!(partition(&local, Some("1970-01-01 00:00:00.123456")).is_ok());
        assert_eq!(
            partition(&local, Some("1970-01-01 00:00:00.123456")).unwrap(),
            partition(&local, Some("1970-01-01T00:00:00.123456")).unwrap()
        );
        assert!(partition(&local, Some("1970-01-01T00:00:00Z")).is_err());
        assert_eq!(
            partition(&DataType::Utf8, Some("")).unwrap(),
            ScalarValue::Null
        );
        assert_eq!(partition(&zoned, None).unwrap(), ScalarValue::Null);
    }
}
