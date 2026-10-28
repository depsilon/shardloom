//! Arrow boundary mapping for `ShardLoom` temporal values.

use arrow_array::{
    Array,
    builder::{ArrayBuilder, PrimitiveBuilder},
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::{DataType, TimeUnit};
use shardloom_core::{
    Result, ScalarValue, ShardLoomError,
    temporal::{TemporalDType, TemporalKind, TemporalUnit, TemporalValue},
};

fn err(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}

/// Standard Arrow logical type for validated temporal metadata.
#[must_use]
pub fn data_type(dtype: &TemporalDType) -> DataType {
    match dtype.kind() {
        TemporalKind::Date32 => DataType::Date32,
        TemporalKind::Date64 => DataType::Date64,
        TemporalKind::Time(TemporalUnit::Seconds) => DataType::Time32(TimeUnit::Second),
        TemporalKind::Time(TemporalUnit::Milliseconds) => DataType::Time32(TimeUnit::Millisecond),
        TemporalKind::Time(TemporalUnit::Microseconds) => DataType::Time64(TimeUnit::Microsecond),
        TemporalKind::Time(TemporalUnit::Nanoseconds) => DataType::Time64(TimeUnit::Nanosecond),
        TemporalKind::Timestamp { unit, timezone } => {
            DataType::Timestamp(unit_arrow(*unit), timezone.clone().map(Into::into))
        }
        TemporalKind::Duration(unit) => DataType::Duration(unit_arrow(*unit)),
    }
}
fn unit_arrow(unit: TemporalUnit) -> TimeUnit {
    match unit {
        TemporalUnit::Seconds => TimeUnit::Second,
        TemporalUnit::Milliseconds => TimeUnit::Millisecond,
        TemporalUnit::Microseconds => TimeUnit::Microsecond,
        TemporalUnit::Nanoseconds => TimeUnit::Nanosecond,
    }
}
fn unit_core(unit: TimeUnit) -> TemporalUnit {
    match unit {
        TimeUnit::Second => TemporalUnit::Seconds,
        TimeUnit::Millisecond => TemporalUnit::Milliseconds,
        TimeUnit::Microsecond => TemporalUnit::Microseconds,
        TimeUnit::Nanosecond => TemporalUnit::Nanoseconds,
    }
}

/// Recognize exact Arrow metadata, validating zones against pinned rules.
#[must_use]
pub fn dtype(dtype: &DataType) -> Option<TemporalDType> {
    let kind = match dtype {
        DataType::Date32 => TemporalKind::Date32,
        DataType::Date64 => TemporalKind::Date64,
        DataType::Time32(TimeUnit::Second) => TemporalKind::Time(TemporalUnit::Seconds),
        DataType::Time32(TimeUnit::Millisecond) => TemporalKind::Time(TemporalUnit::Milliseconds),
        DataType::Time64(TimeUnit::Microsecond) => TemporalKind::Time(TemporalUnit::Microseconds),
        DataType::Time64(TimeUnit::Nanosecond) => TemporalKind::Time(TemporalUnit::Nanoseconds),
        DataType::Timestamp(unit, zone) => {
            let zone = zone.as_ref().map(ToString::to_string);
            if let Some(z) = &zone {
                shardloom_core::temporal::timezone::validate_name(z).ok()?;
            }
            TemporalKind::Timestamp {
                unit: unit_core(*unit),
                timezone: zone,
            }
        }
        DataType::Duration(unit) => TemporalKind::Duration(unit_core(*unit)),
        _ => return None,
    };
    TemporalDType::new(kind).ok()
}

/// Append one exact temporal value to a schema-created Arrow builder.
///
/// # Errors
/// Rejects mismatched builder types/metadata and invalid temporal tick domains.
pub fn append(builder: &mut dyn ArrayBuilder, dtype: &DataType, value: Option<i64>) -> Result<()> {
    let td = self::dtype(dtype).ok_or_else(|| err("unsupported Arrow temporal dtype"))?;
    if let Some(v) = value {
        td.validate_value(v)?;
    }
    macro_rules! app {
        ($ty:ty, $conv:expr) => {{
            let b = builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<$ty>>()
                .ok_or_else(|| err("Arrow builder implementation does not match dtype"))?;
            // The enclosing schema owner creates this builder with make_builder.
            // Inspect metadata while empty without copying the accumulated column.
            if b.len() == 0 && b.finish_cloned().data_type() != dtype {
                return Err(err("Arrow builder metadata does not match its schema"));
            }
            match value {
                Some(v) => {
                    let converted: Result<_> = $conv(v);
                    b.append_value(converted?);
                }
                None => b.append_null(),
            };
        }};
    }
    match dtype {
        DataType::Date32 => app!(Date32Type, |v| i32::try_from(v)
            .map_err(|_| err("date32 out of range"))),
        DataType::Time32(TimeUnit::Second) => app!(Time32SecondType, |v| i32::try_from(v)
            .map_err(|_| err("time32 out of range"))),
        DataType::Time32(TimeUnit::Millisecond) => {
            app!(Time32MillisecondType, |v| i32::try_from(v)
                .map_err(|_| err("time32 out of range")));
        }
        DataType::Date64 => app!(Date64Type, Ok),
        DataType::Time64(TimeUnit::Microsecond) => app!(Time64MicrosecondType, Ok),
        DataType::Time64(TimeUnit::Nanosecond) => app!(Time64NanosecondType, Ok),
        DataType::Timestamp(TimeUnit::Second, _) => app!(TimestampSecondType, Ok),
        DataType::Timestamp(TimeUnit::Millisecond, _) => app!(TimestampMillisecondType, Ok),
        DataType::Timestamp(TimeUnit::Microsecond, _) => app!(TimestampMicrosecondType, Ok),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => app!(TimestampNanosecondType, Ok),
        DataType::Duration(TimeUnit::Second) => app!(DurationSecondType, Ok),
        DataType::Duration(TimeUnit::Millisecond) => app!(DurationMillisecondType, Ok),
        DataType::Duration(TimeUnit::Microsecond) => app!(DurationMicrosecondType, Ok),
        DataType::Duration(TimeUnit::Nanosecond) => app!(DurationNanosecondType, Ok),
        _ => return Err(err("unsupported Arrow temporal dtype")),
    }
    Ok(())
}

/// Read exact ticks without converting through a bounded calendar object.
///
/// # Errors
/// Rejects invalid array metadata, storage, row indices and restricted values.
pub fn value(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if row >= array.len() {
        return Err(err("temporal Arrow row is out of bounds"));
    }
    let dt = array.data_type();
    let td = dtype(dt).ok_or_else(|| err("unsupported Arrow temporal dtype"))?;
    macro_rules! read {
        ($ty:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<$ty>>()
                .ok_or_else(|| err("Arrow array implementation does not match dtype"))?;
            if a.is_null(row) {
                None
            } else {
                Some(i64::from(a.value(row)))
            }
        }};
    }
    let out = match dt {
        DataType::Date32 => read!(Date32Type),
        DataType::Date64 => read!(Date64Type),
        DataType::Time32(TimeUnit::Second) => read!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => read!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => read!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => read!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => read!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => read!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => read!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => read!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => read!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => read!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => read!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => read!(DurationNanosecondType),
        _ => return Err(err("unsupported Arrow temporal dtype")),
    };
    if let Some(v) = out {
        td.validate_value(v)?;
    }
    Ok(out)
}

/// Read a shared typed scalar, retaining the original legacy type aliases.
///
/// # Errors
/// Returns the same array and value errors as `value`.
pub fn scalar(array: &dyn Array, row: usize) -> Result<ScalarValue> {
    let td = dtype(array.data_type()).ok_or_else(|| err("unsupported Arrow temporal dtype"))?;
    match value(array, row)? {
        None => Ok(ScalarValue::Null),
        Some(v) => match td.logical() {
            shardloom_core::LogicalDType::Date32 => Ok(ScalarValue::Date32(
                i32::try_from(v).map_err(|_| err("date32 out of range"))?,
            )),
            shardloom_core::LogicalDType::TimestampMicros => Ok(ScalarValue::TimestampMicros(v)),
            _ => Ok(ScalarValue::Temporal(TemporalValue::new(v, td)?)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::make_builder;

    fn temporal(kind: TemporalKind) -> TemporalDType {
        TemporalDType::new(kind).unwrap()
    }

    fn check(kind: TemporalKind, tick: i64, legacy: bool) {
        let td = temporal(kind);
        let dt = data_type(&td);
        assert_eq!(dtype(&dt), Some(td.clone()));
        let mut builder = make_builder(&dt, 2);
        append(builder.as_mut(), &dt, None).unwrap();
        append(builder.as_mut(), &dt, Some(tick)).unwrap();
        let array = builder.finish();
        assert_eq!(array.data_type(), &dt);
        assert_eq!(dtype(array.data_type()), Some(td.clone()));
        assert_eq!(value(array.as_ref(), 0).unwrap(), None);
        assert_eq!(value(array.as_ref(), 1).unwrap(), Some(tick));
        assert_eq!(scalar(array.as_ref(), 0).unwrap(), ScalarValue::Null);
        let got = scalar(array.as_ref(), 1).unwrap();
        if legacy {
            match td.kind() {
                TemporalKind::Date32 => {
                    assert_eq!(got, ScalarValue::Date32(i32::try_from(tick).unwrap()));
                }
                _ => assert_eq!(got, ScalarValue::TimestampMicros(tick)),
            }
        } else {
            assert_eq!(
                got,
                ScalarValue::Temporal(TemporalValue::new(tick, td.clone()).unwrap())
            );
        }
        let empty = make_builder(&dt, 0).finish();
        assert_eq!(empty.data_type(), &dt);
        assert!(empty.is_empty());
    }

    #[test]
    fn temporal_arrow_roundtrips_supported_domains() {
        check(TemporalKind::Date32, -1, true);
        check(TemporalKind::Date64, -86_400_000, false);
        check(TemporalKind::Time(TemporalUnit::Seconds), 86_399, false);
        check(
            TemporalKind::Time(TemporalUnit::Milliseconds),
            86_399_999,
            false,
        );
        check(
            TemporalKind::Time(TemporalUnit::Microseconds),
            86_399_999_999,
            false,
        );
        check(
            TemporalKind::Time(TemporalUnit::Nanoseconds),
            86_399_999_999_999,
            false,
        );
        for unit in [
            TemporalUnit::Seconds,
            TemporalUnit::Milliseconds,
            TemporalUnit::Microseconds,
            TemporalUnit::Nanoseconds,
        ] {
            check(
                TemporalKind::Timestamp {
                    unit,
                    timezone: None,
                },
                -1,
                unit == TemporalUnit::Microseconds,
            );
            check(
                TemporalKind::Timestamp {
                    unit,
                    timezone: Some("America/New_York".to_owned()),
                },
                -1,
                false,
            );
            check(TemporalKind::Duration(unit), -1, false);
        }
    }

    #[test]
    fn temporal_arrow_rejects_invalid_domains_and_unknown_zones() {
        for (dt, bad) in [
            (DataType::Date64, 1),
            (DataType::Time32(TimeUnit::Second), -1),
            (DataType::Time32(TimeUnit::Second), 86_400),
            (DataType::Time32(TimeUnit::Millisecond), 86_400_000),
            (DataType::Time64(TimeUnit::Microsecond), 86_400_000_000),
            (DataType::Time64(TimeUnit::Nanosecond), 86_400_000_000_000),
        ] {
            let mut builder = make_builder(&dt, 1);
            assert!(
                append(builder.as_mut(), &dt, Some(bad)).is_err(),
                "{dt:?} accepted {bad}"
            );
        }
        let unknown = DataType::Timestamp(TimeUnit::Microsecond, Some("Not/A_Real_Zone".into()));
        assert_eq!(dtype(&unknown), None);
    }
}
