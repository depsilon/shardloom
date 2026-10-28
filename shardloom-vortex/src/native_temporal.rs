//! Typed temporal metadata at native input, execution and output boundaries.

use shardloom_core::{
    Result,
    temporal::{TemporalDType, TemporalKind, TemporalUnit, timezone},
};
use vortex::array::{
    dtype::{DType, PType},
    extension::datetime::{Date, Time, TimeUnit, Timestamp},
};

#[path = "native_duration.rs"]
mod duration;
pub(crate) use duration::Duration;

#[path = "native_temporal_edition.rs"]
mod edition;

#[cfg(feature = "universal-format-io")]
#[path = "native_duration_arrow.rs"]
pub(crate) mod arrow;

/// Recognize provider metadata, never an extension name or its storage alone.
pub(crate) fn temporal_storage(dtype: &DType) -> Option<PType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let storage = if let Some(unit) = extension.metadata_opt::<Date>() {
        match unit {
            TimeUnit::Days => PType::I32,
            TimeUnit::Milliseconds => PType::I64,
            _ => return None,
        }
    } else if let Some(unit) = extension.metadata_opt::<Time>() {
        match unit {
            TimeUnit::Seconds | TimeUnit::Milliseconds => PType::I32,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds => PType::I64,
            TimeUnit::Days => return None,
        }
    } else if let Some(metadata) = extension.metadata_opt::<Timestamp>() {
        fixed_unit(metadata.unit)?;
        if let Some(zone) = &metadata.tz {
            timezone::validate_name(zone).ok()?;
        }
        PType::I64
    } else if extension.metadata_opt::<Duration>().is_some() {
        PType::I64
    } else {
        return None;
    };
    matches!(extension.storage_dtype(), DType::Primitive(ptype, _) if *ptype == storage)
        .then_some(storage)
}

/// Construct owned logical metadata once at binding/schema boundaries.
pub(crate) fn temporal_dtype(dtype: &DType) -> Option<TemporalDType> {
    temporal_storage(dtype)?;
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let kind = if let Some(unit) = extension.metadata_opt::<Date>() {
        if *unit == TimeUnit::Days {
            TemporalKind::Date32
        } else {
            TemporalKind::Date64
        }
    } else if let Some(unit) = extension.metadata_opt::<Time>() {
        TemporalKind::Time(fixed_unit(*unit)?)
    } else if let Some(metadata) = extension.metadata_opt::<Timestamp>() {
        TemporalKind::Timestamp {
            unit: fixed_unit(metadata.unit)?,
            timezone: metadata.tz.as_deref().map(str::to_owned),
        }
    } else {
        TemporalKind::Duration(*extension.metadata_opt::<Duration>()?)
    };
    TemporalDType::new(kind).ok()
}

pub(crate) fn vortex_dtype(
    dtype: &TemporalDType,
    nullable: vortex::array::dtype::Nullability,
) -> Result<DType> {
    let extension = match dtype.kind() {
        TemporalKind::Date32 => Date::new(TimeUnit::Days, nullable).erased(),
        TemporalKind::Date64 => Date::new(TimeUnit::Milliseconds, nullable).erased(),
        TemporalKind::Time(unit) => Time::new(provider_unit(*unit), nullable).erased(),
        TemporalKind::Timestamp {
            unit,
            timezone: zone,
        } => {
            if let Some(zone) = zone {
                timezone::validate_name(zone)?;
            }
            Timestamp::new_with_tz(
                provider_unit(*unit),
                zone.as_deref().map(std::sync::Arc::from),
                nullable,
            )
            .erased()
        }
        TemporalKind::Duration(unit) => Duration::new(*unit, nullable).erased(),
    };
    Ok(DType::Extension(extension))
}

pub(crate) fn register(session: &vortex::session::VortexSession) {
    use vortex::array::dtype::session::DTypeSessionExt as _;
    session.dtypes().register(Duration);
    edition::register(session);
    #[cfg(feature = "universal-format-io")]
    arrow::register(session);
}

pub(crate) fn fixed_unit(unit: TimeUnit) -> Option<TemporalUnit> {
    Some(match unit {
        TimeUnit::Seconds => TemporalUnit::Seconds,
        TimeUnit::Milliseconds => TemporalUnit::Milliseconds,
        TimeUnit::Microseconds => TemporalUnit::Microseconds,
        TimeUnit::Nanoseconds => TemporalUnit::Nanoseconds,
        TimeUnit::Days => return None,
    })
}

pub(crate) const fn provider_unit(unit: TemporalUnit) -> TimeUnit {
    match unit {
        TemporalUnit::Seconds => TimeUnit::Seconds,
        TemporalUnit::Milliseconds => TimeUnit::Milliseconds,
        TemporalUnit::Microseconds => TimeUnit::Microseconds,
        TemporalUnit::Nanoseconds => TimeUnit::Nanoseconds,
    }
}

/// Validate restricted raw domains without constructing calendar scalars or
/// allocating metadata per row. Full timestamp/duration tick domains stay exact.
pub(crate) fn validate_ticks(dtype: &DType, value: i64) -> Result<()> {
    let DType::Extension(extension) = dtype else {
        return Err(failed("temporal ticks require extension metadata"));
    };
    if extension.metadata_opt::<Date>() == Some(&TimeUnit::Milliseconds) && value % 86_400_000 != 0
    {
        return Err(failed("date64 requires whole epoch days in milliseconds"));
    }
    if let Some(unit) = extension.metadata_opt::<Time>() {
        let unit = fixed_unit(*unit)
            .ok_or_else(|| failed("time-of-day requires a fixed temporal unit"))?;
        if !(0..86_400 * unit.ticks_per_second()).contains(&value) {
            return Err(failed(
                "time-of-day ticks must be in [midnight, next midnight)",
            ));
        }
    }
    Ok(())
}

/// Exact retained-row key tags, with no metadata allocation on a row visit.
/// Preserve the historical aliases; frame every new canonical type name.
pub(crate) fn write_key(
    output: &mut impl std::fmt::Write,
    dtype: &DType,
    value: i64,
) -> std::fmt::Result {
    let DType::Extension(extension) = dtype else {
        return Err(std::fmt::Error);
    };
    if let Some(unit) = extension.metadata_opt::<Date>() {
        return match unit {
            TimeUnit::Days => write!(output, "d32:{value}"),
            TimeUnit::Milliseconds => write!(output, "temporal:6:date64:{value}"),
            _ => Err(std::fmt::Error),
        };
    }
    if let Some(unit) = extension.metadata_opt::<Time>() {
        let unit = fixed_unit(*unit).ok_or(std::fmt::Error)?.as_str();
        return write!(output, "temporal:{}:time({unit}):{value}", 6 + unit.len());
    }
    if let Some(metadata) = extension.metadata_opt::<Timestamp>() {
        let unit = fixed_unit(metadata.unit).ok_or(std::fmt::Error)?.as_str();
        if let Some(zone) = &metadata.tz {
            return write!(
                output,
                "temporal:{}:timestamp({unit},{zone}):{value}",
                12 + unit.len() + zone.len()
            );
        }
        if metadata.unit == TimeUnit::Microseconds {
            return write!(output, "tsus:{value}");
        }
        return write!(
            output,
            "temporal:{}:timestamp({unit}):{value}",
            11 + unit.len()
        );
    }
    if let Some(unit) = extension.metadata_opt::<Duration>() {
        let unit = unit.as_str();
        return write!(
            output,
            "temporal:{}:duration({unit}):{value}",
            10 + unit.len()
        );
    }
    Err(std::fmt::Error)
}

fn failed(message: &str) -> shardloom_core::ShardLoomError {
    shardloom_core::ShardLoomError::InvalidOperation(message.to_owned())
}

#[cfg(feature = "universal-format-io")]
pub(crate) fn arrow_dtype(dtype: &TemporalDType) -> arrow_schema::DataType {
    crate::temporal_arrow::data_type(dtype)
}

#[cfg(feature = "universal-format-io")]
pub(crate) fn from_arrow_dtype(dtype: &arrow_schema::DataType) -> Option<TemporalDType> {
    crate::temporal_arrow::dtype(dtype)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cmp::Ordering, sync::Arc};
    use vortex::array::{
        dtype::{
            Nullability,
            extension::{ExtDType, ExtVTable},
        },
        extension::datetime::TimestampOptions,
        scalar::{PValue, Scalar, ScalarValue},
    };

    fn scalar(dtype: &DType, value: i64) -> Scalar {
        Scalar::try_new(
            dtype.clone(),
            Some(ScalarValue::Primitive(PValue::I64(value))),
        )
        .expect("every admitted timestamp storage value must construct a scalar")
    }

    #[test]
    fn full_domain_timestamp_scalars_preserve_storage_order_and_metadata() {
        let extension = Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable);
        let metadata = extension.metadata().clone();
        let serialized = Timestamp.serialize_metadata(&metadata).unwrap();
        assert_eq!(
            Timestamp.deserialize_metadata(&serialized).unwrap(),
            metadata
        );
        let dtype = DType::Extension(extension.erased());
        assert_eq!(temporal_storage(&dtype), Some(PType::I64));
        // Include both storage endpoints, the Jiff span boundary and values
        // outside its calendar but inside its span, as well as ordinary dates.
        let values = [
            i64::MIN,
            i64::MIN + 1,
            -631_107_417_600_000_001,
            -631_107_417_600_000_000,
            -400_000_000_000_000_000,
            -1,
            0,
            1,
            1_700_000_000_000_000,
            300_000_000_000_000_000,
            631_107_417_600_000_000,
            631_107_417_600_000_001,
            i64::MAX - 1,
            i64::MAX,
        ];
        let scalars = values.map(|value| {
            let item = scalar(&dtype, value);
            assert_eq!(item.dtype(), &dtype);
            assert_eq!(
                item.as_extension()
                    .to_storage_scalar()
                    .as_primitive()
                    .as_::<i64>(),
                Some(value)
            );
            item
        });
        for pair in scalars.windows(2) {
            assert_eq!(pair[0].partial_cmp(&pair[1]), Some(Ordering::Less));
        }
        let null = Scalar::try_new(dtype, None).unwrap();
        assert!(null.is_null());
        assert!(null.as_extension().to_storage_scalar().is_null());
    }

    #[test]
    fn full_domain_timestamp_display_is_exact_and_does_not_panic() {
        let dtype = DType::Extension(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
        );
        for value in [
            i64::MIN,
            -400_000_000_000_000_000,
            300_000_000_000_000_000,
            i64::MAX,
        ] {
            assert_eq!(
                scalar(&dtype, value).as_extension().to_string(),
                format!("{value}us since Unix epoch")
            );
        }
        for (value, expected) in [
            (-1, "1969-12-31T23:59:59.999999Z"),
            (0, "1970-01-01T00:00:00Z"),
            (1, "1970-01-01T00:00:00.000001Z"),
        ] {
            assert_eq!(scalar(&dtype, value).as_extension().to_string(), expected);
        }
        assert_eq!(
            Scalar::try_new(dtype, None)
                .unwrap()
                .as_extension()
                .to_string(),
            "null"
        );
    }

    #[test]
    fn temporal_admission_rejects_invalid_metadata_and_storage() {
        let options = TimestampOptions {
            unit: TimeUnit::Microseconds,
            tz: None,
        };
        assert!(
            ExtDType::<Timestamp>::try_new(
                options,
                DType::Primitive(PType::U64, Nullability::Nullable)
            )
            .is_err()
        );
        for (unit, timezone, value) in [
            (TimeUnit::Days, None, 0),
            (TimeUnit::Microseconds, Some(Arc::from("Not/A/Timezone")), 0),
        ] {
            let dtype = DType::Extension(
                Timestamp::new_with_tz(unit, timezone, Nullability::Nullable).erased(),
            );
            assert_eq!(temporal_storage(&dtype), None);
            assert!(
                Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(value)))).is_err()
            );
        }
        let required = DType::Extension(
            Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
        );
        assert!(Scalar::try_new(required, None).is_err());
    }

    #[test]
    fn temporal_units_preserve_full_storage_domains_and_logical_identity() {
        for unit in [
            TemporalUnit::Seconds,
            TemporalUnit::Milliseconds,
            TemporalUnit::Microseconds,
            TemporalUnit::Nanoseconds,
        ] {
            for kind in [
                TemporalKind::Timestamp {
                    unit,
                    timezone: None,
                },
                TemporalKind::Timestamp {
                    unit,
                    timezone: Some("America/New_York".into()),
                },
                TemporalKind::Timestamp {
                    unit,
                    timezone: Some("+05:30".into()),
                },
                TemporalKind::Duration(unit),
            ] {
                let logical = TemporalDType::new(kind).unwrap();
                let dtype = vortex_dtype(&logical, Nullability::Nullable).unwrap();
                assert_eq!(temporal_dtype(&dtype), Some(logical));
                let values = [i64::MIN, -1, 0, 1, i64::MAX].map(|v| scalar(&dtype, v));
                for (value, expected) in values.iter().zip([i64::MIN, -1, 0, 1, i64::MAX]) {
                    assert_eq!(value.dtype(), &dtype);
                    assert_eq!(
                        value
                            .as_extension()
                            .to_storage_scalar()
                            .as_primitive()
                            .as_::<i64>(),
                        Some(expected)
                    );
                    assert_ne!(value.as_extension().to_string(), "");
                }
                for pair in values.windows(2) {
                    assert_eq!(pair[0].partial_cmp(&pair[1]), Some(Ordering::Less));
                }
                assert!(Scalar::try_new(dtype, None).unwrap().is_null());
            }
        }
        assert!(Duration.deserialize_metadata(&[4]).is_err());
        assert!(Duration.deserialize_metadata(&[0, 0]).is_err());
        assert!(
            ExtDType::<Duration>::try_new(
                TemporalUnit::Seconds,
                DType::Primitive(PType::U64, Nullability::Nullable)
            )
            .is_err()
        );
    }

    #[test]
    fn full_domain_timestamp_compression_preserves_storage_statistics_and_predicates() {
        for unit in [
            TimeUnit::Seconds,
            TimeUnit::Milliseconds,
            TimeUnit::Microseconds,
            TimeUnit::Nanoseconds,
        ] {
            for timezone in [None, Some(Arc::from("America/New_York"))] {
                assert_full_domain_timestamp_compression(unit, timezone);
            }
        }
    }

    #[allow(clippy::too_many_lines)] // Keep the shared encoding/predicate fixture and its checks together.
    fn assert_full_domain_timestamp_compression(unit: TimeUnit, timezone: Option<Arc<str>>) {
        use vortex::{
            VortexSessionDefault as _,
            array::{
                IntoArray as _, VortexSessionExecute as _,
                arrays::{BoolArray, ConstantArray, ExtensionArray, PrimitiveArray, TemporalArray},
                builtins::ArrayBuiltins as _,
                expr::stats::Stat,
                scalar_fn::fns::operators::Operator,
            },
            compressor::BtrBlocksCompressorBuilder,
            encodings::datetime_parts::DateTimeParts,
            session::VortexSession,
        };

        let session = VortexSession::default();
        let compressor = BtrBlocksCompressorBuilder::from_session(&session).build();
        let dtype = Timestamp::new_with_tz(unit, timezone, Nullability::Nullable).erased();
        let mut execution = session.create_execution_ctx();
        let mut boundary = vec![
            i64::MIN,
            i64::MIN + 1,
            -631_107_417_600_000_001,
            -400_000_000_000_000_000,
            -86_400_000_001,
            -86_400_000_000,
            -1,
            0,
            1,
            86_400_000_000,
            300_000_000_000_000_000,
            631_107_417_600_000_001,
            i64::MAX - 1,
            i64::MAX,
        ];
        let scale = match unit {
            TimeUnit::Seconds => 1,
            TimeUnit::Milliseconds => 1_000,
            TimeUnit::Microseconds => 1_000_000,
            TimeUnit::Nanoseconds => 1_000_000_000,
            TimeUnit::Days => unreachable!(),
        };
        if let Some(day_limit) = (i64::from(i32::MAX) + 1).checked_mul(86_400 * scale) {
            boundary.extend([day_limit - 1, day_limit, -day_limit, -day_limit - 1]);
        }
        let profiles = [
            boundary.iter().copied().map(Some).chain([None]).collect(),
            vec![Some(i64::MAX); 64],
            (0..257)
                .map(|offset| Some(i64::MIN + offset))
                .chain([None])
                .collect(),
            (0..257)
                .map(|offset| Some(i64::MAX - offset))
                .chain([None])
                .collect(),
            vec![None; 64],
            Vec::new(),
        ];
        for expected in profiles {
            let source = ExtensionArray::new(
                dtype.clone(),
                PrimitiveArray::from_option_iter(expected.iter().copied()).into_array(),
            )
            .into_array();
            // Exercise decomposition directly even if the general compressor
            // selects a constant or another cheaper native representation.
            let decomposed = DateTimeParts::try_from_temporal(
                TemporalArray::try_from(source.clone()).unwrap(),
                &mut execution,
            )
            .unwrap()
            .into_array();
            let compressed = compressor.compress(&source, &mut execution).unwrap();
            for encoded in [decomposed, compressed] {
                assert_eq!(encoded.dtype(), source.dtype());
                assert_eq!(encoded.len(), expected.len());
                let canonical = encoded
                    .clone()
                    .execute::<ExtensionArray>(&mut execution)
                    .unwrap();
                for (row, value) in expected.iter().enumerate() {
                    assert_eq!(
                        encoded
                            .execute_scalar(row, &mut execution)
                            .unwrap()
                            .as_extension()
                            .to_storage_scalar()
                            .as_primitive()
                            .as_::<i64>(),
                        *value
                    );
                    assert_eq!(
                        canonical
                            .execute_scalar(row, &mut execution)
                            .unwrap()
                            .as_extension()
                            .to_storage_scalar()
                            .as_primitive()
                            .as_::<i64>(),
                        *value
                    );
                }
                for (stat, expected_bound) in [
                    (Stat::Min, expected.iter().copied().flatten().min()),
                    (Stat::Max, expected.iter().copied().flatten().max()),
                ] {
                    let actual = encoded
                        .statistics()
                        .compute_stat(stat, &mut execution)
                        .unwrap()
                        .and_then(|value| {
                            value
                                .as_extension()
                                .to_storage_scalar()
                                .as_primitive()
                                .as_::<i64>()
                        });
                    assert_eq!(actual, expected_bound);
                }
                for threshold in boundary.iter().copied().map(Some).chain([None]) {
                    for operator in [
                        Operator::Eq,
                        Operator::NotEq,
                        Operator::Lt,
                        Operator::Lte,
                        Operator::Gt,
                        Operator::Gte,
                    ] {
                        let rhs = ConstantArray::new(
                            threshold.map_or_else(
                                || Scalar::null(source.dtype().clone()),
                                |value| scalar(source.dtype(), value),
                            ),
                            encoded.len(),
                        )
                        .into_array();
                        for reversed in [false, true] {
                            let comparison = if reversed {
                                rhs.binary(encoded.clone(), operator)
                            } else {
                                encoded.binary(rhs.clone(), operator)
                            };
                            // Execute the whole encoded comparison. Scalar-at-a-time
                            // evaluation can bypass the encoding-specific kernel.
                            let result = comparison
                                .unwrap()
                                .execute::<BoolArray>(&mut execution)
                                .unwrap()
                                .into_array();
                            for (row, value) in expected.iter().enumerate() {
                                let expected = value.zip(threshold).map(|(value, threshold)| {
                                    let (left, right) = if reversed {
                                        (threshold, value)
                                    } else {
                                        (value, threshold)
                                    };
                                    match operator {
                                        Operator::Eq => left == right,
                                        Operator::NotEq => left != right,
                                        Operator::Lt => left < right,
                                        Operator::Lte => left <= right,
                                        Operator::Gt => left > right,
                                        Operator::Gte => left >= right,
                                        _ => unreachable!(),
                                    }
                                });
                                assert_eq!(
                                    result
                                        .execute_scalar(row, &mut execution)
                                        .unwrap()
                                        .as_bool()
                                        .value(),
                                    expected,
                                    "{operator:?} {threshold:?} at row {row}; reversed={reversed}; encoding={}",
                                    encoded.encoding_id()
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
