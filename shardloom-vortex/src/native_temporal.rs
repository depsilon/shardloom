//! Shared recognition of the admitted native temporal storage domains.

use vortex::array::{
    dtype::{DType, PType},
    extension::datetime::{Date, TimeUnit, Timestamp},
};

/// Recognize provider metadata, never an extension name or its storage alone.
pub(crate) fn temporal_storage(dtype: &DType) -> Option<PType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let storage = if extension.metadata_opt::<Date>() == Some(&TimeUnit::Days) {
        PType::I32
    } else if extension
        .metadata_opt::<Timestamp>()
        .is_some_and(|metadata| metadata.unit == TimeUnit::Microseconds && metadata.tz.is_none())
    {
        PType::I64
    } else {
        return None;
    };
    matches!(extension.storage_dtype(), DType::Primitive(ptype, _) if *ptype == storage)
        .then_some(storage)
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
    fn full_domain_timestamp_admission_does_not_expand_other_temporal_contracts() {
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
            (TimeUnit::Seconds, None, 300_000_000_000),
            (TimeUnit::Milliseconds, None, 300_000_000_000_000),
            (
                TimeUnit::Microseconds,
                Some(Arc::from("UTC")),
                300_000_000_000_000_000,
            ),
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
    #[allow(clippy::too_many_lines)] // Keep the shared encoding/predicate fixture and its checks together.
    fn full_domain_timestamp_compression_preserves_storage_statistics_and_predicates() {
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
            editions::{ComponentKind, EditionSessionExt as _},
            encodings::datetime_parts::DateTimeParts,
            session::VortexSession,
        };

        let session = VortexSession::default();
        let allowed = session
            .enabled_component_ids(ComponentKind::Array)
            .into_iter()
            .collect();
        let compressor = BtrBlocksCompressorBuilder::default()
            .retain_allowed_encodings(&allowed)
            .build();
        let dtype = Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased();
        let mut execution = session.create_execution_ctx();
        let boundary = [
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
        let profiles = [
            boundary.into_iter().map(Some).chain([None]).collect(),
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
                for threshold in boundary.into_iter().map(Some).chain([None]) {
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
