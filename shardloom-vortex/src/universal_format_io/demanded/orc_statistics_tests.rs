use super::*;
use orc_rust::proto;
use shardloom_core::{ColumnRef, ComparisonOp};

fn predicate(value: StatValue) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("value").unwrap(),
        op: ComparisonOp::Eq,
        value,
    }
}

#[test]
fn orc_statistics_missing_and_inconsistent_null_facts_remain_unknown() {
    let unknown = ColumnStatistics::try_from(&proto::ColumnStatistics::default()).unwrap();
    let stats = primitive_stats(&unknown, 4, &DataType::Int64).unwrap();
    assert_eq!(stats.null_count, None);
    for predicate in [
        predicate(StatValue::Int64(1)),
        PredicateExpr::IsNull {
            column: ColumnRef::new("value").unwrap(),
        },
        PredicateExpr::IsNotNull {
            column: ColumnRef::new("value").unwrap(),
        },
    ] {
        assert!(!statistics::excluded_by_stats(&predicate, &stats));
    }
    for (count, has_null) in [(5, None), (3, Some(false)), (4, Some(true))] {
        let value = ColumnStatistics::try_from(&proto::ColumnStatistics {
            number_of_values: Some(count),
            has_null,
            ..Default::default()
        })
        .unwrap();
        assert!(primitive_stats(&value, 4, &DataType::Int64).is_none());
    }
    let all_null = ColumnStatistics::try_from(&proto::ColumnStatistics {
        number_of_values: Some(0),
        has_null: Some(true),
        ..Default::default()
    })
    .unwrap();
    let stats = primitive_stats(&all_null, 4, &DataType::Int64).unwrap();
    assert!(statistics::excluded_by_stats(
        &predicate(StatValue::Int64(1)),
        &stats
    ));
    assert!(statistics::excluded_by_stats(
        &PredicateExpr::IsNotNull {
            column: ColumnRef::new("value").unwrap()
        },
        &stats
    ));
    assert!(!statistics::excluded_by_stats(
        &PredicateExpr::IsNull {
            column: ColumnRef::new("value").unwrap()
        },
        &stats
    ));
}

#[test]
fn orc_statistics_integer_extrema_require_presence_order_and_matching_width() {
    for (minimum, maximum, expected) in [
        (None, Some(3), false),
        (Some(1), None, false),
        (Some(3), Some(1), false),
        (Some(1), Some(3), true),
        (Some(1), Some(999), false),
    ] {
        let value = ColumnStatistics::try_from(&proto::ColumnStatistics {
            number_of_values: Some(4),
            has_null: Some(false),
            int_statistics: Some(proto::IntegerStatistics {
                minimum,
                maximum,
                sum: None,
            }),
            ..Default::default()
        })
        .unwrap();
        let stats = primitive_stats(&value, 4, &DataType::Int8).unwrap();
        assert_eq!(
            statistics::excluded_by_stats(&predicate(StatValue::Int64(9)), &stats),
            expected
        );
    }
}

#[test]
fn orc_statistics_boolean_and_strings_use_complete_exact_evidence() {
    for (count, true_count, expected) in [
        (4, vec![0], true),
        (4, vec![2], false),
        (4, vec![], false),
        (4, vec![5], false),
    ] {
        let value = ColumnStatistics::try_from(&proto::ColumnStatistics {
            number_of_values: Some(count),
            has_null: Some(false),
            bucket_statistics: Some(proto::BucketStatistics { count: true_count }),
            ..Default::default()
        })
        .unwrap();
        let stats = primitive_stats(&value, 4, &DataType::Boolean).unwrap();
        assert_eq!(
            statistics::excluded_by_stats(&predicate(StatValue::Boolean(true)), &stats),
            expected
        );
    }
    for exact in [false, true] {
        let string_statistics = if exact {
            proto::StringStatistics {
                minimum: Some("a".into()),
                maximum: Some("c".into()),
                ..Default::default()
            }
        } else {
            proto::StringStatistics {
                lower_bound: Some("a".into()),
                upper_bound: Some("c".into()),
                ..Default::default()
            }
        };
        let value = ColumnStatistics::try_from(&proto::ColumnStatistics {
            number_of_values: Some(4),
            string_statistics: Some(string_statistics),
            ..Default::default()
        })
        .unwrap();
        let stats = primitive_stats(&value, 4, &DataType::Utf8).unwrap();
        assert_eq!(
            statistics::excluded_by_stats(&predicate(StatValue::Utf8("z".into())), &stats),
            exact
        );
    }
}

#[test]
fn orc_statistics_do_not_admit_temporal_decimal_or_float_bounds() {
    let value = ColumnStatistics::try_from(&proto::ColumnStatistics {
        number_of_values: Some(4),
        has_null: Some(false),
        ..Default::default()
    })
    .unwrap();
    for dtype in [
        DataType::Date32,
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        DataType::Float64,
        DataType::Decimal128(10, 2),
    ] {
        assert!(primitive_stats(&value, 4, &dtype).is_none());
    }
}
