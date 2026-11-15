use super::*;
use std::sync::Arc;
use vortex::array::{
    dtype::{FieldNames, Nullability, StructFields},
    scalar::ScalarValue,
};

fn schema(ptype: PType, nullable: Nullability) -> DType {
    DType::Struct(
        StructFields::new(
            FieldNames::from(["value"]),
            vec![DType::Primitive(ptype, nullable)],
        ),
        Nullability::NonNullable,
    )
}

fn facts(dtype: &DType, entries: &[(Stat, Precision<ScalarValue>)]) -> FileStatistics {
    let mut stats = StatsSet::default();
    for (stat, value) in entries {
        stats.set(*stat, value.clone());
    }
    FileStatistics::from_stats(
        Arc::from([stats]),
        Arc::from([dtype.as_struct_fields().field("value").unwrap()]),
    )
}

fn predicate(value: StatValue) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("value").unwrap(),
        op: ComparisonOp::Eq,
        value,
    }
}

#[test]
fn source_facts_require_exact_nan_count_and_finite_ordered_extrema() {
    let dtype = schema(PType::F64, Nullability::Nullable);
    let valid = [
        (Stat::Min, Precision::Exact((-2.0_f64).into())),
        (Stat::Max, Precision::Exact(3.0_f64.into())),
        (Stat::NaNCount, Precision::Exact(0_u64.into())),
        (Stat::NullCount, Precision::Exact(1_u64.into())),
    ];
    let input = predicate(StatValue::Float64(1.0));
    let admits = |entries: &[(Stat, Precision<ScalarValue>)]| {
        normalize(input.clone(), &dtype, 4, Some(&facts(&dtype, entries))).is_some()
    };
    assert!(admits(&valid));
    assert!(normalize(input.clone(), &dtype, 4, None).is_none());
    for index in 0..3 {
        let mut missing = valid.to_vec();
        missing.remove(index);
        assert!(!admits(&missing));
        let mut inexact = valid.to_vec();
        let Precision::Exact(value) = inexact[index].1.clone() else {
            unreachable!()
        };
        inexact[index].1 = Precision::Inexact(value);
        assert!(!admits(&inexact));
    }
    for (index, value) in [
        (0, ScalarValue::from(f64::NEG_INFINITY)),
        (1, ScalarValue::from(f64::INFINITY)),
        (0, ScalarValue::from(f64::NAN)),
        (0, ScalarValue::from(4.0_f64)),
        (2, ScalarValue::from(1_u64)),
        (3, ScalarValue::from(5_u64)),
    ] {
        let mut invalid = valid.to_vec();
        invalid[index].1 = Precision::Exact(value);
        assert!(!admits(&invalid), "accepted invalid statistic {index}");
    }
}

#[test]
fn source_facts_empty_and_all_null_are_vacuous_but_inconsistent_facts_decline() {
    let dtype = schema(PType::F32, Nullability::Nullable);
    let column = ColumnRef::new("value").unwrap();
    assert_eq!(finite_column(&dtype, 0, None, &column), Some(PType::F32));
    let nulls = [(Stat::NullCount, Precision::Exact(4_u64.into()))];
    let stats = facts(&dtype, &nulls);
    assert_eq!(
        finite_column(&dtype, 4, Some(&stats), &column),
        Some(PType::F32)
    );
    assert_eq!(finite_column(&dtype, 3, Some(&stats), &column), None);
    for extra in [
        (Stat::Min, Precision::Exact(1.0_f32.into())),
        (Stat::NaNCount, Precision::Exact(1_u64.into())),
    ] {
        let stats = facts(&dtype, &[nulls[0].clone(), extra]);
        assert_eq!(finite_column(&dtype, 4, Some(&stats), &column), None);
    }
    let nullable_root = dtype.with_nullability(Nullability::Nullable);
    assert_eq!(finite_column(&nullable_root, 0, None, &column), None);
    let wrong_dtype = schema(PType::F64, Nullability::Nullable);
    let stats = facts(&wrong_dtype, &nulls);
    assert_eq!(finite_column(&dtype, 4, Some(&stats), &column), None);
}

#[test]
fn source_facts_literal_conversion_never_rounds_or_saturates() {
    for value in [i64::MAX, (1_i64 << 53) + 1] {
        assert!(exact_literal(&StatValue::Int64(value), PType::F64).is_none());
    }
    for value in [u64::MAX, (1_u64 << 53) + 1] {
        assert!(exact_literal(&StatValue::UInt64(value), PType::F64).is_none());
    }
    assert_eq!(
        exact_literal(&StatValue::Int64(i64::MIN), PType::F64),
        Some(-9_223_372_036_854_775_808.0_f64)
    );
    assert!(exact_literal(&StatValue::Int64((1 << 24) + 1), PType::F32).is_none());
    assert!(exact_literal(&StatValue::Float64(0.1), PType::F32).is_none());
    assert_eq!(
        exact_literal(&StatValue::Float64(f64::from(0.1_f32)), PType::F32),
        Some(f64::from(0.1_f32))
    );
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(exact_literal(&StatValue::Float64(value), PType::F64).is_none());
    }
    assert!(exact_literal(&StatValue::Null, PType::F64).is_none());
    assert!(exact_literal(&StatValue::Float64(1.0), PType::F16).is_none());
}

fn provider_evaluate(predicate: &PredicateExpr, row: f64) -> bool {
    match predicate {
        PredicateExpr::And(children) => children.iter().all(|child| provider_evaluate(child, row)),
        PredicateExpr::Compare {
            op,
            value: StatValue::Float64(value),
            ..
        } => {
            let order = row.total_cmp(value);
            match op {
                ComparisonOp::Eq => order.is_eq(),
                ComparisonOp::NotEq => !order.is_eq(),
                ComparisonOp::Lt => order.is_lt(),
                ComparisonOp::LtEq => order.is_le(),
                ComparisonOp::Gt => order.is_gt(),
                ComparisonOp::GtEq => order.is_ge(),
            }
        }
        _ => panic!("unexpected normalized predicate"),
    }
}

#[test]
#[allow(clippy::float_cmp)] // Exact IEEE equality is the signed-zero contract under test.
fn source_facts_signed_zero_rewrites_match_typed_float_comparisons() {
    for op in [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ] {
        for literal in [-0.0, 0.0] {
            let predicate = zero_comparison(&ColumnRef::new("value").unwrap(), op, literal);
            for row in [-1.0, -f64::MIN_POSITIVE, -0.0, 0.0, f64::MIN_POSITIVE, 1.0] {
                let expected = match op {
                    ComparisonOp::Eq => row == literal,
                    ComparisonOp::NotEq => row != literal,
                    ComparisonOp::Lt => row < literal,
                    ComparisonOp::LtEq => row <= literal,
                    ComparisonOp::Gt => row > literal,
                    ComparisonOp::GtEq => row >= literal,
                };
                assert_eq!(
                    provider_evaluate(&predicate, row),
                    expected,
                    "{op:?}: {row:?}"
                );
            }
        }
    }
}

#[test]
fn source_facts_whole_conjunction_and_in_list_keep_decline_contracts() {
    let dtype = schema(PType::F64, Nullability::NonNullable);
    let statistics = facts(
        &dtype,
        &[
            (Stat::Min, Precision::Exact((-0.0_f64).into())),
            (Stat::Max, Precision::Exact(1.0_f64.into())),
            (Stat::NaNCount, Precision::Exact(0_u64.into())),
        ],
    );
    let list = |values, negated| PredicateExpr::InList {
        column: ColumnRef::new("value").unwrap(),
        values,
        negated,
    };
    let normalized = normalize(
        list(
            vec![StatValue::Float64(0.0), StatValue::Float64(1.0)],
            false,
        ),
        &dtype,
        3,
        Some(&statistics),
    )
    .unwrap();
    let PredicateExpr::InList { values, .. } = normalized else {
        panic!()
    };
    let bits: Vec<_> = values
        .iter()
        .map(|value| match value {
            StatValue::Float64(value) => value.to_bits(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        bits,
        [(-0.0_f64).to_bits(), 0.0_f64.to_bits(), 1.0_f64.to_bits()]
    );
    for invalid in [
        list(vec![StatValue::Null], false),
        list(vec![StatValue::Float64(0.0)], true),
        PredicateExpr::And(vec![
            predicate(StatValue::Float64(1.0)),
            predicate(StatValue::Int64(i64::MAX)),
        ]),
    ] {
        assert!(normalize(invalid, &dtype, 3, Some(&statistics)).is_none());
    }
}
