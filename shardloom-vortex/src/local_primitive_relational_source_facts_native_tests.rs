use super::*;
use vortex::{array::expr::stats::Stat, encodings::alp::alp_encode};

const VALUES: [Option<f64>; 8] = [
    Some(-1.5),
    Some(-0.0),
    Some(0.0),
    Some(0.5),
    None,
    Some(1.5),
    Some(-0.25),
    Some(0.25),
];

#[allow(clippy::cast_possible_truncation)] // The fixture explicitly constructs the F32 or F64 domain.
fn float_fixture(ptype: PType, statistics: bool, nonfinite: Option<f64>) -> Fixture {
    let mut values = VALUES;
    if let Some(value) = nonfinite {
        values[5] = Some(value);
    }
    let primitive = match ptype {
        PType::F32 => PrimitiveArray::from_option_iter(
            values
                .into_iter()
                .map(|value| value.map(|value| value as f32)),
        ),
        PType::F64 => PrimitiveArray::from_option_iter(values),
        _ => unreachable!(),
    };
    let session = VortexSession::default();
    let encoded = alp_encode(
        primitive.as_view(),
        None,
        &mut session.create_execution_ctx(),
    )
    .unwrap();
    let array = StructArray::new(
        ["row_id", "value"].into(),
        vec![
            PrimitiveArray::from_iter(0_i64..8).into_array(),
            encoded.into_array(),
        ],
        8,
        Validity::NonNullable,
    )
    .into_array();
    Fixture::with_strategy_statistics(
        array,
        8,
        vec![],
        super::super::super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(1),
        if statistics {
            vec![Stat::Min, Stat::Max, Stat::NullCount, Stat::NaNCount]
        } else {
            vec![]
        },
    )
}

fn ids(result: &CollectedVortexRelational) -> Vec<i64> {
    json_rows(result)
        .iter()
        .map(|row| row["row_id"].as_i64().unwrap())
        .collect()
}

fn observed(result: &CollectedVortexRelational, name: &str) -> u64 {
    result
        .execution
        .native_io_certificate
        .source_pushdown_report
        .proof_basis
        .split_whitespace()
        .find_map(|entry| entry.strip_prefix(&format!("native_encoded_compare_{name}=")))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn source_facts_native_alp_float_filters_preserve_signed_zero_nulls_and_aliases() {
    for ptype in [PType::F32, PType::F64] {
        let fixture = float_fixture(ptype, true, None);
        for alias in [false, true] {
            let name = if alias { "renamed" } else { "value" };
            for op in [
                ComparisonOp::Eq,
                ComparisonOp::NotEq,
                ComparisonOp::Lt,
                ComparisonOp::LtEq,
                ComparisonOp::Gt,
                ComparisonOp::GtEq,
            ] {
                let expected: Vec<_> = VALUES
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| {
                        let value = (*value)?;
                        let keep = match op {
                            ComparisonOp::Eq => value == 0.0,
                            ComparisonOp::NotEq => value != 0.0,
                            ComparisonOp::Lt => value < 0.0,
                            ComparisonOp::LtEq => value <= 0.0,
                            ComparisonOp::Gt => value > 0.0,
                            ComparisonOp::GtEq => value >= 0.0,
                        };
                        keep.then_some(i64::try_from(index).unwrap())
                    })
                    .collect();
                for file_kernels in [false, true] {
                    let input = if alias {
                        project(
                            fixture.scan(),
                            vec![("row_id", column("row_id")), (name, column("value"))],
                        )
                    } else {
                        fixture.scan()
                    };
                    let plan = project(
                        filter(input, name, op, ScalarValue::Float64(-0.0)),
                        vec![("row_id", column("row_id"))],
                    );
                    let mut prepared = prepare_relational(&plan, policy()).unwrap();
                    if !file_kernels {
                        prepared.remove_file_kernels_for_reference().unwrap();
                    }
                    let result = prepared
                        .collect_jsonl(&CancellationToken::default())
                        .unwrap();
                    assert_eq!(
                        ids(&result),
                        expected,
                        "{ptype:?} {op:?} alias={alias} kernels={file_kernels}"
                    );
                    assert!(result.execution.native_io_certificate.is_certified());
                    assert!(
                        result
                            .execution
                            .native_io_certificate
                            .source_pushdown_report
                            .accepted_operations
                            .iter()
                            .any(|operation| operation == "source_footer_finite_float_predicate")
                    );
                    if matches!(op, ComparisonOp::Gt | ComparisonOp::Lt) {
                        assert!(
                            observed(&result, "alp_calls") > 0,
                            "ALP comparison did not dispatch"
                        );
                        assert!(observed(&result, "alp_rows") > 0);
                    }
                }
            }
        }
    }
}

#[test]
fn source_facts_native_missing_stats_and_inexact_literals_keep_typed_evaluation() {
    for (ptype, statistics, literal) in [(PType::F64, false, 0.0), (PType::F32, true, 0.1)] {
        let fixture = float_fixture(ptype, statistics, None);
        let plan = project(
            filter(
                fixture.scan(),
                "value",
                ComparisonOp::Gt,
                ScalarValue::Float64(literal),
            ),
            vec![("row_id", column("row_id"))],
        );
        let result = prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected: Vec<_> = VALUES
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                value
                    .filter(|value| *value > literal)
                    .map(|_| i64::try_from(index).unwrap())
            })
            .collect();
        assert_eq!(ids(&result), expected);
        assert_eq!(observed(&result, "alp_calls"), 0);
        assert!(
            !result
                .execution
                .native_io_certificate
                .source_pushdown_report
                .accepted_operations
                .iter()
                .any(|operation| operation == "source_footer_finite_float_predicate")
        );
    }
}

#[test]
fn source_facts_native_nonfinite_and_replaced_sources_keep_failures() {
    for ptype in [PType::F32, PType::F64] {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let fixture = float_fixture(ptype, true, Some(value));
            let plan = project(
                filter(
                    fixture.scan(),
                    "value",
                    ComparisonOp::Gt,
                    ScalarValue::Float64(0.0),
                ),
                vec![("row_id", column("row_id"))],
            );
            let result = prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default());
            assert!(
                result.is_err(),
                "strict nonfinite source was admitted: {ptype:?} {value}"
            );
        }
        let fixture = float_fixture(ptype, true, None);
        let plan = filter(
            fixture.scan(),
            "value",
            ComparisonOp::Gt,
            ScalarValue::Float64(0.0),
        );
        let prepared = prepare_relational(&plan, policy()).unwrap();
        fixture.replace();
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
    }
}

#[test]
fn source_facts_native_observations_are_per_call_and_release_all_credits() {
    let fixture = float_fixture(PType::F64, true, None);
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let plan = project(
        filter(
            fixture.scan(),
            "value",
            ComparisonOp::Gt,
            ScalarValue::Float64(0.0),
        ),
        vec![("row_id", column("row_id"))],
    );
    let prepared = prepare_relational_in_session(&plan, policy(), &session).unwrap();
    let first = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let calls = observed(&first, "alp_calls");
    drop(first);
    let second = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(observed(&second, "alp_calls"), calls);
    assert!(calls > 0);
    drop(second);
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let session = ResidentVortexSession::with_serving_policy(
        32 << 20,
        1,
        crate::resident_session::ResidentServingPolicy::default(),
    )
    .unwrap();
    let prepared = prepare_relational_in_session(&plan, policy(), &session).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(ids(&result), vec![3, 5, 7]);
    let proof = &result
        .execution
        .native_io_certificate
        .source_pushdown_report
        .proof_basis;
    assert!(
        proof
            .split(|character: char| character.is_whitespace() || character == ';')
            .any(|entry| entry == "native_encoded_compare_scope=unavailable")
    );
    assert!(!proof.contains("native_encoded_compare_alp_calls="));
}

#[test]
fn source_facts_native_fastlanes_integer_dispatch_preserves_existing_semantics() {
    use vortex::encodings::fastlanes::{BitPacked, FoRData};
    let values = [
        Some(501_u64),
        Some(504),
        None,
        Some(500),
        Some(507),
        Some(503),
        Some(501),
        Some(505),
    ];
    let session = VortexSession::default();
    for family in ["bitpacked", "for"] {
        let primitive = PrimitiveArray::from_option_iter(values);
        let mut ctx = session.create_execution_ctx();
        let encoded = if family == "bitpacked" {
            BitPacked::encode(&primitive.into_array(), 10, &mut ctx)
                .unwrap()
                .into_array()
        } else {
            FoRData::encode(primitive, &mut ctx).unwrap().into_array()
        };
        let array = StructArray::new(
            ["row_id", "value"].into(),
            vec![PrimitiveArray::from_iter(0_i64..8).into_array(), encoded],
            8,
            Validity::NonNullable,
        )
        .into_array();
        let fixture = Fixture::new(array, 8);
        for op in [
            ComparisonOp::Eq,
            ComparisonOp::NotEq,
            ComparisonOp::Lt,
            ComparisonOp::LtEq,
            ComparisonOp::Gt,
            ComparisonOp::GtEq,
        ] {
            let plan = project(
                filter(fixture.scan(), "value", op, ScalarValue::UInt64(503)),
                vec![("row_id", column("row_id"))],
            );
            let result = prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected: Vec<_> = values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| {
                    let value = (*value)?;
                    let keep = match op {
                        ComparisonOp::Eq => value == 503,
                        ComparisonOp::NotEq => value != 503,
                        ComparisonOp::Lt => value < 503,
                        ComparisonOp::LtEq => value <= 503,
                        ComparisonOp::Gt => value > 503,
                        ComparisonOp::GtEq => value >= 503,
                    };
                    keep.then_some(i64::try_from(index).unwrap())
                })
                .collect();
            assert_eq!(ids(&result), expected);
            assert!(observed(&result, &format!("{family}_calls")) > 0);
            assert_eq!(observed(&result, "alp_calls"), 0);
        }
    }
}
