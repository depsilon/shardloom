//! Explicit result policies reach the existing resident and stored state owners.
use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound, VortexRelationalFrameExclusion, VortexRelationalFrameFunction,
    VortexRelationalFrameOffset, VortexRelationalFrameUnit, VortexRelationalSpillPolicy,
    VortexRelationalWindow, VortexRelationalWindowExpression, VortexRelationalWindowFrame,
    VortexRelationalWindowFunction,
};
use serde_json::json;
use shardloom_core::{
    ScalarValue,
    decimal_output::{DecimalOutput, DecimalRounding},
};
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

#[path = "local_primitive_relational_decimal_pressure_tests.rs"]
mod pressure_tests;

fn declared(function: &str, alias: &str, scale: u8, mode: DecimalRounding) -> Measure {
    measure(function, Some("amount"), alias)
        .with_decimal_output(DecimalOutput::new(8, scale, mode).unwrap())
}

fn check(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[serde_json::Value]) {
    let workspace = fixture.0.join("decimal-policy-runs");
    fs::create_dir_all(&workspace).unwrap();
    for stored in [false, true] {
        let prepared = prepare_relational(plan, policy()).unwrap();
        let prepared = if stored {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected, "stored={stored}");
        assert!(result.execution.native_io_certificate.is_certified());
        if stored {
            assert!(
                result
                    .execution
                    .spill
                    .as_ref()
                    .unwrap()
                    .owned_cleanup_completed
            );
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn native_decimal_output_grouped_stored_distinct_and_quantile_modes_share_exact_inputs() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "amount"]),
            vec![
                VarBinViewArray::from_iter_str(["A", "A", "A", "B", "N"]).into_array(),
                DecimalArray::from_option_iter(
                    [Some(125_i128), Some(125), Some(250), Some(-125), None],
                    DecimalDType::new(5, 2),
                )
                .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let plan = aggregate(
        fixture.scan(),
        &["group"],
        vec![
            measure("sum", Some("amount"), "default"),
            declared("sum", "total", 1, DecimalRounding::HalfEven),
            declared("avg", "mean", 2, DecimalRounding::HalfEven),
            declared(
                "sum_distinct",
                "distinct_total",
                1,
                DecimalRounding::HalfEven,
            ),
            declared(
                "avg_distinct",
                "distinct_mean",
                2,
                DecimalRounding::HalfEven,
            ),
            declared("median", "even", 1, DecimalRounding::HalfEven),
            declared("median", "away", 1, DecimalRounding::HalfAway),
            declared("quantile_cont", "q", 1, DecimalRounding::Ceil)
                .with_quantile_level(0.1)
                .unwrap(),
        ],
    );
    check(
        &fixture,
        &plan,
        &[
            json!({"group":"A","default":"decimal128(38,2):500","total":"decimal128(8,1):50","mean":"decimal128(8,2):167","distinct_total":"decimal128(8,1):38","distinct_mean":"decimal128(8,2):188","even":"decimal128(8,1):12","away":"decimal128(8,1):13","q":"decimal128(8,1):13"}),
            json!({"group":"B","default":"decimal128(38,2):-125","total":"decimal128(8,1):-12","mean":"decimal128(8,2):-125","distinct_total":"decimal128(8,1):-12","distinct_mean":"decimal128(8,2):-125","even":"decimal128(8,1):-12","away":"decimal128(8,1):-13","q":"decimal128(8,1):-12"}),
            json!({"group":"N","default":null,"total":null,"mean":null,"distinct_total":null,"distinct_mean":null,"even":null,"away":null,"q":null}),
        ],
    );
}

#[test]
fn native_decimal_output_moving_distinct_and_quantile_frames_round_after_removal() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "amount"]),
            vec![
                PrimitiveArray::from_iter(0_i64..5).into_array(),
                DecimalArray::from_option_iter(
                    [Some(125_i128), Some(125), Some(250), Some(-125), None],
                    DecimalDType::new(5, 2),
                )
                .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    for unit in [
        VortexRelationalFrameUnit::Rows,
        VortexRelationalFrameUnit::Range,
    ] {
        let plan = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
            input: fixture.scan(),
            columns: vec![ColumnRef::new("id").unwrap()],
            expressions: vec![
                declared("avg", "mean", 1, DecimalRounding::HalfEven),
                declared("avg_distinct", "distinct", 1, DecimalRounding::HalfAway),
                declared("median", "median", 1, DecimalRounding::Floor),
                declared("quantile_cont", "q", 1, DecimalRounding::Ceil)
                    .with_quantile_level(0.5)
                    .unwrap(),
            ]
            .into_iter()
            .map(|measure| VortexRelationalWindowExpression {
                output_column: measure.alias.clone(),
                function: VortexRelationalWindowFunction::Framed(
                    VortexRelationalFrameFunction::Aggregate(Box::new(measure)),
                ),
                partition_by: vec![],
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("id").unwrap(),
                    descending: false,
                    nulls: None,
                }],
                frame: Some(VortexRelationalWindowFrame {
                    unit,
                    start: VortexRelationalFrameBound::Preceding(
                        VortexRelationalFrameOffset::Number(ScalarValue::Int64(1)),
                    ),
                    end: VortexRelationalFrameBound::CurrentRow,
                    exclusion: VortexRelationalFrameExclusion::NoOthers,
                }),
            })
            .collect(),
        }));
        check(
            &fixture,
            &plan,
            &[
                json!({"id":0,"mean":"decimal128(8,1):12","distinct":"decimal128(8,1):13","median":"decimal128(8,1):12","q":"decimal128(8,1):13"}),
                json!({"id":1,"mean":"decimal128(8,1):12","distinct":"decimal128(8,1):13","median":"decimal128(8,1):12","q":"decimal128(8,1):13"}),
                json!({"id":2,"mean":"decimal128(8,1):19","distinct":"decimal128(8,1):19","median":"decimal128(8,1):18","q":"decimal128(8,1):19"}),
                json!({"id":3,"mean":"decimal128(8,1):6","distinct":"decimal128(8,1):6","median":"decimal128(8,1):6","q":"decimal128(8,1):7"}),
                json!({"id":4,"mean":"decimal128(8,1):-12","distinct":"decimal128(8,1):-13","median":"decimal128(8,1):-13","q":"decimal128(8,1):-12"}),
            ],
        );
    }
}

#[test]
fn native_decimal_output_empty_and_unsupported_aggregate_domains_bind_explicitly() {
    for values in [vec![], vec![None::<i128>, None]] {
        let fixture = Fixture::new(
            single(
                "amount",
                DecimalArray::from_option_iter(values, DecimalDType::new(5, 2)).into_array(),
            ),
            1,
        );
        let plan = aggregate(
            fixture.scan(),
            &[],
            vec![
                declared("sum", "sum", 2, DecimalRounding::Exact),
                declared("avg", "avg", 2, DecimalRounding::Exact),
                declared("median", "median", 2, DecimalRounding::HalfEven),
            ],
        );
        check(
            &fixture,
            &plan,
            &[json!({"sum":null,"avg":null,"median":null})],
        );
        let dtype = prepare_relational(&plan, policy())
            .unwrap()
            .output_dtype()
            .unwrap();
        assert_eq!(
            dtype.as_struct_fields_opt().unwrap().field("sum").unwrap(),
            DType::Decimal(DecimalDType::new(8, 2), Nullability::Nullable)
        );
        for function in ["min", "max", "count", "quantile_disc"] {
            let mut measure = declared(function, "bad", 2, DecimalRounding::Exact);
            if function == "quantile_disc" {
                measure = measure.with_quantile_level(0.5).unwrap();
            }
            assert!(
                prepare_relational(&aggregate(fixture.scan(), &[], vec![measure]), policy())
                    .is_err()
            );
        }
    }
    let fixture = Fixture::new(
        single("amount", PrimitiveArray::from_iter([1_i64, 2]).into_array()),
        1,
    );
    assert!(
        prepare_relational(
            &aggregate(
                fixture.scan(),
                &[],
                vec![declared("sum", "bad", 0, DecimalRounding::Exact)]
            ),
            policy()
        )
        .is_err()
    );
}

#[test]
fn native_decimal_output_legacy_aggregate_admission_never_ignores_the_policy() {
    use crate::query_primitive::{VortexQueryPrimitiveRequest, VortexSimpleAggregateRequest};
    for function in ["sum", "avg"] {
        let ordinary = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("absent-decimal-admission.vortex").unwrap(),
            VortexSimpleAggregateRequest::new(vec![measure(function, Some("amount"), "value")]),
        );
        assert!(crate::local_primitives::required_simple_aggregate(&ordinary).is_ok());
        let mut declared = ordinary;
        declared.simple_aggregate.as_mut().unwrap().measures[0].decimal_output =
            Some(DecimalOutput::new(8, 2, DecimalRounding::HalfEven).unwrap());
        let error = crate::local_primitives::required_simple_aggregate(&declared).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("require the native relational surface"),
            "{error}"
        );
    }
}
