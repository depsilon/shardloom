//! Exact distinct membership precedes numeric reduction on every state owner.
use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Function, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalSpillPolicy,
    VortexRelationalWindow as Window, VortexRelationalWindowExpression as Expression,
    VortexRelationalWindowFrame as Frame, VortexRelationalWindowFunction as Framed,
};
use serde_json::json;
use shardloom_core::ScalarValue;
use std::collections::BTreeSet;
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

fn check(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[serde_json::Value]) {
    let workspace = fixture.0.join("distinct-runs");
    fs::create_dir_all(&workspace).unwrap();
    for spill in [false, true] {
        let prepared = prepare_relational(plan, policy()).unwrap();
        let prepared = if spill {
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
            .unwrap_or_else(|error| panic!("spill={spill}, plan={plan:?}: {error}"));
        assert_eq!(json_rows(&result), expected);
        assert!(result.execution.native_io_certificate.is_certified());
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn native_distinct_additive_grouped_scalar_empty_and_original_width_values() {
    let fixture = fixture();
    let measures = || {
        vec![
            measure("sum_distinct", Some("number"), "sum"),
            measure("avg_distinct", Some("number"), "mean"),
            measure("count_distinct", Some("number"), "distinct"),
        ]
    };
    check(
        &fixture,
        &aggregate(fixture.scan(), &["group"], measures()),
        &[
            json!({"group":"B","sum":2.0,"mean":1.0,"distinct":2}),
            json!({"group":"A","sum":5.0,"mean":5.0,"distinct":1}),
            json!({"group":null,"sum":9.0,"mean":9.0,"distinct":1}),
        ],
    );
    check(
        &fixture,
        &aggregate(fixture.scan(), &[], measures()),
        &[json!({"sum":16.0,"mean":4.0,"distinct":4})],
    );
    for values in [
        vec![],
        vec![None, None],
        vec![Some(1u64 << 60), Some((1u64 << 60) + 1), Some(1u64 << 60)],
    ] {
        let nonempty = !values.iter().all(Option::is_none);
        let fixture = Fixture::new(
            single(
                "number",
                PrimitiveArray::from_option_iter(values).into_array(),
            ),
            1,
        );
        check(
            &fixture,
            &aggregate(fixture.scan(), &[], measures()),
            &[if nonempty {
                json!({"sum":2.0f64.powi(61),"mean":2.0f64.powi(60),"distinct":2})
            } else {
                json!({"sum":null,"mean":null,"distinct":0})
            }],
        );
    }
}

#[test]
fn native_distinct_additive_decimal_slots_mix_with_ordinary_decimal_reducers() {
    let dtype = DecimalDType::new(10, 2);
    let fixture = Fixture::new(
        single(
            "number",
            DecimalArray::from_option_iter([Some(1i128), Some(1), Some(4)], dtype).into_array(),
        ),
        1,
    );
    let plan = aggregate(
        fixture.scan(),
        &[],
        vec![
            measure("sum_distinct", Some("number"), "distinct_sum"),
            measure("sum", Some("number"), "sum"),
            measure("avg_distinct", Some("number"), "distinct_mean"),
            measure("avg", Some("number"), "mean"),
        ],
    );
    check(
        &fixture,
        &plan,
        &[
            json!({"distinct_sum":"decimal128(38,2):5", "sum":"decimal128(38,2):6",
        "distinct_mean":"decimal128(38,6):25000", "mean":"decimal128(38,6):20000"}),
        ],
    );
}

const NUMBERS: [Option<i64>; 8] = [
    Some(3),
    Some(3),
    Some(-3),
    Some(3),
    None,
    Some(-3),
    Some(12),
    Some(12),
];
const KEYS: [u64; 8] = [0, 0, 1, 1, 1, 2, 3, 3];

fn window_reference(unit: Unit, exclusion: Exclusion, decimal: bool) -> Vec<serde_json::Value> {
    (0usize..8).map(|row| {
        let values = (0usize..8).filter(|&other| {
            let nearby = match unit {
                Unit::Rows => other.abs_diff(row) <= 2,
                Unit::Groups | Unit::Range => KEYS[other].abs_diff(KEYS[row]) <= 1,
            };
            nearby && match exclusion {
                Exclusion::NoOthers => true, Exclusion::CurrentRow => other != row,
                Exclusion::Group => KEYS[other] != KEYS[row],
                Exclusion::Ties => other == row || KEYS[other] != KEYS[row],
            }
        }).filter_map(|row| NUMBERS[row]).collect::<BTreeSet<_>>();
        let total = values.iter().sum::<i64>();
        if values.is_empty() { return json!({"id":row,"sum":null,"mean":null,"distinct":0}); }
        let count = i64::try_from(values.len()).unwrap();
        if decimal {
            assert_eq!((total * 10000) % count, 0);
            json!({"id":row,"sum":format!("decimal128(38,2):{total}"),
                "mean":format!("decimal128(38,6):{}", total * 10000 / count),"distinct":count})
        } else {
            json!({"id":row,"sum":f64::from(i32::try_from(total).unwrap()),
                "mean":f64::from(i32::try_from(total).unwrap()) / f64::from(u32::try_from(count).unwrap()),"distinct":count})
        }
    }).collect()
}

fn window(fixture: &Fixture, unit: Unit, exclusion: Exclusion) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: [
            ("sum_distinct", "sum"),
            ("avg_distinct", "mean"),
            ("count_distinct", "distinct"),
        ]
        .map(|(name, alias)| Expression {
            output_column: alias.into(),
            function: Framed::Framed(Function::Aggregate(Box::new(measure(
                name,
                Some("number"),
                alias,
            )))),
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("key").unwrap(),
                descending: false,
                nulls: None,
            }],
            frame: Some(Frame {
                unit,
                exclusion,
                start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(
                    if unit == Unit::Rows { 2 } else { 1 },
                ))),
                end: Bound::Following(Offset::Number(ScalarValue::UInt64(if unit == Unit::Rows {
                    2
                } else {
                    1
                }))),
            }),
        })
        .to_vec(),
    }))
}

#[test]
fn native_distinct_additive_windows_match_interval_unions_and_original_decimal_values() {
    for decimal in [false, true] {
        let number = if decimal {
            DecimalArray::from_option_iter(
                NUMBERS.map(|value| value.map(i128::from)),
                DecimalDType::new(10, 2),
            )
            .into_array()
        } else {
            PrimitiveArray::from_option_iter(NUMBERS).into_array()
        };
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["id", "key", "number"]),
                vec![
                    PrimitiveArray::from_iter(0u64..8).into_array(),
                    PrimitiveArray::from_iter(KEYS).into_array(),
                    number,
                ],
                8,
                Validity::NonNullable,
            )
            .into_array(),
            2,
        );
        for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                check(
                    &fixture,
                    &window(&fixture, unit, exclusion),
                    &window_reference(unit, exclusion, decimal),
                );
            }
        }
    }
}
