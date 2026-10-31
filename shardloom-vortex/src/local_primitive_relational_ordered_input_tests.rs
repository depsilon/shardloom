//! Exercise every real owner of the validated ordered-input check.

use super::*;
use crate::relational_query::{
    VortexRelationalAsOfJoin, VortexRelationalAsOfOptions, VortexRelationalWindow,
    VortexRelationalWindowExpression, VortexRelationalWindowFunction,
};
use serde_json::{Value, json};

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

fn order(names: &[&str]) -> Vec<VortexRelationalOrderKey> {
    names
        .iter()
        .map(|name| VortexRelationalOrderKey {
            column: column(name),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::Last),
        })
        .collect()
}

fn plans(fixture: &Fixture) -> Vec<(&'static str, VortexRelationalPlan)> {
    vec![
        (
            "sort",
            VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
                input: fixture.scan(),
                keys: order(&["group", "key"]),
            })),
        ),
        (
            "window",
            VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
                input: fixture.scan(),
                columns: vec![column("id")],
                expressions: vec![VortexRelationalWindowExpression {
                    output_column: "position".into(),
                    function: VortexRelationalWindowFunction::RowNumber,
                    partition_by: vec![column("group")],
                    order_by: order(&["key"]),
                    frame: None,
                }],
            })),
        ),
        (
            "asof",
            VortexRelationalPlan::AsOfJoin(Box::new(VortexRelationalAsOfJoin {
                left: fixture.scan(),
                right: fixture.scan(),
                by: vec![VortexRelationalJoinKey {
                    left: column("group"),
                    right: column("group"),
                }],
                on: VortexRelationalJoinKey {
                    left: column("key"),
                    right: column("key"),
                },
                options: VortexRelationalAsOfOptions::default(),
                columns: vec![
                    VortexRelationalJoinColumn {
                        side: Side::Left,
                        column: column("id"),
                        output_column: "id".into(),
                    },
                    VortexRelationalJoinColumn {
                        side: Side::Right,
                        column: column("id"),
                        output_column: "matched".into(),
                    },
                ],
            })),
        ),
        (
            "collections",
            aggregate(
                fixture.scan(),
                &["group"],
                vec![measure("array_agg", Some("id"), "items").with_order_by(order(&["key"]))],
            ),
        ),
        (
            "selectors",
            aggregate(
                fixture.scan(),
                &["group"],
                vec![
                    measure("first", Some("id"), "initial").with_order_by(order(&["key"])),
                    measure("last", Some("id"), "ending").with_order_by(order(&["key"])),
                ],
            ),
        ),
    ]
}

fn reference(owner: &str, input: &[i64]) -> Vec<Value> {
    let mut ordered = input.to_vec();
    ordered.sort_by_key(|row| (row / 11, row / 3));
    match owner {
        "sort" => ordered
            .iter()
            .map(|row| json!({"id":row,"group":row/11,"key":row/3}))
            .collect(),
        "window" => input
            .iter()
            .map(|row| {
                let group = ordered
                    .iter()
                    .filter(|other| *other / 11 == row / 11)
                    .collect::<Vec<_>>();
                let position = group.iter().position(|other| *other == row).unwrap() + 1;
                json!({"id":row,"position":position})
            })
            .collect(),
        "asof" => input
            .iter()
            .map(|row| {
                let matched = input
                    .iter()
                    .enumerate()
                    .filter(|(_, other)| *other / 11 == row / 11 && *other / 3 <= row / 3)
                    .max_by_key(|(position, other)| (*other / 3, *position))
                    .unwrap()
                    .1;
                json!({"id":row,"matched":matched})
            })
            .collect(),
        "collections" | "selectors" => (0..3)
            .map(|group| {
                let rows = ordered
                    .iter()
                    .copied()
                    .filter(|row| row / 11 == group)
                    .collect::<Vec<_>>();
                if owner == "collections" {
                    json!({"group":group,"items":rows})
                } else {
                    json!({"group":group,"initial":rows[0],"ending":rows.last().unwrap()})
                }
            })
            .collect(),
        _ => unreachable!(),
    }
}

#[test]
fn ordered_input_all_five_owners_activate_or_return_to_heap_with_complete_results() {
    for shuffled in [false, true] {
        let mut rows = (0_i64..33).collect::<Vec<_>>();
        if shuffled {
            rows.swap(1, 20);
        }
        for chunk in [7, 33] {
            let fixture = Fixture::new(
                StructArray::new(
                    FieldNames::from(["id", "group", "key"]),
                    vec![
                        PrimitiveArray::from_iter(rows.iter().copied()).into_array(),
                        PrimitiveArray::from_iter(rows.iter().map(|row| row / 11)).into_array(),
                        PrimitiveArray::from_iter(rows.iter().map(|row| row / 3)).into_array(),
                    ],
                    rows.len(),
                    Validity::NonNullable,
                )
                .into_array(),
                chunk,
            );
            for (owner, plan) in plans(&fixture) {
                let prepared = prepare_relational(&plan, policy()).unwrap();
                let memory = prepared.session.memory().clone();
                #[cfg(debug_assertions)]
                let observation =
                    crate::local_primitives::native_relational_observation::Session::start();
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert!(
                        counts.order_checks > 0,
                        "{owner}, shuffled={shuffled}, chunk={chunk}: {counts:?}"
                    );
                    if shuffled {
                        assert!(
                            counts.order_heap_sorts > 0,
                            "{owner}, chunk={chunk}: {counts:?}"
                        );
                    } else {
                        assert_eq!(
                            counts.order_heap_sorts, 0,
                            "{owner}, chunk={chunk}: {counts:?}"
                        );
                        assert!(
                            counts.ordered_input_sorts > 0,
                            "{owner}, chunk={chunk}: {counts:?}"
                        );
                        assert!(
                            counts.ordered_input_rows >= 33,
                            "{owner}, chunk={chunk}: {counts:?}"
                        );
                    }
                }
                assert_eq!(
                    json_rows(&result),
                    reference(owner, &rows),
                    "{owner}, shuffled={shuffled}, chunk={chunk}"
                );
                assert!(
                    !result
                        .execution
                        .native_io_certificate
                        .side_effects
                        .fallback_attempted
                );
                drop(result);
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}
