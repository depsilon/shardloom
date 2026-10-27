//! Independent grouping-set references and ownership across resident/spilled execution.

use super::*;
use crate::relational_query::{
    VortexRelationalGroupingIndicator as Indicator, VortexRelationalGroupingSets as Sets,
    VortexRelationalSpillPolicy,
};
use serde_json::{Value, json};
use vortex::array::arrays::BoolArray;

fn key(row: u64) -> Option<&'static str> {
    match row % 4 {
        0 | 1 => Some("A"),
        2 => Some("B"),
        _ => None,
    }
}
fn keep(row: u64) -> Option<bool> {
    (!row.is_multiple_of(5)).then_some(row.is_multiple_of(2))
}
fn source(rows: u64) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["g", "r", "id", "value", "keep", "unused"]),
            vec![
                VarBinViewArray::from_iter_nullable_str((0..rows).map(key)).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| row % 3)).into_array(),
                PrimitiveArray::from_iter(0..rows).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| row % 11)).into_array(),
                BoolArray::from_iter((0..rows).map(keep)).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|_| 99u64)).into_array(),
            ],
            rows.try_into().unwrap(),
            Validity::NonNullable,
        )
        .into_array(),
        257,
    )
}
fn refs(names: &[&str]) -> Vec<ColumnRef> {
    names
        .iter()
        .map(|name| ColumnRef::new(*name).unwrap())
        .collect()
}
fn declaration(sets: &[&[&str]]) -> Sets {
    Sets {
        sets: sets.iter().map(|set| refs(set)).collect(),
        indicators: vec![
            Indicator {
                columns: refs(&["g", "r"]),
                alias: "mask".into(),
            },
            Indicator {
                columns: refs(&["r", "g"]),
                alias: "reverse".into(),
            },
        ],
    }
}
fn plan(fixture: &Fixture) -> VortexRelationalPlan {
    let mut measures = vec![
        measure("count", None, "n"),
        measure("sum", Some("r"), "sum_r"),
        measure("count_distinct", Some("g"), "keys"),
    ];
    for (function, name) in [
        ("list", "items"),
        ("var_pop", "variance"),
        ("sum_distinct", "total"),
        ("median", "median"),
    ] {
        let mut value = measure(function, Some("value"), name)
            .with_filter_column(ColumnRef::new("keep").unwrap());
        if function == "list" {
            value.order_by = vec![VortexRelationalOrderKey {
                column: ColumnRef::new("id").unwrap(),
                descending: true,
                nulls: Some(VortexRelationalNullOrder::Last),
            }];
        }
        measures.push(value);
    }
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: fixture.scan(),
        group_by: refs(&["g", "r"]),
        grouping_sets: Some(declaration(&[&["g", "r"], &["g"], &[], &["g"], &["r"]])),
        measures,
    }))
}
fn expected(rows: u64) -> Vec<Value> {
    let mut result = Vec::new();
    for (use_g, use_r) in [
        (true, true),
        (true, false),
        (false, false),
        (true, false),
        (false, true),
    ] {
        let mut groups = std::collections::BTreeMap::new();
        if !use_g && !use_r {
            groups.insert((None, None), Vec::new());
        }
        for row in 0..rows {
            groups
                .entry((use_g.then(|| key(row)).flatten(), use_r.then_some(row % 3)))
                .or_default()
                .push(row);
        }
        for ((g, r), group) in groups {
            let selected = group
                .iter()
                .rev()
                .copied()
                .filter(|row| keep(*row) == Some(true))
                .map(|row| row % 11)
                .collect::<Vec<_>>();
            let n = u32::try_from(selected.len()).unwrap();
            let sum = u32::try_from(selected.iter().sum::<u64>()).unwrap();
            let squares = selected
                .iter()
                .map(|v| u32::try_from(v * v).unwrap())
                .sum::<u32>();
            let variance = (n != 0).then(|| {
                (f64::from(n) * f64::from(squares) - f64::from(sum).powi(2)) / f64::from(n).powi(2)
            });
            let keys = group
                .iter()
                .filter_map(|row| key(*row))
                .collect::<std::collections::BTreeSet<_>>();
            let distinct = selected
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            let total = (!distinct.is_empty())
                .then(|| f64::from(u32::try_from(distinct.iter().sum::<u64>()).unwrap()));
            let mut ordered = selected.clone();
            ordered.sort_unstable();
            let middle = (!ordered.is_empty()).then(|| {
                f64::from(
                    u32::try_from(ordered[(ordered.len() - 1) / 2] + ordered[ordered.len() / 2])
                        .unwrap(),
                ) / 2.0
            });
            result.push(json!({"g":g,"r":r,"mask":u8::from(!use_g)*2+u8::from(!use_r),
                "reverse":u8::from(!use_r)*2+u8::from(!use_g),"n":group.len(),"keys":keys.len(),
                "sum_r":(!group.is_empty()).then(|| f64::from(u32::try_from(group.iter().map(|row| row%3).sum::<u64>()).unwrap())),
                "items":if selected.is_empty() {Value::Null} else {json!(selected)}, "variance":variance,"total":total,"median":middle}));
        }
    }
    result
}
fn compare(mut actual: Vec<Value>, mut expected: Vec<Value>) {
    // SQL grouping output is unordered; preserve and compare duplicate rows.
    let key = |value: &Value| {
        (
            value["mask"].to_string(),
            value["g"].to_string(),
            value["r"].to_string(),
        )
    };
    actual.sort_by_key(&key);
    expected.sort_by_key(&key);
    assert_eq!(actual.len(), expected.len());
    for (mut actual, expected) in actual.into_iter().zip(expected) {
        if let Some(value) = expected["variance"].as_f64() {
            assert!(
                (actual["variance"].as_f64().unwrap() - value).abs()
                    <= 2e-12 * value.abs().max(1.0)
            );
            actual["variance"] = expected["variance"].clone();
        }
        assert_eq!(actual, expected);
    }
}
fn prepare(fixture: &Fixture, spill: bool) -> PreparedVortexRelational {
    let prepared = prepare_relational(&plan(fixture), policy()).unwrap();
    if spill {
        let workspace = fixture.0.join("grouping-runs");
        fs::create_dir_all(&workspace).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(workspace, 256 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

#[test]
fn grouping_sets_duplicate_null_subtotal_argument_and_filter_semantics() {
    for rows in [0, 1, 37, 8192] {
        let fixture = source(rows);
        for spill in [false, true] {
            let prepared = prepare(&fixture, spill);
            let fields = prepared
                .output_dtype()
                .unwrap()
                .as_struct_fields_opt()
                .unwrap()
                .clone();
            assert_eq!(fields.nfields(), 11);
            assert_eq!(
                fields.field("r").unwrap(),
                DType::Primitive(PType::U64, Nullability::Nullable)
            );
            assert_eq!(
                fields.field("mask").unwrap(),
                DType::Primitive(PType::U64, Nullability::NonNullable)
            );
            assert_eq!(
                fields.field("items").unwrap(),
                DType::List(
                    DType::Primitive(PType::U64, Nullability::NonNullable).into(),
                    Nullability::Nullable
                )
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap_or_else(|error| panic!("rows={rows} spill={spill}: {error}"));
            compare(json_rows(&result), expected(rows));
            assert_eq!(result.execution.scan_rows_delivered, rows);
            assert!(result.execution.native_io_certificate.is_certified());
            assert!(
                !result
                    .execution
                    .native_io_certificate
                    .side_effects
                    .fallback_attempted
            );
            if spill {
                let report = result.execution.spill.as_ref().unwrap();
                if rows == 8192 {
                    assert!(report.runs_written > 0);
                }
                assert!(report.owned_cleanup_completed);
                assert_eq!(fs::read_dir(&report.workspace).unwrap().count(), 0);
                assert_eq!(
                    result.execution.ordered_aggregate_input_rows,
                    if rows == 0 { 1 } else { rows * 5 }
                );
            }
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn grouping_sets_pressure_cancel_delivery_failure_and_retry() {
    let fixture = source(128);
    for spill in [false, true] {
        let prepared = prepare(&fixture, spill);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let pressure = prepared
            .session
            .memory()
            .reserve(prepared.snapshot().memory.limit_bytes - baseline - 1024)
            .unwrap();
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        drop(pressure);
        for cancel in [false, true] {
            let token = CancellationToken::default();
            let mut delivered = 0;
            let result = prepared.for_each_batch(&token, |_, _| {
                delivered += 1;
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(shardloom_core::ShardLoomError::InvalidOperation(
                        "grouping delivery failure".into(),
                    ))
                }
            });
            assert!(result.is_err());
            assert_eq!(delivered, 1);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
        }
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        compare(json_rows(&result), expected(128));
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        if spill {
            assert_eq!(
                fs::read_dir(fixture.0.join("grouping-runs"))
                    .unwrap()
                    .count(),
                0
            );
        }
    }
}

#[test]
fn grouping_sets_reject_invalid_metadata_and_preserve_unomitted_key_types() {
    let fixture = source(1);
    for sets in [
        Sets {
            sets: vec![],
            indicators: vec![],
        },
        Sets {
            sets: vec![refs(&["missing"])],
            indicators: vec![],
        },
        Sets {
            sets: vec![vec![]],
            indicators: vec![Indicator {
                columns: refs(&["value"]),
                alias: "bad".into(),
            }],
        },
        Sets {
            sets: vec![vec![]],
            indicators: vec![Indicator {
                columns: vec![ColumnRef::new("r").unwrap(); 65],
                alias: "bad".into(),
            }],
        },
    ] {
        let mut plan = plan(&fixture);
        let VortexRelationalPlan::Aggregate(aggregate) = &mut plan else {
            unreachable!()
        };
        aggregate.grouping_sets = Some(sets);
        assert!(prepare_relational(&plan, policy()).is_err());
    }
    let mut plan = plan(&fixture);
    let VortexRelationalPlan::Aggregate(aggregate) = &mut plan else {
        unreachable!()
    };
    aggregate.grouping_sets = Some(declaration(&[&["g", "r", "r"], &["r"]]));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert_eq!(
        prepared
            .output_dtype()
            .unwrap()
            .as_struct_fields_opt()
            .unwrap()
            .field("r")
            .unwrap(),
        DType::Primitive(PType::U64, Nullability::NonNullable)
    );
    assert_eq!(prepared.execute_owned().unwrap().execution.output_rows, 2);
}
