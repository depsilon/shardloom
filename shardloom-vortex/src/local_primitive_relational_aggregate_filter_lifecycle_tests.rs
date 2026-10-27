//! Filtered reducer ownership through real ordered runs and failed delivery.

use super::*;
use shardloom_core::ShardLoomError;

fn keep(row: u64) -> Option<bool> {
    (!row.is_multiple_of(5)).then_some(row % 4 != 3 && !row.is_multiple_of(3))
}

fn other(row: u64) -> bool {
    row % 4 != 3 && keep(row) != Some(true)
}

fn value(row: u64) -> Option<u64> {
    (!row.is_multiple_of(7)).then_some(row % 37)
}

fn text(row: u64) -> Option<&'static str> {
    match row % 4 {
        0 => None,
        1 => Some(""),
        2 => Some("東京"),
        _ => Some("a"),
    }
}

fn source(rows: u64) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from([
                "id",
                "window_key",
                "group",
                "value",
                "text",
                "keep",
                "other",
            ]),
            vec![
                PrimitiveArray::from_iter(0..rows).into_array(),
                PrimitiveArray::from_iter(0..rows).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| row % 4)).into_array(),
                PrimitiveArray::from_option_iter((0..rows).map(value)).into_array(),
                VarBinViewArray::from_iter_nullable_str((0..rows).map(text)).into_array(),
                BoolArray::from_iter((0..rows).map(keep)).into_array(),
                BoolArray::from_iter((0..rows).map(other)).into_array(),
            ],
            usize::try_from(rows).unwrap(),
            Validity::NonNullable,
        )
        .into_array(),
        257,
    )
}

fn measures() -> Vec<Measure> {
    let mut values = vec![
        measure("count", None, "n"),
        measure("sum_distinct", Some("value"), "total"),
        measure("var_pop", Some("value"), "variance"),
        measure("quantile_disc", Some("value"), "median")
            .with_quantile_level(0.5)
            .unwrap(),
        measure("first", Some("value"), "first").with_order_by(order("id", true)),
        measure("list", Some("value"), "items").with_order_by(order("id", true)),
        measure("string_agg", Some("text"), "joined")
            .with_separator("|")
            .with_order_by(order("id", true)),
    ]
    .into_iter()
    .map(|value| filtered(value, "keep"))
    .collect::<Vec<_>>();
    values.push(filtered(
        measure("quantile_disc", Some("value"), "other_median")
            .with_quantile_level(0.5)
            .unwrap(),
        "other",
    ));
    values
}

fn frame() -> Frame {
    Frame {
        unit: Unit::Rows,
        start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(3))),
        end: Bound::Following(Offset::Number(ScalarValue::UInt64(2))),
        exclusion: Exclusion::NoOthers,
    }
}

fn median(mut values: Vec<u64>) -> Option<u64> {
    values.sort_unstable();
    values
        .get(values.len().div_ceil(2).saturating_sub(1))
        .copied()
}

fn expected(rows: impl IntoIterator<Item = u64>) -> Value {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let selected = rows
        .iter()
        .copied()
        .rev()
        .filter(|&row| keep(row) == Some(true))
        .collect::<Vec<_>>();
    let mut values = selected
        .iter()
        .copied()
        .filter_map(value)
        .collect::<Vec<_>>();
    let count = u64::try_from(values.len()).unwrap();
    let sum = values.iter().sum::<u64>();
    let squares = values.iter().map(|value| value * value).sum::<u64>();
    let variance = (count != 0).then(|| {
        let numerator = u32::try_from(count * squares - sum * sum).unwrap();
        let denominator = u32::try_from(count * count).unwrap();
        f64::from(numerator) / f64::from(denominator)
    });
    let middle = median(values.clone());
    values.sort_unstable();
    values.dedup();
    let total =
        (!values.is_empty()).then(|| f64::from(u32::try_from(values.iter().sum::<u64>()).unwrap()));
    let strings = selected
        .iter()
        .copied()
        .filter_map(text)
        .collect::<Vec<_>>();
    json!({"n":selected.len(),"total":total,"variance":variance,"median":middle,
        "other_median":median(rows.iter().copied().filter(|&row| other(row)).filter_map(value).collect()),
        "first":selected.first().copied().and_then(value),
        "items":if selected.is_empty() {Value::Null} else {json!(selected.iter().copied().map(value).collect::<Vec<_>>())},
        "joined":(!strings.is_empty()).then(|| strings.join("|"))})
}

fn wanted(rows: u64, grouped: bool) -> Vec<Value> {
    if grouped {
        (0..4)
            .map(|group| {
                let mut value = expected((0..rows).filter(|row| row % 4 == group));
                value["group"] = json!(group);
                value
            })
            .collect()
    } else {
        (0..rows)
            .map(|row| {
                let mut value = expected(row.saturating_sub(3)..(row + 3).min(rows));
                value["id"] = json!(row);
                value
            })
            .collect()
    }
}

fn compare(actual: &[Value], wanted: &[Value]) {
    assert_eq!(actual.len(), wanted.len());
    for (actual, wanted) in actual.iter().zip(wanted) {
        let mut actual = actual.clone();
        if let Some(value) = wanted["variance"].as_f64() {
            let observed = actual["variance"].as_f64().unwrap();
            assert!(
                (observed - value).abs() <= 2e-12 * value.abs().max(1.0),
                "{actual} != {wanted}"
            );
            actual["variance"] = wanted["variance"].clone();
        }
        assert_eq!(&actual, wanted);
    }
}

#[test]
fn aggregate_filter_actual_spill_preserves_separate_domains_and_complete_results() {
    const ROWS: u64 = 8192;
    let fixture = source(ROWS);
    let workspace = fixture.0.join("filter-runs");
    fs::create_dir(&workspace).unwrap();
    for grouped in [true, false] {
        let plan = if grouped {
            aggregate(fixture.scan(), &["group"], measures())
        } else {
            window_plan(&fixture, &frame(), measures())
        };
        let wanted = wanted(ROWS, grouped);
        for spill in [false, true] {
            let prepared = prepare_relational(&plan, policy()).unwrap();
            let prepared = if spill {
                prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(&workspace, 128 << 20, 1 << 20).unwrap(),
                    )
                    .unwrap()
            } else {
                prepared
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap_or_else(|error| panic!("grouped={grouped}, spill={spill}: {error}"));
            compare(&json_rows(&result), &wanted);
            assert!(result.execution.native_io_certificate.is_certified());
            assert!(
                !result
                    .execution
                    .native_io_certificate
                    .side_effects
                    .fallback_attempted
            );
            if spill {
                let evidence = result.execution.spill.as_ref().unwrap();
                assert!(evidence.runs_written > 0);
                assert!(evidence.owned_cleanup_completed);
            }
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn aggregate_filter_pressure_cancellation_and_output_failure_allow_clean_retry() {
    let fixture = source(128);
    let workspace = fixture.0.join("filter-recovery");
    fs::create_dir(&workspace).unwrap();
    for grouped in [true, false] {
        let plan = if grouped {
            aggregate(fixture.scan(), &["group"], measures())
        } else {
            window_plan(&fixture, &frame(), measures())
        };
        for spill in [false, true] {
            let prepared = prepare_relational(&plan, policy()).unwrap();
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
                        Err(ShardLoomError::InvalidOperation(
                            "filtered output failure".into(),
                        ))
                    }
                });
                assert!(result.is_err());
                assert_eq!(delivered, 1);
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            compare(&json_rows(&result), &wanted(128, grouped));
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}
