use super::*;
use shardloom_core::ShardLoomError;

fn source(rows: u64) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "group", "value", "text"]),
            vec![
                PrimitiveArray::from_iter(0..rows).into_array(),
                PrimitiveArray::from_iter(0..rows).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| row % 4)).into_array(),
                PrimitiveArray::from_iter((0..rows).map(|row| (1u64 << 60) + row)).into_array(),
                VarBinViewArray::from_iter_nullable_str((0..rows).map(text)).into_array(),
            ],
            usize::try_from(rows).unwrap(),
            Validity::NonNullable,
        )
        .into_array(),
        257,
    )
}

fn text(row: u64) -> Option<&'static str> {
    match row % 4 {
        0 => None,
        1 => Some(""),
        2 => Some("東京"),
        _ => Some("a"),
    }
}

fn measures() -> Vec<Measure> {
    vec![
        measure("list", Some("value"), "items").with_order_by(order("id", true)),
        measure("string_agg", Some("text"), "joined")
            .with_separator("|")
            .with_order_by(order("id", true)),
    ]
}

fn frame() -> Frame {
    Frame {
        unit: Unit::Rows,
        start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(2))),
        end: Bound::CurrentRow,
        exclusion: Exclusion::NoOthers,
    }
}

fn expected(rows: impl IntoIterator<Item = u64>) -> (Vec<u64>, Option<String>) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let values = rows.iter().map(|row| (1u64 << 60) + row).collect();
    let text = rows.into_iter().filter_map(text).collect::<Vec<_>>();
    (values, (!text.is_empty()).then(|| text.join("|")))
}

#[test]
fn ordered_collection_large_groups_and_windows_spill_and_match_complete_values() {
    const ROWS: u64 = 8192;
    let fixture = source(ROWS);
    let plans = [
        aggregate(fixture.scan(), &["group"], measures()),
        window_plan(&fixture, &frame(), measures()),
    ];
    let workspace = fixture.0.join("collection-runs");
    fs::create_dir(&workspace).unwrap();
    for (index, plan) in plans.iter().enumerate() {
        let wanted = if index == 0 {
            (0..4)
                .map(|group| {
                    let (items, joined) = expected((0..ROWS).rev().filter(|row| row % 4 == group));
                    json!({"group":group,"items":items,"joined":joined})
                })
                .collect::<Vec<_>>()
        } else {
            (0..ROWS)
                .map(|id| {
                    let (items, joined) = expected((id.saturating_sub(2)..=id).rev());
                    json!({"id":id,"items":items,"joined":joined})
                })
                .collect::<Vec<_>>()
        };
        for spill in [false, true] {
            let prepared = prepare_relational(plan, policy()).unwrap();
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
                .unwrap_or_else(|error| panic!("plan={index}, spill={spill}: {error}"));
            assert_eq!(json_rows(&result), wanted, "plan={index}, spill={spill}");
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
fn ordered_collection_pressure_cancellation_output_failure_and_retry_release_owners() {
    let fixture = source(128);
    let workspace = fixture.0.join("collection-recovery");
    fs::create_dir(&workspace).unwrap();
    for plan in [
        aggregate(fixture.scan(), &["group"], measures()),
        window_plan(&fixture, &frame(), measures()),
    ] {
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
                            "collection output failure".into(),
                        ))
                    }
                });
                assert!(result.is_err());
                assert_eq!(delivered, 1);
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
            drop(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap(),
            );
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn ordered_collection_oversized_string_result_is_denied_before_delivery_even_with_spill() {
    let fixture = source(128);
    let selected = measure("string_agg", Some("text"), "large").with_separator("s".repeat(1 << 20));
    let full = Frame {
        start: Bound::UnboundedPreceding,
        end: Bound::UnboundedFollowing,
        ..frame()
    };
    let workspace = fixture.0.join("collection-denial");
    fs::create_dir(&workspace).unwrap();
    for plan in [
        aggregate(fixture.scan(), &[], vec![selected.clone()]),
        window_plan(&fixture, &full, vec![selected]),
    ] {
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
            let mut delivered = 0;
            let error = prepared
                .for_each_batch(&CancellationToken::default(), |_, _| {
                    delivered += 1;
                    Ok(())
                })
                .err()
                .unwrap();
            assert!(error.to_string().contains("reservation"), "{error}");
            assert_eq!(delivered, 0);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}
