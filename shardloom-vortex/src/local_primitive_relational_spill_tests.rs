use super::*;
use crate::local_primitives::logical_field_from_native_array;
use crate::relational_query::{
    VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey, VortexRelationalSort,
    VortexRelationalSpillPolicy,
};
use vortex::array::arrays::VarBinViewArray;

fn key(name: &str, descending: bool) -> VortexRelationalOrderKey {
    VortexRelationalOrderKey {
        column: ColumnRef::new(name).unwrap(),
        descending,
        nulls: Some(NullOrder::Last),
    }
}

fn sorted(
    input: VortexRelationalPlan,
    keys: Vec<VortexRelationalOrderKey>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort { input, keys }))
}

fn fixture(rows: u32) -> (Fixture, Vec<serde_json::Value>) {
    let ids = (0..rows).rev().collect::<Vec<_>>();
    let categories = ids
        .iter()
        .map(|id| (!id.is_multiple_of(17)).then_some(u64::from(id % 7)))
        .collect::<Vec<_>>();
    let labels = ids
        .iter()
        .map(|id| ["001", "東京", "", "a\0z", "ω"][*id as usize % 5])
        .collect::<Vec<_>>();
    let samples = ids
        .iter()
        .map(|id| f64::from(*id % 13) / 2.0 - 3.0)
        .collect::<Vec<_>>();
    let expected = (0..ids.len())
        .map(|row| {
            serde_json::json!({
                "category": categories[row], "label": labels[row],
                "identifier": ids[row], "sample": samples[row],
            })
        })
        .collect();
    let batch = |start: usize, end: usize| {
        StructArray::try_new(
            FieldNames::from(["category", "label", "identifier", "sample"]),
            vec![
                PrimitiveArray::from_option_iter(categories[start..end].iter().copied())
                    .into_array(),
                VarBinViewArray::from_iter_str(labels[start..end].iter().copied()).into_array(),
                PrimitiveArray::from_iter(ids[start..end].iter().copied()).into_array(),
                PrimitiveArray::from_iter(samples[start..end].iter().copied()).into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    };
    // Each source segment owns a bounded payload. Slicing one large variable-
    // width array would let an upstream read retain its entire backing buffer.
    let empty = batch(0, 0);
    let fixture = Fixture::new(empty.clone(), 512);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                (rows as usize).div_ceil(512).max(1),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, empty.dtype().clone());
    if rows == 0 {
        writer.push(empty).unwrap();
    } else {
        for start in (0..rows as usize).step_by(512) {
            writer
                .push(batch(start, (start + 512).min(rows as usize)))
                .unwrap();
        }
    }
    writer.finish().unwrap();
    (fixture, expected)
}

fn sort_reference(rows: &mut [serde_json::Value], names: &[(&str, bool)]) {
    rows.sort_by(|left, right| {
        for (name, descending) in names {
            let (a, b) = (&left[*name], &right[*name]);
            let order = match (a.is_null(), b.is_null()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => {
                    let values = match (a.as_str(), b.as_str()) {
                        (Some(a), Some(b)) => a.cmp(b),
                        _ => a
                            .as_f64()
                            .unwrap()
                            .partial_cmp(&b.as_f64().unwrap())
                            .unwrap(),
                    };
                    if *descending {
                        values.reverse()
                    } else {
                        values
                    }
                }
            };
            if !order.is_eq() {
                return order;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn spill(fixture: &Fixture) -> VortexRelationalSpillPolicy {
    VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap()
}

fn assert_clean(fixture: &Fixture) {
    let entries = fs::read_dir(&fixture.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [std::ffi::OsString::from("input.vortex")]);
}

#[test]
fn native_relational_spill_stable_multikey_complete_values_and_repeated_cleanup() {
    let (fixture, mut expected) = fixture(24_001);
    sort_reference(&mut expected, &[("category", true), ("label", false)]);
    let plan = sorted(
        fixture.scan(),
        vec![key("category", true), key("label", false)],
    );
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
    for call in 1..=2 {
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&collected), expected);
        let report = collected.execution.spill.as_ref().unwrap();
        assert!(report.runs_written >= 3);
        assert!(report.merge_passes >= 1);
        assert_eq!(report.max_open_runs, 2);
        assert!(report.peak_disk_bytes <= report.quota_bytes);
        assert!(report.owned_cleanup_completed);
        assert!(
            collected
                .execution
                .native_io_certificate
                .side_effects
                .spill_io_performed
        );
        assert_eq!(collected.execution.runtime.completed_executions, call);
        assert_eq!(collected.execution.runtime.prepared_source_opens, 1);
        drop(collected);
        assert_clean(&fixture);
    }
}

#[test]
fn native_relational_spill_keeps_exact_scalar_extrema_nulls_and_signed_zero() {
    use shardloom_core::StatValue;
    use std::borrow::Borrow;
    use vortex::array::arrays::BoolArray;

    let count = 20_013usize;
    let unsigned = |row: usize| [u64::MAX, 0, (1 << 53) + 1, u64::MAX - 1][row % 4];
    let signed = |row: usize| [i64::MIN, -1, i64::MAX, 0][row % 4];
    let sample = |row: usize| [-3.0_f64, -0.0, 0.0, 3.0][row % 4];
    let flag = |row: usize| [None, Some(true), Some(false)][row % 3];
    let array = StructArray::try_new(
        FieldNames::from(["identifier", "wide", "signed", "sample", "flag"]),
        vec![
            PrimitiveArray::from_iter(0..count as u64).into_array(),
            PrimitiveArray::from_iter((0..count).map(unsigned)).into_array(),
            PrimitiveArray::from_iter((0..count).map(signed)).into_array(),
            PrimitiveArray::from_iter((0..count).map(sample)).into_array(),
            BoolArray::from_iter((0..count).map(flag)).into_array(),
        ],
        count,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(array, count);
    for (name, descending) in [
        ("wide", false),
        ("signed", true),
        ("sample", false),
        ("flag", true),
    ] {
        let mut expected = (0..count).collect::<Vec<_>>();
        expected.sort_by(|left, right| {
            let order = match name {
                "wide" => unsigned(*left).cmp(&unsigned(*right)),
                "signed" => signed(*left).cmp(&signed(*right)),
                "sample" => sample(*left).partial_cmp(&sample(*right)).unwrap(),
                "flag" => match (flag(*left), flag(*right)) {
                    (None, None) => return std::cmp::Ordering::Equal,
                    (None, _) => return std::cmp::Ordering::Less,
                    (_, None) => return std::cmp::Ordering::Greater,
                    (Some(left), Some(right)) => left.cmp(&right),
                },
                _ => unreachable!(),
            };
            if descending { order.reverse() } else { order }
        });
        let mut key = key(name, descending);
        key.nulls = Some(NullOrder::First);
        let prepared = prepare_relational(&sorted(fixture.scan(), vec![key]), policy())
            .unwrap()
            .with_spill(spill(&fixture))
            .unwrap();
        let mut offset = 0;
        let execution = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut values = super::super::super::prepared_unary::values::NativeBatch::new(
                    &array,
                    &[
                        "identifier".into(),
                        "wide".into(),
                        "signed".into(),
                        "sample".into(),
                        "flag".into(),
                    ],
                    context,
                )?;
                for row in 0..array.len() {
                    let id = expected[offset];
                    let mut stat = |column| -> Result<StatValue> {
                        let owned = values.stat(column, row)?;
                        Ok(Borrow::<StatValue>::borrow(&owned).clone())
                    };
                    assert_eq!(stat(0)?, StatValue::UInt64(id as u64));
                    assert_eq!(stat(1)?, StatValue::UInt64(unsigned(id)));
                    assert_eq!(stat(2)?, StatValue::Int64(signed(id)));
                    let StatValue::Float64(value) = stat(3)? else {
                        panic!("float payload changed type");
                    };
                    assert_eq!(value.to_bits(), sample(id).to_bits());
                    assert_eq!(
                        stat(4)?,
                        flag(id).map_or(StatValue::Null, StatValue::Boolean)
                    );
                    offset += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(offset, count);
        assert!(execution.spill.as_ref().unwrap().merge_passes >= 1);
        assert_clean(&fixture);
    }
}

#[test]
fn native_relational_spill_nested_order_stages_share_quota_and_keep_stable_input_order() {
    let (fixture, mut expected) = fixture(40_013);
    sort_reference(&mut expected, &[("label", true)]);
    sort_reference(&mut expected, &[("category", false)]);
    let plan = sorted(
        sorted(fixture.scan(), vec![key("label", true)]),
        vec![key("category", false)],
    );
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&collected), expected);
    let report = collected.execution.spill.as_ref().unwrap();
    assert!(report.runs_written >= 6);
    assert!(report.merge_passes >= 2);
    assert!(
        report.max_open_runs >= 3,
        "nested stages must count overlapping readers"
    );
    assert!(report.owned_cleanup_completed);
    assert!(
        collected.execution.runtime.memory.peak_reserved_bytes
            <= policy().resource_envelope.memory_budget_bytes
    );
    let quota = report.peak_disk_bytes - 1;
    drop(collected);
    assert_clean(&fixture);

    // Both input runs fit, but their successor must coexist with them. Denying
    // one byte below the observed overlap peak must not discard either input
    // early or produce a successful execution.
    let denied = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, quota, 1 << 20).unwrap())
        .unwrap();
    let merge_started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = std::sync::Arc::clone(&merge_started);
    crate::local_primitives::native_relational_spill::BEFORE_RUN_OPEN.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_| {
            observed.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
    });
    let error = denied
        .collect_jsonl(&CancellationToken::default())
        .err()
        .expect("overlapping input/output runs must obey one disk quota")
        .to_string();
    assert!(error.contains("quota"), "{error}");
    assert!(merge_started.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_clean(&fixture);
}

#[test]
fn native_relational_spill_denial_cancellation_and_consumer_error_never_complete() {
    let (fixture, _) = fixture(20_013);
    let plan = sorted(fixture.scan(), vec![key("category", false)]);
    let denied = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 32 * 1024, 1 << 20).unwrap())
        .unwrap();
    let error = denied
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("quota"), "{error}");
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_clean(&fixture);
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
    let cancel = CancellationToken::default();
    cancel.cancel();
    assert!(
        prepared
            .for_each_batch(&cancel, |_, _| panic!("pre-cancelled consumer ran"))
            .is_err()
    );
    assert_clean(&fixture);
    let cancel = CancellationToken::default();
    let mut calls = 0;
    assert!(
        prepared
            .for_each_batch(&cancel, |_, _| {
                calls += 1;
                cancel.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(calls, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_clean(&fixture);
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Err(failed(
                "consumer rejected batch"
            )))
            .err()
            .unwrap()
            .to_string()
            .contains("consumer rejected batch")
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_clean(&fixture);
    let mut count = 0;
    let completed = prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            count += array.len();
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 20_013);
    assert_eq!(completed.runtime.completed_executions, 1);
    assert_clean(&fixture);
}

#[test]
fn native_relational_spill_configuration_and_unspilled_empty_work_are_inert() {
    let (fixture, _) = fixture(0);
    let plan = sorted(fixture.scan(), vec![key("category", false)]);
    let mut constrained = policy();
    constrained.resource_envelope.memory_budget_bytes = 8 << 20;
    let mut oversized = spill(&fixture);
    oversized.workspace.reserve(8 << 20);
    let error = prepare_relational(&plan, constrained)
        .unwrap()
        .with_spill(oversized)
        .err()
        .expect("the retained workspace capacity must use the same query grant")
        .to_string();
    assert!(error.contains("memory"), "{error}");
    assert_clean(&fixture);
    let absent = fixture.0.join("not-created");
    assert!(VortexRelationalSpillPolicy::new("relative", 1 << 20, 1 << 20).is_err());
    assert!(VortexRelationalSpillPolicy::new(&absent, 1, 1 << 20).is_err());
    assert!(VortexRelationalSpillPolicy::new(&absent, 1 << 20, 1).is_err());
    let declared = VortexRelationalSpillPolicy::new(&absent, 1 << 20, 1 << 20).unwrap();
    assert!(!absent.exists());
    let prepared = prepare_relational(
        &sorted(fixture.scan(), vec![key("category", false)]),
        policy(),
    )
    .unwrap()
    .with_spill(declared)
    .unwrap();
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&collected), [] as [serde_json::Value; 0]);
    assert_eq!(collected.execution.spill.as_ref().unwrap().runs_written, 0);
    assert!(
        !collected
            .execution
            .native_io_certificate
            .side_effects
            .spill_io_performed
    );
    assert!(!absent.exists());
    assert_clean(&fixture);
}

#[test]
fn native_relational_spill_streams_exact_rows_when_resident_sort_exceeds_query_budget() {
    let (fixture, mut expected) = fixture(240_003);
    sort_reference(&mut expected, &[("category", false), ("label", true)]);
    let plan = sorted(
        fixture.scan(),
        vec![key("category", false), key("label", true)],
    );
    let mut constrained = policy();
    constrained.resource_envelope.memory_budget_bytes = 8 << 20;
    let resident = prepare_relational(&plan, constrained).unwrap();
    let error = resident
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
        .err()
        .expect("the complete ordering state must exceed this fixed grant")
        .to_string();
    assert!(error.contains("memory"), "{error}");
    assert_eq!(resident.snapshot().completed_executions, 0);
    drop(resident);

    let prepared = prepare_relational(&plan, constrained)
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
    let before = prepared.snapshot().memory.reserved_bytes;
    let mut offset = 0;
    let execution = prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            // Hold each delivered batch across a slow-consumer boundary while
            // the same constrained grant still owns every native buffer.
            std::thread::sleep(std::time::Duration::from_millis(1));
            let identifiers = logical_field_from_native_array(&array, "identifier")?
                .execute::<PrimitiveArray>(&mut context.native_session().create_execution_ctx())
                .map_err(vortex_error)?;
            for identifier in identifiers.as_slice::<u32>() {
                assert_eq!(
                    u64::from(*identifier),
                    expected[offset]["identifier"].as_u64().unwrap()
                );
                offset += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(offset, expected.len());
    assert!(execution.spill.as_ref().unwrap().merge_passes > 1);
    assert!(execution.runtime.memory.peak_reserved_bytes <= 8 << 20);
    assert_clean(&fixture);
    drop(execution);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
}

#[test]
fn native_relational_spill_corrupt_truncated_and_replaced_runs_cannot_complete() {
    use crate::local_primitives::native_relational_spill::BEFORE_RUN_OPEN;
    for damage in ["corrupt", "truncate", "replace"] {
        let (fixture, _) = fixture(20_013);
        let prepared = prepare_relational(
            &sorted(fixture.scan(), vec![key("category", false)]),
            policy(),
        )
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let opened = std::rc::Rc::new(std::cell::RefCell::new(None));
        let captured = std::rc::Rc::clone(&opened);
        BEFORE_RUN_OPEN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |path| {
                *captured.borrow_mut() = Some(path.to_path_buf());
                match damage {
                    "corrupt" => {
                        use std::io::{Read as _, Seek as _, Write as _};
                        let mut file = fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path)
                            .unwrap();
                        let mut byte = [0];
                        file.read_exact(&mut byte).unwrap();
                        file.rewind().unwrap();
                        file.write_all(&[byte[0] ^ 1]).unwrap();
                    }
                    "truncate" => fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .unwrap()
                        .set_len(1)
                        .unwrap(),
                    "replace" => {
                        let replacement = path.with_extension("replacement");
                        fs::copy(path, &replacement).unwrap();
                        fs::rename(replacement, path).unwrap();
                    }
                    _ => unreachable!(),
                }
            }));
        });
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                panic!("damaged run reached the result consumer")
            })
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("changed"), "{damage}: {error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
        let path = opened.borrow().clone().unwrap();
        if damage == "replace" {
            assert!(path.exists(), "foreign replacement must remain intact");
            assert!(
                spill(&fixture)
                    .cleanup_abandoned(path.parent().unwrap())
                    .is_err()
            );
            assert!(path.exists());
        } else {
            assert_clean(&fixture);
        }
    }
}

#[test]
fn native_relational_spill_source_mutation_after_delivery_prevents_success() {
    let (fixture, _) = fixture(20_013);
    let prepared = prepare_relational(&sorted(fixture.scan(), vec![key("label", false)]), policy())
        .unwrap()
        .with_spill(spill(&fixture))
        .unwrap();
    let before = prepared.snapshot().memory.reserved_bytes;
    let mut calls = 0;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                calls += 1;
                if calls == 1 {
                    fixture.replace();
                }
                Ok(())
            })
            .is_err()
    );
    assert!(calls > 0);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    assert_clean(&fixture);
}

#[test]
fn native_relational_spill_oversized_row_and_missing_workspace_fail_explicitly() {
    let array = StructArray::try_new(
        FieldNames::from(["label"]),
        vec![VarBinViewArray::from_iter_str(["x".repeat(2 << 20)]).into_array()],
        1,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let oversized = Fixture::new(array, 1);
    let prepared = prepare_relational(
        &sorted(oversized.scan(), vec![key("label", false)]),
        policy(),
    )
    .unwrap()
    .with_spill(spill(&oversized))
    .unwrap();
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("one native ordering row exceeds"), "{error}");
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_clean(&oversized);

    let (fixture, _) = fixture(20_013);
    let absent = fixture.0.join("not-created");
    let prepared = prepare_relational(
        &sorted(fixture.scan(), vec![key("category", false)]),
        policy(),
    )
    .unwrap()
    .with_spill(VortexRelationalSpillPolicy::new(&absent, 64 << 20, 1 << 20).unwrap())
    .unwrap();
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .is_err()
    );
    assert!(!absent.exists());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_clean(&fixture);
}
