//! Calendar endpoint retreats through real external ordering and shared leases.

use super::*;
use crate::local_primitives::native_relational_window::spill::{Progress, WINDOW_PROGRESS};
use std::{cell::Cell, rc::Rc};

const ROWS: usize = 24_013;
const GRANT: u64 = 16 << 20;

fn slot(row: usize) -> usize {
    row * 7 % FOLD.len()
}
fn value(row: usize) -> Option<i64> {
    (!row.is_multiple_of(17)).then_some(i64::try_from(row % 23).unwrap() - 11)
}
fn text(row: usize) -> String {
    format!("{row:08}{}", "x".repeat(760))
}

fn large_fixture() -> Fixture {
    let DType::Extension(dtype) = crate::native_temporal::vortex_dtype(
        &TemporalDType::parse("timestamp(s,America/New_York)").unwrap(),
        Nullability::NonNullable,
    )
    .unwrap() else {
        panic!("temporal domain")
    };
    let batch = |start: usize, end: usize| {
        StructArray::new(
            FieldNames::from(["id", "time", "value", "pad"]),
            vec![
                PrimitiveArray::from_iter(start as u64..end as u64).into_array(),
                ExtensionArray::try_new(
                    dtype.clone(),
                    PrimitiveArray::from_iter((start..end).map(|row| FOLD[slot(row)].0))
                        .into_array(),
                )
                .unwrap()
                .into_array(),
                PrimitiveArray::from_option_iter((start..end).map(value)).into_array(),
                VarBinViewArray::from_iter_str((start..end).map(text)).into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .into_array()
    };
    let directory = std::env::temp_dir().join(format!(
        "shardloom-calendar-pressure-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    crate::native_temporal::register(&session);
    let mut file = fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                ROWS.div_ceil(997),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, batch(0, 0).dtype().clone());
    for start in (0..ROWS).step_by(997) {
        writer.push(batch(start, (start + 997).min(ROWS))).unwrap();
    }
    writer.finish().unwrap();
    let bytes = fs::metadata(fixture.path()).unwrap().len();
    assert!(bytes > GRANT && bytes < 32 << 20, "fixture bytes={bytes}");
    fixture
}

fn calendar_plan(fixture: &Fixture) -> VortexRelationalPlan {
    let mut plan = plan(
        fixture.scan(),
        &Offset::CalendarDays {
            days: 1,
            ambiguity: Ambiguity::Earlier,
        },
        Exclusion::NoOthers,
        false,
        NullOrder::Last,
        true,
    );
    let VortexRelationalPlan::Window(window) = &mut plan else {
        unreachable!()
    };
    window.columns = vec![column("id"), column("pad")];
    window.expressions.retain(|expression| {
        ["n", "distinct", "sum", "min", "max"].contains(&expression.output_column.as_str())
    });
    for expression in &mut window.expressions {
        expression.partition_by.clear();
    }
    plan
}

fn prepare(fixture: &Fixture, grant: u64, quota: Option<u64>) -> PreparedVortexRelational {
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = grant;
    let prepared = prepare_relational(&calendar_plan(fixture), policy).unwrap();
    if let Some(quota) = quota {
        let directory = fixture.0.join("window-runs");
        fs::create_dir_all(&directory).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(directory, quota, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

#[allow(clippy::cast_precision_loss)] // Small integer population totals are exactly representable.
fn expected() -> Vec<Value> {
    FOLD.iter().map(|&(_, lower, upper)| {
        let members: Vec<_> = (0..ROWS).filter(|&row| (lower..=upper).contains(&FOLD[slot(row)].0)).collect();
        let mut values: Vec<_> = members.iter().filter_map(|&row| value(row)).collect();
        let sum = values.iter().sum::<i64>();
        values.sort_unstable();
        values.dedup();
        json!({"n":members.len(),"distinct":values.len(),"min":values.first(),"max":values.last(),"sum":sum as f64})
    }).collect()
}

fn execute(prepared: &PreparedVortexRelational) -> Result<ExecutedVortexRelational> {
    let reference = expected();
    let mut delivered = 0;
    let report =
        prepared.for_each_json_batch(&CancellationToken::default(), 509, 4 << 20, |batch| {
            for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                let mut expected = reference[slot(delivered)].clone();
                expected["id"] = json!(delivered);
                expected["pad"] = json!(text(delivered));
                assert_eq!(row, expected, "original ordinal={delivered}");
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(delivered, ROWS);
    assert_eq!(report.output_rows, ROWS as u64);
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    Ok(report)
}

#[test]
fn calendar_window_pressure_refuses_resident_and_completes_retreating_spill_bounds() {
    let fixture = large_fixture();
    let denied = prepare(&fixture, GRANT, None);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied)
        .err()
        .expect("resident partition exceeds its grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(denied.snapshot().completed_executions, 0);
    drop(denied);
    let ample = prepare(&fixture, 128 << 20, None);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = execute(&ample).unwrap();
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    assert_eq!(control.ordered_window_stages, 0);
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);
    let stored = prepare(&fixture, GRANT, Some(256 << 20));
    let baseline = stored.snapshot().memory.reserved_bytes;
    let report = execute(&stored).unwrap();
    let spill = report.spill.as_ref().unwrap();
    assert_eq!(report.ordered_window_stages, 1);
    assert_eq!(report.ordered_window_input_rows, ROWS as u64);
    assert!(report.ordered_window_bounds_rows >= ROWS as u64);
    assert!(report.ordered_window_distinct_intervals > 0);
    assert!(report.ordered_window_extrema_summary_rows > 0);
    assert!(spill.runs_written > 3 && spill.merge_passes > 1);
    assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    eprintln!(
        "{}",
        json!({"family":"calendar_window_fold_retreats","input_rows":ROWS,"grant":GRANT,
        "spilled_peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
        "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
        "complete_output_rows":report.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
    );
    drop(report);
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn calendar_window_failures_release_actual_spill_and_reusable_prepared_owners() {
    let fixture = large_fixture();
    let stored = prepare(&fixture, GRANT, Some(256 << 20));
    let baseline = stored.snapshot().memory.reserved_bytes;
    for target in [
        Progress::Bounds,
        Progress::DistinctIntervals,
        Progress::ExtremaSummaries,
    ] {
        let token = CancellationToken::default();
        let triggered = Rc::new(Cell::new(false));
        let recorded = Rc::clone(&triggered);
        let cancel = token.clone();
        WINDOW_PROGRESS.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase, rows| {
                if phase != target || rows == 0 {
                    return false;
                }
                recorded.set(true);
                cancel.cancel();
                true
            }));
        });
        let error = stored
            .for_each_batch(&token, |_, _| panic!("cancelled before output"))
            .err()
            .unwrap();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(triggered.get());
        assert!(WINDOW_PROGRESS.with(|hook| hook.borrow().is_none()));
        assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(stored.snapshot().completed_executions, 0);
        assert_eq!(
            fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
            0
        );
    }
    let error = stored
        .for_each_batch(&CancellationToken::default(), |_, _| {
            Err(shardloom_core::ShardLoomError::InvalidOperation(
                "calendar output failure".into(),
            ))
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("calendar output failure"));
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(stored.snapshot().completed_executions, 0);
    let denied = prepare(&fixture, GRANT, Some(32 << 10));
    let denied_baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied).err().expect("quota denial");
    assert!(error.to_string().contains("quota"), "{error}");
    assert_eq!(denied.snapshot().memory.reserved_bytes, denied_baseline);
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_eq!(
        fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
        0
    );
    // The same prepared owner can complete after cancellation and output errors.
    drop(execute(&stored).unwrap());
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(
        fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
        0
    );
}
