//! Real ordering/window pressure, fault cleanup and retry for temporal grids.

use super::*;
use crate::relational_query::{
    VortexRelationalFillMethod as Method, VortexRelationalGapFill, VortexRelationalOrderKey,
    VortexRelationalSpillPolicy, VortexRelationalWindow, VortexRelationalWindowExpression,
    VortexRelationalWindowFunction as Function,
};
use shardloom_core::temporal::{TemporalGridStep, TemporalUnit};
use std::{cell::Cell, rc::Rc};
use vortex::array::arrays::VarBinViewArray;

const ROWS: usize = 24_013;
const GRANT: u64 = 16 << 20;

fn text(index: usize) -> String {
    format!("{index:08}{}", "x".repeat(760))
}

fn large_fixture() -> Fixture {
    let batch = |start: usize, end: usize| {
        let indices: Vec<_> = (start..end).map(|row| ROWS - row - 1).collect();
        StructArray::new(
            FieldNames::from(["instant", "value", "pad"]),
            vec![
                temporal_array(
                    "timestamp(s)",
                    &indices
                        .iter()
                        .map(|&row| Some(i64::try_from(row * 4).unwrap()))
                        .collect::<Vec<_>>(),
                ),
                PrimitiveArray::from_option_iter(
                    indices
                        .iter()
                        .map(|&row| row.is_multiple_of(5).then_some(i64::try_from(row).unwrap())),
                )
                .into_array(),
                VarBinViewArray::from_iter_str(indices.into_iter().map(text)).into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .into_array()
    };
    let directory = std::env::temp_dir().join(format!(
        "shardloom-grid-pressure-{}-{}",
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
    assert!(fs::metadata(fixture.path()).unwrap().len() > GRANT);
    fixture
}

fn declaration(fixture: &Fixture, family: &str) -> VortexRelationalPlan {
    let input = if family == "fill" {
        fixture.scan()
    } else {
        VortexRelationalPlan::GapFill(Box::new(VortexRelationalGapFill {
            input: fixture.scan(),
            time: ColumnRef::new("instant").unwrap(),
            partition_by: vec![],
            step: TemporalGridStep::Duration {
                value: 2,
                unit: TemporalUnit::Seconds,
            },
            origin: None,
            start: None,
            end: None,
        }))
    };
    if family == "gap" {
        return input;
    }
    VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input,
        columns: ["instant", "pad"]
            .into_iter()
            .map(|name| ColumnRef::new(name).unwrap())
            .collect(),
        expressions: [
            ("forward", Method::Forward),
            ("backward", Method::Backward),
            ("linear", Method::Linear),
        ]
        .into_iter()
        .map(|(name, method)| VortexRelationalWindowExpression {
            output_column: name.into(),
            function: Function::Fill {
                column: ColumnRef::new("value").unwrap(),
                method,
            },
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("instant").unwrap(),
                descending: false,
                nulls: None,
            }],
            frame: None,
        })
        .collect(),
    }))
}

fn prepare(
    fixture: &Fixture,
    family: &str,
    grant: u64,
    quota: Option<u64>,
) -> PreparedVortexRelational {
    let mut settings = policy();
    settings.resource_envelope.memory_budget_bytes = grant;
    let prepared = prepare_relational(&declaration(fixture, family), settings).unwrap();
    if let Some(quota) = quota {
        let directory = fixture.0.join("grid-runs");
        fs::create_dir_all(&directory).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(directory, quota, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

#[allow(clippy::cast_precision_loss)] // Fixture coordinates stay below 100,000 and halves are exact.
fn execute(prepared: &PreparedVortexRelational, family: &str) -> Result<ExecutedVortexRelational> {
    let mut delivered = 0;
    let report =
        prepared.for_each_json_batch(&CancellationToken::default(), 509, 4 << 20, |batch| {
            for row in
                serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap()
            {
                let tick = if family == "fill" {
                    (ROWS - delivered - 1) * 4
                } else {
                    delivered * 2
                };
                let original = tick.is_multiple_of(4);
                let index = tick / 4;
                let mut expected =
                    json!({"instant":tick,"pad": if original { Some(text(index)) } else { None }});
                if family == "gap" {
                    expected["value"] = json!(if original && index.is_multiple_of(5) {
                        Some(index)
                    } else {
                        None
                    });
                } else {
                    let left = tick / 20 * 5;
                    let right = tick.div_ceil(20) * 5;
                    expected["forward"] = json!(left);
                    expected["backward"] = json!((right < ROWS).then_some(right));
                    expected["linear"] = json!((right < ROWS).then_some(tick as f64 / 4.0));
                }
                assert_eq!(row, expected, "family={family}, row={delivered}");
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(
        delivered,
        if family == "fill" { ROWS } else { 2 * ROWS - 1 }
    );
    assert_eq!(report.output_rows, delivered as u64);
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    Ok(report)
}

#[test]
fn native_temporal_grid_pressure_preserves_complete_rows_with_real_native_spill() {
    let fixture = large_fixture();
    for family in ["gap", "fill", "compose"] {
        let denied = prepare(&fixture, family, GRANT, None);
        let baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, family)
            .err()
            .expect("resident data exceeds grant");
        assert!(
            error.to_string().contains("reservation denied"),
            "{family}: {error}"
        );
        assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(denied.snapshot().completed_executions, 0);
        drop(denied);
        let ample = prepare(&fixture, family, 256 << 20, None);
        let baseline = ample.snapshot().memory.reserved_bytes;
        drop(execute(&ample, family).unwrap());
        assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
        drop(ample);
        let stored = prepare(&fixture, family, GRANT, Some(256 << 20));
        let baseline = stored.snapshot().memory.reserved_bytes;
        let report = execute(&stored, family).unwrap();
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
        eprintln!(
            "{}",
            json!({"family":family,"input_rows":ROWS,"output_rows":report.output_rows,"grant":GRANT,"peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(report);
        assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn native_temporal_grid_faults_release_actual_spill_and_allow_fresh_retry() {
    use crate::local_primitives::native_relational_window::fill::FILL_PROGRESS;
    let fixture = large_fixture();
    let stored = prepare(&fixture, "compose", GRANT, Some(256 << 20));
    let baseline = stored.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    let cancel = token.clone();
    let triggered = Rc::new(Cell::new(false));
    let recorded = Rc::clone(&triggered);
    FILL_PROGRESS.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |row| {
            if row < 1024 {
                return false;
            }
            recorded.set(true);
            cancel.cancel();
            true
        }));
    });
    let error = stored
        .for_each_batch(&token, |_, _| panic!("cancelled during fill"))
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(triggered.get());
    assert!(FILL_PROGRESS.with(|hook| hook.borrow().is_none()));
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(stored.snapshot().completed_executions, 0);
    assert_eq!(
        fs::read_dir(fixture.0.join("grid-runs")).unwrap().count(),
        0
    );
    for family in ["gap", "compose"] {
        let prepared = prepare(&fixture, family, GRANT, Some(256 << 20));
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for cancel_after_output in [false, true] {
            let token = CancellationToken::default();
            let mut calls = 0;
            let result = prepared.for_each_batch(&token, |_, _| {
                calls += 1;
                if cancel_after_output {
                    token.cancel();
                    Ok(())
                } else {
                    Err(shardloom_core::ShardLoomError::InvalidOperation(
                        "grid consumer refused".into(),
                    ))
                }
            });
            let error = result.err().unwrap();
            assert!(
                error.to_string().contains(if cancel_after_output {
                    "cancel"
                } else {
                    "grid consumer refused"
                }),
                "{error}"
            );
            assert_eq!(calls, 1);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(
                fs::read_dir(fixture.0.join("grid-runs")).unwrap().count(),
                0
            );
        }
        let denied = prepare(&fixture, family, GRANT, Some(32 << 10));
        let baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, family).err().expect("quota refusal");
        assert!(error.to_string().contains("quota"), "{error}");
        assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("grid-runs")).unwrap().count(),
            0
        );
    }
    drop(execute(&stored, "compose").unwrap());
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(
        fs::read_dir(fixture.0.join("grid-runs")).unwrap().count(),
        0
    );
}
