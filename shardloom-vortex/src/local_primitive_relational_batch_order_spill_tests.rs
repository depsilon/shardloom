//! Streamed order pressure and publication use the existing native run store.

use super::*;
use crate::{
    local_primitives::{
        VortexLocalPrimitiveRowExportFormat as Format, native_relational_spill::BEFORE_RUN_OPEN,
    },
    relational_query::VortexRelationalSpillPolicy,
};
use std::{cell::RefCell, rc::Rc};

#[cfg(unix)]
#[path = "local_primitive_relational_batch_order_recovery_tests.rs"]
mod recovery_tests;

#[test]
fn streamed_ordering_spill_preserves_multikey_ties_and_nested_stage_order() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let rows = fixture_rows()
        .into_iter()
        .cycle()
        .take(24_019)
        .enumerate()
        .map(|(id, mut row)| {
            row.id = i64::try_from(id).unwrap();
            row
        })
        .collect::<Vec<_>>();
    for nested in [false, true] {
        let keys = vec![
            key("n", true, NullOrder::Last),
            key("s", false, NullOrder::First),
        ];
        let mut expected = rows.clone();
        let plan = if nested {
            reference_sort(&mut expected, &keys[1..]);
            reference_sort(&mut expected, &keys[..1]);
            sorted(sorted(scan(), keys[1..].to_vec()), keys[..1].to_vec())
        } else {
            reference_sort(&mut expected, &keys);
            sorted(scan(), keys)
        };
        let prepared = prepare_rows(&plan)
            .with_spill(spill(&fixture, 64 << 20))
            .unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let mut chunks = rows.chunks(997);
        let mut calls = 0;
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut provider = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            calls += 1;
            let Some(rows) = chunks.next() else {
                return Ok(None);
            };
            let batch = rows_source(session, rows)?;
            prior = Some(batch.batch_release_witness()?);
            Ok(Some(batch))
        };
        let result = prepared
            .with_batch_input(&mut provider)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected.iter().map(Row::json).collect::<Vec<_>>()
        );
        assert_eq!(calls, rows.len().div_ceil(997) + 1);
        let input = result.execution.input.as_ref().unwrap();
        assert!(input.end_of_input_observed);
        assert_eq!(
            input.ordering_rows_detached,
            rows.len() as u64 * if nested { 2 } else { 1 }
        );
        let spill = result.execution.spill.as_ref().unwrap();
        assert!(spill.merge_passes >= if nested { 2 } else { 1 });
        assert!(spill.max_open_runs >= if nested { 3 } else { 2 });
        assert!(spill.owned_cleanup_completed);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        assert_names(&fixture, &["input.vortex"]);
    }
}

struct Input {
    total: usize,
    delivered: usize,
    calls: usize,
    prior: Option<Weak<MemoryLease>>,
}

impl Input {
    fn new(total: usize) -> Self {
        Self {
            total,
            delivered: 0,
            calls: 0,
            prior: None,
        }
    }

    fn next(
        &mut self,
        session: &ResidentVortexSession,
        text: &str,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        self.calls += 1;
        if self.delivered == self.total {
            return Ok(None);
        }
        let end = (self.delivered + 1024).min(self.total);
        let values = (self.delivered..end)
            .map(|row| Some(i64::try_from(self.total - row - 1).unwrap()))
            .collect::<Vec<_>>();
        let source = source(session, &values, &vec![Some(text); values.len()])?;
        self.prior = Some(source.batch_release_witness()?);
        self.delivered = end;
        Ok(Some(source))
    }
}

fn order_plan() -> VortexRelationalPlan {
    sorted(scan(), vec![key("n", false, NullOrder::Last)])
}

fn spill(fixture: &Fixture, quota: u64) -> VortexRelationalSpillPolicy {
    VortexRelationalSpillPolicy::new(&fixture.0, quota, 1 << 20).unwrap()
}

fn assert_names(fixture: &Fixture, expected: &[&str]) {
    let mut names = fs::read_dir(&fixture.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, expected);
}

fn check_batch(
    batch: &crate::local_primitives::collect::SerializedVortexResultBatch,
    offset: &mut usize,
    text: &str,
) {
    let rows = serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap();
    assert_eq!(rows.len(), batch.rows);
    for row in rows {
        assert_eq!(row, serde_json::json!({"n":*offset,"s":text}));
        *offset += 1;
    }
}

fn complete(
    prepared: &PreparedVortexRelational,
    total: usize,
    text: &str,
    slow: bool,
) -> ExecutedVortexRelational {
    let mut input = Input::new(total);
    let mut provider = |session: &ResidentVortexSession| input.next(session, text);
    let mut offset = 0;
    let execution = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 511, 1 << 20, |batch| {
            if slow {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            check_batch(&batch, &mut offset, text);
            Ok(())
        })
        .unwrap();
    assert_eq!(offset, total);
    assert_eq!(input.calls, total.div_ceil(1024) + 1);
    let report = execution.input.as_ref().unwrap();
    assert_eq!(report.rows, total as u64);
    assert_eq!(report.ordering_rows_detached, total as u64);
    assert_eq!(report.max_retained_input_batches, 1);
    assert!(report.end_of_input_observed);
    execution
}

#[test]
fn streamed_ordering_larger_than_grant_denies_resident_and_completes_with_spill_or_ample_memory() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let total = 65 * 1024 + 7;
    let text = "λ".repeat(256);
    let constrained = prepare(&order_plan(), 8 << 20).unwrap();
    let before = constrained.snapshot().memory.reserved_bytes;
    let mut input = Input::new(total);
    let mut provider = |session: &ResidentVortexSession| input.next(session, &text);
    let error = constrained
        .with_batch_input(&mut provider)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("resident sort must exceed its grant")
        })
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("memory"), "{error}");
    assert!(input.delivered < total);
    assert_eq!(constrained.snapshot().completed_executions, 0);
    assert_eq!(constrained.snapshot().memory.reserved_bytes, before);
    drop(constrained);
    assert_names(&fixture, &["input.vortex"]);

    let spilled = prepare(&order_plan(), 8 << 20)
        .unwrap()
        .with_spill(spill(&fixture, 128 << 20))
        .unwrap();
    let before = spilled.snapshot().memory.reserved_bytes;
    let execution = complete(&spilled, total, &text, true);
    assert!(execution.input.as_ref().unwrap().input_logical_bytes > 4 * (8 << 20));
    assert!(execution.runtime.memory.peak_reserved_bytes <= 8 << 20);
    let report = execution.spill.as_ref().unwrap();
    assert!(report.runs_written > 3 && report.merge_passes > 1);
    assert_eq!(report.max_open_runs, 2);
    assert!(report.peak_disk_bytes <= report.quota_bytes && report.owned_cleanup_completed);
    assert!(
        execution
            .native_io_certificate
            .side_effects
            .spill_io_performed
    );
    assert!(
        !execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    assert_names(&fixture, &["input.vortex"]);
    drop(execution);
    assert_eq!(spilled.snapshot().memory.reserved_bytes, before);
    drop(spilled);

    let ample = prepare(&order_plan(), 128 << 20).unwrap();
    let before = ample.snapshot().memory.reserved_bytes;
    let execution = complete(&ample, total, &text, false);
    assert!(execution.spill.is_none());
    assert!(
        !execution
            .native_io_certificate
            .side_effects
            .spill_io_performed
    );
    assert_names(&fixture, &["input.vortex"]);
    drop(execution);
    assert_eq!(ample.snapshot().memory.reserved_bytes, before);
}

fn corrupt_next_run() -> Rc<RefCell<Option<PathBuf>>> {
    let damaged = Rc::new(RefCell::new(None));
    let captured = Rc::clone(&damaged);
    BEFORE_RUN_OPEN.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |run| {
            fs::OpenOptions::new()
                .write(true)
                .open(run)
                .unwrap()
                .set_len(1)
                .unwrap();
            *captured.borrow_mut() = Some(run.to_path_buf());
        }));
    });
    damaged
}

#[test]
fn streamed_ordering_spill_failure_cancel_and_consumer_refusal_release_all_owned_state() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let text = "λ".repeat(256);
    for failure in [
        "producer",
        "schema",
        "pre-cancel",
        "input-cancel",
        "consumer-cancel",
        "consumer",
        "quota",
        "corrupt",
    ] {
        let prepared = prepare(&order_plan(), 8 << 20)
            .unwrap()
            .with_spill(spill(
                &fixture,
                if failure == "quota" {
                    32 * 1024
                } else {
                    64 << 20
                },
            ))
            .unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        if failure == "pre-cancel" {
            token.cancel();
        }
        let damaged = (failure == "corrupt").then(corrupt_next_run);
        let mut input = Input::new(8 * 1024 + 7);
        let mut provider = |session: &ResidentVortexSession| {
            if input.calls == 5 {
                match failure {
                    "producer" => return Err(batch_input::failed("late spilled input failure")),
                    "schema" => {
                        return Ok(Some(ResidentMemorySource::from_batch_columns(
                            session,
                            &[MemoryColumn {
                                name: "wrong",
                                values: MemoryColumnValues::Int64(&[Some(1)]),
                            }],
                        )?));
                    }
                    "input-cancel" => token.cancel(),
                    _ => {}
                }
            }
            input.next(session, &text)
        };
        let mut emitted = 0;
        let result = prepared
            .with_batch_input(&mut provider)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                emitted += 1;
                if failure == "consumer-cancel" {
                    token.cancel();
                }
                if failure == "consumer" {
                    return Err(batch_input::failed("spilled consumer refused"));
                }
                Ok(())
            });
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{failure} completed"))
            .to_string();
        assert!(
            error.contains(match failure {
                "producer" => "late spilled input failure",
                "schema" => "declared",
                "quota" => "quota",
                "consumer" => "spilled consumer refused",
                "corrupt" => "changed",
                _ => "cancel",
            }),
            "{failure}: {error}"
        );
        assert_eq!(
            emitted,
            usize::from(matches!(failure, "consumer" | "consumer-cancel"))
        );
        if failure == "pre-cancel" {
            assert_eq!(input.calls, 0);
        }
        if let Some(damaged) = damaged {
            assert!(damaged.borrow().is_some());
        }
        assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        assert_names(&fixture, &["input.vortex"]);
    }
}

fn verify_reopened(output: &std::path::Path, total: usize, text: &str) {
    let reopened = prepare_relational(
        &VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(output.display().to_string()).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        policy(),
    )
    .unwrap();
    let mut offset = 0;
    reopened
        .for_each_json_batch(&CancellationToken::default(), 511, 1 << 20, |batch| {
            check_batch(&batch, &mut offset, text);
            Ok(())
        })
        .unwrap();
    assert_eq!(offset, total);
}

fn refuse_existing_before_demand(prepared: &PreparedVortexRelational, output: &std::path::Path) {
    let mut calls = 0;
    let mut provider = |_: &ResidentVortexSession| {
        calls += 1;
        Ok(None)
    };
    let error = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .write_controlled(output, Format::Vortex, true, &CancellationToken::default())
        .err()
        .expect("existing native sink refuses replacement before input demand")
        .to_string();
    assert!(
        error.contains("atomic generation-conditional replacement is unavailable"),
        "{error}"
    );
    assert_eq!(calls, 0);
}

#[test]
fn streamed_ordering_native_write_reopens_values_and_publication_failures_preserve_destinations() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let output = fixture.0.join("ordered.vortex");
    let text = "λ".repeat(128);
    let total = 12 * 1024 + 7;
    let prepared = prepare(&order_plan(), 8 << 20)
        .unwrap()
        .with_spill(spill(&fixture, 64 << 20))
        .unwrap();
    let before = prepared.snapshot().memory.reserved_bytes;
    let mut input = Input::new(total);
    let mut provider = |session: &ResidentVortexSession| input.next(session, &text);
    let written = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .write_controlled(
            &output,
            Format::Vortex,
            false,
            &CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(input.calls, total.div_ceil(1024) + 1);
    assert_eq!(written.output.rows_written, total as u64);
    assert!(
        written
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    assert!(written.execution.spill.as_ref().unwrap().merge_passes > 1);
    drop(written);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    assert_names(&fixture, &["input.vortex", "ordered.vortex"]);
    let original = fs::read(&output).unwrap();
    verify_reopened(&output, total, &text);

    refuse_existing_before_demand(&prepared, &output);
    assert_eq!(fs::read(&output).unwrap(), original);

    for failure in ["producer", "cleanup"] {
        let target = fixture.0.join(format!("failed-{failure}.vortex"));
        let foreign = Rc::new(RefCell::new(None));
        if failure == "cleanup" {
            let foreign = Rc::clone(&foreign);
            BEFORE_RUN_OPEN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |run| {
                    let path = run.parent().unwrap().join("unowned-entry");
                    fs::write(&path, b"preserve foreign file").unwrap();
                    *foreign.borrow_mut() = Some(path);
                }));
            });
        }
        let mut input = Input::new(total);
        let mut provider = |session: &ResidentVortexSession| {
            if failure == "producer" && input.calls == 6 {
                return Err(batch_input::failed("writer producer failed"));
            }
            input.next(session, &text)
        };
        let error = prepared
            .with_batch_input(&mut provider)
            .unwrap()
            .write_controlled(
                &target,
                Format::Vortex,
                false,
                &CancellationToken::default(),
            )
            .err()
            .expect("failed execution must not publish")
            .to_string();
        if failure == "producer" {
            assert!(error.contains("writer producer failed"), "{error}");
        }
        if failure == "cleanup" {
            let foreign = foreign.borrow().clone().expect("the run hook must execute");
            assert_eq!(fs::read(&foreign).unwrap(), b"preserve foreign file");
            // This file belongs to the test; production cleanup must preserve it.
            fs::remove_file(&foreign).unwrap();
            fs::remove_dir(foreign.parent().unwrap()).unwrap();
        }
        assert_eq!(fs::read(&output).unwrap(), original);
        assert!(!target.exists());
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
        assert_names(&fixture, &["input.vortex", "ordered.vortex"]);
    }
}
