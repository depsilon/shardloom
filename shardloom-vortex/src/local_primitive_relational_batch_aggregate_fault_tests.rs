//! Faults after actual aggregate run creation preserve ownership and publication.

use super::*;
use crate::local_primitives::{
    VortexLocalPrimitiveRowExportFormat as Format,
    native_relational_spill::{AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN},
};
use std::{cell::RefCell, rc::Rc};

#[cfg(unix)]
#[path = "local_primitive_relational_batch_aggregate_recovery_tests.rs"]
mod recovery_tests;

const TOTAL: usize = 8 * 1024 + 7;
const GRANT: u64 = 16 << 20;

fn plan() -> VortexRelationalPlan {
    aggregate(
        scan(),
        &[],
        vec![
            measure("count", None, "rows"),
            measure("count_distinct", Some("s"), "distinct"),
            measure("sum", Some("n"), "sum"),
            measure("min", Some("n"), "min"),
            measure("max", Some("n"), "max"),
        ],
    )
}

fn expected(total: usize) -> serde_json::Value {
    json!({"rows":total,"distinct":total,"sum":f64::from(u32::try_from(total * (total - 1) / 2).unwrap()),"min":0,"max":total-1})
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

    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
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
        let numbers = (self.delivered..end)
            .map(|row| Some(i64::try_from(self.total - row - 1).unwrap()))
            .collect::<Vec<_>>();
        let strings = numbers
            .iter()
            .map(|number| format!("{:09}{}", number.unwrap(), "x".repeat(503)))
            .collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|text| Some(text.as_str()))
            .collect::<Vec<_>>();
        let source = source(session, &numbers, &refs)?;
        self.prior = Some(source.batch_release_witness()?);
        self.delivered = end;
        Ok(Some(source))
    }
}

fn runs(workspace: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(workspace)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| {
                        path.extension()
                            .is_some_and(|extension| extension == "vortex")
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect()
}

fn spill(workspace: &std::path::Path, quota: u64) -> VortexRelationalSpillPolicy {
    VortexRelationalSpillPolicy::new(workspace, quota, 1 << 20).unwrap()
}

fn hooks_consumed() {
    assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
}

#[test]
// This test covers one failure contract across shared multi-stage setup.
#[allow(clippy::too_many_lines)]
fn ordered_aggregate_spilled_faults_refund_state_and_never_report_completion() {
    for failure in [
        "producer",
        "schema",
        "input-cancel",
        "merge-cancel",
        "consumer-cancel",
        "consumer",
        "quota",
        "corrupt",
    ] {
        let fixture = workspace();
        let directory = fixture.0.join("runs");
        fs::create_dir(&directory).unwrap();
        let prepared = prepare(&plan(), GRANT)
            .unwrap()
            .with_spill(spill(
                &directory,
                if failure == "quota" {
                    32 << 10
                } else {
                    128 << 20
                },
            ))
            .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let triggered = Rc::new(Cell::new(0));
        if failure == "corrupt" {
            let triggered = Rc::clone(&triggered);
            BEFORE_RUN_OPEN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |run| {
                    assert!(fs::metadata(run).unwrap().len() > 1);
                    fs::OpenOptions::new()
                        .write(true)
                        .open(run)
                        .unwrap()
                        .set_len(1)
                        .unwrap();
                    triggered.set(1);
                }));
            });
        }
        if failure == "merge-cancel" {
            let triggered = Rc::clone(&triggered);
            let token = token.clone();
            AFTER_MERGE_BLOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |rows| {
                    assert!(rows > 0);
                    triggered.set(rows);
                    token.cancel();
                }));
            });
        }
        let mut input = Input::new(TOTAL);
        let mut provider = |session: &ResidentVortexSession| {
            if input.calls == 5 && matches!(failure, "producer" | "schema" | "input-cancel") {
                assert!(
                    !runs(&directory).is_empty(),
                    "failure must follow actual aggregate spill"
                );
                triggered.set(1);
                match failure {
                    "producer" => {
                        return Err(batch_input::failed("aggregate producer after spill"));
                    }
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
                    _ => unreachable!(),
                }
            }
            input.next(session)
        };
        let mut emitted = 0;
        let result = prepared
            .with_batch_input(&mut provider)
            .unwrap()
            .for_each_batch(&token, |array, context| {
                assert_eq!(values(&array, context)?, vec![expected(TOTAL)]);
                emitted += 1;
                if failure == "consumer-cancel" {
                    token.cancel();
                }
                if failure == "consumer" {
                    return Err(batch_input::failed("aggregate consumer refused"));
                }
                Ok(())
            });
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{failure} completed"))
            .to_string();
        assert!(
            error.contains(match failure {
                "producer" => "aggregate producer after spill",
                "schema" => "declared",
                "quota" => "quota",
                "consumer" => "aggregate consumer refused",
                "corrupt" => "changed",
                _ => "cancel",
            }),
            "{failure}: {error}"
        );
        assert_eq!(
            emitted,
            usize::from(matches!(failure, "consumer" | "consumer-cancel"))
        );
        if matches!(
            failure,
            "producer" | "schema" | "input-cancel" | "merge-cancel" | "corrupt"
        ) {
            assert!(triggered.get() > 0);
        }
        hooks_consumed();
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
    }
}

fn complete(prepared: &PreparedVortexRelational, total: usize) -> ExecutedVortexRelational {
    let mut input = Input::new(total);
    let mut provider = |session: &ResidentVortexSession| input.next(session);
    let result = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![expected(total)]);
    assert_eq!(input.calls, total.div_ceil(1024) + 1);
    let report = result.execution.spill.as_ref().unwrap();
    assert!(report.runs_written > 3 && report.merge_passes > 1 && report.owned_cleanup_completed);
    assert!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    assert_eq!(result.execution.ordered_aggregate_input_rows, total as u64);
    result.execution
}

#[test]
// This test covers one publication contract across shared multi-stage setup.
#[allow(clippy::too_many_lines)]
fn ordered_aggregate_spilled_write_reopens_and_refuses_partial_publication() {
    let fixture = workspace();
    let prepared = prepared(&plan(), &fixture, true, GRANT);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let output = fixture.0.join("complete.vortex");
    let mut input = Input::new(TOTAL);
    let mut provider = |session: &ResidentVortexSession| input.next(session);
    let result = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .write_controlled(
            &output,
            Format::Vortex,
            false,
            &CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(result.output.rows_written, 1);
    assert!(result.execution.spill.as_ref().unwrap().runs_written > 3);
    assert!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    drop(result);
    let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(output.display().to_string()).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    });
    let reopened = prepare_relational(&scan, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&reopened), vec![expected(TOTAL)]);
    let original = fs::read(&output).unwrap();
    let mut unopened = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("existing destination must be rejected before demand")
    };
    let error = prepared
        .with_batch_input(&mut unopened)
        .unwrap()
        .write_controlled(&output, Format::Vortex, true, &CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("atomic generation-conditional replacement is unavailable"),
        "{error}"
    );
    assert_eq!(fs::read(&output).unwrap(), original);

    for failure in ["producer", "cleanup"] {
        let target = fixture.0.join(format!("failed-{failure}.vortex"));
        let foreign = Rc::new(RefCell::new(None));
        if failure == "cleanup" {
            let foreign = Rc::clone(&foreign);
            BEFORE_RUN_OPEN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |run| {
                    let path = run.parent().unwrap().join("foreign");
                    fs::write(&path, b"keep unknown bytes").unwrap();
                    *foreign.borrow_mut() = Some(path);
                }));
            });
        }
        let mut input = Input::new(TOTAL);
        let mut provider = |session: &ResidentVortexSession| {
            if failure == "producer" && input.calls == 5 {
                assert_ne!(runs(&fixture.0.join("runs")).len(), 0);
                return Err(batch_input::failed("aggregate writer after spill"));
            }
            input.next(session)
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
            .expect("failed aggregation must not publish")
            .to_string();
        if failure == "producer" {
            assert!(error.contains("aggregate writer after spill"), "{error}");
        }
        if failure == "cleanup" {
            let path = foreign
                .borrow()
                .clone()
                .expect("actual aggregate run hook must fire");
            assert_eq!(fs::read(&path).unwrap(), b"keep unknown bytes");
            fs::remove_file(&path).unwrap();
            fs::remove_dir(path.parent().unwrap()).unwrap();
        }
        assert!(!target.exists());
        assert_eq!(fs::read(&output).unwrap(), original);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
        hooks_consumed();
    }
}
