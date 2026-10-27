//! Source replay, completed pivot state and predicate membership have separate owners.

use super::*;
use crate::relational_query::VortexRelationalAggregate;

const ROWS: usize = 1025;
const BATCH: usize = 127;

fn wide_source(
    session: &ResidentVortexSession,
    start: usize,
    end: usize,
    width: usize,
) -> Result<ResidentMemorySource> {
    let strings = (start..end)
        .map(|row| format!("{row:08}{}", "p".repeat(width)))
        .collect::<Vec<_>>();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "entity",
                values: MemoryColumnValues::Utf8(
                    &strings
                        .iter()
                        .map(|text| Some(text.as_str()))
                        .collect::<Vec<_>>(),
                ),
            },
            MemoryColumn {
                name: "category",
                values: MemoryColumnValues::Utf8(&vec![Some("a"); end - start]),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Int64(&vec![Some(1); end - start]),
            },
        ],
    )
}

fn prepare_lower(
    grant: u64,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
) -> PreparedVortexRelational {
    let uris = URIS
        .iter()
        .map(|uri| DatasetUri::new(*uri).unwrap())
        .collect::<Vec<_>>();
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = grant;
    prepare_relational_with_dynamic_inputs(
        &uris,
        policy,
        65_536,
        |schemas| {
            schemas
                .register_batch_source(uris[0].clone(), |session| source(session, &[], &[], &[]))?;
            schemas
                .register_batch_source(uris[1].clone(), |session| wide_source(session, 0, 0, 0))?;
            Ok(())
        },
        lower,
    )
    .unwrap()
}

fn prepare(grant: u64, pivot_inner: bool) -> PreparedVortexRelational {
    prepare_lower(grant, move |schemas| {
        let relation = schemas.defer_subquery_with_sources(
            &[DatasetUri::new(URIS[1])?],
            65_536,
            move |schemas| {
                if !pivot_inner {
                    return Ok(scan(1));
                }
                let aggregate =
                    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                        input: pivot(scan(1)),
                        group_by: vec![],
                        grouping_sets: None,
                        measures: vec![crate::VortexSimpleAggregateMeasure::new(
                            "count",
                            None,
                            "count".into(),
                        )],
                    }));
                schemas.resolve_output(&aggregate).map(|(plan, _)| plan)
            },
        )?;
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
            relation,
            Kind::In {
                columns: vec![VortexRelationalJoinKey {
                    left: ColumnRef::new(if pivot_inner { "entity" } else { "category" })?,
                    right: ColumnRef::new(if pivot_inner { "count" } else { "entity" })?,
                }],
            },
        ))))
    })
}

fn prepared(
    grant: u64,
    pivot_inner: bool,
    spilling: bool,
    directory: &std::path::Path,
) -> PreparedVortexRelational {
    let prepared = prepare(grant, pivot_inner);
    if spilling {
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

struct Producer {
    rows: usize,
    width: usize,
    calls: [u64; 2],
    prior: Option<Weak<MemoryLease>>,
}

impl Producer {
    fn new(rows: usize, width: usize) -> Self {
        Self {
            rows,
            width,
            calls: [0; 2],
            prior: None,
        }
    }

    fn next(
        &mut self,
        uri: &DatasetUri,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        let index = usize::from(uri.as_str() == URIS[1]);
        assert_eq!(ordinal, self.calls[index]);
        self.calls[index] += 1;
        let batch = if index == 0 {
            if ordinal == 0 {
                Some(source(
                    session,
                    &[Some(i64::try_from(self.rows).unwrap()), Some(0)],
                    &[Some("a"), Some("a")],
                    &[Some(1), Some(1)],
                )?)
            } else {
                assert_eq!(ordinal, 1);
                None
            }
        } else {
            let start = usize::try_from(ordinal).unwrap() * BATCH;
            if start < self.rows {
                Some(wide_source(
                    session,
                    start,
                    (start + BATCH).min(self.rows),
                    self.width,
                )?)
            } else {
                assert_eq!(ordinal, self.rows.div_ceil(BATCH) as u64);
                None
            }
        };
        self.prior = batch
            .as_ref()
            .map(ResidentMemorySource::batch_release_witness)
            .transpose()?;
        Ok(batch)
    }
}

fn expected(rows: usize, membership: bool) -> Vec<Value> {
    vec![
        json!({"entity":rows,"category":"a","amount":1,"answer":membership}),
        json!({"entity":0,"category":"a","amount":1,"answer":false}),
    ]
}

fn live_runs(directory: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
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
                vec![]
            }
        })
        .collect()
}

#[test]
fn streaming_dynamic_subqueries_replay_and_pivot_spill_report_distinct_state_and_clean_reuse() {
    let fixture = fixture();
    let directory = fixture.0.join("deferred-pressure");
    fs::create_dir(&directory).unwrap();
    for (grant, spilling) in [(32 << 20, false), (128 << 20, false), (32 << 20, true)] {
        let prepared = prepared(grant, true, spilling, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let mut producer = Producer::new(ROWS, 2048);
            let result = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| {
                    producer.next(uri, ordinal, session)
                })
                .unwrap()
                .collect_jsonl(&CancellationToken::default());
            if !spilling && grant == 32 << 20 {
                let error = result
                    .err()
                    .expect("combined resident replay and pivot exceed this grant")
                    .to_string();
                assert!(error.contains("reservation denied"), "{error}");
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert!(
                    producer
                        .prior
                        .as_ref()
                        .is_none_or(|owner| owner.strong_count() == 0)
                );
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
                continue;
            }
            let result = result.unwrap();
            assert_eq!(json_rows(&result), expected(ROWS, true));
            assert_eq!(producer.calls, [2, (ROWS.div_ceil(BATCH) + 1) as u64]);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.rows, (ROWS + 2) as u64);
            assert_eq!(input.replay_sources, 1);
            assert_eq!(input.replay_prefilled_sources, 0);
            assert_eq!(input.replay_passes, 1);
            assert_eq!(input.replay_rows, ROWS as u64);
            assert_eq!(input.replay_spilled_sources, u64::from(spilling));
            assert_eq!(result.execution.dynamic_schema_stages, 2);
            assert_eq!(
                result.execution.spilled_pivot_stages,
                if spilling { 2 } else { 0 }
            );
            assert_eq!(
                result.execution.spilled_pivot_input_rows,
                if spilling { (ROWS * 2) as u64 } else { 0 }
            );
            assert_eq!(
                result.execution.spilled_pivot_index_rows,
                if spilling { (ROWS * 2) as u64 } else { 0 }
            );
            if spilling {
                assert!(
                    result
                        .execution
                        .spill
                        .as_ref()
                        .unwrap()
                        .owned_cleanup_completed
                );
            }
            assert_eq!(result.execution.runtime.completed_executions, execution);
            assert!(result.execution.runtime.memory.peak_reserved_bytes <= grant);
            assert!(
                producer
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        }
    }
}

#[test]
fn streaming_dynamic_subqueries_membership_original_pressure_spills_and_releases_each_parameter() {
    let fixture = fixture();
    let directory = fixture.0.join("deferred-membership");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    for spilling in [false, true] {
        let prepared = prepared(4 << 20, false, spilling, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut producer = Producer::new(4097, 2048);
        let mut saw_spill = false;
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                saw_spill |= !live_runs(&directory).is_empty();
                producer.next(uri, ordinal, session)
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default());
        if spilling {
            let result = result.unwrap();
            assert_eq!(json_rows(&result), expected(4097, false));
            assert_eq!(producer.calls, [2, (4097_usize.div_ceil(BATCH) + 1) as u64]);
            assert_eq!(result.execution.spillable_subquery_stages, 2);
            assert_eq!(result.execution.spillable_subquery_input_rows, 2 * 4097);
            assert_eq!(result.execution.spillable_subquery_probe_rows, 2);
            assert_eq!(result.execution.input.as_ref().unwrap().replay_sources, 1);
            assert_eq!(result.execution.input.as_ref().unwrap().replay_passes, 1);
            let spill = result.execution.spill.as_ref().unwrap();
            assert!(spill.runs_written > 2 && spill.merge_passes > 0);
            assert!(spill.owned_cleanup_completed);
            assert!(result.execution.runtime.memory.peak_reserved_bytes <= 4 << 20);
        } else {
            let error = result.err().unwrap().to_string();
            assert!(error.contains("reservation denied"), "{error}");
            assert_eq!(producer.calls[0], 1);
            assert!(producer.calls[1] > 1 && producer.calls[1] < 4097_usize.div_ceil(BATCH) as u64);
        }
        assert_eq!(saw_spill, spilling);
        assert!(
            producer
                .prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        assert_eq!(
            prepared.snapshot().completed_executions,
            u64::from(spilling)
        );
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
        let mut producer = Producer::new(2, 8);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| producer.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected(2, false));
        assert_eq!(
            result.execution.runtime.completed_executions,
            1 + u64::from(spilling)
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_failures_after_real_spill_discard_runs_and_new_destination() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex;
    let fixture = fixture();
    let directory = fixture.0.join("deferred-spill-faults");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    let target = fixture.0.join("new.vortex");
    for cancellation in [false, true] {
        let prepared = prepared(32 << 20, true, true, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut producer = Producer::new(ROWS, 2048);
        let mut triggered = false;
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                if uri.as_str() == URIS[1] && ordinal == 6 {
                    assert!(
                        !live_runs(&directory).is_empty(),
                        "fault must follow actual native spill"
                    );
                    triggered = true;
                    if cancellation {
                        token.cancel();
                    } else {
                        return Err(batch_input::failed("spilled deferred sentinel"));
                    }
                }
                producer.next(uri, ordinal, session)
            })
            .unwrap()
            .write_controlled(&target, Vortex, false, &token)
            .err()
            .unwrap()
            .to_string();
        assert!(triggered);
        assert!(
            error.contains(if cancellation {
                "cancel"
            } else {
                "spilled deferred sentinel"
            }),
            "{error}"
        );
        assert!(!target.exists());
        assert!(
            producer
                .prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
        let mut producer = Producer::new(ROWS, 2048);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| producer.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected(ROWS, true));
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    }
}

#[test]
fn streaming_dynamic_subqueries_cancel_during_spilled_parameter_replay_and_recover() {
    use crate::local_primitives::native_relational_spill::BEFORE_RUN_OPEN;
    use std::sync::atomic::AtomicBool;
    let fixture = fixture();
    let directory = fixture.0.join("deferred-replay-cancel");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    let cancellation = CancellationToken::default();
    let cancel = cancellation.clone();
    let failing = Arc::new(AtomicBool::new(true));
    let inject = failing.clone();
    let triggered = Arc::new(AtomicBool::new(false));
    let seen = triggered.clone();
    let prepared = prepare_lower(32 << 20, move |schemas| {
        let calls = AtomicUsize::new(0);
        let cancel = cancel.clone();
        let inject = inject.clone();
        let seen = seen.clone();
        let relation = schemas.defer_subquery_with_sources(
            &[DatasetUri::new(URIS[1])?],
            65_536,
            move |schemas| {
                if calls.fetch_add(1, Ordering::Relaxed) == 1 && inject.load(Ordering::Relaxed) {
                    let cancel = cancel.clone();
                    let seen = seen.clone();
                    BEFORE_RUN_OPEN.with(|hook| {
                        assert!(hook.borrow().is_none());
                        *hook.borrow_mut() = Some(Box::new(move |path| {
                            assert!(path.is_file());
                            seen.store(true, Ordering::Relaxed);
                            cancel.cancel();
                        }));
                    });
                }
                schemas
                    .resolve_output(&pivot(scan(1)))
                    .map(|(plan, _)| plan)
            },
        )?;
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
            relation,
            Kind::Exists,
        ))))
    })
    .with_spill(VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20).unwrap())
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut producer = Producer::new(ROWS, 2048);
    let error = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| producer.next(uri, ordinal, session))
        .unwrap()
        .collect_jsonl(&cancellation)
        .err()
        .unwrap()
        .to_string();
    let remaining_hook = BEFORE_RUN_OPEN.with(|hook| hook.borrow_mut().take());
    assert!(remaining_hook.is_none());
    assert!(triggered.load(Ordering::Relaxed));
    assert!(error.contains("cancel"), "{error}");
    assert_eq!(producer.calls, [1, (ROWS.div_ceil(BATCH) + 1) as u64]);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
    failing.store(false, Ordering::Relaxed);
    let mut producer = Producer::new(ROWS, 2048);
    let result = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| producer.next(uri, ordinal, session))
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut wanted = expected(ROWS, true);
    wanted[1]["answer"] = json!(true);
    assert_eq!(json_rows(&result), wanted);
    assert_eq!(result.execution.runtime.completed_executions, 1);
    assert_eq!(result.execution.input.as_ref().unwrap().replay_passes, 1);
    assert_eq!(result.execution.spilled_pivot_stages, 2);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
}
