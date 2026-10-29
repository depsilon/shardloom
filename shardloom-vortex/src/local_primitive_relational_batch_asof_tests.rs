//! As-of composition, source release and real native spill acceptance.

use super::*;
use crate::relational_query::{
    VortexRelationalAsOfDirection as Direction, VortexRelationalAsOfJoin,
    VortexRelationalAsOfOptions as Options,
};

fn plan(other: VortexRelationalPlan, stream_right: bool, options: Options) -> VortexRelationalPlan {
    let VortexRelationalPlan::Join(join) = joined(other, stream_right, JoinKind::Left) else {
        unreachable!()
    };
    VortexRelationalPlan::AsOfJoin(Box::new(VortexRelationalAsOfJoin {
        left: join.left,
        right: join.right,
        by: vec![],
        on: join.keys[0].clone(),
        options,
        columns: join.columns,
    }))
}

fn expected(stream_right: bool, options: Options) -> Vec<Value> {
    let (left, right) = if stream_right {
        (&ORDINARY[..], &STREAM[..])
    } else {
        (&STREAM[..], &ORDINARY[..])
    };
    left.iter()
        .map(|&(key, value)| {
            let matched = key.and_then(|key| {
                right
                    .iter()
                    .enumerate()
                    .filter_map(|(position, &(other, text))| {
                        let other = other?;
                        if (!options.allow_exact_matches && key == other)
                            || (options.direction == Direction::Backward && other > key)
                            || (options.direction == Direction::Forward && other < key)
                        {
                            return None;
                        }
                        let distance = key.abs_diff(other);
                        if options.tolerance.is_some_and(|limit| distance > limit) {
                            return None;
                        }
                        let forward = other > key || options.direction == Direction::Forward;
                        Some((
                            (
                                distance,
                                forward,
                                if forward {
                                    position
                                } else {
                                    usize::MAX - position
                                },
                            ),
                            text,
                        ))
                    })
                    .min_by_key(|(rank, _)| *rank)
                    .map(|(_, text)| text)
            });
            json!({"left":value,"right":matched})
        })
        .collect()
}

#[test]
fn asof_stream_either_side_preserves_source_order_release_and_owned_outputs() {
    for stream_right in [false, true] {
        for spill in [false, true] {
            for direction in [Direction::Backward, Direction::Forward, Direction::Nearest] {
                for allow_exact_matches in [true, false] {
                    let fixture = ordinary_file();
                    let options = Options {
                        direction,
                        allow_exact_matches,
                        tolerance: Some(1),
                    };
                    let prepared = prepared(
                        &plan(fixture.scan(), stream_right, options),
                        &fixture,
                        None,
                        spill,
                    );
                    let baseline = prepared.snapshot().memory.reserved_bytes;
                    let memory = prepared.session.memory().clone();
                    let mut next = 0;
                    let mut prior: Option<Weak<MemoryLease>> = None;
                    let mut producer = |session: &ResidentVortexSession| {
                        assert!(
                            prior
                                .as_ref()
                                .is_none_or(|witness| witness.strong_count() == 0)
                        );
                        let rows = match next {
                            0 => &STREAM[..2],
                            1 => &[],
                            2 => &STREAM[2..],
                            3 => return Ok(None),
                            _ => unreachable!(),
                        };
                        next += 1;
                        let batch = source(
                            session,
                            &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                            &rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
                        )?;
                        prior = Some(batch.batch_release_witness()?);
                        Ok(Some(batch))
                    };
                    let mut observed = vec![];
                    let mut held = vec![];
                    let result = prepared
                        .with_batch_input(&mut producer)
                        .unwrap()
                        .for_each_batch(&CancellationToken::default(), |array, context| {
                            observed.extend(values(&array, context)?);
                            assert_eq!(array.dtype(), &prepared.output_dtype().unwrap());
                            held.push(array);
                            Ok(())
                        })
                        .unwrap();
                    assert_eq!(observed, expected(stream_right, options));
                    assert_eq!(next, 3);
                    let input = result.input.as_ref().unwrap();
                    assert!(input.end_of_input_observed && input.output_ownership_detached);
                    assert_eq!(input.rows, 4);
                    assert_eq!(
                        input.join_build_rows_detached,
                        if stream_right { 4 } else { 3 }
                    );
                    assert!(result.native_io_certificate.is_certified());
                    assert!(!result.native_io_certificate.side_effects.fallback_attempted);
                    if let Some(spill) = &result.spill {
                        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
                    }
                    drop(result);
                    assert!(prepared.snapshot().memory.reserved_bytes > baseline);
                    drop(prepared);
                    assert!(memory.snapshot().reserved_bytes > 0);
                    drop(held);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}

const GRANT: u64 = 16 << 20;

fn bulk_key(row: usize) -> Option<i64> {
    (!row.is_multiple_of(31)).then(|| i64::try_from(row * 17 % 97).unwrap() - 17)
}
fn bulk_text(row: usize) -> String {
    format!("{row:09}{}", "x".repeat(503))
}

fn bulk_prepared(fixture: &Fixture, spill: bool, grant: u64) -> PreparedVortexRelational {
    let prepared = prepare(&plan(fixture.scan(), true, Options::default()), grant).unwrap();
    if spill {
        let path = fixture.0.join("asof-runs");
        fs::create_dir_all(&path).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(path, 128 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

struct Bulk {
    next: usize,
    total: usize,
    prior: Option<Weak<MemoryLease>>,
}
impl Bulk {
    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        if self.next == self.total {
            return Ok(None);
        }
        let end = (self.next + 1024).min(self.total);
        let keys = (self.next..end).map(bulk_key).collect::<Vec<_>>();
        let text = (self.next..end).map(bulk_text).collect::<Vec<_>>();
        let batch = source(
            session,
            &keys,
            &text.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>(),
        )?;
        self.next = end;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn execute_bulk(
    prepared: &PreparedVortexRelational,
    total: usize,
) -> Result<ExecutedVortexRelational> {
    let mut bulk = Bulk {
        next: 0,
        total,
        prior: None,
    };
    let mut producer = |session: &ResidentVortexSession| bulk.next(session);
    let mut rows = vec![];
    let result = prepared.with_batch_input(&mut producer)?.for_each_batch(
        &CancellationToken::default(),
        |array, context| {
            rows.extend(values(&array, context)?);
            Ok(())
        },
    )?;
    let ids = [1, 3].map(|key| (0..total).rfind(|row| bulk_key(*row) == Some(key)).unwrap());
    assert_eq!(
        rows,
        [
            json!({"left":"r1","right":bulk_text(ids[0])}),
            json!({"left":"r3","right":bulk_text(ids[1])}),
            json!({"left":"rn","right":null})
        ]
    );
    assert_eq!(bulk.next, total);
    Ok(result)
}

#[test]
fn asof_pressure_denies_resident_and_completes_native_spill_under_same_grant() {
    let fixture = ordinary_file();
    let total = 32_771;
    let denied = bulk_prepared(&fixture, false, GRANT);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute_bulk(&denied, total)
        .err()
        .expect("retained right side must exceed grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(denied.snapshot().completed_executions, 0);
    drop(denied);
    for spill in [false, true] {
        let prepared = bulk_prepared(&fixture, spill, if spill { GRANT } else { 128 << 20 });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let result = execute_bulk(&prepared, total).unwrap();
        assert_eq!(
            result.input.as_ref().unwrap().join_build_rows_detached,
            total as u64
        );
        if spill {
            let report = result.spill.as_ref().unwrap();
            assert!(report.runs_written > 3 && report.merge_passes > 1);
            assert!(report.owned_cleanup_completed && report.peak_disk_bytes <= report.quota_bytes);
            assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
            assert_eq!(result.ordered_join_build_rows, total as u64);
            assert_eq!(result.ordered_join_probe_rows, 3);
            assert_eq!(result.ordered_join_match_records, 2);
            assert!(result.ordered_join_lookup_blocks > 0);
            assert_eq!(fs::read_dir(&report.workspace).unwrap().count(), 0);
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn asof_spill_faults_cancel_and_refund_with_fresh_retry() {
    use crate::local_primitives::{
        native_relational_spill::AFTER_MERGE_BLOCK,
        native_relational_window::spill::BEFORE_CACHE_HIT,
    };
    use std::{cell::Cell, rc::Rc};
    for failure in ["producer", "merge-cancel", "consumer", "lookup-corrupt"] {
        let fixture = ordinary_file();
        let prepared = bulk_prepared(&fixture, true, GRANT);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let triggered = Rc::new(Cell::new(false));
        if failure == "merge-cancel" {
            let triggered = triggered.clone();
            let token = token.clone();
            AFTER_MERGE_BLOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |rows| {
                    assert!(rows > 0);
                    triggered.set(true);
                    token.cancel();
                }));
            });
        }
        if failure == "lookup-corrupt" {
            let triggered = triggered.clone();
            BEFORE_CACHE_HIT.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    assert!(fs::metadata(path).unwrap().len() > 1);
                    fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .unwrap()
                        .set_len(1)
                        .unwrap();
                    triggered.set(true);
                }));
            });
        }
        let mut bulk = Bulk {
            next: 0,
            total: 8_193,
            prior: None,
        };
        let mut producer = |session: &ResidentVortexSession| {
            if failure == "producer" && bulk.next >= 6_144 {
                assert!(
                    fs::read_dir(fixture.0.join("asof-runs"))
                        .unwrap()
                        .any(|entry| {
                            let path = entry.unwrap().path();
                            path.is_dir()
                                && fs::read_dir(path).unwrap().any(|file| {
                                    file.unwrap()
                                        .path()
                                        .extension()
                                        .is_some_and(|extension| extension == "vortex")
                                })
                        }),
                    "producer failure must follow real native spill"
                );
                triggered.set(true);
                return Err(batch_input::failed("asof producer after spill"));
            }
            bulk.next(session)
        };
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                if failure == "consumer" {
                    triggered.set(true);
                    return Err(batch_input::failed("asof consumer refused"));
                }
                Ok(())
            });
        assert!(result.is_err(), "{failure}");
        assert!(triggered.get(), "{failure}");
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            baseline,
            "{failure}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(
            fs::read_dir(fixture.0.join("asof-runs")).unwrap().count(),
            0
        );
        assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
        assert!(BEFORE_CACHE_HIT.with(|hook| hook.borrow().is_none()));
        let retry = execute_bulk(&prepared, 1025).unwrap();
        drop(retry);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn asof_zero_limit_drains_producer_and_preserves_late_errors() {
    for spill in [false, true] {
        let fixture = ordinary_file();
        let query = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: plan(fixture.scan(), false, Options::default()),
            offset: 0,
            count: 0,
        }));
        let prepared = prepared(&query, &fixture, None, spill);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 1 {
                return source(session, &[Some(1)], &[Some("left")]).map(Some);
            }
            Err(batch_input::failed("late asof producer error"))
        };
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()));
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("late asof producer error")
        );
        assert_eq!(calls, 2);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
