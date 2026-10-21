use super::*;
use crate::{
    local_primitives::prepared_unary::{sample_observation, sample_preparation::Preparation},
    relational_query::{VortexRelationalLimit, VortexRelationalUnary},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use serde_json::{Value, json};

const ROWS: usize = 4096;

fn weights() -> Vec<Option<f64>> {
    (0..ROWS)
        .map(|row| Some(f64::from(u32::try_from(1 + row % 16).unwrap())))
        .collect()
}

fn sample(input: VortexRelationalPlan) -> VortexRelationalPlan {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::SampleRows,
        shardloom_plan::ProjectionRequest::columns(vec![ColumnRef::new("id").unwrap()]),
    );
    request.source_order_limit = Some(ROWS);
    request.sample_with_replacement = true;
    request.sample_seed = Some(7);
    request.sample_weight_column = Some(ColumnRef::new("weight").unwrap());
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn memory_source(
    session: &ResidentVortexSession,
    weights: &[Option<f64>],
) -> Result<ResidentMemorySource> {
    let ids = (0..weights.len())
        .map(|row| i64::try_from(row).unwrap())
        .collect::<Vec<_>>();
    let reversed = weights.iter().rev().copied().collect::<Vec<_>>();
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "id",
                values: MemoryColumnValues::Int64NonNullable(&ids),
            },
            MemoryColumn {
                name: "weight",
                values: MemoryColumnValues::Float64(weights),
            },
            MemoryColumn {
                name: "other",
                values: MemoryColumnValues::Float64(&reversed),
            },
        ],
        MemorySourceBounds::default(),
    )
}

fn prepared(
    weights: &[Option<f64>],
    modify: impl FnOnce(VortexRelationalPlan) -> VortexRelationalPlan,
) -> PreparedVortexRelational {
    let uri = DatasetUri::new("memory://prepared-sampling-lifecycle").unwrap();
    let plan = modify(sample(VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })));
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = 64 << 20;
    prepare_relational_with_schema(policy, |schemas| {
        schemas.register_memory_source(uri, |session| memory_source(session, weights))?;
        Ok(plan)
    })
    .unwrap()
}

fn collect(prepared: &PreparedVortexRelational) -> (Vec<Value>, sample_observation::Counts) {
    let observed = sample_observation::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let counts = observed.finish();
    assert_eq!(result.execution.scan_rows_delivered, ROWS as u64);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    (json_rows(&result), counts)
}

fn expected(weights: &[Option<f64>], seed: u64) -> Vec<Value> {
    let mut total = 0.0;
    let cumulative = weights
        .iter()
        .map(|weight| {
            total += weight.unwrap();
            total
        })
        .collect::<Vec<_>>();
    (0..ROWS).map(|draw| {
        let threshold = crate::local_primitives::deterministic_sample_unit(seed ^ 0xa076_1d64_78bd_642f, draw) * total;
        json!({"id": cumulative.partition_point(|weight| *weight < threshold).min(cumulative.len() - 1)})
    }).collect()
}

fn retained(prepared: &PreparedVortexRelational) -> u64 {
    prepared
        .sampling_preparation()
        .map_or(0, Preparation::retained_bytes)
}

#[test]
fn prepared_sampling_public_reuse_owns_immutable_copied_source_and_exact_credits() {
    let mut input = weights();
    let original = input.clone();
    let prepared = prepared(&input, |plan| plan);
    assert!(prepared.sampling_preparation().is_some());
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    assert_eq!(retained(&prepared), 0);
    for call in 0..3 {
        let (rows, counts) = collect(&prepared);
        assert_eq!(rows, expected(&original, 7));
        assert_eq!(counts.calibrations, u64::from(call == 0));
        assert_eq!(counts.model_reuses, u64::from(call > 0));
        assert_eq!(counts.populations, 1);
        assert_eq!(counts.population_entries, ROWS as u64);
        assert_eq!(counts.model_searches, ROWS as u64);
        assert_eq!(counts.maximum_radius, 5);
        assert_eq!(retained(&prepared), counts.maximum_model_credit);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            initial + retained(&prepared)
        );
        input.reverse();
        input[0] = Some(999.0);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn prepared_sampling_same_count_total_and_uri_do_not_share_source_or_plan_identity() {
    let input = weights();
    let reversed = input.iter().rev().copied().collect::<Vec<_>>();
    let first = prepared(&input, |plan| plan);
    let second = prepared(&reversed, |plan| plan);
    let third = prepared(&input, |mut plan| {
        let VortexRelationalPlan::Unary(unary) = &mut plan else {
            unreachable!()
        };
        unary.request.sample_seed = Some(9);
        unary.request.sample_weight_column = Some(ColumnRef::new("other").unwrap());
        plan
    });
    for (owner, expected) in [
        (&first, expected(&input, 7)),
        (&second, expected(&reversed, 7)),
        (&third, expected(&reversed, 9)),
    ] {
        for call in 0..2 {
            let (rows, counts) = collect(owner);
            assert_eq!(rows, expected);
            assert_eq!(counts.calibrations, u64::from(call == 0));
            assert_eq!(counts.model_reuses, u64::from(call > 0));
        }
    }
    let pools = [&first, &second, &third].map(|plan| plan.session.memory().clone());
    drop((first, second, third));
    for memory in pools {
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_new_projection_keeps_its_own_schema_and_values() {
    let input = weights();
    let prepared = prepared(&input, |mut plan| {
        let VortexRelationalPlan::Unary(unary) = &mut plan else {
            unreachable!()
        };
        unary.request.projection = shardloom_plan::ProjectionRequest::columns(vec![
            ColumnRef::new("weight").unwrap(),
            ColumnRef::new("id").unwrap(),
        ]);
        plan
    });
    for call in 0..2 {
        let (rows, counts) = collect(&prepared);
        assert_eq!(counts.calibrations, u64::from(call == 0));
        assert_eq!(counts.model_reuses, u64::from(call > 0));
        for (actual, expected) in rows.iter().zip(expected(&input, 7)) {
            let index = usize::try_from(expected["id"].as_u64().unwrap()).unwrap();
            assert_eq!(actual, &json!({"id": index, "weight": input[index]}));
        }
    }
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_cancel_consumer_error_and_unwind_clear_existing_models() {
    for failure in 0..4 {
        let prepared = prepared(&weights(), |plan| plan);
        collect(&prepared);
        assert!(retained(&prepared) > 0);
        let memory = prepared.session.memory().clone();
        let without_model = memory.snapshot().reserved_bytes - retained(&prepared);
        let cancellation = CancellationToken::default();
        let mut callbacks = 0;
        if failure == 0 {
            cancellation.cancel();
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            prepared.for_each_batch(&cancellation, |_, _| {
                callbacks += 1;
                match failure {
                    1 => Err(failed("deliberate sampling consumer failure")),
                    2 => {
                        cancellation.cancel();
                        Ok(())
                    }
                    3 => panic!("deliberate sampling consumer unwind"),
                    _ => unreachable!(),
                }
            })
        }));
        if failure == 3 {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(callbacks, usize::from(failure != 0));
        assert_eq!(retained(&prepared), 0);
        assert_eq!(memory.snapshot().reserved_bytes, without_model);
        // A callback panic can poison the existing session admission lock; do
        // not claim that changing scalar metadata makes that session reusable.
        if failure != 3 {
            assert_eq!(collect(&prepared).1.calibrations, 1);
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_recursive_consumer_is_rejected_without_publishing_or_deadlocking() {
    let prepared = prepared(&weights(), |plan| plan);
    let mut calls = 0;
    let execution = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            calls += 1;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .unwrap();
            assert!(error.to_string().contains("cannot execute recursively"));
            assert_eq!(retained(&prepared), 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(execution.output_rows, ROWS as u64);
    assert!(calls > 0);
    drop(execution);
    assert_eq!(retained(&prepared), 0);
    assert_eq!(collect(&prepared).1.calibrations, 1);
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_denied_execution_and_final_native_ownership_clear_model() {
    for final_ownership in [false, true] {
        let prepared = prepared(&weights(), |plan| plan);
        collect(&prepared);
        let memory = prepared.session.memory().clone();
        let without_model = memory.snapshot().reserved_bytes - retained(&prepared);
        let pressure = std::rc::Rc::new(RefCell::new(None));
        if final_ownership {
            super::super::sample_preparation::BEFORE_RESULT_OWNERSHIP
                .with_borrow_mut(|slot| *slot = Some(Box::new(Vec::clear)));
        } else {
            let snapshot = memory.snapshot();
            *pressure.borrow_mut() = Some(
                memory
                    .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
                    .unwrap(),
            );
        }
        assert!(
            prepared
                .execute_native(&CancellationToken::default())
                .is_err()
        );
        assert!(
            super::super::sample_preparation::BEFORE_RESULT_OWNERSHIP.with_borrow(Option::is_none)
        );
        assert_eq!(retained(&prepared), 0);
        pressure.borrow_mut().take();
        assert_eq!(memory.snapshot().reserved_bytes, without_model);
        assert_eq!(memory.snapshot().denied_reservations > 0, !final_ownership);
        assert_eq!(collect(&prepared).1.calibrations, 1);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_serialized_threads_reuse_one_exclusive_owner() {
    use std::sync::{Arc, Mutex};
    let prepared = prepared(&weights(), |plan| plan);
    let memory = prepared.session.memory().clone();
    let shared = Arc::new(Mutex::new(prepared));
    let workers = (0..3)
        .map(|_| {
            let shared = shared.clone();
            std::thread::spawn(move || collect(&shared.lock().unwrap()))
        })
        .collect::<Vec<_>>();
    let mut calibrations = 0;
    let mut reuses = 0;
    for worker in workers {
        let (rows, counts) = worker.join().unwrap();
        assert_eq!(rows, expected(&weights(), 7));
        calibrations += counts.calibrations;
        reuses += counts.model_reuses;
    }
    assert_eq!((calibrations, reuses), (1, 2));
    drop(shared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_child_limit_and_spill_plans_do_not_retain_models() {
    let child = prepared(&weights(), |input| {
        VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input,
            offset: 0,
            count: ROWS,
        }))
    });
    let limit = prepared(&weights(), |mut plan| {
        let VortexRelationalPlan::Unary(unary) = &mut plan else {
            unreachable!()
        };
        unary.input = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: unary.input.clone(),
            offset: 0,
            count: ROWS,
        }));
        plan
    });
    let root = prepared(&weights(), |plan| plan);
    let workspace = std::env::temp_dir().join(format!(
        "shardloom-prepared-sample-spill-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let spill = root
        .with_spill(
            crate::relational_query::VortexRelationalSpillPolicy::new(
                workspace.display().to_string(),
                16 << 20,
                8 << 20,
            )
            .unwrap(),
        )
        .unwrap();
    for prepared in [child, limit, spill] {
        assert!(prepared.sampling_preparation().is_none());
        let memory = prepared.session.memory().clone();
        let (rows, counts) = collect(&prepared);
        assert_eq!(rows, expected(&weights(), 7));
        assert_eq!(counts.calibrations, 0);
        assert_eq!(counts.model_reuses, 0);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    if workspace.exists() {
        fs::remove_dir_all(workspace).unwrap();
    }
}

#[test]
fn prepared_sampling_filters_and_ineligible_requests_keep_ordinary_search() {
    for variant in 0..5 {
        let prepared = prepared(&weights(), |mut plan| {
            let VortexRelationalPlan::Unary(unary) = &mut plan else {
                unreachable!()
            };
            let predicate = shardloom_core::PredicateExpr::Compare {
                column: ColumnRef::new("id").unwrap(),
                op: shardloom_core::ComparisonOp::Gt,
                value: shardloom_core::StatValue::Int64(-1),
            };
            match variant {
                0 => unary.request.predicate = Some(predicate),
                1 => {
                    let VortexRelationalPlan::Scan(scan) = &mut unary.input else {
                        unreachable!()
                    };
                    scan.predicate = Some(predicate);
                }
                2 => unary.request.sample_weight_column = None,
                3 => unary.request.sample_with_replacement = false,
                4 => unary.request.source_order_limit = Some(8),
                _ => unreachable!(),
            }
            plan
        });
        assert!(prepared.sampling_preparation().is_none());
        let memory = prepared.session.memory().clone();
        for _ in 0..2 {
            let (rows, counts) = collect(&prepared);
            assert_eq!(
                counts.calibrations + counts.model_reuses + counts.refusal_reuses,
                0
            );
            assert_eq!(counts.maximum_owner_credit + counts.maximum_model_credit, 0);
            assert_eq!(rows.len(), if variant == 4 { 8 } else { ROWS });
            if variant <= 1 {
                assert_eq!(rows, expected(&weights(), 7));
            }
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_file_and_dynamic_memory_roots_do_not_retain_models() {
    let input = weights();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "weight"]),
            vec![
                PrimitiveArray::from_iter((0..ROWS).map(|row| i64::try_from(row).unwrap()))
                    .into_array(),
                PrimitiveArray::from_iter(input.iter().map(|weight| weight.unwrap())).into_array(),
            ],
            ROWS,
            Validity::NonNullable,
        )
        .into_array(),
        2048,
    );
    let file = prepare_relational(&sample(fixture.scan()), policy()).unwrap();
    let uri = DatasetUri::new("memory://prepared-sampling-dynamic").unwrap();
    let plan = sample(VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    }));
    let dynamic = prepare_relational_with_dynamic_inputs(
        std::slice::from_ref(&uri),
        policy(),
        65536,
        |schemas| {
            schemas.register_memory_source(uri.clone(), |session| memory_source(session, &input))
        },
        move |_| Ok(plan.clone()),
    )
    .unwrap();
    assert!(matches!(dynamic.root, PreparedRoot::Dynamic(_)));
    for prepared in [file, dynamic] {
        assert!(prepared.sampling_preparation().is_none());
        let memory = prepared.session.memory().clone();
        for _ in 0..2 {
            let (rows, counts) = collect(&prepared);
            assert_eq!(rows, expected(&input, 7));
            assert_eq!(
                counts.calibrations + counts.model_reuses + counts.refusal_reuses,
                0
            );
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_streaming_population_is_fresh_and_never_admitted() {
    let uri = DatasetUri::new("memory://prepared-sampling-stream").unwrap();
    let plan = sample(VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    }));
    let batch = |session: &ResidentVortexSession, start: usize, end: usize| {
        let ids = (start..end)
            .map(|row| i64::try_from(row).unwrap())
            .collect::<Vec<_>>();
        let input = weights();
        ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "id",
                    values: MemoryColumnValues::Int64NonNullable(&ids),
                },
                MemoryColumn {
                    name: "weight",
                    values: MemoryColumnValues::Float64(&input[start..end]),
                },
            ],
        )
    };
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_batch_source(uri, |session| batch(session, 0, 0))?;
        Ok(plan)
    })
    .unwrap();
    assert!(prepared.sampling_preparation().is_none());
    let memory = prepared.session.memory().clone();
    for _ in 0..2 {
        let mut start = 0;
        let mut producer = |session: &ResidentVortexSession| {
            if start == ROWS {
                return Ok(None);
            }
            let result = batch(session, start, start + 2048)?;
            start += 2048;
            Ok(Some(result))
        };
        let observed = sample_observation::Session::start();
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let counts = observed.finish();
        assert_eq!(json_rows(&result), expected(&weights(), 7));
        assert_eq!(counts.populations, 1);
        assert_eq!(counts.population_entries, ROWS as u64);
        assert_eq!(
            counts.calibrations + counts.model_reuses + counts.refusal_reuses,
            0
        );
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_invalid_weights_never_publish_calibration() {
    for invalid in [None, Some(0.0), Some(-1.0)] {
        let mut input = weights();
        input[ROWS - 1] = invalid;
        let prepared = prepared(&input, |plan| plan);
        let memory = prepared.session.memory().clone();
        let initial = memory.snapshot().reserved_bytes;
        for _ in 0..2 {
            let observed = sample_observation::Session::start();
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
            assert_eq!(observed.finish().calibrations, 0);
            assert_eq!(retained(&prepared), 0);
            assert_eq!(memory.snapshot().reserved_bytes, initial);
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_cancellation_during_public_calibration_releases_provisional_credit() {
    use crate::local_primitives::prepared_unary::sample_preparation::CALIBRATION_POLL;
    let prepared = prepared(&weights(), |plan| plan);
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let cancellation = CancellationToken::default();
    let owner = cancellation.clone();
    let last = std::rc::Rc::new(Cell::new(0));
    let recorded = last.clone();
    CALIBRATION_POLL.with_borrow_mut(|slot| {
        *slot = Some(Box::new(move |ordinal| {
            recorded.set(ordinal);
            if ordinal == 256 {
                owner.cancel();
            }
        }));
    });
    let result = prepared.collect_jsonl(&cancellation);
    CALIBRATION_POLL.with_borrow_mut(|slot| *slot = None);
    assert!(result.is_err());
    assert_eq!(last.get(), 256);
    assert_eq!(retained(&prepared), 0);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    assert_eq!(collect(&prepared).1.calibrations, 1);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_pressure_before_reuse_evicts_and_still_delivers_exact_rows() {
    use crate::local_primitives::prepared_unary::sample_preparation::BEFORE_MODEL_PREPARATION;
    let prepared = prepared(&weights(), |plan| plan);
    collect(&prepared);
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes - retained(&prepared);
    let pressure = std::rc::Rc::new(RefCell::new(None));
    let held = pressure.clone();
    let pool = memory.clone();
    BEFORE_MODEL_PREPARATION.with_borrow_mut(|slot| {
        *slot = Some(Box::new(move || {
            let snapshot = pool.snapshot();
            *held.borrow_mut() = Some(
                pool.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - (1 << 20) + 1)
                    .unwrap(),
            );
        }));
    });
    let observed = sample_observation::Session::start();
    let mut actual = Vec::new();
    let execution = prepared
        .for_each_json_batch(&CancellationToken::default(), 256, 1 << 16, |batch| {
            actual.extend(serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap());
            Ok(())
        })
        .unwrap();
    drop(execution);
    let counts = observed.finish();
    pressure.borrow_mut().take();
    assert_eq!(actual, expected(&weights(), 7));
    assert_eq!(counts.credit_bypasses, 1);
    assert_eq!(counts.calibrations + counts.model_reuses, 0);
    assert_eq!(retained(&prepared), 0);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    assert_eq!(memory.snapshot().denied_reservations, 0);
    assert_eq!(collect(&prepared).1.calibrations, 1);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_sampling_failed_small_collection_clears_a_successful_streamed_model() {
    let prepared = prepared(&weights(), |mut plan| {
        let VortexRelationalPlan::Unary(unary) = &mut plan else {
            unreachable!()
        };
        unary.request.source_order_limit = Some(65537);
        plan
    });
    drop(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .unwrap(),
    );
    assert!(retained(&prepared) > 0);
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("65,536 rows"), "{error}");
    assert_eq!(retained(&prepared), 0);
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn prepared_sampling_arrow_completion_cancellation_clears_successful_native_model() {
    for output_window in [false, true] {
        let prepared = prepared(&weights(), |plan| plan);
        collect(&prepared);
        let memory = prepared.session.memory().clone();
        let without_model = memory.snapshot().reserved_bytes - retained(&prepared);
        let cancellation = CancellationToken::default();
        let final_cancel = cancellation.clone();
        super::super::sample_preparation::BEFORE_ARROW_COMPLETION
            .with_borrow_mut(|slot| *slot = Some(Box::new(move || final_cancel.cancel())));
        let observed = sample_observation::Session::start();
        let mut rows = 0;
        let mut consume = |batch: arrow_array::RecordBatch| {
            rows += batch.num_rows();
            Ok(())
        };
        let result = if output_window {
            prepared.for_each_arrow_batch_with_output_window(
                &cancellation,
                || false,
                |window| {
                    assert!(window.is_none());
                    Ok(())
                },
                &mut consume,
            )
        } else {
            prepared.for_each_arrow_batch(&cancellation, &mut consume)
        };
        assert!(result.is_err());
        assert_eq!(rows, ROWS);
        assert_eq!(observed.finish().model_reuses, 1);
        assert_eq!(retained(&prepared), 0);
        assert_eq!(memory.snapshot().reserved_bytes, without_model);
        assert_eq!(collect(&prepared).1.calibrations, 1);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_sampling_writer_publication_failure_clears_provisional_model() {
    use crate::local_primitives::prepared_unary::sample_preparation::BEFORE_MODEL_PREPARATION;
    for fanout in [false, true] {
        let prepared = prepared(&weights(), |plan| plan);
        let memory = prepared.session.memory().clone();
        let initial = memory.snapshot().reserved_bytes;
        let directory = std::env::temp_dir().join(format!(
            "shardloom-prepared-sample-writer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let target = directory.join("sample.vortex");
        let occupied = target.clone();
        BEFORE_MODEL_PREPARATION.with_borrow_mut(|slot| {
            *slot = Some(Box::new(move || {
                fs::write(occupied, b"concurrent destination").unwrap();
            }));
        });
        let observed = sample_observation::Session::start();
        let format = crate::VortexLocalPrimitiveRowExportFormat::Vortex;
        let result = if fanout {
            prepared
                .write_many(
                    &[
                        (target.clone(), format),
                        (directory.join("second.vortex"), format),
                    ],
                    false,
                )
                .map(|_| ())
        } else {
            prepared.write(&target, format, false).map(|_| ())
        };
        assert!(result.is_err());
        let counts = observed.finish();
        assert_eq!(counts.calibrations, 1);
        assert_eq!(counts.model_searches, ROWS as u64 * (1 + u64::from(fanout)));
        assert_eq!(counts.model_reuses, u64::from(fanout));
        assert_eq!(retained(&prepared), 0);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        assert_eq!(fs::read(&target).unwrap(), b"concurrent destination");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        assert_eq!(collect(&prepared).1.calibrations, 1);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn prepared_sampling_json_final_schema_denial_clears_completed_native_model() {
    let prepared = prepared(&weights(), |plan| plan);
    collect(&prepared);
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes - retained(&prepared);
    let pressure = std::rc::Rc::new(RefCell::new(None));
    let held = pressure.clone();
    let pool = memory.clone();
    super::super::sample_preparation::BEFORE_JSON_COMPLETION.with_borrow_mut(|slot| {
        *slot = Some(Box::new(move || {
            let snapshot = pool.snapshot();
            *held.borrow_mut() = Some(
                pool.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
                    .unwrap(),
            );
        }));
    });
    let observed = sample_observation::Session::start();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(observed.finish().model_reuses, 1);
    pressure.borrow_mut().take();
    assert_eq!(retained(&prepared), 0);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    assert_eq!(collect(&prepared).1.calibrations, 1);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
