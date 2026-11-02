use super::*;
use crate::local_primitives::prepared_relational::write_produced_many_controlled;
use crate::{VortexAggregateOrderExpr, VortexSortRowsRequest, VortexSortSpillPolicy};

fn request(
    workspace: &std::path::Path,
    spill: bool,
    quota: u64,
) -> crate::VortexQueryPrimitiveRequest {
    let mut request = crate::VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new("memory://native-producer").unwrap(),
        shardloom_plan::ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new("n", false)]),
        ROWS,
    );
    if spill {
        request.sort_rows.as_mut().unwrap().spill =
            Some(VortexSortSpillPolicy::new(workspace, quota, 1 << 20).unwrap());
    }
    request
}

#[test]
fn produced_fanout_pressure_preserves_one_producer_values_and_the_original_spill_quota() {
    for spill in [false, true] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let workspace = fixture.0.join("runs");
        fs::create_dir(&workspace).unwrap();
        let request = request(&workspace, spill, 128 << 20);
        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let mut policy = policy();
        policy.resource_envelope.memory_budget_bytes = GRANT;
        let dtype = source(&session, &[], &[]).unwrap().dtype().clone();
        let initial = session.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("result.vortex"), Format::Vortex),
            (fixture.0.join("result.parquet"), Format::Parquet),
        ];
        let quota = request
            .sort_rows
            .as_ref()
            .unwrap()
            .spill
            .as_ref()
            .map(|spill| spill.quota_owner.bind(spill.quota_bytes).unwrap());
        let cancellation = CancellationToken::default();
        let mut executions = 0;
        let mut ordinal = 0;
        let result = write_produced_many_controlled(
            &session,
            policy,
            &request,
            &[],
            &targets,
            false,
            &cancellation,
            |consume| {
                executions += 1;
                session.with_native_execution_context(&cancellation, |context| {
                    while let Some(batch) = next(&session, &mut ordinal)? {
                        assert!(targets.iter().all(|(path, _)| !path.exists()));
                        let witness = batch.batch_release_witness()?;
                        consume(batch.batch_array(context)?.clone(), context)?;
                        drop(batch);
                        assert_eq!(witness.strong_count(), 0);
                    }
                    Ok(ordinal)
                })
            },
        );
        assert_eq!(executions, 1);
        if spill {
            let result = result.unwrap();
            assert_eq!(result.execution, ROWS.div_ceil(BATCH) + 1);
            assert_eq!(result.replay.rows_detached, ROWS as u64);
            assert!(result.replay.buffer_bytes_copied > GRANT);
            assert!(result.replay.spilled);
            assert_eq!(result.replay.replays, targets.len());
            assert_replay_decisions(&result.replay);
            let report = result.replay.spill.as_ref().unwrap();
            assert!(report.runs_written > 3 && report.merge_passes > 0);
            assert!(report.owned_cleanup_completed);
            let shared = quota.as_ref().unwrap().snapshot().unwrap();
            assert_eq!(shared.live_bytes, 0);
            assert_eq!(shared.pending_bytes, 0);
            assert_eq!(
                result.replay.shared_spill_peak_disk_bytes,
                Some(shared.peak_live_bytes)
            );
            assert!(shared.peak_live_bytes <= report.quota_bytes);
            for (output, (path, format)) in result.outputs.iter().zip(&targets) {
                assert_eq!(output.rows_written, ROWS as u64);
                assert!(output.evidence.side_effects.spill_io_performed);
                assert!(!output.evidence.side_effects.fallback_attempted);
                let rows = read_rows(path, *format, &dtype);
                assert_eq!(rows.len(), ROWS);
                for (index, row) in rows.into_iter().enumerate() {
                    assert_eq!(row, serde_json::json!({"n":index,"s":text(index)}));
                }
            }
        } else {
            let error = result
                .err()
                .expect("complete result is larger than the resident grant");
            assert!(error.to_string().contains("reservation denied"), "{error}");
            assert!(targets.iter().all(|(path, _)| !path.exists()));
        }
        assert_eq!(session.snapshot().memory.reserved_bytes, initial);
        assert!(session.snapshot().memory.peak_reserved_bytes <= GRANT);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn produced_fanout_spill_denial_cancel_and_late_error_release_every_owned_run() {
    for fault in ["quota", "cancel", "late_error"] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let workspace = fixture.0.join("runs");
        fs::create_dir(&workspace).unwrap();
        let keep = workspace.join("keep.txt");
        fs::write(&keep, b"unrelated").unwrap();
        let quota_bytes = if fault == "quota" {
            32 << 10
        } else {
            128 << 20
        };
        let request = request(&workspace, true, quota_bytes);
        let spill = request.sort_rows.as_ref().unwrap().spill.as_ref().unwrap();
        let quota = spill.quota_owner.bind(quota_bytes).unwrap();
        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let mut policy = policy();
        policy.resource_envelope.memory_budget_bytes = GRANT;
        let initial = session.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("result.vortex"), Format::Vortex),
            (fixture.0.join("result.json"), Format::Json),
        ];
        let cancellation = CancellationToken::default();
        let mut executions = 0;
        let mut ordinal = 0;
        let result = write_produced_many_controlled(
            &session,
            policy,
            &request,
            &[],
            &targets,
            false,
            &cancellation,
            |consume| {
                executions += 1;
                session.with_native_execution_context(&cancellation, |context| {
                    while let Some(batch) = next(&session, &mut ordinal)? {
                        if ordinal == 4 && fault == "cancel" {
                            spill.cancel();
                        }
                        if ordinal == 4 && fault == "late_error" {
                            return Err(batch_input::failed("late primitive producer error"));
                        }
                        consume(batch.batch_array(context)?.clone(), context)?;
                    }
                    Ok(())
                })
            },
        );
        let error = result
            .err()
            .expect("fault must fail before publication")
            .to_string();
        let expected = match fault {
            "quota" => "quota",
            "cancel" => "cancel",
            _ => "late primitive producer error",
        };
        assert!(error.contains(expected), "{fault}: {error}");
        assert_eq!(executions, 1);
        assert!(targets.iter().all(|(path, _)| !path.exists()));
        assert_eq!(session.snapshot().memory.reserved_bytes, initial);
        assert_eq!(quota.snapshot().unwrap().live_bytes, 0);
        assert_eq!(quota.snapshot().unwrap().pending_bytes, 0);
        assert!(quota.snapshot().unwrap().peak_live_bytes <= quota_bytes);
        assert_eq!(fs::read(&keep).unwrap(), b"unrelated");
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Keep overlapping owners alive across staging, publication and cleanup.
fn produced_fanout_cleanup_preserves_other_sessions_live_runs_and_pending_writes() {
    use crate::local_primitives::query_run_store::{
        QueryRunSpec, QueryRunStore, QueryRunStorePolicy,
    };
    use std::sync::Arc;
    use vortex::array::{IntoArray as _, arrays::PrimitiveArray};

    for spilled in [false, true] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let workspace = fixture.0.join("runs");
        fs::create_dir(&workspace).unwrap();
        let original = request(&workspace, true, 128 << 20);
        let request = original.clone();
        let spill = original.sort_rows.as_ref().unwrap().spill.as_ref().unwrap();
        let quota = spill.quota_owner.bind(spill.quota_bytes).unwrap();
        let cancellation = CancellationToken::default();
        let other_session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let other_initial = other_session.snapshot().memory.reserved_bytes;
        let (mut other_store, other_run, other_work) = other_session
            .with_native_execution_context(&cancellation, |context| {
                let mut store = QueryRunStore::new(
                    QueryRunStorePolicy::numeric_sort(spill),
                    context.memory().clone(),
                    context.memory().reserve(128 << 10)?,
                )?;
                let work = Arc::new(context.memory().reserve(1 << 20)?);
                let array = PrimitiveArray::from_iter([7_i64, 11, 13]).into_array();
                let run = store.write_arrays(
                    &QueryRunSpec {
                        dtype: array.dtype().clone(),
                        rows: 3,
                        block_rows: 3,
                        metadata_bytes: 16 << 10,
                    },
                    std::iter::once(Ok(array)),
                    context.runtime(),
                    context.native_session(),
                    &work,
                )?;
                Ok((store, run, work))
            })
            .unwrap();
        let other_payload = fs::read(&other_run.path).unwrap();
        // Hold a second owner's admitted write at a deterministic interleaving;
        // it may remain pending when this independent fanout completes.
        let other_write = quota.reserve(4096).unwrap();
        let shared_before = quota.snapshot().unwrap();
        assert_eq!(
            shared_before.live_bytes,
            other_store.snapshot().live_disk_bytes
        );
        assert!(shared_before.live_bytes > other_run.bytes);
        assert_eq!(shared_before.pending_bytes, 4096);

        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let mut policy = policy();
        policy.resource_envelope.memory_budget_bytes = GRANT;
        let dtype = source(&session, &[], &[]).unwrap().dtype().clone();
        let initial = session.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("result.vortex"), Format::Vortex),
            (fixture.0.join("result.parquet"), Format::Parquet),
        ];
        let rows = if spilled { ROWS } else { BATCH };
        let mut executions = 0;
        let mut ordinal = 0;
        let result = write_produced_many_controlled(
            &session,
            policy,
            &request,
            &[],
            &targets,
            false,
            &cancellation,
            |consume| {
                executions += 1;
                session.with_native_execution_context(&cancellation, |context| {
                    while let Some(batch) = next(&session, &mut ordinal)? {
                        assert!(targets.iter().all(|(path, _)| !path.exists()));
                        consume(batch.batch_array(context)?.clone(), context)?;
                        if !spilled {
                            break;
                        }
                    }
                    Ok(rows)
                })
            },
        )
        .unwrap();
        assert_eq!(executions, 1);
        assert_eq!(result.execution, rows);
        assert_eq!(result.replay.rows_detached, rows as u64);
        assert_eq!(result.replay.spilled, spilled);
        assert_replay_decisions(&result.replay);
        assert!(
            result
                .replay
                .spill
                .as_ref()
                .unwrap()
                .owned_cleanup_completed
        );
        let shared_after = quota.snapshot().unwrap();
        assert_eq!(shared_after.live_bytes, shared_before.live_bytes);
        assert_eq!(shared_after.pending_bytes, shared_before.pending_bytes);
        assert!(shared_after.peak_live_bytes <= spill.quota_bytes);
        assert_eq!(
            result.replay.shared_spill_peak_disk_bytes,
            Some(shared_after.peak_live_bytes)
        );
        assert_eq!(fs::read(&other_run.path).unwrap(), other_payload);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
        for (output, (path, format)) in result.outputs.iter().zip(&targets) {
            assert_eq!(output.rows_written, rows as u64);
            assert!(!output.evidence.side_effects.fallback_attempted);
            let actual = read_rows(path, *format, &dtype);
            assert_eq!(actual.len(), rows);
            for (index, row) in actual.into_iter().enumerate() {
                assert_eq!(row, serde_json::json!({"n":index,"s":text(index)}));
            }
        }
        assert_eq!(session.snapshot().memory.reserved_bytes, initial);
        assert!(session.snapshot().memory.peak_reserved_bytes <= GRANT);
        other_store.cleanup().unwrap();
        drop((other_store, other_run, other_work, other_write));
        assert_eq!(
            other_session.snapshot().memory.reserved_bytes,
            other_initial
        );
        assert_eq!(quota.snapshot().unwrap().live_bytes, 0);
        assert_eq!(quota.snapshot().unwrap().pending_bytes, 0);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}
