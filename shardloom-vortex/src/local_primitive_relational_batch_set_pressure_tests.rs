//! Distinct membership and declared source replay use the same admitted run store.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;

#[path = "local_primitive_relational_batch_set_fault_tests.rs"]
mod fault_tests;

#[path = "local_primitive_relational_batch_set_membership_fault_tests.rs"]
mod membership_fault_tests;

#[path = "local_primitive_relational_batch_set_membership_destination_tests.rs"]
mod membership_destination_tests;

const ROWS: usize = 8193;
const WIDTH: usize = 2048;
const INPUT_ROWS: usize = 251;
const GRANT: u64 = 16 << 20;

fn text(key: usize) -> String {
    format!("{key:08}{}", "p".repeat(WIDTH - 8))
}

fn key(side: usize, row: usize, unique: bool) -> usize {
    if unique { row } else { side * 64 + row % 128 }
}

struct WideInputs {
    positions: [usize; 2],
    calls: [u64; 2],
    ended: [bool; 2],
    prior: Option<Weak<MemoryLease>>,
    unique: bool,
    right_offset: usize,
    single_key: bool,
}

impl WideInputs {
    fn new(unique: bool) -> Self {
        Self {
            positions: [0; 2],
            calls: [0; 2],
            ended: [false; 2],
            prior: None,
            unique,
            right_offset: 0,
            single_key: false,
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
        let side = URIS
            .iter()
            .position(|expected| *expected == uri.as_str())
            .unwrap();
        assert!(!self.ended[side], "an ended set producer was reopened");
        assert_eq!(ordinal, self.calls[side]);
        self.calls[side] += 1;
        if self.positions[side] == ROWS {
            self.ended[side] = true;
            return Ok(None);
        }
        let start = self.positions[side];
        let end = (start + INPUT_ROWS).min(ROWS);
        let row_key = |row| {
            if self.single_key {
                0
            } else {
                key(side, row, self.unique) + side * self.right_offset
            }
        };
        let numbers = (start..end)
            .map(|row| Some(i64::try_from(row_key(row)).unwrap()))
            .collect::<Vec<_>>();
        let strings = (start..end)
            .map(|row| text(row_key(row)))
            .collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let batch = source(session, &numbers, &refs)?;
        self.prior = Some(batch.batch_release_witness()?);
        self.positions[side] = end;
        Ok(Some(batch))
    }
}

fn workspace() -> (Fixture, PathBuf) {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("set-runs");
    fs::create_dir(&directory).unwrap();
    (fixture, directory)
}

fn with_spill(
    prepared: PreparedVortexRelational,
    directory: &std::path::Path,
    quota: u64,
) -> PreparedVortexRelational {
    prepared
        .with_spill(VortexRelationalSpillPolicy::new(directory, quota, 1 << 20).unwrap())
        .unwrap()
}

fn check_row(row: &Value, expected: usize) {
    assert_eq!(row, &json!({"n": expected, "s": text(expected)}));
}

#[test]
fn streaming_sets_release_duplicate_payload_when_cumulative_input_exceeds_grant() {
    for (spill, kind) in [false, true]
        .into_iter()
        .flat_map(|spill| KINDS.map(|kind| (spill, kind)))
    {
        let (prepared, fixture) = optional_spill(
            prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
            spill,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut inputs = WideInputs::new(false);
        let mut delivered = 0;
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
                let rows: Vec<Value> = serde_json::from_str(batch.values_json.value()).unwrap();
                assert!(rows.len() <= 257);
                for row in rows {
                    let expected = match kind {
                        SetKind::UnionAll => key(delivered / ROWS, delivered % ROWS, false),
                        SetKind::UnionDistinct | SetKind::Except => delivered,
                        SetKind::Intersect => 64 + delivered,
                    };
                    check_row(&row, expected);
                    delivered += 1;
                }
                Ok(())
            })
            .unwrap();
        let expected_rows = match kind {
            SetKind::UnionAll => ROWS * 2,
            SetKind::UnionDistinct => 192,
            SetKind::Intersect | SetKind::Except => 64,
        };
        assert_eq!(delivered, expected_rows);
        assert_eq!(result.output_rows, expected_rows as u64);
        assert_eq!(inputs.ended, [true; 2]);
        assert_eq!(inputs.positions, [ROWS; 2]);
        assert_eq!(inputs.calls, [ROWS.div_ceil(INPUT_ROWS) as u64 + 1; 2]);
        let input = result.input.as_ref().unwrap();
        assert_eq!(input.sources, 2);
        assert_eq!(input.rows, (ROWS * 2) as u64);
        assert_eq!(input.max_retained_input_batches, 1);
        assert_eq!(input.replay_sources, 0);
        assert!(input.input_logical_bytes > GRANT * 2);
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
        eprintln!(
            "{}",
            json!({"family":"streamed_set_duplicate_pressure", "kind":format!("{kind:?}"),
            "input_rows":input.rows,"input_logical_bytes":input.input_logical_bytes,"grant":GRANT,
            "peak_reserved_bytes":result.runtime.memory.peak_reserved_bytes,"output_rows":result.output_rows,
            "membership_spill_strategy":spill && kind != SetKind::UnionAll,"replay_sources":input.replay_sources})
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(fixture.0.join("set-runs")).unwrap().count(), 0);
    }
}

#[test]
fn streaming_set_membership_spill_keeps_a_single_hot_key_bounded() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let (_fixture, directory) = workspace();
        let prepared = with_spill(
            prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
            &directory,
            128 << 20,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut inputs = WideInputs::new(false);
        inputs.single_key = true;
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected = if kind == SetKind::Except {
            vec![]
        } else {
            vec![json!({"n":0,"s":text(0)})]
        };
        assert_eq!(json_rows(&result), expected);
        assert_eq!(inputs.ended, [true; 2]);
        assert_eq!(result.execution.ordered_set_groups, 1);
        assert_eq!(result.execution.ordered_set_input_rows, (2 * ROWS) as u64);
        let spill = result.execution.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed);
        assert!(result.execution.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_set_membership_growth_is_denied_without_spill_permission() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let (_fixture, directory) = workspace();
        let prepared = prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut inputs = WideInputs::new(true);
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .expect("unbounded distinct membership must not be admitted");
        assert!(
            error.to_string().contains("reservation denied"),
            "{kind:?}: {error}"
        );
        assert!(inputs.positions.iter().sum::<usize>() < ROWS * 2);
        assert!(
            inputs
                .prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        let snapshot = prepared.snapshot();
        assert_eq!(snapshot.completed_executions, 0);
        assert!(snapshot.memory.denied_reservations > 0);
        assert!(snapshot.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(snapshot.memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    }
}

#[test]
fn streaming_set_membership_spill_completes_exact_values_beyond_the_resident_grant() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let (_fixture, directory) = workspace();
        let prepared = with_spill(
            prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
            &directory,
            128 << 20,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut inputs = WideInputs::new(true);
        let mut delivered = 0;
        let report = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
                for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                    check_row(&row, delivered);
                    delivered += 1;
                }
                Ok(())
            })
            .unwrap();
        let expected = if kind == SetKind::Except { 0 } else { ROWS };
        assert_eq!(delivered, expected, "{kind:?}");
        assert_eq!(report.output_rows, expected as u64);
        assert_eq!(report.ordered_set_stages, 1);
        assert_eq!(report.ordered_set_input_rows, (2 * ROWS) as u64);
        assert_eq!(report.ordered_set_groups, ROWS as u64);
        assert_eq!(report.ordered_set_output_rows, expected as u64);
        assert_eq!(inputs.ended, [true; 2]);
        assert_eq!(inputs.positions, [ROWS; 2]);
        assert_eq!(inputs.calls, [ROWS.div_ceil(INPUT_ROWS) as u64 + 1; 2]);
        let input = report.input.as_ref().unwrap();
        assert!(input.input_logical_bytes > GRANT * 2);
        assert_eq!(input.replay_sources, 0);
        assert_eq!(input.replay_spilled_sources, 0);
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        eprintln!(
            "{}",
            json!({"family":"ordered_set_membership_pressure", "kind":format!("{kind:?}"),
            "input_rows":input.rows, "input_logical_bytes":input.input_logical_bytes, "grant":GRANT,
            "peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,
            "output_rows":report.output_rows, "groups":report.ordered_set_groups,
            "runs_written":spill.runs_written, "merge_passes":spill.merge_passes,
            "peak_disk_bytes":spill.peak_disk_bytes, "source_replay_spill":false,
            "owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

fn repeated_union() -> VortexRelationalPlan {
    set(input_scan(0), input_scan(0), SetKind::UnionAll)
}

fn execute_repeated(prepared: &PreparedVortexRelational) -> Result<ExecutedVortexRelational> {
    let mut inputs = WideInputs::new(true);
    let mut delivered = 0;
    let result = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))?
        .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
            for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                check_row(&row, delivered % ROWS);
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(delivered, ROWS * 2);
    assert_eq!(inputs.ended, [true, false]);
    assert_eq!(inputs.positions, [ROWS, 0]);
    assert_eq!(inputs.calls, [ROWS.div_ceil(INPUT_ROWS) as u64 + 1, 0]);
    let input = result.input.as_ref().unwrap();
    assert_eq!(input.sources, 1);
    assert_eq!(input.rows, ROWS as u64);
    assert_eq!(input.replay_sources, 1);
    assert_eq!(input.replay_passes, 1);
    assert_eq!(input.replay_rows, ROWS as u64);
    assert_eq!(input.replay_rows_detached, ROWS as u64);
    assert!(input.input_logical_bytes > GRANT);
    assert!(input.replay_buffer_bytes_copied > GRANT);
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    assert!(result.native_io_certificate.is_certified());
    assert!(!result.native_io_certificate.side_effects.fallback_attempted);
    Ok(result)
}

#[test]
fn streaming_union_all_replays_one_source_with_actual_native_spill_and_complete_values() {
    let (_fixture, directory) = workspace();
    let denied = prepared_set(&repeated_union(), 1, GRANT);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute_repeated(&denied)
        .err()
        .expect("replay does not fit in the small grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    drop(denied);

    let ample = prepared_set(&repeated_union(), 1, 128 << 20);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let result = execute_repeated(&ample).unwrap();
    assert_eq!(result.input.as_ref().unwrap().replay_spilled_sources, 0);
    let ample_peak = result.runtime.memory.peak_reserved_bytes;
    drop(result);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let spilled = with_spill(
        prepared_set(&repeated_union(), 1, GRANT),
        &directory,
        128 << 20,
    );
    let baseline = spilled.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let result = execute_repeated(&spilled).unwrap();
        let input = result.input.as_ref().unwrap();
        let spill = result.spill.as_ref().unwrap();
        assert_eq!(input.replay_spilled_sources, 1);
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
        assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(result.runtime.completed_executions, execution);
        eprintln!(
            "{}",
            json!({"family":"streamed_union_all_source_replay", "execution":execution,
            "input_rows":input.rows,"input_logical_bytes":input.input_logical_bytes,"grant":GRANT,
            "peak_reserved_bytes":result.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
            "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
            "membership_spill":false,"complete_output_rows":result.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(result);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
    }
}
