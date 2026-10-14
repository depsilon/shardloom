//! Repeated wide input exceeds one grant; only explicit native replay spill wins.

use super::*;
use std::cell::Cell;

#[path = "local_primitive_relational_batch_replay_fault_tests.rs"]
mod fault_tests;

const ROWS: usize = 8193;
const WIDTH: usize = 2048;
const INPUT_ROWS: usize = 251;
const GRANT: u64 = 16 << 20;

fn key(row: usize) -> i64 {
    i64::try_from(row * 17 % ROWS).unwrap()
}

fn text(row: usize) -> String {
    format!("{row:08}{}", "p".repeat(WIDTH - 8))
}

fn plan() -> VortexRelationalPlan {
    let right = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new("memory://stream").unwrap(),
        projection: shardloom_plan::ProjectionRequest::Columns(vec![ColumnRef::new("n").unwrap()]),
        predicate: None,
    });
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan(),
        right,
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("s").unwrap(),
                output_column: "s".into(),
            },
        ],
    }))
}

fn prepared(fixture: &Fixture, grant: u64, spill: bool) -> PreparedVortexRelational {
    let prepared = prepare(&plan(), grant).unwrap();
    if !spill {
        return prepared;
    }
    let directory = fixture.0.join("replay-runs");
    fs::create_dir_all(&directory).unwrap();
    prepared
        .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
        .unwrap()
}

struct Producer {
    rows: usize,
    calls: usize,
    prior: Option<Weak<MemoryLease>>,
    ended: Cell<bool>,
}

impl Producer {
    fn new() -> Self {
        Self {
            rows: 0,
            calls: 0,
            prior: None,
            ended: Cell::new(false),
        }
    }

    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
        assert!(!self.ended.get(), "producer replayed after end");
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        self.calls += 1;
        if self.rows == ROWS {
            self.ended.set(true);
            return Ok(None);
        }
        let end = (self.rows + INPUT_ROWS).min(ROWS);
        let numbers = (self.rows..end)
            .map(|row| Some(key(row)))
            .collect::<Vec<_>>();
        let strings = (self.rows..end).map(text).collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let batch = source(session, &numbers, &refs)?;
        self.prior = Some(batch.batch_release_witness()?);
        self.rows = end;
        Ok(Some(batch))
    }
}

fn execute(prepared: &PreparedVortexRelational) -> Result<ExecutedVortexRelational> {
    let mut producer = Producer::new();
    let mut delivered = 0;
    let result = prepared
        .with_batch_input(&mut |session| producer.next(session))?
        .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
            for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                assert_eq!(row, json!({"n":key(delivered),"s":text(delivered)}));
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(producer.rows, ROWS);
    assert_eq!(producer.calls, ROWS.div_ceil(INPUT_ROWS) + 1);
    assert!(producer.ended.get());
    assert_eq!(delivered, ROWS);
    let input = result.input.as_ref().unwrap();
    assert_eq!(input.rows, ROWS as u64);
    assert_eq!(input.payload_batches, ROWS.div_ceil(INPUT_ROWS) as u64);
    assert_eq!(input.max_retained_input_batches, 1);
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
fn streaming_replay_pressure_denies_resident_and_completes_native_spill_under_same_grant() {
    let fixture = ordinary_file();
    let denied = prepared(&fixture, GRANT, false);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied)
        .err()
        .expect("complete replay cannot fit this grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    drop(denied);

    let ample = prepared(&fixture, 128 << 20, false);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = execute(&ample).unwrap();
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    assert_eq!(control.input.as_ref().unwrap().replay_spilled_sources, 0);
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let spilled = prepared(&fixture, GRANT, true);
    let baseline = spilled.snapshot().memory.reserved_bytes;
    for execution in 0..2 {
        let result = execute(&spilled).unwrap();
        let input = result.input.as_ref().unwrap();
        let spill = result.spill.as_ref().unwrap();
        assert_eq!(input.replay_spilled_sources, 1);
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
        eprintln!(
            "{}",
            json!({"family":"streamed_source_replay", "execution":execution,
            "input_rows":ROWS,"input_logical_bytes":input.input_logical_bytes,"grant":GRANT,
            "spilled_peak_reserved_bytes":result.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
            "replay_buffer_bytes_copied":input.replay_buffer_bytes_copied,
            "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
            "complete_output_rows":result.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(result);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_replay_pressure_native_writer_and_reopen_share_the_same_grant() {
    let fixture = ordinary_file();
    let prepared = prepared(&fixture, GRANT, true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut producer = Producer::new();
    let path = fixture.0.join("replay-pressure.vortex");
    let written = prepared
        .with_batch_input(&mut |session| producer.next(session))
        .unwrap()
        .write_controlled(&path, Format::Vortex, false, &CancellationToken::default())
        .unwrap();
    assert_eq!(producer.rows, ROWS);
    assert_eq!(producer.calls, ROWS.div_ceil(INPUT_ROWS) + 1);
    assert!(producer.ended.get());
    assert_eq!(written.output.rows_written, ROWS as u64);
    let sink = written.output.evidence.native_array_sink.as_ref().unwrap();
    assert!(sink.native_arrays_submitted < ROWS as u64 / 8);
    assert!(sink.metadata_reserved_bytes < GRANT / 2);
    assert!(sink.adapter_payload_bytes_copied > GRANT);
    assert!(sink.scalar_values_materialized > ROWS as u64);
    let input = written.execution.input.as_ref().unwrap();
    assert_eq!(input.replay_spilled_sources, 1);
    assert_eq!(input.replay_passes, 1);
    assert_eq!(input.replay_rows, ROWS as u64);
    let spill = written.execution.spill.as_ref().unwrap();
    assert!(spill.runs_written > 3 && spill.merge_passes > 1);
    assert!(spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    assert!(written.execution.runtime.memory.peak_reserved_bytes <= GRANT);
    assert!(written.execution.native_io_certificate.is_certified());
    assert!(
        !written
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    eprintln!(
        "{}",
        json!({"family":"streamed_source_replay_writer",
        "input_rows":ROWS,"grant":GRANT,
        "peak_reserved_bytes":written.execution.runtime.memory.peak_reserved_bytes,
        "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,
        "output_rows":written.output.rows_written})
    );
    drop(written);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);

    let mut resource_policy = policy();
    resource_policy.resource_envelope.memory_budget_bytes = GRANT;
    let reopened = prepare_relational(
        &other_scan(DatasetUri::new(path.to_string_lossy()).unwrap()),
        resource_policy,
    )
    .unwrap();
    assert_eq!(reopened.output_dtype(), prepared.output_dtype());
    let reopened_baseline = reopened.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let report = reopened
        .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
            for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                assert_eq!(row, json!({"n":key(delivered),"s":text(delivered)}));
                delivered += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(delivered, ROWS);
    assert!(report.native_io_certificate.is_certified());
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    drop(report);
    assert_eq!(reopened.snapshot().memory.reserved_bytes, reopened_baseline);
}
