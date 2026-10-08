//! Complete one-shot partitions whose retained payload exceeds the native grant.

use super::*;

#[path = "local_primitive_relational_batch_window_fault_tests.rs"]
mod fault_tests;

const ROWS: usize = 24_013;
const GRANT: u64 = 16 << 20;
const BATCH: usize = 997;

fn text(row: usize) -> String {
    format!("{row:08}{}", "x".repeat(760))
}

struct LargeInput {
    total: usize,
    delivered: usize,
    calls: usize,
    prior: Option<Weak<MemoryLease>>,
}

impl LargeInput {
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
                .is_none_or(|prior| prior.strong_count() == 0)
        );
        self.calls += 1;
        if self.delivered == self.total {
            return Ok(None);
        }
        let end = (self.delivered + BATCH).min(self.total);
        let numbers = (self.delivered..end)
            .map(|row| Some(i64::try_from(self.total - 1 - row).unwrap()))
            .collect::<Vec<_>>();
        let strings = (self.delivered..end).map(text).collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let batch = source(session, &numbers, &refs)?;
        self.prior = Some(batch.batch_release_witness()?);
        self.delivered = end;
        Ok(Some(batch))
    }
}

fn plan() -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: scan(),
        columns: vec![column("n"), column("s")],
        expressions: [
            ("distinct", FrameFunction::CountDistinct(column("n"))),
            ("minimum", FrameFunction::Min(column("n"))),
            ("maximum", FrameFunction::Max(column("n"))),
            ("sum", FrameFunction::Sum(column("n"))),
        ]
        .into_iter()
        .map(|(name, function)| expression(name, Function::Framed(function), "n", false))
        .collect(),
    }))
}

fn spill(directory: &std::path::Path, quota: u64) -> VortexRelationalSpillPolicy {
    VortexRelationalSpillPolicy::new(directory, quota, 1 << 20).unwrap()
}

fn prepare_window(
    directory: &std::path::Path,
    grant: u64,
    quota: Option<u64>,
) -> PreparedVortexRelational {
    let prepared = prepare(&plan(), grant).unwrap();
    if let Some(quota) = quota {
        prepared.with_spill(spill(directory, quota)).unwrap()
    } else {
        prepared
    }
}

fn fixture() -> Fixture {
    let fixture = Fixture::new(
        single("n", PrimitiveArray::from_iter([1i64]).into_array()),
        1,
    );
    fs::create_dir(fixture.0.join("window-runs")).unwrap();
    fixture
}

#[allow(clippy::cast_precision_loss)] // Values and integer totals are below 2^53.
fn check_batch(
    batch: &crate::local_primitives::collect::SerializedVortexResultBatch,
    total: usize,
    delivered: &mut usize,
) {
    let values = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
    assert_eq!(values.len(), batch.rows);
    for row in values {
        let position = total - 1 - *delivered;
        assert_eq!(
            row,
            json!({"n":position,"s":text(*delivered),"distinct":position + 1,"minimum":0,"maximum":position,"sum":(position * (position + 1) / 2) as f64})
        );
        *delivered += 1;
    }
}

fn complete(prepared: &PreparedVortexRelational, total: usize) -> Result<ExecutedVortexRelational> {
    let mut input = LargeInput::new(total);
    let ended = Cell::new(false);
    let mut producer = |session: &ResidentVortexSession| {
        let result = input.next(session)?;
        ended.set(result.is_none());
        Ok(result)
    };
    let mut delivered = 0;
    let report = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 509, 4 << 20, |batch| {
            assert!(ended.get(), "window results require complete source input");
            check_batch(&batch, total, &mut delivered);
            Ok(())
        })?;
    assert_eq!(delivered, total);
    assert_eq!(input.calls, total.div_ceil(BATCH) + 1);
    assert_eq!(input.prior.as_ref().unwrap().strong_count(), 0);
    let source = report.input.as_ref().unwrap();
    assert_eq!(source.window_rows_detached, total as u64);
    assert_eq!(source.payload_batches, total.div_ceil(BATCH) as u64);
    assert_eq!(
        source.window_batches_detached,
        ((total / BATCH) * BATCH.div_ceil(509) + (total % BATCH).div_ceil(509)) as u64
    );
    assert!(source.end_of_input_observed && source.output_ownership_detached);
    assert_eq!(source.max_retained_input_batches, 1);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    Ok(report)
}

#[test]
fn streaming_window_pressure_exceeds_grant_completes_exactly_and_refunds_every_input_owner() {
    let fixture = fixture();
    let directory = fixture.0.join("window-runs");
    let resident = prepare_window(&directory, GRANT, None);
    let baseline = resident.snapshot().memory.reserved_bytes;
    let error = complete(&resident, ROWS)
        .err()
        .expect("resident retained window exceeds the grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(resident.snapshot().completed_executions, 0);
    assert_eq!(resident.snapshot().memory.reserved_bytes, baseline);
    drop(resident);

    let ample = prepare_window(&directory, 128 << 20, None);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = complete(&ample, ROWS).unwrap();
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    assert_eq!(control.ordered_window_stages, 0);
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let prepared = prepare_window(&directory, GRANT, Some(256 << 20));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let report = complete(&prepared, ROWS).unwrap();
    let source = report.input.as_ref().unwrap();
    assert!(source.input_logical_bytes > GRANT);
    assert!(source.max_retained_input_logical_bytes < 1 << 20);
    assert_eq!(report.ordered_window_stages, 1);
    assert_eq!(report.ordered_window_input_rows, ROWS as u64);
    assert_eq!(report.ordered_window_bounds_rows, 3 * ROWS as u64);
    assert_eq!(report.ordered_window_distinct_intervals, ROWS as u64);
    assert_eq!(report.ordered_window_distinct_events, 2 * ROWS as u64);
    assert!(report.ordered_window_extrema_summary_rows > ROWS as u64);
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    let spill = report.spill.as_ref().unwrap();
    assert!(spill.runs_written > 3 && spill.merge_passes > 1);
    assert!(spill.peak_disk_bytes <= spill.quota_bytes && spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    eprintln!(
        "{}",
        json!({"family":"streamed_native_window_pressure","input_rows":ROWS,"input_logical_bytes":source.input_logical_bytes,"grant":GRANT,"spilled_peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,"runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,"window_rows_detached":source.window_rows_detached,"window_batches_detached":source.window_batches_detached,"complete_output_rows":report.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
    );
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn streaming_window_large_native_output_reopens_every_row_after_spill_cleanup() {
    let fixture = fixture();
    let directory = fixture.0.join("window-runs");
    let prepared = prepare_window(&directory, GRANT, Some(256 << 20));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let target = fixture.0.join("complete.vortex");
    let mut input = LargeInput::new(ROWS);
    let mut producer = |session: &ResidentVortexSession| input.next(session);
    let written = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .write_controlled(
            &target,
            Format::Vortex,
            false,
            &CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(written.output.rows_written, ROWS as u64);
    assert!(
        written
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    assert!(written.execution.spill.as_ref().unwrap().runs_written > 3);
    assert!(
        written
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    assert_eq!(input.calls, ROWS.div_ceil(BATCH) + 1);
    assert_eq!(input.prior.as_ref().unwrap().strong_count(), 0);
    assert!(written.execution.runtime.memory.peak_reserved_bytes <= GRANT);
    let reopened = prepare_relational(
        &VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(target.display().to_string()).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        policy(),
    )
    .unwrap();
    assert_eq!(reopened.output_dtype(), prepared.output_dtype());
    let mut delivered = 0;
    reopened
        .for_each_json_batch(&CancellationToken::default(), 509, 4 << 20, |batch| {
            check_batch(&batch, ROWS, &mut delivered);
            Ok(())
        })
        .unwrap();
    assert_eq!(delivered, ROWS);
    drop(written);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
}
