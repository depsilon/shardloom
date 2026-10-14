//! Complete wide streamed build state under constrained and ample native grants.

use super::*;
use std::cell::Cell;

const ROWS: usize = 65_537;
const GRANT: u64 = 16 << 20;
const INPUT_ROWS: usize = 1024;

fn key(ordinal: usize) -> usize {
    ordinal * 17 % ROWS
}
fn number(ordinal: usize) -> Option<i64> {
    (!ordinal.is_multiple_of(31)).then(|| i64::try_from(key(ordinal)).unwrap())
}
fn text(ordinal: usize) -> String {
    format!("{ordinal:09}{}", "x".repeat(503))
}

fn prepare_join(
    plan: &VortexRelationalPlan,
    fixture: &Fixture,
    spill: bool,
    grant: u64,
    streamed_probe: bool,
) -> PreparedVortexRelational {
    let prepared = if streamed_probe {
        let mut resources = policy();
        resources.resource_envelope.memory_budget_bytes = grant;
        prepare_relational_with_schema(resources, |schemas| {
            for uri in ["memory://stream", "memory://probe"] {
                schemas.register_batch_source(DatasetUri::new(uri)?, |session| {
                    source(session, &[], &[])
                })?;
            }
            Ok(plan.clone())
        })
    } else {
        prepare(plan, grant)
    }
    .unwrap();
    if spill {
        let path = fixture.0.join("join-runs");
        fs::create_dir_all(&path).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(path, 256 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

fn execute(
    prepared: &PreparedVortexRelational,
    validate: bool,
) -> Result<ExecutedVortexRelational> {
    let mut next = 0;
    let ended = Cell::new(false);
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut probe_calls = 0;
    let mut build_calls = 0;
    let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        assert!(
            prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        if uri.as_str() == "memory://probe" {
            assert!(ended.get());
            assert_eq!(ordinal, probe_calls);
            probe_calls += 1;
            if ordinal == 1 {
                return Ok(None);
            }
            let batch = source(
                session,
                &ORDINARY.map(|row| row.0),
                &ORDINARY.map(|row| Some(row.1)),
            )?;
            prior = Some(batch.batch_release_witness()?);
            return Ok(Some(batch));
        }
        assert_eq!(uri.as_str(), "memory://stream");
        assert_eq!(ordinal, build_calls);
        build_calls += 1;
        if next == ROWS {
            ended.set(true);
            return Ok(None);
        }
        let end = (next + INPUT_ROWS).min(ROWS);
        let numbers = (next..end).map(number).collect::<Vec<_>>();
        let strings = (next..end).map(text).collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let batch = source(session, &numbers, &refs)?;
        prior = Some(batch.batch_release_witness()?);
        next = end;
        Ok(Some(batch))
    };
    let matched = [1, 3].map(|key| {
        (0..ROWS)
            .find(|ordinal| number(*ordinal) == Some(key))
            .unwrap()
    });
    let mut unmatched = (0..ROWS).filter(|ordinal| !matched.contains(ordinal));
    let mut delivered = 0;
    let result = prepared.with_batch_inputs(&mut producer)?.for_each_batch(&CancellationToken::default(), |array, context| {
        assert!(ended.get(), "build-side end must precede any result");
        if validate {
            for row in values(&array, context)? {
                let expected = match delivered {
                    0 => json!({"left":"r1","right":text(matched[0])}),
                    1 => json!({"left":"r3","right":text(matched[1])}),
                    2 => json!({"left":"rn","right":null}),
                    _ => json!({"left":null,"right":text(unmatched.next().expect("unexpected output row"))}),
                };
                assert_eq!(row, expected, "row={delivered}");
                delivered += 1;
            }
        }
        Ok(())
    })?;
    assert_eq!(next, ROWS);
    assert_eq!(build_calls, ROWS.div_ceil(INPUT_ROWS) as u64 + 1);
    assert_eq!(
        probe_calls,
        if prepared.batch_sources.len() == 2 {
            2
        } else {
            0
        }
    );
    assert_eq!(
        result.input.as_ref().unwrap().sources,
        prepared.batch_sources.len()
    );
    assert_eq!(delivered, ROWS + 1);
    assert!(unmatched.next().is_none());
    assert!(result.input.as_ref().unwrap().input_logical_bytes > 2 * GRANT);
    assert!(result.input.as_ref().unwrap().end_of_input_observed);
    Ok(result)
}

#[test]
fn streaming_join_pressure_denies_resident_and_completes_spill_under_the_same_grant() {
    check_pressure(false);
}

#[test]
fn multiple_streams_join_pressure_denies_resident_and_completes_spill_under_the_same_grant() {
    check_pressure(true);
}

fn check_pressure(streamed_probe: bool) {
    let fixture = ordinary_file();
    let probe = if streamed_probe {
        other_scan(DatasetUri::new("memory://probe").unwrap())
    } else {
        fixture.scan()
    };
    let plan = joined(probe, true, JoinKind::Full);
    let denied = prepare_join(&plan, &fixture, false, GRANT, streamed_probe);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied, false)
        .err()
        .expect("resident build must exceed this grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    drop(denied);

    let ample = prepare_join(&plan, &fixture, false, 128 << 20, streamed_probe);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = execute(&ample, true).unwrap();
    assert_eq!(control.ordered_join_stages, 0);
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    let input_bytes = control.input.as_ref().unwrap().input_logical_bytes;
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let spilled = prepare_join(&plan, &fixture, true, GRANT, streamed_probe);
    let baseline = spilled.snapshot().memory.reserved_bytes;
    let result = execute(&spilled, true).unwrap();
    let spill = result.spill.as_ref().unwrap();
    assert!(
        spill.runs_written > 3 && spill.merge_passes > 1,
        "{spill:?}"
    );
    assert!(spill.owned_cleanup_completed);
    assert!(spill.peak_disk_bytes <= spill.quota_bytes);
    assert!(spill.max_open_runs <= 4);
    assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
    assert_eq!(result.ordered_join_build_rows, ROWS as u64);
    assert_eq!(result.ordered_join_probe_rows, 3);
    assert_eq!(result.ordered_join_match_records, 2);
    assert_eq!(
        result.input.as_ref().unwrap().join_build_rows_detached,
        ROWS as u64
    );
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    eprintln!(
        "{}",
        json!({"family":"streamed_full_join_build_and_outer_restore", "streamed_probe":streamed_probe,"input_rows":ROWS,"input_logical_bytes":input_bytes,"grant":GRANT,
        "spilled_peak_reserved_bytes":result.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
        "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
        "lookup_blocks":result.ordered_join_lookup_blocks,"complete_output_rows":result.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
    );
    drop(result);
    assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
}
