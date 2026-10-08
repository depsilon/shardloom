//! Complete larger-than-grant input for both group and single-group DISTINCT state.

use super::*;

const ROWS: usize = 133_137;
const GRANT: u64 = 16 << 20;
const INPUT_ROWS: usize = 1024;

fn key(ordinal: usize) -> usize {
    ordinal * 17 % ROWS
}
fn text(key: usize) -> String {
    format!("{key:09}{}", "x".repeat(503))
}
fn number(key: usize) -> Option<i64> {
    (!key.is_multiple_of(31)).then(|| i64::try_from(key).unwrap())
}

fn plan(grouped: bool) -> VortexRelationalPlan {
    if grouped {
        aggregate(
            scan(),
            &["s"],
            vec![
                measure("count", None, "rows"),
                measure("count_distinct", Some("n"), "distinct"),
                measure("sum", Some("n"), "sum"),
            ],
        )
    } else {
        aggregate(
            scan(),
            &[],
            vec![
                measure("count", None, "rows"),
                measure("count", Some("n"), "present"),
                measure("count_distinct", Some("s"), "distinct"),
                measure("sum", Some("n"), "sum"),
                measure("avg", Some("n"), "avg"),
                measure("min", Some("n"), "min"),
                measure("max", Some("n"), "max"),
            ],
        )
    }
}

fn input(session: &ResidentVortexSession, start: usize) -> Result<ResidentMemorySource> {
    let end = (start + INPUT_ROWS).min(ROWS);
    let nums = (start..end).map(|row| number(key(row))).collect::<Vec<_>>();
    let texts = (start..end).map(|row| text(key(row))).collect::<Vec<_>>();
    let refs = texts
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    source(session, &nums, &refs)
}

fn execute(
    prepared: &PreparedVortexRelational,
    grouped: bool,
    validate: bool,
) -> Result<ExecutedVortexRelational> {
    let mut cursor = 0;
    let ended = Cell::new(false);
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut producer = |session: &ResidentVortexSession| {
        assert!(
            prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        if cursor == ROWS {
            ended.set(true);
            return Ok(None);
        }
        let batch = input(session, cursor)?;
        cursor = (cursor + INPUT_ROWS).min(ROWS);
        prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    };
    let mut delivered = 0;
    let mut sum = 0.0_f64;
    let mut count = 0_u64;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for ordinal in 0..ROWS {
        if let Some(value) = number(key(ordinal)) {
            sum += f64::from(u32::try_from(value).unwrap());
            count += 1;
            min = min.min(value);
            max = max.max(value);
        }
    }
    let result = prepared.with_batch_input(&mut producer)?.for_each_batch(&CancellationToken::default(),|array,context| {
        assert!(ended.get());
        if validate {
            for row in values(&array,context)? {
                let expected = if grouped {
                    let index = key(delivered);
                    json!({"s":text(index),"rows":1,"distinct":u8::from(number(index).is_some()),"sum":number(index).map(|value| f64::from(u32::try_from(value).unwrap()))})
                } else {
                    assert_eq!(delivered,0);
                    json!({"rows":ROWS,"present":count,"distinct":ROWS,"sum":sum,"avg":sum/f64::from(u32::try_from(count).unwrap()),"min":min,"max":max})
                };
                assert_eq!(row,expected,"grouped={grouped} row={delivered}");
                delivered += 1;
            }
        }
        Ok(())
    })?;
    assert_eq!(delivered, if grouped { ROWS } else { 1 });
    assert_eq!(cursor, ROWS);
    let input = result.input.as_ref().unwrap();
    assert!(input.end_of_input_observed);
    assert!(input.input_logical_bytes > 4 * GRANT);
    assert_eq!(input.rows, ROWS as u64);
    Ok(result)
}

#[test]
fn ordered_aggregate_pressure_large_groups_and_single_group_distinct_complete_with_native_spill() {
    for grouped in [false, true] {
        let fixture = workspace();
        let plan = plan(grouped);
        let denied = prepared(&plan, &fixture, false, GRANT);
        let baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, grouped, false)
            .err()
            .expect("constrained resident aggregate must deny");
        assert!(error.to_string().contains("reservation denied"), "{error}");
        assert_eq!(denied.snapshot().completed_executions, 0);
        assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
        drop(denied);

        let ample = prepared(&plan, &fixture, false, 256 << 20);
        let baseline = ample.snapshot().memory.reserved_bytes;
        let result = execute(&ample, grouped, true).unwrap();
        assert_eq!(result.ordered_aggregate_stages, 0);
        assert!(result.spill.is_none());
        let input_bytes = result.input.as_ref().unwrap().input_logical_bytes;
        let ample_peak = result.runtime.memory.peak_reserved_bytes;
        drop(result);
        assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
        drop(ample);

        let spilled = prepared(&plan, &fixture, true, GRANT);
        let baseline = spilled.snapshot().memory.reserved_bytes;
        let result = execute(&spilled, grouped, true).unwrap();
        let spill = result.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed);
        assert!(spill.peak_disk_bytes <= spill.quota_bytes);
        assert!(result.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(result.ordered_aggregate_stages, 1);
        assert_eq!(result.ordered_aggregate_input_rows, ROWS as u64);
        assert_eq!(
            result.ordered_aggregate_distinct_rows,
            if grouped {
                (0..ROWS).filter(|row| number(key(*row)).is_some()).count() as u64
            } else {
                ROWS as u64
            }
        );
        assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
        eprintln!(
            "{}",
            json!({"family":if grouped {"high_cardinality_groups"} else {"one_group_high_cardinality_distinct"},
            "input_rows":ROWS,"input_logical_bytes":input_bytes,"grant":GRANT,
            "spilled_peak_reserved_bytes":result.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
            "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
            "complete_output_rows":result.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(result);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
    }
}
