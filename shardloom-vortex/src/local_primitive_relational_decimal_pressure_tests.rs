//! Decimal policies survive real retained-state pressure and native run cleanup.

use super::*;

const ROWS: usize = 24_013;
const GRANT: u64 = 16 << 20;

fn key(group: usize) -> String {
    format!("{group:08}{}", "d".repeat(2296))
}

fn coefficient(row: usize) -> i128 {
    let magnitude = if row % 3 == 2 { 125 } else { 124 };
    if (row / 3).is_multiple_of(2) {
        magnitude
    } else {
        -magnitude
    }
}

fn fixture() -> Fixture {
    let batch = |start: usize, end: usize| {
        let rows: Vec<_> = (start..end).map(|row| ROWS - row - 1).collect();
        StructArray::new(
            FieldNames::from(["id", "group", "amount"]),
            vec![
                PrimitiveArray::from_iter(rows.iter().map(|&row| u64::try_from(row).unwrap()))
                    .into_array(),
                VarBinViewArray::from_iter_str(rows.iter().map(|&row| key(row / 3))).into_array(),
                DecimalArray::from_iter(rows.into_iter().map(coefficient), DecimalDType::new(8, 2))
                    .into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .into_array()
    };
    let directory = std::env::temp_dir().join(format!(
        "shardloom-decimal-pressure-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = fs::File::create(fixture.path()).unwrap();
    // Independently owned leaves keep fixture scan buffers below the query grant.
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                ROWS.div_ceil(251),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, batch(0, 0).dtype().clone());
    for start in (0..ROWS).step_by(251) {
        writer.push(batch(start, (start + 251).min(ROWS))).unwrap();
    }
    writer.finish().unwrap();
    assert!(fs::metadata(fixture.path()).unwrap().len() > GRANT);
    fixture
}

fn measures() -> Vec<Measure> {
    vec![
        declared("sum", "total", 2, DecimalRounding::Exact),
        declared("avg", "mean", 2, DecimalRounding::HalfEven),
        declared("avg_distinct", "distinct", 2, DecimalRounding::HalfAway),
        declared("median", "median", 2, DecimalRounding::Exact),
        declared("quantile_cont", "q75", 2, DecimalRounding::Floor)
            .with_quantile_level(0.75)
            .unwrap(),
    ]
}

fn plan(fixture: &Fixture, window: bool) -> VortexRelationalPlan {
    if !window {
        return aggregate(fixture.scan(), &["group"], measures());
    }
    VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: fixture.scan(),
        columns: ["id", "group"]
            .map(|name| ColumnRef::new(name).unwrap())
            .to_vec(),
        expressions: measures()
            .into_iter()
            .map(|measure| VortexRelationalWindowExpression {
                output_column: measure.alias.clone(),
                function: VortexRelationalWindowFunction::Framed(
                    VortexRelationalFrameFunction::Aggregate(Box::new(measure)),
                ),
                partition_by: vec![],
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("id").unwrap(),
                    descending: false,
                    nulls: None,
                }],
                frame: Some(VortexRelationalWindowFrame {
                    unit: VortexRelationalFrameUnit::Rows,
                    start: VortexRelationalFrameBound::Preceding(
                        VortexRelationalFrameOffset::Number(ScalarValue::Int64(2)),
                    ),
                    end: VortexRelationalFrameBound::CurrentRow,
                    exclusion: VortexRelationalFrameExclusion::NoOthers,
                }),
            })
            .collect(),
    }))
}

fn prepare(
    fixture: &Fixture,
    window: bool,
    grant: u64,
    quota: Option<u64>,
) -> PreparedVortexRelational {
    let mut settings = policy();
    settings.resource_envelope.memory_budget_bytes = grant;
    let prepared = prepare_relational(&plan(fixture, window), settings).unwrap();
    if let Some(quota) = quota {
        let workspace = fixture.0.join("decimal-runs");
        fs::create_dir_all(&workspace).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(workspace, quota, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

// Independent integer rational reference: all denominators here are <=4.
fn nearest(numerator: i128, denominator: i128, even: bool) -> i128 {
    let quotient = numerator / denominator;
    let twice = (numerator % denominator).abs() * 2;
    let increment = twice > denominator || (twice == denominator && (!even || quotient % 2 != 0));
    quotient + if increment { numerator.signum() } else { 0 }
}

fn expected(values: &[i128]) -> serde_json::Value {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let total: i128 = values.iter().sum();
    let count = i128::try_from(values.len()).unwrap();
    let mean = nearest(total, count, true);
    let median = if sorted.len().is_multiple_of(2) {
        assert_eq!((sorted[0] + sorted[1]) % 2, 0);
        sorted[0].midpoint(sorted[1])
    } else {
        sorted[sorted.len() / 2]
    };
    let rank = 3 * (sorted.len() - 1);
    let lower = rank / 4;
    let upper = rank.div_ceil(4);
    let q75 = (sorted[lower] * 4
        + (sorted[upper] - sorted[lower]) * i128::try_from(rank % 4).unwrap())
    .div_euclid(4);
    sorted.dedup();
    let distinct = nearest(
        sorted.iter().sum(),
        i128::try_from(sorted.len()).unwrap(),
        false,
    );
    let value = |value| format!("decimal128(8,2):{value}");
    json!({"total":value(total),"mean":value(mean),"distinct":value(distinct),
           "median":value(median),"q75":value(q75)})
}

fn execute(prepared: &PreparedVortexRelational, window: bool) -> Result<ExecutedVortexRelational> {
    let mut delivered = 0;
    let report =
        prepared.for_each_json_batch(&CancellationToken::default(), 127, 1 << 20, |batch| {
            for actual in
                serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap()
            {
                let (group, values) = if window {
                    let id = ROWS - delivered - 1;
                    (
                        id / 3,
                        (id.saturating_sub(2)..=id)
                            .map(coefficient)
                            .collect::<Vec<_>>(),
                    )
                } else {
                    let group = ROWS.div_ceil(3) - delivered - 1;
                    (
                        group,
                        (group * 3..((group + 1) * 3).min(ROWS))
                            .map(coefficient)
                            .collect(),
                    )
                };
                let mut wanted = expected(&values);
                wanted["group"] = json!(key(group));
                if window {
                    wanted["id"] = json!(ROWS - delivered - 1);
                }
                assert_eq!(actual, wanted, "window={window}, row={delivered}");
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(delivered, if window { ROWS } else { ROWS.div_ceil(3) });
    assert_eq!(report.output_rows, u64::try_from(delivered).unwrap());
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    Ok(report)
}

#[test]
fn native_decimal_output_pressure_uses_actual_runs_and_matches_ample_memory() {
    let fixture = fixture();
    for window in [false, true] {
        let denied = prepare(&fixture, window, GRANT, None);
        let baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, window)
            .err()
            .expect("resident state exceeds grant");
        assert!(error.to_string().contains("reservation denied"), "{error}");
        assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(denied.snapshot().completed_executions, 0);
        drop(denied);
        let ample = prepare(&fixture, window, 256 << 20, None);
        let baseline = ample.snapshot().memory.reserved_bytes;
        drop(execute(&ample, window).unwrap());
        assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
        drop(ample);
        let stored = prepare(&fixture, window, GRANT, Some(256 << 20));
        let baseline = stored.snapshot().memory.reserved_bytes;
        let report = execute(&stored, window).unwrap();
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
        eprintln!(
            "{}",
            json!({"window":window,"input_rows":ROWS,"output_rows":report.output_rows,
            "grant":GRANT,"peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,
            "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,
            "peak_disk_bytes":spill.peak_disk_bytes,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(report);
        assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn native_decimal_output_spill_faults_release_state_and_allow_retry() {
    let fixture = fixture();
    for window in [false, true] {
        let prepared = prepare(&fixture, window, GRANT, Some(256 << 20));
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for cancel in [false, true] {
            let token = CancellationToken::default();
            let mut calls = 0;
            let error = prepared
                .for_each_json_batch(&token, 127, 1 << 20, |_| {
                    calls += 1;
                    if cancel {
                        token.cancel();
                        Ok(())
                    } else {
                        Err(shardloom_core::ShardLoomError::InvalidOperation(
                            "decimal consumer refused".into(),
                        ))
                    }
                })
                .err()
                .unwrap();
            assert!(
                error.to_string().contains(if cancel {
                    "cancel"
                } else {
                    "decimal consumer refused"
                }),
                "{error}"
            );
            assert_eq!(calls, 1);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(
                fs::read_dir(fixture.0.join("decimal-runs"))
                    .unwrap()
                    .count(),
                0
            );
        }
        let denied = prepare(&fixture, window, GRANT, Some(32 << 10));
        let denied_baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, window).err().expect("quota refusal");
        assert!(error.to_string().contains("quota"), "{error}");
        assert_eq!(denied.snapshot().memory.reserved_bytes, denied_baseline);
        assert_eq!(denied.snapshot().completed_executions, 0);
        assert_eq!(
            fs::read_dir(fixture.0.join("decimal-runs"))
                .unwrap()
                .count(),
            0
        );
        drop(denied);
        drop(execute(&prepared, window).unwrap());
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("decimal-runs"))
                .unwrap()
                .count(),
            0
        );
    }
}
