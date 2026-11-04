//! A complete large partition with exact analytic state and wide retained payload.

use super::*;
use crate::relational_query::{
    VortexRelationalOrderKey, VortexRelationalWindow, VortexRelationalWindowExpression,
};
use vortex::array::arrays::VarBinViewArray;

const ROWS: usize = 24_013;
const GRANT: u64 = 16 << 20;

fn text(row: usize) -> String {
    format!("{row:08}{}", "x".repeat(760))
}

fn fixture() -> Fixture {
    let batch = |start: usize, end: usize| {
        StructArray::new(
            FieldNames::from(["id", "position", "value", "pad"]),
            vec![
                PrimitiveArray::from_iter(start as u64..end as u64).into_array(),
                PrimitiveArray::from_iter((start..end).map(|row| (ROWS - 1 - row) as u64))
                    .into_array(),
                PrimitiveArray::from_option_iter((start..end).map(|row| {
                    let position = ROWS - 1 - row;
                    (!position.is_multiple_of(17)).then(|| i64::try_from(position).unwrap())
                }))
                .into_array(),
                VarBinViewArray::from_iter_str((start..end).map(text)).into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .into_array()
    };
    // Slices of a VarBinView retain all backing bytes. Construct each physical
    // leaf independently so the fixture itself has bounded scan buffers.
    let directory = std::env::temp_dir().join(format!(
        "shardloom-window-pressure-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = fs::File::create(fixture.path()).unwrap();
    let dtype = batch(0, 0).dtype().clone();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                ROWS.div_ceil(997),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, dtype);
    for start in (0..ROWS).step_by(997) {
        writer.push(batch(start, (start + 997).min(ROWS))).unwrap();
    }
    writer.finish().unwrap();
    let bytes = fs::metadata(fixture.path()).unwrap().len();
    assert!(bytes > GRANT && bytes < 32 << 20, "fixture bytes={bytes}");
    fixture
}

fn plan(fixture: &Fixture) -> VortexRelationalPlan {
    let value = ColumnRef::new("value").unwrap();
    VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: fixture.scan(),
        columns: ["id", "pad"]
            .map(|name| ColumnRef::new(name).unwrap())
            .to_vec(),
        expressions: [
            ("distinct", Function::CountDistinct(value.clone())),
            ("min", Function::Min(value.clone())),
            ("max", Function::Max(value.clone())),
            ("sum", Function::Sum(value)),
        ]
        .into_iter()
        .map(|(name, function)| VortexRelationalWindowExpression {
            output_column: name.into(),
            function: VortexRelationalWindowFunction::Framed(function),
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("position").unwrap(),
                descending: false,
                nulls: Some(NullOrder::Last),
            }],
            frame: Some(Frame {
                unit: Unit::Rows,
                ..Frame::default()
            }),
        })
        .collect(),
    }))
}

fn prepare(
    fixture: &Fixture,
    plan: &VortexRelationalPlan,
    grant: u64,
    spill: bool,
) -> PreparedVortexRelational {
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = grant;
    let prepared = prepare_relational(plan, policy).unwrap();
    if spill {
        let path = fixture.0.join("window-runs");
        fs::create_dir_all(&path).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(path, 256 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

#[allow(clippy::cast_precision_loss)] // These integer totals are bounded well below 2^53.
fn execute(prepared: &PreparedVortexRelational) -> Result<ExecutedVortexRelational> {
    let mut delivered = 0;
    let report =
        prepared.for_each_json_batch(&CancellationToken::default(), 509, 4 << 20, |batch| {
            let rows = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
            for row in rows {
                let position = ROWS - 1 - delivered;
                let nulls_after_first = position / 17;
                let count = position - nulls_after_first;
                let sum = (position * (position + 1)
                    - 17 * nulls_after_first * (nulls_after_first + 1))
                    / 2;
                let maximum = if position.is_multiple_of(17) {
                    position.checked_sub(1)
                } else {
                    Some(position)
                };
                assert_eq!(
                    row,
                    json!({
                        "id":delivered, "pad":text(delivered), "distinct":count,
                        "min":(count > 0).then_some(1), "max":maximum,
                        "sum":(count > 0).then_some(sum as f64)
                    }),
                    "original ordinal={delivered}"
                );
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(delivered, ROWS);
    assert_eq!(report.output_rows, ROWS as u64);
    Ok(report)
}

#[test]
fn ordered_window_pressure_denies_resident_and_completes_with_shared_native_spill() {
    let fixture = fixture();
    let plan = plan(&fixture);
    let denied = prepare(&fixture, &plan, GRANT, false);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied)
        .err()
        .expect("resident partition exceeds the grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(denied.snapshot().completed_executions, 0);
    drop(denied);

    let ample = prepare(&fixture, &plan, 128 << 20, false);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = execute(&ample).unwrap();
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    assert_eq!(control.ordered_window_stages, 0);
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let ordered = prepare(&fixture, &plan, GRANT, true);
    let baseline = ordered.snapshot().memory.reserved_bytes;
    let report = execute(&ordered).unwrap();
    let spill = report.spill.as_ref().unwrap();
    assert_eq!(report.ordered_window_stages, 1);
    assert_eq!(report.ordered_window_input_rows, ROWS as u64);
    assert_eq!(report.ordered_window_partitions, 1);
    assert_eq!(report.ordered_window_bounds_rows, 3 * ROWS as u64);
    assert!(report.ordered_window_distinct_intervals > 0);
    assert_eq!(
        report.ordered_window_distinct_events,
        2 * report.ordered_window_distinct_intervals
    );
    assert!(report.ordered_window_extrema_summary_rows > 0);
    assert!(
        spill.runs_written > 3 && spill.merge_passes > 1,
        "{spill:?}"
    );
    assert!(spill.owned_cleanup_completed);
    assert!(spill.peak_disk_bytes <= spill.quota_bytes);
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    eprintln!(
        "{}",
        json!({"family":"native_window_distinct_extrema_and_wide_payload", "input_rows":ROWS,"grant":GRANT,
        "spilled_peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
        "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,"peak_disk_bytes":spill.peak_disk_bytes,
        "bounds_rows":report.ordered_window_bounds_rows,"distinct_intervals":report.ordered_window_distinct_intervals,
        "distinct_events":report.ordered_window_distinct_events,"extrema_summary_rows":report.ordered_window_extrema_summary_rows,
        "lookup_blocks":report.ordered_window_lookup_blocks,"complete_output_rows":report.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
    );
    drop(report);
    assert_eq!(ordered.snapshot().memory.reserved_bytes, baseline);
}
