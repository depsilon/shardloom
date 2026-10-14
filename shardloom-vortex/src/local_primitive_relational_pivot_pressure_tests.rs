//! Complete state beyond the resident grant, with independent full-row checks.

use super::*;

const GROUPS: usize = 6145;
const VALUES: [f64; 4] = [1e16, 1.0, -1e16, 3.0];
const ROWS: usize = GROUPS * VALUES.len();
const GRANT: u64 = 16 << 20;

fn text(index: usize) -> String {
    format!("{index:08}{}", "p".repeat(760))
}

fn fixture() -> Fixture {
    let batch = |start: usize, end: usize| {
        StructArray::new(
            FieldNames::from(["entity", "category", "amount"]),
            vec![
                VarBinArray::from(
                    (start..end)
                        .map(|row| text(row % GROUPS))
                        .collect::<Vec<_>>(),
                )
                .into_array(),
                VarBinArray::from(vec!["a"; end - start]).into_array(),
                PrimitiveArray::from_iter((start..end).map(|row| VALUES[row / GROUPS]))
                    .into_array(),
            ],
            end - start,
            Validity::NonNullable,
        )
        .into_array()
    };
    let directory = std::env::temp_dir().join(format!(
        "shardloom-pivot-pressure-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = fs::File::create(fixture.path()).unwrap();
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
    // Separate physical leaves prevent the fixture from retaining a full-file
    // backing allocation merely because its rows are scanned in small batches.
    for start in (0..ROWS).step_by(251) {
        writer.push(batch(start, (start + 251).min(ROWS))).unwrap();
    }
    writer.finish().unwrap();
    assert!(fs::metadata(fixture.path()).unwrap().len() > GRANT);
    fixture
}

fn stream_batch(
    session: &ResidentVortexSession,
    start: usize,
    end: usize,
) -> Result<ResidentMemorySource> {
    use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues};
    let entities = (start..end)
        .map(|row| text(row % GROUPS))
        .collect::<Vec<_>>();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "entity",
                values: MemoryColumnValues::Utf8(
                    &entities
                        .iter()
                        .map(|value| Some(value.as_str()))
                        .collect::<Vec<_>>(),
                ),
            },
            MemoryColumn {
                name: "category",
                values: MemoryColumnValues::Utf8(&vec![Some("a"); end - start]),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Float64(
                    &(start..end)
                        .map(|row| Some(VALUES[row / GROUPS]))
                        .collect::<Vec<_>>(),
                ),
            },
        ],
    )
}

fn prepare(fixture: &Fixture, grant: u64, spill: bool, streamed: bool) -> PreparedVortexRelational {
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = grant;
    let uri = DatasetUri::new(if streamed {
        "memory://pivot-pressure".into()
    } else {
        fixture.path().display().to_string()
    })
    .unwrap();
    let mut plan = plan(fixture, "sum");
    if let VortexRelationalPlan::Unary(unary) = &mut plan
        && let VortexRelationalPlan::Scan(scan) = &mut unary.input
    {
        scan.source_uri = uri.clone();
    }
    let prepared = prepare_relational_with_dynamic_inputs(
        std::slice::from_ref(&uri),
        policy,
        65_536,
        |schemas| {
            if streamed {
                schemas
                    .register_batch_source(uri.clone(), |session| stream_batch(session, 0, 0))?;
            }
            Ok(())
        },
        move |schemas| schemas.resolve_output(&plan).map(|(plan, _)| plan),
    )
    .unwrap();
    if spill {
        let workspace = fixture.0.join("pivot-runs");
        fs::create_dir_all(&workspace).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(workspace, 256 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

fn execute(
    prepared: &PreparedVortexRelational,
    streamed: bool,
) -> Result<ExecutedVortexRelational> {
    let mut index = 0;
    let consume = |batch: crate::local_primitives::collect::SerializedVortexResultBatch| {
        let rows = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
        assert!(rows.len() <= 257);
        for row in rows {
            assert_eq!(
                row,
                json!({"entity":text(index), "pivot_a":3.0}),
                "index={index}"
            );
            index += 1;
        }
        Ok(())
    };
    let report = if streamed {
        let mut start = 0;
        let mut producer = |session: &ResidentVortexSession| {
            if start == ROWS {
                return Ok(None);
            }
            let end = (start + 251).min(ROWS);
            let batch = stream_batch(session, start, end)?;
            start = end;
            Ok(Some(batch))
        };
        prepared
            .with_batch_input(&mut producer)?
            .for_each_json_batch(&CancellationToken::default(), 257, 4 << 20, consume)?
    } else {
        prepared.for_each_json_batch(&CancellationToken::default(), 257, 4 << 20, consume)?
    };
    assert_eq!(index, GROUPS);
    assert_eq!(report.output_rows, GROUPS as u64);
    assert_eq!(report.scan_rows_delivered, ROWS as u64);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    if streamed {
        let input = report.input.as_ref().unwrap();
        assert_eq!(input.sources, 1);
        assert_eq!(input.rows, ROWS as u64);
        assert_eq!(input.payload_batches, ROWS.div_ceil(251) as u64);
        assert!(input.input_logical_bytes > GRANT);
        assert!(input.end_of_input_observed && input.output_ownership_detached);
    }
    Ok(report)
}

#[test]
fn native_pivot_spill_pressure_denies_resident_and_completes_beyond_the_same_grant() {
    pressure(false);
}

#[test]
fn streamed_dynamic_pivot_spill_pressure_denies_resident_and_completes_beyond_the_same_grant() {
    pressure(true);
}

fn pressure(streamed: bool) {
    let fixture = fixture();
    let denied = prepare(&fixture, GRANT, false, streamed);
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute(&denied, streamed)
        .err()
        .expect("resident state exceeds this grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(denied.snapshot().completed_executions, 0);
    drop(denied);

    let ample = prepare(&fixture, 512 << 20, false, streamed);
    let baseline = ample.snapshot().memory.reserved_bytes;
    let control = execute(&ample, streamed).unwrap();
    assert_eq!(control.spilled_pivot_stages, 0);
    let ample_peak = control.runtime.memory.peak_reserved_bytes;
    assert!(ample_peak > GRANT);
    drop(control);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let spilled = prepare(&fixture, GRANT, true, streamed);
    let baseline = spilled.snapshot().memory.reserved_bytes;
    for call in 1..=2 {
        let report = execute(&spilled, streamed).unwrap();
        assert_eq!(report.spilled_pivot_stages, 1);
        assert_eq!(report.spilled_pivot_input_rows, ROWS as u64);
        assert_eq!(report.spilled_pivot_index_rows, GROUPS as u64);
        assert_eq!(report.spilled_pivot_cells, GROUPS as u64);
        assert_eq!(report.spilled_pivot_domains, 1);
        assert!(report.spilled_pivot_lookup_blocks > 0);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.runs_written > 8 && spill.merge_passes > 4);
        assert!(spill.owned_cleanup_completed);
        assert_eq!(report.spilled_pivot_reader_opens, spill.runs_written + 1);
        assert_eq!(spilled.snapshot().completed_executions, call);
        eprintln!(
            "{}",
            json!({"family":"native_sparse_pivot_repeated_floating_updates", "call":call,"streamed":streamed,
                "input_rows":ROWS,"input_file_bytes":fs::metadata(fixture.path()).unwrap().len(),"grant":GRANT,
                "spilled_peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
                "resident_denial":error.to_string(),"runs_written":spill.runs_written,"merge_passes":spill.merge_passes,
                "peak_disk_bytes":spill.peak_disk_bytes,"index_rows":report.spilled_pivot_index_rows,
                "cells":report.spilled_pivot_cells,"domains":report.spilled_pivot_domains,
                "lookup_blocks":report.spilled_pivot_lookup_blocks,"reader_opens":report.spilled_pivot_reader_opens,
                "complete_output_rows":report.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(report);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("pivot-runs")).unwrap().count(),
            0
        );
    }
}
