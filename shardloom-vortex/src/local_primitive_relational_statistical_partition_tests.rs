//! Statistical reductions keep logical row order across physical input partitions.
use super::*;
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues};

const ROWS: usize = 257;

fn offset(row: usize) -> i32 {
    i32::try_from((row * 37) % 101).unwrap() - 50
}

fn source(
    session: &ResidentVortexSession,
    range: std::ops::Range<usize>,
    wide: bool,
) -> Result<ResidentMemorySource> {
    let groups = vec![Some(0_i64); range.len()];
    let x = range
        .clone()
        .map(|row| (row % 13 != 0).then_some(offset(row)))
        .collect::<Vec<_>>();
    let y = range
        .map(|row| (row % 17 != 0).then_some(offset(row)))
        .collect::<Vec<_>>();
    if wide {
        let x = x
            .into_iter()
            .map(|value| value.map(|value| i64::MAX - 100 + i64::from(value)))
            .collect::<Vec<_>>();
        let y = y
            .into_iter()
            .map(|value| value.map(|value| i64::MIN + 300 + 3 * i64::from(value)))
            .collect::<Vec<_>>();
        ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "number",
                    values: MemoryColumnValues::Int64(&x),
                },
                MemoryColumn {
                    name: "paired",
                    values: MemoryColumnValues::Int64(&y),
                },
                MemoryColumn {
                    name: "group",
                    values: MemoryColumnValues::Int64(&groups),
                },
            ],
        )
    } else {
        let x = x
            .into_iter()
            .map(|value| value.map(|value| f64::from(value) * 0.1))
            .collect::<Vec<_>>();
        let y = y
            .into_iter()
            .map(|value| value.map(|value| -3.0 * f64::from(value) * 0.1))
            .collect::<Vec<_>>();
        ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "number",
                    values: MemoryColumnValues::Float64(&x),
                },
                MemoryColumn {
                    name: "paired",
                    values: MemoryColumnValues::Float64(&y),
                },
                MemoryColumn {
                    name: "group",
                    values: MemoryColumnValues::Int64(&groups),
                },
            ],
        )
    }
}

fn reference(paired: bool, sample: bool, wide: bool) -> f64 {
    // Exact small-integer numerator from the original fixture offsets. The
    // large origins cancel algebraically, before any floating conversion.
    let values = (0..ROWS)
        .filter(|row| row % 13 != 0 && (!paired || row % 17 != 0))
        .map(offset)
        .collect::<Vec<_>>();
    let n = i32::try_from(values.len()).unwrap();
    let sum = values.iter().sum::<i32>();
    let squares = values.iter().map(|value| value * value).sum::<i32>();
    let scale = if wide { 1.0 } else { 0.01 };
    let pair_scale = if paired {
        if wide { 3.0 } else { -3.0 }
    } else {
        1.0
    };
    f64::from(n * squares - sum * sum) / f64::from(n * (n - i32::from(sample))) * scale * pair_scale
}

fn complete(wide: bool, grouped: bool, geometry: &[usize]) -> Vec<u64> {
    let uri = DatasetUri::new("memory://statistical-partitions").unwrap();
    let plan = aggregate(
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri.clone(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        if grouped { &["group"] } else { &[] },
        moments()
            .into_iter()
            .chain(["covar_pop", "covar_samp", "corr"].map(|name| {
                measure(name, Some("number"), name)
                    .with_secondary_column(ColumnRef::new("paired").unwrap())
            }))
            .collect(),
    );
    let prepared = prepare_relational_with_schema(policy(), |binding| {
        binding.register_batch_source(uri.clone(), |session| source(session, 0..0, wide))?;
        Ok(plan.clone())
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let mut start = 0;
    let mut window = 0;
    let mut maximum_rows = 0;
    let mut ended = false;
    let mut input = |session: &ResidentVortexSession| {
        if start == ROWS {
            ended = true;
            return Ok(None);
        }
        let stop = (start + geometry[window % geometry.len()]).min(ROWS);
        maximum_rows = maximum_rows.max(stop - start);
        let batch = source(session, start..stop, wide)?;
        start = stop;
        window += 1;
        Ok(Some(batch))
    };
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert!(ended);
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(input.payload_batches, u64::try_from(window).unwrap());
    assert_eq!(input.max_batch_rows, maximum_rows);
    assert_eq!(input.rows, u64::try_from(ROWS).unwrap());
    assert!(input.end_of_input_observed);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    let rows = json_rows(&result);
    assert_eq!(rows.len(), 1);
    let actual = &rows[0];
    let expected = [
        ("var_pop", reference(false, false, wide)),
        ("var_samp", reference(false, true, wide)),
        ("stddev_pop", reference(false, false, wide).sqrt()),
        ("stddev_samp", reference(false, true, wide).sqrt()),
        ("covar_pop", reference(true, false, wide)),
        ("covar_samp", reference(true, true, wide)),
        ("corr", if wide { 1.0 } else { -1.0 }),
    ];
    let bits = expected
        .into_iter()
        .map(|(name, expected)| {
            let value = actual[name].as_f64().unwrap();
            assert!(
                (value / expected - 1.0).abs() <= 1e-12,
                "{name}: {value} != {expected}"
            );
            value.to_bits()
        })
        .collect();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    bits
}

#[test]
fn native_statistical_physical_partitions_preserve_source_order_result_bits() {
    for wide in [false, true] {
        let expected = complete(wide, true, &[ROWS]);
        for grouped in [false, true] {
            for geometry in [&[ROWS][..], &[1], &[2], &[7], &[0, 3, 1, 0, 31, 5]] {
                assert_eq!(
                    complete(wide, grouped, geometry),
                    expected,
                    "wide={wide}, grouped={grouped}, geometry={geometry:?}"
                );
            }
        }
    }
}
