//! Independent complete values through actual native pivot eviction and merging.

use super::*;
use crate::{
    local_primitives::VortexPivotProjectionRequest,
    relational_query::{VortexRelationalSpillPolicy, VortexRelationalUnary},
};
use serde_json::{Value, json};
use vortex::array::arrays::VarBinArray;

#[path = "local_primitive_relational_pivot_spill_composition_tests.rs"]
mod composition;
#[path = "local_primitive_relational_pivot_spill_failure_tests.rs"]
mod failures;
#[path = "local_primitive_relational_pivot_pressure_tests.rs"]
mod pressure;
#[path = "local_primitive_relational_pivot_spill_recovery_tests.rs"]
mod recovery;
#[path = "local_primitive_relational_pivot_spill_semantics_tests.rs"]
mod semantics;
#[path = "local_primitive_relational_pivot_spill_typed_tests.rs"]
mod typed;

fn table(indices: ArrayRef, domains: ArrayRef, values: ArrayRef, chunk_rows: usize) -> Fixture {
    let rows = values.len();
    assert_eq!(indices.len(), rows);
    assert_eq!(domains.len(), rows);
    Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "category", "amount"]),
            vec![indices, domains, values],
            rows,
            Validity::NonNullable,
        )
        .into_array(),
        chunk_rows,
    )
}

fn key(index: usize) -> String {
    format!("group-{index:04}-{}", "k".repeat(128))
}

fn fixture(values: &[Option<f64>], groups: usize, chunk_rows: usize) -> Fixture {
    let keys = (0..values.len())
        .flat_map(|_| (0..groups).map(key))
        .collect::<Vec<_>>();
    let rows = keys.len();
    Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "category", "amount"]),
            vec![
                VarBinArray::from(keys).into_array(),
                VarBinArray::from(vec!["a"; rows]).into_array(),
                PrimitiveArray::from_option_iter(
                    values
                        .iter()
                        .flat_map(|value| std::iter::repeat_n(*value, groups)),
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array(),
        chunk_rows,
    )
}

fn plan(fixture: &Fixture, aggregate: &str) -> VortexRelationalPlan {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::PivotRows,
        shardloom_plan::ProjectionRequest::All,
    );
    request.pivot_projection = Some(VortexPivotProjectionRequest::new(
        ColumnRef::new("entity").unwrap(),
        ColumnRef::new("category").unwrap(),
        ColumnRef::new("amount").unwrap(),
        aggregate,
    ));
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
        input: fixture.scan(),
        request,
    }))
}

fn prepare(fixture: &Fixture, plan: VortexRelationalPlan, spill: bool) -> PreparedVortexRelational {
    prepare_with_policy(fixture, plan, spill, policy())
}

fn prepare_with_policy(
    fixture: &Fixture,
    plan: VortexRelationalPlan,
    spill: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> PreparedVortexRelational {
    let prepared = prepare_relational_with_dynamic_schema(
        &[DatasetUri::new(fixture.path().display().to_string()).unwrap()],
        policy,
        65_536,
        move |schemas| schemas.resolve_output(&plan).map(|(plan, _)| plan),
    )
    .unwrap();
    if spill {
        let workspace = fixture.0.join("pivot-runs");
        fs::create_dir_all(&workspace).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(workspace, 128 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

fn complete(prepared: &PreparedVortexRelational) -> (Vec<Value>, ExecutedVortexRelational) {
    let mut rows = Vec::new();
    let report = prepared
        .for_each_json_batch(&CancellationToken::default(), 17, 1 << 20, |batch| {
            let values = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
            assert!(values.len() <= 17);
            rows.extend(values);
            Ok(())
        })
        .unwrap();
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    if let Some(spill) = &report.spill {
        assert_eq!(report.spilled_pivot_stages, 1);
        assert!(
            report
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
                .contains("spilled_pivot_stages=1")
        );
        assert!(spill.owned_cleanup_completed);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    }
    (rows, report)
}

#[test]
fn native_pivot_spill_sequential_lookups_reuse_covering_blocks() {
    let groups = 2049;
    let fixture = fixture(&[Some(7.0)], groups, 97);
    let spilled = prepare(&fixture, plan(&fixture, "sum"), true);
    let baseline = spilled.snapshot().memory.reserved_bytes;
    let (actual, report) = complete(&spilled);
    let expected = (0..groups)
        .map(|index| json!({"entity":key(index), "pivot_a":7.0}))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    // Every new input key lies after the older runs, so discovery needs no
    // payload lookup. Output visits a five-block run in order. Its exact key
    // range should reuse the block already holding the adjacent index marker.
    let blocks = (groups * 2).div_ceil(1024) as u64;
    assert!(
        report.spilled_pivot_lookup_blocks <= blocks * 2,
        "sequential output loaded {} blocks for {blocks} distinct blocks",
        report.spilled_pivot_lookup_blocks
    );
    drop(report);
    assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_pivot_spill_repeated_updates_bound_lookup_amplification() {
    let groups = 2049;
    let fixture = fixture(&[Some(1e16), Some(1.0), Some(-1e16), Some(3.0)], groups, 97);
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_with_policy(&fixture, plan(&fixture, "sum"), true, policy);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (actual, report) = complete(&prepared);
    assert_eq!(
        actual,
        (0..groups)
            .map(|index| json!({"entity":key(index), "pivot_a":3.0}))
            .collect::<Vec<_>>()
    );
    // Later updates create cell-only runs and gaps in mixed chronological runs.
    // Keep their repeated lookup below one block read per input row in this
    // fixture. The two-slot cache still alternates positive blocks from different
    // runs; requiring one read per output group would ignore that actual work.
    assert!(
        report.spilled_pivot_lookup_blocks < (groups * 4) as u64,
        "repeated updates loaded {} blocks for {} input rows",
        report.spilled_pivot_lookup_blocks,
        groups * 4
    );
    eprintln!(
        "{}",
        json!({"family":"native_pivot_sparse_run_lookup", "input_rows":groups*4,
               "complete_output_rows":groups,"lookup_blocks":report.spilled_pivot_lookup_blocks})
    );
    assert!(report.spill.as_ref().unwrap().merge_passes > 4);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_pivot_spill_nonmonotonic_updates_preserve_keys_at_cached_gap_edges() {
    let groups = 1025;
    let width = groups * 2;
    let values = [1e16, 1.0, -1e16, 3.0];
    let mut indices = Vec::new();
    let mut domains = Vec::new();
    let mut amounts = Vec::new();
    for (pass, value) in values.into_iter().enumerate() {
        for ordinal in 0..width {
            // A bijection visits every cell once, with a different wrap point
            // each pass. Adjacent keys, index markers and both domain keys are
            // revisited on either side of previously discovered missing ranges.
            let cell = (ordinal * 37 + pass * 619) % width;
            indices.push(key(cell / 2));
            domains.push(if cell % 2 == 0 { "a" } else { "b" });
            amounts.push(value);
        }
    }
    let fixture = table(
        VarBinArray::from(indices).into_array(),
        VarBinArray::from(domains).into_array(),
        PrimitiveArray::from_iter(amounts).into_array(),
        257,
    );
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_with_policy(&fixture, plan(&fixture, "sum"), true, policy);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (actual, report) = complete(&prepared);
    assert_eq!(
        actual,
        (0..groups)
            .map(|index| json!({"entity":key(index), "pivot_a":3.0, "pivot_b":3.0}))
            .collect::<Vec<_>>()
    );
    assert_eq!(report.spilled_pivot_cells, width as u64);
    assert_eq!(report.spilled_pivot_index_rows, groups as u64);
    assert!(report.spill.as_ref().unwrap().merge_passes > 4);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_pivot_spill_repeated_evicted_cells_preserve_sequential_float_prefixes() {
    let fixture = fixture(&[Some(1e16), Some(1.0), Some(-1e16), Some(3.0)], 257, 73);
    for (aggregate, expected) in [
        ("sum", json!(3.0)),
        ("mean", json!(0.75)),
        ("count", json!(4)),
        ("min", json!(-1e16)),
        ("max", json!(1e16)),
        ("first", json!(1e16)),
    ] {
        let plan = plan(&fixture, aggregate);
        let resident = prepare(&fixture, plan.clone(), false);
        let spilled = prepare(&fixture, plan, true);
        let baseline = spilled.snapshot().memory.reserved_bytes;
        let expected = (0..257)
            .map(|index| json!({"entity":key(index), "pivot_a":expected}))
            .collect::<Vec<_>>();
        let (actual, report) = complete(&spilled);
        assert_eq!(actual, expected, "{aggregate}");
        assert_eq!(complete(&resident).0, expected, "resident {aggregate}");
        assert_eq!(report.scan_rows_delivered, 1028);
        assert_eq!(report.spilled_pivot_input_rows, 1028);
        assert_eq!(report.spilled_pivot_index_rows, 257);
        assert_eq!(report.spilled_pivot_domains, 1);
        assert_eq!(report.spilled_pivot_cells, 257);
        assert!(report.spilled_pivot_lookup_blocks > 0);
        let spill = report.spill.as_ref().unwrap();
        // Each completed run is opened once, then only the final run is reopened
        // for output. Reopening whole files per point lookup violates this bound.
        assert_eq!(report.spilled_pivot_reader_opens, spill.runs_written + 1);
        assert!(spill.runs_written > 2, "{aggregate}: {spill:?}");
        assert!(spill.merge_passes > 0, "{aggregate}: {spill:?}");
        drop(report);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn native_pivot_spill_first_null_and_exact_duplicate_conflicts_survive_eviction() {
    for values in [vec![None, None], vec![Some(7.0), Some(7.0)]] {
        let fixture = fixture(&values, 257, 51);
        let prepared = prepare(&fixture, plan(&fixture, "first_unique"), true);
        let expected = (0..257)
            .map(|index| json!({"entity":key(index), "pivot_a":values[0]}))
            .collect::<Vec<_>>();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let (actual, report) = complete(&prepared);
        assert_eq!(actual, expected);
        assert!(report.spill.as_ref().unwrap().merge_passes > 0);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    for values in [vec![None, Some(7.0)], vec![Some(7.0), Some(8.0)]] {
        let fixture = fixture(&values, 257, 51);
        let plan = plan(&fixture, "first_unique");
        let resident = prepare(&fixture, plan.clone(), false);
        let prepared = prepare(&fixture, plan, true);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut delivered = 0;
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered += 1;
                Ok(())
            })
            .err()
            .unwrap();
        let reference = resident.execute_owned().err().unwrap();
        assert_eq!(error.to_string(), reference.to_string());
        assert!(error.to_string().contains("multiple values"));
        assert_eq!(delivered, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("pivot-runs")).unwrap().count(),
            0
        );
    }
}

#[test]
fn native_pivot_spill_count_does_not_observe_null_or_nonfinite_values() {
    let fixture = fixture(&[None, Some(f64::NAN), Some(f64::INFINITY)], 257, 83);
    let prepared = prepare(&fixture, plan(&fixture, "count"), true);
    assert_eq!(
        complete(&prepared).0,
        (0..257)
            .map(|index| json!({"entity":key(index), "pivot_a":3}))
            .collect::<Vec<_>>()
    );
}
