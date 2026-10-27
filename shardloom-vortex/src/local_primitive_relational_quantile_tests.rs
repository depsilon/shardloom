//! Exact sorted references cover native grouping, stored values and frame owners.
use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

#[path = "local_primitive_relational_quantile_window_tests.rs"]
mod windows;

fn quantile(function: &str, level: f64, alias: &str) -> Measure {
    measure(function, Some("number"), alias)
        .with_quantile_level(level)
        .unwrap()
}

fn measures() -> Vec<Measure> {
    vec![
        measure("median", Some("number"), "median"),
        quantile("quantile_cont", 0.25, "c25"),
        quantile("quantile_cont", 0.75, "c75"),
        quantile("quantile_disc", 0.0, "d0"),
        quantile("quantile_disc", 0.5, "d50"),
        quantile("quantile_disc", 1.0, "d100"),
    ]
}

fn check(
    fixture: &Fixture,
    plan: &VortexRelationalPlan,
    expected: &[Value],
    counters: Option<(u64, u64)>,
) {
    let workspace = fixture.0.join("quantile-runs");
    fs::create_dir_all(&workspace).unwrap();
    for spill in [false, true] {
        let prepared = prepare_relational(plan, policy()).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for batch_rows in [1, 3] {
            let mut rows = vec![];
            let report = prepared
                .for_each_json_batch(
                    &CancellationToken::default(),
                    batch_rows,
                    1 << 20,
                    |batch| {
                        rows.extend(
                            serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap(),
                        );
                        Ok(())
                    },
                )
                .unwrap_or_else(|error| panic!("spill={spill}, plan={plan:?}: {error}"));
            assert_eq!(rows, expected, "spill={spill}");
            assert!(report.native_io_certificate.is_certified());
            assert!(!report.native_io_certificate.side_effects.fallback_attempted);
            if spill {
                assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
                if let Some((quantiles, distinct)) = counters {
                    assert_eq!(report.ordered_aggregate_quantile_rows, quantiles);
                    assert_eq!(report.ordered_aggregate_distinct_rows, distinct);
                }
            }
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn exact_quantile_grouped_spill_keeps_duplicates_and_shares_distinct_value_records() {
    let fixture = fixture();
    let mut declarations = measures();
    declarations.push(measure("sum_distinct", Some("number"), "sum"));
    declarations.push(measure("count_distinct", Some("number"), "distinct"));
    let plan = aggregate(fixture.scan(), &["group"], declarations);
    check(
        &fixture,
        &plan,
        &[
            json!({"group":"B","median":1.0,"c25":-0.5,"c75":2.5,"d0":-2,"d50":-2,"d100":4,"sum":2.0,"distinct":2}),
            json!({"group":"A","median":5.0,"c25":5.0,"c75":5.0,"d0":5,"d50":5,"d100":5,"sum":5.0,"distinct":1}),
            json!({"group":null,"median":9.0,"c25":9.0,"c75":9.0,"d0":9,"d50":9,"d100":9,"sum":9.0,"distinct":1}),
        ],
        Some((5, 5)),
    );
    check(
        &fixture,
        &aggregate(fixture.scan(), &[], measures()),
        &[json!({"median":5.0,"c25":4.0,"c75":5.0,"d0":-2,"d50":5,"d100":9})],
        Some((5, 0)),
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let schema = prepared.output_dtype().unwrap();
    let fields = schema.as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.field("d50").unwrap(),
        DType::Primitive(PType::I16, Nullability::Nullable)
    );
    assert_eq!(
        fields.field("median").unwrap(),
        DType::Primitive(PType::F64, Nullability::Nullable)
    );
}

#[test]
fn exact_quantile_scalar_empty_singleton_and_uint64_order_preserve_complete_results() {
    for values in [
        vec![],
        vec![None, None],
        vec![Some(u64::MAX), None],
        vec![Some(u64::MAX), Some(u64::MAX - 2), None, Some(u64::MAX - 1)],
    ] {
        let mut sorted = values.iter().flatten().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        let expected = if sorted.is_empty() {
            json!({"median":null,"c25":null,"c75":null,"d0":null,"d50":null,"d100":null})
        } else {
            // All continuous endpoints round to the same Float64, but discrete
            // ranks must still distinguish the original adjacent uint64 values.
            json!({"median":18_446_744_073_709_551_616.0,"c25":18_446_744_073_709_551_616.0,"c75":18_446_744_073_709_551_616.0,
                "d0":sorted[0],"d50":sorted[(sorted.len()-1)/2],"d100":sorted[sorted.len()-1]})
        };
        let fixture = Fixture::new(
            single(
                "number",
                PrimitiveArray::from_option_iter(values).into_array(),
            ),
            1,
        );
        check(
            &fixture,
            &aggregate(fixture.scan(), &[], measures()),
            &[expected],
            Some((sorted.len() as u64, 0)),
        );
    }
}

#[test]
fn exact_quantile_decimal_and_arbitrary_float_levels_keep_declared_output_types() {
    let fixture = Fixture::new(
        single(
            "number",
            DecimalArray::from_option_iter(
                [Some(1i128), Some(1), Some(5), None],
                DecimalDType::new(10, 2),
            )
            .into_array(),
        ),
        1,
    );
    let plan = aggregate(fixture.scan(), &[], measures());
    check(
        &fixture,
        &plan,
        &[
            json!({"median":"decimal128(38,6):10000","c25":"decimal128(38,6):10000",
        "c75":"decimal128(38,6):30000","d0":"decimal128(10,2):1","d50":"decimal128(10,2):1","d100":"decimal128(10,2):5"}),
        ],
        Some((3, 0)),
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let schema = prepared.output_dtype().unwrap();
    let fields = schema.as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.field("median").unwrap(),
        DType::Decimal(DecimalDType::new(38, 6), Nullability::Nullable)
    );
    assert_eq!(
        fields.field("d50").unwrap(),
        DType::Decimal(DecimalDType::new(10, 2), Nullability::Nullable)
    );
    let fixture = Fixture::new(
        single(
            "number",
            PrimitiveArray::from_iter([10.0, 0.0]).into_array(),
        ),
        1,
    );
    check(
        &fixture,
        &aggregate(
            fixture.scan(),
            &[],
            vec![
                quantile("quantile_cont", 0.1, "continuous"),
                quantile("quantile_disc", 0.1, "discrete"),
            ],
        ),
        &[json!({"continuous":1.0,"discrete":0.0})],
        Some((2, 0)),
    );
}

#[test]
fn exact_quantile_rejects_invalid_declarations_and_observed_nonfinite_values() {
    let fixture = fixture();
    for invalid in [
        measure("quantile_cont", Some("number"), "out"),
        measure("quantile_disc", Some("number"), "out"),
        quantile("median", 0.5, "out"),
        quantile("sum", 0.5, "out"),
        quantile("count", 0.5, "out"),
        quantile("quantile_cont", 0.5, "out")
            .with_secondary_column(ColumnRef::new("number").unwrap()),
        measure("median", Some("word"), "out"),
    ] {
        assert!(
            prepare_relational(&aggregate(fixture.scan(), &[], vec![invalid]), policy()).is_err()
        );
    }
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let fixture = Fixture::new(
            single("number", PrimitiveArray::from_iter([0.0, bad]).into_array()),
            1,
        );
        let workspace = fixture.0.join("failed");
        fs::create_dir(&workspace).unwrap();
        for spill in [false, true] {
            let prepared =
                prepare_relational(&aggregate(fixture.scan(), &[], measures()), policy()).unwrap();
            let prepared = if spill {
                prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                    )
                    .unwrap()
            } else {
                prepared
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn exact_quantile_forced_spill_selects_duplicate_ranks_across_runs_without_rebuilding_a_group() {
    let fixture = Fixture::new(
        single(
            "number",
            PrimitiveArray::from_iter((0u64..65_536).map(|row| (row * 73) % 1024)).into_array(),
        ),
        1024,
    );
    let mut declarations = measures();
    declarations.push(measure("sum_distinct", Some("number"), "sum"));
    declarations.push(measure("count_distinct", Some("number"), "distinct"));
    let plan = aggregate(fixture.scan(), &[], declarations);
    let workspace = fixture.0.join("forced");
    fs::create_dir(&workspace).unwrap();
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"median":511.5,"c25":255.75,"c75":767.25,
            "d0":0,"d50":511,"d100":1023,"sum":523_776.0,"distinct":1024})]
    );
    assert_eq!(result.execution.ordered_aggregate_quantile_rows, 65_536);
    assert_eq!(result.execution.ordered_aggregate_distinct_rows, 65_536);
    let spill = result.execution.spill.as_ref().unwrap();
    assert!(spill.runs_written > 0);
    assert!(spill.owned_cleanup_completed);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
}
