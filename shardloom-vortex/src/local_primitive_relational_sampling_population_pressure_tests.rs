//! Full independent sample values at the original resident-population grant.

use super::*;
use shardloom_core::{ComparisonOp, PredicateExpr};

#[path = "local_primitive_relational_sampling_population_failure_tests.rs"]
mod failure_tests;

#[path = "local_primitive_relational_sampling_population_typed_tests.rs"]
mod typed_tests;

fn sample_request(case: &Value) -> VortexQueryPrimitiveRequest {
    let mut req = request(Kind::SampleRows, &["n", "s", "items"]);
    req.source_order_limit = case["count"].as_u64().map(|n| usize::try_from(n).unwrap());
    req.sample_fraction = case["fraction"].as_f64();
    req.sample_seed = case["seed"].as_u64();
    req.sample_with_replacement = case["replace"].as_bool().unwrap();
    if case["weighted"].as_bool().unwrap() {
        req.sample_weight_column = Some(ColumnRef::new("n").unwrap());
        req.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("n").unwrap(),
            op: ComparisonOp::Gt,
            value: shardloom_core::StatValue::Int64(0),
        });
    }
    req
}

#[test]
#[allow(clippy::too_many_lines)] // One unchanged pressure protocol covers all six population variants.
fn sampling_population_spills_at_original_grant_with_exact_rank_and_draw_outputs() {
    let reference: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/native_sampling_population_reference.json"
    ))
    .unwrap();
    let cases = reference["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        assert_eq!(case["rows"], TOTAL);
        assert_eq!(case["grant_bytes"], GRANT);
        let expected = case["expected"].as_array().unwrap();
        let req = sample_request(case);
        for spill in [false, true] {
            let (_fixture, directory) = workspace();
            let mut prepared = prepare_unary(&unary(scan(), req.clone()), GRANT).unwrap();
            if spill {
                prepared = with_spill(prepared, &directory);
            }
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = WideInput::new(TOTAL, 2048, true);
            let mut delivered = 0;
            let result = prepared
                .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |array, context| {
                    for row in values(&array, context)? {
                        let position =
                            usize::try_from(expected[delivered].as_u64().unwrap()).unwrap();
                        assert_eq!(
                            row,
                            expected_row(position, 2048, true),
                            "{name}/{spill}/{delivered}"
                        );
                        delivered += 1;
                    }
                    Ok(())
                });
            if spill {
                let report = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                assert_eq!(delivered, expected.len(), "{name}");
                assert!(input.ended);
                assert_eq!(report.spillable_sampling_stages, 1);
                assert_eq!(report.spillable_sampling_input_rows, TOTAL as u64);
                assert_eq!(
                    report.spillable_sampling_population_rows,
                    case["population"].as_u64().unwrap()
                );
                assert_eq!(report.spillable_sampling_output_rows, expected.len() as u64);
                assert_eq!(
                    report.spillable_sampling_replacement_draws,
                    if req.sample_with_replacement {
                        expected.len() as u64
                    } else {
                        0
                    }
                );
                assert_eq!(
                    report.spillable_sampling_lookup_blocks > 0,
                    req.sample_with_replacement
                );
                assert!(report.spill.as_ref().unwrap().runs_written > 0);
                assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
                assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
                assert!(report.native_io_certificate.is_certified());
                assert!(!report.native_io_certificate.fallback_attempted);
                assert!(report.input.as_ref().unwrap().end_of_input_observed);
                assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
                assert_eq!(prepared.snapshot().completed_executions, 1);
                drop(report);
            } else {
                let error = result
                    .err()
                    .expect("resident sample population exceeds grant");
                assert!(
                    error.to_string().contains("reservation denied"),
                    "{name}: {error}"
                );
                assert!(!input.ended && input.position > INPUT_WIDTH);
                assert_eq!(delivered, 0);
                assert_eq!(prepared.snapshot().completed_executions, 0);
            }
            assert!(
                input
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
            assert_eq!(
                prepared.snapshot().memory.reserved_bytes,
                baseline,
                "{name}/{spill}"
            );
            clean(&directory);
        }
    }
}
