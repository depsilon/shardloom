//! The pre-candidate file witness, now requiring retained domain computation.

use super::*;
use crate::{
    query_primitive::{VortexQueryPrimitiveKind, VortexSimpleAggregateMeasure},
    relational_query::{VortexRelationalAggregate, VortexRelationalSetKind, VortexRelationalUnary},
};
use vortex::array::arrays::DictArray;

#[test]
fn dictionary_text_file_domain_avoids_row_expansion() {
    let long = "  ΣΟΣİß東京\0 mixed CASE  ".repeat(16);
    let domain = [Some(long.as_str()), Some("  ABC  "), Some("abc"), None];
    let rows = 2048;
    let codes =
        PrimitiveArray::from_iter((0..rows).map(|row| u8::try_from(row % 4).unwrap())).into_array();
    let dictionary = DictArray::try_new(
        codes,
        VarBinViewArray::from_iter_nullable_str(domain).into_array(),
    )
    .unwrap()
    .into_array();
    let input = StructArray::from_fields(&[("value", dictionary)])
        .unwrap()
        .into_array();
    let fixture = Fixture::new(input, rows);
    let plan = project(
        fixture.scan(),
        vec![("result", function("lower", vec![col("value")]))],
    );
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let actual = collected(&plan);
    let counts = observation.finish();
    let expected = (0..rows)
        .map(|row| serde_json::json!({"result":domain[row % 4].map(str::to_lowercase)}))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(counts.expression_prepared_utf8_batches, 1);
    assert_eq!(counts.expression_prepared_utf8_rows, 3);
    assert_eq!(counts.expression_variable_values, 4);
}

fn dictionary_fixture(domain: [Option<&str>; 3]) -> Fixture {
    Fixture::new(dictionary_batch(domain), 12)
}

fn dictionary_batch(domain: [Option<&str>; 3]) -> ArrayRef {
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter((0..12).map(|row| u8::try_from(row % 3).unwrap())).into_array(),
        VarBinViewArray::from_iter_nullable_str(domain).into_array(),
    )
    .unwrap()
    .into_array();
    StructArray::from_fields(&[("value", dictionary)])
        .unwrap()
        .into_array()
}

fn different_dictionary_chunks() -> Fixture {
    let directory = std::env::temp_dir().join(format!(
        "shardloom-dictionary-chunks-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let left = dictionary_batch([Some(" A "), Some("a"), None]);
    let right = dictionary_batch([Some(" B "), Some("a"), None]);
    let mut file = fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(2),
        )
        .blocking(&runtime)
        .writer(&mut file, left.dtype().clone());
    writer.push(left).unwrap();
    writer.push(right).unwrap();
    writer.finish().unwrap();
    fixture
}

fn transformed(input: VortexRelationalPlan) -> VortexRelationalPlan {
    project(
        input,
        vec![(
            "key",
            function("trim", vec![function("lower", vec![col("value")])]),
        )],
    )
}

fn ordered(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("key").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}

fn checked(plan: &VortexRelationalPlan, dictionary: bool) -> Vec<serde_json::Value> {
    let prepared = prepare_relational(plan, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let certificate = &result.execution.native_io_certificate;
    assert!(certificate.is_certified());
    assert!(!certificate.side_effects.fallback_attempted);
    let proof = &certificate.source_pushdown_report.proof_basis;
    assert_eq!(
        certificate
            .source_pushdown_report
            .accepted_operations
            .contains(&"native_dictionary_text_expression".into()),
        dictionary,
        "{proof}"
    );
    let expected = if dictionary {
        [
            "dictionary_text_dictionary_batches=4",
            "dictionary_text_dictionary_kernel_calls=8",
            "dictionary_text_domain_slots=12",
            "dictionary_text_code_rows_copied=48",
            "dictionary_text_retained_dictionary_rows=48",
            "dictionary_text_row_output_rows=0",
        ]
    } else {
        [
            "dictionary_text_dictionary_batches=0",
            "dictionary_text_row_kernel_calls=32",
            "dictionary_text_domain_slots=0",
            "dictionary_text_code_rows_copied=0",
            "dictionary_text_retained_dictionary_rows=0",
            "dictionary_text_row_output_rows=48",
        ]
    };
    for expected in expected {
        assert!(
            proof.split_whitespace().any(|token| token == expected),
            "{expected}: {proof}"
        );
    }
    let rows = json_rows(&result);
    drop((result, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    rows
}

#[test]
fn dictionary_text_group_and_distinct_use_transformed_values_across_dictionaries() {
    let fixture = different_dictionary_chunks();
    let left = dictionary_fixture([Some(" A "), Some("a"), None]);
    let right = dictionary_fixture([Some(" B "), Some("a"), None]);
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: left.scan(),
        right: right.scan(),
        kind: VortexRelationalSetKind::UnionAll,
    }));
    // UNION ALL's existing cast/copy boundary expands strings. Keep that exact
    // composed shape as a no-dictionary control alongside actual file chunks.
    for (input, dictionary) in [(fixture.scan(), true), (union, false)] {
        let grouped = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: transformed(input.clone()),
            group_by: vec![ColumnRef::new("key").unwrap()],
            grouping_sets: None,
            measures: vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".into(),
            )],
        }));
        assert_eq!(
            checked(&ordered(grouped), dictionary),
            vec![
                serde_json::json!({"key":"a","rows":12}),
                serde_json::json!({"key":"b","rows":4}),
                serde_json::json!({"key":null,"rows":8}),
            ]
        );
        let distinct = VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
            input: transformed(input),
            request: VortexQueryPrimitiveRequest::for_relational_input(
                VortexQueryPrimitiveKind::DistinctRows,
                shardloom_plan::ProjectionRequest::All,
            ),
        }));
        assert_eq!(
            checked(&ordered(distinct), dictionary),
            vec![
                serde_json::json!({"key":"a"}),
                serde_json::json!({"key":"b"}),
                serde_json::json!({"key":null})
            ]
        );
    }
}

#[test]
fn dictionary_text_filter_and_full_output_preserve_codes_values_nulls_and_order() {
    let fixture = different_dictionary_chunks();
    let left = dictionary_fixture([Some(" A "), Some("a"), None]);
    let right = dictionary_fixture([Some(" B "), Some("a"), None]);
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: left.scan(),
        right: right.scan(),
        kind: VortexRelationalSetKind::UnionAll,
    }));
    for (input, dictionary) in [(fixture.scan(), true), (union, false)] {
        let filter = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: transformed(input.clone()),
            predicate: compare(
                col("key"),
                ComparisonOp::Eq,
                lit(ScalarValue::Utf8("a".into())),
            ),
        }));
        assert_eq!(
            checked(&filter, dictionary),
            vec![serde_json::json!({"key":"a"}); 12]
        );
        let expected = [Some("a"), Some("b"), None]
            .into_iter()
            .zip([12, 4, 8])
            .flat_map(|(key, count)| vec![serde_json::json!({"key":key}); count])
            .collect::<Vec<_>>();
        assert_eq!(checked(&ordered(transformed(input)), dictionary), expected);
    }
}

#[test]
fn dictionary_text_compatible_outputs_reuse_the_same_completed_transform() {
    let fixture = dictionary_fixture([Some("İßΣΟΣ"), Some("é"), None]);
    let lower = || function("lower", vec![col("value")]);
    let plan = project(
        fixture.scan(),
        vec![
            ("lower", lower()),
            ("again", lower()),
            ("characters", function("length", vec![lower()])),
            ("bytes", function("byte_length", vec![lower()])),
        ],
    );
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let counts = observation.finish();
    assert!(counts.expression_output_reuses >= 3);
    assert_eq!(counts.expression_prepared_utf8_rows, 6);
    let expected = [
        serde_json::json!({"lower":"i\u{307}ßσος", "again":"i\u{307}ßσος", "characters":6, "bytes":11}),
        serde_json::json!({"lower":"é", "again":"é", "characters":1, "bytes":2}),
        serde_json::json!({"lower":null, "again":null, "characters":null, "bytes":null}),
    ];
    assert_eq!(
        json_rows(&result),
        (0..12)
            .map(|row| expected[row % 3].clone())
            .collect::<Vec<_>>()
    );
}
