//! Complete requests attribute reads and preparation without timing claims.

use super::*;
use crate::{
    local_primitives::native_relational_observation::Session,
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalProject,
    },
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use shardloom_core::{BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use std::{num::NonZeroUsize, sync::Arc};
use vortex::{
    array::{
        aggregate_fn::{
            AggregateFnVTableExt as _, NumericalAggregateOpts,
            fns::{max::Max, min::Min},
        },
        arrays::VarBinViewArray,
    },
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            repartition::{RepartitionStrategy, RepartitionWriterOptions},
            struct_::StructStrategy,
            zoned::{
                Zoned,
                writer::{ZonedLayoutOptions, ZonedStrategy},
            },
        },
    },
};

fn zoned_fixture(input: ArrayRef) -> Fixture {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let functions = [
        Min.bind(NumericalAggregateOpts::skip_nans()),
        Max.bind(NumericalAggregateOpts::skip_nans()),
    ];
    let zoned = Arc::new(RepartitionStrategy::new(
        ZonedStrategy::new(
            ChunkedLayoutStrategy::new(flat.clone()),
            flat.clone(),
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(256).unwrap(),
                concurrency: NonZeroUsize::new(1).unwrap(),
                aggregate_fns: Some(functions.to_vec().into()),
            },
        ),
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: 256,
            block_size_target: None,
            canonicalize: false,
        },
    ));
    let strategy = StructStrategy::new(flat.clone(), Arc::new(ChunkedLayoutStrategy::new(flat)))
        .with_field_writer("n", zoned);
    Fixture::with_strategy(input, 512, vec![], Arc::new(strategy))
}

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("survivor_work").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn lit(value: ScalarValue) -> Expression {
    expression(ExpressionKind::Literal(value))
}
fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expression(ExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn compare(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    expression(ExpressionKind::Compare {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn function(name: &str, args: Vec<Expression>) -> Expression {
    expression(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}
fn project(
    input: VortexRelationalPlan,
    expressions: Vec<(&str, Expression)>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: expressions
            .into_iter()
            .map(|(name, expression)| (name.into(), expression))
            .collect(),
    }))
}
fn measure(function: &str, name: Option<&str>, output: &str) -> Measure {
    Measure::new(
        function,
        name.map(|name| ColumnRef::new(name).unwrap()),
        output.into(),
    )
}
fn aggregate(
    input: VortexRelationalPlan,
    keys: &[&str],
    measures: Vec<Measure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        measures,
        grouping_sets: None,
        group_by: keys
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
    }))
}

fn layout_zones(prepared: &PreparedVortexRelational) -> Vec<serde_json::Value> {
    let mut output = Vec::new();
    for (source, prepared) in prepared.sources.iter().enumerate() {
        let mut pending = vec![prepared.file().footer().layout().clone()];
        while let Some(layout) = pending.pop() {
            if let Some(zoned) = layout.as_opt::<Zoned>() {
                output.push(json!({"source":source, "rows":layout.row_count(),
                    "zone_rows":zoned.zone_len(), "zone_count":zoned.nzones()}));
            }
            for slot in 0..layout.nslots() {
                if let Some(child) = layout.slot(slot).unwrap() {
                    pending.push(child);
                }
            }
        }
    }
    output
}

fn input_identity(fixture: &Fixture) -> serde_json::Value {
    use std::fmt::Write as _;
    let bytes = fs::read(fixture.path()).unwrap();
    let mut digest = String::new();
    for byte in Sha256::digest(&bytes) {
        write!(digest, "{byte:02x}").unwrap();
    }
    json!({"bytes":bytes.len(), "sha256":digest})
}

fn print_work(
    case: &str,
    result: &CollectedVortexRelational,
    counts: &crate::local_primitives::native_relational_observation::Counts,
    zones: &[serde_json::Value],
    expected: &[serde_json::Value],
    inputs: &[serde_json::Value],
) {
    let reads = result.execution.native_io_certificate.file_scans.sources.iter().map(|source| {
        json!({"generation":source.generation_fingerprint, "segment_requests":source.segment_requests,
            "completed_read_requests":source.completed_read_requests,
            "completed_read_bytes":source.completed_read_bytes,
            "segment_reuse_hits":source.segment_reuse.hits})
    }).collect::<Vec<_>>();
    println!(
        "SURVIVOR_ADMISSION {}",
        json!({"case":case, "complete_result_rows":expected.len(),
        "actual_jsonl":result.result_jsonl.value(), "actual_schema":result.result_schema_json.value(),
        "reference_rows":expected, "inputs":inputs,
        "logical_layout_zones":zones, "logical_zone_exclusions":null, "physical_reads":reads,
        "scan_batches":result.execution.scan_batches, "scan_rows_delivered":result.execution.scan_rows_delivered,
        "output_batches":result.execution.output_batches, "output_rows":result.execution.output_rows,
        "key_preparations":counts.key_columns, "text_expression_preparations":counts.expression_prepared_utf8_batches,
        "text_expression_rows":counts.expression_prepared_utf8_rows,
        "aggregate_value_preparations":counts.aggregate_value_preparations,
        "aggregate_row_updates":counts.aggregate_row_updates, "join_output_rows":counts.join_output_rows,
        "pinned_backing_bytes_at_consumer":null, "engine_timing":null,
        "complete_values_match":true, "fallback_attempted":false})
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep declarations, independent reference and work scope together.
fn survivor_admission_complete_filter_expression_aggregate_work() {
    for (case, rows, stride, width) in [
        ("sparse_filter", 8192, 512, 1),
        ("zoned_sparse_filter", 8192, 512, 1),
        ("dense_filter", 8192, 1, 1),
        ("tiny_filter", 7, 2, 1),
        ("nullable_filter", 1024, 31, 1),
        ("wide_filter", 1024, 128, 1024),
        ("empty_filter", 0, 1, 1),
    ] {
        let strings = (0_usize..rows)
            .map(|row| {
                (!row.is_multiple_of(17))
                    .then(|| [" AΣΟΣ東京", "aΣΟΣ東京", " Bİß "][row % 3].repeat(width))
            })
            .collect::<Vec<_>>();
        let input = StructArray::from_fields(&[
            (
                "n",
                PrimitiveArray::from_iter((0..rows).map(|row| i64::try_from(row).unwrap()))
                    .into_array(),
            ),
            (
                "s",
                VarBinViewArray::from_iter_nullable_str(strings.iter().map(Option::as_deref))
                    .into_array(),
            ),
        ])
        .unwrap()
        .into_array();
        let fixture = if case == "zoned_sparse_filter" {
            zoned_fixture(input)
        } else {
            Fixture::new(input, 512)
        };
        let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: fixture.scan(),
            predicate: compare(
                binary(
                    col("n"),
                    BinaryOp::Remainder,
                    lit(ScalarValue::Int64(i64::try_from(stride).unwrap())),
                ),
                ComparisonOp::Eq,
                lit(ScalarValue::Int64(0)),
            ),
        }));
        let plan = aggregate(
            project(
                filtered,
                vec![("key", function("lower", vec![col("s")])), ("n", col("n"))],
            ),
            &["key"],
            vec![
                measure("count", None, "rows"),
                measure("min", Some("n"), "first"),
                measure("max", Some("n"), "last"),
            ],
        );
        let mut groups: Vec<(Option<String>, u64, usize, usize)> = Vec::new();
        for row in (0..rows).step_by(stride) {
            let key = strings[row].as_deref().map(str::to_lowercase);
            if let Some(group) = groups.iter_mut().find(|group| group.0 == key) {
                group.1 += 1;
                group.3 = row;
            } else {
                groups.push((key, 1, row, row));
            }
        }
        let expected = groups.into_iter().map(|(key, rows, first, last)| {
            json!({"key":key, "rows":rows, "first":first, "last":last})
        }).collect::<Vec<_>>();
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let zones = layout_zones(&prepared);
        if case == "zoned_sparse_filter" {
            assert_eq!(zones.len(), 1);
            assert_eq!(zones[0]["zone_count"], 32);
            assert_eq!(zones[0]["zone_rows"], 256);
        } else {
            assert_eq!(zones, [] as [serde_json::Value; 0]);
        }
        let initial = prepared.snapshot().reserved_without_source_segment_cache();
        let observer = Session::start();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let counts = observer.finish();
        assert_eq!(json_rows(&result), expected);
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        if case == "sparse_filter" {
            assert!(counts.expression_prepared_utf8_batches > 1);
            assert!(counts.aggregate_value_preparations > 1);
            assert_eq!(counts.expression_prepared_utf8_rows, 16);
        }
        print_work(
            case,
            &result,
            &counts,
            &zones,
            &expected,
            &[input_identity(&fixture)],
        );
        drop(result);
        assert_eq!(
            prepared.snapshot().reserved_without_source_segment_cache(),
            initial
        );
    }
}

#[test]
fn survivor_admission_complete_sparse_join_chain_work() {
    let left = Fixture::new(
        StructArray::from_fields(&[("n", PrimitiveArray::from_iter(0..8192_i64).into_array())])
            .unwrap()
            .into_array(),
        512,
    );
    let dimension = Fixture::new(
        StructArray::from_fields(&[(
            "n",
            PrimitiveArray::from_iter((0..16_i64).map(|row| row * 512)).into_array(),
        )])
        .unwrap()
        .into_array(),
        16,
    );
    let joined = |left, right| {
        VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left,
            right,
            kind: JoinKind::Inner,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            }],
            columns: vec![VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            }],
        }))
    };
    let plan = joined(joined(left.scan(), dimension.scan()), dimension.scan());
    let expected = (0..16)
        .map(|row| json!({"n":row * 512}))
        .collect::<Vec<_>>();
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let zones = layout_zones(&prepared);
    let initial = prepared.snapshot().reserved_without_source_segment_cache();
    let observer = Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let counts = observer.finish();
    assert_eq!(json_rows(&result), expected);
    assert!(counts.key_columns > 16);
    assert_eq!(result.execution.output_batches, 4);
    assert_eq!(counts.join_output_rows, 32);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    print_work(
        "sparse_join_chain",
        &result,
        &counts,
        &zones,
        &expected,
        &[input_identity(&left), input_identity(&dimension)],
    );
    drop(result);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        initial
    );
}
