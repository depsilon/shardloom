use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
        VortexRelationalFrameFunction as Reduction, VortexRelationalFrameUnit as Unit,
        VortexRelationalOrderKey, VortexRelationalWindow as Window,
        VortexRelationalWindowExpression as WindowExpression, VortexRelationalWindowFrame as Frame,
        VortexRelationalWindowFunction as Function,
    },
};
use shardloom_core::BinaryOp;

fn arithmetic(op: BinaryOp, value: i64) -> Expression {
    Expression::new(
        ExprId::new("computed").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(column("n")),
            op,
            right: Box::new(Expression::literal(
                ExprId::new("constant").unwrap(),
                ScalarValue::Int64(value),
            )),
        },
    )
}

fn plan(fixture: &ParquetFixture, fail: bool) -> VortexRelationalPlan {
    let mut expressions = vec![
        ("text".into(), column("text")),
        ("n".into(), column("n")),
        ("other".into(), column("other")),
        ("shifted".into(), arithmetic(BinaryOp::Add, 1)),
        ("again".into(), arithmetic(BinaryOp::Add, 1)),
    ];
    if fail {
        expressions.push(("unused_failure".into(), arithmetic(BinaryOp::Divide, 0)));
    }
    let input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions,
    }));
    let reduction = |function, source, alias| {
        Measure::new(function, Some(ColumnRef::new(source).unwrap()), alias)
    };
    let functions = vec![
        (
            "median",
            Function::Framed(Reduction::Aggregate(Box::new(reduction(
                "median",
                "shifted",
                "median".into(),
            )))),
        ),
        (
            "selected",
            Function::Framed(Reduction::Aggregate(Box::new(
                reduction("quantile_disc", "again", "selected".into())
                    .with_quantile_level(0.5)
                    .unwrap(),
            ))),
        ),
        (
            "previous",
            Function::Lag {
                column: ColumnRef::new("other").unwrap(),
                offset: 1,
            },
        ),
        (
            "unused_covariance",
            Function::Framed(Reduction::Aggregate(Box::new(
                reduction("covar_pop", "shifted", "unused_covariance".into())
                    .with_secondary_column(ColumnRef::new("other").unwrap()),
            ))),
        ),
    ];
    let window = VortexRelationalPlan::Window(Box::new(Window {
        input,
        columns: ["text", "n", "other"]
            .map(|name| ColumnRef::new(name).unwrap())
            .to_vec(),
        expressions: functions
            .into_iter()
            .map(|(name, function)| WindowExpression {
                output_column: name.into(),
                function,
                partition_by: vec![],
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("n").unwrap(),
                    descending: false,
                    nulls: None,
                }],
                frame: Some(Frame {
                    unit: Unit::Rows,
                    start: Bound::CurrentRow,
                    end: Bound::CurrentRow,
                    exclusion: Exclusion::NoOthers,
                }),
            })
            .collect(),
    }));
    project(
        window,
        &[
            ("n", "n"),
            ("median", "median"),
            ("selected", "selected"),
            ("previous", "previous"),
        ],
    )
}

#[test]
fn parquet_demand_windows_prune_plain_carriers_rebind_reuse_and_keep_fallible_computations() {
    for fail in [false, true] {
        let fixture = ParquetFixture::new();
        fixture.corrupt_chunk(0, 2);
        let prepared = fixture.prepare(&plan(&fixture, fail));
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        if fail {
            let error = result.err().unwrap().to_string();
            assert!(error.contains("division by zero"), "{error}");
            assert_eq!(prepared.snapshot().completed_executions, 0);
        } else {
            let result = result.unwrap();
            let expected = (0i32..16)
                .map(|n| {
                    serde_json::json!({"n":n,"median":f64::from(n+1),"selected":n+1,
                "previous":(n>0).then_some(n+99)})
                })
                .collect::<Vec<_>>();
            assert_eq!(json_rows(&result), expected);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.columnar_projected_columns, 2);
            assert_eq!(input.columnar_available_columns, 3);
            assert_eq!(input.columnar_physical_leaf_columns, 2);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
