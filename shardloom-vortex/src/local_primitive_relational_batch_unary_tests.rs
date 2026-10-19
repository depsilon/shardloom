//! Existing unary state over finite producers, with complete lifecycle acceptance.

use super::*;

#[path = "local_primitive_relational_batch_unary_fault_tests.rs"]
mod fault_tests;

#[path = "local_primitive_relational_batch_unary_pressure_tests.rs"]
mod pressure_tests;

#[path = "local_primitive_relational_unary_membership_tests.rs"]
mod membership_tests;
use crate::query_primitive::{
    VortexDuplicateKeepPolicy as Keep, VortexExplodeProjectionRequest,
    VortexExpressionProjectionRequest, VortexExpressionRewrite as Rewrite,
    VortexMeltProjectionRequest, VortexQueryPrimitiveKind as Kind, VortexQueryPrimitiveRequest,
    VortexRollingWindowRequest,
};
use crate::relational_query::VortexRelationalUnary;
use serde_json::{Value, json};
use shardloom_plan::ProjectionRequest;
use vortex::array::dtype::{DType, Nullability, PType};

type Row = (Option<i64>, Option<&'static str>, Option<&'static str>);
const ROWS: [Row; 8] = [
    (Some(3), Some("α"), Some("[7,null,8]")),
    (None, None, None),
    (Some(3), Some("α"), Some("[7,null,8]")),
    (Some(1), Some("β"), Some("[]")),
    (Some(2), Some("γ"), Some("[9]")),
    (None, Some("δ"), Some("[10]")),
    (Some(1), Some("ε"), Some("[11,12]")),
    (Some(4), Some(""), Some("[13]")),
];

fn columns(names: &[&str]) -> ProjectionRequest {
    ProjectionRequest::columns(
        names
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
    )
}

fn request(kind: Kind, names: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::for_relational_input(kind, columns(names))
}

fn unary(
    input: VortexRelationalPlan,
    request: VortexQueryPrimitiveRequest,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn native_source(session: &ResidentVortexSession, rows: &[Row]) -> Result<ResidentMemorySource> {
    let numbers = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let strings = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    let lists = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    let dtype = DType::List(
        DType::Primitive(PType::I16, Nullability::Nullable).into(),
        Nullability::Nullable,
    );
    let dtype_json = serde_json::to_string(&dtype).unwrap();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(&numbers),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(&strings),
            },
            MemoryColumn {
                name: "items",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: &dtype_json,
                    values: &lists,
                },
            },
        ],
    )
}

fn prepare_unary(plan: &VortexRelationalPlan, grant: u64) -> Result<PreparedVortexRelational> {
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = grant;
    prepare_relational_with_schema(resources, |schemas| {
        schemas.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            native_source(session, &[])
        })?;
        Ok(plan.clone())
    })
}

fn row_value(row: Row) -> Value {
    let items = row
        .2
        .map_or(Value::Null, |raw| serde_json::from_str(raw).unwrap());
    json!({"n": row.0, "s": row.1, "items": items})
}

#[allow(clippy::too_many_lines)] // Shared inputs define complete independent expectations for each kind.
fn cases(rows: &[Row]) -> Vec<(String, VortexQueryPrimitiveRequest, Vec<Value>)> {
    let all = ["n", "s", "items"];
    let mut result = Vec::new();
    let unique = rows
        .iter()
        .enumerate()
        .filter(|(i, row)| !rows[..*i].contains(row))
        .map(|(_, row)| row_value(*row))
        .collect();
    result.push(("distinct".into(), request(Kind::DistinctRows, &all), unique));
    for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
        let mut selected = Vec::new();
        let mut masks = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let previous = rows[..index].iter().any(|other| other.0 == row.0);
            let following = rows[index + 1..].iter().any(|other| other.0 == row.0);
            let duplicate = match keep {
                Keep::First => previous,
                Keep::Last => following,
                Keep::AllDuplicates => previous || following,
            };
            if !duplicate {
                selected.push(row_value(*row));
            }
            masks.push(json!({"duplicated": duplicate}));
        }
        let mut req = request(Kind::DropDuplicateRows, &all);
        req.deduplicate_key_projection = Some(columns(&["n"]));
        req.duplicate_keep = keep;
        result.push((format!("dedup-{keep:?}"), req, selected));
        let mut req = request(Kind::DuplicateMaskRows, &["n"]);
        req.duplicate_keep = keep;
        result.push((format!("mask-{keep:?}"), req, masks));
    }
    let mut tail = request(Kind::TailRows, &all);
    tail.source_order_limit = Some(3);
    result.push((
        "tail".into(),
        tail,
        rows[rows.len().saturating_sub(3)..]
            .iter()
            .map(|row| row_value(*row))
            .collect(),
    ));
    let mut sample = request(Kind::SampleRows, &all);
    sample.source_order_limit = Some(3);
    sample.sample_seed = Some(7);
    // Fixed independent seed-7 fixture; the other inputs are empty or at most three rows.
    let indices = if rows.len() == ROWS.len() {
        vec![1, 2, 4]
    } else {
        (0..rows.len()).collect()
    };
    result.push((
        "sample".into(),
        sample,
        indices
            .iter()
            .map(|&index| row_value(rows[index]))
            .collect(),
    ));
    let mut expression = request(Kind::ExpressionProjectRows, &all);
    expression.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::ForwardFillNull {
            target_column: ColumnRef::new("n").unwrap(),
            limit: None,
        },
        Rewrite::RowNumber {
            target_column: ColumnRef::new("ordinal").unwrap(),
            start: 10,
        },
    ]));
    let mut last = None;
    let expected = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            if row.0.is_some() {
                last = row.0;
            }
            let mut value = row_value(*row);
            value["n"] = json!(last);
            value["ordinal"] = json!(index + 10);
            value
        })
        .collect();
    result.push(("expression".into(), expression, expected));
    let mut melt = request(Kind::MeltRows, &["n", "s"]);
    melt.melt_projection = Some(VortexMeltProjectionRequest::new(
        vec![ColumnRef::new("s").unwrap()],
        vec![ColumnRef::new("n").unwrap()],
        "variable".into(),
        "value".into(),
    ));
    result.push((
        "melt".into(),
        melt,
        rows.iter()
            .map(|row| json!({"s":row.1,"variable":"n","value":row.0}))
            .collect(),
    ));
    let mut rolling = request(Kind::RollingWindowRows, &["n"]);
    rolling.rolling_window = Some(
        VortexRollingWindowRequest::new(
            ColumnRef::new("n").unwrap(),
            "total".into(),
            3,
            1,
            "sum".into(),
        )
        .with_center(true),
    );
    let expected = (0..rows.len())
        .filter_map(|index| {
            let frame = rows[index.saturating_sub(1)..(index + 2).min(rows.len())]
                .iter()
                .filter_map(|row| row.0)
                .collect::<Vec<_>>();
            (!frame.is_empty()).then(|| {
                let sum = i32::try_from(frame.iter().sum::<i64>()).unwrap();
                json!({"total": f64::from(sum)})
            })
        })
        .collect();
    result.push(("rolling".into(), rolling, expected));
    let mut explode = request(Kind::ExplodeRows, &all);
    explode.explode_projection = Some(VortexExplodeProjectionRequest::new(
        ColumnRef::new("items").unwrap(),
    ));
    let expected = rows
        .iter()
        .flat_map(|row| {
            let items = row.2.map_or_else(
                || vec![Value::Null],
                |raw| serde_json::from_str::<Vec<Value>>(raw).unwrap(),
            );
            items
                .into_iter()
                .map(|item| json!({"n":row.0,"s":row.1,"items":item}))
                .collect::<Vec<_>>()
        })
        .collect();
    result.push(("explode".into(), explode, expected));
    assert_eq!(result.len(), 13);
    result
}

struct Input<'a> {
    rows: &'a [Row],
    width: usize,
    calls: u64,
    position: usize,
    ended: bool,
    prior: Option<Weak<MemoryLease>>,
}

impl<'a> Input<'a> {
    fn new(rows: &'a [Row], width: usize) -> Self {
        Self {
            rows,
            width,
            calls: 0,
            position: 0,
            ended: false,
            prior: None,
        }
    }

    fn next(
        &mut self,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        assert!(!self.ended, "unary producer was reopened");
        assert_eq!(ordinal, self.calls);
        self.calls += 1;
        let rows = if ordinal.is_multiple_of(2) {
            &[][..]
        } else if self.position == self.rows.len() {
            self.ended = true;
            return Ok(None);
        } else {
            let start = self.position;
            self.position = (start + self.width).min(self.rows.len());
            &self.rows[start..self.position]
        };
        let batch = native_source(session, rows)?;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn retained_case(
    rows: &[Row],
    width: usize,
    name: &str,
    req: VortexQueryPrimitiveRequest,
    expected: &[Value],
    spill: bool,
) {
    let plan = unary(scan(), req);
    let (_fixture, directory) = pressure_tests::workspace();
    let mut prepared = prepare_unary(&plan, 8 << 20).unwrap();
    if spill {
        prepared = pressure_tests::with_spill(prepared, &directory);
    }
    let output_dtype = prepared.output_dtype().unwrap();
    let memory = prepared.session.memory().clone();
    let mut input = Input::new(rows, width);
    let mut held = Vec::new();
    let mut actual = Vec::new();
    let report = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert_eq!(uri.as_str(), "memory://stream");
            input.next(ordinal, session)
        })
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            assert_eq!(array.dtype(), &output_dtype);
            actual.extend(values(&array, context)?);
            held.push(array);
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, expected, "{name}/width-{width}");
    assert!(input.ended);
    assert_eq!(input.position, rows.len());
    assert_eq!(input.calls, 2 * rows.len().div_ceil(width) as u64 + 2);
    let trace = report.input.as_ref().unwrap();
    assert_eq!(trace.sources, 1);
    assert_eq!(trace.rows, rows.len() as u64);
    assert_eq!(trace.payload_batches, input.calls - 1);
    assert_eq!(trace.replay_sources, 0);
    assert_eq!(trace.replay_passes, 0);
    assert_eq!(trace.max_retained_input_batches, 1);
    assert!(trace.end_of_input_observed && trace.output_ownership_detached);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    assert!(report.runtime.memory.peak_reserved_bytes <= 8 << 20);
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
    drop(report);
    drop(prepared);
    if !expected.is_empty() {
        assert!(memory.snapshot().reserved_bytes > 0);
    }
    let fresh = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let retained = fresh
        .with_native_execution_context(&CancellationToken::default(), |context| {
            held.iter().try_fold(Vec::new(), |mut result, array| {
                result.extend(values(array, context)?);
                Ok(result)
            })
        })
        .unwrap();
    assert_eq!(retained, expected, "retained {name}/width-{width}");
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    pressure_tests::clean(&directory);
}

#[test]
fn streaming_unary_families_preserve_exact_values_batch_boundaries_and_output_owners() {
    let nulls = [(None, None, None); 3];
    for rows in [&ROWS[..], &[][..], &nulls[..]] {
        for width in [1, 3, 8] {
            for (name, req, expected) in cases(rows) {
                retained_case(rows, width, &name, req, &expected, false);
            }
        }
    }
}

#[test]
fn streaming_unary_admission_keeps_dynamic_pivot_and_unsupported_shape_boundaries() {
    let uri = DatasetUri::new("memory://stream").unwrap();
    for (name, request, _) in cases(&ROWS) {
        let stage = unary(scan(), request);
        assert!(
            !batch_input::classify(&stage, &uri)
                .unwrap()
                .requires_replay(),
            "{name}"
        );
        assert!(
            !batch_input::classify_dynamic(&stage, &uri, false)
                .unwrap()
                .requires_replay(),
            "{name}"
        );
        let repeated = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: stage.clone(),
            right: stage,
            kind: crate::relational_query::VortexRelationalSetKind::UnionAll,
        }));
        assert!(
            batch_input::classify(&repeated, &uri)
                .unwrap()
                .requires_replay(),
            "{name}"
        );
    }
    for kind in [
        Kind::PivotRows,
        Kind::Unsupported,
        Kind::CountAll,
        Kind::SimpleAggregate,
        Kind::SortRows,
    ] {
        let stage = unary(scan(), request(kind, &["n"]));
        let error = batch_input::classify(&stage, &uri).unwrap_err();
        assert!(
            error.to_string().contains("unary stateful"),
            "{kind:?}: {error}"
        );
        if kind == Kind::PivotRows {
            assert!(
                !batch_input::classify_dynamic(&stage, &uri, false)
                    .unwrap()
                    .requires_replay()
            );
        } else {
            assert!(batch_input::classify_dynamic(&stage, &uri, false).is_err());
        }
    }
    let mut stage = scan();
    for _ in 0..25 {
        stage = unary(stage, request(Kind::DistinctRows, &["n"]));
    }
    let error = batch_input::classify(&stage, &uri).unwrap_err();
    assert!(error.to_string().contains("exceeds 24 levels"), "{error}");
    let unsupported = unary(
        VortexRelationalPlan::Outer,
        request(Kind::DistinctRows, &["n"]),
    );
    let error = batch_input::classify(&unsupported, &uri).unwrap_err();
    assert!(error.to_string().contains("correlated source"), "{error}");
}
