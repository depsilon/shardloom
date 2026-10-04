use super::*;
use crate::{
    local_primitives::VortexPivotProjectionRequest,
    relational_query::{VortexRelationalLimit, VortexRelationalProject, VortexRelationalUnary},
};
use shardloom_core::{ExprId, Expression};
use std::{cell::RefCell, rc::Rc};
use vortex::array::arrays::VarBinArray;

#[path = "local_primitive_relational_dynamic_lifecycle_tests.rs"]
mod lifecycle;

fn fixture() -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "category", "amount"]),
            vec![
                PrimitiveArray::from_iter([1_u64, 1, 2, 2]).into_array(),
                VarBinArray::from(vec!["a", "b", "a", "a"]).into_array(),
                PrimitiveArray::from_iter([3_u64, 5, 7, 11]).into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn uri(fixture: &Fixture) -> DatasetUri {
    DatasetUri::new(fixture.path().display().to_string()).unwrap()
}

fn pivot(input: VortexRelationalPlan) -> VortexRelationalPlan {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::PivotRows,
        shardloom_plan::ProjectionRequest::All,
    );
    request.pivot_projection = Some(VortexPivotProjectionRequest::new(
        ColumnRef::new("entity").unwrap(),
        ColumnRef::new("category").unwrap(),
        ColumnRef::new("amount").unwrap(),
        "sum",
    ));
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn project(input: VortexRelationalPlan, names: &[String]) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: names
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap()),
                )
            })
            .collect(),
    }))
}

fn prepare(fixture: &Fixture) -> PreparedVortexRelational {
    let plan = pivot(fixture.scan());
    prepare_relational_with_dynamic_schema(&[uri(fixture)], policy(), 65_536, move |schemas| {
        let (plan, columns) = schemas.resolve_output(&plan)?;
        assert_eq!(columns, ["entity", "pivot_a", "pivot_b"]);
        Ok(project(plan, &columns))
    })
    .unwrap()
}

fn expected() -> Vec<serde_json::Value> {
    serde_json::json!([
        {"entity":1,"pivot_a":3.0,"pivot_b":5.0},
        {"entity":2,"pivot_a":18.0,"pivot_b":null},
    ])
    .as_array()
    .unwrap()
    .clone()
}

#[test]
fn native_dynamic_pivot_prepares_without_execution_and_scans_once_per_fresh_call() {
    let fixture = fixture();
    let prepared = prepare(&fixture);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    assert!(prepared.output_dtype().is_none());
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    for call in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected());
        assert_eq!(result.execution.scan_rows_delivered, 4);
        assert_eq!(result.execution.runtime.completed_executions, call);
        assert!(result.execution.native_io_certificate.is_certified());
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    let memory = prepared.session.memory().clone();
    let owned = prepared.execute_owned().unwrap();
    drop(prepared);
    assert_eq!(owned.result.row_count(), 2);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(owned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_dynamic_pivot_empty_domain_keeps_actual_index_schema() {
    let fixture = fixture();
    let plan = pivot(VortexRelationalPlan::Limit(Box::new(
        VortexRelationalLimit {
            input: fixture.scan(),
            offset: 0,
            count: 0,
        },
    )));
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let (plan, columns) = schemas.resolve_output(&plan)?;
            assert_eq!(columns, ["entity"]);
            Ok(project(plan, &columns))
        },
    )
    .unwrap();
    let mut batches = 0;
    prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            batches += 1;
            assert_eq!(array.len(), 0);
            assert_eq!(
                array.dtype(),
                &DType::struct_(
                    [(
                        "entity",
                        DType::Primitive(PType::U64, Nullability::NonNullable)
                    )],
                    Nullability::NonNullable
                )
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(batches, 1);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_dynamic_pivot_writes_actual_schema_through_all_eight_sinks() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = fixture();
    let prepared = prepare(&fixture);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let dtype = DType::struct_(
        [
            (
                "entity",
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
            (
                "pivot_a",
                DType::Primitive(PType::F64, Nullability::Nullable),
            ),
            (
                "pivot_b",
                DType::Primitive(PType::F64, Nullability::Nullable),
            ),
        ],
        Nullability::NonNullable,
    );
    for (call, format) in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ]
    .into_iter()
    .enumerate()
    {
        let path = fixture.0.join(format!("output-{format:?}"));
        let written = prepared.write(&path, format, false).unwrap();
        assert_eq!(written.execution.scan_rows_delivered, 4);
        assert_eq!(written.output.rows_scanned, 4);
        assert_eq!(written.output.rows_written, 2);
        assert_eq!(
            written.execution.runtime.completed_executions,
            call as u64 + 1
        );
        if format == Format::Csv {
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                "entity,pivot_a,pivot_b\n1,3,5\n2,18,\n"
            );
        } else {
            assert_eq!(
                super::writer_tests::read_rows(&path, format, &dtype),
                expected()
            );
        }
        drop(written);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn native_dynamic_pivot_rejects_repeated_and_foreign_execution_references() {
    let fixture = fixture();
    let plan = pivot(fixture.scan());
    let previous = Rc::new(RefCell::new(None));
    let saved = previous.clone();
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            if let Some(prior) = saved.borrow_mut().take() {
                return Ok(prior);
            }
            let (resolved, _) = schemas.resolve_output(&plan)?;
            *saved.borrow_mut() = Some(resolved.clone());
            Ok(resolved)
        },
    )
    .unwrap();
    drop(prepared.execute_owned().unwrap());
    assert!(
        prepared
            .execute_owned()
            .err()
            .unwrap()
            .to_string()
            .contains("different execution")
    );
    let plan = pivot(fixture.scan());
    let repeated = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let (resolved, _) = schemas.resolve_output(&plan)?;
            Ok(VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                left: resolved.clone(),
                right: resolved,
                kind: SetKind::UnionAll,
            })))
        },
    )
    .unwrap();
    let baseline = repeated.snapshot().memory.reserved_bytes;
    assert!(
        repeated
            .execute_owned()
            .err()
            .unwrap()
            .to_string()
            .contains("consumed twice")
    );
    assert_eq!(repeated.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_dynamic_pivot_late_binding_failure_releases_state_and_publishes_nothing() {
    let fixture = fixture();
    let plan = pivot(fixture.scan());
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let (resolved, _) = schemas.resolve_output(&plan)?;
            Ok(project(resolved, &["not_observed".into()]))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let path = fixture.0.join("not-published.vortex");
    assert!(
        prepared
            .write(
                &path,
                crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
                false
            )
            .is_err()
    );
    assert!(!path.exists());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
