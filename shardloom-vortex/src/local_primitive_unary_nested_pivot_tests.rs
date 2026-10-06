use super::*;
use runtime::{VortexLocalPrimitiveRowExportFormat as Format, VortexPivotProjectionRequest};
use serde_json::{Value as Json, json};
use vortex::array::{
    arrays::{FixedSizeListArray, ListViewArray, VarBinArray},
    dtype::PType,
    scalar::Scalar,
};

#[path = "local_primitive_unary_nested_pivot_resource_tests.rs"]
mod resources;
#[path = "local_primitive_unary_nested_pivot_type_tests.rs"]
mod typed;

fn relation_request(aggregate: &str) -> VortexQueryPrimitiveRequest {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        runtime::VortexQueryPrimitiveKind::PivotRows,
        ProjectionRequest::All,
    );
    request.pivot_projection = Some(VortexPivotProjectionRequest::new(
        ColumnRef::new("entity").unwrap(),
        ColumnRef::new("category").unwrap(),
        ColumnRef::new("amount").unwrap(),
        aggregate,
    ));
    request
}

fn oracle() -> Json {
    serde_json::from_str(include_str!(
        "../../docs/architecture/fixtures/native-nested-pivot-state/core-oracles.json"
    ))
    .unwrap()
}

fn list_i64(rows: &[Json]) -> ArrayRef {
    let mut values = Vec::new();
    let mut offsets = Vec::new();
    let mut sizes = Vec::new();
    for row in rows {
        offsets.push(values.len() as u64);
        let elements = row.as_array().map(Vec::as_slice).unwrap_or_default();
        sizes.push(elements.len() as u64);
        values.extend(elements.iter().map(Json::as_i64));
    }
    ListViewArray::try_new(
        PrimitiveArray::from_option_iter(values).into_array(),
        PrimitiveArray::from_iter(offsets).into_array(),
        PrimitiveArray::from_iter(sizes).into_array(),
        Validity::from_iter(rows.iter().map(|row| !row.is_null())),
    )
    .unwrap()
    .into_array()
}

fn table(index: ArrayRef, domain: ArrayRef, value: ArrayRef) -> ArrayRef {
    let count = index.len();
    StructArray::new(
        FieldNames::from(["entity", "category", "amount"]),
        vec![index, domain, value],
        count,
        Validity::NonNullable,
    )
    .into_array()
}

fn core_array(name: &str) -> ArrayRef {
    let data = oracle();
    let rows = data[name]["source"].as_array().unwrap();
    let column = |index| {
        rows.iter()
            .map(|row| row[index].clone())
            .collect::<Vec<_>>()
    };
    let strings = |index| {
        VarBinArray::from(
            rows.iter()
                .map(|row| row[index].as_str().unwrap())
                .collect::<Vec<_>>(),
        )
        .into_array()
    };
    match name {
        "list_index_and_cells" => table(list_i64(&column(0)), strings(1), list_i64(&column(2))),
        "list_domains" => table(
            strings(0),
            list_i64(&column(1)),
            PrimitiveArray::from_iter(rows.iter().map(|row| row[2].as_i64().unwrap())).into_array(),
        ),
        "nested_extrema_margins" => table(strings(0), strings(1), list_i64(&column(2))),
        _ => panic!("unknown fixture {name}"),
    }
}

fn request(fixture: &Fixture, aggregate: &str) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::pivot_rows(
        fixture.uri(),
        VortexPivotProjectionRequest::new(
            ColumnRef::new("entity").unwrap(),
            ColumnRef::new("category").unwrap(),
            ColumnRef::new("amount").unwrap(),
            aggregate,
        ),
    )
}

// Decode only in the test oracle; execution retains native selected arrays.
fn nested_json(scalar: &Scalar) -> Json {
    let Some(ScalarValue::Tuple(values)) = scalar.value() else {
        return scalar_json(scalar);
    };
    match scalar.dtype() {
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => Json::Array(
            values
                .iter()
                .map(|value| {
                    nested_json(&Scalar::try_new(element.as_ref().clone(), value.clone()).unwrap())
                })
                .collect(),
        ),
        DType::Struct(fields, _) => Json::Object(
            fields
                .names()
                .iter()
                .zip(fields.fields())
                .zip(values)
                .map(|((name, dtype), value)| {
                    (
                        name.to_string(),
                        nested_json(&Scalar::try_new(dtype, value.clone()).unwrap()),
                    )
                })
                .collect(),
        ),
        dtype => panic!("unexpected tuple type {dtype}"),
    }
}

fn complete_rows(result: &OwnedVortexResultBatch) -> Json {
    let names = result.dtype().as_struct_fields_opt().unwrap().names();
    let mut context = result.create_execution_ctx();
    let mut rows = Vec::new();
    for array in result.arrays() {
        let columns = names
            .iter()
            .map(|name| runtime::logical_field_from_native_array(array, name.as_ref()).unwrap())
            .collect::<Vec<_>>();
        for row in 0..array.len() {
            rows.push(Json::Array(
                columns
                    .iter()
                    .map(|column| nested_json(&column.execute_scalar(row, &mut context).unwrap()))
                    .collect(),
            ));
        }
    }
    rows.into()
}

fn assert_core(result: &OwnedVortexResultBatch, case: &str, aggregate: &str) {
    let data = oracle();
    assert_eq!(
        complete_rows(result),
        data[case][aggregate],
        "{case}/{aggregate}"
    );
    let names = result
        .dtype()
        .as_struct_fields_opt()
        .unwrap()
        .names()
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>();
    assert_eq!(json!(names), data[case]["output_columns"]);
}

#[test]
fn unary_nested_pivot_core_values_names_and_repeated_execution_match_frozen_oracles() {
    for chunk_rows in [1, 2, 5] {
        for (case, aggregates) in [
            (
                "list_index_and_cells",
                &["first", "count", "min", "max"][..],
            ),
            ("list_domains", &["sum", "count"][..]),
        ] {
            let input = core_array(case);
            let source = input.dtype().as_struct_fields_opt().unwrap().clone();
            let fixture = Fixture::from_array(input, chunk_rows);
            for aggregate in aggregates {
                let prepared = prepare(&request(&fixture, aggregate));
                for _ in 0..2 {
                    let result = prepared.execute_owned().unwrap();
                    assert_core(&result.result, case, aggregate);
                    let fields = result.result.dtype().as_struct_fields_opt().unwrap();
                    assert_eq!(fields.field("entity"), source.field("entity"));
                    let expected = match *aggregate {
                        "count" => DType::Primitive(PType::U64, Nullability::Nullable),
                        "sum" => DType::Primitive(PType::F64, Nullability::Nullable),
                        _ => source.field("amount").unwrap().as_nullable(),
                    };
                    for dtype in fields.fields().skip(1) {
                        assert_eq!(dtype, expected);
                    }
                    assert!(result.execution.native_io_certificate.is_certified());
                }
                assert_eq!(prepared.snapshot().prepared_source_opens, 1);
                assert_eq!(prepared.snapshot().completed_executions, 2);
            }
        }
    }
}

#[test]
fn unary_nested_pivot_extrema_margins_use_complete_values_and_selected_scope() {
    for chunk_rows in [1, 3] {
        let fixture = Fixture::from_array(core_array("nested_extrema_margins"), chunk_rows);
        for aggregate in ["min", "max"] {
            let mut request = request(&fixture, aggregate);
            let projection = request.pivot_projection.as_mut().unwrap();
            projection.margins = true;
            projection.margins_name = "total".into();
            for limit in [None, Some(3)] {
                request.source_order_limit = limit;
                let prepared = prepare(&request);
                let result = prepared.execute_owned().unwrap();
                let key = if limit.is_some() {
                    format!("{aggregate}_source_order_limit_3")
                } else {
                    aggregate.into()
                };
                assert_core(&result.result, "nested_extrema_margins", &key);
                let path = fixture.0.join(format!("{key}.vortex"));
                prepared.write(&path, Format::Vortex, true).unwrap();
                let (dtype, rows) = native_rows(&path);
                assert_eq!(&dtype, result.result.dtype());
                assert_eq!(
                    json!(
                        rows.iter()
                            .map(|row| row.iter().map(nested_json).collect::<Vec<_>>())
                            .collect::<Vec<_>>()
                    ),
                    oracle()["nested_extrema_margins"][&key]
                );
            }
        }
    }
}

#[test]
fn unary_nested_pivot_null_fill_dropna_empty_and_binding_denials_preserve_contract() {
    let input = core_array("list_index_and_cells");
    for empty in [false, true] {
        let fixture = Fixture::from_array(
            if empty {
                input.slice(0..0).unwrap()
            } else {
                input.clone()
            },
            2,
        );
        for aggregate in ["sum", "mean"] {
            let error = prepare_unary(
                &request(&fixture, aggregate),
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            )
            .err()
            .unwrap();
            assert!(
                error.to_string().contains("numeric value column"),
                "{error}"
            );
        }
        let mut request = request(&fixture, "count");
        request.pivot_projection.as_mut().unwrap().margins = true;
        let error = prepare_unary(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("cannot label a nested index"),
            "{error}"
        );
        let projection = request.pivot_projection.as_mut().unwrap();
        projection.margins = false;
        projection.aggregate = "first".into();
        projection.fill_value = Some(shardloom_core::ScalarValue::Int64(0));
        let error = prepare_unary(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("absent or NULL fill"), "{error}");
        request.pivot_projection.as_mut().unwrap().fill_value =
            Some(shardloom_core::ScalarValue::Null);
        for dropna in [false, true] {
            request.pivot_projection.as_mut().unwrap().dropna = dropna;
            let result = prepare(&request).execute_owned().unwrap();
            if empty {
                assert_eq!(complete_rows(&result.result), json!([]));
                let fields = result.result.dtype().as_struct_fields_opt().unwrap();
                assert_eq!(fields.names(), &FieldNames::from(["entity"]));
                assert_eq!(
                    fields.field("entity"),
                    input
                        .dtype()
                        .as_struct_fields_opt()
                        .unwrap()
                        .field("entity")
                );
            } else {
                assert_core(&result.result, "list_index_and_cells", "first");
            }
        }
    }
    let fixture = Fixture::from_array(input, 1);
    let error = prepare(&request(&fixture, "first_unique"))
        .execute_owned()
        .err()
        .unwrap();
    assert!(error.to_string().contains("multiple values"), "{error}");
}

#[test]
fn unary_nested_pivot_domain_names_match_literal_list_fixed_struct_and_collision_fixtures() {
    let data = oracle();
    let fixed = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([1i64, 2]).into_array(),
        2,
        Validity::NonNullable,
        1,
    )
    .unwrap()
    .into_array();
    let structure = StructArray::new(
        FieldNames::from(["a"]),
        vec![PrimitiveArray::from_iter([1i64]).into_array()],
        1,
        Validity::NonNullable,
    )
    .into_array();
    let strings = ListViewArray::try_new(
        VarBinArray::from(vec!["a-b", "a_b"]).into_array(),
        PrimitiveArray::from_iter([0u64, 1]).into_array(),
        PrimitiveArray::from_iter([1u64, 1]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let columns = [
        (list_i64(&[json!([])]), vec![0]),
        (list_i64(&[json!([1])]), vec![1]),
        (list_i64(&[json!([null])]), vec![2]),
        (fixed, vec![3]),
        (structure, vec![4]),
        (list_i64(&[Json::Null]), vec![5]),
        (strings, vec![6, 7]),
    ];
    for (domain, cases) in columns {
        let count = domain.len();
        let fixture = Fixture::from_array(
            table(
                VarBinArray::from(vec!["a"; count]).into_array(),
                domain,
                PrimitiveArray::from_iter(vec![7i64; count]).into_array(),
            ),
            1,
        );
        let result = prepare(&request(&fixture, "first_unique"))
            .execute_owned()
            .unwrap();
        let expected = std::iter::once("entity")
            .chain(cases.iter().map(|index| {
                data["literal_domain_names"][index]["name"]
                    .as_str()
                    .unwrap()
            }))
            .collect::<Vec<_>>();
        let names = result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .names()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>();
        assert_eq!(names, expected);
        let mut row = vec![json!("a")];
        row.extend(vec![json!(7); cases.len()]);
        assert_eq!(complete_rows(&result.result), json!([row]));
    }
}
