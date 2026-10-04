use super::*;
use crate::relational_query::VortexRelationalProject;
use shardloom_core::{ExprId, Expression, ExpressionKind};
use vortex::array::arrays::{Struct, struct_::StructArrayExt as _};

pub(super) fn dtype(
    fixture: &Fixture,
    request: &VortexQueryPrimitiveRequest,
    direct: bool,
) -> DType {
    if direct {
        let mut request = request.clone();
        request.source_uri = Some(DatasetUri::new(fixture.path().display().to_string()).unwrap());
        prepare_unary(&request, policy())
            .unwrap()
            .execute_owned()
            .unwrap()
            .result
            .dtype()
            .clone()
    } else {
        prepared_composed(fixture, request)
            .unwrap()
            .execute_owned()
            .unwrap()
            .result
            .dtype()
            .clone()
    }
}

#[test]
fn typed_unary_empty_and_all_null_payloads_keep_declared_schema() {
    let full = source();
    let empty = Fixture::new(source_array().slice(0..0).unwrap(), 1);
    let null = Fixture::new(source_array().slice(3..4).unwrap(), 1);
    let mut requests = Vec::new();
    for kind in [
        Kind::TailRows,
        Kind::SampleRows,
        Kind::DistinctRows,
        Kind::DropDuplicateRows,
    ] {
        let mut req = request(kind, &ALL);
        if kind == Kind::DropDuplicateRows {
            req.deduplicate_key_projection = Some(columns(&TYPED));
        }
        if matches!(kind, Kind::TailRows | Kind::SampleRows) {
            req.source_order_limit = Some(1);
        }
        req.sample_seed = (kind == Kind::SampleRows).then_some(11);
        requests.push(req);
    }
    requests.push(rewritten(
        TYPED
            .iter()
            .map(|name| Rewrite::ForwardFillNull {
                target_column: ColumnRef::new(*name).unwrap(),
                limit: None,
            })
            .collect(),
    ));
    for req in requests {
        for direct in [true, false] {
            assert_eq!(rows(&empty, &req, direct), Vec::<Value>::new());
            assert_eq!(rows(&null, &req, direct), oracle()[3..4]);
            assert_eq!(dtype(&empty, &req, direct), dtype(&full, &req, direct));
            assert_eq!(dtype(&null, &req, direct), dtype(&full, &req, direct));
        }
    }
}

#[test]
fn typed_unary_nullable_parent_masks_hidden_extreme_values_before_retention() {
    let original = source_array();
    let structure = original.as_::<Struct>();
    let children = ALL
        .iter()
        .map(|name| structure.unmasked_field_by_name(name).unwrap().clone())
        .collect::<Vec<_>>();
    let nullable = StructArray::new(
        FieldNames::from(ALL),
        children,
        6,
        Validity::from_iter([true, false, true, true, true, true]),
    )
    .into_array();
    let mut expected = oracle();
    expected[1] = json!({"id":null,"bytes":null,"decimal":null,"day":null,"instant":null});
    let mut tail = request(Kind::TailRows, &ALL);
    tail.source_order_limit = Some(6);
    // Vortex's file-statistics accumulator does not admit nullable top-level
    // structs. Exercise the produced native boundary directly instead.
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let bound = crate::local_primitives::prepared_unary::BoundUnary::for_relation(
        &tail,
        nullable.dtype(),
        session.memory(),
    )
    .unwrap();
    let mut actual = Vec::new();
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut execution = context.native_session().create_execution_ctx();
        bound.consume_relation(context, None, 2, |consume| consume(nullable), &mut |array| {
            assert!(array.dtype().as_struct_fields_opt().unwrap().field("id").unwrap().is_nullable());
            for row in 0..array.len() {
                actual.push(Value::Object(ALL.iter().map(|name| {
                    let column = crate::local_primitives::logical_field_from_native_array(&array, name)?;
                    Ok(((*name).into(), result_batch::scalar_value(&column, row, &mut execution)?.into_json()?))
                }).collect::<Result<serde_json::Map<_, _>>>()?));
            }
            Ok(())
        }).map(|_| ())
    }).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn typed_unary_consumes_renamed_transformed_input_and_composes_with_native_sort() {
    let fixture = source();
    let renamed = ["seq", "blob", "money", "date", "time"];
    let input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: sort(fixture.scan(), "id"),
        expressions: ALL
            .iter()
            .zip(renamed)
            .map(|(from, to)| {
                (
                    to.into(),
                    Expression::new(
                        ExprId::new(format!("rename-{from}")).unwrap(),
                        ExpressionKind::Column(ColumnRef::new(*from).unwrap()),
                    ),
                )
            })
            .collect(),
    }));
    let mut req = request(Kind::DropDuplicateRows, &renamed);
    req.deduplicate_key_projection = Some(columns(&["blob"]));
    req.duplicate_keep = Keep::Last;
    let plan = sort(unary(input, req), "seq");
    let expected = [5, 3, 1, 0].map(|index| {
        Value::Object(
            ALL.iter()
                .zip(renamed)
                .map(|(from, to)| (to.into(), oracle()[index][*from].clone()))
                .collect(),
        )
    });
    let prepared = prepare_relational(&plan, policy()).unwrap();
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert!(result.execution.native_io_certificate.is_certified());
    }
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
}

#[test]
fn typed_unary_pivot_fill_rescales_present_decimals_and_preserves_present_null() {
    let fixture = source();
    let mut req = pivot("bytes", "id", "decimal", "first");
    req.pivot_projection.as_mut().unwrap().fill_value = Some(decimal(5, 3, 3));
    let original = oracle();
    let amounts = [
        Some(1230),
        Some(-4560),
        Some(1230),
        None,
        Some(-4560),
        Some(0),
    ];
    let expected = [1, 0, 5, 3].map(|index| {
        let mut row = serde_json::Map::new();
        row.insert("bytes".into(), original[index]["bytes"].clone());
        for (source, value) in original.iter().enumerate() {
            let amount = if value["bytes"] == original[index]["bytes"] {
                amounts[source]
            } else {
                Some(5)
            };
            row.insert(
                format!("pivot_{source}"),
                amount.map_or(Value::Null, |value| {
                    json!(format!("decimal128(21,3):{value}"))
                }),
            );
        }
        Value::Object(row)
    });
    for direct in [true, false] {
        assert_eq!(rows(&fixture, &req, direct), expected);
        let fields = dtype(&fixture, &req, direct);
        assert_eq!(
            fields.as_struct_fields_opt().unwrap().field("pivot_0"),
            Some(DType::Decimal(
                DecimalDType::new(21, 3),
                Nullability::Nullable
            ))
        );
    }
}

#[test]
fn typed_unary_pivot_temporal_domain_names_disambiguate_without_losing_cells() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "day", "bytes"]),
            vec![
                PrimitiveArray::from_iter([0u64, 0, 0]).into_array(),
                ExtensionArray::new(
                    Date::new(TimeUnit::Days, Nullability::NonNullable).erased(),
                    PrimitiveArray::from_iter([-1i32, 1, -1]).into_array(),
                )
                .into_array(),
                VarBinArray::from(vec![&b"\x00\xff"[..], &b"\xfe"[..], &b"\x0a"[..]]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    for direct in [true, false] {
        assert_eq!(
            rows(&fixture, &pivot("id", "day", "bytes", "first"), direct),
            [json!({"id":0,"pivot_date32_1":"00ff","pivot_date32_1_2":"fe"})]
        );
        let error = denied(
            &fixture,
            &pivot("id", "day", "bytes", "first_unique"),
            direct,
        );
        assert!(error.contains("multiple values"), "{error}");
    }
}
