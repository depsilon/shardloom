use super::*;
use vortex::array::{
    arrays::{ChunkedArray, DecimalArray, DictArray, ExtensionArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

fn fixed(children: ArrayRef, width: u32, valid: &[bool]) -> ArrayRef {
    FixedSizeListArray::try_new(
        children,
        width,
        Validity::from_iter(valid.iter().copied()),
        valid.len(),
    )
    .unwrap()
    .into_array()
}

fn reopened_json(path: &std::path::Path) -> (DType, Json) {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session.prepare_file(path).unwrap();
    let dtype = source.dtype().clone();
    let cancellation = CancellationToken::default();
    let rows = source
        .with_native_execution_controlled(&cancellation, |file, context| {
            let mut execution = context.native_session().create_execution_ctx();
            let mut rows = Vec::new();
            for array in file
                .scan()
                .map_err(vortex_error)?
                .with_ordered(true)
                .into_array_iter(context.runtime())
                .map_err(vortex_error)?
            {
                let array = array.map_err(vortex_error)?;
                // The native JSON boundary observes temporal integer storage. The
                // provider's calendar-scalar constructor has a narrower range.
                let column =
                    runtime::native_json::Column::new(&array, &mut execution, context.memory())?;
                for row in 0..array.len() {
                    let mut bytes = Vec::new();
                    column.write(row, &mut bytes, &mut execution, &cancellation)?;
                    rows.push(serde_json::from_slice::<Json>(&bytes).unwrap());
                }
            }
            Ok(rows)
        })
        .unwrap();
    (dtype, rows.into())
}

fn encoded_columns(input: &ArrayRef) -> ArrayRef {
    let mut columns = Vec::new();
    for name in ["entity", "category", "amount"] {
        let original = runtime::logical_field_from_native_array(input, name).unwrap();
        let dictionary = DictArray::try_new(
            PrimitiveArray::from_iter([0u8, 1, 2, 3, 4, 5]).into_array(),
            original.clone(),
        )
        .unwrap()
        .into_array();
        columns.push(
            ChunkedArray::try_new(
                vec![
                    original.slice(0..2).unwrap(),
                    dictionary.slice(2..6).unwrap(),
                ],
                original.dtype().clone(),
            )
            .unwrap()
            .into_array(),
        );
    }
    StructArray::new(
        FieldNames::from(["entity", "category", "amount"]),
        columns,
        6,
        Validity::NonNullable,
    )
    .into_array()
}

fn all_nested_roles() -> ArrayRef {
    table(
        list_i64(&[
            json!([2]),
            json!([1]),
            json!([2]),
            Json::Null,
            json!([1]),
            json!([null]),
        ]),
        list_i64(&[
            json!([1]),
            json!([2]),
            json!([1]),
            json!([]),
            json!([2]),
            json!([]),
        ]),
        fixed(
            PrimitiveArray::from_option_iter([
                Some(7i16),
                Some(9),
                Some(3),
                None,
                Some(7),
                Some(9),
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(8),
                Some(9),
            ])
            .into_array(),
            2,
            &[true; 6],
        ),
    )
}

#[test]
fn unary_nested_pivot_all_roles_match_across_parent_dictionary_and_chunk_boundaries() {
    let input = all_nested_roles();
    let expected = json!([
        [[1], null, null, [3, null]],
        [[2], null, [7, 9], null],
        [[null], [8, 9], null, null],
        [null, [1, 2], null, null],
    ]);
    let encoded = encoded_columns(&input);
    for source in [input, encoded] {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let request = relation_request("first_unique");
        let bound = BoundUnary::for_relation(&request, source.dtype(), &memory).unwrap();
        let mut output = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let completed = bound.complete_relation_pivot(context, None, |accept| {
                    accept(source.slice(0..3).unwrap())?;
                    accept(source.slice(3..6).unwrap())
                })?;
                assert_eq!(
                    completed
                        .fields()
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>(),
                    ["entity", "pivot_l0", "pivot_l1_i1", "pivot_l1_i2"]
                );
                completed.emit(&bound, context, 2, None, &mut |array| {
                    output.push(array);
                    Ok(())
                })
            })
            .unwrap();
        let mut execution = VortexSession::default().create_execution_ctx();
        let mut rows = Vec::new();
        for batch in &output {
            assert!(batch.len() <= 2);
            let fields = batch.dtype().as_struct_fields_opt().unwrap();
            assert_eq!(
                fields.field("pivot_l0"),
                Some(DType::FixedSizeList(
                    std::sync::Arc::new(DType::Primitive(PType::I16, Nullability::Nullable)),
                    2,
                    Nullability::Nullable
                ))
            );
            for row in 0..batch.len() {
                rows.push(
                    fields
                        .names()
                        .iter()
                        .map(|name| {
                            let field =
                                runtime::logical_field_from_native_array(batch, name.as_ref())
                                    .unwrap();
                            nested_json(&field.execute_scalar(row, &mut execution).unwrap())
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert_eq!(json!(rows), expected);
        drop((bound, output, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn unary_nested_pivot_struct_cells_preserve_field_order_exact_leaves_and_hidden_children() {
    let edge = 99_999_999_999_999_999_999_999_999_999_999_999_999i128;
    let cells = StructArray::new(
        FieldNames::from(["z", "day", "instant", "binary", "narrow"]),
        vec![
            DecimalArray::from_option_iter(
                [Some(edge), Some(-edge), Some(1), None],
                DecimalDType::new(38, 6),
            )
            .into_array(),
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::NonNullable).erased(),
                PrimitiveArray::from_iter([i32::MIN, i32::MAX, 0, 0]).into_array(),
            )
            .into_array(),
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
                PrimitiveArray::from_iter([i64::MIN, i64::MAX, 0, 0]).into_array(),
            )
            .into_array(),
            VarBinArray::from(vec![
                &b"\x00\xff"[..],
                &b"\xfe"[..],
                &b""[..],
                &b"hidden"[..],
            ])
            .into_array(),
            fixed(
                PrimitiveArray::from_iter([i8::MIN, i8::MAX, 1, 2, 3, 4, 5, 6]).into_array(),
                2,
                &[true; 4],
            ),
        ],
        4,
        Validity::from_iter([true, true, true, false]),
    )
    .into_array();
    let index = fixed(
        PrimitiveArray::from_iter([1i16, 2, 1, 2, 3, 4, 777, 888]).into_array(),
        2,
        &[true, true, true, false],
    );
    let domain = fixed(
        PrimitiveArray::from_iter([1i64, 2, 1, 2, 2, 3, 1, 2]).into_array(),
        2,
        &[true; 4],
    );
    let input = table(index.clone(), domain, cells.clone());
    let fixture = Fixture::from_array(input, 1);
    let literal_cells = [
        json!({"z":format!("decimal128(38,6):{edge}"),"day":i32::MIN,
            "instant":i64::MIN,"binary":"00ff","narrow":[i8::MIN,i8::MAX]}),
        json!({"z":format!("decimal128(38,6):-{edge}"),"day":i32::MAX,
            "instant":i64::MAX,"binary":"fe","narrow":[1,2]}),
        json!({"z":"decimal128(38,6):1","day":0,
            "instant":0,"binary":"","narrow":[3,4]}),
    ];
    // Selection is specified from these literal values before the candidate
    // runs; neither comparator output nor output readback supplies the oracle.
    for (aggregate, chosen) in [("first", 0), ("min", 1), ("max", 0)] {
        let prepared = prepare(&request(&fixture, aggregate));
        let output = prepared.execute_owned().unwrap();
        let fields = output.result.dtype().as_struct_fields_opt().unwrap();
        assert_eq!(
            fields.names(),
            &FieldNames::from(["entity", "pivot_f2_i1_i2", "pivot_f2_i2_i3"])
        );
        assert_eq!(fields.field("entity"), Some(index.dtype().clone()));
        assert_eq!(
            fields.field("pivot_f2_i1_i2"),
            Some(cells.dtype().as_nullable())
        );
        let path = fixture.0.join(format!("{aggregate}.vortex"));
        prepared.write(&path, Format::Vortex, false).unwrap();
        let (dtype, actual) = reopened_json(&path);
        assert_eq!(&dtype, output.result.dtype());
        let expected = json!([
            {"entity":[1,2],"pivot_f2_i1_i2":literal_cells[chosen],"pivot_f2_i2_i3":null},
            {"entity":[3,4],"pivot_f2_i1_i2":null,"pivot_f2_i2_i3":literal_cells[2]},
            {"entity":null,"pivot_f2_i1_i2":null,"pivot_f2_i2_i3":null},
        ]);
        assert_eq!(actual, expected, "{aggregate}");
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let collected = collected
            .result_jsonl
            .value()
            .lines()
            .map(|line| serde_json::from_str::<Json>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(json!(collected), expected, "{aggregate} collection");
    }
}

#[test]
fn unary_nested_pivot_signed_zero_is_distinct_for_keys_and_equal_for_duplicate_values() {
    let zeros = StructArray::new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_iter([-0.0f64, 0.0]).into_array()],
        2,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::from_array(
        table(
            zeros.clone(),
            zeros.clone(),
            PrimitiveArray::from_iter([7i64, 9]).into_array(),
        ),
        1,
    );
    let result = prepare(&request(&fixture, "first_unique"))
        .execute_owned()
        .unwrap();
    let names = result
        .result
        .dtype()
        .as_struct_fields_opt()
        .unwrap()
        .names();
    assert_eq!(
        names,
        &FieldNames::from([
            "entity",
            "pivot_s1_5_valuef0000000000000000",
            "pivot_s1_5_valuef8000000000000000",
        ])
    );
    assert_eq!(
        complete_rows(&result.result),
        json!([
            [{"value":0.0},9,null], [{"value":-0.0},null,7],
        ])
    );
    let values_fixture = Fixture::from_array(
        table(
            VarBinArray::from(vec!["a", "a"]).into_array(),
            VarBinArray::from(vec!["x", "x"]).into_array(),
            zeros,
        ),
        1,
    );
    let prepared = prepare(&request(&values_fixture, "first_unique"));
    let path = values_fixture.0.join("zeros.vortex");
    prepared.write(&path, Format::Vortex, false).unwrap();
    let (_, rows) = native_rows(&path);
    let value = rows[0][1].as_struct().field("value").unwrap();
    assert_eq!(
        value.as_primitive().as_::<f64>().unwrap().to_bits(),
        (-0.0f64).to_bits()
    );
}

#[test]
fn unary_nested_pivot_null_parents_ignore_hidden_fixed_and_struct_children() {
    for value in [
        fixed(
            PrimitiveArray::from_iter([f64::NAN, f64::INFINITY, -7.0, -8.0]).into_array(),
            2,
            &[false, false],
        ),
        StructArray::new(
            FieldNames::from(["hidden"]),
            vec![PrimitiveArray::from_iter([f64::NAN, f64::INFINITY]).into_array()],
            2,
            Validity::AllInvalid,
        )
        .into_array(),
    ] {
        let fixture = Fixture::from_array(table(value.clone(), value.clone(), value.clone()), 1);
        for aggregate in ["first", "first_unique", "min", "max"] {
            let result = prepare(&request(&fixture, aggregate))
                .execute_owned()
                .unwrap();
            assert_eq!(complete_rows(&result.result), json!([[null, null]]));
            assert_eq!(
                result
                    .result
                    .dtype()
                    .as_struct_fields_opt()
                    .unwrap()
                    .names(),
                &FieldNames::from(["entity", "pivot_value"])
            );
        }
    }
}

#[test]
fn unary_nested_pivot_rejects_unsupported_empty_nested_leaves_and_policy_shapes() {
    let n = Nullability::Nullable;
    let mut deep = DType::Primitive(PType::I64, n);
    for _ in 0..25 {
        deep = DType::List(std::sync::Arc::new(deep), n);
    }
    let invalid = [
        DType::List(std::sync::Arc::new(DType::Variant(n)), n),
        DType::List(std::sync::Arc::new(DType::Primitive(PType::F16, n)), n),
        DType::FixedSizeList(
            std::sync::Arc::new(DType::Decimal(DecimalDType::new(39, 0), n)),
            1,
            n,
        ),
        DType::List(
            std::sync::Arc::new(DType::Extension(
                Timestamp::new(TimeUnit::Nanoseconds, n).erased(),
            )),
            n,
        ),
        deep,
    ];
    let memory = shardloom_exec::live_memory::LiveMemoryPool::new(1 << 20).unwrap();
    for invalid in invalid {
        // Binding sees only declared types, so an empty physical provider cannot
        // turn an unsupported declaration into a successful no-op.
        for role in 0..3 {
            let mut fields = vec![DType::Utf8(n); 3];
            fields[role] = invalid.clone();
            let dtype = DType::struct_(
                ["entity", "category", "amount"].into_iter().zip(fields),
                Nullability::NonNullable,
            );
            assert!(
                BoundUnary::for_relation(&relation_request("first_unique"), &dtype, &memory)
                    .is_err()
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    let values = list_i64(&[json!([1])]);
    let fixture = Fixture::from_array(
        table(
            PrimitiveArray::from_iter([1u8]).into_array(),
            VarBinArray::from(vec!["x"]).into_array(),
            values,
        ),
        1,
    );
    let mut request = request(&fixture, "min");
    request.pivot_projection.as_mut().unwrap().margins = true;
    let error = prepare_unary(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
    )
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("require a UTF8 index"),
        "{error}"
    );
}
