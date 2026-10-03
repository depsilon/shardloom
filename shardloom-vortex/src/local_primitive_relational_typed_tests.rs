use super::*;
use crate::local_primitives::{native_json, native_payload, native_relational_batch};
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalNullOrder, VortexRelationalOrderKey,
    VortexRelationalSort,
};
use serde_json::{Value, json};
use vortex::array::{
    arrays::{DecimalArray, ExtensionArray, ListViewArray, VarBinArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_typed_io_tests.rs"]
mod io_tests;

#[path = "local_primitive_relational_typed_key_tests.rs"]
mod key_tests;

#[path = "local_primitive_relational_typed_expression_tests.rs"]
mod expression_tests;

#[path = "local_primitive_relational_source_handoff_tests.rs"]
mod source_handoff_tests;

const DECIMAL_EDGE: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

fn payloads() -> Vec<ArrayRef> {
    vec![
        VarBinArray::from(vec![
            Some(&b"\x00\xff\x10"[..]),
            Some(&b""[..]),
            None,
            Some(&b"\xc3\xa9"[..]),
        ])
        .into_array(),
        DecimalArray::from_option_iter(
            [Some(1_234_567i128), Some(-DECIMAL_EDGE), None, Some(0)],
            DecimalDType::new(38, 6),
        )
        .into_array(),
        ExtensionArray::new(
            Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([Some(-1i32), Some(20_000), None, Some(0)])
                .into_array(),
        )
        .into_array(),
        ExtensionArray::new(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([
                Some(-1i64),
                Some(1_700_000_000_123_456),
                None,
                Some(0),
            ])
            .into_array(),
        )
        .into_array(),
    ]
}

fn fixture() -> Fixture {
    let mut columns = vec![PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array()];
    columns.extend(payloads());
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "payload", "amount", "day", "instant"]),
            columns,
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn expected() -> Vec<Value> {
    vec![
        json!({"id":1,"payload":"00ff10","amount":"decimal128(38,6):1234567","day":-1,"instant":-1}),
        json!({"id":2,"payload":"","amount":format!("decimal128(38,6):-{DECIMAL_EDGE}"),"day":20000,"instant":1_700_000_000_123_456i64}),
        json!({"id":3,"payload":null,"amount":null,"day":null,"instant":null}),
        json!({"id":4,"payload":"c3a9","amount":"decimal128(38,6):0","day":0,"instant":0}),
    ]
}

fn collect(plan: &VortexRelationalPlan) -> Vec<Value> {
    json_rows(
        &prepare_relational(plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    )
}

fn sort(input: VortexRelationalPlan, name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new(name).unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}

#[test]
fn native_typed_payload_collect_sort_union_and_empty_preserve_values() {
    let fixture = fixture();
    assert_eq!(collect(&fixture.scan()), expected());
    let mut reverse = expected();
    reverse.reverse();
    assert_eq!(collect(&sort(fixture.scan(), "id")), reverse);
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: sort(fixture.scan(), "id"),
        right: fixture.scan(),
        kind: SetKind::UnionAll,
    }));
    reverse.extend(expected());
    assert_eq!(collect(&union), reverse);
    let empty = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: union,
        offset: 0,
        count: 0,
    }));
    assert_eq!(collect(&empty), [] as [Value; 0]);
    assert_eq!(
        prepare_relational(&empty, policy()).unwrap().output_dtype(),
        prepare_relational(&fixture.scan(), policy())
            .unwrap()
            .output_dtype()
    );
}

#[test]
fn native_typed_payload_outer_join_nulls_every_missing_field() {
    let left = fixture();
    let right = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: left.scan(),
        offset: 0,
        count: 2,
    }));
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: left.scan(),
        right,
        kind: JoinKind::Left,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("id").unwrap(),
            right: ColumnRef::new("id").unwrap(),
        }],
        columns: std::iter::once((Side::Left, "id"))
            .chain(
                ["payload", "amount", "day", "instant"]
                    .into_iter()
                    .map(|name| (Side::Right, name)),
            )
            .map(|(side, name)| VortexRelationalJoinColumn {
                side,
                column: ColumnRef::new(name).unwrap(),
                output_column: name.into(),
            })
            .collect(),
    }));
    let mut expected = expected();
    expected[3] = json!({"id":4,"payload":null,"amount":null,"day":null,"instant":null});
    assert_eq!(collect(&plan), expected);
}

#[test]
fn native_typed_payload_keys_preserve_empty_schema() {
    let fixture = fixture();
    let empty = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: fixture.scan(),
        offset: 0,
        count: 0,
    }));
    for name in ["payload", "amount", "day", "instant"] {
        let plan = sort(empty.clone(), name);
        assert_eq!(collect(&plan), [] as [Value; 0]);
        assert_eq!(
            prepare_relational(&plan, policy()).unwrap().output_dtype(),
            prepare_relational(&fixture.scan(), policy())
                .unwrap()
                .output_dtype()
        );
    }
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: empty.clone(),
            right: empty.clone(),
            kind,
        }));
        assert_eq!(collect(&plan), [] as [Value; 0]);
    }
}

#[test]
fn native_typed_payload_schema_rejects_other_metadata_and_decimal_domains() {
    let n = Nullability::Nullable;
    for dtype in [
        DType::Decimal(DecimalDType::new(39, 0), n),
        DType::Decimal(DecimalDType::new(10, -1), n),
        DType::Extension(Date::new(TimeUnit::Milliseconds, n).erased()),
        DType::Extension(Timestamp::new(TimeUnit::Nanoseconds, n).erased()),
        DType::Extension(
            Timestamp::new_with_tz(TimeUnit::Microseconds, Some("UTC".into()), n).erased(),
        ),
    ] {
        assert!(native_payload::metadata_bytes(&dtype).is_err(), "{dtype}");
        assert!(
            native_payload::metadata_bytes(&DType::List(std::sync::Arc::new(dtype), n)).is_err()
        );
    }
}

#[test]
fn native_typed_payload_decimal_precision_is_checked_after_selection() {
    let input = DecimalArray::from_option_iter(
        [Some(9i128), Some(10), Some(-9), None],
        DecimalDType::new(1, 1),
    )
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let select = |rows: &[usize]| {
        session.with_native_execution_context(&CancellationToken::default(), |context| {
            let indices =
                native_relational_batch::index_array(rows.len(), false, context, |row| {
                    Ok(Some(rows[row]))
                })?;
            native_relational_batch::take_column(&input, &indices, input.dtype(), context)
        })
    };
    let output = select(&[2, 3, 0]).unwrap();
    let native = VortexSession::default();
    let mut context = native.create_execution_ctx();
    for (row, expected) in [
        json!("decimal128(1,1):-9"),
        Value::Null,
        json!("decimal128(1,1):9"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            result_batch::scalar_value(&output, row, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            expected
        );
    }
    drop(output);
    let baseline = session.memory().snapshot().reserved_bytes;
    assert!(select(&[1]).is_err());
    assert_eq!(session.memory().snapshot().reserved_bytes, baseline);
}

#[test]
fn native_typed_payload_compacts_selected_binary_and_preserves_credit_lifetime() {
    let huge = vec![255u8; 2 << 20];
    let input = VarBinArray::from(vec![huge.as_slice(), &b"\x00\xff"[..], &b""[..]]).into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = native_relational_batch::index_array(4, true, context, |row| {
                Ok([Some(1), None, Some(2), Some(1)][row])
            })?;
            native_relational_batch::take_column(
                &input,
                &indices,
                &input.dtype().as_nullable(),
                context,
            )
        })
        .unwrap();
    assert!(output.nbytes() < 1024);
    let clone = output.clone();
    let slice = output.slice(1..4).unwrap();
    drop(output);
    drop(input);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    let native = VortexSession::default();
    let mut context = native.create_execution_ctx();
    assert_eq!(
        result_batch::scalar_value(&clone, 0, &mut context)
            .unwrap()
            .into_json()
            .unwrap(),
        json!("00ff")
    );
    assert!(slice.execute_scalar(0, &mut context).unwrap().is_null());
    drop(clone);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_typed_payload_nested_gather_preserves_null_parent_and_leaf_types() {
    let fields = StructArray::new(
        FieldNames::from(["payload", "amount", "day", "instant"]),
        payloads(),
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let list = ListViewArray::try_new(
        fields.clone(),
        PrimitiveArray::from_iter([0u64, 2, 2, 3]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 1, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = native_relational_batch::index_array(4, true, context, |row| {
                Ok([Some(3), Some(0), None, Some(1)][row])
            })?;
            native_relational_batch::take_column(&list, &indices, list.dtype(), context)
        })
        .unwrap();
    assert_eq!(output.dtype(), list.dtype());
    let native = VortexSession::default();
    let mut context = native.create_execution_ctx();
    let column = native_json::Column::new(&output, &mut context, session.memory()).unwrap();
    let mut rows = Vec::new();
    for row in 0..4 {
        let mut bytes = Vec::new();
        column
            .write(row, &mut bytes, &mut context, &CancellationToken::default())
            .unwrap();
        rows.push(serde_json::from_slice::<Value>(&bytes).unwrap());
    }
    let leaf = |row: usize| {
        let mut value = expected()[row].clone();
        value.as_object_mut().unwrap().remove("id");
        value
    };
    assert_eq!(
        rows,
        vec![
            json!([leaf(3)]),
            json!([leaf(0), null]),
            Value::Null,
            json!([])
        ]
    );
}

#[test]
fn native_typed_payload_explode_fields_preserve_values_and_empty_schema() {
    use crate::query_primitive::{
        VortexExplodeProjectionRequest, VortexQueryPrimitiveKind as Kind,
    };
    use crate::relational_query::VortexRelationalUnary;

    let names = ["payload", "amount", "day", "instant"];
    let fields = StructArray::new(
        FieldNames::from(names),
        payloads(),
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let records = ListViewArray::try_new(
        fields,
        PrimitiveArray::from_iter([0u64, 2, 2, 3]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 1, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "records"]),
            vec![
                PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
                records,
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    for name in names {
        let explode = |input| {
            let mut request = VortexQueryPrimitiveRequest::for_relational_input(
                Kind::ExplodeRows,
                shardloom_plan::ProjectionRequest::All,
            );
            request.explode_projection = Some(
                VortexExplodeProjectionRequest::new(ColumnRef::new("records").unwrap())
                    .with_element_field(name.into(), "value".into()),
            );
            VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
        };
        let plan = explode(fixture.scan());
        assert_eq!(
            collect(&plan),
            vec![
                json!({"id":1,"value":expected()[0][name]}),
                json!({"id":1,"value":null}),
                json!({"id":3,"value":null}),
                json!({"id":4,"value":expected()[3][name]}),
            ],
            "{name}",
        );
        let empty = explode(VortexRelationalPlan::Limit(Box::new(
            VortexRelationalLimit {
                input: fixture.scan(),
                offset: 0,
                count: 0,
            },
        )));
        assert_eq!(collect(&empty), [] as [Value; 0], "{name}");
        assert_eq!(
            prepare_relational(&empty, policy()).unwrap().output_dtype(),
            prepare_relational(&plan, policy()).unwrap().output_dtype(),
            "{name}",
        );
    }
}

#[test]
fn native_typed_payload_timestamp_transport_uses_full_integer_storage_without_scalar_panics() {
    let input = ExtensionArray::new(
        Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
        PrimitiveArray::from_iter([i64::MIN, 0, i64::MAX]).into_array(),
    )
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices =
                native_relational_batch::index_array(3, false, context, |row| Ok(Some(2 - row)))?;
            native_relational_batch::take_column(&input, &indices, input.dtype(), context)
        })
        .unwrap();
    assert_eq!(output.dtype(), input.dtype());
    let native = VortexSession::default();
    let mut context = native.create_execution_ctx();
    for (row, expected) in [i64::MAX, 0, i64::MIN].into_iter().enumerate() {
        assert_eq!(
            result_batch::scalar_value(&output, row, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            json!(expected)
        );
    }
}

#[test]
fn native_typed_payload_pressure_and_cancellation_release_all_credits() {
    let input = VarBinArray::from(vec![vec![255u8; 2 << 20].as_slice()]).into_array();
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let baseline = session.memory().snapshot().reserved_bytes;
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let indices = native_relational_batch::index_array(1, false, context, |_| Ok(Some(0)))?;
        native_relational_batch::take_column(&input, &indices, input.dtype(), context)
    });
    assert!(result.is_err());
    assert_eq!(session.memory().snapshot().reserved_bytes, baseline);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        session
            .with_native_execution_context(&cancellation, |_| Ok(()))
            .is_err()
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, baseline);
}
