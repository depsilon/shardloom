use super::*;
use crate::local_primitives::native_relational_batch;
use vortex::array::arrays::{FixedSizeListArray, ListViewArray, VarBinArray};

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_nested_io_tests.rs"]
mod io_tests;

#[path = "local_primitive_relational_nested_resource_tests.rs"]
mod resource_tests;

fn lists() -> ArrayRef {
    ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array()
}

fn fixture() -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "items"]),
            vec![
                PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
                lists(),
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn collect(plan: &VortexRelationalPlan) -> Vec<serde_json::Value> {
    json_rows(
        &prepare_relational(plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    )
}

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

#[test]
fn native_nested_payload_collect_preserves_lists_empty_null_and_element_null() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["id", "items"]),
            vec![
                PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
                lists(),
            ],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    );
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let output = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        serde_json::json!(json_rows(&output)),
        serde_json::json!([
            {"id":1,"items":[9,null]}, {"id":2,"items":[]},
            {"id":3,"items":null}, {"id":4,"items":[-4]}
        ])
    );
    assert!(output.execution.native_io_certificate.is_certified());
}

#[test]
fn native_nested_gather_copies_only_selected_lists_and_retains_buffer_credits() {
    let huge = "unselected".repeat(131_072);
    let input = ListViewArray::try_new(
        VarBinArray::from(vec![huge.as_str(), "東京", "a'b"]).into_array(),
        PrimitiveArray::from_iter([0u64, 1]).into_array(),
        PrimitiveArray::from_iter([1u64, 2]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = native_relational_batch::index_array(2, true, context, |row| {
                Ok((row == 0).then_some(1))
            })?;
            native_relational_batch::take_column(
                &input,
                &indices,
                &input.dtype().as_nullable(),
                context,
            )
        })
        .unwrap();
    assert!(output.nbytes() < 1024, "unselected text retained");
    let lease_bytes = memory.snapshot().reserved_bytes;
    assert!(lease_bytes > baseline);
    let clone = output.clone();
    drop(input);
    drop(output);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    let session = VortexSession::default();
    let mut context = session.create_execution_ctx();
    assert_eq!(
        clone
            .execute_scalar(0, &mut context)
            .unwrap()
            .as_list()
            .elements()
            .unwrap(),
        vec!["東京".into(), "a'b".into()]
    );
    assert!(clone.execute_scalar(1, &mut context).unwrap().is_null());
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_struct_gather_masks_hidden_fixed_children_and_retains_child_credits() {
    use vortex::array::arrays::struct_::StructArrayExt as _;
    let huge = "hidden".repeat(131_072);
    let values = FixedSizeListArray::try_new(
        VarBinArray::from(vec![huge.as_str(), huge.as_str(), "東京", "a'b"]).into_array(),
        2,
        Validity::NonNullable,
        2,
    )
    .unwrap()
    .into_array();
    let source = StructArray::new(
        FieldNames::from(["names", "counter"]),
        vec![
            values,
            PrimitiveArray::from_iter([0u64, u64::MAX]).into_array(),
        ],
        2,
        Validity::from_iter([false, true]),
    )
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = native_relational_batch::index_array(4, true, context, |row| {
                Ok([Some(1), None, Some(0), Some(1)][row])
            })?;
            native_relational_batch::take_column(&source, &indices, source.dtype(), context)
        })
        .unwrap();
    assert_eq!(output.dtype(), source.dtype());
    assert!(output.nbytes() < 1024, "hidden text retained");
    let native = VortexSession::default();
    let mut execution = native.create_execution_ctx();
    assert_eq!(
        output.execute_scalar(0, &mut execution).unwrap(),
        source.execute_scalar(1, &mut execution).unwrap()
    );
    assert!(output.execute_scalar(1, &mut execution).unwrap().is_null());
    assert!(output.execute_scalar(2, &mut execution).unwrap().is_null());
    let array = output
        .clone()
        .execute::<StructArray>(&mut execution)
        .unwrap();
    let child = array.unmasked_field(0).clone();
    let bytes = memory.snapshot().reserved_bytes;
    drop(array);
    drop(source);
    drop(output);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(memory.snapshot().reserved_bytes <= bytes);
    drop(child);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_empty_arrays_keep_dtype_and_metadata_until_last_buffer_owner() {
    let source = StructArray::new(
        FieldNames::from(["items"]),
        vec![lists()],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices =
                native_relational_batch::index_array(0, false, context, |_| unreachable!())?;
            native_relational_batch::take_column(&source, &indices, source.dtype(), context)
        })
        .unwrap();
    assert_eq!(output.dtype(), source.dtype());
    assert!(output.is_empty());
    assert!(memory.snapshot().reserved_bytes > 0);
    let clone = output.clone();
    drop(output);
    drop(source);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_nested_expansion_is_denied_before_large_child_allocation_without_leaks() {
    use vortex::array::arrays::ConstantArray;
    for source in [
        ListViewArray::try_new(
            ConstantArray::new(7i64, 1_000_000).into_array(),
            PrimitiveArray::from_iter([0u64]).into_array(),
            PrimitiveArray::from_iter([1_000_000u64]).into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        FixedSizeListArray::try_new(
            ConstantArray::new(7i64, 1_000_000).into_array(),
            1_000_000,
            Validity::NonNullable,
            1,
        )
        .unwrap()
        .into_array(),
    ] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let before = memory.snapshot().reserved_bytes;
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let indices =
                    native_relational_batch::index_array(1, false, context, |_| Ok(Some(0)))?;
                native_relational_batch::take_column(&source, &indices, source.dtype(), context)
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("reservation denied"), "{error}");
        assert_eq!(memory.snapshot().reserved_bytes, before);
        assert!(memory.snapshot().peak_reserved_bytes < 1 << 20);
    }
}

#[test]
fn native_nested_projection_order_window_limit_and_union_all_compose() {
    use crate::relational_query::{
        VortexRelationalLimit, VortexRelationalOrderKey, VortexRelationalProject,
        VortexRelationalSort, VortexRelationalWindow, VortexRelationalWindowExpression,
        VortexRelationalWindowFunction,
    };
    use shardloom_core::{ExprId, Expression, ExpressionKind};
    let fixture = fixture();
    let project = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(),
        expressions: [("id", "key"), ("items", "payload")]
            .into_iter()
            .map(|(source, output)| {
                (
                    output.into(),
                    Expression::new(
                        ExprId::new(output).unwrap(),
                        ExpressionKind::Column(column(source)),
                    ),
                )
            })
            .collect(),
    }));
    let order = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: project,
        keys: vec![VortexRelationalOrderKey {
            column: column("key"),
            descending: true,
            nulls: None,
        }],
    }));
    let window = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: order,
        columns: vec![column("key"), column("payload")],
        expressions: vec![VortexRelationalWindowExpression {
            output_column: "prior".into(),
            function: VortexRelationalWindowFunction::Lag {
                column: column("payload"),
                offset: 1,
            },
            partition_by: vec![],
            order_by: vec![VortexRelationalOrderKey {
                column: column("key"),
                descending: false,
                nulls: None,
            }],
        }],
    }));
    let range = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: window,
        offset: 1,
        count: 2,
    }));
    let expected = vec![
        serde_json::json!({"key":3,"payload":null,"prior":[]}),
        serde_json::json!({"key":2,"payload":[],"prior":[9,null]}),
    ];
    assert_eq!(collect(&range), expected);
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: range.clone(),
        right: range,
        kind: SetKind::UnionAll,
    }));
    assert_eq!(collect(&union), [expected.clone(), expected].concat());
    let empty = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: union,
        offset: 0,
        count: 0,
    }));
    let prepared = prepare_relational(&empty, policy()).unwrap();
    let dtype = prepared.output_dtype();
    let mut batches = 0;
    prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            assert!(array.is_empty());
            assert_eq!(array.dtype(), &dtype);
            batches += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(batches, 1);
}

#[test]
fn native_nested_full_join_null_extends_both_payload_sides() {
    let left = fixture();
    let right = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "items"]),
            vec![
                PrimitiveArray::from_iter([3u32, 5]).into_array(),
                ListViewArray::try_new(
                    PrimitiveArray::from_option_iter([Some(30i64), Some(50)]).into_array(),
                    PrimitiveArray::from_iter([0u64, 1]).into_array(),
                    PrimitiveArray::from_iter([1u64, 1]).into_array(),
                    Validity::AllValid,
                )
                .unwrap()
                .into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: left.scan(),
        right: right.scan(),
        kind: JoinKind::Full,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: column("id"),
            right: column("id"),
        }],
        columns: [
            (Side::Left, "id", "left_id"),
            (Side::Right, "id", "right_id"),
            (Side::Left, "items", "left_items"),
            (Side::Right, "items", "right_items"),
        ]
        .into_iter()
        .map(|(side, name, output)| VortexRelationalJoinColumn {
            side,
            column: column(name),
            output_column: output.into(),
        })
        .collect(),
    }));
    assert_eq!(
        collect(&plan),
        vec![
            serde_json::json!({"left_id":1,"right_id":null,"left_items":[9,null],"right_items":null}),
            serde_json::json!({"left_id":2,"right_id":null,"left_items":[],"right_items":null}),
            serde_json::json!({"left_id":3,"right_id":3,"left_items":null,"right_items":[30]}),
            serde_json::json!({"left_id":4,"right_id":null,"left_items":[-4],"right_items":null}),
            serde_json::json!({"left_id":null,"right_id":5,"left_items":null,"right_items":[50]}),
        ]
    );
}

#[test]
fn native_nested_repeated_explode_uses_preceding_order_and_preserves_struct_companions() {
    use crate::query_primitive::{
        VortexExplodeProjectionRequest, VortexQueryPrimitiveKind as Kind,
    };
    use crate::relational_query::{
        VortexRelationalOrderKey, VortexRelationalSort, VortexRelationalUnary,
    };
    let items = StructArray::new(
        FieldNames::from(["code"]),
        vec![lists()],
        4,
        Validity::from_iter([true, true, false, true]),
    )
    .into_array();
    let outer = ListViewArray::try_new(
        items,
        PrimitiveArray::from_iter([0u64, 3]).into_array(),
        PrimitiveArray::from_iter([3u64, 1]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let companion = StructArray::new(
        FieldNames::from(["tag"]),
        vec![VarBinArray::from(vec!["東京", "hidden"]).into_array()],
        2,
        Validity::from_iter([true, false]),
    )
    .into_array();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "items", "context"]),
            vec![
                PrimitiveArray::from_iter([1u32, 4]).into_array(),
                outer,
                companion,
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let ordered = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: column("id"),
            descending: true,
            nulls: None,
        }],
    }));
    let explode = |input, projection| {
        let mut request = VortexQueryPrimitiveRequest::for_relational_input(
            Kind::ExplodeRows,
            shardloom_plan::ProjectionRequest::All,
        );
        request.explode_projection = Some(projection);
        VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
    };
    let whole = explode(
        ordered.clone(),
        VortexExplodeProjectionRequest::new(column("items")),
    );
    assert_eq!(
        collect(&whole),
        vec![
            serde_json::json!({"id":4,"items":{"code":[-4]},"context":null}),
            serde_json::json!({"id":1,"items":{"code":[9,null]},"context":{"tag":"東京"}}),
            serde_json::json!({"id":1,"items":{"code":[]},"context":{"tag":"東京"}}),
            serde_json::json!({"id":1,"items":null,"context":{"tag":"東京"}}),
        ]
    );
    let fields = explode(
        ordered,
        VortexExplodeProjectionRequest::new(column("items"))
            .with_element_field("code".into(), "codes".into()),
    );
    let repeated = explode(fields, VortexExplodeProjectionRequest::new(column("codes")));
    assert_eq!(
        collect(&repeated),
        vec![
            serde_json::json!({"id":4,"codes":-4,"context":null}),
            serde_json::json!({"id":1,"codes":9,"context":{"tag":"東京"}}),
            serde_json::json!({"id":1,"codes":null,"context":{"tag":"東京"}}),
            serde_json::json!({"id":1,"codes":null,"context":{"tag":"東京"}}),
        ]
    );
}

#[test]
fn native_nested_keys_and_scalar_operands_are_rejected_even_for_empty_sources() {
    use crate::relational_query::{
        VortexRelationalFilter, VortexRelationalOrderKey, VortexRelationalSort,
    };
    use shardloom_core::{ExprId, Expression, ExpressionKind, UnaryOp};
    let empty = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "items"]),
            vec![
                PrimitiveArray::from_iter(std::iter::empty::<u32>()).into_array(),
                lists().slice(0..0).unwrap(),
            ],
            0,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: empty.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: column("items"),
            descending: false,
            nulls: None,
        }],
    }));
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: empty.scan(),
        predicate: Expression::new(
            ExprId::new("null").unwrap(),
            ExpressionKind::Unary {
                op: UnaryOp::IsNull,
                expr: Box::new(Expression::column(
                    ExprId::new("payload").unwrap(),
                    column("items"),
                )),
            },
        ),
    }));
    let joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: empty.scan(),
        right: empty.scan(),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: column("items"),
            right: column("items"),
        }],
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: column("id"),
            output_column: "id".into(),
        }],
    }));
    for plan in [sorted, filtered, joined] {
        let error = prepare_relational(&plan, policy()).err().unwrap();
        assert!(error.to_string().contains("operated scalar"), "{error}");
    }
}
