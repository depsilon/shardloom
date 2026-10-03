use super::*;
use crate::local_primitives::{native_json, native_payload};
use crate::relational_query::{
    VortexRelationalAggregate, VortexRelationalOrderKey, VortexRelationalSort,
    VortexRelationalSpillPolicy, VortexRelationalSubquery, VortexRelationalSubqueryKind,
};
use serde_json::json;

#[test]
fn native_nested_schema_admission_bounds_recursive_metadata_and_rejects_unowned_empty_structs() {
    let mut deep = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..25 {
        deep = DType::List(std::sync::Arc::new(deep), Nullability::Nullable);
    }
    let wide = DType::struct_(
        (0..1025).map(|index| {
            (
                format!("field{index}"),
                DType::Bool(Nullability::NonNullable),
            )
        }),
        Nullability::NonNullable,
    );
    for dtype in [
        deep,
        wide,
        DType::struct_([] as [(String, DType); 0], Nullability::Nullable),
        DType::Decimal(
            vortex::array::dtype::DecimalDType::new(39, 0),
            Nullability::Nullable,
        ),
        DType::Primitive(PType::F16, Nullability::Nullable),
    ] {
        assert!(native_payload::metadata_bytes(&dtype).is_err(), "{dtype}");
    }
    let duplicate = DType::struct_(
        [
            ("same", DType::Bool(Nullability::Nullable)),
            ("same", DType::Utf8(Nullability::Nullable)),
        ],
        Nullability::Nullable,
    );
    assert!(native_payload::metadata_bytes(&duplicate).is_err());
}

#[test]
fn native_nested_empty_schema_rejects_hash_set_and_aggregate_key_semantics() {
    use crate::query_primitive::VortexSimpleAggregateMeasure as Measure;
    let source = fixture();
    let empty =
        VortexRelationalPlan::Limit(Box::new(crate::relational_query::VortexRelationalLimit {
            input: source.scan(),
            offset: 0,
            count: 0,
        }));
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: empty.clone(),
            right: empty.clone(),
            kind,
        }));
        assert!(
            prepare_relational(&plan, policy())
                .err()
                .unwrap()
                .to_string()
                .contains("operated scalar")
        );
    }
    for (groups, measures) in [
        (vec![column("items")], vec![]),
        (
            vec![],
            vec![Measure::new(
                "count_distinct",
                Some(column("items")),
                "n".into(),
            )],
        ),
        (
            vec![],
            vec![Measure::new("count", Some(column("items")), "n".into())],
        ),
    ] {
        let plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: empty.clone(),
            group_by: groups,
            measures,
        }));
        assert!(
            prepare_relational(&plan, policy())
                .err()
                .unwrap()
                .to_string()
                .contains("operated scalar")
        );
    }
}

#[test]
fn native_nested_payload_survives_subquery_membership_and_exists() {
    let source = fixture();
    let relation =
        VortexRelationalPlan::Limit(Box::new(crate::relational_query::VortexRelationalLimit {
            input: source.scan(),
            offset: 0,
            count: 2,
        }));
    for (kind, predicates) in [
        (
            VortexRelationalSubqueryKind::In {
                columns: vec![VortexRelationalJoinKey {
                    left: column("id"),
                    right: column("id"),
                }],
            },
            [true, true, false, false],
        ),
        (VortexRelationalSubqueryKind::Exists, [true; 4]),
    ] {
        let plan = VortexRelationalPlan::Subquery(Box::new(VortexRelationalSubquery {
            input: source.scan(),
            relation: relation.clone(),
            kind,
            correlation: vec![],
            negated: false,
            output_column: "matches".into(),
        }));
        let expected = [json!([9, null]), json!([]), json!(null), json!([-4])]
            .into_iter()
            .enumerate()
            .map(|(row, items)| json!({"id": row + 1, "items": items, "matches": predicates[row]}))
            .collect::<Vec<_>>();
        assert_eq!(collect(&plan), expected);
    }
}

#[test]
fn native_nested_json_cancellation_is_checked_within_a_single_list() {
    struct CancellingWriter {
        bytes: usize,
        token: CancellationToken,
    }
    impl std::io::Write for CancellingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes += bytes.len();
            if self.bytes >= 64 {
                self.token.cancel();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let array = ListViewArray::try_new(
        PrimitiveArray::from_iter(0i64..100_000).into_array(),
        PrimitiveArray::from_iter([0u64]).into_array(),
        PrimitiveArray::from_iter([100_000u64]).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    let cancelled = CancellationToken::default();
    let mut writer = CancellingWriter {
        bytes: 0,
        token: cancelled.clone(),
    };
    let result = session.with_native_execution_context(&cancelled, |context| {
        let mut execution = context.native_session().create_execution_ctx();
        let column = native_json::Column::new(&array, &mut execution, context.memory())?;
        column.write(0, &mut writer, &mut execution, context.cancellation())
    });
    assert!(result.err().unwrap().to_string().contains("cancel"));
    assert!(writer.bytes < 100);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_nested_order_spill_preserves_payloads_and_cleans_up_after_consumer_failure() {
    let count = 24_001u32;
    let labels = (0..count)
        .rev()
        .map(|id| format!("東京-{id}-{}", "λ".repeat(16)))
        .collect::<Vec<_>>();
    let payload = ListViewArray::try_new(
        PrimitiveArray::from_option_iter(
            (0..count).rev().flat_map(|id| [Some(i64::from(id)), None]),
        )
        .into_array(),
        PrimitiveArray::from_iter((0..u64::from(count)).map(|row| row * 2)).into_array(),
        PrimitiveArray::from_iter((0..count).map(|_| 2u64)).into_array(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "items", "detail"]),
            vec![
                PrimitiveArray::from_iter((0..count).rev()).into_array(),
                payload,
                StructArray::new(
                    FieldNames::from(["label"]),
                    vec![
                        VarBinArray::from(labels.iter().map(String::as_str).collect::<Vec<_>>())
                            .into_array(),
                    ],
                    count as usize,
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            count as usize,
            Validity::NonNullable,
        )
        .into_array(),
        2048,
    );
    let plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: column("id"),
            descending: false,
            nulls: None,
        }],
    }));
    let prepared = prepare_relational(&plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let expected = (0..count).map(|id| json!({"id":id,"items":[id,null],"detail":{"label":labels[(count - 1 - id) as usize]}})).collect::<Vec<_>>();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), expected);
    let spill = result.execution.spill.as_ref().unwrap();
    assert!(spill.runs_written >= 2);
    assert!(spill.merge_passes >= 1);
    assert!(spill.owned_cleanup_completed);
    drop(result);
    for fail in [false, true] {
        let token = CancellationToken::default();
        let result = prepared.for_each_batch(&token, |_, _| {
            if fail {
                return Err(failed("intentional nested consumer failure"));
            }
            token.cancel();
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    }
    fixture.replace();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 1);
}
