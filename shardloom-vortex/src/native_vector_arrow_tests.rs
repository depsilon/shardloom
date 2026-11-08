use super::*;
use arrow_array::{
    Array, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray, RecordBatch,
    StructArray, UInt64Array, types::UInt64Type,
};
use arrow_schema::Schema;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

fn sparse(index: Option<u64>, parent: Option<Vec<bool>>) -> ArrowArray {
    let indices: ArrowArray = Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>([
        Some(vec![index]),
        Some(vec![Some(u64::MAX - 1), Some(0), Some(u64::MAX - 1)]),
    ]));
    let values: ArrowArray = Arc::new(ListArray::from_iter_primitive::<
        arrow_array::types::Float64Type,
        _,
        _,
    >([
        Some(vec![Some(1.0)]),
        Some(vec![Some(-0.0), None, Some(1.0)]),
    ]));
    Arc::new(StructArray::new(
        vec![
            Field::new("size", DataType::UInt64, false),
            Field::new("indices", indices.data_type().clone(), true),
            Field::new("values", values.data_type().clone(), true),
        ]
        .into(),
        vec![
            Arc::new(UInt64Array::from(vec![1, u64::MAX])),
            indices,
            values,
        ],
        parent.map(Into::into),
    ))
}

fn target(array: &ArrowArray, kind: Kind, raw: &str) -> Field {
    Field::new("v", array.data_type().clone(), true).with_metadata(HashMap::from([
        (NAME.into(), kind.id().into()),
        (META.into(), raw.into()),
    ]))
}
fn batch(field: Field, array: ArrowArray) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}
fn intake(batch: RecordBatch) -> shardloom_core::Result<()> {
    let session = crate::resident_session::ResidentVortexSession::new(4 << 20, 1).unwrap();
    let result = crate::resident_memory_source::ResidentMemorySource::from_arrow_record_batch(
        &session,
        batch,
        &CancellationToken::default(),
    )
    .map(drop);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    result
}
const SPARSE: &str = r#"{"version":1,"index_policy":"preserve"}"#;

#[test]
fn native_vector_arrow_slices_and_every_ancestor_mask_hide_only_absent_rows() {
    for bad in [None, Some(u64::MAX)] {
        let array = sparse(bad, None);
        let field = target(&array, Kind::Sparse, SPARSE);
        assert!(intake(batch(field.clone(), array.clone())).is_err());
        intake(batch(field.clone(), array.slice(1, 1))).unwrap();
        intake(batch(field.clone(), array.slice(0, 0))).unwrap();
        for visible in [false, true] {
            let nulls = Some(vec![visible, true].into());
            let structure: ArrowArray = Arc::new(StructArray::new(
                vec![field.clone()].into(),
                vec![array.clone()],
                nulls.clone(),
            ));
            let list: ArrowArray = Arc::new(ListArray::new(
                Arc::new(field.clone()),
                arrow_buffer::OffsetBuffer::new(vec![0_i32, 1, 2].into()),
                array.clone(),
                nulls.clone(),
            ));
            let large: ArrowArray = Arc::new(LargeListArray::new(
                Arc::new(field.clone()),
                arrow_buffer::OffsetBuffer::new(vec![0_i64, 1, 2].into()),
                array.clone(),
                nulls.clone(),
            ));
            let fixed: ArrowArray = Arc::new(FixedSizeListArray::new(
                Arc::new(field.clone()),
                1,
                array.clone(),
                nulls,
            ));
            for parent in [structure, list, large, fixed] {
                let field = Field::new("parent", parent.data_type().clone(), true);
                assert_eq!(
                    intake(batch(field.clone(), parent.clone())).is_ok(),
                    !visible
                );
                intake(batch(field, parent.slice(1, 1))).unwrap();
            }
        }
        let hidden = sparse(bad, Some(vec![false, true]));
        intake(batch(target(&hidden, Kind::Sparse, SPARSE), hidden)).unwrap();
    }
}

#[test]
fn native_vector_arrow_required_scales_remain_finite_positive_in_extended_policy() {
    for invalid in [
        None,
        Some(0.0),
        Some(-0.0),
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ] {
        for visible in [false, true] {
            let codes: ArrowArray = Arc::new(ListArray::from_iter_primitive::<
                arrow_array::types::Int64Type,
                _,
                _,
            >([
                Some(vec![Some(i64::MIN), None, Some(i64::MAX)]),
                Some(vec![]),
            ]));
            let array: ArrowArray = Arc::new(StructArray::new(
                vec![
                    Field::new("codes", codes.data_type().clone(), true),
                    Field::new("scale", DataType::Float64, true),
                    Field::new("zero_point", DataType::Int64, true),
                ]
                .into(),
                vec![
                    codes,
                    Arc::new(Float64Array::from(vec![invalid, Some(0.5)])),
                    Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)])),
                ],
                Some(vec![visible, true].into()),
            ));
            let field = target(
                &array,
                Kind::Quantized,
                r#"{"version":1,"parameter_mode":"per_vector"}"#,
            );
            assert_eq!(
                intake(batch(field.clone(), array.clone())).is_ok(),
                !visible
            );
            intake(batch(field, array.slice(1, 1))).unwrap();
        }
    }
}

#[test]
fn native_vector_arrow_metadata_budget_and_cancellation_release_before_publication() {
    let array = sparse(Some(0), None);
    let input = batch(target(&array, Kind::Sparse, SPARSE), array);
    let denied = LiveMemoryPool::new(256).unwrap();
    assert!(
        crate::native_cross_field_arrow::validate_batch(&input, Some(&denied), &|| Ok(())).is_err()
    );
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(64 << 10).unwrap();
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        crate::native_cross_field_arrow::validate_batch(&input, Some(&memory), &|| token.check())
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    crate::native_cross_field_arrow::validate_batch(&input, Some(&memory), &|| Ok(())).unwrap();
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
