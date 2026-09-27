use super::*;
use vortex::VortexSessionDefault as _;
use vortex::array::arrays::slice::SliceKernel;
use vortex::array::arrays::{BoolArray, ConstantArray, PrimitiveArray, StructArray};
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::scalar_fn::fns::binary::CompareKernel;
use vortex::array::scalar_fn::fns::operators::{CompareOperator, Operator};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::encodings::fastlanes::{BitPacked, BitPackedArrayExt, BitPackedData, FoR};

const OPS: [CompareOperator; 6] = [
    CompareOperator::Eq,
    CompareOperator::NotEq,
    CompareOperator::Lt,
    CompareOperator::Lte,
    CompareOperator::Gt,
    CompareOperator::Gte,
];

fn compare<T: Ord + Copy>(left: T, right: T, op: CompareOperator) -> bool {
    match op {
        CompareOperator::Eq => left == right,
        CompareOperator::NotEq => left != right,
        CompareOperator::Lt => left < right,
        CompareOperator::Lte => left <= right,
        CompareOperator::Gt => left > right,
        CompareOperator::Gte => left >= right,
    }
}

fn assert_mask(
    actual: &BoolArray,
    expected: &[Option<bool>],
    ctx: &mut vortex::array::ExecutionCtx,
) {
    assert_eq!(actual.len(), expected.len());
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(
            actual.execute_scalar(index, ctx).unwrap().as_bool().value(),
            *expected,
            "mask row {index}"
        );
    }
}

#[test]
fn unsigned_packed_provider_returns_exact_masks_without_a_primitive_input() {
    let values: Vec<u32> = (0..2061).map(|i| (i * 7) % 32).collect();
    let mut ctx = vortex::session::VortexSession::default().create_execution_ctx();
    let packed = BitPackedData::encode(
        &PrimitiveArray::from_iter(values.iter().copied()).into_array(),
        5,
        &mut ctx,
    )
    .unwrap();
    assert!(packed.patches().is_none());
    for op in OPS {
        let rhs = ConstantArray::new(17_u32, values.len()).into_array();
        // Some proves this exact provider accepted the input; an ordinary
        // expression result alone would not identify the executing kernel.
        let actual = <BitPacked as CompareKernel>::compare(packed.as_view(), &rhs, op, &mut ctx)
            .unwrap()
            .expect("native packed comparison must be admitted")
            .execute::<BoolArray>(&mut ctx)
            .unwrap();
        let expected: Vec<_> = values.iter().map(|v| Some(compare(*v, 17, op))).collect();
        assert_mask(&actual, &expected, &mut ctx);
    }
    let wrong_type = ConstantArray::new(17_u64, values.len()).into_array();
    assert!(
        <BitPacked as CompareKernel>::compare(
            packed.as_view(),
            &wrong_type,
            CompareOperator::Eq,
            &mut ctx
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn signed_packed_provider_preserves_nulls_patches_empty_inputs_and_tail_rows() {
    let mut ctx = vortex::session::VortexSession::default().create_execution_ctx();
    let mixed: Vec<Option<i64>> = (0..2053)
        .map(|i| match i % 67 {
            0 => None,
            1 => Some(0),
            2 => Some(i64::MAX),
            _ => Some(i % 16),
        })
        .collect();
    let negative = PrimitiveArray::from_iter([i64::MIN]).into_array();
    let error = BitPackedData::encode(&negative, 4, &mut ctx).unwrap_err();
    assert!(error.to_string().contains("negative integers"), "{error}");
    for values in [Vec::new(), vec![None; 1031], mixed] {
        let packed = BitPackedData::encode(
            &PrimitiveArray::from_option_iter(values.iter().copied()).into_array(),
            4,
            &mut ctx,
        )
        .unwrap();
        for op in OPS {
            let rhs = ConstantArray::new(7_i64, values.len()).into_array();
            let actual =
                <BitPacked as CompareKernel>::compare(packed.as_view(), &rhs, op, &mut ctx)
                    .unwrap()
                    .expect("nullable packed comparison must be admitted")
                    .execute::<BoolArray>(&mut ctx)
                    .unwrap();
            let expected: Vec<_> = values
                .iter()
                .map(|v| v.map(|v| compare(v, 7, op)))
                .collect();
            assert_mask(&actual, &expected, &mut ctx);
        }
    }
}

#[test]
fn packed_provider_preserves_patch_positions_after_slicing() {
    let mut ctx = vortex::session::VortexSession::default().create_execution_ctx();
    let values: Vec<i32> = (0..3107)
        .map(|i| if i % 43 == 0 { i32::MAX - i } else { i % 16 })
        .collect();
    let packed = BitPackedData::encode(
        &PrimitiveArray::from_iter(values.iter().copied()).into_array(),
        4,
        &mut ctx,
    )
    .unwrap();
    assert!(packed.patches().is_some());
    let range = 29..2079;
    let slice = <BitPacked as SliceKernel>::slice(packed.as_view(), range.clone(), &mut ctx)
        .unwrap()
        .expect("native packed slice");
    for op in OPS {
        let rhs = ConstantArray::new(6_i32, range.len()).into_array();
        let actual =
            <BitPacked as CompareKernel>::compare(slice.as_::<BitPacked>(), &rhs, op, &mut ctx)
                .unwrap()
                .expect("sliced packed comparison must be admitted")
                .execute::<BoolArray>(&mut ctx)
                .unwrap();
        let expected: Vec<_> = values[range.clone()]
            .iter()
            .map(|v| Some(compare(*v, 6, op)))
            .collect();
        assert_mask(&actual, &expected, &mut ctx);
    }
}

#[test]
fn frame_of_reference_provider_declines_ordered_comparisons_at_wrapping_boundary() {
    let mut ctx = vortex::session::VortexSession::default().create_execution_ctx();
    let reference = i32::MAX - 3;
    let offsets = [0_i32, 3, 4, 7];
    let values = offsets.map(|v| reference.wrapping_add(v));
    let array = FoR::try_new(
        PrimitiveArray::from_iter(offsets).into_array(),
        vortex::scalar::Scalar::from(reference),
    )
    .unwrap();
    for op in OPS {
        let rhs = ConstantArray::new(i32::MAX, values.len()).into_array();
        let provided =
            <FoR as CompareKernel>::compare(array.as_view(), &rhs, op, &mut ctx).unwrap();
        assert_eq!(
            provided.is_some(),
            matches!(op, CompareOperator::Eq | CompareOperator::NotEq)
        );
        // Declining this kernel does not mean the native expression is unsupported.
        // Its general provider may materialize the values, within Vortex itself.
        let actual = array
            .clone()
            .into_array()
            .binary(rhs, Operator::from(op))
            .unwrap()
            .execute::<BoolArray>(&mut ctx)
            .unwrap();
        let expected: Vec<_> = values
            .iter()
            .map(|v| Some(compare(*v, i32::MAX, op)))
            .collect();
        assert_mask(&actual, &expected, &mut ctx);
    }
}

#[test]
fn shared_predicate_lowering_matches_packed_column_dtype() {
    let mut ctx = vortex::session::VortexSession::default().create_execution_ctx();
    let values = [Some(0_u16), None, Some(11), Some(15), Some(2)];
    let packed = BitPackedData::encode(
        &PrimitiveArray::from_option_iter(values).into_array(),
        4,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let chunk = StructArray::try_new(
        ["renamed_metric"].into(),
        vec![packed],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    for (op, native_op) in [
        (ComparisonOp::Eq, CompareOperator::Eq),
        (ComparisonOp::NotEq, CompareOperator::NotEq),
        (ComparisonOp::Lt, CompareOperator::Lt),
        (ComparisonOp::LtEq, CompareOperator::Lte),
        (ComparisonOp::Gt, CompareOperator::Gt),
        (ComparisonOp::GtEq, CompareOperator::Gte),
    ] {
        let predicate = PredicateExpr::Compare {
            column: ColumnRef::new("renamed_metric").unwrap(),
            op,
            value: StatValue::UInt64(11),
        };
        let expression = predicate_to_vortex_expr(
            &predicate,
            chunk.dtype(),
            VortexQueryPrimitiveKind::SimpleAggregate,
        )
        .unwrap();
        let bound = expression.bind(chunk.dtype()).unwrap();
        let actual = chunk
            .clone()
            .apply_bound(&bound)
            .unwrap()
            .execute::<BoolArray>(&mut ctx)
            .unwrap();
        let expected: Vec<_> = values
            .iter()
            .map(|v| v.map(|v| compare(v, 11, native_op)))
            .collect();
        assert_mask(&actual, &expected, &mut ctx);
    }
}
