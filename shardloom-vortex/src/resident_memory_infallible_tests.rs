use super::*;
use vortex::array::arrays::{ChunkedArray, ConstantArray};

#[test]
fn integer_boolean_proof_accepts_only_bounded_canonical_owners() {
    let chunks = ChunkedArray::try_new(
        [
            PrimitiveArray::from_option_iter([Some(i64::MIN), None]).into_array(),
            PrimitiveArray::from_option_iter([Some(i64::MAX)]).into_array(),
        ],
        DType::Primitive(vortex::array::dtype::PType::I64, Nullability::Nullable),
    )
    .unwrap()
    .into_array();
    for array in [
        chunks.clone(),
        BoolArray::from_iter([Some(true), None, Some(false)]).into_array(),
    ] {
        assert!(infallible_fixed_width_values(&array, &mut 32, 0));
    }
    assert!(!infallible_fixed_width_values(&chunks, &mut 2, 0));
    assert!(!infallible_fixed_width_values(&chunks, &mut 32, 9));
    for array in [
        ConstantArray::new(3_i64, 17).into_array(),
        PrimitiveArray::from_iter([f64::NAN]).into_array(),
        VarBinArray::from(vec![Some("text")]).into_array(),
    ] {
        assert!(!infallible_fixed_width_values(&array, &mut 32, 0));
    }
}

#[test]
fn bound_memory_proof_keeps_filters_limits_and_generated_sources_inert() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let source = ResidentMemorySource::from_columns(
        &session,
        &[MemoryColumn {
            name: "n",
            values: MemoryColumnValues::Int64(&[Some(3), None]),
        }],
        MemorySourceBounds::default(),
    )
    .unwrap();
    assert!(
        source
            .prepare_projection(&["n"], None, None)
            .unwrap()
            .has_infallible_fixed_width_values()
    );
    assert!(
        !source
            .prepare_projection(&["n"], Some(vortex::expr::lit(true)), None)
            .unwrap()
            .has_infallible_fixed_width_values()
    );
    assert!(
        !source
            .prepare_projection(&["n"], None, Some(1))
            .unwrap()
            .has_infallible_fixed_width_values()
    );
    let range = ResidentMemorySource::from_int64_range(&session, "n", 0, 1, 2).unwrap();
    assert!(
        !range
            .prepare_projection(&["n"], None, None)
            .unwrap()
            .has_infallible_fixed_width_values()
    );
}
