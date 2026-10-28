// SPDX-License-Identifier: Apache-2.0

//! Test comparisons retain native and canonical checks without rendering payloads.

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::RecursiveCanonical;
use vortex_array::aggregate_fn::fns::all_non_distinct::all_non_distinct;

#[track_caller]
pub(crate) fn assert_fixture_equal(left: ArrayRef, right: ArrayRef, ctx: &mut ExecutionCtx) {
    assert!(left.dtype() == right.dtype(), "fixture array type mismatch");
    assert!(left.len() == right.len(), "fixture array length mismatch");
    let canonical = match left.clone().execute::<RecursiveCanonical>(ctx) {
        Ok(array) => array.0.into_array(),
        Err(_) => panic!("fixture canonicalization failed"),
    };
    assert!(
        matches!(all_non_distinct(&left, &right, ctx), Ok(true)),
        "fixture array values differ"
    );
    assert!(
        matches!(all_non_distinct(&canonical, &right, ctx), Ok(true)),
        "canonical fixture array values differ"
    );
}

macro_rules! assert_arrays_eq {
    ($left:expr, $right:expr, $ctx:expr) => {{
        $crate::test_support::assert_fixture_equal(
            vortex_array::IntoArray::into_array($left.clone()),
            vortex_array::IntoArray::into_array($right.clone()),
            $ctx,
        );
    }};
}

pub(crate) use assert_arrays_eq;

#[test]
fn fixture_comparison_rejects_type_length_value_and_null_mismatches() {
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::PrimitiveArray;

    for right in [
        PrimitiveArray::from_iter([1_i32]).into_array(),
        PrimitiveArray::from_iter([1_i64, 2]).into_array(),
        PrimitiveArray::from_iter([2_i64]).into_array(),
        PrimitiveArray::from_option_iter([None::<i64>]).into_array(),
    ] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut ctx = vortex_array::array_session().create_execution_ctx();
            let left = PrimitiveArray::from_iter([1_i64]).into_array();
            assert_fixture_equal(left, right, &mut ctx);
        }));
        assert!(result.is_err(), "fixture mismatch must fail");
    }
    let mut ctx = vortex_array::array_session().create_execution_ctx();
    let nullable = PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array();
    assert_fixture_equal(nullable.clone(), nullable, &mut ctx);
}
