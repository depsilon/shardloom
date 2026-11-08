use super::*;
use crate::{
    native_variable_tensor::{Metadata, VariableShapeTensor},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::dtype::{FieldNames, extension::ExtDType};

fn tensor() -> ArrayRef {
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    let data = ListArray::try_new(
        PrimitiveArray::from_iter([10_i32, 20, 30, 40]).into_array(),
        PrimitiveArray::from_iter([0_u64, 1, 3, 4]).into_array(),
        Validity::NonNullable,
        &mut execution,
    )
    .unwrap()
    .into_array();
    let shape = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([1_i32, 1, 1, 2, -1, 1]).into_array(),
        2,
        Validity::NonNullable,
        3,
    )
    .unwrap()
    .into_array();
    let storage = StructArray::try_new(
        FieldNames::from(["data", "shape"].as_slice()),
        [data, shape],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dtype = ExtDType::<VariableShapeTensor>::try_new(
        Metadata::parse("{}").unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    ExtensionArray::try_new(dtype, storage)
        .unwrap()
        .into_array()
}

#[test]
fn variable_tensor_native_selection_validates_only_observed_rows_and_keeps_duplicates() {
    let array = tensor();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let good = PrimitiveArray::from_iter([1_u64, 0, 1]).into_array();
            validate_selection(&array, &good, array.dtype(), context)?;
            let copied = take(&array, &good, array.dtype(), context)?;
            let mut execution = context.native_session().create_execution_ctx();
            for (row, original) in [1, 0, 1].into_iter().enumerate() {
                assert_eq!(
                    copied.execute_scalar(row, &mut execution).unwrap(),
                    array.execute_scalar(original, &mut execution).unwrap()
                );
            }
            drop(copied);
            let bad = PrimitiveArray::from_iter([2_u64]).into_array();
            assert!(
                validate_selection(&array, &bad, array.dtype(), context)
                    .unwrap_err()
                    .to_string()
                    .contains("nonnegative")
            );
            assert!(take(&array, &bad, array.dtype(), context).is_err());
            let unobserved = take_with_policy(
                &array,
                &bad,
                array.dtype(),
                CopyPolicy::PreserveUnobserved,
                context,
            )?;
            assert_eq!(unobserved.len(), 1);
            drop(unobserved);
            let empty = PrimitiveArray::from_iter([] as [u64; 0]).into_array();
            validate_selection(&array, &empty, array.dtype(), context)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variable_tensor_native_parent_masks_and_complete_validation_agree() {
    let array = tensor();
    let hidden = validation_tests::record(
        vec![("tensor", array.clone())],
        Validity::from_iter([true, true, false]),
    );
    validation_tests::check_copy_agreement(&hidden, None);
    let visible = validation_tests::record(vec![("tensor", array)], Validity::NonNullable);
    validation_tests::check_copy_agreement(&visible, Some("nonnegative"));
}

#[test]
fn variable_tensor_typed_json_checks_shape_before_source_publication() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let dtype = serde_json::to_string(tensor().dtype()).unwrap();
            let allocator = context.native_session().host_allocator();
            for raw in [
                r#"{"data":[1],"shape":[1,2]}"#,
                r#"{"data":[],"shape":[0,-1]}"#,
                r#"{"data":[],"shape":[0,null]}"#,
                r#"{"data":null,"shape":[0,0]}"#,
                r#"{"data":[],"shape":[0]}"#,
            ] {
                assert!(
                    super::super::native_typed_input::build(
                        &dtype,
                        &[Some(raw)],
                        context.memory(),
                        &allocator,
                        1 << 20
                    )
                    .is_err(),
                    "{raw}"
                );
            }
            let good = super::super::native_typed_input::build(
                &dtype,
                &[Some(r#"{"data":[1,2],"shape":[1,2]}"#)],
                context.memory(),
                &allocator,
                1 << 20,
            )?;
            assert_eq!(good.array.len(), 1);
            drop(good);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
