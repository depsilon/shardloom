use super::*;
use crate::{
    local_primitives::{
        logical_field_from_native_array, native_payload, result_batch, vortex_error,
    },
    relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
    resident_session::ResidentVortexSession,
};
use shardloom_core::ColumnRef;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::fixed_size_list::FixedSizeListArraySlotsExt as _,
    arrays::{DecimalArray, FixedSizeListArray, PrimitiveArray, StructArray},
    dtype::{DecimalDType, FieldNames},
    validity::Validity,
};

const ROWS: usize = 128;
const WIDTH: usize = 128;

fn fixture() -> ArrayRef {
    StructArray::new(
        FieldNames::from(["key", "id", "wide"]),
        vec![
            PrimitiveArray::from_iter((0..ROWS as u64).map(|row| (ROWS as u64 - row) % 7))
                .into_array(),
            PrimitiveArray::from_iter(0..ROWS as u64).into_array(),
            FixedSizeListArray::try_new(
                PrimitiveArray::from_iter(0..(ROWS * WIDTH) as u64).into_array(),
                u32::try_from(WIDTH).unwrap(),
                Validity::NonNullable,
                ROWS,
            )
            .unwrap()
            .into_array(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .into_array()
}

fn binding(array: &ArrayRef) -> super::super::Spec {
    let fields = array.dtype().as_struct_fields();
    super::super::Spec {
        fields: fields
            .names()
            .iter()
            .zip(fields.fields())
            .map(|(name, dtype)| (name.to_string(), dtype))
            .collect(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("key").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::First),
        }],
        names: vec!["key".into()],
        copy_policy: native_payload::CopyPolicy::ValidateValues,
        unobserved_fields: Vec::new(),
    }
}

fn pressure(
    spec: &super::super::Spec,
    available_rows: u64,
    context: &NativeExecutionContext<'_>,
) -> Result<MemoryLease> {
    let cost = cost(&spec.fields)?;
    // finish_prefix first creates the full stable ordinal vector. That owner
    // remains charged when output selection takes its snapshot.
    let keep = cost.fixed_bytes + available_rows * cost.bytes_per_unit + (ROWS * 8) as u64;
    let snapshot = context.memory().snapshot();
    context
        .memory()
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)
}

fn verify_rows(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Vec<u64>> {
    let ids = logical_field_from_native_array(array, "id")?;
    let mut execution = context.native_session().create_execution_ctx();
    let list = logical_field_from_native_array(array, "wide")?
        .execute::<FixedSizeListArray>(&mut execution)
        .map_err(vortex_error)?;
    let children = list
        .elements()
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let children = children.to_buffer::<u64>();
    let mut observed = Vec::new();
    for row in 0..array.len() {
        let result_batch::Value::UInt(id) = result_batch::scalar_value(&ids, row, &mut execution)?
        else {
            panic!("id must remain unsigned")
        };
        for child in 0..WIDTH {
            assert_eq!(
                children[row * WIDTH + child],
                id * WIDTH as u64 + child as u64
            );
        }
        observed.push(id);
    }
    Ok(observed)
}

#[test]
fn adaptive_sort_output_preserves_full_nested_values_stable_ties_and_held_owners() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let held = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let input = native_payload::detach(&fixture(), context)?;
            let spec = binding(&input);
            let mut sort = Sort::new(&spec, context.memory())?;
            sort.build(input, context)?;
            let mut pressure = Some(pressure(&spec, 2, context)?);
            let (mut sizes, mut actual, mut held) = (Vec::new(), Vec::new(), None);
            sort.finish_prefix(context, 32, None, true, &mut |array| {
                sizes.push(array.len());
                actual.extend(verify_rows(&array, context)?);
                if held.is_none() {
                    held = Some(array);
                    pressure.take();
                }
                Ok(())
            })?;
            let mut expected = (0..ROWS as u64).collect::<Vec<_>>();
            expected.sort_by_key(|row| ((ROWS as u64 - row) % 7, *row));
            assert_eq!(actual, expected);
            assert_eq!(sizes[0], 2);
            assert!(sizes.iter().any(|rows| *rows > sizes[0]));
            assert!(sizes.iter().all(|rows| *rows <= 32));
            assert!(sizes.windows(2).all(|rows| rows[1] <= rows[0] * 2));
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains(&format!(
                "adaptive_native_sort_output_completed_windows={}",
                sizes.len()
            )));
            assert!(proof.contains("adaptive_native_sort_output_minimum_rows=2"));
            Ok(held.unwrap())
        })
        .unwrap();
    drop(session);
    assert_eq!(held.len(), 2);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn adaptive_sort_output_denies_the_minimum_before_delivery_and_never_retries_callbacks() {
    for mode in 0..3 {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let input = native_payload::detach(&fixture(), context)?;
            let spec = binding(&input);
            let mut sort = Sort::new(&spec, context.memory())?;
            sort.build(input, context)?;
            let pressure = (mode == 0)
                .then(|| pressure(&spec, 0, context))
                .transpose()?;
            let mut calls = 0;
            let error = sort
                .finish_prefix(context, 32, None, true, &mut |_| {
                    calls += 1;
                    if mode == 2 {
                        token.cancel();
                        Ok(())
                    } else {
                        Err(failed("test sort consumer refused"))
                    }
                })
                .unwrap_err();
            assert_eq!(calls, usize::from(mode != 0));
            assert!(error.to_string().contains(match mode {
                0 => "native_sort_output",
                1 => "test sort consumer refused",
                _ => "cancel",
            }));
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(!proof.contains("adaptive_native_sort_output_completed_windows="));
            drop(pressure);
            Ok(())
        });
        if mode == 2 {
            assert!(result.unwrap_err().to_string().contains("cancel"));
        } else {
            result.unwrap();
        }
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn adaptive_sort_keeps_unvalidated_payload_error_windows_under_pressure() {
    let input = StructArray::new(
        FieldNames::from(["key", "decimal", "float", "wide"]),
        vec![
            PrimitiveArray::from_iter(0..ROWS as u64).into_array(),
            DecimalArray::from_option_iter(
                (0..ROWS).map(|row| Some(if row == 31 { 1000_i128 } else { 1 })),
                DecimalDType::new(3, 0),
            )
            .into_array(),
            PrimitiveArray::from_iter(
                (0..ROWS).map(|row| if row == 0 { f64::NAN } else { 1.0_f64 }),
            )
            .into_array(),
            logical_field_from_native_array(&fixture(), "wide").unwrap(),
        ],
        ROWS,
        Validity::NonNullable,
    )
    .into_array();
    let spec = binding(&input);
    let mut errors = Vec::new();
    for pressured in [false, true] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&spec, context.memory())?;
                sort.build(input.clone(), context)?;
                let pressure = pressured.then(|| pressure(&spec, 2, context)).transpose()?;
                let error = sort
                    .finish_prefix(context, 32, Some(1), false, &mut |_| {
                        panic!("invalid window must fail before any output")
                    })
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("precision"));
                errors.push(error);
                let mut proof = String::new();
                context.append_adaptive_work_certificate(&mut proof);
                assert!(!proof.contains("adaptive_native_sort_output_completed_windows="));
                drop(pressure);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    assert_eq!(errors[0], errors[1]);
}
