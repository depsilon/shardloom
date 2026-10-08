//! Record and public schema metadata stay with surviving native buffer owners.

use super::*;
use crate::{
    local_primitives::logical_field_from_native_array, resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::arrays::Primitive;

fn columns(context: &NativeExecutionContext<'_>) -> Result<ReservedVec<ArrayRef>> {
    let mut columns = ReservedVec::new(context.memory())?;
    for _ in 0..2 {
        columns.push(unsigned(2, context, |row| Ok(row as u64))?)?;
    }
    Ok(columns)
}

#[test]
fn ordered_aggregate_record_metadata_follows_required_ordinal_without_copying_payload() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let name = "x".repeat(4096);
    let fields = vec![(name, u64_type()), (ORDINAL.into(), u64_type())];
    let metadata =
        native_payload::metadata_bytes(&DType::struct_(fields.clone(), Nullability::NonNullable))
            .unwrap();
    let array = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let columns = columns(context)?;
            let original = columns.values[1]
                .as_opt::<Primitive>()
                .unwrap()
                .to_buffer::<u64>()
                .as_ptr();
            let before = memory.snapshot().reserved_bytes;
            let array = structure(&fields, columns, 2)?;
            let ordinal = logical_field_from_native_array(&array, ORDINAL)?;
            assert_eq!(
                ordinal
                    .as_opt::<Primitive>()
                    .unwrap()
                    .to_buffer::<u64>()
                    .as_ptr(),
                original
            );
            assert_eq!(memory.snapshot().reserved_bytes - before, metadata);
            Ok(array)
        })
        .unwrap();
    drop(session);
    let ordinal = logical_field_from_native_array(&array, ORDINAL).unwrap();
    let slice = ordinal.slice(1..2).unwrap();
    drop((array, ordinal));
    assert!(memory.snapshot().reserved_bytes >= metadata);
    assert_eq!(slice.as_opt::<Primitive>().unwrap().as_slice::<u64>(), &[1]);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn ordered_aggregate_public_metadata_follows_selected_child_and_denies_before_parent_allocation() {
    let fields = vec![("x".repeat(4096), u64_type()), ("y".into(), u64_type())];
    let metadata =
        native_payload::metadata_bytes(&DType::struct_(fields.clone(), Nullability::NonNullable))
            .unwrap();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let array = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            delivered(&fields, columns(context)?, 2, context)
        })
        .unwrap();
    drop(session);
    let child = logical_field_from_native_array(&array, "y").unwrap();
    let clone = child.slice(0..1).unwrap();
    drop((array, child));
    assert!(memory.snapshot().reserved_bytes >= metadata);
    assert_eq!(clone.as_opt::<Primitive>().unwrap().as_slice::<u64>(), &[0]);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let denied = ResidentVortexSession::new(4096, 1).unwrap();
    let error = denied
        .with_native_execution_context(&CancellationToken::default(), |context| {
            delivered(&fields, columns(context)?, 2, context)
        })
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("reservation denied"), "{error}");
    assert_eq!(denied.memory().snapshot().reserved_bytes, 0);
}
