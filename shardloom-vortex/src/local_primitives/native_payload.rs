//! Compact ownership of admitted Vortex payloads without a decoded row tree.

use super::{
    NativeNumericOwner,
    native_capacity::ReservedVec,
    native_list,
    native_relational_batch::failed,
    result_batch::{self, Value},
    vortex_error,
};
pub(super) use crate::native_payload_schema::metadata_bytes;
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::cell::Cell;
use vortex::array::{
    ArrayRef, Columnar, IntoArray, VortexSessionExecute as _,
    arrays::{ChunkedArray, PrimitiveArray},
    builtins::ArrayBuiltins as _,
    dtype::{DType, Nullability, PType},
    memory::{HostAllocatorRef, HostMemorySessionExt as _},
    validity::Validity,
};

#[cfg(all(test, feature = "vortex-write"))]
use vortex::array::{
    arrays::struct_::StructArrayExt as _,
    arrays::{ExtensionArray, FixedSizeListArray, ListArray, StructArray},
};

#[path = "native_payload_copy.rs"]
mod copying;
use copying::copy_rows;

#[path = "native_payload_storage.rs"]
mod storage;
pub(super) use storage::decimal_key;

#[path = "native_payload_tensor.rs"]
mod tensor;
#[path = "native_payload_variant.rs"]
mod variant;
#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_variant_tests.rs"]
mod variant_tests;
#[path = "native_payload_vector.rs"]
mod vector;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_tensor_tests.rs"]
mod tensor_tests;

#[path = "native_payload_sizing.rs"]
mod sizing;
pub(super) use sizing::gather_scratch_per_row;

#[path = "native_payload_validation.rs"]
mod validation;
pub(super) use validation::{validate_record, validate_typed_column};

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_storage_tests.rs"]
mod storage_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_numeric_tests.rs"]
mod numeric_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_variable_tests.rs"]
mod variable_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_validation_tests.rs"]
mod validation_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_validation_typed_tests.rs"]
mod typed_validation_tests;

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_payload_validation_text_tests.rs"]
mod text_validation_tests;

/// Raw selection and private window movement do not numerically observe carried
/// components. Intake and arithmetic retain their own value-validation rules.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyPolicy {
    ValidateValues,
    PreserveUnobserved,
}

#[derive(Clone, Copy)]
struct CopyState<'a> {
    policy: CopyPolicy,
    scalars: Option<&'a Cell<u64>>,
    // Only the outer record consumes this positional policy. A selected field
    // then carries one ordinary policy through its own nested value tree.
    unobserved_fields: &'a [usize],
}

impl CopyState<'_> {
    fn unmeasured(policy: CopyPolicy) -> Self {
        Self {
            policy,
            scalars: None,
            unobserved_fields: &[],
        }
    }

    fn field(self, index: usize) -> Self {
        Self {
            policy: if self.unobserved_fields.contains(&index) {
                CopyPolicy::PreserveUnobserved
            } else {
                self.policy
            },
            unobserved_fields: &[],
            ..self
        }
    }

    fn record_scalars(self, count: usize) -> Result<()> {
        if let Some(scalars) = self.scalars {
            scalars.set(
                scalars
                    .get()
                    .checked_add(count as u64)
                    .ok_or_else(|| failed("native payload scalar counter overflow"))?,
            );
        }
        Ok(())
    }
}

pub(super) fn is_nested(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::List(..) | DType::FixedSizeList(..) | DType::Struct(..) | DType::Variant(..)
    ) || crate::native_extensions::payload_storage(dtype).is_some()
}

/// Copy one bounded native payload, retaining schema credits on every child.
pub(crate) fn detach(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    detach_with_policy(array, CopyPolicy::ValidateValues, context)
}

pub(super) fn detach_with_policy(
    array: &ArrayRef,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let indices =
        super::native_relational_batch::index_array(array.len(), false, context, |row| {
            Ok(Some(row))
        })?;
    take_record_with_policy(array, &indices, array.dtype(), policy, context)
}

/// Carry explicitly unobserved output fields without exempting their siblings
/// from the established validation, masking and allocator ownership contract.
pub(super) fn detach_with_unobserved_fields(
    array: &ArrayRef,
    unobserved_fields: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    if unobserved_fields.is_empty() {
        return detach(array, context);
    }
    let DType::Struct(fields, _) = array.dtype() else {
        return Err(failed("field copy policy requires a record"));
    };
    if unobserved_fields
        .iter()
        .any(|index| *index >= fields.nfields())
    {
        return Err(failed("field copy policy exceeds its record"));
    }
    let indices =
        super::native_relational_batch::index_array(array.len(), false, context, |row| {
            Ok(Some(row))
        })?;
    let metadata = crate::native_payload_schema::reserve_schema(array.dtype(), context)?;
    take_admitted::<true>(
        array,
        &indices,
        array.dtype(),
        CopyState {
            policy: CopyPolicy::ValidateValues,
            scalars: None,
            unobserved_fields,
        },
        context,
        metadata,
    )
}

/// Count actual adapter leaf-value construction, including repeated sizing
/// passes and null/default leaves. Indices, offsets, validity and provider
/// internals are excluded; raw floating buffer copies construct no scalars.
#[cfg(all(test, feature = "vortex-write"))]
pub(super) fn detach_with_scalar_count(
    array: &ArrayRef,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<(ArrayRef, u64)> {
    let indices =
        super::native_relational_batch::index_array(array.len(), false, context, |row| {
            Ok(Some(row))
        })?;
    take_record_with_scalar_count(array, &indices, array.dtype(), policy, context)
}

pub(super) fn take_record_with_scalar_count(
    array: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<(ArrayRef, u64)> {
    let metadata = crate::native_payload_schema::reserve_schema(dtype, context)?;
    let scalars = Cell::new(0);
    let output = take_admitted::<true>(
        array,
        indices,
        dtype,
        CopyState {
            policy,
            scalars: Some(&scalars),
            unobserved_fields: &[],
        },
        context,
        metadata,
    )?;
    Ok((output, scalars.get()))
}

pub(super) fn take(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    take_with_policy(source, indices, dtype, CopyPolicy::ValidateValues, context)
}

pub(super) fn take_with_policy(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    // A struct-valued column is one nested value, not a table schema. In
    // particular, sparse state may retain many independent one-row values.
    let metadata = crate::native_payload_schema::reserve_metadata(dtype, context)?;
    take_admitted::<true>(
        source,
        indices,
        dtype,
        CopyState::unmeasured(policy),
        context,
        metadata,
    )
}

/// Gather a whole row record whose top-level width uses shared admission.
pub(super) fn take_record(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    take_record_with_policy(source, indices, dtype, CopyPolicy::ValidateValues, context)
}

pub(super) fn take_record_with_policy(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let metadata = crate::native_payload_schema::reserve_schema(dtype, context)?;
    take_admitted::<true>(
        source,
        indices,
        dtype,
        CopyState::unmeasured(policy),
        context,
        metadata,
    )
}

/// Check exactly the selected payload in its existing visitation order while
/// retaining no values. The source and selection keep their native owners.
pub(super) fn validate_selection(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let metadata = crate::native_payload_schema::reserve_metadata(dtype, context)?;
    take_admitted::<false>(
        source,
        indices,
        dtype,
        CopyState::unmeasured(CopyPolicy::ValidateValues),
        context,
        metadata,
    )
    .map(drop)
}

fn take_admitted<const RETAIN: bool>(
    source: &ArrayRef,
    indices: &ArrayRef,
    dtype: &DType,
    copy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    metadata: MemoryLease,
) -> Result<ArrayRef> {
    if source.dtype().as_nonnullable() != dtype.as_nonnullable() {
        return Err(failed(
            "nested payload gathering requires the same declared child types",
        ));
    }
    let allocator =
        crate::owned_buffers::with_credit(context.native_session().host_allocator(), metadata);
    let mut execution = context.native_session().create_execution_ctx();
    let mut rows = ReservedVec::new(context.memory())?;
    rows.reserve(indices.len())?;
    for row in 0..indices.len() {
        check(row, context)?;
        let scalar = indices
            .execute_scalar(row, &mut execution)
            .map_err(vortex_error)?;
        let index = if scalar.is_null() {
            if !dtype.is_nullable() {
                return Err(failed(
                    "null gather index requires a nullable output payload",
                ));
            }
            None
        } else {
            Some(
                scalar
                    .as_primitive()
                    .as_::<usize>()
                    .filter(|index| *index < source.len())
                    .ok_or_else(|| failed("payload index exceeds native input"))?,
            )
        };
        rows.values.push(index);
    }
    copy_rows::<RETAIN>(Some(source), &rows.values, dtype, copy, context, &allocator)
}

/// Typed empty/null output, including a missing outer-join side. Nonnullable
/// children under a null parent receive unobservable, type-correct placeholders.
pub(super) fn defaults(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let metadata = crate::native_payload_schema::reserve_metadata(dtype, context)?;
    defaults_admitted(dtype, rows, context, metadata)
}

/// A typed empty row record keeps the same width policy as nonempty delivery.
pub(crate) fn empty_record(
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let metadata = crate::native_payload_schema::reserve_schema(dtype, context)?;
    defaults_admitted(dtype, 0, context, metadata)
}

fn defaults_admitted(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
    metadata: MemoryLease,
) -> Result<ArrayRef> {
    let allocator =
        crate::owned_buffers::with_credit(context.native_session().host_allocator(), metadata);
    let mut indices = ReservedVec::new(context.memory())?;
    indices.reserve(rows)?;
    indices.values.resize(rows, None);
    copy_rows::<true>(
        None,
        &indices.values,
        dtype,
        CopyState::unmeasured(CopyPolicy::ValidateValues),
        context,
        &allocator,
    )
}

/// Coalesce retained one-row native values into one compact, bounded column.
/// Missing values denote NULL parents; transient chunk metadata is reserved.
pub(super) fn retained_column(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<Option<ArrayRef>>,
) -> Result<ArrayRef> {
    let mut arrays = ReservedVec::new(context.memory())?;
    let mut positions = ReservedVec::new(context.memory())?;
    arrays.reserve(rows)?;
    positions.reserve(rows)?;
    for row in 0..rows {
        check(row, context)?;
        if let Some(array) = value(row)? {
            if array.len() != 1 || array.dtype().as_nonnullable() != dtype.as_nonnullable() {
                return Err(failed(
                    "retained native value changed its bound dtype or row count",
                ));
            }
            positions.values.push(Some(arrays.values.len()));
            arrays
                .values
                .push(array.cast(dtype.clone()).map_err(vortex_error)?);
        } else {
            if !dtype.is_nullable() {
                return Err(failed("missing retained value requires a nullable output"));
            }
            positions.values.push(None);
        }
    }
    if arrays.values.is_empty() {
        return defaults(dtype, rows, context);
    }
    let (arrays, _ownership) = arrays.into_parts();
    let array = ChunkedArray::try_new(arrays, dtype.clone())
        .map_err(vortex_error)?
        .into_array();
    let indices =
        super::native_relational_batch::index_array(rows, dtype.is_nullable(), context, |row| {
            Ok(positions.values[row])
        })?;
    take(&array, &indices, dtype, context)
}

fn check(row: usize, context: &NativeExecutionContext<'_>) -> Result<()> {
    if row.is_multiple_of(1024) {
        context.check_cancelled()?;
    }
    Ok(())
}

pub(super) fn default_value(dtype: &DType) -> Result<Value<'static>> {
    if dtype.is_nullable() {
        return Ok(Value::Null);
    }
    Ok(match dtype {
        DType::Bool(_) => Value::Bool(false),
        DType::Utf8(_) => Value::Text("".into()),
        DType::Binary(_) => Value::Binary((&[][..]).into()),
        DType::Decimal(dtype, _) if crate::native_payload_schema::admitted_decimal(*dtype) => {
            Value::Decimal(0, *dtype)
        }
        DType::Extension(_) if crate::native_payload_schema::temporal_storage(dtype).is_some() => {
            Value::Int(0)
        }
        DType::Primitive(ptype, _) if ptype.is_signed_int() => Value::Int(0),
        DType::Primitive(ptype, _) if ptype.is_unsigned_int() => Value::UInt(0),
        DType::Primitive(PType::F16 | PType::F32 | PType::F64, _) => Value::Float(0.0),
        _ => return Err(failed("payload default requires a scalar leaf")),
    })
}

fn validity<const RETAIN: bool>(
    dtype: &DType,
    valid: &[bool],
    allocator: &HostAllocatorRef,
) -> Result<Validity> {
    if !dtype.is_nullable() {
        if valid.iter().any(|valid| !valid) {
            return Err(failed("null in a nonnullable nested payload"));
        }
        return Ok(Validity::NonNullable);
    }
    Ok(Validity::Array(result_batch::build_column(
        &DType::Bool(Nullability::NonNullable),
        if RETAIN { valid.len() } else { 0 },
        allocator,
        |row| Ok(Value::Bool(valid[row])),
    )?))
}

#[allow(clippy::too_many_arguments)] // One existing selected leaf and its admitted owners.
fn copy_leaf<const RETAIN: bool>(
    selected: Option<ArrayRef>,
    rows: &[Option<usize>],
    dtype: &DType,
    copy: CopyState<'_>,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
    execution: &mut vortex::array::ExecutionCtx,
) -> Result<ArrayRef> {
    // Selection precedes leaf canonicalization: hidden or unselected
    // strings never become an owned part of the returned payload.
    let selected = selected
        .map(|array| {
            array
                .execute::<Columnar>(execution)
                .map(IntoArray::into_array)
                .map_err(vortex_error)
        })
        .transpose()?;
    if RETAIN
        && (copy.policy == CopyPolicy::PreserveUnobserved
            || context.float_policy() == shardloom_core::FloatPolicy::Extended)
        && matches!(
            dtype,
            DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
        )
        && let Some(selected) = &selected
    {
        return storage::copy(selected, rows, dtype, context, allocator);
    }
    // This boundary already copies every selected value into credited
    // output buffers. Resolve native numeric storage and validity once
    // instead of repeatedly traversing encoded dictionaries/selections
    // for each scalar. Unselected payloads remain outside the copy.
    let numeric = selected
        .as_ref()
        .filter(|_| {
            matches!(dtype, DType::Primitive(..))
                || crate::native_payload_schema::temporal_storage(dtype).is_some()
        })
        .map(|array| {
            let primitive = result_batch::scalar_storage(array, execution)?
                .execute::<PrimitiveArray>(execution)
                .map_err(vortex_error)?;
            NativeNumericOwner::new(primitive, execution)
        })
        .transpose()?;
    if !RETAIN
        && copy.policy == CopyPolicy::ValidateValues
        && rows.iter().all(Option::is_some)
        && let Some(selected) = &selected
        && validation::validate_typed_column(
            selected,
            dtype,
            rows.len(),
            numeric.as_ref(),
            execution,
            context,
        )?
    {
        // Selection, parent masks and field order belong to this
        // recursive owner. Reuse typed validation only after those
        // obligations have produced a complete observed leaf domain.
        // Missing coordinates still use the existing placeholder path.
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.payload_recursive_typed_validation_columns += 1;
        });
        return result_batch::build_column(dtype, 0, allocator, |_| default_value(dtype));
    }
    let variable = selected
        .as_ref()
        .map(|array| result_batch::VariableValues::try_new(array, execution))
        .transpose()?
        .flatten();
    let value = |row: usize| {
        check(row, context)?;
        #[cfg(all(test, debug_assertions))]
        if !RETAIN {
            super::native_relational_observation::record(|counts| {
                counts.payload_validation_scalar_calls += 1;
            });
        }
        let value = if rows[row].is_none() {
            default_value(dtype)
        } else if let Some(numeric) = &numeric {
            numeric.stat_value(row).map(Value::from)
        } else if let Some(variable) = &variable {
            variable.value(row)
        } else {
            result_batch::scalar_value(selected.as_ref().expect("selected leaf"), row, execution)
        }?;
        copy.record_scalars(1)?;
        Ok(value)
    };
    if RETAIN {
        result_batch::build_column(dtype, rows.len(), allocator, value)
    } else {
        result_batch::build_column_prefix_with_float_policy(
            dtype,
            rows.len(),
            0,
            allocator,
            context.float_policy(),
            value,
        )
    }
}
