//! Bounded Variant extraction through the pinned native format provider.

use super::{
    ArrayRef, DType, NativeExecutionContext, ReservedVec, Result, Value, failed, result_batch,
    vortex_error,
};
use crate::local_primitives::native_payload::{self, CopyPolicy};
use std::sync::Arc;
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::ChunkedArray,
        builtins::ArrayBuiltins as _,
        memory::HostMemorySessionExt as _,
        scalar_fn::fns::variant_get::{VariantGet, VariantGetOptions},
    },
    arrow::ArrowSessionExt as _,
    encodings::parquet_variant::{ParquetVariant, ParquetVariantArraySlotsExt as _},
};

const CHUNK_ROWS: usize = 256;

#[path = "native_relational_variant_workspace.rs"]
mod workspace;

#[derive(PartialEq, Eq)]
pub(in crate::local_primitives) enum Kernel {
    Get(VariantGetOptions),
    IsNull,
}

impl Kernel {
    pub(super) fn evaluate(
        &self,
        source: &ArrayRef,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_general_execution()?;
        if source.dtype() == &DType::Null || source.is_empty() {
            return native_payload::defaults(dtype, source.len(), context);
        }
        let mut chunks = ReservedVec::new(context.memory())?;
        chunks.reserve(source.len().div_ceil(CHUNK_ROWS))?;
        for start in (0..source.len()).step_by(CHUNK_ROWS) {
            context.check_cancelled()?;
            let end = start.saturating_add(CHUNK_ROWS).min(source.len());
            let selected = source.slice(start..end).map_err(vortex_error)?;
            // This boundary validates complete selected Variant bytes and
            // shredded storage before provider methods with infallible accessors.
            // Detachment also removes unselected backing allocations from the
            // workspace estimate; compressed nbytes alone is not such a bound.
            let selected = native_payload::detach_with_policy(
                &selected,
                CopyPolicy::PreserveUnobserved,
                context,
            )?;
            let mut execution = context.native_session().create_execution_ctx();
            if selected.all_invalid(&mut execution).map_err(vortex_error)? {
                // The pinned format kernel returns Arrow NullArray for this
                // case, which is not Variant storage. Answer from validity
                // while retaining the already bound native output dtype.
                chunks
                    .values
                    .push(native_payload::defaults(dtype, selected.len(), context)?);
                continue;
            }
            if let Self::Get(options) = self
                && options.path().is_root()
                && options.dtype().is_none_or(DType::is_variant)
            {
                chunks
                    .values
                    .push(selected.cast(dtype.clone()).map_err(vortex_error)?);
                continue;
            }
            let options = match self {
                Self::Get(options) => Some(options),
                Self::IsNull => None,
            };
            let _workspace = workspace::reserve(&selected, dtype, options, context)?;
            let (output, normalized, format_bridge) = match self {
                Self::IsNull => {
                    let allocator = crate::owned_buffers::with_credit(
                        context.native_session().host_allocator(),
                        crate::native_payload_schema::reserve_metadata(dtype, context)?,
                    );
                    let output =
                        result_batch::build_column(dtype, selected.len(), &allocator, |row| {
                            context.check_cancelled()?;
                            Ok(Value::Bool(
                                selected
                                    .execute_scalar(row, &mut execution)
                                    .map_err(vortex_error)?
                                    .as_variant()
                                    .is_variant_null()
                                    == Some(true),
                            ))
                        })?;
                    (output, false, false)
                }
                Self::Get(options) => {
                    // The provider's shredded fast path uses Arrow casts, while
                    // its raw path uses Variant conversions. Normalize shredded
                    // values first so physical storage cannot change semantics.
                    let normalized = selected
                        .as_opt::<ParquetVariant>()
                        .is_none_or(|storage| storage.typed_value().is_some());
                    let input = if normalized {
                        normalize_storage(&selected, context, &mut execution)?
                    } else {
                        selected.clone()
                    };
                    context.check_cancelled()?;
                    let output = VariantGet::try_new(input, options.clone())
                        .map_err(vortex_error)?
                        .into_array()
                        .execute::<vortex::array::Canonical>(&mut execution)
                        .map_err(vortex_error)?
                        .into_array();
                    (output, normalized, true)
                }
            };
            context.check_cancelled()?;
            if output.dtype() != dtype || output.len() != selected.len() {
                return Err(failed(
                    "Variant provider changed the bound output type or row count",
                ));
            }
            // The output's buffers must outlive provider Arrow allocations and
            // their temporary workspace reservation. Shared native detachment
            // also enforces the caller's floating-value observation policy.
            let owned = native_payload::detach(&output, context)?;
            context.record_variant_expression(selected.len() as u64, normalized, format_bridge)?;
            chunks.values.push(owned);
        }
        assemble_chunks(chunks, dtype)
    }
}

fn normalize_storage(
    selected: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    execution: &mut vortex::array::ExecutionCtx,
) -> Result<ArrayRef> {
    // Preserve raw UUIDs and dynamic values via the provider's encoded format
    // bridge. Its general scalar accessor can erase UUID identity and its
    // canonical path can return unpersistable temporary Variant constants.
    let arrow = context.native_session().arrow();
    let field = arrow
        .to_arrow_field("variant", selected.dtype())
        .map_err(vortex_error)?;
    let storage = arrow
        .execute_arrow(selected.clone(), Some(&field), execution)
        .map_err(vortex_error)?;
    let input = arrow
        .from_arrow_array(storage, &field)
        .map_err(vortex_error)?;
    if !input.is::<ParquetVariant>() {
        return Err(failed(
            "Variant format normalization did not produce the admitted native encoding",
        ));
    }
    Ok(input)
}

fn assemble_chunks(chunks: ReservedVec<ArrayRef>, dtype: &DType) -> Result<ArrayRef> {
    let (chunks, credit) = chunks.into_parts();
    ChunkedArray::try_new(chunks, dtype.clone())
        .map_err(vortex_error)?
        .into_array()
        .with_retained_owner(Arc::new(credit))
        .map_err(vortex_error)
}
