//! A batch-owned native index domain shared by field-major payload validation.

use super::{NativeNumericOwner, check};
use crate::local_primitives::native_relational_batch::failed;
use crate::{native_payload_work::Counter, resident_session::NativeExecutionContext};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{Primitive, PrimitiveArray, primitive::PrimitiveArrayExt as _},
    dtype::{DType, PType},
    validity::Validity,
};

/// The enclosing Gather/table or join/record retains the source identity.
/// Keep this cache inside that owner. The enclosing owner provides identity,
/// and the row count check protects its bound coordinate domain.
pub(in crate::local_primitives) struct Selection {
    indices: NativeNumericOwner,
    provider_indices: ArrayRef,
    source_rows: usize,
    first_missing: Option<usize>,
    first_invalid: Option<usize>,
    identity: bool,
}

impl Selection {
    /// Only the existing batch owners' canonical `UInt64` indices are admitted.
    /// Defer index diagnostics until the original field's declaration is checked.
    pub(in crate::local_primitives) fn new(
        indices: &ArrayRef,
        source_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let primitive = indices
            .as_opt::<Primitive>()
            .filter(|array| array.ptype() == PType::U64 && indices.is_host())
            .ok_or_else(|| failed("shared payload selection requires native UInt64 indices"))?;
        let indices = NativeNumericOwner::new(
            primitive.into_owned(),
            &mut context.native_session().create_execution_ctx(),
        )?;
        let mut first_missing = None;
        let mut first_invalid = None;
        let mut identity = indices.len() == source_rows;
        for (row, &value) in indices.primitive().as_slice::<u64>().iter().enumerate() {
            check(row, context)?;
            if indices.validity().value(row) {
                let index = usize::try_from(value).ok();
                if index.is_none_or(|index| index >= source_rows) {
                    first_invalid.get_or_insert(row);
                }
                identity &= index == Some(row);
            } else {
                first_missing.get_or_insert(row);
                identity = false;
            }
        }
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.payload_selection_domains += 1;
            counts.payload_selection_coordinates += indices.len() as u64;
        });
        // Match the original payload gather, which removes nullable index
        // metadata when this batch has no missing coordinates. Buffers stay
        // shared with their original allocation credits.
        let provider_indices =
            if first_missing.is_none() && indices.primitive().dtype().is_nullable() {
                PrimitiveArray::new(
                    indices.primitive().to_buffer::<u64>(),
                    Validity::NonNullable,
                )
                .into_array()
            } else {
                indices.primitive().as_ref().clone()
            };
        context.record_payload_work(&[
            (Counter::PreparedSelections, 1),
            (Counter::PreparedCoordinates, indices.len() as u64),
        ])?;
        Ok(Self {
            indices,
            provider_indices,
            source_rows,
            first_missing,
            first_invalid,
            identity,
        })
    }

    pub(super) fn check_field(&self, source: &ArrayRef, dtype: &DType) -> Result<()> {
        if source.dtype().as_nonnullable() != dtype.as_nonnullable() {
            return Err(failed(
                "nested payload gathering requires the same declared child types",
            ));
        }
        if source.len() != self.source_rows {
            return Err(failed(
                "shared payload selection changed its source row domain",
            ));
        }
        if !dtype.is_nullable()
            && let Some(missing) = self.first_missing
            && self.first_invalid.is_none_or(|invalid| missing < invalid)
        {
            return Err(failed(
                "null gather index requires a nullable output payload",
            ));
        }
        if self.first_invalid.is_some() {
            return Err(failed("payload index exceeds native input"));
        }
        Ok(())
    }
}

/// Recursive masks borrow their credited coordinates. Top-level selections
/// borrow the original native index buffer and its already resolved validity.
#[derive(Clone, Copy)]
pub(super) enum Rows<'a> {
    Values(&'a [Option<usize>]),
    Selected(&'a Selection),
}

impl Rows<'_> {
    pub(super) fn len(self) -> usize {
        match self {
            Self::Values(rows) => rows.len(),
            Self::Selected(selection) => selection.indices.len(),
        }
    }

    pub(super) fn get(self, row: usize) -> Option<usize> {
        match self {
            Self::Values(rows) => rows[row],
            Self::Selected(selection) => selection.indices.validity().value(row).then(|| {
                usize::try_from(selection.indices.primitive().as_slice::<u64>()[row])
                    .expect("selection coordinates passed their source-domain check")
            }),
        }
    }

    pub(super) fn iter(self) -> impl ExactSizeIterator<Item = Option<usize>> {
        (0..self.len()).map(move |row| self.get(row))
    }

    pub(super) fn is_identity(self, source_rows: usize) -> bool {
        match self {
            Self::Values(rows) => {
                rows.len() == source_rows
                    && rows
                        .iter()
                        .enumerate()
                        .all(|(row, index)| *index == Some(row))
            }
            Self::Selected(selection) => selection.source_rows == source_rows && selection.identity,
        }
    }

    pub(super) fn native_indices(self) -> Option<ArrayRef> {
        match self {
            Self::Values(_) => None,
            Self::Selected(selection) => Some(selection.provider_indices.clone()),
        }
    }
}
