//! Borrow native fixed-width UUID bytes without a decoded row representation.

use shardloom_core::{Result, ShardLoomError};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{
            ExtensionArray, FixedSizeListArray, PrimitiveArray,
            extension::ExtensionArrayExt as _,
            fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
        },
        dtype::{DType, Nullability, PType},
    },
    mask::Mask,
};

pub(crate) struct Values {
    bytes: PrimitiveArray,
    valid: Mask,
    rows: usize,
}

impl Values {
    pub(crate) fn new(array: &ArrayRef, context: &mut ExecutionCtx) -> Result<Self> {
        let dtype = super::storage(array.dtype()).ok_or_else(|| {
            failed("requires canonical UUID metadata and sixteen nonnull UInt8 bytes")
        })?;
        let expected = array
            .len()
            .checked_mul(16)
            .ok_or_else(|| failed("byte count overflow"))?;
        let extension = array
            .clone()
            .execute::<ExtensionArray>(context)
            .map_err(failed)?;
        if extension.dtype() != array.dtype() || extension.len() != array.len() {
            return Err(failed("execution changed UUID dtype or row count"));
        }
        let list = extension
            .storage_array()
            .clone()
            .execute::<FixedSizeListArray>(context)
            .map_err(failed)?;
        if list.dtype() != dtype || list.len() != array.len() || list.elements().len() != expected {
            return Err(failed(
                "storage changed UUID dtype, row count or byte count",
            ));
        }
        let bytes = list
            .elements()
            .clone()
            .execute::<PrimitiveArray>(context)
            .map_err(failed)?;
        if bytes.dtype() != &DType::Primitive(PType::U8, Nullability::NonNullable)
            || bytes.len() != expected
        {
            return Err(failed(
                "storage requires exactly sixteen nonnull bytes per UUID",
            ));
        }
        let valid = list
            .fixed_size_list_validity()
            .execute_mask(array.len(), context)
            .map_err(failed)?;
        Ok(Self {
            bytes,
            valid,
            rows: array.len(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    pub(crate) fn value(&self, row: usize) -> Result<Option<[u8; 16]>> {
        if row >= self.rows {
            return Err(failed("selected row exceeds UUID owner"));
        }
        if !self.valid.value(row) {
            return Ok(None);
        }
        let start = row * 16;
        let bytes = self.bytes.as_slice::<u8>();
        Ok(Some(
            bytes[start..start + 16]
                .try_into()
                .expect("checked fixed UUID width"),
        ))
    }
}

fn failed(reason: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native UUID: {reason}; no fallback execution was attempted"
    ))
}
