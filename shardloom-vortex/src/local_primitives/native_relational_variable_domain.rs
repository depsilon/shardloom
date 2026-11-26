//! Existing variable-key semantics over native views or borrowed offset storage.
//! Offset-backed values are admitted only by the exact-match consumer; ordinary
//! key preparation retains its canonical view and complete-domain validation.

use super::{NativeNumericOwner, failed, vortex_error};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::ops::Range;
use vortex::{
    array::arrays::{VarBinViewArray, varbinview::VarBinViewArrayExt as _},
    buffer::ByteBuffer,
};

pub(in crate::local_primitives) enum VariableDomain {
    Views(VarBinViewArray),
    Offsets(Box<OffsetDomain>),
}

/// Only the optional borrowed-offset path pays for this metadata. Keeping it
/// outside the shared key slot preserves ordinary batch/window memory bounds.
pub(in crate::local_primitives) struct OffsetDomain {
    offsets: NativeNumericOwner,
    bytes: ByteBuffer,
    rows: usize,
    _ownership: MemoryLease,
}

impl VariableDomain {
    pub(super) fn try_offsets(
        offsets: NativeNumericOwner,
        bytes: ByteBuffer,
        rows: usize,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
        let Some(ownership) =
            memory.try_reserve(crate::local_primitives::native_capacity::bytes::<
                OffsetDomain,
            >(1)?)?
        else {
            return Ok(None);
        };
        Ok(Some(Self::Offsets(Box::new(OffsetDomain {
            offsets,
            bytes,
            rows,
            _ownership: ownership,
        }))))
    }

    pub(in crate::local_primitives) fn len(&self) -> usize {
        match self {
            Self::Views(values) => values.len(),
            Self::Offsets(domain) => domain.rows,
        }
    }

    pub(in crate::local_primitives) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn views(&self) -> Option<&VarBinViewArray> {
        match self {
            Self::Views(values) => Some(values),
            Self::Offsets(_) => None,
        }
    }

    fn range(offsets: &NativeNumericOwner, bytes: &ByteBuffer, row: usize) -> Result<Range<usize>> {
        let offset = |index| {
            offsets
                .integer_ordinal(index)?
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| failed("borrowed variable offset is invalid"))
        };
        let start = offset(row)?;
        let end = offset(
            row.checked_add(1)
                .ok_or_else(|| failed("variable ordinal overflow"))?,
        )?;
        if start > end || end > bytes.len() {
            return Err(failed("borrowed variable offsets exceed native bytes"));
        }
        Ok(start..end)
    }

    pub(super) fn bytes_at(&self, row: usize) -> Result<ByteBuffer> {
        match self {
            Self::Views(values) => Ok(values.bytes_at(row)),
            Self::Offsets(domain) => {
                Ok(domain
                    .bytes
                    .slice(Self::range(&domain.offsets, &domain.bytes, row)?))
            }
        }
    }

    pub(in crate::local_primitives) fn borrowed_bytes(&self, row: usize) -> Result<&[u8]> {
        match self {
            Self::Views(values) => Ok(crate::local_primitives::native_utf8::borrowed_bytes(
                values, row,
            )),
            Self::Offsets(domain) => {
                Ok(&domain.bytes.as_slice()[Self::range(&domain.offsets, &domain.bytes, row)?])
            }
        }
    }

    pub(super) fn utf8_at(&self, row: usize) -> Result<&str> {
        match self {
            Self::Views(values) => values.utf8_slice_at(row).map_err(vortex_error),
            Self::Offsets(_) => std::str::from_utf8(self.borrowed_bytes(row)?)
                .map_err(|_| failed("borrowed variable value is not valid UTF-8")),
        }
    }

    pub(super) fn validate_at(&self, row: usize, binary: bool) -> Result<()> {
        if matches!(self, Self::Offsets(_)) {
            if binary {
                self.borrowed_bytes(row)?;
            } else {
                self.utf8_at(row)?;
            }
        }
        Ok(())
    }
}
