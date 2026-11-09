//! Credited F64/U64 list results; each selected row is evaluated only once.

use super::{
    ArrayRef, DType, NativeExecutionContext, Result, check, failed, result_batch, vortex_error,
};
use crate::owned_buffers::{
    bool_array_with_retained_buffer, retain_empty_array_credit, with_credit,
};
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{FixedSizeListArray, ListViewArray},
        dtype::PType,
        memory::{HostAllocatorRef, HostMemorySessionExt as _, WritableHostBuffer},
        validity::Validity,
    },
    buffer::{Alignment, BitBuffer},
};

pub(super) struct Builder {
    data: WritableHostBuffer,
    offsets: Option<WritableHostBuffer>,
    sizes: Option<WritableHostBuffer>,
    valid: Option<WritableHostBuffer>,
    allocator: HostAllocatorRef,
    dtype: DType,
    rows: usize,
    next: usize,
    used: usize,
}

impl Builder {
    pub(super) fn new(
        dtype: &DType,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let (DType::List(child, _) | DType::FixedSizeList(child, _, _)) = dtype else {
            return Err(failed("vector result requires list storage"));
        };
        if !matches!(
            child.as_ref(),
            DType::Primitive(
                PType::F64 | PType::U64,
                vortex::array::dtype::Nullability::NonNullable
            )
        ) {
            return Err(failed("vector result requires nonnull 64-bit components"));
        }
        let allocator = with_credit(
            context.native_session().host_allocator(),
            crate::native_payload_schema::reserve_metadata(dtype, context)?,
        );
        let (offsets, sizes) = if matches!(dtype, DType::List(_, _)) {
            let bytes = rows
                .checked_mul(8)
                .ok_or_else(|| failed("vector result coordinate size overflow"))?;
            (
                Some(allocate(&allocator, bytes, 8)?),
                Some(allocate(&allocator, bytes, 8)?),
            )
        } else {
            (None, None)
        };
        let valid = dtype
            .is_nullable()
            .then(|| allocate(&allocator, rows.div_ceil(8), 1))
            .transpose()?;
        Ok(Self {
            data: allocate(&allocator, 0, 8)?,
            offsets,
            sizes,
            valid,
            allocator,
            dtype: dtype.clone(),
            rows,
            next: 0,
            used: 0,
        })
    }

    /// Invalid fixed parents contain unobserved zero placeholders; invalid
    /// variable parents add no children. Valid values are exact 64-bit storage.
    pub(super) fn row(
        &mut self,
        present: bool,
        count: usize,
        context: &NativeExecutionContext<'_>,
        mut value: impl FnMut(usize) -> Result<u64>,
    ) -> Result<()> {
        if self.next >= self.rows || (!present && !self.dtype.is_nullable()) {
            return Err(failed("vector result row violates its declared domain"));
        }
        let count = match &self.dtype {
            DType::FixedSizeList(_, size, _) => {
                if present && count != *size as usize {
                    return Err(failed("vector result changed fixed dimension"));
                }
                *size as usize
            }
            _ => {
                if present {
                    count
                } else {
                    0
                }
            }
        };
        let end = count
            .checked_mul(8)
            .and_then(|n| self.used.checked_add(n))
            .ok_or_else(|| failed("vector result component size overflow"))?;
        self.grow(end)?;
        for index in 0..count {
            check(index, context)?;
            let bits = if present { value(index)? } else { 0 };
            let from = self.used + index * 8;
            self.data.as_mut_slice()[from..from + 8].copy_from_slice(&bits.to_ne_bytes());
        }
        if present && let Some(valid) = &mut self.valid {
            valid.as_mut_slice()[self.next / 8] |= 1 << (self.next % 8);
        }
        if let (Some(offsets), Some(sizes)) = (&mut self.offsets, &mut self.sizes) {
            let offset = u64::try_from(self.used / 8).map_err(vortex_error)?;
            offsets.as_mut_slice()[self.next * 8..(self.next + 1) * 8]
                .copy_from_slice(&offset.to_ne_bytes());
            sizes.as_mut_slice()[self.next * 8..(self.next + 1) * 8]
                .copy_from_slice(&u64::try_from(count).map_err(vortex_error)?.to_ne_bytes());
        }
        self.used = end;
        self.next += 1;
        Ok(())
    }

    fn grow(&mut self, needed: usize) -> Result<()> {
        if needed <= self.data.len() {
            return Ok(());
        }
        let capacity = needed.max(self.data.len().saturating_mul(2).max(256));
        // Full replacement is admitted while the previous buffer is still live.
        let mut replacement = allocate(&self.allocator, capacity, 8)?;
        replacement.as_mut_slice()[..self.used]
            .copy_from_slice(&self.data.as_mut_slice()[..self.used]);
        self.data = replacement;
        Ok(())
    }

    pub(super) fn finish(self, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
        context.check_cancelled()?;
        if self.next != self.rows {
            return Err(failed("vector result has incomplete rows"));
        }
        let data = self.data.freeze();
        // Slicing an empty ByteBuffer drops its original owner. Preserve the
        // initial zero-length allocation itself, including bare-buffer extraction.
        let data = if self.used == data.len() {
            data
        } else {
            data.slice(0..self.used)
        };
        let ptype = match &self.dtype {
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => match child.as_ref() {
                DType::Primitive(ptype, _) => *ptype,
                _ => unreachable!("admitted component"),
            },
            _ => unreachable!("admitted list"),
        };
        let child = retain_empty_array_credit(
            result_batch::primitive(ptype, data, Validity::NonNullable),
            &self.allocator,
        )
        .map_err(vortex_error)?;
        let validity = if let Some(valid) = self.valid {
            let array = bool_array_with_retained_buffer(
                BitBuffer::new(valid.freeze(), self.rows),
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            Validity::Array(
                retain_empty_array_credit(array, &self.allocator).map_err(vortex_error)?,
            )
        } else {
            Validity::NonNullable
        };
        let array = if let (Some(offsets), Some(sizes)) = (self.offsets, self.sizes) {
            let offsets =
                result_batch::primitive(PType::U64, offsets.freeze(), Validity::NonNullable);
            let sizes = result_batch::primitive(PType::U64, sizes.freeze(), Validity::NonNullable);
            // List-to-ListView conversion replaces an empty child with a static
            // array. Construct the native view directly so extracted empty
            // children and bare buffers retain the admitted allocator owners.
            ListViewArray::try_new(
                child,
                offsets,
                sizes,
                validity,
                &mut context.native_session().create_execution_ctx(),
            )
            .map_err(vortex_error)?
            .into_array()
        } else if let DType::FixedSizeList(_, size, _) = self.dtype {
            FixedSizeListArray::try_new(child, size, validity, self.rows)
                .map_err(vortex_error)?
                .into_array()
        } else {
            return Err(failed("vector result lost its list shape"));
        };
        retain_empty_array_credit(array, &self.allocator).map_err(vortex_error)
    }
}

fn allocate(
    allocator: &HostAllocatorRef,
    bytes: usize,
    alignment: usize,
) -> Result<WritableHostBuffer> {
    let mut buffer = allocator
        .allocate(bytes, Alignment::new(alignment))
        .map_err(vortex_error)?;
    buffer.as_mut_slice().fill(0);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::array::dtype::Nullability;

    #[test]
    fn native_vector_wide_output_observes_midrow_cancellation_and_releases_partial_buffers() {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let token = CancellationToken::default();
        let dtype = DType::List(
            DType::Primitive(PType::F64, Nullability::NonNullable).into(),
            Nullability::Nullable,
        );
        let mut visited = 0;
        let error = session
            .with_native_execution_context(&token, |context| {
                let mut output = Builder::new(&dtype, 1, context)?;
                output.row(true, 16_384, context, |index| {
                    visited += 1;
                    if index == 3 {
                        token.cancel();
                    }
                    Ok(1.0f64.to_bits())
                })?;
                output.finish(context)
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert!(
            (4..16_384).contains(&visited),
            "cancellation must interrupt the component loop"
        );
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut output = Builder::new(&dtype, 1, context)?;
                output.row(true, 16_384, context, |_| Ok(1.0f64.to_bits()))?;
                output.finish(context)
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}
