// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-16: admit new buffer tables through their allocator.

use std::hash::Hasher;
use std::mem::size_of;
use std::sync::Arc;

use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::ArrayParts;
use crate::ArrayRef;
use crate::EqMode;
use crate::ExecutionCtx;
use crate::ExecutionResult;
use crate::VortexSessionExecute;
use crate::array::Array;
use crate::array::ArrayId;
use crate::array::ArrayView;
use crate::array::VTable;
use crate::array::child_to_validity;
use crate::arrays::varbinview::BinaryView;
use crate::arrays::varbinview::VarBinViewData;
use crate::arrays::varbinview::array::VarBinViewSlots;
use crate::arrays::varbinview::compute::rules::PARENT_RULES;
use crate::arrays::varbinview::metadata::MetadataVec;
use crate::buffer::BufferHandle;
use crate::builders::ArrayBuilder;
use crate::builders::VarBinViewBuilder;
use crate::dtype::DType;
use crate::hash::ArrayEq;
use crate::hash::ArrayHash;
use crate::match_each_varbin_builder;
use crate::memory::DefaultHostAllocator;
use crate::memory::HostAllocatorRef;
use crate::memory::MemorySessionExt;
use crate::serde::ArrayChildren;
use crate::validity::Validity;
mod kernel;
mod operations;
mod validity;
/// A [`VarBinView`]-encoded Vortex array.
pub type VarBinViewArray = Array<VarBinView>;

pub(crate) fn initialize(session: &VortexSession) {
    kernel::initialize(session);
}

#[derive(Clone, Debug)]
pub struct VarBinView;

impl VarBinView {
    /// Replace native handles with a newly admitted table in `allocator`.
    ///
    /// The dtype and validity stay unchanged. Byte-identical host views and
    /// ordered payload buffers can reuse the source's value validation, including
    /// when only reservation owners change. Any differing or newly host-readable
    /// contents pass ordinary validation. No device contents are read or copied.
    /// Structural checks and table admission apply to both paths. The returned
    /// parts do not retain the source table or its outer array owner.
    pub fn with_buffer_handles_in(
        array: ArrayView<'_, Self>,
        views: BufferHandle,
        buffers: impl ExactSizeIterator<Item = BufferHandle>,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<ArrayParts<Self>> {
        let dtype = array.dtype().clone();
        let validity = array.validity()?;
        VarBinViewData::check_handle_shape(&views, &dtype, &validity)?;
        let (buffers, owner) = MetadataVec::from_iter(buffers, allocator)?.into_arc()?;
        let same_host_bytes = views
            .as_host_opt()
            .zip(array.views_handle().as_host_opt())
            .is_some_and(|(new, original)| new.as_slice() == original.as_slice())
            && buffers.len() == array.data_buffers().len()
            && buffers
                .iter()
                .zip(array.data_buffers().iter())
                .all(|(new, original)| {
                    new.as_host_opt()
                        .zip(original.as_host_opt())
                        .is_some_and(|(new, original)| new.as_slice() == original.as_slice())
                });
        let data = if same_host_bytes {
            // SAFETY: every view and ordered payload buffer is host-readable and
            // byte-identical to the validated source, including null slots. The
            // dtype and validity are unchanged. The new table is admitted and
            // the helper retains structural/validity checks. Owner replacement
            // cannot change the immutable bytes or introduce device contents.
            unsafe {
                VarBinViewData::from_admitted_handles_unchecked(
                    views,
                    buffers,
                    owner,
                    dtype.clone(),
                    validity,
                )?
            }
        } else {
            VarBinViewData::from_admitted_handles(views, buffers, owner, dtype.clone(), validity)?
        };
        Ok(ArrayParts::new(Self, dtype, array.len(), data)
            .with_slots(array.slots().iter().cloned().collect()))
    }
}

impl ArrayHash for VarBinViewData {
    fn array_hash<H: Hasher>(&self, state: &mut H, accuracy: EqMode) {
        for buffer in self.buffers.iter() {
            buffer.array_hash(state, accuracy);
        }
        self.views.array_hash(state, accuracy);
    }
}

impl ArrayEq for VarBinViewData {
    fn array_eq(&self, other: &Self, accuracy: EqMode) -> bool {
        self.buffers.len() == other.buffers.len()
            && self
                .buffers
                .iter()
                .zip(other.buffers.iter())
                .all(|(a, b)| a.array_eq(b, accuracy))
            && self.views.array_eq(&other.views, accuracy)
    }
}

impl VTable for VarBinView {
    type TypedArrayData = VarBinViewData;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.varbinview");
        *ID
    }

    fn nbuffers(array: ArrayView<'_, Self>) -> usize {
        array.data_buffers().len() + 1
    }

    fn validate(
        &self,
        data: &VarBinViewData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure!(
            slots.len() == VarBinViewSlots::COUNT,
            "VarBinViewArray expected {} slots, found {}",
            VarBinViewSlots::COUNT,
            slots.len()
        );
        vortex_ensure!(
            data.len() == len,
            "VarBinViewArray length {} does not match outer length {}",
            data.len(),
            len
        );
        vortex_ensure!(
            matches!(dtype, DType::Binary(_) | DType::Utf8(_)),
            "VarBinViewArray dtype must be binary or utf8, got {dtype}"
        );
        // ArrayParts is public and may pair binary-validated data with a UTF-8
        // dtype. That change must establish the stronger value invariant before
        // any safe reconstruction can rely on this array's validation.
        if matches!(dtype, DType::Utf8(_)) && !data.utf8_values {
            let validity = child_to_validity(
                slots[VarBinViewSlots::VALIDITY].as_ref(),
                dtype.nullability(),
            );
            VarBinViewData::validate_handle_contents(&data.views, &data.buffers, dtype, &validity)?;
        }
        Ok(())
    }

    fn buffer(array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        let ndata = array.data_buffers().len();
        if idx < ndata {
            array.data_buffers()[idx].clone()
        } else if idx == ndata {
            array.views_handle().clone()
        } else {
            vortex_panic!("VarBinViewArray buffer index {idx} out of bounds")
        }
    }

    fn buffer_name(array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        let ndata = array.data_buffers().len();
        if idx < ndata {
            Some(format!("buffer_{idx}"))
        } else if idx == ndata {
            Some("views".to_string())
        } else {
            vortex_panic!("VarBinViewArray buffer_name index {idx} out of bounds")
        }
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        let Some((views, data_buffers)) = buffers.split_last() else {
            vortex_bail!("Expected at least 1 buffer, got 0");
        };
        let allocator = array
            .table_owner
            .as_ref()
            .map(|owner| Arc::clone(&owner.allocator))
            .unwrap_or_else(|| Arc::new(DefaultHostAllocator));
        Self::with_buffer_handles_in(
            array,
            views.clone(),
            data_buffers.iter().cloned(),
            &allocator,
        )
    }

    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    fn deserialize(
        &self,
        dtype: &DType,
        len: usize,
        metadata: &[u8],

        buffers: &[BufferHandle],
        children: &dyn ArrayChildren,
        session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        if !metadata.is_empty() {
            vortex_bail!(
                "VarBinViewArray expects empty metadata, got {} bytes",
                metadata.len()
            );
        }
        let Some((views_handle, data_handles)) = buffers.split_last() else {
            vortex_bail!("Expected at least 1 buffer, got 0");
        };

        let validity = if children.is_empty() {
            Validity::from(dtype.nullability())
        } else if children.len() == 1 {
            let validity = children.get(0, &Validity::DTYPE, len)?;
            Validity::Array(validity)
        } else {
            vortex_bail!("Expected 0 or 1 children, got {}", children.len());
        };

        let views_nbytes = views_handle.len();
        let expected_views_nbytes = len
            .checked_mul(size_of::<BinaryView>())
            .ok_or_else(|| vortex_err!("views byte length overflow for len={len}"))?;
        if views_nbytes != expected_views_nbytes {
            vortex_bail!(
                "Expected views buffer length {} bytes, got {} bytes",
                expected_views_nbytes,
                views_nbytes
            );
        }
        VarBinViewData::check_view_layout(views_handle)?;
        let allocator = session.allocator();

        // Device contents remain opaque; the checked constructor still validates
        // every available host view and every known payload-buffer extent.
        if buffers.iter().any(|b| b.is_on_device()) {
            let data = VarBinViewData::try_new_handle_in(
                views_handle.clone(),
                data_handles.iter().cloned(),
                dtype.clone(),
                validity.clone(),
                &allocator,
            )?;
            let slots = VarBinViewData::make_slots(&validity, len);
            return Ok(ArrayParts::new(self.clone(), dtype.clone(), len, data).with_slots(slots));
        }

        // The temporary host-only validation list is independent of the final
        // handle table. It remains admitted until validation has finished.
        let data_buffers =
            MetadataVec::from_iter(data_handles.iter().map(|b| b.as_host().clone()), &allocator)?;
        let views = Buffer::<BinaryView>::from_byte_buffer(views_handle.clone().as_host().clone());
        let views = VarBinViewData::validate_and_fix(
            views,
            &data_buffers,
            dtype,
            &validity,
            &mut session.create_execution_ctx(),
        )?;
        drop(data_buffers);
        // SAFETY: validate_and_fix checked all non-null values against exactly
        // these host payloads and replaced null slots with valid empty views.
        // Dropping the temporary handle list does not change the buffers.
        let data = unsafe {
            VarBinViewData::try_new_handle_in_unchecked(
                BufferHandle::new_host(views.into_byte_buffer()),
                data_handles.iter().cloned(),
                dtype.clone(),
                validity.clone(),
                &allocator,
            )?
        };
        let slots = VarBinViewData::make_slots(&validity, len);
        Ok(ArrayParts::new(self.clone(), dtype.clone(), len, data).with_slots(slots))
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        VarBinViewSlots::NAMES[idx].to_string()
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        PARENT_RULES.evaluate(array, parent, child_idx)
    }

    fn append_to_builder(
        array: ArrayView<'_, Self>,
        builder: &mut dyn ArrayBuilder,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        if let Some(builder) = builder.as_any_mut().downcast_mut::<VarBinViewBuilder>() {
            return builder.append_varbinview_array(&array.into_owned(), ctx);
        }
        if let Some(result) =
            match_each_varbin_builder!(builder, |builder| builder.append_varbinview(array, ctx))
        {
            return result;
        }
        vortex_bail!("append_to_builder for VarBinView requires a variable-binary builder")
    }

    fn execute(array: Array<Self>, _ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        Ok(ExecutionResult::done(array))
    }
}

#[cfg(test)]
mod tests {
    use vortex_buffer::ByteBufferMut;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::ArrayContext;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::assert_arrays_eq;
    use crate::serde::SerializeOptions;
    use crate::serde::SerializedArray;

    #[test]
    fn test_nullable_varbinview_serde_roundtrip() {
        let array = VarBinViewArray::from_iter_nullable_str([
            Some("hello"),
            None,
            Some("world"),
            None,
            Some("a moderately long string for testing"),
        ]);
        let dtype = array.dtype().clone();
        let len = array.len();

        let session = array_session();
        let mut ctx = session.create_execution_ctx();
        let array_ctx = ArrayContext::empty();
        let serialized = array
            .clone()
            .into_array()
            .serialize(&array_ctx, &session, &SerializeOptions::default())
            .unwrap();

        let mut concat = ByteBufferMut::empty();
        for buf in serialized {
            concat.extend_from_slice(buf.as_ref());
        }
        let parts = SerializedArray::try_from(concat.freeze()).unwrap();
        let decoded = parts
            .decode(&dtype, len, &ReadContext::new(array_ctx.to_ids()), &session)
            .unwrap();

        assert_arrays_eq!(decoded, array, &mut ctx);
    }
}
