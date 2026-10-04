//! Shared native list coordinates for expansion, compact gathering and delivery.

use super::{native_relational_batch::failed, vortex_error};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::{FixedSizeListArray, ListViewArray},
    dtype::DType,
    validity::Validity,
};

pub(super) struct Column {
    pub(super) elements: ArrayRef,
    validity: Validity,
    offsets: Option<ArrayRef>,
    sizes: Option<ArrayRef>,
    fixed: usize,
}

impl Column {
    pub(super) fn new(array: ArrayRef, context: &mut ExecutionCtx) -> Result<Self> {
        match array.dtype() {
            DType::List(_, _) => {
                let list = array
                    .execute::<ListViewArray>(context)
                    .map_err(vortex_error)?;
                Ok(Self {
                    elements: list.elements().clone(),
                    validity: list.listview_validity(),
                    offsets: Some(list.offsets().clone()),
                    sizes: Some(list.sizes().clone()),
                    fixed: 0,
                })
            }
            DType::FixedSizeList(_, _, _) => {
                let list = array
                    .execute::<FixedSizeListArray>(context)
                    .map_err(vortex_error)?;
                Ok(Self {
                    elements: list.elements().clone(),
                    validity: list.fixed_size_list_validity(),
                    offsets: None,
                    sizes: None,
                    fixed: list.list_size() as usize,
                })
            }
            _ => Err(failed("list source changed its declared dtype")),
        }
    }

    pub(super) fn coordinates(
        &self,
        row: usize,
        context: &mut ExecutionCtx,
    ) -> Result<Option<(usize, usize)>> {
        if !self
            .validity
            .execute_is_valid(row, context)
            .map_err(vortex_error)?
        {
            return Ok(None);
        }
        let index = |array: &ArrayRef, context: &mut ExecutionCtx| -> Result<usize> {
            array
                .execute_scalar(row, context)
                .map_err(vortex_error)?
                .as_primitive()
                .as_::<usize>()
                .ok_or_else(|| failed("list coordinate is not a nonnegative platform index"))
        };
        let (start, count) = match (&self.offsets, &self.sizes) {
            (Some(offsets), Some(sizes)) => (index(offsets, context)?, index(sizes, context)?),
            _ => (
                row.checked_mul(self.fixed)
                    .ok_or_else(|| failed("list offset overflow"))?,
                self.fixed,
            ),
        };
        if start
            .checked_add(count)
            .is_none_or(|end| end > self.elements.len())
        {
            return Err(failed("list coordinates exceed native elements"));
        }
        Ok(Some((start, count)))
    }
}
