//! Shared native list coordinates for expansion, compact gathering and delivery.

use super::{
    AggregateDistinctValue, NativeNumericOwner, native_relational_batch::failed, vortex_error,
};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::{FixedSizeListArray, ListViewArray, PrimitiveArray},
    dtype::DType,
    validity::Validity,
};
use vortex::mask::Mask;

pub(super) struct Column {
    pub(super) elements: ArrayRef,
    validity: Validity,
    offsets: Option<ArrayRef>,
    sizes: Option<ArrayRef>,
    fixed: usize,
    rows: usize,
}

impl Column {
    pub(super) fn new(array: ArrayRef, context: &mut ExecutionCtx) -> Result<Self> {
        let rows = array.len();
        let dtype = array.dtype().clone();
        match array.dtype() {
            DType::List(_, _) => {
                let list = array
                    .execute::<ListViewArray>(context)
                    .map_err(vortex_error)?;
                if list.len() != rows || list.dtype() != &dtype {
                    return Err(failed("list execution changed dtype or row count"));
                }
                Ok(Self {
                    elements: list.elements().clone(),
                    validity: list.listview_validity(),
                    offsets: Some(list.offsets().clone()),
                    sizes: Some(list.sizes().clone()),
                    fixed: 0,
                    rows,
                })
            }
            DType::FixedSizeList(_, _, _) => {
                let list = array
                    .execute::<FixedSizeListArray>(context)
                    .map_err(vortex_error)?;
                if list.len() != rows || list.dtype() != &dtype {
                    return Err(failed(
                        "fixed-size list execution changed dtype or row count",
                    ));
                }
                Ok(Self {
                    elements: list.elements().clone(),
                    validity: list.fixed_size_list_validity(),
                    offsets: None,
                    sizes: None,
                    fixed: list.list_size() as usize,
                    rows,
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
        if row >= self.rows {
            return Err(failed("list row index exceeds native input"));
        }
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

    /// Prepare coordinate access once for repeated arbitrary-row key lookups.
    /// The children remain native; no list scalar or decoded child tree is made.
    pub(super) fn into_indexed(
        self,
        context: &mut ExecutionCtx,
    ) -> Result<(ArrayRef, Coordinates)> {
        let execute = |array: ArrayRef, context: &mut ExecutionCtx| {
            let array = array
                .execute::<PrimitiveArray>(context)
                .map_err(vortex_error)?;
            NativeNumericOwner::new(array, context)
        };
        let valid = self
            .validity
            .execute_mask(self.rows, context)
            .map_err(vortex_error)?;
        let coordinates = Coordinates {
            valid,
            offsets: self
                .offsets
                .map(|array| execute(array, context))
                .transpose()?,
            sizes: self
                .sizes
                .map(|array| execute(array, context))
                .transpose()?,
            fixed: self.fixed,
            rows: self.rows,
            elements: self.elements.len(),
        };
        Ok((self.elements, coordinates))
    }
}

pub(super) struct Coordinates {
    valid: Mask,
    offsets: Option<NativeNumericOwner>,
    sizes: Option<NativeNumericOwner>,
    fixed: usize,
    rows: usize,
    elements: usize,
}

impl Coordinates {
    pub(super) fn at(&self, row: usize) -> Result<Option<(usize, usize)>> {
        if row >= self.rows {
            return Err(failed("list row index exceeds native input"));
        }
        if !self.valid.value(row) {
            return Ok(None);
        }
        let index = |owner: &NativeNumericOwner| -> Result<usize> {
            match owner.distinct_value(row)? {
                AggregateDistinctValue::UInt64(value) => {
                    usize::try_from(value).map_err(vortex_error)
                }
                AggregateDistinctValue::Int64(value) => {
                    usize::try_from(value).map_err(vortex_error)
                }
                _ => Err(failed(
                    "list coordinate is not a nonnegative platform index",
                )),
            }
        };
        let (start, count) = match (&self.offsets, &self.sizes) {
            (Some(offsets), Some(sizes)) => (index(offsets)?, index(sizes)?),
            _ => (
                row.checked_mul(self.fixed)
                    .ok_or_else(|| failed("list offset overflow"))?,
                self.fixed,
            ),
        };
        if start
            .checked_add(count)
            .is_none_or(|end| end > self.elements)
        {
            return Err(failed("list coordinates exceed native elements"));
        }
        Ok(Some((start, count)))
    }
}
