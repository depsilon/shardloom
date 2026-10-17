// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-16: preserve shared buffer-table ownership.

use std::ops::Range;

use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::VarBinView;
use crate::arrays::slice::SliceReduce;
use crate::arrays::varbinview::BinaryView;
use crate::arrays::varbinview::VarBinViewArrayExt;

impl SliceReduce for VarBinView {
    fn slice(array: ArrayView<'_, Self>, range: Range<usize>) -> VortexResult<Option<ArrayRef>> {
        let views = array
            .views_handle()
            .slice_typed::<BinaryView>(range.clone());
        let dtype = array.dtype().clone();
        let validity = array.validity()?.slice(range)?;

        // SAFETY: the aligned view slice selects existing valid entries and the
        // same row range is applied to validity; payload buffers do not change.
        let array = unsafe { array.with_views_and_validity_unchecked(views, dtype, validity)? }
            .into_array();

        Ok(Some(array))
    }
}
