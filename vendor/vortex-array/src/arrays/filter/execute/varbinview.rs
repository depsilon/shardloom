// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-16: preserve shared buffer-table ownership.

use std::sync::Arc;

use vortex_buffer::Buffer;
use vortex_error::VortexExpect;
use vortex_mask::MaskValues;

use crate::arrays::VarBinViewArray;
use crate::arrays::filter::execute::buffer;
use crate::arrays::filter::execute::filter_validity;
use crate::arrays::varbinview::BinaryView;
use crate::arrays::varbinview::VarBinViewArrayExt;
use crate::buffer::BufferHandle;

pub fn filter_varbinview(array: &VarBinViewArray, mask: &Arc<MaskValues>) -> VarBinViewArray {
    let filtered_validity = filter_validity(array.varbinview_validity(), mask);

    let views = Buffer::<BinaryView>::from_byte_buffer(array.views_handle().as_host().clone());
    let filtered_views = buffer::filter_buffer(views, mask.as_ref());

    // Only layout/nullability checks run here; the subset of views and the same
    // validity mask preserve the existing invariants without allocating a table.
    array
        .with_views_and_validity(
            BufferHandle::new_host(filtered_views.into_byte_buffer()),
            array.dtype().clone(),
            filtered_validity,
        )
        .vortex_expect("filtered views preserve VarBinView layout and validity")
}

#[cfg(test)]
mod test {
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::VarBinViewArray;
    use crate::compute::conformance::filter::test_filter_conformance;

    #[test]
    fn test_filter_varbinview_conformance() {
        test_filter_conformance(
            &VarBinViewArray::from_iter_str(["one", "two", "three", "four", "five"]).into_array(),
            &mut array_session().create_execution_ctx(),
        );

        test_filter_conformance(
            &VarBinViewArray::from_iter_nullable_str([
                Some("one"),
                None,
                Some("three"),
                Some("four"),
                Some("five"),
            ])
            .into_array(),
            &mut array_session().create_execution_ctx(),
        );
    }
}
