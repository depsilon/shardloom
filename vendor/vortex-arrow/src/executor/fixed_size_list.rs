// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_schema::FieldRef;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::FixedSizeList;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::fixed_size_list::FixedSizeListArrayExt;
use vortex_array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;

use crate::executor::ExportStep;
use crate::executor::ExportTask;
use crate::executor::validity::to_arrow_null_buffer;

pub(super) struct FixedSizeListExportFrame {
    list_size: i32,
    elements_field: FieldRef,
    null_buffer: Option<arrow_buffer::NullBuffer>,
    len: usize,
}

pub(super) fn start_arrow_fixed_list(
    array: ArrayRef,
    list_size: i32,
    elements_field: &FieldRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    vortex_ensure!(
        matches!(array.dtype(), DType::FixedSizeList(..)),
        "Cannot convert Vortex array with dtype {} to an Arrow FixedSizeList array",
        array.dtype()
    );

    // Check for Vortex FixedSizeListArray and convert directly.
    let fixed_size_list = if let Some(array) = array.as_opt::<FixedSizeList>() {
        array.into_owned()
    } else {
        // Otherwise, we execute the array to become a FixedSizeListArray.
        array.execute::<FixedSizeListArray>(ctx)?
    };

    vortex_ensure!(
        Ok(list_size) == i32::try_from(fixed_size_list.list_size()),
        "Cannot convert FixedSizeList with list size {} to Arrow array with list size {}",
        fixed_size_list.list_size(),
        list_size
    );

    let elements = fixed_size_list.elements().clone();
    let null_buffer = to_arrow_null_buffer(fixed_size_list.validity()?, fixed_size_list.len(), ctx)?;
    let len = fixed_size_list.len();
    Ok(ExportStep::Descend {
        frame: FixedSizeListExportFrame {
            list_size,
            elements_field: FieldRef::clone(elements_field),
            null_buffer,
            len,
        }
        .into(),
        task: ExportTask::WithField {
            array: elements,
            target: Some(elements_field.as_ref().clone()),
        },
    })
}

pub(super) fn finish_arrow_fixed_list(
    frame: FixedSizeListExportFrame,
    elements: arrow_array::ArrayRef,
) -> VortexResult<arrow_array::ArrayRef> {
    vortex_ensure!(
        frame.elements_field.is_nullable() || elements.null_count() == 0,
        "Cannot convert FixedSizeListArray to non-nullable Arrow array when elements are nullable"
    );

    Ok(Arc::new(
        arrow_array::FixedSizeListArray::try_new_with_length(
            frame.elements_field,
            frame.list_size,
            elements,
            frame.null_buffer,
            frame.len,
        )?,
    ))
}
