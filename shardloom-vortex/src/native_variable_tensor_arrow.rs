//! Canonical Arrow variable tensors; parent-aware value admission is separate.

use super::{Metadata, VariableShapeTensor};
use arrow_array::ArrayRef as ArrowArray;
use arrow_schema::{DataType, Field};
use std::{collections::HashMap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::{ExtensionArray, extension::ExtensionArrayExt as _},
        dtype::{
            DType,
            extension::{ExtDType, ExtVTable},
        },
    },
    arrow::{
        ArrowExport, ArrowExportOptions, ArrowExportVTable, ArrowImport, ArrowImportVTable,
        ArrowSession, ArrowSessionExt as _,
    },
    error::{VortexResult, vortex_ensure, vortex_err},
    session::{
        VortexSession,
        registry::{CachedId, Id},
    },
};

const NAME: &str = "ARROW:extension:name";
const META: &str = "ARROW:extension:metadata";

#[cfg(feature = "vortex-write")]
#[path = "native_variable_tensor_arrow_values.rs"]
mod values;
#[cfg(feature = "vortex-write")]
pub(crate) use values::validate_batch;

#[derive(Debug)]
struct TensorArrow;

pub(super) fn register(session: &VortexSession) {
    session.arrow().register_importer(Arc::new(TensorArrow));
    session.arrow().register_exporter(Arc::new(TensorArrow));
}

fn plain(field: &Field) -> bool {
    !field.metadata().contains_key(NAME) && !field.metadata().contains_key(META)
}

/// Allocation-free field preflight before metadata scratch is admitted.
pub(crate) fn admitted(field: &Field) -> bool {
    if field
        .metadata()
        .get(NAME)
        .is_none_or(|name| name != super::ARROW_ID)
        || field
            .metadata()
            .get(META)
            .is_some_and(|raw| raw.len() > super::MAX_METADATA_BYTES)
    {
        return false;
    }
    let DataType::Struct(fields) = field.data_type() else {
        return false;
    };
    if fields.len() != 2
        || fields[0].name() != "data"
        || fields[1].name() != "shape"
        || !fields.iter().all(|field| plain(field))
    {
        return false;
    }
    matches!(fields[0].data_type(), DataType::List(child) if plain(child)
        && crate::native_tensor::arrow::element(child.data_type(), child.is_nullable()).is_some())
        && matches!(fields[1].data_type(), DataType::FixedSizeList(child, size)
            if *size >= 0 && plain(child) && child.data_type() == &DataType::Int32)
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let DType::Extension(extension) = dtype else {
        return Err(vortex_err!("variable tensor requires extension dtype"));
    };
    let metadata = extension
        .metadata_opt::<VariableShapeTensor>()
        .ok_or_else(|| vortex_err!("variable tensor requires typed metadata"))?;
    i32::try_from(super::rank(extension.storage_dtype())?)
        .map_err(|_| vortex_err!("tensor rank exceeds Arrow's signed 32-bit list width"))?;
    let physical = ArrowSession::default().to_arrow_field(name, extension.storage_dtype())?;
    Ok(physical.with_metadata(HashMap::from([
        (NAME.into(), super::ARROW_ID.into()),
        (META.into(), metadata.as_str().into()),
    ])))
}

pub(crate) fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(
        admitted(field),
        "invalid Arrow variable tensor extension or storage"
    );
    let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
    let storage = ArrowSession::default().from_arrow_field(&physical)?;
    Ok(DType::Extension(
        ExtDType::<VariableShapeTensor>::try_new(
            Metadata::parse(field.metadata().get(META).map_or("", String::as_str))?,
            storage,
        )?
        .erased(),
    ))
}

fn arrow_id() -> Id {
    static ID: CachedId = CachedId::new(super::ARROW_ID);
    *ID
}

impl ArrowImportVTable for TensorArrow {
    fn arrow_ext_id(&self) -> Id {
        arrow_id()
    }
    fn from_arrow_field(&self, field: &Field, _: &ArrowSession) -> VortexResult<Option<DType>> {
        dtype(field).map(Some)
    }
    fn from_arrow_array(
        &self,
        array: ArrowArray,
        field: &Field,
        declared: &DType,
        session: &ArrowSession,
    ) -> VortexResult<ArrowImport> {
        vortex_ensure!(
            array.data_type() == field.data_type() && &dtype(field)? == declared,
            "Arrow variable tensor storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("variable tensor requires extension metadata"));
        };
        let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
        let storage = session.from_arrow_array(array, &physical)?;
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(extension.clone(), storage)?.into_array(),
        ))
    }
}

impl ArrowExportVTable for TensorArrow {
    fn arrow_ext_id(&self) -> Id {
        arrow_id()
    }
    fn vortex_id(&self) -> Id {
        VariableShapeTensor::default().id()
    }
    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        _: &ArrowSession,
    ) -> VortexResult<Option<Field>> {
        field(name, dtype).map(Some)
    }
    fn execute_arrow(
        &self,
        array: ArrayRef,
        target: &Field,
        options: &ArrowExportOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowExport> {
        vortex_ensure!(
            &dtype(target)? == array.dtype(),
            "variable tensor export requires matching logical metadata"
        );
        let extension = array.execute::<ExtensionArray>(ctx)?;
        let physical = Field::new(
            target.name(),
            target.data_type().clone(),
            target.is_nullable(),
        );
        let exported = ArrowSession::default().exporter(options).execute_arrow(
            extension.storage_array().clone(),
            Some(&physical),
            ctx,
        )?;
        Ok(ArrowExport::Exported(exported))
    }
}

#[cfg(all(test, feature = "vortex-local-primitives", feature = "vortex-write"))]
#[path = "native_variable_tensor_arrow_tests.rs"]
mod tests;
