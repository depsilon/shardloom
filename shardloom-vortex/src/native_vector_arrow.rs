//! Arrow carriage for the two versioned native vector representations.

use super::{Kind, Metadata};
use arrow_array::ArrayRef as ArrowArray;
use arrow_schema::{DataType, Field};
use std::{collections::HashMap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray,
        arrays::{ExtensionArray, extension::ExtensionArrayExt as _},
        dtype::DType,
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
#[path = "native_vector_arrow_values.rs"]
pub(crate) mod values;

#[cfg(all(test, feature = "vortex-local-primitives", feature = "vortex-write"))]
#[path = "native_vector_arrow_tests.rs"]
mod tests;

#[derive(Debug)]
struct VectorArrow(Kind);
pub(super) fn register(session: &VortexSession) {
    for kind in Kind::ALL {
        session
            .arrow()
            .register_importer(Arc::new(VectorArrow(kind)));
        session
            .arrow()
            .register_exporter(Arc::new(VectorArrow(kind)));
    }
}
fn id(kind: Kind) -> Id {
    static SPARSE: CachedId = CachedId::new(Kind::Sparse.id());
    static QUANTIZED: CachedId = CachedId::new(Kind::Quantized.id());
    match kind {
        Kind::Sparse => *SPARSE,
        Kind::Quantized => *QUANTIZED,
    }
}
fn plain(field: &Field) -> bool {
    !field.metadata().contains_key(NAME) && !field.metadata().contains_key(META)
}
fn integer(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}
fn float(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}
fn list(dtype: &DataType) -> Option<&DataType> {
    match dtype {
        DataType::List(child) | DataType::LargeList(child) if plain(child) => {
            Some(child.data_type())
        }
        _ => None,
    }
}
pub(crate) fn kind(field: &Field) -> Option<Kind> {
    Kind::from_id(field.metadata().get(NAME)?)
}

/// Structural preflight allocates no metadata parser or payload buffers.
pub(crate) fn admitted(field: &Field) -> bool {
    let Some(kind) = kind(field) else {
        return false;
    };
    if field
        .metadata()
        .get(META)
        .is_none_or(|raw| raw.len() > super::MAX_METADATA_BYTES)
    {
        return false;
    }
    let DataType::Struct(fields) = field.data_type() else {
        return false;
    };
    let names = match kind {
        Kind::Sparse => ["size", "indices", "values"],
        Kind::Quantized => ["codes", "scale", "zero_point"],
    };
    if fields.len() != 3
        || fields
            .iter()
            .zip(names)
            .any(|(field, name)| field.name() != name || !plain(field))
    {
        return false;
    }
    match kind {
        Kind::Sparse => {
            fields[0].data_type() == &DataType::UInt64
                && list(fields[1].data_type()).is_some_and(integer)
                && list(fields[2].data_type()).is_some_and(float)
        }
        Kind::Quantized => {
            let Some(code) = list(fields[0].data_type()).filter(|dtype| integer(dtype)) else {
                return false;
            };
            let scale = fields[1].data_type();
            let zero = fields[2].data_type();
            (float(scale) && zero == code)
                || (list(scale).is_some_and(float) && list(zero) == Some(code))
        }
    }
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let metadata =
        super::metadata(dtype).ok_or_else(|| vortex_err!("vector requires typed metadata"))?;
    let storage = super::storage(dtype).expect("typed vector");
    let physical = ArrowSession::default().to_arrow_field(name, storage)?;
    Ok(physical.with_metadata(HashMap::from([
        (NAME.into(), metadata.kind().id().into()),
        (META.into(), metadata.as_str().into()),
    ])))
}

pub(crate) fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(admitted(field), "invalid Arrow vector extension or storage");
    let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
    let storage = ArrowSession::default().from_arrow_field(&physical)?;
    super::dtype(
        Metadata::parse(
            kind(field).expect("admitted vector"),
            &field.metadata()[META],
        )?,
        storage,
    )
}

impl ArrowImportVTable for VectorArrow {
    fn arrow_ext_id(&self) -> Id {
        id(self.0)
    }
    fn from_arrow_field(&self, field: &Field, _: &ArrowSession) -> VortexResult<Option<DType>> {
        vortex_ensure!(
            kind(field) == Some(self.0),
            "vector importer identity mismatch"
        );
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
            kind(field) == Some(self.0)
                && array.data_type() == field.data_type()
                && &dtype(field)? == declared,
            "Arrow vector storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("vector requires extension metadata"));
        };
        let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
        let storage = session.from_arrow_array(array, &physical)?;
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(extension.clone(), storage)?.into_array(),
        ))
    }
}
impl ArrowExportVTable for VectorArrow {
    fn arrow_ext_id(&self) -> Id {
        id(self.0)
    }
    fn vortex_id(&self) -> Id {
        id(self.0)
    }
    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        _: &ArrowSession,
    ) -> VortexResult<Option<Field>> {
        vortex_ensure!(
            super::metadata(dtype).is_some_and(|m| m.kind() == self.0),
            "vector exporter identity mismatch"
        );
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
            kind(target) == Some(self.0) && &dtype(target)? == array.dtype(),
            "vector export requires matching logical metadata"
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
