//! Explicit `GeoArrow` carriage; execution stays on native coordinate arrays.

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
#[path = "native_spatial_arrow_values.rs"]
pub(crate) mod values;

#[derive(Debug)]
struct SpatialArrow(Kind);
pub(super) fn register(session: &VortexSession) {
    for kind in Kind::ALL {
        session
            .arrow()
            .register_importer(Arc::new(SpatialArrow(kind)));
        session
            .arrow()
            .register_exporter(Arc::new(SpatialArrow(kind)));
    }
}
fn id(kind: Kind) -> Id {
    static POINT: CachedId = CachedId::new(Kind::Point.id());
    static LINE: CachedId = CachedId::new(Kind::LineString.id());
    static POLYGON: CachedId = CachedId::new(Kind::Polygon.id());
    match kind {
        Kind::Point => *POINT,
        Kind::LineString => *LINE,
        Kind::Polygon => *POLYGON,
    }
}
pub(crate) fn kind(field: &Field) -> Option<Kind> {
    Kind::from_id(field.metadata().get(NAME)?)
}
fn plain(field: &Field) -> bool {
    !field.metadata().contains_key(NAME) && !field.metadata().contains_key(META)
}

/// Preflight before allocating a JSON parser or any coordinate buffer.
pub(crate) fn admitted(field: &Field) -> bool {
    let Some(kind) = kind(field) else {
        return false;
    };
    if field
        .metadata()
        .get(META)
        .is_some_and(|raw| raw.len() > super::MAX_METADATA_BYTES)
    {
        return false;
    }
    let mut data_type = field.data_type();
    for _ in 0..match kind {
        Kind::Point => 0,
        Kind::LineString => 1,
        Kind::Polygon => 2,
    } {
        match data_type {
            DataType::List(child) | DataType::LargeList(child) if plain(child) => {
                data_type = child.data_type();
            }
            _ => return false,
        }
    }
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    fields.len() == 2
        && fields[0].name() == "x"
        && fields[1].name() == "y"
        && fields
            .iter()
            .all(|field| field.data_type() == &DataType::Float64 && plain(field))
}

pub(crate) fn field(name: &str, dtype: &DType) -> VortexResult<Field> {
    let metadata =
        super::metadata(dtype).ok_or_else(|| vortex_err!("spatial metadata is absent"))?;
    let physical = ArrowSession::default()
        .to_arrow_field(name, super::storage(dtype).expect("typed spatial storage"))?;
    let mut fields = HashMap::from([(NAME.into(), metadata.kind().id().into())]);
    if metadata.as_str() != "{}" {
        fields.insert(META.into(), metadata.as_str().into());
    }
    Ok(physical.with_metadata(fields))
}
pub(crate) fn dtype(field: &Field) -> VortexResult<DType> {
    vortex_ensure!(
        admitted(field),
        "spatial Arrow storage requires 2D separated Float64 x/y coordinates and List/LargeList offsets"
    );
    let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
    super::dtype(
        Metadata::parse(
            kind(field).expect("admitted spatial field"),
            field.metadata().get(META).map_or("{}", String::as_str),
        )?,
        ArrowSession::default().from_arrow_field(&physical)?,
    )
}
impl ArrowImportVTable for SpatialArrow {
    fn arrow_ext_id(&self) -> Id {
        id(self.0)
    }
    fn from_arrow_field(&self, field: &Field, _: &ArrowSession) -> VortexResult<Option<DType>> {
        vortex_ensure!(
            kind(field) == Some(self.0),
            "spatial importer identity mismatch"
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
            "Arrow spatial storage or metadata mismatch"
        );
        let DType::Extension(extension) = declared else {
            return Err(vortex_err!("spatial dtype requires extension metadata"));
        };
        let physical = Field::new(field.name(), field.data_type().clone(), field.is_nullable());
        Ok(ArrowImport::Imported(
            ExtensionArray::try_new(
                extension.clone(),
                session.from_arrow_array(array, &physical)?,
            )?
            .into_array(),
        ))
    }
}
impl ArrowExportVTable for SpatialArrow {
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
            "spatial exporter identity mismatch"
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
            "spatial export requires matching logical metadata"
        );
        let extension = array.execute::<ExtensionArray>(ctx)?;
        let physical = Field::new(
            target.name(),
            target.data_type().clone(),
            target.is_nullable(),
        );
        Ok(ArrowExport::Exported(
            ArrowSession::default().exporter(options).execute_arrow(
                extension.storage_array().clone(),
                Some(&physical),
                ctx,
            )?,
        ))
    }
}
