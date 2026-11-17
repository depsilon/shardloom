//! `GeoArrow` identities over native coordinate structs and list offsets.
//!
//! These are payload declarations, not an alternate geometry object model.

use std::hash::{Hash, Hasher};
use vortex::{
    array::{
        dtype::{
            DType, PType,
            extension::{ExtDType, ExtId, ExtVTable},
        },
        memory::HostAllocatorRef,
        scalar::ScalarValue,
    },
    error::{VortexResult, vortex_ensure},
    session::{VortexSession, registry::CachedId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Point,
    LineString,
    Polygon,
}

impl Kind {
    pub(crate) const ALL: [Self; 3] = [Self::Point, Self::LineString, Self::Polygon];
    pub(crate) const fn id(self) -> &'static str {
        match self {
            Self::Point => "geoarrow.point",
            Self::LineString => "geoarrow.linestring",
            Self::Polygon => "geoarrow.polygon",
        }
    }
    pub(crate) fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.id() == id)
    }
}

#[path = "native_spatial_metadata.rs"]
mod metadata;
pub(crate) use metadata::Metadata;
pub(crate) const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;
#[path = "native_spatial_values.rs"]
pub(crate) mod values;

macro_rules! spatial_type {
    ($name:ident, $kind:expr) => {
        #[derive(Debug, Clone, Default)]
        pub(crate) struct $name {
            allocator: Option<HostAllocatorRef>,
        }
        impl PartialEq for $name {
            fn eq(&self, _: &Self) -> bool {
                true
            }
        }
        impl Eq for $name {}
        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                $kind.hash(state);
            }
        }
        impl ExtVTable for $name {
            type Metadata = Metadata;
            type NativeValue<'a> = &'a ScalarValue;
            fn id(&self) -> ExtId {
                static ID: CachedId = CachedId::new($kind.id());
                *ID
            }
            fn serialize_metadata(&self, metadata: &Metadata) -> VortexResult<Vec<u8>> {
                vortex_ensure!(
                    metadata.kind() == $kind,
                    "spatial metadata identity mismatch"
                );
                Ok(metadata.as_str().as_bytes().to_vec())
            }
            fn deserialize_metadata(&self, raw: &[u8]) -> VortexResult<Metadata> {
                Metadata::receive($kind, raw, self.allocator.as_ref())
            }
            fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
                vortex_ensure!(
                    dtype.metadata().kind() == $kind,
                    "spatial metadata identity mismatch"
                );
                validate_storage(dtype.storage_dtype(), $kind)
            }
            fn unpack_native<'a>(
                _: &'a ExtDType<Self>,
                value: &'a ScalarValue,
            ) -> VortexResult<&'a ScalarValue> {
                Ok(value)
            }
        }
    };
}
spatial_type!(Point, Kind::Point);
spatial_type!(LineString, Kind::LineString);
spatial_type!(Polygon, Kind::Polygon);

pub(crate) fn dtype(metadata: Metadata, storage: DType) -> VortexResult<DType> {
    Ok(DType::Extension(match metadata.kind() {
        Kind::Point => ExtDType::<Point>::try_new(metadata, storage)?.erased(),
        Kind::LineString => ExtDType::<LineString>::try_new(metadata, storage)?.erased(),
        Kind::Polygon => ExtDType::<Polygon>::try_new(metadata, storage)?.erased(),
    }))
}

pub(crate) fn metadata(dtype: &DType) -> Option<&Metadata> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension
        .metadata_opt::<Point>()
        .or_else(|| extension.metadata_opt::<LineString>())
        .or_else(|| extension.metadata_opt::<Polygon>())
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    metadata(dtype)?;
    Some(extension.storage_dtype())
}

pub(crate) fn validate_storage(storage: &DType, kind: Kind) -> VortexResult<()> {
    let mut coordinates = storage;
    for _ in 0..match kind {
        Kind::Point => 0,
        Kind::LineString => 1,
        Kind::Polygon => 2,
    } {
        let DType::List(child, _) = coordinates else {
            return Err(vortex::error::vortex_err!(
                "spatial storage requires native list offsets"
            ));
        };
        coordinates = child;
    }
    let DType::Struct(fields, _) = coordinates else {
        return Err(vortex::error::vortex_err!(
            "spatial coordinates require separated x/y Float64 fields"
        ));
    };
    vortex_ensure!(
        fields.nfields() == 2
            && fields.names()[0].as_ref() == "x"
            && fields.names()[1].as_ref() == "y"
            && fields
                .fields()
                .all(|field| matches!(field, DType::Primitive(PType::F64, _))),
        "spatial coordinates require separated x/y Float64 fields in that order; only 2D is admitted"
    );
    Ok(())
}

pub(crate) fn register_dtype(session: &VortexSession, kind: Kind) {
    use vortex::array::{dtype::session::DTypeSessionExt as _, memory::HostMemorySessionExt as _};
    let allocator = Some(session.host_allocator());
    match kind {
        Kind::Point => session.dtypes().register(Point { allocator }),
        Kind::LineString => session.dtypes().register(LineString { allocator }),
        Kind::Polygon => session.dtypes().register(Polygon { allocator }),
    }
}

pub(crate) fn register(session: &VortexSession) {
    for kind in Kind::ALL {
        register_dtype(session, kind);
    }
    edition::register(session);
    #[cfg(feature = "universal-format-io")]
    arrow::register(session);
}

#[cfg(feature = "universal-format-io")]
#[path = "native_spatial_arrow.rs"]
pub(crate) mod arrow;

#[path = "native_spatial_edition.rs"]
mod edition;
#[cfg(test)]
#[path = "native_spatial_tests.rs"]
mod tests;
