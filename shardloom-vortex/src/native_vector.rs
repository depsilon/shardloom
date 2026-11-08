//! Versioned sparse and affine-quantized payloads over existing struct/list storage.
//! Admission preserves the supplied representation; it performs no vector arithmetic.

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
    error::{VortexResult, vortex_ensure, vortex_err},
    session::{VortexSession, registry::CachedId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Sparse,
    Quantized,
}

impl Kind {
    pub(crate) const ALL: [Self; 2] = [Self::Sparse, Self::Quantized];
    pub(crate) const fn id(self) -> &'static str {
        match self {
            Self::Sparse => "shardloom.sparse_vector.v1",
            Self::Quantized => "shardloom.quantized_vector.v1",
        }
    }
    pub(crate) fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.id() == id)
    }
}

#[path = "native_vector_metadata.rs"]
mod metadata;
pub(crate) use metadata::Metadata;
pub(crate) const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

// The two registered identities share only wire/ownership mechanics. Metadata
// and dtype validation still check the exact representation at every boundary.
macro_rules! vector_type {
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
                    "vector metadata identity mismatch"
                );
                Ok(metadata.as_str().as_bytes().to_vec())
            }
            fn deserialize_metadata(&self, raw: &[u8]) -> VortexResult<Metadata> {
                Metadata::receive($kind, raw, self.allocator.as_ref())
            }
            fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
                vortex_ensure!(
                    dtype.metadata().kind() == $kind,
                    "vector metadata identity mismatch"
                );
                validate_storage(dtype.storage_dtype(), dtype.metadata())
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
vector_type!(SparseVector, Kind::Sparse);
vector_type!(QuantizedVector, Kind::Quantized);

pub(crate) fn dtype(metadata: Metadata, storage: DType) -> VortexResult<DType> {
    Ok(DType::Extension(match metadata.kind() {
        Kind::Sparse => ExtDType::<SparseVector>::try_new(metadata, storage)?.erased(),
        Kind::Quantized => ExtDType::<QuantizedVector>::try_new(metadata, storage)?.erased(),
    }))
}

pub(crate) fn metadata(dtype: &DType) -> Option<&Metadata> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    extension
        .metadata_opt::<SparseVector>()
        .or_else(|| extension.metadata_opt::<QuantizedVector>())
}

pub(crate) fn storage(dtype: &DType) -> Option<&DType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    metadata(dtype)?;
    Some(extension.storage_dtype())
}

pub(crate) fn contains(dtype: &DType) -> bool {
    match dtype {
        DType::Extension(_) => storage(dtype).is_some(),
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
        DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
        _ => false,
    }
}

fn list_primitive(dtype: &DType) -> Option<PType> {
    let DType::List(child, _) = dtype else {
        return None;
    };
    if let DType::Primitive(ptype, _) = child.as_ref() {
        Some(*ptype)
    } else {
        None
    }
}

pub(crate) fn validate_storage(storage: &DType, metadata: &Metadata) -> VortexResult<()> {
    let DType::Struct(fields, _) = storage else {
        return Err(vortex_err!("vector storage requires a struct"));
    };
    let names = match metadata.kind() {
        Kind::Sparse => ["size", "indices", "values"],
        Kind::Quantized => ["codes", "scale", "zero_point"],
    };
    vortex_ensure!(
        fields.nfields() == 3
            && fields
                .names()
                .iter()
                .zip(names)
                .all(|(a, b)| a.as_ref() == b),
        "vector storage fields must be {names:?} in that order"
    );
    let mut children = fields.fields();
    let first = children.next().expect("three fields");
    let second = children.next().expect("three fields");
    let third = children.next().expect("three fields");
    match metadata.kind() {
        Kind::Sparse => {
            vortex_ensure!(
                matches!(first, DType::Primitive(PType::U64, _)),
                "sparse size requires UInt64"
            );
            vortex_ensure!(
                list_primitive(&second).is_some_and(PType::is_int),
                "sparse indices require an integer list"
            );
            vortex_ensure!(
                list_primitive(&third).is_some_and(PType::is_float),
                "sparse values require a Float16/Float32/Float64 list"
            );
        }
        Kind::Quantized => {
            let code = list_primitive(&first)
                .filter(|ptype| ptype.is_int())
                .ok_or_else(|| vortex_err!("quantized codes require an integer list"))?;
            let (scale, zero) = if metadata.block_size().is_some() {
                (list_primitive(&second), list_primitive(&third))
            } else {
                let primitive = |dtype: &DType| match dtype {
                    DType::Primitive(ptype, _) => Some(*ptype),
                    _ => None,
                };
                (primitive(&second), primitive(&third))
            };
            vortex_ensure!(
                scale.is_some_and(PType::is_float),
                "quantized scale requires Float16/Float32/Float64 with the declared parameter mode"
            );
            vortex_ensure!(
                zero == Some(code),
                "quantized zero points require the code primitive and declared parameter mode"
            );
        }
    }
    Ok(())
}

pub(crate) fn register_dtype(session: &VortexSession, kind: Kind) {
    use vortex::array::{dtype::session::DTypeSessionExt as _, memory::HostMemorySessionExt as _};
    let allocator = Some(session.host_allocator());
    match kind {
        Kind::Sparse => {
            session.dtypes().register(SparseVector { allocator });
        }
        Kind::Quantized => {
            session.dtypes().register(QuantizedVector { allocator });
        }
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
#[path = "native_vector_arrow.rs"]
pub(crate) mod arrow;
#[path = "native_vector_edition.rs"]
mod edition;
#[cfg(all(test, feature = "vortex-local-primitives"))]
#[path = "native_vector_tests.rs"]
mod tests;
