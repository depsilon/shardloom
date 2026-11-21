// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;
use vortex_error::{VortexResult, vortex_bail};
use vortex_session::{VortexSession, registry::CachedId};

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable, ForeignExtDType};
use crate::dtype::{DType, MapDType, Nullability, StructFields, UnionVariants};
use crate::flatbuffers::{FlatBuffer, WriteFlatBuffer};
use crate::scalar::{Scalar, ScalarValue, UnionValue};

use Nullability::{NonNullable, Nullable};

pub(super) const KINDS: [&str; 8] = [
    "list",
    "fixed",
    "struct",
    "map-key",
    "map-value",
    "union",
    "variant",
    "extension",
];

pub(super) fn wrap(kind: &str, child: Scalar) -> Scalar {
    if kind == "variant" {
        return Scalar {
            value: Some(ScalarValue::Variant(Box::new(child))),
            dtype: DType::Variant(NonNullable),
        };
    }
    let (dtype, value) = child.into_parts();
    let (dtype, value) = match kind {
        "list" => (
            DType::List(Arc::new(dtype), NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "fixed" => (
            DType::FixedSizeList(Arc::new(dtype), 1, NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "struct" => (
            DType::Struct(
                StructFields::new(["child"].into(), vec![dtype]),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "map-key" => (
            DType::Map(
                MapDType::try_new(dtype, DType::Bool(Nullable), false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                value, None,
            ]))])),
        ),
        "map-value" => (
            DType::Map(
                MapDType::try_new(DType::Bool(NonNullable), dtype, false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                Some(ScalarValue::Bool(false)),
                value,
            ]))])),
        ),
        "union" => (
            DType::Union(
                UnionVariants::try_new(["first"].into(), vec![dtype], vec![7]).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Union(UnionValue::new(7, value))),
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.scalar_operations_foreign");
            (
                DType::Extension(ForeignExtDType::from_parts(*ID, vec![3], dtype).unwrap()),
                value,
            )
        }
        _ => panic!("unknown scalar operation shape"),
    };
    Scalar { value, dtype }
}

pub(super) fn lazy_struct(first: &DType) -> DType {
    use crate::dtype::flatbuffers as fb;
    let mut builder = FlatBufferBuilder::new();
    let good = first.write_flatbuffer(&mut builder).unwrap();
    let bad = fb::DType::create(&mut builder, &fb::DTypeArgs::default());
    let a = builder.create_string("first");
    let b = builder.create_string("bad");
    let names = builder.create_vector(&[a, b]);
    let dtypes = builder.create_vector(&[good, bad]);
    let fields = fb::Struct_::create(
        &mut builder,
        &fb::Struct_Args {
            names: Some(names),
            dtypes: Some(dtypes),
            nullable: false,
        },
    );
    let root = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type: fb::Type::Struct_,
            type_: Some(fields.as_union_value()),
        },
    );
    builder.finish_minimal(root);
    DType::from_flatbuffer(
        FlatBuffer::align_from(builder.finished_data().to_vec()),
        &VortexSession::empty(),
    )
    .unwrap()
}

pub(super) type Events = Arc<Mutex<Vec<u8>>>;

#[derive(Clone)]
pub(super) struct Metadata {
    id: u8,
    reject: bool,
    events: Events,
}
impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        (self.id, self.reject) == (other.id, other.reject)
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.id, self.reject).hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub(super) struct Probe;
impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_operations_probe");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("no metadata serialization")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("no metadata deserialization")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn validate_scalar_value(dtype: &ExtDType<Self>, _: &ScalarValue) -> VortexResult<()> {
        let metadata = dtype.metadata();
        metadata.events.lock().push(metadata.id);
        if metadata.reject {
            vortex_bail!("rejected probe {}", metadata.id);
        }
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        _: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        panic!("scalar initialization and inspection do not unpack native values")
    }
}

pub(super) fn probe_dtype(storage: DType, id: u8, reject: bool, events: &Events) -> DType {
    DType::Extension(
        ExtDType::<Probe>::try_new(
            Metadata {
                id,
                reject,
                events: Arc::clone(events),
            },
            storage,
        )
        .unwrap()
        .erased(),
    )
}

pub(super) fn probe(value: bool, id: u8, reject: bool, events: &Events) -> Scalar {
    Scalar {
        value: Some(ScalarValue::Bool(value)),
        dtype: probe_dtype(DType::Bool(NonNullable), id, reject, events),
    }
}
