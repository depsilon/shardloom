// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-11-09: admit lazy struct metadata containers
// through the session host allocator before creating names or field caches.
// Modified on 2026-11-17: read and write native schemas with ordered continuations.

use flatbuffers::FlatBufferBuilder;
use flatbuffers::Follow;
use flatbuffers::WIPOffset;
use flatbuffers::root;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::dtype::DType;
use crate::dtype::FieldDType;
use crate::dtype::FieldName;
use crate::dtype::FieldNames;
use crate::dtype::PType;
use crate::dtype::StructFields;
use crate::dtype::UnionVariants;
use crate::dtype::flatbuffers as fb;
use crate::flatbuffers::FlatBuffer;
use crate::flatbuffers::FlatBufferRoot;
use crate::flatbuffers::WriteFlatBuffer;
use crate::flatbuffers::dtype as fbd;
use crate::memory::HostMemorySessionExt;

/// A lazily evaluated DType, parsed on access from an underlying flatbuffer.
#[derive(Debug, Clone)]
pub(crate) struct ViewedDType {
    /// Underlying flatbuffer
    flatbuffer: FlatBuffer,
    /// Location of the dtype data inside the underlying buffer
    flatbuffer_loc: usize,
    /// The Vortex session used to resolve extensions
    session: VortexSession,
}

impl ViewedDType {
    /// Create a [`ViewedDType`] from a buffer and a flatbuffer location
    fn from_fb_loc(location: usize, buffer: FlatBuffer, session: VortexSession) -> Self {
        Self {
            flatbuffer: buffer,
            flatbuffer_loc: location,
            session,
        }
    }

    /// The viewed [`fbd::DType`] instance.
    fn flatbuffer(&self) -> fbd::DType<'_> {
        unsafe { fbd::DType::follow(self.flatbuffer.as_ref(), self.flatbuffer_loc) }
    }

    /// Returns the underlying shared buffer
    fn buffer(&self) -> &FlatBuffer {
        &self.flatbuffer
    }
}

impl StructFields {
    /// Creates a new instance from a flatbuffer-defined object and its underlying buffer.
    fn from_fb(
        fb_struct: fbd::Struct_<'_>,
        buffer: FlatBuffer,
        session: VortexSession,
    ) -> VortexResult<Self> {
        let source_names = fb_struct
            .names()
            .ok_or_else(|| vortex_err!("failed to parse struct names from flatbuffer"))?;
        let source_dtypes = fb_struct
            .dtypes()
            .ok_or_else(|| vortex_err!("failed to parse struct dtypes from flatbuffer"))?;
        if source_names.len() != source_dtypes.len() {
            vortex_bail!(
                "length mismatch between struct names ({}) and dtypes ({})",
                source_names.len(),
                source_dtypes.len()
            );
        }
        // Cover Vec-to-Arc overlap, each lazy cache/control block, the eventual
        // field lookup table and copied names. Child structs reserve separately
        // on first touch. This is container accounting, not process RSS.
        let per_field = size_of::<FieldDType>()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(size_of::<FieldName>() * 2))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(|| vortex_err!("struct metadata size overflow"))?;
        let bytes = source_names.iter().try_fold(
            source_names
                .len()
                .checked_mul(per_field)
                .and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(|| vortex_err!("struct metadata size overflow"))?,
            |bytes, name| {
                name.len()
                    .checked_mul(2)
                    .and_then(|name_bytes| bytes.checked_add(name_bytes))
                    .ok_or_else(|| vortex_err!("struct metadata size overflow"))
            },
        )?;
        let metadata = session.host_allocator().reserve_external(bytes)?;
        let mut names = Vec::new();
        names
            .try_reserve_exact(source_names.len())
            .map_err(|error| vortex_err!("struct name allocation failed: {error}"))?;
        let mut dtypes = Vec::new();
        dtypes
            .try_reserve_exact(source_dtypes.len())
            .map_err(|error| vortex_err!("struct field allocation failed: {error}"))?;
        names.extend(source_names.iter().map(FieldName::from));
        dtypes.extend(source_dtypes.iter().map(|dt| {
            FieldDType::from(ViewedDType::from_fb_loc(
                dt._tab.loc(),
                buffer.clone(),
                session.clone(),
            ))
        }));
        Ok(StructFields::from_reserved_fields(
            FieldNames::from(names),
            dtypes,
            metadata,
        ))
    }
}

impl UnionVariants {
    /// Creates a new instance from a flatbuffer-defined object and its underlying buffer.
    fn from_fb(
        fb_union: fbd::Union<'_>,
        buffer: FlatBuffer,
        session: VortexSession,
    ) -> VortexResult<Self> {
        let names = fb_union
            .names()
            .ok_or_else(|| vortex_err!("failed to parse union names from flatbuffer"))?
            .iter()
            .collect();

        let dtypes = fb_union
            .dtypes()
            .ok_or_else(|| vortex_err!("failed to parse union dtypes from flatbuffer"))?
            .iter()
            .map(|dt| {
                FieldDType::from(ViewedDType::from_fb_loc(
                    dt._tab.loc(),
                    buffer.clone(),
                    session.clone(),
                ))
            })
            .collect::<Vec<_>>();

        let type_ids: Vec<u8> = fb_union
            .type_ids()
            .ok_or_else(|| vortex_err!("failed to parse union type_ids from flatbuffer"))?
            .iter()
            .map(i8::cast_unsigned)
            .collect();

        UnionVariants::try_from_fields(names, dtypes, type_ids)
    }
}

impl DType {
    /// Create a [`DType`] from a flatbuffer buffer.
    pub fn from_flatbuffer(buffer: FlatBuffer, session: &VortexSession) -> VortexResult<Self> {
        let fb_loc = {
            let fb_dtype = root::<fbd::DType>(&buffer)?;
            fb_dtype._tab.loc()
        };
        let view = ViewedDType::from_fb_loc(fb_loc, buffer, session.clone());
        Self::try_from(view)
    }
}

#[path = "flatbuffers_read.rs"]
mod read;

impl TryFrom<ViewedDType> for DType {
    type Error = VortexError;

    fn try_from(view: ViewedDType) -> Result<Self, Self::Error> {
        read::read_dtype(view)
    }
}

impl FlatBufferRoot for DType {}

#[path = "flatbuffers_write.rs"]
mod write;

impl WriteFlatBuffer for DType {
    type Target<'a> = fb::DType<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        write::write_dtype(self, fbb)
    }
}

impl From<PType> for fb::PType {
    fn from(value: PType) -> Self {
        match value {
            PType::U8 => Self::U8,
            PType::U16 => Self::U16,
            PType::U32 => Self::U32,
            PType::U64 => Self::U64,
            PType::I8 => Self::I8,
            PType::I16 => Self::I16,
            PType::I32 => Self::I32,
            PType::I64 => Self::I64,
            PType::F16 => Self::F16,
            PType::F32 => Self::F32,
            PType::F64 => Self::F64,
        }
    }
}

impl TryFrom<fb::PType> for PType {
    type Error = VortexError;

    fn try_from(value: fb::PType) -> Result<Self, Self::Error> {
        Ok(match value {
            fb::PType::U8 => Self::U8,
            fb::PType::U16 => Self::U16,
            fb::PType::U32 => Self::U32,
            fb::PType::U64 => Self::U64,
            fb::PType::I8 => Self::I8,
            fb::PType::I16 => Self::I16,
            fb::PType::I32 => Self::I32,
            fb::PType::I64 => Self::I64,
            fb::PType::F16 => Self::F16,
            fb::PType::F32 => Self::F32,
            fb::PType::F64 => Self::F64,
            _ => vortex_bail!(
                "flatbuffer Primitive ptype must be a known PType, got {}",
                value.0
            ),
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use flatbuffers::FlatBufferBuilder;
    use flatbuffers::root;
    use vortex_buffer::ByteBuffer;

    use crate::dtype::DType;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::dtype::UnionVariants;
    use crate::dtype::flatbuffers as fb;
    use crate::dtype::nullability::Nullability;
    use crate::dtype::serde::flatbuffers::ViewedDType;
    use crate::dtype::test::SESSION;
    use crate::flatbuffers::FlatBuffer;
    use crate::flatbuffers::WriteFlatBuffer;
    use crate::flatbuffers::WriteFlatBufferExt;

    fn roundtrip_dtype(dtype: DType) {
        let bytes = dtype.write_flatbuffer_bytes().unwrap();
        let root_fb = root::<fb::DType>(&bytes).unwrap();
        let view = ViewedDType::from_fb_loc(
            root_fb._tab.loc(),
            FlatBuffer::from(bytes.clone()),
            SESSION.clone(),
        );

        let deserialized = DType::try_from(view).unwrap();
        assert_eq!(dtype, deserialized);
    }

    #[test]
    fn roundtrip() {
        roundtrip_dtype(DType::Null);
        roundtrip_dtype(DType::Bool(Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U8, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U16, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U32, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U64, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::I8, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::I16, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::I32, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::I64, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::F16, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::F32, Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::F64, Nullability::NonNullable));
        roundtrip_dtype(DType::Binary(Nullability::NonNullable));
        roundtrip_dtype(DType::Utf8(Nullability::NonNullable));
        roundtrip_dtype(DType::List(
            Arc::new(DType::Primitive(PType::F32, Nullability::Nullable)),
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::FixedSizeList(
            Arc::new(DType::Primitive(PType::F32, Nullability::Nullable)),
            2,
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Struct(
            StructFields::new(
                ["strings", "ints"].into(),
                vec![
                    DType::Utf8(Nullability::NonNullable),
                    DType::Primitive(PType::U16, Nullability::Nullable),
                ],
            ),
            Nullability::NonNullable,
        ));
        let inner_map = DType::map(
            DType::Primitive(PType::I32, Nullability::NonNullable),
            DType::Utf8(Nullability::Nullable),
            true,
            Nullability::NonNullable,
        )
        .unwrap();
        let map = DType::map(
            inner_map,
            DType::Utf8(Nullability::Nullable),
            false,
            Nullability::Nullable,
        )
        .unwrap();
        roundtrip_dtype(DType::struct_([("map", map)], Nullability::NonNullable));
        roundtrip_dtype(DType::Variant(Nullability::Nullable));
    }

    fn make_basic_union() -> DType {
        DType::Union(
            UnionVariants::new(
                ["int", "str"].into(),
                vec![
                    DType::Primitive(PType::I32, Nullability::NonNullable),
                    DType::Utf8(Nullability::NonNullable),
                ],
            )
            .unwrap(),
            Nullability::NonNullable,
        )
    }

    #[test]
    fn test_union_round_trip_flatbuffer() {
        roundtrip_dtype(make_basic_union());
    }

    #[test]
    fn test_union_round_trip_flatbuffer_with_nullable_variant() {
        let dtype = DType::Union(
            UnionVariants::new(
                ["null_variant", "str"].into(),
                vec![DType::Null, DType::Utf8(Nullability::NonNullable)],
            )
            .unwrap(),
            Nullability::NonNullable,
        );
        roundtrip_dtype(dtype);
    }

    #[test]
    fn test_union_round_trip_flatbuffer_with_type_id_indirection() {
        let dtype = DType::Union(
            UnionVariants::try_new(
                ["a", "b", "c"].into(),
                vec![
                    DType::Primitive(PType::I32, Nullability::NonNullable),
                    DType::Utf8(Nullability::NonNullable),
                    DType::Bool(Nullability::NonNullable),
                ],
                vec![0, 5, u8::MAX],
            )
            .unwrap(),
            Nullability::Nullable,
        );

        let bytes = dtype.write_flatbuffer_bytes().unwrap();
        let root_fb = root::<fb::DType>(&bytes).unwrap();
        let view = ViewedDType::from_fb_loc(
            root_fb._tab.loc(),
            FlatBuffer::from(bytes.clone()),
            SESSION.clone(),
        );

        let deserialized = DType::try_from(view).unwrap();
        assert_eq!(dtype, deserialized);
        let DType::Union(uv, _) = &deserialized else {
            panic!("Expected Union");
        };
        assert_eq!(uv.type_ids(), &[0, 5, u8::MAX]);
    }

    #[test]
    fn test_nested_union_round_trip_flatbuffer() {
        let inner_union = make_basic_union();
        let struct_with_union = DType::Struct(
            StructFields::from_iter([
                ("id", DType::Primitive(PType::I64, Nullability::NonNullable)),
                ("inner", inner_union),
            ]),
            Nullability::NonNullable,
        );

        let outer_union = DType::Union(
            UnionVariants::new(
                ["plain", "nested"].into(),
                vec![DType::Utf8(Nullability::NonNullable), struct_with_union],
            )
            .unwrap(),
            Nullability::Nullable,
        );

        roundtrip_dtype(outer_union);
    }

    /// A `UnionVariants` parsed lazily from a flatbuffer must compare equal to its eager
    /// counterpart.
    #[test]
    fn test_union_viewed_dtype_equality() {
        let eager = make_basic_union();

        let bytes = eager.write_flatbuffer_bytes().unwrap();
        let root_fb = root::<fb::DType>(&bytes).unwrap();
        let view = ViewedDType::from_fb_loc(
            root_fb._tab.loc(),
            FlatBuffer::from(bytes.clone()),
            SESSION.clone(),
        );

        let viewed = DType::try_from(view).unwrap();
        assert_eq!(eager, viewed);
        assert_eq!(viewed, eager);
    }

    #[test]
    fn test_struct_malformed_flatbuffer() {
        let mut fbb = FlatBufferBuilder::new();
        let names = fbb.create_vector::<flatbuffers::WIPOffset<&str>>(&[]);
        let dtype_offsets = (0..3)
            .map(|_| {
                DType::Primitive(PType::I32, Nullability::NonNullable)
                    .write_flatbuffer(&mut fbb)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let dtypes = fbb.create_vector(&dtype_offsets);

        let struct_table = fb::Struct_::create(
            &mut fbb,
            &fb::Struct_Args {
                names: Some(names),
                dtypes: Some(dtypes),
                nullable: false,
            },
        );

        let dtype = fb::DType::create(
            &mut fbb,
            &fb::DTypeArgs {
                type_type: fb::Type::Struct_,
                type_: Some(struct_table.as_union_value()),
            },
        );
        fbb.finish_minimal(dtype);
        let (vec, start) = fbb.collapse();
        let end = vec.len();
        let buffer = FlatBuffer::align_from(ByteBuffer::from(vec).slice(start..end));

        let root_fb = root::<fb::DType>(&buffer).unwrap();
        let view = ViewedDType::from_fb_loc(root_fb._tab.loc(), buffer, SESSION.clone());

        let result = DType::try_from(view);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length mismatch"));
    }

    /// A malformed flatbuffer (here, `dtypes.len() != type_ids.len()`) must round-trip
    /// to `Err`, not panic.
    #[test]
    fn test_union_malformed_flatbuffer_errors() {
        use flatbuffers::FlatBufferBuilder;
        use vortex_buffer::ByteBuffer;

        use crate::flatbuffers::WriteFlatBuffer;

        let mut fbb = FlatBufferBuilder::new();

        // Build a Union with 2 names + 2 dtypes but only 1 type_id.
        let name_a = fbb.create_string("a");
        let name_b = fbb.create_string("b");
        let names = fbb.create_vector(&[name_a, name_b]);

        let dtype_a = DType::Primitive(PType::I32, Nullability::NonNullable)
            .write_flatbuffer(&mut fbb)
            .unwrap();
        let dtype_b = DType::Utf8(Nullability::NonNullable)
            .write_flatbuffer(&mut fbb)
            .unwrap();
        let dtypes = fbb.create_vector(&[dtype_a, dtype_b]);

        let type_ids = fbb.create_vector::<i8>(&[0]);

        let union_table = fb::Union::create(
            &mut fbb,
            &fb::UnionArgs {
                names: Some(names),
                dtypes: Some(dtypes),
                type_ids: Some(type_ids),
                nullable: false,
            },
        );

        let dtype = fb::DType::create(
            &mut fbb,
            &fb::DTypeArgs {
                type_type: fb::Type::Union,
                type_: Some(union_table.as_union_value()),
            },
        );
        fbb.finish_minimal(dtype);
        let (vec, start) = fbb.collapse();
        let end = vec.len();
        let buffer = FlatBuffer::align_from(ByteBuffer::from(vec).slice(start..end));

        let root_fb = root::<fb::DType>(&buffer).unwrap();
        let view = ViewedDType::from_fb_loc(root_fb._tab.loc(), buffer, SESSION.clone());

        let result = DType::try_from(view);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length mismatch"));
    }

    #[test]
    fn test_unknown_ptype_from_flatbuffer() {
        let result = PType::try_from(fb::PType(11));
        assert!(result.is_err_and(|err| {
            err.to_string()
                .contains("flatbuffer Primitive ptype must be a known PType, got 11")
        }));
    }
}
