// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Original generated verification from 87832b4cc, with recursive marker types
// renamed so tests cannot silently re-enter the new iterative verifier.

use super::super::fb;
use fb::{Binary, Bool, Decimal, Null, Primitive, Type, Utf8, Variant};
use flatbuffers::{ForwardsUOffset, InvalidFlatbuffer, Verifiable, Verifier};

struct OriginalStruct;
impl ::flatbuffers::Verifiable for OriginalStruct {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<
                ::flatbuffers::Vector<'_, ::flatbuffers::ForwardsUOffset<&'_ str>>,
            >>("names", fb::Struct_::VT_NAMES, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<
                ::flatbuffers::Vector<'_, ::flatbuffers::ForwardsUOffset<OriginalDType>>,
            >>("dtypes", fb::Struct_::VT_DTYPES, false)?
            .visit_field::<bool>("nullable", fb::Struct_::VT_NULLABLE, false)?
            .finish();
        Ok(())
    }
}

struct OriginalList;
impl ::flatbuffers::Verifiable for OriginalList {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalDType>>(
                "element_type",
                fb::List::VT_ELEMENT_TYPE,
                false,
            )?
            .visit_field::<bool>("nullable", fb::List::VT_NULLABLE, false)?
            .finish();
        Ok(())
    }
}

struct OriginalFixedSizeList;
impl ::flatbuffers::Verifiable for OriginalFixedSizeList {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalDType>>(
                "element_type",
                fb::FixedSizeList::VT_ELEMENT_TYPE,
                false,
            )?
            .visit_field::<u32>("size", fb::FixedSizeList::VT_SIZE, false)?
            .visit_field::<bool>("nullable", fb::FixedSizeList::VT_NULLABLE, false)?
            .finish();
        Ok(())
    }
}

struct OriginalExtension;
impl ::flatbuffers::Verifiable for OriginalExtension {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<&str>>("id", fb::Extension::VT_ID, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalDType>>(
                "storage_dtype",
                fb::Extension::VT_STORAGE_DTYPE,
                false,
            )?
            .visit_field::<::flatbuffers::ForwardsUOffset<::flatbuffers::Vector<'_, u8>>>(
                "metadata",
                fb::Extension::VT_METADATA,
                false,
            )?
            .finish();
        Ok(())
    }
}

struct OriginalUnion;
impl ::flatbuffers::Verifiable for OriginalUnion {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<
                ::flatbuffers::Vector<'_, ::flatbuffers::ForwardsUOffset<&'_ str>>,
            >>("names", fb::Union::VT_NAMES, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<
                ::flatbuffers::Vector<'_, ::flatbuffers::ForwardsUOffset<OriginalDType>>,
            >>("dtypes", fb::Union::VT_DTYPES, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<::flatbuffers::Vector<'_, i8>>>(
                "type_ids",
                fb::Union::VT_TYPE_IDS,
                false,
            )?
            .visit_field::<bool>("nullable", fb::Union::VT_NULLABLE, false)?
            .finish();
        Ok(())
    }
}

struct OriginalMap;
impl ::flatbuffers::Verifiable for OriginalMap {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalDType>>(
                "key_type",
                fb::Map::VT_KEY_TYPE,
                false,
            )?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalDType>>(
                "value_type",
                fb::Map::VT_VALUE_TYPE,
                false,
            )?
            .visit_field::<bool>("keys_sorted", fb::Map::VT_KEYS_SORTED, false)?
            .visit_field::<bool>("nullable", fb::Map::VT_NULLABLE, false)?
            .finish();
        Ok(())
    }
}

struct OriginalDType;
impl ::flatbuffers::Verifiable for OriginalDType {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
     .visit_union::<Type, _>("type_type", fb::DType::VT_TYPE_TYPE, "type_", fb::DType::VT_TYPE_, false, |key, v, pos| {
        match key {
          Type::Null => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Null>>("Type::Null", pos),
          Type::Bool => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Bool>>("Type::Bool", pos),
          Type::Primitive => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Primitive>>("Type::Primitive", pos),
          Type::Decimal => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Decimal>>("Type::Decimal", pos),
          Type::Utf8 => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Utf8>>("Type::Utf8", pos),
          Type::Binary => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Binary>>("Type::Binary", pos),
          Type::Struct_ => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalStruct>>("Type::Struct_", pos),
          Type::List => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalList>>("Type::List", pos),
          Type::Extension => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalExtension>>("Type::Extension", pos),
          Type::FixedSizeList => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalFixedSizeList>>("Type::FixedSizeList", pos),
          Type::Variant => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<Variant>>("Type::Variant", pos),
          Type::Union => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalUnion>>("Type::Union", pos),
          Type::Map => v.verify_union_variant::<::flatbuffers::ForwardsUOffset<OriginalMap>>("Type::Map", pos),
          _ => Ok(()),
        }
     })?
     .finish();
        Ok(())
    }
}

pub(super) fn verify_root(verifier: &mut Verifier) -> Result<(), InvalidFlatbuffer> {
    <ForwardsUOffset<OriginalDType>>::run_verifier(verifier, 0)
}

// A vector alignment error embeds Rust's marker type name. The reference uses
// a distinct marker only to keep recursive dispatch independent of the change.
// Normalize that spelling alone; all positions, traces and error kinds remain
// part of the exact comparison.
pub(super) fn normalize_marker_alignment(mut error: InvalidFlatbuffer) -> InvalidFlatbuffer {
    if let InvalidFlatbuffer::Unaligned { unaligned_type, .. } = &mut error
        && unaligned_type.as_ref() == std::any::type_name::<ForwardsUOffset<OriginalDType>>()
    {
        *unaligned_type =
            std::borrow::Cow::Borrowed(std::any::type_name::<ForwardsUOffset<fb::DType<'_>>>());
    }
    error
}
