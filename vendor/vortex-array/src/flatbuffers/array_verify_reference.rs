// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Original generated verification from 6d84c2749; recursive marker types are
// renamed so the reference cannot re-enter the iterative verifier.

use super::super::fb;
use flatbuffers::{ForwardsUOffset, InvalidFlatbuffer, Verifiable, Verifier};

struct OriginalArrayNode;
impl ::flatbuffers::Verifiable for OriginalArrayNode {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<u16>("encoding", fb::ArrayNode::VT_ENCODING, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<::flatbuffers::Vector<'_, u8>>>(
                "metadata",
                fb::ArrayNode::VT_METADATA,
                false,
            )?
            .visit_field::<::flatbuffers::ForwardsUOffset<
                ::flatbuffers::Vector<'_, ::flatbuffers::ForwardsUOffset<OriginalArrayNode>>,
            >>("children", fb::ArrayNode::VT_CHILDREN, false)?
            .visit_field::<::flatbuffers::ForwardsUOffset<::flatbuffers::Vector<'_, u16>>>(
                "buffers",
                fb::ArrayNode::VT_BUFFERS,
                false,
            )?
            .visit_field::<::flatbuffers::ForwardsUOffset<fb::ArrayStats>>(
                "stats",
                fb::ArrayNode::VT_STATS,
                false,
            )?
            .finish();
        Ok(())
    }
}

struct OriginalArray;
impl ::flatbuffers::Verifiable for OriginalArray {
    #[inline]
    fn run_verifier(
        v: &mut ::flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), ::flatbuffers::InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<::flatbuffers::ForwardsUOffset<OriginalArrayNode>>(
                "root",
                fb::Array::VT_ROOT,
                false,
            )?
            .visit_field::<::flatbuffers::ForwardsUOffset<::flatbuffers::Vector<'_, fb::Buffer>>>(
                "buffers",
                fb::Array::VT_BUFFERS,
                false,
            )?
            .finish();
        Ok(())
    }
}

pub(super) fn verify_root(verifier: &mut Verifier, array: bool) -> Result<(), InvalidFlatbuffer> {
    if array {
        <ForwardsUOffset<OriginalArray>>::run_verifier(verifier, 0)
    } else {
        <ForwardsUOffset<OriginalArrayNode>>::run_verifier(verifier, 0)
    }
}

// Only normalize the distinct test marker's Rust type name in alignment errors.
pub(super) fn normalize_marker_alignment(mut error: InvalidFlatbuffer) -> InvalidFlatbuffer {
    if let InvalidFlatbuffer::Unaligned { unaligned_type, .. } = &mut error
        && unaligned_type.as_ref() == std::any::type_name::<ForwardsUOffset<OriginalArrayNode>>()
    {
        *unaligned_type =
            std::borrow::Cow::Borrowed(std::any::type_name::<ForwardsUOffset<fb::ArrayNode<'_>>>());
    }
    error
}
