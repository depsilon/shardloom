// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered ArrayNode verification with the original checked verifier budgets.

use std::borrow::Cow;
use std::ops::Range;

use flatbuffers::{
    ErrorTraceDetail, ForwardsUOffset, InvalidFlatbuffer, TableVerifier, TableVerifierContinuation,
    Vector, Verifier, append_trace, verify_vector_range,
};

use super::array as fb;

type Result<T> = std::result::Result<T, InvalidFlatbuffer>;

struct Children {
    remaining: Range<usize>,
    next_index: usize,
    field_position: usize,
}

struct Parent<'opts, 'buf> {
    table: TableVerifierContinuation<'opts, 'buf>,
    children: Children,
    element_position: usize,
    element_index: usize,
}

fn field_trace<T>(position: usize, result: Result<T>) -> Result<T> {
    append_trace(
        result,
        ErrorTraceDetail::TableField {
            field_name: Cow::Borrowed("children"),
            position,
        },
    )
}

pub(super) fn verify<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    position: usize,
) -> Result<()> {
    let mut parents = Vec::new();
    let result = verify_inner(verifier, position, &mut parents);
    // Errors leave unfinished tables active, exactly as the generated verifier
    // does. Its child-to-parent traces are appended in that same order.
    parents.into_iter().rev().fold(result, |result, parent| {
        let result = append_trace(
            result,
            ErrorTraceDetail::VectorElement {
                index: parent.element_index,
                position: parent.element_position,
            },
        );
        field_trace(parent.children.field_position, result)
    })
}

fn verify_inner<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    position: usize,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<()> {
    let mut child = enter(verifier, position, parents)?;
    loop {
        child = if let Some(position) = child {
            let offset = verifier.get_uoffset(position)? as usize;
            enter(verifier, position.saturating_add(offset), parents)?
        } else if let Some(parent) = parents.pop() {
            next_child(parent.table.resume(verifier), parent.children, parents)?
        } else {
            return Ok(());
        };
    }
}

fn enter<'opts, 'buf>(
    verifier: &mut Verifier<'opts, 'buf>,
    position: usize,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Option<usize>> {
    let mut table = verifier
        .visit_table(position)?
        .visit_field::<u16>("encoding", fb::ArrayNode::VT_ENCODING, false)?
        .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
            "metadata",
            fb::ArrayNode::VT_METADATA,
            false,
        )?;
    let field_position = table.deref(fb::ArrayNode::VT_CHILDREN)?;
    let remaining = if let Some(position) = field_position {
        let range = (|| {
            let verifier = table.verifier();
            let offset = verifier.get_uoffset(position)? as usize;
            verify_vector_range::<ForwardsUOffset<fb::ArrayNode>>(
                verifier,
                position.saturating_add(offset),
            )
        })();
        field_trace(position, range)?
    } else {
        0..0
    };
    next_child(
        table,
        Children {
            remaining,
            next_index: 0,
            // An absent vector cannot produce an element trace.
            field_position: field_position.unwrap_or(0),
        },
        parents,
    )
}

fn next_child<'opts, 'buf>(
    table: TableVerifier<'_, 'opts, 'buf>,
    mut children: Children,
    parents: &mut Vec<Parent<'opts, 'buf>>,
) -> Result<Option<usize>> {
    if children.remaining.start < children.remaining.end {
        let position = children.remaining.start;
        children.remaining.start =
            position.saturating_add(size_of::<ForwardsUOffset<fb::ArrayNode>>());
        let index = children.next_index;
        children.next_index += 1;
        parents.push(Parent {
            table: table.suspend(),
            children,
            element_position: position,
            element_index: index,
        });
        return Ok(Some(position));
    }
    table
        .visit_field::<ForwardsUOffset<Vector<'_, u16>>>(
            "buffers",
            fb::ArrayNode::VT_BUFFERS,
            false,
        )?
        .visit_field::<ForwardsUOffset<fb::ArrayStats>>("stats", fb::ArrayNode::VT_STATS, false)?
        .finish();
    Ok(None)
}

#[cfg(test)]
#[path = "array_verify_tests.rs"]
mod tests;
