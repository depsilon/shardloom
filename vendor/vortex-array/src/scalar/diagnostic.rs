// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Bound diagnostic expansion without changing ordinary value Display.

use std::fmt;
use std::slice;

use vortex_buffer::{BufferString, ByteBuffer};
use vortex_error::VortexExpect;

use super::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};

const CHILD_DEPTH: usize = 32;

enum Children<'a> {
    One(Option<&'a ScalarValue>),
    Tuple(slice::Iter<'a, Option<ScalarValue>>),
}

impl<'a> Children<'a> {
    fn new(value: &'a ScalarValue) -> Self {
        match value {
            ScalarValue::Tuple(values) => Self::Tuple(values.iter()),
            ScalarValue::Union(value) => Self::One(value.child_value()),
            ScalarValue::Variant(value) => Self::One(value.value()),
            _ => Self::One(None),
        }
    }

    fn next(&mut self) -> Option<&'a ScalarValue> {
        match self {
            Self::One(child) => child.take(),
            Self::Tuple(values) => values.find_map(Option::as_ref),
        }
    }
}

fn depth_exceeded(value: &ScalarValue) -> bool {
    let mut parents: [Option<Children<'_>>; CHILD_DEPTH] = std::array::from_fn(|_| None);
    let mut depth = 0;
    let mut children = Children::new(value);
    loop {
        if let Some(child) = children.next() {
            if depth == CHILD_DEPTH {
                return true;
            }
            parents[depth] = Some(children);
            depth += 1;
            children = Children::new(child);
        } else if depth != 0 {
            depth -= 1;
            children = parents[depth]
                .take()
                .vortex_expect("pending value children");
        } else {
            return false;
        }
    }
}

fn abbreviated(value: &ScalarValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let name = match value {
        ScalarValue::Tuple(_) => "Tuple",
        ScalarValue::Union(_) => "Union",
        ScalarValue::Variant(_) => "Variant",
        _ => unreachable!("only nested values exceed the diagnostic depth"),
    };
    f.debug_tuple(name).finish_non_exhaustive()
}

#[derive(Debug)]
#[expect(
    dead_code,
    reason = "borrowed fields are read by the original-style Debug derive"
)]
enum Shallow<'a> {
    Bool(&'a bool),
    Primitive(&'a PValue),
    Decimal(&'a DecimalValue),
    Utf8(&'a BufferString),
    Binary(&'a ByteBuffer),
    Tuple(&'a Vec<Option<ScalarValue>>),
    Union(&'a UnionValue),
    Variant(&'a Scalar),
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if depth_exceeded(self) {
            return abbreviated(self, f);
        }
        let shallow = match self {
            Self::Bool(value) => Shallow::Bool(value),
            Self::Primitive(value) => Shallow::Primitive(value),
            Self::Decimal(value) => Shallow::Decimal(value),
            Self::Utf8(value) => Shallow::Utf8(value),
            Self::Binary(value) => Shallow::Binary(value),
            Self::Tuple(value) => Shallow::Tuple(value),
            Self::Union(value) => Shallow::Union(value),
            Self::Variant(value) => Shallow::Variant(value),
        };
        shallow.fmt(f)
    }
}

pub(super) struct ValueDisplay<'a>(pub(super) &'a ScalarValue);

impl fmt::Display for ValueDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if depth_exceeded(self.0) {
            abbreviated(self.0, f)
        } else {
            fmt::Display::fmt(self.0, f)
        }
    }
}

#[cfg(test)]
#[path = "diagnostic_tests.rs"]
mod tests;
