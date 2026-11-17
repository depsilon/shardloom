// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Intrinsic nested-value cleanup through safe, allocation-free field transfers.
//! Continuations use heap scratch but request no new execution reservation.

use std::mem;
use std::vec;

use crate::dtype::DType;

use super::{Scalar, ScalarValue};

enum Frame {
    Elements(vec::IntoIter<Option<ScalarValue>>),
    Union(Option<ScalarValue>),
    Variant {
        value: Option<ScalarValue>,
        // Keep last, including on unwind: schema credit must outlive its value.
        _dtype: DType,
    },
}

impl Frame {
    fn next(&mut self) -> Option<ScalarValue> {
        match self {
            Self::Elements(elements) => elements.find_map(|value| value),
            Self::Union(value) | Self::Variant { value, .. } => value.take(),
        }
    }

    fn must_retain(&self) -> bool {
        match self {
            Self::Elements(elements) => elements.as_slice().iter().any(Option::is_some),
            Self::Union(_) => false,
            Self::Variant { .. } => true,
        }
    }
}

/// Remove nested ownership while leaving ordinary field drop safe and shallow.
fn take(value: &mut ScalarValue) -> Option<Frame> {
    match value {
        ScalarValue::Tuple(elements) => {
            let elements = mem::take(elements);
            (!elements.is_empty()).then(|| Frame::Elements(elements.into_iter()))
        }
        ScalarValue::Union(value) => value.take_child().map(|child| Frame::Union(Some(child))),
        ScalarValue::Variant(value) => {
            let Scalar { value, dtype } = mem::replace(
                &mut **value,
                Scalar {
                    value: None,
                    dtype: DType::Null,
                },
            );
            value.map(|value| Frame::Variant {
                value: Some(value),
                _dtype: dtype,
            })
        }
        ScalarValue::Bool(_)
        | ScalarValue::Primitive(_)
        | ScalarValue::Decimal(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::Binary(_) => None,
    }
}

// Ancestor schema credits must survive pending inner values even when a buffer
// owner panics. Vec's ordinary forward element drop order is not sufficient.
#[derive(Default)]
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

pub(super) fn release(value: &mut ScalarValue) {
    let Some(root) = take(value) else {
        return;
    };
    let mut pending = Pending(vec![root]);
    while let Some(frame) = pending.0.last_mut() {
        if let Some(mut child) = frame.next() {
            if !frame.must_retain() {
                pending.0.pop();
            }
            let descendants = take(&mut child);
            // Nested fields have been transferred. This invokes only shallow
            // value cleanup, then the ordinary leaf buffer/empty-box destructor.
            drop(child);
            if let Some(frame) = descendants {
                pending.0.push(frame);
            }
        } else {
            pending.0.pop();
        }
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
