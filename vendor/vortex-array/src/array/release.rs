// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Consume final array ownership in slot order without recursive child release.
//! No execution, descendant cloning or fresh execution grant occurs here.

use std::any::Any;
use std::sync::Arc;

use super::{ArrayData, ArrayInner, ArrayRef, DynArrayData, VTable};

/// Safety: the argument must have the exact concrete `ArrayData<V>` for which
/// the sealed dynamic data trait selected this function, from that same Arc.
pub(crate) type Extractor = unsafe fn(Arc<ArrayInner<dyn DynArrayData>>) -> Option<Frame>;

pub(crate) struct Frame {
    slots: smallvec::IntoIter<[Option<ArrayRef>; 4]>,
    // Keep last: all remaining slots must finish before their lifetime owner.
    retained_owner: Option<Arc<dyn Any + Send + Sync>>,
}

impl Frame {
    fn next(&mut self) -> Option<Arc<ArrayInner<dyn DynArrayData>>> {
        self.slots.find_map(|child| child.map(ArrayRef::into_inner))
    }

    fn must_retain(&self) -> bool {
        self.retained_owner.is_some() || self.slots.as_slice().iter().any(Option::is_some)
    }
}

/// Extract declared slots while retaining the owner through metadata destruction.
///
/// # Safety
/// `inner` must contain `ArrayData<V>`, selected by its own sealed dynamic data.
pub(super) unsafe fn extract<V: VTable>(inner: Arc<ArrayInner<dyn DynArrayData>>) -> Option<Frame> {
    // SAFETY: the caller obtained this monomorphized function from the same
    // allocation's sealed ArrayData<V> implementation. Its concrete type cannot
    // change while owned by the Arc. The provider's existing downcast transfers
    // that Arc without changing its allocation or reference count.
    let typed = unsafe { ArrayRef::from_erased_inner(inner).downcast_inner_unchecked::<V>() };
    // Unlike failed try_unwrap(...).ok(), simultaneous into_inner calls guarantee
    // one final owner receives the value instead of releasing it behind the loop.
    let ArrayInner {
        len: _,
        encoding_id: _,
        dtype,
        slots,
        stats,
        optimized,
        data:
            ArrayData {
                vtable,
                data,
                retained_owner,
            },
    } = Arc::into_inner(typed)?;
    let frame = Frame {
        slots: slots.into_iter(),
        retained_owner,
    };
    // All these owned fields are moved into one temporary. If one destructor
    // panics, tuple drop glue releases its siblings before `frame` can unwind.
    // A sequence of separate drops could refund the owner before pending data.
    drop((dtype, stats, optimized, vtable, data));
    Some(frame)
}

// Vec's ordinary forward drop order would refund ancestor credits too early
// if an opaque owner or encoding data panics. Drain innermost frames first.
#[derive(Default)]
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

pub(super) fn release(inner: Arc<ArrayInner<dyn DynArrayData>>) {
    let mut current = Some(inner);
    let mut pending = Pending::default();
    loop {
        if let Some(inner) = current.take() {
            let extract = inner.data.release_fn();
            // SAFETY: this exact immutable allocation selected the extractor
            // through its sealed concrete data implementation immediately above.
            // No type, encoding ID or caller-supplied registry selects the call.
            if let Some(mut frame) = unsafe { extract(inner) } {
                current = frame.next();
                if current.is_some() && frame.must_retain() {
                    pending.0.push(frame);
                }
            }
        } else if let Some(frame) = pending.0.last_mut() {
            current = frame.next();
            if current.is_none() {
                pending.0.pop();
            }
        } else {
            return;
        }
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
