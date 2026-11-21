// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Synchronous driving of suspended, ordered array deserialization callbacks.

use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use vortex_error::{VortexResult, vortex_bail, vortex_err};
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use super::{ArrayChildren, SerializedArray};
use crate::array::{ArrayDeserialization, new_foreign_array};
use crate::dtype::DType;
use crate::expr::stats::{Precision, Stat};
use crate::optimizer::mark_decoded_optimized;
use crate::session::ArraySessionExt;
use crate::stats::StatsSet;
use crate::{ArrayRef, ArraySlots};

/// An owned deserialization continuation. This is local, sequential work, not
/// an executor task: only the next child request may suspend the future.
pub type DeserializeFuture<'a, T> = Pin<Box<dyn Future<Output = VortexResult<T>> + 'a>>;

/// Adapt ordinary synchronous children for a shared async decoder body.
///
/// Its async requests immediately call the underlying synchronous callback,
/// preserving the established entrypoint's behavior and callback order.
pub struct SynchronousArrayChildren<'a>(&'a dyn ArrayChildren);

impl<'a> SynchronousArrayChildren<'a> {
    /// Borrow an existing synchronous child source.
    pub fn new(children: &'a dyn ArrayChildren) -> Self {
        Self(children)
    }
}

impl ArrayChildren for SynchronousArrayChildren<'_> {
    fn get(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        self.0.get(index, dtype, len)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Finish a decoder whose children are immediate. No executor or busy wait is
/// introduced; suspension outside the ordered child driver is unsupported.
pub fn finish_deserialize<T>(mut future: DeserializeFuture<'_, T>) -> VortexResult<T> {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => vortex_bail!("Array deserializer suspended outside its child driver"),
    }
}

struct Request {
    serialized: SerializedArray,
    dtype: DType,
    len: usize,
    foreign: bool,
}

enum ChildState {
    Idle,
    Requested(Request),
    Complete(VortexResult<ArrayRef>),
}

struct Children<'a> {
    serialized: &'a SerializedArray,
    state: Rc<RefCell<ChildState>>,
    foreign: bool,
}

impl ArrayChildren for Children<'_> {
    fn get(&self, index: usize, _dtype: &DType, _len: usize) -> VortexResult<ArrayRef> {
        vortex_bail!(
            "Array decoder requires a continuation for child {index}; use ArrayChildren::get_async"
        )
    }

    fn get_async<'a>(
        &'a self,
        index: usize,
        dtype: &'a DType,
        len: usize,
    ) -> DeserializeFuture<'a, ArrayRef> {
        let mut submitted = false;
        Box::pin(poll_fn(move |_| {
            let mut state = self.state.borrow_mut();
            if !submitted {
                if !matches!(*state, ChildState::Idle) {
                    return Poll::Ready(Err(vortex_err!(
                        "Array deserializer requested concurrent or unconsumed children"
                    )));
                }
                // Preserve the original child-index check before decoding and
                // share the dtype's existing children without parsing them.
                *state = ChildState::Requested(Request {
                    serialized: self.serialized.child(index),
                    dtype: dtype.clone(),
                    len,
                    foreign: self.foreign,
                });
                submitted = true;
                return Poll::Pending;
            }
            match mem::replace(&mut *state, ChildState::Idle) {
                ChildState::Complete(result) => Poll::Ready(result),
                other => {
                    *state = other;
                    Poll::Ready(Err(vortex_err!(
                        "Array deserializer resumed before its child completed"
                    )))
                }
            }
        }))
    }

    fn len(&self) -> usize {
        self.serialized.nchildren()
    }
}

struct Frame<'a> {
    future: DeserializeFuture<'a, ArrayRef>,
    state: Rc<RefCell<ChildState>>,
}

impl<'a> Frame<'a> {
    fn new(request: Request, ctx: &'a ReadContext, session: &'a VortexSession) -> Self {
        let state = Rc::new(RefCell::new(ChildState::Idle));
        let child_state = Rc::clone(&state);
        let future = Box::pin(async move {
            let Request {
                serialized,
                dtype,
                len,
                foreign,
            } = request;
            let encoding_idx = serialized.flatbuffer().encoding();
            let encoding_id = ctx
                .resolve(encoding_idx)
                .ok_or_else(|| vortex_err!("Unknown encoding index: {}", encoding_idx))?;
            // Foreign descendants stay foreign even when their ID is registered.
            let plugin = if foreign {
                None
            } else {
                session.arrays().registry().get(&encoding_id)
            };
            let Some(plugin) = plugin else {
                if !foreign && !session.allows_unknown() {
                    vortex_bail!("Unknown encoding: {}", encoding_id);
                }
                let children = Children {
                    serialized: &serialized,
                    state: child_state,
                    foreign: true,
                };
                let mut slots = ArraySlots::with_capacity(serialized.nchildren());
                for index in 0..serialized.nchildren() {
                    slots.push(Some(children.get_async(index, &dtype, len).await?));
                }
                return new_foreign_array(
                    encoding_id,
                    dtype.clone(),
                    len,
                    serialized.metadata().to_vec(),
                    serialized.collect_buffers()?.into_owned(),
                    slots,
                );
            };

            let children = Children {
                serialized: &serialized,
                state: child_state,
                foreign: false,
            };
            let buffers = serialized.collect_buffers()?;
            let decoded = plugin
                .deserialize_async(
                    ArrayDeserialization::new(
                        encoding_id,
                        &dtype,
                        len,
                        serialized.metadata(),
                        &buffers,
                        &children,
                    ),
                    session,
                )
                .await?;

            assert_eq!(
                decoded.len(),
                len,
                "Array decoded from {} has incorrect length {}, expected {}",
                encoding_id,
                decoded.len(),
                len,
            );
            assert_eq!(
                decoded.dtype(),
                &dtype,
                "Array decoded from {} has incorrect dtype {}, expected {}",
                encoding_id,
                decoded.dtype(),
                dtype,
            );
            assert!(
                plugin.is_supported_encoding(&decoded.encoding_id()),
                "Array decoded from {} has incorrect encoding {}",
                encoding_id,
                decoded.encoding_id(),
            );

            if let Some(stats) = serialized.flatbuffer().stats() {
                let mut stats = StatsSet::from_flatbuffer(&stats, &dtype, session)?;
                if len <= 1 {
                    stats.set(Stat::IsSorted, Precision::Exact(true.into()));
                    stats.set(Stat::IsStrictSorted, Precision::Exact(true.into()));
                }
                decoded.statistics().set_all(stats);
            }
            mark_decoded_optimized(&decoded);
            Ok(decoded)
        });
        Self { future, state }
    }
}

// Unlike the natural Vec drop order, unwind active callbacks from the innermost
// child toward its parent. No frame owns another frame or polls during cleanup.
struct Work<'a>(Vec<Frame<'a>>);

impl Drop for Work<'_> {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

pub(super) fn decode(
    serialized: &SerializedArray,
    dtype: &DType,
    len: usize,
    ctx: &ReadContext,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    let mut work = Work(vec![Frame::new(
        Request {
            serialized: serialized.clone(),
            dtype: dtype.clone(),
            len,
            foreign: false,
        },
        ctx,
        session,
    )]);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        let frame = work.0.last_mut().unwrap_or_else(|| unreachable!());
        match frame.future.as_mut().poll(&mut context) {
            Poll::Pending => {
                let next = mem::replace(&mut *frame.state.borrow_mut(), ChildState::Idle);
                let ChildState::Requested(request) = next else {
                    vortex_bail!("Array deserializer suspended without a child request");
                };
                work.0.push(Frame::new(request, ctx, session));
            }
            Poll::Ready(result) => {
                if !matches!(*frame.state.borrow(), ChildState::Idle) {
                    vortex_bail!("Array deserializer completed with an unconsumed child request");
                }
                drop(work.0.pop());
                let Some(parent) = work.0.last_mut() else {
                    return result;
                };
                *parent.state.borrow_mut() = ChildState::Complete(result);
            }
        }
    }
}

#[cfg(test)]
#[path = "deserialization_tests.rs"]
mod tests;
