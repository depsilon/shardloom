// SPDX-License-Identifier: Apache-2.0
// Modified by ShardLoom: ordered optimizer continuations with original rule semantics.

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use super::ArrayByPtr;
use super::MAX_OPTIMIZER_REWRITE_PASS;
use super::Optimizer;
use super::try_session_parent_reduce;
use crate::ArrayRef;
use crate::array::ArraySlots;
use crate::trace_op;

enum Work {
    Array {
        original: ArrayRef,
        key: Option<ArrayByPtr>,
    },
    Root(Root),
    Children(Children),
    Complete(ArrayRef),
}

struct Root {
    current: ArrayRef,
    pass: usize,
    any_optimizations: bool,
}

struct Children {
    // A recursive call drops partial replacements before its input owner.
    new_slots: Option<ArraySlots>,
    array: ArrayRef,
    slot: usize,
}

enum Frame {
    Root {
        // The previous optimized owner survives evaluation of the replacement.
        previous: Option<ArrayRef>,
        original: ArrayRef,
        key: Option<ArrayByPtr>,
        pass: usize,
    },
    Children {
        optimized: ArrayRef,
        original: ArrayRef,
        key: Option<ArrayByPtr>,
        pass: usize,
    },
    Rewrite(Root),
    Child(Children),
}

#[derive(Default)]
struct Pending(Vec<Frame>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

pub(super) fn optimize_array(
    optimizer: &mut Optimizer<'_>,
    array: &ArrayRef,
) -> VortexResult<ArrayRef> {
    let work = begin_array(optimizer, array);
    run(optimizer, work)
}

pub(super) fn optimize_root(
    optimizer: &mut Optimizer<'_>,
    array: ArrayRef,
) -> VortexResult<ArrayRef> {
    let work = begin_root(optimizer, array);
    run(optimizer, work)
}

fn begin_array(optimizer: &Optimizer<'_>, array: &ArrayRef) -> Work {
    // Test before any continuation-owned clone, exactly as the recursive entry.
    if !array.is_shared() {
        return Work::Array {
            original: array.clone(),
            key: None,
        };
    }
    let key = ArrayByPtr(array.clone());
    if let Some(rewritten) = optimizer.rewritten_shared.get(&key) {
        return Work::Complete(rewritten.clone());
    }
    Work::Array {
        original: array.clone(),
        key: Some(key),
    }
}

fn begin_root(_optimizer: &Optimizer<'_>, array: ArrayRef) -> Work {
    trace_op!(record_optimize_start(
        &array,
        _optimizer.session_kernels.is_some()
    ));
    Work::Root(Root {
        current: array,
        pass: 0,
        any_optimizations: false,
    })
}

fn begin_children(array: ArrayRef) -> Work {
    Work::Children(Children {
        new_slots: None,
        array,
        slot: 0,
    })
}

fn finish_array(
    optimizer: &mut Optimizer<'_>,
    original: ArrayRef,
    optimized: ArrayRef,
    key: Option<ArrayByPtr>,
) -> Work {
    if !ArrayRef::ptr_eq(&optimized, &original) {
        optimized.statistics().inherit_from(original.statistics());
    }
    optimized.optimized_state().mark_optimized();
    // optimize() returned before optimize_array() remembered the rewrite.
    drop(original);
    if let Some(key) = key
        && !ArrayRef::ptr_eq(&optimized, &key.0)
    {
        optimizer.rewritten_shared.insert(key, optimized.clone());
    }
    Work::Complete(optimized)
}

fn run(optimizer: &mut Optimizer<'_>, mut work: Work) -> VortexResult<ArrayRef> {
    let mut pending = Pending::default();
    loop {
        work = match work {
            Work::Array { original, key } => {
                if original.optimized_state().is_optimized() {
                    Work::Complete(original)
                } else {
                    let root = original.clone();
                    pending.0.push(Frame::Root {
                        previous: None,
                        original,
                        key,
                        pass: 0,
                    });
                    begin_root(optimizer, root)
                }
            }
            Work::Root(root) => rewrite_root(optimizer, root, &mut pending)?,
            Work::Children(children) => visit_children(optimizer, children, &mut pending)?,
            Work::Complete(complete) => {
                let Some(frame) = pending.0.pop() else {
                    return Ok(complete);
                };
                resume(optimizer, frame, complete, &mut pending)
            }
        };
    }
}

fn rewrite_root(
    optimizer: &Optimizer<'_>,
    mut root: Root,
    pending: &mut Pending,
) -> VortexResult<Work> {
    loop {
        if root.pass > MAX_OPTIMIZER_REWRITE_PASS {
            vortex_bail!("Exceeded maximum optimization iterations (possible infinite loop)");
        }
        trace_op!(record_optimize_loop_start(&root.current));
        let mut rewritten = root.current.reduce()?;
        if rewritten.is_none() {
            trace_op!(record_optimize_reduce_none(&root.current));
            for (slot_idx, slot) in root.current.slots().iter().enumerate() {
                let Some(child) = slot else {
                    continue;
                };
                if let Some(kernels) = optimizer.session_kernels
                    && let Some(new_array) =
                        try_session_parent_reduce(kernels, &root.current, child, slot_idx)?
                {
                    rewritten = Some(new_array);
                    break;
                }
                if let Some(new_array) = child.reduce_parent(&root.current, slot_idx)? {
                    rewritten = Some(new_array);
                    break;
                }
            }
        }
        let Some(rewritten) = rewritten else {
            trace_op!(record_optimize_parent_reduce_none(&root.current));
            trace_op!(record_optimize_loop_end());
            trace_op!(record_optimize_done(&root.current, root.any_optimizations));
            return Ok(Work::Complete(root.current));
        };
        if optimizer.session_kernels.is_some() {
            pending.0.push(Frame::Rewrite(root));
            return Ok(begin_children(rewritten));
        }
        root.current = rewritten;
        root.any_optimizations = true;
        trace_op!(record_optimize_loop_end());
        root.pass += 1;
    }
}

fn visit_children(
    optimizer: &Optimizer<'_>,
    mut children: Children,
    pending: &mut Pending,
) -> VortexResult<Work> {
    while children.slot < children.array.slots().len() {
        let slot = &children.array.slots()[children.slot];
        if let Some(child) = slot
            && !child.optimized_state().is_optimized()
        {
            let next = begin_array(optimizer, child);
            pending.0.push(Frame::Child(children));
            return Ok(next);
        }
        if let Some(new_slots) = &mut children.new_slots {
            new_slots.push(slot.clone());
        }
        children.slot += 1;
    }
    let Some(new_slots) = children.new_slots else {
        return Ok(Work::Complete(children.array));
    };
    // SAFETY: unchanged provider rule contract: replaced child slots are
    // logically equivalent, preserving the parent's values and statistics.
    unsafe { children.array.with_slots(new_slots) }.map(Work::Complete)
}

fn resume(
    optimizer: &mut Optimizer<'_>,
    frame: Frame,
    complete: ArrayRef,
    pending: &mut Pending,
) -> Work {
    match frame {
        Frame::Root {
            previous,
            original,
            key,
            pass,
        } => {
            drop(previous);
            if pass > MAX_OPTIMIZER_REWRITE_PASS {
                return finish_array(optimizer, original, complete, key);
            }
            let children = complete.clone();
            pending.0.push(Frame::Children {
                optimized: complete,
                original,
                key,
                pass,
            });
            begin_children(children)
        }
        Frame::Children {
            optimized,
            original,
            key,
            pass,
        } => {
            if ArrayRef::ptr_eq(&complete, &optimized) {
                drop(complete);
                return finish_array(optimizer, original, optimized, key);
            }
            pending.0.push(Frame::Root {
                previous: Some(optimized),
                original,
                key,
                pass: pass + 1,
            });
            begin_root(optimizer, complete)
        }
        Frame::Rewrite(mut root) => {
            root.current = complete;
            root.any_optimizations = true;
            trace_op!(record_optimize_loop_end());
            root.pass += 1;
            Work::Root(root)
        }
        Frame::Child(mut children) => {
            let slot = &children.array.slots()[children.slot];
            let child = slot.as_ref().vortex_expect("suspended present child");
            let changed = (!ArrayRef::ptr_eq(&complete, child)).then_some(complete);
            match changed {
                Some(complete) => children
                    .new_slots
                    .get_or_insert_with(|| {
                        let mut slots = ArraySlots::with_capacity(children.array.slots().len());
                        slots.extend(children.array.slots()[..children.slot].iter().cloned());
                        slots
                    })
                    .push(Some(complete)),
                None => {
                    if let Some(new_slots) = &mut children.new_slots {
                        new_slots.push(slot.clone());
                    }
                }
            }
            children.slot += 1;
            Work::Children(children)
        }
    }
}
