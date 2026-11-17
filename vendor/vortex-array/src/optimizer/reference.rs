// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Frozen recursive optimizer from 39986b5a1. Only imports and entry visibility differ.

use super::{ArrayKernels, MAX_OPTIMIZER_REWRITE_PASS};
use crate::{ArrayRef, trace_op};
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use vortex_error::{VortexResult, vortex_bail};
use vortex_utils::aliases::hash_map::HashMap;

struct ArrayByPtr(ArrayRef);

impl PartialEq for ArrayByPtr {
    fn eq(&self, other: &Self) -> bool {
        ArrayRef::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ArrayByPtr {}

impl Hash for ArrayByPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.addr().hash(state);
    }
}

/// The optimizer state, which can be kept for many optimize calls.
///
/// Execution keeps one optimizer for all of [`ArrayRef::execute_until`], so a shared node that one
/// step rewrote is not rewritten again when a later step reaches it through another parent.
pub(crate) struct Optimizer<'a> {
    /// The session kernels, tried before the static parent-reduce rules.
    ///
    /// Without them, the optimizer rewrites only the root node and does not mark the result, so
    /// optimizing it later with a session still tries the session kernels.
    session_kernels: Option<&'a ArrayKernels>,
    /// The shared nodes this optimizer rewrote, each with the array it was rewritten into.
    ///
    /// Many parents can hold the same unoptimized node, such as the struct below every field of a
    /// projection. A rewrite returns a new array and does not mark the node, so without this each
    /// parent would rewrite the node and its whole subtree again. The key keeps the node alive
    /// for the life of the optimizer.
    rewritten_shared: HashMap<ArrayByPtr, ArrayRef>,
}

impl<'a> Optimizer<'a> {
    /// Creates an optimizer that tries `session_kernels` before the static parent-reduce rules.
    pub(crate) fn new(session_kernels: Option<&'a ArrayKernels>) -> Self {
        Self {
            session_kernels,
            rewritten_shared: HashMap::new(),
        }
    }

    /// Optimizes `array` and the descendants below it that were never optimized, and marks the
    /// result.
    ///
    /// If another owner holds `array` too, the rewrite is remembered, and a later call with the
    /// same array returns it.
    pub(crate) fn optimize_array(&mut self, array: &ArrayRef) -> VortexResult<ArrayRef> {
        if !array.is_shared() {
            return self.optimize(array.clone());
        }

        let key = ArrayByPtr(array.clone());
        if let Some(rewritten) = self.rewritten_shared.get(&key) {
            return Ok(rewritten.clone());
        }

        let optimized = self.optimize(array.clone())?;
        if !ArrayRef::ptr_eq(&optimized, array) {
            self.rewritten_shared.insert(key, optimized.clone());
        }

        Ok(optimized)
    }

    /// Optimizes the root of `array` top down: first the root, then the descendants below it that
    /// were never optimized, then the root again while its children change. Marks the result.
    fn optimize(&mut self, array: ArrayRef) -> VortexResult<ArrayRef> {
        if array.optimized_state().is_optimized() {
            return Ok(array);
        }

        // The rules of the root get the first try, on children that can still be lazy, so a rule
        // that drops or replaces a child does not pay to optimize that child's subtree first.
        let mut optimized = self.optimize_root(array.clone())?;

        for _ in 0..=MAX_OPTIMIZER_REWRITE_PASS {
            let with_children = self.optimize_new_nodes(optimized.clone())?;
            if ArrayRef::ptr_eq(&with_children, &optimized) {
                break;
            }

            // A child changed its encoding, so a parent rule keyed on that encoding can apply now.
            optimized = self.optimize_root(with_children)?;
        }

        // A rewrite keeps the logical values, so every statistic of the input stays valid. Lazy
        // constructors such as `slice` put the statistics they carry over on the lazy node.
        if !ArrayRef::ptr_eq(&optimized, &array) {
            optimized.statistics().inherit_from(array.statistics());
        }

        optimized.optimized_state().mark_optimized();
        Ok(optimized)
    }

    /// Runs the reduce and parent-reduce rules on the root of `current_array` to fixpoint.
    ///
    /// With session kernels, the nodes each rewrite built are optimized before the next pass.
    pub(super) fn optimize_root(&mut self, mut current_array: ArrayRef) -> VortexResult<ArrayRef> {
        let mut any_optimizations = false;

        trace_op!(record_optimize_start(
            &current_array,
            self.session_kernels.is_some()
        ));

        for _ in 0..=MAX_OPTIMIZER_REWRITE_PASS {
            trace_op!(record_optimize_loop_start(&current_array));

            if let Some(new_array) = current_array.reduce()? {
                current_array = self.optimize_rewritten(new_array)?;
                any_optimizations = true;
                trace_op!(record_optimize_loop_end());
                continue;
            }

            trace_op!(record_optimize_reduce_none(&current_array));

            // Try children in order; the first parent rewrite restarts the fixpoint loop.
            let mut reduced_parent = None;
            for (slot_idx, slot) in current_array.slots().iter().enumerate() {
                let Some(child) = slot else {
                    continue;
                };

                // Session kernels take precedence over the child's static parent-reduce rules.
                if let Some(session_kernels) = self.session_kernels
                    && let Some(new_array) =
                        try_session_parent_reduce(session_kernels, &current_array, child, slot_idx)?
                {
                    reduced_parent = Some(new_array);
                    break;
                }

                if let Some(new_array) = child.reduce_parent(&current_array, slot_idx)? {
                    reduced_parent = Some(new_array);
                    break;
                }
            }

            if let Some(new_array) = reduced_parent {
                current_array = self.optimize_rewritten(new_array)?;
                any_optimizations = true;
                trace_op!(record_optimize_loop_end());
                continue;
            }

            trace_op!(record_optimize_parent_reduce_none(&current_array));
            trace_op!(record_optimize_loop_end());

            trace_op!(record_optimize_done(&current_array, any_optimizations));

            return Ok(current_array);
        }

        vortex_bail!("Exceeded maximum optimization iterations (possible infinite loop)");
    }

    /// Optimizes the nodes that a rule built below the array it returned, if the optimizer has
    /// session kernels.
    fn optimize_rewritten(&mut self, array: ArrayRef) -> VortexResult<ArrayRef> {
        if self.session_kernels.is_none() {
            return Ok(array);
        }

        self.optimize_new_nodes(array)
    }

    /// Optimizes the nodes below `array` that were never optimized, such as those a rule built
    /// below its result.
    ///
    /// Rules and constructors build lazy arrays, so these nodes are not yet optimized, at any
    /// depth. Optimizing them as each rewrite happens keeps the optimizer from undoing one rule
    /// with its inverse: `slice(get_item(s))` and `get_item(slice(s))` rewrite into each other
    /// until the inner `slice(s)` collapses into `s`'s own encoding.
    ///
    /// A child that is already optimized is left as it is without visiting its subtree, which
    /// skips the subtrees a rule carries over from the array it rewrote.
    fn optimize_new_nodes(&mut self, array: ArrayRef) -> VortexResult<ArrayRef> {
        let mut new_slots: Option<SmallVec<_>> = None;
        for (slot_idx, slot) in array.slots().iter().enumerate() {
            let optimized = match slot {
                Some(child) if !child.optimized_state().is_optimized() => {
                    let optimized = self.optimize_array(child)?;
                    (!ArrayRef::ptr_eq(&optimized, child)).then_some(optimized)
                }
                _ => None,
            };

            match optimized {
                Some(optimized) => new_slots
                    .get_or_insert_with(|| {
                        let mut slots = SmallVec::with_capacity(array.slots().len());
                        slots.extend(array.slots()[..slot_idx].iter().cloned());
                        slots
                    })
                    .push(Some(optimized)),
                None => {
                    if let Some(new_slots) = &mut new_slots {
                        new_slots.push(slot.clone());
                    }
                }
            }
        }

        let Some(new_slots) = new_slots else {
            return Ok(array);
        };

        // SAFETY: optimizer rules only replace child slots with logically equivalent arrays, so
        // the parent's logical values and statistics remain valid.
        unsafe { array.with_slots(new_slots) }
    }
}

fn try_session_parent_reduce(
    kernels: &ArrayKernels,
    parent: &ArrayRef,
    child: &ArrayRef,
    slot_idx: usize,
) -> VortexResult<Option<ArrayRef>> {
    let Some(reduce_parent_fns) =
        kernels.find_reduce_parent(parent.encoding_id(), child.encoding_id())
    else {
        return Ok(None);
    };

    #[allow(clippy::unused_enumerate_index)]
    for (_kernel_idx, reduce_parent) in reduce_parent_fns.iter().enumerate() {
        if let Some(new_array) = reduce_parent(child, parent, slot_idx)? {
            trace_op!(record_session_parent_reduce_applied(
                parent,
                child,
                slot_idx,
                _kernel_idx,
                &new_array,
            ));

            return Ok(Some(new_array));
        }

        trace_op!(record_session_parent_reduce_declined(
            parent,
            child,
            slot_idx,
            _kernel_idx,
        ));
    }

    Ok(None)
}
