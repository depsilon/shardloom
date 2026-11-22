// SPDX-License-Identifier: Apache-2.0
// Modified by ShardLoom: ordered optimizer continuations with original rule semantics.

use std::cell::Cell;
use std::cell::RefCell;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_utils::aliases::hash_map::HashMap;

use super::ArrayByPtr;
use super::MAX_OPTIMIZER_REWRITE_PASS;
use super::Optimizer;
use super::inherit_slice_stats;
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

pub(crate) enum DeferredMeta {
    Plain,
    Slice { source: ArrayRef },
}

struct DeferredEntry {
    wrapper: ArrayRef,
    meta: DeferredMeta,
}

#[derive(Default)]
struct DeferredRegistry {
    entries: HashMap<usize, DeferredEntry>,
}

const DEFER_PUSHDOWN_DEPTH: usize = 4;

thread_local! {
    static OPTIMIZE_NESTING_DEPTH: Cell<usize> = const { Cell::new(0) };
    static DEFERRED_PUSHDOWN: RefCell<Option<DeferredRegistry>> = const { RefCell::new(None) };
}

struct DepthGuard;

impl DepthGuard {
    fn enter() -> Self {
        OPTIMIZE_NESTING_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        OPTIMIZE_NESTING_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

struct DeferGuard(Option<DeferredRegistry>);

impl DeferGuard {
    fn install() -> Self {
        let prev =
            DEFERRED_PUSHDOWN.with(|cell| cell.borrow_mut().replace(DeferredRegistry::default()));
        Self(prev)
    }
}

impl Drop for DeferGuard {
    fn drop(&mut self) {
        let prev = self.0.take();
        DEFERRED_PUSHDOWN.with(|cell| {
            *cell.borrow_mut() = prev;
        });
    }
}

pub(crate) fn is_defer_active() -> bool {
    DEFERRED_PUSHDOWN.with(|cell| cell.borrow().is_some())
}

pub(crate) fn should_defer_pushdown() -> bool {
    OPTIMIZE_NESTING_DEPTH.with(|depth| depth.get() >= DEFER_PUSHDOWN_DEPTH)
}

pub(crate) fn defer_wrapper(wrapper: &ArrayRef, meta: DeferredMeta) {
    DEFERRED_PUSHDOWN.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if let Some(reg) = borrow.as_mut() {
            reg.entries.insert(
                wrapper.addr(),
                DeferredEntry {
                    wrapper: wrapper.clone(),
                    meta,
                },
            );
        }
    });
}

enum DeferStage {
    PreChildren {
        orig_addr: usize,
        orig_wrapper: ArrayRef,
        meta: DeferredMeta,
        cache_shared: bool,
    },
    PostChildren {
        completion: Option<(usize, ArrayRef, DeferredMeta, bool)>,
    },
}

struct DeferRebuildFrame {
    parent: ArrayRef,
    updated_slots: Vec<Option<ArrayRef>>,
    cur_slot: usize,
    pending: Vec<(usize, ArrayRef, DeferredMeta)>,
    stage: DeferStage,
}

enum DeferStep {
    Pre {
        orig_wrapper: ArrayRef,
        node: ArrayRef,
        meta: DeferredMeta,
        cache_shared: bool,
    },
    Rewrite {
        orig_addr: usize,
        orig_wrapper: ArrayRef,
        node: ArrayRef,
        meta: DeferredMeta,
        cache_shared: bool,
    },
    Complete(ArrayRef),
}

#[derive(Default)]
struct DeferPending(Vec<DeferRebuildFrame>);

impl Drop for DeferPending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

fn take_deferred_slot(
    slot: &ArrayRef,
    resolved: &HashMap<usize, (ArrayRef, ArrayRef)>,
) -> Option<Result<ArrayRef, (ArrayRef, DeferredMeta)>> {
    let addr = slot.addr();
    if let Some((cached_orig, cached_result)) = resolved.get(&addr)
        && ArrayRef::ptr_eq(cached_orig, slot)
    {
        return Some(Ok(cached_result.clone()));
    }
    let taken = DEFERRED_PUSHDOWN.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let reg = borrow.as_mut()?;
        if reg
            .entries
            .get(&addr)
            .is_some_and(|entry| ArrayRef::ptr_eq(&entry.wrapper, slot))
        {
            reg.entries.remove(&addr)
        } else {
            None
        }
    });
    taken.map(|entry| Err((entry.wrapper, entry.meta)))
}

type PendingChild = (usize, ArrayRef, DeferredMeta);
type SlotReplacements = (Vec<Option<ArrayRef>>, Vec<PendingChild>);

fn collect_slot_replacements(
    parent: &ArrayRef,
    resolved: &HashMap<usize, (ArrayRef, ArrayRef)>,
) -> SlotReplacements {
    let mut updated_slots: Vec<Option<ArrayRef>> = parent.slots().to_vec();
    let mut pending = Vec::new();
    for (idx, slot) in parent.slots().iter().enumerate() {
        let Some(child) = slot else {
            continue;
        };
        if let Some(status) = take_deferred_slot(child, resolved) {
            match status {
                Ok(cached) => updated_slots[idx] = Some(cached),
                Err((wrapper, child_meta)) => pending.push((idx, wrapper, child_meta)),
            }
        }
    }
    (updated_slots, pending)
}

fn push_rebuild_chain(
    pending_frames: &mut DeferPending,
    parent: ArrayRef,
    mut updated_slots: Vec<Option<ArrayRef>>,
    mut direct_pending: Vec<(usize, ArrayRef, DeferredMeta)>,
    stage: DeferStage,
    resolved: &mut HashMap<usize, (ArrayRef, ArrayRef)>,
) -> Option<DeferStep> {
    if direct_pending.is_empty() {
        let has_deferred_entries = DEFERRED_PUSHDOWN.with(|cell| {
            cell.borrow()
                .as_ref()
                .is_some_and(|reg| !reg.entries.is_empty())
        });
        if has_deferred_entries && matches!(stage, DeferStage::PostChildren { .. }) {
            let mut mid_info = None;
            for (idx, slot) in updated_slots.iter().enumerate() {
                let Some(child) = slot else {
                    continue;
                };
                if child.optimized_state().is_optimized() {
                    continue;
                }
                let (child_slots, child_pending) = collect_slot_replacements(child, resolved);
                if !child_pending.is_empty()
                    || !child_slots
                        .iter()
                        .zip(child.slots().iter())
                        .all(|(lhs, rhs)| match (lhs, rhs) {
                            (Some(left_arr), Some(right_arr)) => {
                                ArrayRef::ptr_eq(left_arr, right_arr)
                            }
                            (None, None) => true,
                            _ => false,
                        })
                {
                    mid_info = Some((idx, child.clone(), child_slots, child_pending));
                    break;
                }
            }
            if let Some((mid_idx, mid_child, mid_slots, mut mid_pending)) = mid_info {
                if mid_pending.is_empty() {
                    if let Ok(rebuilt_mid) =
                        unsafe { mid_child.with_slots(mid_slots.into_iter().collect()) }
                    {
                        updated_slots[mid_idx] = Some(rebuilt_mid);
                        let rebuilt = unsafe {
                            parent.with_slots(updated_slots.into_iter().collect())
                        }
                        .ok()?;
                        if let DeferStage::PostChildren {
                            completion: Some((orig_addr, orig_wrapper, meta, cache_shared)),
                        } = stage
                        {
                            return Some(DeferStep::Complete(finish_deferred_node(
                                orig_addr,
                                orig_wrapper,
                                rebuilt,
                                meta,
                                cache_shared,
                                resolved,
                            )));
                        }
                        return Some(DeferStep::Complete(rebuilt));
                    }
                    return None;
                }
                mid_pending.reverse();
                let (first_mid_slot, first_w, first_meta) =
                    mid_pending.pop().vortex_expect("checked non-empty");
                pending_frames.0.push(DeferRebuildFrame {
                    parent,
                    updated_slots,
                    cur_slot: mid_idx,
                    pending: Vec::new(),
                    stage,
                });
                pending_frames.0.push(DeferRebuildFrame {
                    parent: mid_child,
                    updated_slots: mid_slots,
                    cur_slot: first_mid_slot,
                    pending: mid_pending,
                    stage: DeferStage::PostChildren { completion: None },
                });
                return Some(DeferStep::Pre {
                    cache_shared: false,
                    orig_wrapper: first_w.clone(),
                    node: first_w,
                    meta: first_meta,
                });
            }
        }
        return None;
    }

    direct_pending.reverse();
    let (first_slot, first_w, first_meta) =
        direct_pending.pop().vortex_expect("checked non-empty");
    pending_frames.0.push(DeferRebuildFrame {
        parent,
        updated_slots,
        cur_slot: first_slot,
        pending: direct_pending,
        stage,
    });
    Some(DeferStep::Pre {
        cache_shared: false,
        orig_wrapper: first_w.clone(),
        node: first_w,
        meta: first_meta,
    })
}

fn finish_deferred_node(
    orig_addr: usize,
    orig_wrapper: ArrayRef,
    result: ArrayRef,
    meta: DeferredMeta,
    cache_shared: bool,
    resolved: &mut HashMap<usize, (ArrayRef, ArrayRef)>,
) -> ArrayRef {
    if let DeferredMeta::Slice { source } = meta {
        inherit_slice_stats(&source, &result);
    }
    if cache_shared && !ArrayRef::ptr_eq(&result, &orig_wrapper) {
        resolved.insert(orig_addr, (orig_wrapper, result.clone()));
    }
    result
}

pub(crate) fn optimize_deferred_root(
    root: ArrayRef,
    root_meta: DeferredMeta,
) -> VortexResult<ArrayRef> {
    let _guard = DeferGuard::install();
    let mut frames = DeferPending::default();
    let mut resolved: HashMap<usize, (ArrayRef, ArrayRef)> = HashMap::new();
    let cache_shared = root.is_shared();
    let mut step = DeferStep::Pre {
        orig_wrapper: root.clone(),
        node: root,
        meta: root_meta,
        cache_shared,
    };

    loop {
        step = match step {
            DeferStep::Pre {
                orig_wrapper,
                node,
                meta,
                cache_shared,
            } => {
                let orig_addr = orig_wrapper.addr();
                let (updated_slots, pending) = collect_slot_replacements(&node, &resolved);
                let changed_slots = !updated_slots.iter().zip(node.slots().iter()).all(|(lhs, rhs)| {
                    match (lhs, rhs) {
                        (Some(left_arr), Some(right_arr)) => ArrayRef::ptr_eq(left_arr, right_arr),
                        (None, None) => true,
                        _ => false,
                    }
                });
                if !pending.is_empty() {
                    push_rebuild_chain(
                        &mut frames,
                        node,
                        updated_slots,
                        pending,
                        DeferStage::PreChildren {
                            orig_addr,
                            orig_wrapper,
                            meta,
                            cache_shared,
                        },
                        &mut resolved,
                    )
                    .vortex_expect("non-empty pending slots always yield a step")
                } else {
                    let node = if changed_slots {
                        unsafe { node.with_slots(updated_slots.into_iter().collect()) }?
                    } else {
                        node
                    };
                    DeferStep::Rewrite {
                        orig_addr,
                        orig_wrapper,
                        node,
                        meta,
                        cache_shared,
                    }
                }
            }
            DeferStep::Rewrite {
                orig_addr,
                orig_wrapper,
                mut node,
                meta,
                cache_shared,
            } => {
                trace_op!(record_optimize_start(&node, false));
                let mut pass = 0usize;
                loop {
                    if pass > MAX_OPTIMIZER_REWRITE_PASS {
                        vortex_bail!(
                            "Exceeded maximum optimization iterations (possible infinite loop)"
                        );
                    }
                    trace_op!(record_optimize_loop_start(&node));
                    let mut rewritten = node.reduce()?;
                    if rewritten.is_none() {
                        trace_op!(record_optimize_reduce_none(&node));
                        for (slot_idx, slot) in node.slots().iter().enumerate() {
                            let Some(child) = slot else {
                                continue;
                            };
                            if let Some(new_array) = child.reduce_parent(&node, slot_idx)? {
                                rewritten = Some(new_array);
                                break;
                            }
                        }
                    }
                    let Some(rewritten) = rewritten else {
                        trace_op!(record_optimize_parent_reduce_none(&node));
                        trace_op!(record_optimize_loop_end());
                        trace_op!(record_optimize_done(&node, pass > 0));
                        break;
                    };
                    node = rewritten;
                    trace_op!(record_optimize_loop_end());
                    pass += 1;
                }

                let (updated_slots, pending) = collect_slot_replacements(&node, &resolved);
                let changed_slots = !updated_slots.iter().zip(node.slots().iter()).all(|(lhs, rhs)| {
                    match (lhs, rhs) {
                        (Some(left_arr), Some(right_arr)) => ArrayRef::ptr_eq(left_arr, right_arr),
                        (None, None) => true,
                        _ => false,
                    }
                });
                let next_parent = if changed_slots && pending.is_empty() {
                    unsafe { node.with_slots(updated_slots.clone().into_iter().collect()) }?
                } else {
                    node
                };
                if let Some(child_step) = push_rebuild_chain(
                    &mut frames,
                    next_parent.clone(),
                    updated_slots,
                    pending,
                    DeferStage::PostChildren {
                        completion: Some((
                            orig_addr,
                            orig_wrapper.clone(),
                            match &meta {
                                DeferredMeta::Plain => DeferredMeta::Plain,
                                DeferredMeta::Slice { source } => DeferredMeta::Slice {
                                    source: source.clone(),
                                },
                            },
                            cache_shared,
                        )),
                    },
                    &mut resolved,
                ) {
                    child_step
                } else {
                    DeferStep::Complete(finish_deferred_node(
                        orig_addr,
                        orig_wrapper,
                        next_parent,
                        meta,
                        cache_shared,
                        &mut resolved,
                    ))
                }
            }
            DeferStep::Complete(complete) => {
                let Some(mut frame) = frames.0.pop() else {
                    return Ok(complete);
                };
                frame.updated_slots[frame.cur_slot] = Some(complete);
                let mut next_child = None;
                while let Some((slot_idx, wrapper, child_meta)) = frame.pending.pop() {
                    if let Some((cached_orig, cached_result)) = resolved.get(&wrapper.addr())
                        && ArrayRef::ptr_eq(cached_orig, &wrapper)
                    {
                        frame.updated_slots[slot_idx] = Some(cached_result.clone());
                        continue;
                    }
                    next_child = Some((slot_idx, wrapper, child_meta));
                    break;
                }
                if let Some((slot_idx, wrapper, child_meta)) = next_child {
                    frame.cur_slot = slot_idx;
                    frames.0.push(frame);
                    DeferStep::Pre {
                        cache_shared: wrapper.is_shared(),
                        orig_wrapper: wrapper.clone(),
                        node: wrapper,
                        meta: child_meta,
                    }
                } else {
                    let rebuilt = unsafe {
                        frame
                            .parent
                            .with_slots(frame.updated_slots.into_iter().collect())
                    }?;
                    match frame.stage {
                        DeferStage::PreChildren {
                            orig_addr,
                            orig_wrapper,
                            meta,
                            cache_shared,
                        } => DeferStep::Rewrite {
                            orig_addr,
                            orig_wrapper,
                            node: rebuilt,
                            meta,
                            cache_shared,
                        },
                        DeferStage::PostChildren { completion } => match completion {
                            Some((orig_addr, orig_wrapper, meta, cache_shared)) => {
                                DeferStep::Complete(finish_deferred_node(
                                    orig_addr,
                                    orig_wrapper,
                                    rebuilt,
                                    meta,
                                    cache_shared,
                                    &mut resolved,
                                ))
                            }
                            None => DeferStep::Complete(rebuilt),
                        },
                    }
                }
            }
        };
    }
}

pub(super) fn optimize_root(
    optimizer: &mut Optimizer<'_>,
    array: ArrayRef,
) -> VortexResult<ArrayRef> {
    let _depth = DepthGuard::enter();
    let work = begin_root(optimizer, array);
    run(optimizer, work)
}

pub(super) fn optimize_array(
    optimizer: &mut Optimizer<'_>,
    array: &ArrayRef,
) -> VortexResult<ArrayRef> {
    let work = begin_array(optimizer, array);
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
    optimizer: &mut Optimizer<'_>,
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
