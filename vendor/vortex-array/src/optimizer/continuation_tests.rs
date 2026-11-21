// SPDX-License-Identifier: Apache-2.0
// ShardLoom tests for original optimizer semantics and stack-independent traversal.

use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use vortex_buffer::buffer;
use vortex_error::{VortexResult, vortex_bail};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::kernels::{ArrayKernels, ReduceParentFn};
use super::{ArrayOptimizer, Optimizer, reference};
use crate::array::vtable::{NotSupported, ValidityVTable, with_empty_buffers};
use crate::arrays::{FixedSizeListArray, ListArray, PrimitiveArray, SliceArray, StructArray};
use crate::buffer::BufferHandle;
use crate::dtype::{DType, Nullability, PType};
use crate::expr::stats::{Precision, Stat, StatsProvider};
use crate::scalar::ScalarValue;
use crate::serde::ArrayChildren;
use crate::test_harness::trace::{TraceOptions, TraceResolution, trace_op_with};
use crate::validity::Validity;
use crate::{
    Array, ArrayEq, ArrayHash, ArrayId, ArrayParts, ArrayRef, ArraySlots, ArrayView, EqMode,
    ExecutionCtx, ExecutionResult, IntoArray, VTable, VortexSessionExecute,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Keep,
    Peel,
    Countdown(u16),
    Build,
    AwaitChild,
    AwaitParent,
    Error,
    Panic,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Event {
    label: u16,
    kind: &'static str,
    slot: usize,
    shared: bool,
    children: Vec<Option<bool>>,
}

#[derive(Debug, Default)]
struct Ledger {
    events: Mutex<Vec<Event>>,
    live: AtomicUsize,
    quiet: bool,
}

#[derive(Debug)]
struct Owner(Arc<Ledger>);

impl Drop for Owner {
    fn drop(&mut self) {
        assert!(self.0.live.fetch_sub(1, Ordering::Relaxed) > 0);
    }
}

#[derive(Clone, Debug)]
struct Data {
    label: u16,
    action: Action,
    owner: Arc<Owner>,
}

impl Display for Data {
    #[allow(clippy::use_debug)] // Test rule state belongs in fixture diagnostics.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node {} {:?}", self.label, self.action)
    }
}

impl ArrayHash for Data {
    fn array_hash<H: Hasher>(&self, state: &mut H, _mode: EqMode) {
        (self.label, self.action).hash(state);
    }
}

impl ArrayEq for Data {
    fn array_eq(&self, other: &Self, _mode: EqMode) -> bool {
        (self.label, self.action) == (other.label, other.action)
    }
}

#[derive(Clone, Debug)]
struct Node;

fn node(ledger: &Arc<Ledger>, label: u16, action: Action, slots: ArraySlots) -> ArrayRef {
    ledger.live.fetch_add(1, Ordering::Relaxed);
    Array::try_from_parts(ArrayParts::new(
        Node,
        DType::Primitive(PType::I32, Nullability::NonNullable),
        3,
        Data {
            label,
            action,
            owner: Arc::new(Owner(Arc::clone(ledger))),
        },
        slots,
    ))
    .unwrap()
    .into_array()
}

fn record(array: &ArrayRef, kind: &'static str, slot: usize) {
    let data = array.as_::<Node>().data();
    let ledger = &data.owner.0;
    if !ledger.quiet {
        ledger.events.lock().push(Event {
            label: data.label,
            kind,
            slot,
            shared: array.is_shared(),
            children: array
                .slots()
                .iter()
                .map(|child| {
                    child
                        .as_ref()
                        .map(|child| child.optimized_state().is_optimized())
                })
                .collect(),
        });
    }
}

impl ValidityVTable<Node> for Node {
    fn validity(_array: ArrayView<'_, Node>) -> VortexResult<Validity> {
        Ok(Validity::NonNullable)
    }
}

impl VTable for Node {
    type TypedArrayData = Data;
    type OperationsVTable = NotSupported;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.test.optimizer-continuation");
        *ID
    }

    fn validate(
        &self,
        _data: &Data,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        assert_eq!(
            dtype,
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(len, 3);
        for child in slots.iter().flatten() {
            assert_eq!(child.dtype(), dtype);
            assert_eq!(child.len(), len);
        }
        Ok(())
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }
    fn buffer(_array: ArrayView<'_, Self>, _idx: usize) -> BufferHandle {
        unreachable!()
    }
    fn buffer_name(_array: ArrayView<'_, Self>, _idx: usize) -> Option<String> {
        None
    }
    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
    fn deserialize(
        &self,
        _dtype: &DType,
        _len: usize,
        _metadata: &[u8],
        _buffers: &[BufferHandle],
        _children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_bail!("test node is not serialized")
    }
    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }
    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        format!("child{idx}")
    }

    fn execute(array: Array<Self>, _ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        Ok(ExecutionResult::done(
            array
                .slots()
                .iter()
                .flatten()
                .next()
                .cloned()
                .unwrap_or_else(|| buffer![1i32, 2, 3].into_array()),
        ))
    }

    fn reduce(array: ArrayView<'_, Self>) -> VortexResult<Option<ArrayRef>> {
        record(array.array(), "reduce", 0);
        let data = array.data();
        let ledger = &data.owner.0;
        Ok(match data.action {
            Action::Peel => array.slots().iter().flatten().next().cloned(),
            Action::Countdown(count) if count > 0 => Some(node(
                ledger,
                data.label,
                Action::Countdown(count - 1),
                array.slots().iter().cloned().collect(),
            )),
            Action::Build => {
                let child = node(
                    ledger,
                    data.label + 2,
                    Action::Countdown(1),
                    array.slots().iter().cloned().collect(),
                );
                Some(node(
                    ledger,
                    data.label + 1,
                    Action::Keep,
                    [Some(child)].into_iter().collect(),
                ))
            }
            Action::AwaitChild
                if array.slots().iter().flatten().next().is_some_and(|child| {
                    child
                        .as_opt::<Node>()
                        .is_some_and(|child| child.data().action == Action::Countdown(0))
                }) =>
            {
                array.slots().iter().flatten().next().cloned()
            }
            Action::Error => vortex_bail!("optimizer fixture error {}", data.label),
            Action::Panic => panic!("optimizer fixture panic {}", data.label),
            _ => None,
        })
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        slot: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        record(array.array(), "static-parent", slot);
        let Some(parent) = parent.as_opt::<Node>() else {
            return Ok(None);
        };
        if parent.data().action == Action::AwaitParent {
            return Ok(Some(node(
                &parent.data().owner.0,
                300,
                Action::Keep,
                parent.slots().iter().cloned().collect(),
            )));
        }
        Ok(None)
    }
}

fn session_decline(
    child: &ArrayRef,
    _parent: &ArrayRef,
    slot: usize,
) -> VortexResult<Option<ArrayRef>> {
    record(child, "session-decline", slot);
    Ok(None)
}

fn session_rewrite(
    child: &ArrayRef,
    parent: &ArrayRef,
    slot: usize,
) -> VortexResult<Option<ArrayRef>> {
    record(child, "session-rewrite", slot);
    let parent = parent.as_::<Node>();
    if parent.data().action == Action::AwaitParent {
        return Ok(Some(node(
            &parent.data().owner.0,
            200,
            Action::Keep,
            parent.slots().iter().cloned().collect(),
        )));
    }
    Ok(None)
}

fn kernels() -> ArrayKernels {
    let kernels = ArrayKernels::default();
    kernels.register_reduce_parent(
        Node.id(),
        Node.id(),
        &[
            session_decline as ReduceParentFn,
            session_rewrite as ReduceParentFn,
        ],
    );
    kernels
}

fn fixture(case: usize, ledger: &Arc<Ledger>) -> ArrayRef {
    let leaf = || node(ledger, 1, Action::Keep, ArraySlots::new());
    let step = || {
        node(
            ledger,
            2,
            Action::Countdown(2),
            [Some(leaf())].into_iter().collect(),
        )
    };
    match case {
        0 => leaf(),
        1 => node(
            ledger,
            10,
            Action::Keep,
            [None, Some(step()), None, Some(leaf())]
                .into_iter()
                .collect(),
        ),
        2 => {
            let shared = step();
            node(
                ledger,
                10,
                Action::Keep,
                [Some(shared.clone()), None, Some(shared)]
                    .into_iter()
                    .collect(),
            )
        }
        3 => node(
            ledger,
            10,
            Action::Peel,
            [
                Some(leaf()),
                Some(node(ledger, 99, Action::Error, ArraySlots::new())),
            ]
            .into_iter()
            .collect(),
        ),
        4 => node(
            ledger,
            10,
            Action::Build,
            [Some(step())].into_iter().collect(),
        ),
        5 => node(
            ledger,
            10,
            Action::AwaitChild,
            [Some(step())].into_iter().collect(),
        ),
        6 => node(
            ledger,
            10,
            Action::AwaitParent,
            [Some(leaf()), Some(step())].into_iter().collect(),
        ),
        7 => node(
            ledger,
            10,
            Action::Keep,
            (0..257).map(|n| (n % 3 != 0).then(step)).collect(),
        ),
        8 => node(
            ledger,
            10,
            Action::Keep,
            [
                Some(step()),
                Some(node(ledger, 99, Action::Error, ArraySlots::new())),
            ]
            .into_iter()
            .collect(),
        ),
        9 => node(
            ledger,
            10,
            Action::Keep,
            [
                Some(node(ledger, 98, Action::Error, ArraySlots::new())),
                Some(step()),
            ]
            .into_iter()
            .collect(),
        ),
        10 => node(
            ledger,
            10,
            Action::Countdown(100),
            [Some(leaf())].into_iter().collect(),
        ),
        11 => node(
            ledger,
            10,
            Action::Countdown(101),
            [Some(leaf())].into_iter().collect(),
        ),
        _ => unreachable!(),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct GraphNode {
    encoding: String,
    dtype: String,
    len: usize,
    optimized: bool,
    data: Option<(u16, Action)>,
    sorted: String,
    children: Vec<Option<usize>>,
}

fn graph(array: &ArrayRef) -> Vec<GraphNode> {
    let mut arrays = vec![array.clone()];
    let mut result = Vec::new();
    let mut index = 0;
    while index < arrays.len() {
        let array = arrays[index].clone();
        let children = array
            .slots()
            .iter()
            .map(|slot| {
                slot.as_ref().map(|child| {
                    if let Some(index) = arrays
                        .iter()
                        .position(|other| ArrayRef::ptr_eq(other, child))
                    {
                        index
                    } else {
                        arrays.push(child.clone());
                        arrays.len() - 1
                    }
                })
            })
            .collect();
        result.push(GraphNode {
            encoding: array.encoding_id().to_string(),
            dtype: array.dtype().to_string(),
            len: array.len(),
            optimized: array.optimized_state().is_optimized(),
            data: array
                .as_opt::<Node>()
                .map(|node| (node.data().label, node.data().action)),
            sorted: format!("{:?}", array.statistics().get(Stat::IsSorted)),
            children,
        });
        index += 1;
    }
    result
}

enum Runner<'a> {
    Current(Optimizer<'a>),
    Original(reference::Optimizer<'a>),
}
impl Runner<'_> {
    fn run(&mut self, root: &ArrayRef, session: bool) -> VortexResult<ArrayRef> {
        match (self, session) {
            (Self::Current(runner), true) => runner.optimize_array(root),
            (Self::Current(runner), false) => runner.optimize_root(root.clone()),
            (Self::Original(runner), true) => runner.optimize_array(root),
            (Self::Original(runner), false) => runner.optimize_root(root.clone()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    output: Result<Vec<GraphNode>, String>,
    events: Vec<Event>,
    trace: String,
    values: Option<Result<Vec<i32>, String>>,
    reused: Option<bool>,
}

fn observe(case: usize, session: bool, shared: bool, original: bool) -> Observation {
    let ledger = Arc::new(Ledger::default());
    let input = fixture(case, &ledger);
    input
        .statistics()
        .set(Stat::IsSorted, Precision::Exact(ScalarValue::from(true)));
    let alias = shared.then(|| input.clone());
    let kernels = kernels();
    let active = session.then_some(&kernels);
    let mut runner = if original {
        Runner::Original(reference::Optimizer::new(active))
    } else {
        Runner::Current(Optimizer::new(active))
    };
    let traced = trace_op_with(
        TraceOptions {
            resolution: TraceResolution::Attempts,
        },
        || {
            Ok(runner
                .run(&input, session)
                .map_err(|error| error.to_string()))
        },
    )
    .unwrap();
    let trace = traced.trace.to_string();
    let output = traced.output;
    let graph = output.as_ref().map(graph).map_err(Clone::clone);
    let reused = output.as_ref().ok().and_then(|output| {
        session.then(|| ArrayRef::ptr_eq(output, &runner.run(&input, session).unwrap()))
    });
    let events = ledger.events.lock().clone();
    let values = output.as_ref().ok().map(|array| {
        let mut ctx = VortexSession::empty().create_execution_ctx();
        array
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map(|primitive| primitive.as_slice::<i32>().to_vec())
            .map_err(|error| error.to_string())
    });
    drop(output);
    drop(runner);
    drop(alias);
    drop(input);
    drop(traced.trace);
    assert_eq!(
        ledger.live.load(Ordering::Relaxed),
        0,
        "case={case} session={session} shared={shared} original={original}"
    );
    Observation {
        output: graph,
        events,
        trace,
        values,
        reused,
    }
}

#[test]
fn optimizer_continuations_match_original_rules_graphs_sharing_marks_errors_and_traces() {
    for case in 0..12 {
        for session in [false, true] {
            for shared in [false, true] {
                let expected = observe(case, session, shared, true);
                let actual = observe(case, session, shared, false);
                assert_eq!(
                    actual, expected,
                    "case={case} session={session} shared={shared}"
                );
                if case == 11 {
                    assert!(
                        actual
                            .output
                            .unwrap_err()
                            .contains("Exceeded maximum optimization iterations")
                    );
                } else if ![8, 9].contains(&case) {
                    assert_eq!(actual.values, Some(Ok(vec![1, 2, 3])));
                }
            }
        }
    }
}

fn native_chain(shape: usize, depth: usize, mut array: ArrayRef) -> ArrayRef {
    let session = VortexSession::empty();
    let mut ctx = session.create_execution_ctx();
    for level in 0..depth {
        array = match if shape == 3 { level % 3 } else { shape } {
            0 => StructArray::from_fields(&[("child", array)])
                .unwrap()
                .into_array(),
            1 => ListArray::try_new(
                array.clone(),
                buffer![0u64, array.len() as u64].into_array(),
                Validity::NonNullable,
                &mut ctx,
            )
            .unwrap()
            .into_array(),
            _ => FixedSizeListArray::try_new(
                array.clone(),
                u32::try_from(array.len()).unwrap(),
                Validity::NonNullable,
                1,
            )
            .unwrap()
            .into_array(),
        };
    }
    array
}

#[test]
fn optimizer_continuations_match_original_native_compound_rewrites() {
    for shape in 0..4 {
        for depth in [0, 1, 4, 24] {
            let build = || {
                native_chain(
                    shape,
                    depth,
                    SliceArray::try_new(buffer![1i32, 2, 3, 4].into_array(), 1..3)
                        .unwrap()
                        .into_array(),
                )
            };
            let kernels = kernels();
            let old = reference::Optimizer::new(Some(&kernels))
                .optimize_array(&build())
                .unwrap();
            let new = Optimizer::new(Some(&kernels))
                .optimize_array(&build())
                .unwrap();
            assert_eq!(graph(&new), graph(&old));
            crate::assert_arrays_eq!(new, old, &mut VortexSession::empty().create_execution_ctx());
        }
    }
}

fn deep_child() {
    const DEPTH: usize = 8192;
    let session = VortexSession::empty();
    for shape in 0..4 {
        for changed in [false, true] {
            let leaf = buffer![1i32, 2, 3].into_array();
            let leaf = if changed {
                SliceArray::try_new(leaf, 1..2).unwrap().into_array()
            } else {
                leaf
            };
            let input = native_chain(shape, DEPTH, leaf);
            let output = input.optimize_ctx(&session).unwrap();
            assert_eq!(ArrayRef::ptr_eq(&input, &output), !changed);
            let mut pending = vec![output.clone()];
            let mut nodes = 0;
            while let Some(array) = pending.pop() {
                assert!(array.optimized_state().is_optimized());
                pending.extend(array.slots().iter().flatten().cloned());
                nodes += 1;
            }
            assert!(nodes > DEPTH);
            drop(input);
            drop(output);
        }
    }
    for failure in [Action::Error, Action::Panic] {
        let ledger = Arc::new(Ledger {
            quiet: true,
            ..Ledger::default()
        });
        let mut valid = node(&ledger, 1, Action::Countdown(1), ArraySlots::new());
        let mut invalid = node(&ledger, 99, failure, ArraySlots::new());
        for _ in 0..DEPTH {
            valid = node(
                &ledger,
                2,
                Action::Keep,
                [Some(valid)].into_iter().collect(),
            );
            invalid = node(
                &ledger,
                3,
                Action::Keep,
                [Some(invalid)].into_iter().collect(),
            );
        }
        let input = node(
            &ledger,
            4,
            Action::Keep,
            [Some(valid), Some(invalid)].into_iter().collect(),
        );
        let result = catch_unwind(AssertUnwindSafe(|| input.optimize_ctx(&session)));
        if failure == Action::Error {
            assert!(
                result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("optimizer fixture error 99")
            );
        } else {
            assert!(result.is_err());
        }
        drop(input);
        assert_eq!(ledger.live.load(Ordering::Relaxed), 0);
    }
    println!("OPTIMIZER_DEEP_COMPLETE");
}

#[test]
fn optimizer_continuations_complete_unprepared_native_depth_errors_and_unwind_on_small_stack() {
    const ENV: &str = "VORTEX_OPTIMIZER_CONTINUATION_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap()).args([
        "--exact", "optimizer::continuation_tests::optimizer_continuations_complete_unprepared_native_depth_errors_and_unwind_on_small_stack", "--nocapture",
    ]).env(ENV, "1").output().unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("OPTIMIZER_DEEP_COMPLETE"));
}
