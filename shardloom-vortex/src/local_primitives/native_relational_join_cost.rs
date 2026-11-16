//! Test-only disjoint caller-thread attribution. No row-loop or shipping hooks.

use std::{cell::RefCell, marker::PhantomData, rc::Rc, time::Instant};

pub(crate) const ENGINE_PHASES: usize = 12;
pub(crate) const PHASES: [&str; 16] = [
    "setup",
    "build_input_pipeline",
    "build_detach",
    "build_key_preparation",
    "build_retention_and_index",
    "adaptive_seal",
    "probe_input_pipeline",
    "probe_key_preparation",
    "probe_matching_and_pairs",
    "output_gather_and_validation",
    "downstream_consumer",
    "outer_completion",
    "json_sink_excluded",
    "ordered_strategy_excluded",
    "asof_strategy_excluded",
    "cyclic_strategy_excluded",
];
// The declared diagnostic has at most two joins; keep bounded nesting headroom
// without a large stack frame or allocations inside measured execution.
const MAX_NODES: usize = 32;
const MAX_DEPTH: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum Phase {
    Setup,
    BuildInput,
    BuildDetach,
    BuildKeys,
    BuildState,
    Seal,
    ProbeInput,
    ProbeKeys,
    ProbeMatch,
    Payload,
    Downstream,
    OuterFinish,
    SinkExcluded,
    OrderedExcluded,
    AsOfExcluded,
    CyclicExcluded,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NodeReport {
    pub(crate) parent: Option<usize>,
    pub(crate) kind: &'static str,
    pub(crate) key_columns: usize,
    pub(crate) membership_only: bool,
    pub(crate) conditional: bool,
    pub(crate) nanos: [u128; 16],
    pub(crate) calls: [u64; 16],
    pub(crate) build_batches: u64,
    pub(crate) build_rows: u64,
    pub(crate) probe_batches: u64,
    pub(crate) probe_rows: u64,
    pub(crate) output_batches: u64,
    pub(crate) output_rows: u64,
    pub(crate) output_cells: u64,
    pub(crate) validation_only_cells: u64,
}

#[derive(Debug)]
pub(crate) struct Report {
    pub(crate) nodes: [NodeReport; MAX_NODES],
    pub(crate) payload_work: [PayloadWork; MAX_NODES],
    pub(crate) node_count: usize,
    pub(crate) excluded_nanos: [u128; 16],
    pub(crate) excluded_calls: [u64; 16],
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PayloadWork {
    pub(crate) coordinate_batches: u64,
    pub(crate) coordinate_rows: u64,
    pub(crate) coordinate_chunks: u64,
    pub(crate) direct_numeric_batches: u64,
    pub(crate) direct_numeric_rows: u64,
    pub(crate) direct_numeric_chunks: u64,
    pub(crate) direct_numeric_single_chunk_rows: u64,
    pub(crate) selected_numeric_copy_batches: u64,
    pub(crate) selected_numeric_copy_rows: u64,
    pub(crate) variable_copy_rows: u64,
    pub(crate) scalar_copy_rows: u64,
    pub(crate) dictionary_copy_rows: u64,
    pub(crate) dictionary_copy_values: u64,
}

impl Default for Report {
    fn default() -> Self {
        Self {
            nodes: [NodeReport::default(); MAX_NODES],
            payload_work: [PayloadWork::default(); MAX_NODES],
            node_count: 0,
            excluded_nanos: [0; 16],
            excluded_calls: [0; 16],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Frame {
    node: Option<usize>,
    phase: Phase,
}

struct State {
    report: Report,
    stack: [Option<Frame>; MAX_DEPTH],
    depth: usize,
    last: u128,
}

impl Default for State {
    fn default() -> Self {
        Self {
            report: Report::default(),
            stack: [None; MAX_DEPTH],
            depth: 0,
            last: 0,
        }
    }
}

impl State {
    fn current(&self) -> Option<Frame> {
        self.depth
            .checked_sub(1)
            .and_then(|index| self.stack[index])
    }

    fn settle(&mut self, now: u128) {
        assert!(now >= self.last, "join diagnostic clock moved backwards");
        if let Some(frame) = self.current() {
            let nanos = if let Some(node) = frame.node {
                &mut self.report.nodes[node].nanos
            } else {
                &mut self.report.excluded_nanos
            };
            nanos[frame.phase as usize] += now - self.last;
        }
        self.last = now;
    }

    fn enter(&mut self, frame: Frame, now: u128) {
        assert!(self.depth < MAX_DEPTH, "join diagnostic nesting limit");
        assert_eq!(frame.node.is_some(), (frame.phase as usize) < ENGINE_PHASES);
        self.settle(now);
        self.stack[self.depth] = Some(frame);
        self.depth += 1;
        let calls = if let Some(node) = frame.node {
            assert!(node < self.report.node_count);
            &mut self.report.nodes[node].calls
        } else {
            &mut self.report.excluded_calls
        };
        calls[frame.phase as usize] += 1;
    }

    fn leave(&mut self, frame: Frame, now: u128) {
        assert_eq!(self.current(), Some(frame), "join diagnostic scope order");
        self.settle(now);
        self.depth -= 1;
        self.stack[self.depth] = None;
    }
}

struct Active {
    generation: u64,
    started: Instant,
    state: State,
}

#[derive(Default)]
struct Local {
    generation: u64,
    active: Option<Active>,
}

thread_local! {
    static LOCAL: RefCell<Local> = RefCell::new(Local::default());
}

pub(crate) struct Session(u64, PhantomData<Rc<()>>);

impl Session {
    pub(crate) fn start() -> Self {
        let generation = LOCAL.with_borrow_mut(|local| {
            assert!(local.active.is_none(), "join diagnostics cannot be nested");
            local.generation = local
                .generation
                .checked_add(1)
                .expect("diagnostic generation");
            local.active = Some(Active {
                generation: local.generation,
                started: Instant::now(),
                state: State::default(),
            });
            local.generation
        });
        Self(generation, PhantomData)
    }

    pub(crate) fn finish(self) -> Report {
        let state = LOCAL.with_borrow_mut(|local| {
            let active = local.active.take().expect("active join diagnostic");
            assert_eq!(active.generation, self.0);
            active.state
        });
        assert_eq!(state.depth, 0, "unfinished join diagnostic scopes");
        state.report
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        LOCAL.with_borrow_mut(|local| {
            if local
                .active
                .as_ref()
                .is_some_and(|active| active.generation == self.0)
            {
                local.active = None;
            }
        });
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Node(Option<(u64, usize)>, PhantomData<Rc<()>>);

pub(crate) fn node(
    kind: &'static str,
    key_columns: usize,
    membership_only: bool,
    conditional: bool,
) -> Node {
    let identity = LOCAL.with_borrow_mut(|local| {
        let active = local.active.as_mut()?;
        let state = &mut active.state;
        let id = state.report.node_count;
        assert!(id < MAX_NODES, "join diagnostic node limit");
        state.report.nodes[id] = NodeReport {
            parent: state.current().and_then(|frame| frame.node),
            kind,
            key_columns,
            membership_only,
            conditional,
            ..NodeReport::default()
        };
        state.report.node_count += 1;
        Some((active.generation, id))
    });
    Node(identity, PhantomData)
}

impl Node {
    pub(crate) fn scope(self, phase: Phase) -> Scope {
        enter(|active| {
            let (generation, id) = self.0?;
            (generation == active.generation).then_some(Frame {
                node: Some(id),
                phase,
            })
        })
    }

    pub(crate) fn input(self, build: bool, rows: usize) {
        LOCAL.with_borrow_mut(|local| {
            if let Some(active) = &mut local.active
                && let Some((generation, id)) = self.0
                && generation == active.generation
            {
                let report = &mut active.state.report.nodes[id];
                if build {
                    report.build_batches += 1;
                    report.build_rows += rows as u64;
                } else {
                    report.probe_batches += 1;
                    report.probe_rows += rows as u64;
                }
            }
        });
    }
}

pub(crate) struct Scope(Option<(u64, Frame)>, PhantomData<Rc<()>>);

fn enter(frame: impl FnOnce(&Active) -> Option<Frame>) -> Scope {
    let entered = LOCAL.with_borrow_mut(|local| {
        let active = local.active.as_mut()?;
        let frame = frame(active)?;
        active
            .state
            .enter(frame, active.started.elapsed().as_nanos());
        Some((active.generation, frame))
    });
    Scope(entered, PhantomData)
}

pub(crate) fn scope(phase: Phase) -> Scope {
    enter(|active| {
        if (phase as usize) >= ENGINE_PHASES {
            Some(Frame { node: None, phase })
        } else {
            Some(Frame {
                node: Some(active.state.current()?.node?),
                phase,
            })
        }
    })
}

pub(crate) fn key_preparation() -> Scope {
    enter(|active| {
        let current = active.state.current()?;
        let phase = match current.phase {
            Phase::BuildState => Phase::BuildKeys,
            Phase::ProbeMatch => Phase::ProbeKeys,
            _ => return None,
        };
        Some(Frame {
            node: current.node,
            phase,
        })
    })
}

pub(crate) fn output(rows: usize, retained: usize, validated: usize) {
    LOCAL.with_borrow_mut(|local| {
        if let Some(active) = &mut local.active
            && let Some(id) = active.state.current().and_then(|frame| frame.node)
        {
            let report = &mut active.state.report.nodes[id];
            report.output_batches += 1;
            report.output_rows += rows as u64;
            report.output_cells += (rows * retained) as u64;
            report.validation_only_cells += (rows * validated) as u64;
        }
    });
}

fn payload_record(update: impl FnOnce(&mut PayloadWork)) {
    LOCAL.with_borrow_mut(|local| {
        if let Some(active) = &mut local.active
            && let Some(Frame {
                node: Some(id),
                phase: Phase::Payload,
            }) = active.state.current()
        {
            update(&mut active.state.report.payload_work[id]);
        }
    });
}

pub(crate) fn gathered_coordinates(rows: usize, chunks: usize) {
    payload_record(|work| {
        work.coordinate_batches += 1;
        work.coordinate_rows += rows as u64;
        work.coordinate_chunks += chunks as u64;
    });
}

pub(crate) fn direct_numeric_gather(rows: usize, chunks: usize) {
    payload_record(|work| {
        work.direct_numeric_batches += 1;
        work.direct_numeric_rows += rows as u64;
        work.direct_numeric_chunks += chunks as u64;
        if chunks == 1 {
            work.direct_numeric_single_chunk_rows += rows as u64;
        }
    });
}

pub(crate) fn selected_numeric_copy(rows: usize) {
    payload_record(|work| {
        work.selected_numeric_copy_batches += 1;
        work.selected_numeric_copy_rows += rows as u64;
    });
}

pub(crate) fn leaf_copy(rows: usize, variable: bool) {
    payload_record(|work| {
        if variable {
            work.variable_copy_rows += rows as u64;
        } else {
            work.scalar_copy_rows += rows as u64;
        }
    });
}

pub(crate) fn dictionary_copy(rows: usize, values: usize) {
    payload_record(|work| {
        work.dictionary_copy_rows += rows as u64;
        work.dictionary_copy_values += values as u64;
    });
}

impl Drop for Scope {
    fn drop(&mut self) {
        if let Some((generation, frame)) = self.0 {
            LOCAL.with_borrow_mut(|local| {
                if let Some(active) = &mut local.active
                    && generation == active.generation
                {
                    active
                        .state
                        .leave(frame, active.started.elapsed().as_nanos());
                }
            });
        }
    }
}

#[path = "native_relational_join_cost_tests.rs"]
mod tests;
