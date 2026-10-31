//! Test-only disjoint caller-thread scopes; no per-row timing or shipping hooks.

use std::{cell::RefCell, marker::PhantomData, rc::Rc, time::Instant};

pub(crate) const PHASES: [&str; 10] = [
    "source_intake",
    "group_preparation",
    "group_interning",
    "state_reservation",
    "measure_preparation",
    "typed_updates",
    "other_updates",
    "aggregate_remainder",
    "finalization",
    "downstream_excluded",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum Phase {
    SourceIntake,
    GroupPreparation,
    GroupInterning,
    StateReservation,
    MeasurePreparation,
    TypedUpdates,
    OtherUpdates,
    AggregateRemainder,
    Finalization,
    DownstreamExcluded,
}

#[derive(Debug, Default)]
pub(crate) struct Report {
    pub(crate) nanos: [u128; 10],
    pub(crate) calls: [u64; 10],
}

#[derive(Default)]
struct State {
    report: Report,
    stack: [Option<Phase>; 16],
    depth: usize,
    last: u128,
}

impl State {
    fn settle(&mut self, now: u128) {
        assert!(now >= self.last, "diagnostic clock moved backwards");
        if self.depth != 0 {
            let phase = self.stack[self.depth - 1].expect("active diagnostic scope");
            self.report.nanos[phase as usize] += now - self.last;
        }
        self.last = now;
    }

    fn enter(&mut self, phase: Phase, now: u128) {
        assert!(self.depth < self.stack.len(), "diagnostic nesting limit");
        self.settle(now);
        self.stack[self.depth] = Some(phase);
        self.depth += 1;
        self.report.calls[phase as usize] += 1;
    }

    fn leave(&mut self, phase: Phase, now: u128) {
        assert!(self.depth > 0, "diagnostic scope is absent");
        assert_eq!(
            self.stack[self.depth - 1],
            Some(phase),
            "diagnostic scope order"
        );
        self.settle(now);
        self.depth -= 1;
        self.stack[self.depth] = None;
    }
}

struct Active {
    started: Instant,
    state: State,
}

thread_local! {
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
}

pub(crate) struct Session(PhantomData<Rc<()>>);

impl Session {
    pub(crate) fn start() -> Self {
        ACTIVE.with_borrow_mut(|active| {
            assert!(active.is_none(), "grouped diagnostics cannot be nested");
            *active = Some(Active {
                started: Instant::now(),
                state: State::default(),
            });
        });
        Self(PhantomData)
    }

    pub(crate) fn finish(self) -> Report {
        let state =
            ACTIVE.with_borrow_mut(|active| active.take().expect("active diagnostic").state);
        assert_eq!(state.depth, 0, "unfinished diagnostic scopes");
        drop(self);
        state.report
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|active| *active = None);
    }
}

pub(crate) struct Scope(Option<Phase>, PhantomData<Rc<()>>);

pub(crate) fn scope(phase: Phase) -> Scope {
    let entered = ACTIVE.with_borrow_mut(|active| {
        active.as_mut().map(|active| {
            active
                .state
                .enter(phase, active.started.elapsed().as_nanos());
            phase
        })
    });
    Scope(entered, PhantomData)
}

impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(phase) = self.0 {
            ACTIVE.with_borrow_mut(|active| {
                if let Some(active) = active {
                    active
                        .state
                        .leave(phase, active.started.elapsed().as_nanos());
                }
            });
        }
    }
}

#[test]
fn grouped_clock_scopes_are_exact_and_disjoint() {
    let mut state = State::default();
    assert_eq!(PHASES.len(), state.report.nanos.len());
    state.enter(Phase::AggregateRemainder, 10);
    state.enter(Phase::GroupInterning, 20);
    state.enter(Phase::GroupPreparation, 25);
    state.leave(Phase::GroupPreparation, 40);
    state.leave(Phase::GroupInterning, 50);
    state.leave(Phase::AggregateRemainder, 60);
    assert_eq!(state.report.nanos[Phase::AggregateRemainder as usize], 20);
    assert_eq!(state.report.nanos[Phase::GroupInterning as usize], 15);
    assert_eq!(state.report.nanos[Phase::GroupPreparation as usize], 15);
    assert_eq!(state.report.nanos.iter().sum::<u128>(), 50);
    assert_eq!(state.report.calls.iter().sum::<u64>(), 3);
    assert_eq!(state.depth, 0);
}

#[test]
fn grouped_clock_is_idle_without_a_session_and_resets_after_drop() {
    drop(scope(Phase::SourceIntake));
    let session = Session::start();
    drop(scope(Phase::SourceIntake));
    let report = session.finish();
    assert_eq!(report.calls[Phase::SourceIntake as usize], 1);
    assert_eq!(report.calls.iter().sum::<u64>(), 1);
    drop(Session::start());
    let session = Session::start();
    assert_eq!(session.finish().calls, [0; 10]);
}

#[test]
fn grouped_clock_error_unwinding_closes_scopes() {
    fn fail() -> Result<(), ()> {
        let _outer = scope(Phase::AggregateRemainder);
        let _inner = scope(Phase::TypedUpdates);
        Err(())
    }
    let session = Session::start();
    assert!(fail().is_err());
    assert_eq!(session.finish().calls.iter().sum::<u64>(), 2);
}
