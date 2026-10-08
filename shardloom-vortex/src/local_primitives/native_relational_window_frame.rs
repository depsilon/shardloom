//! Bound analytic-frame policy and moving state over existing native row owners.

#[path = "native_relational_window_frame_bounds.rs"]
mod bounds;
#[path = "native_relational_window_frame_input.rs"]
mod input;
#[path = "native_relational_window_frame_state.rs"]
mod state;

use super::{SimpleAggregateFunction, native_relational_window::OrderKey};
use crate::relational_query::{VortexRelationalFrameExclusion, VortexRelationalFrameUnit};
use vortex::array::dtype::DecimalDType;

pub(super) use bounds::{Cursor, Position, intervals, validate_intervals};
pub(super) use input::{Input, Resident};
pub(super) use state::{State, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Function {
    Aggregate(SimpleAggregateFunction),
    First,
    Last,
    Nth(usize),
}

pub(super) struct Spec {
    pub(super) function: Function,
    pub(super) column: Option<String>,
    pub(super) key: Option<usize>,
    pub(super) decimal: Option<DecimalDType>,
    pub(super) frame: Frame,
}

pub(super) struct Frame {
    pub(super) unit: VortexRelationalFrameUnit,
    pub(super) start: Bound,
    pub(super) end: Bound,
    pub(super) exclusion: VortexRelationalFrameExclusion,
    pub(super) order: Option<OrderKey>,
}

#[derive(Clone, Copy)]
pub(super) enum Bound {
    UnboundedPreceding,
    Preceding(Offset),
    CurrentRow,
    Following(Offset),
    UnboundedFollowing,
}

#[derive(Clone, Copy)]
pub(super) enum Offset {
    Count(usize),
    Range(RangeOffset),
}

#[derive(Clone, Copy)]
pub(super) enum RangeOffset {
    Integer(u64),
    Float(f64),
    Decimal { value: i128, scale: u8 },
    Date(u64),
    Timestamp(u64),
}
