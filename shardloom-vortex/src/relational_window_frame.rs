//! Declarative analytic frames; binding resolves offsets against native types.

use shardloom_core::{ColumnRef, ScalarValue};

/// The coordinate system used by a window frame's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRelationalFrameUnit {
    Rows,
    Groups,
    Range,
}

/// Literal offsets are bound before row execution. Durations are fixed elapsed
/// microseconds, with whole days required for a Date32 ordering key.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalFrameOffset {
    Number(ScalarValue),
    DurationMicros(u64),
}

/// Inclusive SQL frame endpoints. Execution uses checked half-open intervals.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexRelationalFrameBound {
    UnboundedPreceding,
    Preceding(VortexRelationalFrameOffset),
    CurrentRow,
    Following(VortexRelationalFrameOffset),
    UnboundedFollowing,
}

/// Rows removed after computing the frame bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VortexRelationalFrameExclusion {
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

/// Default SQL frames include all preceding rows and the current ordering peers.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRelationalWindowFrame {
    pub unit: VortexRelationalFrameUnit,
    pub start: VortexRelationalFrameBound,
    pub end: VortexRelationalFrameBound,
    pub exclusion: VortexRelationalFrameExclusion,
}

impl Default for VortexRelationalWindowFrame {
    fn default() -> Self {
        Self {
            unit: VortexRelationalFrameUnit::Range,
            start: VortexRelationalFrameBound::UnboundedPreceding,
            end: VortexRelationalFrameBound::CurrentRow,
            exclusion: VortexRelationalFrameExclusion::NoOthers,
        }
    }
}

/// Framed reductions and value selection. Computed arguments are ordinary native
/// projection columns, shared with grouped reductions and other expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexRelationalFrameFunction {
    CountAll,
    Count(ColumnRef),
    CountDistinct(ColumnRef),
    Sum(ColumnRef),
    Avg(ColumnRef),
    Min(ColumnRef),
    Max(ColumnRef),
    FirstValue(ColumnRef),
    LastValue(ColumnRef),
    NthValue { column: ColumnRef, index: u64 },
}

impl VortexRelationalFrameFunction {
    /// Input demand for projection pruning; COUNT(*) has no measure column.
    #[must_use]
    pub fn column(&self) -> Option<&ColumnRef> {
        match self {
            Self::CountAll => None,
            Self::Count(column)
            | Self::CountDistinct(column)
            | Self::Sum(column)
            | Self::Avg(column)
            | Self::Min(column)
            | Self::Max(column)
            | Self::FirstValue(column)
            | Self::LastValue(column)
            | Self::NthValue { column, .. } => Some(column),
        }
    }
}
