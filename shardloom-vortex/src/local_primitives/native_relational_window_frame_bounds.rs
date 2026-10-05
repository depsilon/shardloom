//! Monotone frame endpoints and exact RANGE comparisons in native key domains.

use super::{Bound, Frame, Offset, RangeOffset};
use crate::{
    local_primitives::{
        native_relational_batch::{Table, failed},
        native_relational_keys::Cell,
    },
    relational_query::{
        VortexRelationalFrameExclusion as Exclusion, VortexRelationalFrameUnit as Unit,
        VortexRelationalNullOrder as NullOrder,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use std::{cmp::Ordering, ops::Range};
use vortex::array::scalar::DecimalValue;

#[derive(Default)]
pub(in super::super) struct Cursor {
    start: usize,
    end: usize,
}

pub(in super::super) struct Position<'a> {
    pub(in super::super) rows: &'a [usize],
    /// Every peer start, followed by the partition length.
    pub(in super::super) peers: &'a [usize],
    pub(in super::super) peer: usize,
    pub(in super::super) row: usize,
}

impl Cursor {
    pub(in super::super) fn advance(
        &mut self,
        frame: &Frame,
        at: &Position<'_>,
        table: &Table,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Range<usize>> {
        let start = endpoint(frame, frame.start, false, self.start, at, table, context)?;
        let end = endpoint(frame, frame.end, true, self.end, at, table, context)?;
        if start < self.start || end < self.end {
            return Err(failed("window frame endpoints moved backwards"));
        }
        self.start = start;
        self.end = end;
        Ok(start..end.max(start))
    }
}

fn endpoint(
    frame: &Frame,
    bound: Bound,
    end: bool,
    cursor: usize,
    at: &Position<'_>,
    table: &Table,
    context: &NativeExecutionContext<'_>,
) -> Result<usize> {
    let peer_edge = || at.peers[at.peer + usize::from(end)];
    Ok(match bound {
        Bound::UnboundedPreceding => 0,
        Bound::UnboundedFollowing => at.rows.len(),
        Bound::CurrentRow => {
            if frame.unit == Unit::Rows {
                at.row + usize::from(end)
            } else {
                peer_edge()
            }
        }
        Bound::Preceding(Offset::Count(count)) | Bound::Following(Offset::Count(count)) => {
            let current = if frame.unit == Unit::Rows {
                at.row
            } else {
                at.peer
            };
            let position = if matches!(bound, Bound::Preceding(_)) {
                current.checked_sub(count)
            } else {
                Some(current.saturating_add(count))
            };
            match position {
                None => 0,
                Some(position) => {
                    let edge = position.saturating_add(usize::from(end));
                    if frame.unit == Unit::Rows {
                        edge.min(at.rows.len())
                    } else {
                        at.peers.get(edge).copied().unwrap_or(at.rows.len())
                    }
                }
            }
        }
        Bound::Preceding(Offset::Range(offset)) | Bound::Following(Offset::Range(offset)) => {
            let order = frame
                .order
                .as_ref()
                .ok_or_else(|| failed("RANGE frame has no ordering key"))?;
            if table.key_is_null(at.rows[at.row], order.key)? {
                return Ok(peer_edge());
            }
            let current = table.raw_cell(at.rows[at.row], order.key)?;
            let subtract = matches!(bound, Bound::Preceding(_)) ^ order.descending;
            let mut cursor = cursor;
            while cursor < at.rows.len() {
                if cursor.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let comparison = if table.key_is_null(at.rows[cursor], order.key)? {
                    if order.nulls == Some(NullOrder::First) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    let candidate = table.raw_cell(at.rows[cursor], order.key)?;
                    // Compare candidate to the shifted current-row boundary.
                    let value = compare_shifted(&current, &candidate, offset, subtract)?.reverse();
                    if order.descending {
                        value.reverse()
                    } else {
                        value
                    }
                };
                if comparison == Ordering::Greater || (!end && comparison == Ordering::Equal) {
                    break;
                }
                cursor += 1;
            }
            cursor
        }
    })
}

pub(in super::super) fn intervals(
    range: Range<usize>,
    at: &Position<'_>,
    exclusion: Exclusion,
) -> [Range<usize>; 3] {
    let clamp = |position: usize| position.clamp(range.start, range.end);
    match exclusion {
        Exclusion::NoOthers => [range.clone(), range.end..range.end, range.end..range.end],
        Exclusion::CurrentRow => [
            range.start..clamp(at.row),
            clamp(at.row + 1)..range.end,
            range.end..range.end,
        ],
        Exclusion::Group | Exclusion::Ties => [
            range.start..clamp(at.peers[at.peer]),
            clamp(at.row)..if exclusion == Exclusion::Ties {
                clamp(at.row + 1)
            } else {
                clamp(at.row)
            },
            clamp(at.peers[at.peer + 1])..range.end,
        ],
    }
}

fn integer(value: &Cell) -> Result<i128> {
    match value {
        Cell::NegativeInteger(value) | Cell::Timestamp(value) => Ok(i128::from(*value)),
        Cell::NonnegativeInteger(value) => Ok(i128::from(*value)),
        Cell::Date(value) => Ok(i128::from(*value)),
        _ => Err(failed(
            "RANGE integer ordering domain changed after binding",
        )),
    }
}

fn compare_shifted(
    current: &Cell,
    candidate: &Cell,
    offset: RangeOffset,
    subtract: bool,
) -> Result<Ordering> {
    match offset {
        RangeOffset::Integer(offset)
        | RangeOffset::Date(offset)
        | RangeOffset::Timestamp(offset) => {
            // Two 64-bit endpoints plus one unsigned 64-bit distance fit i128.
            let offset = if subtract {
                -i128::from(offset)
            } else {
                i128::from(offset)
            };
            Ok((integer(current)? - integer(candidate)? + offset).cmp(&0))
        }
        RangeOffset::Float(offset) => {
            let (Cell::Float(current), Cell::Float(candidate)) = (current, candidate) else {
                return Err(failed(
                    "RANGE floating ordering domain changed after binding",
                ));
            };
            let current = f64::from_bits(*current);
            // RANGE uses the F64 boundary arithmetic of its ordering domain.
            // An infinite boundary is a comparison sentinel, never an admitted
            // observation or result value. All operands were checked finite.
            let boundary = current + if subtract { -offset } else { offset };
            boundary
                .partial_cmp(&f64::from_bits(*candidate))
                .ok_or_else(|| failed("RANGE floating boundary is unordered"))
        }
        RangeOffset::Decimal { value, scale } => {
            let (Cell::Decimal(current, dtype), Cell::Decimal(candidate, other_dtype)) =
                (current, candidate)
            else {
                return Err(failed(
                    "RANGE decimal ordering domain changed after binding",
                ));
            };
            if dtype != other_dtype {
                return Err(failed("RANGE decimal ordering metadata changed"));
            }
            let source_scale =
                u8::try_from(dtype.scale()).map_err(|_| failed("invalid RANGE decimal scale"))?;
            let common = source_scale.max(scale);
            let widen = |value: i128| DecimalValue::I256(DecimalValue::I128(value).as_i256());
            let value_factor = DecimalValue::I128(10i128.pow(u32::from(common - source_scale)));
            let offset_factor = DecimalValue::I128(10i128.pow(u32::from(common - scale)));
            let difference = widen(*current)
                .checked_sub(&widen(*candidate))
                .and_then(|value| value.checked_mul(&value_factor))
                .ok_or_else(|| failed("wide RANGE decimal difference overflow"))?;
            let offset = widen(if subtract { -value } else { value })
                .checked_mul(&offset_factor)
                .ok_or_else(|| failed("wide RANGE decimal offset overflow"))?;
            let difference = difference
                .checked_add(&offset)
                .ok_or_else(|| failed("wide RANGE decimal boundary overflow"))?;
            difference
                .partial_cmp(&widen(0))
                .ok_or_else(|| failed("wide RANGE decimal comparison failed"))
        }
    }
}
