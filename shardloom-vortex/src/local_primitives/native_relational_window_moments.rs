//! Stable range moments without subtracting departed observations. Each interval
//! retains only its aligned power-of-two block cover. Whole old blocks merge;
//! uncovered fragments read through the existing resident/spilled input owner.
//! A monotone sliding interval rereads a row at most logarithmically many times
//! while retaining logarithmically many charged summaries, independent of width.

use super::{Function, Input, Spec};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_relational_aggregate::Function as Aggregate,
        native_relational_batch::failed,
        native_relational_keys::Cell,
        native_statistical_moments::{Moments, PairedMoments},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::ops::Range;

#[derive(Clone, Copy)]
enum Summary {
    Single(Moments),
    Pair(PairedMoments),
}

impl Summary {
    fn new(function: Aggregate) -> Self {
        if function.is_pair() {
            Self::Pair(PairedMoments::default())
        } else {
            Self::Single(Moments::default())
        }
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        match (self, other) {
            (Self::Single(left), Self::Single(right)) => left.merge(right),
            (Self::Pair(left), Self::Pair(right)) => left.merge(right),
            _ => Err(failed("statistical window summary changed input arity")),
        }
    }

    fn observe(
        &mut self,
        primary: &Cell,
        secondary: Option<&Cell>,
        policy: shardloom_core::FloatPolicy,
    ) -> Result<()> {
        match self {
            Self::Single(value) => value.observe_with_policy(primary, policy),
            Self::Pair(value) => value.observe_with_policy(
                primary,
                secondary.ok_or_else(|| failed("statistical window companion is absent"))?,
                policy,
            ),
        }
    }

    fn finish(self, function: Aggregate) -> Result<Option<f64>> {
        match self {
            Self::Single(value) => value.finish(function.sample(), function.standard_deviation()),
            Self::Pair(value) => value.finish(function.sample(), function == Aggregate::Corr),
        }
    }
}

struct Block {
    range: Range<usize>,
    summary: Summary,
}

struct Cover {
    current: ReservedVec<Block>,
    next: ReservedVec<Block>,
}

impl Cover {
    fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            current: ReservedVec::new(memory)?,
            next: ReservedVec::new(memory)?,
        })
    }

    fn advance(
        &mut self,
        range: &Range<usize>,
        function: Aggregate,
        spec: &Spec,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Summary> {
        self.next.values.clear();
        let mut total = Summary::new(function);
        let mut position = range.start;
        while position < range.end {
            context.check_cancelled()?;
            let remaining = range.end - position;
            let exponent = position
                .trailing_zeros()
                .min(usize::BITS - 1 - remaining.leading_zeros());
            let end = position
                .checked_add(1usize << exponent)
                .ok_or_else(|| failed("statistical window block endpoint overflow"))?;
            let mut summary = Summary::new(function);
            let start = position;
            // Covers are ordered and disjoint. Reuse only complete old blocks;
            // their summaries do not authorize subtraction or partial reuse.
            for prior in &self.current.values {
                if prior.range.start >= end {
                    break;
                }
                if prior.range.start < position || prior.range.end > end {
                    continue;
                }
                observe(
                    &mut summary,
                    position..prior.range.start,
                    spec,
                    input,
                    context,
                )?;
                summary.merge(prior.summary)?;
                position = prior.range.end;
            }
            observe(&mut summary, position..end, spec, input, context)?;
            total.merge(summary)?;
            self.next.push(Block {
                range: start..end,
                summary,
            })?;
            position = end;
        }
        std::mem::swap(&mut self.current, &mut self.next);
        self.next.values.clear();
        Ok(total)
    }
}

fn observe(
    summary: &mut Summary,
    rows: Range<usize>,
    spec: &Spec,
    input: &mut impl Input,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let key = spec
        .key
        .ok_or_else(|| failed("statistical window input key is absent"))?;
    for (offset, row) in rows.enumerate() {
        if offset.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !super::admitted(row, spec, input, context)? {
            continue;
        }
        if let Some(secondary) = spec.secondary_key
            && (input.key_is_null(row, key, context)?
                || input.key_is_null(row, secondary, context)?)
        {
            continue;
        }
        let primary = input.raw_cell(row, key, context)?;
        let secondary = spec
            .secondary_key
            .map(|key| input.raw_cell(row, key, context))
            .transpose()?;
        summary.observe(&primary, secondary.as_ref(), context.float_policy())?;
    }
    Ok(())
}

pub(super) struct Ranges {
    covers: [Cover; 3],
    _ownership: MemoryLease,
}

impl Ranges {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Box<Self>> {
        let ownership = memory.reserve(
            u64::try_from(std::mem::size_of::<Self>())
                .map_err(|_| failed("statistical window state size overflow"))?,
        )?;
        Ok(Box::new(Self {
            covers: [
                Cover::new(memory)?,
                Cover::new(memory)?,
                Cover::new(memory)?,
            ],
            _ownership: ownership,
        }))
    }

    pub(super) fn advance(
        &mut self,
        ranges: &[Range<usize>; 3],
        spec: &Spec,
        input: &mut impl Input,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<f64>> {
        let Function::Aggregate(function) = spec.function else {
            return Err(failed("statistical window state has no aggregate function"));
        };
        let mut summary = Summary::new(function);
        for (cover, range) in self.covers.iter_mut().zip(ranges) {
            summary.merge(cover.advance(range, function, spec, input, context)?)?;
        }
        summary.finish(function)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational_query::{VortexRelationalFrameExclusion, VortexRelationalFrameUnit};
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;

    struct InputRows {
        reads: usize,
    }
    impl Input for InputRows {
        fn raw_cell(
            &mut self,
            position: usize,
            _key: usize,
            _context: &NativeExecutionContext<'_>,
        ) -> Result<Cell> {
            self.reads += 1;
            Ok(Cell::NonnegativeInteger(
                u64::MAX - 8192 + u64::try_from(position).unwrap(),
            ))
        }
        fn key_is_null(
            &mut self,
            _position: usize,
            _key: usize,
            _context: &NativeExecutionContext<'_>,
        ) -> Result<bool> {
            unreachable!()
        }
        fn hash_key(
            &mut self,
            _position: usize,
            _key: usize,
            _context: &NativeExecutionContext<'_>,
        ) -> Result<Option<u64>> {
            unreachable!()
        }
        fn compare_key(
            &mut self,
            _left: usize,
            _right: usize,
            _key: usize,
            _context: &NativeExecutionContext<'_>,
        ) -> Result<std::cmp::Ordering> {
            unreachable!()
        }
    }

    #[test]
    fn statistical_window_cover_has_bounded_state_and_avoids_reobserving_each_full_frame() {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let spec = Spec {
                    decimal_output: None,
                    separator: None,
                    function: Function::Aggregate(Aggregate::VarPop),
                    column: Some("x".into()),
                    key: Some(0),
                    secondary_key: None,
                    filter_key: None,
                    quantile_level: None,
                    selection_order: vec![],
                    decimal: None,
                    frame: super::super::Frame {
                        unit: VortexRelationalFrameUnit::Rows,
                        start: super::super::Bound::UnboundedPreceding,
                        end: super::super::Bound::CurrentRow,
                        exclusion: VortexRelationalFrameExclusion::NoOthers,
                        order: None,
                    },
                };
                let mut state = Ranges::new(context.memory())?;
                let mut input = InputRows { reads: 0 };
                for end in 1usize..=4096 {
                    let start = end.saturating_sub(1024);
                    let expected =
                        f64::from(u32::try_from((end - start).pow(2) - 1).unwrap()) / 12.0;
                    let actual = state
                        .advance(&[start..end, 0..0, 0..0], &spec, &mut input, context)?
                        .unwrap();
                    assert!((actual - expected).abs() < 1e-10 * expected.max(1.0));
                    assert!(state.covers[0].current.values.len() <= 2 * usize::BITS as usize);
                }
                assert!(
                    input.reads < 4096 * 16,
                    "{} observed input rows",
                    input.reads
                );
                assert!(context.memory().snapshot().reserved_bytes < 128 << 10);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
