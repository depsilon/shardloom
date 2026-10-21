// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: provide lazy statistics entries so native
// footer tables can reserve capacity before construction.
// ShardLoom: bounded running file statistics; preserve unknowns and overflow.

use std::future;
use std::sync::Arc;

use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::nan_count::nan_count;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::expr::stats::Precision;
use vortex_array::expr::stats::Stat;
use vortex_array::scalar::Scalar;
use vortex_array::scalar::ScalarTruncation;
use vortex_array::scalar::ScalarValue;
use vortex_array::scalar::lower_bound;
use vortex_array::scalar::upper_bound;
use vortex_array::stats::StatsSet;
use vortex_buffer::BufferString;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

use crate::sequence::SendableSequentialStream;
use crate::sequence::SequenceId;
use crate::sequence::SequentialStreamAdapter;
use crate::sequence::SequentialStreamExt;

#[cfg(test)]
#[path = "file_stats_tests.rs"]
mod tests;

pub fn accumulate_stats(
    stream: SendableSequentialStream,
    stats: Arc<[Stat]>,
    max_variable_length_statistics_size: usize,
    session: &VortexSession,
) -> (FileStatsAccumulator, SendableSequentialStream) {
    let accumulator = FileStatsAccumulator::new(
        stream.dtype(),
        stats,
        max_variable_length_statistics_size,
        session,
    );
    let stream = SequentialStreamAdapter::new(
        stream.dtype().clone(),
        stream.scan(accumulator.clone(), |acc, item| {
            future::ready(Some(acc.process(item)))
        }),
    )
    .sendable();
    (accumulator, stream)
}

/// One scalar per selected statistic, independent of the number of input chunks.
struct StatsAccumulator {
    dtype: DType,
    stats: Vec<RunningStat>,
    max_variable_length_statistics_size: usize,
}

struct RunningStat {
    stat: Stat,
    // None means no contributing values yet. Some(empty) means an unknown
    // observation, which must never be replaced by a later partial result.
    accumulated: Option<StatsSet>,
}

impl StatsAccumulator {
    fn new(dtype: &DType, stats: &[Stat], max_variable_length_statistics_size: usize) -> Self {
        let stats = stats
            .iter()
            .copied()
            .filter(|stat| supported_stat(dtype, *stat))
            .unique()
            .map(|stat| RunningStat {
                stat,
                accumulated: None,
            })
            .collect();
        Self {
            dtype: dtype.clone(),
            stats,
            max_variable_length_statistics_size,
        }
    }

    fn push_chunk(&mut self, array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        if array.is_empty() {
            return Ok(());
        }
        let mut no_extrema = None;
        for running in &mut self.stats {
            if running.accumulated.as_ref().is_some_and(StatsSet::is_empty) {
                continue;
            }
            let scalar = if running.stat == Stat::NullCount {
                // Propagate a failed validity computation; the generic helper
                // currently converts that error to an absent statistic.
                Some(Scalar::from(array.invalid_count(ctx)? as u64))
            } else {
                array.statistics().compute_stat(running.stat, ctx)?
            };
            if matches!(running.stat, Stat::Min | Stat::Max)
                && scalar.as_ref().is_none_or(Scalar::is_null)
            {
                let no_extrema = match no_extrema {
                    Some(value) => value,
                    None => {
                        let valid = array.valid_count(ctx)?;
                        let value = valid == 0
                            || (array.dtype().is_float() && nan_count(array, ctx)? == valid);
                        no_extrema = Some(value);
                        value
                    }
                };
                if no_extrema {
                    continue;
                }
            }
            let value = scalar
                .map(|scalar| {
                    compact_stat(
                        scalar,
                        running.stat,
                        self.max_variable_length_statistics_size,
                    )
                })
                .transpose()?
                .unwrap_or(Precision::Absent);
            let mut next = StatsSet::default();
            if !matches!(value, Precision::Absent) {
                next.set(running.stat, value);
            }
            let mut accumulated = match running.accumulated.take() {
                Some(previous) => previous.merge_ordered(&next, &self.dtype),
                None => next,
            };
            // The general merge can derive unrelated flags. Keep only the
            // requested statistic, including a truly empty unknown state.
            accumulated.retain_only(&[running.stat]);
            running.accumulated = Some(accumulated);
        }
        Ok(())
    }

    fn as_stats_set(&self) -> StatsSet {
        let mut result = StatsSet::default();
        for running in &self.stats {
            if let Some(values) = &running.accumulated {
                for (stat, value) in values.iter() {
                    result.set(*stat, value.clone());
                }
            }
        }
        result
    }
}

fn supported_stat(dtype: &DType, stat: Stat) -> bool {
    if matches!(dtype, DType::Variant(_)) || stat.dtype(dtype).is_none() {
        return false;
    }
    match stat {
        Stat::Min | Stat::Max => match dtype {
            DType::Bool(_)
            | DType::Primitive(..)
            | DType::Decimal(..)
            | DType::Utf8(_)
            | DType::Binary(_) => true,
            // Bound retained scalar state. Nested and variable-length extension
            // storage needs a declared ordering/truncation contract first.
            DType::Extension(extension) => matches!(
                extension.storage_dtype(),
                DType::Bool(_) | DType::Primitive(..) | DType::Decimal(..)
            ),
            _ => false,
        },
        Stat::Sum | Stat::NullCount | Stat::NaNCount | Stat::UncompressedSizeInBytes => true,
        Stat::IsConstant | Stat::IsSorted | Stat::IsStrictSorted => false,
    }
}

fn compact_stat(
    value: Scalar,
    stat: Stat,
    max_length: usize,
) -> VortexResult<Precision<ScalarValue>> {
    let nullability = value.dtype().nullability();
    let bounded = match (stat, value.dtype()) {
        (Stat::Min, DType::Utf8(_)) => {
            lower_bound(BufferString::from_scalar(value)?, max_length, nullability)
        }
        (Stat::Max, DType::Utf8(_)) => {
            upper_bound(BufferString::from_scalar(value)?, max_length, nullability)
        }
        (Stat::Min, DType::Binary(_)) => {
            lower_bound(ByteBuffer::from_scalar(value)?, max_length, nullability)
        }
        (Stat::Max, DType::Binary(_)) => {
            upper_bound(ByteBuffer::from_scalar(value)?, max_length, nullability)
        }
        _ => Some((value, false)),
    };
    let Some((scalar, truncated)) = bounded else {
        return Ok(Precision::Absent);
    };
    let Some(value) = scalar.into_value() else {
        return Ok(Precision::Absent);
    };
    // A small slice can still own an entire input allocation. Copy only the
    // bounded scalar, so a running extremum cannot pin previous input batches.
    let value = match value {
        ScalarValue::Utf8(value) => ScalarValue::Utf8(BufferString::from(&*value)),
        ScalarValue::Binary(value) => ScalarValue::Binary(ByteBuffer::copy_from(value.as_slice())),
        value => value,
    };
    Ok(if truncated {
        Precision::inexact(value)
    } else {
        Precision::exact(value)
    })
}

/// An array stream processor that computes aggregate statistics for all fields.
#[derive(Clone)]
pub struct FileStatsAccumulator {
    stats: Arc<[Stat]>,
    accumulators: Arc<Mutex<Vec<StatsAccumulator>>>,
    ctx: Arc<Mutex<ExecutionCtx>>,
}

impl FileStatsAccumulator {
    fn new(
        dtype: &DType,
        stats: Arc<[Stat]>,
        max_variable_length_statistics_size: usize,
        session: &VortexSession,
    ) -> Self {
        // File statistics have no representation for the root validity mask.
        // Preserve nullable records without publishing unmasked child bounds.
        // The optional optimization stays absent until that contract is defined.
        let requested = !stats.is_empty();
        let stats = if dtype.is_struct() && dtype.is_nullable() {
            Arc::from([])
        } else {
            stats
        };
        let accumulators = if !requested {
            Vec::new()
        } else {
            match dtype.as_struct_fields_opt() {
                Some(fields) => fields
                    .fields()
                    .map(|dtype| {
                        StatsAccumulator::new(&dtype, &stats, max_variable_length_statistics_size)
                    })
                    .collect(),
                None => vec![StatsAccumulator::new(
                    dtype,
                    &stats,
                    max_variable_length_statistics_size,
                )],
            }
        };
        Self {
            stats,
            accumulators: Arc::new(Mutex::new(accumulators)),
            ctx: Arc::new(Mutex::new(session.create_execution_ctx())),
        }
    }

    fn process(
        &self,
        chunk: VortexResult<(SequenceId, ArrayRef)>,
    ) -> VortexResult<(SequenceId, ArrayRef)> {
        let (sequence_id, chunk) = chunk?;
        if self.stats.is_empty() {
            return Ok((sequence_id, chunk));
        }
        let mut ctx = self.ctx.lock();
        if chunk.dtype().is_struct() {
            let struct_chunk = chunk.clone().execute::<StructArray>(&mut ctx)?;
            for (acc, field) in self
                .accumulators
                .lock()
                .iter_mut()
                .zip_eq(struct_chunk.iter_unmasked_fields())
            {
                acc.push_chunk(field, &mut ctx)?;
            }
        } else {
            self.accumulators.lock()[0].push_chunk(&chunk, &mut ctx)?;
        }
        Ok((sequence_id, chunk))
    }

    pub fn stats_sets(&self) -> Vec<StatsSet> {
        self.stats_sets_iter().collect()
    }

    /// Produce statistics lazily while retaining the existing accumulator lock.
    ///
    /// Creating this iterator allocates no statistics table. Native consumers can
    /// admit its exact length before producing entries. Drop it before awaiting
    /// other work; the lock is retained until the iterator is dropped.
    pub fn stats_sets_iter(&self) -> impl ExactSizeIterator<Item = StatsSet> + '_ {
        let accumulators = self.accumulators.lock();
        (0..accumulators.len()).map(move |index| accumulators[index].as_stats_set())
    }
}
