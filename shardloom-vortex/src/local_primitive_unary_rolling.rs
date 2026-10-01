//! A bounded window uses the existing native rolling semantics and flushes its
//! ready values synchronously. Centered lookahead never retains the whole input.

use super::{
    BATCH_ROWS, DType, NativeBatch, NativeExecutionContext, Nullability, PreparedVortexUnary,
    ReservedVec, Result, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;

pub(super) fn dtype(request: &VortexQueryPrimitiveRequest) -> Result<DType> {
    use vortex::array::dtype::PType;
    let request = super::super::required_rolling_window(request)?;
    Ok(DType::Primitive(
        if request.aggregate == "count" {
            PType::U64
        } else {
            PType::F64
        },
        Nullability::NonNullable,
    ))
}

pub(super) struct Rolling {
    state: super::super::RollingWindowState,
    input: [Vec<super::StatValue>; 1],
    pending: ReservedVec<super::StatValue>,
    produced: usize,
    peak_items: usize,
    stopped: bool,
    _state_memory: MemoryLease,
}

impl Rolling {
    pub(super) fn usage(&self) -> super::report::StateUsage {
        super::report::StateUsage {
            items: self.peak_items,
            all_input_retained: false,
        }
    }
    pub(super) fn new(
        plan: &PreparedVortexUnary,
        context: &NativeExecutionContext<'_>,
        source_rows: u64,
    ) -> Result<Self> {
        let request = super::super::required_rolling_window(&plan.request)?;
        // One-row feeding bounds lookahead to one window. Cover growth overlap
        // and the provider's temporary end-of-input result vector before either
        // allocates. The pending output vector has its own capacity owner.
        let capacity = request
            .window_size
            .min(usize::try_from(source_rows).map_err(vortex_error)?)
            .checked_add(1)
            .ok_or_else(|| failed("rolling capacity overflow"))?;
        let bytes = capacity
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| failed("rolling reservation overflow"))?;
        let memory = context
            .memory()
            .reserve(u64::try_from(bytes).map_err(vortex_error)?)?;
        let mut values = std::collections::VecDeque::new();
        values.try_reserve_exact(capacity).map_err(vortex_error)?;
        if values.capacity() > capacity.saturating_mul(2) {
            return Err(failed("rolling storage exceeded its reservation"));
        }
        Ok(Self {
            state: super::super::RollingWindowState {
                values,
                sum: 0.0,
                valid_count: 0,
                center_seen_rows: 0,
                center_next_output_row: 0,
                center_buffer_start_row: 0,
            },
            input: [Vec::with_capacity(1)],
            pending: ReservedVec::new(context.memory())?,
            produced: 0,
            peak_items: 0,
            stopped: false,
            _state_memory: memory,
        })
    }

    pub(super) fn consume(
        &mut self,
        plan: &PreparedVortexUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let request = super::super::required_rolling_window(&plan.request)?;
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            // COUNT needs validity only, so avoid retaining or cloning a string.
            let value = batch.value(plan.output_indices[0], row)?;
            let stat = if matches!(value, Value::Null) {
                super::StatValue::Null
            } else if request.aggregate == "count" {
                super::StatValue::UInt64(1)
            } else {
                match value {
                    Value::Int(v) => super::StatValue::Int64(v),
                    Value::UInt(v) => super::StatValue::UInt64(v),
                    Value::Float(v) => super::StatValue::Float64(v),
                    _ => return Err(failed("rolling numeric aggregate requires numeric input")),
                }
            };
            self.input[0].clear();
            self.input[0].push(stat);
            // Count live observations, including centered lookahead before it
            // is trimmed. Reserved container capacity is separate byte evidence.
            self.peak_items = self
                .peak_items
                .max((self.state.values.len() + 1).min(request.window_size));
            let ready = super::super::rolling_window_values(
                &self.input,
                request,
                &mut self.state,
                1,
                false,
            )?;
            self.deliver(plan, ready, output)?;
            if self.stopped {
                break;
            }
        }
        self.flush(output)?;
        Ok(self.stopped)
    }

    fn deliver(
        &mut self,
        plan: &PreparedVortexUnary,
        ready: Vec<super::StatValue>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<()> {
        self.produced = self
            .produced
            .checked_add(ready.len())
            .ok_or_else(|| failed("rolling output count overflow"))?;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
        for value in ready {
            if output.rows.saturating_add(self.pending.values.len()) >= limit {
                self.stopped = true;
                break;
            }
            if matches!(&value, super::StatValue::Float64(v) if !v.is_finite()) {
                return Err(failed("rolling aggregate produced a non-finite value"));
            }
            self.pending.push(value)?;
            if self.pending.values.len() == BATCH_ROWS {
                self.flush(output)?;
            }
        }
        self.stopped |= output.rows.saturating_add(self.pending.values.len()) >= limit;
        Ok(())
    }

    fn flush(&mut self, output: &mut UnaryOutput<'_, '_>) -> Result<()> {
        output.emit(self.pending.values.len(), |row, _| {
            Ok(Value::from(&self.pending.values[row]))
        })?;
        self.pending.values.clear();
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        plan: &PreparedVortexUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        context.check_cancelled()?;
        let request = super::super::required_rolling_window(&plan.request)?;
        if request.center && !self.stopped {
            let ready = self.state.emit_ready_centered(request, true)?;
            self.deliver(plan, ready, output)?;
        }
        self.flush(output)?;
        Ok(self.produced)
    }
}
