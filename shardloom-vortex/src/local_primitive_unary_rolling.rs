//! A bounded window shares native scheduling and synchronous output delivery.
//! Centered lookahead never retains the whole input.

use super::super::{
    RollingWindowState, native_decimal_reduce,
    rolling_observation::{Decimal, Observation},
};
use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, Nullability, ReservedVec,
    Result, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;

pub(super) fn dtype(request: &VortexQueryPrimitiveRequest, source: &DType) -> Result<DType> {
    use vortex::array::dtype::PType;
    let request = super::super::required_rolling_window(request)?;
    if request.aggregate == "count" {
        return Ok(DType::Primitive(PType::U64, Nullability::NonNullable));
    }
    if let DType::Decimal(decimal, _) = source {
        let output = match request.aggregate.as_str() {
            "sum" | "mean" => {
                native_decimal_reduce::output_dtype(*decimal, request.aggregate == "mean")?
            }
            "min" | "max" => *decimal,
            _ => return Err(failed("decimal rolling aggregate was not admitted")),
        };
        return Ok(DType::Decimal(output, Nullability::NonNullable));
    }
    Ok(DType::Primitive(PType::F64, Nullability::NonNullable))
}

pub(super) struct Rolling {
    state: TypedState,
}

enum TypedState {
    Primitive(Buffered<f64>),
    Decimal(Buffered<Decimal>),
}

struct Buffered<T: NativeObservation> {
    state: RollingWindowState<T>,
    pending: ReservedVec<T::Output>,
    produced: usize,
    peak_items: usize,
    stopped: bool,
    _state_memory: MemoryLease,
}

trait NativeObservation: Observation {
    fn input(value: Value<'_>, count: bool) -> Result<Option<Self>>;
    fn output(value: &Self::Output) -> Result<Value<'_>>;
}

impl NativeObservation for f64 {
    fn input(value: Value<'_>, count: bool) -> Result<Option<Self>> {
        let stat = match value {
            Value::Null => return Ok(None),
            _ if count => return Ok(Some(0.0)),
            Value::Int(value) => super::StatValue::Int64(value),
            Value::UInt(value) => super::StatValue::UInt64(value),
            Value::Float(value) => super::StatValue::Float64(value),
            _ => return Err(failed("rolling numeric aggregate requires numeric input")),
        };
        super::super::stat_value_to_f64(&stat).map(Some)
    }

    fn output(value: &Self::Output) -> Result<Value<'_>> {
        if matches!(value, super::StatValue::Float64(value) if !value.is_finite()) {
            return Err(failed("rolling aggregate produced a non-finite value"));
        }
        Ok(Value::from(value))
    }
}

impl NativeObservation for Decimal {
    fn input(value: Value<'_>, _count: bool) -> Result<Option<Self>> {
        match value {
            Value::Null => Ok(None),
            Value::Decimal(value, dtype) => {
                shardloom_core::expression::Decimal128Operand::decimal(
                    value,
                    dtype.precision(),
                    u8::try_from(dtype.scale()).map_err(vortex_error)?,
                )?;
                Ok(Some(Self { value, dtype }))
            }
            _ => Err(failed("decimal rolling input domain changed")),
        }
    }

    fn output(value: &Self::Output) -> Result<Value<'_>> {
        Ok(Value::Decimal(value.value, value.dtype))
    }
}

impl Rolling {
    pub(super) fn usage(&self) -> super::report::StateUsage {
        let items = match &self.state {
            TypedState::Primitive(state) => state.peak_items,
            TypedState::Decimal(state) => state.peak_items,
        };
        super::report::StateUsage {
            items,
            all_input_retained: false,
        }
    }

    pub(super) fn new(
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        source_rows: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            state: if matches!(plan.fields()[0].1, DType::Decimal(..)) {
                TypedState::Decimal(Buffered::new(plan, context, source_rows)?)
            } else {
                TypedState::Primitive(Buffered::new(plan, context, source_rows)?)
            },
        })
    }

    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        match &mut self.state {
            TypedState::Primitive(state) => state.consume(plan, batch, rows, context, output),
            TypedState::Decimal(state) => state.consume(plan, batch, rows, context, output),
        }
    }

    pub(super) fn finish(
        self,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        match self.state {
            TypedState::Primitive(state) => state.finish(plan, context, output),
            TypedState::Decimal(state) => state.finish(plan, context, output),
        }
    }
}

impl<T: NativeObservation> Buffered<T> {
    fn new(
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        source_rows: Option<u64>,
    ) -> Result<Self> {
        let request = super::super::required_rolling_window(&plan.request)?;
        // One-row feeding bounds lookahead to one window. Credit actual value
        // widths, deque growth overlap and temporary centered output first.
        // Primitive reservation remains at least its previous 128 bytes/item.
        let capacity = request
            .window_size
            .min(
                source_rows
                    .map(usize::try_from)
                    .transpose()
                    .map_err(vortex_error)?
                    .unwrap_or(request.window_size),
            )
            .checked_add(1)
            .ok_or_else(|| failed("rolling capacity overflow"))?;
        let item_bytes = std::mem::size_of::<Option<T>>()
            .checked_add(std::mem::size_of::<T::Output>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| failed("rolling item reservation overflow"))?
            .max(128);
        let bytes = capacity
            .checked_mul(item_bytes)
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
            state: RollingWindowState::with_values(values),
            pending: ReservedVec::new(context.memory())?,
            produced: 0,
            peak_items: 0,
            stopped: false,
            _state_memory: memory,
        })
    }

    fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let request = super::super::required_rolling_window(&plan.request)?;
        let nested_count = request.aggregate == "count"
            && super::super::native_payload::is_nested(
                batch.column(plan.output_indices[0])?.dtype(),
            );
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let value = if nested_count {
                if batch.is_null(plan.output_indices[0], row)? {
                    Value::Null
                } else {
                    Value::UInt(1)
                }
            } else {
                batch.value(plan.output_indices[0], row)?
            };
            let value = T::input(value, request.aggregate == "count")?;
            self.peak_items = self
                .peak_items
                .max((self.state.values.len() + 1).min(request.window_size));
            let ready = if request.center {
                self.state.push_centered(value)?;
                self.state.emit_ready_centered(request, false)?
            } else {
                self.state.push(
                    value,
                    request.window_size,
                    matches!(request.aggregate.as_str(), "sum" | "mean"),
                )?;
                if self.state.ready(request.min_periods) {
                    vec![self.state.current_value(request)?]
                } else {
                    vec![]
                }
            };
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
        plan: &BoundUnary,
        ready: Vec<T::Output>,
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
            T::output(&value)?;
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
            T::output(&self.pending.values[row])
        })?;
        self.pending.values.clear();
        Ok(())
    }

    fn finish(
        mut self,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<usize> {
        context.check_cancelled()?;
        let request = super::super::required_rolling_window(&plan.request)?;
        if request.center && !self.stopped {
            // Do not evaluate unused shrinking frames after reaching the limit:
            // a later exact-decimal failure cannot invalidate earlier results.
            let remaining = plan
                .request
                .source_order_limit
                .unwrap_or(usize::MAX)
                .saturating_sub(output.rows.saturating_add(self.pending.values.len()));
            let ready =
                self.state
                    .emit_ready_centered_controlled(request, true, remaining, || {
                        context.check_cancelled()
                    })?;
            self.deliver(plan, ready, output)?;
        }
        self.flush(output)?;
        Ok(self.produced)
    }
}
