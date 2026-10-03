//! Discover pivot domains once, then deliver the completed sparse state with its
//! native schema. Query and writer share one source generation and admission.

use super::super::{PivotAggregateCell, PivotRowExportState, VortexPivotProjectionRequest};
use super::{
    BATCH_ROWS, BoundUnary, CancellationToken, DType, ExecutedVortexUnary, MemoryLease,
    NativeBatch, NativeExecutionContext, Nullability, PreparedVortexUnary, ReservedVec, Result,
    StatValue, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed, vortex_error,
};
use vortex::array::{ArrayRef, dtype::PType};

pub(super) struct Plan {
    pub(super) index_field: (String, DType),
    cell_dtype: DType,
    indices: [usize; 3],
    aggregate: String,
}

fn flat(dtype: &DType) -> bool {
    matches!(dtype, DType::Bool(_) | DType::Utf8(_))
        || matches!(dtype, DType::Primitive(p, _) if *p != PType::F16)
}

fn literal_dtype(value: &StatValue) -> Option<DType> {
    Some(match value {
        StatValue::Null => return None,
        StatValue::Boolean(_) => DType::Bool(Nullability::NonNullable),
        StatValue::Int64(_) => DType::Primitive(PType::I64, Nullability::NonNullable),
        StatValue::UInt64(_) => DType::Primitive(PType::U64, Nullability::NonNullable),
        StatValue::Float64(_) => DType::Primitive(PType::F64, Nullability::NonNullable),
        StatValue::Utf8(_) => DType::Utf8(Nullability::NonNullable),
    })
}

impl Plan {
    pub(super) fn bind(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        columns: &[String],
    ) -> Result<Self> {
        let projection = super::super::required_pivot_projection(request)?;
        let aggregate = super::super::normalized_pivot_aggregate(projection)?.to_owned();
        if projection.margins && matches!(aggregate.as_str(), "first" | "first_unique") {
            return Err(failed(
                "pivot margins require count, sum, mean, min, or max",
            ));
        }
        let names = [
            projection.index_column.as_str(),
            projection.pivot_column.as_str(),
            projection.value_column.as_str(),
        ];
        let mut indices = [0; 3];
        let mut dtypes = Vec::with_capacity(3);
        for (index, name) in names.iter().enumerate() {
            indices[index] = columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| failed("pivot source column is absent"))?;
            let dtype = super::schema::source_field(dtype, name)?;
            if !flat(&dtype) {
                return Err(failed(
                    "pivot keys and values require admitted scalar source types",
                ));
            }
            dtypes.push(dtype);
        }
        if matches!(aggregate.as_str(), "sum" | "mean" | "min" | "max")
            && !matches!(dtypes[2], DType::Primitive(_, _))
        {
            return Err(failed(
                "pivot numeric aggregate requires a numeric value column",
            ));
        }
        let mut cell_dtype = match aggregate.as_str() {
            "first" | "first_unique" => dtypes[2].as_nullable(),
            "count" => DType::Primitive(PType::U64, Nullability::Nullable),
            _ => DType::Primitive(PType::F64, Nullability::Nullable),
        };
        if let Some(fill) = projection.fill_value.as_ref().and_then(literal_dtype) {
            cell_dtype = super::melt::common_dtype(&[cell_dtype, fill])?;
        }
        let index_dtype = if projection.margins && !matches!(dtypes[0], DType::Utf8(_)) {
            DType::Variant(dtypes[0].nullability())
        } else {
            dtypes[0].clone()
        };
        Ok(Self {
            index_field: (names[0].to_owned(), index_dtype),
            cell_dtype,
            indices,
            aggregate,
        })
    }
}

pub(super) struct Pivot {
    state: PivotRowExportState,
    retained: MemoryLease,
    scratch: MemoryLease,
}

fn text_bytes(value: &StatValue) -> usize {
    if let StatValue::Utf8(value) = value {
        value.len()
    } else {
        32
    }
}

fn bytes(base: u64, payload: usize) -> Result<u64> {
    u64::try_from(payload)
        .map_err(vortex_error)?
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(base))
        .ok_or_else(|| failed("pivot reservation overflow"))
}

impl Pivot {
    pub(super) fn new(context: &NativeExecutionContext<'_>) -> Result<Self> {
        Ok(Self {
            state: PivotRowExportState::default(),
            retained: context.memory().reserve(0)?,
            scratch: context.memory().reserve(65_536)?,
        })
    }

    pub(super) fn usage(&self) -> super::report::StateUsage {
        super::report::StateUsage {
            items: self
                .state
                .index_keys
                .len()
                .saturating_add(self.state.pivot_columns.len())
                .saturating_add(self.state.first_cells.len())
                .saturating_add(self.state.aggregate_cells.len()),
            all_input_retained: false,
        }
    }

    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let compiled = plan
            .pivot
            .as_ref()
            .ok_or_else(|| failed("pivot is not bound"))?;
        let projection = super::super::required_pivot_projection(&plan.request)?;
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            let index = batch.stat(compiled.indices[0], row)?;
            let pivot = batch.stat(compiled.indices[1], row)?;
            if compiled.aggregate == "count" {
                self.update(
                    compiled,
                    projection,
                    index.value(),
                    pivot.value(),
                    &StatValue::Null,
                )?;
            } else {
                let value = batch.stat(compiled.indices[2], row)?;
                self.update(
                    compiled,
                    projection,
                    index.value(),
                    pivot.value(),
                    value.value(),
                )?;
            }
        }
        Ok(())
    }

    fn update(
        &mut self,
        compiled: &Plan,
        projection: &VortexPivotProjectionRequest,
        index: &StatValue,
        pivot: &StatValue,
        value: &StatValue,
    ) -> Result<()> {
        let payload = text_bytes(index)
            .checked_add(text_bytes(pivot))
            .and_then(|n| n.checked_add(text_bytes(value)))
            .ok_or_else(|| failed("pivot key size overflow"))?;
        let scratch = bytes(65_536, payload)?;
        if scratch > self.scratch.bytes() {
            self.scratch.resize(scratch)?;
        }
        let index_key = super::super::pivot_value_key(index);
        let pivot_key = super::super::pivot_value_key(pivot);
        let new_index = !self.state.index_keys.contains(&index_key);
        let new_pivot = !self.state.pivot_columns.contains_key(&pivot_key);
        if new_pivot && self.state.pivot_columns.len() >= 127 - usize::from(projection.margins) {
            return Err(failed("pivot domain exceeds 128 result columns"));
        }
        if new_pivot && super::super::pivot_output_column_name(pivot).len() > 248 {
            return Err(failed(
                "pivot domain name exceeds the native field-name boundary",
            ));
        }
        let cell_key = (index_key, pivot_key);
        let first = matches!(compiled.aggregate.as_str(), "first" | "first_unique");
        let new_cell = if first {
            !self.state.first_cells.contains_key(&cell_key)
        } else {
            !self.state.aggregate_cells.contains_key(&cell_key)
        };
        if let Some(cell) = self.state.aggregate_cells.get(&cell_key) {
            if cell.count == u64::MAX {
                return Err(failed("pivot cell count overflow"));
            }
            if matches!(compiled.aggregate.as_str(), "sum" | "mean") {
                let value = super::super::stat_value_to_f64(value)?;
                if !(cell.sum + value).is_finite() {
                    return Err(failed("pivot numeric accumulation is not finite"));
                }
            }
        }
        let mut growth = 0_u64;
        for (new, base) in [(new_index, 4096), (new_pivot, 2048), (new_cell, 4096)] {
            if new {
                growth = growth
                    .checked_add(bytes(base, payload)?)
                    .ok_or_else(|| failed("pivot state size overflow"))?;
            }
        }
        self.retained.resize(
            self.retained
                .bytes()
                .checked_add(growth)
                .ok_or_else(|| failed("pivot state size overflow"))?,
        )?;
        self.state.update(
            projection,
            &compiled.aggregate,
            std::slice::from_ref(index),
            std::slice::from_ref(pivot),
            std::slice::from_ref(value),
        )
    }
}

pub(super) struct Completed {
    pub(super) execution: ExecutedVortexUnary,
    pub(super) result: CompletedPivot,
}

/// One execution's sparse state and authoritative schema. Direct file calls and
/// relational composition share completion and bounded emission of this owner.
pub(in crate::local_primitives) struct CompletedPivot {
    pub(in crate::local_primitives) fields: Vec<(String, DType)>,
    columns: Vec<String>,
    pub(in crate::local_primitives) rows: usize,
    pre_limit_rows: usize,
    state: Pivot,
    indices: Vec<String>,
    domains: Vec<String>,
    column_margins: ReservedVec<Option<StatValue>>,
    grand_margin: Option<StatValue>,
    _metadata: MemoryLease,
}

impl PreparedVortexUnary {
    pub(super) fn execute_pivot(&self) -> Result<ExecutedVortexUnary> {
        let mut execution = self
            .source
            .with_native_execution_controlled(&CancellationToken::default(), |file, context| {
                Ok(self.complete_pivot(file, context)?.execution)
            })?;
        execution.runtime = self.snapshot();
        Ok(execution)
    }

    pub(super) fn complete_pivot(
        &self,
        file: &vortex::file::VortexFile,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Completed> {
        let mut discarded = UnaryOutput::discard(&self.bound.output_columns);
        let (state, evidence) = self.scan_state(file, context, &mut discarded)?;
        let super::select::State::Pivot(state) = state else {
            return Err(failed("pivot completed with a different state family"));
        };
        let result = state.complete(&self.bound, context)?;
        let execution = self.certify_scan(
            context,
            evidence,
            result.rows,
            result.pre_limit_rows,
            &result.columns,
            result.usage(),
        )?;
        Ok(Completed { execution, result })
    }
}

impl BoundUnary {
    /// Discover a dynamic domain while consuming the preceding relation exactly
    /// once. The returned owner supplies both schema binding and later delivery.
    pub(in crate::local_primitives) fn complete_relation_pivot(
        &self,
        context: &NativeExecutionContext<'_>,
        produce: impl FnOnce(&mut dyn FnMut(ArrayRef) -> Result<()>) -> Result<()>,
    ) -> Result<CompletedPivot> {
        let mut state = Pivot::new(context)?;
        produce(&mut |array| {
            context.check_cancelled()?;
            let mut batch = NativeBatch::new(&array, &self.columns, context)?;
            state.consume(self, &mut batch, array.len(), context)
        })?;
        state.complete(self, context)
    }
}

impl Pivot {
    #[allow(clippy::too_many_lines)] // Preserve shared domain, margin and output-schema completion together.
    fn complete(
        self,
        bound: &BoundUnary,
        context: &NativeExecutionContext<'_>,
    ) -> Result<CompletedPivot> {
        context.check_cancelled()?;
        let state = self;
        let compiled = bound
            .pivot
            .as_ref()
            .ok_or_else(|| failed("pivot is not bound"))?;
        let projection = super::super::required_pivot_projection(&bound.request)?;
        let has_margin = projection.margins && !state.state.index_keys.is_empty();
        let limit = bound
            .request
            .source_order_limit
            .unwrap_or(usize::MAX)
            .saturating_sub(usize::from(has_margin));
        let index_count = state.state.index_keys.len().min(limit);
        let retained_key_bytes = state
            .state
            .index_keys
            .iter()
            .take(index_count)
            .chain(state.state.pivot_columns.keys())
            .try_fold(0usize, |bytes, key| {
                bytes
                    .checked_add(key.len())
                    .ok_or_else(|| failed("pivot domain size overflow"))
            })?;
        let metadata = context.memory().reserve(bytes(
            65_536 + (index_count as u64).saturating_mul(128),
            retained_key_bytes,
        )?)?;
        let indices = state
            .state
            .index_keys
            .iter()
            .take(index_count)
            .cloned()
            .collect::<Vec<_>>();
        // Every observed domain owns a cell, including an explicit null first
        // value. This is the existing provider's dropna contract.
        let domains = state
            .state
            .pivot_columns
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut fields = vec![compiled.index_field.clone()];
        for key in &domains {
            fields.push((
                state.state.pivot_columns[key].clone(),
                compiled.cell_dtype.clone(),
            ));
        }
        if projection.margins {
            fields.push((
                super::super::pivot_output_column_name(&StatValue::Utf8(
                    projection.margins_name.clone(),
                )),
                compiled.cell_dtype.clone(),
            ));
        }
        drop(super::super::completed_result::CompletedRows::new(
            fields.clone(),
            context.memory(),
        )?);
        let columns = fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let mut column_margins = ReservedVec::new(context.memory())?;
        let mut grand = Margin::default();
        if has_margin {
            for pivot in &domains {
                let mut margin = Margin::default();
                for index in &indices {
                    if let Some(cell) = state
                        .state
                        .aggregate_cells
                        .get(&(index.clone(), pivot.clone()))
                    {
                        margin.push(*cell, &compiled.aggregate)?;
                    }
                }
                column_margins.push(margin.value(&compiled.aggregate)?)?;
            }
            // Preserve the provider's row-major floating accumulation order.
            for index in &indices {
                for pivot in &domains {
                    if let Some(cell) = state
                        .state
                        .aggregate_cells
                        .get(&(index.clone(), pivot.clone()))
                    {
                        grand.push(*cell, &compiled.aggregate)?;
                    }
                }
            }
        }
        let rows = index_count
            .checked_add(usize::from(has_margin))
            .ok_or_else(|| failed("pivot row count overflow"))?;
        let pre_limit = state
            .state
            .index_keys
            .len()
            .checked_add(usize::from(has_margin))
            .ok_or_else(|| failed("pivot row count overflow"))?;
        Ok(CompletedPivot {
            fields,
            columns,
            rows,
            pre_limit_rows: pre_limit,
            state,
            indices,
            domains,
            column_margins,
            grand_margin: grand.value(&compiled.aggregate)?,
            _metadata: metadata,
        })
    }
}

#[derive(Default)]
struct Margin {
    cell: PivotAggregateCell,
    present: bool,
}
impl Margin {
    fn push(&mut self, cell: PivotAggregateCell, aggregate: &str) -> Result<()> {
        self.present = true;
        self.cell.count = self
            .cell
            .count
            .checked_add(cell.count)
            .ok_or_else(|| failed("pivot margin count overflow"))?;
        self.cell.sum += cell.sum;
        if matches!(aggregate, "sum" | "mean") && !self.cell.sum.is_finite() {
            return Err(failed("pivot margin accumulation is not finite"));
        }
        if let Some(value) = cell.min {
            self.cell.min = Some(self.cell.min.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = cell.max {
            self.cell.max = Some(self.cell.max.map_or(value, |current| current.max(value)));
        }
        Ok(())
    }
    fn value(&self, aggregate: &str) -> Result<Option<StatValue>> {
        if !self.present {
            return Ok(None);
        }
        super::super::pivot_margin_from_cells(aggregate, std::slice::from_ref(&self.cell))
    }
}

impl CompletedPivot {
    pub(in crate::local_primitives) fn usage(&self) -> super::report::StateUsage {
        self.state.usage()
    }

    pub(in crate::local_primitives) fn emit(
        &self,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let compiled = plan
            .pivot
            .as_ref()
            .ok_or_else(|| failed("pivot is not bound"))?;
        let projection = super::super::required_pivot_projection(&plan.request)?;
        let mut output = super::super::completed_result::CompletedRows::streaming(
            self.fields.clone(),
            context.memory(),
            batch_rows,
            context.cancellation().clone(),
            consume,
        )?;
        let mut offset = 0;
        loop {
            context.check_cancelled()?;
            let count = (self.rows - offset).min(BATCH_ROWS);
            output.push_values(&self.columns, count, |row, column| {
                self.value(compiled, projection, offset + row, column)
            })?;
            offset += count;
            if offset == self.rows {
                break;
            }
        }
        output.finish_stream()
    }

    fn value<'a>(
        &'a self,
        compiled: &Plan,
        projection: &'a VortexPivotProjectionRequest,
        row: usize,
        column: usize,
    ) -> Result<Value<'a>> {
        let fill = |value: Option<Value<'a>>| {
            value.unwrap_or_else(|| {
                projection
                    .fill_value
                    .as_ref()
                    .map_or(Value::Null, Value::from)
            })
        };
        if row == self.indices.len() {
            if column == 0 {
                return Ok(Value::Text(std::borrow::Cow::Borrowed(
                    &projection.margins_name,
                )));
            }
            return Ok(fill(if column <= self.domains.len() {
                self.column_margins.values[column - 1]
                    .as_ref()
                    .map(Value::from)
            } else {
                self.grand_margin.as_ref().map(Value::from)
            }));
        }
        let index = &self.indices[row];
        if column == 0 {
            return Ok(Value::from(&self.state.state.index_values[index]));
        }
        if column > self.domains.len() {
            let mut margin = Margin::default();
            for pivot in &self.domains {
                if let Some(cell) = self
                    .state
                    .state
                    .aggregate_cells
                    .get(&(index.clone(), pivot.clone()))
                {
                    margin.push(*cell, &compiled.aggregate)?;
                }
            }
            return Ok(fill(margin.value(&compiled.aggregate)?.map(Value::from)));
        }
        let key = (index.clone(), self.domains[column - 1].clone());
        let value = match compiled.aggregate.as_str() {
            "first" | "first_unique" => self.state.state.first_cells.get(&key).map(Value::from),
            _ => self.state.state.aggregate_cells.get(&key).and_then(|cell| {
                match compiled.aggregate.as_str() {
                    "count" => Some(Value::UInt(cell.count)),
                    "sum" => Some(Value::Float(cell.sum)),
                    "mean" => {
                        super::super::pivot_mean_value(cell.sum, cell.count).map(Value::Float)
                    }
                    "min" => cell.min.map(Value::Float),
                    "max" => cell.max.map(Value::Float),
                    _ => None,
                }
            }),
        };
        Ok(fill(value))
    }
}
