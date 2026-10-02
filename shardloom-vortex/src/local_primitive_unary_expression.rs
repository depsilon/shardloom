//! Prepared scalar rewrites over bounded native batches. Every stateful rewrite
//! has its own state, so chunk boundaries and repeated calls cannot change values.

use super::super::{MaterializedPredicateEvaluator, VortexExpressionRewrite as Rewrite};
use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, Nullability, ReservedVec,
    Result, StatValue, UnaryOutput, Value, VortexQueryPrimitiveRequest, failed,
    values::{OwnedRow, OwnedStat},
    vortex_error,
};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::PType;

struct Step {
    target: usize,
    predicate: Option<MaterializedPredicateEvaluator>,
    regex: Option<(regex::Regex, String)>,
}

pub(super) struct Plan {
    pub(super) fields: Vec<(String, DType)>,
    source_indices: Vec<usize>,
    output_indices: Vec<usize>,
    extra_columns: usize,
    steps: Vec<Step>,
    _regex_memory: ReservedVec<MemoryLease>,
}

impl Plan {
    pub(super) fn bind(
        request: &VortexQueryPrimitiveRequest,
        source: &DType,
        columns: &[String],
        selected: &[String],
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        let projection = request
            .expression_projection
            .as_ref()
            .ok_or_else(|| failed("expression payload is absent"))?;
        super::super::validate_expression_projection_columns(projection, columns)?;
        let working = super::super::expression_projection_output_columns(columns, projection);
        let output = super::super::expression_projection_output_columns(selected, projection);
        let position = |name: &str| {
            working
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| failed("rewrite target is absent"))
        };
        let output_indices = output
            .iter()
            .map(|name| position(name))
            .collect::<Result<Vec<_>>>()?;
        let mut fields = output
            .iter()
            .map(|name| {
                let field = if columns.contains(name) {
                    super::schema::source_field(source, name)?
                } else {
                    DType::Primitive(PType::U64, Nullability::NonNullable)
                };
                Ok((name.clone(), field))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut regex_memory = ReservedVec::new(memory)?;
        let mut steps = Vec::with_capacity(projection.rewrites.len());
        for rewrite in &projection.rewrites {
            for (name, dtype) in &mut fields {
                if name == rewrite.target_column().as_str() {
                    *dtype = rewritten_dtype(dtype, rewrite)?;
                }
            }
            let predicate = if let Rewrite::MaskScalar { predicate, .. } = rewrite {
                Some(MaterializedPredicateEvaluator::compile(
                    predicate, &working,
                )?)
            } else {
                None
            };
            if matches!(rewrite, Rewrite::RegexReplaceScalar { .. }) {
                // The existing regex provider limits compiled automata to 10 MiB
                // and DFA cache to 2 MiB. Cover compilation overlap as well.
                regex_memory.reserve_one()?;
                regex_memory.values.push(memory.reserve(24 * 1024 * 1024)?);
            }
            steps.push(Step {
                target: position(rewrite.target_column().as_str())?,
                predicate,
                regex: super::super::expression_projection_regex_replacement(rewrite)?,
            });
        }
        Ok(Self {
            fields,
            source_indices: (0..columns.len()).collect(),
            output_indices,
            extra_columns: working.len() - columns.len(),
            steps,
            _regex_memory: regex_memory,
        })
    }
}

fn rewritten_dtype(dtype: &DType, rewrite: &Rewrite) -> Result<DType> {
    let mut nullable = dtype.nullability();
    match rewrite {
        Rewrite::RowNumber { .. } => Ok(DType::Primitive(PType::U64, Nullability::NonNullable)),
        Rewrite::ForwardFillNull { .. } => Ok(dtype.clone()),
        Rewrite::StringReplaceScalar { .. } | Rewrite::RegexReplaceScalar { .. } => {
            if !matches!(dtype, DType::Utf8(_)) {
                return Err(failed("string rewrite requires UTF8 input"));
            }
            Ok(dtype.clone())
        }
        Rewrite::MaskScalar { replacement, .. } | Rewrite::ReplaceScalar { replacement, .. } => {
            if matches!(replacement, StatValue::Null) {
                nullable = Nullability::Nullable;
            }
            Ok(promote(dtype, nullable))
        }
        Rewrite::NumericScalarArithmetic { .. } => {
            if !matches!(dtype, DType::Primitive(p, _) if *p != PType::F16) {
                return Err(failed("arithmetic rewrite requires numeric input"));
            }
            Ok(promote(dtype, nullable))
        }
    }
}

fn promote(dtype: &DType, nullable: Nullability) -> DType {
    match dtype {
        DType::Primitive(p, _) => DType::Primitive(
            if p.is_signed_int() {
                PType::I64
            } else if p.is_unsigned_int() {
                PType::U64
            } else {
                PType::F64
            },
            nullable,
        ),
        _ => dtype.with_nullability(nullable),
    }
}

struct Fill {
    value: Option<OwnedStat>,
    consecutive: usize,
}

pub(super) struct Expression {
    fills: ReservedVec<Fill>,
    ordinal: u64,
}

impl Expression {
    pub(super) fn usage(&self) -> super::report::StateUsage {
        super::report::StateUsage {
            items: self.fills.values.len(),
            all_input_retained: false,
        }
    }
    pub(super) fn new(plan: &BoundUnary, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let mut fills = ReservedVec::new(context.memory())?;
        let compiled = plan
            .expression
            .as_ref()
            .ok_or_else(|| failed("expression is not bound"))?;
        for _ in &compiled.steps {
            fills.push(Fill {
                value: None,
                consecutive: 0,
            })?;
        }
        Ok(Self { fills, ordinal: 0 })
    }

    pub(super) fn consume(
        &mut self,
        plan: &BoundUnary,
        batch: &mut NativeBatch,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<bool> {
        let compiled = plan
            .expression
            .as_ref()
            .ok_or_else(|| failed("expression is not bound"))?;
        let mut pending = ReservedVec::new(context.memory())?;
        let limit = plan.request.source_order_limit.unwrap_or(usize::MAX);
        for row in 0..rows {
            if row % 256 == 0 {
                context.check_cancelled()?;
            }
            if let Some(predicate) = &plan.predicate
                && !predicate.matches_with(&mut |column| batch.stat(column, row))?
            {
                continue;
            }
            pending.reserve_one()?;
            pending.values.push(batch.row_with_padding(
                &compiled.source_indices,
                row,
                compiled.extra_columns,
            )?);
            if pending.values.len() == BATCH_ROWS
                || output.rows.saturating_add(pending.values.len()) >= limit
            {
                self.flush(plan, compiled, &mut pending, context, output)?;
                if output.rows >= limit {
                    return Ok(true);
                }
            }
        }
        self.flush(plan, compiled, &mut pending, context, output)?;
        Ok(output.rows >= limit)
    }

    fn flush(
        &mut self,
        plan: &BoundUnary,
        compiled: &Plan,
        rows: &mut ReservedVec<OwnedRow>,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<()> {
        let request = plan
            .request
            .expression_projection
            .as_ref()
            .ok_or_else(|| failed("expression payload is absent"))?;
        for (step_index, (step, rewrite)) in
            compiled.steps.iter().zip(&request.rewrites).enumerate()
        {
            for (index, row) in rows.values.iter_mut().enumerate() {
                if index % 256 == 0 {
                    context.check_cancelled()?;
                }
                let ordinal = self
                    .ordinal
                    .checked_add(index as u64)
                    .ok_or_else(|| failed("rewrite row ordinal overflow"))?;
                let value = apply(
                    rewrite,
                    step,
                    row,
                    ordinal,
                    &mut self.fills.values[step_index],
                    context.memory(),
                )?;
                row.replace(step.target, value)?;
            }
        }
        output.emit(rows.values.len(), |row, column| {
            Ok(Value::from(
                &rows.values[row].values()[compiled.output_indices[column]],
            ))
        })?;
        self.ordinal = self
            .ordinal
            .checked_add(rows.values.len() as u64)
            .ok_or_else(|| failed("rewrite row count overflow"))?;
        rows.values.clear();
        Ok(())
    }

    pub(super) fn finish(self) -> Result<usize> {
        usize::try_from(self.ordinal).map_err(vortex_error)
    }
}

fn apply(
    rewrite: &Rewrite,
    step: &Step,
    row: &OwnedRow,
    ordinal: u64,
    fill: &mut Fill,
    memory: &LiveMemoryPool,
) -> Result<OwnedStat> {
    let current = &row.values()[step.target];
    match rewrite {
        Rewrite::MaskScalar { replacement, .. } => {
            let predicate = step
                .predicate
                .as_ref()
                .ok_or_else(|| failed("mask predicate is not bound"))?;
            if predicate.matches_with(&mut |column| Ok(&row.values()[column]))? {
                coerce(current, replacement, memory)
            } else {
                OwnedStat::copy(current, memory)
            }
        }
        Rewrite::ReplaceScalar {
            to_replace,
            replacement,
            ..
        } => {
            let comparable = coerce(current, to_replace, memory)?;
            if super::super::stat_value_equal(current, comparable.value()) {
                coerce(current, replacement, memory)
            } else {
                OwnedStat::copy(current, memory)
            }
        }
        Rewrite::StringReplaceScalar {
            needle,
            replacement,
            ..
        } => replace_text(current, needle, replacement, memory),
        Rewrite::RegexReplaceScalar { .. } => replace_regex(current, step, memory),
        Rewrite::NumericScalarArithmetic {
            operator, operand, ..
        } => OwnedStat::produce(memory, 0, || {
            super::super::apply_numeric_scalar_arithmetic(current, operator, operand)
        }),
        Rewrite::ForwardFillNull { limit, .. } => {
            if matches!(current, StatValue::Null) {
                if let Some(value) = &fill.value
                    && limit.is_none_or(|n| fill.consecutive < n)
                {
                    fill.consecutive = fill
                        .consecutive
                        .checked_add(1)
                        .ok_or_else(|| failed("fill length overflow"))?;
                    return OwnedStat::copy(value.value(), memory);
                }
            } else {
                fill.value = Some(OwnedStat::copy(current, memory)?);
                fill.consecutive = 0;
            }
            OwnedStat::copy(current, memory)
        }
        Rewrite::RowNumber { start, .. } => OwnedStat::produce(memory, 0, || {
            Ok(StatValue::UInt64(
                ordinal
                    .checked_add(*start)
                    .ok_or_else(|| failed("row-number overflow"))?,
            ))
        }),
    }
}

fn coerce(
    current: &StatValue,
    replacement: &StatValue,
    memory: &LiveMemoryPool,
) -> Result<OwnedStat> {
    let bytes = if let StatValue::Utf8(text) = replacement {
        text.len()
    } else {
        0
    };
    OwnedStat::produce(memory, bytes, || {
        super::super::coerce_rewrite_value(current, replacement)
    })
}

fn replace_text(
    current: &StatValue,
    needle: &str,
    replacement: &str,
    memory: &LiveMemoryPool,
) -> Result<OwnedStat> {
    let StatValue::Utf8(text) = current else {
        return Err(failed("string replacement requires UTF8 input"));
    };
    let matches = text.match_indices(needle).count();
    let removed = matches
        .checked_mul(needle.len())
        .ok_or_else(|| failed("replacement size overflow"))?;
    let bytes = matches
        .checked_mul(replacement.len())
        .and_then(|n| n.checked_add(text.len() - removed))
        .ok_or_else(|| failed("replacement size overflow"))?;
    OwnedStat::produce(memory, bytes, || {
        let mut value = String::new();
        value.try_reserve_exact(bytes).map_err(vortex_error)?;
        let mut end = 0;
        for (offset, matched) in text.match_indices(needle) {
            value.push_str(&text[end..offset]);
            value.push_str(replacement);
            end = offset + matched.len();
        }
        value.push_str(&text[end..]);
        Ok(StatValue::Utf8(value))
    })
}

fn replace_regex(current: &StatValue, step: &Step, memory: &LiveMemoryPool) -> Result<OwnedStat> {
    let StatValue::Utf8(text) = current else {
        return Err(failed("regex replacement requires UTF8 input"));
    };
    let (regex, replacement) = step
        .regex
        .as_ref()
        .ok_or_else(|| failed("regex is not bound"))?;
    // Every capture is contained by its nonoverlapping whole match. Even if
    // every replacement byte references a capture, this bounds all expansions.
    let bound = regex
        .find_iter(text)
        .try_fold(text.len(), |bytes, matched| {
            matched
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(replacement.len()))
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(|| failed("regex expansion overflow"))
        })?
        .checked_mul(2)
        .ok_or_else(|| failed("regex capacity overflow"))?
        .max(8);
    OwnedStat::produce(memory, bound, || {
        Ok(StatValue::Utf8(
            regex.replace_all(text, replacement).into_owned(),
        ))
    })
}
