//! Prepared scalar rewrites over bounded native batches. Every stateful rewrite
//! has its own state, so chunk boundaries and repeated calls cannot change values.

use super::super::{MaterializedPredicateEvaluator, VortexExpressionRewrite as Rewrite};
use super::{
    BATCH_ROWS, BoundUnary, DType, NativeBatch, NativeExecutionContext, Nullability, ReservedVec,
    Result, UnaryOutput, VortexQueryPrimitiveRequest, failed, scalar,
    values::{OwnedRow, OwnedScalar, OwnedStat, borrowed},
    vortex_error,
};
use shardloom_core::ScalarValue;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::PType;

struct Step {
    target: usize,
    dtype: DType,
    comparison: Option<OwnedScalar>,
    replacement: Option<OwnedScalar>,
    arithmetic: Option<Arithmetic>,
    predicate: Option<MaterializedPredicateEvaluator>,
    regex: Option<(regex::Regex, String)>,
}

struct Arithmetic {
    left_dtype: DType,
    right_dtype: DType,
    operator: shardloom_core::BinaryOp,
    operand: OwnedScalar,
}

impl Arithmetic {
    fn bind(
        rewrite: &Rewrite,
        input_dtype: DType,
        output_dtype: &DType,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
        let Rewrite::NumericScalarArithmetic {
            operator, operand, ..
        } = rewrite
        else {
            return Ok(None);
        };
        let operand = if matches!(output_dtype, DType::Decimal(..)) {
            OwnedScalar::copy(operand, memory)?
        } else {
            scalar::coerce(output_dtype, operand, memory)?
        };
        Ok(Some(Self {
            left_dtype: input_dtype,
            right_dtype: scalar::literal_dtype(operand.value())?,
            operator: scalar::operator(operator)?,
            operand,
        }))
    }
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
        let mut fields = working
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
            let target = position(rewrite.target_column().as_str())?;
            let input_dtype = fields[target].1.clone();
            fields[target].1 = rewritten_dtype(&input_dtype, rewrite)?;
            let dtype = fields[target].1.clone();
            let (comparison, replacement) = match rewrite {
                Rewrite::MaskScalar { replacement, .. } => {
                    (None, Some(scalar::coerce(&dtype, replacement, memory)?))
                }
                Rewrite::ReplaceScalar {
                    to_replace,
                    replacement,
                    ..
                } => (
                    Some(scalar::coerce(&dtype, to_replace, memory)?),
                    Some(scalar::coerce(&dtype, replacement, memory)?),
                ),
                _ => (None, None),
            };
            let arithmetic = Arithmetic::bind(rewrite, input_dtype, &dtype, memory)?;
            let predicate = if let Rewrite::MaskScalar { predicate, .. } = rewrite {
                super::schema::predicate_types(
                    predicate,
                    &DType::struct_(fields.clone(), Nullability::NonNullable),
                )?;
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
                target,
                dtype,
                comparison,
                replacement,
                arithmetic,
                predicate,
                regex: super::super::expression_projection_regex_replacement(rewrite)?,
            });
        }
        Ok(Self {
            fields: output_indices
                .iter()
                .map(|index| fields[*index].clone())
                .collect(),
            source_indices: (0..columns.len()).collect(),
            output_indices,
            extra_columns: working.len() - columns.len(),
            steps,
            _regex_memory: regex_memory,
        })
    }
}

fn rewritten_dtype(dtype: &DType, rewrite: &Rewrite) -> Result<DType> {
    if !crate::native_payload_schema::admitted_scalar(dtype) {
        return Err(failed("rewrite requires a bound flat scalar target"));
    }
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
            if matches!(replacement, ScalarValue::Null) {
                nullable = Nullability::Nullable;
            }
            Ok(promote(dtype, nullable))
        }
        Rewrite::NumericScalarArithmetic {
            operator, operand, ..
        } => {
            let op = scalar::operator(operator)?;
            let right = scalar::literal_dtype(operand)?;
            if right == DType::Null {
                return Err(failed("arithmetic rewrite requires a non-null operand"));
            }
            if matches!(dtype, DType::Decimal(..)) || matches!(right, DType::Decimal(..)) {
                return scalar::arithmetic_dtype(dtype, op, &right)
                    .map_err(|error| failed(&error.to_string()));
            }
            if !matches!(dtype, DType::Primitive(p, _) if *p != PType::F16) {
                return Err(failed("arithmetic rewrite requires numeric input"));
            }
            if op == shardloom_core::BinaryOp::Divide
                && !matches!(dtype, DType::Primitive(PType::F32 | PType::F64, _))
            {
                return Err(failed("primitive integer rewrite division is not admitted"));
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
    value: Option<OwnedScalar>,
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
            borrowed(&rows.values[row].values()[compiled.output_indices[column]])
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
) -> Result<OwnedScalar> {
    let current = &row.values()[step.target];
    match rewrite {
        Rewrite::MaskScalar { .. } => {
            let predicate = step
                .predicate
                .as_ref()
                .ok_or_else(|| failed("mask predicate is not bound"))?;
            if predicate
                .matches_with(&mut |column| OwnedStat::from_scalar(&row.values()[column], memory))?
            {
                OwnedScalar::copy(
                    step.replacement
                        .as_ref()
                        .ok_or_else(|| failed("mask replacement is not bound"))?
                        .value(),
                    memory,
                )
            } else {
                OwnedScalar::copy(current, memory)
            }
        }
        Rewrite::ReplaceScalar { .. } => {
            let comparable = step
                .comparison
                .as_ref()
                .ok_or_else(|| failed("replacement comparison is not bound"))?;
            if current == comparable.value() {
                OwnedScalar::copy(
                    step.replacement
                        .as_ref()
                        .ok_or_else(|| failed("replacement is not bound"))?
                        .value(),
                    memory,
                )
            } else {
                OwnedScalar::copy(current, memory)
            }
        }
        Rewrite::StringReplaceScalar {
            needle,
            replacement,
            ..
        } => replace_text(current, needle, replacement, memory),
        Rewrite::RegexReplaceScalar { .. } => replace_regex(current, step, memory),
        Rewrite::NumericScalarArithmetic { .. } => {
            let arithmetic = step
                .arithmetic
                .as_ref()
                .ok_or_else(|| failed("arithmetic is not bound"))?;
            let value = super::super::native_relational_expression::binary(
                scalar::numeric_cell(current)?,
                arithmetic.operator,
                scalar::numeric_cell(arithmetic.operand.value())?,
                &arithmetic.left_dtype,
                &arithmetic.right_dtype,
                &step.dtype,
            )?;
            OwnedScalar::from_native(value, &step.dtype, memory)
        }
        Rewrite::ForwardFillNull { limit, .. } => {
            if matches!(current, ScalarValue::Null) {
                if let Some(value) = &fill.value
                    && limit.is_none_or(|n| fill.consecutive < n)
                {
                    fill.consecutive = fill
                        .consecutive
                        .checked_add(1)
                        .ok_or_else(|| failed("fill length overflow"))?;
                    return OwnedScalar::copy(value.value(), memory);
                }
            } else {
                fill.value = Some(OwnedScalar::copy(current, memory)?);
                fill.consecutive = 0;
            }
            OwnedScalar::copy(current, memory)
        }
        Rewrite::RowNumber { start, .. } => OwnedScalar::produce(memory, 0, || {
            Ok(ScalarValue::UInt64(
                ordinal
                    .checked_add(*start)
                    .ok_or_else(|| failed("row-number overflow"))?,
            ))
        }),
    }
}

fn replace_text(
    current: &ScalarValue,
    needle: &str,
    replacement: &str,
    memory: &LiveMemoryPool,
) -> Result<OwnedScalar> {
    if matches!(current, ScalarValue::Null) {
        return OwnedScalar::copy(current, memory);
    }
    let ScalarValue::Utf8(text) = current else {
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
    OwnedScalar::produce(memory, bytes, || {
        let mut value = String::new();
        value.try_reserve_exact(bytes).map_err(vortex_error)?;
        let mut end = 0;
        for (offset, matched) in text.match_indices(needle) {
            value.push_str(&text[end..offset]);
            value.push_str(replacement);
            end = offset + matched.len();
        }
        value.push_str(&text[end..]);
        Ok(ScalarValue::Utf8(value))
    })
}

fn replace_regex(
    current: &ScalarValue,
    step: &Step,
    memory: &LiveMemoryPool,
) -> Result<OwnedScalar> {
    if matches!(current, ScalarValue::Null) {
        return OwnedScalar::copy(current, memory);
    }
    let ScalarValue::Utf8(text) = current else {
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
    OwnedScalar::produce(memory, bound, || {
        Ok(ScalarValue::Utf8(
            regex.replace_all(text, replacement).into_owned(),
        ))
    })
}
