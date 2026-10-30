//! Bound scalar kernels over native columns; no decoded row maps or JSON input.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{failed, index_array},
    native_relational_keys::{Cell, KeyColumn},
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{BinaryOp, ComparisonOp, FloatPolicy, Result, UnaryOp};
use vortex::array::{
    ArrayRef, IntoArray, VortexSessionExecute as _,
    arrays::{ChunkedArray, ConstantArray, ListArray, StructArray},
    dtype::{DType, Nullability, PType},
    memory::HostMemorySessionExt as _,
    scalar::Scalar,
    validity::Validity,
};

#[path = "native_relational_scalar.rs"]
pub(super) mod scalar;

#[path = "native_relational_predicate_block.rs"]
mod predicate_block;

#[path = "native_relational_expression_reuse.rs"]
mod reuse;

#[path = "native_relational_expression_numeric.rs"]
mod numeric;

pub(super) use predicate_block::Recipe as FilterRecipe;

pub(super) const PREPARATION_BYTES: usize = 4096;

pub(super) struct Expression {
    pub(super) dtype: DType,
    pub(super) kind: Kind,
    recipe: Option<Box<FilterRecipe>>,
    reuse_output: Option<usize>,
}

// Each bound expression already owns this admission credit. The finite program
// owns at most eight bounded column names; child expressions are charged apart.
const _: () = assert!(
    std::mem::size_of::<Expression>() + predicate_block::MAX_RETAINED_BYTES + 256
        <= PREPARATION_BYTES
);

pub(super) enum Kind {
    Column(String),
    Literal(Scalar),
    List(Vec<Expression>),
    Struct(Vec<Expression>),
    Unary(UnaryOp, Box<Expression>),
    Binary(Box<Expression>, BinaryOp, Box<Expression>),
    Compare(Box<Expression>, ComparisonOp, Box<Expression>),
    Conditional(Box<Expression>, Box<Expression>, Box<Expression>),
    Coalesce(Vec<Expression>),
    NullIf(Box<Expression>, Box<Expression>),
    Cast {
        input: Box<Expression>,
        tolerant: bool,
        temporal: Option<Box<scalar::TemporalCast>>,
    },
    Function {
        function: scalar::Function,
        args: Vec<Expression>,
    },
}

impl Expression {
    pub(super) fn new(dtype: DType, kind: Kind) -> Self {
        let mut expression = Self {
            dtype,
            kind,
            recipe: None,
            reuse_output: None,
        };
        expression.recipe = FilterRecipe::compile(&expression).map(Box::new);
        expression
    }

    pub(super) fn filter_recipe(&self) -> Option<&FilterRecipe> {
        self.recipe.as_deref()
    }

    pub(super) fn visit_columns(&self, visit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
        match &self.kind {
            Kind::Column(name) => visit(name),
            Kind::Literal(_) => Ok(()),
            Kind::Unary(_, child) | Kind::Cast { input: child, .. } => child.visit_columns(visit),
            Kind::Binary(left, _, right)
            | Kind::Compare(left, _, right)
            | Kind::NullIf(left, right) => {
                left.visit_columns(visit)?;
                right.visit_columns(visit)
            }
            Kind::Conditional(condition, yes, no) => {
                condition.visit_columns(visit)?;
                yes.visit_columns(visit)?;
                no.visit_columns(visit)
            }
            Kind::Coalesce(children)
            | Kind::List(children)
            | Kind::Struct(children)
            | Kind::Function { args: children, .. } => {
                for child in children {
                    child.visit_columns(visit)?;
                }
                Ok(())
            }
        }
    }
    pub(super) fn evaluate(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        self.evaluate_in_scope(input, context, None)
    }

    /// Only the current projection supplies completed outputs from this exact
    /// input. Selected sub-inputs and lazy branches use ordinary `evaluate`.
    pub(super) fn evaluate_reusing(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        completed: &[ArrayRef],
    ) -> Result<ArrayRef> {
        self.evaluate_in_scope(input, context, Some(completed))
    }

    fn evaluate_in_scope(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        completed: Option<&[ArrayRef]>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        if let Some((completed, index)) = completed.zip(self.reuse_output) {
            return self.completed_result(completed, index, input.len(), context);
        }
        if let Some(recipe) = &self.recipe {
            let result = recipe.evaluate(self, input, context)?;
            return self.checked_result(result, input.len(), context);
        }
        let result = match &self.kind {
            Kind::Column(name) => super::logical_field_from_native_array(input, name)?,
            Kind::Literal(scalar) => self.literal_array(scalar, input.len(), context)?,
            Kind::List(children) | Kind::Struct(children) => {
                self.construct(input, context, children)?
            }
            Kind::Unary(op, expression) => {
                let array = expression.evaluate_in_scope(input, context, completed)?;
                if super::native_payload::is_nested(array.dtype())
                    && matches!(op, UnaryOp::IsNull | UnaryOp::IsNotNull)
                {
                    let valid = parent_validity(&array, context)?;
                    self.build(input.len(), context, |row| {
                        Ok(Value::Bool(valid.value(row) == (*op == UnaryOp::IsNotNull)))
                    })?
                } else {
                    let values = keys(&array, context)?;
                    self.build(input.len(), context, |row| {
                        unary(*op, values.raw_cell(row)?)
                    })?
                }
            }
            Kind::Binary(left, op, right) => {
                self.evaluate_binary(input, context, completed, left, *op, right)?
            }
            Kind::Compare(left, op, right) => {
                let left = keys(&left.evaluate_in_scope(input, context, completed)?, context)?;
                let right = keys(
                    &right.evaluate_in_scope(input, context, completed)?,
                    context,
                )?;
                self.build(input.len(), context, |row| {
                    if left.is_null(row)? || right.is_null(row)? {
                        return Ok(Value::Null);
                    }
                    Ok(Value::Bool(compare(
                        left.compare_at(row, &right, row)?,
                        *op,
                    )))
                })?
            }
            Kind::Conditional(condition, yes, no) => {
                let condition = keys(&condition.evaluate(input, context)?, context)?;
                self.select(input, context, yes, no, |row| {
                    Ok(condition.cell(row)? == Cell::Boolean(true))
                })?
            }
            Kind::Coalesce(expressions) => {
                if let Some(output) = numeric::coalesce(self, input, context, expressions)? {
                    output
                } else {
                    self.coalesce(input, context, expressions)?
                }
            }
            Kind::NullIf(left, right) => self.null_if(input, context, left, right)?,
            Kind::Cast {
                input: expression,
                tolerant,
                temporal,
            } => {
                let values = keys(&expression.evaluate(input, context)?, context)?;
                let mut scratch = context.memory().reserve(0)?;
                self.build(input.len(), context, |row| {
                    scalar::cast(
                        values.raw_cell(row)?,
                        &self.dtype,
                        *tolerant,
                        temporal.as_deref(),
                        &mut scratch,
                        context.float_policy(),
                    )
                })?
            }
            Kind::Function { function, args } => {
                self.evaluate_function(input, context, function, args, completed)?
            }
        };
        self.checked_result(result, input.len(), context)
    }

    fn evaluate_binary(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        completed: Option<&[ArrayRef]>,
        left: &Self,
        op: BinaryOp,
        right: &Self,
    ) -> Result<ArrayRef> {
        let left_values = keys(&left.evaluate_in_scope(input, context, completed)?, context)?;
        let right_values = keys(
            &right.evaluate_in_scope(input, context, completed)?,
            context,
        )?;
        if let Some(output) =
            numeric::binary(&left_values, op, &right_values, &self.dtype, context)?
        {
            return Ok(output);
        }
        self.build(input.len(), context, |row| {
            binary_with_float_policy(
                left_values.raw_cell(row)?,
                op,
                right_values.raw_cell(row)?,
                &left.dtype,
                &right.dtype,
                &self.dtype,
                context.float_policy(),
            )
        })
    }

    fn evaluate_function(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        function: &scalar::Function,
        args: &[Self],
        completed: Option<&[ArrayRef]>,
    ) -> Result<ArrayRef> {
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(args.len())?;
        for arg in args {
            values.values.push(keys(
                &arg.evaluate_in_scope(input, context, completed)?,
                context,
            )?);
        }
        let mut scratch = context.memory().reserve(0)?;
        self.build(input.len(), context, |row| {
            function.evaluate(&values.values, row, &self.dtype, &mut scratch)
        })
    }

    fn completed_result(
        &self,
        completed: &[ArrayRef],
        index: usize,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let result = completed
            .get(index)
            .ok_or_else(|| failed("prepared expression reuse precedes its output"))?;
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.expression_output_reuses += 1;
        });
        self.checked_result(result.clone(), rows, context)
    }

    fn literal_array(
        &self,
        scalar: &Scalar,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if crate::native_payload_schema::temporal_storage(&self.dtype).is_some() {
            // The pinned provider reduces Extension(Constant(storage)) through
            // a calendar scalar with a narrower domain. A later take (including
            // result ownership) can panic for valid Date32/Int64 values. Keep
            // these literals in credited buffers under their native dtype.
            let value = scalar
                .as_primitive()
                .as_::<i64>()
                .ok_or_else(|| failed("temporal literal has invalid primitive storage"))?;
            self.build(rows, context, |_| Ok(Value::Int(value)))
        } else {
            Ok(ConstantArray::new(scalar.clone(), rows).into_array())
        }
    }

    fn checked_result(
        &self,
        result: ArrayRef,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        context.check_cancelled()?;
        if result.dtype() != &self.dtype || result.len() != rows {
            return Err(failed(
                "scalar expression changed its bound schema or row count",
            ));
        }
        Ok(result)
    }

    fn construct(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        children: &[Expression],
    ) -> Result<ArrayRef> {
        let _metadata = context
            .memory()
            .reserve(super::native_payload::metadata_bytes(&self.dtype)?)?;
        let mut arrays = ReservedVec::new(context.memory())?;
        arrays.reserve(children.len())?;
        for child in children {
            let array = child.evaluate(input, context)?;
            arrays
                .values
                .push(if let DType::List(element, _) = &self.dtype {
                    cast(&array, element, context)?
                } else {
                    array
                });
        }
        let (arrays, _ownership) = arrays.into_parts();
        match &self.dtype {
            DType::Struct(fields, _) => StructArray::try_new(
                fields.names().clone(),
                arrays,
                input.len(),
                Validity::NonNullable,
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error),
            DType::List(element, _) => {
                let count = input
                    .len()
                    .checked_mul(children.len())
                    .ok_or_else(|| failed("list constructor cardinality overflow"))?;
                let values = if count == 0 {
                    super::native_payload::defaults(element, 0, context)?
                } else {
                    let values = ChunkedArray::try_new(arrays, element.as_ref().clone())
                        .map_err(vortex_error)?
                        .into_array();
                    let indices = index_array(count, false, context, |index| {
                        if index.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        Ok(Some(
                            index / children.len() + (index % children.len()) * input.len(),
                        ))
                    })?;
                    super::native_relational_batch::take_column(
                        &values, &indices, element, context,
                    )?
                };
                let offsets = result_batch::build_column_with_float_policy(
                    &DType::Primitive(PType::U64, Nullability::NonNullable),
                    input
                        .len()
                        .checked_add(1)
                        .ok_or_else(|| failed("list constructor offset overflow"))?,
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |row| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        Ok(Value::UInt(
                            u64::try_from(row * children.len()).map_err(vortex_error)?,
                        ))
                    },
                )?;
                ListArray::try_new(
                    values,
                    offsets,
                    Validity::NonNullable,
                    &mut context.native_session().create_execution_ctx(),
                )
                .map(IntoArray::into_array)
                .map_err(vortex_error)
            }
            _ => Err(failed(
                "constructor requires its bound list or struct schema",
            )),
        }
    }

    fn null_if(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        left: &Expression,
        right: &Expression,
    ) -> Result<ArrayRef> {
        let array = left.evaluate(input, context)?;
        let left = keys(&array, context)?;
        let right = keys(&right.evaluate(input, context)?, context)?;
        if super::native_payload::is_nested(&self.dtype) {
            let indices = index_array(input.len(), true, context, |row| {
                Ok((!left.equals_at(row, &right, row, false)?).then_some(row))
            })?;
            super::native_relational_batch::take_column(&array, &indices, &self.dtype, context)
        } else {
            let mut execution = context.native_session().create_execution_ctx();
            self.build(input.len(), context, |row| {
                if left.equals_at(row, &right, row, false)? {
                    Ok(Value::Null)
                } else {
                    result_batch::scalar_value(&array, row, &mut execution)
                }
            })
        }
    }

    fn build<'a>(
        &self,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        mut value: impl FnMut(usize) -> Result<Value<'a>>,
    ) -> Result<ArrayRef> {
        if super::native_payload::is_nested(&self.dtype) {
            if rows != 0 {
                return Err(failed("nested expressions require native selected values"));
            }
            return super::native_payload::defaults(&self.dtype, 0, context);
        }
        if self.dtype == DType::Null {
            return Ok(ConstantArray::new(Scalar::null(DType::Null), rows).into_array());
        }
        result_batch::build_expression_column(
            &self.dtype,
            rows,
            &context.native_session().host_allocator(),
            context.float_policy(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                value(row)
            },
        )
    }

    // Only selected rows enter each branch. In particular, an unused divide by
    // zero, invalid cast, or nested conditional cannot fail the selected branch.
    fn select(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        yes: &Expression,
        no: &Expression,
        mut condition: impl FnMut(usize) -> Result<bool>,
    ) -> Result<ArrayRef> {
        let mut yes_rows = ReservedVec::new(context.memory())?;
        let mut no_rows = ReservedVec::new(context.memory())?;
        let mut output = ReservedVec::new(context.memory())?;
        output.reserve(input.len())?;
        for row in 0..input.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let selected = condition(row)?;
            let rows = if selected {
                &mut yes_rows
            } else {
                &mut no_rows
            };
            output.values.push((selected, rows.values.len()));
            rows.push(row)?;
        }
        let mut arrays = ReservedVec::new(context.memory())?;
        arrays.reserve(2)?;
        for (expression, rows) in [(yes, &yes_rows.values), (no, &no_rows.values)] {
            if rows.is_empty() {
                continue;
            }
            let indices = index_array(rows.len(), false, context, |row| Ok(Some(rows[row])))?;
            let selected = input.take(indices).map_err(vortex_error)?;
            let result = expression.evaluate(&selected, context)?;
            arrays.values.push(cast(&result, &self.dtype, context)?);
        }
        let (arrays, _ownership) = arrays.into_parts();
        if arrays.is_empty() {
            return self.build(0, context, |_| Ok(Value::Null));
        }
        let values = ChunkedArray::try_new(arrays, self.dtype.clone())
            .map_err(vortex_error)?
            .into_array();
        let indices = index_array(input.len(), false, context, |row| {
            let (selected, index) = output.values[row];
            Ok(Some(if selected {
                index
            } else {
                yes_rows.values.len() + index
            }))
        })?;
        super::native_relational_batch::take_column(&values, &indices, &self.dtype, context)
    }

    fn coalesce(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        expressions: &[Expression],
    ) -> Result<ArrayRef> {
        let mut remaining = ReservedVec::new(context.memory())?;
        remaining.reserve(input.len())?;
        remaining.values.extend(0..input.len());
        let mut arrays = ReservedVec::new(context.memory())?;
        let mut positions = ReservedVec::new(context.memory())?;
        positions.reserve(input.len())?;
        positions.values.resize(input.len(), None);
        let mut total = 0usize;
        for expression in expressions {
            if remaining.values.is_empty() {
                break;
            }
            context.check_cancelled()?;
            let indices = index_array(remaining.values.len(), false, context, |row| {
                Ok(Some(remaining.values[row]))
            })?;
            let selected = input.take(indices).map_err(vortex_error)?;
            let array = cast(
                &expression.evaluate(&selected, context)?,
                &self.dtype.as_nullable(),
                context,
            )?;
            let nested = super::native_payload::is_nested(array.dtype());
            let valid = nested
                .then(|| parent_validity(&array, context))
                .transpose()?;
            let keys = (!nested).then(|| keys(&array, context)).transpose()?;
            let mut next = ReservedVec::new(context.memory())?;
            for (index, &row) in remaining.values.iter().enumerate() {
                if index.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let missing = if let Some(valid) = &valid {
                    !valid.value(index)
                } else {
                    keys.as_ref()
                        .ok_or_else(|| failed("coalesce validity owner absent"))?
                        .is_null(index)?
                };
                if missing {
                    next.push(row)?;
                } else {
                    positions.values[row] = Some(
                        total
                            .checked_add(index)
                            .ok_or_else(|| failed("coalesce ordinal overflow"))?,
                    );
                }
            }
            total = total
                .checked_add(array.len())
                .ok_or_else(|| failed("coalesce cardinality overflow"))?;
            arrays.push(array)?;
            remaining = next;
        }
        if arrays.values.is_empty() {
            return self.build(input.len(), context, |_| Ok(Value::Null));
        }
        let (arrays, _ownership) = arrays.into_parts();
        let values = ChunkedArray::try_new(arrays, self.dtype.as_nullable())
            .map_err(vortex_error)?
            .into_array();
        let indices = index_array(input.len(), self.dtype.is_nullable(), context, |row| {
            Ok(positions.values[row])
        })?;
        super::native_relational_batch::take_column(&values, &indices, &self.dtype, context)
    }
}

pub(super) fn keys(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<KeyColumn> {
    KeyColumn::values_with_float_policy(
        array,
        &mut context.native_session().create_execution_ctx(),
        context.memory(),
        context.cancellation(),
        context.float_policy(),
    )
}

/// Parent validity does not require preparing nested coordinates or child keys.
pub(super) fn parent_validity(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<vortex::mask::Mask> {
    context.check_cancelled()?;
    let valid = array
        .validity()
        .map_err(vortex_error)?
        .execute_mask(
            array.len(),
            &mut context.native_session().create_execution_ctx(),
        )
        .map_err(vortex_error)?;
    context.check_cancelled()?;
    Ok(valid)
}

fn cast(array: &ArrayRef, dtype: &DType, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    if array.dtype() == &DType::Null {
        Ok(ConstantArray::new(Scalar::null(dtype.as_nullable()), array.len()).into_array())
    } else if matches!((array.dtype(), dtype),
        (DType::Decimal(from, _), DType::Decimal(to, _)) if from != to)
    {
        // Precision/scale promotion can allocate. Keep it in the same fallible
        // result allocator as explicit casts, after selecting the live branch.
        let values = keys(array, context)?;
        let mut scratch = context.memory().reserve(0)?;
        result_batch::build_column_with_float_policy(
            dtype,
            array.len(),
            &context.native_session().host_allocator(),
            context.float_policy(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                scalar::cast(
                    values.raw_cell(row)?,
                    dtype,
                    false,
                    None,
                    &mut scratch,
                    context.float_policy(),
                )
            },
        )
    } else {
        use vortex::array::builtins::ArrayBuiltins as _;
        array.cast(dtype.clone()).map_err(vortex_error)
    }
}

fn compare(order: std::cmp::Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => order.is_eq(),
        ComparisonOp::NotEq => !order.is_eq(),
        ComparisonOp::Lt => order.is_lt(),
        ComparisonOp::LtEq => !order.is_gt(),
        ComparisonOp::Gt => order.is_gt(),
        ComparisonOp::GtEq => !order.is_lt(),
    }
}

fn unary(op: UnaryOp, value: Cell) -> Result<Value<'static>> {
    Ok(match (op, value) {
        (UnaryOp::IsNull, value) => Value::Bool(value == Cell::Null),
        (UnaryOp::IsNotNull, value) => Value::Bool(value != Cell::Null),
        (_, Cell::Null) => Value::Null,
        (UnaryOp::Not, Cell::Boolean(value)) => Value::Bool(!value),
        (UnaryOp::Negate, Cell::Float(bits)) => Value::Float(
            shardloom_core::float_policy::canonicalize_f64_result(-f64::from_bits(bits)),
        ),
        (UnaryOp::Negate, Cell::Decimal(value, dtype)) => Value::Decimal(-value, dtype),
        (UnaryOp::Negate, value) => {
            Value::Int(i64::try_from(-integer(&value)?).map_err(vortex_error)?)
        }
        _ => return Err(failed("unary expression received an incompatible value")),
    })
}

pub(super) fn binary_with_float_policy(
    left: Cell,
    op: BinaryOp,
    right: Cell,
    left_dtype: &DType,
    right_dtype: &DType,
    dtype: &DType,
    float_policy: FloatPolicy,
) -> Result<Value<'static>> {
    if matches!(op, BinaryOp::And | BinaryOp::Or) {
        return boolean(left, op, right);
    }
    if left == Cell::Null || right == Cell::Null {
        return Ok(Value::Null);
    }
    if let DType::Decimal(dtype, _) = dtype {
        let value = |cell: &Cell| match cell {
            Cell::Decimal(value, _) => Ok(*value),
            other => integer(other),
        };
        let result = scalar::decimal_operand(value(&left)?, left_dtype)?
            .checked_binary(op, scalar::decimal_operand(value(&right)?, right_dtype)?)?;
        return Ok(Value::Decimal(result.value(), *dtype));
    }
    if matches!(dtype, DType::Primitive(vortex::array::dtype::PType::F64, _)) {
        let (left, right) = (float(&left)?, float(&right)?);
        return floating_binary(left, op, right, float_policy).map(Value::Float);
    }
    let (left, right) = (integer(&left)?, integer(&right)?);
    let result = integer_binary(left, op, right)?;
    if matches!(dtype, DType::Primitive(vortex::array::dtype::PType::U64, _)) {
        unsigned_result(result).map(Value::UInt)
    } else {
        signed_result(result).map(Value::Int)
    }
}

fn floating_binary(left: f64, op: BinaryOp, right: f64, float_policy: FloatPolicy) -> Result<f64> {
    if float_policy == FloatPolicy::Strict
        && matches!(op, BinaryOp::Divide | BinaryOp::Remainder)
        && right == 0.0
    {
        return Err(failed("division by zero"));
    }
    let result = match op {
        BinaryOp::Add => left + right,
        BinaryOp::Subtract => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => left / right,
        BinaryOp::Remainder => left % right,
        _ => unreachable!("boolean operation handled above"),
    };
    if !float_policy.admits(result) {
        return Err(failed("nonfinite arithmetic result"));
    }
    Ok(shardloom_core::float_policy::canonicalize_f64_result(
        result,
    ))
}

fn integer_binary(left: i128, op: BinaryOp, right: i128) -> Result<i128> {
    match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::Remainder => left.checked_rem(right),
        _ => unreachable!("boolean operation handled above"),
    }
    .ok_or_else(|| failed("integer arithmetic overflow or division by zero"))
}

fn unsigned_result(value: i128) -> Result<u64> {
    u64::try_from(value).map_err(|_| failed("unsigned integer arithmetic overflow"))
}

fn signed_result(value: i128) -> Result<i64> {
    i64::try_from(value).map_err(|_| failed("signed integer arithmetic overflow"))
}

fn boolean(left: Cell, op: BinaryOp, right: Cell) -> Result<Value<'static>> {
    use Cell::{Boolean, Null};
    Ok(match (op, left, right) {
        (BinaryOp::And, Boolean(false), _) | (BinaryOp::And, _, Boolean(false)) => {
            Value::Bool(false)
        }
        (BinaryOp::Or, Boolean(true), _) | (BinaryOp::Or, _, Boolean(true)) => Value::Bool(true),
        (_, Null, _) | (_, _, Null) => Value::Null,
        (BinaryOp::And, Boolean(a), Boolean(b)) => Value::Bool(a && b),
        (BinaryOp::Or, Boolean(a), Boolean(b)) => Value::Bool(a || b),
        _ => return Err(failed("boolean expression received a non-boolean value")),
    })
}

fn integer(value: &Cell) -> Result<i128> {
    match value {
        Cell::NegativeInteger(value) => Ok(i128::from(*value)),
        Cell::NonnegativeInteger(value) => Ok(i128::from(*value)),
        _ => Err(failed("numeric expression requires an integer")),
    }
}

pub(in crate::local_primitives) fn float(value: &Cell) -> Result<f64> {
    if let Cell::Float(bits) = value {
        return Ok(f64::from_bits(*bits));
    }
    let integer = integer(value)?;
    if !(-9_007_199_254_740_992..=9_007_199_254_740_992).contains(&integer) {
        return Err(failed(
            "mixed floating arithmetic or literal comparison requires an exactly representable integer",
        ));
    }
    #[allow(clippy::cast_precision_loss)] // Checked against the exact integer domain above.
    Ok(integer as f64)
}
