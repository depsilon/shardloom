//! Bound scalar kernels over native columns; no decoded row maps or JSON input.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{failed, index_array},
    native_relational_keys::{Cell, KeyColumn},
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{BinaryOp, ComparisonOp, Result, UnaryOp};
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{ChunkedArray, ConstantArray, ExtensionArray},
    dtype::DType,
    memory::MemorySessionExt as _,
    scalar::Scalar,
};

#[path = "native_relational_scalar.rs"]
pub(super) mod scalar;

pub(super) struct Expression {
    pub(super) dtype: DType,
    pub(super) kind: Kind,
}

pub(super) enum Kind {
    Column(String),
    Literal(Scalar),
    Unary(UnaryOp, Box<Expression>),
    Binary(Box<Expression>, BinaryOp, Box<Expression>),
    Compare(Box<Expression>, ComparisonOp, Box<Expression>),
    Conditional(Box<Expression>, Box<Expression>, Box<Expression>),
    Coalesce(Vec<Expression>),
    NullIf(Box<Expression>, Box<Expression>),
    Cast {
        input: Box<Expression>,
        tolerant: bool,
    },
    Function {
        function: scalar::Function,
        args: Vec<Expression>,
    },
}

impl Expression {
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
            Kind::Coalesce(children) | Kind::Function { args: children, .. } => {
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
        context.check_cancelled()?;
        let result = match &self.kind {
            Kind::Column(name) => super::logical_field_from_native_array(input, name)?,
            Kind::Literal(scalar) => {
                let constant = ConstantArray::new(scalar.clone(), input.len()).into_array();
                if let DType::Extension(extension) = &self.dtype {
                    ExtensionArray::try_new(extension.clone(), constant)
                        .map_err(vortex_error)?
                        .into_array()
                } else {
                    constant
                }
            }
            Kind::Unary(op, expression) => {
                let array = expression.evaluate(input, context)?;
                let values = keys(&array, context)?;
                self.build(input.len(), context, |row| {
                    unary(*op, values.raw_cell(row)?)
                })?
            }
            Kind::Binary(left, op, right) => {
                let left_values = keys(&left.evaluate(input, context)?, context)?;
                let right_values = keys(&right.evaluate(input, context)?, context)?;
                self.build(input.len(), context, |row| {
                    binary(
                        left_values.raw_cell(row)?,
                        *op,
                        right_values.raw_cell(row)?,
                        &left.dtype,
                        &right.dtype,
                        &self.dtype,
                    )
                })?
            }
            Kind::Compare(left, op, right) => {
                let left = keys(&left.evaluate(input, context)?, context)?;
                let right = keys(&right.evaluate(input, context)?, context)?;
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
            Kind::Coalesce(expressions) => self.coalesce(input, context, expressions)?,
            Kind::NullIf(left, right) => {
                let array = left.evaluate(input, context)?;
                let left = keys(&array, context)?;
                let right = keys(&right.evaluate(input, context)?, context)?;
                let mut execution = context.native_session().create_execution_ctx();
                self.build(input.len(), context, |row| {
                    if left.equals_at(row, &right, row, false)? {
                        Ok(Value::Null)
                    } else {
                        result_batch::scalar_value(&array, row, &mut execution)
                    }
                })?
            }
            Kind::Cast {
                input: expression,
                tolerant,
            } => {
                let values = keys(&expression.evaluate(input, context)?, context)?;
                let mut scratch = context.memory().reserve(0)?;
                self.build(input.len(), context, |row| {
                    scalar::cast(values.raw_cell(row)?, &self.dtype, *tolerant, &mut scratch)
                })?
            }
            Kind::Function { function, args } => {
                let mut values = ReservedVec::new(context.memory())?;
                values.reserve(args.len())?;
                for arg in args {
                    values
                        .values
                        .push(keys(&arg.evaluate(input, context)?, context)?);
                }
                let mut scratch = context.memory().reserve(0)?;
                self.build(input.len(), context, |row| {
                    function.evaluate(&values.values, row, &self.dtype, &mut scratch)
                })?
            }
        };
        context.check_cancelled()?;
        if result.dtype() != &self.dtype || result.len() != input.len() {
            return Err(failed(
                "scalar expression changed its bound schema or row count",
            ));
        }
        Ok(result)
    }

    fn build<'a>(
        &self,
        rows: usize,
        context: &NativeExecutionContext<'_>,
        mut value: impl FnMut(usize) -> Result<Value<'a>>,
    ) -> Result<ArrayRef> {
        if self.dtype == DType::Null {
            return Ok(ConstantArray::new(Scalar::null(DType::Null), rows).into_array());
        }
        result_batch::build_column(
            &self.dtype,
            rows,
            &context.native_session().allocator(),
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
            let keys = keys(&array, context)?;
            let mut next = ReservedVec::new(context.memory())?;
            for (index, &row) in remaining.values.iter().enumerate() {
                if index.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if keys.is_null(index)? {
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
    KeyColumn::new(
        array,
        &mut context.native_session().create_execution_ctx(),
        context.memory(),
        context.cancellation(),
    )
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
        result_batch::build_column(
            dtype,
            array.len(),
            &context.native_session().allocator(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                scalar::cast(values.raw_cell(row)?, dtype, false, &mut scratch)
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
        (UnaryOp::Negate, Cell::Float(bits)) => Value::Float(-f64::from_bits(bits)),
        (UnaryOp::Negate, Cell::Decimal(value, dtype)) => Value::Decimal(-value, dtype),
        (UnaryOp::Negate, value) => {
            Value::Int(i64::try_from(-integer(&value)?).map_err(vortex_error)?)
        }
        _ => return Err(failed("unary expression received an incompatible value")),
    })
}

fn binary(
    left: Cell,
    op: BinaryOp,
    right: Cell,
    left_dtype: &DType,
    right_dtype: &DType,
    dtype: &DType,
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
        if op == BinaryOp::Divide && right == 0.0 {
            return Err(failed("division by zero"));
        }
        let result = match op {
            BinaryOp::Add => left + right,
            BinaryOp::Subtract => left - right,
            BinaryOp::Multiply => left * right,
            BinaryOp::Divide => left / right,
            _ => unreachable!("boolean operation handled above"),
        };
        if !result.is_finite() {
            return Err(failed("nonfinite arithmetic result"));
        }
        return Ok(Value::Float(result));
    }
    let (left, right) = (integer(&left)?, integer(&right)?);
    let result = match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide => left.checked_div(right),
        _ => unreachable!("boolean operation handled above"),
    }
    .ok_or_else(|| failed("integer arithmetic overflow or division by zero"))?;
    if matches!(dtype, DType::Primitive(vortex::array::dtype::PType::U64, _)) {
        Ok(Value::UInt(u64::try_from(result).map_err(vortex_error)?))
    } else {
        Ok(Value::Int(i64::try_from(result).map_err(vortex_error)?))
    }
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

fn float(value: &Cell) -> Result<f64> {
    if let Cell::Float(bits) = value {
        return Ok(f64::from_bits(*bits));
    }
    let integer = integer(value)?;
    if !(-9_007_199_254_740_992..=9_007_199_254_740_992).contains(&integer) {
        return Err(failed(
            "mixed floating arithmetic requires an exactly representable integer",
        ));
    }
    #[allow(clippy::cast_precision_loss)] // Checked against the exact integer domain above.
    Ok(integer as f64)
}
