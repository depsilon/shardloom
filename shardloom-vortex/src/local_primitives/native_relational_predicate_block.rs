//! Finite pure predicates share native column preparation and Boolean words.
//! NULL is neither true nor false. Only identical ordered nodes are reused;
//! arithmetic, coercions, functions and lazy branches stay in their existing owner.

use super::{
    ArrayRef, BinaryOp, ComparisonOp, DType, Expression, KeyColumn, Kind, NativeExecutionContext,
    PType, Result, UnaryOp, Value, failed, keys,
};
use crate::local_primitives::native_numeric_owner::NativeNumericOwner;
use vortex::{
    array::scalar::{PValue, ScalarValue},
    mask::Mask,
};

#[cfg(test)]
#[path = "native_relational_predicate_block_tests.rs"]
mod tests;

const MAX_COLUMNS: usize = 8;
const MAX_INSTRUCTIONS: usize = 32;
const MAX_VISITS: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Instruction {
    Empty,
    Compare {
        column: usize,
        op: ComparisonOp,
        literal: i64,
    },
    And(usize, usize),
    Or(usize, usize),
    Not(usize),
}

#[derive(Clone, Copy, Default)]
struct Truth {
    yes: u64,
    no: u64,
}

pub(super) struct Recipe<'a> {
    columns: [Option<&'a Expression>; MAX_COLUMNS],
    column_count: usize,
    instructions: [Instruction; MAX_INSTRUCTIONS],
    instruction_count: usize,
    visits: usize,
    comparisons: usize,
    root: usize,
}

impl<'a> Recipe<'a> {
    pub(super) fn compile(expression: &'a Expression) -> Option<Self> {
        if !matches!(
            expression.kind,
            Kind::Binary(_, BinaryOp::And | BinaryOp::Or, _) | Kind::Unary(UnaryOp::Not, _)
        ) {
            return None;
        }
        let mut recipe = Self {
            columns: [None; MAX_COLUMNS],
            column_count: 0,
            instructions: [Instruction::Empty; MAX_INSTRUCTIONS],
            instruction_count: 0,
            visits: 0,
            comparisons: 0,
            root: 0,
        };
        recipe.root = recipe.lower(expression)?;
        (recipe.comparisons >= 2).then_some(recipe)
    }

    fn lower(&mut self, expression: &'a Expression) -> Option<usize> {
        if self.visits == MAX_VISITS || !matches!(expression.dtype, DType::Bool(_)) {
            return None;
        }
        self.visits += 1;
        let instruction = match &expression.kind {
            Kind::Binary(left, BinaryOp::And, right) => {
                Instruction::And(self.lower(left)?, self.lower(right)?)
            }
            Kind::Binary(left, BinaryOp::Or, right) => {
                Instruction::Or(self.lower(left)?, self.lower(right)?)
            }
            Kind::Unary(UnaryOp::Not, child) => Instruction::Not(self.lower(child)?),
            Kind::Compare(left, op, right) => self.comparison(left, *op, right)?,
            _ => return None,
        };
        if let Some(index) = self.instructions[..self.instruction_count]
            .iter()
            .position(|prior| *prior == instruction)
        {
            return Some(index);
        }
        if self.instruction_count == MAX_INSTRUCTIONS {
            return None;
        }
        let index = self.instruction_count;
        self.instructions[index] = instruction;
        self.instruction_count += 1;
        Some(index)
    }

    fn comparison(
        &mut self,
        left: &'a Expression,
        op: ComparisonOp,
        right: &'a Expression,
    ) -> Option<Instruction> {
        if !matches!(left.dtype, DType::Primitive(PType::I64, _))
            || !matches!(right.dtype, DType::Primitive(PType::I64, _))
        {
            return None;
        }
        let (column, literal_expression, literal, op) = match (&left.kind, &right.kind) {
            (Kind::Column(_), Kind::Literal(value)) => (left, right, value, op),
            (Kind::Literal(value), Kind::Column(_)) => (right, left, value, reverse(op)),
            _ => return None,
        };
        if literal.dtype() != &literal_expression.dtype {
            return None;
        }
        let Some(ScalarValue::Primitive(PValue::I64(literal))) = literal.value() else {
            return None;
        };
        let index = self.column(column)?;
        self.comparisons += 1;
        Some(Instruction::Compare {
            column: index,
            op,
            literal: *literal,
        })
    }

    fn column(&mut self, expression: &'a Expression) -> Option<usize> {
        let Kind::Column(name) = &expression.kind else {
            return None;
        };
        if let Some(index) = self.columns[..self.column_count].iter().position(|prior| {
            prior.is_some_and(|prior| {
                prior.dtype == expression.dtype
                    && matches!(&prior.kind, Kind::Column(prior) if prior == name)
            })
        }) {
            return Some(index);
        }
        if self.column_count == MAX_COLUMNS {
            return None;
        }
        let index = self.column_count;
        self.columns[index] = Some(expression);
        self.column_count += 1;
        Some(index)
    }

    pub(super) fn evaluate(
        &self,
        expression: &Expression,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut owners: [Option<NativeNumericOwner>; MAX_COLUMNS] = std::array::from_fn(|_| None);
        for (index, column) in self.columns[..self.column_count].iter().enumerate() {
            context.check_cancelled()?;
            let column = column.expect("compiled column binding");
            let Kind::Column(name) = &column.kind else {
                unreachable!("compiled column binding");
            };
            let array = super::super::logical_field_from_native_array(input, name)?;
            if array.dtype() != &column.dtype || array.len() != input.len() {
                return Err(failed(
                    "scalar expression changed its bound schema or row count",
                ));
            }
            let KeyColumn::Numeric(owner) = keys(&array, context)? else {
                return Err(failed("pure predicate requires its bound I64 native owner"));
            };
            owners[index] = Some(owner);
        }
        let mut truth = Truth::default();
        expression.build(input.len(), context, |row| {
            if row.is_multiple_of(64) {
                context.check_cancelled()?;
                truth = self.block(&owners, row, input.len().min(row.saturating_add(64)))?;
            }
            let bit = 1_u64 << (row % 64);
            Ok(if truth.yes & bit != 0 {
                Value::Bool(true)
            } else if truth.no & bit != 0 {
                Value::Bool(false)
            } else {
                Value::Null
            })
        })
    }

    fn block(
        &self,
        owners: &[Option<NativeNumericOwner>; MAX_COLUMNS],
        start: usize,
        end: usize,
    ) -> Result<Truth> {
        let mut words = [Truth::default(); MAX_INSTRUCTIONS];
        for (index, instruction) in self.instructions[..self.instruction_count]
            .iter()
            .enumerate()
        {
            words[index] = match *instruction {
                Instruction::Compare {
                    column,
                    op,
                    literal,
                } => {
                    let (values, valid) = owners[column]
                        .as_ref()
                        .and_then(NativeNumericOwner::i64_values_with_validity)
                        .ok_or_else(|| failed("pure predicate lost its I64 native owner"))?;
                    compare_block(values, valid, start, end, op, literal)?
                }
                Instruction::And(left, right) => Truth {
                    yes: words[left].yes & words[right].yes,
                    no: words[left].no | words[right].no,
                },
                Instruction::Or(left, right) => Truth {
                    yes: words[left].yes | words[right].yes,
                    no: words[left].no & words[right].no,
                },
                Instruction::Not(child) => Truth {
                    yes: words[child].no,
                    no: words[child].yes,
                },
                Instruction::Empty => unreachable!("compiled instruction"),
            };
        }
        Ok(words[self.root])
    }
}

fn reverse(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq | ComparisonOp::NotEq => op,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::LtEq => ComparisonOp::GtEq,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::GtEq => ComparisonOp::LtEq,
    }
}

fn compare_block(
    values: &[i64],
    valid: &Mask,
    start: usize,
    end: usize,
    op: ComparisonOp,
    literal: i64,
) -> Result<Truth> {
    let values = values
        .get(start..end)
        .ok_or_else(|| failed("pure predicate block exceeds its native column"))?;
    if valid.len() < end {
        return Err(failed("pure predicate validity is shorter than its column"));
    }
    Ok(match op {
        ComparisonOp::Eq => compare_values(values, valid, start, literal, |a, b| a == b),
        ComparisonOp::NotEq => compare_values(values, valid, start, literal, |a, b| a != b),
        ComparisonOp::Lt => compare_values(values, valid, start, literal, |a, b| a < b),
        ComparisonOp::LtEq => compare_values(values, valid, start, literal, |a, b| a <= b),
        ComparisonOp::Gt => compare_values(values, valid, start, literal, |a, b| a > b),
        ComparisonOp::GtEq => compare_values(values, valid, start, literal, |a, b| a >= b),
    })
}

fn compare_values(
    values: &[i64],
    valid: &Mask,
    start: usize,
    literal: i64,
    test: impl Fn(i64, i64) -> bool,
) -> Truth {
    let mut truth = Truth::default();
    let all_valid = valid.all_true();
    for (offset, value) in values.iter().enumerate() {
        if all_valid || valid.value(start + offset) {
            if test(*value, literal) {
                truth.yes |= 1_u64 << offset;
            } else {
                truth.no |= 1_u64 << offset;
            }
        }
    }
    truth
}
