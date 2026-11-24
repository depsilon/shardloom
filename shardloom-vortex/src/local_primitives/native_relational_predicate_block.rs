//! Finite pure predicates share native column preparation and Boolean words.
//! NULL is neither true nor false. Only identical ordered nodes are reused;
//! arithmetic, coercions, functions and lazy branches stay in their existing owner.

use super::{
    ArrayRef, BinaryOp, ComparisonOp, DType, Expression, KeyColumn, Kind, NativeExecutionContext,
    PType, Result, UnaryOp, failed, keys, parent_validity, result_batch,
};
use crate::local_primitives::{
    native_numeric_owner::NativeNumericOwner, native_relational_keys::validate_float_scalar,
};
use shardloom_core::{FloatPolicy, float_policy::float_key_bits};
use std::cmp::Ordering;
use vortex::{
    array::{
        arrays::{BoolArray, bool::BoolArrayExt as _},
        dtype::half::f16,
        memory::HostMemorySessionExt as _,
        scalar::{PValue, Scalar, ScalarValue},
    },
    buffer::BitBuffer,
    mask::{AllOr, Mask},
};

#[cfg(test)]
#[path = "native_relational_predicate_block_tests.rs"]
mod tests;

const MAX_COLUMNS: usize = 8;
const MAX_INSTRUCTIONS: usize = 32;
const MAX_VISITS: usize = 64;
const MAX_COLUMN_NAME_BYTES: usize = 256;

pub(super) const MAX_RETAINED_BYTES: usize =
    std::mem::size_of::<Recipe>() + MAX_COLUMNS * MAX_COLUMN_NAME_BYTES;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Bool,
    SignedInt,
    UnsignedInt,
    Float,
    Temporal { is_date32: bool },
    ValidityOnly,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LiteralValue {
    Signed(i64),
    Unsigned(u64),
    Float(u64),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Instruction {
    Empty,
    CompareLiteral {
        column: u8,
        op: ComparisonOp,
        literal: LiteralValue,
    },
    CompareColumns {
        left: u8,
        op: ComparisonOp,
        right: u8,
    },
    BoolColumn(u8),
    IsNull(u8),
    IsNotNull(u8),
    And(u8, u8),
    Or(u8, u8),
    Not(u8),
}

#[derive(Clone, Copy, Default)]
struct Truth {
    yes: u64,
    no: u64,
}

struct Column {
    name: Box<str>,
    nullable: bool,
    kind: ColumnKind,
}

enum ColumnOwner {
    Numeric(NativeNumericOwner),
    Bool { values: BoolArray, valid: Mask },
    Validity(Mask),
}

impl ColumnOwner {
    fn validity(&self) -> &Mask {
        match self {
            Self::Numeric(owner) => owner.validity(),
            Self::Bool { valid, .. } | Self::Validity(valid) => valid,
        }
    }
}

pub(in crate::local_primitives) struct Recipe {
    columns: [Option<Column>; MAX_COLUMNS],
    column_count: usize,
    instructions: [Instruction; MAX_INSTRUCTIONS],
    instruction_count: usize,
    visits: usize,
    comparisons: usize,
    root: usize,
}

impl Recipe {
    pub(in crate::local_primitives) fn compile(expression: &Expression) -> Option<Self> {
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.predicate_compile_attempts += 1;
        });
        if matches!(expression.kind, Kind::Column(_)) || !matches!(expression.dtype, DType::Bool(_))
        {
            return None;
        }
        let mut recipe = Self {
            columns: std::array::from_fn(|_| None),
            column_count: 0,
            instructions: [Instruction::Empty; MAX_INSTRUCTIONS],
            instruction_count: 0,
            visits: 0,
            comparisons: 0,
            root: 0,
        };
        recipe.root = recipe.lower(expression)?;
        Some(recipe)
    }

    fn lower(&mut self, expression: &Expression) -> Option<usize> {
        if self.visits == MAX_VISITS || !matches!(expression.dtype, DType::Bool(_)) {
            return None;
        }
        self.visits += 1;
        let instruction = match &expression.kind {
            Kind::Binary(left, BinaryOp::And, right) => {
                let left = self.lower(left)?;
                let right = self.lower(right)?;
                Instruction::And(u8::try_from(left).ok()?, u8::try_from(right).ok()?)
            }
            Kind::Binary(left, BinaryOp::Or, right) => {
                let left = self.lower(left)?;
                let right = self.lower(right)?;
                Instruction::Or(u8::try_from(left).ok()?, u8::try_from(right).ok()?)
            }
            Kind::Unary(UnaryOp::Not, child) => {
                let child = self.lower(child)?;
                Instruction::Not(u8::try_from(child).ok()?)
            }
            Kind::Unary(op @ (UnaryOp::IsNull | UnaryOp::IsNotNull), child) => {
                if !matches!(child.kind, Kind::Column(_)) {
                    return None;
                }
                let column = self.column(child, true)?;
                if *op == UnaryOp::IsNull {
                    Instruction::IsNull(column)
                } else {
                    Instruction::IsNotNull(column)
                }
            }
            Kind::Column(_) => {
                let column = self.column(expression, false)?;
                Instruction::BoolColumn(column)
            }
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
        left: &Expression,
        op: ComparisonOp,
        right: &Expression,
    ) -> Option<Instruction> {
        match (&left.kind, &right.kind) {
            (Kind::Column(_), Kind::Literal(scalar)) => {
                self.compare_column_literal(left, op, right, scalar)
            }
            (Kind::Literal(scalar), Kind::Column(_)) => {
                self.compare_column_literal(right, reverse(op), left, scalar)
            }
            (Kind::Column(_), Kind::Column(_)) => self.compare_column_column(left, op, right),
            _ => None,
        }
    }

    fn compare_column_literal(
        &mut self,
        column: &Expression,
        op: ComparisonOp,
        literal_expr: &Expression,
        scalar: &Scalar,
    ) -> Option<Instruction> {
        let col_kind = value_column_kind(&column.dtype)?;
        let literal = extract_literal_value(col_kind, &column.dtype, literal_expr, scalar)?;
        let col_idx = self.column(column, false)?;
        self.comparisons += 1;
        Some(Instruction::CompareLiteral {
            column: col_idx,
            op,
            literal,
        })
    }

    fn compare_column_column(
        &mut self,
        left: &Expression,
        op: ComparisonOp,
        right: &Expression,
    ) -> Option<Instruction> {
        let left_kind = value_column_kind(&left.dtype)?;
        let right_kind = value_column_kind(&right.dtype)?;
        match (left_kind, right_kind) {
            (ColumnKind::Bool, ColumnKind::Bool)
                if matches!(op, ComparisonOp::Eq | ComparisonOp::NotEq) => {}
            (ColumnKind::SignedInt, ColumnKind::SignedInt)
            | (ColumnKind::UnsignedInt, ColumnKind::UnsignedInt)
            | (ColumnKind::Float, ColumnKind::Float) => {}
            (ColumnKind::Temporal { is_date32: ld }, ColumnKind::Temporal { is_date32: rd })
                if ld == rd => {}
            _ => return None,
        }
        let left_idx = self.column(left, false)?;
        let right_idx = self.column(right, false)?;
        self.comparisons += 1;
        Some(Instruction::CompareColumns {
            left: left_idx,
            op,
            right: right_idx,
        })
    }

    fn column(&mut self, expression: &Expression, validity_only: bool) -> Option<u8> {
        let Kind::Column(name) = &expression.kind else {
            return None;
        };
        if name.len() > MAX_COLUMN_NAME_BYTES {
            return None;
        }
        let requested_kind = if validity_only {
            ColumnKind::ValidityOnly
        } else {
            value_column_kind(&expression.dtype)?
        };
        if let Some(index) = self.columns[..self.column_count].iter().position(|prior| {
            prior.as_ref().is_some_and(|prior| {
                prior.nullable == expression.dtype.is_nullable() && prior.name.as_ref() == name
            })
        }) {
            let slot = self.columns[index].as_mut().expect("found slot");
            if slot.kind == ColumnKind::ValidityOnly {
                slot.kind = requested_kind;
            } else if requested_kind != ColumnKind::ValidityOnly && slot.kind != requested_kind {
                return None;
            }
            return u8::try_from(index).ok();
        }
        if self.column_count == MAX_COLUMNS {
            return None;
        }
        let index = self.column_count;
        self.columns[index] = Some(Column {
            name: name.as_str().into(),
            nullable: expression.dtype.is_nullable(),
            kind: requested_kind,
        });
        self.column_count += 1;
        u8::try_from(index).ok()
    }

    pub(super) fn evaluate(
        &self,
        expression: &Expression,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let owners = self.prepare_owners(input, context)?;
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.predicate_boolean_rows += input.len() as u64;
        });
        let rows = input.len();
        let nullable = expression.dtype.is_nullable();
        let float_policy = context.float_policy();
        result_batch::build_bool_buffers(
            nullable,
            rows,
            &context.native_session().host_allocator(),
            |values_buf, valid_buf| {
                let mut valid_chunks = valid_buf.map(|buf| buf.chunks_mut(8));
                for (index, out_chunk) in values_buf.chunks_mut(8).enumerate() {
                    context.check_cancelled()?;
                    let start = index * 64;
                    let end = rows.min(start.saturating_add(64));
                    let count = end - start;
                    let truth = self.block(&owners, start, end, float_policy)?;
                    let valid_word = truth.yes | truth.no;
                    if !nullable && valid_word != tail_mask(count) {
                        return Err(failed("null in a nonnullable output column"));
                    }
                    out_chunk.copy_from_slice(&truth.yes.to_le_bytes()[..out_chunk.len()]);
                    if let Some(valid_chunk) = valid_chunks.as_mut().and_then(Iterator::next) {
                        valid_chunk.copy_from_slice(&valid_word.to_le_bytes()[..valid_chunk.len()]);
                    }
                }
                Ok(())
            },
        )
    }

    /// WHERE consumes only true bits; projected expressions retain both truth
    /// words through `evaluate`. The session allocator owns the packed credit.
    pub(in crate::local_primitives) fn evaluate_mask(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Mask> {
        use vortex::array::memory::HostMemorySessionExt as _;
        use vortex::buffer::Alignment;

        context.check_cancelled()?;
        let owners = self.prepare_owners(input, context)?;
        let float_policy = context.float_policy();
        let mut bits = context
            .native_session()
            .host_allocator()
            .allocate(input.len().div_ceil(8), Alignment::none())
            .map_err(super::vortex_error)?;
        for (index, output) in bits.as_mut_slice().chunks_mut(8).enumerate() {
            context.check_cancelled()?;
            let start = index * 64;
            let truth = self.block(
                &owners,
                start,
                input.len().min(start.saturating_add(64)),
                float_policy,
            )?;
            // Vortex bit buffers use least-significant-bit-first byte order.
            output.copy_from_slice(&truth.yes.to_le_bytes()[..output.len()]);
        }
        context.check_cancelled()?;
        Ok(Mask::from_buffer(BitBuffer::new(
            bits.freeze(),
            input.len(),
        )))
    }

    fn prepare_owners(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<[Option<ColumnOwner>; MAX_COLUMNS]> {
        let mut owners: [Option<ColumnOwner>; MAX_COLUMNS] = std::array::from_fn(|_| None);
        for (index, column) in self.columns[..self.column_count].iter().enumerate() {
            context.check_cancelled()?;
            let column = column.as_ref().expect("compiled column binding");
            let array = super::super::logical_field_from_native_array(input, &column.name)?;
            if array.dtype().is_nullable() != column.nullable || array.len() != input.len() {
                return Err(failed(
                    "scalar expression changed its bound schema or row count",
                ));
            }
            let owner = match column.kind {
                ColumnKind::ValidityOnly => {
                    ColumnOwner::Validity(parent_validity(&array, context)?)
                }
                ColumnKind::Bool => {
                    let KeyColumn::Boolean { values, valid } = keys(&array, context)? else {
                        return Err(failed(
                            "pure predicate requires its bound Bool native owner",
                        ));
                    };
                    ColumnOwner::Bool { values, valid }
                }
                ColumnKind::SignedInt | ColumnKind::UnsignedInt | ColumnKind::Float => {
                    let expected = match column.kind {
                        ColumnKind::SignedInt => matches!(
                            array.dtype(),
                            DType::Primitive(ptype, _) if ptype.is_signed_int()
                        ),
                        ColumnKind::UnsignedInt => matches!(
                            array.dtype(),
                            DType::Primitive(ptype, _) if ptype.is_unsigned_int()
                        ),
                        ColumnKind::Float => matches!(
                            array.dtype(),
                            DType::Primitive(ptype, _) if ptype.is_float()
                        ),
                        _ => false,
                    };
                    if !expected {
                        return Err(failed(
                            "scalar expression changed its bound schema or row count",
                        ));
                    }
                    let KeyColumn::Numeric(owner) = keys(&array, context)? else {
                        return Err(failed(
                            "pure predicate requires its bound numeric native owner",
                        ));
                    };
                    ColumnOwner::Numeric(owner)
                }
                ColumnKind::Temporal { is_date32 } => {
                    let storage = crate::native_payload_schema::temporal_storage(array.dtype());
                    let expected = if is_date32 {
                        storage == Some(PType::I32)
                            && matches!(
                                shardloom_core::temporal::TemporalKind::Date32,
                                kind if crate::native_temporal::temporal_dtype(array.dtype())
                                    .is_some_and(|d| *d.kind() == kind)
                            )
                    } else {
                        storage == Some(PType::I64)
                    };
                    if !expected {
                        return Err(failed(
                            "scalar expression changed its bound schema or row count",
                        ));
                    }
                    match keys(&array, context)? {
                        KeyColumn::Date(owner) | KeyColumn::Timestamp(owner) => {
                            ColumnOwner::Numeric(owner)
                        }
                        _ => {
                            return Err(failed(
                                "pure predicate requires its bound temporal native owner",
                            ));
                        }
                    }
                }
            };
            owners[index] = Some(owner);
        }
        Ok(owners)
    }

    fn block(
        &self,
        owners: &[Option<ColumnOwner>; MAX_COLUMNS],
        start: usize,
        end: usize,
        float_policy: FloatPolicy,
    ) -> Result<Truth> {
        let count = end - start;
        let row_mask = tail_mask(count);
        let mut words = [Truth::default(); MAX_INSTRUCTIONS];
        for (index, instruction) in self.instructions[..self.instruction_count]
            .iter()
            .enumerate()
        {
            words[index] = match *instruction {
                Instruction::CompareLiteral {
                    column,
                    op,
                    literal,
                } => {
                    let Some(ColumnOwner::Numeric(owner)) = owners[usize::from(column)].as_ref()
                    else {
                        return Err(failed("pure predicate lost its numeric native owner"));
                    };
                    compare_literal_block(owner, start, end, op, literal, float_policy)?
                }
                Instruction::CompareColumns { left, op, right } => compare_columns_block(
                    owners[usize::from(left)]
                        .as_ref()
                        .ok_or_else(|| failed("pure predicate lost left column owner"))?,
                    owners[usize::from(right)]
                        .as_ref()
                        .ok_or_else(|| failed("pure predicate lost right column owner"))?,
                    start,
                    end,
                    op,
                    float_policy,
                )?,
                Instruction::BoolColumn(column) => {
                    let Some(ColumnOwner::Bool { values, valid }) =
                        owners[usize::from(column)].as_ref()
                    else {
                        return Err(failed("pure predicate lost its Bool native owner"));
                    };
                    if valid.len() < end || values.len() < end {
                        return Err(failed("pure predicate boolean is shorter than its column"));
                    }
                    let valid_word = mask_bit_word(valid, start, end);
                    let value_word = read_bit_word(
                        values.bit_buffer_view().inner(),
                        values.bit_buffer_view().offset(),
                        start,
                        end,
                    );
                    Truth {
                        yes: value_word & valid_word,
                        no: (!value_word) & valid_word,
                    }
                }
                Instruction::IsNull(column) => {
                    let owner = owners[usize::from(column)]
                        .as_ref()
                        .ok_or_else(|| failed("pure predicate lost its validity owner"))?;
                    let valid = owner.validity();
                    if valid.len() < end {
                        return Err(failed("pure predicate validity is shorter than its column"));
                    }
                    let valid_word = mask_bit_word(valid, start, end);
                    Truth {
                        yes: (!valid_word) & row_mask,
                        no: valid_word & row_mask,
                    }
                }
                Instruction::IsNotNull(column) => {
                    let owner = owners[usize::from(column)]
                        .as_ref()
                        .ok_or_else(|| failed("pure predicate lost its validity owner"))?;
                    let valid = owner.validity();
                    if valid.len() < end {
                        return Err(failed("pure predicate validity is shorter than its column"));
                    }
                    let valid_word = mask_bit_word(valid, start, end);
                    Truth {
                        yes: valid_word & row_mask,
                        no: (!valid_word) & row_mask,
                    }
                }
                Instruction::And(left, right) => Truth {
                    yes: words[usize::from(left)].yes & words[usize::from(right)].yes,
                    no: words[usize::from(left)].no | words[usize::from(right)].no,
                },
                Instruction::Or(left, right) => Truth {
                    yes: words[usize::from(left)].yes | words[usize::from(right)].yes,
                    no: words[usize::from(left)].no & words[usize::from(right)].no,
                },
                Instruction::Not(child) => Truth {
                    yes: words[usize::from(child)].no,
                    no: words[usize::from(child)].yes,
                },
                Instruction::Empty => unreachable!("compiled instruction"),
            };
        }
        Ok(words[self.root])
    }
}

fn value_column_kind(dtype: &DType) -> Option<ColumnKind> {
    match dtype {
        DType::Bool(_) => Some(ColumnKind::Bool),
        DType::Primitive(ptype, _) if ptype.is_signed_int() => Some(ColumnKind::SignedInt),
        DType::Primitive(ptype, _) if ptype.is_unsigned_int() => Some(ColumnKind::UnsignedInt),
        DType::Primitive(ptype, _) if ptype.is_float() => Some(ColumnKind::Float),
        DType::Extension(_) => match crate::native_temporal::temporal_dtype(dtype) {
            Some(temporal) => match temporal.kind() {
                shardloom_core::temporal::TemporalKind::Date32 => {
                    Some(ColumnKind::Temporal { is_date32: true })
                }
                shardloom_core::temporal::TemporalKind::Timestamp {
                    unit: shardloom_core::temporal::TemporalUnit::Microseconds,
                    timezone: None,
                } => Some(ColumnKind::Temporal { is_date32: false }),
                _ => None,
            },
            None => None,
        },
        _ => None,
    }
}

fn extract_literal_value(
    col_kind: ColumnKind,
    col_dtype: &DType,
    literal_expr: &Expression,
    scalar: &Scalar,
) -> Option<LiteralValue> {
    if matches!(col_kind, ColumnKind::Temporal { .. }) {
        if col_dtype.as_nonnullable() != literal_expr.dtype.as_nonnullable()
            || scalar.dtype().is_nullable() != literal_expr.dtype.is_nullable()
        {
            return None;
        }
        if let Some(ScalarValue::Primitive(pval)) = scalar.value() {
            return match (col_kind, pval) {
                (ColumnKind::Temporal { is_date32: true }, PValue::I32(v)) => {
                    Some(LiteralValue::Signed(i64::from(*v)))
                }
                (ColumnKind::Temporal { is_date32: false }, PValue::I64(v)) => {
                    Some(LiteralValue::Signed(*v))
                }
                _ => None,
            };
        }
        if scalar.dtype() == &literal_expr.dtype {
            let storage = scalar.as_extension().to_storage_scalar();
            if let Some(ScalarValue::Primitive(pval)) = storage.value() {
                return match (col_kind, pval) {
                    (ColumnKind::Temporal { is_date32: true }, PValue::I32(v)) => {
                        Some(LiteralValue::Signed(i64::from(*v)))
                    }
                    (ColumnKind::Temporal { is_date32: false }, PValue::I64(v)) => {
                        Some(LiteralValue::Signed(*v))
                    }
                    _ => None,
                };
            }
        }
        return None;
    }
    if scalar.dtype() != &literal_expr.dtype {
        return None;
    }
    let ScalarValue::Primitive(pval) = scalar.value()? else {
        return None;
    };
    match (col_kind, pval) {
        (ColumnKind::SignedInt, PValue::I8(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::I16(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::I32(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::I64(v)) => Some(LiteralValue::Signed(*v)),
        (ColumnKind::SignedInt, PValue::U8(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::U16(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::U32(v)) => Some(LiteralValue::Signed(i64::from(*v))),
        (ColumnKind::SignedInt, PValue::U64(v)) => i64::try_from(*v).ok().map(LiteralValue::Signed),
        (ColumnKind::UnsignedInt, PValue::U8(v)) => Some(LiteralValue::Unsigned(u64::from(*v))),
        (ColumnKind::UnsignedInt, PValue::U16(v)) => Some(LiteralValue::Unsigned(u64::from(*v))),
        (ColumnKind::UnsignedInt, PValue::U32(v)) => Some(LiteralValue::Unsigned(u64::from(*v))),
        (ColumnKind::UnsignedInt, PValue::U64(v)) => Some(LiteralValue::Unsigned(*v)),
        (ColumnKind::UnsignedInt, PValue::I8(v)) => {
            u64::try_from(*v).ok().map(LiteralValue::Unsigned)
        }
        (ColumnKind::UnsignedInt, PValue::I16(v)) => {
            u64::try_from(*v).ok().map(LiteralValue::Unsigned)
        }
        (ColumnKind::UnsignedInt, PValue::I32(v)) => {
            u64::try_from(*v).ok().map(LiteralValue::Unsigned)
        }
        (ColumnKind::UnsignedInt, PValue::I64(v)) => {
            u64::try_from(*v).ok().map(LiteralValue::Unsigned)
        }
        (ColumnKind::Float, PValue::F16(v)) => {
            let val = shardloom_core::float_policy::widen_f16_bits(v.to_bits());
            val.is_finite()
                .then(|| LiteralValue::Float(float_key_bits(val)))
        }
        (ColumnKind::Float, PValue::F32(v)) => {
            let val = shardloom_core::float_policy::widen_f32(*v);
            val.is_finite()
                .then(|| LiteralValue::Float(float_key_bits(val)))
        }
        (ColumnKind::Float, PValue::F64(v)) => v
            .is_finite()
            .then(|| LiteralValue::Float(float_key_bits(*v))),
        _ => None,
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

#[inline]
const fn tail_mask(len: usize) -> u64 {
    if len >= 64 {
        u64::MAX
    } else if len == 0 {
        0
    } else {
        (1_u64 << len) - 1
    }
}

#[inline]
pub(super) fn read_bit_word(bytes: &[u8], base_bit_offset: usize, start: usize, end: usize) -> u64 {
    let len = end.saturating_sub(start);
    if len == 0 {
        return 0;
    }
    let total_offset = base_bit_offset + start;
    let first_byte = total_offset / 8;
    let bit_offset = total_offset % 8;
    let last_byte = (total_offset + len).div_ceil(8);
    let byte_count = last_byte - first_byte;
    let mut lo = [0_u8; 8];
    let take_lo = byte_count.min(8);
    lo[..take_lo].copy_from_slice(&bytes[first_byte..first_byte + take_lo]);
    let raw_lo = u64::from_le_bytes(lo);
    let word = if bit_offset == 0 {
        raw_lo
    } else {
        let hi = if byte_count > 8 {
            u64::from(bytes[first_byte + 8])
        } else {
            0
        };
        (raw_lo >> bit_offset) | (hi << (64 - bit_offset))
    };
    word & tail_mask(len)
}

#[inline]
pub(super) fn mask_bit_word(valid: &Mask, start: usize, end: usize) -> u64 {
    let len = end.saturating_sub(start);
    if len == 0 {
        return 0;
    }
    match valid.bit_buffer() {
        AllOr::All => tail_mask(len),
        AllOr::None => 0,
        AllOr::Some(bits) => read_bit_word(bits.inner().as_slice(), bits.offset(), start, end),
    }
}

#[inline]
const fn op_matches(order: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => matches!(order, Ordering::Equal),
        ComparisonOp::NotEq => !matches!(order, Ordering::Equal),
        ComparisonOp::Lt => matches!(order, Ordering::Less),
        ComparisonOp::LtEq => matches!(order, Ordering::Less | Ordering::Equal),
        ComparisonOp::Gt => matches!(order, Ordering::Greater),
        ComparisonOp::GtEq => matches!(order, Ordering::Greater | Ordering::Equal),
    }
}

fn compare_literal_block(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    op: ComparisonOp,
    literal: LiteralValue,
    float_policy: FloatPolicy,
) -> Result<Truth> {
    let valid = owner.validity();
    if valid.len() < end || owner.len() < end {
        return Err(failed(
            "pure predicate block exceeds its native column or validity",
        ));
    }
    let valid_word = mask_bit_word(valid, start, end);
    match literal {
        LiteralValue::Signed(rhs) => {
            let mut buf = [0_i64; 64];
            let count = load_i64_slice(owner, start, end, &mut buf)?;
            Ok(compare_block_against_scalar(
                &buf[..count],
                valid_word,
                op,
                |a| a.cmp(&rhs),
            ))
        }
        LiteralValue::Unsigned(rhs) => {
            let mut buf = [0_u64; 64];
            let count = load_u64_slice(owner, start, end, &mut buf)?;
            Ok(compare_block_against_scalar(
                &buf[..count],
                valid_word,
                op,
                |a| a.cmp(&rhs),
            ))
        }
        LiteralValue::Float(rhs_bits) => {
            let mut buf = [0_f64; 64];
            let count = load_f64_slice(owner, start, end, valid_word, float_policy, &mut buf)?;
            let rhs = f64::from_bits(rhs_bits);
            Ok(compare_block_against_scalar(
                &buf[..count],
                valid_word,
                op,
                |a| a.total_cmp(&rhs),
            ))
        }
    }
}

fn compare_columns_block(
    left: &ColumnOwner,
    right: &ColumnOwner,
    start: usize,
    end: usize,
    op: ComparisonOp,
    float_policy: FloatPolicy,
) -> Result<Truth> {
    match (left, right) {
        (
            ColumnOwner::Bool {
                values: lv,
                valid: lvalid,
            },
            ColumnOwner::Bool {
                values: rv,
                valid: rvalid,
            },
        ) => {
            if lv.len() < end || lvalid.len() < end || rv.len() < end || rvalid.len() < end {
                return Err(failed("pure predicate boolean is shorter than its column"));
            }
            let both_valid = mask_bit_word(lvalid, start, end) & mask_bit_word(rvalid, start, end);
            let lword = read_bit_word(
                lv.bit_buffer_view().inner(),
                lv.bit_buffer_view().offset(),
                start,
                end,
            );
            let rword = read_bit_word(
                rv.bit_buffer_view().inner(),
                rv.bit_buffer_view().offset(),
                start,
                end,
            );
            let eq = !(lword ^ rword) & both_valid;
            let ne = (lword ^ rword) & both_valid;
            Ok(match op {
                ComparisonOp::Eq => Truth { yes: eq, no: ne },
                ComparisonOp::NotEq => Truth { yes: ne, no: eq },
                _ => return Err(failed("unsupported boolean comparison operator")),
            })
        }
        (ColumnOwner::Numeric(lo), ColumnOwner::Numeric(ro)) => {
            if lo.len() < end
                || lo.validity().len() < end
                || ro.len() < end
                || ro.validity().len() < end
            {
                return Err(failed(
                    "pure predicate block exceeds its native column or validity",
                ));
            }
            let valid_word =
                mask_bit_word(lo.validity(), start, end) & mask_bit_word(ro.validity(), start, end);
            if lo.ptype().is_signed_int() {
                let mut lbuf = [0_i64; 64];
                let mut rbuf = [0_i64; 64];
                let count = load_i64_slice(lo, start, end, &mut lbuf)?;
                load_i64_slice(ro, start, end, &mut rbuf)?;
                Ok(compare_block_pairs(
                    &lbuf[..count],
                    &rbuf[..count],
                    valid_word,
                    op,
                    i64::cmp,
                ))
            } else if lo.ptype().is_unsigned_int() {
                let mut lbuf = [0_u64; 64];
                let mut rbuf = [0_u64; 64];
                let count = load_u64_slice(lo, start, end, &mut lbuf)?;
                load_u64_slice(ro, start, end, &mut rbuf)?;
                Ok(compare_block_pairs(
                    &lbuf[..count],
                    &rbuf[..count],
                    valid_word,
                    op,
                    u64::cmp,
                ))
            } else {
                let lvalid_word = mask_bit_word(lo.validity(), start, end);
                let rvalid_word = mask_bit_word(ro.validity(), start, end);
                let mut lbuf = [0_f64; 64];
                let mut rbuf = [0_f64; 64];
                let count = load_f64_slice(lo, start, end, lvalid_word, float_policy, &mut lbuf)?;
                load_f64_slice(ro, start, end, rvalid_word, float_policy, &mut rbuf)?;
                Ok(compare_block_pairs(
                    &lbuf[..count],
                    &rbuf[..count],
                    valid_word,
                    op,
                    f64::total_cmp,
                ))
            }
        }
        _ => Err(failed("pure predicate mismatched column owner kinds")),
    }
}

fn compare_block_against_scalar<T: Copy>(
    values: &[T],
    valid_word: u64,
    op: ComparisonOp,
    cmp: impl Fn(T) -> Ordering,
) -> Truth {
    let mut yes = 0_u64;
    let mut no = 0_u64;
    for (offset, &value) in values.iter().enumerate() {
        let bit = 1_u64 << offset;
        if valid_word & bit != 0 {
            if op_matches(cmp(value), op) {
                yes |= bit;
            } else {
                no |= bit;
            }
        }
    }
    Truth { yes, no }
}

fn compare_block_pairs<T: Copy>(
    left: &[T],
    right: &[T],
    valid_word: u64,
    op: ComparisonOp,
    cmp: impl Fn(&T, &T) -> Ordering,
) -> Truth {
    let mut yes = 0_u64;
    let mut no = 0_u64;
    for (offset, (l, r)) in left.iter().zip(right).enumerate() {
        let bit = 1_u64 << offset;
        if valid_word & bit != 0 {
            if op_matches(cmp(l, r), op) {
                yes |= bit;
            } else {
                no |= bit;
            }
        }
    }
    Truth { yes, no }
}

fn load_i64_slice(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    out: &mut [i64; 64],
) -> Result<usize> {
    let count = end - start;
    match owner.ptype() {
        PType::I8 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i8>()[start..end])
            {
                *dst = i64::from(src);
            }
        }
        PType::I16 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i16>()[start..end])
            {
                *dst = i64::from(src);
            }
        }
        PType::I32 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i32>()[start..end])
            {
                *dst = i64::from(src);
            }
        }
        PType::I64 => {
            out[..count].copy_from_slice(&owner.primitive().as_slice::<i64>()[start..end]);
        }
        _ => return Err(failed("pure predicate lost its signed integer owner")),
    }
    Ok(count)
}

fn load_u64_slice(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    out: &mut [u64; 64],
) -> Result<usize> {
    let count = end - start;
    match owner.ptype() {
        PType::U8 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u8>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        PType::U16 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u16>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        PType::U32 => {
            for (dst, &src) in out[..count]
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u32>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        PType::U64 => {
            out[..count].copy_from_slice(&owner.primitive().as_slice::<u64>()[start..end]);
        }
        _ => return Err(failed("pure predicate lost its unsigned integer owner")),
    }
    Ok(count)
}

fn load_f64_slice(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    valid_word: u64,
    float_policy: FloatPolicy,
    out: &mut [f64; 64],
) -> Result<usize> {
    let count = end - start;
    match owner.ptype() {
        PType::F16 => {
            for (offset, &src) in owner.primitive().as_slice::<f16>()[start..end]
                .iter()
                .enumerate()
            {
                if valid_word & (1_u64 << offset) != 0 {
                    let v = shardloom_core::float_policy::widen_f16_bits(src.to_bits());
                    validate_float_scalar(v, float_policy)?;
                    out[offset] = f64::from_bits(float_key_bits(v));
                }
            }
        }
        PType::F32 => {
            for (offset, &src) in owner.primitive().as_slice::<f32>()[start..end]
                .iter()
                .enumerate()
            {
                if valid_word & (1_u64 << offset) != 0 {
                    let v = shardloom_core::float_policy::widen_f32(src);
                    validate_float_scalar(v, float_policy)?;
                    out[offset] = f64::from_bits(float_key_bits(v));
                }
            }
        }
        PType::F64 => {
            for (offset, &src) in owner.primitive().as_slice::<f64>()[start..end]
                .iter()
                .enumerate()
            {
                if valid_word & (1_u64 << offset) != 0 {
                    validate_float_scalar(src, float_policy)?;
                    out[offset] = f64::from_bits(float_key_bits(src));
                }
            }
        }
        _ => return Err(failed("pure predicate lost its floating numeric owner")),
    }
    Ok(count)
}
