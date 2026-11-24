//! Compiled columnar chunk evaluator and slice fast paths for `Expression`.
//! Preserves checked integer overflow, division by zero, lazy conditional branches,
//! and strict vs extended floating-point policies identically to scalar evaluation.

use super::{
    ArrayRef, BinaryOp, ComparisonOp, DType, Expression, KeyColumn, Kind, NativeExecutionContext,
    PType, Result, UnaryOp, failed, floating_binary, integer_binary, keys, parent_validity,
    predicate_block::{mask_bit_word, read_bit_word},
    result_batch,
    scalar::TemporalCast,
    signed_result, unsigned_result, vortex_error,
};
use crate::local_primitives::{
    native_numeric_owner::NativeNumericOwner, native_relational_keys::validate_float_scalar,
};
use shardloom_core::{FloatPolicy, float_policy::canonicalize_f64_result};
use std::cmp::Ordering;
use vortex::array::{
    arrays::bool::BoolArrayExt,
    dtype::half::f16,
    memory::HostMemorySessionExt as _,
    scalar::{PValue, Scalar, ScalarValue},
};

const MAX_COLUMNS: usize = 8;
const MAX_REGS: usize = 8;
const MAX_OPS: usize = 32;
const MAX_COLUMN_NAME_BYTES: usize = 256;
const CHUNK_ROWS: usize = 256;
const CHUNK_MASK_WORDS: usize = CHUNK_ROWS / 64;
const F64_MAX_EXACT_INTEGER: i128 = 9_007_199_254_740_992;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RegType {
    Signed64,
    Unsigned64,
    Float64,
}

#[derive(Clone, Copy)]
enum LiteralValue {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Null,
}

#[derive(Clone, Copy)]
enum ChunkOp {
    LoadColumn {
        dst: u8,
        col: u8,
        ty: RegType,
    },
    LoadLiteral {
        dst: u8,
        val: LiteralValue,
    },
    Negate {
        dst: u8,
        src: u8,
        src_ty: RegType,
    },
    Cast {
        dst: u8,
        src: u8,
        from: RegType,
        to: RegType,
    },
    Binary {
        dst: u8,
        left: u8,
        left_ty: RegType,
        op: BinaryOp,
        right: u8,
        right_ty: RegType,
        out_ty: RegType,
    },
}

struct ColumnBinding {
    name: Box<str>,
    ptype: PType,
    nullable: bool,
}

pub(in crate::local_primitives) struct NumericChunkProgram {
    columns: [Option<ColumnBinding>; MAX_COLUMNS],
    column_count: usize,
    ops: [Option<ChunkOp>; MAX_OPS],
    op_count: usize,
    root_reg: u8,
    root_type: RegType,
}

impl NumericChunkProgram {
    pub(super) fn compile(expression: &Expression) -> Option<Self> {
        let out_type = reg_type(&expression.dtype)?;
        let mut op_count = 0_usize;
        if !count_compound_ops(expression, &mut op_count) || op_count < 2 {
            return None;
        }
        let mut compiler = ProgramCompiler {
            columns: std::array::from_fn(|_| None),
            column_count: 0,
            ops: [None; MAX_OPS],
            op_count: 0,
            used_regs: 0,
        };
        let (root_reg, root_type) = compiler.compile_node(expression)?;
        if root_type != out_type {
            return None;
        }
        Some(Self {
            columns: compiler.columns,
            column_count: compiler.column_count,
            ops: compiler.ops,
            op_count: compiler.op_count,
            root_reg,
            root_type,
        })
    }

    fn load_input_owners(
        &self,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<[Option<NativeNumericOwner>; MAX_COLUMNS]> {
        let rows = input.len();
        let mut owners: [Option<NativeNumericOwner>; MAX_COLUMNS] = std::array::from_fn(|_| None);
        for (index, column) in self.columns[..self.column_count].iter().enumerate() {
            context.check_cancelled()?;
            let col = column.as_ref().expect("compiled chunk column");
            let array = super::super::logical_field_from_native_array(input, &col.name)?;
            if array.len() != rows
                || array.dtype().is_nullable() != col.nullable
                || !matches!(array.dtype(), DType::Primitive(ptype, _) if *ptype == col.ptype)
            {
                return Err(failed(
                    "scalar expression changed its bound schema or row count",
                ));
            }
            let KeyColumn::Numeric(owner) = keys(&array, context)? else {
                return Err(failed(
                    "chunk numeric expression requires its bound native owner",
                ));
            };
            owners[index] = Some(owner);
        }
        Ok(owners)
    }

    pub(super) fn evaluate(
        &self,
        expression: &Expression,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let rows = input.len();
        let owners = self.load_input_owners(input, context)?;
        let float_policy = context.float_policy();
        let nullable = expression.dtype.is_nullable();
        let allocator = context.native_session().host_allocator();
        let mut scratch = ChunkScratch::default();
        match self.root_type {
            RegType::Signed64 => result_batch::build_primitive_buffers::<i64>(
                &expression.dtype,
                rows,
                &allocator,
                |out, mut valid_out| {
                    self.run_chunks(
                        &owners,
                        rows,
                        nullable,
                        float_policy,
                        context,
                        &mut scratch,
                        |chunk_start, chunk_len, reg_vals, reg_valid| {
                            copy_chunk_i64(
                                out,
                                valid_out.as_deref_mut(),
                                chunk_start,
                                chunk_len,
                                nullable,
                                reg_vals,
                                reg_valid,
                            )
                        },
                    )
                },
            ),
            RegType::Unsigned64 => result_batch::build_primitive_buffers::<u64>(
                &expression.dtype,
                rows,
                &allocator,
                |out, mut valid_out| {
                    self.run_chunks(
                        &owners,
                        rows,
                        nullable,
                        float_policy,
                        context,
                        &mut scratch,
                        |chunk_start, chunk_len, reg_vals, reg_valid| {
                            copy_chunk_u64(
                                out,
                                valid_out.as_deref_mut(),
                                chunk_start,
                                chunk_len,
                                nullable,
                                reg_vals,
                                reg_valid,
                            )
                        },
                    )
                },
            ),
            RegType::Float64 => result_batch::build_primitive_buffers::<f64>(
                &expression.dtype,
                rows,
                &allocator,
                |out, mut valid_out| {
                    self.run_chunks(
                        &owners,
                        rows,
                        nullable,
                        float_policy,
                        context,
                        &mut scratch,
                        |chunk_start, chunk_len, reg_vals, reg_valid| {
                            copy_chunk_f64(
                                out,
                                valid_out.as_deref_mut(),
                                chunk_start,
                                chunk_len,
                                nullable,
                                reg_vals,
                                reg_valid,
                            )
                        },
                    )
                },
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_chunks(
        &self,
        owners: &[Option<NativeNumericOwner>; MAX_COLUMNS],
        rows: usize,
        _nullable: bool,
        float_policy: FloatPolicy,
        context: &NativeExecutionContext<'_>,
        scratch: &mut ChunkScratch,
        mut emit: impl FnMut(usize, usize, &[u64; CHUNK_ROWS], &[u64; CHUNK_MASK_WORDS]) -> Result<()>,
    ) -> Result<()> {
        for chunk_start in (0..rows).step_by(CHUNK_ROWS) {
            context.check_cancelled()?;
            let chunk_len = (rows - chunk_start).min(CHUNK_ROWS);
            self.eval_chunk(owners, chunk_start, chunk_len, float_policy, scratch)?;
            let root = usize::from(self.root_reg);
            emit(
                chunk_start,
                chunk_len,
                &scratch.values[root],
                &scratch.valid[root],
            )?;
        }
        Ok(())
    }

    fn eval_chunk(
        &self,
        owners: &[Option<NativeNumericOwner>; MAX_COLUMNS],
        chunk_start: usize,
        chunk_len: usize,
        float_policy: FloatPolicy,
        scratch: &mut ChunkScratch,
    ) -> Result<()> {
        for op in self.ops[..self.op_count].iter().flatten() {
            match *op {
                ChunkOp::LoadColumn { dst, col, ty } => {
                    let owner = owners[usize::from(col)]
                        .as_ref()
                        .expect("prepared column owner");
                    let d = usize::from(dst);
                    load_column_chunk(
                        owner,
                        chunk_start,
                        chunk_len,
                        ty,
                        float_policy,
                        &mut scratch.values[d],
                        &mut scratch.valid[d],
                    )?;
                }
                ChunkOp::LoadLiteral { dst, val } => {
                    let d = usize::from(dst);
                    load_literal_chunk(
                        val,
                        chunk_len,
                        &mut scratch.values[d],
                        &mut scratch.valid[d],
                    );
                }
                ChunkOp::Negate { dst, src, src_ty } => {
                    let s = usize::from(src);
                    let d = usize::from(dst);
                    eval_negate_chunk(scratch, d, s, src_ty, chunk_len)?;
                }
                ChunkOp::Cast { dst, src, from, to } => {
                    let s = usize::from(src);
                    let d = usize::from(dst);
                    eval_cast_chunk(scratch, d, s, from, to, chunk_len, float_policy)?;
                }
                ChunkOp::Binary {
                    dst,
                    left,
                    left_ty,
                    op,
                    right,
                    right_ty,
                    out_ty,
                } => {
                    eval_binary_chunk(
                        scratch,
                        usize::from(dst),
                        usize::from(left),
                        left_ty,
                        op,
                        usize::from(right),
                        right_ty,
                        out_ty,
                        chunk_len,
                        float_policy,
                    )?;
                }
            }
        }
        Ok(())
    }
}

struct ChunkScratch {
    values: [[u64; CHUNK_ROWS]; MAX_REGS],
    valid: [[u64; CHUNK_MASK_WORDS]; MAX_REGS],
}

impl Default for ChunkScratch {
    fn default() -> Self {
        Self {
            values: [[0; CHUNK_ROWS]; MAX_REGS],
            valid: [[0; CHUNK_MASK_WORDS]; MAX_REGS],
        }
    }
}

struct ProgramCompiler {
    columns: [Option<ColumnBinding>; MAX_COLUMNS],
    column_count: usize,
    ops: [Option<ChunkOp>; MAX_OPS],
    op_count: usize,
    used_regs: u8,
}

impl ProgramCompiler {
    fn alloc_reg(&mut self) -> Option<u8> {
        for reg in 0..MAX_REGS {
            let bit = 1_u8 << reg;
            if self.used_regs & bit == 0 {
                self.used_regs |= bit;
                return u8::try_from(reg).ok();
            }
        }
        None
    }

    fn free_reg(&mut self, reg: u8) {
        self.used_regs &= !(1_u8 << reg);
    }

    fn push_op(&mut self, op: ChunkOp) -> Option<()> {
        if self.op_count >= MAX_OPS {
            return None;
        }
        self.ops[self.op_count] = Some(op);
        self.op_count += 1;
        Some(())
    }

    fn register_column(&mut self, name: &str, dtype: &DType) -> Option<u8> {
        let DType::Primitive(ptype, _) = dtype else {
            return None;
        };
        if name.len() > MAX_COLUMN_NAME_BYTES {
            return None;
        }
        if let Some(index) = self.columns[..self.column_count].iter().position(|slot| {
            slot.as_ref().is_some_and(|col| {
                col.name.as_ref() == name
                    && col.ptype == *ptype
                    && col.nullable == dtype.is_nullable()
            })
        }) {
            return u8::try_from(index).ok();
        }
        if self.column_count >= MAX_COLUMNS {
            return None;
        }
        let index = self.column_count;
        self.columns[index] = Some(ColumnBinding {
            name: name.into(),
            ptype: *ptype,
            nullable: dtype.is_nullable(),
        });
        self.column_count += 1;
        u8::try_from(index).ok()
    }

    fn compile_node(&mut self, expression: &Expression) -> Option<(u8, RegType)> {
        if expression.reuse_output.is_some() {
            return None;
        }
        match &expression.kind {
            Kind::Column(name) => {
                let DType::Primitive(ptype, _) = &expression.dtype else {
                    return None;
                };
                let ty = leaf_reg_type(*ptype);
                let col = self.register_column(name, &expression.dtype)?;
                let dst = self.alloc_reg()?;
                self.push_op(ChunkOp::LoadColumn { dst, col, ty })?;
                Some((dst, ty))
            }
            Kind::Literal(scalar) => {
                let (val, ty) = compile_literal(&expression.dtype, scalar)?;
                let dst = self.alloc_reg()?;
                self.push_op(ChunkOp::LoadLiteral { dst, val })?;
                Some((dst, ty))
            }
            Kind::Unary(UnaryOp::Negate, child) => {
                let out_ty = reg_type(&expression.dtype)?;
                if out_ty == RegType::Unsigned64 {
                    return None;
                }
                let (src, src_ty) = self.compile_node(child)?;
                self.free_reg(src);
                let dst = self.alloc_reg()?;
                self.push_op(ChunkOp::Negate { dst, src, src_ty })?;
                Some((dst, out_ty))
            }
            Kind::Cast {
                input,
                tolerant: false,
                temporal: None,
            } => {
                let to = reg_type(&expression.dtype)?;
                let (src, from) = self.compile_node(input)?;
                self.free_reg(src);
                let dst = self.alloc_reg()?;
                self.push_op(ChunkOp::Cast { dst, src, from, to })?;
                Some((dst, to))
            }
            Kind::Binary(left, op, right)
                if matches!(
                    op,
                    BinaryOp::Add
                        | BinaryOp::Subtract
                        | BinaryOp::Multiply
                        | BinaryOp::Divide
                        | BinaryOp::Remainder
                ) =>
            {
                let out_ty = reg_type(&expression.dtype)?;
                let (lreg, lty) = self.compile_node(left)?;
                let (rreg, rty) = self.compile_node(right)?;
                self.free_reg(rreg);
                self.free_reg(lreg);
                let dst = self.alloc_reg()?;
                self.push_op(ChunkOp::Binary {
                    dst,
                    left: lreg,
                    left_ty: lty,
                    op: *op,
                    right: rreg,
                    right_ty: rty,
                    out_ty,
                })?;
                Some((dst, out_ty))
            }
            _ => None,
        }
    }
}

const fn reg_type(dtype: &DType) -> Option<RegType> {
    match dtype {
        DType::Primitive(PType::I64, _) => Some(RegType::Signed64),
        DType::Primitive(PType::U64, _) => Some(RegType::Unsigned64),
        DType::Primitive(PType::F64, _) => Some(RegType::Float64),
        _ => None,
    }
}

const fn leaf_reg_type(ptype: PType) -> RegType {
    match ptype {
        PType::I8 | PType::I16 | PType::I32 | PType::I64 => RegType::Signed64,
        PType::U8 | PType::U16 | PType::U32 | PType::U64 => RegType::Unsigned64,
        PType::F16 | PType::F32 | PType::F64 => RegType::Float64,
    }
}

fn compile_literal(dtype: &DType, scalar: &Scalar) -> Option<(LiteralValue, RegType)> {
    if scalar.dtype() != dtype {
        return None;
    }
    let DType::Primitive(ptype, _) = dtype else {
        return None;
    };
    let ty = leaf_reg_type(*ptype);
    let Some(ScalarValue::Primitive(pval)) = scalar.value() else {
        return Some((LiteralValue::Null, ty));
    };
    let val = match pval {
        PValue::I8(v) => LiteralValue::Signed(i64::from(*v)),
        PValue::I16(v) => LiteralValue::Signed(i64::from(*v)),
        PValue::I32(v) => LiteralValue::Signed(i64::from(*v)),
        PValue::I64(v) => LiteralValue::Signed(*v),
        PValue::U8(v) => LiteralValue::Unsigned(u64::from(*v)),
        PValue::U16(v) => LiteralValue::Unsigned(u64::from(*v)),
        PValue::U32(v) => LiteralValue::Unsigned(u64::from(*v)),
        PValue::U64(v) => LiteralValue::Unsigned(*v),
        PValue::F16(v) => {
            let f = shardloom_core::float_policy::widen_f16_bits(v.to_bits());
            if !f.is_finite() {
                return None;
            }
            LiteralValue::Float(f)
        }
        PValue::F32(v) => {
            let f = shardloom_core::float_policy::widen_f32(*v);
            if !f.is_finite() {
                return None;
            }
            LiteralValue::Float(f)
        }
        PValue::F64(v) => {
            if !v.is_finite() {
                return None;
            }
            LiteralValue::Float(*v)
        }
    };
    Some((val, ty))
}

fn count_compound_ops(expression: &Expression, ops: &mut usize) -> bool {
    if *ops > MAX_OPS || expression.reuse_output.is_some() {
        return false;
    }
    match &expression.kind {
        Kind::Column(_) | Kind::Literal(_) => matches!(expression.dtype, DType::Primitive(..)),
        Kind::Unary(UnaryOp::Negate, child)
        | Kind::Cast {
            input: child,
            tolerant: false,
            temporal: None,
        } => {
            *ops += 1;
            count_compound_ops(child, ops)
        }
        Kind::Binary(
            left,
            BinaryOp::Add
            | BinaryOp::Subtract
            | BinaryOp::Multiply
            | BinaryOp::Divide
            | BinaryOp::Remainder,
            right,
        ) => {
            *ops += 1;
            count_compound_ops(left, ops) && count_compound_ops(right, ops)
        }
        _ => false,
    }
}

#[inline]
const fn valid_bit(valid: &[u64; CHUNK_MASK_WORDS], row: usize) -> bool {
    valid[row / 64] & (1_u64 << (row % 64)) != 0
}

#[inline]
const fn set_valid_slice(buf: &mut [u8], global_row: usize) {
    buf[global_row / 8] |= 1_u8 << (global_row % 8);
}

fn load_column_chunk(
    owner: &NativeNumericOwner,
    start: usize,
    len: usize,
    ty: RegType,
    float_policy: FloatPolicy,
    values_out: &mut [u64; CHUNK_ROWS],
    valid_out: &mut [u64; CHUNK_MASK_WORDS],
) -> Result<()> {
    let end = start + len;
    let valid = owner.validity();
    for (word_idx, dst) in valid_out.iter_mut().enumerate().take(CHUNK_MASK_WORDS) {
        let w_start = start + word_idx * 64;
        *dst = if w_start < end {
            mask_bit_word(valid, w_start, end.min(w_start + 64))
        } else {
            0
        };
    }
    match ty {
        RegType::Signed64 | RegType::Unsigned64 => {
            load_integer_column_chunk(owner, start, end, len, ty, values_out)
        }
        RegType::Float64 => {
            load_float_column_chunk(owner, start, end, len, float_policy, values_out, valid_out)
        }
    }
}

fn load_integer_column_chunk(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    len: usize,
    ty: RegType,
    values_out: &mut [u64; CHUNK_ROWS],
) -> Result<()> {
    let out = &mut values_out[..len];
    match (ty, owner.ptype()) {
        (RegType::Signed64, PType::I8) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i8>()[start..end])
            {
                *dst = i64::from(src).cast_unsigned();
            }
        }
        (RegType::Signed64, PType::I16) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i16>()[start..end])
            {
                *dst = i64::from(src).cast_unsigned();
            }
        }
        (RegType::Signed64, PType::I32) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i32>()[start..end])
            {
                *dst = i64::from(src).cast_unsigned();
            }
        }
        (RegType::Signed64, PType::I64) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<i64>()[start..end])
            {
                *dst = src.cast_unsigned();
            }
        }
        (RegType::Unsigned64, PType::U8) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u8>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        (RegType::Unsigned64, PType::U16) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u16>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        (RegType::Unsigned64, PType::U32) => {
            for (dst, &src) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<u32>()[start..end])
            {
                *dst = u64::from(src);
            }
        }
        (RegType::Unsigned64, PType::U64) => {
            out.copy_from_slice(&owner.primitive().as_slice::<u64>()[start..end]);
        }
        _ => return Err(failed("chunk numeric owner changed its physical type")),
    }
    Ok(())
}

fn load_float_column_chunk(
    owner: &NativeNumericOwner,
    start: usize,
    end: usize,
    len: usize,
    float_policy: FloatPolicy,
    values_out: &mut [u64; CHUNK_ROWS],
    valid_out: &[u64; CHUNK_MASK_WORDS],
) -> Result<()> {
    let out = &mut values_out[..len];
    match owner.ptype() {
        PType::F16 => {
            for (row, (dst, &src)) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<f16>()[start..end])
                .enumerate()
            {
                if valid_bit(valid_out, row) {
                    let v = shardloom_core::float_policy::widen_f16_bits(src.to_bits());
                    validate_float_scalar(v, float_policy)?;
                    *dst = v.to_bits();
                }
            }
        }
        PType::F32 => {
            for (row, (dst, &src)) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<f32>()[start..end])
                .enumerate()
            {
                if valid_bit(valid_out, row) {
                    let v = shardloom_core::float_policy::widen_f32(src);
                    validate_float_scalar(v, float_policy)?;
                    *dst = v.to_bits();
                }
            }
        }
        PType::F64 => {
            for (row, (dst, &src)) in out
                .iter_mut()
                .zip(&owner.primitive().as_slice::<f64>()[start..end])
                .enumerate()
            {
                if valid_bit(valid_out, row) {
                    validate_float_scalar(src, float_policy)?;
                    *dst = src.to_bits();
                }
            }
        }
        _ => return Err(failed("chunk numeric owner changed its physical type")),
    }
    Ok(())
}

fn load_literal_chunk(
    val: LiteralValue,
    len: usize,
    values_out: &mut [u64; CHUNK_ROWS],
    valid_out: &mut [u64; CHUNK_MASK_WORDS],
) {
    let raw = match val {
        LiteralValue::Signed(v) => v.cast_unsigned(),
        LiteralValue::Unsigned(v) => v,
        LiteralValue::Float(v) => v.to_bits(),
        LiteralValue::Null => {
            valid_out.fill(0);
            return;
        }
    };
    values_out[..len].fill(raw);
    for (idx, word) in valid_out.iter_mut().enumerate() {
        let word_start = idx * 64;
        *word = if word_start >= len {
            0
        } else if word_start + 64 <= len {
            u64::MAX
        } else {
            (1_u64 << (len - word_start)) - 1
        };
    }
}

fn eval_negate_chunk(
    scratch: &mut ChunkScratch,
    dst: usize,
    src: usize,
    src_ty: RegType,
    len: usize,
) -> Result<()> {
    let src_valid = scratch.valid[src];
    scratch.valid[dst] = src_valid;
    for row in 0..len {
        if !valid_bit(&src_valid, row) {
            continue;
        }
        let bits = scratch.values[src][row];
        scratch.values[dst][row] = match src_ty {
            RegType::Float64 => canonicalize_f64_result(-f64::from_bits(bits)).to_bits(),
            RegType::Signed64 => i64::try_from(-i128::from(bits.cast_signed()))
                .map_err(vortex_error)?
                .cast_unsigned(),
            RegType::Unsigned64 => i64::try_from(-i128::from(bits))
                .map_err(vortex_error)?
                .cast_unsigned(),
        };
    }
    Ok(())
}

fn eval_cast_chunk(
    scratch: &mut ChunkScratch,
    dst: usize,
    src: usize,
    from: RegType,
    to: RegType,
    len: usize,
    float_policy: FloatPolicy,
) -> Result<()> {
    let src_valid = scratch.valid[src];
    scratch.valid[dst] = src_valid;
    for row in 0..len {
        if !valid_bit(&src_valid, row) {
            continue;
        }
        let bits = scratch.values[src][row];
        scratch.values[dst][row] = cast_primitive_scalar(bits, from, to, float_policy)?;
    }
    Ok(())
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn cast_primitive_scalar(
    bits: u64,
    from: RegType,
    to: RegType,
    float_policy: FloatPolicy,
) -> Result<u64> {
    match (from, to) {
        (RegType::Signed64, RegType::Signed64) | (RegType::Unsigned64, RegType::Unsigned64) => {
            Ok(bits)
        }
        (RegType::Float64, RegType::Float64) => {
            let v = f64::from_bits(bits);
            validate_float_scalar(v, float_policy)?;
            Ok(v.to_bits())
        }
        (RegType::Signed64, RegType::Float64) => {
            let v = canonicalize_f64_result(bits.cast_signed() as f64);
            validate_float_scalar(v, float_policy)?;
            Ok(v.to_bits())
        }
        (RegType::Unsigned64, RegType::Float64) => {
            let v = canonicalize_f64_result(bits as f64);
            validate_float_scalar(v, float_policy)?;
            Ok(v.to_bits())
        }
        (RegType::Signed64, RegType::Unsigned64) => u64::try_from(bits.cast_signed())
            .map_err(|_| failed("value cannot be converted to the bound scalar dtype")),
        (RegType::Unsigned64, RegType::Signed64) => i64::try_from(bits)
            .map(i64::cast_unsigned)
            .map_err(|_| failed("value cannot be converted to the bound scalar dtype")),
        (RegType::Float64, RegType::Signed64) => {
            let v = f64::from_bits(bits);
            if v.fract() != 0.0
                || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&v)
            {
                return Err(failed(
                    "value cannot be converted to the bound scalar dtype",
                ));
            }
            Ok((v as i64).cast_unsigned())
        }
        (RegType::Float64, RegType::Unsigned64) => {
            let v = f64::from_bits(bits);
            if v.fract() != 0.0 || !(0.0..18_446_744_073_709_551_616.0).contains(&v) {
                return Err(failed(
                    "value cannot be converted to the bound scalar dtype",
                ));
            }
            Ok(v as u64)
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
fn eval_binary_chunk(
    scratch: &mut ChunkScratch,
    dst: usize,
    left: usize,
    left_ty: RegType,
    op: BinaryOp,
    right: usize,
    right_ty: RegType,
    out_ty: RegType,
    len: usize,
    float_policy: FloatPolicy,
) -> Result<()> {
    let lvalid = scratch.valid[left];
    let rvalid = scratch.valid[right];
    let mut dst_valid = [0_u64; CHUNK_MASK_WORDS];
    for w in 0..CHUNK_MASK_WORDS {
        dst_valid[w] = lvalid[w] & rvalid[w];
    }
    scratch.valid[dst] = dst_valid;
    for row in 0..len {
        if !valid_bit(&dst_valid, row) {
            continue;
        }
        let lbits = scratch.values[left][row];
        let rbits = scratch.values[right][row];
        scratch.values[dst][row] = match out_ty {
            RegType::Float64 => {
                let l = reg_to_exact_f64(lbits, left_ty)?;
                let r = reg_to_exact_f64(rbits, right_ty)?;
                floating_binary(l, op, r, float_policy)?.to_bits()
            }
            RegType::Signed64 => {
                let l = reg_to_i128(lbits, left_ty)?;
                let r = reg_to_i128(rbits, right_ty)?;
                signed_result(integer_binary(l, op, r)?)?.cast_unsigned()
            }
            RegType::Unsigned64 => {
                let l = reg_to_i128(lbits, left_ty)?;
                let r = reg_to_i128(rbits, right_ty)?;
                unsigned_result(integer_binary(l, op, r)?)?
            }
        };
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn reg_to_exact_f64(bits: u64, ty: RegType) -> Result<f64> {
    match ty {
        RegType::Float64 => Ok(f64::from_bits(bits)),
        RegType::Signed64 => {
            let v = i128::from(bits.cast_signed());
            if !(-F64_MAX_EXACT_INTEGER..=F64_MAX_EXACT_INTEGER).contains(&v) {
                return Err(failed(
                    "mixed floating arithmetic or literal comparison requires an exactly representable integer",
                ));
            }
            Ok(v as f64)
        }
        RegType::Unsigned64 => {
            let v = i128::from(bits);
            if !(-F64_MAX_EXACT_INTEGER..=F64_MAX_EXACT_INTEGER).contains(&v) {
                return Err(failed(
                    "mixed floating arithmetic or literal comparison requires an exactly representable integer",
                ));
            }
            Ok(v as f64)
        }
    }
}

fn reg_to_i128(bits: u64, ty: RegType) -> Result<i128> {
    match ty {
        RegType::Signed64 => Ok(i128::from(bits.cast_signed())),
        RegType::Unsigned64 => Ok(i128::from(bits)),
        RegType::Float64 => Err(failed("numeric expression requires an integer")),
    }
}

fn copy_chunk_i64(
    out: &mut [i64],
    mut valid_out: Option<&mut [u8]>,
    chunk_start: usize,
    chunk_len: usize,
    nullable: bool,
    reg_vals: &[u64; CHUNK_ROWS],
    reg_valid: &[u64; CHUNK_MASK_WORDS],
) -> Result<()> {
    for row in 0..chunk_len {
        if valid_bit(reg_valid, row) {
            out[chunk_start + row] = reg_vals[row].cast_signed();
            if let Some(valid) = valid_out.as_deref_mut() {
                set_valid_slice(valid, chunk_start + row);
            }
        } else if !nullable {
            return Err(failed("null in a nonnullable output column"));
        }
    }
    Ok(())
}

fn copy_chunk_u64(
    out: &mut [u64],
    mut valid_out: Option<&mut [u8]>,
    chunk_start: usize,
    chunk_len: usize,
    nullable: bool,
    reg_vals: &[u64; CHUNK_ROWS],
    reg_valid: &[u64; CHUNK_MASK_WORDS],
) -> Result<()> {
    for row in 0..chunk_len {
        if valid_bit(reg_valid, row) {
            out[chunk_start + row] = reg_vals[row];
            if let Some(valid) = valid_out.as_deref_mut() {
                set_valid_slice(valid, chunk_start + row);
            }
        } else if !nullable {
            return Err(failed("null in a nonnullable output column"));
        }
    }
    Ok(())
}

fn copy_chunk_f64(
    out: &mut [f64],
    mut valid_out: Option<&mut [u8]>,
    chunk_start: usize,
    chunk_len: usize,
    nullable: bool,
    reg_vals: &[u64; CHUNK_ROWS],
    reg_valid: &[u64; CHUNK_MASK_WORDS],
) -> Result<()> {
    for row in 0..chunk_len {
        if valid_bit(reg_valid, row) {
            out[chunk_start + row] = f64::from_bits(reg_vals[row]);
            if let Some(valid) = valid_out.as_deref_mut() {
                set_valid_slice(valid, chunk_start + row);
            }
        } else if !nullable {
            return Err(failed("null in a nonnullable output column"));
        }
    }
    Ok(())
}

pub(super) fn evaluate_unary_fast(
    expression: &Expression,
    op: UnaryOp,
    child: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    completed: Option<&[ArrayRef]>,
) -> Result<Option<ArrayRef>> {
    let array = child.evaluate_in_scope(input, context, completed)?;
    match op {
        UnaryOp::IsNull | UnaryOp::IsNotNull => {
            evaluate_is_null_fast(expression, op, &array, input.len(), context).map(Some)
        }
        UnaryOp::Not => evaluate_not_fast(expression, &array, input.len(), context),
        UnaryOp::Negate => evaluate_negate_fast(expression, &array, input.len(), context),
    }
}

fn evaluate_is_null_fast(
    expression: &Expression,
    op: UnaryOp,
    array: &ArrayRef,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let allocator = context.native_session().host_allocator();
    if !super::super::native_payload::is_nested(array.dtype())
        && !matches!(
            array.dtype(),
            DType::Bool(_)
                | DType::Null
                | DType::Primitive(
                    PType::I8
                        | PType::I16
                        | PType::I32
                        | PType::I64
                        | PType::U8
                        | PType::U16
                        | PType::U32
                        | PType::U64,
                    _
                )
        )
    {
        // Preserve exact strict value validation on floats, decimals, text, and temporal extensions.
        let values = keys(array, context)?;
        return result_batch::build_bool_buffers(
            expression.dtype.is_nullable(),
            rows,
            &allocator,
            |data, mut valid_out| {
                let is_not_null = op == UnaryOp::IsNotNull;
                for row in 0..rows {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    let is_null = values.raw_cell(row)? == super::Cell::Null;
                    if is_null != is_not_null {
                        set_valid_slice(data, row);
                    }
                    if let Some(valid) = valid_out.as_deref_mut() {
                        set_valid_slice(valid, row);
                    }
                }
                Ok(())
            },
        );
    }
    let valid = parent_validity(array, context)?;
    let invert = op == UnaryOp::IsNull;
    result_batch::build_bool_buffers(
        expression.dtype.is_nullable(),
        rows,
        &allocator,
        |data, valid_out| {
            for (index, chunk) in data.chunks_mut(8).enumerate() {
                context.check_cancelled()?;
                let start = index * 64;
                let end = rows.min(start + 64);
                let word = mask_bit_word(&valid, start, end);
                let out_word = if invert {
                    (!word) & tail_mask(end - start)
                } else {
                    word
                };
                chunk.copy_from_slice(&out_word.to_le_bytes()[..chunk.len()]);
            }
            if let Some(valid_out) = valid_out {
                valid_out.fill(u8::MAX);
                let rem = rows % 8;
                if rem != 0
                    && let Some(last) = valid_out.last_mut()
                {
                    *last = (1_u8 << rem) - 1;
                }
            }
            Ok(())
        },
    )
}

fn evaluate_not_fast(
    expression: &Expression,
    array: &ArrayRef,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    if !matches!(array.dtype(), DType::Bool(_)) {
        return Ok(None);
    }
    let KeyColumn::Boolean { values, valid } = keys(array, context)? else {
        return Ok(None);
    };
    let view = values.bit_buffer_view();
    let nullable = expression.dtype.is_nullable();
    let allocator = context.native_session().host_allocator();
    result_batch::build_bool_buffers(nullable, rows, &allocator, |data, valid_out| {
        let mut valid_chunks = valid_out.map(|buf| buf.chunks_mut(8));
        for (index, out_chunk) in data.chunks_mut(8).enumerate() {
            context.check_cancelled()?;
            let start = index * 64;
            let end = rows.min(start + 64);
            let valid_word = mask_bit_word(&valid, start, end);
            if !nullable && valid_word != tail_mask(end - start) {
                return Err(failed("null in a nonnullable output column"));
            }
            let val_word = read_bit_word(view.inner(), view.offset(), start, end);
            let out_word = (!val_word) & valid_word;
            out_chunk.copy_from_slice(&out_word.to_le_bytes()[..out_chunk.len()]);
            if let Some(vchunk) = valid_chunks.as_mut().and_then(Iterator::next) {
                vchunk.copy_from_slice(&valid_word.to_le_bytes()[..vchunk.len()]);
            }
        }
        Ok(())
    })
    .map(Some)
}

fn evaluate_negate_fast(
    expression: &Expression,
    array: &ArrayRef,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let DType::Primitive(out_ptype, _) = expression.dtype else {
        return Ok(None);
    };
    if !matches!(array.dtype(), DType::Primitive(..))
        || !matches!(out_ptype, PType::I64 | PType::F64)
    {
        return Ok(None);
    }
    let KeyColumn::Numeric(owner) = keys(array, context)? else {
        return Ok(None);
    };
    let allocator = context.native_session().host_allocator();
    let float_policy = context.float_policy();
    let nullable = expression.dtype.is_nullable();
    if out_ptype == PType::F64 {
        result_batch::build_primitive_buffers::<f64>(
            &expression.dtype,
            rows,
            &allocator,
            |out, mut valid_out| {
                for (row, dst) in out.iter_mut().enumerate().take(rows) {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if !owner.all_valid() && !owner.validity().value(row) {
                        if !nullable {
                            return Err(failed("null in a nonnullable output column"));
                        }
                        continue;
                    }
                    let val = read_owner_f64(&owner, row, float_policy)?;
                    *dst = canonicalize_f64_result(-val);
                    if let Some(valid) = valid_out.as_deref_mut() {
                        set_valid_slice(valid, row);
                    }
                }
                Ok(())
            },
        )
        .map(Some)
    } else {
        result_batch::build_primitive_buffers::<i64>(
            &expression.dtype,
            rows,
            &allocator,
            |out, mut valid_out| {
                for (row, dst) in out.iter_mut().enumerate().take(rows) {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if !owner.all_valid() && !owner.validity().value(row) {
                        if !nullable {
                            return Err(failed("null in a nonnullable output column"));
                        }
                        continue;
                    }
                    let val = read_owner_i128(&owner, row)?;
                    *dst = i64::try_from(-val).map_err(vortex_error)?;
                    if let Some(valid) = valid_out.as_deref_mut() {
                        set_valid_slice(valid, row);
                    }
                }
                Ok(())
            },
        )
        .map(Some)
    }
}

pub(super) fn evaluate_compare_fast(
    expression: &Expression,
    left: &Expression,
    op: ComparisonOp,
    right: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    completed: Option<&[ArrayRef]>,
) -> Result<Option<ArrayRef>> {
    if !matches!(expression.dtype, DType::Bool(_)) {
        return Ok(None);
    }
    let left_compatible = matches!(left.dtype, DType::Primitive(..) | DType::Bool(_));
    let right_compatible = matches!(right.dtype, DType::Primitive(..) | DType::Bool(_));
    if !left_compatible || !right_compatible {
        return Ok(None);
    }
    let left_arr = left.evaluate_in_scope(input, context, completed)?;
    let right_arr = right.evaluate_in_scope(input, context, completed)?;
    let allocator = context.native_session().host_allocator();
    let rows = input.len();
    let nullable = expression.dtype.is_nullable();
    let float_policy = context.float_policy();
    match (keys(&left_arr, context)?, keys(&right_arr, context)?) {
        (
            KeyColumn::Boolean {
                values: lv,
                valid: lvalid,
            },
            KeyColumn::Boolean {
                values: rv,
                valid: rvalid,
            },
        ) => compare_bool_columns(
            rows, nullable, &lv, &lvalid, &rv, &rvalid, op, &allocator, context,
        )
        .map(Some),
        (KeyColumn::Numeric(lo), KeyColumn::Numeric(ro)) => compare_numeric_columns(
            rows,
            nullable,
            &lo,
            &ro,
            op,
            float_policy,
            &allocator,
            context,
        ),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn compare_numeric_columns(
    rows: usize,
    nullable: bool,
    lo: &NativeNumericOwner,
    ro: &NativeNumericOwner,
    op: ComparisonOp,
    float_policy: FloatPolicy,
    allocator: &vortex::array::memory::HostAllocatorRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let l_is_float = lo.ptype().is_float();
    let r_is_float = ro.ptype().is_float();
    if l_is_float || r_is_float {
        if !l_is_float || !r_is_float {
            // Preserve exact `integer_float_order` for mixed int/float comparisons.
            return Ok(None);
        }
        return compare_numeric_owners(
            rows,
            nullable,
            lo,
            ro,
            op,
            float_policy,
            allocator,
            context,
            |row| {
                let l = f64::from_bits(shardloom_core::float_policy::float_key_bits(
                    read_owner_f64(lo, row, float_policy)?,
                ));
                let r = f64::from_bits(shardloom_core::float_policy::float_key_bits(
                    read_owner_f64(ro, row, float_policy)?,
                ));
                Ok(l.total_cmp(&r))
            },
        )
        .map(Some);
    }
    if lo.ptype().is_signed_int() && ro.ptype().is_signed_int() {
        return compare_numeric_owners(
            rows,
            nullable,
            lo,
            ro,
            op,
            float_policy,
            allocator,
            context,
            |row| {
                let l = i64::try_from(read_owner_i128(lo, row)?).map_err(vortex_error)?;
                let r = i64::try_from(read_owner_i128(ro, row)?).map_err(vortex_error)?;
                Ok(l.cmp(&r))
            },
        )
        .map(Some);
    }
    if lo.ptype().is_unsigned_int() && ro.ptype().is_unsigned_int() {
        return compare_numeric_owners(
            rows,
            nullable,
            lo,
            ro,
            op,
            float_policy,
            allocator,
            context,
            |row| {
                let l = u64::try_from(read_owner_i128(lo, row)?).map_err(vortex_error)?;
                let r = u64::try_from(read_owner_i128(ro, row)?).map_err(vortex_error)?;
                Ok(l.cmp(&r))
            },
        )
        .map(Some);
    }
    compare_numeric_owners(
        rows,
        nullable,
        lo,
        ro,
        op,
        float_policy,
        allocator,
        context,
        |row| Ok(read_owner_i128(lo, row)?.cmp(&read_owner_i128(ro, row)?)),
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
fn compare_bool_columns(
    rows: usize,
    nullable: bool,
    lv: &vortex::array::arrays::BoolArray,
    lvalid: &vortex::mask::Mask,
    rv: &vortex::array::arrays::BoolArray,
    rvalid: &vortex::mask::Mask,
    op: ComparisonOp,
    allocator: &vortex::array::memory::HostAllocatorRef,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    result_batch::build_bool_buffers(nullable, rows, allocator, |data, valid_out| {
        let lview = lv.bit_buffer_view();
        let rview = rv.bit_buffer_view();
        let mut valid_chunks = valid_out.map(|buf| buf.chunks_mut(8));
        for (index, out_chunk) in data.chunks_mut(8).enumerate() {
            context.check_cancelled()?;
            let start = index * 64;
            let end = rows.min(start + 64);
            let both_valid = mask_bit_word(lvalid, start, end) & mask_bit_word(rvalid, start, end);
            if !nullable && both_valid != tail_mask(end - start) {
                return Err(failed("null in a nonnullable output column"));
            }
            let lword = read_bit_word(lview.inner(), lview.offset(), start, end);
            let rword = read_bit_word(rview.inner(), rview.offset(), start, end);
            let out_word = match op {
                ComparisonOp::Eq => !(lword ^ rword) & both_valid,
                ComparisonOp::NotEq => (lword ^ rword) & both_valid,
                ComparisonOp::Lt => (!lword) & rword & both_valid,
                ComparisonOp::LtEq => ((!lword) | rword) & both_valid,
                ComparisonOp::Gt => lword & (!rword) & both_valid,
                ComparisonOp::GtEq => (lword | (!rword)) & both_valid,
            };
            out_chunk.copy_from_slice(&out_word.to_le_bytes()[..out_chunk.len()]);
            if let Some(vchunk) = valid_chunks.as_mut().and_then(Iterator::next) {
                vchunk.copy_from_slice(&both_valid.to_le_bytes()[..vchunk.len()]);
            }
        }
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn compare_numeric_owners(
    rows: usize,
    nullable: bool,
    lo: &NativeNumericOwner,
    ro: &NativeNumericOwner,
    op: ComparisonOp,
    _float_policy: FloatPolicy,
    allocator: &vortex::array::memory::HostAllocatorRef,
    context: &NativeExecutionContext<'_>,
    mut cmp_row: impl FnMut(usize) -> Result<Ordering>,
) -> Result<ArrayRef> {
    let all_valid = lo.all_valid() && ro.all_valid();
    result_batch::build_bool_buffers(nullable, rows, allocator, |data, mut valid_out| {
        for (chunk_idx, out_chunk) in data.chunks_mut(8).enumerate() {
            context.check_cancelled()?;
            let start = chunk_idx * 64;
            let end = rows.min(start + 64);
            let both_valid = if all_valid {
                tail_mask(end - start)
            } else {
                mask_bit_word(lo.validity(), start, end) & mask_bit_word(ro.validity(), start, end)
            };
            if !nullable && both_valid != tail_mask(end - start) {
                return Err(failed("null in a nonnullable output column"));
            }
            let mut yes = 0_u64;
            for offset in 0..(end - start) {
                if both_valid & (1_u64 << offset) != 0 && op_matches(cmp_row(start + offset)?, op) {
                    yes |= 1_u64 << offset;
                }
            }
            out_chunk.copy_from_slice(&yes.to_le_bytes()[..out_chunk.len()]);
            if let Some(vbuf) = valid_out.as_deref_mut() {
                let vslice = &mut vbuf[chunk_idx * 8..][..out_chunk.len()];
                vslice.copy_from_slice(&both_valid.to_le_bytes()[..vslice.len()]);
            }
        }
        Ok(())
    })
}

pub(super) fn evaluate_cast_fast(
    expression: &Expression,
    child: &Expression,
    tolerant: bool,
    temporal: Option<&TemporalCast>,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    if temporal.is_some() {
        return Ok(None);
    }
    let DType::Primitive(out_ptype, _) = expression.dtype else {
        return Ok(None);
    };
    let Some(to) = reg_type(&expression.dtype) else {
        return Ok(None);
    };
    if !matches!(child.dtype, DType::Primitive(..)) {
        return Ok(None);
    }
    let array = child.evaluate(input, context)?;
    let KeyColumn::Numeric(owner) = keys(&array, context)? else {
        return Ok(None);
    };
    let from = leaf_reg_type(owner.ptype());
    let rows = input.len();
    let float_policy = context.float_policy();
    let nullable = expression.dtype.is_nullable();
    let allocator = context.native_session().host_allocator();
    let all_valid = owner.all_valid();
    macro_rules! build_cast {
        ($T:ty, $conv:expr) => {
            result_batch::build_primitive_buffers::<$T>(
                &expression.dtype,
                rows,
                &allocator,
                |out, mut valid_out| {
                    for row in 0..rows {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        if !all_valid && !owner.validity().value(row) {
                            if !nullable {
                                return Err(failed("null in a nonnullable output column"));
                            }
                            continue;
                        }
                        let raw_bits = match from {
                            RegType::Float64 => {
                                read_owner_f64(&owner, row, float_policy)?.to_bits()
                            }
                            RegType::Signed64 => i64::try_from(read_owner_i128(&owner, row)?)
                                .map_err(vortex_error)?
                                .cast_unsigned(),
                            RegType::Unsigned64 => u64::try_from(read_owner_i128(&owner, row)?)
                                .map_err(vortex_error)?,
                        };
                        match cast_primitive_scalar(raw_bits, from, to, float_policy) {
                            Ok(bits) => {
                                out[row] = ($conv)(bits);
                                if let Some(valid) = valid_out.as_deref_mut() {
                                    set_valid_slice(valid, row);
                                }
                            }
                            Err(_) if tolerant => {
                                if !nullable {
                                    return Err(failed("null in a nonnullable output column"));
                                }
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    Ok(())
                },
            )
            .map(Some)
        };
    }
    match out_ptype {
        PType::I64 => build_cast!(i64, u64::cast_signed),
        PType::U64 => build_cast!(u64, |b| b),
        PType::F64 => build_cast!(f64, f64::from_bits),
        _ => Ok(None),
    }
}

pub(super) fn evaluate_conditional_fast(
    expression: &Expression,
    condition: &Expression,
    yes: &Expression,
    no: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let DType::Primitive(out_ptype, _) = expression.dtype else {
        return Ok(None);
    };
    let Some(out_ty) = reg_type(&expression.dtype) else {
        return Ok(None);
    };
    let compatible_branch = |branch: &Expression| {
        matches!(branch.kind, Kind::Column(_) | Kind::Literal(_))
            && branch.reuse_output.is_none()
            && (matches!(branch.dtype, DType::Null)
                || branch.dtype.as_nonnullable() == expression.dtype.as_nonnullable())
    };
    if !compatible_branch(yes) || !compatible_branch(no) {
        return Ok(None);
    }
    let cond_arr = condition.evaluate(input, context)?;
    let (cond_bools, cond_valid) = match keys(&cond_arr, context)? {
        KeyColumn::Boolean { values, valid } => (Some(values), Some(valid)),
        KeyColumn::Null(_) => (None, None),
        _ => return Err(failed("conditional condition requires Boolean values")),
    };
    let rows = input.len();
    let cond_view = cond_bools.as_ref().map(BoolArrayExt::bit_buffer_view);
    let mut has_yes = false;
    let mut has_no = false;
    for row in 0..rows {
        let take_yes = cond_valid
            .as_ref()
            .zip(cond_view.as_ref())
            .is_some_and(|(valid, view)| valid.value(row) && view.value(row));
        has_yes |= take_yes;
        has_no |= !take_yes;
        if has_yes && has_no {
            break;
        }
    }
    let yes_source = if has_yes {
        Some(prepare_leaf_branch(yes, input, context)?)
    } else {
        None
    };
    let no_source = if has_no {
        Some(prepare_leaf_branch(no, input, context)?)
    } else {
        None
    };
    let allocator = context.native_session().host_allocator();
    match out_ptype {
        PType::I64 => build_conditional_column(
            &expression.dtype,
            out_ty,
            rows,
            cond_valid.as_ref(),
            cond_view.as_ref(),
            yes_source.as_ref(),
            no_source.as_ref(),
            &allocator,
            context,
            u64::cast_signed,
        )
        .map(Some),
        PType::U64 => build_conditional_column(
            &expression.dtype,
            out_ty,
            rows,
            cond_valid.as_ref(),
            cond_view.as_ref(),
            yes_source.as_ref(),
            no_source.as_ref(),
            &allocator,
            context,
            |b| b,
        )
        .map(Some),
        PType::F64 => build_conditional_column(
            &expression.dtype,
            out_ty,
            rows,
            cond_valid.as_ref(),
            cond_view.as_ref(),
            yes_source.as_ref(),
            no_source.as_ref(),
            &allocator,
            context,
            f64::from_bits,
        )
        .map(Some),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_conditional_column<
    T: vortex::array::dtype::NativePType + vortex::array::memory::HostBufferElement,
>(
    dtype: &DType,
    out_ty: RegType,
    rows: usize,
    cond_valid: Option<&vortex::mask::Mask>,
    cond_view: Option<&vortex::buffer::BitBufferView<'_>>,
    yes_source: Option<&LeafBranch>,
    no_source: Option<&LeafBranch>,
    allocator: &vortex::array::memory::HostAllocatorRef,
    context: &NativeExecutionContext<'_>,
    convert: impl Fn(u64) -> T,
) -> Result<ArrayRef> {
    let nullable = dtype.is_nullable();
    let float_policy = context.float_policy();
    result_batch::build_primitive_buffers::<T>(dtype, rows, allocator, |out, mut valid_out| {
        for (row, dst) in out.iter_mut().enumerate().take(rows) {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let take_yes = cond_valid
                .zip(cond_view)
                .is_some_and(|(valid, view)| valid.value(row) && view.value(row));
            let branch = if take_yes {
                yes_source.expect("yes branch present")
            } else {
                no_source.expect("no branch present")
            };
            if let Some(bits) = branch.value_bits(row, out_ty, float_policy)? {
                *dst = convert(bits);
                if let Some(valid) = valid_out.as_deref_mut() {
                    set_valid_slice(valid, row);
                }
            } else if !nullable {
                return Err(failed("null in a nonnullable output column"));
            }
        }
        Ok(())
    })
}

enum LeafBranch {
    Null,
    Scalar(LiteralValue),
    Column(NativeNumericOwner),
}

impl LeafBranch {
    fn value_bits(
        &self,
        row: usize,
        target: RegType,
        float_policy: FloatPolicy,
    ) -> Result<Option<u64>> {
        match self {
            Self::Null => Ok(None),
            Self::Scalar(val) => match (val, target) {
                (LiteralValue::Null, _) => Ok(None),
                (LiteralValue::Signed(v), RegType::Signed64) => Ok(Some(v.cast_unsigned())),
                (LiteralValue::Unsigned(v), RegType::Unsigned64) => Ok(Some(*v)),
                (LiteralValue::Float(v), RegType::Float64) => {
                    validate_float_scalar(*v, float_policy)?;
                    Ok(Some(v.to_bits()))
                }
                _ => Err(failed("conditional branch literal changed dtype")),
            },
            Self::Column(owner) => {
                if !owner.all_valid() && !owner.validity().value(row) {
                    return Ok(None);
                }
                Ok(Some(match target {
                    RegType::Signed64 => {
                        let v =
                            i64::try_from(read_owner_i128(owner, row)?).map_err(vortex_error)?;
                        v.cast_unsigned()
                    }
                    RegType::Unsigned64 => {
                        u64::try_from(read_owner_i128(owner, row)?).map_err(vortex_error)?
                    }
                    RegType::Float64 => read_owner_f64(owner, row, float_policy)?.to_bits(),
                }))
            }
        }
    }
}

fn prepare_leaf_branch(
    expr: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<LeafBranch> {
    match &expr.kind {
        Kind::Literal(scalar) => {
            if let Some((val, _)) = compile_literal(&expr.dtype, scalar) {
                Ok(LeafBranch::Scalar(val))
            } else {
                // Non-finite float literal or NULL scalar
                if let Some(ScalarValue::Primitive(PValue::F64(v))) = scalar.value() {
                    Ok(LeafBranch::Scalar(LiteralValue::Float(*v)))
                } else {
                    Ok(LeafBranch::Null)
                }
            }
        }
        Kind::Column(name) => {
            let array = super::super::logical_field_from_native_array(input, name)?;
            match keys(&array, context)? {
                KeyColumn::Numeric(owner) => Ok(LeafBranch::Column(owner)),
                KeyColumn::Null(_) => Ok(LeafBranch::Null),
                _ => Err(failed("conditional branch column changed dtype")),
            }
        }
        _ => unreachable!("guarded by conditional fast path"),
    }
}

fn read_owner_i128(owner: &NativeNumericOwner, row: usize) -> Result<i128> {
    Ok(match owner.ptype() {
        PType::I8 => i128::from(owner.primitive().as_slice::<i8>()[row]),
        PType::I16 => i128::from(owner.primitive().as_slice::<i16>()[row]),
        PType::I32 => i128::from(owner.primitive().as_slice::<i32>()[row]),
        PType::I64 => i128::from(owner.primitive().as_slice::<i64>()[row]),
        PType::U8 => i128::from(owner.primitive().as_slice::<u8>()[row]),
        PType::U16 => i128::from(owner.primitive().as_slice::<u16>()[row]),
        PType::U32 => i128::from(owner.primitive().as_slice::<u32>()[row]),
        PType::U64 => i128::from(owner.primitive().as_slice::<u64>()[row]),
        _ => return Err(failed("numeric expression requires an integer")),
    })
}

fn read_owner_f64(
    owner: &NativeNumericOwner,
    row: usize,
    float_policy: FloatPolicy,
) -> Result<f64> {
    let val = match owner.ptype() {
        PType::F16 => shardloom_core::float_policy::widen_f16_bits(
            owner.primitive().as_slice::<f16>()[row].to_bits(),
        ),
        PType::F32 => {
            shardloom_core::float_policy::widen_f32(owner.primitive().as_slice::<f32>()[row])
        }
        PType::F64 => owner.primitive().as_slice::<f64>()[row],
        _ => return Err(failed("numeric expression requires a floating operand")),
    };
    validate_float_scalar(val, float_policy)?;
    Ok(val)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _, VortexSessionExecute as _,
            arrays::{PrimitiveArray, StructArray},
            dtype::Nullability,
            validity::Validity,
        },
        buffer::Buffer,
    };

    fn pcol(name: &str, ptype: PType, nullable: bool) -> Expression {
        Expression::new(
            DType::Primitive(
                ptype,
                if nullable {
                    Nullability::Nullable
                } else {
                    Nullability::NonNullable
                },
            ),
            Kind::Column(name.into()),
        )
    }

    fn plit(scalar: Scalar) -> Expression {
        Expression::new(scalar.dtype().clone(), Kind::Literal(scalar))
    }

    fn f64_bin(left: Expression, op: BinaryOp, right: Expression) -> Expression {
        let n = if left.dtype.is_nullable() || right.dtype.is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        Expression::new(
            DType::Primitive(PType::F64, n),
            Kind::Binary(Box::new(left), op, Box::new(right)),
        )
    }

    fn i64_bin(left: Expression, op: BinaryOp, right: Expression) -> Expression {
        let n = if left.dtype.is_nullable() || right.dtype.is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        Expression::new(
            DType::Primitive(PType::I64, n),
            Kind::Binary(Box::new(left), op, Box::new(right)),
        )
    }

    #[test]
    fn compound_numeric_chunk_program_evaluates_tpch_pricing_overflow_and_null_masks() {
        let ep = pcol("ep", PType::F64, true);
        let disc = pcol("disc", PType::F64, true);
        let tax = pcol("tax", PType::F64, true);
        // ep * (1.0 - disc) * (1.0 + tax)
        let expr = f64_bin(
            f64_bin(
                ep,
                BinaryOp::Multiply,
                f64_bin(plit(Scalar::from(1.0_f64)), BinaryOp::Subtract, disc),
            ),
            BinaryOp::Multiply,
            f64_bin(plit(Scalar::from(1.0_f64)), BinaryOp::Add, tax),
        );
        assert!(NumericChunkProgram::compile(&expr).is_some());

        let rows = 517;
        let ep_vals: Vec<Option<f64>> = (0..rows)
            .map(|r| (r % 11 != 0).then_some(f64::from(u32::try_from(r).unwrap()) * 1.5))
            .collect();
        let disc_vals: Vec<Option<f64>> = (0..rows)
            .map(|r| (r % 7 != 0).then_some(0.01 * f64::from(u32::try_from(r % 10).unwrap())))
            .collect();
        let tax_vals: Vec<Option<f64>> = (0..rows)
            .map(|r| Some(0.02 * f64::from(u32::try_from(r % 5).unwrap())))
            .collect();
        let input = StructArray::from_fields(&[
            (
                "ep",
                PrimitiveArray::from_option_iter(ep_vals.clone()).into_array(),
            ),
            (
                "disc",
                PrimitiveArray::from_option_iter(disc_vals.clone()).into_array(),
            ),
            (
                "tax",
                PrimitiveArray::from_option_iter(tax_vals.clone()).into_array(),
            ),
        ])
        .unwrap()
        .into_array();

        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let out = session
            .with_native_execution_context(&CancellationToken::default(), |ctx| {
                expr.evaluate(&input, ctx)
            })
            .unwrap();
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        let prim = out
            .clone()
            .execute::<PrimitiveArray>(&mut execution)
            .unwrap();
        let valid = prim
            .validity()
            .unwrap()
            .execute_mask(rows, &mut execution)
            .unwrap();
        for i in 0..rows {
            let expected = match (ep_vals[i], disc_vals[i], tax_vals[i]) {
                (Some(e), Some(d), Some(t)) => Some(e * (1.0 - d) * (1.0 + t)),
                _ => None,
            };
            if let Some(exp) = expected {
                assert!(valid.value(i));
                assert!((prim.as_slice::<f64>()[i] - exp).abs() < 1e-12);
            } else {
                assert!(!valid.value(i));
            }
        }
        drop((valid, prim, out, input));
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }

    #[test]
    fn compound_numeric_chunk_program_preserves_overflow_zero_division_and_strict_nan() {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        // NULL-masked physical zero in denominator must NOT fail; overflow/nonfinite on active row MUST fail.
        let a = pcol("a", PType::I64, true);
        let b = pcol("b", PType::I64, true);
        let expr = i64_bin(
            i64_bin(a, BinaryOp::Add, plit(Scalar::from(1_i64))),
            BinaryOp::Divide,
            b,
        );
        assert!(NumericChunkProgram::compile(&expr).is_some());
        let null_masked_zero = PrimitiveArray::new(
            Buffer::from(vec![10_i64, 0]),
            Validity::from_iter([true, false]),
        )
        .into_array();
        let input = StructArray::from_fields(&[
            (
                "a",
                PrimitiveArray::from_option_iter([Some(9_i64), Some(5)]).into_array(),
            ),
            ("b", null_masked_zero),
        ])
        .unwrap()
        .into_array();
        let out = session
            .with_native_execution_context(&CancellationToken::default(), |ctx| {
                expr.evaluate(&input, ctx)
            })
            .unwrap();
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        assert_eq!(
            out.execute_scalar(0, &mut execution).unwrap(),
            Scalar::primitive(1_i64, Nullability::Nullable)
        );
        assert!(out.execute_scalar(1, &mut execution).unwrap().is_null());

        let bad_input = StructArray::from_fields(&[
            (
                "a",
                PrimitiveArray::from_option_iter([Some(i64::MAX)]).into_array(),
            ),
            (
                "b",
                PrimitiveArray::from_option_iter([Some(1_i64)]).into_array(),
            ),
        ])
        .unwrap()
        .into_array();
        let overflow_err = session
            .with_native_execution_context(&CancellationToken::default(), |ctx| {
                expr.evaluate(&bad_input, ctx)
            })
            .unwrap_err();
        assert!(
            overflow_err.to_string().contains("overflow"),
            "unexpected error: {overflow_err}"
        );
    }

    #[test]
    fn fast_unary_compare_cast_and_lazy_case_when_match_contracts() {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let make_cond = || {
            Expression::new(
                DType::Bool(Nullability::Nullable),
                Kind::Compare(
                    Box::new(pcol("d", PType::I64, true)),
                    ComparisonOp::NotEq,
                    Box::new(plit(Scalar::from(0_i64))),
                ),
            )
        };
        // Unsafe division in yes branch only evaluates where d != 0.
        let unsafe_div = i64_bin(
            plit(Scalar::from(10_i64)),
            BinaryOp::Divide,
            pcol("d", PType::I64, true),
        );
        let case_fallible = Expression::new(
            DType::Primitive(PType::I64, Nullability::Nullable),
            Kind::Conditional(
                Box::new(make_cond()),
                Box::new(unsafe_div),
                Box::new(plit(Scalar::from(-1_i64))),
            ),
        );
        let case_pure = Expression::new(
            DType::Primitive(PType::I64, Nullability::Nullable),
            Kind::Conditional(
                Box::new(make_cond()),
                Box::new(pcol("d", PType::I64, true)),
                Box::new(plit(Scalar::from(-1_i64))),
            ),
        );
        let cast_f64 = Expression::new(
            DType::Primitive(PType::F64, Nullability::Nullable),
            Kind::Cast {
                input: Box::new(pcol("d", PType::I64, true)),
                tolerant: false,
                temporal: None,
            },
        );
        let is_null_expr = Expression::new(
            DType::Bool(Nullability::NonNullable),
            Kind::Unary(UnaryOp::IsNull, Box::new(pcol("d", PType::I64, true))),
        );
        let input = StructArray::from_fields(&[(
            "d",
            PrimitiveArray::from_option_iter([Some(0_i64), Some(2), None]).into_array(),
        )])
        .unwrap()
        .into_array();
        session
            .with_native_execution_context(&CancellationToken::default(), |ctx| {
                let mut exec = ctx.native_session().create_execution_ctx();
                let f = case_fallible.evaluate(&input, ctx)?;
                assert_eq!(
                    f.execute_scalar(0, &mut exec).unwrap(),
                    Scalar::primitive(-1_i64, Nullability::Nullable)
                );
                assert_eq!(
                    f.execute_scalar(1, &mut exec).unwrap(),
                    Scalar::primitive(5_i64, Nullability::Nullable)
                );
                assert_eq!(
                    f.execute_scalar(2, &mut exec).unwrap(),
                    Scalar::primitive(-1_i64, Nullability::Nullable)
                );

                let p = case_pure.evaluate(&input, ctx)?;
                assert_eq!(
                    p.execute_scalar(0, &mut exec).unwrap(),
                    Scalar::primitive(-1_i64, Nullability::Nullable)
                );
                assert_eq!(
                    p.execute_scalar(1, &mut exec).unwrap(),
                    Scalar::primitive(2_i64, Nullability::Nullable)
                );

                let cf = cast_f64.evaluate(&input, ctx)?;
                assert_eq!(
                    cf.execute_scalar(1, &mut exec).unwrap(),
                    Scalar::primitive(2.0_f64, Nullability::Nullable)
                );
                assert!(cf.execute_scalar(2, &mut exec).unwrap().is_null());

                let isn = is_null_expr.evaluate(&input, ctx)?;
                assert_eq!(
                    isn.execute_scalar(0, &mut exec).unwrap(),
                    Scalar::bool(false, Nullability::NonNullable)
                );
                assert_eq!(
                    isn.execute_scalar(2, &mut exec).unwrap(),
                    Scalar::bool(true, Nullability::NonNullable)
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}
