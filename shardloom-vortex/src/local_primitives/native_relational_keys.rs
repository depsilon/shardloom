//! Native key owners shared by relational matching. Numeric execution reuses
//! the existing original-width owner; variable bytes keep native dictionary domains.
//! Equality normalizes finite floating signed zero and integer signedness.

#[cfg(test)]
#[path = "native_relational_nested_keys_tests.rs"]
mod nested_tests;
#[cfg(test)]
#[path = "native_relational_typed_keys_tests.rs"]
mod typed_tests;

#[path = "native_relational_nested_keys.rs"]
mod nested;

use super::{AggregateDistinctValue, NativeNumericOwner, compound_count_partial, vortex_error};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{cmp::Ordering, hash::Hasher as _};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{
            BoolArray, DecimalArray, Dict, PrimitiveArray, VarBinViewArray,
            bool::BoolArrayExt as _, decimal::DecimalArrayExt as _, dict::DictArraySlotsExt as _,
            varbinview::VarBinViewArrayExt as _,
        },
        dtype::{DType, DecimalDType, PType},
        scalar::DecimalValue,
    },
    buffer::ByteBuffer,
    mask::Mask,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Cell {
    Null,
    NegativeInteger(i64),
    NonnegativeInteger(u64),
    Float(u64),
    Boolean(bool),
    Utf8(ByteBuffer),
    Binary(ByteBuffer),
    Decimal(i128, DecimalDType),
    Date(i32),
    Timestamp(i64),
}

pub(super) enum KeyColumn {
    Null(usize),
    Numeric(NativeNumericOwner),
    Boolean { values: BoolArray, valid: Mask },
    Variable(VariableColumn),
    Decimal(DecimalColumn),
    Date(NativeNumericOwner),
    Timestamp(NativeNumericOwner),
    Nested(Box<nested::Column>),
}

pub(super) struct VariableColumn {
    values: VarBinViewArray,
    valid: Mask,
    codes: Option<NativeNumericOwner>,
    hashes: Vec<u64>,
    _hash_ownership: MemoryLease,
    rows: usize,
    binary: bool,
}

pub(super) struct DecimalColumn {
    values: DecimalArray,
    valid: Mask,
    dtype: DecimalDType,
}

impl KeyColumn {
    pub(super) fn new(
        array: &ArrayRef,
        context: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let schema = if super::native_payload::is_nested(array.dtype()) {
            Some(memory.reserve(crate::native_payload_schema::metadata_bytes(array.dtype())?)?)
        } else {
            None
        };
        Self::new_inner(array, context, memory, cancellation, schema)
    }

    fn new_inner(
        array: &ArrayRef,
        context: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        schema: Option<MemoryLease>,
    ) -> Result<Self> {
        cancellation.check()?;
        let column = match array.dtype() {
            DType::Null => Self::Null(array.len()),
            DType::Primitive(ptype, _) if *ptype != PType::F16 => {
                Self::Numeric(numeric_owner(array, context)?)
            }
            DType::Bool(_) => {
                let values = array
                    .clone()
                    .execute::<BoolArray>(context)
                    .map_err(vortex_error)?;
                if values.dtype() != array.dtype() || values.len() != array.len() {
                    return Err(failed("boolean execution changed dtype or row count"));
                }
                let valid = values
                    .validity()
                    .map_err(vortex_error)?
                    .execute_mask(values.len(), context)
                    .map_err(vortex_error)?;
                Self::Boolean { values, valid }
            }
            DType::Utf8(_) | DType::Binary(_) => {
                Self::Variable(VariableColumn::new(array, context, memory, cancellation)?)
            }
            DType::Decimal(dtype, _) if crate::native_payload_schema::admitted_decimal(*dtype) => {
                Self::Decimal(DecimalColumn::new(array, *dtype, context)?)
            }
            DType::Extension(_) => {
                let ptype = crate::native_payload_schema::temporal_storage(array.dtype())
                    .ok_or_else(|| {
                        failed("temporal key requires Date32 or timezone-free TimestampMicros")
                    })?;
                let storage = super::result_batch::scalar_storage(array, context)?;
                if storage.len() != array.len() {
                    return Err(failed("temporal execution changed row count"));
                }
                let values = numeric_owner(&storage, context)?;
                match ptype {
                    PType::I32 => Self::Date(values),
                    PType::I64 => Self::Timestamp(values),
                    _ => return Err(failed("temporal key has an unsupported storage type")),
                }
            }
            DType::List(..) | DType::FixedSizeList(..) | DType::Struct(..) => {
                Self::Nested(Box::new(nested::Column::new(
                    array,
                    context,
                    memory,
                    cancellation,
                    schema,
                )?))
            }
            _ => {
                return Err(failed(
                    "key requires integer, finite float, boolean, UTF8, binary, admitted Decimal128, Date32 or timezone-free TimestampMicros dtype",
                ));
            }
        };
        cancellation.check()?;
        Ok(column)
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Null(rows) => *rows,
            Self::Numeric(values) | Self::Date(values) | Self::Timestamp(values) => values.len(),
            Self::Boolean { values, .. } => values.len(),
            Self::Variable(values) => values.rows,
            Self::Decimal(values) => values.values.len(),
            Self::Nested(values) => values.len(),
        }
    }

    pub(super) fn cell(&self, row: usize) -> Result<Cell> {
        if row >= self.len() {
            return Err(failed("row index exceeds key owner"));
        }
        match self {
            Self::Null(_) => Ok(Cell::Null),
            Self::Numeric(values) => normalize_numeric(&values.distinct_value(row)?),
            Self::Boolean { values, valid } => Ok(if valid.value(row) {
                Cell::Boolean(values.bit_buffer_view().value(row))
            } else {
                Cell::Null
            }),
            Self::Variable(values) => Ok(values.index(row)?.map_or(Cell::Null, |index| {
                let bytes = values.values.bytes_at(index);
                if values.binary {
                    Cell::Binary(bytes)
                } else {
                    Cell::Utf8(bytes)
                }
            })),
            Self::Decimal(values) => values.cell(row),
            Self::Date(values) => Ok(match temporal_value(values, row)? {
                None => Cell::Null,
                Some(value) => Cell::Date(i32::try_from(value).map_err(vortex_error)?),
            }),
            Self::Timestamp(values) => {
                Ok(temporal_value(values, row)?.map_or(Cell::Null, Cell::Timestamp))
            }
            Self::Nested(_) => Err(failed(
                "nested keys require native comparison or selection, not a scalar cell",
            )),
        }
    }

    /// Scalar arithmetic and selected values preserve the sign of floating
    /// zero. Hash/equality keys use the normalized representation in `cell`.
    pub(super) fn raw_cell(&self, row: usize) -> Result<Cell> {
        if row >= self.len() {
            return Err(failed("row index exceeds scalar owner"));
        }
        if let Self::Numeric(values) = self
            && let AggregateDistinctValue::Float64Bits(bits) = values.distinct_value(row)?
        {
            if !f64::from_bits(bits).is_finite() {
                return Err(failed("nonfinite scalar values are not admitted"));
            }
            return Ok(Cell::Float(bits));
        }
        self.cell(row)
    }

    /// Native text comparisons borrow their domain bytes. Numeric keys widen
    /// only in registers, with exact integer signedness and normalized zero.
    /// Nulls sort first here; the bound operator owns its requested null order.
    pub(super) fn compare_at(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
    ) -> Result<Ordering> {
        if row >= self.len() || other_row >= other.len() {
            return Err(failed("comparison row index exceeds key owner"));
        }
        if let (Self::Nested(left), Self::Nested(right)) = (self, other) {
            return left.compare_at(row, right, other_row);
        }
        if let (Self::Variable(left), Self::Variable(right)) = (self, other)
            && left.binary == right.binary
        {
            return Ok(match (left.index(row)?, right.index(other_row)?) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(left_row), Some(right_row)) => {
                    super::native_utf8::borrowed_bytes(&left.values, left_row)
                        .cmp(super::native_utf8::borrowed_bytes(&right.values, right_row))
                }
            });
        }
        compare_cells(self.cell(row)?, other.cell(other_row)?)
    }

    pub(super) fn is_null(&self, row: usize) -> Result<bool> {
        if row >= self.len() {
            return Err(failed("null-check row index exceeds key owner"));
        }
        Ok(match self {
            Self::Null(_) => true,
            Self::Numeric(values) | Self::Date(values) | Self::Timestamp(values) => {
                values.distinct_value(row)? == AggregateDistinctValue::Null
            }
            Self::Boolean { valid, .. } => !valid.value(row),
            Self::Variable(values) => values.index(row)?.is_none(),
            Self::Decimal(values) => !values.valid.value(row),
            Self::Nested(values) => values.is_null(row)?,
        })
    }

    pub(super) fn equals_at(
        &self,
        row: usize,
        other: &Self,
        other_row: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        let nulls = (self.is_null(row)?, other.is_null(other_row)?);
        if nulls.0 || nulls.1 {
            return Ok(nulls_equal && nulls.0 && nulls.1);
        }
        Ok(self.compare_at(row, other, other_row)? == Ordering::Equal)
    }

    /// A hash is only a candidate lookup. Callers must compare complete cells.
    /// Equal integers hash identically across widths/signedness; floats stay typed.
    pub(super) fn hash_into(&self, row: usize, hash: &mut rustc_hash::FxHasher) -> Result<bool> {
        if row >= self.len() {
            return Err(failed("hash row index exceeds key owner"));
        }
        if let Self::Nested(values) = self {
            return values.hash_into(row, hash);
        }
        if let Self::Variable(values) = self {
            if let Some(index) = values.index(row)? {
                hash.write_u8(if values.binary { 6 } else { 5 });
                hash.write_u64(values.hashes[index]);
                return Ok(true);
            }
            hash.write_u8(0);
            return Ok(false);
        }
        match self.cell(row)? {
            Cell::Null => {
                hash.write_u8(0);
                return Ok(false);
            }
            Cell::NegativeInteger(value) => {
                hash.write_u8(1);
                hash.write_i64(value);
            }
            Cell::NonnegativeInteger(value) => {
                hash.write_u8(2);
                hash.write_u64(value);
            }
            Cell::Float(bits) => {
                hash.write_u8(3);
                hash.write_u64(bits);
            }
            Cell::Boolean(value) => {
                hash.write_u8(4);
                hash.write_u8(u8::from(value));
            }
            Cell::Utf8(_) | Cell::Binary(_) => unreachable!("variable owner handled above"),
            Cell::Decimal(value, dtype) => {
                hash.write_u8(7);
                hash.write_u8(dtype.precision());
                hash.write_i8(dtype.scale());
                hash.write_i128(value);
            }
            Cell::Date(value) => {
                hash.write_u8(8);
                hash.write_i32(value);
            }
            Cell::Timestamp(value) => {
                hash.write_u8(9);
                hash.write_i64(value);
            }
        }
        Ok(true)
    }

    /// Exact unary identity inside a newly admitted nested key. This preserves
    /// signed floating zero, unlike relational hash/equality. Self-delimiting
    /// leaf lengths avoid repeated subtree sizing at every nesting level.
    pub(super) fn write_exact_key(
        &self,
        row: usize,
        output: &mut impl std::fmt::Write,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        if let Self::Nested(values) = self {
            return values.write_exact_key(row, output);
        }
        match self.raw_cell(row)? {
            Cell::Null => output.write_str("n;"),
            Cell::NegativeInteger(value) => write!(output, "i{value};"),
            Cell::NonnegativeInteger(value) => {
                let signed = matches!(self, Self::Numeric(owner) if owner.ptype().is_signed_int());
                write!(output, "{}{value};", if signed { 'i' } else { 'u' })
            }
            Cell::Float(bits) => write!(output, "f{bits:016x};"),
            Cell::Boolean(value) => write!(output, "b{};", u8::from(value)),
            Cell::Utf8(value) => {
                let value = std::str::from_utf8(value.as_slice()).map_err(vortex_error)?;
                write!(output, "s{}:", value.len()).map_err(vortex_error)?;
                let mut start = 0;
                while start < value.len() {
                    cancellation.check()?;
                    let mut end = start.saturating_add(4096).min(value.len());
                    while !value.is_char_boundary(end) {
                        end -= 1;
                    }
                    output.write_str(&value[start..end]).map_err(vortex_error)?;
                    start = end;
                }
                Ok(())
            }
            Cell::Binary(value) => {
                write!(output, "x{}:", value.len()).map_err(vortex_error)?;
                for (index, byte) in value.as_slice().iter().enumerate() {
                    if index.is_multiple_of(1024) {
                        cancellation.check()?;
                    }
                    write!(output, "{byte:02x}").map_err(vortex_error)?;
                }
                Ok(())
            }
            Cell::Decimal(value, dtype) => {
                write!(output, "d{},{}:{value};", dtype.precision(), dtype.scale())
            }
            Cell::Date(value) => write!(output, "D{value};"),
            Cell::Timestamp(value) => write!(output, "T{value};"),
        }
        .map_err(vortex_error)
    }
}

pub(super) fn compare_cells(left: Cell, right: Cell) -> Result<Ordering> {
    use Cell::{
        Binary, Boolean, Date, Decimal, Float, NegativeInteger, NonnegativeInteger, Null,
        Timestamp, Utf8,
    };
    Ok(match (left, right) {
        (Null, Null) => Ordering::Equal,
        (Null, _) | (NegativeInteger(_), NonnegativeInteger(_)) => Ordering::Less,
        (_, Null) | (NonnegativeInteger(_), NegativeInteger(_)) => Ordering::Greater,
        (NegativeInteger(left), NegativeInteger(right)) | (Timestamp(left), Timestamp(right)) => {
            left.cmp(&right)
        }
        (NonnegativeInteger(left), NonnegativeInteger(right)) => left.cmp(&right),
        (Float(left), Float(right)) => {
            let (left, right) = (f64::from_bits(left), f64::from_bits(right));
            if left == 0.0 && right == 0.0 {
                Ordering::Equal
            } else {
                left.total_cmp(&right)
            }
        }
        (Boolean(left), Boolean(right)) => left.cmp(&right),
        (Utf8(left), Utf8(right)) | (Binary(left), Binary(right)) => {
            left.as_slice().cmp(right.as_slice())
        }
        (Decimal(left, left_dtype), Decimal(right, right_dtype)) if left_dtype == right_dtype => {
            left.cmp(&right)
        }
        (Date(left), Date(right)) => left.cmp(&right),
        _ => {
            return Err(failed(
                "incompatible relational key types require an explicit cast",
            ));
        }
    })
}

fn numeric_owner(array: &ArrayRef, context: &mut ExecutionCtx) -> Result<NativeNumericOwner> {
    let values = array
        .clone()
        .execute::<PrimitiveArray>(context)
        .map_err(vortex_error)?;
    if values.dtype() != array.dtype() || values.len() != array.len() {
        return Err(failed("numeric execution changed dtype or row count"));
    }
    NativeNumericOwner::new(values, context)
}

fn temporal_value(values: &NativeNumericOwner, row: usize) -> Result<Option<i64>> {
    match values.distinct_value(row)? {
        AggregateDistinctValue::Null => Ok(None),
        AggregateDistinctValue::Int64(value) => Ok(Some(value)),
        _ => Err(failed("temporal storage returned a non-signed integer key")),
    }
}

impl DecimalColumn {
    fn new(array: &ArrayRef, dtype: DecimalDType, context: &mut ExecutionCtx) -> Result<Self> {
        let values = array
            .clone()
            .execute::<DecimalArray>(context)
            .map_err(vortex_error)?;
        if values.dtype() != array.dtype() || values.len() != array.len() {
            return Err(failed("decimal execution changed dtype or row count"));
        }
        let valid = values
            .validity()
            .map_err(vortex_error)?
            .execute_mask(values.len(), context)
            .map_err(vortex_error)?;
        Ok(Self {
            values,
            valid,
            dtype,
        })
    }

    fn cell(&self, row: usize) -> Result<Cell> {
        if !self.valid.value(row) {
            return Ok(Cell::Null);
        }
        let value = vortex::array::match_each_decimal_value_type!(self.values.values_type(), |D| {
            DecimalValue::from(self.values.buffer::<D>()[row]).cast::<i128>()
        })
        .ok_or_else(|| failed("decimal key exceeds signed 128-bit storage"))?;
        if value.unsigned_abs() >= 10_u128.pow(u32::from(self.dtype.precision())) {
            return Err(failed("decimal key exceeds its declared precision"));
        }
        Ok(Cell::Decimal(value, self.dtype))
    }
}

impl VariableColumn {
    fn new(
        array: &ArrayRef,
        context: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let binary = matches!(array.dtype(), DType::Binary(_));
        let dictionary = array.as_opt::<Dict>();
        let domain = dictionary
            .as_ref()
            .map_or_else(|| array.clone(), |dict| dict.values().clone());
        let bytes = domain
            .len()
            .checked_mul(size_of::<u64>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("text hash capacity overflow"))?;
        let ownership = memory.reserve(bytes)?;
        let values = domain
            .clone()
            .execute::<VarBinViewArray>(context)
            .map_err(vortex_error)?;
        if values.dtype() != domain.dtype() || values.len() != domain.len() {
            return Err(failed(
                "text execution changed dtype or value-domain length",
            ));
        }
        let valid = values
            .varbinview_validity()
            .execute_mask(values.len(), context)
            .map_err(vortex_error)?;
        let codes = dictionary
            .map(|dict| {
                let source = dict.codes();
                let codes = source
                    .clone()
                    .execute::<PrimitiveArray>(context)
                    .map_err(vortex_error)?;
                if codes.dtype() != source.dtype() || codes.len() != array.len() {
                    return Err(failed(
                        "dictionary code execution changed dtype or row count",
                    ));
                }
                let owner = NativeNumericOwner::new(codes, context)?;
                if !owner.is_integer() {
                    return Err(failed("dictionary codes must be integers"));
                }
                Ok(owner)
            })
            .transpose()?;
        let mut hashes = Vec::new();
        hashes
            .try_reserve_exact(values.len())
            .map_err(vortex_error)?;
        if hashes.capacity() > values.len() {
            return Err(failed("text hash allocation exceeded its reservation"));
        }
        for index in 0..values.len() {
            if index.is_multiple_of(4096) {
                cancellation.check()?;
            }
            hashes.push(if valid.value(index) {
                let value = values.bytes_at(index);
                if !binary {
                    std::str::from_utf8(value.as_slice()).map_err(vortex_error)?;
                }
                compound_count_partial::string_hash(value.as_slice())
            } else {
                0
            });
        }
        Ok(Self {
            values,
            valid,
            codes,
            hashes,
            _hash_ownership: ownership,
            rows: array.len(),
            binary,
        })
    }

    fn index(&self, row: usize) -> Result<Option<usize>> {
        let index = match &self.codes {
            None => row,
            Some(codes) => match codes.distinct_value(row)? {
                AggregateDistinctValue::Null => return Ok(None),
                AggregateDistinctValue::UInt64(value) => {
                    usize::try_from(value).map_err(vortex_error)?
                }
                AggregateDistinctValue::Int64(value) => {
                    usize::try_from(value).map_err(vortex_error)?
                }
                _ => return Err(failed("dictionary code is not an integer")),
            },
        };
        if index >= self.values.len() {
            return Err(failed("dictionary code exceeds its value domain"));
        }
        Ok(self.valid.value(index).then_some(index))
    }
}

fn normalize_numeric(value: &AggregateDistinctValue) -> Result<Cell> {
    Ok(match value {
        AggregateDistinctValue::Null => Cell::Null,
        AggregateDistinctValue::Int64(value) if *value < 0 => Cell::NegativeInteger(*value),
        AggregateDistinctValue::Int64(value) => Cell::NonnegativeInteger(value.cast_unsigned()),
        AggregateDistinctValue::UInt64(value) => Cell::NonnegativeInteger(*value),
        AggregateDistinctValue::Float64Bits(bits) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() {
                return Err(failed("nonfinite floating join keys are not admitted"));
            }
            Cell::Float(if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                *bits
            })
        }
        _ => return Err(failed("numeric owner returned a nonnumeric key")),
    })
}

fn failed(detail: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native relational key: {detail}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::{
        IntoArray as _, VortexSessionExecute as _, arrays::DictArray, validity::Validity,
    };

    #[allow(clippy::needless_pass_by_value)] // Test factory relinquishes its temporary owner.
    fn column(array: ArrayRef, memory: &LiveMemoryPool) -> KeyColumn {
        KeyColumn::new(
            &array,
            &mut vortex::array::legacy_session().create_execution_ctx(),
            memory,
            &CancellationToken::default(),
        )
        .unwrap()
    }

    fn hash(column: &KeyColumn, row: usize) -> (bool, u64) {
        let mut hash = rustc_hash::FxHasher::default();
        let present = column.hash_into(row, &mut hash).unwrap();
        (present, hash.finish())
    }

    #[test]
    fn native_integer_equality_is_exact_across_widths_and_signedness() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let signed = column(
            PrimitiveArray::new(vec![-1_i64, 0, i64::MAX], Validity::NonNullable).into_array(),
            &memory,
        );
        let unsigned = column(
            PrimitiveArray::new(
                vec![u64::MAX, 0, i64::MAX.cast_unsigned()],
                Validity::NonNullable,
            )
            .into_array(),
            &memory,
        );
        let narrow = column(
            PrimitiveArray::new(vec![0_u8], Validity::NonNullable).into_array(),
            &memory,
        );
        assert_ne!(signed.cell(0).unwrap(), unsigned.cell(0).unwrap());
        for row in 1..3 {
            assert_eq!(signed.cell(row).unwrap(), unsigned.cell(row).unwrap());
            assert_eq!(hash(&signed, row), hash(&unsigned, row));
        }
        assert_eq!(hash(&narrow, 0), hash(&signed, 1));
        assert!(signed.cell(3).is_err());
    }

    #[test]
    fn float_zero_is_canonical_without_coercing_integer_keys_or_accepting_nonfinite() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let floats = column(
            PrimitiveArray::new(
                vec![-0.0_f64, 0.0, f64::NAN, f64::INFINITY],
                Validity::NonNullable,
            )
            .into_array(),
            &memory,
        );
        let integers = column(
            PrimitiveArray::new(vec![0_i32], Validity::NonNullable).into_array(),
            &memory,
        );
        assert_eq!(floats.cell(0).unwrap(), floats.cell(1).unwrap());
        assert_eq!(hash(&floats, 0), hash(&floats, 1));
        assert_ne!(floats.cell(0).unwrap(), integers.cell(0).unwrap());
        assert!(floats.cell(2).is_err());
        assert!(floats.cell(3).is_err());
        let boolean = column(BoolArray::from_iter([false, true]).into_array(), &memory);
        assert_eq!(boolean.cell(1).unwrap(), Cell::Boolean(true));
        assert_ne!(boolean.cell(0).unwrap(), integers.cell(0).unwrap());
    }

    #[test]
    fn dictionaries_compare_values_across_domains_and_preserve_nulls_and_owners() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let first = DictArray::try_new(
            PrimitiveArray::new(vec![0_u8, 1, 0, 2], Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_nullable_str([Some("東京\0"), Some(""), None]).into_array(),
        )
        .unwrap()
        .into_array();
        let second = DictArray::try_new(
            PrimitiveArray::new(vec![1_u16, 0, 1, 2], Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_nullable_str([Some(""), Some("東京\0"), None]).into_array(),
        )
        .unwrap()
        .into_array();
        let left = column(first.clone(), &memory);
        let right = column(second.clone(), &memory);
        drop((first, second));
        for row in 0..4 {
            assert_eq!(left.cell(row).unwrap(), right.cell(row).unwrap());
            assert_eq!(hash(&left, row), hash(&right, row));
        }
        assert_eq!(left.cell(3).unwrap(), Cell::Null);
        assert!(!hash(&left, 3).0);
        assert_eq!(memory.snapshot().reserved_bytes, 6 * 8);
        drop((left, right));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn hash_capacity_and_cancellation_are_admitted_before_native_text_work() {
        let source = VarBinViewArray::from_iter_str(["a", "b"]).into_array();
        let memory = LiveMemoryPool::new(15).unwrap();
        let token = CancellationToken::default();
        let mut context = vortex::array::legacy_session().create_execution_ctx();
        assert!(KeyColumn::new(&source, &mut context, &memory, &token).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        token.cancel();
        let memory = LiveMemoryPool::new(4096).unwrap();
        assert!(KeyColumn::new(&source, &mut context, &memory, &token).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn matching_and_ordering_distinguish_null_policy_and_lossless_numeric_domains() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let left = column(
            PrimitiveArray::from_option_iter([None, Some(-1_i64), Some(0), Some(i64::MAX)])
                .into_array(),
            &memory,
        );
        let right = column(
            PrimitiveArray::from_option_iter([
                None,
                Some(u64::MAX),
                Some(0),
                Some(i64::MAX.cast_unsigned()),
            ])
            .into_array(),
            &memory,
        );
        assert!(!left.equals_at(0, &right, 0, false).unwrap());
        assert!(left.equals_at(0, &right, 0, true).unwrap());
        assert_eq!(left.compare_at(1, &right, 1).unwrap(), Ordering::Less);
        assert!(left.equals_at(2, &right, 2, false).unwrap());
        assert!(left.equals_at(3, &right, 3, false).unwrap());
        assert_eq!(right.compare_at(1, &left, 3).unwrap(), Ordering::Greater);
        let floats = column(
            PrimitiveArray::new(vec![-0.0_f64, 0.0, 1.0], Validity::NonNullable).into_array(),
            &memory,
        );
        assert!(floats.equals_at(0, &floats, 1, false).unwrap());
        assert_eq!(floats.compare_at(1, &floats, 2).unwrap(), Ordering::Less);
        assert!(left.compare_at(2, &floats, 1).is_err());
        assert!(left.compare_at(4, &right, 0).is_err());
        let text = column(
            VarBinViewArray::from_iter_nullable_str([None, Some(""), Some("東京\0"), Some("é")])
                .into_array(),
            &memory,
        );
        assert!(text.equals_at(0, &text, 0, true).unwrap());
        assert!(!text.equals_at(0, &text, 1, true).unwrap());
        assert_eq!(text.compare_at(1, &text, 2).unwrap(), Ordering::Less);
        assert_eq!(text.compare_at(2, &text, 3).unwrap(), Ordering::Greater);
    }
}
