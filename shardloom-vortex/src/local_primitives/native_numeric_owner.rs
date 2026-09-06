//! Native owners with original-width numeric loops. Borrows never outlive the
//! `PrimitiveArray`, and native validity retains its own buffer owners. Filtering
//! and encoded validity may allocate in the admitted execution context; this
//! adapter does not copy or widen a column-sized numeric payload.

use shardloom_core::Result;
use vortex::array::{
    ExecutionCtx,
    arrays::{PrimitiveArray, primitive::PrimitiveArrayExt as _},
    dtype::{NativePType, PType},
};
use vortex::mask::Mask;

use super::{
    AGGREGATE_DIRECT_DISTINCT_DENSE_RANGE_LIMIT, AggregateDirectCountDistinctUpdate,
    AggregateDistinctSet, AggregateDistinctValue, AggregateIntegerKeyPart, AggregateValueTransform,
    ComparisonOp, StatValue, TypedFloatInList, TypedHashInList, coerce_compare_rhs_f64,
    coerce_compare_rhs_i64, coerce_compare_rhs_u64, coerce_in_list_rhs_f64, coerce_in_list_rhs_i64,
    coerce_in_list_rhs_u64, comparison_op_matches_ordering, float_in_list_contains,
    native_numeric_accessor::failed, reserve_hash_set_capacity, vortex_error,
};

pub(super) struct NativeNumericOwner {
    primitive: PrimitiveArray,
    valid: Mask,
}

// Physical type dispatch occurs once around each typed loop. Narrow values are
// widened in registers only, with the same signedness and f32 -> f64 behavior
// as the previous Vec conversion.
macro_rules! numeric_dispatch_type {
    ($owner:expr, $method:ident, $all:literal $(, $arg:expr)*) => {
        match $owner.primitive.ptype() {
            PType::U8 => $owner.$method::<u8, $all>($($arg),*),
            PType::U16 => $owner.$method::<u16, $all>($($arg),*),
            PType::U32 => $owner.$method::<u32, $all>($($arg),*),
            PType::U64 => $owner.$method::<u64, $all>($($arg),*),
            PType::I8 => $owner.$method::<i8, $all>($($arg),*),
            PType::I16 => $owner.$method::<i16, $all>($($arg),*),
            PType::I32 => $owner.$method::<i32, $all>($($arg),*),
            PType::I64 => $owner.$method::<i64, $all>($($arg),*),
            PType::F32 => $owner.$method::<f32, $all>($($arg),*),
            PType::F64 => $owner.$method::<f64, $all>($($arg),*),
            PType::F16 => unreachable!("f16 is not admitted by the numeric owner"),
        }
    };
}

macro_rules! numeric_dispatch {
    ($owner:expr, $method:ident $(, $arg:expr)*) => {
        if $owner.all_valid() {
            numeric_dispatch_type!($owner, $method, true $(, $arg)*)
        } else {
            numeric_dispatch_type!($owner, $method, false $(, $arg)*)
        }
    };
}

impl NativeNumericOwner {
    pub(super) fn new(primitive: PrimitiveArray, ctx: &mut ExecutionCtx) -> Result<Self> {
        if primitive.ptype() == PType::F16 || !primitive.as_ref().is_host() {
            return Err(failed("owner requires a host primitive other than f16"));
        }
        let valid = primitive
            .validity()
            .map_err(vortex_error)?
            .execute_mask(primitive.len(), ctx)
            .map_err(vortex_error)?;
        if valid.len() != primitive.len() {
            return Err(failed("validity mask changed row count"));
        }
        Ok(Self { primitive, valid })
    }

    pub(super) fn len(&self) -> usize {
        self.primitive.len()
    }
    pub(super) fn all_valid(&self) -> bool {
        self.valid.all_true()
    }
    pub(super) fn is_integer(&self) -> bool {
        self.primitive.ptype().is_int()
    }
    pub(super) fn signed(&self) -> bool {
        self.primitive.ptype().is_signed_int()
    }
    pub(super) fn ptype(&self) -> PType {
        self.primitive.ptype()
    }
    #[cfg(test)]
    pub(super) fn primitive(&self) -> &PrimitiveArray {
        &self.primitive
    }
    pub(super) fn u64_values(&self) -> Option<&[u64]> {
        (self.all_valid() && self.ptype() == PType::U64).then(|| self.primitive.as_slice::<u64>())
    }
    pub(super) fn i64_values(&self) -> Option<&[i64]> {
        (self.all_valid() && self.ptype() == PType::I64).then(|| self.primitive.as_slice::<i64>())
    }
    pub(super) fn f64_values(&self) -> Option<&[f64]> {
        (self.all_valid() && self.ptype() == PType::F64).then(|| self.primitive.as_slice::<f64>())
    }
    pub(super) fn evidence_kind(&self) -> &'static str {
        match (self.ptype(), self.all_valid()) {
            (PType::U8 | PType::U16 | PType::U32 | PType::U64, true) => "direct_u64",
            (PType::U8 | PType::U16 | PType::U32 | PType::U64, false) => "direct_u64_nullable",
            (PType::I8 | PType::I16 | PType::I32 | PType::I64, true) => "direct_i64",
            (PType::I8 | PType::I16 | PType::I32 | PType::I64, false) => "direct_i64_nullable",
            (_, true) => "direct_f64",
            (_, false) => "direct_f64_nullable",
        }
    }
    fn value<T: Numeric, const ALL_VALID: bool>(&self, row: usize) -> Result<Option<T::Wide>> {
        let value = self
            .primitive
            .as_slice::<T>()
            .get(row)
            .ok_or_else(|| failed("native typed row index was out of bounds"))?;
        Ok((ALL_VALID || self.valid.value(row)).then(|| value.widen()))
    }
    fn stat_typed<T: Numeric, const ALL_VALID: bool>(&self, row: usize) -> Result<StatValue> {
        Ok(self
            .value::<T, ALL_VALID>(row)?
            .map_or(StatValue::Null, Wide::stat))
    }
    pub(super) fn stat_value(&self, row: usize) -> Result<StatValue> {
        numeric_dispatch!(self, stat_typed, row)
    }
    fn distinct_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        row: usize,
    ) -> Result<AggregateDistinctValue> {
        Ok(self
            .value::<T, ALL_VALID>(row)?
            .map_or(AggregateDistinctValue::Null, Wide::distinct))
    }
    pub(super) fn distinct_value(&self, row: usize) -> Result<AggregateDistinctValue> {
        numeric_dispatch!(self, distinct_typed, row)
    }
    fn numeric_typed<T: Numeric, const ALL_VALID: bool>(&self, row: usize) -> Result<Option<f64>> {
        Ok(self.value::<T, ALL_VALID>(row)?.map(Wide::numeric))
    }
    pub(super) fn numeric_value(&self, row: usize) -> Result<Option<f64>> {
        numeric_dispatch!(self, numeric_typed, row)
    }
    fn integer_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        row: usize,
    ) -> Result<AggregateIntegerKeyPart> {
        self.value::<T, ALL_VALID>(row)?
            .and_then(Wide::integer)
            .ok_or_else(|| failed("native integer key requires a non-null integer"))
    }
    pub(super) fn integer_key(&self, row: usize) -> Result<AggregateIntegerKeyPart> {
        numeric_dispatch!(self, integer_typed, row)
    }
    pub(super) fn transform_integer(
        &self,
        row: usize,
        transform: AggregateValueTransform,
    ) -> Result<AggregateDistinctValue> {
        let value = self.distinct_value(row)?;
        match (value, transform) {
            (AggregateDistinctValue::Null, AggregateValueTransform::DateTruncMinute)
                if self.is_integer() =>
            {
                Ok(AggregateDistinctValue::Null)
            }
            (AggregateDistinctValue::Null, transform) => Ok(AggregateDistinctValue::from(
                &transform.apply(&StatValue::Null)?,
            )),
            (AggregateDistinctValue::UInt64(value), AggregateValueTransform::ExtractMinute) => {
                Ok(AggregateDistinctValue::UInt64((value % 3600) / 60))
            }
            (AggregateDistinctValue::Int64(value), AggregateValueTransform::ExtractMinute) => {
                Ok(AggregateDistinctValue::Int64(value.rem_euclid(3600) / 60))
            }
            (AggregateDistinctValue::UInt64(value), AggregateValueTransform::DateTruncMinute) => {
                Ok(AggregateDistinctValue::UInt64((value / 60) * 60))
            }
            (AggregateDistinctValue::Int64(value), AggregateValueTransform::DateTruncMinute) => {
                Ok(AggregateDistinctValue::Int64(value.div_euclid(60) * 60))
            }
            (AggregateDistinctValue::UInt64(value), AggregateValueTransform::AddOffset(offset)) => {
                if offset >= 0 {
                    value
                        .checked_add(offset.cast_unsigned())
                        .map(AggregateDistinctValue::UInt64)
                        .ok_or_else(|| failed("direct uint64 offset key overflowed"))
                } else {
                    value
                        .checked_sub(offset.unsigned_abs())
                        .map(AggregateDistinctValue::UInt64)
                        .ok_or_else(|| failed("direct uint64 offset key underflowed"))
                }
            }
            (AggregateDistinctValue::Int64(value), AggregateValueTransform::AddOffset(offset)) => {
                value
                    .checked_add(offset)
                    .map(AggregateDistinctValue::Int64)
                    .ok_or_else(|| failed("direct int64 offset key overflowed"))
            }
            // The existing scalar transform contract also admits float offsets.
            (value, transform) => {
                let scalar = match value {
                    AggregateDistinctValue::Float64Bits(bits) => {
                        StatValue::Float64(f64::from_bits(bits))
                    }
                    _ => return Err(failed("unsupported native numeric transform")),
                };
                Ok(AggregateDistinctValue::from(&transform.apply(&scalar)?))
            }
        }
    }
    pub(super) fn non_null_count(&self, rows: Option<&[usize]>) -> Result<u64> {
        let count = if let Some(rows) = rows {
            let mut count = 0usize;
            for &row in rows {
                if row >= self.len() {
                    return Err(failed("native validity row index was out of bounds"));
                }
                count += usize::from(self.valid.value(row));
            }
            count
        } else {
            self.valid.true_count()
        };
        u64::try_from(count).map_err(|_| failed("native count overflow"))
    }
    pub(super) fn null_count(&self, want_null: bool) -> usize {
        if want_null {
            self.len() - self.valid.true_count()
        } else {
            self.valid.true_count()
        }
    }
    pub(super) fn null_rows(&self, want_null: bool) -> Vec<usize> {
        (0..self.len())
            .filter(|&row| self.valid.value(row) != want_null)
            .collect()
    }
    pub(super) fn sum_count(&self, rows: Option<&[usize]>) -> Result<(u64, f64)> {
        numeric_dispatch!(self, sum_count_typed, rows)
    }
    pub(super) fn update_distinct(
        &self,
        rows: Option<&[usize]>,
        distinct: &mut AggregateDistinctSet,
    ) -> Result<AggregateDirectCountDistinctUpdate> {
        numeric_dispatch!(self, update_distinct_typed, rows, distinct)
    }
    fn update_distinct_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        rows: Option<&[usize]>,
        distinct: &mut AggregateDistinctSet,
    ) -> Result<AggregateDirectCountDistinctUpdate> {
        // Preserve dense integer preunion for narrow physical inputs too. This
        // scans borrowed typed values; it does not widen or copy the payload.
        let non_null_rows = self.non_null_count(rows)?;
        let dense_integer_preunion_used =
            self.is_integer() && self.update_dense_typed::<T, ALL_VALID>(rows, distinct)?;
        if !dense_integer_preunion_used {
            let values = self.primitive.as_slice::<T>();
            visit_rows(values.len(), rows, |row| {
                if ALL_VALID || self.valid.value(row) {
                    distinct.insert(values[row].widen().distinct());
                }
                Ok(true)
            })?;
        }
        Ok(AggregateDirectCountDistinctUpdate {
            non_null_rows,
            dense_integer_preunion_used,
        })
    }
    fn update_dense_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        rows: Option<&[usize]>,
        distinct: &mut AggregateDistinctSet,
    ) -> Result<bool> {
        let values = self.primitive.as_slice::<T>();
        let mut range = None::<(i128, i128)>;
        let bounded = visit_rows(values.len(), rows, |row| {
            if !(ALL_VALID || self.valid.value(row)) {
                return Ok(true);
            }
            let value = values[row]
                .widen()
                .ordinal()
                .ok_or_else(|| failed("dense native key is not an integer"))?;
            let (min, max) = range.map_or((value, value), |(min, max)| {
                (min.min(value), max.max(value))
            });
            range = Some((min, max));
            Ok(max - min < i128::from(AGGREGATE_DIRECT_DISTINCT_DENSE_RANGE_LIMIT))
        })?;
        let Some((min, max)) = range else {
            return Ok(false);
        };
        if !bounded {
            return Ok(false);
        }
        let len =
            usize::try_from(max - min + 1).map_err(|_| failed("dense native range overflow"))?;
        let mut used = vec![false; len];
        visit_rows(values.len(), rows, |row| {
            if ALL_VALID || self.valid.value(row) {
                let value = values[row]
                    .widen()
                    .ordinal()
                    .ok_or_else(|| failed("dense native key is not an integer"))?;
                let offset = usize::try_from(value - min)
                    .map_err(|_| failed("dense native offset overflow"))?;
                *used
                    .get_mut(offset)
                    .ok_or_else(|| failed("dense native offset outside range"))? = true;
            }
            Ok(true)
        })?;
        reserve_hash_set_capacity(
            distinct,
            used.iter().filter(|&&used| used).count(),
            "dense native numeric distinct",
        )?;
        for (offset, used) in used.into_iter().enumerate() {
            if used {
                let ordinal = min
                    + i128::try_from(offset).map_err(|_| failed("dense native index overflow"))?;
                distinct.insert(if self.signed() {
                    AggregateDistinctValue::Int64(
                        i64::try_from(ordinal)
                            .map_err(|_| failed("dense native signed value overflow"))?,
                    )
                } else {
                    AggregateDistinctValue::UInt64(
                        u64::try_from(ordinal)
                            .map_err(|_| failed("dense native unsigned value overflow"))?,
                    )
                });
            }
        }
        Ok(true)
    }
    fn sum_count_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        rows: Option<&[usize]>,
    ) -> Result<(u64, f64)> {
        let values = self.primitive.as_slice::<T>();
        let mut count = 0u64;
        let mut sum = 0.0;
        let mut add = |row: usize| -> Result<()> {
            let value = values
                .get(row)
                .ok_or_else(|| failed("native sum row index was out of bounds"))?;
            if !(ALL_VALID || self.valid.value(row)) {
                return Ok(());
            }
            let value = value.widen().numeric();
            if !value.is_finite() {
                return Err(failed(
                    "direct fused numeric update encountered non-finite value",
                ));
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| failed("direct fused numeric count overflowed"))?;
            sum += value;
            if !sum.is_finite() {
                return Err(failed("direct fused numeric sum became non-finite"));
            }
            Ok(())
        };
        if let Some(rows) = rows {
            for &row in rows {
                add(row)?;
            }
        } else {
            for row in 0..values.len() {
                add(row)?;
            }
        }
        Ok((count, sum))
    }
    pub(super) fn compare_count(
        &self,
        column: &str,
        op: ComparisonOp,
        rhs: &StatValue,
    ) -> Result<usize> {
        numeric_dispatch!(self, compare_typed, column, op, rhs, None)
    }
    pub(super) fn compare_rows(
        &self,
        column: &str,
        op: ComparisonOp,
        rhs: &StatValue,
    ) -> Result<Vec<usize>> {
        let mut rows = Vec::new();
        numeric_dispatch!(self, compare_typed, column, op, rhs, Some(&mut rows))?;
        Ok(rows)
    }
    fn compare_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        column: &str,
        op: ComparisonOp,
        rhs: &StatValue,
        mut rows: Option<&mut Vec<usize>>,
    ) -> Result<usize> {
        let Some(rhs) = T::Wide::compare_rhs(self.valid.true_count(), column, rhs)? else {
            return Ok(0);
        };
        let mut count = 0;
        for (row, &value) in self.primitive.as_slice::<T>().iter().enumerate() {
            if !(ALL_VALID || self.valid.value(row)) {
                continue;
            }
            let order = value
                .widen()
                .partial_cmp(&rhs)
                .ok_or_else(|| failed("numeric comparison encountered unordered values"))?;
            if comparison_op_matches_ordering(op, order) {
                count += 1;
                if let Some(rows) = rows.as_deref_mut() {
                    rows.push(row);
                }
            }
        }
        Ok(count)
    }
    pub(super) fn in_count(&self, column: &str, rhs: &[StatValue], negated: bool) -> Result<usize> {
        numeric_dispatch!(self, in_typed, column, rhs, negated, None)
    }
    pub(super) fn in_rows(
        &self,
        column: &str,
        rhs: &[StatValue],
        negated: bool,
    ) -> Result<Vec<usize>> {
        let mut rows = Vec::new();
        numeric_dispatch!(self, in_typed, column, rhs, negated, Some(&mut rows))?;
        Ok(rows)
    }
    fn in_typed<T: Numeric, const ALL_VALID: bool>(
        &self,
        column: &str,
        rhs: &[StatValue],
        negated: bool,
        mut rows: Option<&mut Vec<usize>>,
    ) -> Result<usize> {
        let rhs = T::Wide::in_rhs(column, rhs)?;
        let mut count = 0;
        for (row, &value) in self.primitive.as_slice::<T>().iter().enumerate() {
            let matched = T::Wide::contains(
                &rhs,
                (ALL_VALID || self.valid.value(row)).then(|| value.widen()),
            );
            if matched != negated {
                count += 1;
                if let Some(rows) = rows.as_deref_mut() {
                    rows.push(row);
                }
            }
        }
        Ok(count)
    }
}

trait Numeric: NativePType {
    type Wide: Wide;
    fn widen(self) -> Self::Wide;
}
macro_rules! numeric_types {
    ($wide:ty; $($ty:ty),+) => { $(impl Numeric for $ty {
        type Wide = $wide;
        fn widen(self) -> Self::Wide { <$wide>::from(self) }
    })+ };
}
numeric_types!(u64; u8, u16, u32, u64);
numeric_types!(i64; i8, i16, i32, i64);
numeric_types!(f64; f32, f64);

trait Wide: Copy + PartialOrd {
    type List;
    fn stat(self) -> StatValue;
    fn distinct(self) -> AggregateDistinctValue;
    fn numeric(self) -> f64;
    fn integer(self) -> Option<AggregateIntegerKeyPart>;
    fn ordinal(self) -> Option<i128>;
    fn compare_rhs(len: usize, column: &str, rhs: &StatValue) -> Result<Option<Self>>;
    fn in_rhs(column: &str, rhs: &[StatValue]) -> Result<Self::List>;
    fn contains(list: &Self::List, value: Option<Self>) -> bool;
}
macro_rules! integer_wide {
    ($ty:ty, $variant:ident, $signed:expr, $bits:expr, $compare:ident, $list:ident) => {
        impl Wide for $ty {
            type List = TypedHashInList<Self>;
            fn stat(self) -> StatValue {
                StatValue::$variant(self)
            }
            fn distinct(self) -> AggregateDistinctValue {
                AggregateDistinctValue::$variant(self)
            }
            #[allow(clippy::cast_precision_loss)]
            fn numeric(self) -> f64 {
                self as f64
            }
            fn integer(self) -> Option<AggregateIntegerKeyPart> {
                Some(AggregateIntegerKeyPart {
                    bits: ($bits)(self),
                    signed: $signed,
                })
            }
            fn ordinal(self) -> Option<i128> {
                Some(i128::from(self))
            }
            fn compare_rhs(len: usize, column: &str, rhs: &StatValue) -> Result<Option<Self>> {
                $compare(len, None, column, rhs)
            }
            fn in_rhs(column: &str, rhs: &[StatValue]) -> Result<Self::List> {
                $list(column, rhs)
            }
            fn contains(list: &Self::List, value: Option<Self>) -> bool {
                value.map_or(list.null_matches, |value| list.values.contains(&value))
            }
        }
    };
}
integer_wide!(
    u64,
    UInt64,
    false,
    |value| value,
    coerce_compare_rhs_u64,
    coerce_in_list_rhs_u64
);
integer_wide!(
    i64,
    Int64,
    true,
    i64::cast_unsigned,
    coerce_compare_rhs_i64,
    coerce_in_list_rhs_i64
);
impl Wide for f64 {
    type List = TypedFloatInList;
    fn stat(self) -> StatValue {
        StatValue::Float64(self)
    }
    fn distinct(self) -> AggregateDistinctValue {
        AggregateDistinctValue::Float64Bits(self.to_bits())
    }
    fn numeric(self) -> f64 {
        self
    }
    fn integer(self) -> Option<AggregateIntegerKeyPart> {
        None
    }
    fn ordinal(self) -> Option<i128> {
        None
    }
    fn compare_rhs(len: usize, column: &str, rhs: &StatValue) -> Result<Option<Self>> {
        coerce_compare_rhs_f64(len, None, column, rhs)
    }
    fn in_rhs(column: &str, rhs: &[StatValue]) -> Result<Self::List> {
        coerce_in_list_rhs_f64(column, rhs)
    }
    fn contains(list: &Self::List, value: Option<Self>) -> bool {
        value.map_or(list.null_matches, |value| {
            float_in_list_contains(&list.values, value)
        })
    }
}

fn visit_rows(
    len: usize,
    rows: Option<&[usize]>,
    mut visit: impl FnMut(usize) -> Result<bool>,
) -> Result<bool> {
    if let Some(rows) = rows {
        for &row in rows {
            if row >= len {
                return Err(failed("native selected row index was out of bounds"));
            }
            if !visit(row)? {
                return Ok(false);
            }
        }
    } else {
        for row in 0..len {
            if !visit(row)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
