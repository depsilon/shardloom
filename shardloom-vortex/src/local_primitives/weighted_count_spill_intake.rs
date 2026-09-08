//! Source-native owners for weighted COUNT. Numeric and dictionary-code width
//! dispatch happens once per array; complete UTF8 values remain domain-bound.

use super::{
    AggregateIntegerKeyPart, NativeNumericOwner, vortex_error,
    weighted_count_spill_admission::failed,
};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{
        Dict, PrimitiveArray, VarBinViewArray, dict::DictArraySlotsExt as _,
        varbinview::VarBinViewArrayExt as _,
    },
    validity::Validity,
};

pub(super) struct TextInput {
    values: VarBinViewArray,
    codes: Option<NativeNumericOwner>,
    rows: usize,
}
impl TextInput {
    pub(super) fn new(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<Self> {
        let array = super::encoded_numeric_reduction::resolve_structural_projection(array, ctx)?;
        let dictionary = array.as_opt::<Dict>();
        let values = dictionary
            .map_or_else(|| array.clone(), |dict| dict.values().clone())
            .execute::<VarBinViewArray>(ctx)
            .map_err(vortex_error)?;
        if !matches!(
            values.varbinview_validity(),
            Validity::NonNullable | Validity::AllValid
        ) {
            return Err(failed("native UTF8 value domain contains nulls"));
        }
        let codes = dictionary
            .map(|dict| {
                let primitive = dict
                    .codes()
                    .clone()
                    .execute::<PrimitiveArray>(ctx)
                    .map_err(vortex_error)?;
                NativeNumericOwner::new(primitive, ctx)
            })
            .transpose()?;
        if codes
            .as_ref()
            .is_some_and(|codes| codes.len() != array.len())
            || codes.is_none() && values.len() != array.len()
        {
            return Err(failed("native UTF8 execution changed source row count"));
        }
        Ok(Self {
            values,
            codes,
            rows: array.len(),
        })
    }
    pub(super) fn dictionary(&self) -> bool {
        self.codes.is_some()
    }
    pub(super) fn native_value_bytes(&self) -> u64 {
        self.values.nbytes()
    }
    pub(super) fn visit(
        &self,
        numeric: Option<&NativeNumericOwner>,
        mut visit: impl FnMut(usize, Option<AggregateIntegerKeyPart>, &str) -> Result<()>,
    ) -> Result<()> {
        if numeric.is_some_and(|numeric| numeric.len() != self.rows) {
            return Err(failed("native integer/UTF8 row counts differ"));
        }
        let numbers = numeric
            .map(|numeric| {
                numeric
                    .integer_key_slice()
                    .ok_or_else(|| failed("native integer owner lost all-valid admission"))
            })
            .transpose()?;
        let codes = self
            .codes
            .as_ref()
            .map(|codes| {
                codes
                    .integer_key_slice()
                    .ok_or_else(|| failed("dictionary code is not an all-valid integer"))
            })
            .transpose()?;
        let mut value = |row, key, index| {
            if index >= self.values.len() {
                return Err(failed("dictionary code exceeds its retained value domain"));
            }
            let bytes = self.values.bytes_at(index);
            visit(
                row,
                key,
                std::str::from_utf8(bytes.as_slice())
                    .map_err(|_| failed("native UTF8 bytes are invalid"))?,
            )
        };
        match (numbers, codes) {
            (Some(numbers), Some(codes)) => numbers.for_each_pair(codes, None, |row, pair| {
                let key = AggregateIntegerKeyPart {
                    bits: pair.first_bits,
                    signed: pair.key_kinds & 1 != 0,
                };
                let code = AggregateIntegerKeyPart {
                    bits: pair.second_bits,
                    signed: pair.key_kinds & 2 != 0,
                };
                value(row, Some(key), code_index(code)?)
            }),
            (Some(numbers), None) => numbers.for_each(None, |row, key| value(row, Some(key), row)),
            (None, Some(codes)) => {
                codes.for_each(None, |row, code| value(row, None, code_index(code)?))
            }
            (None, None) => {
                for row in 0..self.rows {
                    value(row, None, row)?;
                }
                Ok(())
            }
        }
    }
}
fn code_index(key: AggregateIntegerKeyPart) -> Result<usize> {
    if key.signed {
        usize::try_from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
            .map_err(|_| failed("dictionary code is negative or exceeds address space"))
    } else {
        usize::try_from(key.bits).map_err(|_| failed("dictionary code exceeds address space"))
    }
}
