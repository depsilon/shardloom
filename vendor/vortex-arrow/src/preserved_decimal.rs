// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Dylan Justin Heinrich

//! Opt-in import after the consumer's ancestor-aware decimal validation.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type};
use arrow_schema::DataType;
use vortex_array::arrays::DecimalArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::{DecimalDType, DecimalType};
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_error::{VortexResult, vortex_bail};

pub(crate) fn from_arrow(array: &dyn Array, nullable: bool) -> VortexResult<ArrayRef> {
    macro_rules! preserve {
        ($arrow_type:ty, $physical:ident) => {{
            let decimal = array.as_primitive::<$arrow_type>();
            let dtype = DecimalDType::try_new(decimal.precision(), decimal.scale())?;
            let validity = crate::convert::nulls(decimal.nulls(), nullable)?;
            // Byte ownership preserves the sliced Arrow allocation, including
            // Decimal256, without casting between native integer types.
            let values = Buffer::from_arrow_scalar_buffer(decimal.values().clone());
            DecimalArray::try_new_handle(
                BufferHandle::new_host(values.into_byte_buffer()),
                DecimalType::$physical,
                dtype,
                validity,
            )
            .map(IntoArray::into_array)
        }};
    }
    match array.data_type() {
        DataType::Decimal32(..) => preserve!(Decimal32Type, I32),
        DataType::Decimal64(..) => preserve!(Decimal64Type, I64),
        DataType::Decimal128(..) => preserve!(Decimal128Type, I128),
        DataType::Decimal256(..) => preserve!(Decimal256Type, I256),
        dtype => vortex_bail!("Expected Arrow decimal storage, got {dtype}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array as _, ArrayRef as ArrowArrayRef, Decimal128Array, StructArray};
    use arrow_schema::{DataType, Field};
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{Decimal, Struct};
    use vortex_array::dtype::DecimalType;
    use vortex_session::VortexSession;

    use crate::ArrowSession;

    macro_rules! check {
        ($name:ident, $array:ty, $value:expr, $physical:ident) => {
            #[test]
            fn $name() {
                let input = <$array>::from(vec![Some($value), None, Some($value)])
                    .with_precision_and_scale(2, 0)
                    .unwrap()
                    .slice(1, 2);
                let data = input.to_data();
                let pointer = input.values().as_ptr().cast::<u8>();
                let input: ArrowArrayRef = Arc::new(input);
                let original = ArrowSession::default();
                let scoped = original.with_preserved_decimal_storage();
                assert!(original.from_arrow_array(Arc::clone(&input), true).is_err());
                let result = scoped.from_arrow_array(Arc::clone(&input), true).unwrap();
                let decimal = result.as_::<Decimal>();
                assert_eq!(result.len(), 2);
                assert_eq!(decimal.values_type(), DecimalType::$physical);
                assert_eq!(decimal.buffer_handle().as_host().as_ptr(), pointer);
                assert_eq!(
                    decimal.buffer_handle().as_host().as_slice(),
                    data.buffers()[0].as_slice()
                );
                let mut ctx = VortexSession::empty().create_execution_ctx();
                let validity = result.validity().unwrap();
                assert!(!validity.execute_is_valid(0, &mut ctx).unwrap());
                assert!(validity.execute_is_valid(1, &mut ctx).unwrap());
                assert!(scoped.from_arrow_array(Arc::clone(&input), false).is_err());
                assert!(original.from_arrow_array(input, true).is_err());
                drop(data);
                assert_eq!(decimal.buffer_handle().as_host().as_ptr(), pointer);
            }
        };
    }

    check!(
        preserved_i32_slices_bits_and_nulls,
        arrow_array::Decimal32Array,
        123i32,
        I32
    );
    check!(
        preserved_i64_slices_bits_and_nulls,
        arrow_array::Decimal64Array,
        123i64,
        I64
    );
    check!(
        preserved_i128_slices_bits_and_nulls,
        Decimal128Array,
        123i128,
        I128
    );
    check!(
        preserved_i256_slices_bits_and_nulls,
        arrow_array::Decimal256Array,
        arrow_buffer::i256::from_i128(123),
        I256
    );

    #[test]
    fn preserved_decimal_policy_recurses_without_mutating_default_session() {
        let values = Decimal128Array::from(vec![123i128, 9])
            .with_precision_and_scale(2, 0)
            .unwrap();
        let pointer = values.values().as_ptr().cast::<u8>();
        let input: ArrowArrayRef = Arc::new(StructArray::new(
            vec![Field::new("amount", DataType::Decimal128(2, 0), false)].into(),
            vec![Arc::new(values)],
            Some(vec![false, true].into()),
        ));
        let session = ArrowSession::default();
        assert!(session.from_arrow_array(Arc::clone(&input), true).is_err());
        let result = session
            .with_preserved_decimal_storage()
            .from_arrow_array(Arc::clone(&input), true)
            .unwrap();
        let structure = result.as_::<Struct>();
        let child = structure.unmasked_field(0);
        let decimal = child.as_::<Decimal>();
        assert_eq!(decimal.values_type(), DecimalType::I128);
        assert_eq!(decimal.buffer_handle().as_host().as_ptr(), pointer);
        assert_eq!(decimal.buffer::<i128>().as_slice(), &[123, 9]);
        assert!(session.from_arrow_array(input, true).is_err());
    }
}
