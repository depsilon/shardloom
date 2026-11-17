// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar validation keeps logical values compatible with their storage representations.
//!
//! The fast check and detailed error path accept the same values. Extension values are checked
//! against the storage dtype before the extension's own validator runs.

use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::dtype::DType;
use crate::scalar::Scalar;
use crate::scalar::ScalarValue;

impl Scalar {
    /// Validate that the given [`ScalarValue`] is compatible with the given [`DType`].
    #[inline]
    pub fn validate(dtype: &DType, value: Option<&ScalarValue>) -> VortexResult<()> {
        if Self::is_compatible(dtype, value) {
            return Ok(());
        }

        Err(Self::incompatible_error(dtype, value))
    }

    /// Returns `true` if the given [`ScalarValue`] is compatible with the given [`DType`].
    ///
    /// This is the fast path of [`Self::validate`]. It must accept exactly the values that
    /// [`Self::validate_detailed`] accepts, but it builds no error messages.
    ///
    /// Null values and leaf types are checked inline, so that callers with a known dtype fold the
    /// check away. Other types go to [`Self::is_compatible_nested`].
    #[inline]
    pub(super) fn is_compatible(dtype: &DType, value: Option<&ScalarValue>) -> bool {
        let Some(value) = value else {
            return dtype.is_nullable();
        };

        match (dtype, value) {
            (DType::Bool(_), ScalarValue::Bool(_))
            | (DType::Utf8(_), ScalarValue::Utf8(_))
            | (DType::Binary(_), ScalarValue::Binary(_)) => true,
            (DType::Primitive(ptype, _), ScalarValue::Primitive(pvalue)) => {
                pvalue.ptype() == *ptype
            }
            _ => Self::is_compatible_nested(dtype, value),
        }
    }

    /// The out-of-line part of compatibility for nested non-null values.
    fn is_compatible_nested(dtype: &DType, value: &ScalarValue) -> bool {
        super::validate_walk::check::<false>(dtype, Some(value)).is_ok()
    }

    /// Builds the error for a [`ScalarValue`] that is not compatible with the [`DType`].
    ///
    /// Kept out of line so that the formatting code does not count against the hot callers when
    /// the inliner sizes them.
    #[cold]
    #[inline(never)]
    pub(super) fn incompatible_error(dtype: &DType, value: Option<&ScalarValue>) -> VortexError {
        match Self::validate_detailed(dtype, value) {
            Err(err) => err,
            Ok(()) => vortex_err!(
                "scalar validation fast path rejected value {value:?} for dtype {dtype}, but the \
                 detailed check accepted it"
            ),
        }
    }

    /// Validates the value with the original shallow messages and bounded deep context.
    fn validate_detailed(dtype: &DType, value: Option<&ScalarValue>) -> VortexResult<()> {
        super::validate_walk::check::<true>(dtype, value)
            .map_err(|error| error.vortex_expect("detailed validation always records its failure"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::DecimalDType;
    use crate::dtype::FieldNames;
    use crate::dtype::MapDType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::dtype::UnionVariants;
    use crate::extension::datetime::Date;
    use crate::extension::datetime::TimeUnit;
    use crate::scalar::DecimalValue;
    use crate::scalar::PValue;
    use crate::scalar::Scalar;
    use crate::scalar::ScalarValue;
    use crate::scalar::UnionValue;

    /// The fast check in `is_compatible` must accept exactly what `validate_detailed` accepts.
    #[test]
    fn fast_and_detailed_checks_agree() -> VortexResult<()> {
        use Nullability::NonNullable;
        use Nullability::Nullable;

        let i32_value = || Some(ScalarValue::Primitive(PValue::I32(1)));
        let i32_dtype = DType::Primitive(PType::I32, NonNullable);
        let struct_dtype = DType::Struct(
            StructFields::new(
                FieldNames::from(["a", "b"]),
                vec![i32_dtype.clone(), DType::Utf8(Nullable)],
            ),
            NonNullable,
        );
        let map_dtype = DType::Map(
            MapDType::try_new(i32_dtype.clone(), DType::Bool(Nullable), false)?,
            NonNullable,
        );
        let union_dtype = DType::Union(
            UnionVariants::try_new(["int"].into(), vec![i32_dtype.clone()], vec![3])?,
            NonNullable,
        );
        let list_dtype = DType::List(Arc::new(i32_dtype.clone()), NonNullable);
        let fsl_dtype = DType::FixedSizeList(Arc::new(i32_dtype.clone()), 2, NonNullable);
        let decimal_dtype = DType::Decimal(DecimalDType::new(3, 0), NonNullable);
        let extension_dtype = DType::Extension(Date::new(TimeUnit::Days, NonNullable).erased());
        let tuple = |values: Vec<Option<ScalarValue>>| Some(ScalarValue::Tuple(values));

        let cases = [
            (DType::Null, None, true),
            (DType::Null, Some(ScalarValue::Bool(true)), false),
            (DType::Bool(Nullable), None, true),
            (DType::Bool(NonNullable), None, false),
            (
                DType::Bool(NonNullable),
                Some(ScalarValue::Bool(true)),
                true,
            ),
            (DType::Bool(NonNullable), i32_value(), false),
            (i32_dtype.clone(), i32_value(), true),
            (
                i32_dtype,
                Some(ScalarValue::Primitive(PValue::I64(1))),
                false,
            ),
            (
                DType::Primitive(PType::F16, NonNullable),
                Some(ScalarValue::Primitive(PValue::U64(1))),
                false,
            ),
            (extension_dtype.clone(), i32_value(), true),
            (
                extension_dtype,
                Some(ScalarValue::Primitive(PValue::F64(1.0))),
                false,
            ),
            (
                decimal_dtype.clone(),
                Some(ScalarValue::Decimal(DecimalValue::I32(999))),
                true,
            ),
            (
                decimal_dtype,
                Some(ScalarValue::Decimal(DecimalValue::I32(1000))),
                false,
            ),
            (
                DType::Utf8(NonNullable),
                Some(ScalarValue::Utf8("a".into())),
                true,
            ),
            (
                DType::Utf8(NonNullable),
                Some(ScalarValue::Bool(true)),
                false,
            ),
            (
                DType::Binary(NonNullable),
                Some(ScalarValue::Utf8("a".into())),
                false,
            ),
            (
                list_dtype.clone(),
                tuple(vec![i32_value(), i32_value()]),
                true,
            ),
            (list_dtype.clone(), tuple(vec![i32_value(), None]), false),
            (list_dtype, i32_value(), false),
            (
                fsl_dtype.clone(),
                tuple(vec![i32_value(), i32_value()]),
                true,
            ),
            (fsl_dtype, tuple(vec![i32_value()]), false),
            (
                map_dtype.clone(),
                tuple(vec![tuple(vec![i32_value(), None])]),
                true,
            ),
            (map_dtype.clone(), tuple(vec![None]), false),
            (
                map_dtype.clone(),
                tuple(vec![tuple(vec![None, None])]),
                false,
            ),
            (map_dtype, tuple(vec![tuple(vec![i32_value()])]), false),
            (
                struct_dtype.clone(),
                tuple(vec![i32_value(), Some(ScalarValue::Utf8("a".into()))]),
                true,
            ),
            (struct_dtype.clone(), tuple(vec![i32_value(), None]), true),
            (struct_dtype.clone(), tuple(vec![None, None]), false),
            (struct_dtype, tuple(vec![i32_value()]), false),
            (
                union_dtype.clone(),
                Some(ScalarValue::Union(UnionValue::new(3, i32_value()))),
                true,
            ),
            (
                union_dtype.clone(),
                Some(ScalarValue::Union(UnionValue::new(4, i32_value()))),
                false,
            ),
            (
                union_dtype,
                Some(ScalarValue::Union(UnionValue::new(3, None))),
                false,
            ),
            (
                DType::Variant(NonNullable),
                Some(ScalarValue::Variant(Box::new(Scalar::null(DType::Null)))),
                true,
            ),
            (
                DType::Variant(NonNullable),
                Some(ScalarValue::Variant(Box::new(Scalar::null(DType::Bool(
                    Nullable,
                ))))),
                false,
            ),
            (
                DType::Variant(NonNullable),
                Some(ScalarValue::Variant(Box::new(Scalar::from(true)))),
                true,
            ),
        ];

        for (dtype, value, expected) in &cases {
            assert_eq!(
                Scalar::is_compatible(dtype, value.as_ref()),
                *expected,
                "unexpected validity for {dtype} and {value:?}"
            );
            assert_eq!(
                Scalar::is_compatible(dtype, value.as_ref()),
                Scalar::validate_detailed(dtype, value.as_ref()).is_ok(),
                "fast and detailed checks disagree for {dtype} and {value:?}",
            );
        }

        Ok(())
    }

    #[test]
    fn union_rejects_unknown_tag_and_wrong_value() -> VortexResult<()> {
        let variants = UnionVariants::try_new(
            ["int", "string"].into(),
            vec![
                DType::Primitive(PType::I32, Nullability::Nullable),
                DType::Utf8(Nullability::NonNullable),
            ],
            vec![5, 9],
        )?;
        let dtype = DType::Union(variants, Nullability::NonNullable);

        assert!(
            Scalar::try_new(
                dtype.clone(),
                Some(ScalarValue::Union(UnionValue::new(
                    7,
                    Scalar::primitive(42_i32, Nullability::Nullable).into_value(),
                ))),
            )
            .is_err()
        );

        assert!(
            Scalar::try_new(
                dtype,
                Some(ScalarValue::Union(UnionValue::new(
                    5,
                    Scalar::utf8("wrong", Nullability::NonNullable).into_value(),
                ))),
            )
            .is_err()
        );

        Ok(())
    }
}

#[cfg(test)]
#[path = "validate_walk_tests.rs"]
mod traversal_tests;
