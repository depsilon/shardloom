// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Original recursive validator from 2664207bd, retained as a shallow test reference.
//!
//! The fast check and detailed error path accept the same values. Extension values are checked
//! against the storage dtype before the extension's own validator runs.

use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;

use crate::dtype::DType;
use crate::dtype::MapDType;
use crate::dtype::UnionVariants;
use crate::scalar::ScalarValue;

pub(super) struct Original;

impl Original {
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

    /// The out-of-line part of [`Self::is_compatible`] for non-null values of all other types.
    fn is_compatible_nested(dtype: &DType, value: &ScalarValue) -> bool {
        match (dtype, value) {
            (DType::Decimal(dec_dtype, _), ScalarValue::Decimal(dvalue)) => {
                dvalue.fits_in_precision(*dec_dtype)
            }
            (DType::List(elem_dtype, _), ScalarValue::Tuple(elements)) => elements
                .iter()
                .all(|element| Self::is_compatible(elem_dtype, element.as_ref())),
            (DType::FixedSizeList(elem_dtype, size, _), ScalarValue::Tuple(elements)) => {
                elements.len() == *size as usize
                    && elements
                        .iter()
                        .all(|element| Self::is_compatible(elem_dtype, element.as_ref()))
            }
            (DType::Map(map, _), ScalarValue::Tuple(entries)) => {
                let key_dtype = map.key_dtype();
                let value_dtype = map.value_dtype();

                entries.iter().all(|entry| match entry {
                    Some(ScalarValue::Tuple(values)) => {
                        values.len() == 2
                            && Self::is_compatible(&key_dtype, values[0].as_ref())
                            && Self::is_compatible(&value_dtype, values[1].as_ref())
                    }
                    _ => false,
                })
            }
            (DType::Struct(fields, _), ScalarValue::Tuple(values)) => {
                values.len() == fields.nfields()
                    && fields
                        .field_dtypes()
                        .zip(values.iter())
                        .all(|(field, value)| Self::is_compatible(field, value.as_ref()))
            }
            (DType::Union(variants, _), ScalarValue::Union(union_value)) => variants
                .tag_to_child_index(union_value.type_id())
                .and_then(|child_index| variants.variant_by_index(child_index))
                .is_some_and(|child_dtype| {
                    Self::is_compatible(&child_dtype, union_value.child_value())
                }),
            (DType::Variant(_), ScalarValue::Variant(inner)) => {
                Self::is_compatible(inner.dtype(), inner.value())
                    && (!inner.is_null() || matches!(inner.dtype(), DType::Null))
            }
            (DType::Extension(ext_dtype), value) => {
                Self::is_compatible(ext_dtype.storage_dtype(), Some(value))
                    && ext_dtype.validate_storage_value(value).is_ok()
            }
            _ => false,
        }
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

    /// Validates the [`ScalarValue`] against the [`DType`], with a detailed error message.
    fn validate_detailed(dtype: &DType, value: Option<&ScalarValue>) -> VortexResult<()> {
        let Some(value) = value else {
            vortex_ensure!(
                dtype.is_nullable(),
                "non-nullable dtype {dtype} cannot hold a null value",
            );
            return Ok(());
        };

        // From here onwards, we know that the value is not null.
        match dtype {
            DType::Null => {
                vortex_bail!("null dtype cannot hold a non-null value {value}");
            }
            DType::Bool(_) => {
                vortex_ensure!(
                    matches!(value, ScalarValue::Bool(_)),
                    "bool dtype expected Bool value, got {value}",
                );
            }
            DType::Primitive(ptype, _) => {
                let ScalarValue::Primitive(pvalue) = value else {
                    vortex_bail!("primitive dtype {ptype} expected Primitive value, got {value}");
                };

                vortex_ensure!(
                    pvalue.ptype() == *ptype,
                    "primitive dtype {ptype} requires a matching value, got {pvalue}",
                );
            }
            DType::Utf8(_) => {
                vortex_ensure!(
                    matches!(value, ScalarValue::Utf8(_)),
                    "utf8 dtype expected Utf8 value, got {value}",
                );
            }
            DType::Binary(_) => {
                vortex_ensure!(
                    matches!(value, ScalarValue::Binary(_)),
                    "binary dtype expected Binary value, got {value}",
                );
            }
            DType::Decimal(dec_dtype, _) => {
                let ScalarValue::Decimal(dvalue) = value else {
                    vortex_bail!("decimal dtype expected Decimal value, got {value}");
                };

                vortex_ensure!(
                    dvalue.fits_in_precision(*dec_dtype),
                    "decimal value {dvalue} does not fit in precision of {dec_dtype}",
                );
            }
            DType::List(elem_dtype, _) => {
                let ScalarValue::Tuple(elements) = value else {
                    vortex_bail!("list dtype expected Tuple value, got {value}");
                };

                for (i, element) in elements.iter().enumerate() {
                    Self::validate_detailed(elem_dtype.as_ref(), element.as_ref())
                        .map_err(|e| e.with_context(format!("list element at index {i}")))?;
                }
            }
            DType::FixedSizeList(elem_dtype, size, _) => {
                let ScalarValue::Tuple(elements) = value else {
                    vortex_bail!("fixed-size list dtype expected Tuple value, got {value}",);
                };

                let len = elements.len();
                vortex_ensure_eq!(
                    len,
                    *size as usize,
                    "fixed-size list scalar has the wrong number of elements",
                );

                for (i, element) in elements.iter().enumerate() {
                    Self::validate_detailed(elem_dtype.as_ref(), element.as_ref()).map_err(
                        |e| e.with_context(format!("fixed-size list element at index {i}")),
                    )?;
                }
            }
            DType::Map(map, _) => Self::validate_map(map, value)?,
            DType::Struct(fields, _) => {
                let ScalarValue::Tuple(values) = value else {
                    vortex_bail!("struct dtype expected Tuple value, got {value}");
                };

                let nfields = fields.nfields();
                let nvalues = values.len();
                vortex_ensure_eq!(
                    nvalues,
                    nfields,
                    "struct scalar has the wrong number of fields",
                );

                for (field, field_value) in fields.field_dtypes().zip(values.iter()) {
                    Self::validate_detailed(field, field_value.as_ref())?;
                }
            }
            DType::Union(variants, _) => Self::validate_union(variants, value)?,
            DType::Variant(_) => {
                let ScalarValue::Variant(inner) = value else {
                    vortex_bail!("variant dtype expected Variant value, got {value}");
                };

                Self::validate_detailed(inner.dtype(), inner.value())?;
                vortex_ensure!(
                    !inner.is_null() || matches!(inner.dtype(), DType::Null),
                    "variant nulls must use a nested null scalar, got {}",
                    inner.dtype(),
                );
            }
            DType::Extension(ext_dtype) => {
                Self::validate_detailed(ext_dtype.storage_dtype(), Some(value))?;

                ext_dtype.validate_storage_value(value)?;
            }
        }

        Ok(())
    }

    /// Validates map entries and adds their positions to storage validation errors.
    fn validate_map(map: &MapDType, value: &ScalarValue) -> VortexResult<()> {
        let ScalarValue::Tuple(entries) = value else {
            vortex_bail!("map dtype expected Tuple value, got {value}");
        };
        let key_dtype = map.key_dtype();
        let value_dtype = map.value_dtype();

        for (index, entry) in entries.iter().enumerate() {
            let entry = entry
                .as_ref()
                .ok_or_else(|| vortex_err!("map entry at index {index} cannot be null"))?;
            let ScalarValue::Tuple(values) = entry else {
                vortex_bail!("map entry at index {index} expected Tuple value, got {entry}");
            };
            vortex_ensure_eq!(
                values.len(),
                2,
                "map entry at index {index} has the wrong number of values",
            );

            Self::validate_detailed(&key_dtype, values[0].as_ref())
                .map_err(|error| error.with_context(format!("map key at entry {index}")))?;
            Self::validate_detailed(&value_dtype, values[1].as_ref())
                .map_err(|error| error.with_context(format!("map value at entry {index}")))?;
        }

        Ok(())
    }

    /// Validates the selected union child and adds its type ID to storage validation errors.
    fn validate_union(variants: &UnionVariants, value: &ScalarValue) -> VortexResult<()> {
        let ScalarValue::Union(union_value) = value else {
            vortex_bail!("union dtype expected Union value, got {value}");
        };

        let type_id = union_value.type_id();
        let Some(child_index) = variants.tag_to_child_index(type_id) else {
            vortex_bail!(
                "union value has unknown type ID {type_id}; expected one of {:?}",
                variants.type_ids()
            );
        };

        let child_dtype = variants
            .variant_by_index(child_index)
            .vortex_expect("resolved union child index must be valid");

        Self::validate_detailed(&child_dtype, union_value.child_value()).map_err(|error| {
            error.with_context(format!(
                "union value for type ID {type_id} is invalid for dtype {child_dtype}"
            ))
        })
    }
}
