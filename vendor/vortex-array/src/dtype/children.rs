// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom extension: borrowed, fallible native schema continuations.

use std::iter::FusedIterator;

use vortex_error::VortexResult;

use super::DType;

/// An ordered, borrowed iterator over the immediate children of a dtype.
///
/// The iterator itself does not allocate or parse lazy field types. A child is
/// inspected only when yielded; skipping children does not inspect them. Lazy
/// parsing uses the dtype owner's existing session and fallible cache.
#[derive(Clone, Debug)]
pub struct DTypeChildren<'a> {
    dtype: &'a DType,
    next: usize,
    end: usize,
}

impl DType {
    /// Borrow immediate child dtypes in their native order.
    ///
    /// Struct fields and union variants retain declaration order; maps yield
    /// keys before values; lists and extensions yield their element or storage
    /// dtype. Leaf types have no children. No dtype is cloned. This is one-level
    /// inspection, so callers can admit their own iterative traversal workspace.
    pub fn try_child_dtypes(&self) -> DTypeChildren<'_> {
        let end = match self {
            Self::Struct(fields, _) => fields.nfields(),
            Self::Union(variants, _) => variants.len(),
            Self::Map(..) => 2,
            Self::List(..) | Self::FixedSizeList(..) | Self::Extension(_) => 1,
            Self::Null
            | Self::Bool(_)
            | Self::Primitive(..)
            | Self::Decimal(..)
            | Self::Utf8(_)
            | Self::Binary(_)
            | Self::Variant(_) => 0,
        };
        DTypeChildren {
            dtype: self,
            next: 0,
            end,
        }
    }
}

impl<'a> Iterator for DTypeChildren<'a> {
    type Item = VortexResult<&'a DType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let index = self.next;
        self.next += 1;
        match self.dtype {
            DType::Struct(fields, _) => fields.try_field_dtypes().nth(index),
            DType::Union(variants, _) => variants.try_variant_dtypes().nth(index),
            DType::Map(map, _) => Some(if index == 0 {
                map.try_key_dtype()
            } else {
                map.try_value_dtype()
            }),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => Some(Ok(child)),
            DType::Extension(extension) => Some(Ok(extension.storage_dtype())),
            // Leaf types construct an empty iterator.
            DType::Null
            | DType::Bool(_)
            | DType::Primitive(..)
            | DType::Decimal(..)
            | DType::Utf8(_)
            | DType::Binary(_)
            | DType::Variant(_) => None,
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.next += n.min(self.len());
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end - self.next;
        (len, Some(len))
    }
}

impl ExactSizeIterator for DTypeChildren<'_> {}
impl FusedIterator for DTypeChildren<'_> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::dtype::UnionVariants;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;

    #[test]
    fn native_children_preserve_order_and_borrow_storage() -> VortexResult<()> {
        let number = DType::Primitive(PType::I64, Nullability::NonNullable);
        let text = DType::Utf8(Nullability::Nullable);
        let map = DType::map(number.clone(), text.clone(), true, Nullability::Nullable)?;
        let union = DType::Union(
            UnionVariants::try_new(
                ["first", "second"].into(),
                vec![number.clone(), text.clone()],
                vec![7, 2],
            )?,
            Nullability::Nullable,
        );
        let structure = DType::Struct(
            StructFields::from_iter([("first", number.clone()), ("second", text.clone())]),
            Nullability::Nullable,
        );
        for dtype in [&map, &union, &structure] {
            let children = dtype.try_child_dtypes().collect::<VortexResult<Vec<_>>>()?;
            assert_eq!(children, [&number, &text]);
            let again = dtype.try_child_dtypes().collect::<VortexResult<Vec<_>>>()?;
            for (first, second) in children.into_iter().zip(again) {
                assert!(std::ptr::eq(first, second));
            }
        }
        let child = Arc::new(number.clone());
        for dtype in [
            DType::List(child.clone(), Nullability::Nullable),
            DType::FixedSizeList(child.clone(), 3, Nullability::NonNullable),
        ] {
            let references = Arc::strong_count(&child);
            assert!(std::ptr::eq(
                dtype.try_child_dtypes().next().unwrap()?,
                child.as_ref(),
            ));
            assert_eq!(Arc::strong_count(&child), references);
        }
        let extension = Timestamp::new(TimeUnit::Nanoseconds, Nullability::NonNullable).erased();
        let dtype = DType::Extension(extension.clone());
        assert!(std::ptr::eq(
            dtype.try_child_dtypes().next().unwrap()?,
            extension.storage_dtype(),
        ));
        Ok(())
    }

    #[test]
    fn children_skip_without_overflow_and_remain_exhausted() -> VortexResult<()> {
        let dtype = DType::Struct(
            StructFields::from_iter([
                ("a", DType::Null),
                ("b", DType::Bool(Nullability::Nullable)),
                ("c", DType::Utf8(Nullability::NonNullable)),
            ]),
            Nullability::Nullable,
        );
        let mut children = dtype.try_child_dtypes();
        assert_eq!(children.size_hint(), (3, Some(3)));
        assert_eq!(
            children.nth(1).unwrap()?,
            &DType::Bool(Nullability::Nullable)
        );
        assert_eq!(children.len(), 1);
        assert_eq!(
            children.next().unwrap()?,
            &DType::Utf8(Nullability::NonNullable)
        );
        assert!(children.next().is_none());
        assert!(children.nth(usize::MAX).is_none());
        assert_eq!(children.len(), 0);
        let mut children = dtype.try_child_dtypes();
        assert!(children.nth(usize::MAX).is_none());
        assert!(children.next().is_none());
        for leaf in [
            DType::Null,
            DType::Bool(Nullability::Nullable),
            DType::Primitive(PType::F64, Nullability::NonNullable),
            DType::Utf8(Nullability::Nullable),
            DType::Binary(Nullability::Nullable),
            DType::Variant(Nullability::NonNullable),
        ] {
            assert_eq!(leaf.try_child_dtypes().len(), 0);
            assert!(leaf.try_child_dtypes().next().is_none());
        }
        Ok(())
    }
}
