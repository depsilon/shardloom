//! Presence classification over the existing admitted native schema cursor.

use super::{Budget, DType, Result, Scope, walk};

/// Search only the payload containers used by the original classifiers.
/// Extension storage, maps and unions are leaves at this semantic boundary.
pub(crate) fn contains_type(
    dtype: &DType,
    scope: Scope<'_>,
    matches: impl Fn(&DType) -> bool,
) -> Result<bool> {
    let mut budget = Budget::new(scope)?;
    let mut pending = walk::Pending::new(budget.credit.pool())?;
    let mut next = Some(dtype);
    while let Some(dtype) = next.take() {
        // Bound repeated shared subtrees by the same operation grant. This is
        // visited work, not another retained schema or an independent pool.
        budget.add(1024)?;
        if matches(dtype) {
            return Ok(true);
        }
        if matches!(
            dtype,
            DType::Struct(..) | DType::List(..) | DType::FixedSizeList(..)
        ) {
            next = pending.enter(dtype, 0)?.map(|(dtype, _)| dtype);
        }
        if next.is_none() {
            next = pending.next()?.map(|(dtype, _)| dtype);
        }
    }
    Ok(false)
}

#[cfg(test)]
#[path = "native_payload_schema_classification_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_payload_schema_classification_tests.rs"]
mod tests;
