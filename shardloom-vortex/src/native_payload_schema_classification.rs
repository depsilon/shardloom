//! Presence classification over the existing admitted native schema cursor.

use super::{Budget, DType, Result, Scope, provider_error, walk};
use shardloom_core::ShardLoomError;
use vortex::error::VortexError;

const NODE_BYTES: u16 = 1024;

/// Search only the payload containers used by the original classifiers.
/// Extension storage, maps and unions are leaves at this semantic boundary.
pub(crate) fn contains_type(
    dtype: &DType,
    scope: Scope<'_>,
    matches: impl Fn(&DType) -> bool,
) -> Result<bool> {
    contains_mapped(dtype, scope, matches, |error| provider_error(&error))
}

pub(crate) fn contains_mapped(
    dtype: &DType,
    scope: Scope<'_>,
    matches: impl Fn(&DType) -> bool,
    map_provider: impl Fn(VortexError) -> ShardLoomError,
) -> Result<bool> {
    let mut budget = Budget::new(scope)?;
    let mut capacity = scope.memory.reserve(0)?;
    search(
        dtype,
        |dtype| {
            budget.add(u64::from(NODE_BYTES))?;
            Ok(matches(dtype))
        },
        |frames| walk::reserve_frame(frames, Some(&mut capacity)),
        map_provider,
    )
}

fn search<'a, E>(
    dtype: &'a DType,
    mut inspect: impl FnMut(&DType) -> std::result::Result<bool, E>,
    mut grow: impl FnMut(&mut Vec<walk::Frame<'a>>) -> std::result::Result<(), E>,
    map_provider: impl Fn(VortexError) -> E,
) -> std::result::Result<bool, E> {
    let mut pending = walk::Cursor::default();
    let mut next = Some(dtype);
    while let Some(dtype) = next.take() {
        if inspect(dtype)? {
            return Ok(true);
        }
        if matches!(
            dtype,
            DType::Struct(..) | DType::List(..) | DType::FixedSizeList(..)
        ) {
            next = pending
                .enter(dtype, 0, &mut grow, &map_provider)?
                .map(|(dtype, _)| dtype);
        }
        if next.is_none() {
            next = pending
                .next()
                .map_err(&map_provider)?
                .map(|(dtype, _)| dtype);
        }
    }
    Ok(false)
}

#[cfg(feature = "native-binary-objects")]
#[path = "native_payload_schema_host_search.rs"]
pub(super) mod host;

#[cfg(test)]
#[path = "native_payload_schema_classification_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "native_payload_schema_classification_tests.rs"]
mod tests;
