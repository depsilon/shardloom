//! Grouping grammar resolves through ordinary SQL aliases into native metadata.

use super::{ColumnRef, Lowered, NativeResult, ParsedSqlLocalSource};
use shardloom_vortex::relational_query::{
    VortexRelationalGroupingIndicator, VortexRelationalGroupingSets,
};

pub(super) fn declaration(
    parsed: &ParsedSqlLocalSource,
    input: &Lowered,
) -> NativeResult<Option<VortexRelationalGroupingSets>> {
    parsed
        .grouping_sets
        .as_ref()
        .map(|grouping| {
            let columns = |names: &[String]| {
                names
                    .iter()
                    .map(|name| ColumnRef::new(input.resolve(name)?))
                    .collect::<NativeResult<Vec<_>>>()
            };
            Ok(VortexRelationalGroupingSets {
                sets: grouping
                    .expand(&parsed.group_by)?
                    .iter()
                    .map(|set| columns(set))
                    .collect::<NativeResult<_>>()?,
                indicators: grouping
                    .indicators
                    .iter()
                    .map(|indicator| {
                        Ok(VortexRelationalGroupingIndicator {
                            columns: columns(&indicator.columns)?,
                            alias: indicator.alias.clone(),
                        })
                    })
                    .collect::<NativeResult<_>>()?,
            })
        })
        .transpose()
}
