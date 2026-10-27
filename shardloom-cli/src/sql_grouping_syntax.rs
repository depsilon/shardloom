//! Compact grouping grammar; exponential expansion occurs only after admission.

use super::{
    ParsedProjectionList, ShardLoomError, matching_closing_parenthesis, parse_group_by_list,
    split_sql_csv, starts_with_keyword, unsupported_sql_error,
};

#[path = "sql_grouping_indicators.rs"]
mod indicators;
pub(super) use indicators::Rewrite;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Indicator {
    pub(super) columns: Vec<String>,
    pub(super) alias: String,
}

#[derive(Debug, Clone, PartialEq)]
enum Item {
    Keys(Vec<String>),
    Product(Vec<Self>),
    Sets(Vec<Self>),
    Rollup(Vec<Vec<String>>),
    Cube(Vec<Vec<String>>),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Grouping {
    item: Item,
    distinct: bool,
    pub(super) indicators: Vec<Indicator>,
}

type Result<T> = std::result::Result<T, ShardLoomError>;

pub(super) fn parse(
    raw: Option<&str>,
    projections: &ParsedProjectionList,
    mut indicators: Vec<Indicator>,
) -> Result<(Vec<String>, Option<Grouping>)> {
    let Some(raw) = raw else {
        if !indicators.is_empty() {
            return Err(unsupported_sql_error("GROUPING requires a GROUP BY clause"));
        }
        return Ok((vec![], None));
    };
    let raw = raw.trim();
    let distinct = starts_with_keyword(raw, "distinct");
    let all = starts_with_keyword(raw, "all");
    let raw = if distinct {
        raw[8..].trim()
    } else if all {
        raw[3..].trim()
    } else {
        raw
    };
    if raw.is_empty() {
        return Err(unsupported_sql_error("GROUP BY items must not be empty"));
    }
    let item = product(raw, projections)?;
    let mut columns = Vec::new();
    item.columns(&mut columns);
    for indicator in &mut indicators {
        for column in &mut indicator.columns {
            if column.parse::<usize>().is_ok() {
                return Err(unsupported_sql_error(
                    "GROUPING arguments must reference grouping expressions, not ordinals",
                ));
            }
            *column = parse_group_by_list(Some(column), projections)?.remove(0);
        }
    }
    if !distinct && indicators.is_empty() && item.plain() {
        return Ok((parse_group_by_list(Some(raw), projections)?, None));
    }
    let grouping = Grouping {
        item,
        distinct,
        indicators,
    };
    // Validate cardinality arithmetically without allocating an expanded tree.
    grouping.metadata_bytes()?;
    Ok((columns, Some(grouping)))
}

fn product(raw: &str, projections: &ParsedProjectionList) -> Result<Item> {
    let entries = split_sql_csv(raw)?;
    if entries.is_empty() {
        return Err(unsupported_sql_error(
            "grouping item list must not be empty",
        ));
    }
    Ok(Item::Product(
        entries
            .iter()
            .map(|raw| item(raw, projections))
            .collect::<Result<_>>()?,
    ))
}

fn contents<'a>(raw: &'a str, keyword: &str) -> Result<Option<&'a str>> {
    if !starts_with_keyword(raw, keyword) {
        return Ok(None);
    }
    let rest = raw[keyword.len()..].trim();
    if !rest.starts_with('(') || matching_closing_parenthesis(rest, 0)? != Some(rest.len() - 1) {
        return Err(unsupported_sql_error(
            "grouping operation requires one parenthesized item list",
        ));
    }
    Ok(Some(rest[1..rest.len() - 1].trim()))
}

fn unit(raw: &str, projections: &ParsedProjectionList) -> Result<Vec<String>> {
    let raw = raw.trim();
    let raw =
        if raw.starts_with('(') && matching_closing_parenthesis(raw, 0)? == Some(raw.len() - 1) {
            raw[1..raw.len() - 1].trim()
        } else {
            raw
        };
    if raw.is_empty() {
        return Ok(vec![]);
    }
    let mut keys = Vec::new();
    for key in split_sql_csv(raw)? {
        let name = parse_group_by_list(Some(&key), projections)?.remove(0);
        if !keys.contains(&name) {
            keys.push(name);
        }
    }
    Ok(keys)
}

fn item(raw: &str, projections: &ParsedProjectionList) -> Result<Item> {
    let raw = raw.trim();
    for (keyword, cube) in [("rollup", false), ("cube", true)] {
        if let Some(inner) = contents(raw, keyword)? {
            if inner.is_empty() {
                return Ok(if cube {
                    Item::Cube(vec![])
                } else {
                    Item::Rollup(vec![])
                });
            }
            let units = split_sql_csv(inner)?
                .iter()
                .map(|value| unit(value, projections))
                .collect::<Result<Vec<_>>>()?;
            return Ok(if cube {
                Item::Cube(units)
            } else {
                Item::Rollup(units)
            });
        }
    }
    if let Some(inner) = contents(raw, "grouping sets")? {
        if inner.is_empty() {
            return Err(unsupported_sql_error(
                "GROUPING SETS requires at least one set",
            ));
        }
        return Ok(Item::Sets(
            split_sql_csv(inner)?
                .iter()
                .map(|value| item(value, projections))
                .collect::<Result<_>>()?,
        ));
    }
    if raw.starts_with('(') && matching_closing_parenthesis(raw, 0)? == Some(raw.len() - 1) {
        let inner = raw[1..raw.len() - 1].trim();
        return if inner.is_empty() {
            Ok(Item::Keys(vec![]))
        } else {
            product(inner, projections)
        };
    }
    Ok(Item::Keys(unit(raw, projections)?))
}

impl Item {
    fn width_bytes(&self) -> Result<usize> {
        let add = |total: usize, key: &String| {
            total.checked_add(key.len().checked_mul(8)?.checked_add(512)?)
        };
        match self {
            Self::Keys(keys) => keys.iter().try_fold(256usize, add).ok_or_else(too_large),
            Self::Rollup(units) | Self::Cube(units) => units
                .iter()
                .flatten()
                .try_fold(256usize, add)
                .ok_or_else(too_large),
            Self::Product(items) | Self::Sets(items) => {
                items.iter().try_fold(256usize, |bytes, item| {
                    bytes.checked_add(item.width_bytes()?).ok_or_else(too_large)
                })
            }
        }
    }
    fn plain(&self) -> bool {
        matches!(self, Self::Product(items) if items.iter().all(|item| matches!(item, Self::Keys(keys) if keys.len()==1)))
    }
    fn columns(&self, columns: &mut Vec<String>) {
        match self {
            Self::Keys(keys) => {
                for key in keys {
                    if !columns.contains(key) {
                        columns.push(key.clone());
                    }
                }
            }
            Self::Rollup(units) | Self::Cube(units) => {
                for key in units.iter().flatten() {
                    if !columns.contains(key) {
                        columns.push(key.clone());
                    }
                }
            }
            Self::Product(items) | Self::Sets(items) => {
                for item in items {
                    item.columns(columns);
                }
            }
        }
    }
    fn count(&self) -> Result<usize> {
        let count = match self {
            Self::Keys(_) => Some(1),
            Self::Rollup(units) => units.len().checked_add(1),
            Self::Cube(units) => {
                1usize.checked_shl(u32::try_from(units.len()).map_err(|_| too_large())?)
            }
            Self::Product(items) => items
                .iter()
                .try_fold(1usize, |count, item| {
                    count.checked_mul(item.count()?).ok_or_else(too_large)
                })
                .ok(),
            Self::Sets(items) => items
                .iter()
                .try_fold(0usize, |count, item| {
                    count.checked_add(item.count()?).ok_or_else(too_large)
                })
                .ok(),
        };
        count.ok_or_else(too_large)
    }
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    fn expand(&self) -> Result<Vec<Vec<String>>> {
        match self {
            Self::Keys(keys) => Ok(vec![keys.clone()]),
            Self::Sets(items) => {
                let mut output = Vec::with_capacity(self.count()?);
                for item in items {
                    output.extend(item.expand()?);
                }
                Ok(output)
            }
            Self::Product(items) => {
                let mut output = vec![vec![]];
                for item in items {
                    let right = item.expand()?;
                    let mut next = Vec::with_capacity(
                        output
                            .len()
                            .checked_mul(right.len())
                            .ok_or_else(too_large)?,
                    );
                    for left in &output {
                        for right in &right {
                            let mut keys = left.clone();
                            keys.extend(right.iter().cloned());
                            next.push(keys);
                        }
                    }
                    output = next;
                }
                Ok(output)
            }
            Self::Rollup(units) => Ok((0..=units.len())
                .rev()
                .map(|end| units[..end].iter().flatten().cloned().collect())
                .collect()),
            Self::Cube(units) => Ok((0..self.count()?)
                .rev()
                .map(|mask| {
                    units
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| mask & (1usize << index) != 0)
                        .flat_map(|(_, unit)| unit.iter().cloned())
                        .collect()
                })
                .collect()),
        }
    }
}

fn too_large() -> ShardLoomError {
    unsupported_sql_error("grouping-set expansion metadata size overflow")
}

impl Grouping {
    pub(super) fn metadata_bytes(&self) -> Result<usize> {
        let width = self
            .item
            .width_bytes()?
            .checked_add(4096)
            .ok_or_else(too_large)?;
        self.item.count()?.checked_mul(width).ok_or_else(too_large)
    }
    /// Called only under the native declaration's expanded-metadata credit.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(super) fn expand(&self, columns: &[String]) -> Result<Vec<Vec<String>>> {
        let mut seen = std::collections::BTreeSet::new();
        let mut result = Vec::new();
        for set in self.item.expand()? {
            let set = columns
                .iter()
                .filter(|column| set.contains(column))
                .cloned()
                .collect::<Vec<_>>();
            if !self.distinct || seen.insert(set.clone()) {
                result.push(set);
            }
        }
        Ok(result)
    }
}
