//! Typed numeric cells inside the shared sparse pivot owner. Primitive cells
//! retain their representation; decimal cells retain wide, unrounded totals.

use std::collections::BTreeMap;

use super::{PivotAggregateCell, PivotValue, Result, ScalarValue, Value, failed};
use crate::local_primitives::native_decimal_reduce::{self, Total};
use vortex::array::dtype::DecimalDType;

type Key = (String, String);

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct DecimalCell {
    total: Total,
    min: Option<i128>,
    max: Option<i128>,
}

#[derive(Debug)]
pub(super) enum Cells {
    Primitive(BTreeMap<Key, PivotAggregateCell>),
    Decimal(DecimalDType, BTreeMap<Key, DecimalCell>),
}

impl Default for Cells {
    fn default() -> Self {
        Self::Primitive(BTreeMap::new())
    }
}

impl Cells {
    pub(super) fn new(source: Option<DecimalDType>) -> Self {
        source.map_or_else(Self::default, |source| {
            Self::Decimal(source, BTreeMap::new())
        })
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Primitive(cells) => cells.len(),
            Self::Decimal(_, cells) => cells.len(),
        }
    }

    pub(super) fn contains_key(&self, key: &Key) -> bool {
        match self {
            Self::Primitive(cells) => cells.contains_key(key),
            Self::Decimal(_, cells) => cells.contains_key(key),
        }
    }

    pub(super) fn update(&mut self, key: Key, value: &ScalarValue, aggregate: &str) -> Result<()> {
        match self {
            Self::Primitive(cells) => {
                let mut cell = cells.get(&key).copied().unwrap_or_default();
                cell.count = cell
                    .count
                    .checked_add(1)
                    .ok_or_else(|| failed("pivot cell count overflow"))?;
                if aggregate != "count" {
                    let value = value.pivot_numeric().map_err(|_| {
                        failed("pivot numeric aggregate requires a numeric non-null value")
                    })?;
                    if !value.is_finite() {
                        return Err(failed("pivot numeric value is not finite"));
                    }
                    cell.sum += value;
                    if matches!(aggregate, "sum" | "mean") && !cell.sum.is_finite() {
                        return Err(failed("pivot numeric accumulation is not finite"));
                    }
                    cell.min = Some(cell.min.map_or(value, |current| current.min(value)));
                    cell.max = Some(cell.max.map_or(value, |current| current.max(value)));
                }
                cells.insert(key, cell);
            }
            Self::Decimal(source, cells) => {
                let ScalarValue::Decimal128 {
                    value,
                    precision,
                    scale,
                } = value
                else {
                    return Err(failed(
                        "pivot numeric aggregate requires a decimal non-null value",
                    ));
                };
                if *precision != source.precision()
                    || i8::try_from(*scale).ok() != Some(source.scale())
                {
                    return Err(failed(
                        "pivot decimal value differs from its bound source type",
                    ));
                }
                let mut cell = cells.get(&key).copied().unwrap_or_default();
                cell.total.add(*value, *source)?;
                cell.min = Some(cell.min.map_or(*value, |current| current.min(*value)));
                cell.max = Some(cell.max.map_or(*value, |current| current.max(*value)));
                cells.insert(key, cell);
            }
        }
        Ok(())
    }

    pub(super) fn value(&self, key: &Key, aggregate: &str) -> Result<Option<Value<'static>>> {
        match self {
            Self::Primitive(cells) => cells
                .get(key)
                .map(|cell| primitive_value(cell, aggregate))
                .transpose()
                .map(Option::flatten),
            Self::Decimal(source, cells) => cells
                .get(key)
                .map(|cell| decimal_value(cell, *source, aggregate))
                .transpose()
                .map(Option::flatten),
        }
    }
}

pub(super) enum Margin {
    Primitive(Option<PivotAggregateCell>),
    Decimal(DecimalDType, Option<DecimalCell>),
}

impl Margin {
    pub(super) fn new(cells: &Cells) -> Self {
        match cells {
            Cells::Primitive(_) => Self::Primitive(None),
            Cells::Decimal(source, _) => Self::Decimal(*source, None),
        }
    }

    pub(super) fn push(&mut self, cells: &Cells, key: &Key, aggregate: &str) -> Result<()> {
        match (self, cells) {
            (Self::Primitive(total), Cells::Primitive(cells)) => {
                if let Some(cell) = cells.get(key) {
                    let current = total.get_or_insert_default();
                    current.count = current
                        .count
                        .checked_add(cell.count)
                        .ok_or_else(|| failed("pivot margin count overflow"))?;
                    current.sum += cell.sum;
                    if matches!(aggregate, "sum" | "mean") && !current.sum.is_finite() {
                        return Err(failed("pivot margin accumulation is not finite"));
                    }
                    if let Some(value) = cell.min {
                        current.min = Some(current.min.map_or(value, |current| current.min(value)));
                    }
                    if let Some(value) = cell.max {
                        current.max = Some(current.max.map_or(value, |current| current.max(value)));
                    }
                }
            }
            (Self::Decimal(source, total), Cells::Decimal(expected, cells))
                if source == expected =>
            {
                if let Some(cell) = cells.get(key) {
                    let current = total.get_or_insert_default();
                    current.total.merge(&cell.total)?;
                    if let Some(value) = cell.min {
                        current.min = Some(current.min.map_or(value, |current| current.min(value)));
                    }
                    if let Some(value) = cell.max {
                        current.max = Some(current.max.map_or(value, |current| current.max(value)));
                    }
                }
            }
            _ => return Err(failed("pivot margin differs from its bound cell type")),
        }
        Ok(())
    }

    pub(super) fn value(&self, aggregate: &str) -> Result<Option<Value<'static>>> {
        match self {
            Self::Primitive(cell) => cell
                .as_ref()
                .map(|cell| primitive_value(cell, aggregate))
                .transpose()
                .map(Option::flatten),
            Self::Decimal(source, cell) => cell
                .as_ref()
                .map(|cell| decimal_value(cell, *source, aggregate))
                .transpose()
                .map(Option::flatten),
        }
    }
}

fn primitive_value(cell: &PivotAggregateCell, aggregate: &str) -> Result<Option<Value<'static>>> {
    Ok(match aggregate {
        "count" => Some(Value::UInt(cell.count)),
        "sum" => Some(Value::Float(cell.sum)),
        "mean" => crate::local_primitives::pivot_mean_value(cell.sum, cell.count).map(Value::Float),
        "min" => cell.min.map(Value::Float),
        "max" => cell.max.map(Value::Float),
        _ => return Err(failed("pivot numeric aggregate was not admitted")),
    })
}

fn decimal_value(
    cell: &DecimalCell,
    source: DecimalDType,
    aggregate: &str,
) -> Result<Option<Value<'static>>> {
    let (value, dtype) = match aggregate {
        "sum" | "mean" => (
            cell.total.finish(source, aggregate == "mean")?,
            native_decimal_reduce::output_dtype(source, aggregate == "mean")?,
        ),
        "min" => (cell.min, source),
        "max" => (cell.max, source),
        _ => return Err(failed("pivot decimal aggregate was not admitted")),
    };
    Ok(value.map(|value| Value::Decimal(value, dtype)))
}
