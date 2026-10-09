//! Shared typed pivot transitions. Both resident maps and private native runs
//! retain the same complete cell state and finalize only at the result boundary.

use std::collections::BTreeMap;

use super::{
    Datum, NativeExecutionContext, PivotAggregateCell, PivotValue, Result, ScalarValue, Value,
    failed,
};
use crate::local_primitives::native_decimal_reduce::{self, Total};
use vortex::array::{ArrayRef, dtype::DecimalDType};

type Key = (String, String);

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct DecimalCell {
    pub(super) total: Total,
    pub(super) min: Option<i128>,
    pub(super) max: Option<i128>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Primitive,
    Decimal(DecimalDType),
    Nested,
}

#[derive(Clone)]
pub(super) enum Cell {
    Primitive(PivotAggregateCell),
    Decimal(DecimalDType, DecimalCell),
    Nested(Option<Datum>),
}

impl Kind {
    pub(super) fn new(source: Option<DecimalDType>, nested: bool) -> Self {
        if nested {
            Self::Nested
        } else {
            source.map_or(Self::Primitive, Self::Decimal)
        }
    }

    pub(super) fn empty(self) -> Cell {
        match self {
            Self::Primitive => Cell::Primitive(PivotAggregateCell::default()),
            Self::Decimal(source) => Cell::Decimal(source, DecimalCell::default()),
            Self::Nested => Cell::Nested(None),
        }
    }
}

impl Cell {
    pub(super) fn update(
        &mut self,
        value: &Datum,
        aggregate: &str,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        match self {
            Self::Primitive(previous) => {
                let mut cell = *previous;
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
                *previous = cell;
            }
            Self::Decimal(source, previous) => {
                let ScalarValue::Decimal128 {
                    value,
                    precision,
                    scale,
                } = value.scalar()?
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
                let mut cell = *previous;
                cell.total.add(*value, *source)?;
                cell.min = Some(cell.min.map_or(*value, |current| current.min(*value)));
                cell.max = Some(cell.max.map_or(*value, |current| current.max(*value)));
                *previous = cell;
            }
            Self::Nested(cell) => {
                if nested_replaces(cell.as_ref(), value, aggregate)? {
                    // Admit the compact replacement while the old payload is held.
                    *cell = Some(value.retain(context)?);
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self, aggregate: &str) -> Result<()> {
        match (self, other) {
            (Self::Primitive(current), Self::Primitive(cell)) => {
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
            (Self::Decimal(source, current), Self::Decimal(expected, cell))
                if source == expected =>
            {
                current.total.merge(&cell.total)?;
                if let Some(value) = cell.min {
                    current.min = Some(current.min.map_or(value, |current| current.min(value)));
                }
                if let Some(value) = cell.max {
                    current.max = Some(current.max.map_or(value, |current| current.max(value)));
                }
            }
            (Self::Nested(current), Self::Nested(Some(value))) => {
                if nested_replaces(current.as_ref(), value, aggregate)? {
                    // Already compact values share their allocation credits.
                    *current = Some(value.clone());
                }
            }
            (Self::Nested(_), Self::Nested(None)) => {}
            _ => return Err(failed("pivot margin differs from its bound cell type")),
        }
        Ok(())
    }

    pub(super) fn value(&self, aggregate: &str) -> Result<Option<Value<'static>>> {
        match self {
            Self::Primitive(cell) => primitive_value(cell, aggregate),
            Self::Decimal(source, cell) => decimal_value(cell, *source, aggregate),
            Self::Nested(_) => Err(failed("nested pivot extrema require native delivery")),
        }
    }

    pub(super) fn native(&self) -> Result<Option<ArrayRef>> {
        match self {
            Self::Nested(value) => value.as_ref().map(Datum::native).transpose(),
            _ => Err(failed("scalar pivot cell cannot supply a nested output")),
        }
    }
}

pub(super) struct Cells {
    kind: Kind,
    values: BTreeMap<Key, Cell>,
}

impl Default for Cells {
    fn default() -> Self {
        Self::new(None, false)
    }
}

impl Cells {
    pub(super) fn new(source: Option<DecimalDType>, nested: bool) -> Self {
        Self {
            kind: Kind::new(source, nested),
            values: BTreeMap::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.values.len()
    }

    pub(super) fn contains_key(&self, key: &Key) -> bool {
        self.values.contains_key(key)
    }

    pub(super) fn update(
        &mut self,
        key: Key,
        value: &Datum,
        aggregate: &str,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut cell = self
            .values
            .get(&key)
            .cloned()
            .unwrap_or_else(|| self.kind.empty());
        cell.update(value, aggregate, context)?;
        self.values.insert(key, cell);
        Ok(())
    }

    pub(super) fn value(&self, key: &Key, aggregate: &str) -> Result<Option<Value<'static>>> {
        if self.kind == Kind::Nested {
            return Err(failed("nested pivot extrema require native delivery"));
        }
        self.values
            .get(key)
            .map(|cell| cell.value(aggregate))
            .transpose()
            .map(Option::flatten)
    }

    pub(super) fn native(&self, key: &Key) -> Result<Option<ArrayRef>> {
        if self.kind != Kind::Nested {
            return Err(failed("scalar pivot cell cannot supply a nested output"));
        }
        self.values
            .get(key)
            .map(Cell::native)
            .transpose()
            .map(Option::flatten)
    }
}

pub(super) struct Margin {
    kind: Kind,
    cell: Option<Cell>,
}

impl Margin {
    pub(super) fn new(cells: &Cells) -> Self {
        Self::for_kind(cells.kind)
    }

    pub(super) fn for_kind(kind: Kind) -> Self {
        Self { kind, cell: None }
    }

    pub(super) fn push(&mut self, cells: &Cells, key: &Key, aggregate: &str) -> Result<()> {
        if self.kind != cells.kind {
            return Err(failed("pivot margin differs from its bound cell type"));
        }
        self.push_cell(cells.values.get(key), aggregate)
    }

    pub(super) fn push_cell(&mut self, cell: Option<&Cell>, aggregate: &str) -> Result<()> {
        if let Some(cell) = cell {
            // Even the first primitive cell is added to zero; copying it instead
            // would change signed-zero and accumulation observation semantics.
            self.cell
                .get_or_insert_with(|| self.kind.empty())
                .merge(cell, aggregate)?;
        }
        Ok(())
    }

    pub(super) fn value(&self, aggregate: &str) -> Result<Option<Value<'static>>> {
        if self.kind == Kind::Nested {
            return Err(failed("nested pivot margin requires native delivery"));
        }
        self.cell
            .as_ref()
            .map(|cell| cell.value(aggregate))
            .transpose()
            .map(Option::flatten)
    }

    pub(super) fn native(&self) -> Result<Option<ArrayRef>> {
        if self.kind != Kind::Nested {
            return Err(failed("scalar pivot margin cannot supply a nested output"));
        }
        self.cell
            .as_ref()
            .map(Cell::native)
            .transpose()
            .map(Option::flatten)
    }
}

fn nested_replaces(current: Option<&Datum>, value: &Datum, aggregate: &str) -> Result<bool> {
    if !matches!(aggregate, "min" | "max") {
        return Err(failed("nested pivot aggregation requires min or max"));
    }
    if value.is_null()? {
        return Ok(false);
    }
    let Some(current) = current else {
        return Ok(true);
    };
    let order = value.compare(current)?;
    Ok(if aggregate == "min" {
        order.is_lt()
    } else {
        order.is_gt()
    })
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
