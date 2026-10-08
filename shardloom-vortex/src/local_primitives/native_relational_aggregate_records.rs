//! Private typed records for native external aggregation; no user-name overlap.

use super::{aggregate, failed};
use crate::{
    local_primitives::{
        SimpleAggregateFunction as Function, logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_payload,
        native_relational_batch::index_array,
        native_relational_expression::{keys, parent_validity},
        native_relational_records::{order, u64_type},
        native_relational_sort,
        result_batch::{self, Value},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
    memory::MemorySessionExt as _,
};

const KIND: &str = "kind";
pub(super) use crate::local_primitives::native_relational_records::{
    ORDINAL, delivered, structure, unsigned,
};

#[cfg(test)]
#[path = "native_relational_aggregate_records_tests.rs"]
mod tests;

struct Column {
    source: String,
    name: String,
    dtype: DType,
    base: bool,
    group: bool,
    validity_only: bool,
}

pub(super) struct Distinct {
    column: usize,
    pub(super) name: String,
    pub(super) dtype: DType,
    pub(super) measures: Vec<usize>,
}

pub(super) struct Layout {
    columns: Vec<Column>,
    pub(super) distinct: Vec<Distinct>,
    pub(super) group_names: Vec<String>,
    pub(super) group_fields: Vec<(String, DType)>,
    pub(super) scalar: aggregate::Spec,
    pub(super) records: native_relational_sort::Spec,
    pub(super) results: native_relational_sort::Spec,
    _metadata: MemoryLease,
}

fn input_columns(spec: &aggregate::Spec, input_fields: &[(String, DType)]) -> Vec<Column> {
    let mut columns = Vec::new();
    for (name, dtype) in input_fields {
        let group = spec.group_names.iter().any(|group| group == name);
        let base = group
            || spec.measures.iter().any(|measure| {
                measure.function != Function::CountDistinct && measure.column.as_ref() == Some(name)
            });
        let needed = base
            || spec
                .measures
                .iter()
                .any(|measure| measure.column.as_ref() == Some(name));
        if needed {
            // Nested COUNT observes parent validity without evaluating children.
            let validity_only = !group
                && native_payload::is_nested(dtype)
                && spec
                    .measures
                    .iter()
                    .filter(|measure| measure.column.as_ref() == Some(name))
                    .all(|measure| measure.function == Function::Count);
            columns.push(Column {
                source: name.clone(),
                name: format!("v{}", columns.len()),
                dtype: if validity_only {
                    DType::Bool(Nullability::Nullable)
                } else {
                    dtype.as_nullable()
                },
                base,
                group,
                validity_only,
            });
        }
    }
    columns
}

fn descriptor_bytes(spec: &aggregate::Spec, input_fields: &[(String, DType)]) -> Result<u64> {
    // Eight copies cover both sorts, the scalar reducer and bounded mappings.
    let selected = |name: &String| {
        spec.group_names.contains(name)
            || spec
                .measures
                .iter()
                .any(|measure| measure.column.as_ref() == Some(name))
    };
    input_fields
        .iter()
        .filter(|(name, _)| selected(name))
        .chain(&spec.fields)
        .try_fold(4096_u64, |bytes, (name, dtype)| {
            native_payload::metadata_bytes(dtype)?
                .checked_add(name.len() as u64 * 2)
                .and_then(|field| field.checked_add(2048))
                .and_then(|field| field.checked_mul(8))
                .and_then(|field| bytes.checked_add(field))
                .ok_or_else(|| failed("ordered aggregate descriptor capacity overflow"))
        })
}

impl Layout {
    pub(super) fn new(
        spec: &aggregate::Spec,
        input_fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let metadata = context
            .memory()
            .reserve(descriptor_bytes(spec, input_fields)?)?;
        let columns = input_columns(spec, input_fields);
        let column = |name: &str| {
            columns
                .iter()
                .position(|column| column.source == name)
                .ok_or_else(|| failed("ordered aggregate column is not bound"))
        };
        let group_names = spec
            .group_names
            .iter()
            .map(|name| Ok(columns[column(name)?].name.clone()))
            .collect::<Result<Vec<_>>>()?;
        let mut group_fields = spec
            .group_names
            .iter()
            .map(|name| {
                let column = &columns[column(name)?];
                Ok((column.name.clone(), column.dtype.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        group_fields.push((ORDINAL.into(), u64_type()));
        let mut distinct = Vec::<Distinct>::new();
        let mut measures = Vec::new();
        for (index, measure) in spec.measures.iter().enumerate() {
            let selected = measure.column.as_deref().map(column).transpose()?;
            if measure.function == Function::CountDistinct {
                let selected = selected.ok_or_else(|| failed("distinct input column absent"))?;
                if let Some(existing) = distinct.iter_mut().find(|entry| entry.column == selected) {
                    existing.measures.push(index);
                } else {
                    distinct.push(Distinct {
                        column: selected,
                        name: format!("d{}", distinct.len()),
                        dtype: columns[selected].dtype.clone(),
                        measures: vec![index],
                    });
                }
            }
            measures.push(aggregate::Measure {
                function: measure.function,
                column: selected.map(|index| columns[index].name.clone()),
                dtype: measure.dtype.clone(),
                decimal_source: measure.decimal_source,
                distinct_fields: vec![],
                distinct_names: vec![],
            });
        }
        let scalar = aggregate::Spec {
            fields: spec.fields[spec.groups.len()..].to_vec(),
            groups: vec![],
            group_names: vec![],
            measures,
        };
        let mut fields = columns
            .iter()
            .map(|column| (column.name.clone(), column.dtype.clone()))
            .collect::<Vec<_>>();
        fields.extend([(KIND.into(), u64_type()), (ORDINAL.into(), u64_type())]);
        fields.extend(
            distinct
                .iter()
                .map(|entry| (entry.name.clone(), entry.dtype.clone())),
        );
        let mut names = group_names.clone();
        names.push(KIND.into());
        names.extend(distinct.iter().map(|entry| entry.name.clone()));
        let records = order(fields, names)?;
        let mut result_fields = spec
            .fields
            .iter()
            .enumerate()
            .map(|(index, (_, dtype))| (format!("o{index}"), dtype.clone()))
            .collect::<Vec<_>>();
        result_fields.push((ORDINAL.into(), u64_type()));
        let results = order(result_fields, vec![ORDINAL.into()])?;
        Ok(Self {
            columns,
            distinct,
            group_names,
            group_fields,
            scalar,
            records,
            results,
            _metadata: metadata,
        })
    }

    pub(super) fn base(
        &self,
        array: &ArrayRef,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let indices = index_array(array.len(), false, context, |row| Ok(Some(row)))?;
        self.record(array, &indices, None, ordinal, context)
    }

    pub(super) fn distinct_record(
        &self,
        array: &ArrayRef,
        kind: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let entry = &self.distinct[kind];
        let value = logical_field_from_native_array(array, &self.columns[entry.column].source)?;
        let values = keys(&value, context)?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(array.len())?;
        for row in 0..array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if !values.is_null(row)? {
                rows.values.push(row);
            }
        }
        if rows.values.is_empty() {
            return Ok(None);
        }
        let indices = index_array(rows.values.len(), false, context, |row| {
            Ok(Some(rows.values[row]))
        })?;
        self.record(array, &indices, Some(kind), 0, context)
            .map(Some)
    }

    fn record(
        &self,
        array: &ArrayRef,
        indices: &ArrayRef,
        kind: Option<usize>,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let rows = indices.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.records.fields.len())?;
        for column in &self.columns {
            context.check_cancelled()?;
            columns
                .values
                .push(if column.group || (kind.is_none() && column.base) {
                    let source = logical_field_from_native_array(array, &column.source)?;
                    if column.validity_only {
                        let valid = parent_validity(&source, context)?;
                        result_batch::build_column(
                            &column.dtype,
                            rows,
                            &context.native_session().allocator(),
                            |row| {
                                if row.is_multiple_of(1024) {
                                    context.check_cancelled()?;
                                }
                                Ok(if valid.value(row) {
                                    Value::Bool(true)
                                } else {
                                    Value::Null
                                })
                            },
                        )?
                    } else {
                        native_payload::take(&source, indices, &column.dtype, context)?
                    }
                } else {
                    native_payload::defaults(&column.dtype, rows, context)?
                });
        }
        columns.values.push(unsigned(rows, context, |_| {
            Ok(kind.map_or(0, |kind| kind as u64 + 1))
        })?);
        columns.values.push(unsigned(rows, context, |row| {
            if kind.is_some() {
                Ok(0)
            } else {
                ordinal
                    .checked_add(row as u64)
                    .ok_or_else(|| failed("aggregate input ordinal overflow"))
            }
        })?);
        for (index, entry) in self.distinct.iter().enumerate() {
            context.check_cancelled()?;
            columns.values.push(if kind == Some(index) {
                native_payload::take(
                    &logical_field_from_native_array(array, &self.columns[entry.column].source)?,
                    indices,
                    &entry.dtype,
                    context,
                )?
            } else {
                native_payload::defaults(&entry.dtype, rows, context)?
            });
        }
        structure(&self.records.fields, columns, rows)
    }
}

pub(super) fn kind(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<crate::local_primitives::native_relational_keys::KeyColumn> {
    keys(&logical_field_from_native_array(array, KIND)?, context)
}
