//! Private typed records for native external aggregation; no user-name overlap.

use super::{aggregate, failed};
use crate::{
    local_primitives::{
        logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_payload,
        native_relational_aggregate::Function,
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
    memory::HostMemorySessionExt as _,
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

pub(super) struct OrderedValue {
    column: usize,
    filter_column: Option<usize>,
    pub(super) name: String,
    pub(super) dtype: DType,
    pub(super) measures: Vec<usize>,
}

struct CollectionKey {
    column: usize,
    name: String,
    order: crate::relational_query::VortexRelationalOrderKey,
}

pub(super) struct Collection {
    column: usize,
    filter_column: Option<usize>,
    pub(super) measure: usize,
    skip_nulls: bool,
    keys: Vec<CollectionKey>,
}

impl Collection {
    fn new(
        measure: &aggregate::Measure,
        index: usize,
        slot: usize,
        column: impl Fn(&str) -> Result<usize>,
    ) -> Result<Self> {
        let spec = measure
            .selection
            .as_ref()
            .ok_or_else(|| failed("collection order is absent"))?;
        let keys = spec
            .order
            .iter()
            .enumerate()
            .map(|(offset, order)| {
                Ok(CollectionKey {
                    column: column(order.column.as_str())?,
                    name: format!("c{slot}_{offset}"),
                    order: order.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            filter_column: measure.filter_column.as_deref().map(&column).transpose()?,
            column: column(
                measure
                    .column
                    .as_deref()
                    .ok_or_else(|| failed("collection value is absent"))?,
            )?,
            measure: index,
            skip_nulls: measure.function == Function::StringAgg,
            keys,
        })
    }
}

pub(super) struct Layout {
    columns: Vec<Column>,
    pub(super) values: Vec<OrderedValue>,
    pub(super) collections: Vec<Collection>,
    ordered_ordinals: bool,
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
                !measure.function.is_distinct()
                    && !measure.function.is_collection()
                    && measure.input_columns().any(|column| column == name)
            });
        let needed = base
            || spec
                .measures
                .iter()
                .any(|measure| measure.input_columns().any(|column| column == name));
        if needed {
            // Nested COUNT observes parent validity without evaluating children.
            let validity_only = !group
                && native_payload::is_nested(dtype)
                && spec
                    .measures
                    .iter()
                    .filter(|measure| measure.input_columns().any(|column| column == name))
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
                .any(|measure| measure.input_columns().any(|column| column == name))
    };
    let bytes = input_fields
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
        })?;
    spec.measures
        .iter()
        .filter(|measure| measure.function.is_collection())
        .try_fold(bytes, |bytes, measure| {
            let mut extra = 4096_u64
                .checked_add(measure.separator.as_ref().map_or(0, String::len) as u64)
                .ok_or_else(|| failed("collection descriptor capacity overflow"))?;
            if measure.function.is_collection() {
                for (_, dtype) in &measure
                    .selection
                    .as_ref()
                    .ok_or_else(|| failed("collection specification is absent"))?
                    .fields
                {
                    extra = extra
                        .checked_add(native_payload::metadata_bytes(dtype)?)
                        .and_then(|value| value.checked_add(4096))
                        .ok_or_else(|| failed("collection descriptor capacity overflow"))?;
                }
            }
            extra
                .checked_mul(8)
                .and_then(|extra| bytes.checked_add(extra))
                .ok_or_else(|| failed("collection descriptor capacity overflow"))
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
        let mut values = Vec::<OrderedValue>::new();
        let mut collections = Vec::new();
        let ordered_ordinals = spec
            .measures
            .iter()
            .any(|measure| measure.function.is_quantile() || measure.function.is_collection());
        let mut measures = Vec::new();
        for (index, measure) in spec.measures.iter().enumerate() {
            let selected = measure.column.as_deref().map(column).transpose()?;
            let filter_column = measure.filter_column.as_deref().map(column).transpose()?;
            if measure.function.is_collection() {
                collections.push(Collection::new(measure, index, collections.len(), column)?);
            }
            if measure.function.is_distinct() || measure.function.is_quantile() {
                let selected =
                    selected.ok_or_else(|| failed("ordered value input column absent"))?;
                if let Some(existing) = values
                    .iter_mut()
                    .find(|entry| entry.column == selected && entry.filter_column == filter_column)
                {
                    existing.measures.push(index);
                } else {
                    values.push(OrderedValue {
                        column: selected,
                        filter_column,
                        name: format!("d{}", values.len()),
                        dtype: columns[selected].dtype.clone(),
                        measures: vec![index],
                    });
                }
            }
            measures.push(aggregate::Measure {
                function: measure.function,
                quantile_level: measure.quantile_level,
                separator: measure.separator.clone(),
                filter_column: filter_column.map(|index| columns[index].name.clone()),
                selection: map_selection(measure.selection.as_ref(), &columns)?,
                column: selected.map(|index| columns[index].name.clone()),
                secondary_column: measure
                    .secondary_column
                    .as_deref()
                    .map(|name| Ok(columns[column(name)?].name.clone()))
                    .transpose()?,
                dtype: measure.dtype.clone(),
                decimal_source: measure.decimal_source,
                decimal_output: measure.decimal_output,
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
        let records = layout_order(spec, &columns, &values, &collections, &group_names)?;
        let results = result_order(spec)?;
        Ok(Self {
            columns,
            values,
            collections,
            ordered_ordinals,
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
        self.record(array, &indices, None, ordinal, None, context)
    }

    pub(super) fn ordered_value_record(
        &self,
        array: &ArrayRef,
        kind: usize,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let entry = &self.values[kind];
        let selected = aggregate::filter::rows(
            array,
            entry
                .filter_column
                .map(|index| self.columns[index].source.as_str()),
            context,
        )?;
        let value = logical_field_from_native_array(array, &self.columns[entry.column].source)?;
        let values = keys(&value, context)?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(array.len())?;
        for offset in 0..selected
            .as_ref()
            .map_or(array.len(), |rows| rows.values.len())
        {
            if offset.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let row = selected.as_ref().map_or(offset, |rows| rows.values[offset]);
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
        self.record(
            array,
            &indices,
            Some(kind),
            ordinal,
            Some(&rows.values),
            context,
        )
        .map(Some)
    }

    pub(super) fn collection_record(
        &self,
        array: &ArrayRef,
        collection: usize,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let entry = &self.collections[collection];
        let selected = aggregate::filter::rows(
            array,
            entry
                .filter_column
                .map(|index| self.columns[index].source.as_str()),
            context,
        )?;
        let validity = if entry.skip_nulls {
            Some(parent_validity(
                &logical_field_from_native_array(array, &self.columns[entry.column].source)?,
                context,
            )?)
        } else {
            None
        };
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(array.len())?;
        for offset in 0..selected
            .as_ref()
            .map_or(array.len(), |rows| rows.values.len())
        {
            if offset.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let row = selected.as_ref().map_or(offset, |rows| rows.values[offset]);
            if validity.as_ref().is_none_or(|validity| validity.value(row)) {
                rows.values.push(row);
            }
        }
        if rows.values.is_empty() {
            return Ok(None);
        }
        for key in &entry.keys {
            if key.order.nulls.is_none() {
                let validity = parent_validity(
                    &logical_field_from_native_array(array, &self.columns[key.column].source)?,
                    context,
                )?;
                for &row in &rows.values {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if !validity.value(row) {
                        return Err(failed(
                            "aggregate ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                        ));
                    }
                }
            }
        }
        let indices = index_array(rows.values.len(), false, context, |row| {
            Ok(Some(rows.values[row]))
        })?;
        self.record(
            array,
            &indices,
            Some(self.values.len() + collection),
            ordinal,
            Some(&rows.values),
            context,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)] // One typed record shares its selection and original source ordinals.
    fn record(
        &self,
        array: &ArrayRef,
        indices: &ArrayRef,
        kind: Option<usize>,
        ordinal: u64,
        source_rows: Option<&[usize]>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let rows = indices.len();
        let collection = kind
            .and_then(|kind| kind.checked_sub(self.values.len()))
            .and_then(|index| self.collections.get(index));
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.records.fields.len())?;
        for (index, column) in self.columns.iter().enumerate() {
            context.check_cancelled()?;
            columns.values.push(
                if column.group
                    || (kind.is_none() && column.base)
                    || collection.is_some_and(|entry| entry.column == index)
                {
                    let source = logical_field_from_native_array(array, &column.source)?;
                    if column.validity_only {
                        let valid = parent_validity(&source, context)?;
                        result_batch::build_column_with_float_policy(
                            &column.dtype,
                            rows,
                            &context.native_session().host_allocator(),
                            context.float_policy(),
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
                        native_payload::take_with_policy(
                            &source,
                            indices,
                            &column.dtype,
                            if column.group || collection.is_some() {
                                native_payload::CopyPolicy::ValidateValues
                            } else {
                                native_payload::CopyPolicy::PreserveUnobserved
                            },
                            context,
                        )?
                    }
                } else {
                    native_payload::defaults(&column.dtype, rows, context)?
                },
            );
        }
        columns.values.push(unsigned(rows, context, |_| {
            Ok(kind.map_or(0, |kind| kind as u64 + 1))
        })?);
        columns.values.push(unsigned(rows, context, |row| {
            if kind.is_some() && !self.ordered_ordinals {
                Ok(0)
            } else {
                ordinal
                    .checked_add(source_rows.map_or(row, |rows| rows[row]) as u64)
                    .ok_or_else(|| failed("aggregate input ordinal overflow"))
            }
        })?);
        for (index, entry) in self.values.iter().enumerate() {
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
        for (index, entry) in self.collections.iter().enumerate() {
            for key in &entry.keys {
                context.check_cancelled()?;
                let column = &self.columns[key.column];
                columns
                    .values
                    .push(if kind == Some(self.values.len() + index) {
                        native_payload::take(
                            &logical_field_from_native_array(array, &column.source)?,
                            indices,
                            &column.dtype,
                            context,
                        )?
                    } else {
                        native_payload::defaults(&column.dtype, rows, context)?
                    });
            }
        }
        structure(&self.records.fields, columns, rows)
    }
}

fn layout_order(
    spec: &aggregate::Spec,
    columns: &[Column],
    values: &[OrderedValue],
    collections: &[Collection],
    group_names: &[String],
) -> Result<native_relational_sort::Spec> {
    let mut fields = columns
        .iter()
        .map(|column| (column.name.clone(), column.dtype.clone()))
        .collect::<Vec<_>>();
    fields.extend([(KIND.into(), u64_type()), (ORDINAL.into(), u64_type())]);
    fields.extend(
        values
            .iter()
            .map(|entry| (entry.name.clone(), entry.dtype.clone())),
    );
    for collection in collections {
        fields.extend(
            collection
                .keys
                .iter()
                .map(|key| (key.name.clone(), columns[key.column].dtype.clone())),
        );
    }
    let mut names = group_names.to_vec();
    names.push(KIND.into());
    names.extend(values.iter().map(|entry| entry.name.clone()));
    names.extend(
        collections
            .iter()
            .flat_map(|entry| entry.keys.iter().map(|key| key.name.clone())),
    );
    let mut records = record_order(spec, fields, names)?;
    for collection in collections {
        for key in &collection.keys {
            let bound = records
                .keys
                .iter_mut()
                .find(|order| order.column.as_str() == key.name)
                .ok_or_else(|| failed("collection record order is absent"))?;
            bound.descending = key.order.descending;
            // Inactive record kinds contain NULL placeholders. The active
            // record validates any omitted null-placement declaration.
            bound.nulls = Some(
                key.order
                    .nulls
                    .unwrap_or(crate::relational_query::VortexRelationalNullOrder::Last),
            );
        }
    }
    Ok(records)
}

fn record_order(
    spec: &aggregate::Spec,
    fields: Vec<(String, DType)>,
    mut names: Vec<String>,
) -> Result<native_relational_sort::Spec> {
    if spec
        .measures
        .iter()
        .any(|measure| measure.function.is_quantile() || measure.function.is_ordered_value())
    {
        names.push(ORDINAL.into());
    }
    let mut order = order(fields, names)?;
    // Base records carry payload for the reducer's own observation domain.
    // Sorting still validates grouping/distinct keys; movement must not observe
    // an unselected selector value or a companion of a NULL statistical pair.
    order.copy_policy = native_payload::CopyPolicy::PreserveUnobserved;
    Ok(order)
}

fn map_selection(
    spec: Option<&aggregate::SelectionSpec>,
    columns: &[Column],
) -> Result<Option<aggregate::SelectionSpec>> {
    spec.map(|spec| {
        let column = |name: &str| {
            columns
                .iter()
                .find(|column| column.source == name)
                .ok_or_else(|| failed("ordered selector column is not bound"))
        };
        Ok(aggregate::SelectionSpec {
            fields: spec
                .fields
                .iter()
                .map(|(name, _)| {
                    let field = column(name)?;
                    Ok((field.name.clone(), field.dtype.clone()))
                })
                .collect::<Result<_>>()?,
            names: spec
                .names
                .iter()
                .map(|name| Ok(column(name)?.name.clone()))
                .collect::<Result<_>>()?,
            order: spec
                .order
                .iter()
                .map(|key| {
                    Ok(crate::relational_query::VortexRelationalOrderKey {
                        column: shardloom_core::ColumnRef::new(
                            column(key.column.as_str())?.name.clone(),
                        )?,
                        descending: key.descending,
                        nulls: key.nulls,
                    })
                })
                .collect::<Result<_>>()?,
        })
    })
    .transpose()
}

fn result_order(spec: &aggregate::Spec) -> Result<native_relational_sort::Spec> {
    let mut fields = spec
        .fields
        .iter()
        .enumerate()
        .map(|(index, (_, dtype))| (format!("o{index}"), dtype.clone()))
        .collect::<Vec<_>>();
    fields.push((ORDINAL.into(), u64_type()));
    order(fields, vec![ORDINAL.into()])
}

pub(super) fn kind(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<crate::local_primitives::native_relational_keys::KeyColumn> {
    keys(&logical_field_from_native_array(array, KIND)?, context)
}
