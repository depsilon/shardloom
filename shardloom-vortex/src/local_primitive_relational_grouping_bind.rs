//! Admit grouping metadata and retain one shared aggregate owner.

use super::super::grouping::Spec;
use super::{
    Binder, DType, Node, NodeKind, Nullability, PType, Result, failed, field, validate_name,
    validate_unique,
};
use crate::{
    local_primitives::native_relational_aggregate as kernel,
    relational_query::VortexRelationalGroupingSets,
};
use shardloom_core::{ColumnRef, ExprId, Expression};
use std::collections::BTreeSet;

fn column(name: &str) -> Result<Expression> {
    Ok(Expression::column(
        ExprId::new("grouping-output")?,
        ColumnRef::new(name)?,
    ))
}

impl Binder<'_> {
    pub(super) fn grouping_aggregate(
        &mut self,
        input: Box<Node>,
        mut aggregate: kernel::Spec,
        grouping: &VortexRelationalGroupingSets,
    ) -> Result<Node> {
        self.admit_grouping(&input, &aggregate, grouping)?;
        let mut expansion = normalized(&input, &aggregate, grouping)?;
        let mut names = Names {
            used: input
                .fields
                .iter()
                .chain(&aggregate.fields)
                .map(|(name, _)| name.clone())
                .chain(grouping.indicators.iter().map(|i| i.alias.clone()))
                .collect(),
            suffix: 0,
        };
        let mut output = Vec::new();
        let groups = private_groups(
            &aggregate,
            grouping,
            &mut expansion,
            &mut names,
            &mut output,
        )?;
        rewrite_filters(&input.fields, &mut aggregate, &mut expansion, &mut names)?;
        output.extend(
            aggregate
                .fields
                .iter()
                .skip(aggregate.groups.len())
                .map(|(name, _)| Ok((name.clone(), column(name)?)))
                .collect::<Result<Vec<_>>>()?,
        );
        if output.is_empty() {
            return Err(failed(
                "grouping aggregation requires at least one output column",
            ));
        }
        let mut result_fields = groups.clone();
        result_fields.extend(
            aggregate
                .fields
                .iter()
                .skip(aggregate.groups.len())
                .cloned(),
        );
        aggregate.group_names = groups.iter().map(|(name, _)| name.clone()).collect();
        aggregate.groups = groups;
        aggregate.fields = result_fields;
        validate_unique(&expansion.fields)?;
        validate_unique(&aggregate.fields)?;
        expansion.batch_metadata =
            expansion
                .fields
                .iter()
                .try_fold(4096u64, |total, (name, dtype)| {
                    total
                        .checked_add(if dtype == &DType::Null {
                            4096
                        } else {
                            crate::native_payload_schema::metadata_bytes(dtype)?
                        })
                        .and_then(|bytes| bytes.checked_add(name.len() as u64 + 512))
                        .ok_or_else(|| failed("grouping-set batch metadata overflow"))
                })?;
        self.project_input(
            &output,
            Node {
                fields: aggregate.fields.clone(),
                kind: NodeKind::Aggregate {
                    input,
                    spec: aggregate,
                    expansion: Some(Box::new(expansion)),
                },
            },
        )
    }

    fn admit_grouping(
        &mut self,
        input: &Node,
        aggregate: &kernel::Spec,
        grouping: &VortexRelationalGroupingSets,
    ) -> Result<()> {
        if grouping.sets.is_empty() {
            return Err(failed("grouping sets require at least one set"));
        }
        self.charge_items(grouping.sets.len(), 256)?;
        self.charge_fields(grouping.indicators.len())?;
        for set in &grouping.sets {
            self.charge_items(set.len(), 512)?;
        }
        for indicator in &grouping.indicators {
            self.charge_items(indicator.columns.len(), 512)?;
        }
        self.charge_fields(
            input
                .fields
                .len()
                .checked_add(aggregate.fields.len())
                .and_then(|count| count.checked_add(aggregate.measures.len()))
                .ok_or_else(|| failed("grouping-set schema width overflow"))?,
        )
    }
}

fn normalized(
    input: &Node,
    aggregate: &kernel::Spec,
    grouping: &VortexRelationalGroupingSets,
) -> Result<Spec> {
    let key_index = |name: &str| {
        aggregate
            .group_names
            .iter()
            .position(|key| key == name)
            .ok_or_else(|| {
                failed("grouping sets and indicators must reference declared grouping keys")
            })
    };
    let mut sets = Vec::with_capacity(grouping.sets.len());
    for set in &grouping.sets {
        let mut indices = set
            .iter()
            .map(|column| key_index(column.as_str()))
            .collect::<Result<Vec<_>>>()?;
        indices.sort_unstable();
        indices.dedup();
        sets.push(indices);
    }
    let mut indicators = Vec::with_capacity(grouping.indicators.len());
    for indicator in &grouping.indicators {
        validate_name(&indicator.alias)?;
        if indicator.columns.is_empty() || indicator.columns.len() > 64 {
            return Err(failed(
                "GROUPING indicators require between one and 64 grouping keys",
            ));
        }
        indicators.push(
            indicator
                .columns
                .iter()
                .map(|column| key_index(column.as_str()))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    let originals = input
        .fields
        .iter()
        .filter(|(name, _)| {
            aggregate.group_names.contains(name)
                || aggregate
                    .measures
                    .iter()
                    .any(|measure| measure.input_columns().any(|column| column == name))
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok(Spec {
        fields: originals.clone(),
        originals,
        group_sources: aggregate.group_names.clone(),
        group_fields: vec![],
        sets,
        indicators,
        filters: vec![],
        batch_metadata: 0,
    })
}

struct Names {
    used: BTreeSet<String>,
    suffix: usize,
}
impl Names {
    fn next(&mut self) -> Result<String> {
        loop {
            let name = format!("__sl_grouping_{}", self.suffix);
            self.suffix = self
                .suffix
                .checked_add(1)
                .ok_or_else(|| failed("grouping helper name overflow"))?;
            if self.used.insert(name.clone()) {
                return Ok(name);
            }
        }
    }
}

fn private_groups(
    aggregate: &kernel::Spec,
    grouping: &VortexRelationalGroupingSets,
    expansion: &mut Spec,
    names: &mut Names,
    output: &mut Vec<(String, Expression)>,
) -> Result<Vec<(String, DType)>> {
    let mut groups = Vec::new();
    for (index, (name, dtype)) in aggregate.groups.iter().enumerate() {
        let dtype = if expansion
            .sets
            .iter()
            .any(|set| set.binary_search(&index).is_err())
        {
            dtype.as_nullable()
        } else {
            dtype.clone()
        };
        let private = names.next()?;
        output.push((name.clone(), column(&private)?));
        groups.push((private, dtype));
    }
    expansion.group_fields.clone_from(&groups);
    expansion.fields.extend(groups.iter().cloned());
    let u64_type = DType::Primitive(PType::U64, Nullability::NonNullable);
    let identity = (names.next()?, u64_type.clone());
    groups.push(identity.clone());
    expansion.fields.push(identity);
    for indicator in &grouping.indicators {
        let private = names.next()?;
        output.push((indicator.alias.clone(), column(&private)?));
        groups.push((private.clone(), u64_type.clone()));
        expansion.fields.push((private, u64_type.clone()));
    }
    Ok(groups)
}

fn rewrite_filters(
    input_fields: &[(String, DType)],
    aggregate: &mut kernel::Spec,
    expansion: &mut Spec,
    names: &mut Names,
) -> Result<()> {
    for measure in &mut aggregate.measures {
        let private = if let Some((name, _)) = expansion
            .filters
            .iter()
            .find(|(_, prior)| prior == &measure.filter_column)
        {
            name.clone()
        } else {
            let private = names.next()?;
            let dtype = measure
                .filter_column
                .as_ref()
                .map(|name| field(input_fields, name).cloned())
                .transpose()?
                .unwrap_or(DType::Bool(Nullability::NonNullable));
            let dtype = if dtype == DType::Null {
                DType::Bool(Nullability::Nullable)
            } else {
                dtype
            };
            expansion.fields.push((private.clone(), dtype));
            expansion
                .filters
                .push((private.clone(), measure.filter_column.clone()));
            private
        };
        measure.filter_column = Some(private);
        if let Some(mut selection) = measure.selection.take() {
            // Ordered spill maps every retained selector field. Rebind that
            // inventory after replacing the filter dependency, retaining an
            // original filter column only when it is also an argument/key.
            let mut fields = Vec::new();
            for name in measure
                .input_columns()
                .chain(selection.order.iter().map(|key| key.column.as_str()))
            {
                if !fields.iter().any(|(prior, _)| prior == name) {
                    fields.push((name.to_owned(), field(&expansion.fields, name)?.clone()));
                }
            }
            selection.fields = fields;
            measure.selection = Some(selection);
        }
    }
    Ok(())
}
