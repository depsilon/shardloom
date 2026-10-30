//! Shared native ranking chooses bounded positions before demanding wide payload.
//! Source adapters own generation and read granularity, never ordering semantics.

use super::super::{logical_field_from_native_array as column, native_payload, result_batch};
use super::batch_input::{RankedConsumer, Source};
use super::{
    ArrayRef, DType, FieldNames, Metrics, NativeExecutionContext, Node, NodeKind, PipelineKind,
    PreparedVortexRelational, ReservedVec, Result, ScanSource, StructArray, Validity, add, failed,
    native_relational_expression, native_relational_sort, vortex_error,
};
use shardloom_core::PredicateExpr;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    dtype::{Nullability, PType},
    memory::HostMemorySessionExt as _,
};

impl PreparedVortexRelational {
    #[allow(clippy::too_many_arguments)] // Existing native operator callback contract.
    pub(super) fn run_ranked_sort(
        &self,
        node: &Node,
        context: &NativeExecutionContext<'_>,
        metrics: &Metrics<'_>,
        batch_rows: usize,
        parameter: Option<&ArrayRef>,
        output: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<bool> {
        let NodeKind::Sort {
            input,
            spec,
            output_prefix: Some(prefix),
        } = &node.kind
        else {
            return Ok(false);
        };
        let Some(producer) = metrics.input else {
            return Ok(false);
        };
        if *prefix > 16_384 || parameter.is_some() {
            return Ok(false);
        }
        let Some(scan) = source_scan(input) else {
            return Ok(false);
        };
        let NodeKind::Scan {
            source: ScanSource::Batch { index, .. },
            predicate,
            residual: None,
            plan,
            ..
        } = &scan.kind
        else {
            return Ok(false);
        };
        let declaration = &self.batch_sources[*index];
        if !declaration.usage.admits_ranked() || (plan.filter.is_some() && predicate.is_none()) {
            return Ok(false);
        }
        context.check_cancelled()?;
        if !source_has_ranked_payload(declaration, input, spec, predicate.as_ref()) {
            return Ok(false);
        }
        // All temporary plan/schema copies are bounded by the already admitted
        // binding, plus the one private position field. This optional admission
        // must not prevent the ordinary native route from using its own budget.
        // No source has opened, and no execution error is caught here.
        let Ok(_metadata) = context.memory().reserve(
            self.binding_metadata
                .bytes()
                .checked_add(8192)
                .ok_or_else(|| failed("ranked binding metadata overflow"))?,
        ) else {
            return Ok(false);
        };
        let mapping = spec
            .fields
            .iter()
            .map(|(name, _)| {
                source_column(input, name)
                    .map(str::to_owned)
                    .ok_or_else(|| failed("ranked direct-column binding changed"))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rank_columns = Vec::new();
        for name in &spec.names {
            let index = spec
                .fields
                .iter()
                .position(|(field, _)| field == name)
                .ok_or_else(|| failed("ranked key is absent"))?;
            if !rank_columns.contains(&mapping[index]) {
                rank_columns.push(mapping[index].clone());
            }
        }
        if let Some(predicate) = predicate {
            predicate_columns(predicate, &mut rank_columns);
        }
        let mut position_name = "__shardloom_source_position".to_owned();
        let source_fields = declaration.schema.dtype().as_struct_fields();
        while source_fields.field(&position_name).is_some()
            || spec.fields.iter().any(|(name, _)| name == &position_name)
        {
            position_name.push('_');
        }
        let rank_spec = ranking_spec(spec, &position_name);
        let rank =
            native_relational_sort::StreamingPrefix::try_new(&rank_spec, Some(*prefix), context)?
                .ok_or_else(|| failed("ranked prefix lost its native ordering admission"))?;
        let mut consumer = Consumer {
            rank: Some(rank),
            result: native_relational_sort::Sort::new(spec, context.memory())?,
            rank_spec: &rank_spec,
            output_spec: spec,
            mapping: &mapping,
            position_name: &position_name,
            predicate: predicate.as_ref(),
            filter: None,
            positions: ReservedVec::new(context.memory())?,
            selected: false,
            delivered: 0,
            metrics,
        };
        if !producer.run_ranked(*index, &rank_columns, *prefix, context, &mut consumer)? {
            return Ok(false);
        }
        consumer
            .result
            .finish_prefix(context, batch_rows, Some(*prefix), true, output)?;
        Ok(true)
    }
}

fn source_has_ranked_payload(
    declaration: &Source,
    input: &Node,
    spec: &native_relational_sort::Spec,
    predicate: Option<&PredicateExpr>,
) -> bool {
    declaration.local.as_ref().is_some_and(|source| {
        source.can_rank_columns(|name| {
            spec.names
                .iter()
                .any(|key| source_column(input, key) == Some(name))
                || predicate.is_some_and(|predicate| predicate_uses_column(predicate, name))
        })
    })
}

fn source_scan(mut node: &Node) -> Option<&Node> {
    loop {
        match &node.kind {
            NodeKind::Project { input, expressions } => {
                if !direct(expressions) {
                    return None;
                }
                node = input;
            }
            NodeKind::Pipeline { input, stages } => {
                if !stages.iter().all(|stage| matches!(&stage.kind, PipelineKind::Project(expressions) if direct(expressions))) {
                    return None;
                }
                node = input;
            }
            NodeKind::Scan { .. } => return Some(node),
            _ => return None,
        }
    }
}

fn direct(expressions: &[native_relational_expression::Expression]) -> bool {
    expressions.iter().all(|expression| {
        matches!(
            expression.kind,
            native_relational_expression::Kind::Column(_)
        )
    })
}

fn source_column<'a>(mut node: &'a Node, mut name: &'a str) -> Option<&'a str> {
    loop {
        match &node.kind {
            NodeKind::Project { input, expressions } => {
                name = projected_name(&node.fields, expressions, name)?;
                node = input;
            }
            NodeKind::Pipeline { input, stages } => {
                for stage in stages.iter().rev() {
                    let PipelineKind::Project(expressions) = &stage.kind else {
                        return None;
                    };
                    name = projected_name(&stage.fields, expressions, name)?;
                }
                node = input;
            }
            NodeKind::Scan { .. } => {
                return node
                    .fields
                    .iter()
                    .find(|(field, _)| field == name)
                    .map(|(field, _)| field.as_str());
            }
            _ => return None,
        }
    }
}

fn projected_name<'a>(
    fields: &[(String, DType)],
    expressions: &'a [native_relational_expression::Expression],
    name: &str,
) -> Option<&'a str> {
    let index = fields.iter().position(|(field, _)| field == name)?;
    let native_relational_expression::Kind::Column(name) = &expressions.get(index)?.kind else {
        return None;
    };
    Some(name)
}

fn predicate_columns(predicate: &PredicateExpr, columns: &mut Vec<String>) {
    if let Some(column) = predicate.column()
        && !columns.iter().any(|name| name == column.as_str())
    {
        columns.push(column.as_str().to_owned());
    }
    if let PredicateExpr::And(children) = predicate {
        for child in children {
            predicate_columns(child, columns);
        }
    }
}

fn predicate_uses_column(predicate: &PredicateExpr, name: &str) -> bool {
    predicate
        .column()
        .is_some_and(|column| column.as_str() == name)
        || matches!(predicate, PredicateExpr::And(children) if children.iter().any(|child| predicate_uses_column(child, name)))
}

struct Consumer<'a, 'm> {
    rank: Option<native_relational_sort::StreamingPrefix<'a>>,
    result: native_relational_sort::Sort<'a>,
    rank_spec: &'a native_relational_sort::Spec,
    output_spec: &'a native_relational_sort::Spec,
    mapping: &'a [String],
    position_name: &'a str,
    predicate: Option<&'a PredicateExpr>,
    filter: Option<vortex::expr::BoundExpression>,
    positions: ReservedVec<u64>,
    selected: bool,
    delivered: usize,
    metrics: &'a Metrics<'m>,
}

impl RankedConsumer for Consumer<'_, '_> {
    fn rank(
        &mut self,
        array: &ArrayRef,
        first_row: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let positions = result_batch::build_numeric_column::<u64>(
            &dtype,
            array.len(),
            &context.native_session().host_allocator(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                first_row
                    .checked_add(row as u64)
                    .map(Some)
                    .ok_or_else(|| failed("source position overflow"))
            },
        )?;
        let fields = array.dtype().as_struct_fields();
        let mut names: Vec<&str> = fields.names().iter().map(AsRef::as_ref).collect();
        let mut columns = names
            .iter()
            .map(|name| column(array, name))
            .collect::<Result<Vec<_>>>()?;
        names.push(self.position_name);
        columns.push(positions);
        let mut input = StructArray::try_new(
            FieldNames::from(names),
            columns,
            array.len(),
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        if let Some(predicate) = self.predicate {
            if self.filter.is_none() {
                self.filter = Some(
                    super::super::predicate_to_vortex_expr(
                        predicate,
                        input.dtype(),
                        super::super::VortexQueryPrimitiveKind::FilterAndProject,
                    )?
                    .bind(input.dtype())
                    .and_then(|bound| bound.optimize_recursive())
                    .map_err(vortex_error)?,
                );
            }
            input = crate::resident_memory_source::filter_bound_array(
                &input,
                self.filter.as_ref().expect("bound rank predicate"),
                &mut context.native_session().create_execution_ctx(),
                &context.native_session().host_allocator(),
            )?;
        }
        let mut columns = Vec::with_capacity(self.rank_spec.fields.len());
        for (name, _) in &self.rank_spec.fields {
            if name == self.position_name {
                columns.push(column(&input, name)?);
            } else {
                let index = self
                    .output_spec
                    .fields
                    .iter()
                    .position(|(field, _)| field == name)
                    .ok_or_else(|| failed("ranked key mapping is absent"))?;
                columns.push(column(&input, &self.mapping[index])?);
            }
        }
        let ranked = record(&self.rank_spec.fields, columns, input.len())?;
        let rows = ranked.len() as u64;
        let copied = self
            .rank
            .as_mut()
            .ok_or_else(|| failed("ranking already completed"))?
            .build(ranked, context)?;
        self.metrics
            .record_ordering_copy(copied.rows, copied.buffer_bytes)?;
        add(&self.metrics.ordering_prefix_batches, 1)?;
        add(&self.metrics.ordering_prefix_rows_validated, rows)?;
        self.metrics.ordering_prefix_peak_retained_buffer_bytes.set(
            self.metrics
                .ordering_prefix_peak_retained_buffer_bytes
                .get()
                .max(copied.retained_buffer_bytes),
        );
        add(&self.metrics.scans_started, 1)?;
        add(&self.metrics.data_scans, u64::from(!array.is_empty()))?;
        add(&self.metrics.scan_batches, 1)?;
        add(&self.metrics.scan_rows, rows)
    }

    fn select(&mut self, context: &NativeExecutionContext<'_>) -> Result<&[u64]> {
        if self.selected {
            return Err(failed("ranking selection was requested twice"));
        }
        let rank = self
            .rank
            .take()
            .ok_or_else(|| failed("ranking state is absent"))?;
        rank.finish(context, 8192, &mut |array| {
            let positions =
                native_relational_expression::keys(&column(&array, self.position_name)?, context)?;
            for row in 0..array.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let super::super::native_relational_keys::Cell::NonnegativeInteger(position) =
                    positions.cell(row)?
                else {
                    return Err(failed("ranked source position changed its UInt64 domain"));
                };
                self.positions.push(position)?;
            }
            Ok(())
        })?;
        self.positions.values.sort_unstable();
        if self
            .positions
            .values
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(failed("ranked source positions are not unique"));
        }
        self.selected = true;
        context.check_cancelled()?;
        Ok(&self.positions.values)
    }

    fn payload(
        &mut self,
        array: &ArrayRef,
        first_row: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if !self.selected {
            return Err(failed("payload arrived before ranking completed"));
        }
        let end = first_row
            .checked_add(array.len() as u64)
            .ok_or_else(|| failed("payload extent overflow"))?;
        let start_index = self
            .positions
            .values
            .partition_point(|row| *row < first_row);
        let end_index = self.positions.values.partition_point(|row| *row < end);
        if start_index == end_index {
            return Ok(());
        }
        // Adapter batches and winner positions both arrive in source order.
        if start_index != self.delivered {
            return Err(failed("ranked payload skipped or repeated a selected row"));
        }
        let mut selection = super::LocalSelection::new(array.len(), context)?;
        for &row in &self.positions.values[start_index..end_index] {
            selection.push(usize::try_from(row - first_row).map_err(vortex_error)?)?;
        }
        let columns = self
            .mapping
            .iter()
            .map(|name| column(array, name))
            .collect::<Result<Vec<_>>>()?;
        let projected = record(&self.output_spec.fields, columns, array.len())?;
        selection.for_each_batch(
            &projected,
            &self.output_spec.fields,
            array.len().max(1),
            context,
            &mut |selected| {
                let retained = native_payload::detach(&selected, context)?;
                self.metrics
                    .record_ordering_copy(retained.len(), retained.nbytes())?;
                self.result.build(retained, context)
            },
        )?;
        self.delivered = end_index;
        add(&self.metrics.scan_batches, 1)?;
        add(&self.metrics.scan_rows, (end_index - start_index) as u64)
    }

    fn complete(&self) -> Result<()> {
        if !self.selected || self.delivered != self.positions.values.len() {
            return Err(failed(
                "ranked payload did not deliver every selected position",
            ));
        }
        Ok(())
    }
}

fn record(fields: &[(String, DType)], columns: Vec<ArrayRef>, rows: usize) -> Result<ArrayRef> {
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        rows,
        Validity::NonNullable,
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(vortex_error)
}

fn ranking_spec(
    spec: &native_relational_sort::Spec,
    position_name: &str,
) -> native_relational_sort::Spec {
    let mut fields = spec
        .fields
        .iter()
        .filter(|(name, _)| spec.names.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    fields.push((
        position_name.to_owned(),
        DType::Primitive(PType::U64, Nullability::NonNullable),
    ));
    native_relational_sort::Spec {
        fields,
        keys: spec.keys.clone(),
        names: spec.names.clone(),
        copy_policy: native_payload::CopyPolicy::ValidateValues,
    }
}
