//! Lossless schema and source binding before native relational row execution.

use super::{
    BoundUnary, DType, MaterializedPredicateEvaluator, MemoryLease, Node, NodeKind, Nullability,
    PathBuf, PreparedVortexSource, ReservedVec, ResidentMemorySource, ResidentVortexSession,
    Result, ScanSource, SetKind, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    VortexRelationalPlan, failed, native_relational_join, native_relational_sort, vortex_error,
};
use crate::relational_query::{
    VortexRelationalJoin, VortexRelationalJoinKind as JoinKind, VortexRelationalScan,
    VortexRelationalSet, VortexRelationalSide as Side,
};
use shardloom_core::{PredicateExpr, StatValue};
use vortex::array::dtype::PType;

#[path = "local_primitive_relational_aggregate_bind.rs"]
mod aggregate;
#[path = "local_primitive_relational_dynamic_bind.rs"]
mod dynamic;
#[path = "local_primitive_relational_expression_bind.rs"]
mod expression;
pub(in crate::local_primitives) use expression::{arithmetic_dtype, literal_dtype};
#[path = "local_primitive_relational_subquery_bind.rs"]
mod subquery;
pub(super) use subquery::validate_relation as validate_subquery_relation;
#[path = "local_primitive_relational_transform_bind.rs"]
mod transform;
#[path = "local_primitive_relational_window_bind.rs"]
mod window;

pub(super) struct Binder<'a> {
    session: &'a ResidentVortexSession,
    sources: ReservedVec<PreparedVortexSource>,
    paths: ReservedVec<PathBuf>,
    memory_sources: ReservedVec<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    batch_source: Option<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    metadata: MemoryLease,
    outer_fields: Option<Vec<(String, DType)>>,
    parameterized_binding: bool,
    execution: Option<dynamic::ExecutionBinding<'a>>,
    resolved: ReservedVec<Option<Node>>,
    scope: std::sync::Arc<()>,
    deferred: ReservedVec<Option<Box<super::DynamicLowerer>>>,
}

pub(super) struct BoundSources {
    pub(super) sources: Vec<PreparedVortexSource>,
    pub(super) source_paths: Vec<PathBuf>,
    pub(super) memory_sources: Vec<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    pub(super) batch_source: Option<(shardloom_core::DatasetUri, ResidentMemorySource)>,
    pub(super) metadata: MemoryLease,
}

#[derive(Clone, Copy)]
enum SourceIndex {
    File(usize),
    Memory(usize),
    Batch,
}

impl<'a> Binder<'a> {
    pub(super) fn shared_memory_pool(&self) -> shardloom_exec::live_memory::LiveMemoryPool {
        self.session.memory().clone()
    }

    pub(super) fn reserve_input_scratch(&self, bytes: u64) -> Result<MemoryLease> {
        self.session.reserve_input_scratch(bytes)
    }

    pub(super) fn new(session: &'a ResidentVortexSession) -> Result<Self> {
        Ok(Self {
            session,
            sources: ReservedVec::new(session.memory())?,
            paths: ReservedVec::new(session.memory())?,
            memory_sources: ReservedVec::new(session.memory())?,
            batch_source: None,
            metadata: session.memory().reserve(4096)?,
            outer_fields: None,
            parameterized_binding: false,
            execution: None,
            resolved: ReservedVec::new(session.memory())?,
            scope: std::sync::Arc::new(()),
            deferred: ReservedVec::new(session.memory())?,
        })
    }

    pub(super) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.metadata.resize(
            self.metadata
                .bytes()
                .checked_add(u64::try_from(bytes).map_err(vortex_error)?)
                .ok_or_else(|| failed("plan metadata size overflow"))?,
        )
    }

    pub(super) fn charge_items(&mut self, count: usize, per_item: usize) -> Result<()> {
        self.charge(
            count
                .checked_mul(per_item)
                .ok_or_else(|| failed("plan metadata size overflow"))?,
        )
    }

    pub(super) fn charge_fields(&mut self, count: usize) -> Result<()> {
        self.charge_items(count, 4096)
    }

    pub(super) fn finish(mut self) -> Result<BoundSources> {
        let (sources, mut source_credit) = self.sources.into_parts();
        let (paths, mut path_credit) = self.paths.into_parts();
        let (memory_sources, mut memory_credit) = self.memory_sources.into_parts();
        self.metadata.absorb(&mut source_credit)?;
        self.metadata.absorb(&mut path_credit)?;
        self.metadata.absorb(&mut memory_credit)?;
        Ok(BoundSources {
            sources,
            source_paths: paths,
            memory_sources,
            batch_source: self.batch_source,
            metadata: self.metadata,
        })
    }

    pub(super) fn register_memory_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        build: impl FnOnce(&ResidentVortexSession) -> Result<ResidentMemorySource>,
    ) -> Result<()> {
        if self.execution.is_some()
            || self
                .batch_source
                .as_ref()
                .is_some_and(|(prior, _)| *prior == uri)
            || !uri.as_str().starts_with("memory://")
            || uri.as_str().len() <= "memory://".len()
            || uri.as_str().len() > 16_384
            || self
                .memory_sources
                .values
                .iter()
                .any(|(prior, _)| *prior == uri)
            || self.source_count() >= 128
        {
            return Err(failed(
                "memory source requires a unique bounded memory URI before execution",
            ));
        }
        self.charge(uri.as_str().len() * 8 + 4096)?;
        self.memory_sources.reserve_one()?;
        let source = build(self.session)?;
        if !source.belongs_to_session(self.session) {
            return Err(failed("memory source belongs to another resource owner"));
        }
        self.memory_sources.values.push((uri, source));
        Ok(())
    }

    pub(super) fn register_batch_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        build_schema: impl FnOnce(&ResidentVortexSession) -> Result<ResidentMemorySource>,
    ) -> Result<()> {
        if self.execution.is_some()
            || self.batch_source.is_some()
            || self
                .memory_sources
                .values
                .iter()
                .any(|(prior, _)| *prior == uri)
            || self.source_count() >= 128
            || !uri.as_str().starts_with("memory://")
            || uri.as_str().len() <= "memory://".len()
            || uri.as_str().len() > 16_384
        {
            return Err(super::batch_input::failed(
                "streaming input requires one unique declared batch source before execution",
            ));
        }
        self.charge(uri.as_str().len() * 8 + 4096)?;
        let schema = build_schema(self.session)?;
        if !schema.belongs_to_session(self.session)
            || !schema.is_batch_source()
            || schema.row_count() != 0
        {
            return Err(super::batch_input::failed(
                "streaming schema requires an empty from_batch_columns source from this session",
            ));
        }
        self.batch_source = Some((uri, schema));
        Ok(())
    }

    pub(super) fn validate_batch_plan(&self, plan: &VortexRelationalPlan) -> Result<()> {
        if let Some((uri, _)) = &self.batch_source {
            super::batch_input::classify(plan, uri)?;
        }
        Ok(())
    }

    fn source_count(&self) -> usize {
        self.sources.values.len()
            + self.memory_sources.values.len()
            + usize::from(self.batch_source.is_some())
    }

    pub(super) fn reject_dynamic_batch_input(&self) -> Result<()> {
        if self.batch_source.is_some() {
            return Err(super::batch_input::failed(
                "dynamic schemas cannot consume streaming input; choose explicit resident mode",
            ));
        }
        Ok(())
    }

    fn input(&mut self, uri: &shardloom_core::DatasetUri) -> Result<SourceIndex> {
        if self
            .batch_source
            .as_ref()
            .is_some_and(|(declared, _)| declared == uri)
        {
            return Ok(SourceIndex::Batch);
        }
        if let Some(index) = self
            .memory_sources
            .values
            .iter()
            .position(|(prior, _)| prior == uri)
        {
            return Ok(SourceIndex::Memory(index));
        }
        self.source(uri).map(SourceIndex::File)
    }

    fn source_dtype(&self, source: SourceIndex) -> &DType {
        match source {
            SourceIndex::File(index) => self.sources.values[index].dtype(),
            SourceIndex::Memory(index) => self.memory_sources.values[index].1.dtype(),
            SourceIndex::Batch => self
                .batch_source
                .as_ref()
                .expect("registered batch source")
                .1
                .dtype(),
        }
    }

    pub(super) fn bind(&mut self, input: &VortexRelationalPlan, depth: usize) -> Result<Node> {
        if depth > 24 {
            return Err(failed("recursive plan exceeds 24 levels"));
        }
        // Total operator count grows under the shared metadata lease. The
        // independent recursion guard remains until traversal is iterative.
        self.charge(4096)?;
        match input {
            VortexRelationalPlan::DeferredSubquery(_) => Err(failed(
                "deferred schema declarations require a correlated subquery relation",
            )),
            VortexRelationalPlan::ExecutionResult(reference) => self.take_resolved(reference),
            VortexRelationalPlan::Scan(scan) => self.scan(scan),
            VortexRelationalPlan::Join(join) => self.join(join, depth),
            VortexRelationalPlan::Set(set) => self.set(set, depth),
            VortexRelationalPlan::Window(window) => self.window(window, depth),
            VortexRelationalPlan::Subquery(subquery) => self.subquery(subquery, depth, false),
            VortexRelationalPlan::CorrelatedSubquery(subquery) => {
                self.subquery(subquery, depth, true)
            }
            VortexRelationalPlan::Outer => {
                let width = self
                    .outer_fields
                    .as_ref()
                    .ok_or_else(|| failed("outer row source requires a correlated subquery scope"))?
                    .len();
                self.charge_fields(width)?;
                Ok(Node {
                    fields: self
                        .outer_fields
                        .as_ref()
                        .ok_or_else(|| failed("outer row schema is absent"))?
                        .clone(),
                    kind: NodeKind::Outer,
                })
            }
            VortexRelationalPlan::Project(project) => self.project(project, depth),
            VortexRelationalPlan::Filter(filter) => self.filter(filter, depth),
            VortexRelationalPlan::Sort(sort) => self.sort(sort, depth),
            VortexRelationalPlan::Limit(limit) => self.limit(limit, depth),
            VortexRelationalPlan::Aggregate(aggregate) => self.aggregate(aggregate, depth),
            VortexRelationalPlan::Unary(unary) => {
                let input = Box::new(self.bind(&unary.input, depth + 1)?);
                self.charge_fields(input.fields.len())?;
                let operation = BoundUnary::for_relation(
                    &unary.request,
                    &DType::struct_(input.fields.clone(), Nullability::NonNullable),
                    self.session.memory(),
                )?;
                if unary.request.kind == VortexQueryPrimitiveKind::PivotRows {
                    return self.complete_pivot(&input, operation);
                }
                validate_width(operation.fields().len())?;
                self.charge_fields(operation.fields().len())?;
                for (name, dtype) in operation.fields() {
                    validate_name(name)?;
                    if unary.request.kind == VortexQueryPrimitiveKind::ExplodeRows {
                        validate_payload(dtype)?;
                    } else {
                        validate_key(dtype)?;
                    }
                }
                validate_unique(operation.fields())?;
                Ok(Node {
                    fields: operation.fields().to_vec(),
                    kind: NodeKind::Unary {
                        input,
                        operation: Box::new(operation),
                    },
                })
            }
        }
    }

    pub(super) fn seed_source(
        &mut self,
        uri: &shardloom_core::DatasetUri,
        source: PreparedVortexSource,
    ) -> Result<()> {
        if uri.as_str().len() > 16_384 {
            return Err(failed("source URI exceeds 16384 bytes"));
        }
        let request = VortexQueryPrimitiveRequest::project(
            uri.clone(),
            shardloom_plan::ProjectionRequest::All,
        );
        let session = super::super::prepared_dispatch::source_session(&source, &request, None)?;
        if !session.same_owner(self.session) {
            return Err(failed("retained source belongs to another resource owner"));
        }
        let path = super::super::local_vortex_path(uri, request.kind)?
            .ok_or_else(|| failed("relational scans require local Vortex input"))?;
        let path = std::path::absolute(path).map_err(vortex_error)?;
        self.charge(
            uri.as_str()
                .len()
                .checked_mul(8)
                .ok_or_else(|| failed("source metadata overflow"))?,
        )?;
        self.paths.reserve_one()?;
        self.sources.reserve_one()?;
        self.paths.values.push(path);
        self.sources.values.push(source);
        Ok(())
    }

    pub(super) fn source(&mut self, uri: &shardloom_core::DatasetUri) -> Result<usize> {
        if uri.as_str().len() > 16_384 {
            return Err(failed("source URI exceeds 16384 bytes"));
        }
        self.charge(uri.as_str().len() * 8)?;
        let path = super::super::local_vortex_path(uri, VortexQueryPrimitiveKind::ProjectColumns)?
            .ok_or_else(|| failed("relational scans require local Vortex input"))?;
        let path = std::path::absolute(path).map_err(vortex_error)?;
        let source = if let Some(index) = self.paths.values.iter().position(|prior| prior == &path)
        {
            index
        } else {
            if self.execution.is_some() {
                return Err(failed(
                    "execution-time binding cannot add an undeclared source",
                ));
            }
            if self.source_count() >= 128 {
                return Err(failed("relational preparation exceeds 128 sources"));
            }
            self.paths.reserve_one()?;
            self.sources.reserve_one()?;
            let source = self.session.prepare_file(&path)?;
            self.paths.values.push(path);
            self.sources.values.push(source);
            self.sources.values.len() - 1
        };
        Ok(source)
    }

    pub(super) fn source_columns(
        &mut self,
        uri: &shardloom_core::DatasetUri,
    ) -> Result<Vec<String>> {
        let source = self.input(uri)?;
        let fields = self
            .source_dtype(source)
            .as_struct_fields_opt()
            .ok_or_else(|| failed("relational source requires a struct schema"))?;
        self.charge(
            fields
                .names()
                .len()
                .checked_mul(4096)
                .ok_or_else(|| failed("source metadata size overflow"))?,
        )?;
        let fields = self
            .source_dtype(source)
            .as_struct_fields_opt()
            .ok_or_else(|| failed("relational source schema is absent"))?;
        fields
            .names()
            .iter()
            .map(|name| {
                validate_name(name.as_ref())?;
                Ok(name.to_string())
            })
            .collect()
    }

    fn scan(&mut self, scan: &VortexRelationalScan) -> Result<Node> {
        if let Some(predicate) = &scan.predicate {
            self.charge(predicate_bytes(predicate, 0)?)?;
        }
        if let shardloom_plan::ProjectionRequest::Columns(columns) = &scan.projection {
            validate_width(columns.len())?;
            for column in columns {
                validate_name(column.as_str())?;
            }
            self.charge_fields(columns.len())?;
        }
        let source = self.input(&scan.source_uri)?;
        let dtype = self.source_dtype(source);
        let source_fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| failed("relational source requires a struct schema"))?;
        let schema_bytes = source_fields
            .names()
            .len()
            .checked_mul(4096)
            .ok_or_else(|| failed("source schema metadata overflow"))?;
        self.charge(schema_bytes)?;
        let dtype = self.source_dtype(source);
        if let Some(predicate) = &scan.predicate {
            validate_predicate_fields(predicate, dtype)?;
        }
        let mut request =
            VortexQueryPrimitiveRequest::project(scan.source_uri.clone(), scan.projection.clone());
        request.predicate.clone_from(&scan.predicate);
        if request.predicate.is_some() {
            request.kind = VortexQueryPrimitiveKind::FilterAndProject;
        }
        let plan = super::super::row_export_scan_plan(&request, dtype)?;
        let names = super::super::projected_column_names(dtype, &scan.projection, request.kind)?;
        validate_width(names.len())?;
        let fields = names
            .iter()
            .map(|name| {
                validate_name(name)?;
                let dtype = super::super::completed_result::source_field(dtype, name)?;
                validate_payload(&dtype)?;
                Ok((name.clone(), dtype))
            })
            .collect::<Result<Vec<_>>>()?;
        validate_unique(&fields)?;
        let columns = if plan.projected_columns.is_empty() {
            super::super::local_field_names(dtype, request.kind)?
        } else {
            plan.projected_columns.clone()
        };
        let residual = plan
            .residual_predicate
            .as_ref()
            .map(|predicate| MaterializedPredicateEvaluator::compile(predicate, &columns))
            .transpose()?;
        let source = match source {
            SourceIndex::File(index) => ScanSource::File(index),
            SourceIndex::Memory(index) => ScanSource::Memory(Box::new(
                self.memory_sources.values[index].1.prepare_projection(
                    &columns.iter().map(String::as_str).collect::<Vec<_>>(),
                    plan.filter.clone(),
                    None,
                )?,
            )),
            SourceIndex::Batch => ScanSource::Batch(Box::new(
                self.batch_source
                    .as_ref()
                    .expect("registered batch source")
                    .1
                    .prepare_projection(
                        &columns.iter().map(String::as_str).collect::<Vec<_>>(),
                        plan.filter.clone(),
                        None,
                    )?,
            )),
        };
        for (_, dtype) in &fields {
            self.charge(
                usize::try_from(super::super::native_payload::metadata_bytes(dtype)?)
                    .map_err(vortex_error)?,
            )?;
        }
        Ok(Node {
            fields,
            kind: NodeKind::Scan {
                source,
                plan,
                columns,
                residual,
            },
        })
    }

    fn join(&mut self, join: &VortexRelationalJoin, depth: usize) -> Result<Node> {
        validate_width(join.columns.len())?;
        if join.keys.len() > 128
            || (join.kind == JoinKind::Cross && (!join.keys.is_empty() || join.condition.is_some()))
            || (join.kind != JoinKind::Cross && join.keys.is_empty() && join.condition.is_none())
        {
            return Err(failed(
                "cross joins require no ON clause; other joins require keys or a native ON predicate",
            ));
        }
        for key in &join.keys {
            validate_name(key.left.as_str())?;
            validate_name(key.right.as_str())?;
        }
        for column in &join.columns {
            validate_name(&column.output_column)?;
            validate_name(column.column.as_str())?;
        }
        self.charge_items(join.keys.len(), 8192)?;
        self.charge_items(join.columns.len(), 8192)?;
        let left = Box::new(self.bind(&join.left, depth + 1)?);
        let right = Box::new(self.bind(&join.right, depth + 1)?);
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for key in &join.keys {
            let left_dtype = field(&left.fields, key.left.as_str())?;
            let right_dtype = field(&right.fields, key.right.as_str())?;
            validate_key_pair(left_dtype, right_dtype)?;
            left_keys.push(key.left.as_str().to_owned());
            right_keys.push(key.right.as_str().to_owned());
        }
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for column in &join.columns {
            if column.side == Side::Right
                && matches!(join.kind, JoinKind::LeftSemi | JoinKind::LeftAnti)
            {
                return Err(failed("semi and anti joins expose left columns only"));
            }
            let dtype = field(
                if column.side == Side::Left {
                    &left.fields
                } else {
                    &right.fields
                },
                column.column.as_str(),
            )?;
            let extend_null = match column.side {
                Side::Left => matches!(join.kind, JoinKind::Right | JoinKind::Full),
                Side::Right => matches!(join.kind, JoinKind::Left | JoinKind::Full),
            };
            fields.push((
                column.output_column.clone(),
                if extend_null {
                    dtype.as_nullable()
                } else {
                    dtype.clone()
                },
            ));
            columns.push((column.side, column.column.as_str().to_owned()));
        }
        validate_unique(&fields)?;
        let condition =
            self.join_condition(join.condition.as_ref(), &left.fields, &right.fields)?;
        let spec = native_relational_join::Spec {
            kind: join.kind,
            left_keys,
            right_keys,
            fields: fields.clone(),
            columns,
            condition,
        };
        Ok(Node {
            fields,
            kind: NodeKind::Join { left, right, spec },
        })
    }

    fn set(&mut self, set: &VortexRelationalSet, depth: usize) -> Result<Node> {
        let left = Box::new(self.bind(&set.left, depth + 1)?);
        let right = Box::new(self.bind(&set.right, depth + 1)?);
        if left.fields.len() != right.fields.len() {
            return Err(failed("set branches have different column counts"));
        }
        self.charge_fields(left.fields.len())?;
        let fields = left
            .fields
            .iter()
            .zip(&right.fields)
            .map(|((name, left), (_, right))| Ok((name.clone(), common_dtype(left, right)?)))
            .collect::<Result<Vec<_>>>()?;
        if set.kind != SetKind::UnionAll {
            for (_, dtype) in &fields {
                validate_key(dtype)?;
            }
        }
        let names = fields.iter().map(|(name, _)| name.clone()).collect();
        Ok(Node {
            fields,
            kind: NodeKind::Set {
                left,
                right,
                kind: set.kind,
                names,
            },
        })
    }
}

fn validate_width(width: usize) -> Result<()> {
    if width == 0 {
        return Err(failed("relational schema requires at least one field"));
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(failed("column names require 1 through 256 bytes"));
    }
    Ok(())
}

fn validate_unique(fields: &[(String, DType)]) -> Result<()> {
    // Binder field admission precedes this temporary, borrowed-name index.
    let mut names = std::collections::BTreeSet::new();
    for (name, _) in fields {
        if !names.insert(name) {
            return Err(failed("output column names must be unique"));
        }
    }
    Ok(())
}

fn validate_scalar(dtype: &DType) -> Result<()> {
    if !matches!(
        dtype,
        DType::Bool(_) | DType::Utf8(_) | DType::Primitive(_, _)
    ) || matches!(dtype, DType::Primitive(PType::F16, _))
    {
        return Err(failed(
            "operated scalar requires bool, integer, F32/F64 or UTF8 fields",
        ));
    }
    Ok(())
}

fn validate_payload(dtype: &DType) -> Result<()> {
    super::super::native_payload::metadata_bytes(dtype).map(|_| ())
}

fn validate_key(dtype: &DType) -> Result<()> {
    if super::super::native_payload::is_nested(dtype) {
        validate_payload(dtype)
    } else {
        validate_flat_operand(dtype)
    }
}

fn validate_flat_operand(dtype: &DType) -> Result<()> {
    match dtype {
        DType::Binary(_) => Ok(()),
        DType::Decimal(decimal, _) if crate::native_payload_schema::admitted_decimal(*decimal) => {
            Ok(())
        }
        DType::Extension(_) if crate::native_payload_schema::temporal_storage(dtype).is_some() => {
            Ok(())
        }
        _ => validate_scalar(dtype),
    }
}

fn validate_predicate_fields(predicate: &PredicateExpr, dtype: &DType) -> Result<()> {
    if let Some(column) = predicate.column() {
        validate_scalar(&super::super::completed_result::source_field(
            dtype,
            column.as_str(),
        )?)?;
    }
    if let PredicateExpr::And(children) = predicate {
        for child in children {
            validate_predicate_fields(child, dtype)?;
        }
    }
    Ok(())
}

fn field<'a>(fields: &'a [(String, DType)], name: &str) -> Result<&'a DType> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, dtype)| dtype)
        .ok_or_else(|| failed(&format!("column '{name}' is absent from the bound input")))
}

fn integer(ptype: PType) -> Option<(bool, u8)> {
    Some(match ptype {
        PType::I8 => (true, 8),
        PType::I16 => (true, 16),
        PType::I32 => (true, 32),
        PType::I64 => (true, 64),
        PType::U8 => (false, 8),
        PType::U16 => (false, 16),
        PType::U32 => (false, 32),
        PType::U64 => (false, 64),
        _ => return None,
    })
}

fn validate_key_pair(left: &DType, right: &DType) -> Result<()> {
    validate_key(left)?;
    validate_key(right)?;
    if left.as_nonnullable() == right.as_nonnullable() {
        return Ok(());
    }
    if super::super::native_payload::is_nested(left) && left.eq_ignore_nullability(right) {
        return Ok(());
    }
    if let (DType::Primitive(left, _), DType::Primitive(right, _)) = (left, right)
        && (integer(*left).is_some() || matches!(left, PType::F32 | PType::F64))
        && (integer(*right).is_some() || matches!(right, PType::F32 | PType::F64))
    {
        return Ok(());
    }
    Err(failed(
        "incompatible key types require an explicit lossless cast",
    ))
}

fn common_dtype(left: &DType, right: &DType) -> Result<DType> {
    let nullability = if left.is_nullable() || right.is_nullable() {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    if left.as_nonnullable() == right.as_nonnullable() {
        return Ok(left.with_nullability(nullability));
    }
    if let (DType::Primitive(left, _), DType::Primitive(right, _)) = (left, right) {
        if matches!(left, PType::F32 | PType::F64) && matches!(right, PType::F32 | PType::F64) {
            return Ok(DType::Primitive(PType::F64, nullability));
        }
        if let (Some((left_signed, left_bits)), Some((right_signed, right_bits))) =
            (integer(*left), integer(*right))
        {
            let signed = left_signed || right_signed;
            let bits = if left_signed == right_signed {
                left_bits.max(right_bits)
            } else if left_signed {
                left_bits.max(right_bits + 1)
            } else {
                right_bits.max(left_bits + 1)
            };
            let ptype = match (signed, bits) {
                (false, 0..=8) => PType::U8,
                (false, 9..=16) => PType::U16,
                (false, 17..=32) => PType::U32,
                (false, 33..=64) => PType::U64,
                (true, 0..=8) => PType::I8,
                (true, 9..=16) => PType::I16,
                (true, 17..=32) => PType::I32,
                (true, 33..=64) => PType::I64,
                _ => {
                    return Err(failed(
                        "set integer domains have no lossless native common dtype",
                    ));
                }
            };
            return Ok(DType::Primitive(ptype, nullability));
        }
    }
    Err(failed(
        "set branches require a lossless common scalar schema",
    ))
}

fn predicate_bytes(predicate: &PredicateExpr, depth: usize) -> Result<usize> {
    if depth > 24 {
        return Err(failed("predicate nesting exceeds 24 levels"));
    }
    let mut bytes = 4096usize;
    let mut add = |value: usize| -> Result<()> {
        bytes = bytes
            .checked_add(value)
            .ok_or_else(|| failed("predicate metadata overflow"))?;
        Ok(())
    };
    if let Some(column) = predicate.column() {
        validate_name(column.as_str())?;
        add(column.as_str().len() * 8)?;
    }
    match predicate {
        PredicateExpr::And(predicates) => {
            for predicate in predicates {
                add(predicate_bytes(predicate, depth + 1)?)?;
            }
        }
        PredicateExpr::Compare {
            value: StatValue::Utf8(value),
            ..
        } => add(value
            .len()
            .checked_mul(8)
            .ok_or_else(|| failed("predicate string overflow"))?)?,
        PredicateExpr::StringContains { needle, .. } => add(needle
            .len()
            .checked_mul(8)
            .ok_or_else(|| failed("predicate string overflow"))?)?,
        PredicateExpr::InList { values, .. } => {
            add(values
                .len()
                .checked_mul(256)
                .ok_or_else(|| failed("predicate list overflow"))?)?;
            for value in values {
                if let StatValue::Utf8(value) = value {
                    add(value
                        .len()
                        .checked_mul(8)
                        .ok_or_else(|| failed("predicate string overflow"))?)?;
                }
            }
        }
        _ => {}
    }
    Ok(bytes)
}
