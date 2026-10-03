//! Lossless schema and source binding before native relational row execution.

use super::{
    BoundUnary, DType, MaterializedPredicateEvaluator, MemoryLease, Node, NodeKind, Nullability,
    PathBuf, PreparedVortexSource, ReservedVec, ResidentVortexSession, Result, SetKind,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexRelationalPlan, failed,
    native_relational_join, native_relational_sort, vortex_error,
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
    metadata: MemoryLease,
    nodes: usize,
    expression_nodes: usize,
    outer_fields: Option<Vec<(String, DType)>>,
    parameterized_binding: bool,
    execution: Option<dynamic::ExecutionBinding<'a>>,
    resolved: ReservedVec<Option<Node>>,
    scope: std::sync::Arc<()>,
    deferred: ReservedVec<Option<Box<super::DynamicLowerer>>>,
}

impl<'a> Binder<'a> {
    pub(super) fn new(session: &'a ResidentVortexSession) -> Result<Self> {
        Ok(Self {
            session,
            sources: ReservedVec::new(session.memory())?,
            paths: ReservedVec::new(session.memory())?,
            metadata: session.memory().reserve(4096)?,
            nodes: 0,
            expression_nodes: 0,
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

    pub(super) fn finish(
        mut self,
    ) -> Result<(Vec<PreparedVortexSource>, Vec<PathBuf>, MemoryLease)> {
        let (sources, mut source_credit) = self.sources.into_parts();
        let (paths, mut path_credit) = self.paths.into_parts();
        self.metadata.absorb(&mut source_credit)?;
        self.metadata.absorb(&mut path_credit)?;
        Ok((sources, paths, self.metadata))
    }

    pub(super) fn bind(&mut self, input: &VortexRelationalPlan, depth: usize) -> Result<Node> {
        self.nodes += 1;
        if depth > 24 || self.nodes > 128 {
            return Err(failed("plan exceeds 24 levels or 128 operators"));
        }
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
                self.charge(width * 4096)?;
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
                let operation = BoundUnary::for_relation(
                    &unary.request,
                    &DType::struct_(input.fields.clone(), Nullability::NonNullable),
                    self.session.memory(),
                )?;
                if unary.request.kind == VortexQueryPrimitiveKind::PivotRows {
                    return self.complete_pivot(&input, operation);
                }
                validate_width(operation.fields().len())?;
                for (name, dtype) in operation.fields() {
                    validate_name(name)?;
                    if unary.request.kind == VortexQueryPrimitiveKind::ExplodeRows {
                        validate_payload(dtype)?;
                    } else {
                        validate_scalar(dtype)?;
                    }
                }
                validate_unique(operation.fields())?;
                self.charge(operation.fields().len() * 4096)?;
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
            if self.sources.values.len() >= 128 {
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
        let source = self.source(uri)?;
        let fields = self.sources.values[source]
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| failed("relational source requires a struct schema"))?;
        self.charge(
            fields
                .names()
                .len()
                .checked_mul(4096)
                .ok_or_else(|| failed("source metadata size overflow"))?,
        )?;
        let fields = self.sources.values[source]
            .dtype()
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
            self.charge(columns.len() * 4096)?;
        }
        let source = self.source(&scan.source_uri)?;
        let dtype = self.sources.values[source].dtype();
        let source_fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| failed("relational source requires a struct schema"))?;
        let schema_bytes = source_fields
            .names()
            .len()
            .checked_mul(4096)
            .ok_or_else(|| failed("source schema metadata overflow"))?;
        self.charge(schema_bytes)?;
        let dtype = self.sources.values[source].dtype();
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
        self.charge((join.keys.len() * 2 + join.columns.len() * 2) * 4096)?;
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
        self.charge(left.fields.len() * 4096)?;
        let fields = left
            .fields
            .iter()
            .zip(&right.fields)
            .map(|((name, left), (_, right))| Ok((name.clone(), common_dtype(left, right)?)))
            .collect::<Result<Vec<_>>>()?;
        if set.kind != SetKind::UnionAll {
            for (_, dtype) in &fields {
                validate_scalar(dtype)?;
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
    if width == 0 || width > 128 {
        return Err(failed("relational schema requires 1 through 128 fields"));
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
    for (index, (name, _)) in fields.iter().enumerate() {
        if fields[..index].iter().any(|(prior, _)| prior == name) {
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
    validate_scalar(left)?;
    validate_scalar(right)?;
    if left.as_nonnullable() == right.as_nonnullable() {
        return Ok(());
    }
    if let (DType::Primitive(left, _), DType::Primitive(right, _)) = (left, right)
        && ((integer(*left).is_some() && integer(*right).is_some())
            || (matches!(left, PType::F32 | PType::F64)
                && matches!(right, PType::F32 | PType::F64)))
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
