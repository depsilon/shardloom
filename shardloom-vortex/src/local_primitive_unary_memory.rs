//! Capacity and payload owners for the native unary state machines.

use super::{Result, ShardLoomError, vortex_error};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};

pub(super) use super::super::native_capacity::ReservedVec;

fn failed() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native unary state capacity overflow; no fallback execution was attempted".into(),
    )
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(failed)
}

/// The table owns no payload copies during growth. Its capacity credit covers
/// both tables until rehash completes; key bytes live with the corresponding entry.
pub(super) struct KeyIndex {
    pub(super) values: rustc_hash::FxHashMap<String, usize>,
    lease: MemoryLease,
}

impl KeyIndex {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            values: rustc_hash::FxHashMap::default(),
            lease: memory.reserve(0)?,
        })
    }

    pub(super) fn reserve_one(&mut self) -> Result<()> {
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        // Hashbrown's load factor is at most 7/8. Reserve a whole power-of-two
        // bucket allocation plus control bytes and alignment, before rehash.
        let entries = self.values.len().checked_add(1).ok_or_else(failed)?;
        let buckets = entries
            .checked_mul(8)
            .and_then(|value| value.checked_div(7))
            .and_then(|value| value.checked_add(1))
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(failed)?
            .max(8);
        let new_bytes = bytes::<(String, usize)>(buckets)?
            .checked_add(u64::try_from(buckets).map_err(vortex_error)?)
            .and_then(|value| value.checked_add(128))
            .ok_or_else(failed)?;
        self.lease.resize(
            self.lease
                .bytes()
                .checked_add(new_bytes)
                .ok_or_else(failed)?,
        )?;
        self.values.try_reserve(1).map_err(vortex_error)?;
        if self.values.capacity() >= buckets {
            return Err(failed());
        }
        self.lease.resize(new_bytes)
    }
}

/// Account for request/lowering copies before cloning or opening. Traversal is
/// allocation-free; bounded depth prevents recursive compiler stack exhaustion.
pub(super) fn request_bytes(request: &super::VortexQueryPrimitiveRequest) -> Result<u64> {
    let mut size = RequestSize(std::mem::size_of_val(request) as u64);
    if let Some(uri) = &request.source_uri {
        size.add(uri.as_str().len())?;
    }
    size.projection(&request.projection)?;
    if let Some(projection) = &request.deduplicate_key_projection {
        size.projection(projection)?;
    }
    if let Some(predicate) = &request.predicate {
        size.predicate(predicate, 0)?;
    }
    if let Some(column) = &request.sample_weight_column {
        size.add(column.as_str().len())?;
    }
    if let Some(rolling) = &request.rolling_window {
        size.add(rolling.source_column.as_str().len())?;
        size.add(rolling.output_column.len())?;
        size.add(rolling.aggregate.len())?;
    }
    if let Some(expression) = &request.expression_projection {
        for rewrite in &expression.rewrites {
            size.rewrite(rewrite)?;
        }
    }
    if let Some(melt) = &request.melt_projection {
        for column in melt.id_columns.iter().chain(&melt.value_columns) {
            size.add(std::mem::size_of_val(column))?;
            size.add(column.as_str().len())?;
        }
        size.add(melt.variable_column.len())?;
        size.add(melt.value_column.len())?;
    }
    if let Some(explode) = &request.explode_projection {
        size.add(explode.column.as_str().len())?;
        for column in &explode.columns {
            size.add(std::mem::size_of_val(column))?;
            size.add(column.as_str().len())?;
        }
        for name in [&explode.element_field, &explode.element_output_column]
            .into_iter()
            .flatten()
        {
            size.add(name.len())?;
        }
    }
    if let Some(pivot) = &request.pivot_projection {
        for name in [
            pivot.index_column.as_str(),
            pivot.pivot_column.as_str(),
            pivot.value_column.as_str(),
            pivot.aggregate.as_str(),
            pivot.margins_name.as_str(),
        ] {
            size.add(name.len())?;
        }
        if let Some(value) = &pivot.fill_value {
            size.value(value)?;
        }
    }
    for diagnostic in &request.diagnostics {
        size.add(std::mem::size_of_val(diagnostic))?;
        size.add(diagnostic.message.len())?;
        size.add(diagnostic.fallback.reason.len())?;
        for value in [
            &diagnostic.feature,
            &diagnostic.reason,
            &diagnostic.suggested_next_step,
            &diagnostic.fallback.engine,
        ]
        .into_iter()
        .flatten()
        {
            size.add(value.len())?;
        }
    }
    // Bound overlapping request, lowered expression, compiled predicate, source
    // capability and diagnostic copies, with independent schema/evidence leases.
    size.0.checked_mul(16).ok_or_else(failed)
}

struct RequestSize(u64);
impl RequestSize {
    fn rewrite(&mut self, rewrite: &super::super::VortexExpressionRewrite) -> Result<()> {
        use super::super::VortexExpressionRewrite as Rewrite;
        self.add(std::mem::size_of_val(rewrite))?;
        self.add(rewrite.target_column().as_str().len())?;
        match rewrite {
            Rewrite::MaskScalar {
                predicate,
                replacement,
                ..
            } => {
                self.predicate(predicate, 0)?;
                self.value(replacement)?;
            }
            Rewrite::ReplaceScalar {
                to_replace,
                replacement,
                ..
            } => {
                self.value(to_replace)?;
                self.value(replacement)?;
            }
            Rewrite::StringReplaceScalar {
                needle,
                replacement,
                ..
            } => {
                self.add(needle.len())?;
                self.add(replacement.len())?;
            }
            Rewrite::RegexReplaceScalar {
                pattern,
                replacement,
                ..
            } => {
                self.add(pattern.len())?;
                self.add(replacement.len())?;
            }
            Rewrite::NumericScalarArithmetic {
                operator, operand, ..
            } => {
                self.add(operator.len())?;
                self.value(operand)?;
            }
            Rewrite::ForwardFillNull { .. } | Rewrite::RowNumber { .. } => {}
        }
        Ok(())
    }
    fn add(&mut self, bytes: usize) -> Result<()> {
        self.0 = self
            .0
            .checked_add(u64::try_from(bytes).map_err(vortex_error)?)
            .ok_or_else(failed)?;
        Ok(())
    }
    fn projection(&mut self, projection: &super::super::ProjectionRequest) -> Result<()> {
        if let super::super::ProjectionRequest::Columns(columns) = projection {
            for column in columns {
                self.add(std::mem::size_of_val(column))?;
                self.add(column.as_str().len())?;
            }
        }
        Ok(())
    }
    fn value(&mut self, value: &super::StatValue) -> Result<()> {
        self.add(std::mem::size_of_val(value))?;
        if let super::StatValue::Utf8(text) = value {
            self.add(text.len())?;
        }
        Ok(())
    }
    fn predicate(&mut self, predicate: &super::super::PredicateExpr, depth: usize) -> Result<()> {
        use super::super::PredicateExpr;
        if depth > 64 {
            return Err(super::failed("predicate nesting exceeds 64 levels"));
        }
        self.add(std::mem::size_of_val(predicate))?;
        if let Some(column) = predicate.column() {
            self.add(column.as_str().len())?;
        }
        match predicate {
            PredicateExpr::And(predicates) => {
                for predicate in predicates {
                    self.predicate(predicate, depth + 1)?;
                }
            }
            PredicateExpr::Compare { value, .. } => self.value(value)?,
            PredicateExpr::StringContains { needle, .. } => self.add(needle.len())?,
            PredicateExpr::InList { values, .. } => {
                for value in values {
                    self.value(value)?;
                }
            }
            PredicateExpr::AlwaysTrue
            | PredicateExpr::AlwaysFalse
            | PredicateExpr::IsNull { .. }
            | PredicateExpr::IsNotNull { .. } => {}
        }
        Ok(())
    }
}
