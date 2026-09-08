//! One pure admission contract shared by runtime/certificate dispatch.
//! No source or workspace is opened by the request-only check.

use super::{
    SimpleAggregateFunction, VortexQueryPrimitiveRequest, split_predicate_for_vortex_pushdown,
    weighted_count_spill::KeyOrder,
};
use shardloom_core::{Result, ShardLoomError};
use vortex::array::dtype::{DType, Nullability};

pub(super) const MAX_KEY_BYTES: usize = 64 << 10;

#[derive(Clone)]
pub(super) struct Contract {
    pub groups: Vec<String>,
    pub dtypes: Vec<DType>,
    pub text_index: usize,
    pub numeric_index: Option<usize>,
    pub count_alias: String,
    pub order: KeyOrder,
    pub offset: usize,
    pub limit: usize,
    pub retained: usize,
}
impl Contract {
    pub(super) fn metadata_bytes(&self) -> Result<u64> {
        let names = self
            .groups
            .iter()
            .try_fold(self.count_alias.capacity(), |sum, name| {
                sum.checked_add(name.capacity())
                    .ok_or_else(|| failed("column capacity overflowed"))
            })?;
        let bytes = names
            .checked_add(self.groups.capacity() * size_of::<String>())
            .and_then(|bytes| bytes.checked_add(self.dtypes.capacity() * size_of::<DType>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or_else(|| failed("contract metadata overflowed"))?;
        u64::try_from(bytes).map_err(|_| failed("contract metadata exceeds u64"))
    }
}

pub(super) fn request_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let (Some(source), Some(aggregate), Some(limit)) = (
        &request.source_uri,
        &request.simple_aggregate,
        request.source_order_limit,
    ) else {
        return false;
    };
    let Some(spill) = aggregate.spill.as_ref() else {
        return false;
    };
    let [measure] = aggregate.measures.as_slice() else {
        return false;
    };
    if limit == 0
        || aggregate.offset.checked_add(limit).is_none()
        || !(1..=2).contains(&aggregate.group_by.len())
        || !aggregate.group_expressions.is_empty()
        || !aggregate.having.is_empty()
        || measure.column.is_some()
        || measure.argument_offset.is_some()
        || measure.value_transform.is_some()
        || !matches!(
            SimpleAggregateFunction::parse(&measure.function),
            Ok(SimpleAggregateFunction::Count)
        )
        || measure.alias.is_empty()
        || aggregate
            .group_by
            .iter()
            .any(|column| column.as_str() == measure.alias)
        || aggregate.group_by.len() == 2 && aggregate.group_by[0] == aggregate.group_by[1]
        || !spill.workspace.is_absolute()
        || spill.quota_bytes < 32 << 10
        || spill.memory_bytes < 4 << 20
    {
        return false;
    }
    if request.predicate.as_ref().is_some_and(|predicate| {
        split_predicate_for_vortex_pushdown(predicate, request.kind)
            .1
            .is_some()
    }) {
        return false;
    }
    let Some(primary) = aggregate.order_by.first() else {
        return false;
    };
    if !primary.descending
        || primary.column != measure.alias
        || aggregate.order_by.len() > aggregate.group_by.len() + 1
    {
        return false;
    }
    if aggregate
        .order_by
        .iter()
        .skip(1)
        .zip(&aggregate.group_by)
        .any(|(order, group)| order.descending || order.column != group.as_str())
    {
        return false;
    }
    let mut canonical =
        VortexQueryPrimitiveRequest::simple_aggregate(source.clone(), aggregate.clone())
            .with_source_order_limit(limit);
    canonical.predicate.clone_from(&request.predicate);
    request == &canonical
}

pub(super) fn admit(request: &VortexQueryPrimitiveRequest, dtype: &DType) -> Result<Contract> {
    if !request_admitted(request) {
        return Err(failed(
            "requires explicit COUNT(*) spill, identity groups and bounded count-descending/declared-key-ascending output",
        ));
    }
    if request.predicate.as_ref().is_some_and(|predicate| {
        split_predicate_for_vortex_pushdown(predicate, request.kind)
            .1
            .is_some()
    }) {
        return Err(failed("residual predicate is outside this family"));
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return Err(failed("requires a nonnullable struct source"));
    };
    let aggregate = request
        .simple_aggregate
        .as_ref()
        .expect("admitted aggregate");
    let mut text = None;
    let mut numeric = None;
    let mut dtypes = Vec::with_capacity(aggregate.group_by.len());
    for (index, group) in aggregate.group_by.iter().enumerate() {
        let dtype = fields
            .field(group.as_str())
            .ok_or_else(|| failed("declared group column is absent"))?;
        match dtype {
            DType::Utf8(Nullability::NonNullable) if text.is_none() => text = Some(index),
            DType::Primitive(ptype, Nullability::NonNullable)
                if ptype.is_int() && numeric.is_none() =>
            {
                numeric = Some((index, ptype.is_signed_int()));
            }
            _ => {
                return Err(failed(
                    "requires one nonnullable UTF8 key and optionally one nonnullable integer key",
                ));
            }
        }
        dtypes.push(dtype);
    }
    let text_index = text.ok_or_else(|| failed("UTF8 key is absent"))?;
    let order = match numeric {
        None => KeyOrder::Text,
        Some((0, signed)) => KeyOrder::IntegerText { signed },
        Some((_, signed)) => KeyOrder::TextInteger { signed },
    };
    let limit = request.source_order_limit.expect("admitted limit");
    Ok(Contract {
        groups: aggregate
            .group_by
            .iter()
            .map(|column| column.as_str().to_owned())
            .collect(),
        dtypes,
        text_index,
        numeric_index: numeric.map(|value| value.0),
        count_alias: aggregate.measures[0].alias.clone(),
        order,
        offset: aggregate.offset,
        limit,
        retained: aggregate
            .offset
            .checked_add(limit)
            .expect("admitted retained count"),
    })
}

pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native weighted complete-key COUNT spill {reason}; no fallback execution was attempted"
    ))
}
