//! Completed exact admitted groups cross the output boundary as typed columns.
//! Query execution and ranking remain the existing admitted aggregate runtime.

use super::{
    AggregateDistinctValue, AggregateGroupKey, AggregateIntegerKeyPart, AggregateSingleNumericKey,
    GroupedAggregateState, GroupedAggregateStates, Result, ShardLoomError, SimpleAggregateFunction,
    SingleNumericAggregateOrderCandidate, compare_single_numeric_candidates, vortex_error,
};
#[cfg(unix)]
use super::{
    VortexQueryPrimitiveRequest, exact_distinct_pairs::workers, required_simple_aggregate,
};
use shardloom_exec::live_memory::LiveMemoryPool;
#[cfg(unix)]
use shardloom_exec::live_memory::MemoryLease;
use std::collections::BinaryHeap;
use vortex::array::dtype::{DType, Nullability};
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{FieldNames, PType},
        memory::HostAllocatorRef,
        validity::Validity,
    },
    buffer::{Alignment, Buffer, ByteBuffer},
};

pub(super) const MAX_ROWS: usize = 65_536;
const MAX_BYTES: usize = 8 * 1024 * 1024;
const INTEGER_BOUNDARY: &str = "owned_native_integer_columns;no_JSON_or_StatValue_output_rows";
const UTF8_BOUNDARY: &str = "owned_native_utf8_columns;no_JSON_or_StatValue_output_rows";

#[path = "local_primitive_aggregate_owned_utf8.rs"]
mod utf8;

#[derive(Clone, Copy)]
// Construction requires the Unix held-source API; shared finalization is still
// compiled on other feature-enabled targets.
#[cfg_attr(not(unix), allow(dead_code))]
enum KeyKind {
    Integer(PType),
    Utf8,
}

pub(super) struct OwnedAggregateFinalizer {
    key_kind: KeyKind,
    columns: [String; 2],
    function: SimpleAggregateFunction,
    offset: usize,
    limit: usize,
    allocator: HostAllocatorRef,
    memory: LiveMemoryPool,
    array: Option<ArrayRef>,
    // Native payload buffers have their own allocator credits. Keep metadata
    // admission until both the completed array and our owned names are dropped.
    #[cfg(unix)]
    ownership: MemoryLease,
}

impl OwnedAggregateFinalizer {
    #[cfg(unix)]
    pub(super) fn new(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        session: &crate::resident_session::ResidentVortexSession,
    ) -> Result<Self> {
        let key_kind = admitted_key_kind(request, dtype)?;
        let aggregate = required_simple_aggregate(request)?;
        let limit = request
            .source_order_limit
            .ok_or_else(|| failed("bounded limit is required"))?;
        let names = [
            aggregate.group_by[0].as_str(),
            aggregate.measures[0].alias.as_str(),
        ];
        // Array owners, field metadata and names are granted before construction;
        // payloads separately acquire credits through the session allocator.
        let ownership = session.memory().reserve(4096)?;
        let columns = names.map(str::to_owned);
        Ok(Self {
            key_kind,
            columns,
            function: SimpleAggregateFunction::parse(&aggregate.measures[0].function)?,
            offset: aggregate.offset,
            limit,
            allocator: session.native_allocator(),
            memory: session.memory().clone(),
            ownership,
            array: None,
        })
    }

    pub(super) fn finish(
        &mut self,
        states: &GroupedAggregateStates<'_>,
    ) -> Result<(usize, String)> {
        if self.array.is_some() {
            return Err(failed("completed array was already finalized"));
        }
        if matches!(self.key_kind, KeyKind::Utf8) {
            return self.finish_utf8(states);
        }
        if let Some(finalized) = &states.finalized_distinct_counts {
            if self.function != SimpleAggregateFunction::CountDistinct {
                return Err(failed(
                    "completed distinct state differs from admitted COUNT(*)",
                ));
            }
            let rows = finalized
                .retained_count()
                .saturating_sub(self.offset)
                .min(self.limit);
            self.array = Some(self.build(rows, |visit| finalized.visit(visit))?);
            return finalized.summary(states, rows, None);
        }
        // Native workers and caller execution can finish in different existing
        // state representations. Visit the actual complete state, never rows.
        let candidate_groups = if let Some(groups) = &states.single_numeric_count_groups {
            if self.function != SimpleAggregateFunction::Count || !states.groups.is_empty() {
                return Err(failed(
                    "completed numeric count state differs from its admitted representation",
                ));
            }
            groups.len()
        } else {
            states.groups.len()
        };
        let capacity = candidate_groups.min(self.offset + self.limit);
        let bytes = capacity
            .checked_mul(size_of::<Ranked>())
            .and_then(|bytes| bytes.checked_add(size_of::<BinaryHeap<Ranked>>()))
            .ok_or_else(|| failed("selection reservation overflow"))?;
        let _selection = self.memory.reserve(bytes as u64)?;
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(capacity).map_err(vortex_error)?;
        if heap.capacity() > capacity {
            return Err(failed("selection allocation exceeded its grant"));
        }
        let mut retain = |key, count| {
            let candidate = Ranked(SingleNumericAggregateOrderCandidate { key, count });
            if heap.len() < capacity {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                *heap.peek_mut().expect("nonempty bounded heap") = candidate;
            }
        };
        if let Some(groups) = &states.single_numeric_count_groups {
            for (key, count) in groups {
                retain(*key, *count);
            }
        }
        for (key, group) in &states.groups {
            let key = match key {
                AggregateGroupKey::Single(AggregateDistinctValue::Int64(value)) => {
                    AggregateSingleNumericKey {
                        bits: u64::from_ne_bytes(value.to_ne_bytes()),
                        signed: true,
                    }
                }
                AggregateGroupKey::Single(AggregateDistinctValue::UInt64(value)) => {
                    AggregateSingleNumericKey {
                        bits: *value,
                        signed: false,
                    }
                }
                _ => {
                    return Err(failed(
                        "completed group key changed its admitted integer type",
                    ));
                }
            };
            retain(key, completed_count(group, self.function)?);
        }
        let selected = heap.into_sorted_vec();
        let rows = selected.len().saturating_sub(self.offset).min(self.limit);
        self.array = Some(self.build(rows, |visit| {
            for Ranked(candidate) in &selected {
                visit(
                    AggregateIntegerKeyPart {
                        bits: candidate.key.bits,
                        signed: candidate.key.signed,
                    },
                    candidate.count,
                )?;
            }
            Ok(())
        })?);
        Ok((rows, self.summary(states, rows, candidate_groups)))
    }

    fn summary(
        &self,
        states: &GroupedAggregateStates<'_>,
        rows: usize,
        candidate_groups: usize,
    ) -> String {
        self.summary_payload(states, rows, candidate_groups)
            .to_string()
    }

    fn summary_payload(
        &self,
        states: &GroupedAggregateStates<'_>,
        rows: usize,
        candidate_groups: usize,
    ) -> serde_json::Value {
        serde_json::json!({
            "rows": rows,
            "group_by": self.columns[0],
            "functions": states.state_template.functions_summary(),
            "aggregate_update_strategy": states.aggregate_update_strategy(),
            "aggregate_result_boundary": match self.key_kind {
                KeyKind::Integer(_) => INTEGER_BOUNDARY,
                KeyKind::Utf8 => UTF8_BOUNDARY,
            },
            "group_output_strategy": if self.function == SimpleAggregateFunction::Count {
                "bounded_heap_after_complete_count_group_reduction"
            } else {
                "bounded_heap_after_complete_distinct_group_reduction"
            },
            "candidate_groups": candidate_groups,
            "materialized_group_value_count": 0,
            "decoded_string_count": 0,
            "offset": self.offset,
            "values": serde_json::Value::Null,
        })
    }

    fn build(
        &self,
        rows: usize,
        visit: impl FnOnce(&mut dyn FnMut(AggregateIntegerKeyPart, u64) -> Result<()>) -> Result<()>,
    ) -> Result<ArrayRef> {
        let KeyKind::Integer(ptype) = self.key_kind else {
            return Err(failed("integer output differs from the admitted key dtype"));
        };
        let width = ptype.byte_width();
        let bytes = rows
            .checked_mul(width + 8)
            .ok_or_else(|| failed("output byte bound overflow"))?;
        if rows > MAX_ROWS || bytes > MAX_BYTES {
            return Err(failed("completed output exceeds row or byte admission"));
        }
        let mut keys = self
            .allocator
            .allocate(rows * width, Alignment::new(width))
            .map_err(vortex_error)?;
        let mut counts = self
            .allocator
            .allocate(rows * 8, Alignment::new(8))
            .map_err(vortex_error)?;
        let mut ordinal = 0_usize;
        let mut written = 0_usize;
        visit(&mut |key, count| {
            if ordinal >= self.offset && written < rows {
                write_key(
                    ptype,
                    key,
                    &mut keys.as_mut_slice()[written * width..(written + 1) * width],
                )?;
                counts.as_mut_slice()[written * 8..(written + 1) * 8]
                    .copy_from_slice(&count.to_ne_bytes());
                written += 1;
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| failed("output ordinal overflow"))?;
            Ok(())
        })?;
        if written != rows {
            return Err(failed(
                "completed group count changed during output construction",
            ));
        }
        StructArray::try_new(
            FieldNames::from(self.columns.iter().map(String::as_str).collect::<Vec<_>>()),
            vec![
                primitive(ptype, keys.freeze())?,
                PrimitiveArray::new(
                    Buffer::<u64>::from_byte_buffer(counts.freeze()),
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .map(vortex::array::IntoArray::into_array)
        .map_err(vortex_error)
    }

    #[cfg(unix)]
    pub(super) fn into_array(self) -> Result<(ArrayRef, MemoryLease)> {
        Ok((
            self.array
                .ok_or_else(|| failed("execution did not finalize owned columns"))?,
            self.ownership,
        ))
    }
}

#[cfg(unix)]
pub(super) fn utf8_count_admitted(request: &VortexQueryPrimitiveRequest, dtype: &DType) -> bool {
    matches!(admitted_key_kind(request, dtype), Ok(KeyKind::Utf8))
}

#[cfg(unix)]
fn admitted_key_kind(request: &VortexQueryPrimitiveRequest, dtype: &DType) -> Result<KeyKind> {
    // A nonnullable child cannot prove a nullable Struct row has a key. Keep
    // COUNT's new output admission within the existing worker schema boundary.
    if dtype.is_nullable() {
        return Err(failed("requires a nonnullable Struct source"));
    }
    let count = count_star_admitted(request);
    if !(count
        || (workers::request_may_be_admitted(request)
            && workers::request_schema_may_be_admitted(request, dtype)))
    {
        return Err(failed(
            "requires one non-null identity key with integer COUNT(*)/COUNT DISTINCT or UTF8 COUNT(*), ordered by count descending and optional key ascending",
        ));
    }
    let aggregate = required_simple_aggregate(request)?;
    let limit = request
        .source_order_limit
        .ok_or_else(|| failed("bounded limit is required"))?;
    if aggregate.spill.is_some()
        || limit == 0
        || limit > MAX_ROWS
        || aggregate
            .offset
            .checked_add(limit)
            .is_none_or(|n| n > MAX_ROWS)
    {
        return Err(failed(
            "requires no explicit spill and offset plus positive limit at most 65536",
        ));
    }
    let names = [
        aggregate.group_by[0].as_str(),
        aggregate.measures[0].alias.as_str(),
    ];
    if names[0] == names[1] || names.iter().any(|name| name.is_empty() || name.len() > 256) {
        return Err(failed("requires two unique output names of 1..=256 bytes"));
    }
    match dtype
        .as_struct_fields_opt()
        .and_then(|fields| fields.field(names[0]))
    {
        Some(DType::Primitive(ptype, Nullability::NonNullable)) if ptype.is_int() => {
            Ok(KeyKind::Integer(ptype))
        }
        Some(DType::Utf8(Nullability::NonNullable)) if count && !dtype.is_nullable() => {
            Ok(KeyKind::Utf8)
        }
        _ => Err(failed(
            "original group dtype is outside admitted non-null integer/UTF8 keys",
        )),
    }
}

#[cfg(unix)]
fn count_star_admitted(request: &VortexQueryPrimitiveRequest) -> bool {
    let Ok(aggregate) = required_simple_aggregate(request) else {
        return false;
    };
    if aggregate.group_by.len() != 1
        || !aggregate.group_expressions.is_empty()
        || !aggregate.having.is_empty()
        || aggregate.measures.len() != 1
    {
        return false;
    }
    let measure = &aggregate.measures[0];
    if SimpleAggregateFunction::parse(&measure.function).ok()
        != Some(SimpleAggregateFunction::Count)
        || measure.column.is_some()
        || measure.argument_offset.is_some()
        || measure.value_transform.is_some()
    {
        return false;
    }
    match aggregate.order_by.as_slice() {
        [primary] => primary.descending && primary.column == measure.alias,
        [primary, secondary] => {
            primary.descending
                && primary.column == measure.alias
                && !secondary.descending
                && secondary.column == aggregate.group_by[0].as_str()
        }
        _ => false,
    }
}

fn completed_count(
    group: &GroupedAggregateState,
    function: SimpleAggregateFunction,
) -> Result<u64> {
    match group {
        GroupedAggregateState::CompactCountStar { count, .. }
            if function == SimpleAggregateFunction::Count =>
        {
            Ok(*count)
        }
        GroupedAggregateState::General { states, .. } if states.states.len() == 1 => {
            let measure = &states.states[0];
            if function == SimpleAggregateFunction::Count && states.is_count_star_only() {
                Ok(measure.count)
            } else if function == SimpleAggregateFunction::CountDistinct
                && measure.function == function
            {
                u64::try_from(measure.distinct_values.len()).map_err(vortex_error)
            } else {
                Err(failed("completed measure differs from the admitted count"))
            }
        }
        _ => Err(failed(
            "completed count state changed its admitted representation",
        )),
    }
}

fn write_key(ptype: PType, key: AggregateIntegerKeyPart, output: &mut [u8]) -> Result<()> {
    if ptype.is_signed_int() != key.signed {
        return Err(failed(
            "completed key signedness differs from the source dtype",
        ));
    }
    macro_rules! signed {
        ($t:ty) => {
            output.copy_from_slice(
                &<$t>::try_from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
                    .map_err(vortex_error)?
                    .to_ne_bytes(),
            )
        };
    }
    macro_rules! unsigned {
        ($t:ty) => {
            output.copy_from_slice(
                &<$t>::try_from(key.bits)
                    .map_err(vortex_error)?
                    .to_ne_bytes(),
            )
        };
    }
    match ptype {
        PType::I8 => signed!(i8),
        PType::I16 => signed!(i16),
        PType::I32 => signed!(i32),
        PType::U8 => unsigned!(u8),
        PType::U16 => unsigned!(u16),
        PType::U32 => unsigned!(u32),
        PType::I64 | PType::U64 => output.copy_from_slice(&key.bits.to_ne_bytes()),
        _ => return Err(failed("non-integer output is not admitted")),
    }
    Ok(())
}

fn primitive(ptype: PType, bytes: ByteBuffer) -> Result<ArrayRef> {
    macro_rules! array {
        ($t:ty) => {
            PrimitiveArray::new(Buffer::<$t>::from_byte_buffer(bytes), Validity::NonNullable)
                .into_array()
        };
    }
    Ok(match ptype {
        PType::I8 => array!(i8),
        PType::I16 => array!(i16),
        PType::I32 => array!(i32),
        PType::I64 => array!(i64),
        PType::U8 => array!(u8),
        PType::U16 => array!(u16),
        PType::U32 => array!(u32),
        PType::U64 => array!(u64),
        _ => return Err(failed("non-integer output is not admitted")),
    })
}

struct Ranked(SingleNumericAggregateOrderCandidate);
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_single_numeric_candidates(&self.0, &other.0)
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "owned aggregate output: {reason}; no fallback execution was attempted"
    ))
}
