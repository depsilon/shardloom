//! Completed exact integer groups cross the output boundary as typed columns.
//! Query execution and ranking remain the existing admitted aggregate runtime.

use super::{
    AggregateDistinctValue, AggregateGroupKey, AggregateIntegerKeyPart, AggregateSingleNumericKey,
    GroupedAggregateState, GroupedAggregateStates, Result, ShardLoomError,
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
#[cfg(unix)]
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

pub(super) struct OwnedAggregateFinalizer {
    ptype: PType,
    columns: [String; 2],
    offset: usize,
    limit: usize,
    allocator: HostAllocatorRef,
    memory: LiveMemoryPool,
    #[cfg(unix)]
    ownership: MemoryLease,
    array: Option<ArrayRef>,
}

impl OwnedAggregateFinalizer {
    #[cfg(unix)]
    pub(super) fn new(
        request: &VortexQueryPrimitiveRequest,
        dtype: &DType,
        session: &crate::resident_session::ResidentVortexSession,
    ) -> Result<Self> {
        if !workers::request_may_be_admitted(request)
            || !workers::request_schema_may_be_admitted(request, dtype)
        {
            return Err(failed(
                "requires non-null integer identity grouping and COUNT DISTINCT, ordered by count descending and optional key ascending",
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
        let Some(DType::Primitive(ptype, Nullability::NonNullable)) = dtype
            .as_struct_fields_opt()
            .and_then(|fields| fields.field(names[0]))
        else {
            return Err(failed("original integer group dtype is unavailable"));
        };
        // Array owners, field metadata and names are granted before construction;
        // payloads separately acquire credits through the session allocator.
        let ownership = session.memory().reserve(4096)?;
        let columns = names.map(str::to_owned);
        Ok(Self {
            ptype,
            columns,
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
        if let Some(finalized) = &states.finalized_distinct_counts {
            let rows = finalized
                .retained_count()
                .saturating_sub(self.offset)
                .min(self.limit);
            self.array = Some(self.build(rows, |visit| finalized.visit(visit))?);
            return finalized.summary(states, rows, None);
        }
        // Existing worker pressure handoff retains exact distinct sets. Select
        // their complete cardinalities directly; do not render or parse rows.
        let capacity = states.groups.len().min(self.offset + self.limit);
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
            let GroupedAggregateState::General { states: group, .. } = group else {
                return Err(failed(
                    "completed distinct state changed its admitted representation",
                ));
            };
            let measure = group
                .states
                .first()
                .ok_or_else(|| failed("completed distinct state is absent"))?;
            let candidate = Ranked(SingleNumericAggregateOrderCandidate {
                key,
                count: u64::try_from(measure.distinct_values.len()).map_err(vortex_error)?,
            });
            if heap.len() < capacity {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                *heap.peek_mut().expect("nonempty bounded heap") = candidate;
            }
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
        Ok((rows, serde_json::json!({
            "rows": rows,
            "group_by": self.columns[0],
            "functions": states.state_template.functions_summary(),
            "aggregate_update_strategy": states.aggregate_update_strategy(),
            "aggregate_result_boundary": "owned_native_integer_columns;no_JSON_or_StatValue_output_rows",
            "group_output_strategy": "bounded_heap_after_complete_distinct_group_reduction",
            "candidate_groups": states.groups.len(),
            "materialized_group_value_count": 0,
            "decoded_string_count": 0,
            "offset": self.offset,
            "values": serde_json::Value::Null,
        }).to_string()))
    }

    fn build(
        &self,
        rows: usize,
        visit: impl FnOnce(&mut dyn FnMut(AggregateIntegerKeyPart, u64) -> Result<()>) -> Result<()>,
    ) -> Result<ArrayRef> {
        let width = self.ptype.byte_width();
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
                    self.ptype,
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
                primitive(self.ptype, keys.freeze())?,
                PrimitiveArray::new(
                    Buffer::<u64>::from_byte_buffer(counts.freeze()),
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .map(|array| array.into_array())
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
