//! One bounded static payload schema policy for native execution and typed intake.

use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::{DType, DecimalDType, PType};

pub(crate) use crate::native_temporal::temporal_storage;

pub(crate) fn admitted_decimal(dtype: DecimalDType) -> bool {
    (1..=38).contains(&dtype.precision())
        && dtype.scale() >= 0
        && i16::from(dtype.scale()) <= i16::from(dtype.precision())
}

/// Flat values admitted by native keys and compact retained scalar state.
pub(crate) fn admitted_scalar(dtype: &DType) -> bool {
    match dtype {
        DType::Bool(_) | DType::Utf8(_) | DType::Binary(_) => true,
        DType::Primitive(ptype, _) => *ptype != PType::F16,
        DType::Decimal(decimal, _) => admitted_decimal(*decimal),
        DType::Extension(_) => temporal_storage(dtype).is_some(),
        _ => false,
    }
}

/// Admit flat schema containers, name copies and the temporary uniqueness index
/// before constructing them. Width consumes the caller's grant, not a fixed
/// column count. The allowance also covers native field/slot wrapper metadata.
pub(crate) fn reserve_names<'a>(
    memory: &LiveMemoryPool,
    count: usize,
    name_at: impl Fn(usize) -> &'a str,
) -> Result<MemoryLease> {
    let bytes = names_bytes(count, &name_at)?;
    let credit = memory.reserve(bytes)?;
    unique_names(count, name_at)?;
    Ok(credit)
}

/// Size name/container workspace without changing the caller's uniqueness policy.
pub(crate) fn names_bytes<'a>(count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<u64> {
    if count == 0 {
        return Err(failed("schemas require at least one field"));
    }
    (0..count).try_fold(4096_u64, |bytes, index| {
        let name = name_at(index);
        if name.is_empty() {
            return Err(failed("field names must be nonempty and distinct"));
        }
        u64::try_from(name.len())
            .ok()
            .and_then(|len| len.checked_mul(4))
            .and_then(|len| len.checked_add(4096))
            .and_then(|field| bytes.checked_add(field))
            .ok_or_else(|| failed("schema metadata overflow"))
    })
}

// Call only while the corresponding names_bytes reservation is held.
fn unique_names<'a>(count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()> {
    let mut names = std::collections::BTreeSet::new();
    for index in 0..count {
        if !names.insert(name_at(index)) {
            return Err(failed("field names must be nonempty and distinct"));
        }
    }
    Ok(())
}

/// Top-level record width is resource-admitted. Individual nested value schemas
/// retain their separate depth/node bounds until their traversal is generalized.
/// Returned credit must outlive every native schema/container it admits.
pub(crate) fn reserve_schema(dtype: &DType, memory: &LiveMemoryPool) -> Result<MemoryLease> {
    let credit = memory.reserve(schema_bytes(dtype)?)?;
    if let DType::Struct(fields, _) = dtype {
        unique_names(fields.nfields(), |index| fields.names()[index].as_ref())?;
    }
    Ok(credit)
}

/// Whole-record estimate for batching or admission; individual value columns
/// use `metadata_bytes` and retain the nested schema traversal policy.
pub(crate) fn schema_bytes(dtype: &DType) -> Result<u64> {
    let DType::Struct(fields, _) = dtype else {
        return metadata_bytes(dtype);
    };
    fields.fields().try_fold(
        names_bytes(fields.nfields(), |index| fields.names()[index].as_ref())?,
        |bytes, child| {
            bytes
                .checked_add(metadata_bytes(&child)?)
                .ok_or_else(|| failed("schema metadata overflow"))
        },
    )
}

/// Reserve before cloning an already-bound field list into a native `DType`.
pub(crate) fn reserve_fields(
    fields: &[(String, DType)],
    memory: &LiveMemoryPool,
) -> Result<MemoryLease> {
    let bytes = fields.iter().try_fold(
        names_bytes(fields.len(), |index| fields[index].0.as_str())?,
        |bytes, (_, dtype)| {
            bytes
                .checked_add(metadata_bytes(dtype)?)
                .ok_or_else(|| failed("schema metadata overflow"))
        },
    )?;
    let credit = memory.reserve(bytes)?;
    unique_names(fields.len(), |index| fields[index].0.as_str())?;
    Ok(credit)
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    bytes: u64,
}

impl Budget {
    fn node(&mut self, depth: usize) -> Result<()> {
        self.nodes += 1;
        if depth > 24 || self.nodes > 4096 {
            return Err(failed(
                "nested payload schema exceeds depth 24 or 4096 nodes",
            ));
        }
        self.add(1024)
    }

    fn add(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| failed("payload metadata overflow"))?;
        if self.bytes > 8 * 1024 * 1024 {
            return Err(failed("nested payload schema metadata exceeds 8 MiB"));
        }
        Ok(())
    }

    fn names<'a>(&mut self, count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()> {
        if count == 0 || count > 1024 {
            return Err(failed("nested structs require 1..=1024 fields"));
        }
        for index in 0..count {
            let name = name_at(index);
            if name.is_empty() || (0..index).any(|prior| name_at(prior) == name) {
                return Err(failed(
                    "nested struct field names must be nonempty and distinct",
                ));
            }
            self.add((name.len() as u64).saturating_mul(2))?;
        }
        Ok(())
    }
}

pub(crate) fn metadata_bytes(dtype: &DType) -> Result<u64> {
    fn visit(dtype: &DType, depth: usize, budget: &mut Budget) -> Result<()> {
        budget.node(depth)?;
        match dtype {
            DType::Bool(_) | DType::Utf8(_) | DType::Binary(_) => {}
            DType::Primitive(ptype, _) if *ptype != PType::F16 => {}
            DType::Decimal(decimal, _) if admitted_decimal(*decimal) => {}
            DType::Extension(_) if temporal_storage(dtype).is_some() => {}
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                visit(child, depth + 1, budget)?;
            }
            DType::Struct(fields, _) => {
                budget.names(fields.nfields(), |index| fields.names()[index].as_ref())?;
                for child in fields.fields() {
                    visit(&child, depth + 1, budget)?;
                }
            }
            _ => return Err(unsupported()),
        }
        Ok(())
    }
    let mut budget = Budget::default();
    visit(dtype, 0, &mut budget)?;
    Ok(budget.bytes)
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub(crate) fn arrow_metadata_bytes(dtype: &arrow_schema::DataType) -> Result<u64> {
    use arrow_schema::DataType as A;
    fn field(child: &arrow_schema::Field, depth: usize, budget: &mut Budget) -> Result<()> {
        if child.metadata().contains_key("ARROW:extension:name") {
            return Err(unsupported());
        }
        visit(child.data_type(), depth, budget)
    }
    fn visit(dtype: &A, depth: usize, budget: &mut Budget) -> Result<()> {
        budget.node(depth)?;
        match dtype {
            A::Boolean
            | A::Int8
            | A::Int16
            | A::Int32
            | A::Int64
            | A::UInt8
            | A::UInt16
            | A::UInt32
            | A::UInt64
            | A::Float32
            | A::Float64
            | A::Utf8
            | A::LargeUtf8
            | A::Utf8View
            | A::Binary
            | A::LargeBinary
            | A::BinaryView
            | A::Date32
            | A::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {}
            A::Decimal128(precision, scale)
                if (1..=38).contains(precision)
                    && *scale >= 0
                    && i16::from(*scale) <= i16::from(*precision) => {}
            A::List(child) | A::LargeList(child) => {
                field(child, depth + 1, budget)?;
            }
            A::FixedSizeList(child, size) if *size >= 0 => {
                field(child, depth + 1, budget)?;
            }
            A::Struct(fields) => {
                budget.names(fields.len(), |index| fields[index].name().as_str())?;
                for child in fields {
                    field(child, depth + 1, budget)?;
                }
            }
            _ => return Err(unsupported()),
        }
        Ok(())
    }
    let mut budget = Budget::default();
    visit(dtype, 0, &mut budget)?;
    Ok(budget.bytes)
}

fn unsupported() -> ShardLoomError {
    failed(
        "payload requires bool, integer, F32/F64, UTF8, binary, decimal128(precision 1..38, scale 0..precision), Date32, timezone-free TimestampMicros or static list/struct fields",
    )
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native payload schema: {reason}; no fallback execution was attempted"
    ))
}
