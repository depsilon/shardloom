//! Bounded schema accounting for execution payloads and physical native transfer.

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
    reserve_schema_bytes(dtype, memory, schema_bytes(dtype)?)
}

/// Physical transfer preserves Null leaves without admitting Null execution keys.
#[cfg(any(
    test,
    all(feature = "vortex-local-primitives", feature = "vortex-write", unix)
))]
pub(crate) fn reserve_encoded_schema(
    dtype: &DType,
    memory: &LiveMemoryPool,
) -> Result<MemoryLease> {
    reserve_schema_bytes(dtype, memory, encoded_schema_bytes(dtype)?)
}

fn reserve_schema_bytes(dtype: &DType, memory: &LiveMemoryPool, bytes: u64) -> Result<MemoryLease> {
    let credit = memory.reserve(bytes)?;
    if let DType::Struct(fields, _) = dtype {
        unique_names(fields.nfields(), |index| fields.names()[index].as_ref())?;
    }
    Ok(credit)
}

/// Whole-record estimate for batching or admission; individual value columns
/// use `metadata_bytes` and retain the nested schema traversal policy.
pub(crate) fn schema_bytes(dtype: &DType) -> Result<u64> {
    schema_bytes_with_null(dtype, false)
}

/// Account every physical schema node, including Null fields and nested elements.
/// All existing name, depth, node and metadata bounds still apply.
#[cfg(any(
    test,
    all(feature = "vortex-local-primitives", feature = "vortex-write", unix)
))]
pub(crate) fn encoded_schema_bytes(dtype: &DType) -> Result<u64> {
    schema_bytes_with_null(dtype, true)
}

fn schema_bytes_with_null(dtype: &DType, allow_null: bool) -> Result<u64> {
    let DType::Struct(fields, _) = dtype else {
        return metadata_bytes_with_null(dtype, allow_null);
    };
    fields.fields().try_fold(
        names_bytes(fields.nfields(), |index| fields.names()[index].as_ref())?,
        |bytes, child| {
            bytes
                .checked_add(metadata_bytes_with_null(&child, allow_null)?)
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
    metadata_bytes_with_null(dtype, false)
}

fn metadata_bytes_with_null(dtype: &DType, allow_null: bool) -> Result<u64> {
    fn visit(dtype: &DType, depth: usize, budget: &mut Budget, allow_null: bool) -> Result<()> {
        budget.node(depth)?;
        match dtype {
            DType::Null if allow_null => {}
            DType::Bool(_) | DType::Utf8(_) | DType::Binary(_) => {}
            DType::Primitive(ptype, _) if *ptype != PType::F16 => {}
            DType::Decimal(decimal, _) if admitted_decimal(*decimal) => {}
            DType::Extension(_) if temporal_storage(dtype).is_some() => {}
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                visit(child, depth + 1, budget, allow_null)?;
            }
            DType::Struct(fields, _) => {
                budget.names(fields.nfields(), |index| fields.names()[index].as_ref())?;
                for child in fields.fields() {
                    visit(&child, depth + 1, budget, allow_null)?;
                }
            }
            _ => return Err(unsupported()),
        }
        Ok(())
    }
    let mut budget = Budget::default();
    visit(dtype, 0, &mut budget, allow_null)?;
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
            #[cfg(feature = "native-encoded-exchange-experiment")]
            A::Dictionary(keys, values)
                if matches!(
                    keys.as_ref(),
                    A::Int8
                        | A::Int16
                        | A::Int32
                        | A::Int64
                        | A::UInt8
                        | A::UInt16
                        | A::UInt32
                        | A::UInt64
                ) && encoded_arrow_leaf(values) =>
            {
                visit(keys, depth + 1, budget)?;
                visit(values, depth + 1, budget)?;
            }
            #[cfg(feature = "native-encoded-exchange-experiment")]
            A::RunEndEncoded(ends, values)
                if !ends.is_nullable()
                    && matches!(ends.data_type(), A::Int16 | A::Int32 | A::Int64)
                    && encoded_arrow_leaf(values.data_type()) =>
            {
                field(ends, depth + 1, budget)?;
                field(values, depth + 1, budget)?;
            }
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

// The experimental transport control does not broaden the logical-value
// validator. These leaves have no finite-float or decimal-precision condition;
// Arrow's full validation checks their buffers before the owned copy/import.
#[cfg(feature = "native-encoded-exchange-experiment")]
fn encoded_arrow_leaf(dtype: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType as A;
    matches!(
        dtype,
        A::Boolean
            | A::Int8
            | A::Int16
            | A::Int32
            | A::Int64
            | A::UInt8
            | A::UInt16
            | A::UInt32
            | A::UInt64
            | A::Utf8
            | A::LargeUtf8
            | A::Utf8View
            | A::Binary
            | A::LargeBinary
            | A::BinaryView
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::dtype::{Nullability, StructFields};

    #[test]
    fn encoded_null_schemas_do_not_admit_execution_payloads() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        for dtype in [
            DType::Null,
            DType::List(DType::Null.into(), Nullability::NonNullable),
            DType::FixedSizeList(DType::Null.into(), 2, Nullability::Nullable),
            DType::Struct(
                StructFields::from_iter([("null", DType::Null)]),
                Nullability::NonNullable,
            ),
        ] {
            assert!(metadata_bytes(&dtype).is_err());
            assert!(schema_bytes(&dtype).is_err());
            assert!(reserve_schema(&dtype, &memory).is_err());
            let credit = reserve_encoded_schema(&dtype, &memory).unwrap();
            assert!(credit.bytes() >= 1024);
            assert_eq!(credit.bytes(), encoded_schema_bytes(&dtype).unwrap());
            drop(credit);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        assert!(!admitted_scalar(&DType::Null));
    }

    #[test]
    fn encoded_null_schemas_keep_depth_width_and_name_bounds() {
        let mut dtype = DType::Null;
        for _ in 0..24 {
            dtype = DType::List(dtype.into(), Nullability::NonNullable);
        }
        assert!(encoded_schema_bytes(&dtype).is_ok());
        dtype = DType::List(dtype.into(), Nullability::NonNullable);
        assert!(encoded_schema_bytes(&dtype).is_err());
        let duplicate = DType::Struct(
            StructFields::from_iter([("same", DType::Null), ("same", DType::Null)]),
            Nullability::NonNullable,
        );
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        assert!(reserve_encoded_schema(&duplicate, &memory).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let nested = DType::List(duplicate.into(), Nullability::NonNullable);
        assert!(encoded_schema_bytes(&nested).is_err());
        let wide = DType::Struct(
            StructFields::from_iter((0..1025).map(|index| (format!("n{index}"), DType::Null))),
            Nullability::NonNullable,
        );
        assert!(encoded_schema_bytes(&wide).is_ok());
        assert!(encoded_schema_bytes(&DType::List(wide.into(), Nullability::NonNullable)).is_err());
    }
}
