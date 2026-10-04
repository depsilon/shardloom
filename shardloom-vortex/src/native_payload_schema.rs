//! One bounded static payload schema policy for native execution and typed intake.

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    dtype::{DType, DecimalDType, PType},
    extension::datetime::{Date, TimeUnit, Timestamp},
};

pub(crate) fn admitted_decimal(dtype: DecimalDType) -> bool {
    (1..=38).contains(&dtype.precision())
        && dtype.scale() >= 0
        && i16::from(dtype.scale()) <= i16::from(dtype.precision())
}

/// Recognize provider metadata, never an extension name or its storage alone.
pub(crate) fn temporal_storage(dtype: &DType) -> Option<PType> {
    let DType::Extension(extension) = dtype else {
        return None;
    };
    let storage = if extension.metadata_opt::<Date>() == Some(&TimeUnit::Days) {
        PType::I32
    } else if extension
        .metadata_opt::<Timestamp>()
        .is_some_and(|metadata| metadata.unit == TimeUnit::Microseconds && metadata.tz.is_none())
    {
        PType::I64
    } else {
        return None;
    };
    matches!(extension.storage_dtype(), DType::Primitive(ptype, _) if *ptype == storage)
        .then_some(storage)
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
