//! Bounded typed intake into immutable, caller-owned native Vortex memory.
//!
//! Borrowed input is caller-owned. Native value/offset/validity buffers are
//! allocated through the session's reservation owner before publication. This
//! does not claim to account for caller storage, array metadata, all upstream
//! operator scratch, or process RSS. No file, answer cache, or external engine
//! participates in this source.

use std::sync::Arc;

use shardloom_core::{Result, ShardLoomError};
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{BoolArray, PrimitiveArray, StructArray, VarBinArray},
        dtype::{DType, FieldNames, Nullability},
        memory::HostAllocatorRef,
        validity::Validity,
    },
    buffer::{Alignment, BitBuffer, Buffer, ByteBuffer},
    expr::{BoundExpression, Expression, root, select},
    mask::Mask,
};

use crate::local_primitives::collect::{
    CollectedVortexRows, memory_certificate, render_owned_json,
};
use crate::resident_session::{OwnedVortexResultBatch, ResidentVortexSession};

/// Explicit nullable flat-scalar input. Slices are borrowed only for intake;
/// published Vortex buffers retain no references into caller memory.
#[derive(Clone, Copy)]
pub enum MemoryColumnValues<'a> {
    Int64(&'a [Option<i64>]),
    Float64(&'a [Option<f64>]),
    Bool(&'a [Option<bool>]),
    Utf8(&'a [Option<&'a str>]),
}

/// A named typed column in one immutable snapshot.
#[derive(Clone, Copy)]
pub struct MemoryColumn<'a> {
    pub name: &'a str,
    pub values: MemoryColumnValues<'a>,
}

/// Hard input and result bounds for this admitted flat-scalar source.
#[derive(Debug, Clone, Copy)]
pub struct MemorySourceBounds {
    pub max_input_rows: usize,
    pub max_input_bytes: usize,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

impl Default for MemorySourceBounds {
    fn default() -> Self {
        Self {
            max_input_rows: 65_536,
            max_input_bytes: 32 * 1024 * 1024,
            max_output_rows: 65_536,
            max_output_bytes: 32 * 1024 * 1024,
        }
    }
}

struct MemorySourceOwner {
    array: ArrayRef,
    session: ResidentVortexSession,
    bounds: MemorySourceBounds,
    input_logical_bytes: usize,
}

/// Validated immutable native memory, visible without durable publication.
#[derive(Clone)]
pub struct ResidentMemorySource(Arc<MemorySourceOwner>);

impl ResidentMemorySource {
    /// Validate all input sizes and values before allocating native buffers.
    /// Caller-owned input bytes are copied once into session-owned native arrays.
    ///
    /// # Errors
    /// Rejects duplicate/empty names, mismatched column lengths, nonfinite
    /// floats, more than 64 columns, and input/shared-memory bound violations.
    pub fn from_columns(
        session: &ResidentVortexSession,
        columns: &[MemoryColumn<'_>],
        bounds: MemorySourceBounds,
    ) -> Result<Self> {
        let (rows, input_logical_bytes) = validate_columns(columns, bounds)?;
        let allocator = session.native_allocator();
        let fields = columns
            .iter()
            .map(|column| build_column(column.values, &allocator))
            .collect::<Result<Vec<_>>>()?;
        let names = FieldNames::from(columns.iter().map(|column| column.name).collect::<Vec<_>>());
        let array = StructArray::try_new(names, fields, rows, Validity::NonNullable)
            .map_err(native_error)?
            .into_array();
        Ok(Self(Arc::new(MemorySourceOwner {
            array,
            session: session.clone(),
            bounds,
            input_logical_bytes,
        })))
    }

    #[must_use]
    pub fn row_count(&self) -> usize {
        self.0.array.len()
    }

    #[must_use]
    pub fn dtype(&self) -> &DType {
        self.0.array.dtype()
    }

    #[must_use]
    pub fn input_logical_bytes(&self) -> usize {
        self.0.input_logical_bytes
    }

    /// Bind a native projection and optional exact Vortex expression. This is
    /// an internal Rust provider boundary; public adapters lower their admitted
    /// predicates before reaching it. A limit preserves input order after filtering.
    ///
    /// # Errors
    /// Rejects missing/duplicate fields, invalid/nonboolean filters, and limits
    /// beyond the source's admitted result bound.
    pub fn prepare_projection(
        &self,
        columns: &[&str],
        filter: Option<Expression>,
        limit: Option<usize>,
    ) -> Result<PreparedMemoryProjection> {
        if columns.is_empty() || columns.len() > 64 {
            return Err(memory_error("projection requires 1..=64 fields"));
        }
        for (index, name) in columns.iter().enumerate() {
            if columns[..index].contains(name) {
                return Err(memory_error("duplicate projection fields"));
            }
        }
        if limit.is_some_and(|limit| limit > self.0.bounds.max_output_rows) {
            return Err(memory_error(
                "requested row limit exceeds admitted output bound",
            ));
        }
        let projection = select(columns.to_vec(), root())
            .bind(self.dtype())
            .map_err(native_error)?;
        let filter = filter
            .map(|filter| {
                filter
                    .optimize_recursive(self.dtype())
                    .and_then(|filter| filter.bind(self.dtype()))
                    .map_err(native_error)
            })
            .transpose()?;
        if filter
            .as_ref()
            .is_some_and(|filter| !matches!(filter.dtype(), DType::Bool(_)))
        {
            return Err(memory_error("memory filter must return a boolean value"));
        }
        Ok(PreparedMemoryProjection {
            source: self.clone(),
            projection,
            filter,
            limit,
            columns: columns.iter().map(|name| (*name).to_owned()).collect(),
        })
    }
}

/// Bound exact native operations over one immutable memory snapshot.
pub struct PreparedMemoryProjection {
    source: ResidentMemorySource,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    limit: Option<usize>,
    columns: Vec<String>,
}

impl PreparedMemoryProjection {
    #[must_use]
    pub fn projected_columns(&self) -> &[String] {
        &self.columns
    }

    /// Complete native filter/project/limit execution and retain owned arrays.
    ///
    /// # Errors
    /// Rejects native provider errors and result/shared-memory bound violations.
    pub fn execute_arrays(&self) -> Result<OwnedVortexResultBatch> {
        let source = &self.source.0;
        source.session.execute_owned_array(
            source.bounds.max_output_rows as u64,
            source.bounds.max_output_bytes as u64,
            |session| {
                let mut context = session.create_execution_ctx();
                let mut array = source.array.clone();
                if let Some(filter) = &self.filter {
                    let predicate = array
                        .clone()
                        .apply_bound(filter)
                        .map_err(native_error)?
                        .execute::<BoolArray>(&mut context)
                        .map_err(native_error)?;
                    let allocator = source.session.native_allocator();
                    // The final mask has a session-owned buffer. Null predicate
                    // values are false, matching WHERE semantics. Evaluation is
                    // still the provider's bound expression, not a row evaluator.
                    let mut bits = allocator
                        .allocate(array.len().div_ceil(8), Alignment::none())
                        .map_err(native_error)?;
                    bits.as_mut_slice().fill(0);
                    for row in 0..array.len() {
                        if predicate
                            .execute_scalar(row, &mut context)
                            .map_err(native_error)?
                            .as_bool()
                            .value()
                            .unwrap_or(false)
                        {
                            bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
                        }
                    }
                    array = array
                        .filter(Mask::from_buffer(BitBuffer::new(
                            bits.freeze(),
                            array.len(),
                        )))
                        .map_err(native_error)?;
                }
                if let Some(limit) = self.limit {
                    array = array
                        .slice(0..limit.min(array.len()))
                        .map_err(native_error)?;
                }
                array
                    .apply_bound(&self.projection)
                    .and_then(|array| array.execute::<StructArray>(&mut context))
                    .map(vortex::array::IntoArray::into_array)
                    .map_err(native_error)
            },
        )
    }

    /// Complete the bounded native operation and its explicitly requested JSON
    /// materialization. The result retains its output reservation independently
    /// of the source. This publishes no durable file and caches no answer.
    ///
    /// # Errors
    /// Rejects provider, row, native-byte, JSON-byte, and shared-memory violations.
    pub fn execute(&self) -> Result<CollectedVortexRows> {
        let result = self.execute_arrays()?;
        let source = &self.source.0;
        let values_json = render_owned_json(
            &result,
            &self.columns,
            source.session.memory(),
            source.bounds.max_output_bytes,
        )?;
        let rows = result.row_count();
        let native_io_certificate = memory_certificate(rows, self.filter.is_some())?;
        drop(result);
        Ok(CollectedVortexRows {
            rows,
            projected_columns: self.columns.clone(),
            source_order_limit: self.limit,
            values_json,
            runtime: source.session.snapshot(),
            native_io_certificate,
        })
    }
}

impl MemoryColumnValues<'_> {
    fn len(self) -> usize {
        match self {
            Self::Int64(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::Utf8(values) => values.len(),
        }
    }

    fn present(self, row: usize) -> bool {
        match self {
            Self::Int64(values) => values[row].is_some(),
            Self::Float64(values) => values[row].is_some(),
            Self::Bool(values) => values[row].is_some(),
            Self::Utf8(values) => values[row].is_some(),
        }
    }
}

fn validate_columns(
    columns: &[MemoryColumn<'_>],
    bounds: MemorySourceBounds,
) -> Result<(usize, usize)> {
    if columns.is_empty()
        || columns.len() > 64
        || bounds.max_input_rows > 65_536
        || bounds.max_output_rows > 65_536
        || bounds.max_input_rows == 0
        || bounds.max_output_rows == 0
        || bounds.max_input_bytes == 0
        || bounds.max_output_bytes == 0
    {
        return Err(memory_error(
            "memory intake requires 1..=64 columns and positive bounds up to 65,536 rows",
        ));
    }
    let rows = columns[0].values.len();
    if rows > bounds.max_input_rows {
        return Err(memory_error("input row bound exceeded"));
    }
    let mut bytes = 0_usize;
    for (index, column) in columns.iter().enumerate() {
        if column.name.is_empty()
            || column.name.len() > 256
            || columns[..index]
                .iter()
                .any(|prior| prior.name == column.name)
        {
            return Err(memory_error(
                "column names must be distinct and contain 1..=256 UTF8 bytes",
            ));
        }
        if column.values.len() != rows {
            return Err(memory_error("typed memory column lengths disagree"));
        }
        let value_bytes = match column.values {
            MemoryColumnValues::Int64(_) => rows.checked_mul(8),
            MemoryColumnValues::Float64(values) => {
                if values.iter().flatten().any(|value| !value.is_finite()) {
                    return Err(memory_error(
                        "nonfinite float64 is not admitted by bounded JSON output",
                    ));
                }
                rows.checked_mul(8)
            }
            MemoryColumnValues::Bool(_) => Some(rows.div_ceil(8)),
            MemoryColumnValues::Utf8(values) => values.iter().flatten().fold(
                rows.checked_add(1).and_then(|rows| rows.checked_mul(8)),
                |total, value| total.and_then(|total| total.checked_add(value.len())),
            ),
        }
        .ok_or_else(|| memory_error("typed memory byte count overflow"))?;
        bytes = bytes
            .checked_add(value_bytes)
            .and_then(|bytes| bytes.checked_add(rows.div_ceil(8)))
            .and_then(|bytes| bytes.checked_add(column.name.len()))
            .ok_or_else(|| memory_error("typed memory byte count overflow"))?;
        if bytes > bounds.max_input_bytes {
            return Err(memory_error("input byte bound exceeded"));
        }
    }
    Ok((rows, bytes))
}

fn packed_bits(
    allocator: &HostAllocatorRef,
    len: usize,
    value: impl Fn(usize) -> bool,
) -> Result<BitBuffer> {
    let mut bytes = allocator
        .allocate(len.div_ceil(8), Alignment::none())
        .map_err(native_error)?;
    bytes.as_mut_slice().fill(0);
    for row in 0..len {
        if value(row) {
            bytes.as_mut_slice()[row / 8] |= 1 << (row % 8);
        }
    }
    Ok(BitBuffer::new(bytes.freeze(), len))
}

fn fixed_bytes(
    allocator: &HostAllocatorRef,
    len: usize,
    value: impl Fn(usize) -> [u8; 8],
) -> Result<ByteBuffer> {
    let bytes = len
        .checked_mul(8)
        .ok_or_else(|| memory_error("native buffer length overflow"))?;
    let mut bytes = allocator
        .allocate(bytes, Alignment::new(8))
        .map_err(native_error)?;
    for row in 0..len {
        bytes.as_mut_slice()[row * 8..row * 8 + 8].copy_from_slice(&value(row));
    }
    Ok(bytes.freeze())
}

fn build_column(values: MemoryColumnValues<'_>, allocator: &HostAllocatorRef) -> Result<ArrayRef> {
    let rows = values.len();
    let validity = if (0..rows).all(|row| values.present(row)) {
        Validity::AllValid
    } else if (0..rows).all(|row| !values.present(row)) {
        Validity::AllInvalid
    } else {
        Validity::Array(
            BoolArray::new(
                packed_bits(allocator, rows, |row| values.present(row))?,
                Validity::NonNullable,
            )
            .into_array(),
        )
    };
    match values {
        MemoryColumnValues::Int64(values) => Ok(PrimitiveArray::new(
            Buffer::<i64>::from_byte_buffer(fixed_bytes(allocator, rows, |row| {
                values[row].unwrap_or_default().to_ne_bytes()
            })?),
            validity,
        )
        .into_array()),
        MemoryColumnValues::Float64(values) => Ok(PrimitiveArray::new(
            Buffer::<f64>::from_byte_buffer(fixed_bytes(allocator, rows, |row| {
                values[row].unwrap_or_default().to_ne_bytes()
            })?),
            validity,
        )
        .into_array()),
        MemoryColumnValues::Bool(values) => Ok(BoolArray::new(
            packed_bits(allocator, rows, |row| values[row].unwrap_or_default())?,
            validity,
        )
        .into_array()),
        MemoryColumnValues::Utf8(values) => {
            let total = values.iter().flatten().map(|value| value.len()).sum();
            let mut bytes = allocator
                .allocate(total, Alignment::none())
                .map_err(native_error)?;
            let mut offsets = allocator
                .allocate((rows + 1) * 8, Alignment::new(8))
                .map_err(native_error)?;
            offsets.as_mut_slice()[..8].copy_from_slice(&0_u64.to_ne_bytes());
            let mut offset = 0;
            for (row, value) in values.iter().enumerate() {
                let value = value.unwrap_or_default().as_bytes();
                bytes.as_mut_slice()[offset..offset + value.len()].copy_from_slice(value);
                offset += value.len();
                offsets.as_mut_slice()[(row + 1) * 8..(row + 2) * 8]
                    .copy_from_slice(&(offset as u64).to_ne_bytes());
            }
            let offsets = PrimitiveArray::new(
                Buffer::<u64>::from_byte_buffer(offsets.freeze()),
                Validity::NonNullable,
            )
            .into_array();
            VarBinArray::try_new(
                offsets,
                bytes.freeze(),
                DType::Utf8(Nullability::Nullable),
                validity,
            )
            .map(vortex::array::IntoArray::into_array)
            .map_err(native_error)
        }
    }
}

fn memory_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}

fn native_error(error: impl std::fmt::Display) -> ShardLoomError {
    memory_error(&error.to_string())
}

#[cfg(test)]
#[path = "resident_memory_source_tests.rs"]
mod tests;
