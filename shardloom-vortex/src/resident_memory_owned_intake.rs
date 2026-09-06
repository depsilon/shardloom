//! Safe ownership transfer into the existing flat native memory source.

use std::{ops::Range, sync::Arc};

use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{BoolArray, PrimitiveArray, VarBinArray},
        dtype::{DType, NativePType, Nullability},
        validity::Validity,
    },
    buffer::{Buffer, ByteBuffer},
};

use super::{memory_error, native_error, packed_bits};
use crate::resident_session::ResidentVortexSession;

/// A flat native column created only through capacity-admitted ownership
/// transfer. Immutable slices retain the original backing allocation's credit.
/// The identity lease establishes the pool only; the real credits live inside
/// each buffer owner. Arbitrary `ArrayRef` values cannot enter this contract.
#[derive(Clone)]
pub struct OwnedMemoryColumn {
    pub(super) array: ArrayRef,
    pub(super) name: String,
    pub(super) identity: Arc<MemoryLease>,
}

impl OwnedMemoryColumn {
    /// Consume Int64 storage without copying its numeric payload. An optional
    /// validity vector is packed into a separate admitted native bitmap.
    ///
    /// # Errors
    /// Rejects invalid names/lengths, more than 65,536 rows and shared-byte denial.
    pub fn int64(
        session: &ResidentVortexSession,
        name: &str,
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    ) -> Result<Self> {
        validate(name, values.len(), valid.as_deref())?;
        let validity = consume_validity(session, values.len(), valid)?;
        let array = PrimitiveArray::new(admit_vec(session, values)?, validity).into_array();
        Self::new(session, name, array)
    }

    /// Consume finite Float64 storage without copying its numeric payload.
    ///
    /// # Errors
    /// Rejects nonfinite present values, invalid shape and shared-byte denial.
    pub fn float64(
        session: &ResidentVortexSession,
        name: &str,
        values: Vec<f64>,
        valid: Option<Vec<bool>>,
    ) -> Result<Self> {
        validate(name, values.len(), valid.as_deref())?;
        if values
            .iter()
            .enumerate()
            .any(|(row, value)| valid.as_ref().is_none_or(|valid| valid[row]) && !value.is_finite())
        {
            return Err(memory_error(
                "nonfinite float64 is not admitted by bounded JSON output",
            ));
        }
        let validity = consume_validity(session, values.len(), valid)?;
        Self::new(
            session,
            name,
            PrimitiveArray::new(admit_vec(session, values)?, validity).into_array(),
        )
    }

    /// Pack owned Boolean values into a native bitmap. This operation builds
    /// bitmap bytes; unlike numeric/UTF8 payload transfer, it is not zero-copy.
    ///
    /// # Errors
    /// Rejects invalid shape and shared-byte denial.
    pub fn boolean(
        session: &ResidentVortexSession,
        name: &str,
        values: Vec<bool>,
        valid: Option<Vec<bool>>,
    ) -> Result<Self> {
        validate(name, values.len(), valid.as_deref())?;
        let validity = consume_validity(session, values.len(), valid)?;
        let bits = packed_bits(&session.native_allocator(), values.len(), |row| values[row])?;
        drop(values);
        Self::new(session, name, BoolArray::new(bits, validity).into_array())
    }

    /// Consume contiguous UTF8 bytes and u64 offsets without copying either
    /// payload. Offsets must begin at zero, be monotonic, and end at `bytes.len()`.
    /// Nullability is explicit through the optional validity vector.
    ///
    /// # Errors
    /// Rejects invalid UTF8/offset boundaries, invalid shape and shared-byte denial.
    pub fn utf8(
        session: &ResidentVortexSession,
        name: &str,
        offsets: Vec<u64>,
        bytes: Vec<u8>,
        valid: Option<Vec<bool>>,
    ) -> Result<Self> {
        let rows = offsets
            .len()
            .checked_sub(1)
            .ok_or_else(|| memory_error("UTF8 offsets require at least one entry"))?;
        validate(name, rows, valid.as_deref())?;
        let text = std::str::from_utf8(&bytes).map_err(native_error)?;
        if offsets.first() != Some(&0)
            || offsets.last().copied() != Some(bytes.len() as u64)
            || offsets.windows(2).any(|pair| pair[0] > pair[1])
            || offsets.iter().any(|&offset| {
                usize::try_from(offset)
                    .ok()
                    .is_none_or(|offset| !text.is_char_boundary(offset))
            })
        {
            return Err(memory_error(
                "UTF8 offsets must bound complete valid strings",
            ));
        }
        let nullable = if valid.is_some() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        let validity = consume_validity(session, rows, valid)?;
        let offsets =
            PrimitiveArray::new(admit_vec(session, offsets)?, Validity::NonNullable).into_array();
        let bytes = admit_vec(session, bytes)?.into_byte_buffer();
        let array = VarBinArray::try_new(offsets, bytes, DType::Utf8(nullable), validity)
            .map_err(native_error)?
            .into_array();
        Self::new(session, name, array)
    }

    fn new(session: &ResidentVortexSession, name: &str, array: ArrayRef) -> Result<Self> {
        Ok(Self {
            array,
            name: name.to_owned(),
            identity: Arc::new(session.memory().reserve(0)?),
        })
    }

    /// Native ownership for embedding callers; clones retain all backing credit.
    #[must_use]
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    /// Create an immutable native slice without releasing backing-buffer credit.
    ///
    /// # Errors
    /// Rejects reversed/out-of-bounds ranges and native slicing errors.
    pub fn slice(&self, range: Range<usize>) -> Result<Self> {
        if range.start > range.end || range.end > self.array.len() {
            return Err(memory_error("owned column slice exceeds source rows"));
        }
        Ok(Self {
            array: self.array.slice(range).map_err(native_error)?,
            name: self.name.clone(),
            identity: Arc::clone(&self.identity),
        })
    }
}

fn validate(name: &str, rows: usize, valid: Option<&[bool]>) -> Result<()> {
    if name.is_empty()
        || name.len() > 256
        || rows > 65_536
        || valid.is_some_and(|valid| valid.len() != rows)
    {
        return Err(memory_error(
            "owned column requires a 1..=256-byte name, at most 65,536 rows and matching validity",
        ));
    }
    Ok(())
}

fn make_validity(
    session: &ResidentVortexSession,
    rows: usize,
    valid: Option<&[bool]>,
) -> Result<Validity> {
    let Some(valid) = valid else {
        return Ok(Validity::NonNullable);
    };
    if valid.iter().all(|value| *value) {
        return Ok(Validity::AllValid);
    }
    if valid.iter().all(|value| !*value) {
        return Ok(Validity::AllInvalid);
    }
    Ok(Validity::Array(
        BoolArray::new(
            packed_bits(&session.native_allocator(), rows, |row| valid[row])?,
            Validity::NonNullable,
        )
        .into_array(),
    ))
}

fn consume_validity(
    session: &ResidentVortexSession,
    rows: usize,
    valid: Option<Vec<bool>>,
) -> Result<Validity> {
    let packed = make_validity(session, rows, valid.as_deref());
    // The owned intake consumes caller validity storage after packing it; the
    // native bitmap has its own allocation owner and never borrows this vector.
    drop(valid);
    packed
}

fn admit_vec<T: NativePType>(session: &ResidentVortexSession, values: Vec<T>) -> Result<Buffer<T>> {
    let capacity = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| memory_error("owned vector capacity overflow"))?;
    // The caller allocated this Vec before intake; admission transfers that
    // existing full capacity without a second allocation or payload copy.
    let lease = session.memory().reserve(capacity)?;
    let buffer = Buffer::from(values).into_byte_buffer();
    let alignment = buffer.alignment();
    let owner = ImportedBufferOwner {
        buffer,
        _lease: lease,
    };
    Ok(Buffer::from_byte_buffer(ByteBuffer::from_bytes_aligned(
        bytes::Bytes::from_owner(owner),
        alignment,
    )))
}

// The payload drops before the credit, including when a sliced clone is last.
struct ImportedBufferOwner {
    buffer: ByteBuffer,
    _lease: MemoryLease,
}
impl AsRef<[u8]> for ImportedBufferOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
