// SPDX-License-Identifier: Apache-2.0
//! Admission for bounded provider run buffers, before constructing their owners.
use crate::{
    compression::Decompressor,
    encoding::{
        boolean::BooleanDecoder,
        byte::{ByteRleDecoder, DECODER_BUFFER_BYTES},
        integer::{self, NInt, RleVersion},
        PrimitiveValueDecoder,
    },
    error::{IoSnafu, Result},
    reader::memory::{decoded::Memory, DecoderMemoryReservation},
};
use snafu::ResultExt;

struct HeldDecoder<T> {
    inner: Box<dyn PrimitiveValueDecoder<T> + Send>,
    credit: Box<dyn DecoderMemoryReservation>,
    bytes: usize,
}

impl<T> PrimitiveValueDecoder<T> for HeldDecoder<T> {
    fn decode(&mut self, out: &mut [T]) -> Result<()> {
        self.credit.resize(self.bytes).context(IoSnafu)?;
        self.inner.decode(out)
    }
    fn skip(&mut self, count: usize) -> Result<()> {
        self.credit.resize(self.bytes).context(IoSnafu)?;
        self.inner.skip(count)
    }
}

fn held<T: 'static>(
    memory: Memory,
    bytes: usize,
    make: impl FnOnce() -> Box<dyn PrimitiveValueDecoder<T> + Send>,
) -> Result<Box<dyn PrimitiveValueDecoder<T> + Send>> {
    let credit = memory
        .as_ref()
        .map(|memory| memory.reserve(bytes))
        .transpose()
        .context(IoSnafu)?;
    let inner = make();
    Ok(match credit {
        Some(credit) => Box::new(HeldDecoder {
            inner,
            credit,
            bytes,
        }),
        None => inner,
    })
}

pub(super) fn boolean(
    reader: Decompressor,
    memory: Memory,
) -> Result<Box<dyn PrimitiveValueDecoder<bool> + Send>> {
    held(memory, DECODER_BUFFER_BYTES, || {
        Box::new(BooleanDecoder::new(reader))
    })
}

pub(super) fn byte(
    reader: Decompressor,
    memory: Memory,
) -> Result<Box<dyn PrimitiveValueDecoder<i8> + Send>> {
    held(memory, DECODER_BUFFER_BYTES, || {
        Box::new(ByteRleDecoder::new(reader))
    })
}

pub(super) fn signed<N: NInt>(
    reader: Decompressor,
    version: RleVersion,
    memory: Memory,
) -> Result<Box<dyn PrimitiveValueDecoder<N> + Send>> {
    held(memory, integer::decoder_buffer_bytes::<N>(version), || {
        integer::get_signed_int_decoder(reader, version)
    })
}

pub(super) fn unsigned<N: NInt>(
    reader: Decompressor,
    version: RleVersion,
    memory: Memory,
) -> Result<Box<dyn PrimitiveValueDecoder<N> + Send>> {
    held(memory, integer::decoder_buffer_bytes::<N>(version), || {
        integer::get_unsigned_int_decoder(reader, version)
    })
}
