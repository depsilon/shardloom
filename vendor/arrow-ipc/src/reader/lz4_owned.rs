// SPDX-License-Identifier: Apache-2.0
//! Bounded framing and capacities for the pinned lz4_flex 0.14 frame decoder.

use super::decoder_memory::{DecoderMemory, invalid};
use arrow_schema::ArrowError;
use std::io::Read;

const MODERN_MAGIC: u32 = 0x184d_2204;
const LEGACY_MAGIC: u32 = 0x184c_2102;

pub(crate) fn decompress(
    input: &[u8],
    output: &mut [u8],
    memory: &dyn DecoderMemory,
) -> Result<(), ArrowError> {
    // The pinned reader accepts EOF even in place of a modern EndMark. Verify
    // complete framing independently, without constructing decoded values.
    let capacity = workspace(input, output.len())?;
    let _credit = memory.reserve(capacity)?;
    let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
    decoder.read_exact(output)?;
    let mut extra = [0];
    if decoder.read(&mut extra)? != 0 {
        return Err(invalid("LZ4 output exceeds its declared IPC length"));
    }
    if !decoder.get_ref().is_empty() {
        return Err(invalid("LZ4 input has unconsumed bytes"));
    }
    Ok(())
}

fn word(input: &mut &[u8]) -> Result<u32, ArrowError> {
    let bytes = take(input, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], ArrowError> {
    if count > input.len() {
        return Err(invalid("truncated LZ4 framing"));
    }
    let (head, tail) = input.split_at(count);
    *input = tail;
    Ok(head)
}

fn workspace(mut input: &[u8], output_length: usize) -> Result<usize, ArrowError> {
    let magic = word(&mut input)?;
    let (maximum, linked, block_checksums, content_checksum, legacy) = match magic {
        LEGACY_MAGIC => (8 * 1024 * 1024, false, false, false, true),
        MODERN_MAGIC => {
            let flags = take(&mut input, 2)?;
            let (flg, bd) = (flags[0], flags[1]);
            if flg & 0xc0 != 0x40 || flg & 0x02 != 0 || bd & 0x8f != 0 {
                return Err(invalid("invalid LZ4 frame flags"));
            }
            let maximum = match (bd >> 4) & 7 {
                4 => 64 * 1024,
                5 => 256 * 1024,
                6 => 1024 * 1024,
                7 => 4 * 1024 * 1024,
                _ => return Err(invalid("invalid LZ4 block capacity")),
            };
            if flg & 8 != 0 {
                let size = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
                if size != output_length as u64 {
                    return Err(invalid("LZ4 content size differs from its IPC length"));
                }
            }
            if flg & 1 != 0 {
                return Err(invalid("LZ4 frame dictionaries are unsupported"));
            }
            // The existing codec validates this header checksum and all block
            // and content checksums. This pass only validates storage extents.
            take(&mut input, 1)?;
            (
                maximum,
                flg & 0x20 == 0,
                flg & 0x10 != 0,
                flg & 4 != 0,
                false,
            )
        }
        _ => return Err(invalid("invalid LZ4 frame magic")),
    };
    loop {
        // Legacy framing may end at a complete block boundary without EndMark.
        if legacy && input.is_empty() {
            break;
        }
        let block = word(&mut input)?;
        if block == 0 {
            if content_checksum {
                take(&mut input, 4)?;
            }
            if !input.is_empty() {
                return Err(invalid("trailing data after LZ4 frame"));
            }
            break;
        }
        let length = (block & 0x7fff_ffff) as usize;
        if length == 0 || length > maximum {
            return Err(invalid("invalid LZ4 block extent"));
        }
        take(&mut input, length)?;
        if block_checksums {
            take(&mut input, 4)?;
        }
    }
    // Pinned FrameDecoder allocates src=max_block, plus dst=max_block for
    // independent blocks or dst=2*max_block+64KiB for linked blocks.
    Ok(if linked {
        maximum * 3 + 64 * 1024
    } else {
        maximum * 2
    })
}
