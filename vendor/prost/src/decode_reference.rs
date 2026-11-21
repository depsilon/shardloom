// Unmodified published Prost 0.14.4 helper; independent test-only reference.
use super::encoding::{decode_key, decode_varint, DecodeContext, WireType};
use super::error::DecodeErrorKind;
use super::DecodeError;
use bytes::Buf;

pub fn skip_field(
    wire_type: WireType,
    tag: u32,
    buf: &mut impl Buf,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    ctx.limit_reached()?;
    let len = match wire_type {
        WireType::Varint => decode_varint(buf).map(|_| 0)?,
        WireType::ThirtyTwoBit => 4,
        WireType::SixtyFourBit => 8,
        WireType::LengthDelimited => decode_varint(buf)?,
        WireType::StartGroup => loop {
            let (inner_tag, inner_wire_type) = decode_key(buf)?;
            match inner_wire_type {
                WireType::EndGroup => {
                    if inner_tag != tag {
                        return Err(DecodeErrorKind::UnexpectedEndGroupTag.into());
                    }
                    break 0;
                }
                _ => skip_field(inner_wire_type, inner_tag, buf, ctx.enter_recursion())?,
            }
        },
        WireType::EndGroup => return Err(DecodeErrorKind::UnexpectedEndGroupTag.into()),
    };

    if len > buf.remaining() as u64 {
        return Err(DecodeErrorKind::BufferUnderflow.into());
    }

    buf.advance(len as usize);
    Ok(())
}
