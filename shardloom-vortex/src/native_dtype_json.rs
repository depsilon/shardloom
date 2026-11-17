//! The native compact `DType` JSON boundary, with admitted borrowed continuations.
//! Generic provider Serde remains the compatibility reference.

use std::io::Write;

use serde::ser::Error as _;
use shardloom_core::ShardLoomError;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::extension::ExtDTypeRef;
use vortex::array::dtype::{DType, DTypeChildren, MapDType, Nullability};

use super::{Scope, failed};

#[derive(Debug)]
pub(crate) enum Error {
    Resource(ShardLoomError),
    Json(serde_json::Error),
}

impl Error {
    pub(crate) fn into_error(
        self,
        json_error: impl FnOnce(serde_json::Error) -> ShardLoomError,
    ) -> ShardLoomError {
        match self {
            Self::Resource(error) => error,
            Self::Json(error) => json_error(error),
        }
    }
}

impl From<ShardLoomError> for Error {
    fn from(error: ShardLoomError) -> Self {
        Self::Resource(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Json(serde_json::Error::io(error))
    }
}

type Result<T> = std::result::Result<T, Error>;

enum Frame<'a> {
    List(Nullability),
    FixedList(u32, Nullability),
    MapValue(&'a MapDType, Nullability),
    MapEnd(bool, Nullability),
    Fields {
        dtype: &'a DType,
        children: DTypeChildren<'a>,
        first: bool,
    },
    Extension(&'a ExtDTypeRef),
}

struct Pending<'a> {
    frames: Vec<Frame<'a>>,
    // Drop borrowed frames before their capacity reservation.
    credit: MemoryLease,
}

impl<'a> Pending<'a> {
    fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            frames: Vec::new(),
            credit: memory.reserve(0)?,
        })
    }

    fn push(&mut self, frame: Frame<'a>) -> Result<()> {
        if self.frames.len() == self.frames.capacity() {
            let capacity = self
                .frames
                .capacity()
                .max(4)
                .checked_mul(2)
                .ok_or_else(|| failed("schema JSON continuation capacity overflow"))?;
            let bytes = capacity
                .checked_mul(size_of::<Frame<'_>>())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| failed("schema JSON continuation capacity overflow"))?;
            // Keep the old allocation charged until replacement capacity is installed.
            self.credit.resize(
                self.credit
                    .bytes()
                    .checked_add(bytes)
                    .ok_or_else(|| failed("schema JSON continuation capacity overflow"))?,
            )?;
            self.frames
                .try_reserve_exact(capacity - self.frames.len())
                .map_err(|_| failed("schema JSON continuation allocation failed"))?;
            if self.frames.capacity() > capacity {
                return Err(failed("schema JSON continuation exceeded admitted capacity").into());
            }
            self.credit.resize(bytes)?;
        }
        self.frames.push(frame);
        Ok(())
    }
}

fn provider(error: &vortex::error::VortexError) -> Error {
    serde_json::Error::custom(error.to_string()).into()
}

fn field(writer: &mut impl Write, name: &str, first: bool) -> Result<()> {
    if !first {
        writer.write_all(b",")?;
    }
    serde_json::to_writer(&mut *writer, name)?;
    writer.write_all(b":")?;
    Ok(())
}

fn variant(writer: &mut impl Write, name: &str, tuple: bool) -> Result<()> {
    writer.write_all(b"{")?;
    field(writer, name, true)?;
    if tuple {
        writer.write_all(b"[")?;
    }
    Ok(())
}

fn finish_tuple(writer: &mut impl Write, nullable: Nullability) -> Result<()> {
    writer.write_all(b",")?;
    serde_json::to_writer(&mut *writer, &nullable)?;
    // Keep the compact serializer's two separate closing writes.
    writer.write_all(b"]")?;
    writer.write_all(b"}")?;
    Ok(())
}

fn enter<'a>(
    dtype: &'a DType,
    scope: Scope<'_>,
    pending: &mut Pending<'a>,
    writer: &mut impl Write,
) -> Result<Option<&'a DType>> {
    let mut current: Option<&DType> = None;
    match dtype {
        DType::List(child, nullable) => {
            pending.push(Frame::List(*nullable))?;
            variant(writer, "List", true)?;
            current = Some(child);
        }
        DType::FixedSizeList(child, size, nullable) => {
            pending.push(Frame::FixedList(*size, *nullable))?;
            variant(writer, "FixedSizeList", true)?;
            current = Some(child);
        }
        DType::Map(map, nullable) => {
            pending.push(Frame::MapValue(map, *nullable))?;
            variant(writer, "Map", true)?;
            current = Some(map.try_key_dtype().map_err(|error| provider(&error))?);
        }
        DType::Struct(..) | DType::Union(..) => {
            pending.push(Frame::Fields {
                dtype,
                children: dtype.try_child_dtypes(),
                first: true,
            })?;
            let (tag, names) = match dtype {
                DType::Struct(fields, _) => ("Struct", fields.names()),
                DType::Union(variants, _) => ("Union", variants.names()),
                _ => unreachable!(),
            };
            variant(writer, tag, true)?;
            writer.write_all(b"{")?;
            field(writer, "names", true)?;
            serde_json::to_writer(&mut *writer, names)?;
            // Generic Serde collects direct children before starting
            // the dtype sequence. Warm the same lazy fields in order;
            // later continuation reads borrow those cached values.
            for child in dtype.try_child_dtypes() {
                scope.check_cancelled()?;
                child.map_err(|error| provider(&error))?;
            }
            field(writer, "dtypes", false)?;
            writer.write_all(b"[")?;
        }
        DType::Extension(extension) => {
            pending.push(Frame::Extension(extension))?;
            variant(writer, "Extension", false)?;
            writer.write_all(b"{")?;
            field(writer, "id", true)?;
            serde_json::to_writer(&mut *writer, extension.id().as_ref())?;
            field(writer, "storage_dtype", false)?;
            current = Some(extension.storage_dtype());
        }
        // These variants have no nested DType. Retain their provider
        // spelling, numeric formatting and nullability serialization.
        _ => serde_json::to_writer(writer, dtype)?,
    }
    Ok(current)
}

pub(crate) fn write<'a>(
    dtype: &DType,
    scope: impl Into<Scope<'a>>,
    mut writer: impl Write,
) -> Result<()> {
    let scope = scope.into();
    let mut pending = Pending::new(scope.memory())?;
    let mut current = Some(dtype);
    loop {
        scope.check_cancelled()?;
        if let Some(dtype) = current.take() {
            current = enter(dtype, scope, &mut pending, &mut writer)?;
            continue;
        }
        let Some(frame) = pending.frames.pop() else {
            return Ok(());
        };
        match frame {
            Frame::List(nullable) => finish_tuple(&mut writer, nullable)?,
            Frame::FixedList(size, nullable) => {
                writer.write_all(b",")?;
                serde_json::to_writer(&mut writer, &size)?;
                finish_tuple(&mut writer, nullable)?;
            }
            Frame::MapValue(map, nullable) => {
                // The original evaluates value_dtype before writing this
                // tuple element's separator.
                let value = map.try_value_dtype().map_err(|error| provider(&error))?;
                pending.push(Frame::MapEnd(map.keys_sorted(), nullable))?;
                writer.write_all(b",")?;
                current = Some(value);
            }
            Frame::MapEnd(sorted, nullable) => {
                writer.write_all(b",")?;
                serde_json::to_writer(&mut writer, &sorted)?;
                finish_tuple(&mut writer, nullable)?;
            }
            Frame::Fields {
                dtype,
                mut children,
                first,
            } => {
                if let Some(child) = children.next() {
                    let child = child.map_err(|error| provider(&error))?;
                    pending.push(Frame::Fields {
                        dtype,
                        children,
                        first: false,
                    })?;
                    if !first {
                        writer.write_all(b",")?;
                    }
                    current = Some(child);
                } else {
                    writer.write_all(b"]")?;
                    if let DType::Union(variants, _) = dtype {
                        field(&mut writer, "type_ids", false)?;
                        serde_json::to_writer(&mut writer, variants.type_ids())?;
                    }
                    writer.write_all(b"}")?;
                    finish_tuple(&mut writer, dtype.nullability())?;
                }
            }
            Frame::Extension(extension) => {
                let metadata = extension
                    .serialize_metadata()
                    .map_err(|error| provider(&error))?;
                field(&mut writer, "metadata", false)?;
                serde_json::to_writer(&mut writer, &metadata)?;
                writer.write_all(b"}")?;
                writer.write_all(b"}")?;
            }
        }
    }
}

#[cfg(test)]
#[path = "native_dtype_json_tests.rs"]
mod tests;
