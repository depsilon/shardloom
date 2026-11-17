//! Ordered conversion of the restricted typed-input schema vocabulary.

use serde_json::{Map, Value};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::{DType, FieldNames, Nullability, PType, StructFields};

use super::super::native_capacity::ReservedVec;
use super::{decimal_dtype, failed, fields, nullability, temporal_dtype};

const MAX_DEPTH: usize = 24;

#[derive(Clone, Copy)]
enum Extension {
    #[cfg(feature = "native-binary-objects")]
    Binary,
    Embedding,
    Tensor,
    VariableTensor,
    Vector(crate::native_vector::Kind),
    Spatial(crate::native_spatial::Kind),
}

impl Extension {
    fn from_json(value: &Value) -> Option<Self> {
        let id = value.get("id").and_then(Value::as_str);
        #[cfg(feature = "native-binary-objects")]
        if id == Some(crate::native_binary_type::ID) {
            return Some(Self::Binary);
        }
        match id {
            Some(crate::native_embedding::ID) => Some(Self::Embedding),
            Some(crate::native_tensor::ID) => Some(Self::Tensor),
            Some(crate::native_variable_tensor::ID) => Some(Self::VariableTensor),
            _ => id
                .and_then(crate::native_vector::Kind::from_id)
                .map(Self::Vector)
                .or_else(|| {
                    id.and_then(crate::native_spatial::Kind::from_id)
                        .map(Self::Spatial)
                }),
        }
    }

    fn finish(self, fields: &Map<String, Value>, storage: DType) -> Result<DType> {
        match self {
            #[cfg(feature = "native-binary-objects")]
            Self::Binary => super::binary_object_dtype(storage),
            Self::Embedding => super::embedding_dtype(fields, storage),
            Self::Tensor => super::tensor_dtype(fields, storage),
            Self::VariableTensor => super::variable_tensor_dtype(fields, storage),
            Self::Vector(kind) => super::vector_dtype(kind, fields, storage),
            Self::Spatial(kind) => super::spatial_dtype(kind, fields, storage),
        }
    }
}

enum Frame<'a> {
    List(Nullability),
    FixedSizeList(u32, Nullability),
    Struct {
        names: Vec<&'a str>,
        inputs: &'a [Value],
        types: ReservedVec<DType>,
        child_depth: usize,
        nullable: Nullability,
    },
    Extension(Extension, &'a Map<String, Value>),
}

struct Pending<'a>(ReservedVec<Frame<'a>>);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        while self.0.values.pop().is_some() {}
    }
}

enum Started<'a> {
    Complete(DType),
    Child(Frame<'a>, &'a Value),
}

fn start<'a>(
    value: &'a Value,
    depth: usize,
    nodes: &mut usize,
    memory: &LiveMemoryPool,
    max_depth: usize,
) -> Result<Started<'a>> {
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| failed("input dtype node count overflow"))?;
    if depth > max_depth {
        return Err(failed(&format!(
            "nested input dtype exceeds depth {max_depth}"
        )));
    }
    let object = value
        .as_object()
        .filter(|value| value.len() == 1)
        .ok_or_else(|| failed("native input dtype requires one variant"))?;
    let (kind, value) = object.iter().next().expect("one dtype variant");
    if matches!(kind.as_str(), "Bool" | "Utf8" | "Binary") {
        let nullable = nullability(value)?;
        return Ok(Started::Complete(match kind.as_str() {
            "Bool" => DType::Bool(nullable),
            "Utf8" => DType::Utf8(nullable),
            _ => DType::Binary(nullable),
        }));
    }
    if kind == "Extension" {
        let Some(kind) = Extension::from_json(value) else {
            return temporal_dtype(value).map(Started::Complete);
        };
        let fields = fields(value, &["id", "metadata", "storage_dtype"])?;
        #[cfg(feature = "native-binary-objects")]
        if matches!(kind, Extension::Binary) {
            super::binary_object_metadata(fields)?;
        }
        return Ok(Started::Child(
            Frame::Extension(kind, fields),
            &fields["storage_dtype"],
        ));
    }
    let size = if kind == "FixedSizeList" { 3 } else { 2 };
    let parts = value
        .as_array()
        .filter(|parts| parts.len() == size)
        .ok_or_else(|| failed("invalid native input dtype parameters"))?;
    let nullable = nullability(&parts[size - 1])?;
    Ok(match kind.as_str() {
        "Primitive" => Started::Complete(DType::Primitive(
            match parts[0].as_str() {
                Some("i8") => PType::I8,
                Some("i16") => PType::I16,
                Some("i32") => PType::I32,
                Some("i64") => PType::I64,
                Some("u8") => PType::U8,
                Some("u16") => PType::U16,
                Some("u32") => PType::U32,
                Some("u64") => PType::U64,
                Some("f16") => PType::F16,
                Some("f32") => PType::F32,
                Some("f64") => PType::F64,
                _ => return Err(failed("unsupported native input primitive")),
            },
            nullable,
        )),
        "Decimal" => Started::Complete(decimal_dtype(&parts[0], nullable)?),
        "List" => Started::Child(Frame::List(nullable), &parts[0]),
        "FixedSizeList" => {
            let size = parts[1]
                .as_u64()
                .and_then(|size| u32::try_from(size).ok())
                .ok_or_else(|| failed("fixed-size input width must be uint32"))?;
            Started::Child(Frame::FixedSizeList(size, nullable), &parts[0])
        }
        "Struct" => record(&parts[0], depth, nullable, memory)?,
        _ => return Err(failed("unsupported native input dtype")),
    })
}

fn record<'a>(
    value: &'a Value,
    depth: usize,
    nullable: Nullability,
    memory: &LiveMemoryPool,
) -> Result<Started<'a>> {
    let fields = fields(value, &["names", "dtypes"])?;
    let names = fields["names"]
        .as_array()
        .filter(|names| !names.is_empty())
        .ok_or_else(|| failed("nested input structs require at least one field"))?;
    let inputs = fields["dtypes"]
        .as_array()
        .filter(|types| types.len() == names.len())
        .ok_or_else(|| failed("nested input names and dtypes must have equal length"))?;
    let mut distinct = std::collections::BTreeSet::new();
    let names = names
        .iter()
        .map(|name| {
            let name = name
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| failed("nested input field names must be nonempty strings"))?;
            if !distinct.insert(name) {
                return Err(failed("nested input field names must be distinct"));
            }
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut types = ReservedVec::new(memory)?;
    types.reserve(inputs.len())?;
    Ok(Started::Child(
        Frame::Struct {
            names,
            inputs,
            types,
            child_depth: depth + 1,
            nullable,
        },
        &inputs[0],
    ))
}

pub(super) fn convert(value: &Value, memory: &LiveMemoryPool) -> Result<DType> {
    convert_with_depth(value, memory, MAX_DEPTH, &mut 0)
}

pub(super) fn convert_with_depth(
    value: &Value,
    memory: &LiveMemoryPool,
    max_depth: usize,
    nodes: &mut usize,
) -> Result<DType> {
    let mut pending = Pending(ReservedVec::new(memory)?);
    let mut next = (value, 0);
    loop {
        let (value, depth) = next;
        let mut complete = match start(value, depth, nodes, memory, max_depth)? {
            Started::Complete(dtype) => dtype,
            Started::Child(frame, value) => {
                pending.0.push(frame)?;
                next = (value, depth + 1);
                continue;
            }
        };
        loop {
            let Some(frame) = pending.0.values.pop() else {
                return Ok(complete);
            };
            complete = match frame {
                Frame::List(nullable) => DType::List(complete.into(), nullable),
                Frame::FixedSizeList(size, nullable) => {
                    DType::FixedSizeList(complete.into(), size, nullable)
                }
                Frame::Extension(kind, fields) => kind.finish(fields, complete)?,
                Frame::Struct {
                    names,
                    inputs,
                    mut types,
                    child_depth,
                    nullable,
                } => {
                    types.values.push(complete);
                    if types.values.len() < inputs.len() {
                        next = (&inputs[types.values.len()], child_depth);
                        pending.0.values.push(Frame::Struct {
                            names,
                            inputs,
                            types,
                            child_depth,
                            nullable,
                        });
                        break;
                    }
                    // Keep the admitted child vector intact through name
                    // allocation; the constructor then owns it before credit
                    // is released. The JSON workspace covers the native result
                    // until its complete persistent metadata is admitted.
                    let names = FieldNames::from(names);
                    let (types, credit) = types.into_parts();
                    let dtype = DType::Struct(StructFields::new(names, types), nullable);
                    drop(credit);
                    dtype
                }
            };
        }
    }
}

#[cfg(test)]
#[path = "native_input_dtype_tests.rs"]
mod tests;
