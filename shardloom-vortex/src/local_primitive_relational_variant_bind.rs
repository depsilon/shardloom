//! Literal path and output-type admission for the pinned native Variant provider.

use super::{
    Binder, DType, DecimalDType, Expression, Function, Input, Kind, Nullability, PType, Result,
    ScalarDefinition, ScalarTypeRule, failed, literal_text,
};
use crate::local_primitives::native_relational_expression::variant::Kernel;
use vortex::array::{
    dtype::extension::ExtDType,
    scalar_fn::fns::variant_get::{VariantGetOptions, VariantPath, VariantPathElement},
};

pub(super) fn bind(
    binder: &mut Binder<'_>,
    definition: &ScalarDefinition,
    inputs: &[Input],
    fields: &[(String, DType)],
    depth: usize,
) -> Result<Expression> {
    binder.charge_fields(inputs.len())?;
    let mut args = inputs
        .iter()
        .map(|input| binder.expression(input, fields, depth + 1))
        .collect::<Result<Vec<_>>>()?;
    if !matches!(args[0].dtype, DType::Null | DType::Variant(_)) {
        return Err(failed("Variant functions require a native Variant operand"));
    }
    let (dtype, kernel) = if definition.type_rule == ScalarTypeRule::VariantIsNull {
        (DType::Bool(Nullability::NonNullable), Kernel::IsNull)
    } else {
        let text = literal_text(&args[1], "Variant path")?
            .ok_or_else(|| failed("Variant path must be a nonnull literal string"))?;
        binder.charge(
            text.len()
                .checked_mul(64)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| failed("Variant path metadata overflow"))?,
        )?;
        let path = path(text)?;
        let target = args
            .get(2)
            .map(|arg| {
                literal_text(arg, "Variant target type")?
                    .ok_or_else(|| failed("Variant target type must be a nonnull literal string"))
                    .and_then(target)
            })
            .transpose()?;
        let dtype = target
            .clone()
            .unwrap_or(DType::Variant(Nullability::Nullable));
        super::super::validate_payload(&dtype, binder.schema_scope())?;
        (dtype, Kernel::Get(VariantGetOptions::new(path, target)))
    };
    args.truncate(1);
    Ok(Expression::new(
        dtype,
        Kind::Function {
            function: Function::Variant(Box::new(kernel)),
            args,
        },
    ))
}

fn path(text: &str) -> Result<VariantPath> {
    if text.len() > 16_384 {
        return Err(failed("Variant path exceeds 16384 bytes"));
    }
    let mut tail = text
        .strip_prefix('$')
        .ok_or_else(|| failed("Variant path must begin with $"))?;
    let mut steps = Vec::new();
    while !tail.is_empty() {
        if steps.len() == 24 {
            return Err(failed("Variant path exceeds 24 steps"));
        }
        if let Some(field) = tail.strip_prefix('.') {
            let end = field.find(['.', '[']).unwrap_or(field.len());
            let name = &field[..end];
            if !name.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_')
                || !name
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return Err(failed(
                    "Variant dot paths require an identifier; use a JSON-quoted bracket field for other names",
                ));
            }
            steps.push(VariantPathElement::field(name));
            tail = &field[end..];
        } else if let Some(field) = tail.strip_prefix("[\"") {
            // Locate the unescaped quote, then let the JSON string parser own
            // Unicode/escape validation. Brackets inside the string are data.
            let mut escaped = false;
            let close = field
                .bytes()
                .position(|byte| {
                    if escaped {
                        escaped = false;
                        false
                    } else if byte == b'\\' {
                        escaped = true;
                        false
                    } else {
                        byte == b'"'
                    }
                })
                .ok_or_else(|| failed("Variant quoted field is unterminated"))?;
            let end = close + 3;
            let name: String =
                serde_json::from_str(&tail[1..end]).map_err(super::super::vortex_error)?;
            tail = tail[end..]
                .strip_prefix(']')
                .ok_or_else(|| failed("Variant quoted field requires a closing bracket"))?;
            steps.push(VariantPathElement::field(name));
        } else if let Some(index) = tail.strip_prefix('[') {
            let end = index
                .find(']')
                .ok_or_else(|| failed("Variant index requires a closing bracket"))?;
            let digits = &index[..end];
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(failed("Variant index must be a nonnegative integer"));
            }
            let index_value = digits
                .parse::<usize>()
                .map_err(|_| failed("Variant index exceeds native address capacity"))?;
            steps.push(VariantPathElement::index(index_value as u64));
            tail = &index[end + 1..];
        } else {
            return Err(failed(
                "Variant path admits only literal object fields and array indexes",
            ));
        }
    }
    Ok(VariantPath::new(steps))
}

fn target(text: &str) -> Result<DType> {
    let name = text.trim().to_ascii_lowercase();
    let n = Nullability::Nullable;
    let primitive = match name.as_str() {
        "int8" | "tinyint" => Some(PType::I8),
        "int16" | "smallint" => Some(PType::I16),
        "int32" | "int" | "integer" => Some(PType::I32),
        "int64" | "bigint" | "long" => Some(PType::I64),
        "uint8" => Some(PType::U8),
        "uint16" => Some(PType::U16),
        "uint32" => Some(PType::U32),
        "uint64" => Some(PType::U64),
        "float16" => Some(PType::F16),
        "float32" | "float" => Some(PType::F32),
        "float64" | "double" => Some(PType::F64),
        _ => None,
    };
    if let Some(primitive) = primitive {
        return Ok(DType::Primitive(primitive, n));
    }
    match name.as_str() {
        "boolean" | "bool" => return Ok(DType::Bool(n)),
        "utf8" | "string" | "text" => return Ok(DType::Utf8(n)),
        "binary" => return Ok(DType::Binary(n)),
        "variant" => return Ok(DType::Variant(n)),
        "uuid" => {
            return ExtDType::<vortex::encodings::uuid::Uuid>::try_new(
                vortex::encodings::uuid::UuidMetadata::default(),
                DType::FixedSizeList(
                    DType::Primitive(PType::U8, Nullability::NonNullable).into(),
                    16,
                    n,
                ),
            )
            .map(|dtype| DType::Extension(dtype.erased()))
            .map_err(super::super::vortex_error);
        }
        _ => {}
    }
    if let Some(arguments) = name
        .strip_prefix("decimal(")
        .and_then(|value| value.strip_suffix(')'))
    {
        let (precision, scale) = arguments
            .split_once(',')
            .ok_or_else(|| failed("Variant decimal target requires precision and scale"))?;
        let precision = precision
            .trim()
            .parse::<u8>()
            .map_err(super::super::vortex_error)?;
        let scale = scale
            .trim()
            .parse::<u8>()
            .map_err(super::super::vortex_error)?;
        shardloom_core::expression::Decimal128Operand::decimal(0, precision, scale)
            .map_err(|error| failed(&error.to_string()))?;
        return Ok(DType::Decimal(
            DecimalDType::new(
                precision,
                i8::try_from(scale).map_err(super::super::vortex_error)?,
            ),
            n,
        ));
    }
    let temporal = match name.as_str() {
        "date" => shardloom_core::temporal::TemporalDType::parse("date32"),
        "timestamp" => shardloom_core::temporal::TemporalDType::parse("timestamp(us)"),
        _ => shardloom_core::temporal::TemporalDType::parse_declaration(text),
    };
    if let Some(temporal) = temporal {
        if matches!(
            temporal.kind(),
            shardloom_core::temporal::TemporalKind::Duration(_)
        ) {
            return Err(failed(
                "Variant duration extraction has no admitted provider conversion",
            ));
        }
        return crate::native_temporal::vortex_dtype(&temporal, n);
    }
    Err(failed(
        "Variant target requires an admitted primitive, DECIMAL(p,s), temporal, UUID or Variant literal type",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_path_literals_preserve_names_and_refuse_unsupported_steps() {
        let parsed = path("$[\"a.b[猫]\\\"\"].items[12]").unwrap();
        assert_eq!(
            parsed.elements(),
            &[
                VariantPathElement::field("a.b[猫]\""),
                VariantPathElement::field("items"),
                VariantPathElement::index(12),
            ]
        );
        assert!(path("$").unwrap().is_root());
        assert_eq!(
            path("$[\"\"]").unwrap().elements(),
            &[VariantPathElement::field("")]
        );
        assert!(path(&format!("${}", ".a".repeat(24))).is_ok());
        for invalid in [
            "",
            "a",
            "$.a*",
            "$..a",
            "$[-1]",
            "$[*]",
            "$[1",
            "$[]",
            "$[\"x\"",
            "$[\"x\\q\"]",
            "$[18446744073709551616]",
        ] {
            assert!(path(invalid).is_err(), "{invalid}");
        }
        assert!(path(&format!("${}", ".a".repeat(25))).is_err());
        assert!(path(&format!("$[\"{}\"]", "x".repeat(16_384))).is_err());
    }
}
