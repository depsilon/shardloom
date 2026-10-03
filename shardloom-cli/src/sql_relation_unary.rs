//! Inert table-expression spelling for the shared native unary request types.

use super::*;
use crate::vortex_primitive_execution::{
    parse_explode_primitive_request, parse_expression_project_primitive_request,
    parse_melt_primitive_request, parse_projection_columns, parse_rolling_primitive_request,
};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexDuplicateKeepPolicy, VortexQueryPrimitiveKind as Kind,
    VortexQueryPrimitiveRequest as Request,
};

pub(super) fn parse(raw: &str) -> Result<Option<ParsedRelationUnary>, ShardLoomError> {
    let Some(open) = raw.find('(') else {
        return Ok(None);
    };
    let name = raw[..open].trim();
    if validate_sql_identifier(name).is_err() {
        return Ok(None);
    }
    let kind = match name.to_ascii_uppercase().as_str() {
        "DISTINCT_ROWS" => Kind::DistinctRows,
        "DROP_DUPLICATES" => Kind::DropDuplicateRows,
        "DUPLICATED" => Kind::DuplicateMaskRows,
        "TAIL" => Kind::TailRows,
        "SAMPLE" => Kind::SampleRows,
        "REWRITE" => Kind::ExpressionProjectRows,
        "MELT" => Kind::MeltRows,
        "ROLLING" => Kind::RollingWindowRows,
        "EXPLODE" => Kind::ExplodeRows,
        _ => {
            return Err(unsupported_sql_error(
                "unknown native relation table expression",
            ));
        }
    };
    let close = matching_closing_parenthesis(raw, open)?
        .ok_or_else(|| unsupported_sql_error("table expression parentheses must be balanced"))?;
    if close + 1 != raw.len() || raw[open + 1..close].trim_end().ends_with(',') {
        return Err(unsupported_sql_error(
            "unexpected text or trailing comma in table expression",
        ));
    }
    let args = split_sql_csv(&raw[open + 1..close])?;
    let required = if matches!(kind, Kind::DropDuplicateRows | Kind::DuplicateMaskRows) {
        3
    } else {
        2
    };
    if args.len() != required {
        return Err(unsupported_sql_error(
            "table expression has an invalid argument count",
        ));
    }
    let input = &args[0];
    if !input.starts_with('(') || matching_closing_parenthesis(input, 0)? != Some(input.len() - 1) {
        return Err(unsupported_sql_error(
            "table expression requires a parenthesized input query",
        ));
    }
    let request = operation(kind, &args[1..])?;
    let input = ParsedRelationQuery::parse(&input[1..input.len() - 1])?;
    Ok(Some(ParsedRelationUnary {
        input: Arc::new(input),
        request,
    }))
}

fn operation(kind: Kind, args: &[String]) -> Result<Request, ShardLoomError> {
    let mut request = Request::for_relational_input(kind, ProjectionRequest::All);
    if kind == Kind::TailRows {
        request.source_order_limit = Some(
            args[0]
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| {
                    unsupported_sql_error("TAIL requires a positive integer row count")
                })?,
        );
        return Ok(request);
    }
    let value = parse_sql_string_literal(&args[0])?;
    if value.len() > 65_536 {
        return Err(unsupported_sql_error(
            "unary operation arguments exceed 64 KiB",
        ));
    }
    match kind {
        Kind::DistinctRows => request.projection = parse_projection_columns(&value)?,
        Kind::DropDuplicateRows | Kind::DuplicateMaskRows => {
            let keys = parse_projection_columns(&value)?;
            if kind == Kind::DropDuplicateRows {
                request.deduplicate_key_projection = Some(keys);
            } else {
                request.projection = keys;
            }
            request.duplicate_keep = match parse_sql_string_literal(&args[1])?
                .to_ascii_lowercase()
                .as_str()
            {
                "first" => VortexDuplicateKeepPolicy::First,
                "last" => VortexDuplicateKeepPolicy::Last,
                "false" => VortexDuplicateKeepPolicy::AllDuplicates,
                _ => {
                    return Err(unsupported_sql_error(
                        "duplicate keep must be first, last or false",
                    ));
                }
            };
        }
        Kind::SampleRows => return sample(&value),
        Kind::ExpressionProjectRows => {
            validate_fields(&value, &[&["columns"], &["rewrites"]])?;
            return parse_expression_project_primitive_request(None, &value);
        }
        Kind::MeltRows => {
            validate_fields(
                &value,
                &[
                    &["id_columns", "id_vars"],
                    &["value_columns", "value_vars"],
                    &["variable_column", "var_name"],
                    &["value_column", "value_name"],
                ],
            )?;
            return parse_melt_primitive_request(None, &value);
        }
        Kind::RollingWindowRows => {
            validate_fields(
                &value,
                &[
                    &["source_column", "column", "on"],
                    &["output_column", "alias"],
                    &["window_size", "window"],
                    &["min_periods"],
                    &["aggregate", "agg"],
                    &["center"],
                ],
            )?;
            return parse_rolling_primitive_request(None, &value);
        }
        Kind::ExplodeRows => {
            validate_fields(
                &value,
                &[
                    &["column", "explode_column", "target_column"],
                    &["columns"],
                    &["explode_columns", "target_columns"],
                    &["output_columns", "projected_columns"],
                    &["element_field", "field", "field_path"],
                    &["element_output_column", "output_column"],
                ],
            )?;
            return parse_explode_primitive_request(None, &value);
        }
        _ => return Err(unsupported_sql_error("unsupported unary table expression")),
    }
    Ok(request)
}

fn validate_fields(payload: &str, groups: &[&[&str]]) -> Result<(), ShardLoomError> {
    use serde::Deserializer;
    struct Fields;
    impl<'de> serde::de::Visitor<'de> for Fields {
        type Value = serde_json::Map<String, serde_json::Value>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a unary options object with unique fields")
        }

        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            let mut fields = serde_json::Map::new();
            while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                if fields.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate unary option field"));
                }
            }
            Ok(fields)
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(payload);
    let object = decoder.deserialize_map(Fields).map_err(|error| {
        unsupported_sql_error(&format!("unary payload must be valid JSON: {error}"))
    })?;
    decoder
        .end()
        .map_err(|error| unsupported_sql_error(&format!("invalid unary JSON: {error}")))?;
    if object
        .keys()
        .any(|key| !groups.iter().any(|group| group.contains(&key.as_str())))
        || groups.iter().any(|group| {
            group
                .iter()
                .filter(|key| object.contains_key(**key))
                .count()
                > 1
        })
    {
        return Err(unsupported_sql_error(
            "unary payload has an unknown or conflicting field",
        ));
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    n: Option<usize>,
    fraction: Option<f64>,
    #[serde(default)]
    seed: u64,
    #[serde(default)]
    replace: bool,
    weights: Option<String>,
    columns: Option<String>,
}

fn sample(payload: &str) -> Result<Request, ShardLoomError> {
    let sample: Sample = serde_json::from_str(payload)
        .map_err(|error| unsupported_sql_error(&format!("SAMPLE options are invalid: {error}")))?;
    if sample.n.is_some() == sample.fraction.is_some()
        || sample.n == Some(0)
        || sample
            .fraction
            .is_some_and(|f| !f.is_finite() || f <= 0.0 || f > 1.0)
    {
        return Err(unsupported_sql_error(
            "SAMPLE requires exactly one positive n or fraction in (0, 1]",
        ));
    }
    Ok(Request {
        source_order_limit: sample.n,
        sample_fraction: sample.fraction,
        sample_seed: Some(sample.seed),
        sample_with_replacement: sample.replace,
        sample_weight_column: sample
            .weights
            .map(shardloom_core::ColumnRef::new)
            .transpose()?,
        ..Request::for_relational_input(
            Kind::SampleRows,
            parse_projection_columns(sample.columns.as_deref().unwrap_or("*"))?,
        )
    })
}
