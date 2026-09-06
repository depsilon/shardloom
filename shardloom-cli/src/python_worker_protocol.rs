//! Bounded newline framing before JSON parsing or argument allocation.

use std::io::{self, BufRead};

use serde::{
    Deserialize, Deserializer,
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
};

/// Maximum UTF-8 request bytes before the newline delimiter. Larger frames end
/// the worker after one deterministic error, without draining an unbounded input.
pub(crate) const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_ARGUMENTS: usize = 4096;

/// Decode only the admitted argument vector. Unknown metadata is skipped without
/// constructing a JSON value tree, preserving the previous protocol behavior.
pub(crate) fn parse_request_args(input: &str) -> Result<Vec<String>, serde_json::Error> {
    let mut parser = serde_json::Deserializer::from_str(input);
    let request = WorkerRequest::deserialize(&mut parser)?;
    parser.end()?;
    Ok(request.0)
}

struct WorkerRequest(Vec<String>);
impl<'de> Deserialize<'de> for WorkerRequest {
    fn deserialize<D: Deserializer<'de>>(parser: D) -> Result<Self, D::Error> {
        parser.deserialize_map(RequestVisitor)
    }
}

struct RequestVisitor;
impl<'de> Visitor<'de> for RequestVisitor {
    type Value = WorkerRequest;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a python worker request object")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut args = None;
        while let Some(field) = map.next_key::<RequestField>()? {
            if matches!(field, RequestField::Args) {
                if args.is_some() {
                    return Err(de::Error::custom("duplicate python worker request args"));
                }
                args = Some(map.next_value::<Arguments>()?.0);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        args.map(WorkerRequest)
            .ok_or_else(|| de::Error::custom("python worker request missing args"))
    }
}

enum RequestField {
    Args,
    Other,
}
impl<'de> Deserialize<'de> for RequestField {
    fn deserialize<D: Deserializer<'de>>(parser: D) -> Result<Self, D::Error> {
        struct FieldVisitor;
        impl Visitor<'_> for FieldVisitor {
            type Value = RequestField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a request field name")
            }

            fn visit_str<E: de::Error>(self, field: &str) -> Result<Self::Value, E> {
                Ok(if field == "args" {
                    RequestField::Args
                } else {
                    RequestField::Other
                })
            }
        }
        parser.deserialize_identifier(FieldVisitor)
    }
}

struct Arguments(Vec<String>);
impl<'de> Deserialize<'de> for Arguments {
    fn deserialize<D: Deserializer<'de>>(parser: D) -> Result<Self, D::Error> {
        struct ArgumentsVisitor;
        impl<'de> Visitor<'de> for ArgumentsVisitor {
            type Value = Arguments;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a python worker request args array containing only strings")
            }

            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                let mut args = Vec::new();
                loop {
                    // Check before parsing another owned string or growing the vector.
                    if args.len() == MAX_ARGUMENTS {
                        if sequence.next_element::<IgnoredAny>()?.is_some() {
                            return Err(de::Error::custom(format!(
                                "python worker request exceeds {MAX_ARGUMENTS} argument limit"
                            )));
                        }
                        break;
                    }
                    let Some(argument) = sequence.next_element::<String>()? else {
                        break;
                    };
                    args.try_reserve(1).map_err(de::Error::custom)?;
                    args.push(argument);
                }
                Ok(Arguments(args))
            }
        }
        parser.deserialize_seq(ArgumentsVisitor)
    }
}

pub(crate) fn read_bounded_frame(
    input: &mut impl BufRead,
    max_bytes: usize,
) -> io::Result<Option<String>> {
    let mut bytes = Vec::new();
    loop {
        let available = input.fill_buf()?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if count > max_bytes.saturating_sub(bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("python worker request exceeds {max_bytes} byte frame limit"),
            ));
        }
        let required = bytes.len() + count;
        if required > bytes.capacity() {
            let target = required
                .max(bytes.capacity().saturating_mul(2))
                .min(max_bytes);
            bytes
                .try_reserve_exact(target - bytes.len())
                .map_err(|error| {
                    io::Error::other(format!(
                        "cannot reserve bounded python worker frame: {error}"
                    ))
                })?;
        }
        bytes.extend_from_slice(&available[..count]);
        input.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            break;
        }
    }
    String::from_utf8(bytes).map(Some).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "python worker request is not valid UTF-8",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn bounded_arguments_accept_limit_and_reject_growth_duplicates_and_wrong_types() {
        let exact = format!(
            "{{\"args\":[{}]}}",
            vec!["\"status\""; MAX_ARGUMENTS].join(",")
        );
        assert_eq!(parse_request_args(&exact).unwrap().len(), MAX_ARGUMENTS);
        let excess = format!(
            "{{\"args\":[{}]}}",
            vec!["\"status\""; MAX_ARGUMENTS + 1].join(",")
        );
        assert!(
            parse_request_args(&excess)
                .unwrap_err()
                .to_string()
                .contains("4096 argument limit")
        );
        for invalid in [
            r#"{"args":["status"],"args":["--version"]}"#,
            r#"{"args":null}"#,
            r#"{"args":{}}"#,
            r#"{"args":["status",1]}"#,
            r#"{"args":["status","--format",true]}"#,
            r#"{"other":[]}"#,
            r#"{"args":["status"]} []"#,
        ] {
            assert!(parse_request_args(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn unknown_nested_metadata_is_skipped_and_escaped_args_key_is_preserved() {
        let payload = format!(
            r#"{{"metadata":{{"rows":[{}]}},"ar\u0067s":["status","λ\n"],"tail":{{"nested":[true,null,{{"x":1}}]}}}}"#,
            vec![r#"{"a":[1,2,3],"s":"ignored"}"#; 8192].join(",")
        );
        assert_eq!(parse_request_args(&payload).unwrap(), vec!["status", "λ\n"]);
        // Ignoring a value still validates its JSON syntax.
        assert!(parse_request_args(r#"{"args":["status"],"ignored":[1,,2]}"#).is_err());
    }

    #[test]
    fn preserves_blank_crlf_and_final_frame_without_newline() {
        let mut input = BufReader::with_capacity(3, Cursor::new(b"\n{}\r\n{\"x\":1}"));
        assert_eq!(
            read_bounded_frame(&mut input, 8).unwrap(),
            Some(String::new())
        );
        assert_eq!(
            read_bounded_frame(&mut input, 8).unwrap().as_deref(),
            Some("{}\r")
        );
        assert_eq!(
            read_bounded_frame(&mut input, 8).unwrap().as_deref(),
            Some("{\"x\":1}")
        );
        assert!(read_bounded_frame(&mut input, 8).unwrap().is_none());
    }

    #[test]
    fn accepts_exact_byte_limit_and_rejects_oversized_frame_without_draining() {
        let mut exact = BufReader::with_capacity(3, Cursor::new(b"12345678\nnext\n"));
        assert_eq!(
            read_bounded_frame(&mut exact, 8).unwrap().as_deref(),
            Some("12345678")
        );
        assert_eq!(
            read_bounded_frame(&mut exact, 8).unwrap().as_deref(),
            Some("next")
        );
        let mut oversized = BufReader::with_capacity(3, Cursor::new(b"123456789_and_more\nnext\n"));
        let error = read_bounded_frame(&mut oversized, 8).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "python worker request exceeds 8 byte frame limit"
        );
        assert_eq!(oversized.buffer(), b"789");
        assert!(oversized.get_ref().position() < 18);
    }

    #[test]
    fn truncated_json_reaches_parser_but_truncated_utf8_is_rejected() {
        let mut truncated = Cursor::new(b"{\"args\":[");
        let line = read_bounded_frame(&mut truncated, 64).unwrap().unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&line).is_err());
        assert!(read_bounded_frame(&mut truncated, 64).unwrap().is_none());
        let mut utf8 = Cursor::new([0xe2, 0x82]);
        let error = read_bounded_frame(&mut utf8, 64).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "python worker request is not valid UTF-8"
        );
    }
}
