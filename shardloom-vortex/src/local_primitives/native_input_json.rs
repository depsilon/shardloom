//! Ordered typed JSON parsing and prepaid destruction of its private value tree.

use std::mem;

use serde::{Deserialize as _, de::Error as _};
use serde_json::{Map, Value, value::RawValue};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;

use super::super::native_capacity::ReservedVec;
use super::{failed, number};

const MAX_DEPTH: usize = 96;

enum Children {
    Array(std::vec::IntoIter<Value>),
    Object(serde_json::map::IntoIter),
}

impl Children {
    fn next(&mut self) -> Option<Value> {
        match self {
            Self::Array(values) => values.next(),
            Self::Object(values) => values.next().map(|(_, value)| value),
        }
    }
}

/// Every constructed container prepaid a cleanup slot before it was entered.
/// Consume one child at a time, so a wide container does not enlarge this stack.
fn drain(mut value: Value, pending: &mut ReservedVec<Children>) {
    debug_assert!(pending.values.is_empty());
    loop {
        let children = match value {
            Value::Array(values) => Some(Children::Array(values.into_iter())),
            Value::Object(values) => Some(Children::Object(values.into_iter())),
            _ => None,
        };
        if let Some(mut children) = children
            && let Some(child) = children.next()
        {
            debug_assert!(pending.values.len() < pending.values.capacity());
            pending.values.push(children);
            value = child;
            continue;
        }
        loop {
            let Some(children) = pending.values.last_mut() else {
                return;
            };
            if let Some(child) = children.next() {
                value = child;
                break;
            }
            pending.values.pop();
        }
    }
}

/// No owned Value escapes this private intake boundary without its cleanup owner.
pub(in crate::local_primitives) struct ParsedJson {
    value: Value,
    cleanup: ReservedVec<Children>,
}

impl ParsedJson {
    pub(in crate::local_primitives) fn null(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            value: Value::Null,
            cleanup: ReservedVec::new(memory)?,
        })
    }

    pub(in crate::local_primitives) fn value(&self) -> &Value {
        &self.value
    }
}

impl Drop for ParsedJson {
    fn drop(&mut self) {
        drain(
            mem::replace(&mut self.value, Value::Null),
            &mut self.cleanup,
        );
    }
}

struct Frame<'a> {
    raw: &'a str,
    cursor: usize,
    partial: Value,
    key: Option<String>,
    first: bool,
}

impl<'a> Frame<'a> {
    fn new(raw: &'a str) -> Self {
        Self {
            raw,
            cursor: 1,
            partial: if raw.starts_with('[') {
                Value::Array(Vec::new())
            } else {
                Value::Object(Map::new())
            },
            key: None,
            first: true,
        }
    }

    fn array(&self) -> bool {
        matches!(self.partial, Value::Array(_))
    }

    fn attach(&mut self, value: Value) {
        match &mut self.partial {
            Value::Array(values) => values.push(value),
            Value::Object(values) => {
                values.insert(self.key.take().expect("pending JSON object key"), value);
            }
            _ => unreachable!("a JSON continuation owns a container"),
        }
    }

    fn next(&mut self, depth: usize, max_depth: usize) -> serde_json::Result<Option<&'a str>> {
        skip_space(self.raw, &mut self.cursor);
        if matches!(self.raw.as_bytes()[self.cursor], b']' | b'}') {
            return Ok(None);
        }
        // Only RawValue-validated containers reach this cursor. Syntax remains
        // the provider's contract; these delimiters do not admit new grammar.
        if self.first {
            self.first = false;
        } else {
            debug_assert_eq!(self.raw.as_bytes()[self.cursor], b',');
            self.cursor += 1;
            skip_space(self.raw, &mut self.cursor);
        }
        if let Value::Object(values) = &self.partial {
            let start = self.cursor;
            let mut parser = serde_json::Deserializer::from_str(&self.raw[start..]);
            let key = String::deserialize(&mut parser)
                .map_err(|error| rebase(&error, self.raw, start))?;
            self.cursor += parser.into_iter::<serde::de::IgnoredAny>().byte_offset();
            if values.contains_key(&key) {
                return Err(serde_json::Error::custom(
                    "duplicate typed input JSON field",
                ));
            }
            self.key = Some(key);
            skip_space(self.raw, &mut self.cursor);
            debug_assert_eq!(self.raw.as_bytes()[self.cursor], b':');
            self.cursor += 1;
        }
        // Match JsonSeed's check before its RawValue read. For object values
        // the cursor still precedes any whitespace after the colon.
        if depth > max_depth {
            return Err(serde_json::Error::custom(format!(
                "typed input JSON nesting exceeds {max_depth} levels"
            )));
        }
        let mut parser = serde_json::Deserializer::from_str(&self.raw[self.cursor..]);
        let raw = <&RawValue>::deserialize(&mut parser)
            .map_err(|error| rebase(&error, self.raw, self.cursor))?;
        self.cursor += parser.into_iter::<serde::de::IgnoredAny>().byte_offset();
        Ok(Some(raw.get()))
    }

    /// `deserialize_any` runs `end_seq`/`end_map` even when its visitor failed, then
    /// fixes an error without a position. Preserve that cursor behavior exactly.
    fn error_position(&self) -> (usize, usize) {
        let mut cursor = self.cursor;
        skip_space(self.raw, &mut cursor);
        match self.raw.as_bytes().get(cursor) {
            Some(b']') if self.array() => cursor += 1,
            Some(b'}') if !self.array() => cursor += 1,
            Some(b',') if self.array() => {
                cursor += 1;
                skip_space(self.raw, &mut cursor);
            }
            _ => {}
        }
        position(self.raw, cursor)
    }
}

fn skip_space(raw: &str, cursor: &mut usize) {
    while matches!(
        raw.as_bytes().get(*cursor),
        Some(b' ' | b'\n' | b'\r' | b'\t')
    ) {
        *cursor += 1;
    }
}

fn position(raw: &str, cursor: usize) -> (usize, usize) {
    let prefix = &raw.as_bytes()[..cursor];
    let line = 1 + memchr::memchr_iter(b'\n', prefix).count();
    let start = memchr::memrchr(b'\n', prefix).map_or(0, |index| index + 1);
    (line, cursor - start)
}

fn rebase(error: &serde_json::Error, raw: &str, start: usize) -> serde_json::Error {
    let (line, column) = position(raw, start);
    let suffix = format!(" at line {} column {}", error.line(), error.column());
    let message = error.to_string();
    let message = message
        .strip_suffix(&suffix)
        .expect("positioned JSON token error");
    serde_json::Error::custom(format!(
        "{message} at line {} column {}",
        line + error.line() - 1,
        if error.line() == 1 {
            column + error.column()
        } else {
            error.column()
        },
    ))
}

struct Building<'a> {
    current: Option<Value>,
    frames: ReservedVec<Frame<'a>>,
    cleanup: Option<ReservedVec<Children>>,
}

impl Building<'_> {
    fn error(&self, error: &serde_json::Error) -> ShardLoomError {
        if error.line() == 0
            && let Some(frame) = self.frames.values.last()
        {
            let (line, column) = frame.error_position();
            return failed(&format!("{error} at line {line} column {column}"));
        }
        failed(&error.to_string())
    }
}

impl Drop for Building<'_> {
    fn drop(&mut self) {
        if let Some(cleanup) = &mut self.cleanup {
            if let Some(value) = self.current.take() {
                drain(value, cleanup);
            }
            while let Some(frame) = self.frames.values.pop() {
                drain(frame.partial, cleanup);
            }
        }
    }
}

pub(in crate::local_primitives) fn parse(raw: &str, memory: &LiveMemoryPool) -> Result<ParsedJson> {
    parse_with_depth(raw, memory, MAX_DEPTH)
}

pub(super) fn parse_with_depth(
    raw: &str,
    memory: &LiveMemoryPool,
    max_depth: usize,
) -> Result<ParsedJson> {
    let mut parser = serde_json::Deserializer::from_str(raw);
    let root = <&RawValue>::deserialize(&mut parser).map_err(|error| failed(&error.to_string()))?;
    let mut building = Building {
        current: None,
        frames: ReservedVec::new(memory)?,
        cleanup: Some(ReservedVec::new(memory)?),
    };
    let mut next = Some(root.get());
    loop {
        if let Some(raw) = next.take() {
            if matches!(raw.as_bytes().first(), Some(b'[' | b'{')) {
                building
                    .cleanup
                    .as_mut()
                    .expect("building cleanup")
                    .reserve(building.frames.values.len() + 1)?;
                building.frames.reserve_one()?;
                building.frames.values.push(Frame::new(raw));
            } else {
                let value = if matches!(raw.as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
                    number(raw).map_err(serde_json::Error::custom)
                } else {
                    serde_json::from_str(raw)
                };
                building.current = Some(value.map_err(|error| building.error(&error))?);
            }
        }
        if let Some(value) = building.current.take() {
            if let Some(frame) = building.frames.values.last_mut() {
                frame.attach(value);
            } else {
                building.current = Some(value);
                break;
            }
        }
        let depth = building.frames.values.len();
        let child = building
            .frames
            .values
            .last_mut()
            .expect("pending JSON container")
            .next(depth, max_depth);
        match child.map_err(|error| building.error(&error))? {
            Some(raw) => next = Some(raw),
            None => {
                building.current = Some(
                    building
                        .frames
                        .values
                        .pop()
                        .expect("completed JSON container")
                        .partial,
                );
            }
        }
    }
    parser.end().map_err(|error| failed(&error.to_string()))?;
    Ok(ParsedJson {
        value: building.current.take().expect("complete JSON root"),
        cleanup: building
            .cleanup
            .take()
            .expect("complete JSON cleanup owner"),
    })
}

#[cfg(test)]
#[path = "native_input_json_tests.rs"]
mod tests;
