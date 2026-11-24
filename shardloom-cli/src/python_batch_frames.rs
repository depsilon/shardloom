//! Frame bytes, JSON workspace and owned cells share the native execution grant.

use super::{Incoming, MAX_FRAME, Result, failed};
use crate::native_memory_input::MemoryRow;
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use shardloom_core::ShardLoomError;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, MemoryLease},
};
use std::{
    cell::RefCell,
    fmt,
    io::BufRead,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Default)]
pub(super) struct ScratchStats {
    transitions: Mutex<()>,
    live: AtomicU64,
    peak: AtomicU64,
}

impl ScratchStats {
    pub(super) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }
}

/// An observer of actual leases, not an additional memory pool.
pub(super) struct Scratch {
    lease: MemoryLease,
    stats: Arc<ScratchStats>,
}

impl Scratch {
    pub(super) fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    pub(super) fn new(lease: MemoryLease, stats: Arc<ScratchStats>) -> Self {
        {
            let _transition = stats
                .transitions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let live = stats.live.fetch_add(lease.bytes(), Ordering::AcqRel) + lease.bytes();
            stats.peak.fetch_max(live, Ordering::AcqRel);
        }
        Self { lease, stats }
    }

    pub(super) fn branch(&mut self) -> Result<Self> {
        Ok(Self::new(self.lease.split(0)?, Arc::clone(&self.stats)))
    }

    pub(super) fn split_budgeted_lease(&mut self, bytes: u64) -> Result<MemoryLease> {
        let _transition = self
            .stats
            .transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let child = self.lease.split(bytes)?;
        self.stats.live.fetch_sub(bytes, Ordering::AcqRel);
        Ok(child)
    }

    pub(super) fn resize(&mut self, bytes: u64) -> Result<()> {
        // Serialize the observed leases' pool transitions with their counters.
        // Otherwise a reader release can overlap a decoder acquisition between
        // the pool update and the observation, falsely inflating the peak.
        let _transition = self
            .stats
            .transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = self.lease.bytes();
        self.lease.resize(bytes)?;
        if bytes >= before {
            let growth = bytes - before;
            let live = self.stats.live.fetch_add(growth, Ordering::AcqRel) + growth;
            self.stats.peak.fetch_max(live, Ordering::AcqRel);
        } else {
            self.stats.live.fetch_sub(before - bytes, Ordering::AcqRel);
        }
        Ok(())
    }

    fn add(&mut self, bytes: usize) -> Result<()> {
        self.resize(
            self.lease
                .bytes()
                .checked_add(u64::try_from(bytes).map_err(failed)?)
                .ok_or_else(|| failed("input scratch size overflow"))?,
        )
    }

    fn remove(&mut self, bytes: usize) -> Result<()> {
        self.resize(
            self.lease
                .bytes()
                .checked_sub(u64::try_from(bytes).map_err(failed)?)
                .ok_or_else(|| failed("input scratch ownership underflow"))?,
        )
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        self.resize(0).expect("returning memory credit cannot fail");
    }
}

/// Field order keeps every owned byte alive only while its credit is retained.
pub(super) struct Frame {
    text: String,
    payload: Option<Vec<u8>>,
    scratch: Scratch,
}

pub(super) struct Decoded {
    pub(super) value: Incoming,
    scratch: Scratch,
}

impl Decoded {
    pub(super) fn conversion_scratch(&mut self, bytes: u64) -> Result<Scratch> {
        let mut scratch = self.scratch.branch()?;
        scratch.resize(bytes)?;
        Ok(scratch)
    }
}

/// Reserve replacement overlap before asking Vec to allocate. Old capacity is
/// released from the grant only after the replacement allocation succeeds.
fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    limit: usize,
    scratch: &mut Scratch,
) -> Result<()> {
    let needed = values
        .len()
        .checked_add(additional)
        .filter(|needed| *needed <= limit)
        .ok_or_else(|| failed("input vector size overflow"))?;
    if needed <= values.capacity() {
        return Ok(());
    }
    let target = values
        .capacity()
        .max(4)
        .checked_mul(2)
        .ok_or_else(|| failed("input vector capacity overflow"))?
        .max(needed)
        .min(limit);
    let bytes = target
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| failed("input vector byte count overflow"))?;
    let old_bytes = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| failed("input vector byte count overflow"))?;
    scratch.add(bytes)?;
    if let Err(error) = values.try_reserve_exact(target - values.len()) {
        scratch.remove(bytes)?;
        return Err(failed(error));
    }
    if values.capacity() > target {
        return Err(failed("input allocator exceeded admitted capacity"));
    }
    scratch.remove(old_bytes)
}

#[derive(Deserialize)]
struct BinaryHeader {
    kind: Kind,
    byte_len: Option<usize>,
}

pub(super) fn read(
    input: &mut impl BufRead,
    owner: &mut Scratch,
    cancellation: &CancellationToken,
) -> Result<Option<Frame>> {
    let mut scratch = owner.branch()?;
    let mut bytes = Vec::new();
    loop {
        cancellation.check()?;
        let available = input.fill_buf().map_err(failed)?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if count > MAX_FRAME.saturating_sub(bytes.len()) {
            return Err(failed("input exceeds its 8 MiB frame bound"));
        }
        reserve(&mut bytes, count, MAX_FRAME, &mut scratch)?;
        bytes.extend_from_slice(&available[..count]);
        input.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            break;
        }
    }
    cancellation.check()?;
    let text = String::from_utf8(bytes).map_err(failed)?;
    let mut payload = None;
    if text.contains("\"rows_ipc\"")
        && let Ok(header) = serde_json::from_str::<BinaryHeader>(&text)
        && matches!(header.kind, Kind::RowsIpc)
    {
        let Some(byte_len) = header.byte_len.filter(|len| *len > 0 && *len <= MAX_FRAME) else {
            return Err(failed(
                "binary input frame requires a 1..=8 MiB byte_len header",
            ));
        };
        let mut raw = Vec::new();
        while raw.len() < byte_len {
            cancellation.check()?;
            let available = input.fill_buf().map_err(failed)?;
            if available.is_empty() {
                return Err(failed(
                    "binary batch payload ended before declared byte_len",
                ));
            }
            let take = available.len().min(byte_len - raw.len());
            reserve(&mut raw, take, byte_len, &mut scratch)?;
            raw.extend_from_slice(&available[..take]);
            input.consume(take);
        }
        payload = Some(raw);
    }
    Ok(Some(Frame {
        text,
        payload,
        scratch,
    }))
}

#[derive(Clone, Copy)]
enum Kind {
    Rows,
    RowsIpc,
    End,
    Ack,
    Cancel,
}

impl<'de> Deserialize<'de> for Kind {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct KindVisitor;
        impl Visitor<'_> for KindVisitor {
            type Value = Kind;
            fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
                out.write_str("a known native batch message kind")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Kind, E> {
                match value {
                    "rows" => Ok(Kind::Rows),
                    "rows_ipc" => Ok(Kind::RowsIpc),
                    "end" => Ok(Kind::End),
                    "ack" => Ok(Kind::Ack),
                    "cancel" => Ok(Kind::Cancel),
                    _ => Err(E::custom("unknown native batch message kind")),
                }
            }
        }
        deserializer.deserialize_str(KindVisitor)
    }
}

struct Index(u64);
impl<'de> Deserialize<'de> for Index {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct IndexVisitor;
        impl Visitor<'_> for IndexVisitor {
            type Value = Index;
            fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
                out.write_str("an unsigned 64-bit batch index")
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Index, E> {
                Ok(Index(value))
            }
            fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Index, E> {
                Err(E::custom("batch index must be an unsigned 64-bit integer"))
            }
        }
        deserializer.deserialize_any(IndexVisitor)
    }
}

impl Frame {
    fn parser_scratch(&mut self) -> Result<Scratch> {
        let mut scratch = self.scratch.branch()?;
        // serde_json's escape buffer grows geometrically. Decoded UTF8 cannot
        // exceed the complete raw frame; twice that length covers its capacity.
        scratch.resize(
            u64::try_from(self.text.len())
                .map_err(failed)?
                .checked_mul(2)
                .ok_or_else(|| failed("JSON workspace size overflow"))?
                .max(8),
        )?;
        Ok(scratch)
    }

    pub(super) fn is_cancel(&mut self) -> Result<bool> {
        #[derive(Deserialize)]
        struct Header {
            kind: Kind,
        }
        if !self.text.trim_start().starts_with('{') {
            return Ok(false);
        }
        let _scratch = self.parser_scratch()?;
        Ok(serde_json::from_str::<Header>(&self.text)
            .is_ok_and(|header| matches!(header.kind, Kind::Cancel)))
    }

    pub(super) fn decode(mut self, width: Option<usize>) -> Result<Decoded> {
        let _parser = self.parser_scratch()?;
        let payload = self
            .payload
            .take()
            .map(|bytes| {
                let cap = u64::try_from(bytes.capacity()).map_err(failed)?;
                let lease = self.scratch.split_budgeted_lease(cap)?;
                Ok::<Budgeted<Vec<u8>>, ShardLoomError>(Budgeted::new(bytes, lease))
            })
            .transpose()?;
        let budget = RefCell::new(self.scratch.branch()?);
        let mut deserializer = serde_json::Deserializer::from_str(&self.text);
        let value = MessageSeed {
            width,
            payload: RefCell::new(payload),
            budget: &budget,
        }
        .deserialize(&mut deserializer)
        .map_err(failed)?;
        deserializer.end().map_err(failed)?;
        Ok(Decoded {
            value,
            scratch: budget.into_inner(),
        })
    }
}

struct Reject(&'static str);
impl<'de> DeserializeSeed<'de> for Reject {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        _deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        Err(de::Error::custom(self.0))
    }
}

#[derive(Clone, Copy)]
struct StringSeed<'a>(&'a RefCell<Scratch>);
impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = String;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<String, D::Error> {
        deserializer.deserialize_str(self)
    }
}
impl Visitor<'_> for StringSeed<'_> {
    type Value = String;
    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("a string")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<String, E> {
        self.0.borrow_mut().add(value.len()).map_err(E::custom)?;
        let mut text = String::new();
        text.try_reserve_exact(value.len()).map_err(E::custom)?;
        if text.capacity() > value.len() {
            return Err(E::custom("string exceeded admitted capacity"));
        }
        text.push_str(value);
        Ok(text)
    }
}

#[derive(Clone, Copy)]
struct CellSeed<'a>(&'a RefCell<Scratch>);
impl<'de> DeserializeSeed<'de> for CellSeed<'_> {
    type Value = Option<String>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_option(self)
    }
}
impl<'de> Visitor<'de> for CellSeed<'_> {
    type Value = Option<String>;
    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("a string or null")
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_some<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        StringSeed(self.0).deserialize(deserializer).map(Some)
    }
}

#[derive(Clone, Copy)]
struct RowSeed<'a> {
    width: usize,
    budget: &'a RefCell<Scratch>,
}
impl<'de> DeserializeSeed<'de> for RowSeed<'_> {
    type Value = MemoryRow;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for RowSeed<'_> {
    type Value = MemoryRow;
    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("a row matching the declared schema")
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
        Err(E::custom("input row must be an array"))
    }
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        loop {
            if values.len() == self.width {
                sequence.next_element_seed(Reject("input row exceeds its declared width"))?;
                break;
            }
            let Some(value) = sequence.next_element_seed(CellSeed(self.budget))? else {
                break;
            };
            reserve(&mut values, 1, self.width, &mut self.budget.borrow_mut())
                .map_err(de::Error::custom)?;
            values.push(value);
        }
        if values.len() != self.width {
            return Err(de::Error::custom(
                "input row differs from its declared width",
            ));
        }
        Ok(MemoryRow(values))
    }
}

struct RowsSeed<'a> {
    width: usize,
    budget: &'a RefCell<Scratch>,
}
impl<'de> DeserializeSeed<'de> for RowsSeed<'_> {
    type Value = Vec<MemoryRow>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for RowsSeed<'_> {
    type Value = Vec<MemoryRow>;
    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("at most 2048 typed input rows")
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
        Err(E::custom("input rows must be an array"))
    }
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut rows = Vec::new();
        loop {
            if rows.len() == 2048 {
                sequence.next_element_seed(Reject("input frame exceeds 2048 rows"))?;
                break;
            }
            let Some(row) = sequence.next_element_seed(RowSeed {
                width: self.width,
                budget: self.budget,
            })?
            else {
                break;
            };
            reserve(&mut rows, 1, 2048, &mut self.budget.borrow_mut())
                .map_err(de::Error::custom)?;
            rows.push(row);
        }
        Ok(rows)
    }
}

struct MessageSeed<'a> {
    width: Option<usize>,
    payload: RefCell<Option<Budgeted<Vec<u8>>>>,
    budget: &'a RefCell<Scratch>,
}
impl<'de> DeserializeSeed<'de> for MessageSeed<'_> {
    type Value = Incoming;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Incoming, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for MessageSeed<'_> {
    type Value = Incoming;
    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("a native batch protocol object")
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
        Err(E::custom("batch message must be an object"))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Incoming, A::Error> {
        let (mut kind, mut uri, mut index, mut rows, mut byte_len) = (None, None, None, None, None);
        while let Some(field) = map.next_key::<IncomingField>()? {
            match field {
                IncomingField::Kind => {
                    if kind.is_some() {
                        return Err(de::Error::duplicate_field("kind"));
                    }
                    kind = Some(map.next_value::<Kind>()?);
                }
                IncomingField::Uri => {
                    if uri.is_some() {
                        return Err(de::Error::duplicate_field("uri"));
                    }
                    uri = Some(map.next_value_seed(StringSeed(self.budget))?);
                }
                IncomingField::Index => {
                    if index.is_some() {
                        return Err(de::Error::duplicate_field("index"));
                    }
                    index = Some(map.next_value::<Index>()?.0);
                }
                IncomingField::Rows => {
                    if rows.is_some() {
                        return Err(de::Error::duplicate_field("rows"));
                    }
                    let width = self
                        .width
                        .ok_or_else(|| de::Error::custom("rows arrived without input demand"))?;
                    rows = Some(map.next_value_seed(RowsSeed {
                        width,
                        budget: self.budget,
                    })?);
                }
                IncomingField::ByteLen => {
                    if byte_len.is_some() {
                        return Err(de::Error::duplicate_field("byte_len"));
                    }
                    let len = map.next_value::<Index>()?.0;
                    let len = usize::try_from(len)
                        .ok()
                        .filter(|value| (1..=MAX_FRAME).contains(value))
                        .ok_or_else(|| de::Error::custom("byte_len exceeds 1..=8 MiB bound"))?;
                    byte_len = Some(len);
                }
            }
        }
        let payload = self.payload.borrow_mut().take();
        match (kind, uri, index, rows, byte_len, payload) {
            (Some(Kind::Rows), Some(uri), Some(index), Some(rows), None, None) => {
                Ok(Incoming::Rows { uri, index, rows })
            }
            (Some(Kind::RowsIpc), Some(uri), Some(index), None, Some(byte_len), Some(payload))
                if payload.value().len() == byte_len =>
            {
                let _ = self
                    .width
                    .ok_or_else(|| de::Error::custom("rows_ipc arrived without input demand"))?;
                Ok(Incoming::RowsIpc {
                    uri,
                    index,
                    payload,
                })
            }
            (Some(Kind::End), Some(uri), Some(index), None, None, None) => {
                Ok(Incoming::End { uri, index })
            }
            (Some(Kind::Ack), None, Some(index), None, None, None) => Ok(Incoming::Ack { index }),
            (Some(Kind::Cancel), None, None, None, None, None) => Ok(Incoming::Cancel),
            _ => Err(de::Error::custom(
                "batch fields do not match the declared message kind",
            )),
        }
    }
}

enum IncomingField {
    Kind,
    Uri,
    Index,
    Rows,
    ByteLen,
}

impl<'de> Deserialize<'de> for IncomingField {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct FieldVisitor;
        impl Visitor<'_> for FieldVisitor {
            type Value = IncomingField;
            fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
                out.write_str("a known native batch field")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<IncomingField, E> {
                match value {
                    "kind" => Ok(IncomingField::Kind),
                    "uri" => Ok(IncomingField::Uri),
                    "index" => Ok(IncomingField::Index),
                    "rows" => Ok(IncomingField::Rows),
                    "byte_len" => Ok(IncomingField::ByteLen),
                    _ => Err(E::custom("unknown field in native batch message")),
                }
            }
        }
        deserializer.deserialize_identifier(FieldVisitor)
    }
}

#[cfg(test)]
#[path = "python_batch_frames_tests.rs"]
mod tests;
