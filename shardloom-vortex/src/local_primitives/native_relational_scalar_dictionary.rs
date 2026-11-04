//! Pure unary text results are computed at the first referenced dictionary code.
//! Argument/domain preparation and row-ordered code validation remain unchanged.

use super::{
    ArrayRef, Expression, Function, NativeExecutionContext, Result, Value, growth, output,
    record_row, vortex_error,
};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec, native_relational_batch::failed,
        native_relational_keys::VariableColumn,
    },
    native_payload_work::Counter,
};
use shardloom_exec::live_memory::MemoryLease;
use std::{borrow::Cow, cell::OnceCell};

enum Cached<'a> {
    Text(String),
    Trimmed(&'a str),
    Length(i64),
}

impl Cached<'_> {
    fn value(&self) -> Value<'_> {
        match self {
            Self::Text(text) => Value::Text(Cow::Borrowed(text)),
            Self::Trimmed(text) => Value::Text(Cow::Borrowed(text)),
            Self::Length(value) => Value::Int(*value),
        }
    }
}

struct Cache<'a> {
    // Values drop before their cumulative string credit.
    entries: ReservedVec<OnceCell<Cached<'a>>>,
    strings: MemoryLease,
}

#[derive(Default)]
struct Work {
    values: u64,
    hits: u64,
}

pub(super) fn evaluate(
    function: &Function,
    expression: &Expression,
    values: &VariableColumn,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    if !matches!(
        function,
        Function::Lower | Function::Upper | Function::Trim | Function::Length
    ) {
        return Ok(None);
    }
    let Some(domain) = values
        .dictionary_domain_len()
        .filter(|&domain| domain != 0 && domain < rows)
    else {
        return Ok(None);
    };
    let denied_before = context.memory().snapshot().denied_reservations;
    let mut work = Work::default();
    match run(
        function, expression, values, rows, domain, context, &mut work,
    ) {
        Ok(result) => {
            context.record_payload_work(&[
                (Counter::DictionaryBatches, 1),
                (Counter::DictionaryRows, rows as u64),
                (Counter::DictionaryValues, work.values),
                (Counter::DictionaryHits, work.hits),
            ])?;
            Ok(Some(result))
        }
        Err(error)
            if context.memory().snapshot().denied_reservations > denied_before
                && error.to_string().contains("memory reservation denied:") =>
        {
            // run() has dropped the cache, its credits and partial output.
            // Only these pure functions can reuse the original prepared loop.
            context.check_cancelled()?;
            context.record_payload_work(&[(Counter::DictionaryDeclines, 1)])?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn run(
    function: &Function,
    expression: &Expression,
    values: &VariableColumn,
    rows: usize,
    domain: usize,
    context: &NativeExecutionContext<'_>,
    work: &mut Work,
) -> Result<ArrayRef> {
    let mut cache = Cache {
        entries: ReservedVec::new(context.memory())?,
        strings: context.memory().reserve(0)?,
    };
    cache.entries.reserve(domain)?;
    cache.entries.values.resize_with(domain, OnceCell::new);
    let entries = &cache.entries.values;
    let mut scratch = context.memory().reserve(0)?;
    expression.build(rows, context, |row| {
        record_row();
        let Some((index, text)) = values.utf8_entry_at(row)? else {
            return Ok(Value::Null);
        };
        let entry = &entries[index];
        if entry.get().is_none() {
            let value = match function {
                Function::Lower | Function::Upper => {
                    let Value::Text(Cow::Owned(text)) =
                        output(&mut scratch, growth(text.len(), 8)?, || {
                            if matches!(function, Function::Lower) {
                                text.to_lowercase()
                            } else {
                                text.to_uppercase()
                            }
                        })?
                    else {
                        return Err(failed(
                            "unary text kernel changed its output representation",
                        ));
                    };
                    scratch.resize(text.capacity() as u64)?;
                    cache.strings.absorb(&mut scratch)?;
                    Cached::Text(text)
                }
                Function::Trim => Cached::Trimmed(text.trim()),
                Function::Length => {
                    Cached::Length(i64::try_from(text.chars().count()).map_err(vortex_error)?)
                }
                _ => unreachable!("pure unary function admitted before cache allocation"),
            };
            entry
                .set(value)
                .map_err(|_| failed("dictionary expression entry was already initialized"))?;
            work.values += 1;
        } else {
            work.hits += 1;
        }
        Ok(entry.get().expect("entry initialized above").value())
    })
}
