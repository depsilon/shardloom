//! Bind unary text dispatch after the unchanged complete argument preparation.
//! Borrowed inputs remain inside their native owner; output is independently copied.

use super::super::{ArrayRef, Expression, NativeExecutionContext};
use super::{Function, KeyColumn, Result, Value, growth, output, vortex_error};
use std::borrow::Cow;

#[cfg(test)]
#[path = "native_relational_scalar_batch_tests.rs"]
mod tests;

impl Function {
    pub(in crate::local_primitives) fn evaluate_prepared(
        &self,
        expression: &Expression,
        columns: &[KeyColumn],
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let (values, pattern) = match (self, columns) {
            (Self::Regex(_), [KeyColumn::Variable(values), pattern]) => (values, Some(pattern)),
            (
                Self::Lower | Self::Upper | Self::Trim | Self::Length | Self::ByteLength,
                [KeyColumn::Variable(values)],
            ) => (values, None),
            _ => return Ok(None),
        };
        if !values.is_utf8() {
            return Ok(None);
        }
        #[cfg(all(test, debug_assertions))]
        super::super::super::native_relational_observation::record(|counts| {
            counts.expression_prepared_utf8_batches += 1;
        });
        let mut scratch = context.memory().reserve(0)?;
        macro_rules! build {
            ($row:ident, $text:ident, $value:expr) => {
                expression.build(rows, context, |$row| {
                    record_row();
                    match values.utf8_at($row)? {
                        Some($text) => $value,
                        None => Ok(Value::Null),
                    }
                })
            };
        }
        let result = match self {
            Self::Lower => build!(
                row,
                text,
                output(&mut scratch, growth(text.len(), 8)?, || text.to_lowercase())
            ),
            Self::Upper => build!(
                row,
                text,
                output(&mut scratch, growth(text.len(), 8)?, || text.to_uppercase())
            ),
            Self::Trim => build!(row, text, Ok(Value::Text(Cow::Borrowed(text.trim())))),
            Self::Length => build!(
                row,
                text,
                Ok(Value::Int(
                    i64::try_from(text.chars().count()).map_err(vortex_error)?
                ))
            ),
            Self::ByteLength => expression.build(rows, context, |row| {
                record_row();
                // Preparation already validated every UTF8 domain value. Byte
                // length needs the borrowed byte view, not another text scan.
                Ok(match values.byte_length_at(row)? {
                    Some(bytes) => Value::Int(i64::try_from(bytes).map_err(vortex_error)?),
                    None => Value::Null,
                })
            }),
            Self::Regex(regex) => {
                let pattern = pattern.expect("regex keeps its prepared pattern argument");
                build!(
                    row,
                    text,
                    Ok(if pattern.is_null(row)? {
                        Value::Null
                    } else {
                        Value::Bool(regex.is_match(text))
                    })
                )
            }
            _ => unreachable!("unary UTF8 function admitted above"),
        }?;
        Ok(Some(result))
    }
}

fn record_row() {
    #[cfg(all(test, debug_assertions))]
    super::super::super::native_relational_observation::record(|counts| {
        counts.expression_prepared_utf8_rows += 1;
    });
}
