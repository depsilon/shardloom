//! Native values borrow a batch until a sparse pivot decision retains them.
//! Retention compacts only the selected row and carries its allocation credits.

use std::{cmp::Ordering, sync::Arc};

use super::super::values::{ByteCount, OwnedScalar, SharedKeyColumn, shared_key};
use super::{
    NativeBatch, NativeExecutionContext, PivotValue, Result, ScalarValue, failed, vortex_error,
};
use crate::local_primitives::{native_payload, native_relational_batch};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::array::{ArrayRef, VortexSessionExecute as _};

#[derive(Clone)]
pub(super) enum Datum {
    Scalar(Arc<OwnedScalar>),
    Nested {
        array: ArrayRef,
        key: SharedKeyColumn,
        row: usize,
        cancellation: CancellationToken,
    },
}

impl Datum {
    pub(super) fn from_batch(
        batch: &mut NativeBatch,
        column: usize,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let array = batch.column(column)?;
        if native_payload::is_nested(array.dtype()) {
            Ok(Self::Nested {
                array,
                key: batch.nested_key(column)?,
                row,
                cancellation: context.cancellation().clone(),
            })
        } else {
            Ok(Self::Scalar(batch.retained(column, row)?.into_shared()?))
        }
    }

    pub(super) fn null(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self::Scalar(
            OwnedScalar::copy(&ScalarValue::Null, memory)?.into_shared()?,
        ))
    }

    pub(super) fn retain(&self, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let Self::Nested { array, row, .. } = self else {
            return Ok(self.clone());
        };
        let indices = native_relational_batch::index_array(1, false, context, |_| Ok(Some(*row)))?;
        let selected = native_payload::take(array, &indices, array.dtype(), context)?;
        let mut execution = context.native_session().create_execution_ctx();
        let key = shared_key(
            &selected,
            &mut execution,
            context.memory(),
            context.cancellation(),
        )?;
        Ok(Self::Nested {
            array: selected,
            key,
            row: 0,
            cancellation: context.cancellation().clone(),
        })
    }

    pub(super) fn scalar(&self) -> Result<&ScalarValue> {
        match self {
            Self::Scalar(value) => Ok(value.value()),
            Self::Nested { .. } => Err(failed("nested pivot value requires native delivery")),
        }
    }

    pub(super) fn native(&self) -> Result<ArrayRef> {
        match self {
            Self::Nested { array, row: 0, .. } if array.len() == 1 => Ok(array.clone()),
            _ => Err(failed("nested pivot state is not a compact retained value")),
        }
    }

    pub(super) fn is_null(&self) -> Result<bool> {
        match self {
            Self::Scalar(value) => Ok(matches!(value.value(), ScalarValue::Null)),
            Self::Nested { key, row, .. } => key.value().is_null(*row),
        }
    }

    pub(super) fn compare(&self, other: &Self) -> Result<Ordering> {
        match (self, other) {
            (
                Self::Nested { key, row, .. },
                Self::Nested {
                    key: other,
                    row: other_row,
                    ..
                },
            ) => key.value().compare_at(*row, other.value(), *other_row),
            _ => Err(failed(
                "nested pivot comparison requires matching native values",
            )),
        }
    }

    /// Sparse key/name capacity is distinct from the native payload's credits.
    pub(super) fn key_bytes(&self) -> Result<usize> {
        match self {
            Self::Scalar(value) => Ok(super::text_bytes(value.value())),
            Self::Nested {
                key,
                row,
                cancellation,
                ..
            } => {
                let mut count = ByteCount::default();
                key.value()
                    .write_exact_key(*row, &mut count, cancellation)?;
                Ok(count.0)
            }
        }
    }

    pub(super) fn scalar_bytes(&self) -> usize {
        match self {
            Self::Scalar(value) => super::text_bytes(value.value()),
            Self::Nested { .. } => 0,
        }
    }
}

impl PivotValue for Datum {
    fn pivot_key(&self) -> Result<String> {
        match self {
            Self::Scalar(value) => value.value().pivot_key(),
            Self::Nested {
                key,
                row,
                cancellation,
                ..
            } => {
                let bytes = self.key_bytes()?;
                let mut output = String::new();
                output.try_reserve_exact(bytes).map_err(vortex_error)?;
                if output.capacity() > bytes {
                    return Err(failed("nested pivot key exceeded its reserved capacity"));
                }
                key.value()
                    .write_exact_key(*row, &mut output, cancellation)?;
                if output.len() != bytes {
                    return Err(failed("nested pivot key changed during construction"));
                }
                Ok(output)
            }
        }
    }

    fn pivot_name(&self) -> Result<String> {
        match self {
            Self::Scalar(value) => value.value().pivot_name(),
            Self::Nested { .. } => {
                let label = if self.is_null()? {
                    shardloom_core::StatValue::Null
                } else {
                    shardloom_core::StatValue::Utf8(self.pivot_key()?)
                };
                Ok(crate::local_primitives::pivot_output_column_name(&label))
            }
        }
    }

    fn pivot_equal(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Self::Scalar(left), Self::Scalar(right)) => left.value().pivot_equal(right.value()),
            (Self::Nested { .. }, Self::Nested { .. }) => {
                Ok(self.compare(other)? == Ordering::Equal)
            }
            _ => Err(failed(
                "pivot duplicate values changed their bound representation",
            )),
        }
    }

    fn pivot_numeric(&self) -> Result<f64> {
        self.scalar()?.pivot_numeric()
    }
}
