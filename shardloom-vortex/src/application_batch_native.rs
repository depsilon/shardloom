//! Native-only callback types. Schema/code inspection is separate from invocation.

use super::{ApplicationBatchFunction, CALLBACK_ACTIVE, failed};
use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
use shardloom_core::{
    Result,
    expression::{Determinism, EffectLevel, NullBehavior},
};
use std::sync::Arc;
use vortex::array::{ArrayRef, dtype::DType};

/// Per-invocation declared limits. The workspace allowance is reserved before
/// application code runs; it is not a hard cap on arbitrary host allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationBatchLimits {
    pub max_batch_rows: usize,
    pub max_output_bytes: u64,
    pub workspace_bytes: u64,
}

/// The admitted first contract is deterministic, rowwise, null-aware and pure.
/// Exact schemas include field order, nullability and logical type metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationBatchDeclaration {
    pub name: String,
    pub version: String,
    pub input: DType,
    pub output: DType,
    pub determinism: Determinism,
    pub null_behavior: NullBehavior,
    pub effect: EffectLevel,
    pub limits: ApplicationBatchLimits,
}

impl ApplicationBatchDeclaration {
    /// Parse ordered fields in the existing native `DType` JSON format.
    /// This only constructs bounded caller-owned schema metadata. Binding
    /// reserves its complete native metadata before any application invocation.
    /// # Errors
    /// Rejects malformed, duplicate or unsupported field declarations.
    pub fn parse_fields(fields: &[(String, String)]) -> Result<DType> {
        crate::local_primitives::native_input_schema::application_fields(fields)
    }
}

/// Borrowed cancellation and semantic policy for one callback invocation.
/// Native execution remains synchronous and cancellation is cooperative.
pub struct ApplicationBatchContext<'a> {
    pub(crate) execution: &'a NativeExecutionContext<'a>,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    session: &'a ResidentVortexSession,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    input: &'a ArrayRef,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    export: std::cell::Cell<crate::resident_session::NativeArrowExportReport>,
}

impl ApplicationBatchContext<'_> {
    /// # Errors
    /// Returns the current operation's cancellation/deadline failure.
    pub fn check_cancelled(&self) -> Result<()> {
        self.execution.check_cancelled()
    }

    #[must_use]
    pub fn cancellation(&self) -> &shardloom_exec::compute_pool::CancellationToken {
        self.execution.cancellation()
    }

    #[must_use]
    pub fn float_policy(&self) -> shardloom_core::FloatPolicy {
        self.execution.float_policy()
    }

    /// Use the already admitted native allocator and Vortex array providers.
    /// This borrows the active provider session; it does not start another query.
    #[must_use]
    pub fn native_session(&self) -> &vortex::session::VortexSession {
        self.execution.native_session()
    }

    /// Convert this invocation's input through the existing credited columnar
    /// export boundary. Retained Arrow aliases keep their buffer reservations.
    ///
    /// # Errors
    /// Rejects cancellation, unsupported representation and native memory denial.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn to_arrow(&self) -> Result<arrow_array::RecordBatch> {
        let mut output = None;
        let mut delivery = crate::resident_session::NativeArrowBatchDelivery::new(
            self.session,
            self.execution.cancellation(),
            |batch| {
                output = Some(batch);
                Ok(())
            },
        );
        delivery.push(self.input.clone(), self.execution)?;
        let report = delivery.finish(self.input.len() as u64)?;
        let mut observed = self.export.get();
        observed.append(report)?;
        self.export.set(observed);
        output.ok_or_else(|| failed("columnar callback input was not delivered"))
    }
}

/// A declared callback result awaiting native schema, value and ownership checks.
/// Neither variant is a completed query or a transferable execution grant.
pub enum ApplicationBatchOutput {
    Native(ArrayRef),
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    Arrow(arrow_array::RecordBatch),
}

impl From<ArrayRef> for ApplicationBatchOutput {
    fn from(array: ArrayRef) -> Self {
        Self::Native(array)
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
impl From<arrow_array::RecordBatch> for ApplicationBatchOutput {
    fn from(batch: arrow_array::RecordBatch) -> Self {
        Self::Arrow(batch)
    }
}

type Callback =
    dyn Fn(&ArrayRef, &ApplicationBatchContext<'_>) -> Result<ApplicationBatchOutput> + Send + Sync;

pub(crate) struct Invocation {
    pub(crate) output: ApplicationBatchOutput,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub(crate) export: crate::resident_session::NativeArrowExportReport,
    // Keep refusal active while returned application arrays are validated and
    // independently copied: lazy native arrays can still execute caller code.
    _active: Active,
}

struct Active;

impl Drop for Active {
    fn drop(&mut self) {
        CALLBACK_ACTIVE.set(false);
    }
}

pub(crate) struct Definition {
    pub(crate) declaration: ApplicationBatchDeclaration,
    callback: Arc<Callback>,
}

impl ApplicationBatchFunction {
    /// Admit caller-trusted application code without invoking it. The callback
    /// must be rowwise, preserve row order/count, honor its declared schemas and
    /// effects, and cooperate with cancellation. It must not invoke another query
    /// engine or reenter the active native context. This is not a plugin sandbox.
    ///
    /// Returned buffers pass native validation and independent ownership before
    /// publication; arbitrary callback allocations stay the caller's responsibility.
    ///
    /// # Errors
    /// Rejects unsupported semantic declarations and invalid names/limits. Exact
    /// native payload schema admission occurs during ordinary plan binding.
    pub fn new_trusted(
        declaration: ApplicationBatchDeclaration,
        callback: impl Fn(&ArrayRef, &ApplicationBatchContext<'_>) -> Result<ApplicationBatchOutput>
        + Send
        + Sync
        + 'static,
    ) -> Result<Self> {
        validate(&declaration)?;
        Ok(Self {
            name: Arc::from(declaration.name.as_str()),
            version: Arc::from(declaration.version.as_str()),
            definition: Arc::new(Definition {
                declaration,
                callback: Arc::new(callback),
            }),
        })
    }

    #[must_use]
    pub fn declaration(&self) -> &ApplicationBatchDeclaration {
        &self.definition.declaration
    }

    pub(crate) fn invoke(
        &self,
        input: &ArrayRef,
        session: &ResidentVortexSession,
        execution: &NativeExecutionContext<'_>,
    ) -> Result<Invocation> {
        super::check_application_callback_reentry()?;
        execution.check_cancelled()?;
        CALLBACK_ACTIVE.set(true);
        let active = Active;
        #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
        let _ = session;
        let context = ApplicationBatchContext {
            execution,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            session,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            input,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
            export: std::cell::Cell::default(),
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.definition.callback)(input, &context)
        }));
        match outcome {
            Ok(result) => {
                let output = result.map_err(|error| {
                    if matches!(
                        error,
                        shardloom_core::ShardLoomError::ApplicationFunction(_)
                    ) {
                        error
                    } else {
                        shardloom_core::ShardLoomError::application_function(
                            format!(
                                "application batch function '{}' failed: {error}",
                                self.name()
                            ),
                            error,
                        )
                    }
                })?;
                execution.check_cancelled()?;
                Ok(Invocation {
                    output,
                    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
                    export: context.export.get(),
                    _active: active,
                })
            }
            Err(_) => Err(failed(&format!("'{}' unwound with a panic", self.name()))),
        }
    }
}

fn validate(declaration: &ApplicationBatchDeclaration) -> Result<()> {
    if [&declaration.name, &declaration.version]
        .into_iter()
        .any(|value| {
            value.is_empty()
                || value.len() > 256
                || value.trim() != value
                || value.chars().any(char::is_control)
        })
    {
        return Err(failed(
            "name and version require 1..=256 bytes without control characters or surrounding whitespace",
        ));
    }
    if declaration.determinism != Determinism::Deterministic
        || declaration.null_behavior != NullBehavior::NullAware
        || declaration.effect != EffectLevel::PureDeterministic
    {
        return Err(failed(
            "only deterministic, rowwise, null-aware functions without external effects are admitted",
        ));
    }
    if declaration.limits.max_batch_rows == 0 || declaration.limits.max_output_bytes == 0 {
        return Err(failed("batch row and output byte limits must be positive"));
    }
    for dtype in [&declaration.input, &declaration.output] {
        if !matches!(
            dtype,
            DType::Struct(_, vortex::array::dtype::Nullability::NonNullable)
        ) {
            return Err(failed(
                "input and output must be nonnullable record schemas",
            ));
        }
        // Registration validates declared types without allocating query state
        // or invoking application code. Binding separately reserves its metadata.
        crate::native_payload_schema::schema_bytes(dtype)?;
    }
    Ok(())
}
