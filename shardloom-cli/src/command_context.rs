//! Explicit complete-command ownership for embedded and standalone callers.

use crate::execution_resources::ResourceArguments;
use shardloom_core::{ExecutionResources, OutputFormat, Result};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

/// Standalone callers have no inherited allocation. Embedded callers carry the
/// same native owner used by their plans, results and imported arrays.
#[derive(Default)]
pub(crate) struct CommandContext<'a> {
    cancellation: Option<&'a CancellationToken>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    owner: Option<&'a crate::native::EmbeddedExecutionOwner>,
}

pub(crate) struct CommandAdmission<'a> {
    _output: crate::cli_output::AdmittedOutput,
    resources: ExecutionResources,
    memory: LiveMemoryPool,
    cancellation: Option<&'a CancellationToken>,
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    native: Option<shardloom_vortex::resident_session::ResidentOperationAdmission<'a>>,
}

impl<'a> CommandContext<'a> {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn embedded(
        owner: &'a crate::native::EmbeddedExecutionOwner,
        cancellation: &'a CancellationToken,
    ) -> Self {
        Self {
            owner: Some(owner),
            cancellation: Some(cancellation),
        }
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn with_default_owner<'b>(
        &'b self,
        owner: &'b crate::native::EmbeddedExecutionOwner,
        cancellation: &'b CancellationToken,
    ) -> CommandContext<'b> {
        CommandContext {
            owner: Some(self.owner.unwrap_or(owner)),
            cancellation: Some(self.cancellation.unwrap_or(cancellation)),
        }
    }

    // Reduced builds retain the same invocation interface without native owners.
    #[cfg_attr(
        not(all(feature = "vortex-local-primitives", unix)),
        allow(clippy::unused_self)
    )]
    pub(crate) fn resolve(&self, arguments: &ResourceArguments) -> Result<ExecutionResources> {
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        if let Some(owner) = self.owner {
            return owner.authorize(arguments.resolve_with(owner.configured_resources())?);
        }
        arguments.resolve()
    }

    pub(crate) fn check(&self) -> Result<()> {
        self.cancellation.map_or(Ok(()), CancellationToken::check)
    }

    pub(crate) fn admit(&self, resources: ExecutionResources) -> Result<CommandAdmission<'a>> {
        self.check()?;
        #[cfg(all(feature = "vortex-local-primitives", unix))]
        if let Some(owner) = self.owner {
            let cancellation = self.cancellation.expect("embedded cancellation owner");
            let native = owner
                .controller(resources, cancellation)?
                .admit(resources, cancellation)?;
            return Ok(CommandAdmission {
                _output: crate::cli_output::AdmittedOutput::new(
                    native.resources(),
                    &native.session().shared_memory_pool(),
                    Some(native.session()),
                ),
                resources: native.resources(),
                memory: native.session().shared_memory_pool().clone(),
                cancellation: Some(cancellation),
                native: Some(native),
            });
        }
        let memory = LiveMemoryPool::new(resources.memory_bytes())?;
        Ok(CommandAdmission {
            _output: crate::cli_output::AdmittedOutput::new(
                resources,
                &memory,
                #[cfg(all(feature = "vortex-local-primitives", unix))]
                None,
            ),
            resources,
            memory,
            cancellation: self.cancellation,
            #[cfg(all(feature = "vortex-local-primitives", unix))]
            native: None,
        })
    }

    pub(crate) fn admit_for_command(
        &self,
        resources: ExecutionResources,
        command: &str,
        format: OutputFormat,
    ) -> std::result::Result<CommandAdmission<'a>, std::process::ExitCode> {
        self.admit(resources).map_err(|error| {
            crate::cli_output::emit_error(
                command,
                format,
                "command resource admission failed",
                &error,
            )
        })
    }
}

impl CommandAdmission<'_> {
    pub(crate) fn resources(&self) -> ExecutionResources {
        self.resources
    }

    pub(crate) fn check(&self) -> Result<()> {
        self.cancellation.map_or(Ok(()), CancellationToken::check)
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.cloned().unwrap_or_default()
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn native_session(
        &self,
    ) -> Result<&shardloom_vortex::resident_session::ResidentVortexSession> {
        self.native
            .as_ref()
            .map(shardloom_vortex::resident_session::ResidentOperationAdmission::session)
            .ok_or_else(|| shardloom_core::ShardLoomError::InvalidOperation(
                "native command requires its admitted session owner; no fallback execution was attempted".into(),
            ))
    }
}

impl std::ops::Deref for CommandAdmission<'_> {
    type Target = LiveMemoryPool;

    fn deref(&self) -> &Self::Target {
        &self.memory
    }
}
