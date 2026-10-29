//! Resource inheritance and lazy initialization for one embedded native owner.

use shardloom_core::{
    ExecutionResourceLimits, ExecutionResourceOrigin, ExecutionResourceRequest, ExecutionResources,
    FloatPolicy, Result, ShardLoomError,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::resident_session::{ConfigurableResidentSession, ResidentSessionSnapshot};
use std::sync::{Mutex, OnceLock, TryLockError};

/// One embedded client's immutable configuration and retained native runtime.
/// An unconfigured client can inspect capabilities or construct plans. Its first
/// complete explicit operation initializes the owner without becoming a default
/// allocation or installing that call's narrower authorization as session policy.
pub struct EmbeddedExecutionOwner {
    configured: Option<ExecutionResources>,
    float_policy: FloatPolicy,
    limits: Option<ExecutionResourceLimits>,
    controller: OnceLock<ConfigurableResidentSession>,
    initializing: Mutex<()>,
}

impl EmbeddedExecutionOwner {
    /// Create an explicitly configured owner, or a resource-free declaration.
    /// # Errors
    /// Rejects unauthorized configuration and native runtime construction failure.
    pub fn new(
        configured: Option<ExecutionResources>,
        limits: Option<ExecutionResourceLimits>,
    ) -> Result<Self> {
        Self::new_with_float_policy(configured, limits, FloatPolicy::Strict)
    }

    /// Select immutable floating semantics before lazy or eager initialization.
    /// # Errors
    /// Rejects unauthorized resources or native runtime construction failure.
    pub fn new_with_float_policy(
        configured: Option<ExecutionResources>,
        limits: Option<ExecutionResourceLimits>,
        float_policy: FloatPolicy,
    ) -> Result<Self> {
        let configured = configured
            .map(|resources| authorize(resources, limits))
            .transpose()?;
        let limits = configured.and_then(ExecutionResources::limits).or(limits);
        let controller = OnceLock::new();
        if let Some(resources) = configured {
            let _ = controller.set(ConfigurableResidentSession::new_with_float_policy(
                resources,
                float_policy,
            )?);
        }
        Ok(Self {
            configured,
            float_policy,
            limits,
            controller,
            initializing: Mutex::new(()),
        })
    }

    /// The original declaration, independent of later per-operation overrides.
    #[must_use]
    pub const fn configured_resources(&self) -> Option<ExecutionResources> {
        self.configured
    }

    /// The session's fixed semantic choice, independent of per-call resources.
    #[must_use]
    pub const fn float_policy(&self) -> FloatPolicy {
        self.float_policy
    }

    /// Intersect operation policy with this owner's administrator ceilings.
    /// # Errors
    /// Refuses grants exceeding either source of authorization.
    pub fn authorize(&self, resources: ExecutionResources) -> Result<ExecutionResources> {
        authorize(resources, self.limits)
    }

    /// Resolve a complete native declaration before demanding input.
    /// # Errors
    /// Rejects missing configuration and unauthorized per-operation declarations.
    pub fn resources(&self, supplied: Option<ExecutionResources>) -> Result<ExecutionResources> {
        let resources = match supplied.or(self.configured) {
            Some(resources) => resources,
            None => ExecutionResources::resolve(
                ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall),
                None,
                self.limits,
            )?,
        };
        self.authorize(resources)
    }

    /// Retain one controller across commands, plans, sources and results.
    /// The caller must admit its complete operation on this controller before
    /// input, preparation, execution or output. Initialization alone admits none.
    /// # Errors
    /// Rejects unauthorized resources, cancellation, poisoned initialization or
    /// native runtime construction failure.
    pub fn controller(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
    ) -> Result<&ConfigurableResidentSession> {
        let resources = self.authorize(resources)?;
        cancellation.check()?;
        if let Some(controller) = self.controller.get() {
            return Ok(controller);
        }
        let _initializing = loop {
            cancellation.check()?;
            match self.initializing.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => {
                    std::thread::park_timeout(std::time::Duration::from_millis(5));
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(failed("native owner initialization poisoned"));
                }
            }
        };
        cancellation.check()?;
        if self.controller.get().is_none() {
            // The first call's grant initializes physical capacity, but only
            // owner policy persists as authorization across subsequent calls.
            let initial = ExecutionResources::from_declaration(
                resources.memory_bytes(),
                resources.max_parallelism(),
                resources.memory_origin(),
                resources.parallelism_origin(),
                self.limits,
            )?;
            let _ = self
                .controller
                .set(ConfigurableResidentSession::new_with_float_policy(
                    initial,
                    self.float_policy,
                )?);
        }
        cancellation.check()?;
        self.controller
            .get()
            .ok_or_else(|| failed("native owner initialization did not retain its controller"))
    }

    /// Observe counters without creating a runtime or inventing an allocation.
    #[must_use]
    pub fn snapshot(&self) -> Option<ResidentSessionSnapshot> {
        self.controller
            .get()
            .map(ConfigurableResidentSession::snapshot)
    }
}

fn authorize(
    resources: ExecutionResources,
    limits: Option<ExecutionResourceLimits>,
) -> Result<ExecutionResources> {
    ExecutionResources::resolve(
        ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall),
        Some(resources),
        limits,
    )
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "embedded native owner: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocation(
        bytes: u64,
        lanes: usize,
        limits: Option<ExecutionResourceLimits>,
    ) -> ExecutionResources {
        ExecutionResources::from_declaration(
            bytes,
            lanes,
            ExecutionResourceOrigin::ExecutionCall,
            ExecutionResourceOrigin::ExecutionCall,
            limits,
        )
        .expect("fixture allocation")
    }

    #[test]
    fn float_policy_survives_lazy_initialization_and_resource_overrides() {
        let allocation = allocation(4096, 1, None);
        let token = CancellationToken::default();
        let strict = EmbeddedExecutionOwner::new(Some(allocation), None).unwrap();
        for configured in [None, Some(allocation)] {
            let extended = EmbeddedExecutionOwner::new_with_float_policy(
                configured,
                None,
                FloatPolicy::Extended,
            )
            .unwrap();
            assert_eq!(extended.float_policy(), FloatPolicy::Extended);
            let controller = extended.controller(allocation, &token).unwrap();
            assert_eq!(controller.float_policy(), FloatPolicy::Extended);
            controller
                .with_resources(allocation, &token, |session, _| {
                    assert_eq!(session.float_policy(), FloatPolicy::Extended);
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                strict
                    .controller(allocation, &token)
                    .unwrap()
                    .float_policy(),
                FloatPolicy::Strict
            );
            assert!(!controller.has_same_owner(strict.controller(allocation, &token).unwrap()));
        }
    }

    #[test]
    fn first_call_initializes_owner_without_default_or_sticky_call_limits() {
        let owner = EmbeddedExecutionOwner::new(
            None,
            Some(ExecutionResourceLimits::new(Some(4096), Some(4)).expect("limits")),
        )
        .expect("unconfigured owner");
        assert!(owner.snapshot().is_none());
        assert!(owner.resources(None).is_err());
        let token = CancellationToken::default();
        let narrow = allocation(
            1024,
            1,
            Some(ExecutionResourceLimits::new(Some(1024), Some(1)).expect("call limits")),
        );
        let controller = owner.controller(narrow, &token).expect("initialize");
        let lease = controller
            .with_resources(narrow, &token, |session, _| {
                session.shared_memory_pool().reserve(768)
            })
            .expect("retain memory");
        assert!(owner.configured_resources().is_none());
        assert!(owner.resources(None).is_err());
        let wider = allocation(4096, 4, None);
        let next = owner.controller(wider, &token).expect("same controller");
        assert!(controller.has_same_owner(next));
        next.with_resources(wider, &token, |session, _| {
            assert_eq!(session.shared_memory_pool().snapshot().reserved_bytes, 768);
            Ok(())
        })
        .expect("call-specific limits did not persist");
        assert!(
            next.with_resources(allocation(767, 1, None), &token, |_, _| Ok(()))
                .is_err()
        );
        assert!(owner.controller(allocation(4097, 1, None), &token).is_err());
        assert!(owner.controller(allocation(1024, 5, None), &token).is_err());
        drop(lease);
        next.with_resources(allocation(1, 1, None), &token, |_, _| Ok(()))
            .expect("released retained owner");
    }

    #[test]
    fn cancelled_initialization_is_inert_and_configured_inheritance_stays_fixed() {
        let unconfigured = EmbeddedExecutionOwner::new(None, None).expect("owner");
        let token = CancellationToken::default();
        token.cancel();
        assert!(
            unconfigured
                .controller(allocation(1024, 1, None), &token)
                .is_err()
        );
        assert!(unconfigured.snapshot().is_none());
        let configured = allocation(2048, 2, None);
        let owner = EmbeddedExecutionOwner::new(Some(configured), None).expect("configured");
        owner
            .controller(allocation(4096, 4, None), &CancellationToken::default())
            .expect("override");
        assert_eq!(owner.resources(None).expect("inherit"), configured);
    }
}
