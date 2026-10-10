//! Explicit caller allocation, before input inspection or native execution.
//!
//! This configuration grants permission; it neither allocates memory nor measures
//! usage. Native providers must attach the resolved values to their existing
//! shared memory and scheduling owners. No environment or hardware is inspected.

use crate::{Result, ShardLoomError};

/// Bytes in the public `memory_gb` unit, which denotes a binary GiB.
pub const BYTES_PER_GIB: u64 = 1 << 30;

/// Where an explicitly supplied execution resource value originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionResourceOrigin {
    ExecutionCall,
    Context,
    Session,
    Environment,
    Platform,
}

impl ExecutionResourceOrigin {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionCall => "execution_call",
            Self::Context => "context",
            Self::Session => "session",
            Self::Environment => "environment",
            Self::Platform => "platform",
        }
    }
}

/// Optional declarations for resolution against an explicit inherited allocation.
///
/// Both memory spellings in one declaration conflict, even when numerically equal.
/// A missing field inherits only from `ExecutionResources`, never from the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResourceRequest {
    pub memory_gb: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub max_parallelism: Option<usize>,
    pub origin: ExecutionResourceOrigin,
}

impl ExecutionResourceRequest {
    #[must_use]
    pub const fn new(origin: ExecutionResourceOrigin) -> Self {
        Self {
            memory_gb: None,
            memory_bytes: None,
            max_parallelism: None,
            origin,
        }
    }
}

/// Explicit deployment ceilings, independent of a requested job allocation.
///
/// These values must come from an authorized deployment configuration. This type
/// enforces them during resolution; it does not authenticate the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResourceLimits {
    memory_bytes: Option<u64>,
    max_parallelism: Option<usize>,
}

impl ExecutionResourceLimits {
    /// # Errors
    /// Rejects a supplied zero ceiling rather than interpreting it as unlimited.
    pub fn new(memory_bytes: Option<u64>, max_parallelism: Option<usize>) -> Result<Self> {
        require_positive(memory_bytes, "memory_bytes ceiling")?;
        if max_parallelism == Some(0) {
            return Err(configuration_error(
                "max_parallelism ceiling must be positive",
            ));
        }
        Ok(Self {
            memory_bytes,
            max_parallelism,
        })
    }

    #[must_use]
    pub const fn memory_bytes(self) -> Option<u64> {
        self.memory_bytes
    }

    #[must_use]
    pub const fn max_parallelism(self) -> Option<usize> {
        self.max_parallelism
    }

    fn intersect(self, other: Self) -> Self {
        Self {
            memory_bytes: smaller(self.memory_bytes, other.memory_bytes),
            max_parallelism: smaller(self.max_parallelism, other.max_parallelism),
        }
    }
}

/// Validated, immutable allocation for one complete native operation.
///
/// There is deliberately no `Default`. Memory is exact bytes and parallelism is
/// a positive ceiling on integer execution lanes, not a CPU quota or utilization
/// measurement. Each field retains its origin through partial call overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResources {
    memory_bytes: u64,
    max_parallelism: usize,
    memory_origin: ExecutionResourceOrigin,
    parallelism_origin: ExecutionResourceOrigin,
    limits: Option<ExecutionResourceLimits>,
}

impl ExecutionResources {
    /// Resolve explicit call/context declarations and preserve inherited ceilings.
    ///
    /// Supplied fields override inherited fields. New ceilings may tighten an
    /// inherited ceiling, but can never remove or increase it. Exceeding a ceiling
    /// is an error; no resource quantity is silently reduced or substituted.
    ///
    /// # Errors
    /// Rejects missing, zero, conflicting, overflowing, or unauthorized resources.
    pub fn resolve(
        request: ExecutionResourceRequest,
        inherited: Option<Self>,
        limits: Option<ExecutionResourceLimits>,
    ) -> Result<Self> {
        if request.memory_gb.is_some() && request.memory_bytes.is_some() {
            return Err(configuration_error(
                "supply only one of memory_gb (GiB) and memory_bytes",
            ));
        }
        require_positive(request.memory_gb, "memory_gb")?;
        require_positive(request.memory_bytes, "memory_bytes")?;
        if request.max_parallelism == Some(0) {
            return Err(configuration_error("max_parallelism must be positive"));
        }
        let supplied_memory = match request.memory_gb {
            Some(gib) => Some(gib.checked_mul(BYTES_PER_GIB).ok_or_else(|| {
                configuration_error("memory_gb exceeds the supported unsigned 64-bit byte range")
            })?),
            None => request.memory_bytes,
        };
        let memory = supplied_memory
            .map(|value| (value, request.origin))
            .or(inherited.map(|value| (value.memory_bytes, value.memory_origin)));
        let max_parallelism = request
            .max_parallelism
            .map(|value| (value, request.origin))
            .or(inherited.map(|value| (value.max_parallelism, value.parallelism_origin)));
        let (Some((memory_bytes, memory_origin)), Some((max_parallelism, parallelism_origin))) =
            (memory, max_parallelism)
        else {
            let missing = match (memory.is_none(), max_parallelism.is_none()) {
                (true, true) => "memory_gb (or memory_bytes) and max_parallelism",
                (true, false) => "memory_gb (or memory_bytes)",
                (false, _) => "max_parallelism",
            };
            return Err(configuration_error(format!(
                "missing required execution resources: {missing}; configure a context/session or supply them on this execution call"
            )));
        };
        let limits = match (inherited.and_then(|value| value.limits), limits) {
            (Some(previous), Some(next)) => Some(previous.intersect(next)),
            (previous, next) => previous.or(next),
        };
        if let Some(ceiling) = limits {
            if ceiling
                .memory_bytes
                .is_some_and(|value| memory_bytes > value)
            {
                return Err(configuration_error(
                    "memory allocation exceeds the authorized memory_bytes ceiling",
                ));
            }
            if ceiling
                .max_parallelism
                .is_some_and(|value| max_parallelism > value)
            {
                return Err(configuration_error(
                    "max_parallelism exceeds the authorized execution-lane ceiling",
                ));
            }
        }
        Ok(Self {
            memory_bytes,
            max_parallelism,
            memory_origin,
            parallelism_origin,
            limits,
        })
    }

    /// Construct a complete exact-byte declaration with one explicit origin.
    ///
    /// # Errors
    /// Rejects zero memory or parallelism.
    pub fn from_bytes(
        memory_bytes: u64,
        max_parallelism: usize,
        origin: ExecutionResourceOrigin,
    ) -> Result<Self> {
        Self::resolve(
            ExecutionResourceRequest {
                memory_gb: None,
                memory_bytes: Some(memory_bytes),
                max_parallelism: Some(max_parallelism),
                origin,
            },
            None,
            None,
        )
    }

    /// Construct a complete declaration using binary GiB.
    ///
    /// # Errors
    /// Rejects zero memory/parallelism or a GiB-to-bytes overflow.
    pub fn from_gib(
        memory_gb: u64,
        max_parallelism: usize,
        origin: ExecutionResourceOrigin,
    ) -> Result<Self> {
        Self::resolve(
            ExecutionResourceRequest {
                memory_gb: Some(memory_gb),
                memory_bytes: None,
                max_parallelism: Some(max_parallelism),
                origin,
            },
            None,
            None,
        )
    }

    #[must_use]
    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    /// Exact whole GiB when representable, without rounding a byte allocation.
    #[must_use]
    pub const fn whole_gib(self) -> Option<u64> {
        if self.memory_bytes.is_multiple_of(BYTES_PER_GIB) {
            Some(self.memory_bytes / BYTES_PER_GIB)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn max_parallelism(self) -> usize {
        self.max_parallelism
    }

    #[must_use]
    pub const fn memory_origin(self) -> ExecutionResourceOrigin {
        self.memory_origin
    }

    #[must_use]
    pub const fn parallelism_origin(self) -> ExecutionResourceOrigin {
        self.parallelism_origin
    }

    #[must_use]
    pub const fn limits(self) -> Option<ExecutionResourceLimits> {
        self.limits
    }
}

fn require_positive(value: Option<u64>, name: &str) -> Result<()> {
    if value == Some(0) {
        return Err(configuration_error(format!("{name} must be positive")));
    }
    Ok(())
}

fn configuration_error(message: impl Into<String>) -> ShardLoomError {
    ShardLoomError::new(message)
}

fn smaller<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiagnosticCode;

    #[test]
    fn missing_fields_never_receive_numeric_defaults() {
        for request in [
            ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall),
            ExecutionResourceRequest {
                memory_gb: Some(16),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::Context)
            },
            ExecutionResourceRequest {
                max_parallelism: Some(8),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::Session)
            },
        ] {
            let error = ExecutionResources::resolve(request, None, None).unwrap_err();
            assert_eq!(
                error.to_diagnostic().code,
                DiagnosticCode::ConfigurationError
            );
            assert!(
                error
                    .message()
                    .contains("missing required execution resources")
            );
            assert!(!error.to_diagnostic().fallback.attempted);
        }
    }

    #[test]
    fn exact_bytes_and_gib_units_preserve_the_declared_allocation() {
        let bytes =
            ExecutionResources::from_bytes(1_500_000_001, 3, ExecutionResourceOrigin::Platform)
                .unwrap();
        assert_eq!(bytes.memory_bytes(), 1_500_000_001);
        assert_eq!(bytes.whole_gib(), None);
        assert_eq!(bytes.max_parallelism(), 3);
        let gib = ExecutionResources::from_gib(16, 8, ExecutionResourceOrigin::Context).unwrap();
        assert_eq!(gib.memory_bytes(), 17_179_869_184);
        assert_eq!(gib.whole_gib(), Some(16));
        assert!(
            ExecutionResources::from_gib(
                u64::MAX / BYTES_PER_GIB,
                1,
                ExecutionResourceOrigin::ExecutionCall
            )
            .is_ok()
        );
        assert!(
            ExecutionResources::from_gib(
                u64::MAX / BYTES_PER_GIB + 1,
                1,
                ExecutionResourceOrigin::ExecutionCall
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_override_cannot_fall_back_to_valid_inheritance() {
        let inherited =
            ExecutionResources::from_gib(16, 8, ExecutionResourceOrigin::Context).unwrap();
        for request in [
            ExecutionResourceRequest {
                memory_gb: Some(0),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            ExecutionResourceRequest {
                memory_bytes: Some(0),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            ExecutionResourceRequest {
                max_parallelism: Some(0),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            ExecutionResourceRequest {
                memory_gb: Some(u64::MAX),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            ExecutionResourceRequest {
                memory_gb: Some(1),
                memory_bytes: Some(BYTES_PER_GIB),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
        ] {
            assert!(ExecutionResources::resolve(request, Some(inherited), None).is_err());
        }
    }

    #[test]
    fn partial_overrides_keep_the_other_fields_origin() {
        let inherited =
            ExecutionResources::from_gib(16, 8, ExecutionResourceOrigin::Context).unwrap();
        let overridden = ExecutionResources::resolve(
            ExecutionResourceRequest {
                max_parallelism: Some(4),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            Some(inherited),
            None,
        )
        .unwrap();
        assert_eq!(overridden.memory_bytes(), inherited.memory_bytes());
        assert_eq!(overridden.max_parallelism(), 4);
        assert_eq!(overridden.memory_origin(), ExecutionResourceOrigin::Context);
        assert_eq!(
            overridden.parallelism_origin(),
            ExecutionResourceOrigin::ExecutionCall
        );
        let unchanged = ExecutionResources::resolve(
            ExecutionResourceRequest::new(ExecutionResourceOrigin::Session),
            Some(overridden),
            None,
        )
        .unwrap();
        assert_eq!(unchanged, overridden);
    }

    #[test]
    fn inherited_ceilings_cannot_be_removed_or_weakened() {
        let limits = ExecutionResourceLimits::new(Some(2 * BYTES_PER_GIB), Some(4)).unwrap();
        let inherited = ExecutionResources::resolve(
            ExecutionResourceRequest {
                memory_gb: Some(1),
                memory_bytes: None,
                max_parallelism: Some(2),
                origin: ExecutionResourceOrigin::Platform,
            },
            None,
            Some(limits),
        )
        .unwrap();
        for next_limits in [
            None,
            Some(ExecutionResourceLimits::new(None, None).unwrap()),
            Some(ExecutionResourceLimits::new(Some(9 * BYTES_PER_GIB), Some(12)).unwrap()),
        ] {
            for request in [
                ExecutionResourceRequest {
                    memory_gb: Some(3),
                    ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
                },
                ExecutionResourceRequest {
                    max_parallelism: Some(5),
                    ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
                },
            ] {
                assert!(
                    ExecutionResources::resolve(request, Some(inherited), next_limits).is_err()
                );
            }
        }
        let exact = ExecutionResources::resolve(
            ExecutionResourceRequest {
                memory_bytes: Some(2 * BYTES_PER_GIB),
                max_parallelism: Some(4),
                ..ExecutionResourceRequest::new(ExecutionResourceOrigin::ExecutionCall)
            },
            Some(inherited),
            None,
        )
        .unwrap();
        assert_eq!(exact.limits(), Some(limits));
        assert!(
            ExecutionResources::resolve(
                ExecutionResourceRequest::new(ExecutionResourceOrigin::Session),
                Some(exact),
                Some(ExecutionResourceLimits::new(Some(BYTES_PER_GIB), None).unwrap())
            )
            .is_err()
        );
        assert!(ExecutionResourceLimits::new(Some(0), None).is_err());
        assert!(ExecutionResourceLimits::new(None, Some(0)).is_err());
    }
}
