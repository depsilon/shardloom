// SPDX-License-Identifier: Apache-2.0
//! Finite request authorization and attempt owners. No durable/retry state.

use super::{
    authority::{Authority, SQL_QUERY, SUITE_QUERY},
    *,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use wire::ReplacePolicy;

const MAX_ATTEMPTS: usize = 256;
const ATTEMPT_TIME: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Principal {
    Reader,
    Reader2,
    Denied,
    Administrator,
}

impl Principal {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Reader2 => "reader2",
            Self::Denied => "denied",
            Self::Administrator => "administrator",
        }
    }

    fn reader(self) -> Result<()> {
        if matches!(self, Self::Reader | Self::Reader2) {
            Ok(())
        } else {
            Err(permission(
                "principal has no source, operation or result grant",
            ))
        }
    }
}

fn permission(detail: &'static str) -> Error {
    Error {
        code: "SLX_PERMISSION",
        detail,
    }
}

pub(crate) struct Authentication {
    fingerprints: [Bytes; 4],
    lookups: std::sync::atomic::AtomicUsize,
}

impl Authentication {
    pub(crate) fn new(fingerprints: [Bytes; 4]) -> Result<Self> {
        for (index, fingerprint) in fingerprints.iter().enumerate() {
            if fingerprint.len() != 32 || fingerprints[..index].contains(fingerprint) {
                return Err(invalid(
                    "certificate allowlist requires four unique fingerprints",
                ));
            }
        }
        Ok(Self {
            fingerprints,
            lookups: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    // The caller obtains this DER only from the successfully authenticated TLS
    // connection. This lookup does not replace certificate verification.
    pub(crate) fn verified_leaf(&self, der: &[u8]) -> Result<Principal> {
        self.lookups.fetch_add(1, Ordering::AcqRel);
        let fingerprint = hash(der);
        self.fingerprints
            .iter()
            .position(|known| *known == fingerprint)
            .map(|index| {
                [
                    Principal::Reader,
                    Principal::Reader2,
                    Principal::Denied,
                    Principal::Administrator,
                ][index]
            })
            .ok_or(Error {
                code: "SLX_UNAUTHENTICATED",
                detail: "verified certificate is not in this fixture allowlist",
            })
    }

    pub(crate) fn lookups(&self) -> usize {
        self.lookups.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub(crate) struct Connection {
    data: Arc<Semaphore>,
    abort: Arc<ConnectionAbort>,
}

struct ConnectionAbort {
    aborted: AtomicBool,
    reader: futures::task::AtomicWaker,
    writer: futures::task::AtomicWaker,
}

impl Connection {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(Semaphore::new(1)),
            abort: Arc::new(ConnectionAbort {
                aborted: AtomicBool::new(false),
                reader: futures::task::AtomicWaker::new(),
                writer: futures::task::AtomicWaker::new(),
            }),
        }
    }

    pub(crate) fn abort(&self) {
        self.abort.aborted.store(true, Ordering::Release);
        self.abort.reader.wake();
        self.abort.writer.wake();
    }

    pub(crate) fn readable(&self, context: &std::task::Context<'_>) -> bool {
        self.abort.reader.register(context.waker());
        !self.abort.aborted.load(Ordering::Acquire)
    }

    pub(crate) fn writable(&self, context: &std::task::Context<'_>) -> bool {
        self.abort.writer.register(context.waker());
        !self.abort.aborted.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub(crate) struct Registry(Arc<Inner>);

struct Inner {
    authority: Arc<Authority>,
    epoch: Bytes,
    active: Arc<Semaphore>,
    state: Mutex<State>,
}

struct State {
    version: u64,
    next_ticket: u64,
    entries: BTreeMap<Bytes, Entry>,
}

struct Entry {
    binding: Binding,
    phase: Phase,
}
enum Phase {
    Issued,
    Active(Weak<Attempt>),
    Complete,
    Failed,
}

pub(crate) struct Attempt {
    pub(crate) binding: Binding,
    pub(crate) cancellation: CancellationToken,
    deadline: Instant,
    notify: Notify,
    completed: AtomicBool,
    registry: Arc<Inner>,
    // These permits must outlive the handler and every application buffer.
    _connection: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl Inner {
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Error {
            code: "SLX_INTERNAL",
            detail: "attempt state is poisoned",
        })
    }

    fn authorize(&self, state: &State, principal: Principal, binding: &Binding) -> Result<()> {
        principal.reader()?;
        if binding.principal != principal.name() {
            return Err(permission("operation belongs to another principal"));
        }
        let case = self.authority.case(&binding.source_id)?;
        let expected = case.binding(
            principal.name(),
            &self.epoch,
            binding.attempt_id.clone(),
            state.version,
            &binding.query_id,
        )?;
        validate_binding(binding, &expected)
    }
}

impl Registry {
    pub(crate) fn new(authority: Arc<Authority>, epoch: Bytes) -> Result<Self> {
        if epoch.len() != 16 || epoch.iter().all(|byte| *byte == 0) {
            return Err(invalid(
                "server epoch must be a nonzero sixteen-byte identity",
            ));
        }
        Ok(Self(Arc::new(Inner {
            authority,
            epoch,
            active: Arc::new(Semaphore::new(2)),
            state: Mutex::new(State {
                version: 1,
                next_ticket: 0,
                entries: BTreeMap::new(),
            }),
        })))
    }

    pub(crate) fn begin(
        &self,
        principal: Principal,
        binding: &Binding,
        connection: &Connection,
        issued_ticket: bool,
    ) -> Result<Arc<Attempt>> {
        let mut state = self.0.lock()?;
        self.0.authorize(&state, principal, binding)?;
        if issued_ticket {
            let entry = state
                .entries
                .get(&binding.attempt_id)
                .ok_or_else(|| permission("unknown result ticket"))?;
            validate_binding(binding, &entry.binding)?;
            if binding.query_id != SQL_QUERY {
                return Err(permission(
                    "result ticket requires the frozen SQL projection",
                ));
            }
            if !matches!(entry.phase, Phase::Issued) {
                return Err(Error {
                    code: "SLX_REPLAY",
                    detail: "result ticket has already started or terminated",
                });
            }
        } else {
            if binding.query_id != SUITE_QUERY {
                return Err(permission(
                    "direct exchange requires the frozen projection suite",
                ));
            }
            if state.entries.contains_key(&binding.attempt_id) {
                return Err(Error {
                    code: "SLX_REPLAY",
                    detail: "attempt ID cannot be resumed or reused",
                });
            }
            if state.entries.len() >= MAX_ATTEMPTS {
                return Err(limit("epoch replay-state capacity is exhausted"));
            }
        }
        let connection = connection
            .data
            .clone()
            .try_acquire_owned()
            .map_err(|_| limit("connection already owns a data attempt"))?;
        let global = self
            .0
            .active
            .clone()
            .try_acquire_owned()
            .map_err(|_| limit("peer already owns two active attempts"))?;
        let deadline = Instant::now() + ATTEMPT_TIME;
        let attempt = Arc::new(Attempt {
            binding: binding.clone(),
            cancellation: CancellationToken::default().with_deadline(deadline),
            deadline,
            notify: Notify::new(),
            completed: AtomicBool::new(false),
            registry: self.0.clone(),
            _connection: connection,
            _global: global,
        });
        state.entries.insert(
            binding.attempt_id.clone(),
            Entry {
                binding: binding.clone(),
                phase: Phase::Active(Arc::downgrade(&attempt)),
            },
        );
        Ok(attempt)
    }

    pub(crate) fn issue_sql(&self, principal: Principal) -> Result<Binding> {
        principal.reader()?;
        let mut state = self.0.lock()?;
        if state.entries.len() >= MAX_ATTEMPTS {
            return Err(limit("epoch replay-state capacity is exhausted"));
        }
        let case = self.0.authority.case("dictionary_large")?;
        let binding = loop {
            state.next_ticket = state
                .next_ticket
                .checked_add(1)
                .ok_or_else(|| limit("ticket sequence overflow"))?;
            let mut hash = Sha256::new();
            hash.update(&self.0.epoch);
            hash.update(state.next_ticket.to_le_bytes());
            let id = Bytes::copy_from_slice(&hash.finalize()[..16]);
            if !state.entries.contains_key(&id) {
                break case.binding(
                    principal.name(),
                    &self.0.epoch,
                    id,
                    state.version,
                    SQL_QUERY,
                )?;
            }
        };
        state.entries.insert(
            binding.attempt_id.clone(),
            Entry {
                binding: binding.clone(),
                phase: Phase::Issued,
            },
        );
        Ok(binding)
    }

    pub(crate) fn cancel(&self, principal: Principal, binding: &Binding) -> Result<()> {
        let active = {
            let mut state = self.0.lock()?;
            self.0.authorize(&state, principal, binding)?;
            let entry = state
                .entries
                .get_mut(&binding.attempt_id)
                .ok_or_else(|| permission("unknown cancellation target"))?;
            validate_binding(binding, &entry.binding)?;
            match &entry.phase {
                Phase::Active(attempt) => attempt.upgrade(),
                Phase::Issued => {
                    entry.phase = Phase::Failed;
                    None
                }
                Phase::Complete | Phase::Failed => {
                    return Err(permission("cancellation target is already terminal"));
                }
            }
        };
        // Never drop a possibly last Attempt reference while holding its registry lock.
        if let Some(active) = active {
            active.cancel();
        }
        Ok(())
    }

    pub(crate) fn replace_policy(
        &self,
        principal: Principal,
        replace: &ReplacePolicy,
    ) -> Result<()> {
        if principal != Principal::Administrator {
            return Err(permission("policy replacement requires administrator"));
        }
        let active = {
            let mut state = self.0.lock()?;
            if replace.server_epoch != self.0.epoch
                || replace.expected_version != state.version
                || state.version.checked_add(1) != Some(replace.new_version)
            {
                return Err(mismatch(
                    "policy replacement requires current epoch and next version",
                ));
            }
            state.version = replace.new_version;
            state
                .entries
                .values_mut()
                .filter_map(|entry| match &entry.phase {
                    Phase::Active(attempt) => attempt.upgrade(),
                    Phase::Issued => {
                        entry.phase = Phase::Failed;
                        None
                    }
                    Phase::Complete | Phase::Failed => None,
                })
                .collect::<Vec<_>>()
        };
        for attempt in active {
            attempt.cancel();
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<(usize, usize, u64)> {
        let state = self.0.lock()?;
        Ok((
            2 - self.0.active.available_permits(),
            state.entries.len(),
            state.version,
        ))
    }
}

impl Attempt {
    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
        self.notify.notify_waiters();
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            Err(Error {
                code: "SLX_CANCELLED",
                detail: "attempt is cancelled or its deadline expired",
            })
        } else {
            Ok(())
        }
    }

    pub(crate) async fn cancelled(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.cancellation.is_cancelled() {
            return;
        }
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep_until(self.deadline.into()) => self.cancel(),
        }
    }

    pub(crate) fn complete(&self) -> Result<()> {
        let state = self.registry.lock()?;
        self.check()?;
        if state.version != self.binding.policy_version {
            return Err(permission("attempt policy version is no longer current"));
        }
        self.completed.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        let complete = self.completed.load(Ordering::Acquire) && !self.cancellation.is_cancelled();
        self.cancel();
        let mut state = self
            .registry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = state.entries.get_mut(&self.binding.attempt_id) {
            entry.phase = if complete {
                Phase::Complete
            } else {
                Phase::Failed
            };
        }
    }
}

#[cfg(test)]
#[path = "native_exchange_attempt_tests.rs"]
mod tests;
