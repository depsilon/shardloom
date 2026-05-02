# Skill: Object Store Runtime

## Purpose
Guide safe, deterministic object-store and runtime integration for distributed I/O paths.

## When to use
Use for object store connectors, retries, read/write paths, and runtime resource handling.

## Rules
- Keep failures explicit with context (path, operation, retry state).
- Bound retries/timeouts; avoid infinite or silent retry loops.
- Preserve idempotency and atomicity expectations where applicable.
- Avoid runtime coupling that forces external execution engines.
- Validate behavior under partial failures and eventual-consistency edge cases.

## Validation checklist
- [ ] Error surfaces include actionable context.
- [ ] Retry/timeout policy is bounded and documented.
- [ ] Partial-failure scenarios are tested or simulated.
- [ ] Runtime integration preserves standalone operation.
