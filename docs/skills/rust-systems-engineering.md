# Skill: Rust Systems Engineering

## Purpose
Provide reliable, maintainable Rust guidance for engine/runtime code without overbuilding.

## When to use
Use for any Rust implementation, refactor, API change, or error-path update.

## Rules
- Keep control flow explicit; prefer clear typed errors over opaque failures.
- Preserve standalone architecture; no Spark/DataFusion/external execution fallback.
- Keep modules cohesive and boundaries narrow; avoid speculative abstractions.
- Prefer deterministic behavior and stable ordering where outputs are compared.
- Avoid unsafe code unless strictly required; document invariants when used.

## Validation checklist
- [ ] Error messages are actionable and explicit for unsupported paths.
- [ ] Changes preserve existing public contracts unless intentionally revised.
- [ ] No new fallback/delegation path to other execution engines.
- [ ] Code is formatted and lint-clean under repository checks.
