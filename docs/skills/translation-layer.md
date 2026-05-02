# Skill: Translation Layer

## Purpose
Control conversion boundaries between Vortex-native structures and external/lakehouse interfaces.

## When to use
Use for schema/type mapping, format adapters, and interoperability endpoints.

## Rules
- Keep translation edges explicit; avoid leaking external assumptions into core.
- Preserve precision, null semantics, and ordering through conversions.
- Vortex-native representation is authoritative inside core execution.
- Unsupported mappings must fail with clear diagnostics, not implicit coercion.
- Avoid adding dependencies that compromise standalone architecture.

## Validation checklist
- [ ] Round-trip behavior is defined for supported mappings.
- [ ] Precision/nullability regressions are covered by tests.
- [ ] Conversion failures are explicit and debuggable.
- [ ] Core remains independent from external engine APIs.
