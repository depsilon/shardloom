# Skill: License & Provenance

## Purpose
Protect ShardLoom from incompatible licensing and unclear source provenance.

## When to use
Use before adding dependencies, copying patterns, or importing datasets/snippets.

## Rules
- Accept only Apache-2.0-compatible dependencies unless explicitly approved.
- Do not copy from GPL/AGPL/SSPL/BUSL/proprietary/unknown-license sources.
- Record origin and license for non-trivial borrowed ideas in PR notes/docs.
- Prefer original implementations over ports from existing engines.
- Keep NOTICE/license metadata current when dependency surface changes.

## Validation checklist
- [ ] New dependencies have confirmed Apache-2.0-compatible licenses.
- [ ] No code copied from disallowed or unverified sources.
- [ ] Provenance for externally-derived logic is documented.
- [ ] License/NOTICE updates were reviewed when needed.
