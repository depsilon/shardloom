# License and Provenance Skill

## Purpose

Use this skill when adding dependencies, reviewing generated code, using external references, or
implementing algorithms inspired by papers, documentation, or other projects.

The goal is to keep ShardLoom clean, enterprise-adoptable, and compatible with Apache-2.0.

## When to use

Use this skill for tasks involving:

- New dependencies.
- Copied or adapted code.
- AI-generated code.
- Implementations inspired by papers or external repositories.
- License metadata.
- NOTICE updates.
- Contributor guidance.
- Release preparation.

## Rules

- ShardLoom is Apache-2.0 licensed.
- Prefer dependencies with permissive licenses such as Apache-2.0, MIT, BSD, ISC, Zlib, or
  Unicode-style licenses.
- MPL-2.0 may require additional review because it has file-level copyleft obligations.
- Do not copy implementation code from GPL, AGPL, SSPL, BUSL, proprietary, source-available, or
  unknown-license sources.
- Do not paste code from blogs, forums, generated snippets, or repositories unless provenance and
  license compatibility are clear.
- It is acceptable to independently implement ideas from papers, specifications, standards, and
  documentation.
- Attribute external ideas when appropriate.
- Keep dependency choices minimal and justified.
- AI-assisted code is allowed, but the contributor is responsible for originality, tests, and
  license compatibility.
- If a dependency is required for parsing, testing, or benchmarking, it must not become an execution
  fallback unless explicitly approved by RFC. Spark and DataFusion fallback are not allowed.

## Required checks

For dependency changes:

- Identify the dependency license.
- Confirm compatibility with Apache-2.0.
- Confirm the dependency is necessary.
- Confirm it does not introduce fallback execution.
- Update relevant documentation if the dependency affects architecture.
- Update NOTICE if required.

For externally inspired implementations:

- Do not copy implementation code.
- Write the implementation independently.
- Add tests that verify behavior against expected semantics, not against copied code.
- Document the source of ideas when appropriate.

## Red flags

- "I copied this small helper from another repo."
- "The license was not listed."
- "This dependency is source-available, so it should be fine."
- "This GPL project has the algorithm we need."
- "Codex generated it, so license does not matter."
- "Let's temporarily use DataFusion or Spark internally until we replace it."

## Example Codex prompt fragment

When adding or reviewing dependencies, include this instruction:

"Use the License and Provenance skill. Verify dependency license compatibility with Apache-2.0. Do
not copy implementation code from incompatible or unknown-license sources. Do not add Spark,
DataFusion, or fallback execution."
