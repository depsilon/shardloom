<!-- SPDX-License-Identifier: Apache-2.0 -->

# License Provenance

ShardLoom is licensed under the Apache License, Version 2.0. The repository root
`LICENSE` file contains the full Apache-2.0 license text, and `NOTICE` carries
project attribution and source-distribution notice posture.

## Dependency License Policy

Runtime, build, test, and release dependencies must be compatible with
Apache-2.0 project distribution goals unless explicitly approved through an RFC.
Preferred dependency licenses are permissive licenses such as Apache-2.0, MIT,
BSD, ISC, Zlib, and Unicode-style licenses. MPL-2.0 dependencies require
additional review because of file-level copyleft obligations.

New dependencies require a provenance review that records:

- dependency purpose and package scope
- declared license and compatibility with Apache-2.0
- whether the dependency affects runtime, build, test, benchmark, or packaging
- security and maintenance posture where material
- no-fallback architecture impact
- NOTICE or generated third-party notice obligations

## Benchmark-Only Dependency Separation

Benchmark-only dependencies must stay isolated from ShardLoom runtime packages
and must not become execution fallback paths. External engines used by benchmark
harnesses are comparison baselines or correctness oracles only. They are not
ShardLoom execution providers and must not be reported as ShardLoom execution.

Release artifacts must keep runtime dependencies separate from optional
benchmark/dev tooling. Benchmark claims require reproducible evidence and clear
labels for external-baseline-only rows.

## Incompatible Or Unknown Source Policy

ShardLoom must not copy implementation code from GPL, AGPL, SSPL, BUSL,
proprietary, source-available, or unknown-license projects. The same rule
applies to code copied from blogs, forums, generated snippets, or repositories
where provenance and license compatibility are unclear.

It is acceptable to independently implement ideas from papers, public
specifications, standards, and documentation. Attribute external ideas where
appropriate, and validate behavior with ShardLoom-owned tests rather than copied
implementation code.

## SPDX And REUSE Posture

ShardLoom uses Apache-2.0 metadata in Cargo manifests, Python package metadata,
Conda recipes, and release documentation. New source or policy files should use
`SPDX-License-Identifier: Apache-2.0` where the file format supports a clear
comment form. Existing files do not need noisy mass rewrites unless a future
repo policy explicitly requires full per-file SPDX coverage.

The root `REUSE.toml` records aggregate Apache-2.0 posture for repository
families that do not yet carry per-file SPDX headers. This is a lightweight
posture marker, not permission to skip dependency or copied-code review.

## AI-Assisted Contribution Review Policy

AI-assisted contributions are allowed when the contributor reviews the output
for originality, correctness, license compatibility, and no-fallback
architecture compliance. AI generation does not remove contributor
responsibility for provenance. Contributions must not include copied code from
incompatible or unknown-license sources, and non-trivial changes should include
tests or evidence appropriate to the touched surface.
