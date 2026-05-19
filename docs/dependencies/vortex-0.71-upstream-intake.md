# Vortex 0.71 Upstream Intake

## Purpose

This document records the first ShardLoom intake review for upstream Vortex `0.71.0`.
It is not an approval to change runtime behavior, broaden support claims, or enable external
query-engine fallback.

Canonical actionable work lives in `docs/architecture/phased-execution-plan.md`.

## Source Snapshot

- Upstream release: `0.71.0`
- Release date: 2026-05-18
- Release URL: <https://github.com/vortex-data/vortex/releases/tag/0.71.0>
- Crates.io version check: `cargo info vortex@0.71.0`
- Initial ShardLoom direct dependency at intake time: `vortex = "0.70"` in
  `shardloom-vortex/Cargo.toml`
- Initial Cargo compatibility check:
  - `cargo update -p vortex --precise 0.71.0 --dry-run` fails because the current requirement is
    `^0.70`.
  - A real upgrade requires a manifest change to `0.71` or `0.71.0`.
- Post-`GAR-VORTEX-071B` dependency state:
  - `shardloom-vortex` requests optional `vortex = "0.71"`.
  - `Cargo.lock` records Vortex `0.71.0`.
  - The bump remains feature-gated and does not broaden runtime support.

## Release Notes Summary

The upstream 0.71 release notes include one breaking item, several runtime-relevant features,
performance changes, and bug fixes. The items below are grouped by ShardLoom relevance rather than
by upstream category.

### Must Review Before Dependency Bump

- Python runtime switches to `CurrentThreadRuntime`.
  - ShardLoom impact: relevant only for Python/PyVortex-facing workflows. ShardLoom must ensure no
    local Python helper assumes a previous Vortex Python runtime model.
- `DType::Union` is added.
  - ShardLoom impact: update DType mapping and unsupported diagnostics so union values are explicit,
    not misclassified.
- Variant array and `VariantGet` expression are updated.
  - ShardLoom impact: potentially useful for nested/semi-structured local runtime, but must remain
    blocked until source/sink and expression evidence exists.
- Struct cast implementation becomes pluggable.
  - ShardLoom impact: relevant to cast-family runtime, schema coercion, and local writer/fanout work.
- Input/export Arrow kernel registry becomes pluggable.
  - ShardLoom impact: relevant to Arrow boundary import/export and OutputPlan, not to external
    engine fallback.

### Runtime Opportunity Review

- Statistic expression, stats rewrite session API, `NullCount`, and `UncompressedSize` aggregate
  functions.
  - ShardLoom impact: candidate inputs for metadata-first aggregate planning, claim evidence, and
    benchmark attribution.
- `register_splits` now returns both offset and relative row range.
  - ShardLoom impact: candidate input for SplitManifest evidence and scale-readiness planning.
- `VortexReadAt::read_at` result checking in the I/O driver.
  - ShardLoom impact: candidate input for Native I/O certificate hardening.
- FastLanes delta supports signed bases, SparseArray iterative execution, faster
  `Mask::from_slices`, and improved rank intersection.
  - ShardLoom impact: candidate inputs for encoded predicate, sparse/selection-vector, and
    prepared/native performance slices.
- `Executor::spawn_io` and local async file write behavior changes.
  - ShardLoom impact: candidate inputs for local Vortex lifecycle, future object-store admission,
    and split execution, but no runtime claim without ShardLoom evidence.

### Baseline-Only Or Not Currently Actionable

- DuckDB logger and DuckDB projection simplification are external integration changes.
  - ShardLoom impact: baseline/reference only. They must not become ShardLoom execution paths.
- C FFI Arrow-to-Vortex conversion is relevant to future ABI/interoperability but not a current
  user-facing Rust runtime path.
- CUDA/GPU FSST decompression and GPU fixes are optional future accelerator context, not required
  for current CPU-local runtime readiness.
- Upstream benchmark-server, website, Storybook, Java, and dependency-maintenance entries do not
  directly change ShardLoom runtime support.

## Dependabot Assessment

The current `.github/dependabot.yml` already enables weekly Cargo updates for `/`, so Dependabot can
open a dependency PR that changes `vortex = "0.70"` to `0.71.x` and refreshes `Cargo.lock`.

Dependabot cannot, by itself:

- interpret Vortex release notes into ShardLoom architecture implications,
- update `docs/architecture/vortex-public-api-inventory.md`,
- update dependency-footprint/license-provenance docs,
- decide which new upstream APIs are native-provider candidates,
- add phase-plan work,
- prove no-fallback/no-external-engine invariants,
- run benchmark or feature-gated ShardLoom-specific evidence checks beyond configured CI.

Because Vortex is still pre-1.0, `0.70 -> 0.71` is a semver-breaking minor update under Cargo's
caret rules. The dependency PR should therefore be treated as an upstream-intake PR, not as a
routine patch update.

## Required ShardLoom Follow-Through

The phase plan owns these follow-through slices:

- `GAR-VORTEX-071A`: release-note and API-delta inventory. Complete; moved to
  `docs/architecture/phased-execution-completed-ledger.md`.
- `GAR-VORTEX-071B`: feature-gated dependency bump and dependency-footprint proof. Complete once
  the matching PR lands; moved to the completed ledger in that PR.
- `GAR-VORTEX-071C`: runtime opportunity mapping into existing runtime slices.
- `GAR-VORTEX-071D`: Dependabot and release-intake workflow hardening.

No ShardLoom runtime support changes are approved until those slices attach evidence.
