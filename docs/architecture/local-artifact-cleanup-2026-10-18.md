# Local artifact cleanup — October 18, 2026

The requested cleanup retires completed compiler outputs and the superseded
large Vortex output while preserving source, current execution artifacts and
compact result/failure evidence. Storage limits and execution guards are unchanged.

| Retired scope | Accounted bytes | Verification |
| --- | ---: | --- |
| Completed Cargo debug cache | 26,352,421,883 regular-file bytes | Exact member manifest, two closed-handle checks, unchanged source/release cache/frozen artifacts/datasets |
| Superseded `fresh-text-aggregation-588-a1.vortex` | 15,713,610,513 | Full hash and stable resident generation, closed handles, byte-identical accepted replacement |

The cache retirement increases observed free space by
25,669,435,392 bytes. After fresh UAT acceptance, the
Vortex retirement increases observed free space by
15,714,283,520 bytes. These are separate observations;
intervening builds and UAT writes mean they are not one before/after measurement.

The resident original Parquet and latest accepted `fresh-roadmap-663-a1.vortex`
remain available. The current release cache and frozen merged-source executables
are retained. Historical evidence paths keep their original identities; retired
payloads require regeneration. No CloudDocs internals, cloud placeholders or
unrelated user files are changed.

The [fresh-UAT packet](../benchmarks/evidence/native-roadmap-fresh-uat-2026-10-18.json)
contains both retirement receipts, exact member/generation manifests and the
scoped cleanup helpers. The [acceptance report](../benchmarks/native-roadmap-fresh-uat-2026-10-18.md)
records the complete replacement validation before the old output was removed.
