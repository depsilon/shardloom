<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native analytic frames: local core acceptance

The frozen local acceptance records 22,658 complete public workflow checks covering 15,349,350 row comparisons. The analytic-frame family contributes 2,213 checks and 1,067,280 row comparisons: 2,159 complete value/resource proofs and 54 explicit denials. A separate retained-workflow matrix records 202 checks and 131,734 rows. Full43 completes all 129 runs (43 queries × 3 runs) through the native family. These are correctness and availability observations; no comparative performance claim is allowed.

The accepted implementation extends the existing shared Vortex-native window path with framed COUNT, COUNT DISTINCT, SUM, AVG, MIN, MAX, FIRST_VALUE, LAST_VALUE and NTH_VALUE. It does not introduce an alternate execution mode or external-engine fallback. The implementation contract is [Native analytic frames](../architecture/native-analytic-frames-2026-10-05.md); the preceding engine acceptance remains in [native typed reductions](native-typed-reductions-full43-2026-10-05.md).

## Frozen identities

| Artifact | Identity |
| --- | --- |
| Runtime source commit | `22f1e6ba5a5ca7855a85862e89612fe8fbc5e06b` |
| Runtime source tree | `cef9f83b68a8a81579bb513bed82d606deb5e981` |
| Native executable SHA-256 | `fd46e887249c6b6eeba7fb7ed26f6beb10149f785461d277e49b3338a296f5f0` |
| Source-check manifest SHA-256 | `c13b62ecdc317d1ea12184f5a505e72653b8a24f7d17354c3be2acec9f0add9f` |
| Acceptance packet | [native analytic-frame packet](evidence/native-analytic-frames-2026-10-05.json.xz) |
| Packet compressed size / SHA-256 | 40,779,540 bytes / `16b59c7b4ceb59d8e1d479ebddd7e8e5aeb97eea93df83724ecf8d5d6840f463` |
| Packet uncompressed size / SHA-256 | 3,187,609,675 bytes / `590ad7168242ebaf9d774b217a8c85031d02a07cc1322b4e71fe911b7f929bf9` |

The source-check manifest records 22 passing gates: `fmt`, `default-clippy`, `default-tests`, `native-clippy`, `native-vortex-tests`, `native-cli-tests`, `python-tests`, `native-without-write`, `lean-workspace`, `msrv-lean`, `msrv-native`, `uat-harness-tests`, `storage-guard-tests`, `uat-consumer-tests`, `suite-harness-tests`, `contribution-governance`, `ci-gate-matrix`, `api-schema`, `user-surface-reference`, `public-status-docs`, `docs-productization`, and `front-door-scope`. The packet retains exact gate commands, receipts and hashed logs. Test totals across configurations overlap and are not a unique-test count.

The frozen input summaries are:

| Family | Summary SHA-256 |
| --- | --- |
| Focused frames | `e142ae8f3224af9bc2d0e38750838de76b93406992f691cdeb3b7e2104852ab5` |
| Complete public workflows | `46f69ea23627aa7822d82d96e5dcf0ccd8e1e0797e05542f230768700d6f6db6` |
| Direct retained workflows | `5ad597819dd7840296cddf9ca831359863ba68f66a083f75d0a021f8645391c9` |
| Full43 | `d6ab8b4da92d683e37c9475e3d2fefa9d112485b58fb3498553515db66c92ed0` |

The final core receipt reports 46,884 public raw envelopes, 145 admitted-semantics stages and nine golden workflow stages. The packet retains 893 frozen source assets, independent frame oracles, original envelopes and complete outputs. A separate streaming parser independently verifies its identities, case counts, resource evidence, native-family reports and no-fallback fields; the [inspection receipt](evidence/native-analytic-frames-inspection-2026-10-05.json) records that pass.

## Public workflows and regression

The public complete-workflow total is 22,658 checks / 15,349,350 rows. It covers the accepted public SQL, Python/DataFrame and CLI surfaces and includes complete results, explicit denials and representable output boundaries. The frame subset contributes 2,213 / 1,067,280, including 2,159 complete value/resource proofs and 54 denials. The direct retained-workflow matrix is a separate 202 / 131,734 cross-check, including dynamic-schema binding and repeated declaration reuse.

Full43 runs each of 43 queries three times through the public SQL path; all 129 complete results match the retained native regression reference. That reference is not an independent correctness oracle. The run is regression evidence only, not a speedup, engine-superiority or Spark-replacement claim. The modular workload UAT records were produced with a prior binary and are not recertified by this acceptance.

## Frame contract and limits

The admitted units are ROWS, GROUPS and RANGE, with explicit bounds, empty intervals and CURRENT ROW/GROUP/TIES/NO OTHERS exclusions. The omitted frame defaults to RANGE UNBOUNDED PRECEDING through CURRENT ROW. GROUPS requires ORDER BY. A bounded RANGE frame requires exactly one ordering key. ROWS and GROUPS offsets are nonnegative integer literals counting rows or peer groups and must fit an addressable row count. RANGE offsets must match the order domain: integer offsets for integer keys, finite nonnegative numeric offsets for floating keys, nonnegative scaled decimal offsets for decimal keys, whole-day durations for Date32, or fixed microseconds for TimestampMicros. Bind-time checks reject reversed bounds, invalid unbounded endpoints, unsupported offset forms and incompatible range domains. NTH_VALUE positions must be positive and no larger than the admitted input-row limit.

ROWS offsets count ordered rows; GROUPS offsets count peer groups; RANGE current-row bounds include peers. With no ordering all rows in a partition are peers. Original row ordinals provide stable positional selection and results return in input order. FIRST/LAST/NTH_VALUE respect NULLs; empty frames and out-of-range NTH_VALUE return typed NULL. COUNT(*) counts frame rows, COUNT(argument) ignores parent NULLs, COUNT DISTINCT uses the existing exact native key domain, and empty SUM/AVG/MIN/MAX return typed NULL.

Named windows, variable offsets, calendar-month intervals and IGNORE NULLS remain outside this contract and require separately declared semantics. Ranking and LAG/LEAD navigation retain their prior behavior; a frame does not change them. General window-state spill remains unimplemented in this unit and must fail explicitly when the current resource grant is insufficient. Bounded output batches do not establish bounded operator state, zero decode, or an RSS guarantee. The wider universal-workflow, adapter and general spill queues remain open.

## Resource and evidence boundaries

Native builds, tests and acceptance run sequentially under the existing process and storage guards. Public frame calls use the frozen 1-GiB resource policy, with 4 GiB for explicit format conversion, a 3,000-second family deadline, 12-GiB free-space headroom and unchanged workspace/log ceilings. Full43 retains its 24-GiB policy and 12-worker maximum. The independent inspection checks all 2,159 frame resource proofs against their declared budgets; these reservations are not a total-process RSS guarantee.

The packet preserves failed and interrupted development and public observations, original frozen references, exact-source retention decisions and verified log compaction. Completed logs were archived with per-member identity and byte verification; failed observations and storage limits remain unchanged. Incomplete cohorts are not combined to claim a complete acceptance.

The first independent inspection failed because its suffix predicate incorrectly classified ordinary native fields as fallback fields. A minimal fixture reproduced the checker error. The repaired predicate passes 96 policy-field cases and four ordinary-field cases, then passes a fresh inspection of the identical packet. Both inspection attempts, the original and repaired queries, and the focused tests remain in the inspection receipt.

Core-local evidence is recorded as `passed_core_local`. Hosted checks and review remain pending. No release publication, competitive gate completion, production certification or performance claim follows from this report. CG-1 through CG-23 retain their independent obligations, and real Vortex payload proof remains distinct from placeholder artifact status.
