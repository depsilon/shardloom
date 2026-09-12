# Performance control progression

Status: active control policy and evidence ledger, updated September 12. The
maintainer clarified that recorded ingest and query controls advance as faster
versions are completed. Avoiding unnecessary unchanged-control runs does not
make the roughly 95-second ingest version a permanent target.

## Promotion rule

1. At each completed, validated retained version, record the immutable source,
   native binary and build profile; workload and artifact identities; actual
   resource configuration; complete-result proof; and every timing sample.
2. Advance the applicable ingest and query controls to the newly completed faster
   version. Start subsequent candidates from the latest retained implementation.
   Ingest and query records progress separately when their acceptance completes
   separately; a faster ingest does not supply an unmeasured query score.
3. Preserve previous controls, slower candidates and failed observations as
   history. Keep the established scoring definition: Full43 best-of-three sum,
   hot total and all 129 executions are separate measures. A single best ingest
   observation is not a stable performance distribution.
4. Reuse existing unchanged-control evidence when it answers the comparison.
   Repeat a control only for a useful matched comparison or an explicit proof
   gap, not to keep an old number current or populate another table.
5. Freeze identities and settings within each comparison. A changed physical
   representation requires complete value/schema/order, statistics and query
   acceptance. Do not transfer a completed revision's measurements to a later
   combined binary before its own required acceptance is complete.

The machine-local clarification is preserved in
`/Users/dylan/LocalData/shardloom/perf-all-20260906/performance-control-progression-20260912.md`.
Historical packets retain the policy and completion status recorded at their
checkpoint; this ledger governs subsequent work.

## Ingest ledger

| Revision and role | Recorded complete ingest | Identity and acceptance scope |
| --- | --- | --- |
| Previous accepted native runtime `572bd52c`, merged native code `d51429e3` | 95.447305458 s | 18,591,586,804 bytes; complete native values compared; [matched-owner packet](../benchmarks/retained-ingest-owner4-2026-09-08.md). Preserve as the previous control. |
| Latest retained numeric ingest implementation `6bc73e8de6bfe713ff6926d90374272291d40451` | 90.303309291 s and 93.945037458 s | Two completed isolated candidate observations, identical native artifact bytes to `572bd52c`; six focused tests; [numeric probe packet](../benchmarks/ingest-numeric-probe-2026-09-12.md). Carry both samples as the retained ingest revision's recorded reference. |
| Combined continuation `2ad143da` | No completed full-size ingest measurement recorded here | Includes retained numeric work and subsequent runtime changes. Final combined acceptance is in progress; neither numeric observation is a measurement of this binary. |
| Storage candidate and scoped owned-result improvement | Full-workload promotion pending | The owned-result screen below is complete; bounded fixture results do not establish a new ingest or Full43 control. |

The numeric measurements are both below the previous recorded ingest, but vary
across runs and uncontrolled host/cache state. They do not establish a stable
5.39% gain or a 90.303309-second distribution. The maintainer retained this
implementation; the 95.447305-second observation remains history rather than a
permanent screen target. Complete the combined version's outstanding acceptance
and link its exact records before describing it as a completed combined control.

These ingest records use the immutable 99,997,497-row, 112-column Parquet input,
SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`,
P4 and a 24 GiB request. The numeric and previous accepted outputs share SHA-256
`7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`.
The ordinary release-user-surfaces build uses no PGO; exact binary, toolchain,
CPU-owner, reservation and observed RSS evidence remains in the linked packets.

The 104.044137-second unchanged-runtime observation and rejected
118.604707-second allocation remain in the
[stage-balance packet](../benchmarks/ingest-stage-balance-2026-09-12.md).
Neither overwrites a previous sample or promotes a slower allocation.

## Query ledger by artifact profile

The latest completed retained Full43 runtime recorded here is `572bd52c`,
release-user-surfaces binary SHA-256
`9251e10babcfc235b984fd256bc67a123b0b4126b13b375b253b55558a8eef9e`.
Both profiles request P12 and 24 GiB on arm64 macOS; every query uses a fresh
process with uncontrolled OS cache. Timing ends after complete CLI output and
process exit. These records are complete-result acceptance, not a paired claim
of stable throughput improvement.

| Artifact profile and packet | Best-of-three sum | Hot total | All 129 executions | Complete results |
| --- | ---: | ---: | ---: | ---: |
| Protected 18,643,482,956-byte artifact, `full43_20260908T191423419110Z`; [retained runtime](../benchmarks/retained-runtime-acceptance-2026-09-08.md) | 91.215296 s | 91.292520 s | 278.782517 s | 129/129 |
| Fresh 18,591,586,804-byte artifact, `full43_20260912T113159011731Z`; [fresh-artifact acceptance](../benchmarks/ingest-stage-balance-2026-09-12.md#newly-written-artifact-complete-values-and-full43) | 102.485398 s | 103.076700 s | 313.564493 s | 129/129 |

The protected artifact SHA-256 is
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`;
the fresh artifact has the numeric ingest output identity above. SQL SHA-256 is
`4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`.
Best-of-three sums each query's fastest sample; hot total sums each query's
fastest of runs two and three. Keep these profiles separate when choosing the
next comparison: different artifact bytes and dates prevent substituting one
score for the other. No `6bc73e8d` or `2ad143da` Full43 run is claimed here.

Once a faster completed version passes the corresponding profile's acceptance,
advance that query control and retain these rows as history. Changed-output
storage candidates need their own full lifecycle proof before that promotion.

## Completed owned-result screen, separate from Full43

Frozen `2ad143dae444d3027fb07f11e5b605e991e79b61` completed 184 native
executions against an independent complete-result oracle. The ordinary release
screen compares the existing prepared grouped integer COUNT DISTINCT API's JSON
report with its owned columns on 262,144 rows and 32,768 groups. Each arm has
three warmups and 20 measured samples per case, with balanced execution order
and fresh aggregate state. The clock covers query/result/certificate construction
plus returned-result drop; preparation, validation, transport and persistence
are excluded.

| Returned rows K | Requested P | JSON median | Owned median | Scope |
| ---: | ---: | ---: | ---: | --- |
| 32 | 1 | 14.101834 ms | 14.017875 ms | About 0.6% lower; no material small-result gain established |
| 32 | 4 | 9.182500 ms | 9.063541 ms | About 1.3% lower; owned p95 is slightly worse |
| 32,768 | 1 | 47.983000 ms | 16.429750 ms | 2.92x median speedup for this result path |
| 32,768 | 4 | 44.124000 ms | 11.670500 ms | 3.78x median speedup for this result path |

The full guarded screen took 4.659313 seconds, with 143,409,152-byte observed
process RSS including fixture/oracle/harness work. This is not per-arm RSS.
Shared reservation credits return to zero after every result. Complete values,
ordering and native certificates pass with no external engine or answer cache.
Raw samples and build identities are retained in
`/Users/dylan/LocalData/shardloom/perf-all-20260906/owned-result-cost-20260912/receipt.json`.
This completed scoped result does not advance either Full43 artifact profile or
provide an ingest timing for the combined binary.
