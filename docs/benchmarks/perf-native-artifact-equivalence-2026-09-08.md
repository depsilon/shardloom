# Complete native artifact equivalence

The September 8 one-worker ingest candidate at runtime `48182c5a` produces
different physical bytes from the retained artifact, but **all 99,997,497 rows
and all 112 columns match exactly**. The complete comparison covers
11,199,719,664 logical values in source order. It also requires identical native
field names, order, types and nullability. This is artifact-equivalence evidence,
not a new query-performance result, independent execution oracle, or topology
acceptance packet.

## Physical difference and complete proof

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| Retained `perf-current-c71a558e.vortex` | 18,643,482,956 | `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2` |
| Candidate `48182c5a`, requested one worker | 18,644,193,828 | `679d3343ef64572a61987291f0012bccbf3677e88a84482687576576a8eab758` |

The initial ingest matrix stopped when hashes differed and preserved the
candidate. A separate read-only native comparator then checked every value,
without sampling, row dictionaries, aggregate surrogates or integer-to-float
conversion. It ignores encoding, dictionary codes, batch boundaries and null
payload bytes; valid floating values retain exact bits, including signed zero
and NaN payloads. Arrow conversion is an explicit validation boundary and is
not used as a query execution fallback.

Each source opened once and completed one held-generation execution. Both
descriptor and path generations were validated around the entire comparison.
Prior complete-file hash receipts were re-admitted by unchanged current
generations. This is not an atomic filesystem snapshot, and physical layout
statistics/user metadata are outside logical-value equality.

The comparison completed in 213.513916 seconds with an observed process peak
of 675,856,384 bytes. Its bounded task queues peaked at two native split futures
per source. Each native allocator had a 512 MiB limit; observed peak reservations
were 60,252,848 and 30,422,556 bytes. A separate conservative 256 MiB Arrow work
grant covered conversion; the largest retained Arrow batch was 5,923,739 bytes.
All reported reservations returned to zero. These grants exclude provider
allocations bypassing its host allocator and are not process-RSS limits.

The helper uses owned ordered futures on the existing native runtime, avoiding
the provider's host-core concurrency multiplier. Dropping an iterator destroys
those futures synchronously; it does not leave separately spawned split tasks
behind an early validation error. Six native tests cover exact primitive bits,
dictionary/direct and chunk differences, empty/schema/row mismatches, source
replacement, bounds and requested concurrency 1/2/4.

## Ingest measurement scope

Only two of eight initial curve observations have completed:

| Runtime | Requested workers | Public ceiling | Known configured CPU owners | Native seconds | Peak RSS bytes |
|---|---:|---:|---:|---:|---:|
| `75fc09a0` control | 1 | 2 | unavailable | 95.837785 | 3,051,814,912 |
| `48182c5a` candidate | 1 | 1 | 1 | 187.600824 | 1,972,207,616 |

These are not matched CPU grants and do not establish a speedup or regression
at equal resources. The old source-executor parallelism field does not count
background threads. One observation per setting does not establish a stable
curve; requested 2/4/8 and informative repetitions remain pending. Hashing and
complete artifact comparison occur outside ingest clocks.

After complete equality and another generation check, only the exact
runner-owned candidate was retired to restore the guarded workspace headroom.
The retained artifact, original source and unrelated artifacts remain available.
Raw failed and successful receipts are preserved.

## Reproduction and receipts

The [JSON packet](perf-native-artifact-equivalence-2026-09-08.json) records the
exact invocation, binary hash, source generations, counts, memory scope and
guard outcomes. The comparison executable hash is
`8673cd00615129265475404ad4b0e26b7c1850801effb95ac6a8f9f68bc21d25`.
It was built from the integrated validation helper atop `48182c5a`, not from a
new frozen query candidate.

Build the `compare_native_artifacts` example in `shardloom-vortex` with
`release-user-surfaces`, then compare the two immutable local Vortex paths with
`--batch-rows 8192 --right-batch-rows 8192 --parallelism-per-source 2`. Use the
repository's local storage, residency, exclusive-workspace and process guards
for production-size comparison. The helper does not generate another artifact.

Local complete receipts are under `perf-all-20260906`:
`native-artifact-comparison-binary-20260908.json`,
`native-artifact-comparison-tests-5.log`,
`complete-native-comparison-20260908.log`,
`ingest-candidate-p1-retirement-20260908.json`, and
`ingest-scaling-20260908T083440299333Z/matrix.json`.
The detailed comparison lives in the sibling UAT workspace's
`logs/native_artifact_comparison_20260908T091802419850Z/manifest.json`.
