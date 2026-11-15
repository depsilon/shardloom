<!-- SPDX-License-Identifier: Apache-2.0 -->

# Necessary text-signature comparison — November 14, 2026

Decision: **drop**. Complete fresh engine time increases from 682.167470629 to
692.796513379 seconds. Separate complete lifecycle engine time decreases from
0.520543159 to 0.399726140 seconds. The prospective rule requires both totals to
decrease after correctness, ownership, resources and no-fallback checks pass.
It remains unchanged. The [architecture disposition](../architecture/native-text-signatures-2026-11-14.md)
records source restoration and the continuing queue.

## Runtime and population

The baseline is the accepted source-domain C `runtime-r4`; the candidate is
text `runtime-r3`, built from accepted parent
`0e6d15a658c060bd11e9a1001a71515f69f2064e` plus the preserved candidate changes.
The candidate source fingerprint is
`57164ef0b47a70f7e4e5f51bfc0e346d1389c19968751bd39f744ae6cbb82ef0`.
Its exact source is preserved at
`0c4aee0b7e7c97f73598482d2b4b69b4b0f5c531`. This preservation commit is not
misrepresented as the build's original revision. Both runtimes use the pinned
Vortex 0.93.0 providers and ordinary ShardLoom-native execution.

Each runtime has 9,040 fresh declarations: 4,150 original text/consumer controls,
1,380 dictionary declarations and 3,510 new text-summary declarations. The new
matrix is two sizes × nine profiles × three encodings × thirteen consumers ×
five user surfaces. Profiles are selective, unclustered, dense, absent,
saturated, all-null, short, empty and missing-summary. Encodings are flat,
dictionary and actual FSST. Public fixtures have 67 rows and work fixtures
8,195, except empty fixtures; text zone sizes are 8 and 1,024, while identifier
zones deliberately use 7 and 1,023. Complete literal populations provide
independent expected values and schemas.

The surfaces are CLI SQL, Python SQL, DataFrame, typed relation and embedded
workflow. Consumer forms include positive substring, Unicode, escaped, short,
empty, negated, folded, count, group, materialize, shared, barrier and unfiltered
operations. New baseline files use the plain layout and candidate files use
explicit summaries; logical cells are identical. Inherited cases use unchanged
physical inputs. All 108 plain/summary files have full independent readbacks.

The separately frozen lifecycle population contains public shared and work
grouped-count plans across all profiles, encodings and surfaces: 270 plans per
runtime, with two counted calls each. These 540 operations per runtime are not
folded into the fresh total. Returned values, schema, generation identity,
prepared-owner release and session cleanup are checked.

Execution is sequential under the standard storage, process, deadline and
concurrency guards: two native threads, 2 GiB native admission, a 2.5 GiB
process-group RSS ceiling and 120 seconds per request. Other task work is idle
during queries. Source/input/evidence inventories run before and after query
execution. Cache state is uncontrolled. There are no warmups, successful-request
repeats, cache manipulations or artificial pacing. Hardware and supervision
observations remain in the original records. Only `engine_execution_seconds`
is used for these comparisons.

## Complete engine results

| Fresh population, per runtime | Cases | Baseline seconds | Candidate seconds | Candidate minus baseline |
| --- | ---: | ---: | ---: | ---: |
| Original declarations | 4,150 | 675.799882008 | 687.030864785 | +11.230982777 |
| Dictionary declarations | 1,380 | 2.208568271 | 2.222441254 | +0.013872983 |
| New text-signature declarations | 3,510 | 4.159020350 | 3.543207340 | −0.615813010 |
| Complete fresh total | 9,040 | 682.167470629 | 692.796513379 | +10.629042750 |

All 18,080 fresh results pass; all 1,080 lifecycle operations pass. The complete
lifecycle totals are 0.520543159 and 0.399726140 engine seconds. All 840 required
logical exclusion witnesses pass. All 4,581 individual fresh regressions are
retained; their positive contributions sum to 18.451585205 engine seconds.
Every subgroup and individual observation remains in the full replay artifact.
The favorable new-text and lifecycle results do not override the complete fresh
increase. This is one comparison, not a Full43, full10k or causal speedup claim.

## Read and construction evidence

The separate direct-provider diagnostic records exact surviving IDs, logical
summary tests, requested segment IDs and completed positional ranges for six
patterns per fixture. All 648 scans and 108 complete readbacks pass. Across 324
plain/summary pairs, 36 read fewer completed positional bytes, 103 the same and
185 more. Work-size selective examples are 479,675 → 137,087 bytes for flat,
185,647 → 97,247 for dictionary and 238,075 → 104,759 for FSST. Public fixtures
can be prefetched whole, so logical exclusions do not guarantee fewer reads.
These are diagnostic provider scans, not additional scored query observations,
public-operation byte totals, physical-device reads or decoded-byte counters.

Construction executes once per each of the 108 files and reproduces the scored
fixtures byte-for-byte. Plain payloads total 30,825,376 bytes; summary payloads
31,235,608. The recorded provider construction clock covers strategy setup,
push and finish within an already registered session. It excludes session and
runtime setup, registration, resource admission, input generation/encoding,
open/sync, validation and reporting. It is not complete first-use construction
or public ingest cost. No first-use break-even result is claimed.

## Recovery and unchanged acceptance

The first baseline attempt reuses 336 previously accepted, fully replayed
source-domain observations and captures 4,295 new successes. Sixteen older
literal-helper requests fail before collection. Interruption leaves one started
request with worker output but without a persisted enclosing supervision packet,
and 4,392 requests not started. That incomplete stage has no accepted total.

All 4,631 complete successes are reused byte-for-byte after full verification.
The recovery performs the remaining 4,409 requests: sixteen first collections,
one justified retry of the unaccepted interrupted capture, and 4,392 first
executions. An intervening empty stale-lock refusal occurs before any query.
The verified removal affects only that empty task-owned directory. The next
launch keeps all source/input/reference/resource/acceptance fields unchanged.
The protocol freezer's tuple/list assertion failure is preserved and the written
protocol independently validated. No successful observation is discarded or
repeated. All original partial ledgers, raw worker output and failure receipts
remain in the packet.

## Verification and reproducibility

The [portable evidence packet](evidence/native-text-signatures-2026-11-14/README.md)
contains complete raw result ledgers, independent references, source snapshots,
build and validation commands/logs, runtime/input identities, recovery proofs,
the exact candidate patch and every earlier failure. Large generated fixtures,
native runtime binaries and environment payloads are explicit identity-only
artifacts; complete fixture readbacks and checked-in source fixtures are bundled.

Run from the checkout with Python 3.13 or newer:

```sh
python3 -I -B docs/benchmarks/evidence/native-text-signatures-2026-11-14/verify.py
```

This checks all packet/supplement hashes, replays every saved result, schema,
resource/ownership certificate and lifecycle, verifies construction/read and
recovery provenance, and recomputes the frozen decline without querying an
engine. Relocated replay forbids original source/runtime/evidence reads, network
and subprocesses. All 28 saved-evidence and 61 packet corruption controls pass.
Two missing shared verification modules were supplied through their already
frozen accepted prior packet identities. Both failed packaging attempts remain
bound; all thirteen original archive parts, measurements and rules are unchanged.

Candidate validation passes format, strict default/native lint, 3,344 default
tests, 7,275 native tests with 39 declared ignores, 5,779 provider tests with one
declared ignore, feature/MSRV checks, 457 installed-package tests with no skips,
and all 21 new regressions. The restored source matches the accepted C runtime
outside documentation, so the decline introduces no new production engine code.

The finite text candidate is closed; the predicate-summary parent remains open.
The [survivor admission screen](../architecture/native-survivor-admission-2026-11-15.md)
declines unconditional callback batching before performance. Broader I
attribution and narrower batching remain open; the existing batch contract
remains in force for FSST. Required item-19 spatial execution, optional spatial
summaries, codec choices
and bounded CPU fusion retain their own gates. Item 20, RFC 0044 and CG-1 through
CG-23 remain open where recorded. This local result does not publish or merge.
