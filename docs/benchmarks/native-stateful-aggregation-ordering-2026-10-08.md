# Native aggregation and streamed ordering acceptance

Status: complete local engine acceptance and independent portable-packet
inspection pass for runtime `8a207745a55fb5678cd2b56866effb6fb0eb9352`.
Affected support pages, native examples and local browser checks also pass.
Hosted integration completed in
[PR #1531](https://github.com/depsilon/shardloom/pull/1531) at `9529bd78`; the
[hosted receipt](evidence/native-stateful-hosted-2026-10-08.json) records all 39
checks and the preserved accepted runtime. This report retains the original
local measurements and v0.4.0-era checkpoint; subsequent capability and release
status follow the
[remaining local workflow scope](../architecture/native-local-completion-scope-2026-10-07.md).

## Accepted behavior

One finite, single-use `streaming=True` source now executes through the complete
shared Scan/Filter/Project/Sort/Limit/Aggregate tree. Global limits and offsets
drain and validate the producer, including zero limits. Retaining operators and
delivered results own credited native payload independently of the input batch.
Incremental results, bounded collection and one new Vortex destination preserve
the existing completion and publication contract.

An explicit spill policy admits general relational aggregation through the
existing stable native ordering, run store and exact scalar reducers. Grouping
and COUNT DISTINCT no longer require a resident entry per distinct key/value
under that strategy. First-seen group order, null behavior, logical types and
ordered floating reductions remain intact. Without the policy, the resident
strategy and deterministic memory denial remain available. All nested stages
share the same memory grant and spill quota.

The [ordering design](../architecture/native-streamed-ordering-2026-10-07.md),
[aggregate design](../architecture/native-aggregate-pressure-2026-10-07.md) and
[public spill example](../reference/native-query-spill.md#general-aggregation-and-completion-aware-input)
describe admission, reuse, ownership and failures. File-backed aggregates retain
all eight admitted writers; this does not admit compatibility writers for
streamed batch input. There is no external query-engine execution.

## Constrained-state proof

Both aggregate cases consume 133,137 rows and 70,330,932 original logical input
bytes, exceeding four times the 16 MiB native grant. The constrained resident
control denies; the native spill strategy and separate 256 MiB resident control
complete and compare every output value. The preserved focused observations are:

| Native case | Complete output rows | Spill peak reserved bytes | Runs / merges | Peak disk bytes |
| --- | ---: | ---: | ---: | ---: |
| High-cardinality groups | 133,137 | 9,090,433 | 676 / 337 | 289,792,624 |
| One group with high-cardinality DISTINCT | 1 | 9,257,041 | 259 / 129 | 158,249,316 |

Both verify quota enforcement, completed owned cleanup and return to the
reservation baseline. The final full native suite reruns the identical
pressure-test sources; the table's counters come from the retained focused run.
Streamed ordering separately completes 66,567 rows whose logical input exceeds
four times its 8 MiB native grant, with resident denial, a 128 MiB ample control,
multiple real spill merges and every output value checked.

The file-backed eight-writer aggregate proof uses a 64 MiB grant to accommodate
its reader/writer overlap. Its earlier smaller-grant denial is preserved.
Public streaming cases use a separate 1 GiB grant and do not establish a
larger-than-grant result. Native reservations are not process RSS, and this unit
does not claim complete coverage of all provider allocations.

## Complete workflow and regression proof

| Check | Result |
| --- | --- |
| Public finite streaming suite | 109 cases; 94 raw envelopes, 61 small-value files and ten malformed protocol traces reopened |
| Saved public pressure output | All 248,846 rows reopened and independently reconstructed across eight resident/spill/write captures |
| Existing public relational portfolio | 27,373 cases and 15,820,181 complete row comparisons across five materializations; all 54,733 raw envelopes reopened |
| Direct unary portfolio | 202 cases and 131,734 complete rows; 367 raw envelopes reopened |
| Resident batch adapters / format fidelity | 48 / 19 checks, including independent compatibility-format readback |
| Admitted semantics / golden workflows | 145 / 9 stages |
| Retained full dataset | All 43 queries run three times; every complete result matches the preexisting reference |
| Source and build gates | Fourteen gates pass, including required workspace format/lint/tests, native CLI/Vortex tests and lint, examples, feature-light and MSRV checks, and 615 Python tests |

Native and public tests cover empty/null/typed/nested groups and values,
COUNT/COUNT DISTINCT/SUM/AVG/MIN/MAX, alias sharing, first-seen order,
cross-batch distinct values, nested aggregate/order/limit composition, input
release and retained output. Failures cover late producer/schema/value errors,
zero limits, cancellation, exhausted grants/quota, actual corrupt runs, failed
consumers, source mutation, publication and verified abandoned-run cleanup.
Unknown files remain protected. Cleanup and restart are proven; execution resume
and automatic recovery of crashed output staging are not claimed.

Required workspace commands pass with the frozen implementation:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

The packet retains exact offline/locked commands and every gate receipt. After
an obsolete CLI test expected explicit streaming limits to be denied, the test
was corrected to verify complete drain and late failures. Only that CLI test
file changed between the two gate attempts. Four unaffected gates were reused
with exact asset and feature-gating proof; all CLI checks and subsequent gates
passed on the corrected source. No dependency audit rerun is claimed: all 23
manifest, lockfile and vendored assets match the prior accepted build.

The full-dataset observation uses the unchanged, fully resident
15,682,956,489-byte Vortex artifact, 24 GiB policy and maximum parallelism 12.
The sum of per-query minima is 70.113672 seconds, medians 71.381934 seconds and
all 129 calls 216.147038 seconds. These are complete-process correctness
observations with uncontrolled ordinary host activity and a prehashed source;
they support no comparative speedup or RSS enforcement claim. An earlier long
host-observation gap during the public regression is retained separately and
is not represented as uninterrupted timing evidence.

Full43 preflight initially refused accumulated log storage before queries.
The 516 call artifacts from one completed historical cohort were archived and
verified before removing redundant originals, recovering 3,362,816 accounted
bytes. Failed/incomplete observations and storage limits were unchanged. Fresh
preflight and Full43 then passed. Finalization independently reopened all 516
archive members; original development failures and corrections remain recorded.

## Frozen and portable evidence

- [Acceptance index](evidence/native-stateful-aggregation-ordering-2026-10-08.json)
- [Complete portable packet](evidence/native-stateful-aggregation-ordering-2026-10-08.json.xz)
- [Independent stream inspection](evidence/native-stateful-aggregation-ordering-2026-10-08-inspection.json)

The frozen release CLI SHA-256 is
`3d061ee4fe8cf3aea8c8ce9bc35a800a0ec0d440cd5a4798c2a09700b8bbfe26`.
All 964 source assets match runtime commit `8a207745`; their aggregate identity is
`68a8dd2de06778c1f0b4b5e99f96b9373a1e7e49b44901fdd37f1875fb03348d`.
The packet is 44,394,172 bytes, SHA-256
`2b367aea82d7b31a76b2e026d039f929c4c547deccbb47a174f22642ff5d88ef`.
Its complete 3,504,653,974 decompressed bytes are separately hashed and inspected.
The independent inspector passes 165 positive/negative contract cases.
Executable and large data payloads are represented by exact identities and
complete-value proof rather than embedded copies.

Support alignment subsequently corrects only the `from_batches` Python
docstring in one frozen source file. The other 963 assets remain byte-identical;
whole-module AST comparison after replacing only that named docstring proves no
executable change. Documentation verification records that distinction rather
than claiming all current source bytes still match the earlier snapshot.

The [support receipt](evidence/native-stateful-support-2026-10-08.json) and
[portable support archive](evidence/native-stateful-support-2026-10-08.tar.xz)
retain seven passing documentation/site checks and four complete native examples,
including streamed aggregation, descending order and a draining limit under an
explicit spill policy. Desktop/mobile rendering, local navigation and Pagefind
search pass. The original missing scope-matrix rows and subsequent correction of
stale aggregate-spill wording remain recorded; no validator was weakened.

Join/window/pivot pressure, repeated-source spooling, wider typed intake,
compatibility streaming destinations/fanout, remaining allocation coverage,
broader platform runtime acceptance and the eight conditional investigations
retain their existing owners. No version bump or broader PERF/CG closure follows
from this ordering and aggregation unit.
