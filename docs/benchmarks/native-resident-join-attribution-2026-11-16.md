<!-- SPDX-License-Identifier: Apache-2.0 -->

# Resident join attribution and variable-copy comparison

The complete frozen 403-request comparison retains the candidate at
`ffe0d077a676fd8b23cfebed9e11bf189e1ae459`. Local source, installed-package,
complete-result and portable acceptance pass; hosted integration is pending.
Engine time is **60.778284678 → 60.597904154 seconds**, a 0.180380524-second
gain, approximately 0.3%. This is a small single-observation difference,
not a material-performance or causal-speedup claim.

[Design and subsequent priority](../architecture/native-resident-join-attribution-2026-11-16.md)
· [Complete portable evidence](evidence/native-resident-join-attribution-2026-11-16/README.md)

## Frozen comparison

The baseline is the retained dictionary-selection runtime built at
`75596d0ee32e59ba77ea7cf4429166cb3052b3de`; its source fingerprint is
`c01bccc4c62d14b29c34741b6907cd00284feb74cc4b69555758e5c9eb58baa1`.
The candidate fingerprint is
`f6c5dbf3317a9b0099e19c3f93a8acaf460356fe85f5503f8e95a71f31584523`.
Both complete source snapshots are archived. Their 20 differences comprise
two production files, twelve test/instrumentation files, one opt-in registry
and five pre-existing retention-status documents. The candidate is a clean
fresh release build; the baseline is an accepted ancestor build, not an exact
build of the later merge commit.

Selection is frozen before timing from the existing request inventory:
197 join requests and 206 neighboring controls, covering 18 families,
five public interfaces and three formats. It includes 357 primary,
36 holdout, seven smoke and three scale cases. Existing fixtures contain
1M-row primary data and smaller/larger controls, including 5M-row scale data;
exact source identities and unchanged independent references are in the
protocol. Selection does not guarantee variable-copy activation in every
request. Binary, malformed-text, cancellation and ownership boundaries have
separate native tests.

Each runtime executes all 403 requests once, baseline first, with no warmup,
repeat, baseline-result reuse or artificial pacing. Both use two threads,
the same 2-GiB declared native memory grant and per-query deadlines. Active
supervision uses bounded file metadata/process checks; full recursive input,
source and evidence verification occurs before/after the queries. This is an
explicit change from earlier supervision, applied equally to both fresh runs.
Other task-owned work is idle during execution. Ordinary host/cache state is
uncontrolled. The recorded host is macOS 27 arm64, Rust 1.99 and Python 3.13.13.

All 806 complete packets match expected values, schemas, ordering and declared
float policies. Native execution, no fallback, declared resources, complete
consumption and zero cross-call source-cache hits pass offline replay. The
reported metric is the sum of native `engine_execution_seconds` only.

## Complete and subgroup results

| Population | Requests per runtime | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Complete cohort | 403 | 60.778284678 | 60.597904154 |
| Joins and multiway joins | 197 | 33.279858638 | 33.072162742 |
| Other neighboring requests | 206 | 27.498426040 | 27.525741412 |
| Arrow IPC | 130 | 10.680326049 | 10.586359579 |
| Parquet | 142 | 14.279806580 | 14.252307991 |
| Vortex | 131 | 35.818152049 | 35.759236584 |
| Python SQL | 88 | 11.374028259 | 11.321405751 |
| DataFrame | 74 | 15.368764293 | 15.350427124 |
| Typed relation | 76 | 9.637478166 | 9.588019079 |
| CLI SQL | 82 | 16.404372882 | 16.365162865 |
| Embedded workflow | 83 | 7.993641078 | 7.972889335 |

There are 186 individual increases totaling 0.215804699 engine seconds. The
nonjoin, holdout and smoke totals increase even though every format/interface
total decreases. Holdout is 0.574349042 → 0.583948874; smoke is
0.005212877 → 0.005221582; scale is 0.817003918 → 0.801176250. All per-request,
family, archetype, format, interface and profile results remain in the archive;
no subgroup is removed from the frozen decision.

## Diagnostic and verification evidence

The separately instrumented 54-request run supplies cost attribution, not a
production speed comparison. It validates 1,659 rows and observes 62 join
nodes, 3,826,977 build rows, 21,958,717 probe rows and 20,585,109 output rows.
Its 7.736501631 instrumented engine seconds include 2.491412065 in probe
input, 2.123419020 in gather/validation and 1.719474851 in matching/pairs.
Three debug-only gather observations classify actual payload copy work. Do
not extrapolate these fractions to all 403 requests or the full corpus.

The original diagnostic's one-ULP transport failure remains failed. Corrected
JSONL transport changes no result tolerance or production execution. All
historical build source bytes are reconstructable from archived variants;
24 historical source records lack original filesystem generation tuples,
which remain unavailable rather than inferred from present files.

The candidate passes formatting, strict default/native lint, all-feature
checking, 3,344 default and 7,308 native tests, plus all 455 clean installed-
wheel tests with zero failures/errors/skips. Forty-two native ignored tests
match the explicit opt-in/platform registry. Six new copy tests cover errors,
memory ownership/refunds, cancellation and complete output equivalence.

The inner packet contains 6,399 members and complete raw ledgers/references.
It independently reconstructs both source snapshots, all complete production
results, the corrected diagnostic and gather results, and the frozen retain
decision. Original and relocated replay pass without native execution or
original-path reads. It rejects 126 fresh-result corruptions and seven
decision-boundary controls; the exact archive transport rejects 19 further
corruptions. The earlier 126 verifier controls and six active-guard controls
remain separately preserved. Payloads for 440 native/input/environment/source-
archive/wheel identities are excluded from portable transport; every source
member required for source proof is included.

Run `python3 -I -B packet/verify.py` from the evidence directory using Python
3.13 or later. The companion README gives the separate transport-control
command. Replay executes no workload queries. Evidence packaging, failed
attempts and subsequent verification have separate identities.

## Decision and later direction

The original complete-cohort rule retains a lower total after correctness,
resource and no-fallback acceptance. The candidate passes that rule; no
percentage threshold was added after measurement. This does not change the
earlier propagation, dictionary-selection or join-payload-mapping decisions.

The subsequent November 16 maintainer clarification requires **material
potential when choosing new optimizations**, rather than more work whose
best plausible outcome is a sliver gain. Useful existing work may stay, and
a smaller-than-expected realized gain alone does not force a drop. Prioritize
join input/intermediate volume, repeated scans/builds, expensive sorting and
spill only where actual cost and removable work support the investigation.
No whole-engine coverage, superiority, CG closure or package release follows
from this finite result.
