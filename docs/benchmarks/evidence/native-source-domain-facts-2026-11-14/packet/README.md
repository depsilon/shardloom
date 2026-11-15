# Source-fact domain experiment

Decision: **retain**. Run `python3 -I -B verify.py` with Python 3.13 or newer.
Keep every archive part and the manifest together. Verification checks transport and member
identities, then replays the complete fresh results and prepared lifecycle operations against
their original requests, schemas, and independent references. It also checks the frozen
validation and decision evidence. It executes no native engine, competitor, subprocess, or query.

The frozen cohort contains 2,786 cases: 746 original predicate cases and 2,040 new source-fact
cases across 12 profiles, two sizes, 17 forms, and five surfaces. The accepted baseline contributes
336 byte-reused observations within the complete 2,786-case cohort; new baseline executions total
2,450 and candidate executions total 2,786. The lifecycle cohort has 60 plans and 120 operations per runtime, with
two calls per plan. Selection was frozen before candidate observations. There are no assumed
capture-recovery observations.

The complete fresh-result totals are
34.706028110 -> 33.876871033 engine seconds.
The complete lifecycle totals are
0.112387206 -> 0.085650882 engine seconds.
Replay covers 5,572 fresh results and 240 lifecycle operations across both runtimes.
Admission evidence requires 620 finite-admission witnesses and 150 actual encoded-comparison
dispatch witnesses. These are evidence requirements, not claims that a packet replay creates new
observations. The decision uses complete fresh and lifecycle engine totals and the frozen rules;
see the replay record for measured totals, completeness, regressions, and disposition. This packet
does not convert those measurements into a broader performance claim.

The archive retains raw ledgers, references, guard logs, failed validation logs and receipts,
source snapshots, compressed JSONL snapshots, and the exact original `vortex-array-0.93.0.crate`.
It preserves all present `*-source` directories and exact installed-wheel reports/logs without
including recursive environments. The source-facts array-provider r2 and r3 records are retained,
including the r2 clippy failure. Candidate and earlier runtime payloads and generated benchmark
fixture inputs are excluded from the archive while their frozen identities remain recorded;
checked-in source fixture bytes remain included. The r3 package-notice failure is preserved; r4
corrects notice provenance only, with engine bytes identical to r3. The inherited
dictionary manifest is retained as provenance. Continuation and integration-planning
documents, including the pre-C transport inventory, are supplemental and excluded.

Original paths and generations remain historical provenance and need not exist on the replay
machine. The packet does not imply publication, release, competitive superiority, or completion
of the broader investigation queue.
