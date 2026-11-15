# Native FSST prefilter evidence

Decision: **retain** under the frozen complete-total rule. Run `python3 -I -B verify.py`
with Python 3.13 or newer. Keep the manifest and all archive parts together.
Replay imports only archived semantic helpers and uses complete saved results and independent
references. It executes no native engine, competitor, subprocess or query.

Each runtime has 9,040 complete fresh-session results plus 270 prepared plans with two calls
per plan. All 18,080 results and 1,080 prepared operations pass. All 9,040 baseline results
and 540 baseline prepared calls were reused with exact request/input/runtime/worker bindings;
the candidate executed each declared request and prepared call once, without warmups/repeats.

Fresh engine time: 682.167470629 -> 663.612327731 seconds.
Prepared engine time: 0.520543159 -> 0.511136028 seconds.
All 1759 fresh-request regressions remain in the complete ledger and comparison.
These single observations with a reused baseline do not prove causal attribution of the total
gain to FSST. Per-request release prefilter counters were not measured.

The packet separately replays 864 untimed physical/shared declarations, 378 complete native-core
requests and their refunds/dispatch records against 288 independent nullable Boolean vectors.
The bounded conservative filter is admitted only inside the existing positive source LIKE route.
Existing provider scratch exclusions remain; this is not a full-provider allocation-accounting claim.

The measured source, build, 3,344 default tests, 7,266 native tests, 113 provider tests and 455
installed-package tests are bound and checked. The 41 native/one provider intentional ignores are
explicit. Earlier failed diagnostics, validators and lint attempts remain archived.

Original paths/generations are provenance. Native executables, input payloads and the original
Python execution environment are identity-only exclusions and need not exist for replay.
This does not complete exact-table matcher lifetime, required spatial execution, item 20,
RFC 0044 or any competitive gate, and implies no publication or merge.
