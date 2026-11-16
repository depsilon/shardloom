# Resident join attribution and variable result copying

Run `python3 -I -B verify.py` with Python 3.13 or later and all packet files together.
The archive includes complete raw results and independent references for 403 fresh
requests per runtime, complete source bytes for both runtimes and all diagnostic
source variants, check logs, preserved failures, and the unchanged frozen protocol.
Replay forbids original-path reads, execution-library imports, processes and network.
It executes zero queries and tests deliberately corrupted stored results.

Decision: retain the candidate under the frozen complete-cohort rule. Engine time
is 60.778284678 -> 60.597904154 seconds; all 186 individual regressions are retained.
One observation per request/runtime does not establish repeatability or per-change
causation. These 403 requests are not the full corpus or all join workloads.

The 54 corrected instrumented diagnostics and three debug gather observations are
replayed separately; the earlier failed one-ULP transport attempt remains failed.
Native binaries, input payloads, execution environments, installed wheel and source
archive containers are identity-only exclusions; their source members, complete
results and proof records are archived. Replaying evidence does not rebuild or
rerun a native runtime. Local acceptance does not imply a merge or release.
