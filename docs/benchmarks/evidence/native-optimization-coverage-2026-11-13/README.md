# Native optimization coverage observation

This packet records one execution of each of the original 10,000 analytical
cases after the equivalent-pipeline optimization fixes. It retains the original
case order, client, inputs, independent reference results, resource policy and
per-case attempt count. The previous complete observation is a separate source
snapshot. Neither one-pass comparison nor activation counters isolate the
speed benefit of an individual change.

Run `python3 -B verify.py` from this directory. Verification executes no query.
It checks every file and archive member identity, complete case and declaration
alignment, engine clocks, no-fallback and pipeline certificates, recorded
optimization counters, source-bound validation and installed-package evidence.
The corruption controls also relocate the packet and reject modified identities,
duplicate cases, nonfinite clocks, fallback claims and incorrect activation
counts, including controls with updated outer hashes.

Complete output correctness is independently replayed locally against the
original result/reference ledgers before this compact packet is admitted.
Those full ledgers and release binaries remain in local storage. This compact
verifier checks the saved attestations and selected raw certificate fields; it
does not independently regenerate or re-compare full answers.

`optimization-activation.json` reports overlapping observed mechanisms. These
counts do not define total optimization coverage. Existing and planned work
remains open; the overall coverage percentage is deliberately unavailable.
`observations.tar.gz` preserves failed and interrupted checks as well as accepted
checks. The selected native owner tests and all CLI/Python target tests passed;
the interrupted broad native-library run does not count as a completed suite.

Historical absolute paths are provenance. Replay with native execution requires
new local bindings and a fresh output identity, together with the original
storage, isolation, deadline, process-cleanup and complete-result guards.
This packet neither publishes packages nor makes a competitive engine claim.
