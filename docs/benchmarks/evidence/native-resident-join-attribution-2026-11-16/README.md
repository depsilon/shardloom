# Resident join attribution and selected-variable copying

The measured candidate is `ffe0d077a676fd8b23cfebed9e11bf189e1ae459`.
Both complete 403-request runs pass independent result, schema, resource,
ownership, native-clock and no-fallback checks. The frozen decision retains the
candidate: 60.778284678 -> 60.597904154 engine seconds. All 186 individual
regressions remain in the complete result ledger. The gain is about 0.3%; this
is not a material-performance or repeatability claim.

The maintainer subsequently prioritizes material gains over further sliver
optimizations. That prospective direction is recorded separately; it changes
the next candidate selection, not this frozen observation or earlier decisions.

Run `python3 -I -B packet/verify.py` with Python 3.13 or newer. This reconstructs
all 806 complete packets, both complete source snapshots, the 54 corrected
instrumented diagnostics and three debug gather observations, checks the frozen
source/test/installed-package records, and rejects 126 fresh-packet corruptions
plus seven decision-boundary controls. It uses no execution library, native
binary, original data, subprocess or network request. Original and relocated
replay reports are under `verification/`.

Run the exact transport controls separately with:
`python3 -B postprocessing/check_variable_copy_transport.py --packet packet --output /tmp/variable-copy-transport-result.json`
The output must be a new path. Nineteen malformed archive/manifest controls are
rejected and one valid transport control passes. The controls use small
synthetic archives, not new query executions. Their source and report bind the
same transport bytes used by complete replay.

The inner packet manifest binds every part and extracted member. Large inputs,
native binaries, execution environments, wheel and original source archive
containers are identity-only exclusions. All source members and diagnostic
source variants are included. Complete historical build source bytes match;
24 preserved historical source records lack original filesystem generation
tuples, which are not reconstructed or claimed.

The original one-ULP diagnostic failure, source/test/build failures and retirement
inventories remain in the inner archive. Later packaging failures are recorded
under `failures/`, with their old manifest and changed source bytes; the complete
failed packet also remains retained at its recorded local path. The initial
packager compared access time; the corrected generation check excludes it.
The first portable replay lacked the inherited build guard and misresolved
relative cleanup reads; the final packet fixes both without changing any query.

`acceptance.json` and `files.json` bind subsequent verification and the preceding
PR's exact-main completion. They do not imply that this candidate is merged or
released. No full10k, full11k, Full43, competitor run or publication occurs here.
