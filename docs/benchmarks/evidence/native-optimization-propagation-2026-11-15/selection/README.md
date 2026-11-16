# Selected dictionary evidence

Keep this packet together with the unchanged propagation-packet-r4 companion.
Run `python3 -I -B verify.py --baseline-packet /path/to/propagation-packet-r4`
using Python 3.13 or newer. Both packets may be relocated away from the original
checkout. The verifier refuses original paths, native engines, subprocesses and
network access after extracting their verified archived members.

It replays all 23,158 original fresh results and 1,080 original prepared operations,
then reconstructs the exact saved 2,190-result / 540-operation baseline subset and
checks the new 2,190 fresh results and 540 prepared operations. It verifies complete
values, schema, resources, ownership, work evidence, clocks, source and checks.
The original not-admitted comparison stays unchanged. Read the verifier output
for the focused decision and complete engine totals; saved-versus-new observations
do not establish per-change causation or repeatability. No query is rerun.

Native executables, input payloads and the original execution environment remain
explicit identity-only exclusions. The candidate source archive is excluded as a
duplicate container: all 5,807 source members are verified through these packets.
All failures remain retained. No publication or broader queue completion is claimed.
