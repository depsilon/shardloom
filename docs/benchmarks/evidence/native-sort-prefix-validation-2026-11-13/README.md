# Shared Sort prefix validation candidate

This packet preserves the fixed 388-case Sort prefix candidate and its single
current baseline at source `4d5fa4a46268380a634e78f057abba087f4042f5`. Every
candidate case is compared with the matching baseline observation from that
same source. Historical classification provenance is preserved in the frozen
cohort but is not used to supply comparison clocks or a mixed baseline total.

The archive includes complete replay records and certificates, source-bound
checks and runtime identity, work/resource deltas, per-case engine clocks,
failed and stopped focused attempts, the exact Rust source patch, and the
prefix validation design. Complete answers were independently replayed locally
against their reference results. The portable verifier checks saved identities
and evidence without repeating the full oracle or running queries.

Run `python3 -B verify.py` from this directory. Do not use optimized Python;
the verifier requires assertions to remain enabled.
