<!-- SPDX-License-Identifier: Apache-2.0 -->

# Survivor batching admission evidence

The [decision](../../../architecture/native-survivor-admission-2026-11-15.md)
declines unconditional callback buffering before performance measurement.
The existing native batch contract remains in force. Broader I attribution
and a separately admitted narrow batching route remain open.

Run with Python 3.11 or newer:

```sh
python3 -B docs/benchmarks/evidence/native-survivor-admission-2026-11-15/verify.py \
  docs/benchmarks/evidence/native-survivor-admission-2026-11-15
```

This directory can be copied, including its notices, and replayed elsewhere.
The verifier uses only packet files and Python's standard library. It reads the
archive in memory without extraction, engine imports, original source paths,
subprocesses or network use. Passing verifies the saved evidence and independent
scalar references; it does not rerun the native engine or authenticate an
external execution.

The 89 archive members preserve all 13 observation records, seven test-source
versions, original failed test/lint attempts, two failed packaging attempts,
source inventories and exact cited provider files. All eight complete requests
and their 40 output rows are checked for values, order, types and nullability.
Replay verifies four preparation strategies, error/callback counterexamples,
retained allocation credits, eight existing boundary controls and 47 literal
source excerpts. The complete source inventory proves that production code
differs from the accepted predecessor only by test module registrations.

Formatting and strict default/native lint pass. Default tests pass 3,344;
native tests pass 7,261 with 39 existing ignored tests; all seven focused
admission tests pass. Engine timing, logical zone exclusions and consumer
backing-allocation measurements are explicitly unavailable where unobserved.
The slice ownership control is a separate observation, not whole-query I/O proof.

The manifest binds the archive and every member by name, size and SHA-256.
The archive SHA-256 is
`5c607b0f99a04b8a4e69950cb92102cfbf34d0d241e4861c52368e5be4201355`.
Supplementary records preserve the first failed relocation check (a lazy
standard-library decoder import crossed its read barrier), the corrected check,
and the installed packet's relocated replay. All 22 altered-evidence controls
pass, and a deliberately attempted outside-packet read verifies the barrier.

`review-tools` contains the exact construction and control helpers; their local
paths are historical. Only `verify.py` is the standalone portable entry point.
`notices` retains ShardLoom and Vortex source provenance and licenses. The cited
source subset and test fixtures are preserved, but this is not a complete rebuild
bundle. No production binary, performance comparison, writer default, release
or completion of item 19, item 20, RFC 0044 or CG-1 through CG-23 is claimed.
