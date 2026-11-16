# One Polars pass over the saved propagation requests

Exactly 11,579 requests ran once: 11,489 passed and 90 retained a constant-only
projection error in the benchmark adapter. No request was retried and no new
native execution was added. There is no accepted complete-set competitive total.

The passing subset uses 101.07273444440216 seconds in Polars collect calls.
The exact same historical native request IDs use 888.133524476 engine seconds.
These clocks differ; no speedup ratio is claimed. Polars engine time remains
unavailable. The historical native runtime and its original not-admitted
performance/work decision are unchanged; this is not a latest-runtime comparison.

Run `python3 -I -B packet/verify.py` with Python 3.13 or later and every packet
file present. The verifier reopens all 11,579 Polars packets and all 11,579 saved
native packets, reconstructs independent references, checks complete values,
schemas, ordering, clocks and source identities, and rejects 1,120 corruptions.
Original and relocated verification both pass. Neither executes query engines,
spawns processes, uses the network or reads original evidence/input locations.
Allow the unchanged 12 GiB free-space floor plus the unpacked evidence.

Run the small transport controls with:
`python3 -B postprocessing/check_polars_transport.py --packet packet --output /tmp/polars-transport-result.json`
Use a new output path. The exact Store sources reject 24 malformed packets and
pass the valid byte-readback and canonical-directory-alias control. These tests
use tiny synthetic archives, not another workload execution.

The archive contains complete raw ledgers, frozen jobs, original references,
source bytes, preparation/capture receipts, independent input-equivalence and
Vortex-readback proofs, failed attempts, all results and clocks. Bulk inputs
and installed engines are explicitly identity-only. Polars uses declared
IPC/Parquet counterparts for native Vortex slots; native interface and encoding
labels preserve their original slots, not additional Polars APIs or encodings.

All 90 incorrect results remain unchanged. The adapter's constant projection
collapses the input to one row, so COUNT returns 1 instead of the original
population. This is an adapter defect, not evidence of an engine defect.

The periodic memory guard passes, but 69 whole-worker OS high-water readings
exceed its sampled 2.5 GiB threshold. The maximum is 5,302,255,616 bytes. These
are not isolated engine-memory readings or proof of a hard peak cap.

Original offline replay/packaging failures remain archived. The corrected
portable wrapper fixes lazy import lookup and path aliases, then stops the
comparison helper only after its complete paired result exactly matches;
the following location-specific import-map export is replaced by checking and
loading its already frozen source map. Semantic helpers and results are unchanged.
The first synthetic transport test's missing-part exception expectation was
wrong. Its log and receipt remain, but four failed/intermediate source/report
variants were overwritten before preservation. The provenance report identifies
this gap explicitly. Final transport source/report identities are checked; this
limitation affects no scored query source, result, expectation or timing.

`acceptance.json`, `files.json` and `verification/` bind this local evidence.
They do not imply a PR merge, release, full-set baseline or competitive claim.
No repeat, fresh native full11k, Full43 or prepared-lifecycle pass is authorized.
