# Cross-column storage admission — R7

Status: **drop at the bounded analytical screen**. Neither numeric residuals nor
conditional dictionaries establishes a material opportunity under the frozen
15% whole-artifact storage gate. No runtime prototype, custom layout, full-size
replacement artifact or new performance claim is retained. This closes R7 in
PERF-INTAKE; it does not reject cross-column compression on all datasets.

## Workload and method

The current native artifact has 15,682,956,116 bytes after R1.a; a storage candidate
needs at least 2,352,443,418 fewer bytes, with nonregressing ingest and affected
queries. Slower preparation requires demonstrated lifecycle break-even. Neither
a large percentage on a small column pair nor sums of overlapping pair savings
satisfy this gate.

Both screens read the first 65,536 rows in each of source Parquet row groups 0,
113 and 225: 196,608 selected rows, divided into 8,192-row blocks. PyArrow 25.0.1
is a reference reader for these analyses, never ShardLoom execution. Source and
script generations remain unchanged, all selected values are reconstructed
exactly, and the guarded runners finish without storage-budget failures. These
are structured samples, not random or full-artifact observations. Analysis
elapsed time and RSS are not engine performance measurements.

The numeric model chooses the cheapest of frame-of-reference, dictionary and
run-end costs independently for source, target and exact signed residual. It
charges the unchanged source plus residual plus 16 bytes of relationship metadata
per block. Arithmetic uses exact integers, with unsupported overflow reported
explicitly. A 2,048-row regional probe considers all 5,852 ordered integer pairs;
the top two positive range-reduction references per target/region and nine fixed
mechanism pairs produce eleven full-block comparisons. A range probe can miss
other dictionary or exception-based opportunities.

The conditional model charges an independent dictionary for the reference even
when its independent baseline is cheaper. The dependent cost includes its value
domain, reference-to-dependent offsets and IDs, per-row local codes and relationship
metadata. String byte-buffer compression uses actual PyArrow Zstd level 3 sizes;
integer, code, offset and relationship costs are analytical. Independent string
baselines choose the cheaper raw or dictionary model. Two separate shared-union
dictionary estimates cover URL/OriginalURL and URL/Referer. They are pair-local
hypotheses, not a native persistence implementation.

All models omit Vortex framing, alignment, statistics and several existing
encodings. They do not estimate full lifecycle CPU or certify a reader. Retained
per-block evidence reports unsupported inputs and partial coverage explicitly.

## Numeric residual outcome

Only three directed comparisons save bytes in the selected sample. Two are
opposite directions of the same pair and cannot be combined into a dependency
cycle.

| Reference → dependent | Baseline model bytes | Candidate model bytes | Sample saving | Dependent-only read cost / independent dependent |
| --- | ---: | ---: | ---: | ---: |
| EventTime → ClientEventTime | 1,058,101 | 1,048,188 | 9,913 | 1.63× |
| EventTime → LocalEventTime | 830,642 | 820,851 | 9,791 | 1.98× |
| LocalEventTime → EventTime | 830,642 | 820,851 | 9,791 | 1.98× |

The other eight comparisons grow. The LocalEventTime relationship grows in two
of three regions. Even the sum of all eleven candidate target columns' current
referenced segment bytes is only 1,638,060,228 bytes, and that sum can overcount
shared segments. It is a scope comparison, not an achievable saving. The model's
small residual benefit and added dependent-read cost do not justify a new
representation here. No full-size native test is warranted by this screen.

## Conditional and shared-domain outcome

Five of thirteen conditional pairs save modeled bytes. The remaining pairs,
including URL → Title, OriginalURL, Referer and URLHash, grow after charging the
full reference dictionary.

| Reference → dependent | Sample model saving | Pair reduction | Full-parent read scenario / independent dependent |
| --- | ---: | ---: | ---: |
| RefererCategoryID → RefererRegionID | 55,164 bytes | 28.02% | 1.60× |
| URLCategoryID → URLRegionID | 12,160 bytes | 18.79% | 1.40× |
| UserAgent → UserAgentMajor | 5,014 bytes | 8.31% | 1.80× |
| MobilePhone → MobilePhoneModel | 3,296 bytes | 23.61% | 1.66× |
| Referer → RefererHash | 105,503 bytes | 3.85% | 3.88× |

The first four target columns currently account for about 7–121 MB each of
referenced native segments. RefererHash is larger at about 793 MB, but the modeled
pair reduction is small. The read column deliberately charges the full parent
dictionary; a decoder with independently readable parent codes could omit the
parent value domain. The receipt reports that companion scenario separately.
Neither scenario establishes an available provider or measured I/O requests.
The storage savings are not additive whole-artifact savings.

The shared URL/OriginalURL union grows by 63,187 modeled bytes. URL/Referer saves
28,454 bytes out of 8,207,324 baseline bytes (0.35%), and grows in one region.
That is insufficient admission evidence for introducing shared persistence and
read dependencies. Both conditional dictionaries and the bounded union variant
are dropped for this workload and mechanism.

## Vortex-first decision and reopening conditions

Pinned Vortex 0.85.0 already permits dictionary values with a Struct dtype and
supports projection/filtering over dictionary-of-Struct arrays. Its scalar rules
can lift operations over compatible dictionary domains/codes. These capabilities
must not be described as absent.

The default dictionary layout writer admits primitive, UTF8 and binary values;
the default Struct strategy executes a Struct and emits separate field streams.
The inspected public writer path does not establish persistent sibling-domain
sharing or a sibling-column residual provider. `DictLayout::new` is crate-private
and `DictData` fields are private; metadata constructors and generic layout parts
are public. Those surfaces alone are not the required certified write/read
contract. Therefore new layouts remain
`blocked_until_vortex_or_shardloom_evidence`; no implementation is added merely
to work around the weak screen.

Source checks: `vortex-array-0.85.0/src/arrays/dict/{array,execute}.rs`,
`arrays/dict/compute/rules.rs`, and
`vortex-layout-0.85.0/src/{plan/tests.rs,layouts/dict/writer.rs,layouts/struct_/writer.rs}`.
The [Corra paper](https://arxiv.org/html/2403.17229v1) motivates exact residuals,
conditional domains and charging reference reads; no implementation code was copied.

Reopen only with a different, measured mechanism or workload: for example,
economical native exception handling that changes this result, broader domain
sharing with a material byte opportunity, or a certified provider that preserves
selective reads. Require shallow acyclic dependencies, exact null/overflow/error
semantics, complete native value/schema/order/statistics comparison, and full
ingest/query acceptance before any retention. R1.b source-dictionary preservation
is next and is independently decidable.

## Evidence and reproduction

The compact receipt is `docs/benchmarks/cross-column-storage-screen-2026-09-26.json`.
Full block evidence and watchdog receipts are in:

- `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/dictionary-proof-20260926-r7-numeric`.
- `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/dictionary-proof-20260926-r7-conditional`.

The original frozen scripts are retained under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/` as
`r7_numeric_residual_screen.py` and `r7_conditional_dictionary_screen.py`.
The tracked reproductions are
[`scripts/r7_numeric_residual_screen.py`](../../scripts/r7_numeric_residual_screen.py),
[`scripts/r7_conditional_dictionary_screen.py`](../../scripts/r7_conditional_dictionary_screen.py)
and [`scripts/run_local_readonly_proof.py`](../../scripts/run_local_readonly_proof.py).
Their analytical functions match the frozen versions; only input-path handling
changes. Both analysis scripts accept an explicit `--source` and support
`--self-test` without opening the dataset. The conditional script finds its
numeric helper beside itself. The portable runner reuses the existing watchdog,
lock, timeout and storage limits, validates local source/log destinations, and
fingerprints the executable and additional script inputs. It preserves existing
logs and never deletes source data.

From the repository root, with an existing Python 3.11+ environment containing
PyArrow 25.0.1, resident official `hits.parquet`, and an unsynced UAT directory:

```sh
SCREEN_PYTHON=/absolute/path/to/python
SCREEN_SOURCE=/absolute/local/path/to/hits.parquet
SCREEN_ROOT=/absolute/local/path/to/uat
"$SCREEN_PYTHON" -B scripts/r7_numeric_residual_screen.py --self-test
"$SCREEN_PYTHON" -B scripts/r7_conditional_dictionary_screen.py --self-test
"$SCREEN_PYTHON" -B scripts/run_local_readonly_proof.py \
  --uat-root "$SCREEN_ROOT" --source "$SCREEN_SOURCE" --name r7-numeric-new \
  --input scripts/r7_numeric_residual_screen.py -- \
  "$SCREEN_PYTHON" -B scripts/r7_numeric_residual_screen.py --source "$SCREEN_SOURCE"
"$SCREEN_PYTHON" -B scripts/run_local_readonly_proof.py \
  --uat-root "$SCREEN_ROOT" --source "$SCREEN_SOURCE" --name r7-conditional-new \
  --input scripts/r7_numeric_residual_screen.py \
  --input scripts/r7_conditional_dictionary_screen.py -- \
  "$SCREEN_PYTHON" -B scripts/r7_conditional_dictionary_screen.py --source "$SCREEN_SOURCE"
```

Use unique names for reruns. The full historical block reports are also retained
in the compressed [evidence bundle](../benchmarks/evidence/cross-column-storage-2026-09-26.json.gz);
`python3 -m gzip -d` can decode a copy for inspection. This portability change
does not replace the original measured script identities or rerun the dataset.
The receipt records original hashes, generation metadata, models and limits.
No new dependency or dataset copy was installed or created.
