# Where the expanded workload spends work

Two changes to the **declared query** lowered complete-cohort engine time on
the existing native runtime. Selecting columns before prepared membership
improved all 11 cases. Selecting columns before Top-K improved eight of ten.
Deferring nullable expressions lost on its complete 12-case cohort. The
strongest remaining window and join leads are partition ordering and column
gathering, respectively. [Case results and physical evidence](evidence/native-expanded-query-2026-11-03/batch-investigations/findings.json).

The investigation used the frozen 10,000-case corpus, including the original
five failures, across all 18 families and 104 archetypes. Fresh observations
comprised **33 pairs of explicit plans and four separately profiled
requests**. All 70 returned their independent expected results. They used
the same CLI and native library from `9a8dd89c`. The saved Polars pass and
the original release observations retain their original denominators.
[Original study](native-expanded-query-2026-11-03.md) ·
[Earlier matched implementation comparison](native-expanded-operator-investigations-2026-11-03.md).

## Which declarations changed execution

| Public-plan change | Pairs | Original engine s | Alternate engine s | Faster / slower |
|---|---:|---:|---:|---:|
| Select required columns on both prepared membership inputs | 11 | 10.251 | 1.114 | 11 / 0 |
| Select output and ordering columns before Top-K | 10 | 18.137 | 5.843 | 8 / 2 |
| Evaluate integral NULLIF or COALESCE after the bounded sort | 12 | 5.721 | 5.848 | 9 / 3 |

The selector took the first available frozen row per declared cell, preferring
an unfiltered population. It excluded the previous matched requests and the
five original failures. Three of 36 requested cells were absent. The two
forms used identical inputs, surface, runtime, independent schema/value/order
expectation, resource limits and engine clock. First role alternated by row
ordinal. The protocol records both declarations and their digests. The
reader reconstructs the submitted SQL and relation for each arm.
[Frozen protocols, workers and raw results](evidence/native-expanded-query-2026-11-03/batch-investigations/index.json).

**Prepared membership.** The preceding implementation narrowed every one of
the 21 owned Parquet/IPC sources in its affected comparison from 39 columns
to four or five. The 12 prepared Vortex pairs went from 4.327 to 4.387 s.
The new experiment placed ordinary column selections directly on each file
scan. It kept the original filters, nullable membership keys, grouping keys
and measure inputs. The 11 improvements on different cases establish that
the public native path can execute the narrower declarations. Automatic
pushdown still needs an admissible validation contract. A previous prepared
join regression showed that dropping an unused nonfinite field can hide a
required error. [Source-demand contract and original regression](../architecture/native-source-demand-2026-10-26.md).

**Top-K.** Among original requests with at least 20 available source columns
and at least 75% projected, 99 successful cases fell outside membership.
Of these, 98 were hidden-payload ranking and one was an explicit wide
projection. In the new 1M-row prepared wide case ending `ade4e107deda3e58`,
selecting the actual inputs before ordering lowered engine time from 15.604
to 5.520 s. This case retained the same ordering, tie-breaker and seven
outputs. The original 5M prepared wide deadline remains open.

**Computed output.** The four output-expression ordering archetypes account
for 788 original requests and 160.168 completed engine seconds. In 123
unfiltered cases, scans delivered 106,500,816 rows for 8,469 outputs. The
computed value is absent from both ordering keys. Even so, the explicit
nullable rewrite raised the total, particularly on the four prepared cases
(5.172 to 5.384 s). Drop the general rewrite. A future narrower change must
preserve validation and error ordering for discarded rows and compete on its
own complete cohort. [Original expression declarations](../../benchmarks/traditional_analytics/query_surface/expanded_catalog.py).

## Four operator profiles

Each fresh worker used the original public declaration and independent
expectation. The profiler attached to that worker after its native library
loaded. The counts below are main-thread samples whose stack contained the
named call path. Named paths can overlap. Sampling did not produce an
exclusive stage clock, and these requests contribute no comparison times.

| Case | Main-thread samples | Named call paths | Engineering consequence |
|---|---:|---|---|
| Prepared 5M rolling window, `473addced11c72c6` | 1,711 | 1,483 in partition ordering, 133 in frame state | Test partition construction and source ordinal comparisons first. |
| Prepared product left join and distinct group, `612dc5dd63944870` | 1,112 | 882 gathering columns inside join output, 128 accumulating distinct values | Measure source column selection, take and independent output copies on matched and unmatched pairs. |
| 5M products-first two-join group, `587556c8d3d8f5ac` | 373 | 122 gathering inside join output, 129 in final ordering | Separate both consumers when comparing join orders. |
| Prepared distinct-additive group, `5e5f5f289572c9ad` | 170 | 102 in final ordering, 25 in distinct accumulation | Include a control with the same distinct state and a different output order. |

The rolling window validates ordering keys, sorts source ordinals by
partition and order, then writes window values back by original row. The
5M input had ten `group_key` partitions and 4,501,325 transitions between
them. Within each partition, `row_id` advanced strictly throughout. The
100,003-row shuffled input had 49,999 within-partition reversals. Of 776
expanded window declarations, 696 use source profiles with strictly
increasing physical `row_id`. A candidate can check the **delivered** order,
form partition-local ordinal lists, and retain the current ordering algorithm
for groups that fail the check. Test ties, duplicate ordinals, NULL
partitions, descending order, cancellation, memory credit and first errors.
The earlier dropped comparator experiment retained the global sort and row
lookups. [Window execution](../../shardloom-vortex/src/local_primitives/native_relational_window.rs) ·
[Earlier comparator disposition](../architecture/native-typed-ordering-2026-10-31.md).

Resident joins already construct a gather descriptor for a bounded batch
of match pairs. Gathering still prepares selected column views and creates
independently owned output. The 5M profile and the left-join profile
support targeting that work while keeping fanout and outer-null extension
intact. The original scalar count control returned 980,077 rows counted for
the inner join and 1,000,000 for the left join. It cannot stand in for the
composed distinct or multiway cases. Only **five** naturally occurring
star-order pairs matched all factors, surface, format, profile and
independent result. Their totals were 0.597 s entities-first and 0.681 s
products-first. That is a small control set for a future order decision.
[Join output](../../shardloom-vortex/src/local_primitives/native_relational_join.rs) ·
[Batch gather](../../shardloom-vortex/src/local_primitives/native_relational_batch.rs) ·
[Earlier join-count disposition](../architecture/native-join-count-work-elimination-2026-10-25.md).

## What the prepared file contains

The 5M multi-key group reads `group_key`, `category` and `signed_value`.
Parsing the actual Vortex footer attributed **all 50,713 layout segments**
exactly once to the file's 40 field subtrees over 611 chunks. The three
projected subtrees contain 4,277 segments occupying **13,071,648 bytes**.
The largest individual layout segment is 153,004 bytes. The earlier
reservation census recorded 510,973,468 live bytes at a shared host-buffer
allocation site.

Using the file's actual offsets, the following model sorts all selected
ranges together and merges adjacent requests subject to a 4 MiB span. It
shows the transfer-versus-request-count tradeoff under one scheduling
assumption. The runtime's pending request order and the lifetimes of the
returned buffers were not recorded.

| Maximum gap | Modeled read ranges | Modeled bytes |
|---|---:|---:|
| 0 | 4,277 | 13,071,648 |
| 4 KiB | 2,937 | 14,271,536 |
| 64 KiB | 1,222 | 68,435,124 |
| 1 MiB | 136 | 543,214,772 |

Grouping requests separately within each root chunk gives 611 ranges and
575,738,784 bytes at the 1 MiB gap. Measure the actual issued extents,
useful bytes, completion and last-owner drop before choosing a gap policy.
The sampled 4,748,649-byte allocation exactly matches the required missing
footer prefix (4,748,393 bytes) plus 256 bytes of allocator alignment. The
sample lacks its file offset, so it cannot establish which request retained
most of the data. [Native file reads](../../shardloom-vortex/src/resident_session.rs) ·
[Pinned coalescer](../../vendor/vortex-file/src/read/driver.rs) ·
[Required footer reads](../../vendor/vortex-file/src/footer/deserializer.rs).

**Preparation and size.** The Vortex fact file exceeded its Parquet input
by 66,760,868 bytes. Vortex field-segment lengths and Parquet compressed
column-chunk lengths reconcile to that difference after accounting for
container bytes. The nested `detail` and `items` fields occupied 93,033,764
and 75,727,260 Vortex bytes, up 86,116,042 and 68,657,842 respectively.
Both use one flat subtree per root chunk. Two derived time columns add
20,716,300 bytes. Several numeric fields shrink, including `event_time`
by 14,139,616 bytes. The other four scale tables all shrink. A nested-field
encoding experiment can therefore report a specific persisted-byte target
and check subsequent reads.

The 11.599 s provider encode/write clock remains inclusive. The pinned writer
feeds segment buffers to the sink, serializes and writes the footer, then
flushes. The outer workspace residual also includes validation and commit
work. Instrument these boundaries in one writer pass if exclusive sink and
encoding costs are needed. [Writer](../../vendor/vortex-file/src/writer.rs) ·
[Stage clock definitions](../../shardloom-vortex/src/vortex_ingest_stage_timing.rs).

## Other decisions from the frozen corpus

| Investigation | Observation | Disposition |
|---|---|---|
| Parquet pruning | 809 selective-scope cases read every reported row group. In 1,557 comparable single-fact requests whose total matched the file footer, 1,544 read exactly the maximum groups admitted by scope min/max and 13 read fewer. | Close the generic missed-min/max hypothesis for this subset. Preserve shuffled and multi-source controls. |
| Unicode predicate | The 5M text column has six values, all at most four Unicode scalar values long. Counting every character and stopping at four both visit 9,994,763 characters. The 611 Parquet groups report dictionary encoding and their Arrow dictionaries contain 3,666 entries in total. | Test validated dictionary-level predicate evaluation and removal of the intermediate integer column. Early character exit alone cannot save visits in this case. |
| Client construction | One fresh uncollected declaration per surface observed 47 dataclass constructions during client export and 31 during plan construction on both DataFrame and CLI. Both source-compiled `client.py` and `query.py`. | Isolate report-model imports and check import compatibility, introspection and first-error behavior. Keep the original uninstrumented stage observations for timing. |
| Grouped and global provider admission | The file provider handled 89 of 1,756 grouped cases and 16 of 264 global cases. None of the 197 cases each for exact median, variance, deviation and additive DISTINCT entered it. | Separate lowering, source shape and typed semantic admission. Extending a single-key accumulator addresses only the cases that can reach it. |
| CTE materialization | One 5M case materialized the source once, reused it once and replayed 10M consumer rows for three aggregate outputs. A recursive case had a 5,113-row peak frontier across four iterations. | Investigate replayed columns and consumers before changing cache lookup or recursion. |
| As-of and nested | The 392 forward/backward as-of cases consumed 9.174 original engine seconds. The nested cohort covers 196 explode-and-reduce cases. Resident as-of probe counts and exploded-row counts are absent. | Keep as-of probing and post-explode grouping separate. Add the missing cardinality measurements in the next shared instrumentation pass. |

The original corpus contains uncorrelated IN and NOT IN shapes. Correlated
subqueries and pivot/unpivot need distinct future cases. The offline reader
checks complete results and sampled call paths. The source-inspection
programs preserve the inputs and method for the physical file observations.
[Replay guide](../../benchmarks/traditional_analytics/query_surface/README.md) ·
[Evidence inventory](evidence/native-expanded-query-2026-11-03/batch-investigations/index.json).
