# Selected dictionary cost inventory (R1)

## Scope and totals

- Source population: 2,190 complete pair observations; all baseline and candidate rows passed.
- Engine time: baseline **99.601052609 s**; candidate **100.161598045 s**; candidate minus baseline **+0.560545436 s**.
- Pair directions: **605 regressing**, **1585 improving**, **0 unchanged**.
- This is a finite single-observation inventory. It makes no causal speedup, exclusive-stage attribution, global eligibility, or decision claim.
- The candidate was executed once per fresh request and prepared operation; the baseline observations are reused. Offline replay executes no queries. No external engine executions are recorded.

## Prepared operations (separate population)

- 540 prepared operations per runtime: baseline **0.521095814 s**; candidate **0.513496733 s**; candidate minus baseline **-0.007599081 s**.
- These prepared operations are not included in the 2,190 pair totals.

## Complete pair partitions

- Composite dimensions: `kind × profile × shape × consumer × surface`; **677 groups** cover all 2,190 pairs. Undeclared dimensions are explicitly recorded as `null` in JSON.
- Each single-axis table is a complete partition and reconciles independently to the full population and time totals.

### By kind

| Kind | Pairs | Baseline (s) | Candidate (s) | Δ cand−base (s) | Regressions | Improvements |
|---|---:|---:|---:|---:|---:|---:|
| dictionary_v1 | 1380 | 2.193434837 | 2.177555234 | -0.015879603 | 310 | 1070 |
| original | 205 | 53.575320586 | 53.329504455 | -0.245816131 | 73 | 132 |
| propagation_v1 | 525 | 43.692967725 | 44.517480398 | +0.824512673 | 202 | 323 |
| text_signature_v1 | 80 | 0.139329461 | 0.137057958 | -0.002271503 | 20 | 60 |

### By profile

| Profile | Pairs | Baseline (s) | Candidate (s) | Δ cand−base (s) | Regressions | Improvements |
|---|---:|---:|---:|---:|---:|---:|
| dictionary_text_public_all_null | 145 | 0.100509635 | 0.104297512 | +0.003787877 | 50 | 95 |
| dictionary_text_public_dictionary | 145 | 0.119097886 | 0.117723504 | -0.001374382 | 52 | 93 |
| dictionary_text_public_empty | 145 | 0.024366369 | 0.023197669 | -0.001168700 | 28 | 117 |
| dictionary_text_public_flat | 145 | 0.104510584 | 0.102568491 | -0.001942093 | 29 | 116 |
| dictionary_text_public_sparse | 145 | 0.113872871 | 0.116970464 | +0.003097593 | 50 | 95 |
| dictionary_text_public_unique | 145 | 0.105158329 | 0.103407083 | -0.001751246 | 30 | 115 |
| dictionary_text_scale_all_null | 30 | 7.010630833 | 7.708590874 | +0.697960041 | 21 | 9 |
| dictionary_text_scale_dictionary | 30 | 8.331054583 | 8.138963582 | -0.192091001 | 11 | 19 |
| dictionary_text_scale_empty | 30 | 0.004288919 | 0.003927579 | -0.000361340 | 1 | 29 |
| dictionary_text_scale_flat | 30 | 14.391248040 | 14.325061624 | -0.066186416 | 7 | 23 |
| dictionary_text_scale_sparse | 30 | 7.711583502 | 8.118414292 | +0.406830790 | 17 | 13 |
| dictionary_text_scale_unique | 15 | 5.384920623 | 5.352132418 | -0.032788205 | 4 | 11 |
| dictionary_text_work_all_null | 145 | 0.276879999 | 0.281765701 | +0.004885702 | 45 | 100 |
| dictionary_text_work_dictionary | 145 | 0.356349259 | 0.349306168 | -0.007043091 | 37 | 108 |
| dictionary_text_work_empty | 145 | 0.024624497 | 0.023244413 | -0.001380084 | 26 | 119 |
| dictionary_text_work_flat | 145 | 0.551221382 | 0.542502796 | -0.008718586 | 21 | 124 |
| dictionary_text_work_sparse | 145 | 0.347741331 | 0.348126794 | +0.000385463 | 47 | 98 |
| dictionary_text_work_unique | 145 | 0.928343920 | 0.934834668 | +0.006490748 | 36 | 109 |
| holdout | 18 | 0.537134585 | 0.543037957 | +0.005903372 | 9 | 9 |
| primary | 185 | 52.524302377 | 52.427544414 | -0.096757963 | 64 | 121 |
| scale | 1 | 0.512474666 | 0.357570625 | -0.154904041 | 0 | 1 |
| smoke | 1 | 0.001408958 | 0.001351459 | -0.000057499 | 0 | 1 |
| text_signature_public_absent_dictionary | 5 | 0.005612958 | 0.005566750 | -0.000046208 | 2 | 3 |
| text_signature_public_all_null_dictionary | 5 | 0.005538625 | 0.005561918 | +0.000023293 | 2 | 3 |
| text_signature_public_dense_dictionary | 5 | 0.006022999 | 0.005958332 | -0.000064667 | 2 | 3 |
| text_signature_public_missing_dictionary | 5 | 0.005676043 | 0.005609208 | -0.000066835 | 1 | 4 |
| text_signature_public_saturated_dictionary | 5 | 0.005734834 | 0.005691750 | -0.000043084 | 2 | 3 |
| text_signature_public_selective_dictionary | 5 | 0.005679001 | 0.005720335 | +0.000041334 | 3 | 2 |
| text_signature_public_short_dictionary | 5 | 0.005618083 | 0.005541752 | -0.000076331 | 1 | 4 |
| text_signature_public_unclustered_dictionary | 5 | 0.005758083 | 0.005707916 | -0.000050167 | 1 | 4 |
| text_signature_work_absent_dictionary | 5 | 0.009803459 | 0.009705042 | -0.000098417 | 0 | 5 |
| text_signature_work_all_null_dictionary | 5 | 0.006734373 | 0.006616709 | -0.000117664 | 0 | 5 |
| text_signature_work_dense_dictionary | 5 | 0.013037667 | 0.012931541 | -0.000106126 | 2 | 3 |
| text_signature_work_missing_dictionary | 5 | 0.010245750 | 0.010128833 | -0.000116917 | 1 | 4 |
| text_signature_work_saturated_dictionary | 5 | 0.023393418 | 0.022156958 | -0.001236460 | 0 | 5 |
| text_signature_work_selective_dictionary | 5 | 0.010236499 | 0.010083583 | -0.000152916 | 1 | 4 |
| text_signature_work_short_dictionary | 5 | 0.009534960 | 0.009419124 | -0.000115836 | 1 | 4 |
| text_signature_work_unclustered_dictionary | 5 | 0.010702709 | 0.010658207 | -0.000044502 | 1 | 4 |

### By shape

| Shape | Pairs | Baseline (s) | Candidate (s) | Δ cand−base (s) | Regressions | Improvements |
|---|---:|---:|---:|---:|---:|---:|
| direct | 175 | 7.448492547 | 7.368403045 | -0.080089502 | 20 | 155 |
| not_declared | 1665 | 55.908084884 | 55.644117647 | -0.263967237 | 403 | 1262 |
| union_all | 175 | 19.641866087 | 20.642355652 | +1.000489565 | 105 | 70 |
| union_selected | 175 | 16.602609091 | 16.506721701 | -0.095887390 | 77 | 98 |

### By consumer

| Consumer | Pairs | Baseline (s) | Candidate (s) | Δ cand−base (s) | Regressions | Improvements |
|---|---:|---:|---:|---:|---:|---:|
| checksum | 270 | 25.345311468 | 25.888640620 | +0.543329152 | 103 | 167 |
| group_summary | 255 | 18.347656257 | 18.628839778 | +0.281183521 | 99 | 156 |
| not_declared | 1665 | 55.908084884 | 55.644117647 | -0.263967237 | 403 | 1262 |

### By surface

| Surface | Pairs | Baseline (s) | Candidate (s) | Δ cand−base (s) | Regressions | Improvements |
|---|---:|---:|---:|---:|---:|---:|
| cli_sql | 434 | 18.136170005 | 18.397411455 | +0.261241450 | 314 | 120 |
| dataframe | 430 | 20.970949175 | 20.986047004 | +0.015097829 | 77 | 353 |
| embedded_workflow | 439 | 24.458710677 | 24.622140964 | +0.163430287 | 66 | 373 |
| python_sql | 449 | 17.650998594 | 17.756862005 | +0.105863411 | 72 | 377 |
| typed_relation | 438 | 18.384224158 | 18.399136617 | +0.014912459 | 76 | 362 |

## Largest per-pair changes

Times are candidate minus reused baseline for the same pair. Positive values regress; negative values improve.

### 15 largest regressions

| Case | Kind | Profile | Shape | Consumer | Surface | Baseline (s) | Candidate (s) | Δ (s) |
|---|---|---|---|---|---|---:|---:|---:|
| prop_dictionary_v1_scale_all_null_union_selected_checksum_dataframe | propagation_v1 | dictionary_text_scale_all_null | union_selected | checksum | dataframe | 0.286514708 | 0.345004000 | +0.058489292 |
| prop_dictionary_v1_scale_all_null_union_selected_checksum_embedded_workflow | propagation_v1 | dictionary_text_scale_all_null | union_selected | checksum | embedded_workflow | 0.287868542 | 0.344888125 | +0.057019583 |
| prop_dictionary_v1_scale_all_null_union_selected_checksum_python_sql | propagation_v1 | dictionary_text_scale_all_null | union_selected | checksum | python_sql | 0.288383709 | 0.344219333 | +0.055835624 |
| prop_dictionary_v1_scale_flat_union_all_group_summary_cli_sql | propagation_v1 | dictionary_text_scale_flat | union_all | group_summary | cli_sql | 0.642675875 | 0.697124207 | +0.054448332 |
| prop_dictionary_v1_scale_all_null_union_selected_checksum_cli_sql | propagation_v1 | dictionary_text_scale_all_null | union_selected | checksum | cli_sql | 0.284884667 | 0.337585083 | +0.052700416 |
| prop_dictionary_v1_scale_sparse_union_all_checksum_cli_sql | propagation_v1 | dictionary_text_scale_sparse | union_all | checksum | cli_sql | 0.332781665 | 0.378186666 | +0.045405001 |
| prop_dictionary_v1_scale_all_null_union_selected_checksum_typed_relation | propagation_v1 | dictionary_text_scale_all_null | union_selected | checksum | typed_relation | 0.300273167 | 0.344434250 | +0.044161083 |
| prop_dictionary_v1_scale_sparse_union_all_group_summary_python_sql | propagation_v1 | dictionary_text_scale_sparse | union_all | group_summary | python_sql | 0.291805792 | 0.331686208 | +0.039880416 |
| prop_dictionary_v1_scale_all_null_union_selected_group_summary_cli_sql | propagation_v1 | dictionary_text_scale_all_null | union_selected | group_summary | cli_sql | 0.285946373 | 0.325387707 | +0.039441334 |
| prop_dictionary_v1_scale_dictionary_union_all_checksum_typed_relation | propagation_v1 | dictionary_text_scale_dictionary | union_all | checksum | typed_relation | 0.360651167 | 0.399523333 | +0.038872166 |
| prop_dictionary_v1_scale_dictionary_union_all_checksum_cli_sql | propagation_v1 | dictionary_text_scale_dictionary | union_all | checksum | cli_sql | 0.339451917 | 0.376399583 | +0.036947666 |
| prop_dictionary_v1_scale_dictionary_union_all_checksum_embedded_workflow | propagation_v1 | dictionary_text_scale_dictionary | union_all | checksum | embedded_workflow | 0.362084125 | 0.397134291 | +0.035050166 |
| prop_dictionary_v1_scale_all_null_union_all_checksum_dataframe | propagation_v1 | dictionary_text_scale_all_null | union_all | checksum | dataframe | 0.333177541 | 0.367871333 | +0.034693792 |
| prop_dictionary_v1_scale_sparse_union_all_group_summary_cli_sql | propagation_v1 | dictionary_text_scale_sparse | union_all | group_summary | cli_sql | 0.279174667 | 0.313778210 | +0.034603543 |
| prop_dictionary_v1_scale_dictionary_union_all_group_summary_cli_sql | propagation_v1 | dictionary_text_scale_dictionary | union_all | group_summary | cli_sql | 0.281915582 | 0.316292250 | +0.034376668 |

### 15 largest improvements

| Case | Kind | Profile | Shape | Consumer | Surface | Baseline (s) | Candidate (s) | Δ (s) |
|---|---|---|---|---|---|---:|---:|---:|
| v2_text_filtered_reduction_2583732980a661ec | original | scale | — | — | dataframe | 0.512474666 | 0.357570625 | -0.154904041 |
| prop_dictionary_v1_scale_dictionary_union_selected_group_summary_typed_relation | propagation_v1 | dictionary_text_scale_dictionary | union_selected | group_summary | typed_relation | 0.401996750 | 0.332528709 | -0.069468041 |
| prop_dictionary_v1_scale_dictionary_union_selected_group_summary_python_sql | propagation_v1 | dictionary_text_scale_dictionary | union_selected | group_summary | python_sql | 0.400028500 | 0.333256458 | -0.066772042 |
| prop_dictionary_v1_scale_dictionary_union_selected_group_summary_dataframe | propagation_v1 | dictionary_text_scale_dictionary | union_selected | group_summary | dataframe | 0.400413541 | 0.336029792 | -0.064383749 |
| prop_dictionary_v1_scale_dictionary_union_selected_group_summary_embedded_workflow | propagation_v1 | dictionary_text_scale_dictionary | union_selected | group_summary | embedded_workflow | 0.396642333 | 0.334890834 | -0.061751499 |
| prop_dictionary_v1_scale_dictionary_union_selected_checksum_dataframe | propagation_v1 | dictionary_text_scale_dictionary | union_selected | checksum | dataframe | 0.416903875 | 0.358989250 | -0.057914625 |
| prop_dictionary_v1_scale_dictionary_union_selected_checksum_typed_relation | propagation_v1 | dictionary_text_scale_dictionary | union_selected | checksum | typed_relation | 0.402947125 | 0.355169584 | -0.047777541 |
| prop_dictionary_v1_scale_dictionary_union_selected_group_summary_cli_sql | propagation_v1 | dictionary_text_scale_dictionary | union_selected | group_summary | cli_sql | 0.372697709 | 0.326053000 | -0.046644709 |
| prop_dictionary_v1_scale_dictionary_union_selected_checksum_python_sql | propagation_v1 | dictionary_text_scale_dictionary | union_selected | checksum | python_sql | 0.396718958 | 0.357688833 | -0.039030125 |
| prop_dictionary_v1_scale_dictionary_union_selected_checksum_embedded_workflow | propagation_v1 | dictionary_text_scale_dictionary | union_selected | checksum | embedded_workflow | 0.394908833 | 0.357659042 | -0.037249791 |
| v2_text_expression_order_195102357a18a3f0 | original | primary | — | — | cli_sql | 2.276337750 | 2.239336749 | -0.037001001 |
| v2_text_expression_order_628940d7cf7bafd3 | original | primary | — | — | typed_relation | 2.193353458 | 2.162297875 | -0.031055583 |
| prop_dictionary_v1_scale_dictionary_union_selected_checksum_cli_sql | propagation_v1 | dictionary_text_scale_dictionary | union_selected | checksum | cli_sql | 0.375766333 | 0.345005459 | -0.030760874 |
| prop_dictionary_v1_scale_flat_union_selected_group_summary_python_sql | propagation_v1 | dictionary_text_scale_flat | union_selected | group_summary | python_sql | 0.441954833 | 0.422657459 | -0.019297374 |
| prop_dictionary_v1_scale_sparse_union_selected_group_summary_typed_relation | propagation_v1 | dictionary_text_scale_sparse | union_selected | group_summary | typed_relation | 0.356166833 | 0.339637667 | -0.016529166 |

## Propagation coverage witnesses

- Declared propagation rows: **525**; status counts: `passed` 525.
- Non-null witness records: `original_gate_witness` 525/525; `maintained_baseline` 525/525; `candidate` 525/525; `maintained_baseline_representation` 275/525.
- Declaration flag totals:
  - `encoding_eligible`: true 270, false 255, undeclared 0.
  - `cost_admission_guaranteed`: true 120, false 405, undeclared 0.
  - `required_retention`: true 120, false 405, undeclared 0.
  - `required_kernel_reduction`: true 80, false 445, undeclared 0.
  - `required_new_selection_reduction`: true 40, false 485, undeclared 0.
  - `preserve_achieved_union_reduction`: true 40, false 485, undeclared 0.

- Exact frequency counts for `actual_new_avoided_kernel_invocations`, including null values, are recorded in JSON. Coverage status and witness presence are inventory facts; they do not imply global eligibility or exclusive attribution.

## Existing replay subgroup inventory

- Preserved all **81** subgroup summaries from the replay in JSON under `existing_replay_subgroups.groups`.
- This avoids substituting newly inferred groupings for the source report’s existing subgroup definitions.

## Source hashes

| Source file | Bytes | SHA-256 |
|---|---:|---|
| `/Users/dylan/LocalData/shardloom/selected-dictionary-2026-11-15/selection-replay-r1.json` | 69347470 | `4279ffbf49c7336ede24853472bb457fa8b905d47fa032d236b784be125e14e4` |
| `/Users/dylan/LocalData/shardloom/selected-dictionary-2026-11-15/selection-protocol-r1.json` | 29521949 | `55bc015a36d60759617e51dbe4bb3102c2921910edae6179d5013ec64781bc30` |
| `/Users/dylan/LocalData/shardloom/selected-dictionary-2026-11-15/selection-r1.json` | 39012801 | `af8db5fea585f295b477b5fc4b8c8dd662acdd7f76a2e085fe2734b1e809045b` |
| `/Users/dylan/LocalData/shardloom/selected-dictionary-2026-11-15/baseline-reuse-r1.json` | 42672146 | `88bf6c69db0f698e319904c3ce7e6881b696e06a1f9b457637b6051268cc1a07` |

Artifact companion: `selection-cost-inventory-r1.json` contains complete per-axis and composite partitions, all 81 source subgroup records, top-pair tables, propagation witness counts, and machine-readable reconciliation checks.
