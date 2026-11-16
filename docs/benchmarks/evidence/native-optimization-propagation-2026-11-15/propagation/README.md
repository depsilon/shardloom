# Native optimization propagation evidence

Run `python3 -I -B verify.py` using Python 3.13 or newer, with all packet files together.
The verifier uses archived source, complete raw results and independent references. It runs no
query, native engine, competitor, subprocess or network request and forbids original-path reads.

Original decision: **not admitted**. Both runtimes returned all 11,579 fresh requests and all
540 prepared operations correctly. Sixty required downstream dictionary-consumption witnesses
failed after UNION ALL plus row selection. The complete fresh and prepared engine totals also
increased. This packet preserves that outcome and all per-request regressions unchanged.

Fresh engine seconds: 894.235685490 -> 894.646518681.
Prepared engine seconds: 0.516182452 -> 0.521095814.
One observation per request/runtime, no warmups, repeats or reused observations. These totals
do not establish per-change causation or repeatability. This is a different population from the
original full10k: 5,309 exact shared requests plus 6,270 mechanism-focused requests.

The later maintainer direction preserves useful candidate behavior and requests a targeted fix,
impact-relevant investigation tests, a coherent PR when ready, and continued development. It does
not turn the original failed gate into a pass. No merge or publication is implied by this packet.

Complete source/build/test and installed-package evidence is bound for both runtimes. Five
1,048,577-row files and an empty file have complete physical-array inspections and independent
cell/schema/ownership readbacks. All 1,095 new references are independently regenerated offline.
Earlier failed preparation/validation attempts remain in the packet. Native executables, source
payloads and the original execution environment are explicitly identity-only exclusions.
