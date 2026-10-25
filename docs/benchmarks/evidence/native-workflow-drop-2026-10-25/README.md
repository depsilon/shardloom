# Native workflow drop evidence packet

This packet preserves the three isolated Full43 observations for the dropped native workflow batch, their acceptance and supervision receipts, the runtime restoration receipt, and the independent reference. It is an offline audit artifact; `verify.py` reads the manifest and object-addressed `packet.tar.xz` without extracting files or executing ShardLoom.

The verifier checks archive and per-file SHA-256 identities, safe regular tar members, duplicate JSON keys, exact one-pass query coverage, process cleanup and input stability, every completed gzip output, complete result values against the pinned reference, schema continuity, engine-time sums, the failed candidate regression gate, and restoration to current main. It reports engine totals as evidence only. It makes no causal attribution and does not claim that this dropped batch changed runtime behavior.

Run from this directory:

```sh
python3 verify.py manifest.json packet.tar.xz
python3 -O test_verify.py -v
```

The packet is bounded to 16 MiB per file and 96 MiB expanded. Query records retain engine time; supervised wall time and other diagnostic clocks are not presented as engine time. The packet does not establish completion of the dropped phase item, and placeholder output payloads are not evidence of real Vortex output.

It contains 594 original files in 462 content-addressed objects: 20,317,793
expanded file bytes and 2,634,588 archive bytes. Nine test methods include
malformed JSON, mismatched identities, archive traversal/links/duplicates,
bounded decompression, missing/duplicate queries, incomplete identity checks,
altered engine times/result hashes, incomplete result payloads, fallback and
changed retain/drop receipts. Validation remains active under Python `-O`.
