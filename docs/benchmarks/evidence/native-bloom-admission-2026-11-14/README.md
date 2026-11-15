<!-- SPDX-License-Identifier: Apache-2.0 -->

# Pinned Bloom admission evidence

The [architecture decision](../../../architecture/native-bloom-admission-2026-11-14.md)
declines production admission of the existing unversioned Vortex 0.93.0 Bloom
provider before performance measurement. This packet contains no benchmark
result, production implementation, Bloom data file or executable binary.

Run with Python 3.11 or newer, without `-O`:

```sh
python3 docs/benchmarks/evidence/native-bloom-admission-2026-11-14/verify.py
```

This directory can also be copied, including its source notices, and verified
elsewhere. The verifier reads only `verify.py`, `manifest.json`, `evidence.tar.xz`
and Python's standard library. It does not extract the archive,
import an engine, open original source paths, invoke a subprocess or use the network.

The 558,980-byte archive has 92 regular members. Its SHA-256 is
`62862b6a674dcc2a4d9c73997c5f5f2e63c1dfe0b9d76fc06def4e56c95e7d4a`.
The manifest binds each member's name, size and hash, plus the verifier identity.
Source files are the exact cited review subset, not a complete rebuild bundle.
The build receipts contain the complete source inventory and the preserved
probe source; their original paths are diagnostic identities, not replay inputs.

The verifier checks all 28 named native observations, independently checks the
four signed-zero bit/equality relationships, binds source/build/executable/log
identities, checks the pinned persistence warning and locked dependency hashes,
and verifies exact restoration after removal of the temporary example.
Passing means the saved evidence and decision agree. It does not independently
rerun native membership or authenticate an external execution.

The supplementary validation records preserve 38 rejected corruptions and a
fresh relocated process that blocks reads from original checkouts, caches and
historical helpers, and blocks subprocess/network use. A deliberate denied read
first proves the read barrier is active. Original inventory omissions and both
pre-archive packaging failures are retained in the archive. No native checks were
repeated because of those packaging corrections.

`review-tools` contains the exact local construction/control helpers for review;
their recorded local paths are historical, and only `verify.py` is the standalone
portable entry point. `notices` retains the source licenses and provenance.
The current runtime retains the accepted source-domain C validation unchanged.
