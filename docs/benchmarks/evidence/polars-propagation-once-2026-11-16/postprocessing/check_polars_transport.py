"""Small synthetic controls for the portable Polars packet transport boundary.

These tests exercise the Store implementations in the explicitly selected packet.
They do not read the packet's evidence archive or execute any query engine.
"""

import argparse
import hashlib
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--packet', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args, remaining = parser.parse_known_args()
assert __debug__ and not args.output.exists()
PACKET = args.packet.resolve()
PART = "evidence.tar.gz.part-001"
PAYLOAD = b"tiny synthetic transport payload\x00\xff\n"
ORIGIN = "/synthetic/original/tiny.bin"
RESULTS = []

sys.path.insert(0, str(PACKET))
import propagation_portable_store as transport  # noqa: E402
import polars_portable_store as polars_store  # noqa: E402


def _tar_bytes(members):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for member in members:
            info = tarfile.TarInfo(member["name"])
            info.mode = 0o600
            info.type = member.get("type", tarfile.REGTYPE)
            info.linkname = member.get("linkname", "")
            data = member.get("data", b"")
            info.size = len(data) if info.isfile() else 0
            archive.addfile(info, io.BytesIO(data) if info.isfile() else None)
    return output.getvalue()


def _member(name="data/tiny.bin", data=PAYLOAD, origin=ORIGIN, **kwargs):
    return dict(name=name, data=data, origin=origin, **kwargs)


def _manifest_member(member):
    data = member.get("data", b"")
    origin = member["origin"]
    return {
        "origin": origin,
        "identity": {
            "path": origin,
            "generation": {
                "device": 1,
                "inode": 2,
                "bytes": len(data),
                "mtime_ns": 3,
                "ctime_ns": 4,
            },
            "sha256": hashlib.sha256(data).hexdigest(),
        },
    }


def make_packet(root, members=None, *, mutate=None, extra_file=None, symlink=False):
    members = members if members is not None else [_member()]
    archive_bytes = _tar_bytes(members)
    part_path = root / PART
    part_path.write_bytes(archive_bytes)
    manifest = {
        "schema": "shardloom.polars_propagation_once_portable.v1",
        "query_executions_during_packaging": 0,
        "publication_attempted": False,
        "complete_raw_ledgers_retained": True,
        "original_references_retained": True,
        "declared_polars_attempts": 11579,
        "historical_native_observations": 11579,
        "polars_outcomes": {"passed": 11489, "incorrect": 90},
        "complete_competitive_baseline": False,
        "archive": {
            "format": "concatenated_gzip_tar",
            "parts": [PART],
            "bytes": len(archive_bytes),
            "sha256": hashlib.sha256(archive_bytes).hexdigest(),
        },
        "archive_members": {m["name"]: _manifest_member(m) for m in members},
        "external_artifacts": {},
        "files": {
            PART: {
                "bytes": len(archive_bytes),
                "sha256": hashlib.sha256(archive_bytes).hexdigest(),
            }
        },
    }
    if mutate:
        mutate(manifest)
    if extra_file:
        (root / extra_file).write_bytes(b"unlisted root item")
    if symlink:
        target = root / "symlink-target"
        target.write_bytes(b"small target")
        (root / "packet-link").symlink_to(target)
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


class PolarsTransportControls(unittest.TestCase):
    @classmethod
    def write_report(cls):
        report = {
            "schema": "shardloom.polars_transport_controls.v1",
            "status": "passed",
            "packet": str(PACKET),
            "verifier": transport.content(Path(__file__)),
            "scope": "synthetic transport fixtures only; no query execution",
            "sources": {
                "polars_portable_store.py": transport.content(PACKET / "polars_portable_store.py"),
                "propagation_portable_store.py": transport.content(PACKET / "propagation_portable_store.py"),
            },
            "controls": RESULTS,
            "control_count": len(RESULTS),
            "rejections": sum(1 for result in RESULTS if result["outcome"] == "rejected"),
            "positive_controls": sum(1 for result in RESULTS if result["outcome"] == "accepted"),
            "query_executions": 0,
            "large_archive_opened": False,
            "engines_imported": [],
        }
        assert not any(name in sys.modules for name in ('polars', 'pyarrow', 'duckdb', 'shardloom._native'))
        with args.output.open('x', encoding='utf-8') as destination:
            destination.write(json.dumps(report, indent=2, sort_keys=True) + "\n")

    def reject(self, name, *, expected=AssertionError, members=None, mutate=None,
               extra_file=None, symlink=False, remove_part=False):
        with tempfile.TemporaryDirectory(prefix="sl-polars-transport-") as temporary:
            root = Path(temporary) / "packet"
            root.mkdir()
            make_packet(root, members, mutate=mutate, extra_file=extra_file, symlink=symlink)
            if remove_part:
                (root / PART).unlink()
            destination = Path(temporary) / "extract"
            destination.mkdir()
            try:
                polars_store.Store(root, destination)
            except expected as error:
                RESULTS.append({
                    "name": name,
                    "outcome": "rejected",
                    "exception_class": type(error).__name__,
                    "detail": str(error)[:240],
                })
            else:
                self.fail(f"control {name} was accepted")

    def test_transport_control_matrix(self):
        with tempfile.TemporaryDirectory(prefix="sl-polars-transport-positive-") as temporary:
            root = Path(temporary) / "packet"
            root.mkdir()
            make_packet(root)
            # Model /var -> /private/var with a temporary directory symlink.
            canonical = Path(temporary) / "private" / "var" / "extract"
            canonical.mkdir(parents=True)
            alias = Path(temporary) / "var"
            alias.symlink_to(canonical.parent)
            store = polars_store.Store(root, alias / "extract")
            stored = store.path(ORIGIN)
            self.assertEqual(stored.read_bytes(), PAYLOAD)
            self.assertEqual(store.original(str(stored)), ORIGIN)
            self.assertEqual(store.path(store.original(str(stored))), stored)
            RESULTS.append({
                "name": "positive_payload_bytes_and_canonical_directory_alias_round_trip",
                "outcome": "accepted",
                "payload_bytes": len(PAYLOAD),
                "payload_sha256": hashlib.sha256(PAYLOAD).hexdigest(),
                "alias_resolved_to": str(store.directory),
                "original_path_round_trip": True,
            })

        self.reject("traversal_member_name", members=[_member(name="../absolute")])
        self.reject("absolute_member_name", members=[_member(name="/absolute")])
        self.reject("backslash_member_name", members=[_member(name="data\\tiny.bin")])
        self.reject("tar_symbolic_link", members=[_member(name="data/link", type=tarfile.SYMTYPE, linkname="target")])
        self.reject("tar_hard_link", members=[_member(name="data/link", type=tarfile.LNKTYPE, linkname="target")])
        self.reject("duplicate_tar_member", members=[_member(), _member()])
        missing_member = _member()
        def claim_missing_member(manifest):
            manifest["archive_members"][missing_member["name"]] = _manifest_member(missing_member)
        self.reject("missing_tar_member", members=[], mutate=claim_missing_member)

        def omit_extra_member_claim(manifest):
            del manifest["archive_members"]["data/extra"]
        self.reject("extra_tar_member", members=[_member(), _member(name="data/extra", origin="/synthetic/extra")],
                    mutate=omit_extra_member_claim)

        def wrong_size(manifest):
            manifest["archive_members"]["data/tiny.bin"]["identity"]["generation"]["bytes"] += 1
        self.reject("wrong_member_size", mutate=wrong_size)

        def wrong_member_hash(manifest):
            manifest["archive_members"]["data/tiny.bin"]["identity"]["sha256"] = "0" * 64
        self.reject("wrong_member_hash", mutate=wrong_member_hash)

        def origin_identity_mismatch(manifest):
            manifest["archive_members"]["data/tiny.bin"]["identity"]["path"] = "/synthetic/other.bin"
        self.reject("origin_identity_mismatch", mutate=origin_identity_mismatch)

        duplicate_members = [_member(name="data/one", origin="/synthetic/same"),
                             _member(name="data/two", origin="/synthetic/same")]
        self.reject("duplicate_origins", members=duplicate_members)

        def wrong_archive_hash(manifest):
            manifest["archive"]["sha256"] = "0" * 64
        self.reject("combined_archive_hash_mismatch", mutate=wrong_archive_hash)

        def wrong_part_hash(manifest):
            manifest["files"][PART]["sha256"] = "0" * 64
        self.reject("part_hash_mismatch", mutate=wrong_part_hash)

        def wrong_sequence(manifest):
            manifest["archive"]["parts"] = ["evidence.tar.gz.part-002"]
        self.reject("part_sequence_failure", mutate=wrong_sequence)
        # The Store rejects this during its exact packet-root inventory check,
        # before it attempts to open the absent part.
        self.reject("missing_part", remove_part=True)
        self.reject("extra_root_file", extra_file="unlisted.txt")
        self.reject("packet_member_symlink", symlink=True)

        def bad_external_reason(manifest):
            path = "/synthetic/external"
            manifest["external_artifacts"][path] = {
                "identity": {"path": path},
                "reason": "unrecognized_reason",
            }
        self.reject("unrecognized_external_reason", mutate=bad_external_reason)

        def internal_external_collision(manifest):
            manifest["external_artifacts"][ORIGIN] = {
                "identity": {"path": ORIGIN},
                "reason": "input_payload_identity_only_independent_references_and_readback_proofs_retained",
            }
        self.reject("internal_external_origin_collision", mutate=internal_external_collision)

        def altered_outcomes(manifest):
            manifest["polars_outcomes"]["passed"] += 1
        self.reject("altered_expected_outcome_counts", mutate=altered_outcomes)

        def claimed_complete_baseline(manifest):
            manifest["complete_competitive_baseline"] = True
        self.reject("incorrect_complete_competitive_baseline", mutate=claimed_complete_baseline)

        def nonzero_packaging_queries(manifest):
            manifest["query_executions_during_packaging"] = 1
        self.reject("nonzero_packaging_query_count", mutate=nonzero_packaging_queries)

        def publication_attempted(manifest):
            manifest["publication_attempted"] = True
        self.reject("publication_true", mutate=publication_attempted)

        self.assertEqual(len(RESULTS), 25)
        self.assertEqual(sum(r["outcome"] == "rejected" for r in RESULTS), 24)
        self.assertEqual(sum(r["outcome"] == "accepted" for r in RESULTS), 1)


if __name__ == "__main__":
    program = unittest.main(argv=[sys.argv[0], *remaining], verbosity=2, exit=False)
    if not program.result.wasSuccessful():
        raise SystemExit(1)
    PolarsTransportControls.write_report()
