# SPDX-License-Identifier: Apache-2.0
"""Bounded multipart evidence transport; archived code is never executed."""
from collections import OrderedDict, defaultdict
import io
import json
import lzma
from pathlib import Path, PurePosixPath
import re
import tarfile

from native_workflow_protocol import strict_json
from query_surface.replay import (
    MAX_ARCHIVE, MAX_EXPANDED, MAX_FILE, MAX_FILES, bounded_bytes, need, object_files, sha,
)

SCHEMA = "shardloom.shared_performance_bundle.v1"
MAX_RECORD = 96 << 20
MAX_PARTS = 8
PART_TARGET = 192 << 20
MAX_TOTAL_RECORD = MAX_PARTS * PART_TARGET
MAX_CACHED_RECORD = 64 << 10
MAX_RECORD_CACHE = 16 << 20
DIGEST = re.compile(r"[0-9a-f]{64}")


def digest_string(value):
    return isinstance(value, str) and DIGEST.fullmatch(value) is not None


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


class Bundle:
    """Resolve original paths and versions from verified regular archive objects."""

    def __init__(self, folder):
        self.folder = Path(folder)
        need(not self.folder.is_symlink() and self.folder.is_dir(), "invalid bundle directory")
        self.index = strict_json(bounded_bytes(self.folder / "manifest.json", MAX_FILE))
        need(isinstance(self.index, dict) and set(self.index) == {
            "schema_version", "parts", "records", "path_map", "evidence"}
            and self.index["schema_version"] == SCHEMA, "wrong shared-performance bundle schema")
        self.parts = self.index["parts"]
        self.records = self.index["records"]
        self.paths = self.index["path_map"]
        need(isinstance(self.parts, list) and 0 < len(self.parts) <= MAX_PARTS, "invalid part count")
        need(isinstance(self.records, dict) and 0 < len(self.records) <= MAX_FILES, "invalid record count")
        need(isinstance(self.paths, dict) and 0 < len(self.paths) <= MAX_FILES, "invalid original-path count")
        expected_folders = {f"part-{i:03d}" for i in range(len(self.parts))}
        actual = {path.name for path in self.folder.iterdir()}
        need(actual - {"manifest.json", "acceptance.json"} == expected_folders, "extra or missing bundle entry")
        expected_chunks = defaultdict(dict)
        chunk_locations = {}
        total_record_bytes = 0
        for digest, record in self.records.items():
            need(digest_string(digest) and set(record) == {"bytes", "chunks"}, "invalid record descriptor")
            size, chunks = record["bytes"], record["chunks"]
            need(type(size) is int and 0 <= size <= MAX_RECORD, "record exceeds bound")
            total_record_bytes += size
            need(total_record_bytes <= MAX_TOTAL_RECORD, "reconstructed records exceed aggregate bound")
            need(isinstance(chunks, list) and len(chunks) == max(1, (size + MAX_FILE - 1) // MAX_FILE),
                 "invalid record chunk count")
            for i, chunk in enumerate(chunks):
                need(set(chunk) == {"part", "name", "bytes", "sha256"}, "invalid chunk descriptor")
                number, value = chunk["part"], chunk["sha256"]
                need(type(number) is int and 0 <= number < len(self.parts) and digest_string(value),
                     "invalid chunk part or digest")
                need(chunk["name"] == "chunks/" + value and type(chunk["bytes"]) is int
                     and chunk["bytes"] == min(MAX_FILE, max(0, size - i * MAX_FILE)),
                     "chunk name or size changed")
                location = (number, chunk["name"], chunk["bytes"])
                need(value not in chunk_locations or chunk_locations[value] == location,
                     "ambiguous duplicate chunk location")
                chunk_locations[value] = location
                expected_chunks[number][chunk["name"]] = (chunk["bytes"], value)
        used = set()
        for path, versions in self.paths.items():
            need(isinstance(path, str) and path.startswith("/") and not path.startswith("//")
                 and "\0" not in path and str(PurePosixPath(path)) == path
                 and not {".", ".."}.intersection(PurePosixPath(path).parts),
                 "invalid original evidence path")
            need(isinstance(versions, list) and versions and versions == sorted(set(versions))
                 and set(versions) <= self.records.keys(), "missing or duplicate path version")
            used.update(versions)
        need(used == self.records.keys(), "unreferenced evidence record")
        self._cache = OrderedDict()
        self._record_cache = OrderedDict()
        self._record_cache_bytes = 0
        for number, part in enumerate(self.parts):
            need(set(part) == {"directory", "manifest_sha256", "manifest_bytes"}
                 and part["directory"] == f"part-{number:03d}", "unsafe or duplicate part")
            directory = self.folder / part["directory"]
            need(not directory.is_symlink() and directory.is_dir()
                 and {path.name for path in directory.iterdir()} == {"manifest.json", "packet.tar.xz"},
                 "invalid part contents")
            files = self._part(number)
            need(set(files) == set(expected_chunks[number]), "extra or missing chunks")
            for name, data in files.items():
                need((len(data), sha(data)) == expected_chunks[number][name], "chunk content changed")
        # Check every reconstructed record, including evidence not used by a
        # specific semantic replay. Two parts bound memory while allowing
        # independent references and candidate outputs in different parts.
        for digest in sorted(self.records, key=lambda key: self.records[key]["chunks"][0]["part"]):
            self.by_digest(digest)

    def _part(self, number):
        if number not in self._cache:
            if len(self._cache) == 2:
                self._cache.popitem(last=False)
            item = self.parts[number]
            folder = self.folder / item["directory"]
            data = bounded_bytes(folder / "manifest.json", MAX_FILE)
            need(type(item["manifest_bytes"]) is int and len(data) == item["manifest_bytes"]
                 and sha(data) == item["manifest_sha256"], "part manifest identity changed")
            manifest = strict_json(data)
            files = object_files(manifest, bounded_bytes(folder / "packet.tar.xz", MAX_ARCHIVE))
            self._cache[number] = files
        self._cache.move_to_end(number)
        return self._cache[number]

    def by_digest(self, digest):
        need(digest in self.records, "missing evidence record: " + str(digest))
        if digest in self._record_cache:
            self._record_cache.move_to_end(digest)
            return self._record_cache[digest]
        record = self.records[digest]
        chunks = [self._part(chunk["part"])[chunk["name"]] for chunk in record["chunks"]]
        data = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        need(len(data) == record["bytes"] and sha(data) == digest, "reconstructed record identity changed")
        # Empty stderr and small references recur between parts containing large
        # outputs. Keep a bounded cache so those records do not repeatedly evict
        # a whole verified archive part. Large public records remain uncached.
        if len(data) <= MAX_CACHED_RECORD:
            while self._record_cache and self._record_cache_bytes + len(data) > MAX_RECORD_CACHE:
                _, previous = self._record_cache.popitem(last=False)
                self._record_cache_bytes -= len(previous)
            self._record_cache[digest] = data
            self._record_cache_bytes += len(data)
        return data

    def raw(self, path, digest=None):
        path = str(path)
        need(path in self.paths, "missing retained path: " + path)
        versions = self.paths[path]
        if digest is None:
            need(len(versions) == 1, "historical evidence path requires an explicit digest: " + path)
            digest = versions[0]
        need(digest in versions, "unrecorded path version: " + path)
        return self.by_digest(digest)

    def identity(self, item):
        data = self.raw(item["path"], item["sha256"])
        size = item.get("bytes", item.get("generation", {}).get("bytes"))
        need(type(size) is int and len(data) == size, "linked record size differs")
        return data


def _archive(files):
    objects, inventory = {}, []
    for name, data in sorted(files.items()):
        digest = sha(data)
        objects[digest] = data
        inventory.append(dict(path=name, bytes=len(data), sha256=digest, member="objects/sha256/" + digest))
    expanded = sum(map(len, files.values()))
    need(len(files) <= MAX_FILES and expanded <= MAX_EXPANDED, "part exceeds existing bounds")
    data = json_bytes(inventory)
    need(len(data) <= MAX_FILE, "part inventory exceeds bound")
    digest = sha(data)
    objects[digest] = data
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as stream:
        for key, value in sorted(objects.items()):
            member = tarfile.TarInfo("objects/sha256/" + key)
            member.size, member.mode, member.mtime = len(value), 0o644, 0
            stream.addfile(member, io.BytesIO(value))
    need(buffer.tell() <= MAX_ARCHIVE, "part tar exceeds existing archive bound")
    packed = lzma.compress(buffer.getvalue(), preset=6)
    manifest = dict(schema_version="shardloom.broad_query.packet.v1",
        archive=dict(path="packet.tar.xz", bytes=len(packed), sha256=sha(packed)),
        expanded_file_bytes=expanded, unique_objects=len(objects), file_count=len(files),
        file_inventory=dict(member="objects/sha256/" + digest, sha256=digest, bytes=len(data)))
    need(object_files(manifest, packed) == files, "part archive readback differs")
    return json_bytes(manifest), packed


def write_bundle(folder, raw_records, path_map, evidence):
    """Write new parts from already-collected bytes; never replace a prior bundle."""
    folder = Path(folder)
    need(not folder.exists(), "refuse to replace evidence bundle")
    records, chunks, locations = {}, [], {}
    current, size = {}, 0
    for digest, data in raw_records.items():
        need(sha(data) == digest and len(data) <= MAX_RECORD, "invalid collected record")
        parts = []
        for start in range(0, max(len(data), 1), MAX_FILE):
            chunk = data[start:start + MAX_FILE]
            chunk_sha = sha(chunk)
            if chunk_sha not in locations:
                if current and size + len(chunk) > PART_TARGET:
                    chunks.append(current)
                    current, size = {}, 0
                name = "chunks/" + chunk_sha
                current[name], size = chunk, size + len(chunk)
                locations[chunk_sha] = dict(part=len(chunks), name=name, bytes=len(chunk), sha256=chunk_sha)
            parts.append(locations[chunk_sha])
        records[digest] = dict(bytes=len(data), chunks=parts)
    if current:
        chunks.append(current)
    need(0 < len(chunks) <= MAX_PARTS, "bundle requires too many bounded parts")
    folder.mkdir()
    index = dict(schema_version=SCHEMA, records=records,
        path_map={path: sorted(set(versions)) for path, versions in sorted(path_map.items())},
        evidence=evidence, parts=[])
    for i, files in enumerate(chunks):
        directory = folder / f"part-{i:03d}"
        directory.mkdir()
        header, archive = _archive(files)
        (directory / "manifest.json").write_bytes(header)
        (directory / "packet.tar.xz").write_bytes(archive)
        index["parts"].append(dict(directory=directory.name, manifest_sha256=sha(header), manifest_bytes=len(header)))
    data = json_bytes(index)
    need(len(data) <= MAX_FILE, "bundle manifest exceeds existing file bound")
    (folder / "manifest.json").write_bytes(data)
    return Bundle(folder)

