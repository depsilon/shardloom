#!/usr/bin/env python3
"""Portable stdlib verifier for inventory.tar.gz and its adjacent manifest."""
from __future__ import annotations
import argparse, gzip, hashlib, json, re, tarfile
from pathlib import Path, PurePosixPath

def sha(data): return hashlib.sha256(data).hexdigest()
def safe(name):
    p=PurePosixPath(name)
    return bool(name) and not p.is_absolute() and ".." not in p.parts and "\\" not in name and p.as_posix()==name
def main():
    if not __debug__:
        raise SystemExit("verification requires Python assertions; do not use -O")
    ap=argparse.ArgumentParser()
    ap.add_argument("archive", nargs="?", default=str(Path(__file__).with_name("inventory.tar.gz")))
    ap.add_argument("--manifest", type=Path)
    a=ap.parse_args(); archive=Path(a.archive); outer=a.manifest or archive.with_name("manifest.json")
    ext=json.loads(outer.read_text()); raw=archive.read_bytes()
    assert ext["schema"]=="shardloom.compact_coverage_inventory.outer_manifest.v1", "unsupported outer manifest schema"
    assert len(raw)==ext["archive_bytes"] and sha(raw)==ext["archive_sha256"], "archive hash/size mismatch"
    with tarfile.open(archive, "r:gz") as tf:
        members=tf.getmembers(); names=[m.name for m in members]
        assert len(names)==len(set(names)), "duplicate archive members"
        assert all(safe(n) and m.isfile() for n,m in zip(names,members)), "unsafe/non-regular archive member"
        by_name={m.name:m for m in members}; assert "MANIFEST.json" in by_name
        inner_bytes=tf.extractfile(by_name["MANIFEST.json"]).read()
        assert sha(inner_bytes)==ext["inner_manifest_sha256"], "inner manifest hash mismatch"
        inner=json.loads(inner_bytes)
        assert inner["schema"]=="shardloom.compact_coverage_inventory.manifest.v1", "unsupported inner manifest schema"
        member_paths=[m["member_path"] for m in inner["members"]]
        assert len(member_paths)==len(set(member_paths)), "duplicate member paths in inner manifest"
        assert inner["member_count_excluding_manifest"]==len(member_paths), "inner manifest payload count mismatch"
        listed={m["member_path"]:m for m in inner["members"]}
        assert set(names)==set(listed)|{"MANIFEST.json"}, "missing or extra archive members"
        for name,record in listed.items():
            data=tf.extractfile(by_name[name]).read()
            assert len(data)==record["bytes"] and sha(data)==record["sha256"], f"member hash mismatch: {name}"
        all_sources=[]
        for registry_name,registry in inner["source_registries"].items():
            entries=registry["entries"]; paths=[e["source_path"] for e in entries]
            assert len(paths)==len(set(paths)), f"duplicate source registry path: {registry_name}"
            assert registry["missing_members"]==[] and registry["negative_members"]==[]
            assert registry["status"]=="complete"
            for entry in entries:
                member=entry["member_path"]
                assert member in listed and listed[member]["sha256"]==entry["sha256"], f"source registry/member mismatch: {member}"
                all_sources.append((registry_name,entry["source_path"]))
        counts=inner["source_registry_counts"]
        assert counts["cac86"]["entries"]==253 and counts["c79f27a"]["entries"]==15
        assert len(all_sources)==268
        assert inner["missing_members"]==[] and inner["negative_members"]==[]
        catalog_path="artifacts/coverage-inventory-r1/catalog-r2.json"
        obligations_path="artifacts/coverage-inventory-r1/vortex-validation-obligations.json"
        catalog=json.loads(tf.extractfile(by_name[catalog_path]).read())
        obligations=json.loads(tf.extractfile(by_name[obligations_path]).read())
        source_maps={name:{e["source_path"]:e["sha256"] for e in reg["entries"]} for name,reg in inner["source_registries"].items()}
        assert source_maps["cac86"]==catalog["source_sha256"], "cac86 source registry differs from catalog-r2"
        expected_c79={e["path"]:e["sha256"] for e in obligations["source_sha256_registry"]}
        assert source_maps["c79f27a"]==expected_c79, "c79 source registry differs from validation obligations"
        assert inner["source_registries"]["cac86"]["commit"]==catalog["inventory_source_commit"]
        assert inner["source_registries"]["c79f27a"]["commit"]==obligations["source_commit"]
        external=inner["external_design_registry"]
        assert len(external)==2
        for entry in external:
            member=entry["member_path"]
            assert member in listed and listed[member]["sha256"]==entry["sha256"], f"external design mismatch: {member}"
        assert {e["original_path"]:e["sha256"] for e in external}==catalog["external_design_sha256"]
        primary_path="artifacts/coverage-inventory-r1/operator-test-primary-r5.json"
        primary=json.loads(tf.extractfile(by_name[primary_path]).read())
        assert primary["unique_source_tests"]==338
        assert primary["unique_test_statuses"]=={"passed":337,"ignored":1}
        assert inner["claim_limits"]["universal_activation_claimed"] is False
        assert inner["claim_limits"]["independent_semantic_reconstruction_claimed"] is False
        assert inner["claim_limits"]["complete_result_replay_claimed"] is False
    assert ext["status"]=="complete_no_negative_or_missing_members"
    assert ext["missing_members"]==[] and ext["negative_members"]==[]
    assert ext["member_count_excluding_manifest"]==len(listed)
    assert ext["source_registry_counts"]==inner["source_registry_counts"]
    print(json.dumps({"status":"verified","archive_bytes":len(raw),"members":len(names),"source_files":268,"source_registry_counts":ext["source_registry_counts"]},indent=2))
    return 0
if __name__=="__main__": raise SystemExit(main())
