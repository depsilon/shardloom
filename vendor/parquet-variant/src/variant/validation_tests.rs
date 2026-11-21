// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

const METADATA: &[u8] = &[0x11, 2, 0, 1, 2, b'a', b'b'];

fn list(children: &[&[u8]]) -> Vec<u8> {
    let mut bytes = vec![15, u8::try_from(children.len()).unwrap()];
    let mut offset = 0_u32;
    bytes.extend_from_slice(&offset.to_le_bytes());
    for child in children {
        offset += u32::try_from(child.len()).unwrap();
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    for child in children {
        bytes.extend_from_slice(child);
    }
    bytes
}

fn object(children: &[&[u8]], reversed_storage: bool) -> Vec<u8> {
    let mut bytes = vec![14, u8::try_from(children.len()).unwrap()];
    bytes.extend(0..u8::try_from(children.len()).unwrap());
    let mut offsets = Vec::new();
    let mut storage = Vec::new();
    for child in
        children
            .iter()
            .copied()
            .rev()
            .take(if reversed_storage { children.len() } else { 0 })
    {
        offsets.push(u32::try_from(storage.len()).unwrap());
        storage.extend_from_slice(child);
    }
    if reversed_storage {
        offsets.reverse();
    } else {
        for child in children {
            offsets.push(u32::try_from(storage.len()).unwrap());
            storage.extend_from_slice(child);
        }
    }
    offsets.push(u32::try_from(storage.len()).unwrap());
    for offset in offsets {
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    bytes.extend(storage);
    bytes
}

fn snapshot(value: Variant<'_, '_>) -> Vec<String> {
    let mut values = vec![value];
    let mut result = Vec::new();
    while let Some(value) = values.pop() {
        match value {
            Variant::Object(object) => {
                result.push(format!(
                    "object:{}:{}:{}:{:?}",
                    object.len(),
                    object.is_fully_validated(),
                    object.metadata.is_fully_validated(),
                    object.value
                ));
                let children = object.iter().collect::<Vec<_>>();
                for (name, child) in children.into_iter().rev() {
                    result.push(format!("key:{name}"));
                    values.push(child);
                }
            }
            Variant::List(list) => {
                result.push(format!(
                    "list:{}:{}:{}:{:?}",
                    list.len(),
                    list.is_fully_validated(),
                    list.metadata.is_fully_validated(),
                    list.value
                ));
                values.extend(list.iter().collect::<Vec<_>>().into_iter().rev());
            }
            leaf => result.push(format!("leaf:{leaf:?}")),
        }
    }
    result
}

fn parity(metadata: &[u8], bytes: &[u8]) {
    let Ok(metadata) = VariantMetadata::try_new_with_shallow_validation(metadata) else {
        return;
    };
    let expected = outcome(|| Variant::try_new_with_metadata_reference(metadata.clone(), bytes));
    let actual = outcome(|| Variant::try_new_with_metadata(metadata.clone(), bytes));
    assert_eq!(actual, expected, "encoded bytes: {bytes:?}");
    if let Ok(metadata) = metadata.with_full_validation() {
        let expected =
            outcome(|| Variant::try_new_with_metadata_reference(metadata.clone(), bytes));
        let actual = outcome(|| Variant::try_new_with_metadata(metadata, bytes));
        assert_eq!(actual, expected, "validated metadata, bytes: {bytes:?}");
    }
}

fn outcome<'m, 'v>(
    read: impl FnOnce() -> Result<Variant<'m, 'v>, ArrowError> + std::panic::UnwindSafe,
) -> Result<Result<Vec<String>, String>, String> {
    std::panic::catch_unwind(|| read().map(snapshot).map_err(|error| error.to_string())).map_err(
        |panic| {
            if let Some(message) = panic.downcast_ref::<String>() {
                message.clone()
            } else if let Some(message) = panic.downcast_ref::<&str>() {
                (*message).to_owned()
            } else {
                "non-string panic".to_owned()
            }
        },
    )
}

#[test]
fn full_validation_continuations_match_original_values_and_malformed_order() {
    let metadata = [
        METADATA,
        &[1, 2, 0, 1, 2, b'a', b'a'], // unsorted dictionary accepts duplicates here
        &[0x11, 2, 0, 1, 2, b'b', b'a'],
        &[1, 2, 0, 2, 1, b'a', b'b'],
        &[1, 2, 0, 1, 2, 0xff, b'b'],
        &[1, 0, 0],
    ];
    let leaves = [
        vec![0],
        vec![4],
        vec![12, 42],
        vec![9, b'h', b'i'],
        vec![28, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f],
    ];
    let mut corpus = vec![Vec::new()];
    for first in 0..=u8::MAX {
        for size in [0, 1, 3, 8, 17] {
            let mut bytes = vec![first];
            bytes.extend((0..size).map(|i| (i * 17 + 1) as u8));
            corpus.push(bytes);
        }
    }
    for leaf in leaves {
        corpus.push(leaf.clone());
        let inner = object(&[&leaf, &[0]], true);
        let nested = list(&[&inner, &leaf]);
        corpus.push(inner);
        corpus.push(nested.clone());
        corpus.push(object(&[&nested, &leaf], false));
        for index in 0..nested.len() {
            for replacement in [0, 1, 2, 3, 0x7f, 0xff] {
                let mut malformed = nested.clone();
                malformed[index] = replacement;
                corpus.push(malformed);
            }
            corpus.push(nested[..index].to_vec());
        }
    }
    for bytes in corpus {
        for metadata in metadata {
            parity(metadata, &bytes);
        }
    }
    // All object names must be checked before the first malformed child.
    let mut bad_names_and_child = object(&[&[0xff], &[0]], false);
    bad_names_and_child[3] = 0;
    assert!(
        Variant::try_new(METADATA, &bad_names_and_child)
            .unwrap_err()
            .to_string()
            .contains("field names not sorted")
    );
    // The first child's invalid tag must win over a later invalid list range.
    let mut child_before_offset = list(&[&[0xfc], &[0], &[0]]);
    child_before_offset[10..14].copy_from_slice(&0_u32.to_le_bytes());
    parity(METADATA, &child_before_offset);
    assert!(
        Variant::try_new(METADATA, &child_before_offset)
            .unwrap_err()
            .to_string()
            .contains("unknown primitive type: 63")
    );
}

#[test]
fn full_validation_preserves_public_entries_lazy_children_and_validated_noops() {
    let inner = list(&[&[0]]);
    let object_bytes = object(&[&inner, &[4]], true);
    let list_bytes = list(&[&object_bytes, &inner]);
    let metadata = VariantMetadata::new(METADATA);
    let object = VariantObject::try_new(metadata.clone(), &object_bytes).unwrap();
    let expected = VariantObject::new(metadata.clone(), &object_bytes)
        .with_full_validation_reference()
        .unwrap();
    assert_eq!(
        snapshot(Variant::Object(object.clone())),
        snapshot(Variant::Object(expected))
    );
    assert!(object.is_fully_validated());
    assert!(object.metadata.is_fully_validated());
    assert!(!object.field(0).unwrap().is_fully_validated());
    assert!(object.try_field(0).unwrap().is_fully_validated());
    assert!(
        object
            .iter_try()
            .all(|value| value.unwrap().1.is_fully_validated())
    );
    let list = VariantList::try_new(metadata.clone(), &list_bytes).unwrap();
    let expected = VariantList::new(metadata.clone(), &list_bytes)
        .with_full_validation_reference()
        .unwrap();
    assert_eq!(
        snapshot(Variant::List(list.clone())),
        snapshot(Variant::List(expected))
    );
    assert!(!list.get(0).unwrap().is_fully_validated());
    assert!(list.try_get(0).unwrap().is_fully_validated());
    assert!(
        list.iter_try()
            .all(|value| value.unwrap().is_fully_validated())
    );
    // Public fields can be changed after validation. The existing no-op contract
    // is preserved even for such callers; this patch does not alter that API.
    let mut changed = object;
    changed.value = &[];
    changed.metadata = metadata.clone();
    let changed = changed.with_full_validation().unwrap();
    assert!(changed.value.is_empty());
    assert!(!changed.metadata.is_fully_validated());
    let mut changed = list;
    changed.value = &[];
    changed.metadata = metadata;
    let changed = changed.with_full_validation().unwrap();
    assert!(changed.value.is_empty());
    assert!(!changed.metadata.is_fully_validated());
    let wide = vec![&[0][..]; 200];
    parity(METADATA, &self::list(&wide));
}

fn deep_bytes(depth: usize, shape: usize, leaf: &[u8]) -> Vec<u8> {
    let mut prefixes = Vec::with_capacity(depth);
    let mut len = leaf.len();
    for level in 0..depth {
        let object = shape == 1 || (shape == 2 && level % 2 == 0);
        let mut header = if object { vec![14, 1, 0] } else { vec![15, 1] };
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&u32::try_from(len).unwrap().to_le_bytes());
        len += header.len();
        prefixes.push(header);
    }
    let mut bytes = Vec::with_capacity(len);
    for prefix in prefixes.into_iter().rev() {
        bytes.extend(prefix);
    }
    bytes.extend_from_slice(leaf);
    bytes
}

#[test]
fn full_validation_continuations_deep_small_stack() {
    const NAME: &str = "variant::validation_tests::full_validation_continuations_deep_small_stack";
    const CHILD: &str = "SHARDLOOM_VARIANT_VALIDATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            for shape in 0..3 {
                let bytes = deep_bytes(8192, shape, &[12, 42]);
                let mut value = Variant::try_new(METADATA, &bytes).unwrap();
                assert!(value.is_fully_validated());
                for _ in 0..8192 {
                    value = match value {
                        Variant::Object(object) => object.field(0).unwrap(),
                        Variant::List(list) => list.get(0).unwrap(),
                        _ => panic!("missing encoded child"),
                    };
                }
                assert_eq!(value, Variant::Int8(42));
                assert!(Variant::try_new(METADATA, &deep_bytes(8192, shape, &[0xff])).is_err());
                let late = list(&[&bytes, &[0xff]]);
                assert!(Variant::try_new(METADATA, &late).is_err());
                // The returned wrapper borrows flat bytes; no descendant owners are
                // recursively destroyed when dropping the root or an intermediate.
                let root = Variant::try_new(METADATA, &bytes).unwrap();
                let alias = root.clone();
                assert!(alias.is_fully_validated());
                assert!(!std::mem::needs_drop::<Variant<'_, '_>>());
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
