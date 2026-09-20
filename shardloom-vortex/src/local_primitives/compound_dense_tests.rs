use super::*;

#[test]
fn dense_compound_pages_preserve_collisions_updates_and_exact_replay() {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let partitions = CompoundPartitions::try_new(&memory, 10_000, 7, true)
        .unwrap()
        .unwrap();
    let mut reference = BTreeMap::new();
    for reverse in [false, true] {
        let mut keys = (0_i64..2065).collect::<Vec<_>>();
        if reverse {
            keys.reverse();
        }
        let text = keys
            .iter()
            .map(|key| format!("東京-{key}-α"))
            .collect::<Vec<_>>();
        let refs = text.iter().map(String::as_str).collect::<Vec<_>>();
        let mut counted = partial(&integers(&keys), &strings(&refs), &memory);
        counted.force_collision_hashes();
        let receipt = partitions.reduce(counted, &worker()).unwrap();
        assert!(receipt.deferred.is_none());
        for (key, text) in keys.into_iter().zip(text) {
            *reference.entry((i128::from(key), text)).or_insert(0_u64) += 1;
        }
    }
    assert_eq!(partitions.evidence().unwrap().groups, 2065);
    assert_eq!(partitions.evidence().unwrap().rows, 4130);
    assert_eq!(
        selected(&partitions),
        reference
            .iter()
            .take(7)
            .map(|(key, count)| (key.clone(), *count))
            .collect()
    );
    let mut replay = BTreeMap::new();
    partitions
        .replay_and_release(|key, text, count| {
            assert!(
                replay
                    .insert((logical(key), text.to_owned()), count)
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(replay, reference);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dense_distinct_counts_pairs_once_across_domain_and_group_growth() {
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let partitions = CompoundPartitions::try_new_text_distinct(&memory, 100_000, 8)
        .unwrap()
        .unwrap();
    // Text hashes all select the same partition, exercising dense domain pages
    // without relying on a query-specific or test-only runtime hash override.
    let mut text = Vec::new();
    let mut index = 0;
    while text.len() < 2065 {
        let value = format!("域-{index}");
        if compound_count_partial::partition::<PARTITIONS>(compound_count_partial::string_hash(
            value.as_bytes(),
        )) == 0
        {
            text.push(value);
        }
        index += 1;
    }
    let mut keys = Vec::new();
    let mut rows = Vec::new();
    for (index, value) in text.iter().enumerate() {
        for key in 0..=(index % 5) {
            // Three equal rows must still contribute only one distinct pair.
            for _ in 0..3 {
                keys.push(i64::try_from(key).unwrap());
                rows.push(value.as_str());
            }
        }
    }
    let counted = partial(&integers(&keys), &strings(&rows), &memory);
    assert!(
        partitions
            .reduce(counted, &worker())
            .unwrap()
            .deferred
            .is_none()
    );
    let selected = partitions.select_text_distinct(0, &worker()).unwrap();
    assert_eq!(selected.complete_groups, text.len());
    let mut actual = Vec::new();
    partitions
        .visit_text_distinct(&selected, |text, count| {
            actual.push((text.to_owned(), count));
            Ok(())
        })
        .unwrap();
    let mut expected = text
        .into_iter()
        .enumerate()
        .map(|(index, text)| (text, (index % 5 + 1) as u64))
        .collect::<Vec<_>>();
    let order = |a: &(String, u64), b: &(String, u64)| b.1.cmp(&a.1).then(a.0.cmp(&b.0));
    actual.sort_by(order);
    expected.sort_by(order);
    expected.truncate(8);
    assert_eq!(actual, expected);
    drop(selected);
    partitions.release().unwrap();
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
