use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::collections::BTreeMap;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{PrimitiveArray, VarBinViewArray},
    validity::Validity,
};

fn counted(keys: Vec<i64>, text: &[String], memory: &LiveMemoryPool) -> CompoundPartial {
    let numeric = PrimitiveArray::new(keys, Validity::NonNullable).into_array();
    let text = VarBinViewArray::from_iter_str(text.iter().map(String::as_str)).into_array();
    let mut lease = memory
        .reserve(CompoundPartial::bytes(numeric.len()).unwrap())
        .unwrap();
    let mut partial = compound_count_partial::count(
        &numeric,
        &text,
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap();
    partial.force_collision_hashes();
    partial
}

#[test]
fn reducer_denial_after_directory_or_page_growth_preserves_exact_prefix_and_domains() {
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut observed_grown_directory_denial = false;
    let mut observed_grown_payload_denial = false;
    for available in [0, 1, 512, 1791, 1792, 2048, 2303, 2304, 4096, 8192] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let partitions = CompoundPartitions::try_new(&memory, 1000, 3, true)
            .unwrap()
            .unwrap();
        let seed_text = (0..16).map(|i| format!("seed-{i}")).collect::<Vec<_>>();
        let seed = counted((0..16).collect(), &seed_text, &memory);
        assert!(partitions.reduce(seed, &worker).unwrap().deferred.is_none());
        let next_text = vec!["α".repeat(1024), "東京".repeat(1024)];
        let next = counted(vec![16, 17], &next_text, &memory);
        let before = memory.snapshot().reserved_bytes;
        let held = memory.reserve((2 << 20) - before - available).unwrap();
        let receipt = partitions.reduce(next, &worker).unwrap();
        {
            let p = partitions.partitions[0].lock().unwrap();
            // Every string is unique here: no failed append may intern a domain
            // without committing its complete pair and advancing input weight.
            assert_eq!(p.text.len(), p.groups.len());
            if receipt.deferred.is_some() && p.group_slots.len() > 32 {
                observed_grown_directory_denial = true;
                if memory.snapshot().reserved_bytes - held.bytes() > before + 512 {
                    observed_grown_payload_denial = true;
                }
            }
        }
        let evidence = partitions.evidence().unwrap();
        let mut actual = BTreeMap::new();
        partitions
            .replay_and_release(|key, text, count| {
                assert!(actual.insert((key.bits, text.to_owned()), count).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual.values().sum::<u64>(), evidence.rows);
        assert_eq!(actual.len(), evidence.groups);
        if let Some(suffix) = receipt.deferred.as_ref() {
            suffix
                .visit(|key, text, count| {
                    assert!(actual.insert((key.bits, text.to_owned()), count).is_none());
                    Ok(())
                })
                .unwrap();
        }
        let expected = seed_text
            .into_iter()
            .chain(next_text)
            .enumerate()
            .map(|(i, text)| ((i as u64, text), 1))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
        drop(receipt);
        drop(partitions);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(observed_grown_directory_denial && observed_grown_payload_denial);
}

#[test]
fn cancelled_dense_selection_releases_committed_pages_and_selection_credit() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let partitions = CompoundPartitions::try_new(&memory, 10_000, 7, true)
        .unwrap()
        .unwrap();
    let text = (0..2065).map(|i| format!("value-{i}")).collect::<Vec<_>>();
    let partial = counted((0..2065).collect(), &text, &memory);
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    assert!(
        partitions
            .reduce(partial, &worker)
            .unwrap()
            .deferred
            .is_none()
    );
    assert_eq!(partitions.evidence().unwrap().groups, 2065);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(
        partitions
            .select(0, &ChunkWorkerContext::Inline(cancelled))
            .is_err()
    );
    // Cancellation returns an error, never a partial Top-K result. Dropping
    // already committed multi-page state must release every remaining owner.
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
