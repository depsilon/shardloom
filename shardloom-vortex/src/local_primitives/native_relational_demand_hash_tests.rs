//! Exact hashing and validation stay unchanged when membership asks on demand.

use super::*;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, StructArray, VarBinArray},
    dtype::FieldNames,
    validity::Validity,
};

fn hash(column: &KeyColumn, row: usize) -> Result<(bool, u64)> {
    let mut hash = rustc_hash::FxHasher::default();
    let present = column.hash_into(row, &mut hash)?;
    Ok((present, hash.finish()))
}

#[test]
fn demand_hash_keeps_exact_bytes_nulls_codebooks_and_slices_without_hash_slots() {
    let text = VarBinViewArray::from_iter_nullable_str([
        Some("東京\0é"),
        Some(""),
        None,
        Some("independent long native value λ"),
        Some("東京\0é"),
    ])
    .into_array();
    let binary = VarBinArray::from(vec![
        Some(&b"\xff\0"[..]),
        Some(&b""[..]),
        None,
        Some(&[0xff; 31][..]),
        Some(&b"\xff\0"[..]),
    ])
    .into_array();
    for domain in [text, binary] {
        let encoded = DictArray::try_new(
            PrimitiveArray::from_option_iter([
                Some(4_u8),
                Some(1),
                None,
                Some(2),
                Some(3),
                Some(0),
            ])
            .into_array(),
            domain.clone(),
        )
        .unwrap()
        .into_array();
        for source in [domain, encoded] {
            for array in [
                source.clone(),
                source.slice(1..source.len() - 1).unwrap(),
                source.slice(1..1).unwrap(),
            ] {
                let memory = LiveMemoryPool::new(4096).unwrap();
                let mut execution = vortex::array::array_session().create_execution_ctx();
                let cancellation = CancellationToken::default();
                let demand =
                    KeyColumn::for_membership(&array, &mut execution, &memory, &cancellation)
                        .unwrap();
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
                let eager = KeyColumn::new(&array, &mut execution, &memory, &cancellation).unwrap();
                for row in 0..array.len() {
                    assert_eq!(hash(&demand, row).unwrap(), hash(&eager, row).unwrap());
                    assert_eq!(demand.cell(row).unwrap(), eager.cell(row).unwrap());
                    assert_eq!(
                        demand.compare_at(row, &eager, row).unwrap(),
                        Ordering::Equal
                    );
                }
                assert_eq!(
                    hash(&demand, array.len()).unwrap_err().to_string(),
                    hash(&eager, array.len()).unwrap_err().to_string()
                );
                drop((demand, eager));
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn demand_hash_uses_no_hash_slot_credit_and_keeps_cancellation_and_nested_preparation() {
    let source = VarBinViewArray::from_iter_str(["a", "b"]).into_array();
    let memory = LiveMemoryPool::new(15).unwrap();
    let mut execution = vortex::array::array_session().create_execution_ctx();
    let cancellation = CancellationToken::default();
    let demand =
        KeyColumn::for_membership(&source, &mut execution, &memory, &cancellation).unwrap();
    assert!(hash(&demand, 1).unwrap().0);
    assert!(KeyColumn::new(&source, &mut execution, &memory, &cancellation).is_err());
    drop(demand);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    cancellation.cancel();
    assert!(KeyColumn::for_membership(&source, &mut execution, &memory, &cancellation).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let nested = StructArray::try_new(
        FieldNames::from(["word"]),
        vec![source],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let cancellation = CancellationToken::default();
    #[cfg(debug_assertions)]
    let observation = super::super::native_relational_observation::Session::start();
    let demand =
        KeyColumn::for_membership(&nested, &mut execution, &memory, &cancellation).unwrap();
    hash(&demand, 1).unwrap();
    #[cfg(debug_assertions)]
    {
        let counts = observation.finish();
        assert_eq!(counts.variable_hash_on_demand_values, 0);
        assert_eq!(counts.variable_hash_slot_bytes, 16);
    }
    drop(demand);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
