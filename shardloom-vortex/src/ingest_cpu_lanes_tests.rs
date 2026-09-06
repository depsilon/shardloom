use super::*;

fn demand() -> IngestCpuDemand {
    // An explicit candidate recipe for tests, not a new production default.
    IngestCpuDemand {
        source_workers: 1,
        conversion_workers: 1,
        prefetch_slots: 4,
    }
}

#[test]
fn one_two_four_eight_partition_one_grant_without_additive_stage_budgets() {
    for (grant, expected) in [
        (1, (0, 0, 0)),
        (2, (1, 0, 0)),
        (4, (1, 1, 1)),
        (8, (1, 1, 5)),
    ] {
        let lanes = IngestCpuLanes::allocate(grant, 8, demand()).unwrap();
        assert_eq!(lanes.requested(), grant);
        assert_eq!(IngestCpuLanes::CALLER_LANES, 1);
        assert_eq!(lanes.configured_cpu_lanes(), grant);
        assert_eq!(
            (
                lanes.source_workers(),
                lanes.conversion_workers(),
                lanes.provider_drivers()
            ),
            expected
        );
        assert_eq!(lanes.prefetch_slots(), if grant <= 2 { 0 } else { 4 });
        assert_eq!(lanes.source_runs_on_caller(), grant == 1);
    }
}

#[test]
fn stage_caps_and_large_requests_do_not_multiply_or_overflow_the_grant() {
    for grant in [1, 2, 3, 4, 8, 12, usize::MAX] {
        for source_capacity in [0, 1, 2, usize::MAX] {
            let lanes = IngestCpuLanes::allocate(
                grant,
                source_capacity,
                IngestCpuDemand {
                    source_workers: usize::MAX,
                    conversion_workers: usize::MAX,
                    prefetch_slots: 4,
                },
            )
            .unwrap();
            assert_eq!(lanes.configured_cpu_lanes(), grant);
            assert!(lanes.source_workers() <= source_capacity);
            assert!(lanes.conversion_workers() <= 4);
            assert_eq!(lanes.prefetch_slots() == 0, lanes.conversion_workers() == 0);
        }
    }
    assert!(IngestCpuLanes::allocate(0, 1, demand()).is_err());
    assert!(
        IngestCpuLanes::allocate(
            2,
            1,
            IngestCpuDemand {
                prefetch_slots: 0,
                ..demand()
            }
        )
        .is_err()
    );
}

#[test]
fn synchronous_sources_and_conversion_keep_explicit_progress_owners() {
    let conversion = IngestCpuLanes::allocate(2, 0, demand()).unwrap();
    assert_eq!(conversion.source_workers(), 0);
    assert_eq!(conversion.conversion_workers(), 1);
    assert_eq!(conversion.provider_drivers(), 0);
    assert!(!conversion.source_runs_on_caller());
    let writer = IngestCpuLanes::allocate(
        8,
        8,
        IngestCpuDemand {
            source_workers: 0,
            conversion_workers: 0,
            prefetch_slots: 0,
        },
    )
    .unwrap();
    assert!(writer.source_runs_on_caller());
    assert_eq!(writer.provider_drivers(), 7);
}

#[test]
fn adjustment_requires_actual_joined_owners_not_just_an_empty_task_queue() {
    let old = IngestCpuLanes::allocate(8, 8, demand()).unwrap();
    let next = IngestCpuLanes::allocate(2, 8, demand()).unwrap();
    for activity in [
        IngestCpuActivity {
            source_threads: 1,
            ..Default::default()
        },
        IngestCpuActivity {
            conversion_threads: 1,
            ..Default::default()
        },
        IngestCpuActivity {
            provider_threads: 1,
            ..Default::default()
        },
        IngestCpuActivity {
            queued_or_active_jobs: 1,
            ..Default::default()
        },
        IngestCpuActivity {
            retained_unpublished_batches: 1,
            ..Default::default()
        },
    ] {
        assert!(old.validate_replacement(next, activity).is_err());
        assert!(old.validate_replacement(old, activity).is_ok());
    }
    assert!(
        old.validate_replacement(next, IngestCpuActivity::default())
            .is_ok()
    );
    assert_eq!(
        old.provider_drivers(),
        5,
        "validation does not mutate live owners"
    );
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)] // Keep source, caller progress, native reopen and I/O proof together.
fn one_and_two_lanes_write_and_reopen_without_a_background_provider_driver() {
    use std::{sync::mpsc, thread, time::Duration};
    use vortex::{
        VortexSessionDefault as _,
        array::{
            ArrayRef, IntoArray as _, VortexSessionExecute as _,
            arrays::PrimitiveArray,
            dtype::{DType, Nullability, PType},
            iter::ArrayIteratorAdapter,
            scalar::Scalar,
            validity::Validity,
        },
        buffer::ByteBuffer,
        error::{VortexResult, vortex_err},
        file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };
    type Source = Box<dyn Iterator<Item = VortexResult<ArrayRef>> + Send>;
    const BASE: i64 = 1_i64 << 60;
    for grant in [1, 2] {
        let lanes = IngestCpuLanes::allocate(grant, 2, demand()).unwrap();
        let runtime = CurrentThreadRuntime::new();
        // Do not create a pool or invoke set_workers; prove caller progress.
        let session = VortexSession::default().with_handle(runtime.handle());
        assert_eq!(lanes.provider_drivers(), 0);
        assert_eq!(lanes.conversion_workers(), 0);
        let arrays = (0..2)
            .map(|group| {
                PrimitiveArray::new(
                    vec![BASE + group * 2, BASE + group * 2 + 1],
                    Validity::NonNullable,
                )
                .into_array()
            })
            .collect::<Vec<_>>();
        let (source, join): (Source, _) = if lanes.source_workers() == 0 {
            (Box::new(arrays.into_iter().map(Ok)), None)
        } else {
            let (sender, receiver) = mpsc::sync_channel(1);
            let worker = thread::spawn(move || {
                for array in arrays {
                    if sender.send(array).is_err() {
                        break;
                    }
                }
            });
            let mut remaining = 2;
            let iter = std::iter::from_fn(move || {
                if remaining == 0 {
                    return None;
                }
                remaining -= 1;
                Some(
                    receiver
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|error| vortex_err!("bounded source progress failed: {error}")),
                )
            });
            (Box::new(iter), Some(worker))
        };
        let mut output = Vec::new();
        let summary = session
            .write_options()
            .blocking(&runtime)
            .write(
                &mut output,
                ArrayIteratorAdapter::new(
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                    source,
                ),
            )
            .unwrap();
        if let Some(worker) = join {
            worker.join().unwrap();
        }
        assert_eq!(summary.row_count(), 4);
        assert!(output.len() < 65536);
        let file = session
            .open_options()
            .open_buffer(ByteBuffer::from(output))
            .unwrap();
        let mut context = session.create_execution_ctx();
        let mut values = Vec::new();
        for array in file
            .scan()
            .unwrap()
            .with_ordered(true)
            .into_array_iter(&runtime)
            .unwrap()
        {
            let array = array.unwrap();
            for row in 0..array.len() {
                values.push(array.execute_scalar(row, &mut context).unwrap());
            }
        }
        assert_eq!(
            values,
            (0..4)
                .map(|row| Scalar::from(BASE + row))
                .collect::<Vec<_>>()
        );
        // Blocking I/O remains a distinct provider facility; zero CPU drivers
        // must not remove it or prevent the caller from polling its completion.
        assert_eq!(
            runtime.block_on(runtime.handle().spawn_blocking(|| 7_u8)),
            7
        );
    }
}
