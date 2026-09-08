use super::*;
use shardloom_exec::compute_pool::CancellationToken;

fn block(credits: &EntryCredits, requested: usize) -> EntryBlock<'_> {
    let Claim::Block(block) = credits.claim(requested, || Ok(true)).unwrap() else {
        panic!("expected admitted credit block");
    };
    block
}

#[test]
fn hard_limits_and_small_claims_never_include_unadmitted_entries() {
    for limit in [0, 1, 1023, 1024, 1025] {
        let credits = EntryCredits::new(limit);
        let mut inserted = 0;
        loop {
            match credits.claim(usize::MAX, || Ok(true)).unwrap() {
                Claim::Block(mut owned) => {
                    let evidence = credits.evidence().unwrap();
                    assert_eq!(
                        evidence.committed + evidence.reserved,
                        (inserted + BLOCK_ENTRIES).min(limit)
                    );
                    assert!(owned.remaining() <= BLOCK_ENTRIES);
                    while owned.remaining() != 0 {
                        owned.consume_one().unwrap();
                        inserted += 1;
                    }
                    assert_eq!(
                        credits.committed(),
                        evidence.committed,
                        "active block is not published per key"
                    );
                }
                Claim::Exhausted => break,
                Claim::Stopped => panic!("unexpected stop"),
            }
        }
        assert_eq!(inserted, limit);
        let evidence = credits.evidence().unwrap();
        assert_eq!(credits.committed(), limit);
        assert_eq!(evidence.reserved, 0);
        assert_eq!(evidence.refunded_entries, 0);
        assert_eq!(evidence.claim_calls, limit.div_ceil(BLOCK_ENTRIES) as u64);
        assert_eq!(evidence.return_calls, evidence.claim_calls);
    }
}

#[test]
fn unused_credits_return_on_success_and_error_with_used_entries_preserved() {
    let credits = EntryCredits::new(8);
    drop(block(&credits, 8)); // A duplicate-only block consumes no distinct entries.
    let result: Result<()> = (|| {
        let mut owned = block(&credits, 8);
        owned.consume_one()?;
        owned.consume_one()?;
        Err(failed("injected insertion failure"))
    })();
    assert!(result.is_err());
    let evidence = credits.evidence().unwrap();
    assert_eq!(evidence.committed, 2);
    assert_eq!(evidence.reserved, 0);
    assert_eq!(evidence.refunded_entries, 14);
    let owned = block(&credits, 8);
    assert_eq!(
        owned.remaining(),
        6,
        "claim shrinks to actual free capacity"
    );
    drop(owned);
}

#[test]
fn temporary_ownership_waits_for_refund_instead_of_reporting_exhaustion() {
    let credits = EntryCredits::new(4);
    let mut holder = block(&credits, 4);
    holder.consume_one().unwrap();
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| {
            let mut owned = block(&credits, 4);
            assert_eq!(owned.remaining(), 3);
            owned.consume_one().unwrap();
        });
        credits.wait_until_blocked();
        assert_eq!(credits.evidence().unwrap().reserved, 4);
        drop(holder);
        waiting.join().unwrap();
    });
    let evidence = credits.evidence().unwrap();
    assert_eq!(evidence.committed, 2);
    assert_eq!(evidence.reserved, 0);
    assert_eq!(evidence.refunded_entries, 5);
    assert!(evidence.wait_calls > 0);
}

#[test]
fn waiting_cancel_and_pressure_stop_do_not_steal_another_blocks_credits() {
    for cancel in [false, true] {
        let credits = EntryCredits::new(1);
        let holder = block(&credits, 1);
        let token = CancellationToken::default();
        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| {
                credits.claim(1, || {
                    token.check()?;
                    Ok(!stop.load(Ordering::Acquire))
                })
            });
            credits.wait_until_blocked();
            if cancel {
                token.cancel();
            } else {
                stop.store(true, Ordering::Release);
            }
            credits.wake();
            let result = waiting.join().unwrap();
            if cancel {
                assert!(result.is_err());
            } else {
                assert!(matches!(result, Ok(Claim::Stopped)));
            }
            assert_eq!(credits.evidence().unwrap().reserved, 1);
        });
        drop(holder);
        assert_eq!(credits.evidence().unwrap().reserved, 0);
        assert_eq!(credits.evidence().unwrap().refunded_entries, 1);
    }
}

#[test]
fn checked_evidence_failure_still_refunds_before_rejecting_final_evidence() {
    let credits = EntryCredits::new(4);
    let mut owned = block(&credits, 4);
    owned.consume_one().unwrap();
    credits.state.lock().unwrap().evidence.return_calls = u64::MAX;
    drop(owned);
    let state = credits.state.lock().unwrap();
    assert_eq!(state.evidence.committed, 1);
    assert_eq!(state.evidence.reserved, 0);
    drop(state);
    assert!(credits.evidence().is_err());
    assert!(credits.claim(1, || Ok(true)).is_err());
}
