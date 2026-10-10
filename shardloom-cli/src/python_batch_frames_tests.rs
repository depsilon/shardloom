use super::*;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::io::{BufReader, Cursor};

fn owner(limit: u64) -> (LiveMemoryPool, Arc<ScratchStats>, Scratch) {
    let pool = LiveMemoryPool::new(limit).unwrap();
    let stats = Arc::new(ScratchStats::default());
    let scratch = Scratch::new(pool.reserve(0).unwrap(), Arc::clone(&stats));
    (pool, stats, scratch)
}

fn frame(text: &str, scratch: &mut Scratch) -> Result<Frame> {
    read(
        &mut BufReader::with_capacity(17, Cursor::new(text.as_bytes())),
        scratch,
        &CancellationToken::default(),
    )?
    .ok_or_else(|| failed("missing test frame"))
}

#[test]
fn wide_frame_preserves_nulls_escapes_order_and_credit_through_conversion() {
    let width = 1025;
    let row = (0..width)
        .map(|index| match index % 3 {
            0 => None,
            1 => Some(format!("東京 λ \" \n {index}")),
            _ => Some(i64::MIN.to_string()),
        })
        .collect::<Vec<_>>();
    let text = serde_json::json!({"kind":"rows","uri":"memory://wide","index":8193,"rows":[row.clone(),row.clone()]}).to_string();
    let (pool, stats, mut scratch) = owner(4 << 20);
    let raw = frame(&text, &mut scratch).unwrap();
    assert!(pool.snapshot().reserved_bytes >= text.len() as u64);
    let mut decoded = raw.decode(Some(width)).unwrap();
    assert!(
        matches!(&decoded.value, Incoming::Rows { uri, index:8193, rows }
        if uri == "memory://wide" && rows.len()==2 && rows.iter().all(|value| value.0 == row))
    );
    let prior = pool.snapshot().reserved_bytes;
    let conversion = decoded.conversion_scratch(128 << 10).unwrap();
    assert_eq!(pool.snapshot().reserved_bytes, prior + (128 << 10));
    assert!(stats.peak() >= pool.snapshot().reserved_bytes);
    drop(conversion);
    assert_eq!(pool.snapshot().reserved_bytes, prior);
    drop(decoded);
    drop(scratch);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert_eq!(stats.live.load(Ordering::Acquire), 0);
}

#[test]
fn successive_frames_and_decoder_overlap_are_all_admitted() {
    let text = serde_json::json!({"kind":"rows","uri":"memory://x","index":0,"rows":[vec![Some("x".repeat(100));257]]}).to_string();
    let (pool, stats, mut scratch) = owner(1 << 20);
    let first = frame(&text, &mut scratch)
        .unwrap()
        .decode(Some(257))
        .unwrap();
    let prior = pool.snapshot().reserved_bytes;
    let next = frame(&text, &mut scratch).unwrap();
    assert!(pool.snapshot().reserved_bytes >= prior + text.len() as u64);
    drop(first);
    assert!(pool.snapshot().reserved_bytes >= text.len() as u64);
    drop(next);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert_eq!(stats.live.load(Ordering::Acquire), 0);
}

#[test]
fn concurrent_scratch_observation_matches_actual_lease_transitions() {
    let (pool, stats, mut scratch) = owner(1 << 20);
    let start = Arc::new(std::sync::Barrier::new(8));
    let handles = (0..8)
        .map(|index| {
            let mut worker = scratch.branch().unwrap();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..2048 {
                    worker.resize((index + 1) * 1024).unwrap();
                    worker.resize(128).unwrap();
                    let mut child = worker.branch().unwrap();
                    child.resize(4096).unwrap();
                    drop(child);
                    worker.resize(0).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert_eq!(stats.live.load(Ordering::Acquire), 0);
    assert_eq!(stats.peak(), pool.snapshot().peak_reserved_bytes);
}

#[test]
fn malformed_large_strings_do_not_expand_diagnostics() {
    let (pool, _, mut scratch) = owner(16 << 20);
    let large = "x".repeat(1 << 20);
    for value in [
        serde_json::json!(large),
        serde_json::json!({"kind":large}),
        serde_json::json!({large.as_str():null}),
        serde_json::json!({"kind":"ack","index":large}),
        serde_json::json!({"kind":"rows","uri":"memory://x","index":0,"rows":large}),
        serde_json::json!({"kind":"rows","uri":"memory://x","index":0,"rows":[large]}),
    ] {
        let mut raw = frame(&value.to_string(), &mut scratch).unwrap();
        assert!(!raw.is_cancel().unwrap());
        let error = raw.decode(Some(1)).err().unwrap().to_string();
        assert!(
            error.len() < 512,
            "diagnostic retained {} input bytes",
            error.len()
        );
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn malformed_messages_and_excess_values_release_credit_without_parsing_extra_payloads() {
    let (pool, _, mut scratch) = owner(1 << 20);
    for text in [
        r#"{"kind":"rows","uri":"memory://x","index":0,"rows":[["x","unterminated"#,
        r#"{"kind":"rows","uri":"memory://x","index":0,"rows":[[]]}"#,
        r#"{"kind":"rows","uri":"memory://x","index":0,"rows":[[1]]}"#,
        r#"{"kind":"rows","uri":"memory://x","index":0,"rows":[[true]]}"#,
        r#"{"kind":"rows","uri":"memory://x","index":0,"rows":[[{}]]}"#,
        r#"{"kind":"ack","index":0,"index":1}"#,
        r#"{"kind":"ack","index":0,"unknown":"unterminated"#,
        r#"{"kind":"ack","index":-1}"#,
        r#"{"kind":"ack","index":true}"#,
        r#"{"kind":"end","uri":"memory://x","index":0,"rows":[]}"#,
        r#"{"kind":"missing"}"#,
    ] {
        let error = frame(text, &mut scratch)
            .unwrap()
            .decode(Some(1))
            .err()
            .unwrap()
            .to_string();
        if text.contains("x\",\"unterminated") {
            assert!(error.contains("exceeds its declared width"), "{error}");
        }
        if text.contains("unknown") {
            assert!(error.contains("unknown field"), "{error}");
        }
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
    let rows = std::iter::repeat_n("[null]", 2048)
        .collect::<Vec<_>>()
        .join(",");
    let text = format!(
        "{{\"kind\":\"rows\",\"uri\":\"memory://x\",\"index\":0,\"rows\":[{rows},[\"unterminated"
    );
    let error = frame(&text, &mut scratch)
        .unwrap()
        .decode(Some(1))
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("exceeds 2048 rows"), "{error}");
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn denial_before_raw_or_decoded_growth_preserves_other_owners() {
    let (pool, _, mut scratch) = owner(32 << 10);
    let retained = pool.reserve(1024).unwrap();
    assert!(frame(&"x".repeat(32 << 10), &mut scratch).is_err());
    assert_eq!(pool.snapshot().reserved_bytes, 1024);
    let text = serde_json::json!({"kind":"rows","uri":"memory://x","index":0,"rows":[vec![None::<String>;2048]]}).to_string();
    assert!(
        frame(&text, &mut scratch)
            .unwrap()
            .decode(Some(2048))
            .is_err()
    );
    assert_eq!(pool.snapshot().reserved_bytes, 1024);
    assert!(pool.snapshot().denied_reservations > 0);
    drop(retained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn cancellation_framing_and_message_order_are_explicit() {
    let (pool, _, mut scratch) = owner(32 << 20);
    let token = CancellationToken::default();
    token.cancel();
    let mut input = Cursor::new(b"{\"kind\":\"ack\",\"index\":0}\n");
    assert!(read(&mut input, &mut scratch, &token).is_err());
    assert_eq!(input.position(), 0);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    assert!(frame(&"x".repeat(MAX_FRAME + 1), &mut scratch).is_err());
    assert_eq!(pool.snapshot().reserved_bytes, 0);
    let mut cancel = frame(r#"{"kind":"can\u0063el"}"#, &mut scratch).unwrap();
    assert!(cancel.is_cancel().unwrap());
    assert!(matches!(
        cancel.decode(None).unwrap().value,
        Incoming::Cancel
    ));
    let reordered = frame(
        r#"{"rows":[["x"]],"index":4097,"uri":"memory://x","kind":"rows"}"#,
        &mut scratch,
    )
    .unwrap();
    assert!(matches!(
        reordered.decode(Some(1)).unwrap().value,
        Incoming::Rows { index: 4097, .. }
    ));
    let unsolicited = frame(r#"{"rows":[["unterminated"#, &mut scratch).unwrap();
    let error = unsolicited.decode(None).err().unwrap().to_string();
    assert!(error.contains("without input demand"), "{error}");
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}
