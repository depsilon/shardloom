// SPDX-License-Identifier: Apache-2.0
// ShardLoom dependency regressions; synthetic inputs only.

use std::io::{Read, Write};

#[test]
fn borrowed_encoder_restarts_after_an_abandoned_stream() {
    let mut context = zstd::zstd_safe::CCtx::create();
    {
        let mut abandoned = zstd::stream::write::Encoder::with_context(Vec::new(), &mut context);
        abandoned
            .write_all(b"first request, abandoned before finish")
            .unwrap();
    }

    let mut next = zstd::stream::write::Encoder::with_context(Vec::new(), &mut context);
    next.write_all(b"second request").unwrap();
    let compressed = next.finish().unwrap();
    assert_eq!(
        zstd::decode_all(compressed.as_slice()).unwrap(),
        b"second request"
    );
}

#[test]
fn borrowed_decoder_restarts_after_partial_delivery() {
    let first = zstd::bulk::compress(&vec![b'a'; 200_000], 1).unwrap();
    let second = zstd::bulk::compress(b"independent second request", 1).unwrap();
    let mut context = zstd::zstd_safe::DCtx::create();
    {
        let mut abandoned =
            zstd::stream::read::Decoder::with_context(first.as_slice(), &mut context);
        let mut prefix = [0_u8; 1];
        abandoned.read_exact(&mut prefix).unwrap();
        assert_eq!(prefix, [b'a']);
    }

    let mut next = zstd::stream::read::Decoder::with_context(second.as_slice(), &mut context);
    let mut delivered = Vec::new();
    next.read_to_end(&mut delivered).unwrap();
    assert_eq!(delivered, b"independent second request");
}

#[test]
fn bulk_decoder_recovers_after_corrupt_input() {
    let mut decoder = zstd::bulk::Decompressor::new().unwrap();
    assert!(decoder.decompress(b"not a compressed frame", 1024).is_err());
    let values = (0_u8..=255).collect::<Vec<_>>();
    let compressed = zstd::bulk::compress(&values, 3).unwrap();
    assert_eq!(
        decoder.decompress(&compressed, values.len()).unwrap(),
        values
    );
}
