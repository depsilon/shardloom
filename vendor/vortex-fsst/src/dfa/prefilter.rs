// SPDX-License-Identifier: Apache-2.0
// ShardLoom modification: conservative compressed-code candidate selection.

//! A necessary-byte filter; positive results always require exact matching.

use fsst::{ESCAPE_CODE, Symbol};

/// The escape byte is always a candidate. Extra code matches can only add work.
#[derive(Clone, Copy, Debug)]
pub(super) enum CodePrefilter {
    Escape,
    One(u8),
    Two(u8, u8),
}

impl CodePrefilter {
    pub(super) fn new(symbols: &[Symbol], lengths: &[u8], needle: &[u8]) -> Option<Self> {
        if symbols.len() != lengths.len()
            || symbols.len() > usize::from(ESCAPE_CODE)
            || lengths.iter().any(|length| !(1..=8).contains(length))
        {
            return None;
        }
        // Fixed 768-byte tables, no heap allocation. Counts saturate at three:
        // more than two symbol codes cannot use the three-byte search primitive
        // while retaining every escape as a possible match.
        let mut counts = [0u8; 256];
        let mut candidates = [[0u8; 2]; 256];
        for (code, (symbol, length)) in symbols.iter().zip(lengths).enumerate() {
            let bytes = symbol.to_u64().to_le_bytes();
            for (index, byte) in bytes[..usize::from(*length)].iter().enumerate() {
                if bytes[..index].contains(byte) {
                    continue;
                }
                let count = &mut counts[usize::from(*byte)];
                if *count < 2 {
                    candidates[usize::from(*byte)][usize::from(*count)] =
                        u8::try_from(code).ok()?;
                }
                *count = count.saturating_add(1).min(3);
            }
        }
        // Iterator::min_by_key retains the first minimum, including repeated
        // pattern bytes. Selection depends only on the exact pattern and table.
        let byte = usize::from(
            *needle
                .iter()
                .min_by_key(|byte| counts[usize::from(**byte)])?,
        );
        match counts[byte] {
            0 => Some(Self::Escape),
            1 => Some(Self::One(candidates[byte][0])),
            2 => Some(Self::Two(candidates[byte][0], candidates[byte][1])),
            _ => None,
        }
    }

    pub(super) fn may_match(self, codes: &[u8]) -> bool {
        match self {
            Self::Escape => memchr::memchr(ESCAPE_CODE, codes).is_some(),
            Self::One(code) => memchr::memchr2(ESCAPE_CODE, code, codes).is_some(),
            Self::Two(first, second) => {
                memchr::memchr3(ESCAPE_CODE, first, second, codes).is_some()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfa::flat_contains::FlatContainsDfa;

    fn symbol(bytes: &[u8]) -> Symbol {
        let mut raw = [0; 8];
        raw[..bytes.len()].copy_from_slice(bytes);
        Symbol::from_slice(&raw)
    }

    #[test]
    fn necessary_byte_never_rejects_exact_matches_including_escapes() {
        // Every length-0..4 stream over symbol, unused, literal and escape bytes.
        // Malformed streams are included to preserve the existing DFA result.
        let symbols = [symbol(b"aa"), symbol(b"bc"), symbol(b"ab"), symbol(b"c")];
        let lengths = [2, 2, 2, 1];
        let alphabet = [0, 1, 2, 3, 7, b'a', b'b', ESCAPE_CODE];
        for needle in [
            b"a".as_slice(),
            b"b",
            b"c",
            b"d",
            b"abc",
            b"aab",
            b"\xff",
            b"a\xff",
        ] {
            let exact = FlatContainsDfa::new(&symbols, &lengths, needle).unwrap();
            let filter = CodePrefilter::new(&symbols, &lengths, needle).unwrap();
            for len in 0usize..=4 {
                for mut number in 0..alphabet.len().pow(u32::try_from(len).unwrap()) {
                    let mut codes = [0; 4];
                    for code in &mut codes[..len] {
                        *code = alphabet[number % alphabet.len()];
                        number /= alphabet.len();
                    }
                    let codes = &codes[..len];
                    assert!(
                        !exact.matches(codes) || filter.may_match(codes),
                        "{needle:?} {codes:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn repeated_symbol_bytes_count_once_and_dense_tables_decline() {
        let symbols = [symbol(b"aaaa"), symbol(b"ba"), symbol(b"ca")];
        assert!(matches!(
            CodePrefilter::new(&symbols[..1], &[4], b"a"),
            Some(CodePrefilter::One(0))
        ));
        assert!(matches!(
            CodePrefilter::new(&symbols[..2], &[4, 2], b"a"),
            Some(CodePrefilter::Two(0, 1))
        ));
        assert!(CodePrefilter::new(&symbols, &[4, 2, 2], b"a").is_none());
        assert!(CodePrefilter::new(&symbols, &[4, 2, 2], b"").is_none());
        assert!(CodePrefilter::new(&symbols, &[4], b"a").is_none());
        assert!(CodePrefilter::new(&symbols[..1], &[9], b"a").is_none());
        assert!(CodePrefilter::new(&symbols[..1], &[0], b"a").is_none());
    }

    #[test]
    fn complete_backing_rejection_skips_construction_without_changing_values() {
        use crate::dfa::FsstMatcher;

        let symbols = [symbol(b"aa"), symbol(b"bc")];
        let lengths = [2, 2];
        let all_codes = [0, 1, 1, 0];
        for pattern in [b"%z%".as_slice(), b"%ab%", b"aa%", b"%%", b"%\\%%"] {
            let original = FsstMatcher::try_new(&symbols, &lengths, pattern, None)
                .unwrap()
                .unwrap();
            let candidate = FsstMatcher::try_new(&symbols, &lengths, pattern, Some(&all_codes))
                .unwrap()
                .unwrap();
            for codes in [&all_codes[..0], &all_codes[..2], &all_codes[2..]] {
                assert_eq!(
                    candidate.matches(codes),
                    original.matches(codes),
                    "{pattern:?} {codes:?}"
                );
            }
            #[cfg(debug_assertions)]
            if pattern == b"%z%" || pattern == b"%\\%%" {
                let work = candidate.observation(3);
                assert_eq!(work.matcher_constructions, 0);
                assert_eq!(work.block_rejections, 1);
                assert_eq!(work.prefilter_calls, 1);
                assert_eq!(work.rejected_rows, 3);
                assert_eq!(work.exact_rows, 0);
            }
        }
        // Escapes always preserve exact verification, even when their literal
        // byte happens to equal a populated code byte or the escape byte itself.
        let escaped = [ESCAPE_CODE, 0, ESCAPE_CODE, ESCAPE_CODE, ESCAPE_CODE, b'z'];
        let original = FsstMatcher::try_new(&symbols, &lengths, b"%z%", None)
            .unwrap()
            .unwrap();
        let candidate = FsstMatcher::try_new(&symbols, &lengths, b"%z%", Some(&escaped))
            .unwrap()
            .unwrap();
        assert!(candidate.matches(&escaped));
        assert_eq!(
            candidate.matches(&escaped[..4]),
            original.matches(&escaped[..4])
        );
        #[cfg(debug_assertions)]
        {
            let work = candidate.observation(2);
            assert_eq!(work.block_rejections, 0);
            assert_eq!(work.matcher_constructions, 1);
            assert_eq!(work.exact_rows, 2);
        }
    }

    #[test]
    fn candidate_bytes_are_lazy_and_offset_errors_propagate() {
        use std::cell::Cell;

        use vortex_error::vortex_err;

        use crate::dfa::FsstMatcher;

        let symbols = [symbol(b"a"), symbol(b"z")];
        let lengths = [1, 1];
        for pattern in [b"a%".as_slice(), b"%", b"%%", b"a_b%", b"exact", b"%a%b%"] {
            FsstMatcher::try_new_with_candidate_codes(
                &symbols,
                &lengths,
                pattern,
                Some(|| panic!("non-filter pattern must not execute offsets")),
            )
            .unwrap();
        }
        let too_long = format!("%{}%", "a".repeat(255));
        assert!(
            FsstMatcher::try_new_with_candidate_codes(
                &symbols,
                &lengths,
                too_long.as_bytes(),
                Some(|| panic!("unsupported contains must not execute offsets")),
            )
            .unwrap()
            .is_none()
        );
        // All three symbols contain the necessary byte, so this table declines
        // filtering and retains the original construction-before-offset order.
        FsstMatcher::try_new_with_candidate_codes(
            &[symbol(b"a"), symbol(b"ba"), symbol(b"ca")],
            &[1, 2, 2],
            b"%a%",
            Some(|| panic!("dense table must not execute offsets early")),
        )
        .unwrap();

        let calls = Cell::new(0);
        let matcher = FsstMatcher::try_new_with_candidate_codes(
            &symbols,
            &lengths,
            b"%z%",
            Some(|| {
                calls.set(calls.get() + 1);
                Ok([0u8].as_slice())
            }),
        )
        .unwrap()
        .unwrap();
        assert_eq!(calls.get(), 1);
        assert!(!matcher.matches(&[0]));
        let result = FsstMatcher::try_new_with_candidate_codes(
            &symbols,
            &lengths,
            b"%z%",
            Some(|| Err(vortex_err!("offset execution witness"))),
        );
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("offset execution witness")
        );
    }
}
