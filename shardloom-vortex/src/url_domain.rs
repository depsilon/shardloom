pub(crate) fn shardloom_url_domain(value: &str) -> &str {
    let bytes = value.as_bytes();
    let mut start = match ascii_scheme_end(bytes) {
        Some(scheme_end) => scheme_end + 3,
        None if bytes.starts_with(b"//") => 2,
        None => 0,
    };
    let authority_end = memchr::memchr3(b'/', b'?', b'#', &bytes[start..])
        .map_or(bytes.len(), |offset| start + offset);
    if let Some(userinfo_at) = memchr::memrchr(b'@', &bytes[start..authority_end]) {
        start += userinfo_at + 1;
    }
    if ascii_starts_with_ignore_case(&bytes[start..authority_end], b"www.") {
        start += 4;
    }
    let host_end = host_end_without_port(bytes, start, authority_end);
    &value[start..host_end]
}

fn host_end_without_port(bytes: &[u8], start: usize, authority_end: usize) -> usize {
    if bytes.get(start) == Some(&b'[') {
        return memchr::memchr(b']', &bytes[start..authority_end])
            .map_or(authority_end, |offset| start + offset + 1);
    }
    memchr::memchr(b':', &bytes[start..authority_end])
        .map_or(authority_end, |offset| start + offset)
}

fn ascii_scheme_end(bytes: &[u8]) -> Option<usize> {
    let colon = memchr::memchr(b':', bytes)?;
    if bytes.get(colon + 1..colon + 3) == Some(b"//") && is_ascii_scheme(&bytes[..colon]) {
        Some(colon)
    } else {
        None
    }
}

fn is_ascii_scheme(bytes: &[u8]) -> bool {
    if bytes.is_empty() || !bytes[0].is_ascii_alphabetic() {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'.' | b'-'))
}

fn ascii_starts_with_ignore_case(value: &[u8], prefix: &[u8]) -> bool {
    value.len() >= prefix.len()
        && value[..prefix.len()]
            .iter()
            .zip(prefix)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

#[cfg(test)]
mod tests {
    use super::shardloom_url_domain;

    fn check_borrowed_domain(input: &str, expected: &str) {
        let actual = shardloom_url_domain(input);
        assert_eq!(actual, expected, "input: {input:?}");
        let source = input.as_ptr().addr()..=input.as_ptr().addr() + input.len();
        assert!(source.contains(&actual.as_ptr().addr()));
        assert!(source.contains(&(actual.as_ptr().addr() + actual.len())));
    }

    #[test]
    fn domain_search_preserves_authority_forms_and_borrowed_payloads() {
        for scheme in ["", "//", "https://", "HTTP://", "a+1.-://", "x://"] {
            for userinfo in ["", "user:password@", "first@last@", "東京🙂@"] {
                for (host, expected) in [
                    ("example.com", "example.com"),
                    ("www.Example.net", "Example.net"),
                    ("WWW.example.org:80", "example.org"),
                    ("münich.example:443", "münich.example"),
                    ("[2001:db8::1]:80", "[2001:db8::1]"),
                    ("www.[::1]:80", "[::1]"),
                    ("[broken", "[broken"),
                    ("", ""),
                ] {
                    for suffix in ["", "/path?q=1#part", "?query/path@x", "#part/path:x"] {
                        check_borrowed_domain(
                            &format!("{scheme}{userinfo}{host}{suffix}"),
                            expected,
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn domain_search_preserves_malformed_and_empty_authorities() {
        for (input, expected) in [
            ("1abc://example.com/path", "1abc"),
            ("ä://example.com/path", "ä"),
            ("http:/example.com/path", "http"),
            ("://example.com", ""),
            ("https:///example.com", ""),
            ("https://www.", ""),
            ("www.www.example:443", "www.example"),
            ("https://[2001:db8::1", "[2001:db8::1"),
            ("https://[broken]extra:80/path", "[broken]"),
            ("mailto:user@example.com", "example.com"),
            ("\0http://www.example.com", "\0http"),
            (
                "https://user@www.東京.example:443/path@other",
                "東京.example",
            ),
            ("?x://www.example.com", ""),
            ("#x://www.example.com", ""),
        ] {
            check_borrowed_domain(input, expected);
        }
    }

    #[test]
    fn domain_search_handles_delimiters_at_short_and_long_byte_boundaries() {
        for width in [
            0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
        ] {
            let host = format!("{}é.example", "a".repeat(width));
            let userinfo = format!("{}東京@", "b".repeat(width));
            for scheme in ["", "https://", "long+valid-scheme://"] {
                for suffix in ["", "/path", "?query", "#part"] {
                    check_borrowed_domain(
                        &format!("{scheme}{userinfo}www.{host}:443{suffix}"),
                        &host,
                    );
                    check_borrowed_domain(&format!("{scheme}{host}{suffix}"), &host);
                }
            }
        }
    }

    #[test]
    fn extracts_domain_with_delimiter_scan_without_allocating() {
        assert_eq!(
            shardloom_url_domain("https://www.example.com:443/path?q=1"),
            "example.com"
        );
        assert_eq!(
            shardloom_url_domain("HTTP://WWW.Google.com/search"),
            "Google.com"
        );
        assert_eq!(shardloom_url_domain("//www.docs.rs/crate"), "docs.rs");
        assert_eq!(
            shardloom_url_domain("example.org?from=search"),
            "example.org"
        );
        assert_eq!(
            shardloom_url_domain("https://user:pass@www.example.net/path"),
            "example.net"
        );
        assert_eq!(
            shardloom_url_domain("https://[2001:db8::1]:8443/path"),
            "[2001:db8::1]"
        );
        assert_eq!(
            shardloom_url_domain("https://user:pass@[2001:db8::2]/path"),
            "[2001:db8::2]"
        );
        assert_eq!(shardloom_url_domain(""), "");
    }
}
