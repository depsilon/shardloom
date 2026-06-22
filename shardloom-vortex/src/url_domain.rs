pub(crate) fn shardloom_url_domain(value: &str) -> &str {
    let bytes = value.as_bytes();
    let mut start = match ascii_scheme_end(bytes) {
        Some(scheme_end) => scheme_end + 3,
        None if bytes.starts_with(b"//") => 2,
        None => 0,
    };
    let authority_end = bytes[start..]
        .iter()
        .position(|byte| matches!(*byte, b'/' | b'?' | b'#'))
        .map_or(bytes.len(), |offset| start + offset);
    if let Some(userinfo_at) = bytes[start..authority_end]
        .iter()
        .rposition(|byte| *byte == b'@')
    {
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
        return bytes[start..authority_end]
            .iter()
            .position(|byte| *byte == b']')
            .map_or(authority_end, |offset| start + offset + 1);
    }
    bytes[start..authority_end]
        .iter()
        .position(|byte| *byte == b':')
        .map_or(authority_end, |offset| start + offset)
}

fn ascii_scheme_end(bytes: &[u8]) -> Option<usize> {
    let colon = bytes.iter().position(|byte| *byte == b':')?;
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
