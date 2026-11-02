//! Deterministic local file collections under one generation and memory owner.
//! File collections are not lakehouse tables or filesystem snapshots.

use crate::source_identity::SourceIdentity;
use regex::Regex;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    collections::BTreeSet,
    fs,
    io::Read as _,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) struct DatasetFile {
    pub(crate) path: PathBuf,
    pub(crate) partitions: Vec<(String, Option<String>)>,
}

enum Membership {
    Single,
    Directory(PathBuf),
    Glob {
        root: PathBuf,
        matcher: Regex,
        depth: Option<usize>,
    },
    Manifest(Arc<SourceIdentity>),
}

pub(crate) struct LocalDataset {
    pub(crate) files: Vec<DatasetFile>,
    pub(crate) union_by_name: bool,
    membership: Membership,
    format: String,
    memory: LiveMemoryPool,
    credit: MemoryLease,
}

impl LocalDataset {
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn is_single_file(&self) -> bool {
        matches!(self.membership, Membership::Single)
    }

    pub(crate) fn manifest_identity(&self) -> Option<Arc<SourceIdentity>> {
        if let Membership::Manifest(identity) = &self.membership {
            Some(Arc::clone(identity))
        } else {
            None
        }
    }

    pub(crate) fn admit(
        source: &Path,
        format: &str,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let mut credit = memory.reserve(8192 + source.as_os_str().len() as u64 * 16)?;
        let source = std::path::absolute(source).map_err(failed)?;
        let manifest = source
            .extension()
            .is_some_and(|value| value.eq_ignore_ascii_case("manifest"));
        let (membership, union_by_name) = if manifest {
            let identity = Arc::new(SourceIdentity::capture(&source)?);
            (Membership::Manifest(identity), false)
        } else if source.is_dir() {
            reject_table(&source)?;
            (Membership::Directory(source.clone()), false)
        } else if has_glob(&source) {
            let (root, matcher) = glob(&source, &mut credit)?;
            let depth = (!source.to_string_lossy().contains("**"))
                .then(|| source.components().count() - root.components().count());
            (
                Membership::Glob {
                    root,
                    matcher,
                    depth,
                },
                false,
            )
        } else {
            (Membership::Single, false)
        };
        let mut dataset = Self {
            files: Vec::new(),
            union_by_name,
            membership,
            format: if format == "arrow_ipc" {
                "arrow-ipc"
            } else {
                format
            }
            .into(),
            memory: memory.clone(),
            credit,
        };
        if let Membership::Manifest(identity) = &dataset.membership {
            let identity = Arc::clone(identity);
            dataset.read_manifest(&identity, cancellation)?;
        } else {
            let mut paths = Vec::new();
            let mut path_credit = memory.reserve(0)?;
            let mut push = |path: &Path| {
                grow(&mut path_credit, path_bytes(path)?)?;
                paths.try_reserve(1).map_err(failed)?;
                paths.push(path.to_owned());
                Ok(())
            };
            match &dataset.membership {
                Membership::Single => push(&source)?,
                Membership::Directory(root) => {
                    walk(root, format, None, None, memory, cancellation, &mut push)?;
                }
                Membership::Glob {
                    root,
                    matcher,
                    depth,
                } => walk(
                    root,
                    format,
                    Some(matcher),
                    *depth,
                    memory,
                    cancellation,
                    &mut push,
                )?,
                Membership::Manifest(_) => unreachable!(),
            }
            dataset.credit.absorb(&mut path_credit)?;
            dataset.files = paths
                .into_iter()
                .map(|path| DatasetFile {
                    path,
                    partitions: Vec::new(),
                })
                .collect();
        }
        dataset
            .files
            .sort_by(|left, right| left.path.cmp(&right.path));
        if dataset.files.is_empty() {
            return Err(failed("source collection has no admitted files"));
        }
        if dataset
            .files
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path)
        {
            return Err(failed("source collection has duplicate paths"));
        }
        dataset.validate(cancellation)?;
        Ok(dataset)
    }

    #[allow(clippy::too_many_lines)]
    fn read_manifest(
        &mut self,
        identity: &Arc<SourceIdentity>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut file = identity.open_checked()?;
        let len = file.metadata().map_err(failed)?.len();
        // Parsing workspace is charged before any content or JSON allocation.
        let _parse = self.memory.reserve(
            len.checked_mul(32)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("manifest workspace overflow"))?,
        )?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(usize::try_from(len).map_err(failed)?)
            .map_err(failed)?;
        for _ in 0..len.div_ceil(65_536) {
            cancellation.check()?;
            let remaining = len.saturating_sub(bytes.len() as u64).min(65_536);
            file.by_ref()
                .take(remaining)
                .read_to_end(&mut bytes)
                .map_err(failed)?;
        }
        identity.validate()?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(failed)?;
        let object = value
            .as_object()
            .ok_or_else(|| failed("manifest must be an object"))?;
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "version" | "format" | "files" | "schema" | "partitioning"
            )
        }) {
            return Err(failed("manifest contains an unknown field"));
        }
        if value["version"].as_u64() != Some(1)
            || value["format"].as_str() != Some(self.format.as_str())
        {
            return Err(failed(
                "manifest version or format differs from the declared source",
            ));
        }
        self.union_by_name = match value.get("schema").and_then(serde_json::Value::as_str) {
            None if !object.contains_key("schema") => false,
            Some("strict") => false,
            Some("union_by_name") => true,
            _ => return Err(failed("manifest schema must be strict or union_by_name")),
        };
        let hive = match value
            .get("partitioning")
            .and_then(serde_json::Value::as_str)
        {
            None if !object.contains_key("partitioning") => false,
            Some("none") => false,
            Some("hive") => true,
            _ => return Err(failed("manifest partitioning must be none or hive")),
        };
        let entries = value["files"]
            .as_array()
            .ok_or_else(|| failed("manifest files must be an array"))?;
        let base = identity
            .path()
            .parent()
            .ok_or_else(|| failed("manifest has no parent directory"))?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            cancellation.check()?;
            let raw = entry
                .as_str()
                .ok_or_else(|| failed("manifest paths must be strings"))?;
            if raw.is_empty() || raw.contains("://") || has_glob(Path::new(raw)) {
                return Err(failed("manifest entries must be explicit local file paths"));
            }
            grow(&mut self.credit, raw.len() as u64 * 16 + path_bytes(base)?)?;
            let path = std::path::absolute(base.join(raw)).map_err(failed)?;
            let resolved = path.canonicalize().map_err(failed)?;
            if !path.is_file() || !matches_extension(&path, &self.format) {
                return Err(failed(
                    "manifest entry must be a regular file of the declared format",
                ));
            }
            if !identities.insert(resolved.clone()) {
                return Err(failed("manifest contains duplicate resolved file paths"));
            }
            let partitions = if hive {
                // Resolve aliases and parent components before deriving
                // partitions; lexical containment accepts `base/../outside`.
                hive_values(&base.canonicalize().map_err(failed)?, &resolved)?
            } else {
                Vec::new()
            };
            self.files.try_reserve(1).map_err(failed)?;
            self.files.push(DatasetFile { path, partitions });
        }
        if let Some(first) = self.files.first() {
            for file in &self.files[1..] {
                if !file
                    .partitions
                    .iter()
                    .map(|(name, _)| name)
                    .eq(first.partitions.iter().map(|(name, _)| name))
                {
                    return Err(failed("manifest files have different Hive partition keys"));
                }
            }
        }
        cancellation.check()?;
        identity.validate()
    }

    pub(crate) fn validate(&self, cancellation: &CancellationToken) -> Result<()> {
        cancellation.check()?;
        let mut count = 0_usize;
        let mut visit = |path: &Path| {
            if self
                .files
                .binary_search_by(|file| file.path.as_path().cmp(path))
                .is_err()
            {
                return Err(failed(
                    "source collection membership changed; prepare the source again",
                ));
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| failed("collection size overflow"))?;
            Ok(())
        };
        match &self.membership {
            Membership::Single => return Ok(()),
            Membership::Manifest(identity) => {
                identity.validate()?;
                let base = identity
                    .path()
                    .parent()
                    .ok_or_else(|| failed("manifest has no parent"))?;
                for file in &self.files {
                    cancellation.check()?;
                    if !file.partitions.is_empty()
                        && hive_values(
                            &base.canonicalize().map_err(failed)?,
                            &file.path.canonicalize().map_err(failed)?,
                        )? != file.partitions
                    {
                        return Err(failed(
                            "source Hive partition paths changed; prepare the source again",
                        ));
                    }
                }
                return Ok(());
            }
            Membership::Directory(root) => walk(
                root,
                &self.format,
                None,
                None,
                &self.memory,
                cancellation,
                &mut visit,
            )?,
            Membership::Glob {
                root,
                matcher,
                depth,
            } => walk(
                root,
                &self.format,
                Some(matcher),
                *depth,
                &self.memory,
                cancellation,
                &mut visit,
            )?,
        }
        if count != self.files.len() {
            return Err(failed(
                "source collection membership changed; prepare the source again",
            ));
        }
        cancellation.check()
    }

    pub(crate) fn validate_destination(&self, path: &Path) -> Result<()> {
        let path = std::path::absolute(path).map_err(failed)?;
        let conflict = match &self.membership {
            Membership::Single => false,
            Membership::Manifest(identity) => {
                path == identity.path()
                    || path.canonicalize().ok()
                        == Some(identity.path().canonicalize().map_err(failed)?)
            }
            Membership::Directory(root) => {
                matches_extension(&path, &self.format)
                    && (path.starts_with(root)
                        || path
                            .parent()
                            .and_then(|p| p.canonicalize().ok())
                            .is_some_and(|p| {
                                root.canonicalize().is_ok_and(|root| p.starts_with(root))
                            }))
            }
            Membership::Glob { root, matcher, .. } => {
                let resolved_parent = path.parent().and_then(|p| p.canonicalize().ok());
                let rebased = resolved_parent.as_ref().and_then(|parent| {
                    let canonical_root = root.canonicalize().ok()?;
                    let relative = parent.strip_prefix(canonical_root).ok()?;
                    Some(root.join(relative).join(path.file_name()?))
                });
                matches_extension(&path, &self.format)
                    && (matcher.is_match(&path.to_string_lossy())
                        || rebased.is_some_and(|p| matcher.is_match(&p.to_string_lossy())))
            }
        };
        if conflict {
            return Err(failed(
                "output must not change the input collection or its membership",
            ));
        }
        Ok(())
    }
}

pub(crate) fn matches_extension(path: &Path, format: &str) -> bool {
    let mut path = path;
    let unwrapped;
    if matches!(format, "csv" | "tsv" | "json" | "jsonl" | "ndjson")
        && path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "gz" | "zst" | "zstd")
            })
    {
        unwrapped = path.with_extension("");
        path = &unwrapped;
    }
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match format {
        "arrow_ipc" | "arrow-ipc" => matches!(
            extension.as_str(),
            "arrow" | "ipc" | "feather" | "arrow-ipc"
        ),
        "jsonl" | "ndjson" => matches!(extension.as_str(), "jsonl" | "ndjson"),
        other => extension == other,
    }
}

fn has_glob(path: &Path) -> bool {
    path.as_os_str()
        .as_encoded_bytes()
        .iter()
        .any(|b| matches!(b, b'*' | b'?' | b'[' | b']'))
}

fn glob(source: &Path, credit: &mut MemoryLease) -> Result<(PathBuf, Regex)> {
    let raw = source
        .to_str()
        .ok_or_else(|| failed("glob paths must be UTF-8"))?;
    if raw.contains(['[', ']']) {
        return Err(failed("local globs admit only *, ? and ** wildcards"));
    }
    grow(credit, raw.len() as u64 * 128 + 8192)?;
    let mut root = PathBuf::new();
    for component in source.components() {
        if has_glob(Path::new(component.as_os_str())) {
            break;
        }
        root.push(component);
    }
    let mut expression = String::from("^");
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    expression.push_str("(?:.*/)?");
                } else {
                    expression.push_str(".*");
                }
            }
            '*' => expression.push_str("[^/]*"),
            '?' => expression.push_str("[^/]"),
            ch => expression.push_str(&regex::escape(&ch.to_string())),
        }
    }
    expression.push('$');
    Ok((root, Regex::new(&expression).map_err(failed)?))
}

struct Frame {
    entries: fs::ReadDir,
    canonical: PathBuf,
    path_bytes: usize,
    _credit: MemoryLease,
}

fn push_directory(
    path: &Path,
    stack: &mut Vec<Frame>,
    memory: &LiveMemoryPool,
    credit: &mut MemoryLease,
) -> Result<()> {
    let held = memory.reserve(path_bytes(path)?)?;
    reject_table(path)?;
    let canonical = path.canonicalize().map_err(failed)?;
    if stack.iter().any(|frame| frame.canonical == canonical) {
        return Err(failed("source collection contains a directory link cycle"));
    }
    if stack.len() == stack.capacity() {
        let additional = stack.capacity().max(4);
        grow(
            credit,
            additional as u64 * std::mem::size_of::<Frame>() as u64 * 2,
        )?;
        stack.try_reserve_exact(additional).map_err(failed)?;
    }
    stack.push(Frame {
        entries: fs::read_dir(path).map_err(failed)?,
        canonical,
        path_bytes: path.as_os_str().len(),
        _credit: held,
    });
    Ok(())
}

fn walk(
    root: &Path,
    format: &str,
    matcher: Option<&Regex>,
    depth: Option<usize>,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    visit: &mut impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    let mut credit = memory.reserve(0)?;
    let mut stack = Vec::new();
    push_directory(root, &mut stack, memory, &mut credit)?;
    while let Some(frame) = stack.last_mut() {
        cancellation.check()?;
        let Some(entry) = frame.entries.next() else {
            stack.pop();
            continue;
        };
        let entry = entry.map_err(failed)?;
        let _path_credit = memory
            .reserve((frame.path_bytes as u64 + entry.file_name().len() as u64 + 1) * 16 + 8192)?;
        let path = entry.path();
        if entry.file_type().map_err(failed)?.is_symlink() {
            let selected_file = matches_extension(&path, format)
                && matcher.is_none_or(|m| m.is_match(&path.to_string_lossy()));
            let traversable_directory =
                depth.is_none_or(|limit| stack.len() < limit) && path.is_dir();
            if !selected_file && !traversable_directory {
                // Unmatched files and directories outside the glob's depth
                // cannot affect membership; never open their targets.
                continue;
            }
            return Err(failed(
                "directory collections do not follow symbolic-link entries; use explicit manifest file paths",
            ));
        }
        let metadata = entry.metadata().map_err(failed)?;
        if metadata.is_dir() {
            if depth.is_none_or(|depth| stack.len() < depth) {
                push_directory(&path, &mut stack, memory, &mut credit)?;
            }
        } else if metadata.is_file()
            && matches_extension(&path, format)
            && matcher.is_none_or(|m| m.is_match(&path.to_string_lossy()))
        {
            visit(&path)?;
        }
    }
    cancellation.check()
}

fn reject_table(path: &Path) -> Result<()> {
    let iceberg = path.join("metadata");
    let iceberg_metadata = if iceberg.is_dir() {
        fs::read_dir(&iceberg)
            .map_err(failed)?
            .try_fold(false, |found, entry| {
                let entry = entry.map_err(failed)?;
                Ok::<_, ShardLoomError>(
                    found
                        || entry
                            .file_name()
                            .to_string_lossy()
                            .ends_with(".metadata.json"),
                )
            })?
    } else {
        false
    };
    if ["_delta_log", ".hoodie"]
        .iter()
        .any(|name| path.join(name).exists())
        || path.join("metadata/version-hint.text").exists()
        || iceberg_metadata
    {
        return Err(failed(
            "table metadata requires an admitted table-state adapter; raw file enumeration is not table execution",
        ));
    }
    Ok(())
}

fn hive_values(base: &Path, path: &Path) -> Result<Vec<(String, Option<String>)>> {
    let relative = path
        .strip_prefix(base)
        .map_err(|_| failed("Hive partition paths must be inside the manifest directory"))?;
    let mut values = Vec::new();
    for component in relative.parent().into_iter().flat_map(Path::components) {
        let text = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| failed("Hive partition paths must be UTF-8"))?;
        if let Some((name, value)) = text.split_once('=') {
            if name.is_empty() || values.iter().any(|(key, _)| key == name) {
                return Err(failed("Hive partition keys must be nonempty and unique"));
            }
            values.push((
                name.to_owned(),
                if value == "__HIVE_DEFAULT_PARTITION__" {
                    None
                } else {
                    Some(percent_decode(value)?)
                },
            ));
        }
    }
    Ok(values)
}

fn percent_decode(raw: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(raw.len());
    let mut input = raw.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let high = input
                .next()
                .and_then(|b| char::from(b).to_digit(16))
                .ok_or_else(|| failed("invalid Hive percent escape"))?;
            let low = input
                .next()
                .and_then(|b| char::from(b).to_digit(16))
                .ok_or_else(|| failed("invalid Hive percent escape"))?;
            bytes.push(u8::try_from(high * 16 + low).map_err(failed)?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).map_err(failed)
}

fn path_bytes(path: &Path) -> Result<u64> {
    (path.as_os_str().len() as u64)
        .checked_mul(16)
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(|| failed("source path capacity overflow"))
}

fn grow(credit: &mut MemoryLease, bytes: u64) -> Result<()> {
    credit.resize(
        credit
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| failed("source metadata capacity overflow"))?,
    )
}

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local dataset: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::fs::symlink,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "shardloom-dataset-boundary-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn flat_glob_ignores_unrelated_links_and_rejects_selected_links() {
        let fixture = Fixture::new();
        fs::write(fixture.0.join("a.csv"), "id\n1\n").unwrap();
        symlink(fixture.0.join("missing"), fixture.0.join("unrelated.link")).unwrap();
        symlink(&fixture.0, fixture.0.join("unrelated-directory")).unwrap();
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let token = CancellationToken::default();
        let dataset =
            LocalDataset::admit(&fixture.0.join("?.csv"), "csv", &memory, &token).unwrap();
        assert_eq!(dataset.files.len(), 1);
        dataset.validate(&token).unwrap();
        symlink(fixture.0.join("a.csv"), fixture.0.join("b.csv")).unwrap();
        assert!(
            dataset
                .validate(&token)
                .err()
                .unwrap()
                .to_string()
                .contains("symbolic-link")
        );
        drop(dataset);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn hive_manifest_rejects_parent_and_symlink_escape_before_partition_values() {
        let fixture = Fixture::new();
        let base = fixture.0.join("dataset");
        let outside = fixture.0.join("region=outside");
        fs::create_dir(&base).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("part.csv"), "id\n1\n").unwrap();
        symlink(&outside, base.join("region=alias")).unwrap();
        let manifest = base.join("source.manifest");
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        for file in ["../region=outside/part.csv", "region=alias/part.csv"] {
            fs::write(
                &manifest,
                serde_json::to_vec(&serde_json::json!({
                    "version":1, "format":"csv", "files":[file], "partitioning":"hive"
                }))
                .unwrap(),
            )
            .unwrap();
            let error =
                LocalDataset::admit(&manifest, "csv", &memory, &CancellationToken::default())
                    .err()
                    .unwrap()
                    .to_string();
            assert!(error.contains("inside the manifest directory"), "{error}");
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn glob_destination_guard_resolves_parent_aliases() {
        let fixture = Fixture::new();
        let base = fixture.0.join("source");
        fs::create_dir(&base).unwrap();
        fs::write(base.join("a.csv"), "id\n1\n").unwrap();
        let alias = fixture.0.join("alias");
        symlink(&base, &alias).unwrap();
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let dataset = LocalDataset::admit(
            &base.join("*.csv"),
            "csv",
            &memory,
            &CancellationToken::default(),
        )
        .unwrap();
        assert!(
            dataset
                .validate_destination(&alias.join("new.csv"))
                .is_err()
        );
        assert!(
            dataset
                .validate_destination(&alias.join("new.vortex"))
                .is_ok()
        );
    }
}
