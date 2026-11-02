//! Hudi v8/v9 Copy-on-Write over a complete active layout-v2 timeline.

use super::{ColumnMapping, DataFile, Metadata, Result, Snapshot, failed, metadata, schema};
use serde_json::Value;
use shardloom_core::input::LocalTableSelection;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

struct Instant {
    begin: String,
    completed: String,
    action: String,
    path: PathBuf,
}

// Replay and admission must share the same held timeline and resource owner.
#[allow(clippy::too_many_lines)]
pub(super) fn read(
    path: &Path,
    selection: &LocalTableSelection,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Snapshot> {
    let root = path.canonicalize().map_err(failed)?;
    if !root.is_dir() {
        return Err(failed("Hudi input must be a local table root"));
    }
    let mut held = Metadata::new(memory, cancellation)?;
    let encoded = held.bytes(&root.join(".hoodie/hoodie.properties"))?;
    let _workspace = memory.reserve(encoded.value().len() as u64 * 32 + 8192)?;
    let config = properties(std::str::from_utf8(encoded.value()).map_err(failed)?)?;
    let version: u32 = config
        .get("hoodie.table.version")
        .ok_or_else(|| failed("Hudi table version is absent"))?
        .parse()
        .map_err(failed)?;
    if !matches!(version, 8 | 9) {
        return Err(failed(
            "Hudi local reader admits table versions 8 and 9 with timeline layout 2 only",
        ));
    }
    if config.get("hoodie.table.type").map(String::as_str) != Some("COPY_ON_WRITE") {
        return Err(failed("Hudi Merge-on-Read/log merging is not admitted"));
    }
    if config
        .get("hoodie.timeline.layout.version")
        .is_some_and(|v| v != "2")
    {
        return Err(failed("Hudi timeline layout must be 2"));
    }
    if config
        .get("hoodie.bootstrap.base.path")
        .is_some_and(|v| !v.is_empty())
    {
        return Err(failed("Hudi bootstrap index reads are not admitted"));
    }
    if config
        .get("hoodie.table.base.file.format")
        .is_some_and(|v| !v.eq_ignore_ascii_case("parquet"))
        || config
            .get("hoodie.table.multiple.base.file.formats.enable")
            .is_some_and(|v| v != "false")
    {
        return Err(failed("Hudi local reader requires Parquet base files"));
    }
    if config
        .get("hoodie.timeline.path")
        .is_some_and(|v| v != "timeline")
        || config
            .get("hoodie.timeline.history.path")
            .is_some_and(|v| v != "history")
    {
        return Err(failed(
            "custom Hudi timeline/history paths are not admitted",
        ));
    }
    let timeline = root.join(".hoodie/timeline");
    let history = timeline.join("history");
    require_active_history(&history)?;
    let admitted_names = timeline_names(&timeline, &mut held)?;
    let mut instants = Vec::new();
    for name in &admitted_names {
        cancellation.check()?;
        if name == "history" || name.starts_with('.') {
            continue;
        }
        held.retain(1024 + name.len() as u64 * 4)?;
        let pieces = name.split('.').collect::<Vec<_>>();
        if pieces.len() == 3 && matches!(pieces[2], "requested" | "inflight") {
            validate_time(pieces[0])?;
            continue;
        }
        if pieces.len() != 2 {
            return Err(failed("unsupported Hudi timeline filename"));
        }
        let (begin, completed) = pieces[0].split_once('_').ok_or_else(|| {
            failed("legacy Hudi instants require separately admitted serialization")
        })?;
        validate_time(begin)?;
        validate_time(completed)?;
        if begin > completed {
            return Err(failed("Hudi instant completes before its requested time"));
        }
        if !matches!(
            pieces[1],
            "commit" | "replacecommit" | "clean" | "savepoint" | "indexing"
        ) {
            return Err(failed(format!(
                "Hudi completed {} visibility is not admitted",
                pieces[1]
            )));
        }
        instants.push(Instant {
            begin: begin.into(),
            completed: completed.into(),
            action: pieces[1].into(),
            path: timeline.join(name),
        });
    }
    instants.sort_by(|a, b| {
        (&a.completed, &a.begin, &a.action).cmp(&(&b.completed, &b.begin, &b.action))
    });
    let target = if let Some(instant) = &selection.instant {
        let matching = instants
            .iter()
            .filter(|v| {
                v.begin == *instant && matches!(v.action.as_str(), "commit" | "replacecommit")
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(failed("Hudi requested instant is not one completed write"));
        }
        Some(matching[0].completed.clone())
    } else {
        instants
            .iter()
            .filter(|v| matches!(v.action.as_str(), "commit" | "replacecommit"))
            .map(|v| v.completed.clone())
            .max()
    };
    let mut files: BTreeMap<(String, String), (String, String)> = BTreeMap::new();
    let mut current_schema = config.get("hoodie.table.create.schema").cloned();
    let mut schema_instant: Option<&str> = None;
    let mut writes = std::collections::BTreeSet::new();
    let mut replaced_groups = BTreeSet::new();
    for instant in &instants {
        cancellation.check()?;
        if target
            .as_ref()
            .is_none_or(|target| instant.completed > *target)
        {
            continue;
        }
        if matches!(instant.action.as_str(), "clean" | "savepoint" | "indexing") {
            continue;
        }
        if !matches!(instant.action.as_str(), "commit" | "replacecommit") {
            return Err(failed(format!(
                "Hudi completed {} visibility is not admitted",
                instant.action
            )));
        }
        if !writes.insert(instant.begin.as_str()) {
            return Err(failed("duplicate completed Hudi write instant"));
        }
        let records = held.records(&instant.path, "avro")?;
        if records.value().rows.len() != 1 {
            return Err(failed(
                "Hudi layout-v2 commit metadata requires exactly one Avro record",
            ));
        }
        let record = &records.value().rows[0];
        let operation = record
            .get("operationType")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase);
        if let Some(operation) = operation.as_deref()
            && matches!(operation, "BOOTSTRAP" | "COMPACT" | "LOG_COMPACT")
        {
            return Err(failed(
                "Hudi bootstrap/compaction operation is not admitted for this CoW profile",
            ));
        }
        if let Some(encoded) = record
            .get("extraMetadata")
            .and_then(|v| v.get("schema"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            && !matches!(operation.as_deref(), Some("CLUSTER" | "INDEX"))
            && schema_instant.is_none_or(|previous| previous < instant.begin.as_str())
        {
            // Completion determines membership; the latest requested
            // schema-changing write determines schema. Clustering may carry
            // a stale schema and does not establish a schema change.
            held.retain(encoded.len() as u64 * 32 + 8192)?;
            current_schema = Some(encoded.into());
            schema_instant = Some(&instant.begin);
        }
        if instant.action == "replacecommit" {
            let replaced = metadata::object(
                record
                    .get("partitionToReplaceFileIds")
                    .ok_or_else(|| failed("Hudi replacecommit has no replaced-file-group map"))?,
            )?;
            for (partition, ids) in replaced {
                for id in ids
                    .as_array()
                    .ok_or_else(|| failed("Hudi replacement file IDs must be an array"))?
                {
                    let id = id
                        .as_str()
                        .ok_or_else(|| failed("Hudi replacement file ID must be a string"))?;
                    files.remove(&(partition.clone(), id.into()));
                    held.retain((partition.len() + id.len()) as u64 * 4 + 512)?;
                    replaced_groups.insert((partition.clone(), id.to_owned()));
                }
            }
        }
        let stats = metadata::object(
            record
                .get("partitionToWriteStats")
                .ok_or_else(|| failed("Hudi commit has no write statistics"))?,
        )?;
        let mut observed = std::collections::BTreeSet::new();
        for (partition, stats) in stats {
            for stat in stats
                .as_array()
                .ok_or_else(|| failed("Hudi write statistics must be an array"))?
            {
                let id = metadata::string(stat, "fileId")?;
                let location = metadata::string(stat, "path")?;
                let filename = Path::new(location)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| failed("invalid Hudi base filename"))?;
                let Some(stem) = filename.strip_suffix(".parquet") else {
                    return Err(failed("Hudi selected base file must be Parquet"));
                };
                let (prefix, written) = stem
                    .rsplit_once('_')
                    .ok_or_else(|| failed("Hudi base filename has no requested instant"))?;
                let (file_id, token) = prefix
                    .split_once('_')
                    .ok_or_else(|| failed("Hudi base filename has no write token"))?;
                if file_id != id || token.is_empty() || written != instant.begin {
                    return Err(failed(
                        "Hudi write statistic disagrees with base-file identity",
                    ));
                }
                let key = (partition.clone(), id.to_owned());
                if !observed.insert(key.clone()) {
                    return Err(failed("Hudi commit contains duplicate file-group writes"));
                }
                held.retain((partition.len() + id.len() + location.len()) as u64 * 4 + 1024)?;
                // Replacement visibility applies to the whole file group,
                // including an older requested write completing out of order.
                if replaced_groups.contains(&key) {
                    continue;
                }
                if files
                    .get(&key)
                    .is_none_or(|(begin, _)| *begin < instant.begin)
                {
                    files.insert(key, (instant.begin.clone(), location.into()));
                }
            }
        }
    }
    let current_schema = current_schema
        .ok_or_else(|| failed("Hudi selected state has no authoritative Avro schema"))?;
    let _schema_workspace = memory.reserve(current_schema.len() as u64 * 32 + 8192)?;
    let fields = schema::hudi(&serde_json::from_str(&current_schema).map_err(failed)?)?;
    held.retain(current_schema.len() as u64 * 32 + 8192)?;
    require_active_history(&history)?;
    if timeline_names(&timeline, &mut held)? != admitted_names {
        return Err(failed(
            "Hudi timeline changed during snapshot admission; retry with stable metadata",
        ));
    }
    let id = target.map_or_else(|| "empty".into(), |v| format!("completion:{v}"));
    let mut snapshot = Snapshot::new(
        "hudi",
        version,
        id,
        root.clone(),
        fields,
        ColumnMapping::Name,
        held,
    )?;
    if let Some(partitions) = config.get("hoodie.table.partition.fields") {
        for name in partitions.split(',').filter(|name| !name.is_empty()) {
            if !snapshot
                .fields
                .iter()
                .any(|field| field.field.name() == name)
            {
                return Err(failed(
                    "Hudi partition field is absent from authoritative schema",
                ));
            }
            snapshot.metadata.retain(name.len() as u64 * 4 + 256)?;
            snapshot.required_physical.insert(name.into());
        }
    }
    for (_, (_, location)) in files {
        let path = metadata::local_path(&root, &location)?;
        snapshot.push_file(DataFile::plain(path, location))?;
    }
    Ok(snapshot)
}

fn validate_time(value: &str) -> Result<()> {
    if !matches!(value.len(), 14 | 17) || !value.bytes().all(|v| v.is_ascii_digit()) {
        return Err(failed("invalid Hudi instant identifier"));
    }
    Ok(())
}

fn require_active_history(path: &Path) -> Result<()> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => {
            if entries.next().transpose().map_err(failed)?.is_some() {
                return Err(failed(
                    "Hudi archived LSM timeline is not admitted; active timeline alone cannot prove complete table state",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failed(error)),
    }
    Ok(())
}

fn timeline_names(path: &Path, held: &mut Metadata) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for entry in std::fs::read_dir(path).map_err(failed)? {
        held.cancellation.check()?;
        let name = entry
            .map_err(failed)?
            .file_name()
            .into_string()
            .map_err(|_| failed("Hudi timeline filename must be UTF8"))?;
        held.retain(name.len() as u64 * 4 + 512)?;
        names.insert(name);
    }
    Ok(names)
}

/// Java properties escaping is part of the wire format; JSON schema values can
/// contain escaped separators, Unicode and continued lines.
fn properties(input: &str) -> Result<BTreeMap<String, String>> {
    let mut logical = String::new();
    let mut output = BTreeMap::new();
    for line in input.lines() {
        let line = line.trim_start_matches([' ', '\t', '\u{c}']);
        if logical.is_empty() && (line.starts_with(['#', '!']) || line.is_empty()) {
            continue;
        }
        let backslashes = line.chars().rev().take_while(|c| *c == '\\').count();
        logical.push_str(if backslashes % 2 == 1 {
            &line[..line.len() - 1]
        } else {
            line
        });
        if backslashes % 2 == 1 {
            continue;
        }
        let mut escaped = false;
        let mut split = logical.len();
        for (index, character) in logical.char_indices() {
            if !escaped && matches!(character, '=' | ':' | ' ' | '\t' | '\u{c}') {
                split = index;
                break;
            }
            escaped = character == '\\' && !escaped;
        }
        let key = unescape(&logical[..split])?;
        let suffix = logical[split..].trim_start_matches([' ', '\t', '\u{c}']);
        let suffix = suffix
            .strip_prefix(['=', ':'])
            .unwrap_or(suffix)
            .trim_start_matches([' ', '\t', '\u{c}']);
        let value = unescape(suffix)?;
        if output.insert(key, value).is_some() {
            return Err(failed("duplicate Hudi table property"));
        }
        logical.clear();
    }
    if !logical.is_empty() {
        return Err(failed("unterminated Hudi table-property continuation"));
    }
    Ok(output)
}

fn unescape(value: &str) -> Result<String> {
    let mut chars = value.chars();
    let mut utf16 = Vec::new();
    while let Some(character) = chars.next() {
        let character = if character == '\\' {
            match chars
                .next()
                .ok_or_else(|| failed("unterminated Hudi property escape"))?
            {
                'u' => {
                    let mut code = 0_u16;
                    for _ in 0..4 {
                        code = code * 16
                            + u16::try_from(
                                chars.next().and_then(|v| v.to_digit(16)).ok_or_else(|| {
                                    failed("invalid Hudi property Unicode escape")
                                })?,
                            )
                            .map_err(failed)?;
                    }
                    utf16.push(code);
                    continue;
                }
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'f' => '\u{c}',
                other => other,
            }
        } else {
            character
        };
        let mut buffer = [0_u16; 2];
        utf16.extend_from_slice(character.encode_utf16(&mut buffer));
    }
    String::from_utf16(&utf16).map_err(failed)
}
