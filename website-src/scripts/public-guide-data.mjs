// Projection only: narrative belongs to MDX; facts and examples keep their owners.
export function projectRelease(matrix, proof) {
  const guide = matrix.user_guide;
  if (guide.release_tag !== matrix.selected_v0_1_0_release_tag ||
      guide.source_revision !== matrix.selected_v0_1_0_release_commit) {
    throw new Error('The published guide needs review for the selected release identity');
  }
  const channel = matrix.channels.find((row) => row.channel_id === guide.artifact_channel);
  if (!channel?.ready) throw new Error('The guide requires a verified package channel');
  const version = guide.release_tag.replace(/^v/, '');
  if (proof.package_version !== version || proof.proof_status !== 'passed') {
    throw new Error('The installation proof does not match the named release');
  }
  const platforms = guide.platforms.map((platform) => {
    const filename = `shardloom-${version}-${platform.wheel_tag}.whl`;
    const artifact = proof.registry_release_artifacts.find((row) => row.filename === filename);
    if (!artifact || !/^[a-f0-9]{64}$/.test(artifact.sha256)) {
      throw new Error(`Missing verified guide artifact: ${filename}`);
    }
    return { ...platform, artifact };
  });
  return {
    ...guide, version, platforms, installCommand: channel.install_command,
    sourceUrl: `https://github.com/depsilon/shardloom/tree/${guide.source_revision}`,
    evidenceUrl: `https://github.com/depsilon/shardloom/blob/main/${guide.evidence_ref}`,
  };
}

export function projectExamples(source) {
  const snippets = Object.fromEntries(
    [...source.matchAll(/^# snippet: (\w+)\n([\s\S]*?)(?=^# snippet: |$(?![\s\S]))/gm)]
      .filter(([, name]) => name !== 'end')
      .map(([, name, code]) => [name, code.trim()]),
  );
  for (const name of ['setup', 'dataframe', 'sql', 'write', 'cleanup']) {
    if (!snippets[name]) throw new Error(`Missing paid-orders snippet: ${name}`);
  }
  const csvFixture = JSON.parse(source.match(/orders\.write_text\(("(?:[^"\\]|\\.)*")/u)[1]);
  return {
    snippets,
    pythonExample: [snippets.setup, snippets.dataframe, snippets.cleanup].join('\n\n'),
    sqlExample: [snippets.setup, snippets.sql, snippets.cleanup].join('\n\n'),
    expectedRows: "({'order_id': 1, 'status': 'paid'}, {'order_id': 3, 'status': 'paid'})",
    csvFixture,
    cliExample: [
      'workspace="$(mktemp -d)"',
      'trap \'rm -rf "$workspace"\' EXIT',
      `printf '%s' '${csvFixture}' > "$workspace/orders.csv"`,
      'shardloom run sql --input "$workspace/orders.csv" \\',
      '  --sql "SELECT order_id, status FROM \'$workspace/orders.csv\' WHERE status = \'paid\' ORDER BY order_id" \\',
      '  --request collect --bounded true --memory-bytes 268435456 --max-parallelism 2 --format json',
    ].join('\n'),
  };
}
