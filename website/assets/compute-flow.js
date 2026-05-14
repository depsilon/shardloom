(function () {
  "use strict";

  var SOURCE_URL = "https://raw.githubusercontent.com/depsilon/shardloom/main/docs/architecture/compute-engine-flow-reference.md";

  var fallbackFlow = {
    views: [
      { title: "Access and users", detail: "CLI, Python, benchmarks, adapters, and future REST/event surfaces preserve the shared typed protocol." },
      { title: "Runtime contract", detail: "Policy, capability, semantic profile, execution mode, and engine mode are admitted before execution." },
      { title: "Mode lanes", detail: "Compatibility, prepared Vortex, native Vortex, direct transient, and auto lanes stay explicit." },
      { title: "Evidence and downstream use", detail: "Typed outputs carry result refs, diagnostics, certificates, timing fields, and claim gates." }
    ],
    access: [
      { title: "CLI", detail: "Current canonical entrypoint" },
      { title: "Python client", detail: "Typed wrapper over CLI protocol" },
      { title: "Benchmark harness", detail: "Comparison reports and evidence" },
      { title: "Thin adapters", detail: "Planned DB-API, SQLAlchemy, Ibis, dbt, and BI surfaces" }
    ],
    runtime: [
      { title: "Policy", detail: "Governance, credentials, and no fallback" },
      { title: "Capability matrix", detail: "Source, operator, sink, and feature gates" },
      { title: "Explicit execution mode", detail: "Requested, selected, and reason" },
      { title: "Claim gate", detail: "claim_grade, fixture_smoke_only, or not_claim_grade" }
    ],
    lanes: [
      { title: "compatibility_import_certified", detail: "Current ingest/stage certification lane" },
      { title: "prepared_vortex", detail: "Current/preferred performance lane" },
      { title: "native_vortex", detail: "Current scoped native-artifact lane" },
      { title: "direct_compatibility_transient", detail: "Scoped CSV smoke and unsupported diagnostics" },
      { title: "auto", detail: "Transparent selector, not a hidden engine" }
    ],
    engine: [
      { title: "batch", detail: "Bounded snapshot local Vortex analytics" },
      { title: "live fixture", detail: "In-memory fixture operators and certificates" },
      { title: "hybrid overlay", detail: "Base snapshot plus hot delta fixture evidence" },
      { title: "streaming reports", detail: "Capability matrix and blocked diagnostics" }
    ],
    downstream: [
      { title: "Compatibility files", detail: "CSV, Parquet, JSONL, Arrow IPC, Avro, and ORC inputs" },
      { title: "Existing Vortex artifacts", detail: "Native input path" },
      { title: "Typed output envelope", detail: "Result refs, diagnostics, evidence, and claim gates" },
      { title: "Adapter consumers", detail: "Downstream readers do not imply hidden execution modes" }
    ],
    modes: [
      ["compatibility_import_certified", "Read compatibility input, import to Vortex, write/reopen/scan, compute, certify", "Certified ingest/stage workflow", "Can be claim-grade for ingest/stage workload"],
      ["prepared_vortex", "Prepare Vortex once, then run many queries/scenarios from prepared artifacts", "Main performance comparison path", "Preferred benchmark path"],
      ["native_vortex", "Existing .vortex input, Vortex-native scan/operator path", "Cleanest native query path", "Cleanest native-engine lane"],
      ["direct_compatibility_transient", "Read compatibility input and compute directly without persistent Vortex write/reopen", "Small one-shot jobs, quick ETL", "Not Vortex-native"],
      ["auto", "Transparent mode choice based on input/request/policy", "User convenience", "Must report selected mode and reason"]
    ],
    timing: [
      "source_read_millis",
      "compatibility_parse_millis",
      "compatibility_to_vortex_import_millis",
      "vortex_write_millis",
      "vortex_reopen_millis",
      "vortex_scan_millis",
      "operator_compute_millis",
      "result_sink_write_millis",
      "evidence_render_millis",
      "total_runtime_millis"
    ],
    engineFields: [
      "requested_engine_mode",
      "selected_engine_mode",
      "allowed_engine_modes",
      "rejected_engine_modes",
      "runtime_execution",
      "data_read",
      "write_io",
      "fallback_attempted=false",
      "external_engine_invoked=false"
    ],
    never: [
      "Unsupported work silently runs through Spark.",
      "Auto mode hides what it selected.",
      "Compatibility import timing is reported as pure query timing.",
      "A fixture-smoke row becomes a public performance claim."
    ]
  };

  function stripMarkdown(value) {
    return String(value || "")
      .replace(/<br\s*\/?>/gi, " - ")
      .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
      .replace(/`([^`]+)`/g, "$1")
      .replace(/\*\*([^*]+)\*\*/g, "$1")
      .replace(/\s+/g, " ")
      .trim();
  }

  function compact(value, limit) {
    var text = stripMarkdown(value);
    if (text.length <= limit) {
      return text;
    }
    return text.slice(0, limit - 1).trim() + "...";
  }

  function splitTableRow(line) {
    return line
      .trim()
      .replace(/^\|/, "")
      .replace(/\|$/, "")
      .split("|")
      .map(stripMarkdown);
  }

  function tableAfter(markdown, headerLine) {
    var start = markdown.indexOf(headerLine);
    if (start < 0) {
      return [];
    }
    var lines = markdown.slice(start).split(/\r?\n/);
    var rows = [];
    var inTable = false;
    for (var index = 0; index < lines.length; index += 1) {
      var line = lines[index].trim();
      if (!line.startsWith("|")) {
        if (inTable) {
          break;
        }
        continue;
      }
      inTable = true;
      if (/^\|\s*-/.test(line)) {
        continue;
      }
      rows.push(splitTableRow(line));
    }
    return rows.slice(1);
  }

  function codeBlockAfter(markdown, marker) {
    var start = markdown.indexOf(marker);
    if (start < 0) {
      return "";
    }
    var fence = markdown.indexOf("```", start);
    if (fence < 0) {
      return "";
    }
    var bodyStart = markdown.indexOf("\n", fence);
    var bodyEnd = markdown.indexOf("```", bodyStart + 1);
    if (bodyStart < 0 || bodyEnd < 0) {
      return "";
    }
    return markdown.slice(bodyStart + 1, bodyEnd).trim();
  }

  function mermaidFor(markdown, heading) {
    var start = markdown.indexOf(heading);
    if (start < 0) {
      return "";
    }
    return codeBlockAfter(markdown.slice(start), "```mermaid");
  }

  function parseNodeLabel(label) {
    var parts = String(label || "").split(/<br\s*\/?>/i).map(stripMarkdown).filter(Boolean);
    return {
      title: parts[0] || "",
      detail: parts.slice(1).join(" - ")
    };
  }

  function nodesFromMermaid(block, ids) {
    var nodes = {};
    var pattern = /^\s*([A-Z0-9_]+)\["([^"]+)"\]/gm;
    var match;
    while ((match = pattern.exec(block)) !== null) {
      nodes[match[1]] = parseNodeLabel(match[2]);
    }
    return ids.map(function (id) {
      return nodes[id];
    }).filter(Boolean);
  }

  function parseFlow(markdown) {
    var viewRows = tableAfter(markdown, "| View | Question answered | Primary audience | Stop here when |");
    var modeRows = tableAfter(markdown, "| Mode | What it means | Primary use | Vortex-native claim? | Claim posture |");
    var timingBlock = codeBlockAfter(markdown, "Mode timing fields must stay visible:");
    var engineFieldBlock = codeBlockAfter(markdown, "Engine-mode report fields must stay visible:");
    var neverBlock = codeBlockAfter(markdown, "## What Should Never Happen");

    return {
      views: viewRows.map(function (row) {
        return {
          title: row[0],
          detail: compact(row[1] + " Audience: " + row[2], 170)
        };
      }).slice(0, 5),
      access: nodesFromMermaid(
        mermaidFor(markdown, "### View 1 - Access And Users"),
        ["CLI", "PYTHON", "BENCH", "REST", "SDK", "ADAPTER", "OUTPUT"]
      ),
      runtime: nodesFromMermaid(
        mermaidFor(markdown, "### View 2 - Runtime Contract"),
        ["POLICY", "CAPABILITY", "MODE", "ENGINE", "ADMISSION", "EXECUTE", "DIAGNOSTIC", "EVIDENCE", "CLAIM", "ENVELOPE"]
      ),
      lanes: nodesFromMermaid(
        mermaidFor(markdown, "### View 3 - Execution Mode Lanes"),
        ["AUTO", "COMPAT", "PREPARED", "NATIVE", "DIRECT", "PROVIDER", "SELECTIVE_BLOCKER"]
      ),
      engine: nodesFromMermaid(
        mermaidFor(markdown, "### View 4 - Engine Fabric Layer"),
        ["BATCH", "LIVE_CONTRACT", "LIVE_FIXTURE", "HYBRID_FIXTURE", "STREAM_REPORTS", "FIXTURE_CLAIM", "NO_EFFECTS"]
      ),
      downstream: nodesFromMermaid(
        mermaidFor(markdown, "### View 5 - I/O, Evidence, And Downstream Use"),
        ["COMPAT_INPUT", "VORTEX_INPUT", "OBJECT_INPUT", "TABLE_INPUT", "STREAM_INPUT", "VORTEX_SINK", "REST_EVENT", "CLI_RESULT", "PY_RESULT", "ADAPTER_RESULT", "BENCH_RESULT"]
      ),
      modes: modeRows.map(function (row) {
        return [row[0], row[1], row[2], row[4]];
      }),
      timing: timingBlock.split(/\r?\n/).map(stripMarkdown).filter(Boolean),
      engineFields: engineFieldBlock.split(/\r?\n/).map(stripMarkdown).filter(Boolean),
      never: neverBlock.split(/\r?\n/).map(stripMarkdown).filter(Boolean)
    };
  }

  function clear(node) {
    while (node && node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function renderCards(container, items) {
    clear(container);
    items.forEach(function (item, index) {
      var article = document.createElement("article");
      article.className = "step";

      var number = document.createElement("span");
      number.className = "step-index";
      number.textContent = String(index + 1).padStart(2, "0");

      var title = document.createElement("h3");
      title.textContent = item.title;

      var detail = document.createElement("p");
      detail.textContent = item.detail;

      article.appendChild(number);
      article.appendChild(title);
      article.appendChild(detail);
      container.appendChild(article);
    });
  }

  function renderList(container, items) {
    clear(container);
    items.forEach(function (item) {
      var wrapper = document.createElement("div");
      wrapper.className = "flow-item";

      var title = document.createElement("strong");
      title.textContent = item.title;

      var detail = document.createElement("span");
      detail.textContent = item.detail || "Current architecture reference item.";

      wrapper.appendChild(title);
      wrapper.appendChild(detail);
      container.appendChild(wrapper);
    });
  }

  function renderPills(container, items) {
    clear(container);
    items.forEach(function (item) {
      var pill = document.createElement("li");
      pill.textContent = item;
      container.appendChild(pill);
    });
  }

  function renderModes(container, rows) {
    clear(container);
    rows.forEach(function (row) {
      var tr = document.createElement("tr");
      row.forEach(function (cell) {
        var td = document.createElement("td");
        td.textContent = cell;
        tr.appendChild(td);
      });
      container.appendChild(tr);
    });
  }

  function hydrate(root, flow, statusText) {
    var status = root.querySelector("[data-flow-source-status]");
    if (status) {
      status.textContent = statusText;
    }

    renderCards(root.querySelector("[data-flow-views]"), flow.views.length ? flow.views : fallbackFlow.views);
    renderList(root.querySelector("[data-flow-access]"), flow.access.length ? flow.access : fallbackFlow.access);
    renderList(root.querySelector("[data-flow-runtime]"), flow.runtime.length ? flow.runtime : fallbackFlow.runtime);
    renderList(root.querySelector("[data-flow-lanes]"), flow.lanes.length ? flow.lanes : fallbackFlow.lanes);
    renderList(root.querySelector("[data-flow-engine]"), flow.engine.length ? flow.engine : fallbackFlow.engine);
    renderList(root.querySelector("[data-flow-downstream]"), flow.downstream.length ? flow.downstream : fallbackFlow.downstream);
    renderPills(root.querySelector("[data-flow-timing]"), flow.timing.length ? flow.timing : fallbackFlow.timing);
    renderPills(root.querySelector("[data-flow-engine-fields]"), flow.engineFields.length ? flow.engineFields : fallbackFlow.engineFields);
    renderPills(root.querySelector("[data-flow-never]"), flow.never.length ? flow.never : fallbackFlow.never);
    renderModes(root.querySelector("[data-flow-modes]"), flow.modes.length ? flow.modes : fallbackFlow.modes);
  }

  function init() {
    var root = document.querySelector("[data-flow-root]");
    if (!root) {
      return;
    }

    hydrate(root, fallbackFlow, "Using static fallback");

    fetch(SOURCE_URL, { cache: "no-store" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Architecture reference request failed: " + response.status);
        }
        return response.text();
      })
      .then(function (markdown) {
        hydrate(root, parseFlow(markdown), "Loaded from canonical architecture reference");
      })
      .catch(function () {
        hydrate(root, fallbackFlow, "Using static fallback");
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
}());
