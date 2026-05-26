import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..");
const BetriebDir = path.join(repoRoot, "VIP-AI-Memory", "03-Betrieb");
const statePath = path.join(BetriebDir, "Watcher-State.json");
const queuePath = path.join(BetriebDir, "Watcher-Queue.jsonl");
const logPath = path.join(BetriebDir, "Watcher-Log.md");

const DEFAULT_MCP_URL = "https://vip-tools-mcp.onrender.com/mcp";
const AGENT_PREFIX = "vip-ai-";
const LOCAL_TIME_ZONE = "Europe/Berlin";
const TASK_FIELDS = [
  "gid",
  "name",
  "modified_at",
  "due_on",
  "due_at",
  "completed",
  "permalink_url",
  "assignee.gid",
  "assignee.name",
  "tags.name",
  "memberships.project.name",
  "custom_fields.name",
  "custom_fields.display_value",
  "custom_fields.text_value",
  "custom_fields.enum_value.name"
].join(",");
const STORY_FIELDS = [
  "gid",
  "created_at",
  "text",
  "type",
  "resource_subtype",
  "created_by.gid",
  "created_by.name"
].join(",");
const ATTACHMENT_FIELDS = [
  "gid",
  "name",
  "created_at",
  "resource_subtype",
  "download_url",
  "view_url"
].join(",");

function parseArgs(argv) {
  const opts = {
    dryRun: true,
    write: false,
    baseline: false,
    mcpUrl: process.env.WATCHER_MCP_URL || DEFAULT_MCP_URL,
    agents: null,
    limit: 20,
    maxSignalsPerAgent: 3,
    lookbackHours: 72
  };

  for (const arg of argv) {
    if (arg === "--write") {
      opts.write = true;
      opts.dryRun = false;
    } else if (arg === "--dry-run") {
      opts.write = false;
      opts.dryRun = true;
    } else if (arg === "--baseline") {
      opts.baseline = true;
    } else if (arg.startsWith("--mcp-url=")) {
      opts.mcpUrl = arg.slice("--mcp-url=".length);
    } else if (arg.startsWith("--agents=")) {
      opts.agents = arg
        .slice("--agents=".length)
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
    } else if (arg.startsWith("--limit=")) {
      opts.limit = parsePositiveInt(arg.slice("--limit=".length), "limit");
    } else if (arg.startsWith("--max-signals-per-agent=")) {
      opts.maxSignalsPerAgent = parsePositiveInt(
        arg.slice("--max-signals-per-agent=".length),
        "max-signals-per-agent"
      );
    } else if (arg.startsWith("--lookback-hours=")) {
      opts.lookbackHours = parsePositiveInt(arg.slice("--lookback-hours=".length), "lookback-hours");
    } else if (arg === "--help" || arg === "-h") {
      printHelpAndExit();
    } else {
      throw new Error(`Unbekanntes Argument: ${arg}`);
    }
  }

  return opts;
}

function parsePositiveInt(value, label) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} muss eine positive Zahl sein.`);
  }
  return parsed;
}

function printHelpAndExit() {
  console.log(`Asana Watcher Phase 1

Usage:
  node scripts/asana-watcher.mjs --dry-run
  node scripts/asana-watcher.mjs --write --baseline
  node scripts/asana-watcher.mjs --write --agents=vip-ai-marketing,vip-ai-content

Optionen:
  --dry-run                  Nur pruefen, nichts schreiben. Default.
  --write                    State/Queue/Log lokal schreiben.
  --baseline                 Aktuellen Stand als gesehen markieren, ohne Queue-Signale.
  --agents=a,b               Nur bestimmte agent_id-Werte pruefen.
  --limit=20                 Max. offene Tasks je Agent.
  --max-signals-per-agent=3  Max. Queue-Signale je Agent und Lauf.
  --lookback-hours=72        Nur neuere Modifikationen vertiefen.
`);
  process.exit(0);
}

function nowIso() {
  return new Date().toISOString();
}

function berlinDateString(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: LOCAL_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(date);
}

function addDays(dateString, days) {
  const date = new Date(`${dateString}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function sha1(value) {
  return crypto.createHash("sha1").update(value).digest("hex");
}

function truncate(value, max = 180) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= max) return text;
  return `${text.slice(0, max - 3)}...`;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

async function readJsonOrDefault(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    if (error?.code === "ENOENT") return fallback;
    throw error;
  }
}

async function callTool(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  if (result?.isError) {
    const message = result.content?.map((item) => item.text).filter(Boolean).join("\n") || "MCP tool error";
    throw new Error(message);
  }
  const text = result?.content?.find((item) => item.type === "text")?.text;
  if (!text) return {};
  return JSON.parse(text);
}

async function asanaGet(client, agentId, pathName, params = {}) {
  const result = await callTool(client, "asana_request", {
    agent_id: agentId,
    method: "GET",
    path: pathName,
    params
  });
  return result?.response?.data;
}

function createEmptyState() {
  return {
    schema_version: 1,
    created_at: nowIso(),
    updated_at: null,
    agents: {},
    emitted_signals: {}
  };
}

function getAgentState(state, agentId) {
  if (!state.agents[agentId]) {
    state.agents[agentId] = {
      last_checked_at: null,
      workspace_gid: null,
      tasks: {}
    };
  }
  return state.agents[agentId];
}

function getDueInfo(task, now = new Date()) {
  const today = berlinDateString(now);
  const tomorrow = addDays(today, 1);

  if (task.due_at) {
    const dueAt = new Date(task.due_at);
    if (!Number.isFinite(dueAt.getTime())) return null;

    const dueDate = berlinDateString(dueAt);
    let signalType = null;
    if (dueAt.getTime() <= now.getTime()) {
      signalType = "overdue";
    } else if (dueDate === today) {
      signalType = "due_later_today";
    } else if (dueDate === tomorrow) {
      signalType = "due_tomorrow";
    }

    if (!signalType) return null;
    return {
      signal_type: signalType,
      due_on: task.due_on || dueDate,
      due_at: task.due_at,
      exact_time: true
    };
  }

  const due = task.due_on || null;
  if (!due) return null;
  if (due < today) {
    return { signal_type: "overdue", due_on: due, due_at: null, exact_time: false };
  }
  if (due === today) {
    return { signal_type: "due_today", due_on: due, due_at: null, exact_time: false };
  }
  if (due === tomorrow) {
    return { signal_type: "due_tomorrow", due_on: due, due_at: null, exact_time: false };
  }
  return null;
}

function getDueSignal(task) {
  return getDueInfo(task)?.signal_type || null;
}

function signalPriority(type, task) {
  if (type === "overdue" || type === "due_today") return "high";
  if (type === "due_later_today" || type === "due_tomorrow" || type === "new_attachment") return "medium";
  if (type === "new_task") {
    const dueSignal = getDueSignal(task);
    if (dueSignal === "overdue" || dueSignal === "due_today") return "high";
    if (dueSignal === "due_later_today" || dueSignal === "due_tomorrow") return "medium";
  }
  return "normal";
}

function makeSignal(input) {
  const base = [
    input.agent_id,
    input.task_gid,
    input.signal_type,
    input.story_gid || "",
    input.attachment_gid || "",
    input.modified_at || "",
    input.due_on || "",
    input.due_at || ""
  ].join("|");

  return {
    id: sha1(base),
    detected_at: input.detected_at,
    agent_id: input.agent_id,
    task_gid: input.task_gid,
    task_name: input.task_name,
    task_url: input.task_url || null,
    signal_type: input.signal_type,
    priority: input.priority,
    due_on: input.due_on || null,
    due_at: input.due_at || null,
    modified_at: input.modified_at || null,
    story_gid: input.story_gid || null,
    attachment_gid: input.attachment_gid || null,
    snippet: truncate(input.snippet || "", 180),
    suggested_action: input.suggested_action || "include_in_next_agent_run"
  };
}

function shouldInspectDetails(task, previousTaskState, opts) {
  if (!previousTaskState) return true;
  if (previousTaskState.last_modified_at !== task.modified_at) return true;
  if (getDueSignal(task)) return true;

  const modifiedAt = Date.parse(task.modified_at || "");
  if (!Number.isFinite(modifiedAt)) return false;
  return Date.now() - modifiedAt <= opts.lookbackHours * 60 * 60 * 1000;
}

async function inspectTaskDetails(client, agentId, task, previousTaskState) {
  const [stories, attachments] = await Promise.all([
    asanaGet(client, agentId, `/tasks/${task.gid}/stories`, {
      limit: 20,
      opt_fields: STORY_FIELDS
    }).catch((error) => ({ error: error.message, data: [] })),
    asanaGet(client, agentId, "/attachments", {
      parent: task.gid,
      limit: 20,
      opt_fields: ATTACHMENT_FIELDS
    }).catch((error) => ({ error: error.message, data: [] }))
  ]);

  const previousStories = previousTaskState?.seen_stories || {};
  const previousAttachments = previousTaskState?.seen_attachments || {};
  const storyData = safeArray(stories?.data ?? stories);
  const attachmentData = safeArray(attachments?.data ?? attachments);

  return {
    stories: storyData,
    attachments: attachmentData,
    newStories: previousTaskState ? storyData.filter((story) => !previousStories[story.gid]) : [],
    newAttachments: previousTaskState
      ? attachmentData.filter((attachment) => !previousAttachments[attachment.gid])
      : [],
    errors: [stories?.error, attachments?.error].filter(Boolean)
  };
}

function updateTaskState(agentState, task, details) {
  const seenStories = {};
  for (const story of safeArray(details?.stories)) {
    if (story?.gid) seenStories[story.gid] = story.created_at || null;
  }

  const seenAttachments = {};
  for (const attachment of safeArray(details?.attachments)) {
    if (attachment?.gid) seenAttachments[attachment.gid] = attachment.created_at || null;
  }

  agentState.tasks[task.gid] = {
    last_seen_at: nowIso(),
    last_modified_at: task.modified_at || null,
    due_on: task.due_on || null,
    due_at: task.due_at || null,
    completed: Boolean(task.completed),
    name: task.name || "",
    permalink_url: task.permalink_url || null,
    seen_stories: seenStories,
    seen_attachments: seenAttachments
  };
}

function buildSignalsForTask(agentId, task, previousTaskState, details, detectedAt) {
  const signals = [];
  const dueInfo = getDueInfo(task);
  const dueSignal = dueInfo?.signal_type || null;

  if (!previousTaskState) {
    signals.push(
      makeSignal({
        detected_at: detectedAt,
        agent_id: agentId,
        task_gid: task.gid,
        task_name: task.name,
        task_url: task.permalink_url,
        signal_type: "new_task",
        priority: signalPriority("new_task", task),
        due_on: dueInfo?.due_on || task.due_on,
        due_at: task.due_at,
        modified_at: task.modified_at,
        snippet: task.name,
        suggested_action: "start_or_include_in_next_standard_run"
      })
    );
  } else if (previousTaskState.last_modified_at !== task.modified_at) {
    signals.push(
      makeSignal({
        detected_at: detectedAt,
        agent_id: agentId,
        task_gid: task.gid,
        task_name: task.name,
        task_url: task.permalink_url,
        signal_type: "modified_task",
        priority: "normal",
        due_on: dueInfo?.due_on || task.due_on,
        due_at: task.due_at,
        modified_at: task.modified_at,
        snippet: task.name
      })
    );
  }

  if (dueSignal) {
    signals.push(
      makeSignal({
        detected_at: detectedAt,
        agent_id: agentId,
        task_gid: task.gid,
        task_name: task.name,
        task_url: task.permalink_url,
        signal_type: dueSignal,
        priority: signalPriority(dueSignal, task),
        due_on: dueInfo.due_on,
        due_at: dueInfo.due_at,
        modified_at: task.modified_at,
        snippet: task.name,
        suggested_action:
          dueSignal === "due_later_today"
            ? "prepare_but_do_not_execute_before_due_at"
            : "prioritize_in_next_agent_run"
      })
    );
  }

  for (const story of safeArray(details?.newStories)) {
    signals.push(
      makeSignal({
        detected_at: detectedAt,
        agent_id: agentId,
        task_gid: task.gid,
        task_name: task.name,
        task_url: task.permalink_url,
        signal_type: "new_story",
        priority: "normal",
        due_on: dueInfo?.due_on || task.due_on,
        due_at: task.due_at,
        modified_at: task.modified_at,
        story_gid: story.gid,
        snippet: story.text || story.resource_subtype || story.type
      })
    );
  }

  for (const attachment of safeArray(details?.newAttachments)) {
    signals.push(
      makeSignal({
        detected_at: detectedAt,
        agent_id: agentId,
        task_gid: task.gid,
        task_name: task.name,
        task_url: task.permalink_url,
        signal_type: "new_attachment",
        priority: "medium",
        due_on: dueInfo?.due_on || task.due_on,
        due_at: task.due_at,
        modified_at: task.modified_at,
        attachment_gid: attachment.gid,
        snippet: attachment.name || attachment.resource_subtype,
        suggested_action: "include_attachment_context_in_next_agent_run"
      })
    );
  }

  return signals;
}

function sortSignals(signals) {
  const weight = { high: 0, medium: 1, normal: 2 };
  return [...signals].sort((a, b) => {
    const priorityDiff = (weight[a.priority] ?? 9) - (weight[b.priority] ?? 9);
    if (priorityDiff !== 0) return priorityDiff;
    return String(b.modified_at || "").localeCompare(String(a.modified_at || ""));
  });
}

async function run() {
  const opts = parseArgs(process.argv.slice(2));
  const detectedAt = nowIso();
  const state = await readJsonOrDefault(statePath, createEmptyState());
  const client = new Client({ name: "vip-asana-watcher", version: "0.1.0" });
  const transport = new StreamableHTTPClientTransport(new URL(opts.mcpUrl));
  const summary = {
    detected_at: detectedAt,
    mode: opts.write ? "write" : "dry-run",
    baseline: opts.baseline,
    agents_checked: 0,
    tasks_checked: 0,
    signals_found: 0,
    signals_queued: 0,
    agents: {}
  };

  await client.connect(transport);
  try {
    const list = await callTool(client, "asana_list_agents");
    const agentIds = safeArray(list)
      .filter((agent) => agent?.agent_id?.startsWith(AGENT_PREFIX))
      .filter((agent) => agent.token_configured)
      .map((agent) => agent.agent_id)
      .filter((agentId) => !opts.agents || opts.agents.includes(agentId));

    const allQueuedSignals = [];

    for (const agentId of agentIds) {
      const agentState = getAgentState(state, agentId);
      const agentSummary = {
        tasks_checked: 0,
        signals_found: 0,
        signals_queued: 0,
        errors: []
      };
      summary.agents[agentId] = agentSummary;

      try {
        const whoami = await callTool(client, "asana_whoami", { agent_id: agentId });
        const workspaceGid = whoami?.user?.workspaces?.[0]?.gid;
        if (!workspaceGid) throw new Error("Kein Asana-Workspace im whoami-Result gefunden.");
        agentState.workspace_gid = workspaceGid;

        const taskResult = await asanaGet(client, agentId, "/tasks", {
          assignee: "me",
          workspace: workspaceGid,
          completed_since: "now",
          limit: opts.limit,
          opt_fields: TASK_FIELDS
        });

        const tasks = safeArray(taskResult?.data ?? taskResult);
        const candidateSignals = [];

        for (const task of tasks) {
          const previousTaskState = agentState.tasks[task.gid];
          let details = { stories: [], attachments: [], newStories: [], newAttachments: [], errors: [] };

          if (shouldInspectDetails(task, previousTaskState, opts)) {
            details = await inspectTaskDetails(client, agentId, task, previousTaskState);
            agentSummary.errors.push(...details.errors.map((message) => `${task.gid}: ${message}`));
          }

          updateTaskState(agentState, task, details);
          const taskSignals = buildSignalsForTask(agentId, task, previousTaskState, details, detectedAt);
          candidateSignals.push(...taskSignals);
          agentSummary.tasks_checked += 1;
        }

        const freshSignals = sortSignals(candidateSignals).filter((signal) => !state.emitted_signals[signal.id]);
        const queuedSignals = opts.baseline ? [] : freshSignals.slice(0, opts.maxSignalsPerAgent);
        const signalsToMarkSeen = opts.baseline ? freshSignals : queuedSignals;

        for (const signal of signalsToMarkSeen) {
          state.emitted_signals[signal.id] = detectedAt;
        }

        for (const signal of queuedSignals) {
          allQueuedSignals.push(signal);
        }

        agentSummary.signals_found = freshSignals.length;
        agentSummary.signals_queued = queuedSignals.length;
        agentState.last_checked_at = detectedAt;
        summary.agents_checked += 1;
        summary.tasks_checked += agentSummary.tasks_checked;
        summary.signals_found += agentSummary.signals_found;
        summary.signals_queued += agentSummary.signals_queued;
      } catch (error) {
        agentSummary.errors.push(error.message);
      }
    }

    state.updated_at = detectedAt;

    if (opts.write) {
      await fs.mkdir(BetriebDir, { recursive: true });
      await fs.writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`);
      if (allQueuedSignals.length > 0) {
        await fs.appendFile(queuePath, `${allQueuedSignals.map((signal) => JSON.stringify(signal)).join("\n")}\n`);
      }
      await fs.appendFile(logPath, renderLog(summary, allQueuedSignals));
    }

    console.log(JSON.stringify({ summary, queued_signals: allQueuedSignals }, null, 2));
  } finally {
    await client.close().catch(() => {});
  }
}

function renderLog(summary, signals) {
  const lines = [
    `\n## ${summary.detected_at} - Asana Watcher`,
    "",
    `- mode: ${summary.mode}${summary.baseline ? " / baseline" : ""}`,
    `- agents_checked: ${summary.agents_checked}`,
    `- tasks_checked: ${summary.tasks_checked}`,
    `- signals_found: ${summary.signals_found}`,
    `- signals_queued: ${summary.signals_queued}`
  ];

  for (const [agentId, data] of Object.entries(summary.agents)) {
    lines.push(`- ${agentId}: tasks=${data.tasks_checked}, found=${data.signals_found}, queued=${data.signals_queued}`);
    for (const error of data.errors.slice(0, 3)) {
      lines.push(`  - error: ${truncate(error, 220)}`);
    }
  }

  for (const signal of signals) {
    lines.push(
      `- queued: ${signal.agent_id} / ${signal.signal_type} / ${signal.task_gid} / ${truncate(signal.task_name, 120)}`
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

run().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
});
