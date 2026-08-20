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
const healthPath = path.join(BetriebDir, "Watcher-Health.json");

const DEFAULT_MCP_URL = "https://vip-tools-mcp.onrender.com/mcp";
const AGENT_PREFIX = "vip-ai-";
const LOCAL_TIME_ZONE = "Europe/Berlin";
const WATCHER_CONNECT_TIMEOUT_MS = Number.parseInt(process.env.WATCHER_CONNECT_TIMEOUT_MS || "20000", 10);
const WATCHER_TOOL_TIMEOUT_MS = Number.parseInt(process.env.WATCHER_TOOL_TIMEOUT_MS || "25000", 10);
const WATCHER_TRANSIENT_MAX_ATTEMPTS = Number.parseInt(
  process.env.WATCHER_TRANSIENT_MAX_ATTEMPTS || "3",
  10
);
const WATCHER_TRANSIENT_RETRY_DELAY_MS = Number.parseInt(
  process.env.WATCHER_TRANSIENT_RETRY_DELAY_MS || "15000",
  10
);

function parseArgs(argv) {
  const opts = {
    dryRun: true,
    write: false,
    baseline: false,
    mcpUrl: process.env.WATCHER_MCP_URL || DEFAULT_MCP_URL,
    agents: null,
    limit: 100,
    maxSignalsPerAgent: 3,
    selfTest: false
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
      parsePositiveInt(arg.slice("--lookback-hours=".length), "lookback-hours");
    } else if (arg === "--help" || arg === "-h") {
      printHelpAndExit();
    } else if (arg === "--self-test") {
      opts.selfTest = true;
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
  --limit=100                Max. offene Tasks je Agent.
  --max-signals-per-agent=3  Max. Queue-Signale je Agent und Lauf.
  --lookback-hours=72        Veralteter, kompatibel akzeptierter Parameter ohne Zusatzabrufe.
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientMcpError(error) {
  const message = String(error?.message || error || "");
  return (
    /\b(429|502|503|504)\b/.test(message) ||
    /cf-mitigated|cloudflare|managed challenge/i.test(message) ||
    message.includes("Streamable HTTP error") ||
    message.includes("Error POSTing to endpoint") ||
    message.includes("fetch failed") ||
    message.includes("ECONNRESET") ||
    message.includes("ECONNREFUSED") ||
    message.includes("EAI_AGAIN") ||
    message.includes("ENOTFOUND") ||
    message.includes("ETIMEDOUT") ||
    message.includes("timeout after")
  );
}

async function retryTransient(label, operation) {
  let lastError;
  for (let attempt = 1; attempt <= WATCHER_TRANSIENT_MAX_ATTEMPTS; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (!isTransientMcpError(error) || attempt >= WATCHER_TRANSIENT_MAX_ATTEMPTS) throw error;
    }
    const delayMs = WATCHER_TRANSIENT_RETRY_DELAY_MS * attempt;
    console.error(
      `[watcher] ${label} transient failure: ${truncate(
        lastError?.message || String(lastError),
        220
      )}; retry ${attempt + 1}/${WATCHER_TRANSIENT_MAX_ATTEMPTS} in ${delayMs} ms`
    );
    await sleep(delayMs);
  }
  throw lastError;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

async function withTimeout(promise, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timeout after ${timeoutMs} ms`)), timeoutMs);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
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
  return retryTransient(`MCP tool ${name}`, async () => {
    const result = await withTimeout(
      client.callTool({ name, arguments: args }),
      WATCHER_TOOL_TIMEOUT_MS,
      `MCP tool ${name}`
    );
    if (result?.isError) {
      const message = result.content?.map((item) => item.text).filter(Boolean).join("\n") || "MCP tool error";
      throw new Error(message);
    }
    const text = result?.content?.find((item) => item.type === "text")?.text;
    if (!text) return {};
    return JSON.parse(text);
  });
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

function getTaskDueBucket(task, now = new Date()) {
  const today = berlinDateString(now);
  const tomorrow = addDays(today, 1);

  if (task.due_at) {
    const dueAt = new Date(task.due_at);
    if (!Number.isFinite(dueAt.getTime())) return "invalid";
    const dueDate = berlinDateString(dueAt);
    if (dueAt.getTime() <= now.getTime()) return "overdue";
    if (dueDate === today) return "due_later_today";
    if (dueDate === tomorrow) return "due_tomorrow";
    return "future";
  }

  if (!task.due_on) return "without_due";
  if (task.due_on < today) return "overdue";
  if (task.due_on === today) return "due_today";
  if (task.due_on === tomorrow) return "due_tomorrow";
  return "future";
}

function looksLikeRoutineTask(task) {
  return /^\s*R\s*:/i.test(String(task?.name || ""));
}

function hasRoutineTag(task) {
  return safeArray(task?.tags).some((tag) => String(tag?.name || "").trim().toLowerCase() === "routine");
}

function incrementDueBucketSummary(summary, dueBucket) {
  const fieldByBucket = {
    overdue: "overdue_tasks",
    due_today: "due_today_tasks",
    due_later_today: "due_later_today_tasks",
    due_tomorrow: "due_tomorrow_tasks",
    future: "future_tasks",
    without_due: "tasks_without_due"
  };
  const field = fieldByBucket[dueBucket];
  if (field) summary[field] += 1;
}

function updateTaskState(agentState, task) {
  const previousTaskState = agentState.tasks[task.gid] || {};
  agentState.tasks[task.gid] = {
    last_seen_at: nowIso(),
    last_modified_at: task.modified_at || null,
    due_on: task.due_on || null,
    due_at: task.due_at || null,
    completed: Boolean(task.completed),
    name: task.name || "",
    permalink_url: task.permalink_url || null,
    seen_stories: previousTaskState.seen_stories || {},
    seen_attachments: previousTaskState.seen_attachments || {}
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

function coalesceSignalsByTask(signals) {
  const byTask = new Map();
  for (const signal of sortSignals(signals)) {
    const key = `${signal.agent_id}:${signal.task_gid}`;
    const current = byTask.get(key);
    if (!current) {
      byTask.set(key, {
        ...signal,
        related_signal_types: [signal.signal_type],
        related_signal_ids: [signal.id],
        coalesced_signal_count: 1
      });
      continue;
    }
    if (!current.related_signal_types.includes(signal.signal_type)) {
      current.related_signal_types.push(signal.signal_type);
    }
    current.related_signal_ids.push(signal.id);
    current.coalesced_signal_count = current.related_signal_ids.length;
  }
  return [...byTask.values()];
}

async function run() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.selfTest) {
    const sample = [
      { id: "new", agent_id: "vip-ai-test", task_gid: "1", signal_type: "new_task", priority: "medium" },
      { id: "due", agent_id: "vip-ai-test", task_gid: "1", signal_type: "overdue", priority: "high" },
      { id: "story", agent_id: "vip-ai-test", task_gid: "1", signal_type: "new_story", priority: "normal" },
      { id: "other", agent_id: "vip-ai-test", task_gid: "2", signal_type: "new_task", priority: "normal" }
    ];
    const result = coalesceSignalsByTask(sample);
    const taskOne = result.find((signal) => signal.task_gid === "1");
    const passed =
      result.length === 2 &&
      taskOne?.signal_type === "overdue" &&
      taskOne?.coalesced_signal_count === 3 &&
      taskOne?.related_signal_types?.length === 3 &&
      getTaskDueBucket({ due_on: "2099-01-01" }, new Date("2026-08-20T10:00:00.000Z")) === "future" &&
      getTaskDueBucket({}, new Date("2026-08-20T10:00:00.000Z")) === "without_due" &&
      looksLikeRoutineTask({ name: "R: Test" }) &&
      hasRoutineTag({ tags: [{ name: "Routine" }] }) &&
      !hasRoutineTag({ tags: [] });
    console.log(JSON.stringify({ passed, result }));
    if (!passed) process.exitCode = 1;
    return;
  }
  const detectedAt = nowIso();
  const state = await readJsonOrDefault(statePath, createEmptyState());
  let client;
  const connectClient = async () => {
    const nextClient = new Client({ name: "vip-asana-watcher", version: "0.1.0" });
    const transport = new StreamableHTTPClientTransport(new URL(opts.mcpUrl));
    try {
      await withTimeout(nextClient.connect(transport), WATCHER_CONNECT_TIMEOUT_MS, "MCP connect");
      return nextClient;
    } catch (error) {
      await nextClient.close().catch(() => {});
      throw error;
    }
  };
  const summary = {
    detected_at: detectedAt,
    mode: opts.write ? "write" : "dry-run",
    baseline: opts.baseline,
    agents_checked: 0,
    agents_succeeded: 0,
    agents_failed: 0,
    tasks_checked: 0,
    overdue_tasks: 0,
    due_today_tasks: 0,
    due_later_today_tasks: 0,
    due_tomorrow_tasks: 0,
    future_tasks: 0,
    tasks_without_due: 0,
    routine_tasks: 0,
    routine_missing_tag: 0,
    raw_signals_found: 0,
    signals_found: 0,
    signals_queued: 0,
    agents: {}
  };

  try {
    client = await retryTransient("MCP connect", connectClient);
    console.error(`[watcher] connected ${opts.mcpUrl}`);
    const taskSnapshot = await callTool(client, "asana_agents_open_task_snapshot", {
      ...(opts.agents ? { agent_ids: opts.agents } : {}),
      limit: opts.limit,
      concurrency: 4
    });
    const agentSnapshots = safeArray(taskSnapshot?.agents).filter((agent) =>
      String(agent?.agent_id || "").startsWith(AGENT_PREFIX)
    );

    const allQueuedSignals = [];

    for (const agentSnapshot of agentSnapshots) {
      const agentId = agentSnapshot.agent_id;
      console.error(`[watcher] checking ${agentId}`);
      const agentState = getAgentState(state, agentId);
      const agentSummary = {
        tasks_checked: 0,
        overdue_tasks: 0,
        due_today_tasks: 0,
        due_later_today_tasks: 0,
        due_tomorrow_tasks: 0,
        future_tasks: 0,
        tasks_without_due: 0,
        routine_tasks: 0,
        routine_missing_tag: 0,
        raw_signals_found: 0,
        signals_found: 0,
        signals_queued: 0,
        errors: []
      };
      summary.agents[agentId] = agentSummary;
      summary.agents_checked += 1;

      try {
        if (!agentSnapshot.ok) {
          throw new Error(agentSnapshot.error || "Gebündelter Asana-Readback fehlgeschlagen.");
        }
        const workspaceGid = agentSnapshot.workspace_gid;
        if (!workspaceGid) throw new Error("Kein Asana-Workspace im whoami-Result gefunden.");
        agentState.workspace_gid = workspaceGid;
        const tasks = safeArray(agentSnapshot.tasks);
        const candidateSignals = [];

        for (const task of tasks) {
          const previousTaskState = agentState.tasks[task.gid];
          const details = { newStories: [], newAttachments: [] };

          const dueBucket = getTaskDueBucket(task);
          incrementDueBucketSummary(agentSummary, dueBucket);
          if (looksLikeRoutineTask(task)) {
            agentSummary.routine_tasks += 1;
            if (!hasRoutineTag(task)) agentSummary.routine_missing_tag += 1;
          }

          updateTaskState(agentState, task);
          const taskSignals = buildSignalsForTask(agentId, task, previousTaskState, details, detectedAt);
          candidateSignals.push(...taskSignals);
          agentSummary.tasks_checked += 1;
        }

        const freshRawSignals = sortSignals(candidateSignals).filter((signal) => !state.emitted_signals[signal.id]);
        const freshSignals = coalesceSignalsByTask(freshRawSignals);
        const queuedSignals = opts.baseline ? [] : freshSignals.slice(0, opts.maxSignalsPerAgent);
        const signalIdsToMarkSeen = opts.baseline
          ? freshRawSignals.map((signal) => signal.id)
          : queuedSignals.flatMap((signal) => signal.related_signal_ids || [signal.id]);

        for (const signalId of signalIdsToMarkSeen) {
          state.emitted_signals[signalId] = detectedAt;
        }

        for (const signal of queuedSignals) {
          allQueuedSignals.push(signal);
        }

        agentSummary.raw_signals_found = freshRawSignals.length;
        agentSummary.signals_found = freshSignals.length;
        agentSummary.signals_queued = queuedSignals.length;
        agentState.last_checked_at = detectedAt;
        summary.agents_succeeded += 1;
        summary.tasks_checked += agentSummary.tasks_checked;
        summary.overdue_tasks += agentSummary.overdue_tasks;
        summary.due_today_tasks += agentSummary.due_today_tasks;
        summary.due_later_today_tasks += agentSummary.due_later_today_tasks;
        summary.due_tomorrow_tasks += agentSummary.due_tomorrow_tasks;
        summary.future_tasks += agentSummary.future_tasks;
        summary.tasks_without_due += agentSummary.tasks_without_due;
        summary.routine_tasks += agentSummary.routine_tasks;
        summary.routine_missing_tag += agentSummary.routine_missing_tag;
        summary.raw_signals_found += agentSummary.raw_signals_found;
        summary.signals_found += agentSummary.signals_found;
        summary.signals_queued += agentSummary.signals_queued;
      } catch (error) {
        agentSummary.errors.push(error.message);
        summary.agents_failed += 1;
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
      await fs.writeFile(
        healthPath,
        `${JSON.stringify(
          {
            generated_at: nowIso(),
            status: summary.agents_failed > 0 ? "partial" : "ok",
            detected_at: detectedAt,
            agents_checked: summary.agents_checked,
            agents_succeeded: summary.agents_succeeded,
            agents_failed: summary.agents_failed,
            tasks_checked: summary.tasks_checked,
            overdue_tasks: summary.overdue_tasks,
            due_today_tasks: summary.due_today_tasks,
            due_later_today_tasks: summary.due_later_today_tasks,
            due_tomorrow_tasks: summary.due_tomorrow_tasks,
            future_tasks: summary.future_tasks,
            tasks_without_due: summary.tasks_without_due,
            routine_tasks: summary.routine_tasks,
            routine_missing_tag: summary.routine_missing_tag,
            raw_signals_found: summary.raw_signals_found,
            signals_found: summary.signals_found,
            signals_queued: summary.signals_queued
          },
          null,
          2
        )}\n`
      );
    }

    console.log(JSON.stringify({ summary, queued_signals: allQueuedSignals }, null, 2));
  } finally {
    if (client) await client.close().catch(() => {});
  }
}

function renderLog(summary, signals) {
  const lines = [
    `\n## ${summary.detected_at} - Asana Watcher`,
    "",
    `- mode: ${summary.mode}${summary.baseline ? " / baseline" : ""}`,
    `- agents_checked: ${summary.agents_checked}`,
    `- agents_succeeded: ${summary.agents_succeeded}`,
    `- agents_failed: ${summary.agents_failed}`,
    `- tasks_checked: ${summary.tasks_checked}`,
    `- overdue_tasks: ${summary.overdue_tasks}`,
    `- due_today_tasks: ${summary.due_today_tasks}`,
    `- due_later_today_tasks: ${summary.due_later_today_tasks}`,
    `- due_tomorrow_tasks: ${summary.due_tomorrow_tasks}`,
    `- future_tasks: ${summary.future_tasks}`,
    `- tasks_without_due: ${summary.tasks_without_due}`,
    `- routine_tasks: ${summary.routine_tasks}`,
    `- routine_missing_tag: ${summary.routine_missing_tag}`,
    `- raw_signals_found: ${summary.raw_signals_found}`,
    `- signals_found: ${summary.signals_found}`,
    `- signals_queued: ${summary.signals_queued}`
  ];

  for (const [agentId, data] of Object.entries(summary.agents)) {
    lines.push(
      `- ${agentId}: tasks=${data.tasks_checked}, overdue=${data.overdue_tasks}, due_today=${data.due_today_tasks}, due_later_today=${data.due_later_today_tasks}, due_tomorrow=${data.due_tomorrow_tasks}, future=${data.future_tasks}, no_due=${data.tasks_without_due}, routines=${data.routine_tasks}, routine_missing_tag=${data.routine_missing_tag}, raw=${data.raw_signals_found}, found=${data.signals_found}, queued=${data.signals_queued}`
    );
    for (const error of data.errors.slice(0, 3)) {
      lines.push(`  - error: ${truncate(error, 220)}`);
    }
  }

  for (const signal of signals) {
    lines.push(
      `- queued: ${signal.agent_id} / ${signal.signal_type} / ${signal.task_gid} / ${truncate(
        signal.task_name,
        120
      )} / related=${(signal.related_signal_types || [signal.signal_type]).join(",")}`
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

run().catch(async (error) => {
  console.error(error?.stack || error?.message || String(error));
  if (process.argv.includes("--write")) {
    await fs.mkdir(BetriebDir, { recursive: true }).catch(() => {});
    await fs
      .writeFile(
        healthPath,
        `${JSON.stringify(
          {
            generated_at: nowIso(),
            status: "failed",
            error: error?.message || String(error)
          },
          null,
          2
        )}\n`
      )
      .catch(() => {});
    await fs
      .appendFile(
        logPath,
        `\n## ${nowIso()} - Asana Watcher Technical Failure\n\n- status: failed\n- error: ${truncate(
          error?.message || String(error),
          500
        )}\n\n`
      )
      .catch(() => {});
  }
  process.exit(1);
});
