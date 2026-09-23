import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import fs from "node:fs/promises";
import path from "node:path";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { dependencyReadySignal, newCommentSignals, taskSignal } from "./asana-dispatch-signals.mjs";

const MCP_URL = process.env.WATCHER_MCP_URL || "https://vip-tools-mcp.onrender.com/mcp";
const TASK_FIELDS = "gid,name,completed,created_by.gid,assignee.gid,due_on,due_at,modified_at,permalink_url,tags.name,followers.gid";
const STORY_FIELDS = "gid,created_at,created_by.gid,resource_subtype,type,text,html_text";

function options(argv) {
  const opts = { write: false, agentIds: null, db: DEFAULT_DB_PATH };
  for (const arg of argv) {
    if (arg === "--write") opts.write = true;
    else if (arg === "--dry-run") opts.write = false;
    else if (arg.startsWith("--agents=")) opts.agentIds = arg.slice(9).split(",").filter(Boolean);
    else if (arg.startsWith("--db=")) opts.db = arg.slice(5);
    else throw new Error(`Unknown option: ${arg}`);
  }
  return opts;
}

function toolErrorStatus(error) {
  const status = Number(error?.code ?? error?.status ?? error?.statusCode);
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : null;
}

export function retryableToolError(error) {
  const status = toolErrorStatus(error);
  if ([429, 502, 503, 504].includes(status)) return true;
  return /\b(429|502|503|504)\b|timeout|fetch failed|cloudflare/i.test(String(error));
}

export function formatToolError(error) {
  const message = String(error?.message || error || "unknown tool error");
  const status = toolErrorStatus(error);
  return status ? `${message} (HTTP ${status})` : message;
}

export async function tool(client, name, args) {
  let lastError;
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      const result = await client.callTool({ name, arguments: args }, undefined,
        { timeout: 45_000, maxTotalTimeout: 45_000 });
      if (result?.isError) throw new Error(result.content?.map((item) => item.text).join("\n") || `${name} failed`);
      const payload = result.content?.find((item) => item.type === "text")?.text;
      if (!payload) throw new Error(`${name} returned no JSON`);
      return JSON.parse(payload);
    } catch (error) {
      lastError = error;
      if (attempt === 2 || !retryableToolError(error)) break;
      const status = toolErrorStatus(error);
      await new Promise((resolve) => setTimeout(resolve,
        status === 429 || /429|cloudflare/i.test(String(error)) ? 60_000 : 10_000));
    }
  }
  throw new Error(`${name}: ${formatToolError(lastError)}`);
}

export async function storiesForTask(client, agentId, taskGid) {
  const stories = [];
  let offset = null;
  const seenOffsets = new Set();
  for (let page = 0; page < 30; page += 1) {
    const result = await tool(client, "asana_request", {
      agent_id: agentId, method: "GET", path: `/tasks/${taskGid}/stories`,
      params: { limit: 100, opt_fields: STORY_FIELDS, ...(offset ? { offset } : {}) }
    });
    stories.push(...(result.response?.data || []));
    offset = result.response?.next_page?.offset || null;
    if (!offset) return stories;
    if (seenOffsets.has(offset)) throw new Error(`Repeated story offset on ${taskGid}`);
    seenOffsets.add(offset);
  }
  throw new Error(`Story pagination incomplete for ${taskGid}`);
}

function assertSearchComplete(search, agentId) {
  if (search.search_status !== "ok" || search.search_complete === false ||
      search.result_truncated === true || !Array.isArray(search.tasks)) {
    throw new Error(`Incomplete involved search for ${agentId}: ${search.search_status || "unknown"}`);
  }
}

export async function scanAgent(client, store, snapshot, agentUserGids, scanStartedAt, write) {
  const agentId = snapshot.agent_id;
  if (!snapshot.ok || snapshot.truncated || !snapshot.user_gid || !snapshot.workspace_gid) {
    throw new Error(`${agentId}: incomplete assigned task snapshot: ${snapshot.error || "truncated"}`);
  }
  const cursor = store.pollCursor(agentId);
  const modifiedSince = new Date(Math.max(0,
    (cursor ? Date.parse(cursor) : scanStartedAt.getTime() - 24 * 60 * 60_000) - 15 * 60_000)).toISOString();
  const search = await tool(client, "asana_search_tasks", {
    agent_id: agentId, workspace_gid: snapshot.workspace_gid,
    involved_any: String(snapshot.user_gid), completed: false, modified_since: modifiedSince,
    max_pages_per_branch: 10, limit: 100, opt_fields: TASK_FIELDS
  });
  assertSearchComplete(search, agentId);

  const tasks = new Map();
  for (const task of search.tasks) if (task?.gid) tasks.set(String(task.gid), task);
  for (const task of snapshot.tasks || []) if (task?.gid) {
    tasks.set(String(task.gid), { ...tasks.get(String(task.gid)), ...task });
  }

  const newSignals = [];
  const observations = [];
  let storiesRead = 0;
  for (const task of tasks.values()) {
    if (task.completed) continue;
    const taskGid = String(task.gid);
    const observation = store.observation(agentId, taskGid);
    const assignedHere = String(task.assignee?.gid || "") === String(snapshot.user_gid);
    const owned = assignedHere || (!task.assignee?.gid &&
      String(task.created_by?.gid || "") === String(snapshot.user_gid));
    if (assignedHere) {
      const signal = taskSignal(agentId, task, { firstSeen: !observation, now: scanStartedAt });
      if (signal) newSignals.push(signal);
    }
    let latest = null;
    if (!observation || task.modified_at !== observation.modified_at) {
      const stories = await storiesForTask(client, agentId, taskGid);
      storiesRead += 1;
      const comments = newCommentSignals(agentId, task, stories, {
        observation, userGid: snapshot.user_gid, allAgentUserGids: agentUserGids,
        owned, now: scanStartedAt
      });
      newSignals.push(...comments.signals);
      latest = comments.latest;
    }
    observations.push({ agent_id: agentId, task_gid: taskGid,
      modified_at: task.modified_at || null,
      latest_story_at: latest?.at || null, latest_story_gid: latest?.gid || null });
  }

  const dependencyUpdates = [];
  for (const watch of store.dependencyWatches(agentId, scanStartedAt.getTime())) {
    const result = await tool(client, "asana_request", {
      agent_id: agentId, method: "GET", path: `/tasks/${watch.linked_task_gid}`,
      params: { opt_fields: "gid,completed,modified_at" }
    });
    const linkedTask = result.response?.data;
    if (String(linkedTask?.gid || "") !== watch.linked_task_gid ||
        typeof linkedTask.completed !== "boolean") {
      throw new Error(`Dependency readback incomplete for ${watch.task_gid}`);
    }
    const ready = dependencyReadySignal(agentId, watch.task_gid, linkedTask);
    if (linkedTask.completed && !ready) {
      throw new Error(`Dependency completion version missing for ${watch.task_gid}`);
    }
    if (ready) newSignals.push(ready);
    dependencyUpdates.push({ watch, ready: Boolean(ready) });
  }

  let inserted = 0;
  if (write) {
    store.transaction(() => {
      for (const signal of newSignals) if (store.enqueue(signal, scanStartedAt.getTime())) inserted += 1;
      for (const observation of observations) store.observeTask(observation, scanStartedAt.getTime());
      for (const item of dependencyUpdates) {
        if (item.ready) store.clearDependencyWatch(agentId, item.watch.task_gid);
        else store.markDependencyChecked(agentId, item.watch.task_gid, scanStartedAt.getTime());
      }
      store.setPollCursor(agentId, scanStartedAt.toISOString());
    });
  }
  return { agent_id: agentId, assigned_tasks: (snapshot.tasks || []).length,
    involved_tasks: search.tasks.length, tasks_scanned: tasks.size, stories_read: storiesRead,
    candidate_signals: newSignals.length, signals_inserted: inserted,
    dependencies_checked: dependencyUpdates.length };
}

export async function poll({ write = false, agentIds = null, db = DEFAULT_DB_PATH } = {}) {
  if (write && agentIds) throw new Error("Partial write scans cannot establish all agent identities");
  const scanStartedAt = new Date();
  const registry = JSON.parse(await fs.readFile(path.resolve(import.meta.dirname, "..", "..",
    "VIP-AI-Memory/03-Betrieb/Agenten-Scope-Registry.json"), "utf8"));
  const expectedAgentIds = agentIds || Object.keys(registry.agents || {});
  if (!expectedAgentIds.length) throw new Error("Agent registry is empty");
  const store = new AsanaDispatchStore(write ? db : ":memory:");
  const client = new Client({ name: "vip-asana-dispatch-sensor", version: "1.0.0" });
  const result = { at: scanStartedAt.toISOString(), mode: write ? "write" : "dry-run",
    agents: [], errors: [], counts: {} };
  try {
    await client.connect(new StreamableHTTPClientTransport(new URL(MCP_URL)));
    const snapshots = await tool(client, "asana_agents_open_task_snapshot", {
      agent_ids: expectedAgentIds, limit: 100, max_pages: 10, concurrency: 4
    });
    if (!Array.isArray(snapshots.agents) || snapshots.agents.length === 0) {
      throw new Error("No agent snapshots returned");
    }
    const incomplete = snapshots.agents.filter((item) =>
      !item.ok || item.truncated || !item.user_gid || !item.workspace_gid);
    const seenAgentIds = new Set(snapshots.agents.map((item) => item.agent_id));
    const missing = expectedAgentIds.filter((agentId) => !seenAgentIds.has(agentId));
    if (incomplete.length || missing.length) {
      throw new Error(`Agent identity snapshot incomplete: ${[
        ...incomplete.map((item) => item.agent_id), ...missing].join(",")}`);
    }
    const agentUserGids = new Set(snapshots.agents.filter((item) => item.ok).map((item) => String(item.user_gid)));
    for (const snapshot of snapshots.agents) {
      try {
        result.agents.push(await scanAgent(client, store, snapshot, agentUserGids, scanStartedAt, write));
      } catch (error) {
        result.errors.push({ agent_id: snapshot.agent_id, error: String(error?.message || error).slice(0, 500) });
      }
    }
    result.counts = store.counts();
    if (result.errors.length) process.exitCode = 2;
    return result;
  } finally {
    await Promise.race([
      client.close().catch(() => {}),
      new Promise((resolve) => setTimeout(resolve, 5_000))
    ]);
    store.close();
  }
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  poll(options(process.argv.slice(2))).then((result) => console.log(JSON.stringify(result)))
    .catch((error) => { console.error(error); process.exitCode = 1; });
}
