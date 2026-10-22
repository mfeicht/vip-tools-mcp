import { connectCanonicalMcp, classifyConnectionError } from "./vip-mcp-call.mjs";
import fs from "node:fs/promises";
import path from "node:path";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { dependencyReadySignal, newCommentSignals, taskSignal } from "./asana-dispatch-signals.mjs";

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

export async function connectMcpClient(connect = connectCanonicalMcp) {
  try { return await connect(); }
  catch (error) { throw new Error(`mcp_connect: ${formatToolError(error)}`); }
}

export async function tool(client, name, args) {
  try {
    const result = await client.callTool({ name, arguments: args }, undefined,
      { timeout: 45_000, maxTotalTimeout: 45_000 });
    if (result?.isError) throw new Error(result.content?.map((item) => item.text).join("\n") || `${name} failed`);
    const payload = result.content?.find((item) => item.type === "text")?.text;
    if (!payload) throw new Error(`${name} returned no JSON`);
    return JSON.parse(payload);
  } catch (error) { throw new Error(`${name}: ${formatToolError(error)}`); }
}

export async function storiesForTask(client, agentId, taskGid) {
  const stories = [];
  let offset = null;
  const seenOffsets = new Set();
  const seenStories = new Set();
  for (let page = 0; page < 30; page += 1) {
    const result = await tool(client, "asana_request", {
      agent_id: agentId, method: "GET", path: `/tasks/${taskGid}/stories`,
      params: { limit: 100, opt_fields: STORY_FIELDS, ...(offset ? { offset } : {}) }
    });
    if (!Array.isArray(result.response?.data) ||
        !Object.hasOwn(result.response, "next_page") ||
        (result.response.next_page !== null &&
          !result.response.next_page?.offset)) throw new Error(`Incomplete story page on ${taskGid}`);
    for(const story of result.response.data) {
      if(!/^\d+$/.test(story?.gid || "") || seenStories.has(story.gid) ||
          !Number.isFinite(Date.parse(story.created_at))) throw new Error(`Invalid or duplicate story on ${taskGid}`);
      seenStories.add(story.gid);
    }
    stories.push(...result.response.data);
    offset = result.response?.next_page?.offset || null;
    if (!offset) return stories;
    if (seenOffsets.has(offset)) throw new Error(`Repeated story offset on ${taskGid}`);
    seenOffsets.add(offset);
  }
  throw new Error(`Story pagination incomplete for ${taskGid}`);
}

function assertSearchComplete(search, agentId) {
  if (search.search_status !== "ok" || search.search_complete !== true ||
      search.result_truncated !== false || !Array.isArray(search.tasks)) {
    throw new Error(`Incomplete involved search for ${agentId}: ${search.search_status || "unknown"}`);
  }
}

export async function scanAgent(client, store, snapshot, agentUserGids, scanStartedAt, write) {
  const agentId = snapshot.agent_id;
  if (snapshot.ok !== true || snapshot.truncated !== false || !Array.isArray(snapshot.tasks) || !snapshot.user_gid || !snapshot.workspace_gid) {
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

  const openTasks = [...tasks.values()].filter(task => task.completed === false);
  const auditLimit = Math.min(8, Math.ceil(openTasks.length / 12));
  const audit = new Set(openTasks.filter(task => {
    const observation = store.observation(agentId,String(task.gid));
    return !observation?.stories_checked_at_ms || observation.stories_checked_at_ms <= scanStartedAt.getTime()-15*60000;
  }).sort((a,b) => (store.observation(agentId,String(a.gid))?.stories_checked_at_ms || 0) -
    (store.observation(agentId,String(b.gid))?.stories_checked_at_ms || 0) || String(a.gid).localeCompare(String(b.gid)))
    .slice(0,auditLimit).map(task=>String(task.gid)));
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
    const readStories = !observation || task.modified_at !== observation.modified_at || audit.has(taskGid);
    if (readStories) {
      const stories = await storiesForTask(client, agentId, taskGid);
      storiesRead += 1;
      const comments = newCommentSignals(agentId, task, stories, {
        observation, userGid: snapshot.user_gid, allAgentUserGids: agentUserGids,
        owned, historicalBackfill: !observation?.history_checked_at_ms, now: scanStartedAt
      });
      newSignals.push(...comments.signals);
      latest = comments.latest;
    }
    observations.push({ agent_id: agentId, task_gid: taskGid,
      modified_at: task.modified_at || null,
      latest_story_at: latest?.at || null, latest_story_gid: latest?.gid || null,
      stories_checked_at_ms: readStories ? scanStartedAt.getTime() : null,
      history_checked_at_ms: readStories ? scanStartedAt.getTime() : null });
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

export async function scanSnapshots(client, store, snapshots, agentUserGids, scanStartedAt, write,
  scan = scanAgent) {
  const result={agents:[],errors:[]};
  for (const snapshot of snapshots) {
    try { result.agents.push(await scan(client,store,snapshot,agentUserGids,scanStartedAt,write)); }
    catch(error) {
      result.errors.push({agent_id:snapshot.agent_id,error:String(error?.message || error).slice(0,500)});
      // Stop this tick after transport failure. The next scheduled tick owns recovery.
      if(["remote_rate_limited","cloudflare_managed_challenge","remote_mcp_transient_error",
          "network_transport_error","dns_resolution_error","mcp_coordination_timeout"]
          .includes(classifyConnectionError(error))) break;
    }
  }
  return result;
}

export async function poll({ write = false, agentIds = null, db = DEFAULT_DB_PATH } = {}) {
  if (write && agentIds) throw new Error("Partial write scans cannot establish all agent identities");
  const scanStartedAt = new Date();
  const registry = JSON.parse(await fs.readFile(path.resolve(import.meta.dirname, "..", "..",
    "VIP-AI-Memory/03-Betrieb/Agenten-Scope-Registry.json"), "utf8"));
  const expectedAgentIds = agentIds || Object.keys(registry.agents || {});
  if (!expectedAgentIds.length) throw new Error("Agent registry is empty");
  const store = new AsanaDispatchStore(write ? db : ":memory:");
  let client;
  const result = { at: scanStartedAt.toISOString(), mode: write ? "write" : "dry-run",
    agents: [], errors: [], counts: {} };
  try {
    client = await connectMcpClient();
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
    Object.assign(result,await scanSnapshots(client,store,snapshots.agents,agentUserGids,scanStartedAt,write));
    result.counts = store.counts();
    if (result.errors.length) process.exitCode = 2;
    return result;
  } finally {
    if (client) await Promise.race([
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
