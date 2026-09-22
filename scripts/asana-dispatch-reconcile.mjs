import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { storiesForTask, tool } from "./asana-dispatch-poller.mjs";
import { archiveCompletedThread } from "./codex-app-rpc.mjs";

const MCP_URL = process.env.WATCHER_MCP_URL || "https://vip-tools-mcp.onrender.com/mcp";

export function summarizeReconciliationReadback(run, task, stories, ownGid) {
  if (String(task?.gid || "") !== run.task_gid || !Array.isArray(stories) || !ownGid) {
    throw new Error("Incomplete reconciliation readback");
  }
  const later = stories.filter((story) => Date.parse(story.created_at || "") >= run.started_at_ms);
  return {
    run_id: run.run_id, agent_id: run.agent_id, task_gid: run.task_gid,
    thread_id: run.thread_id, run_state: run.state,
    task_completed: task.completed, task_assignee_gid: task.assignee?.gid || null,
    task_modified_at: task.modified_at || null,
    task_modified_after_start: Number.isFinite(Date.parse(task.modified_at || ""))
      ? Date.parse(task.modified_at) >= run.started_at_ms : null,
    stories_after_start: later.map((story) => ({ gid: story.gid,
      created_at: story.created_at, created_by_gid: story.created_by?.gid || null })),
    own_stories_after_start: later.filter((story) =>
      String(story.created_by?.gid || "") === String(ownGid)).length
  };
}

export async function inspectReconciliationRun(store, { runId, readback = null } = {}) {
  const run = store.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId);
  if (!run || run.state !== "needs_reconciliation") {
    throw new Error(`Run ${runId} is not awaiting reconciliation`);
  }
  if (readback) return summarizeReconciliationReadback(run,
    readback.task, readback.stories, readback.ownGid);
  const client = new Client({ name: "vip-asana-dispatch-reconciliation-inspector", version: "1.0.0" });
  try {
    await client.connect(new StreamableHTTPClientTransport(new URL(MCP_URL)));
    const taskResult = await tool(client, "asana_request", { agent_id: run.agent_id,
      method: "GET", path: `/tasks/${run.task_gid}`,
      params: { opt_fields: "gid,completed,assignee.gid,modified_at" } });
    const stories = await storiesForTask(client, run.agent_id, run.task_gid);
    const userResult = await tool(client, "asana_request", { agent_id: run.agent_id,
      method: "GET", path: "/users/me" });
    return summarizeReconciliationReadback(run, taskResult.response?.data,
      stories, userResult.response?.data?.gid);
  } finally {
    await client.close().catch(() => {});
  }
}

export async function resolveReconciliationRun(store, { runId, outcome, evidence,
  archive = archiveCompletedThread, now = Date.now() } = {}) {
  if (typeof runId !== "string" || !/^[a-zA-Z0-9_-]{2,180}$/.test(runId)) {
    throw new Error("A valid reconciliation runId is required");
  }
  if (!["acknowledged", "retry_after"].includes(outcome)) {
    throw new Error("Reconciliation outcome must be acknowledged or retry_after");
  }
  if (typeof evidence !== "string" || evidence.trim().length < 20) {
    throw new Error("Reconciliation requires a concrete evidence note");
  }
  const run = store.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId);
  if (!run) throw new Error(`Unknown reconciliation run ${runId}`);
  if (run.state !== "needs_reconciliation") {
    throw new Error(`Run ${runId} is not awaiting reconciliation`);
  }
  const claim = store.claimForRun(runId);
  store.settle(claim, { outcome, error: outcome === "retry_after" ? evidence.trim() : null, now });
  store.recordRun(claim, { threadId: run.thread_id, turnId: run.turn_id, state: "completed",
    error: `Manual reconciliation (${outcome}): ${evidence.trim()}`, now });
  let archived = false;
  let archiveError = null;
  if (outcome === "acknowledged" && run.thread_id) {
    try { archived = await archive(run.thread_id); }
    catch (error) { archiveError = String(error).slice(0, 500); }
  }
  return { run_id: runId, agent_id: run.agent_id, task_gid: run.task_gid,
    outcome, signals_resolved: claim.signals.length, archived, archive_error: archiveError };
}

export async function reconcileCompletedRuns(store, { readTask = null,
  archive = archiveCompletedThread, limit = 5 } = {}) {
  const runs = store.runsNeedingReconciliation()
    .filter((run) => run.state === "needs_reconciliation").slice(0, limit);
  const result = { checked: 0, acknowledged: 0, still_open: 0, archived: 0, errors: [] };
  if (!runs.length) return result;

  let client = null;
  try {
    if (!readTask) {
      client = new Client({ name: "vip-asana-dispatch-reconciler", version: "1.0.0" });
      await client.connect(new StreamableHTTPClientTransport(new URL(MCP_URL)));
      readTask = async (run) => {
        const response = await tool(client, "asana_request", { agent_id: run.agent_id,
          method: "GET", path: `/tasks/${run.task_gid}`,
          params: { opt_fields: "gid,completed,modified_at" } });
        return response.response?.data;
      };
    }
    for (const run of runs) {
      result.checked += 1;
      try {
        const task = await readTask(run);
        if (String(task?.gid || "") !== run.task_gid) throw new Error("Task readback GID mismatch");
        if (task.completed !== true) { result.still_open += 1; continue; }
        const claim = store.claimForRun(run.run_id);
        store.settle(claim, { outcome: "acknowledged" });
        store.recordRun(claim, { threadId: run.thread_id, turnId: run.turn_id, state: "completed" });
        result.acknowledged += 1;
        if (run.thread_id) {
          try { if (await archive(run.thread_id)) result.archived += 1; }
          catch (error) { result.errors.push({ run_id: run.run_id, stage: "archive", error: String(error) }); }
        }
      } catch (error) {
        result.errors.push({ run_id: run.run_id, stage: "readback", error: String(error).slice(0, 500) });
      }
    }
  } catch (error) {
    result.errors.push({ stage: "connect", error: String(error).slice(0, 500) });
  } finally {
    if (client) await client.close().catch(() => {});
  }
  return result;
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const args = process.argv.slice(2);
  const value = (name) => args.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const dbPath = value("db") || DEFAULT_DB_PATH;
  const runId = value("run");
  const inspectRunId = value("inspect-run");
  const outcome = value("outcome");
  const evidence = value("evidence");
  const store = new AsanaDispatchStore(dbPath);
  const operation = inspectRunId
    ? inspectReconciliationRun(store, { runId: inspectRunId })
    : runId || outcome || evidence
    ? resolveReconciliationRun(store, { runId, outcome, evidence })
    : reconcileCompletedRuns(store);
  operation.then((result) => {
    console.log(JSON.stringify(result));
    if (result.errors?.length || result.archive_error) process.exitCode = 2;
  }).catch((error) => { console.error(error); process.exitCode = 1; })
    .finally(() => store.close());
}
