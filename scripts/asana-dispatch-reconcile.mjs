import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { tool } from "./asana-dispatch-poller.mjs";
import { archiveCompletedThread } from "./codex-app-rpc.mjs";

const MCP_URL = process.env.WATCHER_MCP_URL || "https://vip-tools-mcp.onrender.com/mcp";

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
  const store = new AsanaDispatchStore(DEFAULT_DB_PATH);
  reconcileCompletedRuns(store).then((result) => {
    console.log(JSON.stringify(result));
    if (result.errors.length) process.exitCode = 2;
  }).catch((error) => { console.error(error); process.exitCode = 1; })
    .finally(() => store.close());
}
