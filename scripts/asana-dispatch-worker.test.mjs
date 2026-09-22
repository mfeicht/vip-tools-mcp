import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";

test("MCP failure before agent start retries and releases both leases", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-worker-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const db = path.join(dir, "store.sqlite");
  const store = new AsanaDispatchStore(db);
  store.enqueue({ id: "preflight-test", agent_id: "vip-ai-research",
    task_gid: "123456", source_version: "preflight-test", kind: "due_task",
    priority: 60, available_at_ms: Date.now() - 1000 });
  store.close();

  const result = spawnSync(process.execPath,
    [path.join(import.meta.dirname, "asana-dispatch-worker.mjs"), `--db=${db}`],
    { env: { ...process.env, WATCHER_MCP_URL: "http://127.0.0.1:1/mcp" },
      encoding: "utf8", timeout: 10_000 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).status, "retry_after");

  const after = new AsanaDispatchStore(db);
  assert.deepEqual(after.counts(), { retry_after: 1 });
  assert.deepEqual(after.activeLeases(), []);
  const run = after.db.prepare("SELECT state,thread_id FROM runs LIMIT 1").get();
  assert.equal(run.state, "completed");
  assert.equal(run.thread_id, null);
  after.close();
});
