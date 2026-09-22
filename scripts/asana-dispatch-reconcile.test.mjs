import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { reconcileCompletedRuns } from "./asana-dispatch-reconcile.mjs";

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-reconcile-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const store = new AsanaDispatchStore(path.join(dir, "store.sqlite"));
  t.after(() => store.close());
  store.enqueue({ id: "signal-1", agent_id: "vip-ai-test", task_gid: "123456",
    source_version: "signal-1", kind: "due_task", priority: 50, available_at_ms: 1000 }, 1000);
  const claim = store.claimNext("run-1", { now: 1000 });
  store.recordRun(claim, { threadId: "thread-1", state: "needs_reconciliation",
    error: "post-readback transport failure", now: 2000 });
  return store;
}

test("completed task readback releases ambiguous run and archives its thread", async (t) => {
  const store = fixture(t);
  const archived = [];
  const result = await reconcileCompletedRuns(store, {
    readTask: async () => ({ gid: "123456", completed: true }),
    archive: async (id) => { archived.push(id); return true; }
  });
  assert.deepEqual(result, { checked: 1, acknowledged: 1, still_open: 0, archived: 1, errors: [] });
  assert.deepEqual(archived, ["thread-1"]);
  assert.deepEqual(store.activeLeases(), []);
  assert.deepEqual(store.counts(), { acknowledged: 1 });
  assert.equal(store.db.prepare("SELECT state FROM runs WHERE run_id='run-1'").get().state, "completed");
});

test("open task stays fenced after ambiguous worker outcome", async (t) => {
  const store = fixture(t);
  const result = await reconcileCompletedRuns(store, {
    readTask: async () => ({ gid: "123456", completed: false }),
    archive: async () => { throw new Error("must not archive"); }
  });
  assert.equal(result.still_open, 1);
  assert.equal(result.acknowledged, 0);
  assert.equal(store.activeLeases().length, 2);
  assert.deepEqual(store.counts(), { leased: 1 });
});
