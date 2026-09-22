import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { inspectReconciliationRun, reconcileCompletedRuns, resolveReconciliationRun } from "./asana-dispatch-reconcile.mjs";

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

test("operator can acknowledge an open run only with concrete reconciliation evidence", async (t) => {
  const store = fixture(t);
  const archived = [];
  await assert.rejects(resolveReconciliationRun(store, {
    runId: "run-1", outcome: "acknowledged", evidence: "too short"
  }), /concrete evidence note/);
  const result = await resolveReconciliationRun(store, {
    runId: "run-1", outcome: "acknowledged",
    evidence: "Codex thread completed and the expected Asana story was read back.",
    archive: async (id) => { archived.push(id); return true; }, now: 3000
  });
  assert.deepEqual(result, { run_id: "run-1", agent_id: "vip-ai-test", task_gid: "123456",
    outcome: "acknowledged", signals_resolved: 1, archived: true, archive_error: null });
  assert.deepEqual(archived, ["thread-1"]);
  assert.deepEqual(store.activeLeases(), []);
  assert.deepEqual(store.counts(), { acknowledged: 1 });
  const run = store.db.prepare("SELECT state,last_error FROM runs WHERE run_id='run-1'").get();
  assert.equal(run.state, "completed");
  assert.match(run.last_error, /expected Asana story/);
});

test("operator can safely retry a run proven to have made no external change", async (t) => {
  const store = fixture(t);
  const result = await resolveReconciliationRun(store, {
    runId: "run-1", outcome: "retry_after",
    evidence: "Codex stopped before mutation and the task modified timestamp is unchanged.",
    archive: async () => { throw new Error("must not archive retries"); }, now: 3000
  });
  assert.equal(result.outcome, "retry_after");
  assert.equal(result.archived, false);
  assert.deepEqual(store.activeLeases(), []);
  assert.deepEqual(store.counts(), { retry_after: 1 });
  assert.match(store.db.prepare("SELECT last_error FROM signals").get().last_error,
    /stopped before mutation/);
});

test("read-only reconciliation inspection reports post-run stories without releasing leases", async (t) => {
  const store = fixture(t);
  const result = await inspectReconciliationRun(store, { runId: "run-1", readback: {
    task: { gid: "123456", completed: false, modified_at: "1970-01-01T00:00:01Z",
      assignee: { gid: "user-1" } },
    stories: [{ gid: "story-1", created_at: "1970-01-01T00:00:03Z",
      created_by: { gid: "user-1" } }], ownGid: "user-1"
  } });
  assert.equal(result.task_modified_after_start, false);
  assert.equal(result.own_stories_after_start, 1);
  assert.equal(result.stories_after_start[0].gid, "story-1");
  assert.equal(store.activeLeases().length, 2);
  assert.deepEqual(store.counts(), { leased: 1 });
});
