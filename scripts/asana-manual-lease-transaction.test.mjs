import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { processScanBinding } from "./asana-worker-process-readback.mjs";

const context = { threadId: "01a13d34-b615-7451-b51d-e5d75b31dbcf",
  source: "CODEX_THREAD_ID" };
const agent = "vip-ai-test";
const user = "user-1";

function fixture(t, taskIds = ["123"]) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "manual-reconcile-"));
  const db = new AsanaDispatchStore(path.join(dir, "queue.sqlite"));
  t.after(() => { db.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  const started = Date.now() - 12 * 60_000;
  db.acquireManualAgent(agent, "manual-run", context, started);
  for (const [index, gid] of taskIds.entries()) {
    db.acquireManualTask("manual-run", gid, context, started + 1000 + index);
  }
  const run = db.manualRunsForReconciliation({ now: Date.now() })[0];
  assert.ok(run);
  return { db, run };
}

function evidence(run, { open = [], unstable = [], terminal = "completed" } = {}) {
  const now = Date.now();
  const base = { source: "codex_app.read_thread", host_id: "local",
    thread_id: run.thread_id, runtime_status: "idle", turn_id: "turn-1",
    turn_status: terminal, started_at_ms: run.acquired_at_ms - 1000,
    completed_at_ms: now - 10_000, activity_digest: "a".repeat(64) };
  const nativeReadbacks = [{ ...base, observed_at_ms: now - 4000 },
    { ...base, observed_at_ms: now - 2000 }];
  const taskReadbacks = run.tasks.map((binding) => {
    const isOpen = open.includes(binding.task_gid);
    const story = { gid: `story-${binding.task_gid}`, resource_subtype: "comment_added",
      created_by: { gid: user },
      created_at: new Date(binding.acquired_at_ms + 5000).toISOString() };
    const row = { agent_id: run.agent_id, task: { gid: binding.task_gid,
      completed: !isOpen, assignee: { gid: user }, modified_at: new Date(now - 5000).toISOString() },
      stories: isOpen ? [story] : [], next_page: null };
    return { task_gid: binding.task_gid,
      evidence_story_gid: isOpen && !unstable.includes(binding.task_gid) ? story.gid : null,
      readbacks: [{ ...row, observed_at_ms: now - 4000 },
        { ...row, observed_at_ms: now - 2000,
          ...(unstable.includes(binding.task_gid) ? { next_page: "missing" } : {}) }] };
  });
  return { nativeReadbacks, taskReadbacks, agentUserGid: user };
}

function apply(db, run, proof, matches = []) {
  const scan = { ...processScanBinding(run), observed_at_ms: Date.now(),
    scan_succeeded: true, raw_sha256: "b".repeat(64), matching_processes: matches };
  return db._reconcileManualRunWithProcessReadback(run, proof, scan);
}

test("stable terminal and task evidence releases all exact leases once", t => {
  const { db, run } = fixture(t);
  const result = apply(db, run, evidence(run));
  assert.equal(result.applied, true);
  assert.equal(result.decision.action, "release");
  assert.equal(db.activeLeases().length, 0);
  assert.equal(db.manualRun(run.run_id).state, "released");
  assert.deepEqual(apply(db, run, evidence(run)),
    { applied: false, reason: "manual_fence_changed" });
});

test("unverified task stays quarantined while agent and verified task release", t => {
  const { db, run } = fixture(t, ["123", "456"]);
  const result = apply(db, run, evidence(run, { unstable: ["456"] }));
  assert.equal(result.applied, true);
  assert.equal(result.decision.action, "partial_quarantine");
  assert.deepEqual(db.activeLeases().map((row) => row.key), ["task:456"]);
  assert.equal(db.manualRun(run.run_id).state, "active");
  assert.deepEqual(db.manualRunsForReconciliation({ now: Date.now() + 61 * 60_000 }), []);
});

test("changed task fence, stale native proof and a live process leave all leases", t => {
  const { db, run } = fixture(t);
  const changed = { ...run, tasks: [{ ...run.tasks[0], lease_fence: run.tasks[0].lease_fence + 1 }] };
  assert.equal(apply(db, changed, evidence(changed)).reason,
    "manual_fence_changed");
  const stale = evidence(run);
  stale.nativeReadbacks = stale.nativeReadbacks.map((row) =>
    ({ ...row, observed_at_ms: row.observed_at_ms - 10 * 60_000 }));
  assert.equal(apply(db, run, stale).applied, false);
  assert.equal(apply(db, run, evidence(run), [{ pid: 42, reason: "dispatch-worker" }]).reason,
    "worker_inactivity_unverified");
  assert.equal(db.activeLeases().length, 2);
});

test("owner change quarantines the open task and legacy rows are never selected", t => {
  const { db, run } = fixture(t);
  const proof = evidence(run, { open: ["123"] });
  for (const pair of proof.taskReadbacks) for (const row of pair.readbacks) {
    row.task.assignee.gid = "other-owner";
  }
  const result = apply(db, run, proof);
  assert.equal(result.decision.action, "partial_quarantine");
  assert.deepEqual(db.activeLeases().map((row) => row.key), ["task:123"]);
  db.acquireLease("agent:vip-ai-old", "legacy-run", 60_000, Date.now() - 12 * 60_000);
  assert.deepEqual(db.manualRunsForReconciliation({ now: Date.now() }), []);
});

test("database failure rolls back every task and agent release", t => {
  const { db, run } = fixture(t, ["123", "456"]);
  db.db.exec(`CREATE TRIGGER block_manual_update BEFORE UPDATE ON manual_runs
    BEGIN SELECT RAISE(ABORT, 'simulated crash'); END;`);
  assert.throws(() => apply(db, run, evidence(run)), /simulated crash/);
  assert.equal(db.activeLeases().length, 3);
  assert.equal(db.manualRun(run.run_id).state, "active");
});
