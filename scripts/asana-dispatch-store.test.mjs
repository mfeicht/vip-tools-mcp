import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { executeLeaseCommand } from "./asana-agent-lease.mjs";

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const filename = path.join(dir, "store.sqlite");
  const first = new AsanaDispatchStore(filename);
  const second = new AsanaDispatchStore(filename);
  t.after(() => { first.close(); second.close(); });
  return { first, second };
}

function signal(id, task = "123", agent = "vip-ai-test", now = 1000) {
  return { id, task_gid: task, agent_id: agent, story_gid: id, source_version: id,
    kind: "human_comment", priority: 90, available_at_ms: now };
}

test("deduplicates signals and groups comments on one task", (t) => {
  const { first, second } = fixture(t);
  assert.equal(first.enqueue(signal("story-1"), 1000), true);
  assert.equal(second.enqueue(signal("story-1"), 1000), false);
  assert.equal(first.enqueue(signal("story-2"), 1000), true);
  const claim = first.claimNext("run-1", { now: 1000 });
  assert.deepEqual(claim.signals.map((item) => item.id), ["story-1", "story-2"]);
  assert.equal(second.claimNext("run-2", { now: 1000 }), null);
  assert.equal(first.settle(claim, { outcome: "acknowledged", now: 2000 }), 2);
  assert.deepEqual(second.counts(), { acknowledged: 2 });
});

test("agent and task leases fence parallel starts across connections", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1", "123", "vip-ai-test"), 1000);
  first.enqueue(signal("story-2", "456", "vip-ai-test"), 1000);
  first.enqueue(signal("story-3", "789", "vip-ai-other"), 1000);
  const firstClaim = first.claimNext("run-1", { now: 1000 });
  const otherClaim = second.claimNext("run-2", { now: 1000 });
  assert.equal(firstClaim.signals[0].id, "story-1");
  assert.equal(otherClaim.signals[0].id, "story-3");
  assert.equal(first.claimNext("run-3", { now: 1000 }), null);
  first.settle(firstClaim, { outcome: "acknowledged", now: 2000 });
  const resumed = second.claimNext("run-3", { now: 2000 });
  assert.equal(resumed.signals[0].id, "story-2");
});

test("claim persists preflight with both leases and signals in one transaction", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  const claim = first.claimNext("run-1", { now: 1000 });
  assert.equal(second.db.prepare("SELECT launch_phase FROM runs WHERE run_id=?")
    .get("run-1").launch_phase, "preflight");
  assert.equal(second.claimForRun("run-1").signals[0].id, "story-1");
  assert.equal(claim.agent_lease.expires_at_ms, 1000 + 20 * 60_000);
  assert.equal(second.claimNext("run-2", { now: 2000 }), null);
});

test("a duplicate run ID rolls back the entire new claim", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1", "123", "vip-ai-first"), 1000);
  first.claimNext("same-run", { now: 1000 });
  first.enqueue(signal("story-2", "456", "vip-ai-second"), 1000);
  assert.throws(() => second.claimNext("same-run", { now: 2000 }), /UNIQUE constraint failed/);
  assert.equal(first.db.prepare("SELECT status FROM signals WHERE id='story-2'").get().status,
    "pending");
  assert.equal(first.db.prepare("SELECT COUNT(*) AS n FROM leases WHERE run_id='same-run'")
    .get().n, 2);
});

test("launch fence verifies run, both leases and every signal before spawn", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  const claim = first.claimNext("run-1", { now: 1000 });
  const stale = { ...claim, task_lease: { ...claim.task_lease, fence: claim.task_lease.fence + 1 } };
  assert.equal(second.beginCodexLaunch(stale, 2000), false);
  assert.equal(second.db.prepare("SELECT launch_phase FROM runs WHERE run_id=?")
    .get("run-1").launch_phase, "preflight");
  second.db.prepare("UPDATE signals SET lease_fence=lease_fence+1 WHERE id='story-1'").run();
  assert.equal(first.beginCodexLaunch(claim, 2000), false);
  second.db.prepare("UPDATE signals SET lease_fence=? WHERE id='story-1'")
    .run(claim.task_lease.fence);
  assert.equal(first.beginCodexLaunch(claim, 2000), true);
  assert.equal(second.beginCodexLaunch(claim, 2001), false);
  assert.equal(second.db.prepare("SELECT launch_phase FROM runs WHERE run_id=?")
    .get("run-1").launch_phase, "launching");
  assert.equal(second.db.prepare("SELECT expires_at_ms FROM leases WHERE key=?")
    .get(claim.task_lease.key).expires_at_ms, 2000 + 2 * 60 * 60_000);
});

test("only an expired, intact preflight claim can be retried after a crash", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  const claim = first.claimNext("run-1", { now: 1000, ttlMs: 60_000 });
  assert.equal(second.recoverNeverLaunchedRun("run-1", 61_000), false);
  assert.equal(second.recoverNeverLaunchedRun("run-1", 61_001), true);
  assert.equal(first.recoverNeverLaunchedRun("run-1", 61_002), false);
  assert.deepEqual(first.activeLeases(), []);
  assert.deepEqual(second.counts(), { retry_after: 1 });
  assert.equal(second.db.prepare("SELECT attempts,available_at_ms FROM signals WHERE id='story-1'")
    .get().attempts, 0);
  assert.equal(second.claimNext("run-2", { now: 61_001 + 5 * 60_000 - 1 }), null);
  assert.equal(second.claimNext("run-2", { now: 61_001 + 5 * 60_000 })?.signals[0].id,
    "story-1");
  assert.equal(claim.run_id, "run-1");
});

test("launching, legacy NULL and changed fences never get preflight recovery", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1", "123"), 1000);
  const launched = first.claimNext("run-launching", { now: 1000, ttlMs: 60_000 });
  assert.equal(first.beginCodexLaunch(launched, 2000), true);
  assert.equal(second.recoverNeverLaunchedRun("run-launching", 2 * 60 * 60_000), false);
  first.enqueue(signal("story-2", "456", "vip-ai-other"), 1000);
  first.claimNext("run-legacy", { now: 1000, ttlMs: 60_000 });
  second.db.prepare("UPDATE runs SET launch_phase=NULL WHERE run_id='run-legacy'").run();
  assert.equal(first.recoverNeverLaunchedRun("run-legacy", 120_000), false);
  first.enqueue(signal("story-3", "789", "vip-ai-third"), 1000);
  first.claimNext("run-broken", { now: 1000, ttlMs: 60_000 });
  second.db.prepare("UPDATE signals SET lease_fence=lease_fence+1 WHERE id='story-3'").run();
  assert.equal(first.recoverNeverLaunchedRun("run-broken", 120_000), false);
  assert.equal(second.db.prepare("SELECT status FROM signals WHERE id='story-3'").get().status,
    "leased");
});

test("old run schemas migrate without labeling historical runs as preflight", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-migrate-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const filename = path.join(dir, "store.sqlite");
  const old = new DatabaseSync(filename);
  old.exec(`CREATE TABLE runs (run_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL,
    task_gid TEXT NOT NULL, thread_id TEXT, turn_id TEXT, state TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL, last_error TEXT);
    INSERT INTO runs VALUES ('old-run','vip-ai-test','123',NULL,NULL,'claimed',1000,1000,NULL);`);
  old.close();
  const store = new AsanaDispatchStore(filename);
  t.after(() => store.close());
  const migrated = store.db.prepare("SELECT launch_phase FROM runs WHERE run_id='old-run'").get();
  assert.equal(migrated.launch_phase, null);
  assert.equal(store.recoverNeverLaunchedRun("old-run", 120_000), false);
});

test("comment arriving during an active task waits for the next run", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1", "123", "vip-ai-test"), 1000);
  const active = first.claimNext("run-1", { now: 1000 });
  second.enqueue(signal("story-2", "123", "vip-ai-test"), 2000);
  assert.equal(second.claimNext("run-2", { now: 2000 }), null);
  first.settle(active, { outcome: "acknowledged", now: 3000 });
  const next = second.claimNext("run-2", { now: 3000 });
  assert.deepEqual(next.signals.map((item) => item.id), ["story-2"]);
});

test("expired leases remain blocked until verified reconciliation", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  const claim = first.claimNext("run-1", { now: 1000, ttlMs: 60_000 });
  first.enqueue(signal("story-2", "456"), 1000);
  assert.equal(second.claimNext("run-2", { now: 120_000 }), null);
  assert.equal(first.settle(claim, { outcome: "retry_after", error: "transport", now: 120_000 }), 1);
  assert.deepEqual(first.counts(), { pending: 1, retry_after: 1 });
  const retry = second.claimNext("run-2", { now: 120_000 });
  assert.equal(retry.signals[0].id, "story-2");
});

test("fencing tokens reject stale acknowledgements and retries dead-letter", (t) => {
  const { first, second } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  let claim = first.claimNext("run-1", { now: 1000 });
  const stale = { ...claim, task_lease: { ...claim.task_lease, fence: claim.task_lease.fence + 1 } };
  assert.throws(() => first.settle(stale, { outcome: "acknowledged", now: 2000 }), /stale fencing token/);
  for (let attempt = 1; attempt <= 5; attempt += 1) {
    const now = attempt * 1_000_000;
    first.settle(claim, { outcome: "retry_after", error: "test", now, maxAttempts: 5 });
    if (attempt < 5) claim = second.claimNext(`run-${attempt + 1}`, { now: now + 1_000_000 });
  }
  assert.deepEqual(first.counts(), { dead_letter: 1 });
});

test("scheduled agent leases share the same gate as dispatch", (t) => {
  const { first, second } = fixture(t);
  const acquired = executeLeaseCommand(first, { command: "acquire-agent", agent: "vip-ai-research" });
  assert.equal(acquired.acquired, true);
  assert.equal(executeLeaseCommand(second, { command: "acquire-agent", agent: "vip-ai-research" }).acquired, false);
  assert.equal(executeLeaseCommand(first, { command: "acquire-task", run: acquired.run_id, task: "123" }).acquired, true);
  first.enqueue(signal("story-1", "123", "vip-ai-research"));
  assert.equal(second.claimNext("run-dispatch"), null);
  assert.equal(executeLeaseCommand(first, { command: "release", run: acquired.run_id }).released, 2);
  assert.equal(second.claimNext("run-dispatch")?.signals[0].id, "story-1");
});

test("observations and cursors persist atomically", (t) => {
  const { first, second } = fixture(t);
  first.transaction(() => {
    first.observeTask({ agent_id: "vip-ai-research", task_gid: "123",
      modified_at: "2026-09-21T10:00:00Z", latest_story_at: "2026-09-21T09:00:00Z",
      latest_story_gid: "789" });
    first.setPollCursor("vip-ai-research", "2026-09-21T10:01:00Z");
  });
  assert.equal(second.observation("vip-ai-research", "123").latest_story_gid, "789");
  assert.equal(second.pollCursor("vip-ai-research"), "2026-09-21T10:01:00Z");
});

test("resource events require reconciliation before cursor pages can advance", (t) => {
  const { first, second } = fixture(t);
  const agent = "vip-ai-research";
  const resource = "123456";
  assert.equal(first.stageEventReconciliation(agent, resource, "bootstrap-1", 1000).state,
    "needs_reconciliation");
  assert.throws(() => second.applyEventPage(agent, resource, "bootstrap-1", "page-1", false,
    () => second.enqueue(signal("event-1")), 2000), /requires reconciliation/);
  assert.equal(second.counts().pending || 0, 0);
  assert.throws(() => first.finishEventReconciliation(agent, resource, "old-token", () => {}, 2000),
    /cursor changed/);
  const ready = first.finishEventReconciliation(agent, resource, "bootstrap-1",
    () => first.enqueue(signal("reconciled-1")), 2000);
  assert.equal(ready.state, "ready");
  assert.equal(second.counts().pending, 1);
  assert.equal(second.eventCursor(agent, resource).sync_token, "bootstrap-1");
});

test("resource event pages commit signals and cursor together across connections", (t) => {
  const { first, second } = fixture(t);
  const agent = "vip-ai-research";
  const resource = "123456";
  first.stageEventReconciliation(agent, resource, "bootstrap-1", 1000);
  first.finishEventReconciliation(agent, resource, "bootstrap-1", () => {}, 2000);
  assert.throws(() => first.applyEventPage(agent, resource, "bootstrap-1", "page-1", true,
    () => { first.enqueue(signal("event-1")); throw new Error("page processing failed"); }, 3000),
  /page processing failed/);
  assert.equal(second.counts().pending || 0, 0);
  assert.equal(second.eventCursor(agent, resource).sync_token, "bootstrap-1");

  assert.equal(first.applyEventPage(agent, resource, "bootstrap-1", "page-1", true,
    () => first.enqueue(signal("event-1")), 3000).state, "draining");
  assert.throws(() => second.applyEventPage(agent, resource, "bootstrap-1", "page-2", false,
    () => second.enqueue(signal("event-2")), 4000), /cursor changed/);
  assert.equal(second.applyEventPage(agent, resource, "page-1", "page-2", false,
    () => second.enqueue(signal("event-2")), 4000).state, "ready");
  assert.equal(first.counts().pending, 2);
  assert.equal(first.eventCursor(agent, resource).sync_token, "page-2");

  second.stageEventReconciliation(agent, resource, "reset-1", 5000);
  assert.equal(first.eventCursor(agent, resource).state, "needs_reconciliation");
  assert.throws(() => first.applyEventPage(agent, resource, "reset-1", "page-3", false,
    () => {}, 6000), /requires reconciliation/);
});

test("verified dependency acknowledgement stores a durable watch and releases leases", (t) => {
  const { first, second } = fixture(t);
  first.enqueue({ id: "due-1", agent_id: "vip-ai-sales", task_gid: "123",
    source_version: "due-1", kind: "due_task", priority: 60, available_at_ms: 1000 }, 1000);
  const claim = first.claimNext("run-1", { now: 1000 });
  first.settle(claim, { outcome: "acknowledged", now: 2000, dependencyWatch: {
    agent_id: "vip-ai-sales", task_gid: "123", linked_task_gid: "456",
    evidence_story_gid: "789" } });
  assert.deepEqual(first.activeLeases(), []);
  assert.equal(second.dependencyWatches("vip-ai-sales", 2000 + 15 * 60_000)[0].linked_task_gid, "456");
  second.markDependencyChecked("vip-ai-sales", "123", 3000);
  assert.deepEqual(first.dependencyWatches("vip-ai-sales", 3000 + 14 * 60_000), []);
  first.clearDependencyWatch("vip-ai-sales", "123");
  assert.deepEqual(second.dependencyWatches("vip-ai-sales", 999999999), []);
});

test("historical dependency watch backfill requires a settled due run", (t) => {
  const { first } = fixture(t);
  first.enqueue({ id: "due-1", agent_id: "vip-ai-sales", task_gid: "123",
    source_version: "due-1", kind: "due_task", priority: 60, available_at_ms: 1000 }, 1000);
  const claim = first.claimNext("run-1", { now: 1000 });
  const input = { runId: "run-1", agentId: "vip-ai-sales", taskGid: "123",
    linkedTaskGid: "456", evidenceStoryGid: "789", now: 2000 };
  first.recordRun(claim, { state: "started", now: 1000 });
  assert.throws(() => first.backfillDependencyWatch(input), /completed, acknowledged due run/);
  first.settle(claim, { outcome: "acknowledged", now: 2000 });
  first.recordRun(claim, { state: "completed", now: 2000 });
  assert.equal(first.backfillDependencyWatch(input).linked_task_gid, "456");
  assert.equal(first.backfillDependencyWatch(input).linked_task_gid, "456");
  assert.throws(() => first.backfillDependencyWatch({ ...input, linkedTaskGid: "999" }),
    /conflicting dependency/);
});

test("uncertain runs surface immediately and progress updates prevent false stalls", (t) => {
  const { first } = fixture(t);
  first.enqueue(signal("story-1"), 1000);
  const claim = first.claimNext("run-1", { now: 1000 });
  first.recordRun(claim, { state: "started", now: 1000 });
  assert.equal(first.stalledRuns(25 * 60_000).length, 1);
  first.touchRun("run-1", 24 * 60_000);
  assert.equal(first.stalledRuns(25 * 60_000).length, 0);
  first.recordRun(claim, { state: "needs_reconciliation", error: "unknown side effect", now: 25 * 60_000 });
  assert.equal(first.runsNeedingReconciliation(25 * 60_000).length, 1);
});

test("poll history keeps compact load and reliability evidence", (t) => {
  const { first, second } = fixture(t);
  const health = (startedAt, status, overrides = {}) => ({
    started_at: new Date(startedAt).toISOString(),
    finished_at: new Date(startedAt + 60_000).toISOString(),
    status,
    dispatch_enabled: true,
    duration_ms: 60_000,
    poll: { agents: [{ tasks_scanned: 7, stories_read: 2, signals_inserted: 1 }] },
    errors: status === "degraded" ? [{ source: "poll_agent", agent_id: "vip-ai-test",
      error: "temporary MCP failure" }] : [],
    workers_started: 1,
    ready: 3,
    active_agents: 1,
    stale_leases: 0,
    stalled_runs: 0,
    counts: { acknowledged: 10, pending: 3 },
    ...overrides
  });
  const day = 24 * 60 * 60_000;
  first.recordPollRun(health(day, "ok"), { now: 3 * day, retentionMs: 2 * day });
  first.recordPollRun(health(2 * day, "attention", { workers_started: 0,
    counts: { acknowledged: 11, pending: 4, dead_letter: 1 } }),
  { now: 3 * day, retentionMs: 2 * day });
  first.recordPollRun(health(3 * day, "degraded"), { now: 3 * day, retentionMs: 2 * day });
  first.recordPollRun(health(3 * day, "degraded", { workers_started: 2 }),
    { now: 3 * day, retentionMs: 2 * day });
  const stats = second.pollStats(day + 1);
  assert.equal(stats.runs, 2);
  assert.equal(stats.attention, 1);
  assert.equal(stats.degraded, 1);
  assert.equal(stats.ok, 0);
  assert.equal(stats.errors, 1);
  assert.equal(stats.workers_started, 2);
  assert.equal(stats.maximum_pending, 4);
  assert.equal(stats.maximum_leased, 0);
  assert.equal(stats.maximum_retry_after, 0);
  assert.equal(stats.maximum_dead_letter, 1);
  assert.equal(stats.maximum_stale_leases, 0);
  assert.equal(stats.maximum_stalled_runs, 0);
  assert.deepEqual(stats.error_sources, { poll_agent: 1 });
  assert.deepEqual(stats.recent_error_samples, [{
    at: new Date(3 * day).toISOString(), source: "poll_agent", agent_id: "vip-ai-test",
    error: "temporary MCP failure"
  }]);
  assert.equal(stats.first_started_at, new Date(2 * day).toISOString());
  assert.equal(stats.last_finished_at, new Date(3 * day + 60_000).toISOString());
});
