import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
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
    errors: status === "degraded" ? [{ source: "poll" }] : [],
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
  assert.equal(stats.maximum_dead_letter, 1);
  assert.equal(stats.first_started_at, new Date(2 * day).toISOString());
  assert.equal(stats.last_finished_at, new Date(3 * day + 60_000).toISOString());
});
