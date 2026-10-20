import assert from "node:assert/strict";
import test from "node:test";
import { decideManualLeaseReconciliation as decide } from "./asana-manual-lease-decision.mjs";

const now = 10_000_000;
const run = { run_id: "manual-run", agent_id: "vip-ai-test", thread_id: "thread-1",
  state: "active", acquired_at_ms: 1_000_000, agent_lease_token: "agent-token",
  agent_lease_fence: 1, tasks: [{ task_gid: "123", acquired_at_ms: 1_001_000,
    lease_token: "task-token", lease_fence: 1 }] };
const native = [0, 1].map((index) => ({ observed_at_ms: now - 3_000 + 1_500 * index,
  source: "codex_app.read_thread", host_id: "local", thread_id: "thread-1",
  runtime_status: "idle", turn_id: "turn-1", turn_status: "completed",
  started_at_ms: 900_000, completed_at_ms: 9_000_000,
  activity_digest: "a".repeat(64) }));
const processReadback = { observed_at_ms: now - 1_000, run_id: "manual-run",
  thread_id: "thread-1", scan_succeeded: true, matching_processes: [] };
const completedTask = [0, 1].map((index) => ({ observed_at_ms: now - 3_000 + 1_500 * index,
  agent_id: "vip-ai-test", task: { gid: "123", completed: true,
    modified_at: "2026-10-20T15:00:00Z" }, stories: [], next_page: null }));
const args = { run, nativeReadbacks: native, processReadback, agentUserGid: "user-1",
  taskReadbacks: [{ task_gid: "123", readbacks: completedTask }], now };

test("terminal turn and stable completed task permit a fenced release decision", () => {
  assert.deepEqual(decide(args), { action: "release", reason: "terminal_tasks_verified",
    release_agent: true, release_tasks: ["123"], quarantine_tasks: [] });
  assert.equal(decide({ ...args, nativeReadbacks: native.map((row) =>
    ({ ...row, turn_status: "interrupted" })) }).action, "release");
});

test("active process or advancing Codex activity keeps every lease", () => {
  assert.equal(decide({ ...args, processReadback: { ...processReadback,
    matching_processes: [{ pid: 12 }] } }).action, "hold");
  assert.equal(decide({ ...args, nativeReadbacks: [native[0],
    { ...native[1], activity_digest: "b".repeat(64) }] }).action, "hold");
  assert.equal(decide({ ...args, nativeReadbacks: [native[0],
    { ...native[1], completed_at_ms: null, turn_status: "interrupted" }] }).action, "hold");
  assert.equal(decide({ ...args, nativeReadbacks: native.map((row) =>
    ({ ...row, started_at_ms: run.acquired_at_ms + 60_000 })) }).action, "hold");
});

test("open task needs the exact own result story after task acquisition", () => {
  const stories = [{ gid: "story-1", resource_subtype: "comment_added",
    created_at: new Date(2_000_000).toISOString(), created_by: { gid: "user-1" } }];
  const taskReadbacks = [{ task_gid: "123", evidence_story_gid: "story-1",
    readbacks: completedTask.map((row) => ({ ...row, task: { ...row.task,
      completed: false, assignee: { gid: "user-1" } }, stories })) }];
  assert.equal(decide({ ...args, taskReadbacks }).action, "release");
  const noStory = [{ ...taskReadbacks[0], evidence_story_gid: null }];
  assert.deepEqual(decide({ ...args, taskReadbacks: noStory }).quarantine_tasks,
    [{ task_gid: "123", reason: "open_task_without_result" }]);
  assert.equal(decide({ ...args, taskReadbacks,
    nativeReadbacks: native.map((row) => ({ ...row, turn_status: "failed" })) }).action,
    "partial_quarantine");
  const ownerChanged = [{ ...taskReadbacks[0], readbacks: taskReadbacks[0].readbacks.map((row) =>
    ({ ...row, task: { ...row.task, assignee: { gid: "user-2" } } })) }];
  assert.equal(decide({ ...args, taskReadbacks: ownerChanged }).action, "partial_quarantine");
});

test("changed or incomplete Asana pages quarantine only the affected task", () => {
  const unstable = [{ task_gid: "123", readbacks: [completedTask[0],
    { ...completedTask[1], next_page: "more" }] }];
  assert.deepEqual(decide({ ...args, taskReadbacks: unstable }).quarantine_tasks,
    [{ task_gid: "123", reason: "task_readback_unverified" }]);
  assert.equal(decide({ ...args, taskReadbacks: unstable }).release_agent, true);
  const secondTask = { task_gid: "456", acquired_at_ms: 1_002_000,
    lease_token: "second-token", lease_fence: 2 };
  const partial = decide({ ...args, run: { ...run, tasks: [...run.tasks, secondTask] },
    taskReadbacks: [{ task_gid: "123", readbacks: completedTask },
      { task_gid: "456", readbacks: unstable[0].readbacks.map((row) =>
        ({ ...row, task: { ...row.task, gid: "456" } })) }] });
  assert.deepEqual(partial.release_tasks, ["123"]);
  assert.deepEqual(partial.quarantine_tasks,
    [{ task_gid: "456", reason: "task_readback_unverified" }]);
});

test("terminal turn without a task binding can release only the agent", () => {
  assert.deepEqual(decide({ ...args, run: { ...run, tasks: [] }, taskReadbacks: [] }),
    { action: "release", reason: "terminal_without_tasks", release_agent: true,
      release_tasks: [], quarantine_tasks: [] });
});
