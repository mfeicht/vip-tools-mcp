import { processScanBinding } from "./asana-worker-process-readback.mjs";

const TERMINAL = new Set(["completed", "failed", "interrupted"]);
const MAX_READBACK_AGE_MS = 5 * 60_000;

function fresh(row, now) {
  return Number.isSafeInteger(row?.observed_at_ms) &&
    row.observed_at_ms <= now + 1_000 &&
    now - row.observed_at_ms <= MAX_READBACK_AGE_MS;
}

function same(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// This is a read-only decision. The caller must recheck the native process and
// every local lease token/fence immediately before any database transition.
export function decideManualLeaseReconciliation({ run, nativeReadbacks, processReadback,
  taskReadbacks, agentUserGid, now = Date.now() } = {}) {
  const hold = (reason) => ({ action: "hold", reason,
    release_agent: false, release_tasks: [], quarantine_tasks: [] });
  if (!run?.run_id || !run.agent_id || !run.thread_id || run.state !== "active" ||
      !Number.isSafeInteger(run.acquired_at_ms) || !Array.isArray(run.tasks)) {
    return hold("manual_binding_incomplete");
  }
  const agentAlreadyReleased = Number.isSafeInteger(run.agent_released_at_ms) &&
    run.agent_released_at_ms >= run.acquired_at_ms && run.agent_released_at_ms <= now &&
    run.agent_lease_token == null && run.agent_lease_fence == null;
  if ((!agentAlreadyReleased && (!run.agent_lease_token || !Number.isSafeInteger(run.agent_lease_fence) ||
      run.agent_lease_fence < 1 || run.agent_fence !== run.agent_lease_fence)) ||
      run.tasks.some((task) =>
        !task.task_gid || !Number.isSafeInteger(task.acquired_at_ms)) ||
      new Set(run.tasks.map((task) => task.task_gid)).size !== run.tasks.length) {
    return hold("lease_binding_incomplete");
  }
  if (!Array.isArray(nativeReadbacks) || nativeReadbacks.length !== 2 ||
      nativeReadbacks.some((row) => !fresh(row, now) ||
        !["codex_app.read_thread", "codex_app.wait_threads"].includes(row.source) ||
        row.host_id !== "local" || !["idle", "notLoaded"].includes(row.runtime_status) ||
        row.thread_id !== run.thread_id || !TERMINAL.has(row.turn_status) ||
        !row.turn_id || !Number.isSafeInteger(row.activity_item_count) || row.activity_item_count < 1 ||
        !row.activity_last_item_id || !/^[a-f0-9]{64}$/.test(row.activity_digest || "") ||
        !Number.isSafeInteger(row.started_at_ms) ||
        row.started_at_ms > row.observed_at_ms ||
        row.started_at_ms > run.acquired_at_ms + 2_000 ||
        (row.completed_at_ms != null &&
          (!Number.isSafeInteger(row.completed_at_ms) ||
            row.completed_at_ms < Math.max(run.acquired_at_ms,
              ...run.tasks.map((task) => task.acquired_at_ms)) ||
            row.completed_at_ms > row.observed_at_ms + 1_000))) ||
      nativeReadbacks[1].observed_at_ms - nativeReadbacks[0].observed_at_ms < 1_000 ||
      !same({ ...nativeReadbacks[0], observed_at_ms: null },
        { ...nativeReadbacks[1], observed_at_ms: null })) {
    return hold("native_terminal_unverified");
  }
  const scanBinding = processScanBinding(run);
  if (!fresh(processReadback, now) || processReadback.scan_succeeded !== true ||
      processReadback.run_id !== run.run_id ||
      processReadback.thread_id !== run.thread_id ||
      Object.entries(scanBinding).some(([key, value]) =>
        !same(processReadback[key], value)) ||
      !/^[a-f0-9]{64}$/.test(processReadback.raw_sha256 || "") ||
      !Array.isArray(processReadback.matching_processes) ||
      processReadback.matching_processes.length) {
    return hold("worker_inactivity_unverified");
  }
  if (run.tasks.length === 0) return { action: "release", reason: "terminal_without_tasks",
    release_agent: !agentAlreadyReleased, release_tasks: [], quarantine_tasks: [] };

  const readbacks = new Map();
  if (!Array.isArray(taskReadbacks) || !agentUserGid) {
    return hold("task_readbacks_incomplete");
  }
  for (const row of taskReadbacks) {
    if (!row?.task_gid || readbacks.has(row.task_gid)) return hold("task_readbacks_ambiguous");
    readbacks.set(row.task_gid, row);
  }
  const releaseTasks = [];
  const quarantineTasks = [];
  for (const binding of run.tasks) {
    const pair = readbacks.get(binding.task_gid);
    if (!pair || !Array.isArray(pair.readbacks) || pair.readbacks.length !== 2 ||
        pair.readbacks.some((row) => !fresh(row, now) ||
          row.agent_id !== run.agent_id || row.task?.gid !== binding.task_gid ||
          typeof row.task.completed !== "boolean" ||
          !row.task.modified_at || !Array.isArray(row.stories) ||
          row.next_page !== null) ||
        pair.readbacks[1].observed_at_ms - pair.readbacks[0].observed_at_ms < 1_000 ||
        !same({ ...pair.readbacks[0], observed_at_ms: null },
          { ...pair.readbacks[1], observed_at_ms: null })) {
      quarantineTasks.push({ task_gid: binding.task_gid, reason: "task_readback_unverified" });
      continue;
    }
    const { task, stories } = pair.readbacks[1];
    if (!binding.lease_token || !Number.isSafeInteger(binding.lease_fence) ||
        binding.lease_fence < 1 || binding.bound_lease_fence !== binding.lease_fence) {
      quarantineTasks.push({ task_gid: binding.task_gid, reason: "task_lease_missing" });
    } else if (task.completed === true) {
      releaseTasks.push(binding.task_gid);
    } else if (nativeReadbacks[1].turn_status === "completed" &&
        task.assignee?.gid === agentUserGid &&
        pair.evidence_story_gid && stories.some((story) =>
          story.gid === pair.evidence_story_gid &&
          story.resource_subtype === "comment_added" &&
          story.created_by?.gid === agentUserGid &&
          Date.parse(story.created_at || "") > binding.acquired_at_ms)) {
      releaseTasks.push(binding.task_gid);
    } else {
      quarantineTasks.push({ task_gid: binding.task_gid, reason: "open_task_without_result" });
    }
  }
  if (readbacks.size !== run.tasks.length) return hold("task_readbacks_ambiguous");
  return { action: quarantineTasks.length ? "partial_quarantine" : "release",
    reason: quarantineTasks.length ? "terminal_with_unverified_tasks" : "terminal_tasks_verified",
    release_agent: !agentAlreadyReleased, release_tasks: releaseTasks, quarantine_tasks: quarantineTasks };
}
