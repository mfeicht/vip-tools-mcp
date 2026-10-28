import { assertSnapshotIdentities, assertTaskRows, storiesForTask, tool } from "./asana-dispatch-poller.mjs";
import { newCommentSignals } from "./asana-dispatch-signals.mjs";

const TASK_FIELDS = "gid,name,completed,created_by.gid,assignee.gid,due_on,due_at,modified_at,permalink_url,tags.name,followers.gid";

function followerOnly(task, userGid) {
  if (!Array.isArray(task.followers) || task.followers.some(row => !/^\d+$/.test(row?.gid || ""))) {
    throw new Error(`Follower membership missing or invalid on ${task.gid}`);
  }
  return task.completed === false && task.followers.some(row => row.gid === userGid) &&
    task.assignee?.gid !== userGid && task.created_by?.gid !== userGid;
}

async function directFollowerTask(client, agentId, listed, userGid) {
  const response = await tool(client, "asana_request", {
    agent_id: agentId, method: "GET", path: `/tasks/${listed.gid}`,
    params: { opt_fields: TASK_FIELDS }
  });
  const task = response.response?.data;
  assertTaskRows([task], `${agentId} direct follower task`);
  if (task.gid !== listed.gid || task.modified_at !== listed.modified_at ||
      task.assignee?.gid !== listed.assignee?.gid ||
      task.created_by?.gid !== listed.created_by?.gid ||
      !followerOnly(task, userGid)) throw new Error(`Follower task changed during audit: ${listed.gid}`);
  return task;
}

// An explicit, bounded reconciliation route. The regular five-minute poll does
// not invoke it until an isolated shadow and capacity/readback gate are passed.
export async function auditFollowerTasks(client, store, snapshot, agentUserGids,
  { now = new Date(), write = false, maxTasks = 8 } = {}) {
  assertSnapshotIdentities({ agents: [snapshot] }, [snapshot?.agent_id]);
  if (!(agentUserGids instanceof Set) || !agentUserGids.has(String(snapshot.user_gid)) ||
      !Number.isInteger(maxTasks) || maxTasks < 1 || maxTasks > 8 ||
      !Number.isFinite(now?.getTime?.())) throw new Error("Invalid follower audit scope");

  const agentId = snapshot.agent_id;
  const userGid = String(snapshot.user_gid);
  const search = await tool(client, "asana_search_tasks", {
    agent_id: agentId, workspace_gid: snapshot.workspace_gid,
    involved_any: userGid, completed: false,
    max_pages_per_branch: 10, limit: 100, opt_fields: TASK_FIELDS
  });
  if (search.agent_id !== agentId || search.user_gid !== userGid ||
      search.workspace_gid !== snapshot.workspace_gid ||
      search.search_status !== "ok" || search.search_complete !== true ||
      search.result_truncated !== false) throw new Error("Incomplete or mismatched follower inventory");
  assertTaskRows(search.tasks, `${agentId} full involved inventory`);

  const followerTasks = search.tasks.filter(task => followerOnly(task, userGid));
  const selected = followerTasks.sort((a, b) =>
    (store.observation(agentId, a.gid)?.stories_checked_at_ms || 0) -
    (store.observation(agentId, b.gid)?.stories_checked_at_ms || 0) ||
    a.gid.localeCompare(b.gid)).slice(0, maxTasks);
  const pending = [];
  for (const listed of selected) {
    const task = await directFollowerTask(client, agentId, listed, userGid);
    const stories = await storiesForTask(client, agentId, task.gid);
    await directFollowerTask(client, agentId, listed, userGid);
    const observation = store.observation(agentId, task.gid);
    const comments = newCommentSignals(agentId, task, stories, {
      observation, userGid, allAgentUserGids: agentUserGids, owned: false,
      historicalBackfill: !observation?.history_checked_at_ms, now
    });
    pending.push({ task, comments });
  }
  let inserted = 0;
  if (write) store.transaction(() => {
    for (const { task, comments } of pending) {
      for (const signal of comments.signals) if (store.enqueue(signal, now.getTime())) inserted += 1;
      store.observeTask({ agent_id: agentId, task_gid: task.gid,
        modified_at: task.modified_at,
        latest_story_at: comments.latest?.at || null,
        latest_story_gid: comments.latest?.gid || null,
        stories_checked_at_ms: now.getTime(), history_checked_at_ms: now.getTime()
      }, now.getTime());
    }
  });
  return { agent_id: agentId, inventory_count: search.tasks.length,
    follower_only_count: followerTasks.length, audited_task_gids: selected.map(task => task.gid),
    candidate_signals: pending.reduce((n, item) => n + item.comments.signals.length, 0),
    signals_inserted: inserted };
}
