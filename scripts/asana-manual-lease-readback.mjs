import { createHash } from "node:crypto";
import { tool, storiesForTask } from "./asana-dispatch-poller.mjs";
import { collectWorkerProcessReadback } from "./asana-worker-process-readback.mjs";
import { decideManualLeaseReconciliation } from "./asana-manual-lease-decision.mjs";

const digest = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const timestamp = (value) => Number.isFinite(value) && value > 0
  ? Math.round(value < 1_000_000_000_000 ? value * 1000 : value) : null;

export function boundedReadClients(rpc, client, now = () => Date.now()) {
  const deadline=now()+120000;
  const remaining=()=>{
    const ms=deadline-now();
    if(ms<=0) throw new Error("Recovery readback budget exceeded");
    return ms;
  };
  return {
    rpc:{call:(method,params,timeout=30000)=>rpc.call(method,params,Math.min(timeout,remaining()))},
    client:client ? {callTool:(request,schema,options={})=>{
      const timeout=Math.min(options.timeout || 45000,remaining());
      return client.callTool(request,schema,{...options,timeout,maxTotalTimeout:timeout,deadlineAtMs:deadline});
    }} : null
  };
}

async function pages(rpc, method, base, maxPages = 20) {
  const data = [];
  const cursors = new Set();
  let cursor = null;
  for (let page = 0; page < maxPages; page += 1) {
    const response = await rpc.call(method, { ...base, ...(cursor ? { cursor } : {}) }, 30_000);
    if (!Array.isArray(response?.data) || !Object.hasOwn(response,"nextCursor"))
      throw new Error(`${method}: incomplete page`);
    data.push(...response.data);
    if (data.length > 2000) throw new Error(`${method}: item cap exceeded`);
    cursor = response.nextCursor || null;
    if (!cursor) return data;
    if (cursors.has(cursor)) throw new Error(`${method}: repeated cursor`);
    cursors.add(cursor);
  }
  throw new Error(`${method}: pagination incomplete`);
}

// Native Codex reads are bounded and fail closed on a missing turn or item page.
// This function only observes; it never resumes, archives or modifies a thread.
export async function readManualNative(run, rpc) {
  return readNative(run, rpc, "manual");
}

export async function readDispatchNative(run, rpc) {
  return readNative(run, rpc, "dispatch");
}

async function readNative(run, rpc, mode) {
  const thread = (await rpc.call("thread/read", {
    threadId: run.thread_id, includeTurns: false }, 30_000))?.thread;
  if (thread?.id !== run.thread_id) throw new Error("Native thread identity mismatch");
  const turns = await pages(rpc, "thread/turns/list", {
    threadId: run.thread_id, limit: 100, sortDirection: "desc", itemsView: "notLoaded"
  });
  const matching = turns.filter((turn) => {
    const start = timestamp(turn.startedAt);
    const end = timestamp(turn.completedAt);
    if (mode === "dispatch") return start !== null && start >= run.started_at_ms - 2000 &&
      (!run.turn_id || turn.id === run.turn_id);
    return start !== null && start <= run.acquired_at_ms + 2_000 &&
      (end === null || end >= run.acquired_at_ms);
  });
  if (matching.length !== 1 || turns.some((turn) =>
    turn.id !== matching[0].id && timestamp(turn.startedAt) > timestamp(matching[0].startedAt))) {
    throw new Error("Native turn binding ambiguous or followed by another turn");
  }
  const turn = matching[0];
  const entries = await pages(rpc, "thread/items/list", {
    threadId: run.thread_id, turnId: turn.id, limit: 100, sortDirection: "desc"
  });
  if (!entries.length || entries.some((entry) => !entry?.item?.id ||
      ["inProgress", "in_progress", "running", "pending"].includes(entry.item.status))) {
    throw new Error("Native activity items incomplete");
  }
  return {
    observed_at_ms: Date.now(), source: "codex_app.read_thread", host_id: "local",
    thread_id: run.thread_id, runtime_status: thread.status?.type,
    turn_id: turn.id, turn_status: turn.status,
    started_at_ms: timestamp(turn.startedAt), completed_at_ms: timestamp(turn.completedAt),
    activity_digest: digest(entries.map((entry) => entry.item)),
    activity_item_count: entries.length, activity_last_item_id: entries[0].item.id
  };
}

export async function readManualAsanaTask(run, binding, client) {
  const response = await tool(client, "asana_request", {
    agent_id: run.agent_id, method: "GET", path: `/tasks/${binding.task_gid}`,
    params: { opt_fields: "gid,completed,assignee.gid,modified_at" }
  });
  const task = response.response?.data;
  if (task?.gid !== binding.task_gid || typeof task.completed !== "boolean" ||
      !task.modified_at) throw new Error("Asana task readback incomplete");
  const stories = await storiesForTask(client, run.agent_id, binding.task_gid);
  if (stories.some((story) => !story?.gid || !story.created_at ||
      !story.resource_subtype ||
      (story.created_by !== null && !story.created_by?.gid) ||
      (story.resource_subtype === "comment_added" && !story.created_by?.gid))) {
    throw new Error("Asana story readback incomplete");
  }
  return { observed_at_ms: Date.now(), agent_id: run.agent_id,
    task, stories, next_page: null };
}

// No database mutation. Callers must implement a separate, fenced transaction
// with a fresh process scan before applying any release decision.
export async function readManualLeaseDecision(run, {
  rpc, client, readNative = readManualNative, readAsana = readManualAsanaTask,
  scan = collectWorkerProcessReadback, wait = sleep, now = () => Date.now()
} = {}) {
  if (!rpc || !Array.isArray(run?.tasks) || run.tasks.length > 20 ||
      (run.tasks.length && !client)) {
    throw new Error("Bounded native and Asana readers required");
  }
  ({rpc,client}=boundedReadClients(rpc,client,now));
  const deadline = now() + 120_000;
  const checkBudget = () => { if (now() > deadline) throw new Error("Manual readback budget exceeded"); };
  const identity = run.tasks.length ? await tool(client, "asana_request", {
    agent_id: run.agent_id, method: "GET", path: "/users/me",
    params: { opt_fields: "gid" }
  }) : null;
  const agentUserGid = identity?.response?.data?.gid || null;
  if (run.tasks.length && !agentUserGid) throw new Error("Asana agent identity missing");
  const nativeReadbacks = [];
  const taskReadbacks = run.tasks.map((binding) => ({ task_gid: binding.task_gid,
    readbacks: [], evidence_story_gid: null }));
  for (let pass = 0; pass < 2; pass += 1) {
    if (pass) await wait(1100);
    checkBudget();
    nativeReadbacks.push(await readNative(run, rpc));
    for (let index = 0; index < run.tasks.length; index += 1) {
      checkBudget();
      taskReadbacks[index].readbacks.push(await readAsana(run, run.tasks[index], client));
    }
  }
  for (let index = 0; index < run.tasks.length; index += 1) {
    const own = taskReadbacks[index].readbacks[1].stories.filter((story) =>
      story.resource_subtype === "comment_added" && story.created_by?.gid === agentUserGid &&
      Date.parse(story.created_at) > run.tasks[index].acquired_at_ms);
    if (own.length === 1) taskReadbacks[index].evidence_story_gid = own[0].gid;
  }
  const processReadback = scan(run);
  const decision = decideManualLeaseReconciliation({ run, nativeReadbacks,
    processReadback, taskReadbacks, agentUserGid, now: now() });
  return { decision, nativeReadbacks, processReadback, taskReadbacks,
    agentUserGid };
}
