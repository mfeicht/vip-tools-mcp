import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { connectMcpClient, storiesForTask, tool } from "./asana-dispatch-poller.mjs";
import { CodexAppRpc } from "./codex-app-rpc.mjs";
import { dispatchRecoverySnapshot, readDispatchRecovery, applyDispatchRecovery } from "./asana-dispatch-terminal-recovery.mjs";


export function summarizeReconciliationReadback(run, task, stories, ownGid) {
  if (String(task?.gid || "") !== run.task_gid || !Array.isArray(stories) || !ownGid) {
    throw new Error("Incomplete reconciliation readback");
  }
  const later = stories.filter((story) => Date.parse(story.created_at || "") >= run.started_at_ms);
  return {
    run_id: run.run_id, agent_id: run.agent_id, task_gid: run.task_gid,
    thread_id: run.thread_id, run_state: run.state,
    task_completed: task.completed, task_assignee_gid: task.assignee?.gid || null,
    task_modified_at: task.modified_at || null,
    task_modified_after_start: Number.isFinite(Date.parse(task.modified_at || ""))
      ? Date.parse(task.modified_at) >= run.started_at_ms : null,
    stories_after_start: later.map((story) => ({ gid: story.gid,
      created_at: story.created_at, created_by_gid: story.created_by?.gid || null })),
    own_stories_after_start: later.filter((story) =>
      String(story.created_by?.gid || "") === String(ownGid)).length
  };
}

export async function inspectReconciliationRun(store, { runId, readback = null } = {}) {
  const run = store.db.prepare("SELECT * FROM runs WHERE run_id=?").get(runId);
  if (!run || run.state !== "needs_reconciliation") {
    throw new Error(`Run ${runId} is not awaiting reconciliation`);
  }
  if (readback) return summarizeReconciliationReadback(run,
    readback.task, readback.stories, readback.ownGid);
  let client = null;
  try {
    client = await connectMcpClient();
    const taskResult = await tool(client, "asana_request", { agent_id: run.agent_id,
      method: "GET", path: `/tasks/${run.task_gid}`,
      params: { opt_fields: "gid,completed,assignee.gid,modified_at" } });
    const stories = await storiesForTask(client, run.agent_id, run.task_gid);
    const userResult = await tool(client, "asana_request", { agent_id: run.agent_id,
      method: "GET", path: "/users/me" });
    return summarizeReconciliationReadback(run, taskResult.response?.data,
      stories, userResult.response?.data?.gid);
  } finally {
    if (client) await client.close().catch(() => {});
  }
}

export async function resolveReconciliationRun() {
  throw new Error("A free-text evidence note cannot authorize release; use fenced native/Asana recovery");
}

export function resolveDeadLetters(store, { signalIds, agentId, taskGid, outcome,
  evidence, now = Date.now() } = {}) {
  return store.reviewDeadLetters({ signalIds, agentId, taskGid, outcome, evidence, now });
}

export async function reconcileCompletedRuns(store, {
  connect = connectMcpClient, makeRpc = () => new CodexAppRpc(),
  read = readDispatchRecovery, apply = applyDispatchRecovery, now = () => Date.now(), limit = 2,
  remoteEnabled = true
} = {}) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 2) throw new Error("Recovery limit is one or two");
  const runs = store.runsNeedingReconciliation(now()).filter(run =>
    run.last_reconcile_check_at_ms == null || run.last_reconcile_check_at_ms <= now() - 3600000)
    .sort((a,b) => (a.last_reconcile_check_at_ms ?? a.started_at_ms) -
      (b.last_reconcile_check_at_ms ?? b.started_at_ms)).slice(0,limit);
  const result = { checked:0, acknowledged:0, still_open:0, held:0,
    preflight_retried:0, preflight_unverified:0, capacity_retried:0, archived:0, errors:[], blocked_runs:[] };
  const pending = [];
  for (const run of runs) {
    if (run.state !== "claimed" && !run.thread_id) {
      result.checked++;result.held++;
      result.blocked_runs.push({run_id:run.run_id,reason:"legacy_thread_binding_missing"});
      store.db.prepare(`UPDATE runs SET last_reconcile_check_at_ms=?
        WHERE run_id=? AND updated_at_ms=? AND last_reconcile_check_at_ms IS ?`)
        .run(now(),run.run_id,run.updated_at_ms,run.last_reconcile_check_at_ms);
      continue;
    }
    if (run.state !== "claimed") { pending.push(run); continue; }
    result.checked++;
    try {
      if (run.launch_phase === "preflight" && store.recoverNeverLaunchedRun(run.run_id, now()))
        result.preflight_retried++;
      else result.preflight_unverified++;
    } catch(error) {
      result.errors.push({run_id:run.run_id, stage:"preflight", error:String(error).slice(0,500)});
    }
  }
  if (!pending.length) return result;
  if (!remoteEnabled) { result.held += pending.length; return result; }
  let client, rpc;
  try {
    client = await connect();
    rpc = makeRpc();
    await rpc.start();
    for (const selected of pending) {
      result.checked++;
      const snapshot = dispatchRecoverySnapshot(store, selected.run_id);
      try {
        const proof = await read(snapshot, {rpc,client});
        const decision = apply(store,snapshot,proof,{now:now()});
        if (decision.action === "acknowledge") result.acknowledged++;
        else if (decision.action === "retry_after") result.capacity_retried++;
        else if (decision.action === "quarantine") result.still_open++;
        else {result.held++;result.blocked_runs.push({run_id:selected.run_id,reason:decision.reason});}
      } catch(error) {
        const message=String(error).slice(0,500);
        // A native read failure affects only this run. A canonical MCP failure
        // blocks further remote work for the tick; never start a retry cascade.
        if (/asana_request:|mcp_connect:/.test(message)) {
          result.errors.push({run_id:selected.run_id, stage:"readback", error:message});
          break;
        }
        result.held++;
        result.blocked_runs.push({run_id:selected.run_id,reason:"native_or_local_readback_unavailable",error:message});
      } finally {
        store.db.prepare(`UPDATE runs SET last_reconcile_check_at_ms=?
          WHERE run_id=? AND updated_at_ms=? AND last_reconcile_check_at_ms IS ?`)
          .run(now(),selected.run_id,snapshot.run.updated_at_ms,snapshot.run.last_reconcile_check_at_ms);
      }
    }
  } catch(error) {
    result.errors.push({stage:"connect",error:String(error).slice(0,500)});
  } finally {
    await client?.close().catch(()=>{});
    await rpc?.close().catch(()=>{});
  }
  return result;
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const args = process.argv.slice(2);
  const value = (name) => args.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const dbPath = value("db") || DEFAULT_DB_PATH;
  const runId = value("run");
  const inspectRunId = value("inspect-run");
  const outcome = value("outcome");
  const evidence = value("evidence");
  const deadLetterTask = value("dead-letter-task");
  const deadLetterAgent = value("dead-letter-agent");
  const deadLetterSignals = value("dead-letter-signals")?.split(",").filter(Boolean);
  const deadLetterOutcome = value("dead-letter-outcome");
  const store = new AsanaDispatchStore(dbPath);
  const operation = deadLetterTask || deadLetterAgent || deadLetterSignals || deadLetterOutcome
    ? Promise.resolve(resolveDeadLetters(store, { taskGid: deadLetterTask,
      agentId: deadLetterAgent, signalIds: deadLetterSignals,
      outcome: deadLetterOutcome, evidence }))
    : inspectRunId
    ? inspectReconciliationRun(store, { runId: inspectRunId })
    : runId || outcome || evidence
    ? resolveReconciliationRun(store, { runId, outcome, evidence })
    : reconcileCompletedRuns(store);
  operation.then((result) => {
    console.log(JSON.stringify(result));
    if (result.errors?.length || result.archive_error) process.exitCode = 2;
  }).catch((error) => { console.error(error); process.exitCode = 1; })
    .finally(() => store.close());
}
