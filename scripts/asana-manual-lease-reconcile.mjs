import { CodexAppRpc } from "./codex-app-rpc.mjs";
import { connectMcpClient } from "./asana-dispatch-poller.mjs";
import { readManualLeaseDecision } from "./asana-manual-lease-readback.mjs";

// At most two chat-bound runs per watcher tick. Legacy leases are never adopted.
// The store independently repeats the fixed process scan and fence checks.
export async function reconcileManualLeases(store, {
  now = () => Date.now(), connect = connectMcpClient,
  makeRpc = () => new CodexAppRpc(), read = readManualLeaseDecision
} = {}) {
  const result = { checked: 0, released: 0, partial_quarantine: 0, held: 0, errors: [], blocked_runs: [] };
  const runs = store.manualRunsForReconciliation({ now: now(), limit: 2 });
  if (!runs.length) return result;
  let rpc;
  let client;
  let connectionError;
  try {
    rpc = makeRpc();
    await rpc.start();
    // An agent-only abandoned run needs no Asana connection or identity read.
    for (const run of runs) {
      result.checked += 1;
      try {
        if (run.tasks.length && connectionError) { result.held++; continue; }
        if (run.tasks.length && !client) {
          if (connectionError) throw connectionError;
          try { client = await connect(); }
          catch(error) { connectionError=error; throw error; }
        }
        const evidence = await read(run, { rpc, client, now });
        const applied = store.reconcileManualRun(run, evidence, now());
        if (!applied.applied) result.held += 1;
        else if (applied.decision.action === "partial_quarantine") result.partial_quarantine += 1;
        else result.released += 1;
      } catch (error) {
        const message=String(error).slice(0,500);
        if (connectionError || /asana_request:|mcp_connect:/.test(message)) {
          result.errors.push({ run_id: run.run_id, error: message });
          connectionError=error;
          continue;
        }
        result.held++;
        result.blocked_runs.push({run_id:run.run_id,reason:"native_or_local_readback_unavailable",error:message});
      } finally {
        // CAS cannot overwrite a run renewed or released during the readback.
        store.markManualRunChecked(run, now());
      }
    }
  } catch (error) {
    result.errors.push({ stage: "connect", error: String(error).slice(0, 500) });
    // Keep failures eligible on the next tick; no per-run evidence was read.
  } finally {
    await client?.close().catch(() => {});
    await rpc?.close().catch(() => {});
  }
  return result;
}
