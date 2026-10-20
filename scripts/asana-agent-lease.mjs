import { randomUUID } from "node:crypto";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";

function parse(argv) {
  const [command, ...args] = argv;
  const options = Object.fromEntries(args.map((arg) => {
    if (!arg.startsWith("--") || !arg.includes("=")) throw new Error(`Unknown argument ${arg}`);
    const index = arg.indexOf("=");
    return [arg.slice(2, index), arg.slice(index + 1)];
  }));
  return { command, ...options };
}

export function manualThreadContext(env = process.env) {
  const source = env.CODEX_THREAD_ID ? "CODEX_THREAD_ID" : "CODEX_SESSION_ID";
  const threadId = env[source];
  if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(threadId || "")) {
    throw new Error("Real CODEX_THREAD_ID or CODEX_SESSION_ID required; no synthetic fallback");
  }
  return { threadId, source };
}

export function executeLeaseCommand(store, { command, agent, task, run }, env = process.env) {
  if (command === "status") return {
    leases: store.activeLeases(), counts: store.counts(),
    manual_runs: store.db.prepare("SELECT * FROM manual_runs WHERE state='active' ORDER BY created_at_ms").all(),
    manual_run_tasks: store.db.prepare(`SELECT t.* FROM manual_run_tasks t
      JOIN manual_runs r ON r.run_id=t.run_id WHERE r.state='active'
      ORDER BY t.run_id,t.task_gid`).all(),
  };
  if (command === "acquire-agent") {
    if (!/^vip-ai-[a-z-]+$/.test(agent || "")) throw new Error("Valid --agent required");
    if (env.VIP_ASANA_DISPATCH_RUN_ID) throw new Error("Dispatcher already owns local leases");
    const context = manualThreadContext(env);
    const runId = run || randomUUID().replaceAll("-", "");
    const lease = store.acquireManualAgent(agent, runId, context);
    return { acquired: Boolean(lease), run_id: runId, lease: lease || null,
      ...(lease ? { thread_id: context.threadId, binding: "manual_runs" } : {}) };
  }
  if (command === "acquire-task") {
    if (!run || !/^\d+$/.test(task || "")) throw new Error("--run and numeric --task required");
    if (env.VIP_ASANA_DISPATCH_RUN_ID) throw new Error("Dispatcher already owns local leases");
    if (store.manualRun(run)) {
      const lease = store.acquireManualTask(run, task, manualThreadContext(env));
      return { acquired: Boolean(lease), run_id: run, lease: lease || null };
    }
    // Preserve already-running legacy callers; never invent a retroactive chat binding.
    const agentLease = store.leasesForRun(run).find((lease) => lease.key.startsWith("agent:"));
    if (!agentLease) throw new Error("Acquire the agent lease first");
    const lease = store.acquireLease(`task:${task}`, run, 2 * 60 * 60_000);
    return { acquired: Boolean(lease), run_id: run, lease: lease || null, legacy_unbound: true };
  }
  if (command === "renew") {
    if (!run) throw new Error("--run required");
    if (store.manualRun(run)) return {
      renewed: store.finishManualLeaseCommand(run, command, manualThreadContext(env)), run_id: run,
    };
    const leases = store.leasesForRun(run);
    if (!leases.length) throw new Error("No active leases for run");
    for (const lease of leases) store.acquireLease(lease.key, run, 2 * 60 * 60_000);
    return { renewed: leases.length, run_id: run, legacy_unbound: true };
  }
  if (command === "release") {
    if (!run) throw new Error("--run required");
    if (store.manualRun(run)) return {
      released: store.finishManualLeaseCommand(run, command, manualThreadContext(env)), run_id: run,
    };
    return { released: store.releaseRun(run), run_id: run, legacy_unbound: true };
  }
  throw new Error("Usage: acquire-agent --agent=vip-ai-X | acquire-task --run=ID --task=GID | renew --run=ID | release --run=ID | status");
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const store = new AsanaDispatchStore();
  try {
    const result = executeLeaseCommand(store, parse(process.argv.slice(2)));
    console.log(JSON.stringify(result));
    if (result.acquired === false) process.exitCode = 3;
  } catch (error) {
    console.error(error?.message || error);
    process.exitCode = 1;
  } finally {
    store.close();
  }
}
