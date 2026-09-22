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

export function executeLeaseCommand(store, { command, agent, task, run }) {
  if (command === "status") return { leases: store.activeLeases(), counts: store.counts() };
  if (command === "acquire-agent") {
    if (!/^vip-ai-[a-z-]+$/.test(agent || "")) throw new Error("Valid --agent required");
    const runId = run || randomUUID().replaceAll("-", "");
    const lease = store.acquireLease(`agent:${agent}`, runId, 2 * 60 * 60_000);
    return { acquired: Boolean(lease), run_id: runId, lease: lease || null };
  }
  if (command === "acquire-task") {
    if (!run || !/^\d+$/.test(task || "")) throw new Error("--run and numeric --task required");
    const agentLease = store.leasesForRun(run).find((lease) => lease.key.startsWith("agent:"));
    if (!agentLease) throw new Error("Acquire the agent lease first");
    const lease = store.acquireLease(`task:${task}`, run, 2 * 60 * 60_000);
    return { acquired: Boolean(lease), run_id: run, lease: lease || null };
  }
  if (command === "renew") {
    if (!run) throw new Error("--run required");
    const leases = store.leasesForRun(run);
    if (!leases.length) throw new Error("No active leases for run");
    for (const lease of leases) store.acquireLease(lease.key, run, 2 * 60 * 60_000);
    return { renewed: leases.length, run_id: run };
  }
  if (command === "release") {
    if (!run) throw new Error("--run required");
    return { released: store.releaseRun(run), run_id: run };
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
