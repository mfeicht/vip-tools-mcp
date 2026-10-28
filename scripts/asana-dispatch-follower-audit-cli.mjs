import fs from "node:fs";
import path from "node:path";
import { AsanaDispatchStore, DEFAULT_DB_PATH } from "./asana-dispatch-store.mjs";
import { assertSnapshotIdentities, connectMcpClient, tool } from "./asana-dispatch-poller.mjs";
import { auditFollowerTasks } from "./asana-dispatch-follower-audit.mjs";

const args = Object.fromEntries(process.argv.slice(2).map(value => {
  const match = /^--(agent|db)=(.+)$/.exec(value);
  if (!match) throw new Error(`Invalid argument: ${value}`);
  return [match[1], match[2]];
}));
if (!args.agent || !args.db) throw new Error("--agent and --db are required");
const database = fs.realpathSync(args.db);
const production = fs.realpathSync(DEFAULT_DB_PATH);
const cloneStat = fs.statSync(database);
const productionStat = fs.statSync(production);
if (!database.startsWith("/private/tmp/") || database === production ||
    !cloneStat.isFile() ||
    (cloneStat.dev === productionStat.dev && cloneStat.ino === productionStat.ino)) {
  throw new Error("Follower audit writes require an existing isolated SQLite copy under /private/tmp");
}
const registry = JSON.parse(fs.readFileSync(path.resolve(import.meta.dirname,
  "../../VIP-AI-Memory/03-Betrieb/Agenten-Scope-Registry.json"), "utf8"));
const agentIds = Object.keys(registry.agents || {});
if (!agentIds.includes(args.agent)) throw new Error("Unknown agent identity");

let client;
const store = new AsanaDispatchStore(database);
try {
  client = await connectMcpClient();
  const snapshots = await tool(client, "asana_agents_open_task_snapshot", {
    agent_ids: agentIds, limit: 100, max_pages: 10, concurrency: 4
  });
  assertSnapshotIdentities(snapshots, agentIds);
  const snapshot = snapshots.agents.find(row => row.agent_id === args.agent);
  const userGids = new Set(snapshots.agents.map(row => String(row.user_gid)));
  const result = await auditFollowerTasks(client, store, snapshot, userGids,
    { now: new Date(), write: true, maxTasks: 8 });
  console.log(JSON.stringify({ mode: "isolated-copy", database, ...result }));
} finally {
  if (client) await Promise.race([client.close().catch(() => {}),
    new Promise(resolve => setTimeout(resolve, 5000))]);
  store.close();
}
