import assert from "node:assert/strict";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { auditFollowerTasks } from "./asana-dispatch-follower-audit.mjs";

const agentId = "vip-ai-operations";
const userGid = "456";
const now = new Date("2026-10-28T07:00:00Z");
const task = gid => ({ gid, name: `Follower ${gid}`, completed: false,
  created_by: { gid: "900" }, assignee: { gid: "901" },
  due_on: null, due_at: null, modified_at: "2026-10-27T10:00:00Z",
  tags: [], followers: [{ gid: userGid }] });
const snapshot = { agent_id: agentId, ok: true, truncated: false,
  user_gid: userGid, workspace_gid: "111", tasks: [] };
const mention = gid => ({ gid, target: { gid: "123" },
  type: "comment", resource_subtype: "comment_added",
  created_at: "2026-10-28T06:30:00Z", created_by: { gid: "999" },
  html_text: '<a data-asana-gid="456">@Operations</a> Bitte prüfen.' });

function client(inventory, stories, { complete = true, direct = null } = {}) {
  const calls = [];
  return { calls, async callTool({ name, arguments: args }) {
    calls.push({ name, args });
    let value;
    if (name === "asana_search_tasks") value = { agent_id: agentId,
      user_gid: userGid, workspace_gid: "111", search_status: "ok",
      search_complete: complete, result_truncated: !complete, tasks: inventory };
    else if (!args.path.endsWith("/stories")) value = {
      response: { data: direct || inventory.find(row => row.gid === args.path.split("/")[2]) }
    };
    else value = { response: { data: stories.map(story => ({
      ...story, target: story.target || { gid: args.path.split("/")[2] }
    })), next_page: null } };
    return { content: [{ type: "text", text: JSON.stringify(value) }] };
  } };
}

test("complete follower sweep reads direct task and unchanged stories without new signal", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const input = client([task("123")], [{ ...mention("777"), html_text: "Nur zur Kenntnis." }]);
    const result = await auditFollowerTasks(input, store, snapshot, new Set([userGid]),
      { now, write: true });
    assert.deepEqual(result.audited_task_gids, ["123"]);
    assert.equal(result.signals_inserted, 0);
    assert.equal(store.observation(agentId, "123").stories_checked_at_ms, now.getTime());
    assert.equal(store.pollCursor(agentId), null);
    assert.equal(input.calls.filter(call => call.name === "asana_request").length, 3);
    assert.equal(input.calls[0].args.modified_since, undefined);
  } finally { store.close(); }
});

test("unchanged modified_at with new target-bound mention enqueues once", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const initial = client([task("123")], []);
    await auditFollowerTasks(initial, store, snapshot, new Set([userGid]), { now, write: true });
    const changed = client([task("123")], [mention("778")]);
    const first = await auditFollowerTasks(changed, store, snapshot, new Set([userGid]),
      { now: new Date(now.getTime() + 300000), write: true });
    assert.equal(first.signals_inserted, 1);
    const second = await auditFollowerTasks(changed, store, snapshot, new Set([userGid]),
      { now: new Date(now.getTime() + 600000), write: true });
    assert.equal(second.signals_inserted, 0);
    assert.equal(store.counts().pending, 1);
  } finally { store.close(); }
});

test("incomplete search, changed direct task and bad story target leave store untouched", async () => {
  for (const input of [
    client([task("123")], [], { complete: false }),
    client([task("123")], [], { direct: { ...task("123"), modified_at: "2026-10-28T06:00:00Z" } }),
    client([task("123")], [{ ...mention("778"), target: { gid: "999" } }])
  ]) {
    const store = new AsanaDispatchStore(":memory:");
    try {
      await assert.rejects(auditFollowerTasks(input, store, snapshot, new Set([userGid]),
        { now, write: true }));
      assert.equal(store.observation(agentId, "123"), null);
      assert.equal(store.pollCursor(agentId), null);
      assert.deepEqual(store.counts(), {});
    } finally { store.close(); }
  }
});

test("a task version changing after story pagination aborts the whole batch", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const input = client([task("123")], [mention("778")]);
    const original = input.callTool;
    let directReads = 0;
    input.callTool = async call => {
      const result = await original(call);
      if (call.name === "asana_request" && call.arguments.path === "/tasks/123" &&
          ++directReads === 2) {
        const changed = { response: { data: { ...task("123"),
          modified_at: "2026-10-28T06:00:00Z" } } };
        return { content: [{ type: "text", text: JSON.stringify(changed) }] };
      }
      return result;
    };
    await assert.rejects(auditFollowerTasks(input, store, snapshot, new Set([userGid]),
      { now, write: true }), /changed during audit/);
    assert.equal(store.observation(agentId, "123"), null);
    assert.deepEqual(store.counts(), {});
  } finally { store.close(); }
});

test("sweep caps direct reads and rotates to oldest follower observations", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const rows = Array.from({ length: 10 }, (_, n) => task(String(100 + n)));
    const input = client(rows, []);
    const first = await auditFollowerTasks(input, store, snapshot, new Set([userGid]),
      { now, write: true });
    assert.equal(first.audited_task_gids.length, 8);
    const second = await auditFollowerTasks(input, store, snapshot, new Set([userGid]),
      { now: new Date(now.getTime() + 300000), write: true });
    assert.deepEqual(second.audited_task_gids.slice(0, 2), ["108", "109"]);
    assert.equal(store.pollCursor(agentId), null);
  } finally { store.close(); }
});
