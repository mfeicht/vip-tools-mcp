import assert from "node:assert/strict";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { scanAgent } from "./asana-dispatch-poller.mjs";

function fakeClient({ searchTasks = [], stories = [], searchComplete = true } = {}) {
  const calls = [];
  return {
    calls,
    async callTool({ name, arguments: args }) {
      calls.push({ name, args });
      const data = name === "asana_search_tasks"
        ? { search_status: "ok", search_complete: searchComplete,
          result_truncated: !searchComplete, tasks: searchTasks }
        : { response: { data: stories, next_page: null } };
      return { content: [{ type: "text", text: JSON.stringify(data) }] };
    }
  };
}

test("assigned due task and new human comment enqueue once across polls", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const now = new Date("2026-09-21T10:00:00Z");
    const task = { gid: "123", name: "Investigate", completed: false,
      assignee: { gid: "456" }, due_on: "2020-01-01", modified_at: "2026-09-21T09:00:00Z" };
    const client = fakeClient({ stories: [{ gid: "789", type: "comment", resource_subtype: "comment_added",
      created_at: "2026-09-21T09:30:00Z", created_by: { gid: "900" }, html_text: "Please check" }] });
    const snapshot = { agent_id: "vip-ai-research", ok: true, user_gid: "456",
      workspace_gid: "111", tasks: [task], truncated: false };
    const first = await scanAgent(client, store, snapshot, new Set(["456"]), now, true);
    assert.equal(first.signals_inserted, 2);
    assert.equal(first.stories_read, 1);
    const second = await scanAgent(client, store, snapshot, new Set(["456"]),
      new Date("2026-09-21T10:05:00Z"), true);
    assert.equal(second.signals_inserted, 0);
    assert.equal(second.stories_read, 0);
    assert.equal(store.readyCount(), 2);
  } finally { store.close(); }
});

test("creator who is not assignee needs a direct mention", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const task = { gid: "123", name: "Someone else's task", completed: false,
      created_by: { gid: "456" }, assignee: { gid: "999" }, modified_at: "2026-09-21T09:30:00Z" };
    const story = { gid: "789", type: "comment", created_at: "2026-09-21T09:45:00Z",
      created_by: { gid: "900" }, html_text: "Please check" };
    const client = fakeClient({ searchTasks: [task], stories: [story] });
    const snapshot = { agent_id: "vip-ai-research", ok: true, user_gid: "456",
      workspace_gid: "111", tasks: [], truncated: false };
    const first = await scanAgent(client, store, snapshot, new Set(["456"]),
      new Date("2026-09-21T10:00:00Z"), true);
    assert.equal(first.signals_inserted, 0);
    task.modified_at = "2026-09-21T10:05:00Z";
    story.gid = "790";
    story.created_at = "2026-09-21T10:04:00Z";
    story.html_text = '<a data-asana-gid="456">@Research</a> please check';
    const second = await scanAgent(client, store, snapshot, new Set(["456"]),
      new Date("2026-09-21T10:10:00Z"), true);
    assert.equal(second.signals_inserted, 1);
    assert.equal(store.counts().pending, 1);
  } finally { store.close(); }
});

test("truncated involved search does not advance cursor or enqueue", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const snapshot = { agent_id: "vip-ai-research", ok: true, user_gid: "456",
      workspace_gid: "111", tasks: [{ gid: "123", name: "Due", due_on: "2020-01-01" }], truncated: false };
    await assert.rejects(scanAgent(fakeClient({ searchComplete: false }), store,
      snapshot, new Set(["456"]), new Date("2026-09-21T10:00:00Z"), true), /Incomplete involved search/);
    assert.equal(store.pollCursor("vip-ai-research"), null);
    assert.deepEqual(store.counts(), {});
  } finally { store.close(); }
});
