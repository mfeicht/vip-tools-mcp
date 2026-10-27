import assert from "node:assert/strict";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { taskSignal } from "./asana-dispatch-signals.mjs";
import { connectMcpClient, formatToolError, retryableToolError, scanAgent, tool } from "./asana-dispatch-poller.mjs";

function fakeClient({ searchTasks = [], stories = [], searchComplete = true,
  linkedTask = null } = {}) {
  const calls = [];
  return {
    calls,
    async callTool({ name, arguments: args }) {
      calls.push({ name, args });
      const data = name === "asana_search_tasks"
        ? { search_status: "ok", search_complete: searchComplete,
          result_truncated: !searchComplete, tasks: searchTasks }
        : args.path === `/tasks/${linkedTask?.gid}`
          ? { response: { data: linkedTask } }
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

test("completed linked task wakes the source once without rerunning while dependency is open", async () => {
  const store = new AsanaDispatchStore(":memory:");
  try {
    const start = Date.parse("2026-09-21T10:00:00Z");
    const task = { gid: "123", name: "Source task", completed: false,
      assignee: { gid: "sales-user" }, due_on: "2026-09-20",
      modified_at: "2026-09-21T09:00:00Z" };
    store.enqueue({ ...taskSignal("vip-ai-sales", task, { now: new Date(start) }),
      available_at_ms: start }, start);
    const claim = store.claimNext("run-1", { now: start });
    store.settle(claim, { outcome: "acknowledged", now: start, dependencyWatch: {
      agent_id: "vip-ai-sales", task_gid: "123", linked_task_gid: "456",
      evidence_story_gid: "789" } });
    const snapshot = { agent_id: "vip-ai-sales", ok: true, user_gid: "sales-user",
      workspace_gid: "111", tasks: [task], truncated: false };
    const linkedTask = { gid: "456", completed: false,
      modified_at: "2026-09-21T09:30:00Z" };
    const client = fakeClient({ linkedTask });
    const agents = new Set(["sales-user"]);
    const early = await scanAgent(client, store, snapshot, agents,
      new Date(start + 5 * 60_000), true);
    assert.equal(early.dependencies_checked, 0);
    const pending = await scanAgent(client, store, snapshot, agents,
      new Date(start + 15 * 60_000), true);
    assert.equal(pending.dependencies_checked, 1);
    assert.equal(pending.signals_inserted, 0);
    linkedTask.completed = true;
    linkedTask.modified_at = "2026-09-21T10:20:00Z";
    const ready = await scanAgent(client, store, snapshot, agents,
      new Date(start + 30 * 60_000), true);
    assert.equal(ready.signals_inserted, 1);
    assert.equal(store.counts().pending, 1);
    assert.deepEqual(store.dependencyWatches("vip-ai-sales", start + 60 * 60_000), []);
    const repeated = await scanAgent(client, store, snapshot, agents,
      new Date(start + 35 * 60_000), true);
    assert.equal(repeated.signals_inserted, 0);
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

test("tool failures retain the failing MCP tool name for poll diagnostics", async () => {
  const client = { async callTool() { throw new Error("Streamable HTTP error: endpoint unavailable"); } };
  await assert.rejects(tool(client, "asana_search_tasks", {}),
    /asana_search_tasks: Streamable HTTP error: endpoint unavailable/);
});

test("streamable HTTP status codes are retryable even when the response body omits the status", () => {
  const transient = new Error("Streamable HTTP error: Error POSTing to endpoint: ");
  transient.code = 503;
  assert.equal(retryableToolError(transient), true);
  assert.equal(formatToolError(transient),
    "Streamable HTTP error: Error POSTing to endpoint:  (HTTP 503)");

  const permanent = new Error("Streamable HTTP error: Error POSTing to endpoint: bad request");
  permanent.code = 400;
  assert.equal(retryableToolError(permanent), false);
  assert.match(formatToolError(permanent), /HTTP 400/);
});

test("poller delegates one canonical connect cycle without an extra retry tier", async () => {
  let calls = 0;
  await assert.rejects(connectMcpClient(async () => { calls++; throw new Error("HTTP 503"); }), /mcp_connect.*503/);
  assert.equal(calls, 1);
  const client = {};assert.equal(await connectMcpClient(async () => client), client);
});

test("tool errors and ambiguous writes are never replayed by the poller", async () => {
  for (const message of ["HTTP 503", "HTTP 429", "HTTP 403"]) {
    let calls = 0;
    await assert.rejects(tool({callTool: async () => {calls++;throw new Error(message);}},
      "asana_comment", {}), /asana_comment/);
    assert.equal(calls, 1);
  }
});


test("cyclic audit discovers comments without modified_at changes and backfills once",async()=>{
  const store=new AsanaDispatchStore(":memory:");
  try{
    const now=new Date("2026-10-22T10:00:00Z");
    const task={gid:"123",name:"Task",completed:false,assignee:{gid:"456"},modified_at:"2026-10-20T10:00:00Z"};
    store.observeTask({agent_id:"vip-ai-research",task_gid:"123",modified_at:task.modified_at,
      latest_story_at:"2026-10-21T10:00:00Z",latest_story_gid:"888"},now.getTime()-3600000);
    const stories=[{gid:"777",resource_subtype:"comment_added",created_by:{gid:"900"},
      created_at:"2026-10-20T11:00:00Z",text:"Bitte den offenen Punkt bearbeiten."}];
    const client=fakeClient({stories});
    const snapshot={agent_id:"vip-ai-research",ok:true,truncated:false,user_gid:"456",workspace_gid:"111",tasks:[task]};
    const first=await scanAgent(client,store,snapshot,new Set(["456"]),now,true);
    assert.equal(first.signals_inserted,1);assert.equal(first.stories_read,1);
    const observation=store.observation("vip-ai-research","123");
    assert.equal(observation.history_checked_at_ms,now.getTime());
    assert.equal((await scanAgent(client,store,snapshot,new Set(["456"]),new Date(now.getTime()+300000),true)).stories_read,0);
    stories.push({...stories[0],gid:"999",created_at:"2026-10-22T10:10:00Z"});
    const later=await scanAgent(client,store,snapshot,new Set(["456"]),new Date(now.getTime()+900000),true);
    assert.equal(later.signals_inserted,1);assert.equal(store.counts().pending,2);
  }finally{store.close();}
});

test("failed cyclic audit never advances observation or poll cursor",async()=>{
  const store=new AsanaDispatchStore(":memory:");
  try{
    const task={gid:"123",name:"Task",completed:false,assignee:{gid:"456"},modified_at:"2026-10-20T10:00:00Z"};
    store.observeTask({agent_id:"vip-ai-research",task_gid:"123",modified_at:task.modified_at},1000);
    const before=store.observation("vip-ai-research","123");
    const story={gid:"777",created_at:"2026-10-20T11:00:00Z",type:"comment",created_by:{gid:"900"}};
    const snapshot={agent_id:"vip-ai-research",ok:true,truncated:false,user_gid:"456",workspace_gid:"111",tasks:[task]};
    await assert.rejects(scanAgent(fakeClient({stories:[story,story]}),store,snapshot,new Set(["456"]),new Date(),true),/duplicate story/);
    assert.deepEqual(store.observation("vip-ai-research","123"),before);
    assert.equal(store.pollCursor("vip-ai-research"),null);assert.deepEqual(store.counts(),{});
  }finally{store.close();}
});


test("one transport failure stops later agent scans while a scoped permission error stays isolated",async()=>{
  const {scanSnapshots}=await import("./asana-dispatch-poller.mjs");
  for(const message of ["HTTP 503","HTTP 503 <title>Service Suspended</title> This service has been suspended by its owner.","HTTP 429","Cloudflare managed challenge on the canonical MCP endpoint","Request timed out","HTTP 403"]){
    const seen=[];
    const result=await scanSnapshots(null,null,[{agent_id:"a"},{agent_id:"b"},{agent_id:"c"}],new Set(),new Date(),false,
      async(_client,_store,snapshot)=>{seen.push(snapshot.agent_id);if(snapshot.agent_id==="b")throw new Error(message);return {agent_id:snapshot.agent_id};});
    assert.equal(result.errors.length,1);
    assert.deepEqual(seen,message==="HTTP 403" ? ["a","b","c"] : ["a","b"]);
  }
});
