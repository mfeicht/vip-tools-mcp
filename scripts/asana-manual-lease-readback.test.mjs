import assert from "node:assert/strict";
import test from "node:test";
import { readManualNative, readManualLeaseDecision, readManualAsanaTask } from "./asana-manual-lease-readback.mjs";
import { processScanBinding } from "./asana-worker-process-readback.mjs";

const now = Date.now();
const run = { run_id: "manual-test", agent_id: "vip-ai-operations",
  thread_id: "thread-1", state: "active", acquired_at_ms: now - 30_000,
  agent_lease_token: "agent-token", agent_lease_fence: 1, agent_fence: 1,
  tasks: [{ task_gid: "123", acquired_at_ms: now - 29_000,
    lease_token: "task-token", lease_fence: 1, bound_lease_fence: 1 }] };

function rpc(overrides = {}) {
  return { async call(method) {
    if (method === "thread/read") return { thread: { id: run.thread_id,
      status: { type: "idle" } } };
    if (method === "thread/turns/list") return { data: [{ id: "turn-1",
      startedAt: (run.acquired_at_ms - 1000) / 1000,
      completedAt: (now - 20_000) / 1000, status: "completed" }], nextCursor: null };
    if (method === "thread/items/list") return { data: [{ item: { id: "item-1",
      type: "agentMessage", text: "Finished" } }], nextCursor: null };
    throw new Error(`Unexpected ${method}`);
  }, ...overrides };
}

test("native collector binds one complete turn and hashes its activity", async () => {
  const row = await readManualNative(run, rpc());
  assert.equal(row.turn_id, "turn-1");
  assert.equal(row.activity_item_count, 1);
  assert.match(row.activity_digest, /^[a-f0-9]{64}$/);
  await assert.rejects(readManualNative(run, rpc({ call: async (method) =>
    method === "thread/items/list" ? { data: [], nextCursor: "more" } :
      rpc().call(method) })), /pagination incomplete|repeated cursor|activity items incomplete/);
});

test("read-only driver takes two native and scoped Asana reads before deciding", async () => {
  const calls = [];
  const client = { async callTool({ name, arguments: args }) {
    calls.push([name, args.path, args.agent_id]);
    return { content: [{ type: "text", text: JSON.stringify({ response: {
      data: { gid: "user-1" } } }) }] };
  } };
  let nativeCount = 0;
  let taskCount = 0;
  const result = await readManualLeaseDecision(run, { rpc: rpc(), client,
    readNative: async () => { nativeCount += 1; return {
      observed_at_ms: now - 2500 + nativeCount * 1500,
      source: "codex_app.read_thread", host_id: "local", thread_id: run.thread_id,
      runtime_status: "idle", turn_id: "turn-1", turn_status: "completed",
      started_at_ms: run.acquired_at_ms - 1000, completed_at_ms: now - 20_000,
      activity_item_count: 2, activity_last_item_id: "item-2", activity_digest: "a".repeat(64) }; },
    readAsana: async () => { taskCount += 1; return {
      observed_at_ms: now - 2500 + taskCount * 1500,
      agent_id: run.agent_id, task: { gid: "123", completed: true,
        modified_at: new Date(now - 20_000).toISOString() },
      stories: [], next_page: null }; },
    scan: () => ({ ...processScanBinding(run), observed_at_ms: now - 100,
      raw_sha256: "b".repeat(64), scan_succeeded: true, matching_processes: [] }),
    wait: async () => {}, now: () => now });
  assert.equal(result.decision.action, "release");
  assert.equal(nativeCount, 2);
  assert.equal(taskCount, 2);
  assert.deepEqual(calls, [["asana_request", "/users/me", run.agent_id]]);
});


test("system stories may have no author but comments require attributable authors", async () => {
  let comment=false;
  const client={callTool:async({arguments:args})=>({content:[{type:"text",text:JSON.stringify({response:
    args.path.endsWith("/stories") ? {data:[{gid:"333",created_at:"2026-10-21T08:00:00Z",
      resource_subtype:comment?"comment_added":"task_due_time_approaching_story",created_by:null}],next_page:null} :
      {data:{gid:"123",completed:false,modified_at:"2026-10-21T08:00:00Z"}}})}]})};
  assert.equal((await readManualAsanaTask(run,run.tasks[0],client)).stories.length,1);
  comment=true;
  await assert.rejects(readManualAsanaTask(run,run.tasks[0],client),/story readback incomplete/);
});

test("pending native tool items cannot attest a terminal run", async () => {
  await assert.rejects(readManualNative(run,rpc({call:async(method)=>method==="thread/items/list"?
    {data:[{item:{id:"unfinished",type:"commandExecution",status:"inProgress"}}],nextCursor:null}:
    rpc().call(method)})),/activity items incomplete/);
});


test("dispatch answer comes only from final native assistant item with exact schema",async()=>{
  const {dispatchAnswerFromItems}=await import("./asana-manual-lease-readback.mjs");
  const answer={outcome:"progress",summary:"Ergebnisstory 222",next_step:"Nächster geprüfter Schritt",
    linked_task_gid:null,evidence_story_gid:"222"};
  const item={type:"agentMessage",phase:"final_answer",text:JSON.stringify(answer)};
  assert.deepEqual(dispatchAnswerFromItems([{item}]),answer);
  for(const patch of [{phase:"commentary"},{type:"userMessage"},{text:"Kein JSON"},
    {text:JSON.stringify({...answer,evidence_story_gid:222})},{text:JSON.stringify({...answer,extra:true})}])
    assert.equal(dispatchAnswerFromItems([{item:{...item,...patch}}]),null);
});

test("capacity refusal is resumable only with one user input and zero executable activity",async()=>{
  const {capacityFailedWithoutWork}=await import('./asana-manual-lease-readback.mjs');
  const turn={status:'failed',completedAt:1792738322,
    error:{message:'Selected model is at capacity. Please try a different model.'}};
  const entries=[{item:{type:'userMessage',id:'u'}},{item:{type:'reasoning',id:'r'}},
    {item:{type:'agentMessage',id:'a',phase:'commentary'}}];
  assert.equal(capacityFailedWithoutWork(turn,entries),true);
  for(const item of [{type:'commandExecution',status:'completed',exitCode:0},
    {type:'mcpToolCall',status:'completed'},{type:'fileChange'},
    {type:'agentMessage',phase:'final_answer'},{type:'unknownTool'},{type:'userMessage'}])
    assert.equal(capacityFailedWithoutWork(turn,[...entries,{item}]),false);
  for(const patch of [{status:'completed'},{completedAt:null},{error:{message:'network failure'}}])
    assert.equal(capacityFailedWithoutWork({...turn,...patch},entries),false);
  assert.equal(capacityFailedWithoutWork(turn,[]),false);
  assert.equal(capacityFailedWithoutWork(turn,entries.slice(1)),false);
});
