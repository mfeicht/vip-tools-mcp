import { workOnce } from "./asana-dispatch-worker.mjs";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { assertCodexOutputSchema, cleanNoWriteDueTask, documentedDependencyNoWrite, promptFor,
  noStorySignalsDeferredByDue, noWriteDisposition, verifiedOpenTaskHandoff, documentedHumanWaitNoWrite, transientPreStartFailure } from "./asana-dispatch-worker.mjs";

test("all agent dispatch prompts bind the same no-Render and safe-retry contract", () => {
  for (const agent of ["vip-ai-social-media", "vip-ai-finance", "vip-ai-communication", "vip-ai-operations"]) {
    const prompt = promptFor({ run_id: "test", signals: [{ agent_id: agent, kind: "due_task" }] },
      { gid: "1", name: "Routine" }, { tier: "standard" });
    assert.match(prompt, /VIP-MCP-NO-RENDER-FALLBACK-V1/);
    assert.match(prompt, /auch nicht read-only/);
    assert.match(prompt, /Kein Retry einer unklaren Mutation ohne Readback\/Dedupe/);
    assert.ok(prompt.includes(`agent_id=${agent}`));
  }
});

test("Codex output schema requires every property and permits null dependency fields", () => {
  const schema = JSON.parse(fs.readFileSync(path.join(import.meta.dirname,
    "asana-dispatch-result.schema.json"), "utf8"));
  assert.deepEqual([...schema.required].sort(), Object.keys(schema.properties).sort());
  assert.deepEqual(schema.properties.linked_task_gid.type, ["string", "null"]);
  assert.deepEqual(schema.properties.evidence_story_gid.type, ["string", "null"]);
});

test("worker rejects invalid output schema before a model turn", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-output-schema-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const valid = path.join(import.meta.dirname, "asana-dispatch-result.schema.json");
  assert.equal((await assertCodexOutputSchema(valid)).type, "object");
  const invalid = JSON.parse(fs.readFileSync(valid, "utf8"));
  invalid.required.pop();
  const invalidPath = path.join(dir, "invalid.json");
  fs.writeFileSync(invalidPath, JSON.stringify(invalid));
  await assert.rejects(assertCodexOutputSchema(invalidPath), /require exactly every/);
});

test("future due gate suppresses mixed non-story continuation signals before a model turn", () => {
  const now = new Date("2026-09-22T11:45:00.000Z");
  const task = { due_on: "2026-09-23", due_at: "2026-09-23T08:30:00.000Z" };
  const mixed = { signals: [
    { kind: "due_task", story_gid: null },
    { kind: "continuation", story_gid: null }
  ] };
  assert.equal(noStorySignalsDeferredByDue(mixed, task, now), true);
  assert.equal(noStorySignalsDeferredByDue({ signals: [
    ...mixed.signals, { kind: "human_comment", story_gid: "999001" }
  ] }, task, now), false);
  assert.equal(noStorySignalsDeferredByDue({ signals: [
    { kind: "continuation", story_gid: null }
  ] }, { due_on: "2026-09-22", due_at: "2026-09-22T08:30:00.000Z" }, now), false);
});

test("MCP failure before agent start retries and releases both leases", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-worker-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const db = path.join(dir, "store.sqlite");
  const store = new AsanaDispatchStore(db);
  store.enqueue({ id: "preflight-test", agent_id: "vip-ai-research",
    task_gid: "123456", source_version: "preflight-test", kind: "due_task",
    priority: 60, available_at_ms: Date.now() - 1000 });
  store.close();

  let calls = 0;
  const result = await workOnce({ db, resolveBinary: () => process.execPath,
    connect: async () => { calls++; throw new Error("HTTP 503 fixture"); } });
  assert.equal(result.status, "retry_after");
  assert.equal(calls, 1);

  const after = new AsanaDispatchStore(db);
  assert.deepEqual(after.counts(), { retry_after: 1 });
  assert.deepEqual(after.activeLeases(), []);
  const run = after.db.prepare("SELECT state,thread_id FROM runs LIMIT 1").get();
  assert.equal(run.state, "completed");
  assert.equal(run.thread_id, null);
  after.close();
});

test("documented due-task dependency qualifies for a no-write acknowledgement", () => {
  const startedAt = Date.parse("2026-09-22T05:36:12Z");
  const story = { gid: "999001", resource_subtype: "comment_added",
    created_at: "2026-09-22T03:44:17Z", created_by: { gid: "review-user" },
    text: "Schlussabnahme wartet auf Routine 1218681595288097." };
  const before = { gid: "1218614928369934", completed: false,
    assignee: { gid: "sales-user" }, modified_at: "2026-09-22T03:44:26Z" };
  const input = { claim: { signals: [{ kind: "due_task", story_gid: null }] }, before,
    after: { ...before }, beforeStories: [story], afterStories: [story],
    linkedTask: { gid: "1218681595288097", completed: false,
      modified_at: "2026-09-21T22:04:10Z" },
    answer: { outcome: "blocked", summary: "Die dokumentierte Abhaengigkeit liegt ausschliesslich in der offenen Routine.",
      linked_task_gid: "1218681595288097", evidence_story_gid: "999001" },
    codex: { exitCode: 0, timedOut: false, threadId: "thread-1" },
    ownGid: "sales-user", startedAt };
  assert.equal(cleanNoWriteDueTask(input), true);
  assert.equal(documentedDependencyNoWrite(input), true);
  assert.equal(noWriteDisposition(input), "acknowledged");
  assert.equal(cleanNoWriteDueTask({ ...input,
    claim: { signals: [{ kind: "human_comment", story_gid: "999001" }] } }), false);
  assert.equal(noWriteDisposition({ ...input,
    claim: { signals: [{ kind: "human_comment", story_gid: "999001" }] } }), "dead_letter");
  assert.equal(noWriteDisposition({ ...input,
    claim: { signals: [{ kind: "dependency_ready", story_gid: null }] } }), "dead_letter");
  assert.equal(cleanNoWriteDueTask({ ...input,
    after: { ...before, modified_at: "2026-09-22T05:40:00Z" } }), false);
  assert.equal(cleanNoWriteDueTask({ ...input,
    codex: { ...input.codex, threadId: null } }), false);
  assert.equal(documentedDependencyNoWrite({ ...input,
    linkedTask: { ...input.linkedTask, completed: true } }), false);
  assert.equal(documentedDependencyNoWrite({ ...input,
    linkedTask: { ...input.linkedTask, modified_at: "2026-09-22T05:39:00Z" } }), true);
  assert.equal(documentedDependencyNoWrite({ ...input,
    answer: { ...input.answer, evidence_story_gid: "999002" } }), false);
  assert.equal(noWriteDisposition({ ...input,
    answer: { ...input.answer, evidence_story_gid: "999002" } }), "dead_letter");
  assert.equal(documentedDependencyNoWrite({ ...input,
    afterStories: [story, { ...story, gid: "999003", created_by: { gid: "sales-user" },
      created_at: "2026-09-22T05:40:00Z" }] }), false);
});

test("unverified clean no-write due task dead-letters without retaining an agent lease", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "asana-dispatch-no-write-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const store = new AsanaDispatchStore(path.join(dir, "store.sqlite"));
  t.after(() => store.close());
  store.enqueue({ id: "due-1", agent_id: "vip-ai-sales", task_gid: "123456",
    source_version: "due-1", kind: "due_task", priority: 60, available_at_ms: 1000 }, 1000);
  const claim = store.claimNext("run-1", { now: 1000 });
  store.recordRun(claim, { threadId: "thread-1", state: "started", now: 1000 });
  store.settle(claim, { outcome: "dead_letter", error: "Unverified no-write dependency", now: 2000 });
  store.recordRun(claim, { threadId: "thread-1", state: "completed",
    error: "Operations review required", now: 2000 });
  assert.deepEqual(store.counts(), { dead_letter: 1 });
  assert.deepEqual(store.activeLeases(), []);
  assert.equal(store.claimNext("run-2", { now: 3000 }), null);
});


test("documented open-task handoff acknowledges worker work, never Asana completion", () => {
  const startedAt=Date.parse("2026-10-22T15:06:00Z");
  const input={before:{gid:"123",completed:false,assignee:{gid:"111"}},
    after:{gid:"123",completed:false,assignee:{gid:"222"},modified_at:"2026-10-22T15:13:00Z"},
    afterStories:[{gid:"333",resource_subtype:"comment_added",text:"Dokumentation übergeben; menschliche QA bleibt offen.",created_by:{gid:"111"},created_at:"2026-10-22T15:12:00Z"}],
    answer:{outcome:"handed_off",summary:"Dokumentation ist belegt übergeben; fachliche QA bleibt offen."},
    codex:{exitCode:0,timedOut:false,threadId:"thread"},ownGid:"111",startedAt};
  assert.equal(verifiedOpenTaskHandoff(input),true);
  assert.equal(verifiedOpenTaskHandoff({...input,answer:{...input.answer,outcome:"completed"}}),true);
  for(const patch of [
    {after:{...input.after,assignee:{gid:"111"}}},
    {after:{...input.after,assignee:null}},
    {after:{...input.after,gid:"999"}},
    {after:{...input.after,modified_at:"2026-10-21T00:00:00Z"}},
    {before:{...input.before,assignee:{gid:"222"}}},
    {afterStories:[]},
    {afterStories:[{...input.afterStories[0],created_by:{gid:"222"}}]},
    {afterStories:[{...input.afterStories[0],resource_subtype:"assigned"}]},
    {codex:{...input.codex,timedOut:true}},
    {codex:{...input.codex,exitCode:1}},
    {answer:{...input.answer,outcome:"failed"}}
  ])assert.equal(verifiedOpenTaskHandoff({...input,...patch}),false,JSON.stringify(patch));
});


test("human wait acknowledges only unchanged own question without newer replies", () => {
  const story={gid:"222",resource_subtype:"comment_added",created_by:{gid:"111"},
    created_at:"2026-10-21T10:00:00Z",text:"Bitte entscheide über den Vorschlag; ich warte auf deine Antwort."};
  const before={gid:"123",completed:false,assignee:{gid:"111"},modified_at:"2026-10-21T10:01:00Z"};
  const input={claim:{signals:[{kind:"due_task",story_gid:null}]},before,after:{...before},
    beforeStories:[story],afterStories:[story],answer:{outcome:"blocked",summary:"Ich warte auf die bereits angefragte menschliche Entscheidung.",
      linked_task_gid:null,evidence_story_gid:"222"},codex:{exitCode:0,timedOut:false,threadId:"thread"},
    ownGid:"111",startedAt:Date.parse("2026-10-22T10:00:00Z")};
  assert.equal(documentedHumanWaitNoWrite(input),true);
  assert.equal(noWriteDisposition(input),"acknowledged");
  for(const patch of [
    {answer:{...input.answer,outcome:"no_action"}},
    {answer:{...input.answer,linked_task_gid:"333"}},
    {claim:{signals:[{kind:"human_comment",story_gid:"222"}]}},
    {afterStories:[{...story,text:"Editiert"}]},
    {beforeStories:[{...story,created_by:{gid:"333"}}]},
    {after:{...before,modified_at:"2026-10-22T10:02:00Z"}},
    {afterStories:[story,{...story,gid:"223",created_by:{gid:"333"},created_at:"2026-10-21T12:00:00+02:00"}]}
  ]) assert.equal(documentedHumanWaitNoWrite({...input,...patch}),false,JSON.stringify(patch));
});

test("prestart classifier preserves permission errors and uses canonical transport categories",()=>{
  for(const e of [new Error("HTTP 503"),new Error("fetch failed"),Object.assign(new Error("gateway"),{code:502}),
    new Error("Cloudflare managed challenge on the canonical MCP endpoint")])
    assert.equal(transientPreStartFailure(e),true,String(e));
  for(const e of [new Error("HTTP 403"),new Error("Invalid output schema"),
    Object.assign(new Error("gateway 503"),{code:403})]) assert.equal(transientPreStartFailure(e),false);
});

test("repeated preflight outages do not exhaust task attempts or retain leases",async t=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),"dispatch-outage-"));
  t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
  const db=path.join(dir,"store.sqlite");
  const store=new AsanaDispatchStore(db); t.after(()=>store.close());
  store.enqueue({id:"due",agent_id:"vip-ai-research",task_gid:"123",source_version:"due",kind:"due_task",priority:60});
  for(let i=0;i<7;i++){
    store.db.prepare("UPDATE signals SET available_at_ms=0 WHERE id='due'").run();
    const start=Date.now();
    await workOnce({db,resolveBinary:()=>process.execPath,connect:async()=>{throw new Error("HTTP 503 fixture");}});
    const signal=store.db.prepare("SELECT * FROM signals WHERE id='due'").get();
    assert.equal(signal.status,"retry_after");assert.equal(signal.attempts,0);
    assert.ok(signal.available_at_ms>=start+900000);assert.deepEqual(store.activeLeases(),[]);
  }
  store.db.prepare("UPDATE signals SET available_at_ms=0,attempts=4 WHERE id='due'").run();
  await workOnce({db,resolveBinary:()=>process.execPath,connect:async()=>{throw new Error("HTTP 403 fixture");}});
  assert.deepEqual(store.counts(),{dead_letter:1});
});

test("preflight deferral cannot release a launched claim",t=>{
  const store=new AsanaDispatchStore(":memory:");t.after(()=>store.close());
  store.enqueue({id:"due",agent_id:"vip-ai-research",task_gid:"123",source_version:"due",kind:"due_task",priority:60});
  const claim=store.claimNext("run-1");assert.equal(store.beginCodexLaunch(claim),true);
  assert.throws(()=>store.settle(claim,{outcome:"retry_after",transientPreflight:true}),/unlaunched/);
  assert.deepEqual(store.counts(),{leased:1});assert.equal(store.activeLeases().length,2);
});
