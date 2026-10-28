import assert from "node:assert/strict";
import test from "node:test";
import {AsanaDispatchStore} from "./asana-dispatch-store.mjs";
import {resolveDeadLetters} from "./asana-dispatch-reconcile.mjs";

const context = {threadId:"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",source:"CODEX_THREAD_ID"};
function setup(t, target="vip-ai-sales") {
  const store=new AsanaDispatchStore(":memory:");t.after(()=>store.close());
  store.enqueue({id:"dead-1",agent_id:target,task_gid:"123456",source_version:"v1",kind:"due_task"},1000);
  const claim=store.claimNext("old-run",{now:1000});
  store.settle(claim,{outcome:"dead_letter",error:"review required",now:2000});
  store.acquireManualAgent("vip-ai-operations","ops-run",context,3000);
  store.acquireManualTask("ops-run","123456",context,3000);
  const args={signalIds:["dead-1"],agentId:target,taskGid:"123456",outcome:"acknowledged",
    evidence:"Two current task readbacks and verified handoff prove the historical signal is obsolete.",
    operator:{runId:"ops-run",context},now:4000};
  return {store,args};
}

test("thread-bound Operations review retains its task and agent leases and records acceptance",t=>{
  const {store,args}=setup(t);const before=store.activeLeases();
  assert.equal(resolveDeadLetters(store,args).signals_resolved,1);
  assert.deepEqual(store.activeLeases(),before);
  assert.equal(store.counts().acknowledged,1);
  const event=store.db.prepare("SELECT * FROM recovery_events WHERE kind='dead_letter_operator_review'").get();
  assert.equal(event.run_id,"ops-run");
  assert.equal(JSON.parse(event.evidence_json).thread_id,context.threadId);
});

test("Operations can review its own historical signal without dropping its agent lease",t=>{
  const {store,args}=setup(t,"vip-ai-operations");
  assert.equal(resolveDeadLetters(store,args).signals_resolved,1);
  assert.equal(store.activeLeases().length,2);
});

test("foreign owner activity, thread, task fence, absent binding and expired leases fail closed",t=>{
  const cases=[
    s=>s.store.acquireLease("agent:vip-ai-sales","foreign-run",60000,3000),
    s=>{s.args.operator={...s.args.operator,context:{...context,threadId:"bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"}};},
    s=>s.store.db.exec("UPDATE manual_run_tasks SET lease_fence=lease_fence+1"),
    s=>s.store.db.exec("DELETE FROM manual_run_tasks"),
    s=>s.store.db.exec("UPDATE leases SET expires_at_ms=3999 WHERE key LIKE 'agent:%'"),
    s=>s.store.db.exec("UPDATE leases SET expires_at_ms=3999 WHERE key LIKE 'task:%'"),
    s=>s.store.db.exec("UPDATE lease_fences SET last_fence=last_fence+1"),
    s=>s.store.db.exec("UPDATE manual_runs SET state='released'"),
    s=>{s.args.operator=null;},
  ];
  for(const change of cases){
    const s=setup(t);change(s);const before=s.store.activeLeases();
    assert.throws(()=>resolveDeadLetters(s.store,s.args));
    assert.equal(s.store.counts().dead_letter,1);
    assert.deepEqual(s.store.activeLeases(),before);
    assert.equal(s.store.db.prepare("SELECT count(*) n FROM recovery_events WHERE kind='dead_letter_operator_review'").get().n,0);
  }
});

test("wrong signal rolls back the entire reviewed batch without changing operator fences",t=>{
  const {store,args}=setup(t);const before=store.activeLeases();
  args.signalIds.push("missing-signal");
  assert.throws(()=>resolveDeadLetters(store,args),/state or scope changed/);
  assert.equal(store.counts().dead_letter,1);
  assert.deepEqual(store.activeLeases(),before);
});
