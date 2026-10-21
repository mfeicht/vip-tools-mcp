import test from "node:test";
import assert from "node:assert/strict";
import { AsanaDispatchStore } from "./asana-dispatch-store.mjs";
import { dispatchRecoverySnapshot, decideDispatchRecovery, applyDispatchRecovery } from "./asana-dispatch-terminal-recovery.mjs";
import { processScanBinding } from "./asana-worker-process-readback.mjs";

function fixture(t) {
  const store=new AsanaDispatchStore(":memory:");
  t.after(()=>store.close());
  const now=Date.now();
  store.enqueue({id:"signal-1",agent_id:"vip-ai-test",task_gid:"123456",source_version:"version-1",
    kind:"due_task",priority:50,available_at_ms:now-900000},now-900000);
  const claim=store.claimNext("run-1",{now:now-900000});
  store.recordRun(claim,{threadId:"thread-1",turnId:"turn-1",state:"needs_reconciliation",now:now-800000});
  const snapshot=dispatchRecoverySnapshot(store,"run-1");
  const native={source:"codex_app.read_thread",host_id:"local",thread_id:"thread-1",
    runtime_status:"notLoaded",turn_id:"turn-1",turn_status:"failed",
    started_at_ms:now-850000,completed_at_ms:now-10000,activity_item_count:3,
    activity_last_item_id:"item-3",activity_digest:"a".repeat(64)};
  const asana={agent_id:"vip-ai-test",task:{gid:"123456",completed:true,
    modified_at:new Date(now-5000).toISOString()},stories:[],next_page:null};
  const proof={native:[{...native,observed_at_ms:now-4000},{...native,observed_at_ms:now-2000}],
    asana:[{...asana,observed_at_ms:now-4000},{...asana,observed_at_ms:now-2000}]};
  const scan={...processScanBinding(snapshot.run),observed_at_ms:now,scan_succeeded:true,
    raw_sha256:"b".repeat(64),matching_processes:[]};
  return {store,snapshot,proof,scan,now};
}

test("failed turn with two stable completed-task proofs releases exact claim",t=>{
  const {store,snapshot,proof,scan,now}=fixture(t);
  assert.equal(applyDispatchRecovery(store,snapshot,proof,{scan:()=>scan,now}).action,"acknowledge");
  assert.deepEqual(store.activeLeases(),[]);
  assert.deepEqual(store.counts(),{acknowledged:1});
  assert.equal(applyDispatchRecovery(store,snapshot,proof,{scan:()=>scan,now}).reason,"recovery_fence_changed");
});

test("open task quarantines only that task, then a later completion releases it",t=>{
  const f=fixture(t);
  for(const row of f.proof.asana) row.task={...row.task,completed:false};
  assert.equal(applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).action,"quarantine");
  assert.deepEqual(f.store.activeLeases().map(l=>l.key),["task:123456"]);
  assert.deepEqual(f.store.counts(),{leased:1});
  f.store.acquireLease("agent:vip-ai-test","new-run",60000,f.now);
  const next=dispatchRecoverySnapshot(f.store,"run-1");
  for(const row of f.proof.asana) row.task={...row.task,completed:true};
  assert.equal(applyDispatchRecovery(f.store,next,f.proof,{scan:()=>f.scan,now:f.now}).action,"acknowledge");
  assert.equal(f.store.activeLeases()[0].run_id,"new-run");
});

test("activity, process, pagination, missing end time and stale proofs fail closed",t=>{
  const f=fixture(t);
  for(const mutate of [p=>{p.native[1].activity_digest="c".repeat(64);},
    p=>{p.asana[1].next_page="more";},p=>{p.native.forEach(r=>r.completed_at_ms=null);},
    p=>{p.native.forEach(r=>r.observed_at_ms-=600000);},p=>{p.asana[1].task={...p.asana[1].task,completed:false};}]) {
    const p=structuredClone(f.proof);mutate(p);
    assert.equal(decideDispatchRecovery(f.snapshot,p,f.scan,f.now).action,"hold");
  }
  assert.equal(decideDispatchRecovery(f.snapshot,f.proof,{...f.scan,matching_processes:[{pid:123}]},f.now).action,"hold");
  assert.equal(f.store.activeLeases().length,2);
});

test("lease renewal, extra task and transaction crash never orphan a claim",t=>{
  const f=fixture(t);
  f.store.acquireLease("task:123456","run-1",120000,f.now);
  assert.equal(applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).reason,"recovery_fence_changed");
  f.store.acquireLease("task:999999","run-1",120000,f.now);
  assert.equal(decideDispatchRecovery(dispatchRecoverySnapshot(f.store,"run-1"),f.proof,f.scan,f.now).reason,"claim_incomplete_or_extra_leases");
  assert.equal(f.store.activeLeases().length,3);
});

test("a database error rolls signal and lease changes back together",t=>{
  const f=fixture(t);
  f.store.db.exec("CREATE TRIGGER crash BEFORE UPDATE ON runs BEGIN SELECT RAISE(ABORT,'crash'); END");
  assert.throws(()=>applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}),/crash/);
  assert.equal(f.store.activeLeases().length,2);
  assert.deepEqual(f.store.counts(),{leased:1});
});
