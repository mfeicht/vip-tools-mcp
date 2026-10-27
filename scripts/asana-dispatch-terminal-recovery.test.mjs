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

test("missing end time only frees the agent after 30s stable native and process proofs",t=>{
  const f=fixture(t);
  f.proof.native.forEach((row,index)=>{
    row.completed_at_ms=null; row.observed_at_ms=f.now-40000+index*31000;
  });
  f.proof.processes=f.proof.native.map(row=>({...f.scan,observed_at_ms:row.observed_at_ms+100}));
  for(const mutate of [
    p=>{p.native[1].observed_at_ms=p.native[0].observed_at_ms+2000;},
    p=>{p.processes[0].matching_processes=[{pid:42}];},
    p=>{p.processes[1].scan_succeeded=false;},
    p=>{p.processes[0].thread_id="another-thread";},
    p=>{p.native[1].activity_digest="c".repeat(64);}
  ]) {
    const proof=structuredClone(f.proof);mutate(proof);
    assert.equal(decideDispatchRecovery(f.snapshot,proof,f.scan,f.now).action,"hold");
  }
  assert.equal(applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).action,"quarantine");
  assert.deepEqual(f.store.activeLeases().map(row=>row.key),["task:123456"]);
  assert.deepEqual(f.store.counts(),{leased:1});
  assert.equal(f.store.db.prepare("SELECT state FROM runs").get().state,"needs_reconciliation");
  const later=dispatchRecoverySnapshot(f.store,"run-1");
  assert.equal(decideDispatchRecovery(later,f.proof,f.scan,f.now).action,"hold");
});


function openFixture(t,outcome="progress") {
  const f=fixture(t);f.proof.ownGid="111";
  f.proof.native.forEach(row=>{row.turn_status="completed";row.answer={outcome,
    summary:"Geprüfter Fortschritt mit Ergebnisstory 222.",evidence_story_gid:"222",linked_task_gid:null,
    next_step:outcome==="progress" ? "Den nächsten geprüften Teil des Auftrags bearbeiten." : null};});
  f.proof.asana.forEach(row=>{row.task={...row.task,completed:false,assignee:{gid:"111"}};
    row.stories=[{gid:"222",resource_subtype:"comment_added",created_by:{gid:"111"},
      created_at:new Date(f.now-20000).toISOString(),text:"Geprüfter Zwischenstand, der Haupttask bleibt offen."}];});
  return f;
}

test("documented open progress settles the exact run and preserves throttled continuation",t=>{
  const f=openFixture(t);
  const decision=applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now});
  assert.equal(decision.action,"acknowledge");assert.equal(decision.reason,"documented_open_task");
  assert.equal(decision.open.outcome,"progress");assert.deepEqual(f.store.activeLeases(),[]);
  assert.deepEqual(f.store.counts(),{acknowledged:1,pending:1});
  const continuation=f.store.db.prepare("SELECT * FROM signals WHERE kind='continuation'").get();
  assert.ok(continuation.available_at_ms>=f.now+3600000);
  assert.equal(f.store.db.prepare("SELECT state FROM runs WHERE run_id='run-1'").get().state,"completed");
  assert.equal(f.proof.asana[1].task.completed,false);
});

test("documented blocked run frees its task without falsely completing it",t=>{
  const f=openFixture(t,"blocked");
  assert.equal(applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).action,"acknowledge");
  assert.deepEqual(f.store.counts(),{acknowledged:1});
});

test("missing or contradictory open-task evidence never releases the task",t=>{
  const f=openFixture(t);
  for(const mutate of [
    p=>{p.ownGid="333";},
    p=>{p.native.forEach(row=>row.turn_status="failed");},
    p=>{p.native.forEach(row=>row.answer.next_step="");},
    p=>{p.native.forEach(row=>{row.answer.evidence_story_gid=null;row.answer.summary="Keine Story-GID.";});},
    p=>{p.asana.forEach(row=>row.task.assignee.gid="333");},
    p=>{p.asana.forEach(row=>row.stories[0].created_at=new Date(f.now-999999).toISOString());},
    p=>{p.asana.forEach(row=>row.stories[0].created_at=new Date(f.now-5000).toISOString());},
    p=>{p.asana.forEach(row=>row.stories.push({...row.stories[0],gid:"223",created_by:{gid:"333"}}));}
  ]) {
    const proof=structuredClone(f.proof);mutate(proof);
    assert.equal(decideDispatchRecovery(f.snapshot,proof,f.scan,f.now).action,"quarantine");
  }
});

function handoffFixture(t) {
  const f=openFixture(t,"handed_off");
  f.proof.asana.forEach(row=>{
    row.task.assignee={gid:"333"};
    row.stories.push({gid:"224",resource_subtype:"assigned",created_by:{gid:"111"},
      target:{gid:"123456"},assignee:{gid:"333"},created_at:new Date(f.now-15000).toISOString()});
  });
  return f;
}

test("verified terminal handoff settles only the old claim and creates no continuation",t=>{
  const f=handoffFixture(t);
  const decision=applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now});
  assert.equal(decision.action,"acknowledge");
  assert.equal(decision.open.outcome,"handed_off");
  assert.equal(decision.open.assignment_story_gid,"224");
  assert.equal(decision.open.assignee_gid,"333");
  assert.deepEqual(f.store.counts(),{acknowledged:1});
  assert.deepEqual(f.store.activeLeases(),[]);
  assert.equal(f.proof.asana[1].task.completed,false);
});

test("handoff requires exact result, own assignment, current target and no later ownership change",t=>{
  const f=handoffFixture(t);
  const mutations=[
    p=>{p.asana.forEach(r=>r.stories.pop());},
    p=>{p.asana.forEach(r=>r.task.assignee=null);},
    p=>{p.asana.forEach(r=>r.task.assignee.gid="111");},
    p=>{p.asana.forEach(r=>r.task.assignee.gid="444");},
    p=>{p.asana.forEach(r=>r.stories[1].assignee.gid="444");},
    p=>{p.asana.forEach(r=>r.stories[1].target.gid="999");},
    p=>{p.asana.forEach(r=>r.stories[1].created_by.gid="333");},
    p=>{p.asana.forEach(r=>r.stories[1].created_at=new Date(f.now-5000).toISOString());},
    p=>{p.asana.forEach(r=>r.stories[1].created_at=new Date(f.now-25000).toISOString());},
    p=>{p.asana.forEach(r=>r.stories.push({...r.stories[1],gid:"225"}));},
    p=>{p.asana.forEach(r=>r.stories.push({...r.stories[1],gid:"225",resource_subtype:"unassigned"}));},
    p=>{p.native.forEach(r=>r.answer.evidence_story_gid=null);},
    p=>{p.native.forEach(r=>r.answer.next_step="Start another worker");},
    p=>{p.native.forEach(r=>r.answer.linked_task_gid="999");},
    p=>{p.native.forEach(r=>r.turn_status="failed");}
  ];
  for(const mutate of mutations) {
    const proof=structuredClone(f.proof);mutate(proof);
    assert.equal(decideDispatchRecovery(f.snapshot,proof,f.scan,f.now).action,"quarantine");
  }
  assert.equal(f.store.activeLeases().length,2);
});

function capacityFixture(t) {
  const f=fixture(t);f.proof.ownGid='111';
  f.proof.native.forEach((r,i)=>{r.capacity_failed_without_work=true;r.answer=null;
    r.observed_at_ms=f.now-40000+i*31000;r.completed_at_ms=f.now-50000;});
  f.proof.asana.forEach(r=>{r.task={...r.task,completed:false,assignee:{gid:'111'},
    modified_at:new Date(f.snapshot.run.started_at_ms-1000).toISOString()};});
  return f;
}

test('terminal capacity refusal without work retries once with delay and unchanged attempt count',t=>{
  const f=capacityFixture(t);
  const attempts=f.snapshot.signals[0].attempts;
  const result=applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now});
  assert.equal(result.action,'retry_after');assert.equal(result.preserve_attempts,true);
  assert.deepEqual(f.store.activeLeases(),[]);assert.deepEqual(f.store.counts(),{retry_after:1});
  const s=f.store.db.prepare('SELECT * FROM signals').get();
  assert.equal(s.attempts,attempts);assert.equal(s.available_at_ms,f.now+900000);
  assert.equal(s.run_id,null);assert.equal(f.store.claimNext('early',{now:f.now}),null);
  assert.equal(applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now}).reason,'recovery_fence_changed');
  assert.equal(f.proof.asana[1].task.completed,false);
});

test('capacity retry rejects executable activity, incomplete terminal proof or changed owner',t=>{
  const f=capacityFixture(t);
  for(const mutate of [
    p=>{p.native.forEach(r=>r.capacity_failed_without_work=false);},
    p=>{p.native.forEach(r=>r.answer={outcome:'blocked'});},
    p=>{p.native[1].observed_at_ms=p.native[0].observed_at_ms+2000;},
    p=>{p.asana.forEach(r=>r.task.assignee.gid='333');},
    p=>{p.ownGid=null;},
    p=>{p.asana.forEach(r=>r.task.modified_at=new Date(f.now).toISOString());},
    p=>{p.asana.forEach(r=>r.stories=[{gid:'222',created_at:new Date(f.now).toISOString()}]);}
  ]) {
    const p=structuredClone(f.proof);mutate(p);
    assert.equal(decideDispatchRecovery(f.snapshot,p,f.scan,f.now).action,'quarantine');
  }
  assert.equal(f.store.activeLeases().length,2);
});

// A correction may cite its predecessor; the dedicated evidence field selects
// one story while every existing owner, time, stability and fence gate remains.
test("explicit corrected story disambiguates predecessor references",t=>{
  const f=openFixture(t,"blocked");
  f.proof.native.forEach(row=>row.answer.summary="Ergebnisstory 223, Korrekturstory 222 verifiziert.");
  f.proof.asana.forEach(row=>row.stories.unshift({...row.stories[0],gid:"223",
    created_at:new Date(f.now-25000).toISOString()}));
  const decision=applyDispatchRecovery(f.store,f.snapshot,f.proof,{scan:()=>f.scan,now:f.now});
  assert.equal(decision.action,"acknowledge");
  assert.equal(decision.open.evidence_story_gid,"222");
  assert.deepEqual(f.store.activeLeases(),[]);
  assert.equal(f.proof.asana[1].task.completed,false);
});

test("explicit invalid or ineligible evidence never falls back to prose",t=>{
  const f=openFixture(t,"blocked");
  for(const id of ["999","malformed",""]){
    const proof=structuredClone(f.proof);
    proof.native.forEach(row=>row.answer.evidence_story_gid=id);
    assert.equal(decideDispatchRecovery(f.snapshot,proof,f.scan,f.now).action,"quarantine");
  }
  for(const mutate of [
    p=>p.asana.forEach(row=>row.stories[0].created_by.gid="333"),
    p=>p.asana.forEach(row=>row.stories[0].created_at=new Date(f.now-5000).toISOString()),
    p=>p.asana.forEach(row=>row.stories.push({...row.stories[0]}))
  ]) {
    const proof=structuredClone(f.proof);mutate(proof);
    assert.equal(decideDispatchRecovery(f.snapshot,proof,f.scan,f.now).action,"quarantine");
  }
  assert.equal(f.store.activeLeases().length,2);
});

test("legacy prose-only proof accepts one story but holds ambiguous references",t=>{
  const f=openFixture(t,"blocked");
  f.proof.native.forEach(row=>row.answer.evidence_story_gid=null);
  assert.equal(decideDispatchRecovery(f.snapshot,f.proof,f.scan,f.now).action,"acknowledge");
  f.proof.native.forEach(row=>row.answer.summary="Ergebnisstory 222 und Korrekturstory 223.");
  f.proof.asana.forEach(row=>row.stories.push({...row.stories[0],gid:"223"}));
  assert.equal(decideDispatchRecovery(f.snapshot,f.proof,f.scan,f.now).action,"quarantine");
});
