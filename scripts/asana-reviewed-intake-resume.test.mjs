import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {processScanBinding} from './asana-worker-process-readback.mjs';
import {REVIEWED_INTAKES,intakeNativeObservation,decideReviewedIntake,applyReviewedIntake} from './asana-reviewed-intake-resume.mjs';
const fixtureData=t=>JSON.parse(fs.readFileSync(new URL('./fixtures/'+t.key+'-reviewed-intake-20261027.json',import.meta.url)));
function fixture(ctx,t=REVIEWED_INTAKES[0]) {
 const now=Date.now(),store=new AsanaDispatchStore(':memory:');ctx.after(()=>store.close());
 store.enqueue({id:'signal',agent_id:t.agent_id,task_gid:t.task_gid,source_version:'v1',kind:'due_task',priority:60,available_at_ms:t.run_started_at_ms-1});
 const claim=store.claimNext(t.run_id,{now:t.run_started_at_ms});
 store.recordRun(claim,{threadId:t.thread_id,state:'needs_reconciliation',now:t.run_started_at_ms});
 store.db.prepare('UPDATE runs SET turn_id=? WHERE run_id=?').run(t.turn_id,t.run_id);
 store.releaseLease(store.leasesForRun(t.run_id).find(l=>l.key.startsWith('agent:')));
 const snapshot=dispatchRecoverySnapshot(store,t.run_id);
 const data=fixtureData(t),native=[intakeNativeObservation(data,now-45000),intakeNativeObservation(data,now-5000)];
 const scan={...processScanBinding(snapshot.run),observed_at_ms:now,scan_succeeded:true,raw_sha256:'a'.repeat(64),matching_processes:[]};
 const stories=JSON.parse(fs.readFileSync(new URL('./fixtures/'+t.key+'-reviewed-stories-20261027.json',import.meta.url))).data;
 const asana={agent_id:'vip-ai-operations',task:{gid:t.task_gid,assignee:{gid:t.assignee},completed:false,modified_at:t.modified_at},stories,next_page:null};
 const remote={agent_id:'vip-ai-operations',include_all:true,active_locks:[]};
 const proof={native,asana:[{...asana,observed_at_ms:now-42000},{...asana,observed_at_ms:now-2000}],
  processes:[{...scan,observed_at_ms:now-44000},{...scan,observed_at_ms:now-4000}],
  remote:[{...remote,observed_at_ms:now-41000},{...remote,observed_at_ms:now-1000}]};
 const operator={run_id:'operator',thread_id:'12345678-1234-1234-1234-123456789012',task_gid:'1218924687084315'};
 const context={threadId:operator.thread_id,source:'CODEX_THREAD_ID'};
 store.acquireManualAgent('vip-ai-operations','operator',context,now);store.acquireManualTask('operator',operator.task_gid,context,now);
 return {store,snapshot,proof,operator,now,scan,t,options:{operator,scan:()=>scan,now}};
}
for(const t of REVIEWED_INTAKES)test(t.key+': exact historical fixture resumes only its signal, preserves attempts and owner work',ctx=>{
 const f=fixture(ctx,t);assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,f.options).action,'retry_after');
 assert.deepEqual(dispatchRecoverySnapshot(f.store,t.run_id),f.snapshot);
 const result=applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true});
 assert.equal(result.task_completed,false);assert.equal(result.owner_fresh_readback_required,true);
 const s=f.store.db.prepare("SELECT * FROM signals WHERE id='signal'").get();
 assert.equal(s.status,'retry_after');assert.equal(s.attempts,f.snapshot.signals[0].attempts);
 assert.equal(s.available_at_ms,f.now+900000);assert.equal(s.run_id,null);
 assert.equal(f.store.leasesForRun(t.run_id).length,0);assert.equal(f.store.leasesForRun('operator').length,2);
 assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM recovery_events').get().n,1);
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true}).reason,'recovery_fence_changed');
});
test('changed command, tool arguments, local write, final answer and omitted activity fail even with a recomputed hash',ctx=>{
 const f=fixture(ctx);
 for(const alter of [
  d=>d.turns[0].items.find(i=>i.type==='commandExecution').command+='; touch /tmp/x',
  d=>d.turns[0].items.push({type:'mcpToolCall',tool:'asana_comment',arguments:{text:'done'}}),
  d=>d.turns[0].items.push({type:'fileChange',path:'business.json'}),
  d=>d.turns[0].items.push({type:'agentMessage',phase:'final_answer',text:'done'}),
  d=>d.turns[0].items.pop(),
  d=>d.turns[0].items.find(i=>i.type==='commandExecution').cwd='/other'
 ]) {
  const d=fixtureData(f.t);alter(d);const p=structuredClone(f.proof);
  p.native=p.native.map(n=>intakeNativeObservation(d,n.observed_at_ms));
  assert.equal(decideReviewedIntake(f.snapshot,p,f.scan,f.now).action,'hold');
 }
 const d=fixtureData(f.t);d.page.hasMore=true;assert.throws(()=>intakeNativeObservation(d),/Complete/);
 d.page.hasMore=false;d.turns.push(d.turns[0]);assert.throws(()=>intakeNativeObservation(d),/Complete/);
});
test('stale, unstable, foreign, changed Asana story, remote lock or live process evidence holds',ctx=>{
 const f=fixture(ctx);
 for(const alter of [
  p=>p.native[1].turn_status='inProgress',p=>p.native[1].error.message='other',
  p=>p.native[0].observed_at_ms=f.now-300001,p=>p.native[1].observed_at_ms=f.now+1,
  p=>p.native[1].observed_at_ms=p.native[0].observed_at_ms+100,
  p=>p.native[1].host_id='remote',p=>p.native[1].runtime_status='active',
  p=>p.asana[1].task.completed=true,p=>p.asana[1].task.assignee.gid='999',
  p=>p.asana[1].agent_id='vip-ai-finance',p=>p.asana[1].next_page={offset:'more'},
  p=>{p.asana[0].stories[0].text='Changed';p.asana[1].stories[0].text='Changed';},
  p=>p.remote[1].active_locks=[{key:'task:any'}],p=>p.remote[1].include_all=false,
  p=>p.processes[1].scanner_sha256='b'.repeat(64),p=>p.processes[0].matching_processes=[{pid:1}],
  p=>p.processes=[]
 ]) {
  const p=structuredClone(f.proof);alter(p);assert.equal(decideReviewedIntake(f.snapshot,p,f.scan,f.now).action,'hold');
 }
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,operator:{...f.operator,task_gid:'999'}}).action,'hold');
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,scan:()=>({...f.scan,matching_processes:[{pid:1}]})}).action,'hold');
});
test('unknown run, extra lease, signal change and historical fence change are rejected',ctx=>{
 const f=fixture(ctx);
 for(const alter of [s=>s.run.run_id='unknown',s=>s.run.started_at_ms++,s=>s.leases.push({...s.leases[0],key:'task:other'}),
  s=>s.signals[0].lease_fence++,s=>s.signals[0].status='acknowledged']) {
  const s=structuredClone(f.snapshot);alter(s);assert.equal(decideReviewedIntake(s,f.proof,f.scan,f.now).action,'hold');
 }
 f.store.db.prepare('UPDATE lease_fences SET last_fence=last_fence+1 WHERE key=?').run(f.snapshot.leases[0].key);
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,f.options).reason,'lease_fence_history_changed');
});
test('audit failure rolls back all effects atomically',ctx=>{
 const f=fixture(ctx);f.store.db.exec("CREATE TRIGGER abort_audit BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'audit refused'); END");
 assert.throws(()=>applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true}),/audit refused/);
 assert.deepEqual(dispatchRecoverySnapshot(f.store,f.t.run_id),f.snapshot);
});
test('evaluation clock follows process observation',ctx=>{
 const f=fixture(ctx);let clock=f.now;ctx.mock.method(Date,'now',()=>clock);const {now,...options}=f.options;
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,{...options,scan:()=>{clock+=40;return {...f.scan,observed_at_ms:clock};}}).action,'retry_after');
});
