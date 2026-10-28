import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {verifyPartialArtifacts,verifyReviewedPartialContext,reviewedPartialContext} from './asana-reviewed-partial-context.mjs';
import {resumeContextForClaim,resumePrompt} from './asana-task-quarantine-resume.mjs';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {processScanBinding} from './asana-worker-process-readback.mjs';
import {REVIEWED_INTAKES,intakeNativeObservation,decideReviewedIntake,applyReviewedIntake} from './asana-reviewed-intake-resume.mjs';
// Full historical business payloads for the later incidents stay in local audit storage.
const FIXTURE_CASES=REVIEWED_INTAKES.filter(t=>['regime','sales','crossdesk','sector'].includes(t.key));
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
 return {store,snapshot,proof,operator,now,scan,t,options:{operator,scan:()=>scan,now,verifyArtifacts:()=>{}}};
}
for(const t of FIXTURE_CASES)test(t.key+': exact historical fixture resumes only its signal, preserves attempts and owner work',ctx=>{
 const f=fixture(ctx,t);assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,f.options).action,'retry_after');
 assert.deepEqual(dispatchRecoverySnapshot(f.store,t.run_id),f.snapshot);
 const result=applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true});
 assert.equal(result.task_completed,false);assert.equal(result.owner_fresh_readback_required,true);
 const s=f.store.db.prepare("SELECT * FROM signals WHERE id='signal'").get();
 assert.equal(s.status,'retry_after');assert.equal(s.attempts,f.snapshot.signals[0].attempts);
 assert.equal(s.available_at_ms,f.now+900000);assert.equal(s.run_id,null);
 assert.equal(f.store.leasesForRun(t.run_id).length,0);assert.equal(f.store.leasesForRun('operator').length,2);
 assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM recovery_events').get().n,1);
 if(t.partial){
  assert.equal(result.old_run_completed,false);assert.equal(result.no_effect_claim,false);
  assert.equal(f.store.db.prepare('SELECT state FROM runs WHERE run_id=?').get(t.run_id).state,'needs_reconciliation');
  let checked=false;
  const context=resumeContextForClaim(f.store,{signals:f.snapshot.signals},{verifyPartial:(a,o)=>{assert.deepEqual(a,t.artifacts);assert.equal(o.includeOriginals,false);checked=true;}});
  assert.equal(checked,true);assert.match(resumePrompt(context),/Kein No-Effect-/);
  assert.equal(context.artifacts.length,t.artifacts.length);
 }
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

test('unrelated remote work may progress while incident-bound coordination still holds',ctx=>{
 const f=fixture(ctx,REVIEWED_INTAKES[1]);
 const foreign={key:'task:999',scope:'task',task_gid:'999',agent_id:'vip-ai-finance',run_id:'foreign',seconds_remaining:90};
 f.proof.remote[0].active_locks=[foreign];f.proof.remote[1].active_locks=[{...foreign,seconds_remaining:50}];
 assert.equal(decideReviewedIntake(f.snapshot,f.proof,f.scan,f.now).action,'retry_after');
 for(const lock of [
  {...foreign,run_id:f.t.run_id},
  {...foreign,key:'task:'+f.t.task_gid,task_gid:f.t.task_gid},
  {...foreign,key:'resource:'+f.t.resource_key,scope:'resource',task_gid:null,resource_key:f.t.resource_key},
  {...foreign,scope:'unknown'},
  {...foreign,key:'task:inconsistent'},
  {...foreign,key:'resource:x',scope:'resource',resource_key:'y'}
 ]){
  const p=structuredClone(f.proof);p.remote[1].active_locks=[lock];
  assert.equal(decideReviewedIntake(f.snapshot,p,f.scan,f.now).action,'hold');
 }
});

for(const target of FIXTURE_CASES.filter(t=>t.partial)){
 test(target.key+': changed evidence receipt, signal binding or context blocks replacement worker',ctx=>{
  const f=fixture(ctx,target);applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true});
  const original={...f.store.db.prepare('SELECT * FROM task_quarantine_resumptions').get()};
  const accepted=f.store.db.prepare('SELECT evidence_json FROM recovery_events').get().evidence_json;
  const claim={signals:f.snapshot.signals},opts={verifyPartial:()=>{}};
  for(const mutate of [c=>c.artifacts.pop(),c=>c.artifacts[0].path='/tmp/other',c=>c.artifacts[0].sha256='b'.repeat(64),
   c=>c.next_step='changed',c=>c.agent_id='invalid-agent',c=>c.no_effect_claim=true,c=>c.old_run_id='unknown']){
   const c=JSON.parse(original.context_json);mutate(c);
   f.store.db.prepare('UPDATE task_quarantine_resumptions SET context_json=?').run(JSON.stringify(c));
   assert.throws(()=>resumeContextForClaim(f.store,claim,opts));
  }
  f.store.db.prepare('UPDATE task_quarantine_resumptions SET context_json=?,evidence_sha256=?').run(original.context_json,'c'.repeat(64));
  assert.throws(()=>resumeContextForClaim(f.store,claim,opts));
  f.store.db.prepare('UPDATE task_quarantine_resumptions SET evidence_sha256=?').run(original.evidence_sha256);
  f.store.db.prepare('UPDATE recovery_events SET evidence_json=?').run(JSON.stringify({...JSON.parse(accepted),proof:{changed:true}}));
  assert.throws(()=>resumeContextForClaim(f.store,claim,opts),/receipt changed/);
  f.store.db.prepare('UPDATE recovery_events SET evidence_json=?').run(accepted);
  f.store.db.prepare('UPDATE task_quarantine_resumptions SET signal_ids_json=?').run(JSON.stringify(['signal','unreviewed']));
  assert.throws(()=>resumeContextForClaim(f.store,claim,opts),/receipt changed/);
 });
 test(target.key+': missing files and audit failure preserve claim and omit resume receipt',ctx=>{
  const f=fixture(ctx,target);
  assert.throws(()=>applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true,verifyArtifacts:()=>{throw new Error('missing artifact');}}),/missing artifact/);
  assert.deepEqual(dispatchRecoverySnapshot(f.store,target.run_id),f.snapshot);
  f.store.db.exec("CREATE TRIGGER abort_audit BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'audit refused'); END");
  assert.throws(()=>applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true}),/audit refused/);
  assert.deepEqual(dispatchRecoverySnapshot(f.store,target.run_id),f.snapshot);
  assert.equal(f.store.db.prepare("SELECT name FROM sqlite_master WHERE name='task_quarantine_resumptions'").get(),undefined);
 });
}
test('artifact bytes, original provenance, size and symlinks are checked independently',ctx=>{
 const dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'vip-partial-')));ctx.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 const a={original_path:path.join(dir,'original'),path:path.join(dir,'preserved'),bytes:4,sha256:createHash('sha256').update('data').digest('hex')};
 for(const p of [a.original_path,a.path])fs.writeFileSync(p,'data');
 verifyPartialArtifacts([a]);fs.writeFileSync(a.original_path,'fail');assert.throws(()=>verifyPartialArtifacts([a]));
 verifyPartialArtifacts([a],{includeOriginals:false});fs.writeFileSync(a.original_path,'data');
 fs.writeFileSync(a.path,'fail');assert.throws(()=>verifyPartialArtifacts([a]));
 fs.unlinkSync(a.path);fs.symlinkSync(a.original_path,a.path);assert.throws(()=>verifyPartialArtifacts([a]));
 fs.unlinkSync(a.path);assert.throws(()=>verifyPartialArtifacts([a]));
});
test('reviewed registry rejects altered context even when receipt is altered consistently',()=>{
 const t=REVIEWED_INTAKES.find(t=>t.partial),record={old_run_id:t.run_id,task_gid:t.task_gid,agent_id:t.agent_id,evidence_sha256:'d'.repeat(64)};
 const c=reviewedPartialContext(t,record.evidence_sha256);verifyReviewedPartialContext(c,record,{verify:()=>{}});
 for(const edit of [x=>x.next_step='override',x=>x.artifacts=[],x=>x.disposition='completed',x=>x.schema='unknown']){
  const altered=structuredClone(c);edit(altered);assert.throws(()=>verifyReviewedPartialContext(altered,record,{verify:()=>{}}));
 }
});

test('interrupted display cannot release an unreviewed claim',ctx=>{
 const f=fixture(ctx);for(const n of f.proof.native){n.turn_status='interrupted';n.completed_at=null;n.error=null;}
 assert.equal(decideReviewedIntake(f.snapshot,f.proof,f.scan,f.now).action,'hold');
});
for(const t of REVIEWED_INTAKES.filter(t=>['watcher','content','sales_umzug','weekly_health'].includes(t.key)))test(t.key+': only the pinned checkpoint context reaches its owner',()=>{
 const record={old_run_id:t.run_id,task_gid:t.task_gid,agent_id:t.agent_id,evidence_sha256:'e'.repeat(64)};
 const context=reviewedPartialContext(t,record.evidence_sha256);let count=0;
 verifyReviewedPartialContext(context,record,{verify:(a,o)=>{assert.deepEqual(a,t.artifacts);assert.equal(o.includeOriginals,false);count++;}});
 assert.equal(count,1);assert.equal(context.task_completed,false);assert.equal(context.no_effect_claim,false);
 for(const change of [c=>c.next_step='unreviewed',c=>c.artifacts=[],c=>c.agent_id='wrong-owner',c=>c.old_run_id='another-run']){
  const c=structuredClone(context);change(c);assert.throws(()=>verifyReviewedPartialContext(c,record,{verify:()=>{}}));
 }
});

for(const change of ['missing-binding','changed-binding','resolved-binding','task-history','agent-history','expired-task','foreign-thread'])test('operator authorization rejects '+change,ctx=>{
 const f=fixture(ctx);
 if(change==='missing-binding')f.store.db.prepare('DELETE FROM manual_run_tasks WHERE run_id=?').run('operator');
 if(change==='changed-binding')f.store.db.prepare('UPDATE manual_run_tasks SET lease_fence=lease_fence+1 WHERE run_id=?').run('operator');
 if(change==='resolved-binding')f.store.db.prepare('UPDATE manual_run_tasks SET resolved_at_ms=? WHERE run_id=?').run(f.now,'operator');
 if(change==='task-history')f.store.db.prepare('UPDATE lease_fences SET last_fence=last_fence+1 WHERE key=?').run('task:'+f.operator.task_gid);
 if(change==='agent-history')f.store.db.prepare('UPDATE lease_fences SET last_fence=last_fence+1 WHERE key=?').run('agent:vip-ai-operations');
 if(change==='expired-task')f.store.db.prepare('UPDATE leases SET expires_at_ms=? WHERE key=?').run(f.now,'task:'+f.operator.task_gid);
 if(change==='foreign-thread')f.options.operator={...f.operator,thread_id:'other'};
 assert.equal(applyReviewedIntake(f.store,f.snapshot,f.proof,{...f.options,apply:true}).reason,'current_operations_lease_required');
 assert.deepEqual(dispatchRecoverySnapshot(f.store,f.t.run_id),f.snapshot);
 assert.equal(f.store.db.prepare('SELECT count(*) n FROM recovery_events').get().n,0);
});
