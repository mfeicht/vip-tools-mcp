import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {processScanBinding} from './asana-worker-process-readback.mjs';
import {TARGET,resumeTaskQuarantine,resumeContextForClaim,resumePrompt} from './asana-task-quarantine-resume.mjs';
const hash=x=>createHash('sha256').update(x).digest('hex');
const NOW=1792495200000;
function fixture(t){
 const dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'vip-resume-test-')));
 const store=new AsanaDispatchStore(':memory:');
 const started=1792051618707;
 const task=store.acquireLease('task:'+TARGET.task_gid,TARGET.run_id,60000,started);
 store.db.prepare('UPDATE leases SET fence=50 WHERE key=?').run(task.key);
 store.db.prepare('UPDATE lease_fences SET last_fence=50 WHERE key=?').run(task.key);
 store.acquireLease('agent:'+TARGET.agent_id,'protected-finance-fixture',7200000,NOW);
 store.db.prepare(`INSERT INTO signals(id,agent_id,task_gid,story_gid,source_version,kind,priority,available_at_ms,status,attempts,run_id,lease_token,lease_fence,created_at_ms,updated_at_ms,last_error)
 VALUES(?,?,?,NULL,'fixture','continuation',0,?,'leased',1,?,?,50,?,?,NULL)`).run('fixture-signal',TARGET.agent_id,TARGET.task_gid,started,TARGET.run_id,task.token,started,started);
 store.recordRun({run_id:TARGET.run_id,signals:[{agent_id:TARGET.agent_id,task_gid:TARGET.task_gid}]},{threadId:'fixture-old-thread',turnId:'fixture-old-turn',state:'needs_reconciliation',now:started});
 t.after(()=>{store.db.close();fs.rmSync(dir,{recursive:true,force:true});});
 store.db.prepare('DELETE FROM leases WHERE key IN (?,?)').run('agent:vip-ai-operations','task:1218693508638175');
 const operatorLeases=['agent:vip-ai-operations','task:1218693508638175'].map(key=>{store.acquireLease(key,'operations-fixture',7200000,NOW);return {...store.db.prepare('SELECT * FROM leases WHERE key=?').get(key)};});
 const run={...store.db.prepare('SELECT * FROM runs WHERE run_id=?').get(TARGET.run_id)};
 const scan={...processScanBinding(run),observed_at_ms:NOW-100,scan_succeeded:true,matching_processes:[],raw_sha256:'1'.repeat(64)};
 const artifacts=['dossier.json','handoff.md','run-state.json'].map((n,i)=>{const p=path.join(dir,n);fs.writeFileSync(p,'preserved partial work '+i);return {path:p,sha256:hash(fs.readFileSync(p))};});
 const evidence={schema:'asana-native-terminal-quarantine-v1',expected_run:run,operations_authorization:{run_id:'operations-fixture',thread_id:'fixture-thread',leases:operatorLeases},
  expected_leases:store.leasesForRun(run.run_id).map(x=>({...x})),
  expected_signals:store.db.prepare('SELECT * FROM signals WHERE run_id=? ORDER BY id').all(run.run_id).map(x=>({...x})),
  expected_current_agent_lease:{...store.db.prepare('SELECT * FROM leases WHERE key=?').get('agent:vip-ai-finance')},
  native_readbacks:[NOW-2000,NOW-100].map(time=>({observed_at_ms:time,source:'codex_app.read_thread',host_id:'local',thread_id:run.thread_id,runtime_status:'notLoaded',turn_status:'interrupted',turn_id:run.turn_id,started_at_ms:run.started_at_ms+1000,completed_at_ms:null,activity_digest:'2'.repeat(64),last_event_at_ms:run.started_at_ms+120000})),
  asana_readbacks:[NOW-2000,NOW-100].map(time=>({observed_at_ms:time,agent_id:'vip-ai-operations',task:{gid:TARGET.task_gid,completed:false,assignee:{gid:TARGET.assignee_gid},modified_at:'2026-10-15T05:41:05.235Z'},stories:[{gid:'prior_story',created_at:'2026-10-15T05:41:05.234Z'}],next_page:null})),
  process_readback:scan,side_effects:artifacts,
  resume_review:{reviewer:'vip-ai-operations',disposition:'preserve_partial_work_then_resume',task_must_remain_open:true,no_effect_claim:false,external_mutations_authorized:false,reviewed_at_ms:NOW-100,artifacts,next_step:'Read preserved dossier and verify remaining dated source evidence before posting a real progress checkpoint.'}};
 // Native/Asana inputs above are synthetic test fixtures, never live acceptance.
 return {store,evidence,dir,scan,opts:{now:NOW,processScanner:()=>scan}};
}
function snapshot(store){return JSON.stringify(store.db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all().map(({name})=>[name,store.db.prepare('SELECT * FROM "'+name+'" ORDER BY rowid').all()]));}
test('dry run changes no rows or tables',t=>{const f=fixture(t),before=snapshot(f.store);assert.equal(resumeTaskQuarantine(f.store,f.evidence,f.opts).disposition,'review_ready');assert.equal(snapshot(f.store),before);});
test('resume preserves active agent, unfinished run, artifacts and retry count',t=>{
 const f=fixture(t),old=f.evidence,result=resumeTaskQuarantine(f.store,old,{...f.opts,apply:true});
 assert.equal(result.disposition,'resume_authorized_partial_work');assert.equal(result.task_completed,false);assert.equal(result.old_run_completed,false);
 assert.deepEqual({...f.store.db.prepare('SELECT * FROM leases WHERE key=?').get('agent:vip-ai-finance')},old.expected_current_agent_lease);
 assert.equal(f.store.leasesForRun(TARGET.run_id).length,0);
 assert.equal(f.store.db.prepare('SELECT state FROM runs WHERE run_id=?').get(TARGET.run_id).state,'needs_reconciliation');
 for(const s of old.expected_signals){const actual=f.store.db.prepare('SELECT * FROM signals WHERE id=?').get(s.id);assert.equal(actual.status,'retry_after');assert.equal(actual.available_at_ms,NOW+3600000);assert.equal(actual.attempts,s.attempts);assert.equal(actual.run_id,null);assert.equal(actual.lease_token,null);assert.equal(actual.lease_fence,null);}
 assert.equal(f.store.db.prepare('SELECT last_fence FROM lease_fences WHERE key=?').get('task:'+TARGET.task_gid).last_fence,50);
 const context=resumeContextForClaim(f.store,{signals:old.expected_signals});assert.equal(context.artifacts.length,3);assert.match(resumePrompt(context),/unvollständige Vorarbeit/);assert.match(resumePrompt(context),/vor weiterer Recherche/);assert.equal(context.no_effect_claim,false);
 assert.equal(f.store.claimNext('competing',{now:NOW+3600001,agentId:TARGET.agent_id}),null);
});
const changes={
 'missing operations authority':f=>{delete f.evidence.operations_authorization;},
 'operations lease changed':f=>f.store.db.prepare('UPDATE leases SET fence=fence+1 WHERE key=?').run('agent:vip-ai-operations'),
 'stale native':f=>{f.evidence.native_readbacks[0].observed_at_ms-=600000;},
 'changing activity':f=>{f.evidence.native_readbacks[1].activity_digest='3'.repeat(64);},
 'active turn':f=>{f.evidence.native_readbacks[1].turn_status='inProgress';},
 'incomplete pagination':f=>{f.evidence.asana_readbacks[1].next_page={};},
 'invalid story time':f=>{for(const x of f.evidence.asana_readbacks)x.stories[0].created_at='invalid';},
 'new story':f=>{for(const x of f.evidence.asana_readbacks)x.stories.push({gid:'new',created_at:new Date(NOW-10000).toISOString()});},
 'completed task':f=>{for(const x of f.evidence.asana_readbacks)x.task.completed=true;},
 'wrong owner':f=>{for(const x of f.evidence.asana_readbacks)x.task.assignee.gid='other';},
 'changed artifact':f=>fs.appendFileSync(f.evidence.side_effects[0].path,'changed'),
 'no-effect claim':f=>{f.evidence.resume_review.no_effect_claim=true;},
 'mutation authorization':f=>{f.evidence.resume_review.external_mutations_authorized=true;},
 'foreign task':f=>{f.evidence.expected_run.task_gid='other';},
 'changed run':f=>f.store.db.prepare('UPDATE runs SET updated_at_ms=updated_at_ms+1 WHERE run_id=?').run(TARGET.run_id),
 'changed task fence':f=>f.store.db.prepare('UPDATE leases SET fence=fence+1 WHERE key=?').run('task:'+TARGET.task_gid),
 'changed signal fence':f=>f.store.db.prepare('UPDATE signals SET lease_fence=lease_fence+1 WHERE run_id=?').run(TARGET.run_id),
 'renewed active agent':f=>f.store.db.prepare('UPDATE leases SET expires_at_ms=expires_at_ms+1 WHERE key=?').run('agent:vip-ai-finance'),
 'changed fence counter':f=>f.store.db.prepare('UPDATE lease_fences SET last_fence=last_fence+1 WHERE key=?').run('task:'+TARGET.task_gid)
};
for(const [name,change] of Object.entries(changes))test('reject '+name+' without mutation',t=>{const f=fixture(t);change(f);const before=snapshot(f.store);assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true}));assert.equal(snapshot(f.store),before);});
test('worker appears at final scan',t=>{const f=fixture(t),before=snapshot(f.store);assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true,processScanner:()=>({...f.scan,matching_processes:[{pid:9,reason:'exact-run-or-thread'}]})}));assert.equal(snapshot(f.store),before);});
test('mid-transaction failure rolls back audit, signal and lease',t=>{const f=fixture(t);f.store.db.exec("CREATE TRIGGER abort_release BEFORE DELETE ON leases WHEN OLD.key='task:"+TARGET.task_gid+"' BEGIN SELECT RAISE(ABORT,'fixture interrupt'); END");const before=snapshot(f.store);assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true}),/fixture interrupt/);assert.equal(snapshot(f.store),before);assert.equal(f.store.db.prepare("SELECT name FROM sqlite_master WHERE name='task_quarantine_resumptions'").get(),undefined);});
test('repeat apply and changed resumption artifact rejected',t=>{const f=fixture(t);resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true});const before=snapshot(f.store);assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true}));assert.equal(snapshot(f.store),before);fs.appendFileSync(f.evidence.side_effects[2].path,'changed');assert.throws(()=>resumeContextForClaim(f.store,{signals:f.evidence.expected_signals}),/artifact changed/);});

test('replacement worker prompt carries accepted artifacts and current gates', async t=>{
 const f=fixture(t);resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true});
 const context=resumeContextForClaim(f.store,{signals:f.evidence.expected_signals});
 const source=fs.readFileSync(new URL('./asana-dispatch-worker.mjs',import.meta.url),'utf8');
 const {promptFor}=await import('./asana-dispatch-worker.mjs');
 const output=promptFor({run_id:'replacement',signals:f.evidence.expected_signals,resume_context:context},{gid:TARGET.task_gid,name:'fixture'},{tier:'fixture'});
 assert.match(output,/unvollständige Vorarbeit/);assert.match(output,/AGENTS.md/);assert.match(output,/Kein Subagent/);
 for(const artifact of context.artifacts){assert.ok(output.includes(artifact.path));assert.ok(output.includes(artifact.sha256));}
 const bind=source.indexOf('claim.resume_context = resumeContextForClaim(store, claim)');
 assert.ok(bind>0 && bind<source.indexOf('const route = await selectModel(agentId, before)'));
});

test('CLI rejects ambiguous arguments and defaults to dry run',async()=>{
 const {parseResumeArguments}=await import('./asana-task-quarantine-resume-cli.mjs');
 assert.equal(parseResumeArguments(['--evidence=/tmp/review.json','--operations-run=fixture']).apply,false);
 for(const args of [[],['--evidence=relative','--operations-run=x'],['--evidence=/tmp/x','--operations-run=x','--apply'],['--evidence=/tmp/x','--operations-run=x','--backup=/tmp/x'],['--evidence=/tmp/x','--operations-run=x','--db=/tmp/x'],['--evidence=/tmp/x','--evidence=/tmp/y','--operations-run=x']])assert.throws(()=>parseResumeArguments(args));
});

test('native item-sequence proof supports an unavailable event timestamp only in resume path',async t=>{
 const f=fixture(t);for(const row of f.evidence.native_readbacks)Object.assign(row,{last_event_at_ms:null,event_time_availability:'not_exposed_by_native_read_thread',activity_item_count:111,activity_last_item_id:'last-item'});
 const {validateQuarantineEvidence}=await import('./asana-terminal-quarantine.mjs');
 assert.throws(()=>validateQuarantineEvidence(f.evidence,NOW));
 assert.equal(resumeTaskQuarantine(f.store,f.evidence,f.opts).disposition,'review_ready');
 f.evidence.native_readbacks[1].activity_item_count=112;
 assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true}));
 f.evidence.native_readbacks[1].activity_item_count=111;
 delete f.evidence.native_readbacks[1].activity_last_item_id;
 assert.throws(()=>resumeTaskQuarantine(f.store,f.evidence,{...f.opts,apply:true}));
});
