import test from 'node:test';
import assert from 'node:assert/strict';
import {FOLLOWUP_INCIDENT as T,decideCompletedFollowup,followupNativeObservation} from './asana-completed-followup-recovery.mjs';
import {processScanBinding} from './asana-worker-process-readback.mjs';
function fixture(){
  const now=Date.now(),snapshot={run:{run_id:T.run_id,agent_id:T.agent_id,thread_id:T.thread_id,
    task_gid:T.source_task,state:'completed',turn_id:null},signals:[],leases:[{key:`task:${T.followup_task}`,run_id:T.run_id}]};
  const native={source:'codex_app.read_thread',thread_id:T.thread_id,host_id:'local',runtime_status:'notLoaded',
    turn_id:T.turn_id,turn_status:'completed',started_at:1792501918,completed_at:1792502968,
    activity_count:140,activity_sha256:T.activity_sha256};
  const asana={agent_id:'vip-ai-operations',source:{gid:T.source_task,completed:true,completed_at:T.completed_at,
    modified_at:T.source_modified,assignee:{gid:T.assignee}},followup:{gid:T.followup_task,completed:false,completed_at:null,
    modified_at:T.followup_modified,assignee:{gid:T.assignee},due_at:T.due_at},
    stories:[{gid:'1219726059370192',resource_subtype:'task_due_time_approaching_story',created_by:null,created_at:T.followup_modified}],next_page:null};
  const proof={native:[{...native,observed_at_ms:now-3000},{...native,observed_at_ms:now-1000}],
    asana:[{...structuredClone(asana),observed_at_ms:now-3000},{...structuredClone(asana),observed_at_ms:now-1000}]};
  const scan={...processScanBinding(snapshot.run),observed_at_ms:now,scan_succeeded:true,raw_sha256:'a'.repeat(64),matching_processes:[]};
  return {snapshot,proof,scan,now};
}
test('only reviewed completed run with its one extra lease is eligible',()=>{
 const f=fixture();assert.deepEqual(decideCompletedFollowup(f.snapshot,f.proof,f.scan,f.now),
  {action:'release_followup_only',task_completed:false,signals_unchanged:true,old_work_preserved:true});
});
test('other incidents, unsettled signals, primary lease and extra leases stay held',()=>{
 for(const change of [s=>s.run.run_id='other',s=>s.run.state='needs_reconciliation',s=>s.run.thread_id='other',
  s=>s.run.task_gid=T.followup_task,s=>s.signals.push({status:'leased'}),s=>s.leases.push({key:'agent:vip-ai-finance'}),
  s=>s.leases[0].key=`task:${T.source_task}`,s=>s.leases[0].run_id='new-owner']){
  const f=fixture();change(f.snapshot);assert.equal(decideCompletedFollowup(f.snapshot,f.proof,f.scan,f.now).action,'hold');
 }
});
test('active, failed, partial, changed and stale native proofs stay held',()=>{
 for(const change of [r=>r.runtime_status='running',r=>r.turn_status='failed',r=>r.host_id='remote',
  r=>r.turn_id='other',r=>r.activity_count=139,r=>r.activity_sha256='b'.repeat(64),
  r=>r.completed_at=null,r=>r.observed_at_ms-=600000,r=>r.observed_at_ms+=600000]){
  const f=fixture();change(f.proof.native[1]);assert.equal(decideCompletedFollowup(f.snapshot,f.proof,f.scan,f.now).action,'hold');
 }
});
test('changed recurrence, reassignment, completed followup and unread stories stay held',()=>{
 for(const change of [r=>r.followup.completed=true,r=>r.followup.due_at='2026-10-28T10:35:00.000Z',
  r=>r.followup.assignee={gid:'other'},r=>r.followup.modified_at=new Date().toISOString(),
  r=>r.source.completed=false,r=>r.source.modified_at=new Date().toISOString(),r=>r.agent_id='vip-ai-finance',
  r=>r.next_page={offset:'more'},r=>r.stories.push({gid:'new-story'}),r=>r.observed_at_ms-=600000]){
  const f=fixture();f.proof=structuredClone(f.proof);change(f.proof.asana[1]);
  assert.equal(decideCompletedFollowup(f.snapshot,f.proof,f.scan,f.now).action,'hold');
 }
});
test('failed, stale, foreign and nonempty process scan stays held',()=>{
 for(const change of [s=>s.scan_succeeded=false,s=>s.matching_processes=[{pid:42}],s=>s.thread_id='other',
  s=>s.scan_policy='caller-defined',s=>s.observed_at_ms-=600000,s=>s.raw_sha256=null]){
  const f=fixture();change(f.scan);assert.equal(decideCompletedFollowup(f.snapshot,f.proof,f.scan,f.now).action,'hold');
 }
});
test('native observation rejects missing pages and other turns; commands affect digest',()=>{
 const data={thread:{id:T.thread_id,hostId:'local',status:{type:'notLoaded'}},
  page:{hasMore:false,nextCursor:null},turns:[{id:T.turn_id,status:'completed',startedAt:1792501918,completedAt:1792502968,
   items:[{type:'commandExecution',id:'one',command:'read',status:'completed',output:{text:'large output'}}]}]};
 const first=followupNativeObservation(data,1000);data.turns[0].items[0].output.text='bounded output';
 assert.equal(followupNativeObservation(data,1000).activity_sha256,first.activity_sha256);
 data.turns[0].items[0].command='write';assert.notEqual(followupNativeObservation(data,1000).activity_sha256,first.activity_sha256);
 data.page.hasMore=true;assert.throws(()=>followupNativeObservation(data),/Complete single-turn/);
 data.page.hasMore=false;data.turns.push(data.turns[0]);assert.throws(()=>followupNativeObservation(data),/Complete single-turn/);
});
