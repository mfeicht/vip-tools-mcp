import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {AsanaDispatchStore} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {processScanBinding} from './asana-worker-process-readback.mjs';
import {startupReadsOnly,decideStartupReadResume,applyStartupReadResume,readStartupNative} from './asana-startup-read-resume.mjs';

const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const command=`/bin/zsh -lc "pwd && sed -n '1,260p' AGENTS.md && sed -n '1,260p' VIP-AI-Memory/10-Agenten/VIP-AI-Content/Start-hier.md"`;
function fixture(t){
 const now=Date.now(),store=new AsanaDispatchStore(':memory:');t.after(()=>store.close());
 store.enqueue({id:'signal',agent_id:'vip-ai-content',task_gid:'123',source_version:'v1',kind:'due_task',priority:60,available_at_ms:now-900000});
 const claim=store.claimNext('old',{now:now-900000});
 store.recordRun(claim,{threadId:'thread',state:'needs_reconciliation',now:now-800000});
 store.db.prepare("UPDATE runs SET thread_id='thread',turn_id='turn' WHERE run_id='old'").run();
 store.releaseLease(store.leasesForRun('old').find(l=>l.key.startsWith('agent:')));
 const snapshot=dispatchRecoverySnapshot(store,'old');
 const items=[{type:'commandExecution',id:'cmd',command,cwd:'/fixture',status:'completed',exitCode:0},
   {type:'agentMessage',id:'comment',phase:'commentary',text:'Starting'},
   {type:'reasoning',id:'reason'},{type:'userMessage',id:'user'}];
 const n={source:'codex_app.read_thread',host_id:'local',thread_id:'thread',turn_id:'turn',runtime_status:'notLoaded',
  turn_status:'failed',started_at_ms:now-900000,completed_at_ms:now-800000,activity_item_count:items.length,
  activity_last_item_id:'cmd',activity_digest:hash(items),items,answer:null,
  error:{message:'Selected model is at capacity. Please try a different model.',additionalDetails:null}};
 const scan={...processScanBinding(snapshot.run),observed_at_ms:now,scan_succeeded:true,raw_sha256:'a'.repeat(64),matching_processes:[]};
 const a={agent_id:'vip-ai-operations',task:{gid:'123',completed:false,modified_at:new Date(now-700000).toISOString(),assignee:{gid:'234'}},stories:[],next_page:null};
 const proof={native:[{...n,observed_at_ms:now-40000},{...n,observed_at_ms:now-5000}],
  processes:[{...scan,observed_at_ms:now-39000},{...scan,observed_at_ms:now-4000}],
  asana:[{...a,observed_at_ms:now-38000},{...a,observed_at_ms:now-3000}]};
 const operator={run_id:'operator',thread_id:'12345678-1234-1234-1234-123456789012',task_gid:'456'};
 const context={threadId:operator.thread_id,source:'CODEX_THREAD_ID'};
 store.acquireManualAgent('vip-ai-operations','operator',context,now);store.acquireManualTask('operator','456',context,now);
 return {store,snapshot,proof,scan,now,operator,options:{operator,scan:()=>scan,now,workspace:'/fixture'}};
}
test('dry run and apply preserve attempts, current operator and open task; repeated apply fails closed',t=>{
 const f=fixture(t);assert.equal(applyStartupReadResume(f.store,f.snapshot,f.proof,f.options).action,'retry_after');
 assert.deepEqual(dispatchRecoverySnapshot(f.store,'old'),f.snapshot);
 const result=applyStartupReadResume(f.store,f.snapshot,f.proof,{...f.options,apply:true});
 assert.equal(result.action,'retry_after');assert.equal(result.task_completed,false);
 const signal=f.store.db.prepare("SELECT * FROM signals WHERE id='signal'").get();
 assert.equal(signal.attempts,1);assert.equal(signal.available_at_ms,f.now+900000);assert.equal(signal.run_id,null);
 assert.equal(f.store.leasesForRun('old').length,0);assert.equal(f.store.leasesForRun('operator').length,2);
 assert.equal(applyStartupReadResume(f.store,f.snapshot,f.proof,{...f.options,apply:true}).reason,'recovery_fence_changed');
});
test('all other execution, shell syntax, paths and uncertain activity are rejected even with a recomputed digest',t=>{
 const f=fixture(t);
 for(const alter of [r=>r.items.push({id:'write',type:'fileChange'}),r=>r.items.push({id:'mcp',type:'mcpToolCall'}),
   r=>r.items[0].command+='; touch /tmp/x',r=>r.items[0].command=command.replace('pwd','$(touch /tmp/x)'),
   r=>r.items[0].command=command.replace('AGENTS.md','/tmp/AGENTS.md'),r=>r.items[0].cwd='/foreign',
   r=>r.items[0].status='running',r=>r.items[0].exitCode=1,r=>r.items[1].phase='final_answer',
   r=>r.error.message='Other failure',r=>r.items.push({...r.items[0],id:'second'}),r=>r.items.pop()]){
   const n=structuredClone(f.proof.native[0]);alter(n);n.activity_item_count=n.items.length;n.activity_digest=hash(n.items);
   assert.equal(startupReadsOnly(n,'/fixture'),false);
 }
 const n=structuredClone(f.proof.native[0]);n.activity_digest='b'.repeat(64);assert.equal(startupReadsOnly(n,'/fixture'),false);
});
test('native, Asana, process, identity and claim gates remain strict',t=>{
 const f=fixture(t);
 for(const alter of [p=>p.native[1].turn_id='foreign',p=>p.native[1].completed_at_ms=null,p=>p.native[1].runtime_status='active',
  p=>p.native[1].observed_at_ms=f.now+1,p=>p.native[0].observed_at_ms=f.now-1000000,p=>p.native[1].activity_item_count++,
  p=>p.asana[1].agent_id='vip-ai-content',p=>p.asana[1].next_page='more',p=>p.asana[1].task.completed=true,
  p=>p.asana[1].task.modified_at='invalid',p=>p.processes[1].matching_processes.push({pid:1}),
  p=>p.processes[1].scanner_sha256='b'.repeat(64),p=>p.processes=[]]){
  const p=structuredClone(f.proof);alter(p);assert.equal(decideStartupReadResume(f.snapshot,p,f.scan,f.now,'/fixture').action,'hold');
 }
 for(const alter of [s=>s.leases.push({...s.leases[0],key:'task:789'}),s=>s.signals[0].lease_fence++,
  s=>s.run.thread_id=null,s=>s.signals[0].status='acknowledged']){
  const s=structuredClone(f.snapshot);alter(s);assert.equal(decideStartupReadResume(s,f.proof,f.scan,f.now,'/fixture').action,'hold');
 }
 assert.equal(applyStartupReadResume(f.store,f.snapshot,f.proof,{...f.options,operator:{...f.operator,thread_id:'other'},apply:true}).action,'hold');
 assert.equal(applyStartupReadResume(f.store,f.snapshot,f.proof,{...f.options,scan:()=>({...f.scan,matching_processes:[{pid:1}]}),apply:true}).action,'hold');
});
test('audit failure rolls back every signal, lease and attempt change',t=>{
 const f=fixture(t);f.store.db.exec("CREATE TRIGGER abort_audit BEFORE INSERT ON recovery_events BEGIN SELECT RAISE(ABORT,'audit refused'); END");
 assert.throws(()=>applyStartupReadResume(f.store,f.snapshot,f.proof,{...f.options,apply:true}),/audit refused/);
 assert.deepEqual(dispatchRecoverySnapshot(f.store,'old'),f.snapshot);
});
test('native collector binds complete pagination to digest and original error',async t=>{
 const f=fixture(t),items=f.proof.native[0].items;const turn={id:'turn',status:'failed',startedAt:f.now-900000,completedAt:f.now-800000,error:f.proof.native[0].error};
 const rpc={call:async(method,params)=>{
  if(method==='thread/read')return {thread:{id:'thread',status:{type:'notLoaded'}}};
  if(method==='thread/turns/list')return {data:[turn],nextCursor:null};
  return params.cursor?{data:items.slice(2).map(item=>({item})),nextCursor:null}:{data:items.slice(0,2).map(item=>({item})),nextCursor:'next'};
 }};
 const read=await readStartupNative(f.snapshot.run,rpc);assert.deepEqual(read.items,items);assert.equal(read.activity_digest,hash(items));
 assert.equal(startupReadsOnly(read,'/fixture'),true);
});
