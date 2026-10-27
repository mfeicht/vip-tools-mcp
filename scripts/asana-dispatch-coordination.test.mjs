import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { AsanaDispatchStore } from './asana-dispatch-store.mjs';
import { taskSignal } from './asana-dispatch-signals.mjs';
import { coordinationPrestartGate } from './asana-dispatch-watch.mjs';
import { needsMaterialCommentStorePreflight } from './asana-dispatch-coordination.mjs';
import { workOnce } from './asana-dispatch-worker.mjs';

const due = (id, task='123', agent='vip-ai-test') => ({id,task_gid:task,agent_id:agent,
  source_version:id,kind:'due_task',priority:60,requires_coordination:true});
const reply = (data) => ({content:[{type:'text',text:JSON.stringify(data)}]});

test('routine classification reaches queue and refreshes old metadata without replay',()=>{
  const store=new AsanaDispatchStore(':memory:');
  try {
    const task={gid:'123',name:'R: Routine',due_on:'2020-01-01'};
    const signal=taskSignal('vip-ai-test',task);
    store.enqueue({...signal,requires_coordination:undefined},1);
    assert.equal(store.coordinationBlockedCount(2),1);
    assert.equal(store.enqueue({...signal,requires_coordination:false},2),false);
    assert.equal(store.coordinationBlockedCount(2),0);
    assert.equal(store.db.prepare('SELECT attempts,status FROM signals').get().status,'pending');
    assert.equal(signal.requires_coordination,true);
  } finally {store.close();}
});

test('store outage guards due siblings but allows other tasks and real human signals',async()=>{
  const store=new AsanaDispatchStore(':memory:');
  try {
    store.enqueue(due('routine'),1);
    store.enqueue({...due('continuation'),kind:'continuation',requires_coordination:undefined},1);
    store.enqueue({...due('deferred'),kind:'deferred_comment',story_gid:'910'},1);
    store.enqueue({...due('ordinary','456','vip-ai-other'),requires_coordination:false},1);
    let calls=0;
    const gate=await coordinationPrestartGate(store,async()=>{calls++;return {ready:false};});
    assert.equal(calls,1);
    assert.equal(gate.ready,false);
    assert.deepEqual(store.readyAgentIds(2,2,false),['vip-ai-other']);
    assert.equal(store.claimNext('denied',{now:2,agentId:'vip-ai-test',coordinationReady:false}),null);
    assert.deepEqual(store.activeLeases(),[]);
    store.enqueue({...due('human'),kind:'human_comment',story_gid:'911',priority:85},1);
    assert.equal(store.claimNext('allowed',{now:2,agentId:'vip-ai-test',coordinationReady:false}).signals.length,4);
  } finally {store.close();}
});

test('watcher skips health when no guarded work and fails closed on exception',async()=>{
  const store=new AsanaDispatchStore(':memory:');
  try {
    const noCall=()=>{throw new Error('must not call');};
    assert.deepEqual(await coordinationPrestartGate(store,noCall),{ready:true,checked:false,blocked_signals:0});
    store.enqueue(due('routine'),1);
    assert.equal((await coordinationPrestartGate(store,noCall)).ready,false);
    assert.deepEqual(store.counts(),{pending:1});
    assert.equal((await coordinationPrestartGate(store,async()=>({ready:true}))).blocked_signals,0);
  } finally {store.close();}
});

test('five-minute unlaunched deferral consumes no attempts, releases leases and forbids launched runs',()=>{
  const store=new AsanaDispatchStore(':memory:');
  try {
    store.enqueue(due('routine'),1);
    const claim=store.claimNext('run-first',{now:2});
    store.settle(claim,{outcome:'retry_after',now:3,coordinationPreflight:true,maxAttempts:1});
    const row=store.db.prepare('SELECT * FROM signals').get();
    assert.equal(row.status,'retry_after');assert.equal(row.attempts,0);
    assert.equal(row.available_at_ms,300003);assert.deepEqual(store.activeLeases(),[]);
    const second=store.claimNext('run-second',{now:300003});
    assert.equal(store.beginCodexLaunch(second,300004),true);
    assert.throws(()=>store.settle(second,{outcome:'retry_after',coordinationPreflight:true}),/unlaunched/);
    assert.equal(store.activeLeases().length,2);
  } finally {store.close();}
});

test('worker rechecks live routine tags, ignores fake deferred stories, permits independent human action',()=>{
  const task={name:'Daily',tags:[{name:'Routine'}]};
  assert.equal(needsMaterialCommentStorePreflight([due('routine')],task),true);
  assert.equal(needsMaterialCommentStorePreflight([due('routine'),{kind:'deferred_comment',story_gid:'1'}],task),true);
  assert.equal(needsMaterialCommentStorePreflight([due('routine'),{kind:'human_comment',story_gid:'1'}],task),false);
  assert.equal(needsMaterialCommentStorePreflight([due('routine')],{name:'Ordinary'}),false);
});

test('worker outage end-to-end stops before launch with complete task/story preflight',async t=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'vip-worker-prestart-'));
  t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
  const db=path.join(dir,'store.sqlite');const setup=new AsanaDispatchStore(db);
  setup.enqueue(due('routine'));setup.close();let checks=0,closed=false;
  const result=await workOnce({db,resolveBinary:()=>'/does/not/launch',readHealth:async()=>{checks++;return {ready:false};},
    connect:async()=>({close:async()=>{closed=true;},callTool:async ({arguments:args})=>{
      assert.equal(args.agent_id,'vip-ai-test');
      if(args.path==='/users/me')return reply({response:{data:{gid:'101'}}});
      if(args.path==='/tasks/123/stories')return reply({response:{data:[],next_page:null}});
      assert.equal(args.path,'/tasks/123');
      return reply({response:{data:{gid:'123',name:'R: Test',completed:false,assignee:{gid:'101'},due_on:'2020-01-01'}}});
    }})});
  assert.equal(result.status,'coordination_retry_after');assert.equal(result.model_started,false);
  assert.equal(checks,1);assert.equal(closed,true);
  const read=new AsanaDispatchStore(db);
  try {assert.equal(read.db.prepare('SELECT attempts FROM signals').get().attempts,0);
    assert.deepEqual(read.activeLeases(),[]);assert.equal(read.db.prepare('SELECT thread_id FROM runs').get().thread_id,null);
  } finally {read.close();}
});

for(const scenario of ['ready','not-ready','503','suspended','challenge','oversize','invalid-json','cooldown']) {
  test(`public health ${scenario}: fixed GET, no retry, shared mutex/cooldown`,t=>{
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'vip-health-'));
    t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
    if(scenario==='cooldown')fs.writeFileSync(path.join(dir,'state.json'),JSON.stringify({cooldownUntil:new Date(Date.now()+60000).toISOString()}));
    const code=`import assert from 'node:assert/strict';
      import {readMaterialCommentStoreHealth} from ${JSON.stringify(new URL('./vip-mcp-call.mjs',import.meta.url).href)};
      let calls=0;
      const scenario=${JSON.stringify(scenario)};
      const result=await readMaterialCommentStoreHealth({request:async(url,options)=>{
        calls++;assert.equal(url,'https://vip-tools-mcp.onrender.com/material-comment-store/health');
        assert.equal(options.method,'GET');assert.equal(options.redirect,'error');assert.equal(options.headers,undefined);
        if(scenario==='503')return new Response('unavailable',{status:503});
        if(scenario==='suspended')return new Response('<title>Service Suspended</title> This service has been suspended by its owner.',{status:503});
        if(scenario==='challenge')return new Response('Just a moment',{status:403});
        if(scenario==='oversize')return new Response('x'.repeat(32769));
        if(scenario==='invalid-json')return new Response('not json');
        return new Response(JSON.stringify({ready:scenario==='ready'}));
      }});
      assert.equal(calls,scenario==='cooldown'?0:1);assert.equal(result.ready,scenario==='ready');
      if(scenario==='suspended')assert.equal(result.reason,'remote_service_suspended');`;
    const child=spawnSync(process.execPath,['--input-type=module','-'],{input:code,encoding:'utf8',timeout:10000,
      env:{...process.env,VIP_MCP_COORDINATION_DIR:dir}});
    assert.equal(child.status,0,child.stderr);
    assert.equal(fs.existsSync(path.join(dir,'connect.lock')),false);
    if(scenario==='challenge')assert.ok(Date.parse(JSON.parse(fs.readFileSync(path.join(dir,'state.json'))).cooldownUntil)>Date.now());
  });
}
