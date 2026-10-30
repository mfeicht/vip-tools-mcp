import test from 'node:test';
import assert from 'node:assert/strict';
import {readAgentSnapshots,tool} from './asana-dispatch-poller.mjs';
const ids=Array.from({length:19},(_,i)=>'vip-ai-'+String.fromCharCode(97+i));
function fixture({elapsed=20_000,edit=()=>{}}={}){
 let time=0,active=0,maxActive=0;
 const calls=[];
 const client={async callTool(request,unused,options){
  active++;maxActive=Math.max(active,maxActive);calls.push({request,options});
  await Promise.resolve();time+=elapsed;
  const data={agents:request.arguments.agent_ids.map((id,i)=>({agent_id:id,ok:true,truncated:false,user_gid:String(100+ids.indexOf(id)),workspace_gid:'999',tasks:[]}))};
  edit(data,calls.length);active--;
  return {content:[{type:'text',text:JSON.stringify(data)}]};
 }};
 return {client,calls,clock:()=>time,maxActive:()=>maxActive,setTime:v=>{time=v}};
}
test('19-agent read completes sequentially beyond one-call ceiling, with full bounded identity coverage',async()=>{
 const before=Date.now();
 const f=fixture(),r=await readAgentSnapshots(f.client,ids,{clock:f.clock});
 assert.deepEqual(r.agents.map(a=>a.agent_id),ids);assert.equal(r.agents_succeeded,19);
 assert.deepEqual(f.calls.map(c=>c.request.arguments.agent_ids.length),[4,4,4,4,3]);
 assert.equal(f.maxActive(),1);assert.equal(f.clock(),100000);
 assert.ok(f.calls.every(c=>c.request.name==='asana_agents_open_task_snapshot'&&c.request.arguments.limit===100&&c.request.arguments.max_pages===10&&c.request.arguments.concurrency===4));
 assert.deepEqual(f.calls.map(c=>c.options.timeout),[45000,45000,45000,45000,40000]);
 assert.ok(f.calls.every(c=>c.options.timeout===c.options.maxTotalTimeout));
 const deadline=f.calls[0].options.deadlineAtMs;
 assert.ok(deadline>=before+120000&&deadline<=Date.now()+120000);
 assert.ok(f.calls.every(c=>c.options.deadlineAtMs===deadline));
});
test('deadline stops further calls even if the client returns at or after the bound',async()=>{
 for(const elapsed of [40000,41000]){
  const f=fixture({elapsed});await assert.rejects(readAgentSnapshots(f.client,ids,{clock:f.clock}),/total read budget timed out/);
  assert.equal(f.calls.length,3);assert.equal(f.calls[2].options.timeout,120000-2*elapsed);
 }
});
test('invalid, failed or truncated batch rejects the full read and does not fetch later agents',async()=>{
 for(const mutate of [d=>d.agents.pop(),d=>d.agents[0].ok=false,d=>d.agents[0].truncated=true,d=>d.agents[0].agent_id=ids[0],d=>d.agents[0].user_gid=null,d=>d.agents[0].tasks=null]){
  const f=fixture({elapsed:1,edit:(d,n)=>{if(n===2)mutate(d)}});
  await assert.rejects(readAgentSnapshots(f.client,ids,{clock:f.clock}),/snapshot incomplete/);assert.equal(f.calls.length,2);
 }
});
test('transport and tool errors stop once without retry or fallback',async()=>{
 for(const message of ['HTTP 502','HTTP 429','Request timed out']){
  let calls=0;const client={async callTool(){calls++;throw Error(message)}};
  await assert.rejects(readAgentSnapshots(client,ids),/asana_agents_open_task_snapshot/);assert.equal(calls,1);
 }
 let calls=0;await assert.rejects(readAgentSnapshots({async callTool(){calls++;return {isError:true,content:[{type:'text',text:'HTTP 503'}]}}},ids),/503/);assert.equal(calls,1);
});
test('invalid registry inventory is rejected before remote calls',async()=>{
 for(const invalid of [[],null,[ids[0],ids[0]],['other-agent'],Array(26).fill(ids[0]),[null]]){
  const f=fixture();await assert.rejects(readAgentSnapshots(f.client,invalid),/inventory/);assert.equal(f.calls.length,0);
 }
});
test('backward or invalid monotonic clock cannot prolong the budget',async()=>{
 for(const sequence of [[10,9],[0,0,NaN],[0,0,-1]]){
  let i=0;const f=fixture();await assert.rejects(readAgentSnapshots(f.client,ids,{clock:()=>sequence[i++]}),/clock/);
  assert.ok(f.calls.length<=1);
 }
});
test('ordinary tools keep their 45 second ceiling and are never replayed',async()=>{
 let options,calls=0;const client={async callTool(r,u,o){options=o;calls++;throw Error('HTTP 503')}};
 await assert.rejects(tool(client,'asana_comment',{}),/503/);assert.equal(calls,1);assert.deepEqual(options,{timeout:45000,maxTotalTimeout:45000});
 for(const timeoutMs of [0,-1,NaN,45001,1.1])await assert.rejects(tool(client,'asana_comment',{}, {timeoutMs}),/bounded tool timeout/);
 for(const deadlineAtMs of [0,-1,NaN,1.1])await assert.rejects(tool(client,'asana_comment',{}, {deadlineAtMs}),/Invalid tool deadline/);
 assert.equal(calls,1);
});
