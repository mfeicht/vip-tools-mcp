import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {z} from 'zod';

const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
const start=source.indexOf('  server.tool(\n    "asana_verify_next_routine_instance",');
const end=source.indexOf('  server.tool(',start+10);
const registration=source.slice(start,end);

async function invoke(task) {
  let handler, reads=0,searches=0;
  const context={z,agentIdSchema:z.string(),TOOL_READ_ONLY:{},ASANA_RECURRENCE_GENERATION_GRACE_SECONDS:600,
    ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS:'gid,completed',server:{tool:(name,description,schema,annotations,fn)=>{handler=fn;}},
    validateAsanaGid:()=>{},validateAsanaDate:()=>{},validateAsanaDateTime:()=>{},validateAsanaLocalTime:()=>{},
    getAsana:()=>({}),asanaRequestWithRetry:async(_asana,request)=>{reads++;assert.equal(request.method,'GET');return {data:{data:task}};},
    getAsanaTaskProjectGids:()=>['888'],findAsanaNextRoutineCandidates:async()=>{searches++;return {candidates:[],search_fallback_error:null};},
    asanaTaskCompletionAgeSeconds:()=>0,out:value=>value,sleep:()=>{throw new Error('No polling before completion');}};
  vm.runInNewContext(registration,context);
  const value=await handler({agent_id:'vip-ai-operations',source_task_gid:'123',generation_grace_seconds:600,recheck_attempts:1});
  return {value,reads,searches};
}

test('open source performs one GET, no search/wait and explicitly forbids a false incident',async()=>{
  const {value,reads,searches}=await invoke({gid:'123',completed:false});
  assert.equal(reads,1);assert.equal(searches,0);assert.equal(value.verification_status,'source_not_completed');
  assert.equal(value.source_completion_blocked_by_this_check,false);assert.equal(value.alert_task_creation_allowed,false);
  assert.equal(value.generation_grace_started,false);assert.equal(value.candidate_count,null);
  assert.match(value.note,/erst durch seinen Abschluss/);assert.doesNotMatch(value.note,/Alarm ist jetzt erlaubt/);
});

test('completed source retains post-completion generation grace',async()=>{
  const {value,searches}=await invoke({gid:'123',completed:true});
  assert.equal(searches,1);assert.equal(value.verification_status,'pending_generation');
  assert.equal(value.alert_task_creation_allowed,false);assert.equal(value.generation_grace_active,true);
});

test('incomplete or mismatched source cannot instruct a completion',async()=>{
  await assert.rejects(invoke({gid:'456',completed:false}),/Quelltask-Readback/);
  await assert.rejects(invoke({gid:'123'}),/Quelltask-Readback/);
});
