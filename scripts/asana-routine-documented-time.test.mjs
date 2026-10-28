import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import vm from 'node:vm';
import { z } from 'zod';
import { assertRoutineCatchupOnCadence, nextRoutineCadenceSlot } from '../lib/asana-routine-cadence.js';

const rule={unit:'day',interval:1,anchor_due_on:'2026-10-15',weekdays:[1,2,3,4,5]};
const source={due_on:'2026-10-15',due_at:'2026-10-15T10:25:00Z'};
const documented={...rule,time_berlin:'09:10',time_basis:'Synthetic Operations resolution: current weekday contract at 09:10 Europe/Berlin.'};
const now=new Date('2026-10-28T01:00:00Z');

test('explicit documented time corrects drift without rewriting historical source',()=>{
  const before=JSON.stringify(source);
  assert.deepEqual(assertRoutineCatchupOnCadence(documented,source,
    {expected_due_at:'2026-10-28T08:10:00Z'},now),{due_on:'2026-10-28',due_time_berlin:'09:10:00'});
  assert.equal(JSON.stringify(source),before);
  assert.throws(()=>assertRoutineCatchupOnCadence(documented,source,
    {expected_due_at:'2026-10-28T11:25:00Z'},now),/off the documented cadence/);
  assert.throws(()=>assertRoutineCatchupOnCadence(documented,source,
    {expected_due_on:'2026-10-28'},now),/off the documented cadence/);
});

test('omitted clock retains source-time gate and date-only behavior',()=>{
  assert.deepEqual(nextRoutineCadenceSlot(rule,source,now),{due_on:'2026-10-28',due_time_berlin:'12:25:00'});
  assert.throws(()=>assertRoutineCatchupOnCadence(rule,source,
    {expected_due_at:'2026-10-28T08:10:00Z'},now),/off the documented cadence/);
  assert.deepEqual(nextRoutineCadenceSlot(rule,{due_on:source.due_on},now),
    {due_on:'2026-10-29',due_time_berlin:null});
});

test('clock override still obeys next slot, weekday, anchor and DST gates',()=>{
  for(const [instant,date] of [['2026-10-28T08:10:00Z','2026-10-29'],
    ['2026-10-23T07:10:00Z','2026-10-26'],['2026-10-25T06:00:00Z','2026-10-26']]){
    assert.deepEqual(nextRoutineCadenceSlot(documented,source,new Date(instant)),
      {due_on:date,due_time_berlin:'09:10:00'});
  }
  assert.doesNotThrow(()=>assertRoutineCatchupOnCadence(documented,source,
    {expected_due_at:'2026-10-26T08:10:00Z'},new Date('2026-10-23T07:10:00Z')));
  assert.throws(()=>assertRoutineCatchupOnCadence(documented,source,
    {expected_due_at:'2026-10-26T07:10:00Z'},new Date('2026-10-23T07:10:00Z')),/off the documented cadence/);
  assert.throws(()=>nextRoutineCadenceSlot({...documented,anchor_due_on:'2026-10-16'},source,now),/anchor cannot follow/);
  assert.throws(()=>nextRoutineCadenceSlot(documented,{due_on:'2026-10-17'},now),/documented weekdays/);
});

test('invalid or unsupported time values and missing evidence fail closed',()=>{
  for(const time_berlin of [null,9,'9:10','24:00','09:60','09:10:00','09:10Z',' 09:10']){
    assert.throws(()=>nextRoutineCadenceSlot({...documented,time_berlin},source,now),/requires HH:MM/);
  }
  for(const time_basis of [undefined,null,'','  ',33,'a'.repeat(2001)]){
    assert.throws(()=>nextRoutineCadenceSlot({...documented,time_basis},source,now),/time_basis/);
  }
  assert.throws(()=>nextRoutineCadenceSlot({...rule,time_basis:'Unpaired source'},source,now),/requires HH:MM/);
});

test('both native schedule tools expose and retain the explicit time contract',()=>{
  const server=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
  const start=server.indexOf('const ASANA_ROUTINE_CADENCE_RULE_SCHEMA =');
  const end=server.indexOf('const ASANA_TASK_SCHEDULE_VERIFY_OPT_FIELDS',start);
  const schema=vm.runInNewContext(server.slice(start,end)+'\nASANA_ROUTINE_CADENCE_RULE_SCHEMA',{z});
  assert.deepEqual(JSON.parse(JSON.stringify(schema.parse(documented))),documented);
  assert.throws(()=>schema.parse({...documented,time_berlin:'24:00'}));
  for(const name of ['asana_update_task_schedule','asana_repair_routine_catchup_due']){
    const section=server.slice(server.indexOf(`    "${name}",`));
    assert.match(section.slice(0,6000),/schedule_rule: ASANA_ROUTINE_CADENCE_RULE_SCHEMA/);
    assert.match(section.slice(0,6000),/assertRoutineCatchupOnCadence\(/);
  }
});
