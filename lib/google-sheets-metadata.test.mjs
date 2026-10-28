import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { readSpreadsheetMetadata } from './google-sheets-metadata.js';

const spreadsheetId = 'synthetic_sheet_123', agentId = 'vip-ai-operations';
const payload = () => ({ spreadsheetId, properties: {title:'Synthetic workbook',locale:'de_DE',timeZone:'Europe/Berlin'},
  sheets: [{properties:{sheetId:0,title:'First tab',index:0,sheetType:'GRID',gridProperties:{rowCount:100,columnCount:12}}},
    {properties:{sheetId:7,title:'Hidden tab',index:1,sheetType:'GRID',hidden:true}},
    {properties:{sheetId:9,title:'Chart',index:2,sheetType:'OBJECT'}}] });
const call = data => readSpreadsheetMetadata({spreadsheetId,agentId,request:async()=>({data})});

test('one metadata-only GET with explicit identity preserves zero ID, hidden and object tabs',async()=>{
  const calls=[];
  const result=await readSpreadsheetMetadata({spreadsheetId,agentId,request:async(...args)=>{calls.push(args);return {data:payload()};}});
  assert.equal(calls.length,1);const [config,context]=calls[0];
  assert.equal(config.method,'GET');assert.equal(config.url,`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}`);
  assert.deepEqual(context,{agent_id:agentId});assert.equal(config.timeout,30000);
  assert.equal(config.params.includeGridData,false);assert.equal(config.params.ranges,undefined);
  assert.doesNotMatch(config.params.fields,/rowData|values|note|formula|dataSource/);
  assert.equal(result.sheet_count,3);assert.deepEqual(result.sheets.map(x=>x.sheet_id),[0,7,9]);
  assert.equal(result.sheets[0].row_count,100);assert.equal(result.sheets[1].hidden,true);
  assert.equal(result.sheets[2].row_count,undefined);assert.equal(result.spreadsheet_id,spreadsheetId);
});
test('rejects malformed IDs before request and does not substitute identities',async()=>{
  for(const id of ['', '../other','https://example.com/sheet','bad?fields=data','x'.repeat(257),null]){
    await assert.rejects(readSpreadsheetMetadata({spreadsheetId:id,agentId,request:()=>assert.fail('request forbidden')}),/Spreadsheet-ID/);
  }
  await assert.rejects(readSpreadsheetMetadata({spreadsheetId,agentId:'',request:()=>assert.fail('request forbidden')}),/Agentenidentität/);
});
test('incomplete, mismatched and duplicate inventory fails rather than claiming missing tabs',async()=>{
  const mutations=[d=>{d.spreadsheetId='other';},d=>{delete d.properties;},d=>{delete d.sheets;},d=>{d.sheets=[];},
    d=>{delete d.sheets[0].properties.sheetId;},d=>{d.sheets[0].properties.sheetId=-1;},
    d=>{d.sheets[1].properties.sheetId=0;},d=>{d.sheets[1].properties.title='First tab';},
    d=>{d.sheets[0].properties.title='';}];
  for(const mutate of mutations){const d=payload();mutate(d);await assert.rejects(call(d),/Readback/);}
});
test('upstream 403 propagates once and never becomes empty successful metadata',async()=>{
  let calls=0;await assert.rejects(readSpreadsheetMetadata({spreadsheetId,agentId,request:async()=>{calls++;throw new Error('Google 403');}}),/Google 403/);assert.equal(calls,1);
});
test('unexpected grid content cannot escape in response',async()=>{
  const d=payload();d.sheets[0].data=[{rowData:[{values:[{formattedValue:'PRIVATE_CELL'}]}]}];
  d.sheets[0].properties.unexpected='PRIVATE_CELL';d.properties.unexpected='PRIVATE_CELL';
  assert.doesNotMatch(JSON.stringify(await call(d)),/PRIVATE_CELL|rowData|formattedValue/);
});
test('MCP registration uses existing Google request and readonly annotation',()=>{
  const s=readFileSync(new URL('../server.js',import.meta.url),'utf8');
  const b=s.slice(s.indexOf('    "google_sheets_metadata",'),s.indexOf('    "google_sheets_format_snapshot",'));
  assert.match(b,/agent_id: z\.enum\(Object\.keys\(ASANA_TOKEN_ENVS\)\)/);assert.match(b,/TOOL_READ_ONLY/);assert.match(b,/request: googleRequest/);
  assert.match(b,/agentId: agent_id/);assert.doesNotMatch(b,/sheet_name:|method:|authorization:|requestFields/);
});
