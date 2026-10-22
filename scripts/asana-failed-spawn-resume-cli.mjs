import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {DatabaseSync,backup} from 'node:sqlite';
import {AsanaDispatchStore,DEFAULT_DB_PATH} from './asana-dispatch-store.mjs';
import {failedSpawnResumeSnapshot,applyFailedSpawnResume} from './asana-failed-spawn-resume.mjs';
import {collectFailedSpawnProcessReadback} from './asana-worker-process-readback.mjs';
import {connectMcpClient} from './asana-dispatch-poller.mjs';
import {readManualAsanaTask} from './asana-manual-lease-readback.mjs';
import {manualThreadContext} from './asana-agent-lease.mjs';

export function parseSpawnResumeArguments(args) {
  const opts={apply:false};
  for(const arg of args) {
    if(arg==='--apply'&&!opts.apply){opts.apply=true;continue;}
    const m=/^--(run|operations-run|operations-task|output|backup)=(.+)$/.exec(arg);
    if(!m||opts[m[1]])throw new Error('Unknown or duplicate argument');opts[m[1]]=m[2];
  }
  if(!/^[a-f0-9]{32}$/.test(opts.run||'') || !/^[a-f0-9]{32}$/.test(opts['operations-run']||'') ||
    !/^\d+$/.test(opts['operations-task']||'') || !opts.output?.startsWith('/') ||
    opts.apply!==Boolean(opts.backup))throw new Error('Explicit run, operator, output and exclusive apply backup required');
  return opts;
}
function newExport(filename,extension) {
  const root=path.resolve(DEFAULT_DB_PATH,'..','..','exports')+path.sep;
  const full=path.resolve(filename);
  if(!full.startsWith(root)||!full.endsWith(extension)||fs.existsSync(full)||
    fs.realpathSync(path.dirname(full))!==path.dirname(full))throw new Error('New canonical exports path required');
  return full;
}
export async function runFailedSpawnResume(args) {
  const opts=parseSpawnResumeArguments(args),context=manualThreadContext();
  const output=newExport(opts.output,'.json');
  const backupPath=opts.apply?newExport(opts.backup,'.sqlite'):null;
  if(!fs.existsSync(DEFAULT_DB_PATH))throw new Error('Existing canonical database required');
  const store=Object.create(AsanaDispatchStore.prototype);
  store.db=new DatabaseSync(DEFAULT_DB_PATH);store.db.exec('PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL');
  let client;
  try {
    const snapshot=failedSpawnResumeSnapshot(store,opts.run);
    if(snapshot.prior_events.length!==1)throw new Error('Single previous accepted spawn audit required');
    const prior=JSON.parse(snapshot.prior_events[0].evidence_json);
    const proof={read_agent_id:'vip-ai-operations',worker_record:prior.proof.worker_record,processes:[],asana:[]};
    const operator={run_id:opts['operations-run'],thread_id:context.threadId,task_gid:opts['operations-task']};
    client=await connectMcpClient();
    for(let pass=0;pass<2;pass++) {
      if(pass)await new Promise(r=>setTimeout(r,30000));
      proof.processes.push(collectFailedSpawnProcessReadback(snapshot.run));
      proof.asana.push(await readManualAsanaTask({agent_id:'vip-ai-operations'},
        {task_gid:snapshot.run.task_gid},client));
    }
    const dry=applyFailedSpawnResume(store,snapshot,proof,{operator});
    let result=dry;
    if(opts.apply&&dry.action==='retry_after') {
      fs.closeSync(fs.openSync(backupPath,'wx',0o600));
      await backup(store.db,backupPath);
      const check=new DatabaseSync(backupPath,{readOnly:true});
      try{if(check.prepare('PRAGMA quick_check').get().quick_check!=='ok')throw new Error('Backup invalid');}
      finally{check.close();}
      result=applyFailedSpawnResume(store,snapshot,proof,{operator,apply:true});
    }
    const evidence={observed_at:new Date().toISOString(),snapshot,proof,operator,result,backup_path:backupPath,
      after:failedSpawnResumeSnapshot(store,opts.run)};
    fs.writeFileSync(output,JSON.stringify(evidence,null,2)+'\n',{flag:'wx',mode:0o600});
    return {result,evidence_path:output,backup_path:backupPath};
  } finally {await client?.close().catch(()=>{});store.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)
  runFailedSpawnResume(process.argv.slice(2)).then(r=>console.log(JSON.stringify(r)))
    .catch(e=>{console.error(e.message);process.exitCode=1;});
