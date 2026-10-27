import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {DatabaseSync,backup} from 'node:sqlite';
import {AsanaDispatchStore,DEFAULT_DB_PATH,openDispatchDatabase} from './asana-dispatch-store.mjs';
import {dispatchRecoverySnapshot} from './asana-dispatch-terminal-recovery.mjs';
import {readStartupNative,applyStartupReadResume} from './asana-startup-read-resume.mjs';
import {parseSpawnResumeArguments} from './asana-failed-spawn-resume-cli.mjs';
import {CodexAppRpc} from './codex-app-rpc.mjs';
import {collectWorkerProcessReadback} from './asana-worker-process-readback.mjs';
import {connectMcpClient} from './asana-dispatch-poller.mjs';
import {readManualAsanaTask,boundedReadClients} from './asana-manual-lease-readback.mjs';
import {manualThreadContext} from './asana-agent-lease.mjs';

function newExport(filename,extension) {
  const root=path.resolve(DEFAULT_DB_PATH,'..','..','exports')+path.sep;
  const full=path.resolve(filename);
  if(!full.startsWith(root)||!full.endsWith(extension)||fs.existsSync(full)||
    fs.realpathSync(path.dirname(full))!==path.dirname(full))throw new Error('New canonical exports path required');
  return full;
}
export async function runStartupReadResume(args) {
  const opts=parseSpawnResumeArguments(args),context=manualThreadContext();
  const output=newExport(opts.output,'.json');
  const backupPath=opts.apply?newExport(opts.backup,'.sqlite'):null;
  if(!fs.existsSync(DEFAULT_DB_PATH))throw new Error('Existing canonical database required');
  const store=Object.create(AsanaDispatchStore.prototype);
  store.db=openDispatchDatabase(DEFAULT_DB_PATH);store.db.exec('PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL');
  let client,rpc;
  try {
    const snapshot=dispatchRecoverySnapshot(store,opts.run);
    if(!snapshot.run?.thread_id || !snapshot.run?.turn_id)throw new Error('Bound native turn required');
    const proof={native:[],processes:[],asana:[]};
    const operator={run_id:opts['operations-run'],thread_id:context.threadId,task_gid:opts['operations-task']};
    client=await connectMcpClient();
    rpc=new CodexAppRpc();await rpc.start();
    const readers=boundedReadClients(rpc,client);
    for(let pass=0;pass<2;pass++) {
      if(pass)await new Promise(r=>setTimeout(r,30000));
      proof.native.push(await readStartupNative(snapshot.run,readers.rpc));
      proof.processes.push(collectWorkerProcessReadback(snapshot.run));
      proof.asana.push(await readManualAsanaTask({agent_id:'vip-ai-operations'},
        {task_gid:snapshot.run.task_gid},readers.client));
    }
    const dry=applyStartupReadResume(store,snapshot,proof,{operator});
    let result=dry;
    if(opts.apply&&dry.action==='retry_after') {
      fs.closeSync(fs.openSync(backupPath,'wx',0o600));
      await backup(store.db,backupPath);
      const check=new DatabaseSync(backupPath,{readOnly:true});
      try{if(check.prepare('PRAGMA quick_check').get().quick_check!=='ok')throw new Error('Backup invalid');}
      finally{check.close();}
      result=applyStartupReadResume(store,snapshot,proof,{operator,apply:true});
    }
    const evidence={observed_at:new Date().toISOString(),snapshot,proof,operator,result,backup_path:backupPath,
      after:dispatchRecoverySnapshot(store,opts.run)};
    fs.writeFileSync(output,JSON.stringify(evidence,null,2)+'\n',{flag:'wx',mode:0o600});
    return {result,evidence_path:output,backup_path:backupPath};
  } finally {await client?.close().catch(()=>{});await rpc?.close().catch(()=>{});store.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)
  runStartupReadResume(process.argv.slice(2)).then(r=>console.log(JSON.stringify(r)))
    .catch(e=>{console.error(e.message);process.exitCode=1;});
