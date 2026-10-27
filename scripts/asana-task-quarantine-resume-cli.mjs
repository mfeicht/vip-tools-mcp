import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {DatabaseSync,backup} from 'node:sqlite';
import {AsanaDispatchStore,DEFAULT_DB_PATH,openDispatchDatabase} from './asana-dispatch-store.mjs';
import {resumeTaskQuarantine} from './asana-task-quarantine-resume.mjs';
const sha=p=>createHash('sha256').update(fs.readFileSync(p)).digest('hex');
export function parseResumeArguments(args) {
  const options={apply:false};
  for(const arg of args){
    if(arg==='--apply'){if(options.apply)throw new Error('Duplicate --apply');options.apply=true;continue;}
    const m=/^--(evidence|operations-run|backup)=(.+)$/.exec(arg);
    if(!m||options[m[1]])throw new Error('Unknown or duplicate argument');options[m[1]]=m[2];
  }
  if(!options.evidence?.startsWith('/')||!options['operations-run'])throw new Error('Explicit evidence and Operations run required');
  if(options.apply!==Boolean(options.backup))throw new Error('Apply requires an exclusive backup path; dry-run accepts none');
  return options;
}
export async function runResumeCli(args) {
  const opts=parseResumeArguments(args);
  const evidencePath=opts.evidence;
  if(fs.realpathSync(evidencePath)!==evidencePath||!fs.lstatSync(evidencePath).isFile()||fs.statSync(evidencePath).size>1024*1024)throw new Error('Bounded regular evidence file required');
  const evidence=JSON.parse(fs.readFileSync(evidencePath,'utf8'));
  const thread=process.env.CODEX_THREAD_ID||process.env.CODEX_SESSION_ID;
  if(!thread||evidence.operations_authorization?.thread_id!==thread||evidence.operations_authorization?.run_id!==opts['operations-run'])throw new Error('Current Operations thread/run binding required');
  const worker=path.join(import.meta.dirname,'asana-dispatch-worker.mjs');
  const resolver=path.join(import.meta.dirname,'asana-task-quarantine-resume.mjs');
  const source=fs.readFileSync(worker,'utf8');
  if(sha(worker)!==evidence.worker_sha256||sha(resolver)!==evidence.resolver_sha256||
     !source.includes('resumeContextForClaim(store, claim)')||!source.includes('resumePrompt(claim.resume_context)'))throw new Error('Reviewed replacement worker and resolver binding required');
  if(!fs.existsSync(DEFAULT_DB_PATH))throw new Error('Existing canonical dispatcher database required');
  const store=Object.create(AsanaDispatchStore.prototype);store.db=openDispatchDatabase(DEFAULT_DB_PATH);
  store.db.exec('PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL');
  try {
    const dry=resumeTaskQuarantine(store,evidence);
    if(!opts.apply)return {...dry,production_transition:false};
    const backupPath=path.resolve(opts.backup);
    const exportsRoot=path.resolve(DEFAULT_DB_PATH,'..','..','exports')+path.sep;
    if(!backupPath.startsWith(exportsRoot)||!backupPath.endsWith('.sqlite')||fs.existsSync(backupPath))throw new Error('New canonical Operations exports backup path required');
    if(fs.realpathSync(path.dirname(backupPath))!==path.dirname(backupPath))throw new Error('Backup parent must be real');
    const fd=fs.openSync(backupPath,'wx',0o600);fs.closeSync(fd);
    await backup(store.db,backupPath);
    const check=new DatabaseSync(backupPath,{readOnly:true});
    try{if(check.prepare('PRAGMA quick_check').get().quick_check!=='ok')throw new Error('Backup quick_check failed');}finally{check.close();}
    const backupSha=sha(backupPath);
    const result=resumeTaskQuarantine(store,evidence,{apply:true});
    return {...result,production_transition:true,backup_path:backupPath,backup_sha256:backupSha,evidence_file_sha256:sha(evidencePath)};
  } finally {store.db.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  if(process.argv.slice(2).join(' ')==='--help')console.log('Usage: --evidence=/absolute/file.json --operations-run=ID [--apply --backup=/canonical/Operations/exports/new.sqlite]');
  else runResumeCli(process.argv.slice(2)).then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
}
