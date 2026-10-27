// Explicit local maintenance primitive. Deployment of the connection barrier to
// every writer and absence of pre-deployment processes are caller prerequisites.
import fs from 'node:fs';
import path from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {DatabaseSync,backup} from 'node:sqlite';
import {DISPATCH_STORAGE_BARRIER} from './asana-dispatch-store.mjs';

const sha=raw=>createHash('sha256').update(raw).digest('hex');
const quote=name=>'"'+name.replaceAll('"','""')+'"';
const exists=p=>{try{fs.lstatSync(p);return true;}catch(e){if(e.code==='ENOENT')return false;throw e;}};
function sync(p,directory=false){const fd=fs.openSync(p,directory?'r':'r+');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}}

export function dispatchStorageSnapshot(db) {
  if(db.prepare('PRAGMA integrity_check').all().some(r=>r.integrity_check!=='ok'))throw new Error('Database integrity failed');
  const schema=db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name').all();
  const hash=createHash('sha256').update(JSON.stringify(schema));const counts={};
  for(const {name} of schema.filter(r=>r.type==='table')) {
    const query=db.prepare(`SELECT * FROM ${quote(name)}`);query.setReadBigInts(true);
    const rows=query.all().map(row=>JSON.stringify(row,(_k,v)=>typeof v==='bigint'?{integer:v.toString()}:v)).sort();
    counts[name]=rows.length;hash.update(JSON.stringify([name,rows]));
  }
  return {integrity:'ok',sha256:hash.digest('hex'),table_counts:counts};
}

export function dispatchOpenProcesses(source) {
  const files=[source,source+'-wal',source+'-shm'].filter(exists);
  if(!files.includes(source))throw new Error('Source disappeared');
  const result=spawnSync('/usr/sbin/lsof',['-nP','-t','--',...files],{encoding:'utf8',timeout:5000});
  if(result.error||result.signal||result.stderr.trim()||![0,1].includes(result.status))throw new Error('Open-file inspection failed');
  const pids=result.stdout.trim()?result.stdout.trim().split(/\s+/):[];
  if(pids.some(p=>!/^\d+$/.test(p))||(result.status===1&&pids.length))throw new Error('Unexpected open-file inspection');
  return [...new Set(pids.map(Number))];
}

export async function migrateDispatchStorage({source,destination,backupPath,barrier=DISPATCH_STORAGE_BARRIER,
  inspectOpenFiles=dispatchOpenProcesses,timeoutMs=30000,publicationPath=source,publicationKind='symlink'}) {
  if(![source,destination,backupPath,barrier].every(path.isAbsolute)||new Set([source,destination,backupPath,barrier]).size!==4)throw new Error('Distinct absolute paths required');
  if(!path.isAbsolute(publicationPath)||!["symlink","file"].includes(publicationKind)||[destination,backupPath,barrier].includes(publicationPath))throw new Error('Invalid publication path or kind');
  const initial=fs.lstatSync(source);
  if(!initial.isFile()||initial.nlink!==1||initial.size===0||initial.blocks===0||fs.realpathSync(source)!==source)throw new Error('Materialized canonical regular source required');
  for(const p of [destination,backupPath]) {
    if(exists(p)||fs.realpathSync(path.dirname(p))!==path.dirname(p))throw new Error('New destination and backup in existing physical directories required');
  }
  const publication=fs.lstatSync(publicationPath);
  if(publicationPath!==source&&(!publication.isSymbolicLink()||fs.realpathSync(publicationPath)!==source))throw new Error('Publication alias must reference the current source');
  const marker=JSON.stringify({pid:process.pid,id:randomUUID(),source,destination,created_at:new Date().toISOString()});
  fs.writeFileSync(barrier,marker,{flag:'wx',mode:0o600});sync(barrier);sync(path.dirname(barrier),true);
  let sourceDb,check,published=false,verified=false,temporary;
  try {
    // Persist the epoch beyond barrier removal. A process delayed across the
    // entire maintenance interval must still reject its obsolete connection.
    const epochTemporary=barrier+'.epoch-'+randomUUID();
    fs.writeFileSync(epochTemporary,randomUUID(),{flag:'wx',mode:0o600});sync(epochTemporary);
    fs.renameSync(epochTemporary,barrier+'.epoch');sync(path.dirname(barrier),true);
    const until=Date.now()+timeoutMs;
    while(inspectOpenFiles(source).length) {
      if(Date.now()>=until)throw new Error('Existing dispatch connections did not drain');
      await new Promise(resolve=>setTimeout(resolve,100));
    }
    const current=fs.lstatSync(source);
    if(current.dev!==initial.dev||current.ino!==initial.ino||current.size!==initial.size)throw new Error('Source changed before snapshot');
    // The maintenance process alone opens SQLite without the blocked helper.
    sourceDb=new DatabaseSync(source,{readOnly:true});
    const before=dispatchStorageSnapshot(sourceDb);
    fs.closeSync(fs.openSync(backupPath,'wx',0o600));
    await backup(sourceDb,backupPath);
    sourceDb.close();sourceDb=null;sync(backupPath);
    check=new DatabaseSync(backupPath,{readOnly:true});
    const saved=dispatchStorageSnapshot(check);check.close();check=null;
    if(saved.sha256!==before.sha256)throw new Error('Backup differs from source');
    fs.copyFileSync(backupPath,destination,fs.constants.COPYFILE_EXCL);fs.chmodSync(destination,0o600);sync(destination);sync(path.dirname(destination),true);
    check=new DatabaseSync(destination,{readOnly:true});
    const copied=dispatchStorageSnapshot(check);check.close();check=null;
    if(copied.sha256!==before.sha256)throw new Error('Destination differs from source');
    // Revalidate all rows after the asynchronous backup and immediately before
    // publishing. No lease, fence, observation or signal is reset.
    sourceDb=new DatabaseSync(source);
    const final=dispatchStorageSnapshot(sourceDb);
    const checkpoint=sourceDb.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
    if(checkpoint.busy!==0)throw new Error('Source WAL could not be checkpointed');
    sourceDb.close();sourceDb=null;
    if(final.sha256!==before.sha256||inspectOpenFiles(source).length)throw new Error('Source changed or reopened during cutover');
    if(exists(source+'-wal')||exists(source+'-shm'))throw new Error('Source sidecars remain after final close');
    if(fs.readFileSync(barrier,'utf8')!==marker)throw new Error('Storage barrier changed');
    const last=fs.lstatSync(source);
    if(last.dev!==initial.dev||last.ino!==initial.ino)throw new Error('Source identity changed');
    const lastPublication=fs.lstatSync(publicationPath);
    if(lastPublication.dev!==publication.dev||lastPublication.ino!==publication.ino)throw new Error('Publication path changed');
    if(publicationKind==='file')fs.renameSync(destination,publicationPath);
    else {
      temporary=publicationPath+'.migration-'+randomUUID();fs.symlinkSync(destination,temporary);
      fs.renameSync(temporary,publicationPath);temporary=null;
    }
    published=true;sync(path.dirname(publicationPath),true);
    const active=publicationKind==='file'?publicationPath:destination;
    if(fs.realpathSync(publicationPath)!==active)throw new Error('Published path mismatch');
    check=new DatabaseSync(active,{readOnly:true});
    const after=dispatchStorageSnapshot(check);check.close();check=null;
    if(after.sha256!==before.sha256)throw new Error('Published database differs');
    verified=true;
    return {status:'verified',source,destination:active,publication_path:publicationPath,publication_kind:publicationKind,backup:backupPath,before,after,backup_sha256:sha(fs.readFileSync(backupPath)),
      checkpoint,barrier_released:true,logical_rows_changed:false,completed_at:new Date().toISOString()};
  } finally {
    sourceDb?.close();check?.close();if(temporary&&exists(temporary))fs.unlinkSync(temporary);
    // On an uncertain post-publication outcome keep the barrier for Operations
    // inspection; never expose two stores or guess a rollback after publication.
    if((!published||verified)&&fs.readFileSync(barrier,'utf8')===marker){fs.unlinkSync(barrier);sync(path.dirname(barrier),true);}
  }
}
