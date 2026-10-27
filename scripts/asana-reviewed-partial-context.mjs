import fs from 'node:fs';
import {createHash} from 'node:crypto';
// Individually reviewed historical cases, not a general partial-work detector.
const cases = [
  {
    "key": "crossdesk",
    "partial": true,
    "run_id": "edd866de1013456fab8a32ae20be0fa5",
    "agent_id": "vip-ai-finance",
    "task_gid": "1219511201585628",
    "assignee": "1215003777954631",
    "thread_id": "01a159a7-d91f-7730-83ad-5aa4b57f2706",
    "turn_id": "01a159a7-dbc7-7f91-aa93-e2371a16ab50",
    "run_started_at_ms": 1792505517276,
    "turn_status": "completed",
    "error_message": null,
    "started_at": 1792505535,
    "completed_at": 1792505971,
    "activity_count": 66,
    "activity_sha256": "029897b8dc90c3c35c87fad7a8d5e007a1b1c8e2700aeac48ddaf46ad711333d",
    "stories_sha256": "874524279124cacb5f97fb52d65dad6d3b5b25fb9f24e35a71992547996ee46f",
    "modified_at": "2026-10-15T10:55:00.298Z",
    "artifacts": [
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/handoffs/2026-10-20__cross-desk__1219511201585628__edd866de__prepared.md",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/edd866de1013456fab8a32ae20be0fa5/2026-10-20__cross-desk__1219511201585628__edd866de__prepared.md",
        "sha256": "2e9093e0d2fb406755df2666e51e64620bd1c3c1c68900ffe4d7e66a84294f7d",
        "bytes": 10107
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-20__cross-desk__1219511201585628__edd866de__sources.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/edd866de1013456fab8a32ae20be0fa5/2026-10-20__cross-desk__1219511201585628__edd866de__sources.json",
        "sha256": "b83ca38969a0ac3c6b2c0aebeab6976d58bb3741f8af2bcb48eb45d736619f30",
        "bytes": 433835
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-20__cross-desk__1219511201585628__edd866de__checkpoint.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/edd866de1013456fab8a32ae20be0fa5/2026-10-20__cross-desk__1219511201585628__edd866de__checkpoint.json",
        "sha256": "2e2d083939ced211fb52ccef625e3e05bf1372e3f88150cff27efe044db76f74",
        "bytes": 6151
      }
    ]
  },
  {
    "key": "sector",
    "partial": true,
    "run_id": "d0439631bbef485ea7e50b2ff834c2d5",
    "agent_id": "vip-ai-finance",
    "task_gid": "1219713797496175",
    "assignee": "1215003777954631",
    "thread_id": "01a15d5b-b15c-71b3-aad6-b266c452e91e",
    "turn_id": "01a15d5b-b2c5-7123-aec4-1dfc167c7175",
    "run_started_at_ms": 1792567641163,
    "turn_status": "failed",
    "error_message": "Selected model is at capacity. Please try a different model.",
    "started_at": 1792567653,
    "completed_at": 1792567916,
    "activity_count": 32,
    "activity_sha256": "8425ff44079b8d8d02e1b40e3e23a3699fbaa3ec18307f598a6fcd30fd8709d6",
    "stories_sha256": "b72f6b882d07225042478fa8662e7ee76aa86d176874e193775b1d9283121abb",
    "modified_at": "2026-10-21T06:45:00.336Z",
    "artifacts": [
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-21__sector-radar__d0439631__sector_rotation-source.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/d0439631bbef485ea7e50b2ff834c2d5/2026-10-21__sector-radar__d0439631__sector_rotation-source.json",
        "sha256": "8b6deef695382419ad279f2d0a8067ace6680090887545b1e1a93c5a80b56e8a",
        "bytes": 151502
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-21__sector-radar__d0439631__regime_allocator-source.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/d0439631bbef485ea7e50b2ff834c2d5/2026-10-21__sector-radar__d0439631__regime_allocator-source.json",
        "sha256": "41845274c47b3fd5652d76a529db7b8338d890bfb9642570b20f109a327d1699",
        "bytes": 20298
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-21__sector-radar__d0439631__research_prep-source.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/d0439631bbef485ea7e50b2ff834c2d5/2026-10-21__sector-radar__d0439631__research_prep-source.json",
        "sha256": "faa524d9073b9d9222d66ffd803fff5e0cc77d51f0839bcb36e7b7c19e1467a3",
        "bytes": 24873
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Finance/research/run_state/2026-10-21__sector-radar__d0439631__polymarket-source.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/d0439631bbef485ea7e50b2ff834c2d5/2026-10-21__sector-radar__d0439631__polymarket-source.json",
        "sha256": "c60e8671e3fa61d33e362aceb3299f4eabae0660b3058aa03f37ca9eed76cc56",
        "bytes": 26910
      }
    ]
  }
];
function freeze(x) { if(x && typeof x==='object'){Object.values(x).forEach(freeze);Object.freeze(x);}return x; }
export const REVIEWED_PARTIALS=freeze(cases);
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const nextStep='Prüfe die gesicherte Vorarbeit und ihre Quellenzeitpunkte, dann den aktuellen Asana-Task mit allen Stories. Setze die offene Facharbeit als zuständiger Agent unter den aktuellen Fach-, Quellen- und Freigabegates fort. Die alten Quellen sind kein aktueller Marktbeleg. Keine Wiederholung bereits belegter Schritte und kein fachlicher Abschluss allein aufgrund dieser technischen Wiederaufnahme.';
export function reviewedPartialContext(target,evidenceSha) {
 return {schema:'task-resume-partial-work-v1',old_run_id:target.run_id,task_gid:target.task_gid,
  agent_id:target.agent_id,evidence_sha256:evidenceSha,disposition:'preserve_partial_work_then_resume',
  artifacts:target.artifacts,next_step:nextStep,task_completed:false,no_effect_claim:false};
}
export function verifyPartialArtifacts(artifacts,{includeOriginals=true}={}) {
 if(!Array.isArray(artifacts)||!artifacts.length)throw new Error('Preserved artifact inventory required');
 for(const a of artifacts) {
  if(!Number.isSafeInteger(a.bytes)||a.bytes<1||a.bytes>1024*1024||!/^[a-f0-9]{64}$/.test(a.sha256||''))throw new Error('Invalid artifact metadata');
  for(const p of includeOriginals?[a.original_path,a.path]:[a.path]) {
   if(typeof p!=='string'||!p.startsWith('/')||fs.realpathSync(p)!==p||!fs.lstatSync(p).isFile()||fs.statSync(p).size!==a.bytes||
    createHash('sha256').update(fs.readFileSync(p)).digest('hex')!==a.sha256)throw new Error('Preserved artifact changed');
  }
 }
}
export function verifyReviewedPartialContext(context,record,{verify=verifyPartialArtifacts}={}) {
 const target=REVIEWED_PARTIALS.find(t=>t.run_id===record.old_run_id);
 if(!target||record.task_gid!==target.task_gid||record.agent_id!==target.agent_id||
  !/^[a-f0-9]{64}$/.test(record.evidence_sha256||'')||
  !same(context,reviewedPartialContext(target,record.evidence_sha256)))throw new Error('Unreviewed partial resume context');
 // Worker relies on the retained snapshot. Original source paths may later be
 // archived by their owner; their bytes were verified at operator acceptance.
 verify(context.artifacts,{includeOriginals:false});
 return context;
}
