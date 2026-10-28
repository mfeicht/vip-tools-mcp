import fs from 'node:fs';
import {createHash} from 'node:crypto';
// Individually reviewed historical cases, not a general partial-work detector.
const cases = [
{
  "key": "weekly_health",
  "partial": true,
  "run_id": "9f9a0cd63f99437db654821972e2bf27",
  "agent_id": "vip-ai-operations",
  "task_gid": "1218924687084315",
  "assignee": "1214979008788676",
  "thread_id": "01a18115-7177-72b3-98da-a9dcfbbe5388",
  "turn_id": "01a18115-8dda-7821-8d8b-13efb06e5234",
  "run_started_at_ms": 1793167005211,
  "turn_status": "failed",
  "error_message": "Selected model is at capacity. Please try a different model.",
  "started_at": 1793167035,
  "completed_at": 1793167415,
  "activity_count": 91,
  "activity_sha256": "14e8eba5e31255a5a95743a2f373980a4d312f1d26836b60dda7162b38648af7",
  "stories_sha256": "8689d3c07db046aa027efa70e72e3f9751bb13c42c992f451fea6ff4153489a4",
  "modified_at": "2026-10-28T06:03:03.675Z",
  "artifacts": [
    {
      "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/2026-10-28__asana-1218924687084315__weekly-health__run-9f9a0cd6.json",
      "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/9f9a0cd63f99437db654821972e2bf27/2026-10-28__asana-1218924687084315__weekly-health__run-9f9a0cd6.json",
      "sha256": "7db43ceb39ea55e0b3785816d28c8f83fa3de2dc3fe35bdf15981ade1b3dae37",
      "bytes": 4188
    },
    {
      "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/exports/2026-10-28__weekly-recovery/asana-reviewed.json",
      "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/9f9a0cd63f99437db654821972e2bf27/asana-reviewed.json",
      "sha256": "496c33558273c4dc71c5f2b26ce825550aaf8f5bab4a394c9229cda54889f663",
      "bytes": 146979
    }
  ],
  "next_step": "Lies den gesicherten Wochen-Checkpoint und Story 1219958682155894, dann den aktuellen Operations-Stand. Der Statuskommentar wurde bereits erfolgreich gespeichert; nicht nochmals posten. Die Zeitzonen-Korrektur ist inzwischen im Standardlauf erledigt und darf nicht anhand des historischen Checkpoints wiederholt werden. Prüfe aktuelle Restbefunde und natürliche Folgeinstanzen unter eigenen Task-Leases. Kein Gesamtabschluss aus der technischen Wiederaufnahme."
},
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
  },
  {
    "key": "watcher",
    "partial": true,
    "run_id": "4cd045916a094d9393115a9fe3f2beba",
    "agent_id": "vip-ai-operations",
    "task_gid": "1218693508638175",
    "assignee": "1214979008788676",
    "thread_id": "01a15a83-7648-71f2-be3c-5d62dd2ae6db",
    "turn_id": "01a15a83-7973-7b22-ad84-083960901d4c",
    "run_started_at_ms": 1792519908106,
    "turn_status": "interrupted",
    "error_message": null,
    "started_at": 1792519928,
    "completed_at": null,
    "activity_count": 136,
    "activity_sha256": "581cfbbb9dab5d549190a4fae475a06f6ec4b3bf2886960dca22cf40a9c6e262",
    "stories_sha256": "5e6b4962197a2f1721ff82495ce3520afa030056598a7146dbd9d4482987c29f",
    "modified_at": "2026-10-20T18:20:20.190Z",
    "artifacts": [
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/2026-10-20__asana-watcher-manual-transaction__run-4cd04591__v01.md",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/4cd045916a094d9393115a9fe3f2beba/2026-10-20__asana-watcher-manual-transaction__run-4cd04591__v01.md",
        "sha256": "e248187fb2619e4333ce1dc65bc79bc26e1b66c1bb1374018bc8769e1b2e5aab",
        "bytes": 3471
      }
    ],
    "next_step": "Lies den gesicherten Checkpoint und den aktuellen Operations-Stand. Die frühere Transaktionsarbeit wurde in main weiterentwickelt (d1bf144); nicht anhand des historischen Branch-Status erneut implementieren. Prüfe die aktuellen offenen Watcher-Abnahmegates. Kein Gesamtabschluss aus diesem technischen Wiederaufnahmeschritt."
  },
  {
    "key": "content",
    "partial": true,
    "run_id": "420b03d332354d04bdf2035164321fdb",
    "agent_id": "vip-ai-content",
    "task_gid": "1219640139226246",
    "assignee": "1214338261069896",
    "thread_id": "01a1642f-b9e4-7ee0-9bc7-b63cb74bd635",
    "turn_id": "01a1642f-bc8d-7081-8f10-7d9e06dc119f",
    "run_started_at_ms": 1792682199133,
    "turn_status": "completed",
    "error_message": null,
    "started_at": 1792682212,
    "completed_at": 1792682631,
    "activity_count": 112,
    "activity_sha256": "88bcdb91790e34cd58cb8b017bd65484592d81ca7017e8dca734c9c60cfa2908",
    "stories_sha256": "e18375a53d29b13834eefaca8c71a5f857cc94db4bf7c2bc8ad115c5d841c0c0",
    "modified_at": "2026-10-23T15:03:21.426Z",
    "artifacts": [
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Memory/10-Agenten/VIP-AI-Content/Run-States/2026-10-22-1645-onpage-9167.md",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/420b03d332354d04bdf2035164321fdb/2026-10-22-1645-onpage-9167.md",
        "sha256": "b2e1cd1d9b8b4216e2c33bd0f995753e646ab25d4aeb1e797de41830d9d80f7a",
        "bytes": 5936
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Content/temp/onpage-9167-20261022T1718.html",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/420b03d332354d04bdf2035164321fdb/onpage-9167-20261022T1718.html",
        "sha256": "8a6de3a73c7449625710692df9f430b28be82976636a54933d60c93b1477630a",
        "bytes": 863741
      },
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Content/temp/onpage-9167-20261022T1718.headers",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/420b03d332354d04bdf2035164321fdb/onpage-9167-20261022T1718.headers",
        "sha256": "763545795deee39cc4f9e04f5d254003bf303b45ff2020e406c545d176c2ff8c",
        "bytes": 446
      }
    ],
    "next_step": "Lies den gesicherten Run-State und den historischen HTML-/Header-Beleg. Der fehlgeschlagene Fortschrittskommentar wurde im vollständigen Operations-Story-Readback nicht gefunden. Prüfe den aktuellen Task und Dedupe erneut; setze die offene Arbeit ausschließlich unter den aktuellen Content-/WordPress-/Asana-Gates fort. Historisches HTML ist kein aktueller Live-Readback."
  },
  {
    "key": "sales_umzug",
    "partial": true,
    "run_id": "5c85118f466844889ee796f5c22530cc",
    "agent_id": "vip-ai-sales",
    "task_gid": "1219723474750143",
    "assignee": "1214338261069890",
    "thread_id": "01a168c0-192d-7cd2-9025-781aca5cea28",
    "turn_id": "01a168c0-42f7-71a0-9158-eb5baeecce9f",
    "run_started_at_ms": 1792758760078,
    "turn_status": "completed",
    "error_message": null,
    "started_at": 1792758792,
    "completed_at": 1792759017,
    "activity_count": 70,
    "activity_sha256": "442a35f1dcd1751fdf0f506ec2ceb7b0a82f020454b059d7a6baac17ec753429",
    "stories_sha256": "b6b6074afb5461765c5685ed4a513f4e88cdfa116c25b0c4ee34fb1f57490f01",
    "modified_at": "2026-10-26T12:20:50.155Z",
    "artifacts": [
      {
        "original_path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/exports/2026-10-27__system-recovery/sales-umzug-resumption-context.json",
        "path": "/Users/moritzfeichtmeyer/Documents/Codex/VIP-Studios/VIP-AI-Workspace/10-Agenten/VIP-AI-Operations/run_state/recovery-artifacts/5c85118f466844889ee796f5c22530cc/sales-umzug-resumption-context.json",
        "sha256": "1e2633247d85220afd290497e1e14317df002bcc62b0e45a2ed5a8b0310a33c9",
        "bytes": 44067
      }
    ],
    "next_step": "Lies den gesicherten Quelltask- und Operations-Handoff-Readback. Die fünf Lead-Imports sind bereits dokumentiert: keine erneute Recherche oder Imports. PR #17 erläutert den reinen Entscheidstatus ohne erneute Ergebnisbehauptung. Prüfe aktuelle Stories, den technischen Handoff und verbleibende Nacharbeit, dann nur den noch nötigen Follow-up-Entscheid/Abschluss mit Dry-Run und nativer Folgeinstanz unter den geltenden Gates. Keine manuelle Ersatzroutine und kein Abschluss ohne aktuelle Belege."
  }
];
function freeze(x) { if(x && typeof x==='object'){Object.values(x).forEach(freeze);Object.freeze(x);}return x; }
export const REVIEWED_PARTIALS=freeze(cases);
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const nextStep='Prüfe die gesicherte Vorarbeit und ihre Quellenzeitpunkte, dann den aktuellen Asana-Task mit allen Stories. Setze die offene Facharbeit als zuständiger Agent unter den aktuellen Fach-, Quellen- und Freigabegates fort. Die alten Quellen sind kein aktueller Marktbeleg. Keine Wiederholung bereits belegter Schritte und kein fachlicher Abschluss allein aufgrund dieser technischen Wiederaufnahme.';
export function reviewedPartialContext(target,evidenceSha) {
 return {schema:'task-resume-partial-work-v1',old_run_id:target.run_id,task_gid:target.task_gid,
  agent_id:target.agent_id,evidence_sha256:evidenceSha,disposition:'preserve_partial_work_then_resume',
  artifacts:target.artifacts,next_step:target.next_step||nextStep,task_completed:false,no_effect_claim:false};
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
