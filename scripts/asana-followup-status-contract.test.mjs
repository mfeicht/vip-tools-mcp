import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {isRoutineMaterialComment} from '../lib/asana-material-comment-coordinator.js';
import {hasRoutineNoFollowUpDecision, inspectRoutineNoFollowUpStatusIdempotency,
  inspectRoutineMaterialCommentIdempotency, validateRoutineVisibleFollowUpStatus} from '../lib/asana-completion-guard.js';

// Exercise the actual server classifier, not a copy of its regex.
const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
const start=source.indexOf('function asanaCommentHasMaterialResultSignals(');
const end=source.indexOf('\nfunction prepareAsanaEvidenceSections(',start);
const context=vm.createContext({plainAsanaParagraph:p=>typeof p==='string'?p:p.runs.map(r=>r.text).join('')});
vm.runInContext(source.slice(start,end),context);
const classify=text=>context.asanaCommentHasMaterialResultSignals({sections:[{title:'Follow-up-Entscheid',paragraphs:[text]}]});
const result={gid:'100',created_by:{gid:'200'},text:'Ergebnis: fünf Leads importiert.\nEvidenz / Verifikation\nImport-Receipt bestätigt.'};
const decision='Keine gesonderte Folgeaufgabe oder Nacharbeit erforderlich.';

test('the real failed status repeats a material result and remains blocked',()=>{
  const text='Keine gesonderte Folgeaufgabe erforderlich. Der definierte Routineumfang ist durch die Ergebnisstory vollständig belegt: fünf Survivor wurden nach Partnergate und Deduplizierung erfolgreich importiert.';
  assert.equal(hasRoutineNoFollowUpDecision(text),true);
  assert.equal(isRoutineMaterialComment({routineLike:true,commentKind:'status',materialResultSignals:classify(text)}),true);
  assert.equal(inspectRoutineMaterialCommentIdempotency({stories:[result],agentUserGid:'200'}).allowed,false);
});

test('a decision-only status uses the existing separate coordinated path once',()=>{
  assert.equal(classify(decision),false);
  assert.equal(isRoutineMaterialComment({routineLike:true,commentKind:'status',materialResultSignals:classify(decision)}),false);
  assert.equal(hasRoutineNoFollowUpDecision(decision),true);
  assert.equal(inspectRoutineNoFollowUpStatusIdempotency({stories:[result],agentUserGid:'200',proposedText:decision}).allowed,true);
  const status={gid:'101',created_by:{gid:'200'},text:decision+'\nEvidenz / Verifikation\nQuelle: Ergebnisstory 100; aktueller Scope-Abgleich.'};
  assert.equal(validateRoutineVisibleFollowUpStatus({finalComment:status,hasFollowUpTask:false}).ok,true);
  assert.equal(inspectRoutineNoFollowUpStatusIdempotency({stories:[result,status],agentUserGid:'200',proposedText:decision}).allowed,false);
});

test('the short decision cannot override contradictory pending work',()=>{
  const status={text:decision+' Aber eine Freigabe ist noch offen.\nEvidenz / Verifikation\nQuelle: aktueller Task.'};
  assert.equal(validateRoutineVisibleFollowUpStatus({finalComment:status,hasFollowUpTask:false}).ok,false);
});
