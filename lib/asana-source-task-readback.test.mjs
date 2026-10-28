import test from 'node:test';import assert from 'node:assert/strict';
import {detectRoutineFollowUpSignals,hasRoutineNoFollowUpDecision,inspectRoutineNoFollowUpStatusIdempotency} from './asana-completion-guard.js';
const decision='Keine aktive Nacharbeit oder separate Folgeaufgabe aus dieser Kalenderinstanz nötig.';
const meta='Verifiziert: Task 1234567890123456 offen, Test-Agent zugewiesen, Routine-Tag vorhanden und seit 07:20 Uhr Berlin fällig.';
const inspect=text=>detectRoutineFollowUpSignals({finalComment:{text}});
test('source task identity and due readback do not imply follow-up coverage',()=>{
 for(const text of [meta,meta.replace(' und seit 07:20 Uhr Berlin fällig',''),meta.replace('Test-Agent','Operations'),meta.replace(' offen',' ist offen')]){
  const r=inspect(decision+'\n'+text);assert.equal(r.has_existing_task_coverage_claim,false);assert.equal(r.no_follow_up_claim,true);assert.equal(r.blocked_without_follow_up_task,false);
 }
});
test('scoped negative decisions agree with existing comment dedupe',()=>{
 for(const scope of ['Kalenderinstanz','Test-Instanz','Routineinstanz']){
  const text=decision.replace('Kalenderinstanz',scope);assert.equal(hasRoutineNoFollowUpDecision(text),true);assert.equal(inspect(text).no_follow_up_claim,true);
  const result=inspectRoutineNoFollowUpStatusIdempotency({stories:[{gid:'123',created_by:{gid:'456'},text:text+'\nEvidenz / Verifikation\nAlle Readbacks sind ok.'}],agentUserGid:'456',proposedText:text});assert.equal(result.allowed,false);
 }
});
test('questions uncertainty and non-creation are not accepted decisions',()=>{
 for(const text of [decision.replace('nötig.','nötig?'),'Vielleicht ist '+decision.toLowerCase(),'Falls keine aktive Nacharbeit oder separate Folgeaufgabe aus dieser Kalenderinstanz nötig ist.', 'Keine aktive Nacharbeit oder separate Folgeaufgabe aus dieser Kalenderinstanz angelegt.'])assert.equal(inspect(text).no_follow_up_claim,false,text);
});
test('metadata-like text never hides obligations or task coverage',()=>{
 for(const tail of [' Die Freigabe bleibt offen.',' Die Folgeaufgabe bleibt erforderlich.',' Die bestehende Aufgabe übernimmt offene Punkte.',' Bitte die Nacharbeit erledigen.'])assert.equal(inspect(decision+'\n'+meta+tail).blocked_without_follow_up_task,true,tail);
 for(const text of ['Verifiziert: Task 123 offen, Test-Agent zugewiesen, Routine-Tag vorhanden und übernimmt die Nacharbeit.','Verifiziert: Task 123 offen, Test-Agent zugewiesen, Routine-Tag vorhanden und eingeplant.'])assert.equal(inspect(decision+'\n'+text).blocked_without_follow_up_task,true,text);
 assert.equal(detectRoutineFollowUpSignals({finalComment:{text:decision+'\n'+meta,html_text:'<a data-asana-gid="123" />'}}).blocked_without_follow_up_task,true);
});
