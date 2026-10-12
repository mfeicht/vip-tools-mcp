import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { detectRoutineFollowUpSignals, validateRoutineVisibleFollowUpStatus } from "../lib/asana-completion-guard.js";

const sentence = "Keine aktive Nacharbeit oder separate Folgeaufgabe ist für den Monatsentscheid erforderlich.";
const fixture = JSON.parse(readFileSync(new URL("./fixtures/monthly-no-followup-story.json", import.meta.url), "utf8"));

test("original monthly story passes visible status and action-signal guard", () => {
  for (const finalComment of [fixture, { text: fixture.text }, { html_text: fixture.html_text }]) {
    assert.equal(validateRoutineVisibleFollowUpStatus({ finalComment, hasFollowUpTask: false }).ok, true);
    assert.equal(detectRoutineFollowUpSignals({ finalComment }).blocked_without_follow_up_task, false);
  }
});

test("explicit exact monthly decision also permits noetig transliteration", () => {
  assert.equal(detectRoutineFollowUpSignals({ finalComment: { text: sentence.replace("für", "fuer").replace("erforderlich", "noetig") } }).no_follow_up_claim, true);
});

test("uncertain, future or affirmative work decisions remain unrecognized", () => {
  for (const text of [sentence.replace("ist für", "ist möglicherweise für"), sentence.replace("ist für", "ist voraussichtlich für"), sentence.replace("Keine", "Eine"), "Die Entscheidung über aktive Nacharbeit oder separate Folgeaufgabe für den Monatsentscheid ist noch offen."]) {
    assert.equal(detectRoutineFollowUpSignals({ finalComment: { text } }).no_follow_up_claim, false, text);
  }
});

test("explicit outstanding approval contradicts the monthly decision", () => {
  for (const conjunction of ["aber", "jedoch"]) {
    const text = sentence.replace(/\.$/, `, ${conjunction} ist die Freigabe noch offen.`);
    assert.equal(validateRoutineVisibleFollowUpStatus({ finalComment: { text }, hasFollowUpTask: false }).ok, false);
  }
});

test("real mention and active coverage remain blocking", () => {
  assert.equal(detectRoutineFollowUpSignals({ finalComment: { text: sentence, html_text: '<body><a data-asana-gid="1108801330389276"/> Bitte prüfen.</body>' } }).blocked_without_follow_up_task, true);
  assert.equal(detectRoutineFollowUpSignals({ finalComment: { text: sentence + " Die bestehende Routine übernimmt die Nacharbeit." } }).blocked_without_follow_up_task, true);
});
