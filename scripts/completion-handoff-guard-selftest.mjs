import assert from "node:assert/strict";
import {
  detectRoutineFollowUpSignals,
  inspectRoutineMaterialCommentIdempotency,
  validateRoutineFollowUpTaskContract,
  validateRoutineVisibleFollowUpStatus
} from "../lib/asana-completion-guard.js";

const closedEvidenceStory = {
  gid: "1217000000000001",
  created_at: "2026-08-23T08:00:00.000Z",
  created_by: { gid: "1214979008788676" },
  text: "Ergebnis\nEvidenz / Verifikation\nAlle Readbacks sind ok."
};
const openEvidenceStory = {
  gid: "1217000000000002",
  created_at: "2026-08-23T07:00:00.000Z",
  created_by: { gid: "1214979008788676" },
  text:
    "Status\nEvidenz / Verifikation\nDer Zwischenstand ist belegt.\nOffene Evidenzluecken\nDeployment fehlt."
};

assert.deepEqual(
  inspectRoutineMaterialCommentIdempotency({
    stories: [closedEvidenceStory],
    agentUserGid: "1214979008788676"
  }).status,
  "blocked_duplicate_material_comment"
);
assert.equal(
  inspectRoutineMaterialCommentIdempotency({
    stories: [closedEvidenceStory],
    agentUserGid: "1214979008788676",
    supersedesStoryGid: closedEvidenceStory.gid
  }).allowed,
  true
);
assert.equal(
  inspectRoutineMaterialCommentIdempotency({
    stories: [closedEvidenceStory],
    agentUserGid: "1214979008788676",
    supersedesStoryGid: "1217000000000999"
  }).allowed,
  false
);
assert.deepEqual(
  inspectRoutineMaterialCommentIdempotency({
    stories: [openEvidenceStory],
    agentUserGid: "1214979008788676",
    supersedesStoryGid: openEvidenceStory.gid
  }),
  {
    status: "allowed_explicit_correction",
    allowed: true,
    prior_material_story_gids: [],
    supersedes_story_gid: openEvidenceStory.gid
  }
);
assert.equal(
  inspectRoutineMaterialCommentIdempotency({
    stories: [openEvidenceStory, closedEvidenceStory],
    agentUserGid: "1214979008788676",
    supersedesStoryGid: openEvidenceStory.gid
  }).allowed,
  true
);
assert.equal(
  inspectRoutineMaterialCommentIdempotency({
    stories: [
      {
        ...closedEvidenceStory,
        text: `${closedEvidenceStory.text}\nOffene Evidenzluecken\nDeployment fehlt.`
      }
    ],
    agentUserGid: "1214979008788676"
  }).status,
  "first_material_comment"
);

const coverageSignal = detectRoutineFollowUpSignals({
  finalComment: { text: "Die bestehende Routine deckt die Nacharbeit ab.", html_text: "" },
  completionBasis: "Die künftige Routine ist eingeplant.",
  followUpNotRequiredBasis: ""
});
assert.equal(coverageSignal.has_existing_task_coverage_claim, true);
assert.equal(coverageSignal.blocked_without_follow_up_task, true);

const contradictoryCoverageSignal = detectRoutineFollowUpSignals({
  finalComment: { text: "Die bestehende Routine deckt die Nacharbeit ab.", html_text: "" },
  completionBasis: "Der vorhandene Task stellt die weitere Ausfuehrung sicher.",
  followUpNotRequiredBasis: "Keine weitere Folgeaufgabe erforderlich."
});
assert.equal(contradictoryCoverageSignal.no_follow_up_claim, true);
assert.equal(contradictoryCoverageSignal.has_existing_task_coverage_claim, true);
assert.equal(contradictoryCoverageSignal.blocked_without_follow_up_task, true);

const noFollowUpSignal = detectRoutineFollowUpSignals({
  finalComment: { text: "Keine weitere Folgeaufgabe erforderlich.", html_text: "" },
  completionBasis: "Die Aufgabe ist vollstaendig abgeschlossen.",
  followUpNotRequiredBasis: "Keine weitere Folgeaufgabe erforderlich, da kein offener Punkt verbleibt."
});
assert.equal(noFollowUpSignal.no_follow_up_claim, true);
assert.equal(noFollowUpSignal.blocked_without_follow_up_task, false);

const coordinatedNoFollowUpSignal = detectRoutineFollowUpSignals({
  finalComment: {
    text:
      "Der abgegrenzte 10:30-Lead-Routine-Scope ist vollständig erledigt. " +
      "Keine weitere Folgeaufgabe oder Nacharbeit ist nötig, weil Sheet-Deduplizierung, Survivor-Readback und lokaler WordPress-Import bestätigt sind."
  },
  completionBasis: "Der abgegrenzte Routine-Scope ist vollständig abgeschlossen.",
  followUpNotRequiredBasis:
    "Keine weitere Folgeaufgabe oder Nacharbeit ist nötig, weil alle Readbacks geschlossen sind."
});
assert.equal(coordinatedNoFollowUpSignal.no_follow_up_claim, true);
assert.equal(coordinatedNoFollowUpSignal.has_action_signal, true);
assert.equal(coordinatedNoFollowUpSignal.blocked_without_follow_up_task, false);

const financeNoFollowUpSignal = detectRoutineFollowUpSignals({
  finalComment: {
    text:
      "Follow-up: keines erforderlich, weil kein neuer eigenstaendiger Arbeitsgegenstand entstanden ist.\n" +
      "Verifiziert: Finance ist Assignee und Creator; Routine-Tag und Projekt sind vorhanden.\n" +
      "Quelle/Readback: Asana-Task-Readback 1217842217630584."
  },
  completionBasis: "Der definierte Instanzscope ist vollstaendig abgeschlossen.",
  followUpNotRequiredBasis:
    "Kein Follow-up erforderlich, weil evidence.unresolved=[] und kein eigenstaendiger Arbeitsgegenstand entstanden ist."
});
assert.equal(financeNoFollowUpSignal.no_follow_up_claim, true);
assert.equal(financeNoFollowUpSignal.has_existing_task_coverage_claim, false);
assert.equal(financeNoFollowUpSignal.blocked_without_follow_up_task, false);

const explicitCoverageSignal = detectRoutineFollowUpSignals({
  finalComment: { text: "Die naechste Routine uebernimmt die weitere Bearbeitung.", html_text: "" },
  completionBasis: "Der Scope dieser Instanz ist abgeschlossen.",
  followUpNotRequiredBasis: "Keine weitere Folgeaufgabe erforderlich."
});
assert.equal(explicitCoverageSignal.has_existing_task_coverage_claim, true);
assert.equal(explicitCoverageSignal.blocked_without_follow_up_task, true);

assert.deepEqual(
  validateRoutineVisibleFollowUpStatus({
    finalComment: { text: "Evidenz / Verifikation\nAlle Readbacks sind gruen." },
    hasFollowUpTask: false
  }).issues,
  ["final_comment_missing_visible_no_follow_up_status"]
);
assert.equal(
  validateRoutineVisibleFollowUpStatus({
    finalComment: {
      text: "Follow-up-Status\nKeine weitere Folgeaufgabe erforderlich; es bleibt keine Nacharbeit offen."
    },
    hasFollowUpTask: false
  }).ok,
  true
);
assert.equal(
  validateRoutineVisibleFollowUpStatus({
    finalComment: {
      text: "Keine weitere Folgeaufgabe oder Nacharbeit ist nötig, weil der Routine-Scope vollständig abgeschlossen ist."
    },
    hasFollowUpTask: false
  }).ok,
  true
);
assert.equal(
  validateRoutineVisibleFollowUpStatus({
    finalComment: { text: "Follow-up https://app.asana.com/0/0/1217000000000300" },
    hasFollowUpTask: true
  }).mode,
  "follow_up_task_readback"
);

const sourceTask = {
  gid: "1217000000000100",
  memberships: [{ project: { gid: "1217000000000200", name: "(VIP) AI-Buero" } }]
};
const followUpTask = {
  gid: "1217000000000300",
  name: "Operations-Folgeaufgabe",
  completed: false,
  assignee: { gid: "1214979008788676", name: "VIP AI-Operations" },
  due_on: "2026-08-26",
  permalink_url: "https://app.asana.com/0/1217000000000200/1217000000000300",
  memberships: [{ project: { gid: "1217000000000200", name: "(VIP) AI-Buero" } }],
  custom_fields: [{ name: "Status", enum_value: { name: "Todo" } }]
};
const validFollowUpContract = validateRoutineFollowUpTaskContract({
  sourceTask,
  followUpTask,
  finalComment: {
    text:
      "Follow-up https://app.asana.com/0/1217000000000200/1217000000000300; Assignee VIP AI-Operations; Status Todo; faellig 2026-08-26."
  }
});
assert.equal(validFollowUpContract.ok, true);
assert.deepEqual(validFollowUpContract.issues, []);

const missingReadbackContract = validateRoutineFollowUpTaskContract({
  sourceTask,
  followUpTask,
  finalComment: { text: "Eine Folgeaufgabe existiert." }
});
assert.equal(missingReadbackContract.ok, false);
assert.deepEqual(
  missingReadbackContract.issues.sort(),
  [
    "final_comment_missing_follow_up_assignee_readback",
    "final_comment_missing_follow_up_due_readback",
    "final_comment_missing_follow_up_status_readback",
    "final_comment_missing_follow_up_task_link_or_gid"
  ].sort()
);

console.log(
  JSON.stringify({
    routine_material_comment_idempotency: "ok",
    routine_existing_task_coverage_detection: "ok",
    finance_no_follow_up_phrase_detection: "ok",
    routine_visible_follow_up_status: "ok",
    routine_follow_up_readback_contract: "ok"
  })
);
