import assert from "node:assert/strict";
import {
  detectRoutineFollowUpSignals,
  inspectRoutineMaterialCommentIdempotency,
  validateRoutineMaterialCorrection,
  validateRoutineFollowUpTaskContract,
  validateRoutineVisibleFollowUpStatus
} from "../lib/asana-completion-guard.js";
import {
  createAsanaMaterialCommentCoordinator,
  isRoutineMaterialComment
} from "../lib/asana-material-comment-coordinator.js";

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

const duplicateCommunicationStory = {
  gid: "1218428058256536",
  text: "Vier BCC-Asana-Mails, 0 menschliche Stilbelege. Keine separate Follow-up-Aufgabe nötig. Evidenz / Verifikation. Quelle/Readback: BCC Readback UID 53-56"
};
assert.equal(validateRoutineMaterialCorrection({
  priorStory: duplicateCommunicationStory,
  proposedText: "Vier BCC-Asana-Mails, 0 menschliche Stilbelege. Keine weitere Folgeaufgabe nötig."
}).status, "blocked_correction_delta");
assert.equal(validateRoutineMaterialCorrection({
  priorStory: duplicateCommunicationStory,
  correction: {
    reason: "Der Abschluss wurde bei unveraenderter Datenbasis nur neu formuliert.",
    before: "Keine separate Follow-up-Aufgabe nötig",
    after: "Keine weitere Folgeaufgabe nötig",
    source: "BCC Readback UID 53-56"
  },
  proposedText: "Keine weitere Folgeaufgabe nötig."
}).issues.includes("correction_source_already_in_prior_story"), true);
assert.equal(validateRoutineMaterialCorrection({
  priorStory: duplicateCommunicationStory,
  correction: {
    reason: "Der Abschlussclaim wurde nur umformuliert und nicht sachlich verändert.",
    before: "0 menschliche Stilbelege",
    after: "0 menschliche Stilbelege",
    source: "Unveränderter BCC-Readback"
  },
  proposedText: "Vier BCC-Asana-Mails, 0 menschliche Stilbelege."
}).status, "blocked_correction_delta");
assert.deepEqual(validateRoutineMaterialCorrection({
  priorStory: {
    gid: "1217000000000010",
    text: "Ergebnis: 4 Datensaetze. Evidenz / Verifikation"
  },
  correction: {
    reason: "Der neue Zielsystem-Readback korrigiert die zuvor falsch angegebene Anzahl.",
    before: "4 Datensaetze",
    after: "5 Datensaetze",
    source: "Asana Task-Readback 2026-09-13 12:20 CEST"
  },
  proposedText: "Korrigiertes Ergebnis: 5 Datensaetze."
}), {
  allowed: true,
  status: "correction_delta_present",
  issues: [],
  supersedes_story_gid: "1217000000000010"
});

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

const coordinator = createAsanaMaterialCommentCoordinator();
let releaseFirst;
const firstCanPost = new Promise((resolve) => {
  releaseFirst = resolve;
});
const firstCoordinatedPost = coordinator.run("agent:task", async ({ recentStories, rememberStory }) => {
  assert.equal(recentStories.length, 0);
  await firstCanPost;
  rememberStory(closedEvidenceStory);
  return "posted";
});
const secondCoordinatedPost = coordinator.run("agent:task", async ({ recentStories }) =>
  inspectRoutineMaterialCommentIdempotency({
    stories: recentStories,
    agentUserGid: closedEvidenceStory.created_by.gid
  })
);
releaseFirst();
assert.equal(await firstCoordinatedPost, "posted");
const secondCoordinatedResult = await secondCoordinatedPost;
assert.deepEqual(
  {
    allowed: secondCoordinatedResult.allowed,
    status: secondCoordinatedResult.status
  },
  { allowed: false, status: "blocked_duplicate_material_comment" }
);
assert.equal(coordinator.pendingCount(), 0);

assert.equal(
  isRoutineMaterialComment({
    routineLike: true,
    commentKind: "status",
    materialResultSignals: true
  }),
  true
);
assert.equal(
  isRoutineMaterialComment({
    routineLike: true,
    commentKind: "status",
    materialResultSignals: false
  }),
  false
);
assert.equal(
  isRoutineMaterialComment({
    routineLike: false,
    commentKind: "completion",
    materialResultSignals: true
  }),
  false
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
    routine_material_comment_concurrency_guard: "ok",
    routine_material_status_signal_guard: "ok",
    routine_existing_task_coverage_detection: "ok",
    finance_no_follow_up_phrase_detection: "ok",
    routine_visible_follow_up_status: "ok",
    routine_follow_up_readback_contract: "ok"
  })
);
