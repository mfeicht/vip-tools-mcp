import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
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
const bccNoFollowUpRestatement = validateRoutineMaterialCorrection({
  priorStory: {
    gid: "1218442568303874",
    text: "Für diesen read-only Lernlauf ist kein aktives Follow-up nötig; die nächste tägliche Routine übernimmt neue Inbox-UIDs. Evidenz / Verifikation"
  },
  correction: {
    reason: "Der neue Null-Readback soll die Fortsetzung ausführlicher beschreiben.",
    before: "Für diesen read-only Lernlauf ist kein aktives Follow-up nötig",
    after: "Keine weitere Folgeaufgabe oder Nacharbeit nötig: Es liegt kein menschlicher Lernbeleg und kein offener Schritt aus diesem Routine-Scope vor",
    source: "Frischer BCC-Null-Readback nach UID 57 um 09:22Z"
  },
  proposedText: "Keine weitere Folgeaufgabe oder Nacharbeit nötig: Es liegt kein menschlicher Lernbeleg und kein offener Schritt aus diesem Routine-Scope vor"
});
assert.equal(bccNoFollowUpRestatement.allowed, false);
assert.equal(bccNoFollowUpRestatement.issues.includes("correction_restates_no_follow_up"), true);
assert.equal(validateRoutineMaterialCorrection({
  priorStory: { gid: "1217000000000011", text: "Follow-up: keines erforderlich. Evidenz / Verifikation" },
  correction: {
    reason: "Ein weiterer Readback bestätigte denselben Abschlusszustand erneut.",
    before: "Follow-up: keines erforderlich",
    after: "Keine weitere Folgeaufgabe nötig",
    source: "Neuer Zielsystem-Null-Readback 2026-09-14"
  },
  proposedText: "Keine weitere Folgeaufgabe nötig"
}).issues.includes("correction_restates_no_follow_up"), true);
assert.equal(validateRoutineMaterialCorrection({
  priorStory: { gid: "1218438096068915", text: "Kein neues High/Critical-Finance-Signal, keine Signal-to-Action- oder Risk-Overlay-Eskalation. Evidenz / Verifikation" },
  correction: {
    reason: "Der erste Kommentar ließ den ausdrücklichen Follow-up-Status aus.",
    before: "Kein neues High/Critical-Finance-Signal, keine Signal-to-Action- oder Risk-Overlay-Eskalation",
    after: "Kein neues High/Critical-Finance-Signal, keine Signal-to-Action- oder Risk-Overlay-Eskalation. Follow-up: keines erforderlich",
    source: "Frischer Finance-Snapshot mit Follow-up-Readback 2026-09-14"
  },
  proposedText: "Kein neues High/Critical-Finance-Signal, keine Signal-to-Action- oder Risk-Overlay-Eskalation. Follow-up: keines erforderlich"
}).allowed, true);
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

// A prior Finance result can be followed by one status-only decision when its
// visible follow-up sentence was omitted. The second story must not repeat the
// material result, and the completion gate must see its new decision.
const financeResultWithoutFollowUp = {
  gid: "1218696346033144",
  created_by: { gid: "1215003777954631" },
  text: "Execution Quality – 21.09.2026\nKeine offenen Orders.\nEvidenz / Verifikation\nBroker-No-op 14:05 UTC"
};
const financeFollowUpStatus = {
  gid: "1218696346033999",
  created_by: { gid: "1215003777954631" },
  text: "Follow-up: keines erforderlich. Aus diesem Routine-Scope ist keine aktive Nacharbeit fuer eine andere Person offen.\nEvidenz / Verifikation\nQuelle/Readback: Finance-Ergebnisstory 1218696346033144"
};
assert.equal(inspectRoutineMaterialCommentIdempotency({
  stories: [financeResultWithoutFollowUp],
  agentUserGid: "1215003777954631"
}).status, "blocked_duplicate_material_comment");
assert.equal(isRoutineMaterialComment({
  routineLike: true,
  commentKind: "status",
  materialResultSignals: false
}), false);
assert.equal(validateRoutineVisibleFollowUpStatus({
  finalComment: financeFollowUpStatus,
  hasFollowUpTask: false
}).ok, true);
assert.equal(detectRoutineFollowUpSignals({
  finalComment: financeFollowUpStatus,
  completionBasis: "Execution-Quality-Scope bearbeitet; keine aktive Nacharbeit aus dieser Instanz.",
  followUpNotRequiredBasis: "Keine aktive Nacharbeit aus dem Routine-Scope, da keine offenen Orders und kein eigener neuer Arbeitsschritt belegt sind."
}).blocked_without_follow_up_task, false);

// Actual closed-evidence Retail story and the Review reproductions must agree
// across plain text and HTML. Separate evidence rows are not task coverage.
const retailStory = JSON.parse(readFileSync(new URL("./fixtures/retail-completion-story-1218613928272290.json", import.meta.url), "utf8"));
const evidenceRows = "Eigene faellige Routine\nQuellenversuche und Scope-Output vorhanden";
for (const finalComment of [
  retailStory,
  { text: `Follow-up: keines erforderlich.\n${evidenceRows}` },
  { html_text: "<body>Follow-up: keines erforderlich.<ul><li>Eigene faellige Routine</li><li>Quellenversuche und Scope-Output vorhanden</li></ul></body>" },
  { text: `Follow-up: keines erforderlich.\n${evidenceRows}`, html_text: "<body>Follow-up: keines erforderlich.<ul><li>Eigene faellige Routine</li><li>Quellenversuche und Scope-Output vorhanden</li></ul></body>" }
]) {
  const signals = detectRoutineFollowUpSignals({ finalComment });
  assert.equal(signals.no_follow_up_claim, true);
  assert.equal(signals.has_existing_task_coverage_claim, false);
  assert.equal(signals.blocked_without_follow_up_task, false);
}
for (const ending of ["", "e", "en", "er", "es", "em"]) {
  for (const separator of ["-", " "]) {
    assert.equal(detectRoutineFollowUpSignals({
      finalComment: { text: `Task${separator}spezifisch${ending} Szenariodossiers und Quellenreadback vorhanden. Follow-up: keines erforderlich.` }
    }).blocked_without_follow_up_task, false);
  }
}
for (const html_text of [
  "<body>Follow-up: keines erforderlich.<p>Eigene faellige Routine</p><p>Quellenversuche und Scope-Output vorhanden</p></body>",
  "<body>Follow-up: keines erforderlich.<div>Eigene faellige Routine</div><div>Quellenversuche und Scope-Output vorhanden</div></body>",
  "<body>Follow-up: keines erforderlich.<strong>Eigene faellige Routine</strong><br>Quellenversuche und Scope-Output vorhanden</body>"
]) {
  assert.equal(detectRoutineFollowUpSignals({ finalComment: { html_text } }).blocked_without_follow_up_task, false);
}
for (const html_text of [
  "<body>Follow-up: keines erforderlich.<ul><li>Die bestehende <strong>Routine</strong> uebernimmt die Nacharbeit.</li></ul></body>",
  "<body>Follow-up: keines erforderlich.<p>Die Routine &uuml;bernimmt die Nacharbeit.</p></body>",
  "<body>Follow-up: keines erforderlich.<p>Der vorhandene Task ist eingeplant.</p></body>",
  '<body>Follow-up: keines erforderlich.<a data-asana-gid="1108801330389276"/></body>'
]) {
  assert.equal(detectRoutineFollowUpSignals({ finalComment: { html_text } }).blocked_without_follow_up_task, true);
}

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

// Natural Sales completion stories use a heading or a coordinated negative
// phrase. Both must be visible no-follow-up claims without creating a dummy task.
for (const text of [
  "Follow-up\nKeines erforderlich: Der Routine-Scope einschließlich Import ist erledigt. Die reguläre Folgeinstanz wird nach dem Abschluss live geprüft.",
  "Folgearbeit\nKeine weitere Recherche oder Folgeaufgabe erforderlich; die reguläre Routinefolge wird nach dem Abschluss live geprüft."
]) {
  const finalComment = { text: `${text}\nEvidenz / Verifikation\nDer lokale Pflichtimport meldete success_count=1 und error_count=0.` };
  const signals = detectRoutineFollowUpSignals({ finalComment });
  assert.equal(signals.no_follow_up_claim, true);
  assert.equal(signals.blocked_without_follow_up_task, false);
  assert.equal(validateRoutineVisibleFollowUpStatus({ finalComment, hasFollowUpTask: false }).ok, true);
  assert.equal(detectRoutineFollowUpSignals({
    finalComment: { text: `${text} Die bestehende Routine uebernimmt die Nacharbeit.` }
  }).blocked_without_follow_up_task, true);
}
for (const text of ["Follow-up\nKeines nachgewiesen.", "Keine weitere Recherche oder Folgeaufgabe angelegt."]) {
  assert.equal(validateRoutineVisibleFollowUpStatus({ finalComment: { text }, hasFollowUpTask: false }).ok, false);
}

// Actual Sales result and first clarification from 1218769088476682 already
// make the no-follow-up decision visible; a third decision is redundant.
for (const text of [
  "Keine manuelle Folgeaufgabe oder Nacharbeit ist für diesen Lauf erforderlich. Die reguläre Folgeinstanz wird nach der Completion per Live-Readback geprüft.",
  "Für diesen abgeschlossenen Lauf besteht keine aktive Folgeaufgabe und keine noch ausstehende Handlung für eine Person oder ein System."
]) {
  const finalComment = { text: `${text}\nEvidenz / Verifikation\nDer Lead-Scope ist importiert und zurückgelesen.` };
  const signals = detectRoutineFollowUpSignals({ finalComment });
  assert.equal(signals.no_follow_up_claim, true);
  assert.equal(signals.blocked_without_follow_up_task, false);
  assert.equal(validateRoutineVisibleFollowUpStatus({ finalComment, hasFollowUpTask: false }).ok, true);
}
assert.equal(validateRoutineVisibleFollowUpStatus({
  finalComment: { text: "Keine manuelle Folgeaufgabe erforderlich, aber automatische Nacharbeit ist noch offen." },
  hasFollowUpTask: false
}).ok, false);

// Actual Sales result wording from story 1218717124815791 must not be
// misread as active work merely because an earlier sentence mentions import.
const salesImportedResultWithCompoundFollowUp = {
  text:
    "6 neue Leads wurden erfolgreich importiert.\n" +
    "Keine weitere Nacharbeit oder Follow-up-Aufgabe erforderlich.\n" +
    "Nächster Suchraum: Für die nächste Routine andere Regionen priorisieren.\n" +
    "Evidenz / Verifikation\nLive-Readbacks bestätigen den Task-Scope."
};
const salesCompoundFollowUpSignals = detectRoutineFollowUpSignals({
  finalComment: salesImportedResultWithCompoundFollowUp,
  completionBasis:
    "The canonical result story records six successfully imported leads and the task-specific clarification confirms that no follow-up work is required.",
  followUpNotRequiredBasis:
    "Story 1218717300136922 states that no follow-up task is required for this task scope."
});
assert.equal(salesCompoundFollowUpSignals.has_action_signal, true);
assert.equal(salesCompoundFollowUpSignals.no_follow_up_claim, true);
assert.equal(salesCompoundFollowUpSignals.has_existing_task_coverage_claim, false);
assert.equal(salesCompoundFollowUpSignals.blocked_without_follow_up_task, false);
assert.equal(validateRoutineVisibleFollowUpStatus({
  finalComment: salesImportedResultWithCompoundFollowUp,
  hasFollowUpTask: false
}).ok, true);
assert.equal(validateRoutineVisibleFollowUpStatus({
  finalComment: { text: "Keine weitere Follow-up-Aufgabe angelegt." },
  hasFollowUpTask: false
}).ok, false);
assert.equal(validateRoutineMaterialCorrection({
  priorStory: { gid: "1218717124815791", text: "Keine weitere Follow-up-Aufgabe erforderlich." },
  correction: {
    reason: "Der sichtbare Abschlussstatus soll ohne neue Evidenz nur umformuliert werden.",
    before: "Keine weitere Follow-up-Aufgabe erforderlich",
    after: "Keine weitere Folgeaufgabe nötig",
    source: "Unveränderter Sales-Ergebnisreadback"
  },
  proposedText: "Keine weitere Folgeaufgabe nötig."
}).issues.includes("correction_restates_no_follow_up"), true);

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

// Event-Pulse completion story 1218491009441385 already states its no-follow-up
// status. Reuse that story instead of allowing a third material restatement.
const legacyEventPulseText = "Keine eigenständige neue Folgeaufgabe erforderlich.";
for (const text of [legacyEventPulseText, "Keine eigenstaendige neue Folgeaufgabe erforderlich."]) {
  assert.equal(validateRoutineVisibleFollowUpStatus({
    finalComment: { text }, hasFollowUpTask: false
  }).ok, true);
}
for (const text of [
  "Eine eigenständige neue Folgeaufgabe erforderlich.",
  "Keine eigenständige neue Folgeaufgabe bereits angelegt.",
  "Keine eigenständige neue Folgeaufgabe möglicherweise erforderlich.",
  "Keine eigenständige neue Folgeaufgabe nicht erforderlich.",
  "Keine eigenständige neue Folgeaufgabe NICHT nötig."
]) {
  assert.equal(validateRoutineVisibleFollowUpStatus({
    finalComment: { text }, hasFollowUpTask: false
  }).ok, false);
}
assert.equal(detectRoutineFollowUpSignals({
  finalComment: { text: legacyEventPulseText, html_text: '<body><a data-asana-gid="1108801330389276"/></body>' }
}).blocked_without_follow_up_task, true);
for (const coverage of [
  "Die naechste Routine uebernimmt die weitere Bearbeitung.",
  "Die nächste Routine übernimmt die weitere Bearbeitung.",
  "Die Routine stellt sicher, dass die Bearbeitung erfolgt.",
  "Übernimmt die nächste Routine die Bearbeitung?"
]) {
  assert.equal(detectRoutineFollowUpSignals({
    finalComment: { text: `${legacyEventPulseText} ${coverage}` }
  }).blocked_without_follow_up_task, true);
}
assert.equal(detectRoutineFollowUpSignals({
  finalComment: { text: `${legacyEventPulseText} Die Routine unübernimmt keine Arbeit.` }
}).has_existing_task_coverage_claim, false);
assert.equal(validateRoutineMaterialCorrection({
  priorStory: { gid: "1218491009441385", text: legacyEventPulseText },
  correction: {
    reason: "Der bereits sichtbare Abschlussstatus soll lediglich präzisiert werden.",
    before: legacyEventPulseText,
    after: "Keine weitere Folgeaufgabe oder Nacharbeit nötig",
    source: "Erneuter schreibfreier Event-Pulse-Readback 2026-09-16"
  },
  proposedText: "Keine weitere Folgeaufgabe oder Nacharbeit nötig"
}).issues.includes("correction_restates_no_follow_up"), true);

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

const workerC = JSON.parse(readFileSync(new URL("./fixtures/worker-c-followup-story-1218568446337764.json", import.meta.url), "utf8"));
const workerCSource = { gid: "1218388727116488", memberships: workerC.followup.memberships };
assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: workerC.followup, finalComment: workerC.story }).ok, true);
for (const dueText of ["20.09.2026", "20.9.2026", "2026-09-20"]) {
  const finalComment = { text: `${workerC.followup.gid}; Assignee ${workerC.followup.assignee.gid}; Status Todo; faellig ${dueText}.` };
  assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: workerC.followup, finalComment }).ok, true);
}
for (const dueText of ["19.09.2026", "20.09.", "morgen", "120.09.2026", "20.09.20260", "31.02.2026"]) {
  const finalComment = { text: `${workerC.followup.gid}; Assignee ${workerC.followup.assignee.gid}; Status Todo; faellig ${dueText}.` };
  assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: workerC.followup, finalComment }).issues.includes("final_comment_missing_follow_up_due_readback"), true);
}
assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: { ...workerC.followup, due_on: "2026-03-03" }, finalComment: { ...workerC.story, text: workerC.story.text.replaceAll("20.09.2026", "31.02.2026"), html_text: "" } }).ok, false);
assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: { ...workerC.followup, completed: true }, finalComment: workerC.story }).ok, false);
assert.equal(validateRoutineFollowUpTaskContract({ sourceTask: workerCSource, followUpTask: { ...workerC.followup, assignee: null }, finalComment: workerC.story }).ok, false);

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
    retail_html_evidence_and_task_artifact_regressions: "ok",
    finance_no_follow_up_phrase_detection: "ok",
    routine_visible_follow_up_status: "ok",
    routine_follow_up_readback_contract: "ok"
  })
);
