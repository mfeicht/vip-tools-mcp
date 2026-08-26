import assert from "node:assert/strict";

import {
  classifyAsanaCommentAuthority,
  validateAsanaObserverCommentIntent
} from "../lib/asana-observer-guard.js";

const foreignTask = {
  assignee: { gid: "200", name: "Bettina" },
  created_by: { gid: "300", name: "Moritz" },
  followers: [{ gid: "100", name: "VIP AI-Developer" }]
};
const observerAuthority = classifyAsanaCommentAuthority({
  task: foreignTask,
  agentUserGid: "100"
});
assert.equal(observerAuthority.role, "observer");
assert.equal(observerAuthority.observer_only, true);
assert.equal(
  validateAsanaObserverCommentIntent({ authority: observerAuthority }).status,
  "blocked_observer_without_authority"
);

const explicitInstruction = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "explicit_instruction",
  authorizationReceipt: {
    source: "asana",
    authorization_basis: "task_story",
    authorization_story_mentions_agent: true
  }
});
assert.equal(explicitInstruction.allowed, true);

const unmentionedInstruction = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "explicit_instruction",
  authorizationReceipt: {
    source: "asana",
    authorization_basis: "task_creator",
    authorization_story_mentions_agent: false
  }
});
assert.equal(unmentionedInstruction.allowed, false);

const criticalAnomaly = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "critical_anomaly",
  basis: "Akuter Produktionsausfall mit unmittelbarem Datenverlustrisiko.",
  evidence: {
    verified_claims: ["Der produktive Datenbankendpunkt antwortet nicht."],
    unresolved: ["Wiederherstellungsstatus ist noch offen."]
  },
  commentText: "Kritischer Produktionsausfall ist read-only verifiziert."
});
assert.equal(criticalAnomaly.allowed, true);

const ordinaryTechnicalSignal = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "critical_anomaly",
  basis: "Technisches Update wurde fuer gestern geplant.",
  evidence: {
    verified_claims: ["Termin steht in der Beschreibung."],
    unresolved: ["Zustand danach ist unbekannt."]
  },
  commentText: "Bitte im Standardlauf pruefen."
});
assert.equal(ordinaryTechnicalSignal.allowed, false);

const governanceAllowed = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "governance_scope",
  basis: "Dokumentierter Review-Auftrag fuer diese konkrete Ausgabe.",
  evidence: { verified_claims: ["Pflichtfeld fehlt im aktuellen Readback."] },
  governanceAgent: true
});
assert.equal(governanceAllowed.allowed, true);

const governanceDenied = validateAsanaObserverCommentIntent({
  authority: observerAuthority,
  reason: "governance_scope",
  basis: "Developer moechte die fremde Aufgabe proaktiv begleiten.",
  evidence: { verified_claims: ["Developer ist Follower."] },
  governanceAgent: false
});
assert.equal(governanceDenied.allowed, false);

const ownAuthority = classifyAsanaCommentAuthority({
  task: { ...foreignTask, assignee: { gid: "100" } },
  agentUserGid: "100"
});
assert.equal(
  validateAsanaObserverCommentIntent({ authority: ownAuthority }).status,
  "not_applicable"
);

console.log(
  JSON.stringify({
    follower_only_default_block: "ok",
    explicit_gid_instruction: "ok",
    critical_anomaly_gate: "ok",
    governance_scope_gate: "ok",
    own_task_unaffected: "ok"
  })
);
