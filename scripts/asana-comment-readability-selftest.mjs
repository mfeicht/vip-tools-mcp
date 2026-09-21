import assert from "node:assert/strict";
import { inspectAsanaCommentReadability } from "../lib/asana-comment-readability.js";

const financeFinding = inspectAsanaCommentReadability({
  sections: [{
    title: "Begrenzte Wochenforensik dokumentiert",
    paragraphs: [
      "Die fokussierte Preisstichprobe umfasst98/98 Namen mit Tagespreisstand18.09.2026; Validierung68/100. Werte wurden nicht aus20D-Werten erfunden."
    ],
    bullets: [
      "Quelle/Readback: Aktueller Task-Readback18Sep21:24UTC",
      "Coverage-Tests27/27 bestanden"
    ]
  }],
  effortNote: "Aufwand: etwa5 Minuten."
});
assert.equal(financeFinding.status, "warning_suspicious_alphanumeric_boundaries");
for (const expectedToken of [
  "umfasst98/98",
  "Tagespreisstand18.09.2026",
  "Validierung68/100",
  "aus20D-Werten",
  "Task-Readback18Sep21:24UTC",
  "Coverage-Tests27/27",
  "etwa5"
]) {
  assert.equal(financeFinding.issues.some((issue) => issue.token === expectedToken), true, expectedToken);
}

const operationsFinding = inspectAsanaCommentReadability({
  sections: [{
    paragraphs: [
      "Toolliste114 und Identitaet1214979008788676 verifiziert.",
      "Worker C am17.09.20:19Berlin hatte0Abschluesse/1Guardblocker.",
      "System-Health12:34:57CEST; exact555e35de8de85bf6f1af5ed42b902dfdd83e0326."
    ]
  }]
});
assert.equal(operationsFinding.status, "warning_suspicious_alphanumeric_boundaries");
for (const expectedToken of [
  "Toolliste114",
  "Identitaet1214979008788676",
  "am17.09.20:19Berlin",
  "hatte0Abschluesse/1Guardblocker",
  "System-Health12:34:57CEST",
  "exact555e35de8de85bf6f1af5ed42b902dfdd83e0326"
]) {
  assert.equal(operationsFinding.issues.some((issue) => issue.token === expectedToken), true, expectedToken);
}

const readableControl = inspectAsanaCommentReadability({
  sections: [{
    title: "Aktueller Abschluss",
    paragraphs: [
      "Vier Pflichtimporte sind abgeschlossen. Die Instanz lief am 19.09. um 09:00 CEST.",
      "Commit 67178ebb8ddf384e4cf96fdda2792ae333ad55da, P0/P1, TLS1.3 und SHA256 sind technische Tokens."
    ],
    bullets: [
      "Quelle/Readback: Akquise!A2858:J2870",
      "Quelle/Readback: https://example.com/reports/2026Q3",
      "Verifiziert: 20D ist hier ein absichtlich kompakter Zeitraumtoken."
    ],
    code_blocks: ["Toolliste114 bleibt als bewusst gezeigtes Rohbeispiel unveraendert."]
  }]
});

const richTextFinding = inspectAsanaCommentReadability({
  sections: [{
    paragraphs: [{ runs: [
      { text: "Bitte pruefen: ", style: "plain" },
      { text: "Deadline22.09.", style: "strong" }
    ] }],
    numbered: ["Aktion1 sofort pruefen"]
  }]
});
assert.equal(richTextFinding.issue_count >= 2, true);
assert.equal(richTextFinding.issues.some((issue) => issue.path.includes("numbered")), true);
assert.deepEqual(
  {
    status: readableControl.status,
    issue_count: readableControl.issue_count,
    code_blocks_excluded: readableControl.code_blocks_excluded,
    external_locators_excluded: readableControl.external_locators_excluded
  },
  {
    status: "ok",
    issue_count: 0,
    code_blocks_excluded: true,
    external_locators_excluded: true
  }
);

console.log(JSON.stringify({ financeFinding, operationsFinding, readableControl }, null, 2));
