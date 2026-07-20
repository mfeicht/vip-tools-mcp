import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = await readFile(new URL("../server.js", import.meta.url), "utf8");

assert.match(source, /ASANA_RECURRENCE_GENERATION_GRACE_SECONDS/);
assert.match(source, /verification_status = "pending_generation"/);
assert.match(source, /incident_classification = "pending_asana_generation"/);
assert.match(source, /alert_task_creation_allowed: alertTaskCreationAllowed/);
assert.match(source, /keine Asana-Orchestrierungsaufgabe, keinen Blocker und keinen Kommentar erzeugen/);
assert.match(source, /Orchestrierungsalarm als Fehlalarm blockiert/);
assert.match(source, /Die belastbare Workspace-Zweitsuche ist nicht verfuegbar/);
assert.match(source, /Asana-Generierungsfrist laeuft noch/);

const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
assert.equal(
  packageJson.scripts["test:recurrence-settle"],
  "node scripts/recurrence-settle-selftest.mjs"
);

console.log("recurrence settle self-test passed");
