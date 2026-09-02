import assert from "node:assert/strict";

import {
  isRoutineInstanceMissingAlarmIntent,
  sameAsanaDateTimeInstant
} from "../lib/asana-schedule-guards.js";

assert.equal(
  isRoutineInstanceMissingAlarmIntent({
    name: "Finance-Orchestrierung: Execution-Quality-Routine Folgeinstanz fehlt"
  }),
  true
);
assert.equal(
  isRoutineInstanceMissingAlarmIntent({
    name: "Folgeinstanz der Routine wurde nicht erstellt"
  }),
  true
);
assert.equal(
  isRoutineInstanceMissingAlarmIntent({
    name: "R: Kontrolliertes Mac-System- und Tool-Wartungsfenster",
    description:
      "Nach Abschluss die Folgeinstanz verifizieren. Bei fehlendem Recovery-Pfad keine Installation ausfuehren.",
    creation_basis: "Monatliche Routine auf direkte Anweisung anlegen."
  }),
  false
);
assert.equal(
  isRoutineInstanceMissingAlarmIntent({
    name: "R: Monatliche Routine",
    description: "Fehlende Zugangsdaten als Blocker dokumentieren."
  }),
  false
);

assert.equal(
  sameAsanaDateTimeInstant("2026-10-01T00:00:00.000Z", "2026-10-01T02:00:00+02:00"),
  true
);
assert.equal(
  sameAsanaDateTimeInstant("2026-10-01T00:00:01.000Z", "2026-10-01T02:00:00+02:00"),
  false
);
assert.equal(sameAsanaDateTimeInstant(null, "2026-10-01T02:00:00+02:00"), false);

console.log("asana schedule guard self-test passed");
