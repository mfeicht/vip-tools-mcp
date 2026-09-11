import { strict as assert } from "node:assert";

import { isRoutineTaskCreationIntent } from "../lib/asana-routine-intent.js";

assert.equal(
  isRoutineTaskCreationIntent({
    name: "Finance: Kapazitaetsbasis klaeren",
    description: "Die Routine-Erkennung des Tools pruefen.",
    creation_basis: "Einmaliger Operations-Handoff aus einer Finance-Aufgabe.",
    routine_task: false
  }),
  false,
  "Ein explizites routine_task=false muss Textsignale uebersteuern."
);

assert.equal(
  isRoutineTaskCreationIntent({
    name: "R: Woechentlicher Systemcheck",
    description: "",
    creation_basis: "Kanonische wiederkehrende Aufgabe.",
    routine_task: undefined
  }),
  true,
  "Ein R:-Titel muss ohne expliziten Override als Routine erkannt werden."
);

assert.equal(
  isRoutineTaskCreationIntent({
    name: "Systemcheck einrichten",
    description: "Bitte als wöchentliche Aufgabe anlegen.",
    creation_basis: "Direkte Anweisung.",
    routine_task: undefined
  }),
  true,
  "Textsignale muessen bei fehlendem explizitem Feld weiter funktionieren."
);

assert.equal(
  isRoutineTaskCreationIntent({
    name: "R: Nur historischer Titel",
    description: "Einmalige Reparatur.",
    creation_basis: "Explizit keine Wiederholung.",
    routine_task: false
  }),
  false,
  "Ein expliziter Nicht-Routine-Auftrag darf auch einen irrefuehrenden Titel korrigieren."
);

console.log(
  JSON.stringify({
    explicit_false_authoritative: true,
    explicit_true_and_inference_preserved: true
  })
);
