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

assert.equal(
  isRoutineTaskCreationIntent({
    name: "Follower-Fachblatt an Live-Gate angleichen",
    description: "Diese Nicht-Routine-Aufgabe erklärt, wie Folgeaufgaben und wiederkehrende Aufgaben geprüft werden.",
    creation_basis: "Einzelauftrag aus dem Review; keine Routine-Aufgabe anlegen."
  }),
  false,
  "Fachtext über Routinen und negierte Aufgabenart darf keinen Routine-Tag auslösen."
);

assert.equal(
  isRoutineTaskCreationIntent({
    name: "Systemcheck einrichten",
    description: "Die bestehende wöchentliche Aufgabe im Prozess beschreiben.",
    creation_basis: "Einmalige Dokumentationskorrektur."
  }),
  false,
  "Die Erwähnung einer bestehenden Routine ist kein Auftrag zu ihrer Neuanlage."
);

assert.equal(
  isRoutineTaskCreationIntent({
    name: "Systemcheck einrichten",
    description: "Bitte keine wöchentliche Aufgabe anlegen.",
    creation_basis: "Einmaliger Auftrag."
  }),
  false,
  "Eine ausdrücklich negierte Wiederholung darf nicht als positive Anlage gelten."
);

console.log(
  JSON.stringify({
    explicit_false_authoritative: true,
    explicit_true_and_inference_preserved: true
  })
);
