function normalizeLabel(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/ß/g, "ss")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function isRoutineTaskCreationIntent({
  name,
  description,
  creation_basis,
  routine_task
}) {
  if (routine_task === true) return true;
  if (routine_task === false) return false;
  if (/^\s*r\s*:/i.test(String(name || ""))) return true;

  const normalized = normalizeLabel(
    [name, description, creation_basis].filter(Boolean).join(" ")
  );
  const routineSignals = [
    "routine aufgabe",
    "routineaufgabe",
    "wiederkehrende aufgabe",
    "wiederkehrend eingestellte aufgabe",
    "regelmaessige aufgabe",
    "regelmassige aufgabe",
    "tagliche aufgabe",
    "taegliche aufgabe",
    "wochentliche aufgabe",
    "woechentliche aufgabe",
    "monatliche aufgabe"
  ];
  return routineSignals.some((signal) => normalized.includes(signal));
}
