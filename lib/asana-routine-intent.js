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

function hasAffirmativeRoutineSignal(text, signals, { requireCreationVerb = false } = {}) {
  const normalized = normalizeLabel(text);
  for (const signal of signals) {
    let start = 0;
    while ((start = normalized.indexOf(signal, start)) !== -1) {
      const end = start + signal.length;
      const before = normalized.slice(0, start);
      const after = normalized.slice(end);
      start = end;
      if ((before && !before.endsWith(" ")) || (after && !after.startsWith(" "))) continue;
      if (/(?:^| )(?:nicht|kein|keine|keinen|keinem|ohne|non) $/.test(before)) continue;
      if (requireCreationVerb && !/^ (?:anlegen|erstellen|einrichten|planen|setzen|hinterlegen)(?: |$)/.test(after)) continue;
      return true;
    }
  }
  return false;
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
  // The title can state the task type. Prose in the description or creation
  // basis must actually request creating a recurring task, not merely discuss
  // another routine or say that this task is a "Nicht-Routine-Aufgabe".
  return hasAffirmativeRoutineSignal(name, routineSignals) ||
    hasAffirmativeRoutineSignal([description, creation_basis].filter(Boolean).join(" "),
      routineSignals, { requireCreationVerb: true });
}
