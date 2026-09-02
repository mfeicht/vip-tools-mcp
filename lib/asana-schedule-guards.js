function normalizeGuardText(value) {
  return String(value || "")
    .toLowerCase()
    .replaceAll("ae", "a")
    .replaceAll("oe", "o")
    .replaceAll("ue", "u")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

const ROUTINE_INSTANCE_PATTERN =
  "(?:routine[- ]*)?(?:folge[- ]?instanz|wiederholungs[- ]?instanz|wiederholungs[- ]?aufgabe|nachste routine[- ]?instanz|next routine instance|recurrence instance)";

const ROUTINE_INSTANCE_MISSING_PATTERNS = [
  new RegExp(`\\b${ROUTINE_INSTANCE_PATTERN}\\s+(?:der routine\\s+)?(?:fehlt|fehlend|missing)\\b`),
  new RegExp(`\\b(?:fehlende?|missing)\\s+${ROUTINE_INSTANCE_PATTERN}\\b`),
  new RegExp(
    `\\b${ROUTINE_INSTANCE_PATTERN}\\b.{0,60}\\b(?:nicht|not)\\s+(?:erstellt|erzeugt|angelegt|sichtbar|gefunden|created|generated|found)\\b`
  ),
  new RegExp(
    `\\b(?:nicht|not)\\s+(?:erstellt|erzeugt|angelegt|sichtbar|gefunden|created|generated|found)\\b.{0,60}\\b${ROUTINE_INSTANCE_PATTERN}\\b`
  )
];

export function isRoutineInstanceMissingAlarmIntent({ name, description, creation_basis }) {
  return [name, creation_basis, description]
    .map(normalizeGuardText)
    .filter(Boolean)
    .some((field) => ROUTINE_INSTANCE_MISSING_PATTERNS.some((pattern) => pattern.test(field)));
}

export function sameAsanaDateTimeInstant(actual, expected) {
  const actualMs = Date.parse(String(actual || ""));
  const expectedMs = Date.parse(String(expected || ""));
  return Number.isFinite(actualMs) && Number.isFinite(expectedMs) && actualMs === expectedMs;
}
