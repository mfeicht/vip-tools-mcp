const ASANA_COMMENT_READABILITY_POLICY_VERSION = "asana-comment-readability-v1";

const COMPACT_TECHNICAL_TOKEN_PATTERNS = [
  /^[a-f0-9]{7,64}$/i,
  /^[a-f0-9]{8}-[a-f0-9-]{27,}$/i,
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?Z?$/,
  /^(?:P[0-3]|B2[BC]|2FA|3D|4K|5G|IPv[46]|HTTP[1-3]|TLS1(?:\.[0-3])?|OAuth2|SHA(?:1|224|256|384|512)|MD5|UTF(?:8|16|32)|ISO\d+|RFC\d+|HTML5|CSS3|ES\d{4}|x509|S3|EC2)$/i,
  /^[A-Z]{1,3}\d+:[A-Z]{1,3}\d+$/,
  /^v\d+(?:\.\d+)*$/i,
  /^\d+(?:ms|s|min|h|d|D|KB|MB|GB|TB)$/
];

function stripExternalLocators(text) {
  return String(text || "")
    .replace(/\b(?:https?:\/\/|www\.)\S+/giu, " ")
    .replace(/\b[^\s@]+@[^\s@]+\b/gu, " ");
}

function isLetter(value) {
  return /^\p{L}$/u.test(value);
}

function isDigit(value) {
  return /^\p{N}$/u.test(value);
}

function isAllowedCompactTechnicalToken(token) {
  return COMPACT_TECHNICAL_TOKEN_PATTERNS.some((pattern) => pattern.test(token));
}

function inspectToken(token) {
  if (!token || isAllowedCompactTechnicalToken(token)) return [];

  const characters = [...token];
  const boundaries = [];
  for (let index = 1; index < characters.length; index += 1) {
    const previous = characters[index - 1];
    const current = characters[index];
    if (isLetter(previous) && isDigit(current)) {
      let runStart = index - 1;
      while (runStart > 0 && isLetter(characters[runStart - 1])) runStart -= 1;
      const letterRun = characters.slice(runStart, index).join("");
      if (letterRun.length >= 3) {
        boundaries.push({ direction: "letter_to_digit", boundary_index: index, letter_run: letterRun });
      }
    }
    if (isDigit(previous) && isLetter(current)) {
      let runEnd = index + 1;
      while (runEnd < characters.length && isLetter(characters[runEnd])) runEnd += 1;
      const letterRun = characters.slice(index, runEnd).join("");
      if (letterRun.length >= 3) {
        boundaries.push({ direction: "digit_to_letter", boundary_index: index, letter_run: letterRun });
      }
    }
  }
  return boundaries;
}

function inspectField(path, value) {
  const text = stripExternalLocators(value);
  const issues = [];
  for (const match of text.matchAll(/[\p{L}\p{N}][\p{L}\p{N}._:/+-]*/gu)) {
    const token = String(match[0] || "").replace(/[._:/+-]+$/u, "");
    const boundaries = inspectToken(token);
    if (!boundaries.length) continue;
    issues.push({
      path,
      token,
      boundaries
    });
  }
  return issues;
}

export function inspectAsanaCommentReadability({
  greeting,
  sections = [],
  mentionText,
  effortNote,
  maxIssues = 24
} = {}) {
  const fields = [];
  if (greeting) fields.push(["greeting", greeting]);
  for (const [sectionIndex, section] of sections.entries()) {
    if (section?.title) fields.push([`sections[${sectionIndex}].title`, section.title]);
    for (const [paragraphIndex, paragraph] of (section?.paragraphs || []).entries()) {
      fields.push([`sections[${sectionIndex}].paragraphs[${paragraphIndex}]`, paragraph]);
    }
    for (const [bulletIndex, bullet] of (section?.bullets || []).entries()) {
      fields.push([`sections[${sectionIndex}].bullets[${bulletIndex}]`, bullet]);
    }
  }
  if (mentionText) fields.push(["mention_text", mentionText]);
  if (effortNote) fields.push(["effort_note", effortNote]);

  const allIssues = fields.flatMap(([path, value]) => inspectField(path, value));
  const issues = allIssues.slice(0, maxIssues);
  return {
    policy_version: ASANA_COMMENT_READABILITY_POLICY_VERSION,
    status: allIssues.length ? "warning_suspicious_alphanumeric_boundaries" : "ok",
    enforcement: "report_only",
    issue_count: allIssues.length,
    issues,
    issues_truncated: allIssues.length > issues.length,
    scanned_field_count: fields.length,
    code_blocks_excluded: true,
    external_locators_excluded: true,
    guidance: allIssues.length
      ? "Pruefe sichtbare Wort-/Zahlgrenzen. Natuerliche Sprache mit Leerzeichen oder Bindestrich schreiben; absichtlich kompakte technische Tokens bei Bedarf in code_blocks ausgeben."
      : null
  };
}

export { ASANA_COMMENT_READABILITY_POLICY_VERSION };
