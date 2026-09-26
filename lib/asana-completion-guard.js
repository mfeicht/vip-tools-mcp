import { load } from "cheerio";

function normalizeGuardText(value) {
  return String(value || "").normalize("NFKC").toLowerCase();
}

function visibleGuardHtmlText(value) {
  const $ = load(String(value || ""));
  $("br").replaceWith("\n");
  $("body,p,li,ul,ol,div,section,blockquote,pre,h1,h2,h3,h4,h5,h6").each((_, element) => {
    $(element).prepend("\n").append("\n");
  });
  return $.root().text();
}

function normalizeGuardLabel(value) {
  return String(value || "")
    .replace(/ß/g, "ss")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function storyHasClosedEvidence(story) {
  const text = normalizeGuardText(`${story?.text || ""}\n${story?.html_text || ""}`);
  return (
    /evidenz\s*\/\s*verifikation/.test(text) &&
    !/offene\s+evidenzluecken|offene\s+evidenzlücken/.test(text)
  );
}

function storyHasEvidence(story) {
  const text = normalizeGuardText(`${story?.text || ""}\n${story?.html_text || ""}`);
  return /evidenz\s*\/\s*verifikation/.test(text);
}

function normalizeCorrectionClaim(value) {
  return String(value || "").normalize("NFKC").toLowerCase().replace(/\s+/g, " ").trim();
}

function isNoFollowUpClaim(value) {
  const claim = normalizeCorrectionClaim(value);
  return /\bfollow[\s-]?up\s*:\s*(?:keines?|nicht)\s+(?:noetig|nötig|erforderlich)\b/u.test(claim) ||
    /\bkein(?:e|en|er|es)?\b[^.!?\n]{0,100}\b(?:follow[\s-]?up(?:[\s-]?aufgabe)?|folgeaufgabe|nacharbeit)\b[^.!?\n]{0,100}\b(?:noetig|nötig|erforderlich|offen)\b/u.test(claim);
}

export function validateRoutineMaterialCorrection({ priorStory, correction, proposedText }) {
  const issues = [];
  const reason = String(correction?.reason || "").trim();
  const before = normalizeCorrectionClaim(correction?.before);
  const after = normalizeCorrectionClaim(correction?.after);
  const source = String(correction?.source || "").trim();
  const priorText = normalizeCorrectionClaim(priorStory?.text || priorStory?.html_text);
  const nextText = normalizeCorrectionClaim(proposedText);

  if (!priorStory?.gid) issues.push("superseded_story_not_found");
  if (reason.length < 24) issues.push("correction_reason_missing_or_too_short");
  if (source.length < 10) issues.push("correction_source_missing_or_too_short");
  if (source && priorText.includes(normalizeCorrectionClaim(source))) issues.push("correction_source_already_in_prior_story");
  if (before.length < 8 || !priorText.includes(before)) issues.push("prior_claim_not_in_superseded_story");
  if (after.length < 8 || !nextText.includes(after)) issues.push("corrected_claim_not_in_new_comment");
  if (before && before === after) issues.push("correction_claim_unchanged");
  if (isNoFollowUpClaim(before) && isNoFollowUpClaim(after)) {
    issues.push("correction_restates_no_follow_up");
  }

  return {
    allowed: issues.length === 0,
    status: issues.length === 0 ? "correction_delta_present" : "blocked_correction_delta",
    issues,
    supersedes_story_gid: priorStory?.gid || null
  };
}

export function inspectRoutineMaterialCommentIdempotency({
  stories = [],
  agentUserGid,
  supersedesStoryGid
}) {
  const agentGid = String(agentUserGid || "");
  const authoredEvidenceStories = (stories || [])
    .filter((story) => String(story?.created_by?.gid || "") === agentGid)
    .filter(storyHasEvidence);
  const priorMaterialStories = authoredEvidenceStories
    .filter(storyHasClosedEvidence)
    .sort((left, right) => String(left?.created_at || "").localeCompare(String(right?.created_at || "")));
  const priorMaterialStoryGids = priorMaterialStories.map((story) => String(story.gid));
  const supersedableStoryGids = authoredEvidenceStories.map((story) => String(story.gid));
  const supersedesGid = String(supersedesStoryGid || "");

  if (!priorMaterialStories.length && supersedesGid) {
    if (supersedableStoryGids.includes(supersedesGid)) {
      return {
        status: "allowed_explicit_correction",
        allowed: true,
        prior_material_story_gids: [],
        supersedes_story_gid: supersedesGid
      };
    }
    return {
      status: "invalid_supersedes_story",
      allowed: false,
      prior_material_story_gids: [],
      supersedes_story_gid: supersedesGid
    };
  }
  if (!priorMaterialStories.length) {
    return {
      status: "first_material_comment",
      allowed: true,
      prior_material_story_gids: [],
      supersedes_story_gid: null
    };
  }
  if (!supersedesGid) {
    return {
      status: "blocked_duplicate_material_comment",
      allowed: false,
      prior_material_story_gids: priorMaterialStoryGids,
      supersedes_story_gid: null
    };
  }
  if (!supersedableStoryGids.includes(supersedesGid)) {
    return {
      status: "invalid_supersedes_story",
      allowed: false,
      prior_material_story_gids: priorMaterialStoryGids,
      supersedes_story_gid: supersedesGid
    };
  }
  return {
    status: "allowed_explicit_correction",
    allowed: true,
    prior_material_story_gids: priorMaterialStoryGids,
    supersedes_story_gid: supersedesGid
  };
}

export function detectRoutineFollowUpSignals({ finalComment, completionBasis, followUpNotRequiredBasis }) {
  const htmlText = String(finalComment?.html_text || "");
  const combinedText = normalizeGuardText(
    [finalComment?.text || "", visibleGuardHtmlText(htmlText), completionBasis || "", followUpNotRequiredBasis || ""].join("\n")
  );
  const hasMention = /<a\s+data-asana-gid="\d+"\s*\/?>/i.test(htmlText);
  const noFollowUpClaim =
    /\bfollow[\s-]?up\s*:\s*(?:keines?|nicht)\s+(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bfollow[\s-]?up\s*\n\s*(?:keines?|nicht)\s+(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bkeine\s+(?:weitere\s+)?recherche\s+oder\s+folgeaufgabe\s+(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bkeine\s+eigenst(?:ä|ae)ndige\s+neue\s+folgeaufgabe\s+(?:ist\s+)?(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bkeine\s+manuelle\s+folgeaufgabe\s+oder\s+nacharbeit\s+(?:ist|sind)\s+(?:f(?:ü|ue)r\s+diesen\s+lauf\s+)?(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bkeine\s+(?:aktive\s+)?nacharbeit\s+oder\s+(?:(?:separate|eigenst(?:ä|ae)ndige(?:\s+neue)?)\s+)?folgeaufgabe\s+(?:ist\s+)?(?:f(?:ü|ue)r\s+(?:diesen\s+)?(?:routine[\s-]?scope|lauf)\s+)?(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bbesteht\s+keine\s+aktive\s+folgeaufgabe\s+und\s+keine\s+noch\s+ausstehende\s+handlung\s+f(?:ü|ue)r\s+eine\s+person\s+oder\s+ein\s+system\b/i.test(combinedText) ||
    /\bkein(?:e|er|en)?\s+(?:weitere\s+)?(?:aktive\s+)?(?:nacharbeit|folgeaufgabe|follow[\s-]?up(?:[\s-]?aufgabe)?|handoff|aktion)(?:\s+(?:oder|und)\s+(?:weitere\s+)?(?:aktive\s+)?(?:nacharbeit|folgeaufgabe|follow[\s-]?up(?:[\s-]?aufgabe)?|handoff|aktion))*\s+(?:(?:ist|sind)\s+)?(?:noetig|nötig|erforderlich|offen)\b/i.test(
      combinedText
    ) ||
    /\b(?:keine|kein)\s+(?:weitere\s+)?(?:to-?dos?|aktion|aufgabe)\s+(?:noetig|nötig|erforderlich|offen)\b/i.test(
      combinedText
    );
  const actionSignal =
    /\b(?:bitte|soll|muss|kann\s+jetzt|naechster\s+schritt|nächster\s+schritt|weitergabe|handoff|follow[\s-]?up|folgeaufgabe|nacharbeit)\b/i.test(
      combinedText
    ) &&
    /\b(?:pruef|prüf|freigeb|importier|weiterbearbeit|bearbeit|erledig|umsetz|einpfleg|hochlad|veroeffentlich|veröffentlich|antwort|rueckmeld|rückmeld|nachzieh|uebernehm|übernehm|anleg|erstel)\w*/i.test(
      combinedText
    );
  const existingTaskCoverageClaim = combinedText
    .split(/[\n.;!?]+/)
    .some((segment) => {
      const workItem =
        "(?:routine(?![\\s-]?tag\\b)|folgeaufgabe|follow[\\s-]?up|task(?![\\s-]?(?:readback|spezifisch(?:e[nmrs]?)?)\\b)|aufgabe)";
      const qualifier =
        "(?:bestehende[nrs]?|vorhandene[nrs]?|naechste[nrs]?|nächste[nrs]?|kuenftige[nrs]?|künftige[nrs]?|zukuenftige[nrs]?|zukünftige[nrs]?)";
      const coverageVerb =
        "(?:deckt|abgedeckt|uebernimmt|übernimmt|stellt\\s+sicher|existiert|vorhanden|angelegt|eingeplant|terminiert|laeuft|läuft)";
      return (
        new RegExp(`\\b(?:${qualifier}\\s+)?${workItem}\\b.{0,80}(?<![\\p{L}\\p{N}_])${coverageVerb}(?![\\p{L}\\p{N}_])`, "iu").test(segment) ||
        new RegExp(`(?<![\\p{L}\\p{N}_])${coverageVerb}(?![\\p{L}\\p{N}_]).{0,80}\\b(?:${qualifier}\\s+)?${workItem}\\b`, "iu").test(segment)
      );
    });
  return {
    has_mention: hasMention,
    has_action_signal: actionSignal,
    has_existing_task_coverage_claim: existingTaskCoverageClaim,
    no_follow_up_claim: noFollowUpClaim,
    blocked_without_follow_up_task: hasMention || existingTaskCoverageClaim || (actionSignal && !noFollowUpClaim)
  };
}

export function validateRoutineVisibleFollowUpStatus({ finalComment, hasFollowUpTask }) {
  if (hasFollowUpTask) {
    return {
      ok: true,
      issues: [],
      mode: "follow_up_task_readback"
    };
  }

  const visibleSignals = detectRoutineFollowUpSignals({
    finalComment,
    completionBasis: "",
    followUpNotRequiredBasis: ""
  });
  const issues = visibleSignals.no_follow_up_claim
    ? []
    : ["final_comment_missing_visible_no_follow_up_status"];
  return {
    ok: issues.length === 0,
    issues,
    mode: "no_follow_up_required",
    visible_signals: visibleSignals
  };
}

function uniqueTaskProjects(task) {
  const projects = [];
  const seen = new Set();
  for (const membership of task?.memberships || []) {
    const project = membership?.project;
    if (!project?.gid || seen.has(String(project.gid))) continue;
    seen.add(String(project.gid));
    projects.push({ gid: String(project.gid), name: project.name || null });
  }
  return projects;
}

function readTaskStatus(task) {
  const field = (task?.custom_fields || []).find((candidate) => normalizeGuardLabel(candidate?.name) === "status");
  return field?.enum_value?.name || field?.display_value || field?.text_value || null;
}

function textContainsIdentifier(text, value) {
  const needle = normalizeGuardLabel(value);
  return needle.length >= 2 && normalizeGuardLabel(text).includes(needle);
}

function containsDueDateReadback(text, dueDate) {
  if (text.includes(dueDate)) return true;
  // Explicit German calendar dates are the same readback, never an inferred
  // year, relative date or silently repaired invalid calendar value.
  for (const match of text.matchAll(/(?<!\d)(\d{1,2})\.(\d{1,2})\.(20\d{2})(?!\d)/g)) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = Number(match[3]);
    const date = new Date(Date.UTC(year, month - 1, day));
    if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) continue;
    if (date.toISOString().slice(0, 10) === dueDate) return true;
  }
  return false;
}

export function validateRoutineFollowUpTaskContract({ sourceTask, followUpTask, finalComment }) {
  const issues = [];
  const sourceTaskGid = String(sourceTask?.gid || "");
  const followUpTaskGid = String(followUpTask?.gid || "");
  const sourceProjects = uniqueTaskProjects(sourceTask);
  const followUpProjects = uniqueTaskProjects(followUpTask);
  const dueValue = followUpTask?.due_at || followUpTask?.due_on || null;
  const dueDate = dueValue ? String(dueValue).slice(0, 10) : null;
  const statusValue = readTaskStatus(followUpTask);
  const evidenceText = `${finalComment?.text || ""}\n${finalComment?.html_text || ""}`;
  const permalinkUrl = String(followUpTask?.permalink_url || "");
  const assigneeGid = String(followUpTask?.assignee?.gid || "");
  const assigneeName = String(followUpTask?.assignee?.name || "");

  if (!followUpTaskGid || followUpTaskGid === sourceTaskGid) issues.push("follow_up_task_must_differ_from_source");
  if (followUpTask?.completed) issues.push("follow_up_task_must_be_open");
  if (!assigneeGid) issues.push("follow_up_task_missing_assignee");
  if (!dueValue) issues.push("follow_up_task_missing_due");
  if (followUpProjects.length !== 1) issues.push("follow_up_task_requires_exactly_one_project");
  if (
    sourceProjects.length === 1 &&
    followUpProjects.length === 1 &&
    sourceProjects[0].gid !== followUpProjects[0].gid
  ) {
    issues.push("follow_up_task_project_mismatch");
  }
  if (!statusValue) {
    issues.push("follow_up_task_missing_status");
  } else if (!["todo", "to do"].includes(normalizeGuardLabel(statusValue))) {
    issues.push("follow_up_task_status_not_todo");
  }

  const taskReferencePresent =
    (followUpTaskGid && evidenceText.includes(followUpTaskGid)) ||
    (permalinkUrl && evidenceText.includes(permalinkUrl));
  if (!taskReferencePresent) issues.push("final_comment_missing_follow_up_task_link_or_gid");
  if (
    assigneeGid &&
    !evidenceText.includes(assigneeGid) &&
    !(assigneeName && textContainsIdentifier(evidenceText, assigneeName))
  ) {
    issues.push("final_comment_missing_follow_up_assignee_readback");
  }
  if (statusValue && !textContainsIdentifier(evidenceText, statusValue)) {
    issues.push("final_comment_missing_follow_up_status_readback");
  }
  if (dueDate && !containsDueDateReadback(evidenceText, dueDate)) {
    issues.push("final_comment_missing_follow_up_due_readback");
  }

  return {
    ok: issues.length === 0,
    issues,
    readback: {
      task_gid: followUpTaskGid || null,
      task_name: followUpTask?.name || null,
      completed: Boolean(followUpTask?.completed),
      assignee: followUpTask?.assignee || null,
      due_on: followUpTask?.due_on || null,
      due_at: followUpTask?.due_at || null,
      status: statusValue,
      projects: followUpProjects,
      permalink_url: followUpTask?.permalink_url || null,
      final_comment_contains_task_reference: taskReferencePresent
    }
  };
}
