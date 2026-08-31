function normalizeGuardText(value) {
  return String(value || "").normalize("NFKC").toLowerCase();
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
    [finalComment?.text || "", htmlText, completionBasis || "", followUpNotRequiredBasis || ""].join("\n")
  );
  const hasMention = /<a\s+data-asana-gid="\d+"\s*\/?>/i.test(htmlText);
  const noFollowUpClaim =
    /\bfollow[\s-]?up\s*:\s*(?:keines?|nicht)\s+(?:noetig|nötig|erforderlich)\b/i.test(combinedText) ||
    /\bkein(?:e|er|en)?\s+(?:weitere\s+)?(?:aktive\s+)?(?:nacharbeit|folgeaufgabe|follow[\s-]?up|handoff|aktion)(?:\s+(?:oder|und)\s+(?:weitere\s+)?(?:aktive\s+)?(?:nacharbeit|folgeaufgabe|follow[\s-]?up|handoff|aktion))*\s+(?:(?:ist|sind)\s+)?(?:noetig|nötig|erforderlich|offen)\b/i.test(
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
        "(?:routine(?![\\s-]?tag\\b)|folgeaufgabe|follow[\\s-]?up|task(?![\\s-]?(?:readback|spezifisch)\\b)|aufgabe)";
      const qualifier =
        "(?:bestehende[nrs]?|vorhandene[nrs]?|naechste[nrs]?|nächste[nrs]?|kuenftige[nrs]?|künftige[nrs]?|zukuenftige[nrs]?|zukünftige[nrs]?)";
      const coverageVerb =
        "(?:deckt|abgedeckt|uebernimmt|übernimmt|stellt\\s+sicher|existiert|vorhanden|angelegt|eingeplant|terminiert|laeuft|läuft)";
      return (
        new RegExp(`\\b(?:${qualifier}\\s+)?${workItem}\\b.{0,80}\\b${coverageVerb}\\b`, "i").test(segment) ||
        new RegExp(`\\b${coverageVerb}\\b.{0,80}\\b(?:${qualifier}\\s+)?${workItem}\\b`, "i").test(segment)
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
  if (dueDate && !evidenceText.includes(dueDate)) {
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
