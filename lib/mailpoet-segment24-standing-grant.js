export const MAILPOET_SEGMENT24_GRANT = Object.freeze({
  id: "goklever-mailpoet-segment24-2026-10-13",
  source_thread_id: "019de50c-318f-7050-a2f6-70944878f1a7",
  source_user_message_id: "01a136cc-66dc-70e2-9631-eed52b29ea5a",
  root_task_gid: "1219462031349113",
  project_gid: "1214338226145917",
  developer_gid: "1214338261069878",
  task_name: "R: MailPoet-Liste „Leads: Kontaktiert“ prüfen und freigegebene Kontakte endgültig löschen",
  site_host: "app.goklever.de",
  segment_id: "24",
  anchor_due_on: "2026-10-15",
  interval_days: 14,
  due_time_berlin: "08:45:00"
});

export const MAILPOET_SEGMENT24_GRANT_MARKER =
  `VIP-STANDING-GRANT: ${MAILPOET_SEGMENT24_GRANT.id}`;

const DAY_MS = 86_400_000;

function berlinDateTime(value) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  const parts = Object.fromEntries(new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Berlin", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"
  }).formatToParts(date).map((part) => [part.type, part.value]));
  return { date: `${parts.year}-${parts.month}-${parts.day}`,
    time: `${parts.hour}:${parts.minute}:${parts.second}` };
}

function hasProject(task, projectGid) {
  return (task.projects || []).some((project) => String(project.gid) === projectGid) ||
    (task.memberships || []).some((membership) => String(membership.project?.gid) === projectGid);
}

export function inspectMailpoetSegment24StandingGrant({
  agent_id, task, root_task, same_slot_task_gids = [], now = new Date(), shadow_only = false
}) {
  const grant = MAILPOET_SEGMENT24_GRANT;
  const errors = [];
  if (agent_id !== "vip-ai-developer") errors.push("Nur VIP AI-Developer darf diese Freigabe verwenden.");
  if (!task?.gid || !/^\d+$/.test(String(task.gid))) errors.push("Aktuelle Asana-GID fehlt.");
  if (task?.completed !== false) errors.push("Die aktuelle Aufgabe muss offen sein.");
  if (task?.name !== grant.task_name) errors.push("Aufgabentitel weicht vom freigegebenen Auftrag ab.");
  if (String(task?.assignee?.gid || "") !== grant.developer_gid) errors.push("Developer ist nicht Assignee.");
  if (!hasProject(task || {}, grant.project_gid)) errors.push("Projekt stimmt nicht überein.");
  if (!(task?.tags || []).some((tag) => String(tag.name || "").toLowerCase() === "routine")) {
    errors.push("Routine-Tag fehlt.");
  }
  if (!String(task?.notes || "").includes(MAILPOET_SEGMENT24_GRANT_MARKER)) {
    errors.push("Freigabemarker fehlt in der aktuellen Aufgabe.");
  }

  if (String(root_task?.gid || "") !== grant.root_task_gid ||
      root_task?.name !== grant.task_name ||
      String(root_task?.assignee?.gid || "") !== grant.developer_gid ||
      !hasProject(root_task || {}, grant.project_gid) ||
      !String(root_task?.notes || "").includes(MAILPOET_SEGMENT24_GRANT_MARKER)) {
    errors.push("Freigabewurzel ist nicht unverändert nachweisbar.");
  }
  const notes = String(task?.notes || "");
  if (!notes.includes("App.Goklever.de") || !notes.includes("Segment-ID 24")) {
    errors.push("Standort oder Segment fehlt im aktuellen Auftrag.");
  }

  const localDue = task?.due_at ? berlinDateTime(task.due_at) : null;
  if (!localDue || localDue.date !== task?.due_on || localDue.time !== grant.due_time_berlin) {
    errors.push("Fälligkeit ist nicht Donnerstag 08:45 Uhr Europe/Berlin.");
  } else {
    const slot = Date.parse(`${localDue.date}T00:00:00Z`);
    const anchor = Date.parse(`${grant.anchor_due_on}T00:00:00Z`);
    if (slot < anchor || (slot - anchor) % (grant.interval_days * DAY_MS) !== 0) {
      errors.push("Fälligkeit liegt nicht im belegten 14-Tage-Rhythmus.");
    }
  }

  const sameSlot = [...new Set(same_slot_task_gids.map(String))];
  if (sameSlot.length !== 1 || sameSlot[0] !== String(task?.gid || "")) {
    errors.push("Die aktuelle Routine-Instanz ist für diesen Fälligkeitsslot nicht eindeutig.");
  }
  if (!(now instanceof Date) || !Number.isFinite(now.getTime())) {
    errors.push("Prüfzeitpunkt ist ungültig.");
  } else if (!shadow_only && task?.due_at && Date.parse(task.due_at) > now.getTime()) {
    errors.push("Die Routine ist noch nicht fällig.");
  }

  return {
    authorized: errors.length === 0 && !shadow_only,
    status: errors.length ? "blocked" : shadow_only ? "shadow_only" : "authorized",
    errors,
    grant_id: grant.id,
    source_thread_id: grant.source_thread_id,
    source_user_message_id: grant.source_user_message_id,
    task_gid: task?.gid || null,
    due_at: task?.due_at || null,
    scope: { site_host: grant.site_host, segment_id: grant.segment_id,
      exclusive_segment_membership: true, no_linked_wp_or_woocommerce_user: true }
  };
}
