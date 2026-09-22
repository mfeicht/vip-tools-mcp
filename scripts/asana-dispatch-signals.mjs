import crypto from "node:crypto";
import { load as loadHtml } from "cheerio";

const TIME_ZONE = "Europe/Berlin";

export function berlinDate(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIME_ZONE, year: "numeric", month: "2-digit", day: "2-digit"
  }).format(date);
}

export function isRoutine(task) {
  const name = String(task?.name || "");
  return /^\s*R\s*:/i.test(name) || (task?.tags || []).some((tag) =>
    /^(routine|routinen|wiederkehrend)$/i.test(String(tag?.name || "").trim()));
}

export function dueIsReady(task, now = new Date()) {
  if (task?.due_at) {
    const due = Date.parse(task.due_at);
    return Number.isFinite(due) && due <= now.getTime();
  }
  return Boolean(task?.due_on && /^\d{4}-\d{2}-\d{2}$/.test(task.due_on) &&
    task.due_on <= berlinDate(now));
}

export function signalId(agentId, taskGid, kind, version) {
  return crypto.createHash("sha256").update(
    JSON.stringify([agentId, taskGid, kind, version])
  ).digest("hex");
}

export function taskSignal(agentId, task, { firstSeen, now = new Date() } = {}) {
  if (!task?.gid || task.completed) return null;
  const due = dueIsReady(task, now);
  const routine = isRoutine(task);
  if (!due && (!firstSeen || routine || task.due_at || task.due_on)) return null;
  const kind = due ? "due_task" : "new_assigned_task";
  const version = due ? String(task.due_at || task.due_on) : "first_seen";
  return {
    id: signalId(agentId, String(task.gid), kind, version), agent_id: agentId,
    task_gid: String(task.gid), source_version: signalId(agentId, String(task.gid), "version", version),
    kind, priority: due ? 60 : 45
  };
}

export function storyIsComment(story) {
  return story?.resource_subtype === "comment_added" || story?.type === "comment";
}

export function storyMentionsUser(story, userGid) {
  if (!story?.html_text || !userGid) return false;
  const $ = loadHtml(story.html_text);
  return $("a").toArray().some((node) => {
    const attrs = node.attribs || {};
    if ([attrs["data-asana-gid"], attrs["data-gid"], attrs["data-user-gid"]]
      .some((value) => value === String(userGid))) return true;
    try {
      const url = new URL(attrs.href || "", "https://app.asana.com");
      return /(^|\.)asana\.com$/.test(url.hostname) &&
        url.pathname.split("/").filter(Boolean).includes(String(userGid));
    } catch { return false; }
  });
}

export function newCommentSignals(agentId, task, stories, {
  observation, userGid, allAgentUserGids, owned, now = new Date()
}) {
  const previous = observation?.latest_story_at || null;
  const firstScanFloor = new Date(now.getTime() - 24 * 60 * 60_000).toISOString();
  const floor = previous || firstScanFloor;
  const sorted = [...stories].sort((a, b) =>
    String(a.created_at || "").localeCompare(String(b.created_at || "")) ||
    String(a.gid || "").localeCompare(String(b.gid || "")));
  const ownReplyAt = sorted.filter((story) => storyIsComment(story) &&
    String(story.created_by?.gid || "") === String(userGid))
    .map((story) => String(story.created_at || "")).at(-1) || null;
  const signals = [];
  for (const story of sorted) {
    if (!storyIsComment(story) || !story.gid || !story.created_at || story.created_at < floor) continue;
    if (previous && story.created_at === previous && String(story.gid) <= String(observation.latest_story_gid || "")) continue;
    if (ownReplyAt && story.created_at <= ownReplyAt) continue;
    const author = String(story.created_by?.gid || "");
    if (!author || allAgentUserGids.has(author)) continue;
    if (!owned && !storyMentionsUser(story, userGid)) continue;
    const storyGid = String(story.gid);
    signals.push({
      id: signalId(agentId, String(task.gid), "comment", storyGid),
      agent_id: agentId, task_gid: String(task.gid), story_gid: storyGid,
      source_version: signalId(agentId, String(task.gid), "story", storyGid),
      kind: owned ? "human_comment" : "direct_mention", priority: owned ? 85 : 95
    });
  }
  return {
    signals,
    latest: sorted.at(-1) ? { at: sorted.at(-1).created_at, gid: String(sorted.at(-1).gid) } : null
  };
}
