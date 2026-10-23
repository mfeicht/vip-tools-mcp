import { isRoutine } from "./asana-dispatch-signals.mjs";

export const executableStoryKinds = ["human_comment", "direct_mention", "historical_comment"];

export function needsMaterialCommentStorePreflight(signals, task) {
  return isRoutine(task) && signals.some(signal => signal.kind === "due_task") &&
    !signals.some(signal => signal.story_gid && executableStoryKinds.includes(signal.kind));
}

// A due signal guards the whole task, including continuation/dependency siblings.
// Unknown historical due metadata is guarded until a fresh poll classifies it.
export const coordinationSelectionSql = `NOT EXISTS (
  SELECT 1 FROM signals due WHERE due.agent_id=signals.agent_id AND due.task_gid=signals.task_gid
  AND due.kind='due_task' AND COALESCE(due.requires_coordination,1)=1
  AND due.status IN ('pending','retry_after') AND due.available_at_ms<=:now
) OR EXISTS (
  SELECT 1 FROM signals story WHERE story.agent_id=signals.agent_id AND story.task_gid=signals.task_gid
  AND story.story_gid IS NOT NULL AND story.kind IN ('human_comment','direct_mention','historical_comment')
  AND story.status IN ('pending','retry_after') AND story.available_at_ms<=:now
)`;
