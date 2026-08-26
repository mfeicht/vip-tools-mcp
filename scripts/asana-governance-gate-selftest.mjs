import { readFileSync } from "node:fs";

const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");

const checks = {
  routine_supervisor_comment_reason_schema:
    source.includes('routine_supervisor_mention_reason: z') &&
    source.includes('["blocker", "question", "critical_anomaly", "decision_required"]'),
  normal_routine_supervisor_mention_blocked:
    source.includes("Routine-Supervisor-Gate: Moritz darf in einer Routine nicht routinemaessig erwaehnt werden"),
  boolean_readd_bypass_removed:
    source.includes("allow_routine_supervisor_readd ist kein Bypass") &&
    !source.includes("isRoutineSupervisorDoNotReaddCandidate(task, finalSupervisorFollowerGid) &&\n        !allow_routine_supervisor_readd") &&
    !source.includes("isRoutineSupervisorDoNotReaddCandidate(before_task, followerGid) && !allow_routine_supervisor_readd"),
  observer_task_creation_gate:
    source.includes("asana_create_task_observer_gate") &&
    source.includes("authorization_story_mentions_agent") &&
    source.includes("Beobachter-Gate: Der Agent ist in der Ausgangsaufgabe weder Assignee noch Creator"),
  observer_comment_gate:
    source.includes("asana_comment_observer_gate") &&
    source.includes("observer_comment_reason") &&
    source.includes("Reiner Follower-/Beteiligtenstatus ist read-only"),
  governance_scope_is_bounded:
    source.includes("ASANA_GOVERNANCE_AGENT_IDS") &&
    source.includes("isGovernanceTaskCreationIntent"),
  temporary_routine_observer_auto_leave:
    source.includes("observer_should_leave_after_comment") &&
    source.includes("keep_routine_observer_subscription") &&
    source.includes('/tasks/${task_gid}/removeFollowers'),
  observer_leave_is_nonduplicating:
    source.includes("leave_failed_do_not_retry_comment") &&
    source.includes("failed_still_follower_do_not_retry_comment"),
  full_description_replacement_requires_moritz:
    source.includes("replace_full_description") &&
    source.includes("requireMoritz: replace_full_description")
};

console.log(JSON.stringify(checks));
process.exit(Object.values(checks).every(Boolean) ? 0 : 1);
