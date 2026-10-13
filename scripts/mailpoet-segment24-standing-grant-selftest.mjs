import assert from "node:assert/strict";
import { MAILPOET_SEGMENT24_GRANT as grant, MAILPOET_SEGMENT24_GRANT_MARKER as marker,
  inspectMailpoetSegment24StandingGrant as inspect } from "../lib/mailpoet-segment24-standing-grant.js";

const base = {
  gid: grant.root_task_gid,
  name: grant.task_name,
  completed: false,
  due_on: "2026-10-15",
  due_at: "2026-10-15T06:45:00Z",
  assignee: { gid: grant.developer_gid },
  projects: [{ gid: grant.project_gid }],
  tags: [{ name: "Routine" }],
  notes: `App.Goklever.de; Segment-ID 24; ${marker}`
};
const args = { agent_id: "vip-ai-developer", task: base, root_task: base,
  same_slot_task_gids: [base.gid], now: new Date("2026-10-15T07:00:00Z") };
assert.equal(inspect(args).status, "authorized");
assert.equal(inspect({ ...args, shadow_only: true }).status, "shadow_only");
assert.equal(inspect({ ...args, now: new Date("2026-10-14T07:00:00Z") }).status, "blocked");
assert.equal(inspect({ ...args, agent_id: "vip-ai-operations" }).status, "blocked");
assert.equal(inspect({ ...args, task: { ...base, notes: "App.Goklever.de; Segment-ID 24" } }).status, "blocked");
assert.equal(inspect({ ...args, task: { ...base, assignee: { gid: "123" } } }).status, "blocked");
assert.equal(inspect({ ...args, root_task: { ...base, assignee: { gid: "123" } } }).status, "blocked");
assert.equal(inspect({ ...args, root_task: { ...base, projects: [] } }).status, "blocked");
assert.equal(inspect({ ...args, now: new Date("invalid") }).status, "blocked");
assert.equal(inspect({ ...args, task: { ...base, due_at: "2026-10-16T06:45:00Z", due_on: "2026-10-16" } }).status, "blocked");
assert.equal(inspect({ ...args, same_slot_task_gids: [base.gid, "123"] }).status, "blocked");
const successor = { ...base, gid: "999", due_on: "2026-10-29", due_at: "2026-10-29T07:45:00Z" };
assert.equal(inspect({ ...args, task: successor, same_slot_task_gids: [successor.gid],
  now: new Date("2026-10-29T08:00:00Z") }).status, "authorized");
assert.equal(inspect({ ...args, task: { ...successor, due_at: "2026-10-29T06:45:00Z" },
  same_slot_task_gids: [successor.gid], now: new Date("2026-10-29T08:00:00Z") }).status, "blocked");
console.log("mailpoet segment24 standing grant self-test passed");
