import assert from "node:assert/strict";
import test from "node:test";
import { dueIsReady, newCommentSignals, storyMentionsUser, taskSignal } from "./asana-dispatch-signals.mjs";

test("past due dates have no lookback limit; future routines wait", () => {
  const now = new Date("2026-09-21T10:00:00Z");
  assert.equal(dueIsReady({ due_on: "2020-01-01" }, now), true);
  assert.equal(taskSignal("vip-ai-research", { gid: "123", name: "R: Monthly", due_on: "2026-10-15" }, { firstSeen: true, now }), null);
  assert.equal(taskSignal("vip-ai-research", { gid: "123", name: "R: Monthly", due_on: "2020-01-01" }, { firstSeen: true, now })?.kind, "due_task");
});

test("new undated manual task gets one stable signal", () => {
  const task = { gid: "123", name: "Please research this" };
  const first = taskSignal("vip-ai-research", task, { firstSeen: true });
  assert.equal(first.kind, "new_assigned_task");
  assert.equal(taskSignal("vip-ai-research", task, { firstSeen: false }), null);
  assert.equal(first.id, taskSignal("vip-ai-research", task, { firstSeen: true }).id);
});

test("comment mentions require real rich-text user link", () => {
  assert.equal(storyMentionsUser({ html_text: '<a href="https://app.asana.com/0/111/profile/456">@Research</a>' }, "456"), true);
  assert.equal(storyMentionsUser({ html_text: "@Research please look" }, "456"), false);
  assert.equal(storyMentionsUser({ html_text: '<a href="https://evil.example/456">@Research</a>' }, "456"), false);
});

test("only new human comments enqueue; observers need verified mention", () => {
  const stories = [
    { gid: "10", created_at: "2026-09-21T09:01:00Z", type: "comment", created_by: { gid: "900" }, html_text: "hello" },
    { gid: "11", created_at: "2026-09-21T09:02:00Z", type: "comment", created_by: { gid: "456" }, html_text: "agent reply" },
    { gid: "12", created_at: "2026-09-21T09:03:00Z", type: "comment", created_by: { gid: "900" }, html_text: '<a data-asana-gid="456">@Research</a>' }
  ];
  const options = { observation: { latest_story_at: "2026-09-21T09:01:00Z", latest_story_gid: "10" }, userGid: "456", allAgentUserGids: new Set(["456"]), owned: false, now: new Date("2026-09-21T10:00:00Z") };
  const result = newCommentSignals("vip-ai-research", { gid: "123" }, stories, options);
  assert.deepEqual(result.signals.map((signal) => signal.story_gid), ["12"]);
  assert.equal(result.latest.gid, "12");
});
