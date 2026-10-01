import assert from "node:assert/strict";
import test from "node:test";

import { evaluateGeneratedImageCleanup } from "./generated-image-retention.js";

const baseFile = {
  id: "drive-file-1",
  createdTime: "2026-08-01T08:00:00Z",
  trashed: false,
  appProperties: {
    provider: "google-gemini",
    pipeline: "gemini-batch-v1",
    project_key: "tradingpulse"
  }
};
const sentPost = {
  id: "abcdefabcdefabcdefabcdef",
  status: "sent",
  sentAt: "2026-08-02T08:00:00Z"
};

test("allows trash only after the seven-day correction window", () => {
  const early = evaluateGeneratedImageCleanup({
    file: baseFile,
    post: sentPost,
    projectKey: "tradingpulse",
    action: "trash",
    now: "2026-08-08T07:59:59Z"
  });
  assert.equal(early.eligible, false);
  assert.ok(early.reasons.includes("correction_window_active"));

  const mature = evaluateGeneratedImageCleanup({
    file: baseFile,
    post: sentPost,
    projectKey: "tradingpulse",
    action: "trash",
    now: "2026-08-09T08:00:00Z"
  });
  assert.equal(mature.eligible, true);
});

test("blocks unsent posts and project mismatches", () => {
  const result = evaluateGeneratedImageCleanup({
    file: baseFile,
    post: { ...sentPost, status: "scheduled" },
    projectKey: "aipulse",
    action: "trash",
    now: "2026-10-01T00:00:00Z"
  });
  assert.equal(result.eligible, false);
  assert.ok(result.reasons.includes("buffer_post_not_sent"));
  assert.ok(result.reasons.includes("project_mismatch"));
});

test("allows permanent deletion only 30 days after the tool recorded trash binding", () => {
  const trashedFile = {
    ...baseFile,
    trashed: true,
    appProperties: {
      ...baseFile.appProperties,
      vip_cleanup_post: sentPost.id,
      vip_cleanup_project: "tradingpulse",
      vip_trashed_at: "2026-08-10T08:00:00Z"
    }
  };
  const early = evaluateGeneratedImageCleanup({
    file: trashedFile,
    post: sentPost,
    projectKey: "tradingpulse",
    action: "permanent_delete",
    now: "2026-09-09T07:59:59Z"
  });
  assert.equal(early.eligible, false);
  assert.ok(early.reasons.includes("trash_retention_window_active"));

  const mature = evaluateGeneratedImageCleanup({
    file: trashedFile,
    post: sentPost,
    projectKey: "tradingpulse",
    action: "permanent_delete",
    now: "2026-09-10T08:00:00Z"
  });
  assert.equal(mature.eligible, true);
});

