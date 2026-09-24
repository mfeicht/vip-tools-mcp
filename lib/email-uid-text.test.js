import test from "node:test";
import assert from "node:assert/strict";
import { selectBoundedEmailText } from "./email-uid-text.js";

test("prefers plain text and reports complete coverage", () => {
  assert.deepEqual(selectBoundedEmailText({ plainText: "  Editorial copy  ", htmlText: "HTML copy" }), {
    text: "Editorial copy",
    source: "text/plain",
    total_chars: 14,
    offset_chars: 0,
    end_chars: 14,
    has_more: false,
    coverage_status: "complete"
  });
});

test("returns bounded windows and explicit partial coverage", () => {
  const result = selectBoundedEmailText({ htmlText: "abcdefghij", offsetChars: 3, limitChars: 4 });
  assert.equal(result.text, "defg");
  assert.equal(result.source, "text/html");
  assert.equal(result.total_chars, 10);
  assert.equal(result.end_chars, 7);
  assert.equal(result.has_more, true);
  assert.equal(result.coverage_status, "partial");
});

test("reports empty content without claiming coverage", () => {
  const result = selectBoundedEmailText({});
  assert.equal(result.text, "");
  assert.equal(result.coverage_status, "no_text");
});
