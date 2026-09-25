import assert from "node:assert/strict";
import test from "node:test";
import { assertBufferEditDueAtReadback, bufferEditScheduleFields } from "../lib/buffer-edit-schedule.js";

const target = "2026-09-26T15:00:00.000Z";

test("rescheduling sends customScheduled with the requested UTC time", () => {
  assert.equal(bufferEditScheduleFields(undefined, (x) => x), "");
  assert.equal(
    bufferEditScheduleFields(target, (x) => x),
    `\n        mode: customScheduled\n        dueAt: "${target}"`
  );
});

test("rescheduling requires both mutation and persisted post to show the target", () => {
  assert.doesNotThrow(() => assertBufferEditDueAtReadback(target,
    { dueAt: "2026-09-26T15:00:00Z" }, { dueAt: target }));
  assert.throws(() => assertBufferEditDueAtReadback(target,
    { dueAt: target }, { dueAt: "2026-09-26T14:00:00.000Z" }), /Termin nicht angewendet/);
  assert.throws(() => assertBufferEditDueAtReadback(target,
    { dueAt: "2026-09-26T14:00:00.000Z" }, { dueAt: target }), /Termin nicht angewendet/);
});
