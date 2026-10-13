import assert from "node:assert/strict";
import { assertRoutineCatchupOnCadence, nextRoutineCadenceSlot,
  routineFutureRescheduleRequiresCadence } from "../lib/asana-routine-cadence.js";

const salesRule = { unit: "day", interval: 2, anchor_due_on: "2026-10-03" };
const salesSource = { due_on: "2026-10-03", due_at: "2026-10-03T07:00:00.000Z" };
const salesNow = new Date("2026-10-13T18:00:00.000Z");
const overdueNext = { name: "R: Lead-Adressen hinzufügen (Badumbau für App.Goklever)",
  completed: false, due_on: "2026-10-05", due_at: "2026-10-05T07:00:00.000Z" };
assert.equal(routineFutureRescheduleRequiresCadence(overdueNext,
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), true);
assert.equal(routineFutureRescheduleRequiresCadence(overdueNext,
  { expected_due_at: "2026-10-15T07:00:00.000Z" }, salesNow), true);
assert.equal(routineFutureRescheduleRequiresCadence({ ...overdueNext, name: "Einmalige Aufgabe" },
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), false);
assert.equal(routineFutureRescheduleRequiresCadence({ ...overdueNext, completed: true },
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), false);
assert.equal(routineFutureRescheduleRequiresCadence({ ...overdueNext, due_on: "2026-10-15",
  due_at: "2026-10-15T07:00:00.000Z" },
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), true);
assert.equal(routineFutureRescheduleRequiresCadence({ ...overdueNext, due_on: "2026-10-15",
  due_at: "2026-10-15T07:00:00.000Z" },
  { expected_due_at: "2026-10-17T07:00:00.000Z" }, salesNow), true);
assert.equal(routineFutureRescheduleRequiresCadence({ name: "Andere Routine", tags: [{ name: "Routine" }],
  due_on: "2026-10-05", completed: false },
  { expected_due_on: "2026-10-14" }, salesNow), true);
assert.deepEqual(nextRoutineCadenceSlot(salesRule, salesSource, salesNow), {
  due_on: "2026-10-15", due_time_berlin: "09:00:00"
});
assert.throws(() => assertRoutineCatchupOnCadence(salesRule, salesSource,
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), /expected 2026-10-15/);
assert.deepEqual(assertRoutineCatchupOnCadence(salesRule, salesSource,
  { expected_due_at: "2026-10-15T07:00:00.000Z" }, salesNow), {
  due_on: "2026-10-15", due_time_berlin: "09:00:00"
});
assert.throws(() => assertRoutineCatchupOnCadence(salesRule, salesSource,
  { expected_due_at: "2026-10-15T08:00:00.000Z" }, salesNow), /off the documented cadence/);
assert.throws(() => nextRoutineCadenceSlot(salesRule,
  { due_on: "2026-10-04", due_at: "2026-10-04T07:00:00.000Z" }, salesNow),
  /source due date is not on/);
assert.throws(() => assertRoutineCatchupOnCadence(salesRule, overdueNext,
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), /expected 2026-10-15/);
assert.throws(() => assertRoutineCatchupOnCadence(salesRule,
  { due_on: "2026-10-15", due_at: "2026-10-15T07:00:00.000Z" },
  { expected_due_at: "2026-10-14T07:00:00.000Z" }, salesNow), /expected 2026-10-15/);

assert.deepEqual(nextRoutineCadenceSlot(
  { unit: "day", interval: 1, anchor_due_on: "2026-10-12" },
  { due_on: "2026-10-12" }, salesNow), {
  due_on: "2026-10-14", due_time_berlin: null
});
assert.deepEqual(nextRoutineCadenceSlot(
  { unit: "week", interval: 1, anchor_due_on: "2026-10-05" },
  { due_on: "2026-10-05" }, salesNow), {
  due_on: "2026-10-19", due_time_berlin: null
});
assert.deepEqual(nextRoutineCadenceSlot(
  { unit: "month", interval: 1, anchor_due_on: "2026-09-15" },
  { due_on: "2026-09-15" }, new Date("2026-10-13T18:00:00.000Z")), {
  due_on: "2026-10-15", due_time_berlin: null
});
assert.deepEqual(nextRoutineCadenceSlot(
  { unit: "month", interval: 1, anchor_due_on: "2026-01-31", short_month: "last_day" },
  { due_on: "2026-01-31" }, new Date("2026-02-01T12:00:00.000Z")), {
  due_on: "2026-02-28", due_time_berlin: null
});
assert.deepEqual(nextRoutineCadenceSlot(
  { unit: "month", interval: 1, anchor_due_on: "2026-01-31", short_month: "skip" },
  { due_on: "2026-01-31" }, new Date("2026-02-01T12:00:00.000Z")), {
  due_on: "2026-03-31", due_time_berlin: null
});
assert.throws(() => nextRoutineCadenceSlot(
  { unit: "month", interval: 1, anchor_due_on: "2026-01-31" },
  { due_on: "2026-01-31" }, new Date("2026-02-01T12:00:00.000Z")), /short_month/);

const dstRule = { unit: "day", interval: 1, anchor_due_on: "2026-10-24" };
const dstSource = { due_on: "2026-10-24", due_at: "2026-10-24T07:00:00.000Z" };
const beforeDstSlot = new Date("2026-10-25T07:00:00.000Z");
assert.deepEqual(nextRoutineCadenceSlot(dstRule, dstSource, beforeDstSlot), {
  due_on: "2026-10-25", due_time_berlin: "09:00:00"
});
assert.deepEqual(assertRoutineCatchupOnCadence(dstRule, dstSource,
  { expected_due_at: "2026-10-25T08:00:00.000Z" }, beforeDstSlot), {
  due_on: "2026-10-25", due_time_berlin: "09:00:00"
});

console.log("asana routine cadence self-test passed");
