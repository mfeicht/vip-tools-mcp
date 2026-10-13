const DAY_MS = 86_400_000;

function dateMs(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error("schedule_rule.anchor_due_on must be a YYYY-MM-DD date");
  }
  const ms = Date.parse(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(ms) || new Date(ms).toISOString().slice(0, 10) !== value) {
    throw new Error("schedule_rule.anchor_due_on is not a valid calendar date");
  }
  return ms;
}

function berlinParts(date) {
  const values = Object.fromEntries(new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Berlin", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"
  }).formatToParts(date).map((part) => [part.type, part.value]));
  return { date: `${values.year}-${values.month}-${values.day}`,
    time: `${values.hour}:${values.minute}:${values.second}` };
}

export function isOpenRoutineTask(task) {
  const routine = /^\s*R\s*:/i.test(String(task?.name || "")) ||
    (task?.tags || []).some((tag) =>
      /^(routine|routinen|wiederkehrend)$/i.test(String(tag?.name || "").trim()));
  return routine && !task?.completed;
}

export function assertRoutineDueCanBeCleared(task) {
  if (isOpenRoutineTask(task)) {
    throw new Error("Eine offene Routine darf ihre Fälligkeit nicht verlieren. Belegten Rhythmus prüfen und den Reparaturpfad nutzen.");
  }
}

export function routineFutureRescheduleRequiresCadence(task, proposedSchedule, now = new Date()) {
  if (!isOpenRoutineTask(task)) return false;
  const today = berlinParts(now).date;
  const proposedIsFuture = proposedSchedule.expected_due_at
    ? Date.parse(proposedSchedule.expected_due_at) > now.getTime()
    : proposedSchedule.expected_due_on > today;
  return proposedIsFuture;
}

function monthlySlot(anchorMs, interval, monthOffset, shortMonth) {
  const anchor = new Date(anchorMs);
  const first = new Date(Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth() + monthOffset, 1));
  const lastDay = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 0)).getUTCDate();
  const day = anchor.getUTCDate();
  if (day > lastDay && shortMonth === "skip") return null;
  if (day > lastDay && shortMonth !== "last_day") {
    throw new Error("A monthly day absent in a short month needs short_month=skip or last_day");
  }
  return Date.UTC(first.getUTCFullYear(), first.getUTCMonth(), Math.min(day, lastDay));
}

export function nextRoutineCadenceSlot(rule, sourceTask, now = new Date()) {
  if (!rule || !["day", "week", "month"].includes(rule.unit) ||
      !Number.isInteger(rule.interval) || rule.interval < 1 || rule.interval > 366) {
    throw new Error("A documented day, week or month schedule_rule with positive interval is required");
  }
  if (rule.unit === "month" && rule.interval > 120) {
    throw new Error("Monthly schedule_rule.interval must be at most 120");
  }
  const anchorMs = dateMs(rule.anchor_due_on);
  const sourceDueOn = sourceTask?.due_on ||
    (sourceTask?.due_at ? berlinParts(new Date(sourceTask.due_at)).date : null);
  if (!sourceDueOn || !Number.isFinite(Date.parse(`${sourceDueOn}T00:00:00Z`))) {
    throw new Error("The completed source task needs a valid due date");
  }
  const sourceMs = dateMs(sourceDueOn);
  if (anchorMs > sourceMs) throw new Error("Schedule anchor cannot follow the source due date");
  const stepDays = rule.unit === "day" ? rule.interval : rule.unit === "week" ? rule.interval * 7 : null;
  if (stepDays && ((sourceMs - anchorMs) / DAY_MS) % stepDays !== 0) {
    throw new Error("The source due date is not on the documented cadence");
  }
  if (rule.unit === "month") {
    const anchor = new Date(anchorMs);
    const source = new Date(sourceMs);
    const monthDelta = (source.getUTCFullYear() - anchor.getUTCFullYear()) * 12 +
      source.getUTCMonth() - anchor.getUTCMonth();
    if (monthDelta % rule.interval !== 0 ||
        monthlySlot(anchorMs, rule.interval, monthDelta, rule.short_month) !== sourceMs) {
      throw new Error("The source due date is not on the documented monthly cadence");
    }
  }
  const nowLocal = berlinParts(now);
  const sourceTime = sourceTask.due_at ? berlinParts(new Date(sourceTask.due_at)).time : null;
  const nowMs = dateMs(nowLocal.date);
  let candidateMs;
  if (stepDays) {
    const days = Math.max(0, Math.floor((nowMs - anchorMs) / DAY_MS));
    candidateMs = anchorMs + Math.ceil(days / stepDays) * stepDays * DAY_MS;
  } else {
    const anchor = new Date(anchorMs);
    const today = new Date(nowMs);
    const months = Math.max(0, (today.getUTCFullYear() - anchor.getUTCFullYear()) * 12 +
      today.getUTCMonth() - anchor.getUTCMonth());
    let monthOffset = Math.floor(months / rule.interval) * rule.interval;
    for (let i = 0; i < 1200; i += 1, monthOffset += rule.interval) {
      candidateMs = monthlySlot(anchorMs, rule.interval, monthOffset, rule.short_month);
      if (candidateMs !== null && candidateMs >= nowMs) break;
    }
    if (candidateMs === null || candidateMs < nowMs) throw new Error("No future monthly slot found");
  }
  if (candidateMs === nowMs && (!sourceTime || sourceTime <= nowLocal.time)) {
    if (stepDays) candidateMs += stepDays * DAY_MS;
    else {
      const anchor = new Date(anchorMs);
      const candidate = new Date(candidateMs);
      let offset = (candidate.getUTCFullYear() - anchor.getUTCFullYear()) * 12 +
        candidate.getUTCMonth() - anchor.getUTCMonth() + rule.interval;
      for (let i = 0; i < 1200; i += 1, offset += rule.interval) {
        candidateMs = monthlySlot(anchorMs, rule.interval, offset, rule.short_month);
        if (candidateMs !== null) break;
      }
    }
  }
  return { due_on: new Date(candidateMs).toISOString().slice(0, 10),
    due_time_berlin: sourceTime };
}

export function assertRoutineCatchupOnCadence(rule, sourceTask, proposedSchedule, now = new Date()) {
  const expected = nextRoutineCadenceSlot(rule, sourceTask, now);
  const proposedAt = proposedSchedule.expected_due_at
    ? berlinParts(new Date(proposedSchedule.expected_due_at)) : null;
  const proposedDate = proposedAt?.date || proposedSchedule.expected_due_on;
  const proposedTime = proposedAt?.time || null;
  if (proposedDate !== expected.due_on || proposedTime !== expected.due_time_berlin) {
    throw new Error(`Catch-up due is off the documented cadence; expected ${expected.due_on}` +
      (expected.due_time_berlin ? ` ${expected.due_time_berlin} Europe/Berlin` : ""));
  }
  return expected;
}
