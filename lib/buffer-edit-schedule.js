export function bufferEditScheduleFields(dueAt, escapeGraphqlString) {
  if (dueAt === undefined) return "";
  return `\n        mode: customScheduled\n        dueAt: "${escapeGraphqlString(dueAt)}"`;
}

export function assertBufferEditDueAtReadback(requestedDueAt, mutationPost, after) {
  if (requestedDueAt === undefined) return;
  const requested = Date.parse(requestedDueAt);
  const mutation = Date.parse(mutationPost?.dueAt || "");
  const persisted = Date.parse(after?.dueAt || "");
  if (!Number.isFinite(requested) || mutation !== requested || persisted !== requested) {
    throw new Error(
      `Buffer-Edit-Readback: Termin nicht angewendet (erwartet ${requestedDueAt}, ` +
      `Mutation ${mutationPost?.dueAt || "null"}, Post ${after?.dueAt || "null"}).`
    );
  }
}
