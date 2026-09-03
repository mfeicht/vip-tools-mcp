export function selectImapUidWindow(uids, { limit, offset = 0, order = "newest_first" }) {
  const normalizedOrder = String(order || "newest_first");
  if (!new Set(["newest_first", "oldest_first"]).has(normalizedOrder)) {
    throw new Error(`Unbekannte IMAP-Sortierung: ${normalizedOrder}`);
  }

  const normalized = [...new Set((uids || []).filter((value) => /^\d+$/.test(String(value))))]
    .map(String)
    .sort((left, right) => Number(left) - Number(right));
  if (normalizedOrder === "newest_first") normalized.reverse();

  return normalized.slice(offset, offset + limit);
}

export function selectImapUidPage(
  uids,
  { limit, cursorUid = null, order = "oldest_first", excludeUids = [] }
) {
  const normalizedOrder = String(order || "oldest_first");
  if (!new Set(["newest_first", "oldest_first"]).has(normalizedOrder)) {
    throw new Error(`Unbekannte IMAP-Sortierung: ${normalizedOrder}`);
  }
  const normalizedLimit = Math.max(1, Number(limit) || 1);
  const excluded = new Set(
    (excludeUids || []).filter((value) => /^\d+$/.test(String(value))).map(String)
  );
  const normalizedCursor = /^\d+$/.test(String(cursorUid || ""))
    ? Number(cursorUid)
    : null;
  const normalized = [...new Set((uids || []).filter((value) => /^\d+$/.test(String(value))))]
    .map(String)
    .filter((uid) => !excluded.has(uid))
    .sort((left, right) => Number(left) - Number(right));
  if (normalizedOrder === "newest_first") normalized.reverse();

  const afterCursor = normalizedCursor === null
    ? normalized
    : normalized.filter((uid) =>
        normalizedOrder === "oldest_first"
          ? Number(uid) > normalizedCursor
          : Number(uid) < normalizedCursor
      );
  const selected = afterCursor.slice(0, normalizedLimit);
  const hasMore = afterCursor.length > selected.length;
  const lastUid = selected.at(-1) || null;

  return {
    uids: selected,
    order: normalizedOrder,
    cursor_uid: normalizedCursor === null ? null : String(normalizedCursor),
    last_uid: lastUid,
    next_cursor_uid: hasMore ? lastUid : null,
    has_more: hasMore,
    remaining_count: Math.max(0, afterCursor.length - selected.length)
  };
}

export function sortImapMessagesByUidWindow(messages, selectedUids) {
  const position = new Map((selectedUids || []).map((uid, index) => [String(uid), index]));
  return [...(messages || [])].sort(
    (left, right) =>
      (position.get(String(left?.uid)) ?? Number.MAX_SAFE_INTEGER) -
      (position.get(String(right?.uid)) ?? Number.MAX_SAFE_INTEGER)
  );
}
