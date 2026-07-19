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

export function sortImapMessagesByUidWindow(messages, selectedUids) {
  const position = new Map((selectedUids || []).map((uid, index) => [String(uid), index]));
  return [...(messages || [])].sort(
    (left, right) =>
      (position.get(String(left?.uid)) ?? Number.MAX_SAFE_INTEGER) -
      (position.get(String(right?.uid)) ?? Number.MAX_SAFE_INTEGER)
  );
}
