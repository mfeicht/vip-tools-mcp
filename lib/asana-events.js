function requireNumericGid(value, label) {
  const normalized = String(value || "").trim();
  if (!/^\d+$/.test(normalized)) throw new Error(`${label} muss eine numerische Asana-GID sein.`);
  return normalized;
}

function requireOptionalToken(value) {
  if (value === undefined || value === null) return undefined;
  const normalized = String(value).trim();
  if (!normalized) throw new Error("sync darf nicht leer sein.");
  if (normalized.length > 2048) throw new Error("sync ist unerwartet lang.");
  return normalized;
}

export function extractAsanaEventsSyncToken(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
  for (const candidate of [payload.sync, payload.data?.sync, payload.error?.sync]) {
    const token = String(candidate || "").trim();
    if (token) return token;
  }
  return null;
}

export async function readAsanaResourceEvents({ request, resourceGid, sync, optFields }) {
  if (typeof request !== "function") throw new Error("request muss eine Funktion sein.");
  const resource = requireNumericGid(resourceGid, "resource_gid");
  const inputSync = requireOptionalToken(sync);
  const params = { resource };
  if (inputSync) params.sync = inputSync;
  if (String(optFields || "").trim()) params.opt_fields = String(optFields).trim();

  try {
    const response = await request({ method: "GET", url: "/events", params });
    const payload = response?.data;
    const nextSync = extractAsanaEventsSyncToken(payload);
    if (!nextSync) throw new Error("Asana Events lieferte HTTP 200 ohne neuen Sync-Token; Cursor bleibt unveraendert.");
    if (!Array.isArray(payload?.data)) throw new Error("Asana Events lieferte HTTP 200 ohne Event-Liste.");
    return {
      events_status: "ok",
      resource_gid: resource,
      sync_token: nextSync,
      has_more: payload.has_more === true,
      event_count: payload.data.length,
      events: payload.data,
      reconciliation_required: true
    };
  } catch (error) {
    if (error?.response?.status !== 412) throw error;
    const nextSync = extractAsanaEventsSyncToken(error.response.data);
    if (!nextSync) {
      const wrapped = new Error("Asana Events lieferte HTTP 412 ohne neuen Sync-Token; kein Cursor darf fortgeschrieben werden.");
      wrapped.cause = error;
      throw wrapped;
    }
    return {
      events_status: inputSync ? "sync_reset_required" : "bootstrap_required",
      resource_gid: resource,
      sync_token: nextSync,
      has_more: false,
      event_count: 0,
      events: [],
      reconciliation_required: true
    };
  }
}
