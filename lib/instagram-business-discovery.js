import axios from "axios";

const DEFAULT_GRAPH_VERSION = "v26.0";
const DEFAULT_SOURCE_IG_USER_ID = "17841421933756277";
const GRAPH_ORIGIN = "https://graph.facebook.com";
const MAX_MEDIA = 10;
const MAX_IMAGE_BYTES = 8_000_000;
const GRAPH_TIMEOUT_MS = 20_000;
const MAX_CONCURRENT_IMAGE_FETCHES = 2;
const IMAGE_RETRY_DELAY_MS = 300;
const USERNAME_PATTERN = /^[A-Za-z0-9._]{1,30}$/;
const ID_PATTERN = /^\d{10,25}$/;
const VERSION_PATTERN = /^v\d{1,3}\.\d{1,2}$/;
const IMAGE_MIME_TYPES = new Set(["image/jpeg", "image/png", "image/webp"]);
let activeImageFetches = 0;
const imageFetchWaiters = [];

async function withImageFetchSlot(work) {
  if (activeImageFetches < MAX_CONCURRENT_IMAGE_FETCHES && imageFetchWaiters.length === 0) {
    activeImageFetches += 1;
  } else {
    await new Promise((resolve) => imageFetchWaiters.push(resolve));
  }
  try {
    return await work();
  } finally {
    const next = imageFetchWaiters.shift();
    if (next) next();
    else activeImageFetches -= 1;
  }
}

function isRetryableImageError(error) {
  const status = Number(error?.response?.status);
  if (Number.isInteger(status) && status > 0) {
    return status === 429 || (status >= 500 && status <= 599);
  }
  return ["ECONNABORTED", "ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ENOTFOUND", "ERR_NETWORK"].includes(error?.code);
}

function imageTransportCode(error) {
  const code = error?.code || error?.cause?.code;
  return typeof code === "string" && /^[A-Z][A-Z0-9_]{1,40}$/.test(code) ? code : "";
}

function getConfig(env = process.env) {
  const sourceIgUserId = String(env.META_IG_ACCOUNT_ID || DEFAULT_SOURCE_IG_USER_ID).trim();
  const apiVersion = String(env.META_IG_GRAPH_API_VERSION || DEFAULT_GRAPH_VERSION).trim();
  if (!ID_PATTERN.test(sourceIgUserId)) throw new Error("META_IG_ACCOUNT_ID ist ungueltig.");
  if (!VERSION_PATTERN.test(apiVersion)) throw new Error("META_IG_GRAPH_API_VERSION ist ungueltig.");
  return {
    token: String(env.META_IG_SYSTEM_USER_TOKEN || "").trim(),
    sourceIgUserId,
    apiVersion
  };
}

export function getInstagramBusinessDiscoveryConfig(env = process.env) {
  const config = getConfig(env);
  return {
    token_configured: Boolean(config.token),
    token_env_name: "META_IG_SYSTEM_USER_TOKEN",
    source_ig_user_id: config.sourceIgUserId,
    graph_api_version: config.apiVersion,
    max_media_per_call: MAX_MEDIA,
    max_image_bytes: MAX_IMAGE_BYTES
  };
}

function requireToken(config) {
  if (!config.token) throw new Error("META_IG_SYSTEM_USER_TOKEN ist nicht konfiguriert.");
}

function normalizeUsername(value) {
  const username = String(value || "").trim().replace(/^@/, "");
  if (!USERNAME_PATTERN.test(username)) {
    throw new Error("Instagram-Benutzername ist ungueltig.");
  }
  return username.toLowerCase();
}

function normalizeLimit(value) {
  if (!Number.isInteger(value) || value < 1 || value > MAX_MEDIA) {
    throw new Error(`limit muss zwischen 1 und ${MAX_MEDIA} liegen.`);
  }
  return value;
}

function sanitizeMetaDiagnostic(value, secrets = []) {
  if (typeof value !== "string") return "";
  let sanitized = value;
  for (const secret of secrets) {
    if (secret) sanitized = sanitized.split(secret).join("[redacted]");
  }
  sanitized = sanitized
    .replace(/((?:access[_-]?token|authorization)\s*[:=]\s*)([^&\s,;]+)/gi, "$1[redacted]")
    .replace(/\b(?:EAA|IGQ)[A-Za-z0-9_-]{20,}\b/g, "[redacted]")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 300);
  return sanitized === "[redacted]" ? "" : sanitized;
}

function graphError(error, secrets = []) {
  const metaError = error?.response?.data?.error;
  const status = Number(error?.response?.status);
  const code = Number(metaError?.code);
  const subcode = Number(metaError?.error_subcode);
  const type = typeof metaError?.type === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(metaError.type)
    ? metaError.type
    : "";
  const message = sanitizeMetaDiagnostic(
    metaError?.error_user_msg || metaError?.message,
    secrets
  );
  const parts = [];
  if (Number.isInteger(status) && status >= 100 && status <= 599) parts.push(`HTTP ${status}`);
  if (Number.isInteger(code)) parts.push(`Meta-Code ${code}`);
  if (Number.isInteger(subcode)) parts.push(`Subcode ${subcode}`);
  if (type) parts.push(`Typ ${type}`);
  if (message) parts.push(`Meta-Meldung: ${message}`);
  return new Error(`Instagram Graph API-Abfrage fehlgeschlagen${parts.length ? ` (${parts.join(", ")})` : ""}.`);
}

async function graphRead(fields, config, httpGet) {
  requireToken(config);
  let response;
  try {
    response = await httpGet(`${GRAPH_ORIGIN}/${config.apiVersion}/${config.sourceIgUserId}`, {
      params: { fields },
      headers: { Authorization: `Bearer ${config.token}`, Accept: "application/json" },
      timeout: GRAPH_TIMEOUT_MS,
      maxRedirects: 0,
      maxContentLength: 2_000_000
    });
  } catch (error) {
    throw graphError(error, [config.token]);
  }
  if (response?.status && (response.status < 200 || response.status >= 300)) {
    throw graphError({ response }, [config.token]);
  }
  const discovery = response?.data?.business_discovery;
  if (!discovery || typeof discovery !== "object" || Array.isArray(discovery)) {
    throw new Error("Instagram Graph API lieferte keine Business-Discovery-Daten.");
  }
  return discovery;
}

function mediaUrl(value) {
  return typeof value === "string" && value.startsWith("https://") ? value : null;
}

function normalizeMedia(item) {
  const children = Array.isArray(item?.children?.data)
    ? item.children.data.map((child) => ({
        id: String(child?.id || ""),
        media_type: String(child?.media_type || ""),
        media_url: mediaUrl(child?.media_url)
      }))
    : [];
  return {
    id: String(item?.id || ""),
    media_type: String(item?.media_type || ""),
    caption: typeof item?.caption === "string" ? item.caption.slice(0, 5_000) : null,
    permalink: typeof item?.permalink === "string" ? item.permalink : null,
    timestamp: typeof item?.timestamp === "string" ? item.timestamp : null,
    media_url: mediaUrl(item?.media_url),
    children,
    children_complete:
      item?.media_type !== "CAROUSEL_ALBUM" ||
      (Array.isArray(item?.children?.data) && !item.children.paging?.next)
  };
}

export async function getInstagramBusinessDiscoveryProfile({ username }, { env = process.env, httpGet = axios.get, now = () => new Date() } = {}) {
  const normalizedUsername = normalizeUsername(username);
  const config = getConfig(env);
  const discovery = await graphRead(
    `business_discovery.username(${normalizedUsername}){id,username,followers_count,media_count}`,
    config,
    httpGet
  );
  return {
    source: "Meta Instagram Graph API Business Discovery",
    fetched_at: now().toISOString(),
    source_ig_user_id: config.sourceIgUserId,
    profile: {
      id: String(discovery.id || ""),
      username: String(discovery.username || ""),
      followers_count: discovery.followers_count ?? null,
      media_count: discovery.media_count ?? null
    }
  };
}

export async function getInstagramBusinessDiscoveryMedia(
  { username, limit = 3 },
  { env = process.env, httpGet = axios.get, now = () => new Date() } = {}
) {
  const normalizedUsername = normalizeUsername(username);
  const normalizedLimit = normalizeLimit(limit);
  const config = getConfig(env);
  const discovery = await graphRead(
    `business_discovery.username(${normalizedUsername}){id,username,media.limit(${normalizedLimit}){id,caption,media_type,media_url,permalink,timestamp,children{id,media_type,media_url}}}`,
    config,
    httpGet
  );
  const items = Array.isArray(discovery.media?.data) ? discovery.media.data : [];
  return {
    source: "Meta Instagram Graph API Business Discovery",
    fetched_at: now().toISOString(),
    source_ig_user_id: config.sourceIgUserId,
    target_username: String(discovery.username || normalizedUsername),
    media: items.map(normalizeMedia),
    has_more_media: Boolean(discovery.media?.paging?.next),
    note: "media_url kann fehlen oder spaeter ungueltig werden; diese Antwort belegt keine visuelle Auswertung."
  };
}

export function normalizeInstagramCdnUrl(value) {
  if (typeof value !== "string" || value.length > 6_000) {
    throw new Error("Instagram-Medienadresse ist ungueltig.");
  }
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error("Instagram-Medienadresse ist ungueltig.");
  }
  const host = url.hostname.toLowerCase();
  const allowedHost =
    host === "cdninstagram.com" ||
    host.endsWith(".cdninstagram.com") ||
    host === "fbcdn.net" ||
    host.endsWith(".fbcdn.net");
  if (url.protocol !== "https:" || !allowedHost || url.username || url.password || url.port || url.searchParams.has("access_token")) {
    throw new Error("Nur direkte HTTPS-Medienadressen von Meta-CDNs sind erlaubt.");
  }
  url.hash = "";
  return url.toString();
}

function hasImageSignature(bytes, mimeType) {
  if (mimeType === "image/jpeg") return bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff;
  if (mimeType === "image/png") return bytes.length >= 8 && bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]));
  if (mimeType === "image/webp") return bytes.length >= 12 && bytes.toString("ascii", 0, 4) === "RIFF" && bytes.toString("ascii", 8, 12) === "WEBP";
  return false;
}

export async function fetchInstagramBusinessDiscoveryImage(
  { media_url },
  { httpGet = axios.get, now = () => new Date(), wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) } = {}
) {
  const url = normalizeInstagramCdnUrl(media_url);
  let response;
  await withImageFetchSlot(async () => {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        response = await httpGet(url, {
          responseType: "arraybuffer",
          headers: { Accept: "image/jpeg,image/png,image/webp" },
          timeout: GRAPH_TIMEOUT_MS,
          maxRedirects: 0,
          maxContentLength: MAX_IMAGE_BYTES
        });
        return;
      } catch (error) {
        if (attempt === 0 && isRetryableImageError(error)) {
          await wait(IMAGE_RETRY_DELAY_MS);
          continue;
        }
        const status = Number(error?.response?.status);
        const code = imageTransportCode(error);
        const details = [
          Number.isInteger(status) && status > 0 ? `HTTP ${status}` : "",
          code ? `Transport ${code}` : ""
        ].filter(Boolean);
        throw new Error(`Instagram-Bild konnte nicht abgerufen werden${details.length ? ` (${details.join(", ")})` : ""}.`);
      }
    }
  });
  if (response?.status && (response.status < 200 || response.status >= 300)) {
    throw new Error(`Instagram-Bild konnte nicht abgerufen werden (HTTP ${response.status}).`);
  }
  const mimeType = String(response?.headers?.["content-type"] || "").split(";", 1)[0].trim().toLowerCase();
  const bytes = Buffer.isBuffer(response?.data) ? response.data : Buffer.from(response?.data || []);
  if (!IMAGE_MIME_TYPES.has(mimeType) || bytes.length === 0 || bytes.length > MAX_IMAGE_BYTES || !hasImageSignature(bytes, mimeType)) {
    throw new Error("Instagram-Medienantwort ist kein unterstuetztes Bild innerhalb des Groessenlimits.");
  }
  return {
    mimeType,
    bytes,
    byteLength: bytes.length,
    fetchedAt: now().toISOString()
  };
}
