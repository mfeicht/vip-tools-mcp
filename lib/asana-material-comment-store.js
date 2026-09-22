import { createHash, randomUUID } from "node:crypto";
import Redis from "ioredis";

const DEFAULT_NAMESPACE = "vip:asana:material-comment:v1";
const DEFAULT_CLAIM_TTL_MS = 90_000;
const DEFAULT_RECEIPT_TTL_SECONDS = 7 * 24 * 60 * 60;

function sha256(value) {
  return createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function parseJson(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    throw new Error("material_comment_store_invalid_json_fail_closed");
  }
}

function assertRedisUrl(url) {
  const value = String(url || "").trim();
  if (!/^rediss?:\/\//i.test(value)) {
    throw new Error("ASANA_MATERIAL_COMMENT_REDIS_URL must use redis:// or rediss://.");
  }
  return value;
}

function publicClaim(claim) {
  if (!claim) return null;
  return {
    token: claim.token,
    fence: Number(claim.fence),
    state: claim.state,
    payload_hash: claim.payload_hash,
    payload_probe: claim.payload_probe,
    acquired_at: claim.acquired_at,
    posting_at: claim.posting_at || null
  };
}

const ACQUIRE_SCRIPT = `
local fence = redis.call('INCR', KEYS[2])
local claim = cjson.encode({
  token = ARGV[1],
  fence = fence,
  state = 'claimed',
  payload_hash = ARGV[2],
  payload_probe = ARGV[3],
  acquired_at = ARGV[4]
})
local inserted = redis.call('SET', KEYS[1], claim, 'NX', 'PX', ARGV[5])
if inserted then
  return {1, tostring(fence), claim}
end
return {0, '0', redis.call('GET', KEYS[1]) or ''}
`;

const BEGIN_POSTING_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, 'missing_claim'} end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[1] or tostring(claim.fence) ~= ARGV[2] then
  return {0, 'fence_mismatch'}
end
if claim.state ~= 'claimed' then return {0, 'invalid_state_' .. tostring(claim.state)} end
claim.state = 'posting'
claim.posting_at = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(claim))
redis.call('PERSIST', KEYS[1])
return {1, cjson.encode(claim)}
`;

const COMPLETE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, 'missing_claim'} end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[1] or tostring(claim.fence) ~= ARGV[2] then
  return {0, 'fence_mismatch'}
end
if claim.state ~= 'posting' and ARGV[7] ~= 'reconcile' then
  return {0, 'invalid_state_' .. tostring(claim.state)}
end
local receipt = cjson.encode({
  payload_hash = ARGV[3],
  story_gid = ARGV[4],
  fence = claim.fence,
  completed_at = ARGV[5],
  completion_mode = ARGV[7]
})
redis.call('SET', KEYS[2], receipt, 'EX', ARGV[6])
redis.call('DEL', KEYS[1])
return {1, receipt}
`;

const RELEASE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
if not raw then return {1, 'already_released'} end
local claim = cjson.decode(raw)
if claim.token ~= ARGV[1] or tostring(claim.fence) ~= ARGV[2] then
  return {0, 'fence_mismatch'}
end
if claim.state ~= 'claimed' then return {0, 'posting_claim_is_fail_closed'} end
redis.call('DEL', KEYS[1])
return {1, 'released'}
`;

export function createRedisMaterialCommentStore({
  url,
  namespace = DEFAULT_NAMESPACE,
  claimTtlMs = DEFAULT_CLAIM_TTL_MS,
  receiptTtlSeconds = DEFAULT_RECEIPT_TTL_SECONDS,
  client
} = {}) {
  const redisUrl = client ? null : assertRedisUrl(url);
  const redis = client || new Redis(redisUrl, {
    lazyConnect: true,
    connectTimeout: 5_000,
    commandTimeout: 5_000,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    retryStrategy: null
  });
  if (!client) redis.on("error", () => undefined);

  async function ensureConnected() {
    if (!redis.connect || redis.status === "ready") return;
    if (["wait", "end", "close"].includes(redis.status)) await redis.connect();
    if (redis.status !== "ready" && redis.status !== "connect") {
      throw new Error("material_comment_store_not_ready_fail_closed");
    }
  }

  function keys(key, payloadHash) {
    const keyHash = sha256(key);
    return {
      claim: `${namespace}:claim:${keyHash}`,
      fence: `${namespace}:fence:${keyHash}`,
      receipt: `${namespace}:receipt:${keyHash}:${payloadHash}`
    };
  }

  async function evalScript(script, scriptKeys, args) {
    try {
      await ensureConnected();
      return await redis.eval(script, scriptKeys.length, ...scriptKeys, ...args.map(String));
    } catch (error) {
      const wrapped = new Error("material_comment_store_unavailable_fail_closed");
      wrapped.code = "MATERIAL_COMMENT_STORE_UNAVAILABLE";
      wrapped.cause = error;
      throw wrapped;
    }
  }

  return {
    async acquire({ key, payloadHash, payloadProbe, token = randomUUID(), nowIso = new Date().toISOString() }) {
      const storeKeys = keys(key, payloadHash);
      const result = await evalScript(
        ACQUIRE_SCRIPT,
        [storeKeys.claim, storeKeys.fence],
        [token, payloadHash, payloadProbe, nowIso, claimTtlMs]
      );
      return {
        acquired: Number(result?.[0]) === 1,
        claim: publicClaim(parseJson(result?.[2])),
        token,
        fence: Number(result?.[1] || 0)
      };
    },

    async beginPosting({ key, payloadHash, token, fence, nowIso = new Date().toISOString() }) {
      const storeKeys = keys(key, payloadHash);
      const result = await evalScript(
        BEGIN_POSTING_SCRIPT,
        [storeKeys.claim],
        [token, fence, nowIso]
      );
      return {
        ok: Number(result?.[0]) === 1,
        status: Number(result?.[0]) === 1 ? "posting" : String(result?.[1] || "unknown"),
        claim: Number(result?.[0]) === 1 ? publicClaim(parseJson(result?.[1])) : null
      };
    },

    async complete({
      key,
      payloadHash,
      token,
      fence,
      storyGid,
      completionMode = "posted",
      nowIso = new Date().toISOString()
    }) {
      const storeKeys = keys(key, payloadHash);
      const result = await evalScript(
        COMPLETE_SCRIPT,
        [storeKeys.claim, storeKeys.receipt],
        [token, fence, payloadHash, storyGid, nowIso, receiptTtlSeconds, completionMode]
      );
      return {
        ok: Number(result?.[0]) === 1,
        status: Number(result?.[0]) === 1 ? "completed" : String(result?.[1] || "unknown"),
        receipt: Number(result?.[0]) === 1 ? parseJson(result?.[1]) : null
      };
    },

    async release({ key, payloadHash, token, fence }) {
      const storeKeys = keys(key, payloadHash);
      const result = await evalScript(RELEASE_SCRIPT, [storeKeys.claim], [token, fence]);
      return {
        ok: Number(result?.[0]) === 1,
        status: String(result?.[1] || "unknown")
      };
    },

    async readReceipt({ key, payloadHash }) {
      const storeKeys = keys(key, payloadHash);
      try {
        await ensureConnected();
        return parseJson(await redis.get(storeKeys.receipt));
      } catch (error) {
        const wrapped = new Error("material_comment_store_unavailable_fail_closed");
        wrapped.code = "MATERIAL_COMMENT_STORE_UNAVAILABLE";
        wrapped.cause = error;
        throw wrapped;
      }
    },

    async close() {
      if (redis.quit && !["wait", "end"].includes(redis.status)) await redis.quit();
    }
  };
}

export const MATERIAL_COMMENT_STORE_DEFAULTS = {
  namespace: DEFAULT_NAMESPACE,
  claim_ttl_ms: DEFAULT_CLAIM_TTL_MS,
  receipt_ttl_seconds: DEFAULT_RECEIPT_TTL_SECONDS
};
