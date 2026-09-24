import assert from "node:assert/strict";

import {
  fetchInstagramBusinessDiscoveryImage,
  getInstagramBusinessDiscoveryConfig,
  getInstagramBusinessDiscoveryMedia,
  getInstagramBusinessDiscoveryProfile,
  normalizeInstagramCdnUrl
} from "../lib/instagram-business-discovery.js";

const secret = "test-secret-never-output";
const env = { META_IG_SYSTEM_USER_TOKEN: secret };
const fixedNow = () => new Date("2026-09-21T11:40:00.000Z");
const graphUrl = "https://graph.facebook.com/v26.0/17841421933756277";

const config = getInstagramBusinessDiscoveryConfig(env);
assert.equal(config.token_configured, true);
assert.equal(config.token_env_name, "META_IG_SYSTEM_USER_TOKEN");
assert.equal(JSON.stringify(config).includes(secret), false);

let graphCalls = 0;
const graphGet = async (url, options) => {
  graphCalls += 1;
  assert.equal(url, graphUrl);
  assert.equal(options.headers.Authorization, `Bearer ${secret}`);
  assert.equal(options.params.access_token, undefined);
  assert.equal(options.maxRedirects, 0);
  if (options.params.fields.includes("media.limit(2)")) {
    return {
      status: 200,
      data: {
        business_discovery: {
          id: "17841463062582858",
          username: "daytrading",
          media: {
            data: [
              {
                id: "18117092659752744",
                media_type: "CAROUSEL_ALBUM",
                caption: "Example caption",
                permalink: "https://www.instagram.com/p/example/",
                timestamp: "2026-09-20T22:48:30+0000",
                children: {
                  data: [
                    { id: "1", media_type: "IMAGE", media_url: "https://scontent-muc2-1.cdninstagram.com/a.jpg" },
                    { id: "2", media_type: "IMAGE" }
                  ],
                  paging: { next: "https://graph.facebook.com/next" }
                }
              },
              { id: "18127227922750123", media_type: "CAROUSEL_ALBUM" }
            ],
            paging: { next: "https://graph.facebook.com/next" }
          }
        }
      }
    };
  }
  assert.equal(
    options.params.fields,
    "business_discovery.username(daytrading){id,username,followers_count,media_count}"
  );
  return {
    status: 200,
    data: {
      business_discovery: {
        id: "17841463062582858",
        username: "daytrading",
        followers_count: 2053196,
        media_count: 3091
      }
    }
  };
};

const profile = await getInstagramBusinessDiscoveryProfile(
  { username: "@DayTrading" },
  { env, httpGet: graphGet, now: fixedNow }
);
assert.equal(profile.profile.username, "daytrading");
assert.equal(profile.profile.followers_count, 2053196);
assert.equal(profile.fetched_at, "2026-09-21T11:40:00.000Z");
assert.equal(JSON.stringify(profile).includes(secret), false);

const media = await getInstagramBusinessDiscoveryMedia(
  { username: "daytrading", limit: 2 },
  { env, httpGet: graphGet, now: fixedNow }
);
assert.equal(media.media.length, 2);
assert.equal(media.media[0].children.length, 2);
assert.equal(media.media[0].children[0].media_type, "IMAGE");
assert.equal(media.media[0].children[1].media_url, null);
assert.equal(media.media[0].children_complete, false);
assert.equal(media.media[1].children_complete, false);
assert.equal(media.has_more_media, true);
assert.equal(JSON.stringify(media).includes(secret), false);
assert.equal(graphCalls, 2);

await assert.rejects(
  getInstagramBusinessDiscoveryProfile(
    { username: "daytrading){id,access_token}" },
    { env, httpGet: graphGet }
  ),
  /ungueltig/
);
await assert.rejects(
  getInstagramBusinessDiscoveryMedia({ username: "daytrading", limit: 11 }, { env, httpGet: graphGet }),
  /limit/
);
await assert.rejects(
  getInstagramBusinessDiscoveryProfile({ username: "daytrading" }, { env: {}, httpGet: graphGet }),
  /META_IG_SYSTEM_USER_TOKEN/
);
await assert.rejects(
  getInstagramBusinessDiscoveryProfile(
    { username: "daytrading" },
    {
      env,
      httpGet: async () => {
        throw {
          message: secret,
          response: { status: 403, data: { error: { code: 10, message: secret } } }
        };
      }
    }
  ),
  (error) => {
    assert.match(error.message, /HTTP 403, Meta-Code 10/);
    assert.equal(error.message.includes(secret), false);
    return true;
  }
);

await assert.rejects(
  getInstagramBusinessDiscoveryProfile(
    { username: "daytrading" },
    {
      env,
      httpGet: async () => {
        throw {
          response: {
            status: 400,
            data: {
              error: {
                type: "OAuthException",
                code: 200,
                error_subcode: 2332002,
                message: `Application does not have permission for this action; access_token=${secret}`
              }
            }
          }
        };
      }
    }
  ),
  (error) => {
    assert.match(error.message, /HTTP 400, Meta-Code 200, Subcode 2332002, Typ OAuthException/);
    assert.match(error.message, /Application does not have permission for this action/);
    assert.equal(error.message.includes(secret), false);
    return true;
  }
);

const cdnUrl = "https://scontent-muc2-1.cdninstagram.com/v/t51.29350-15/example.jpg?foo=bar";
assert.equal(normalizeInstagramCdnUrl(cdnUrl), cdnUrl);
for (const blocked of [
  "http://scontent-muc2-1.cdninstagram.com/x.jpg",
  "https://cdninstagram.com.evil.example/x.jpg",
  "https://127.0.0.1/x.jpg",
  "https://scontent-muc2-1.cdninstagram.com/x.jpg?access_token=secret"
]) {
  assert.throws(() => normalizeInstagramCdnUrl(blocked), /Meta-CDNs/);
}

const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0xd9]);
const image = await fetchInstagramBusinessDiscoveryImage(
  { media_url: cdnUrl },
  {
    now: fixedNow,
    httpGet: async (url, options) => {
      assert.equal(url, cdnUrl);
      assert.equal(options.headers.Authorization, undefined);
      assert.equal(options.maxRedirects, 0);
      assert.equal(options.responseType, "arraybuffer");
      return { status: 200, headers: { "content-type": "image/jpeg" }, data: jpeg };
    }
  }
);
assert.equal(image.mimeType, "image/jpeg");
assert.equal(image.byteLength, jpeg.length);
assert.deepEqual(image.bytes, jpeg);

let retries = 0;
const retriedImage = await fetchInstagramBusinessDiscoveryImage(
  { media_url: cdnUrl },
  {
    wait: async () => {},
    httpGet: async () => {
      retries += 1;
      if (retries === 1) throw { code: "ECONNRESET" };
      return { status: 200, headers: { "content-type": "image/jpeg" }, data: jpeg };
    }
  }
);
assert.equal(retries, 2);
assert.equal(retriedImage.byteLength, jpeg.length);

let deniedCalls = 0;
await assert.rejects(
  fetchInstagramBusinessDiscoveryImage(
    { media_url: cdnUrl },
    {
      wait: async () => {},
      httpGet: async () => {
        deniedCalls += 1;
        throw { response: { status: 403 } };
      }
    }
  ),
  /HTTP 403/
);
assert.equal(deniedCalls, 1);

await assert.rejects(
  fetchInstagramBusinessDiscoveryImage(
    { media_url: cdnUrl },
    { wait: async () => {}, httpGet: async () => { throw { code: "ERR_BAD_RESPONSE", message: secret }; } }
  ),
  (error) => {
    assert.match(error.message, /Transport ERR_BAD_RESPONSE/);
    assert.equal(error.message.includes(secret), false);
    return true;
  }
);

let activeDownloads = 0;
let maximumDownloads = 0;
await Promise.all(Array.from({ length: 4 }, () => fetchInstagramBusinessDiscoveryImage(
  { media_url: cdnUrl },
  {
    httpGet: async () => {
      activeDownloads += 1;
      maximumDownloads = Math.max(maximumDownloads, activeDownloads);
      await new Promise((resolve) => setTimeout(resolve, 5));
      activeDownloads -= 1;
      return { status: 200, headers: { "content-type": "image/jpeg" }, data: jpeg };
    }
  }
)));
assert.equal(maximumDownloads, 2);

await assert.rejects(
  fetchInstagramBusinessDiscoveryImage(
    { media_url: cdnUrl },
    { httpGet: async () => ({ status: 302, headers: { location: "http://127.0.0.1/" } }) }
  ),
  /HTTP 302/
);
await assert.rejects(
  fetchInstagramBusinessDiscoveryImage(
    { media_url: cdnUrl },
    { httpGet: async () => ({ status: 200, headers: { "content-type": "text/html" }, data: Buffer.from("<html>") }) }
  ),
  /kein unterstuetztes Bild/
);
await assert.rejects(
  fetchInstagramBusinessDiscoveryImage(
    { media_url: cdnUrl },
    { httpGet: async () => ({ status: 200, headers: { "content-type": "image/jpeg" }, data: Buffer.alloc(8_000_001) }) }
  ),
  /Groessenlimit/
);

console.log("instagram business discovery selftest: ok");
