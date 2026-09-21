import assert from "node:assert/strict";
import axios from "axios";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = process.env.INSTAGRAM_BUSINESS_DISCOVERY_TEST_PORT || "39713";
const token = "local-ig-test-token";
const imageUrl = "https://scontent-muc2-1.cdninstagram.com/v/example.jpg";
const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0xd9]);
const calls = [];

process.env.PORT = port;
process.env.DOTENV_CONFIG_PATH = "/dev/null";
process.env.META_IG_SYSTEM_USER_TOKEN = token;

axios.get = async (url, options) => {
  calls.push({ url, options });
  if (url === imageUrl) {
    assert.equal(options.headers.Authorization, undefined);
    return { status: 200, headers: { "content-type": "image/jpeg" }, data: jpeg };
  }
  assert.equal(url, "https://graph.facebook.com/v26.0/17841421933756277");
  assert.equal(options.headers.Authorization, `Bearer ${token}`);
  assert.equal(options.params.access_token, undefined);
  if (options.params.fields.includes("media.limit(1)")) {
    assert.match(options.params.fields, /children\{id,media_type,media_url\}/);
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
                children: { data: [{ id: "17936195607369773", media_type: "IMAGE", media_url: imageUrl }] }
              }
            ]
          }
        }
      }
    };
  }
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

function parseText(result) {
  return JSON.parse(result.content.find((item) => item.type === "text")?.text || "{}");
}

const client = new Client({ name: "vip-ig-business-discovery-selftest", version: "1.0.0" });
try {
  await import("../server.js");
  await new Promise((resolve) => setTimeout(resolve, 300));
  await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`)));

  const listed = await client.listTools();
  const igTools = listed.tools.filter((tool) => tool.name.startsWith("instagram_business_discovery_"));
  assert.equal(igTools.length, 4);
  assert.equal(igTools.every((tool) => tool.annotations?.readOnlyHint === true), true);

  const config = parseText(await client.callTool({ name: "instagram_business_discovery_check_config", arguments: {} }));
  assert.equal(config.token_configured, true);
  assert.equal(JSON.stringify(config).includes(token), false);

  const profile = parseText(
    await client.callTool({
      name: "instagram_business_discovery_profile",
      arguments: { agent_id: "vip-ai-monitoring", username: "daytrading" }
    })
  );
  assert.equal(profile.profile.followers_count, 2053196);
  assert.equal(JSON.stringify(profile).includes(token), false);

  const media = parseText(
    await client.callTool({
      name: "instagram_business_discovery_media",
      arguments: { agent_id: "vip-ai-monitoring", username: "daytrading", limit: 1 }
    })
  );
  assert.equal(media.media[0].children[0].media_url, imageUrl);
  assert.equal(media.media[0].children_complete, true);
  assert.equal(JSON.stringify(media).includes(token), false);

  const image = await client.callTool({
    name: "instagram_business_discovery_image",
    arguments: { agent_id: "vip-ai-monitoring", media_url: imageUrl }
  });
  assert.equal(image.isError, undefined);
  assert.equal(parseText(image).mime_type, "image/jpeg");
  assert.deepEqual(Buffer.from(image.content.find((item) => item.type === "image")?.data || "", "base64"), jpeg);

  const callsBeforeRejected = calls.length;
  const rejected = await client.callTool({
    name: "instagram_business_discovery_image",
    arguments: { agent_id: "vip-ai-monitoring", media_url: "https://evil.example/image.jpg" }
  });
  assert.equal(rejected.isError, true);
  assert.equal(calls.length, callsBeforeRejected);

  await client.close();
  console.log("instagram business discovery MCP selftest: ok");
  process.exit(0);
} catch (error) {
  console.error(error);
  process.exit(1);
}
