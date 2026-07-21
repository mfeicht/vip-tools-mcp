import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = process.env.WEB_RESEARCH_SHADOW_PORT || "3010";
const url = process.env.WEB_RESEARCH_SHADOW_URL || "https://example.com/";
const renderModes = String(process.env.WEB_RESEARCH_SHADOW_RENDER_MODES || "http,browser")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
process.env.PORT = port;

const client = new Client({ name: "vip-web-research-shadow", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));

function parseToolResult(result) {
  const text = (result.content || []).map((item) => item.text || "").join("\n");
  if (result.isError) throw new Error(text || "MCP tool returned an error.");
  return JSON.parse(text);
}

await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const toolList = await client.listTools();
  const requiredTools = [
    "web_research_check_config",
    "web_research_extract",
    "web_research_crawl",
    "web_research_listings",
    "web_research_monitor"
  ];
  const availableTools = new Set(toolList.tools.map((tool) => tool.name));
  const missingTools = requiredTools.filter((tool) => !availableTools.has(tool));
  if (missingTools.length) throw new Error(`MCP-Tools fehlen: ${missingTools.join(", ")}`);

  const config = parseToolResult(
    await client.callTool({ name: "web_research_check_config", arguments: {} })
  );
  if (!config.safety?.public_http_https_only || !config.safety?.non_get_browser_requests_blocked) {
    throw new Error("Web-Research-Sicherheitskonfiguration ist nicht vollstaendig aktiv.");
  }

  const results = [];
  for (const renderMode of renderModes) {
    const result = parseToolResult(
      await client.callTool({
        name: "web_research_extract",
        arguments: {
          agent_id: "vip-ai-operations",
          purpose: "research",
          url,
          render_mode: renderMode,
          include: ["seo", "headings", "text"],
          fields: [
            { name: "primary_heading", selector: "h1", attribute: "text", required: true }
          ],
          text_max_chars: 1_000,
          timeout_ms: 30_000
        }
      })
    );
    const extraction = result.extraction;
    if (!extraction.ok) throw new Error(`${renderMode}: HTTP-Status ${extraction.status}`);
    if (extraction.missing_required_fields?.length) {
      throw new Error(`${renderMode}: Pflichtfelder fehlen: ${extraction.missing_required_fields.join(", ")}`);
    }
    results.push({
      render_mode: renderMode,
      final_url: extraction.final_url,
      status: extraction.status,
      title: extraction.seo?.title || "",
      primary_heading: extraction.fields?.primary_heading || "",
      html_sha256: extraction.evidence.html_sha256,
      normalized_text_sha256: extraction.evidence.normalized_text_sha256,
      robots_status: result.policy.robots?.robots_status || null
    });
  }

  console.log(
    JSON.stringify(
      {
        shadow_status: "SHADOW OK",
        mcp_tool_contract: "ok",
        required_tools_present: true,
        safety_config_readback: "ok",
        url,
        results
      },
      null,
      2
    )
  );
  process.exit(0);
} catch (error) {
  console.error(error);
  process.exit(1);
} finally {
  await client.close().catch(() => {});
}
