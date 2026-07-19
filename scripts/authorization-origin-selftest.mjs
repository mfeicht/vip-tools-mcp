import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = process.env.AUTHORIZATION_ORIGIN_TEST_PORT || "3008";
process.env.PORT = port;

const client = new Client({ name: "vip-authorization-origin-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));

async function preflight(authorization, extra = {}) {
  return client.callTool({
    name: "action_authorization_preflight",
    arguments: {
      agent_id: "vip-ai-marketing",
      action_name: "authorization-origin-selftest",
      require_moritz: true,
      authorization,
      ...extra
    }
  });
}

function text(result) {
  return (result.content || []).map((item) => item.text || "").join("\n");
}

await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const toolList = await client.listTools();
  const toolsByName = new Map((toolList.tools || []).map((tool) => [tool.name, tool]));
  const authorizationTools = [
    "asana_create_task",
    "asana_update_task_description",
    "asana_update_task_tags",
    "asana_update_task_recurrence",
    "asana_normalize_open_task_section",
    "asana_attachment_copy_to_drive",
    "accounting_mail_scan_upload_attachment",
    "accounting_email_archive_processed",
    "buffer_schedule_post",
    "templated_render",
    "freepik_download_resource",
    "unsplash_track_download",
    "google_ads_mutate",
    "dataforseo_google_keyword_overview",
    "dataforseo_google_keyword_ideas",
    "dataforseo_google_search_volume",
    "dataforseo_google_keyword_research",
    "google_drive_copy_file_to_agent_folder",
    "google_sheets_update_range",
    "rs_redaktionsplan_update_row",
    "google_sheets_delete_duplicates",
    "agent_email_send_prepared",
    "agent_email_send",
    "wp_import_csv"
  ];
  const missingAuthorizationSchema = authorizationTools.filter(
    (name) => !toolsByName.get(name)?.inputSchema?.properties?.authorization
  );

  const validDirect = await preflight({
    source: "direct_codex",
    direct_authorized_by: "Moritz Feichtmeyer",
    direct_instruction: "Passe die vorhandenen Google Ads jetzt wie in diesem aktuellen Codex-Chat beschrieben an."
  });
  const foreignDirect = await preflight({
    source: "direct_codex",
    direct_authorized_by: "Andere Person",
    direct_instruction: "Passe die vorhandenen Google Ads jetzt wie beschrieben an."
  });
  const embeddedNameSpoof = await preflight({
    source: "direct_codex",
    direct_authorized_by: "Nicht Moritz Feichtmeyer",
    direct_instruction: "Passe die vorhandenen Google Ads jetzt wie beschrieben an."
  });
  const mixedOrigin = await preflight(
    {
      source: "direct_codex",
      direct_authorized_by: "Moritz Feichtmeyer",
      direct_instruction: "Passe die vorhandenen Google Ads jetzt wie beschrieben an."
    },
    { confirmed_by_asana: true, asana_task_gid: "1234567890" }
  );
  const vagueDirect = await preflight({
    source: "direct_codex",
    direct_authorized_by: "Moritz Feichtmeyer",
    direct_instruction: "Mach das."
  });
  const missingAsanaBasis = await preflight({ source: "asana" });

  const validText = text(validDirect);
  const report = {
    authorization_tool_schema_coverage:
      missingAuthorizationSchema.length === 0 &&
      Boolean(toolsByName.get("action_authorization_preflight")) &&
      Boolean(toolsByName.get("google_ads_mutate")?.inputSchema?.properties?.extra_authorization),
    valid_direct_moritz_passed:
      !validDirect.isError &&
      /"authorization_status"\s*:\s*"verified"/.test(validText) &&
      /"source"\s*:\s*"direct_codex"/.test(validText),
    foreign_direct_blocked: Boolean(foreignDirect.isError),
    embedded_name_spoof_blocked: Boolean(embeddedNameSpoof.isError),
    mixed_origin_blocked: Boolean(mixedOrigin.isError),
    vague_direct_blocked: Boolean(vagueDirect.isError),
    asana_without_basis_blocked: Boolean(missingAsanaBasis.isError)
  };

  console.log(JSON.stringify({ ...report, missing_authorization_schema: missingAuthorizationSchema }));
  process.exit(Object.values(report).every(Boolean) ? 0 : 1);
} finally {
  await client.close().catch(() => {});
}
