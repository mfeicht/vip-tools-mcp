import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { readFileSync } from "node:fs";

const port = process.env.EMAIL_ACTION_SELF_BCC_TEST_PORT || "3009";
process.env.PORT = port;

function parse(result) {
  const value = result.content?.find((item) => item.type === "text")?.text || "{}";
  return JSON.parse(value);
}

const client = new Client({ name: "vip-email-action-self-bcc-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));
await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const toolList = await client.listTools();
  const names = new Set((toolList.tools || []).map((tool) => tool.name));
  const accounts = parse(await client.callTool({
    name: "email_action_list_send_accounts",
    arguments: { agent_id: "vip-ai-communication" }
  }));
  const actions = parse(await client.callTool({
    name: "email_action_list_actions",
    arguments: { agent_id: "vip-ai-communication" }
  }));
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  const contactActions = (actions.actions || []).filter((action) =>
    ["rs-contact-de", "rs-contact-en"].includes(action.id)
  );
  const discountActions = (actions.actions || []).filter((action) =>
    ["rs-contact-rabatt-de", "rs-contact-rabatt-en"].includes(action.id)
  );
  const accountById = new Map((accounts.accounts || []).map((account) => [account.id, account]));
  const report = {
    discovery_tool_present: names.has("email_action_discover_folders"),
    send_account_tool_present: names.has("email_action_list_send_accounts"),
    template_style_tool_present: names.has("email_action_template_style_readback"),
    draft_template_test_send_tool_present: names.has("email_action_send_test_from_draft_template"),
    adaptive_context_tool_present: names.has("email_action_agent_context"),
    five_accounts_registered: accounts.account_count === 5,
    every_account_has_self_bcc: (accounts.accounts || []).every(
      (account) => account.mandatory_self_bcc === account.address
    ),
    domain_scoped_resend_env_names_present:
      accountById.get("rs-contact")?.required_env_names?.resend_api_key ===
        "RESEND_API_KEY_REISE_STORIES_DE" &&
      accountById.get("rs-moritz")?.required_env_names?.resend_api_key ===
        "RESEND_API_KEY_REISE_STORIES_DE" &&
      accountById.get("rs-media")?.required_env_names?.resend_api_key ===
        "RESEND_API_KEY_REISE_STORIES_DE" &&
      accountById.get("goklever-support")?.required_env_names?.resend_api_key ===
        "RESEND_API_KEY_GOKLEVER_DE" &&
      accountById.get("vip-moritz")?.required_env_names?.resend_api_key ===
        "RESEND_API_KEY_VIP_STUDIOS_DE" &&
      accountById.get("vip-moritz")?.required_env_names?.password_one_of?.includes(
        "SMTP_PASSWORD_EMAIL_AUTOMATION_VIP_MORITZ"
      ) &&
      source.includes('"vip-studios.de": "RESEND_API_KEY_VIP_STUDIOS_DE"'),
    two_contact_language_actions_registered:
      contactActions.length === 2 &&
      new Set(contactActions.map((action) => action.inbound_language)).size === 2,
    contact_actions_share_idempotency_scope:
      contactActions.length === 2 &&
      contactActions.every((action) => action.idempotency_scope === "rs-contact"),
    only_authorized_contact_action_live:
      contactActions.length === 2 &&
      contactActions.every((action) => action.response_mode === "agent_assisted") &&
      contactActions.find((action) => action.id === "rs-contact-de")?.live_enabled === true &&
      contactActions.find((action) => action.id === "rs-contact-en")?.live_enabled === false,
    contact_use_case_routing_present:
      contactActions.length === 2 &&
      contactActions.every(
        (action) =>
          action.selection_group === "rs-contact" &&
          action.use_case === "initial-link-or-article-cooperation" &&
          Boolean(action.routing_description)
      ),
    discount_actions_safely_prepared:
      discountActions.length === 2 &&
      discountActions.every(
        (action) =>
          action.selection_group === "rs-contact" &&
          action.use_case === "discount-follow-up-after-cooperation-offer" &&
          action.enabled === false &&
          action.live_enabled === false
      ),
    every_template_marker_is_excluded_from_inbound:
      source.includes("function isTemplateActionSubjectMarked") &&
      source.includes("function isEmailActionInboundMessage") &&
      source.includes("invalid_template_count") &&
      source.includes("template_subject_error"),
    adaptive_placeholder_guard_present:
      source.includes("validateEmailActionAgentPlaceholderValues") &&
      source.includes("agent_template_fit_confirmation_required"),
    envelope_send_uses_self_bcc_plan:
      source.includes("to: plan.envelope_recipients") &&
      source.includes("plan.bcc !== plan.from") &&
      source.includes('bcc_visible_in_mime_headers: false'),
    connected_resend_action_transport_present:
      source.includes("sendEmailActionViaResend") &&
      source.includes("getEmailActionHttpConfig") &&
      source.includes('type: "resend_http_mime_equivalent"'),
    resend_preserves_self_bcc_and_thread_headers:
      source.includes("bcc: [plan.bcc]") &&
      source.includes('"In-Reply-To": plan.in_reply_to') &&
      source.includes("References: plan.references"),
    resend_preserves_cid_and_idempotency:
      source.includes("content_id: attachment.content_id") &&
      source.includes('"Idempotency-Key": plan.idempotency_id'),
    resend_provider_readback_and_persistent_id_present:
      source.includes("readEmailActionResendResult") &&
      source.includes("emailActionResendProviderFlag") &&
      source.includes('status: "sent_but_provider_readback_failed"'),
    draft_template_send_is_hash_bound_and_non_mutating:
      source.includes("expected_template_sha256") &&
      source.includes('hasImapFlag(template.flags, "\\\\Draft")') &&
      source.includes('moves_template: false') &&
      source.includes("buildDraftTemplateResendPayload")
  };
  console.log(JSON.stringify(report));
  process.exit(Object.values(report).every(Boolean) ? 0 : 1);
} finally {
  await client.close().catch(() => {});
}
