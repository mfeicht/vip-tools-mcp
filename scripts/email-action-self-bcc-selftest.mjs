import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { readFileSync } from "node:fs";
import axios from "axios";
import {
  RESEND_API_KEY_ENV_BY_DOMAIN,
  RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN
} from "../lib/resend-domain-preflight.js";

const port = process.env.EMAIL_ACTION_SELF_BCC_TEST_PORT || "3009";
process.env.PORT = port;

function parse(result) {
  const value = result.content?.find((item) => item.type === "text")?.text || "{}";
  return JSON.parse(value);
}

const client = new Client({ name: "vip-email-action-self-bcc-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));
const serverModule = await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const toolList = await client.listTools();
  const names = new Set((toolList.tools || []).map((tool) => tool.name));
  const sendAccountTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_list_send_accounts"
  );
  const resendDomainStatusTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_resend_domain_status"
  );
  const processFolderTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_process_folder"
  );
  const shadowRunTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_shadow_run"
  );
  const templateReadbackTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_template_readback"
  );
  const templateStyleReadbackTool = (toolList.tools || []).find(
    (tool) => tool.name === "email_action_template_style_readback"
  );
  const accounts = parse(await client.callTool({
    name: "email_action_list_send_accounts",
    arguments: { agent_id: "vip-ai-communication" }
  }));
  const actions = parse(await client.callTool({
    name: "email_action_list_actions",
    arguments: { agent_id: "vip-ai-communication" }
  }));
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
  const preflightSource = readFileSync(new URL("../lib/resend-domain-preflight.js", import.meta.url), "utf8");
  const domainPreflights = [];
  const originalGet = axios.get;
  const testEnvNames = [
    ...Object.values(RESEND_API_KEY_ENV_BY_DOMAIN),
    ...Object.values(RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN)
  ];
  const originalEnv = new Map(testEnvNames.map((name) => [name, process.env[name]]));
  try {
    for (const name of Object.values(RESEND_API_KEY_ENV_BY_DOMAIN)) process.env[name] = "fake-send-only-test-key";
    for (const name of Object.values(RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN)) delete process.env[name];
    axios.get = async (url) => {
      if (url !== "https://api.resend.com/domains") throw new Error("Unexpected external read in test");
      throw { response: { status: 401, data: { message: "This API key is restricted to only send emails" } } };
    };
    for (const domain of Object.keys(RESEND_API_KEY_ENV_BY_DOMAIN)) {
      domainPreflights.push(parse(await client.callTool({
        name: "email_action_resend_domain_status",
        arguments: { agent_id: "vip-ai-communication", domain }
      })));
    }
  } finally {
    axios.get = originalGet;
    for (const [name, value] of originalEnv) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
  const contactActions = (actions.actions || []).filter((action) =>
    ["rs-contact-de", "rs-contact-en"].includes(action.id)
  );
  const discountActions = (actions.actions || []).filter((action) =>
    ["rs-contact-rabatt-de", "rs-contact-rabatt-en"].includes(action.id)
  );
  const accountById = new Map((accounts.accounts || []).map((account) => [account.id, account]));
  const adaptiveListHtml = serverModule.renderEmailActionReplyBodyHtml(
    "Hallo\n\n- Preis: 365 EUR\n- Dauer: mindestens 1 Jahr\n- A & B\n\nJetzt buchen."
  );
  const signatureRaw = [
    "MIME-Version: 1.0", 'Content-Type: multipart/related; boundary="sig-related"', "",
    "--sig-related", 'Content-Type: multipart/alternative; boundary="sig-alt"', "",
    "--sig-alt", "Content-Type: text/plain; charset=utf-8", "", "TEXT\nSIGNATURE-FIXTURE",
    "--sig-alt", "Content-Type: text/html; charset=utf-8", "",
    '<html><body><div>TEXT</div><div>SIGNATURE-FIXTURE<img src="cid:signature-fixture"></div></body></html>',
    "--sig-alt--", "--sig-related", "Content-Type: image/png", "Content-ID: <signature-fixture>",
    'Content-Disposition: inline; filename="fixture.png"', "Content-Transfer-Encoding: base64", "", "aWNvbg==",
    "--sig-related--"
  ].join("\r\n");
  const proposalSignature = {
    binding: { body_marker: "TEXT", trailing_identity_lines: [] },
    template: { uid: "23", raw: signatureRaw, raw_sha256: "fixture-signature-hash", parsed: {} }
  };
  const reviewPlans = ["de", "en"].flatMap((language) => ["plain", "html"].map((bodyType) => {
    const raw = [
      "From: Original <original@example.com>", "Reply-To: reply@example.com",
      "Subject: Existing request", "Message-ID: <original@example.com>",
      "References: <parent@example.com>", "Date: Thu, 17 Sep 2026 11:00:00 +0200", "MIME-Version: 1.0",
      `Content-Type: text/${bodyType}; charset=utf-8`, "",
      bodyType === "html"
        ? '<p>ORIGINAL-FIXTURE &amp; context cid:untrusted</p><img src="https://tracker.example.com/image"><script>bad()</script>'
        : "ORIGINAL-FIXTURE & context cid:untrusted"
    ].join("\r\n");
    return serverModule.buildEmailActionReviewProposalPlan({
      action: { id: `fixture-${language}`, idempotency_scope: "fixture", mailbox: "INBOX.Fixture", inbound_language: language, include_quoted_original: true },
      sourceMessage: { uid: "42", raw, raw_sha256: "fixture-source-hash", parsed: {} },
      sendAccount: { from: "contact@reise-stories.de" }, signatureTemplate: proposalSignature,
      proposalBody: "REPLY-FIXTURE\n\nBest regards"
    });
  }));
  let missingReviewSignatureBlocked = false;
  try {
    serverModule.buildEmailActionReviewProposalPlan({
      action: { id: "fixture", mailbox: "INBOX.Fixture", include_quoted_original: true },
      sourceMessage: { uid: "42", raw: "From: original@example.com\r\nMessage-ID: <fixture@example.com>\r\n\r\nOriginal", parsed: {} },
      sendAccount: { from: "contact@reise-stories.de" }, proposalBody: "Reply"
    });
  } catch (error) {
    missingReviewSignatureBlocked = error.message.includes("registrierte Signaturkomposition");
  }
  const report = {
    review_original_history_follows_signature_in_html_and_text: reviewPlans.every((plan) =>
      plan.quoted_original?.body_chars > 0 &&
      plan.proposal_html.indexOf("REPLY-FIXTURE") < plan.proposal_html.indexOf("SIGNATURE-FIXTURE") &&
      plan.proposal_html.indexOf("SIGNATURE-FIXTURE") < plan.proposal_html.indexOf("ORIGINAL-FIXTURE") &&
      plan.proposal_body.indexOf("SIGNATURE-FIXTURE") < plan.proposal_body.indexOf("ORIGINAL-FIXTURE")),
    review_original_history_preserves_internal_envelope_thread_and_signature: reviewPlans.every((plan) =>
      plan.to === plan.from && plan.envelope_recipients.length === 1 && plan.reply_to === "reply@example.com" &&
      plan.in_reply_to === "<original@example.com>" && plan.references.includes("<parent@example.com>") &&
      plan.signature_template.sha256 === "fixture-signature-hash" && plan.signature_template.inline_resource_count === 1 &&
      plan.proposal_attachments.length === 1 && plan.proposal_html.includes('src="cid:signature-fixture"')),
    review_original_history_does_not_embed_untrusted_resources: reviewPlans.every((plan) =>
      !plan.proposal_html.includes('<img src="https://tracker') && !plan.proposal_html.includes("<script>") &&
      !plan.proposal_html.includes("cid:untrusted")),
    review_original_history_requires_registered_signature: missingReviewSignatureBlocked,
    discovery_tool_present: names.has("email_action_discover_folders"),
    send_account_tool_present: names.has("email_action_list_send_accounts"),
    send_account_tool_allows_operations_readback:
      sendAccountTool?.inputSchema?.properties?.agent_id?.enum?.includes(
        "vip-ai-operations"
      ) === true,
    resend_domain_status_tool_present: Boolean(resendDomainStatusTool),
    resend_domain_status_allows_operations_readback:
      resendDomainStatusTool?.inputSchema?.properties?.agent_id?.enum?.includes(
        "vip-ai-operations"
      ) === true,
    all_three_send_only_domains_pass_mcp_preflight_without_false_verification:
      domainPreflights.length === 3 && domainPreflights.every((result) =>
        result.policy_version === "resend-send-only-preflight-v1" &&
        result.ready_for_live_send === true && result.domain_read_required === false &&
        result.domain_registered === null && result.domain_verification_confirmed === false &&
        result.send_confirmation_required === true && result.error === null && Boolean(result.warning)),
    template_style_tool_present: names.has("email_action_template_style_readback"),
    signature_readback_tool_present: names.has("email_action_signature_readback"),
    operations_readback_is_rs_contact_scoped:
      templateReadbackTool?.inputSchema?.properties?.agent_id?.enum?.includes("vip-ai-operations") === true &&
      templateStyleReadbackTool?.inputSchema?.properties?.agent_id?.enum?.includes("vip-ai-operations") === true &&
      shadowRunTool?.inputSchema?.properties?.agent_id?.enum?.includes("vip-ai-operations") === true &&
      source.includes("EMAIL_ACTION_OPERATIONS_READ_ACTION_IDS") &&
      source.includes("delegated_read_only"),
    draft_template_test_send_tool_present: names.has("email_action_send_test_from_draft_template"),
    adaptive_context_tool_present: names.has("email_action_agent_context"),
    internal_review_proposal_tool_present: names.has("email_action_send_review_proposal"),
    answered_thread_ancestor_cleanup_tool_present:
      names.has("email_action_cleanup_answered_thread_ancestor"),
    five_accounts_registered: accounts.account_count === 5,
    every_account_has_self_bcc: (accounts.accounts || []).every(
      (account) => account.mandatory_self_bcc === account.address
    ),
    rs_contact_signature_is_hash_bound:
      accountById.get("rs-contact")?.signature_template?.action_id === "rs-signatur-de-en" &&
      accountById.get("rs-contact")?.signature_template?.uid === "23" &&
      accountById.get("rs-contact")?.signature_template?.sha256 ===
        "ab4410e3545a27507607ceb47d0049a8e53c684f7a4475a7390ccd17c9003500" &&
      accountById.get("rs-contact")?.signature_template?.body_marker === "TEXT",
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
      preflightSource.includes('"vip-studios.de": "RESEND_API_KEY_VIP_STUDIOS_DE"'),
    two_contact_language_actions_registered:
      contactActions.length === 2 &&
      new Set(contactActions.map((action) => action.inbound_language)).size === 2,
    contact_actions_share_idempotency_scope:
      contactActions.length === 2 &&
      contactActions.every((action) => action.idempotency_scope === "rs-contact"),
    approved_contact_language_actions_live:
      contactActions.length === 2 &&
      contactActions.every((action) => action.response_mode === "agent_assisted") &&
      contactActions.every((action) => action.live_enabled === true),
    adaptive_contact_replies_are_explicitly_enabled:
      contactActions.length === 2 &&
      contactActions.every((action) => action.adaptive_external_enabled === true) &&
      contactActions.every((action) => action.adaptive_request_types.includes("press_release")) &&
      contactActions.every((action) => action.adaptive_request_types.includes("discount_negotiation")) &&
      contactActions.every((action) => action.adaptive_request_types.includes("account_or_platform_setup")) &&
      Boolean(processFolderTool?.inputSchema?.properties?.adaptive_replies_by_uid),
    contact_use_case_routing_present:
      contactActions.length === 2 &&
      contactActions.every(
        (action) =>
          action.selection_group === "rs-contact" &&
          action.use_case === "link-or-article-cooperation-including-discount-follow-up" &&
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
    thread_ancestor_cleanup_is_reference_bound_and_no_send:
      source.includes("function findEmailActionThreadAncestors") &&
      source.includes("move_thread_ancestors_to_done") &&
      source.includes("thread_ancestor_moves: threadAncestorMoves") &&
      source.includes("$VIPAI-THREAD-HANDLED") &&
      source.includes("move_failed_but_marked_handled") &&
      source.includes("sent: false"),
    answered_thread_ancestor_cleanup_is_header_only_and_idempotent:
      source.includes('"email_action_cleanup_answered_thread_ancestor"') &&
      source.includes("provider_marker_validated: true") &&
      source.includes("thread_reference_validated: true") &&
      source.includes("full_body_fetched: false") &&
      source.includes('status: "already_moved_and_verified"') &&
      source.includes("expectedAncestorMessageIdHash"),
    successful_actions_move_to_imap_trash:
      contactActions.every((action) => action.done_mailbox === "INBOX.Trash"),
    internal_review_proposal_is_self_only_and_threaded:
      source.includes("function buildEmailActionReviewProposalPlan") &&
      source.includes('`ENTWURF: AN ${externalRecipient.email} | ${buildReplySubject(originalSubject)}`') &&
      source.includes('`Reply-To: <${externalRecipient.email}>`') &&
      source.includes("reply_to: plan.reply_to") &&
      source.includes("Ziel-Empfaenger:") &&
      source.includes("external_recipient_visible_in_subject") &&
      source.includes("external_recipient_visible_in_body") &&
      source.includes("reply_to_matches_external_recipient") &&
      source.includes("plan.reply_to !== plan.external_recipient") &&
      source.includes("!plan.subject.includes(plan.external_recipient)") &&
      source.includes("!plan.proposal_body.includes(plan.external_recipient)") &&
      source.includes("external_recipient_contacted: false") &&
      source.includes("plan.envelope_recipients.includes(plan.external_recipient)") &&
      source.includes("sendEmailActionReviewProposalViaResend") &&
      source.includes("proposal_sent_and_source_trashed"),
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
    resend_domain_status_is_read_only_and_secret_safe:
      source.includes("async function readResendDomainStatus") &&
      source.includes("readResendDomainPreflight(domain") &&
      preflightSource.includes('get("https://api.resend.com/domains"') &&
      preflightSource.includes("api_key_env_name: apiKeyEnvName || null") &&
      preflightSource.includes("ready_for_live_send") &&
      preflightSource.includes('message.replaceAll(secret, "[REDACTED]")'),
    resend_domain_read_credential_is_optional_and_separate_from_send_key:
      accountById.get("rs-contact")?.optional_env_names?.resend_domain_read_api_key ===
        "RESEND_DOMAIN_READ_API_KEY_REISE_STORIES_DE" &&
      accountById.get("vip-moritz")?.optional_env_names?.resend_domain_read_api_key ===
        "RESEND_DOMAIN_READ_API_KEY_VIP_STUDIOS_DE" &&
      accountById.get("goklever-support")?.optional_env_names?.resend_domain_read_api_key ===
        "RESEND_DOMAIN_READ_API_KEY_GOKLEVER_DE" &&
      (accounts.accounts || []).every((account) =>
        account.resend_domain_read_required === false &&
        account.resend_preflight_policy_version === "resend-send-only-preflight-v1" &&
        !Object.hasOwn(account.required_env_names, "resend_domain_read_api_key")),
    registered_templates_are_fetched_by_uid:
      Boolean(templateReadbackTool) &&
      source.includes("registeredEmailActionTemplateUidsForMailbox") &&
      source.includes("requiredUids: registeredEmailActionTemplateUidsForMailbox(action.mailbox)") &&
      source.includes("includeQueuePage: !action.template.uid"),
    action_queue_is_cursor_paginated_oldest_first:
      processFolderTool?.inputSchema?.properties?.scan_order?.default === "oldest_first" &&
      Boolean(processFolderTool?.inputSchema?.properties?.scan_cursor_uid) &&
      shadowRunTool?.inputSchema?.properties?.scan_order?.default === "oldest_first" &&
      Boolean(shadowRunTool?.inputSchema?.properties?.scan_cursor_uid) &&
      source.includes("selectImapUidPage") &&
      source.includes("queue_page: scan.queue_page"),
    oversized_messages_are_preflighted_before_full_body_fetch:
      source.includes("RFC822.SIZE BODY.PEEK[HEADER.FIELDS") &&
      source.includes('parseError: "message_too_large"') &&
      source.indexOf("declaredBytes > maxEmailBytes") <
        source.indexOf("UID FETCH ${normalizedUid} (UID FLAGS BODY.PEEK[])") &&
      source.includes("oversized_skipped_count"),
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
    send_only_resend_recovery_is_compact_and_idempotent:
      source.includes('return `$VR-${Buffer.from(uuidHex, "hex").toString("base64url")}`') &&
      source.includes("isEmailActionTransportSendConfirmed") &&
      source.includes("submission_verified: true") &&
      source.includes("confirmed_resend_provider_ids_by_uid") &&
      source.includes("explicit_recovery_after_accepted_response"),
    draft_template_send_is_hash_bound_and_non_mutating:
      source.includes("expected_template_sha256") &&
      source.includes('hasImapFlag(template.flags, "\\\\Draft")') &&
      source.includes('moves_template: false') &&
      source.includes("buildDraftTemplateResendPayload"),
    routine_signature_composition_is_readback_gated:
      source.includes("resolveEmailActionSignatureTemplate") &&
      source.includes("fehlgeschlagen: ${failedChecks.join") &&
      source.includes("composeEmailActionContentWithSignature") &&
      source.includes("signatureTemplate.binding.trailing_identity_lines") &&
      source.includes("signature_template: plan.signature_template"),
    adaptive_reply_is_focused_sourced_and_idempotent:
      source.includes("validateEmailActionAdaptiveReply") &&
      source.includes("buildEmailActionAdaptiveReplyPlan") &&
      source.includes("requested_product_only") &&
      source.includes("single_best_fit_offer") &&
      source.includes("dynamic_sources_checked") &&
      source.includes("dynamic_sources_checked_at") &&
      source.includes("buildEmailActionIdempotencyId") &&
      source.includes("adaptive_reply: plan.adaptive_reply"),
    adaptive_reply_html_uses_semantic_lists:
      adaptiveListHtml.includes("<ul>") &&
      adaptiveListHtml.includes("<li>Preis: 365 EUR</li>") &&
      adaptiveListHtml.includes("<li>Dauer: mindestens 1 Jahr</li>") &&
      adaptiveListHtml.includes("<li>A &amp; B</li>") &&
      !adaptiveListHtml.includes("- Preis:"),
    adaptive_reply_sections_keep_a_visible_blank_line:
      adaptiveListHtml.includes("Hallo<br><br>\n<ul>") &&
      adaptiveListHtml.includes("</ul><br><br>\nJetzt buchen.") &&
      serverModule.renderEmailActionReplyBodyHtml("Anrede\n\nEinleitung\n\nBuchung\n\nAbschluss") ===
        "<div>Anrede<br><br>\nEinleitung<br><br>\nBuchung<br><br>\nAbschluss</div>",
    discount_floor_is_staged_and_source_gated:
      source.includes('requestType === "discount_negotiation"') &&
      source.includes("Rabatt-Endpreis unter 100 EUR ist gesperrt") &&
      source.includes("previousOfferAmountsEur.length < 2") &&
      source.includes("Rabattcode-Tabelle") &&
      source.includes("final_floor"),
    counterparty_account_setup_is_required:
      source.includes('requestType === "account_or_platform_setup"') &&
      source.includes("counterparty_setup_required") &&
      source.includes("fremder Account muss vom anfragenden Partner eingerichtet werden"),
    guest_article_second_link_is_negotiated_above_floor:
      source.includes("includedLinkCount === 2") &&
      source.includes('requestType !== "guest_article"') &&
      source.includes("Zwei Links brauchen einen finalen Kooperationspreis ueber 100 EUR") &&
      source.includes("negotiationRoundsCompleted < 1"),
    prohibited_link_industries_are_blocked:
      contactActions.every((action) => action.agent_allowed_adjustments.includes("industry_safety_classification")) &&
      source.includes('industryRisk === "prohibited"') &&
      source.includes("Casino-, Gluecksspiel-, Crypto-, Spam- oder sonstige unserioese Linkziele sind gesperrt") &&
      source.includes('industryRisk !== "safe"'),
    visible_original_history_is_appended_after_signature:
      contactActions.every((action) => action.include_quoted_original === true) &&
      source.includes("function appendEmailActionQuotedOriginal") &&
      source.includes('class="vip-original-message"') &&
      source.includes("quoted_original: quotedComposition.quoted_original") &&
      source.includes("html: quotedComposition.html") &&
      source.includes("text: quotedComposition.text"),
    quoted_original_cid_markers_are_not_treated_as_attachments:
      source.includes('replace(/\\bcid:/giu, "cid&#58;")'),
    german_umlaut_substitutions_are_blocked:
      source.includes("function assertGermanEmailOrthography") &&
      source.includes("deutsche Antwort enthaelt ae/oe/ue-Ersatzschreibweisen") &&
      source.includes('if (language === "de") assertGermanEmailOrthography(replyBody, action.id)')
  };
  await new Promise((resolve) => process.stdout.write(`${JSON.stringify(report)}\n`, resolve));
  process.exit(Object.values(report).every(Boolean) ? 0 : 1);
} finally {
  await client.close().catch(() => {});
}
