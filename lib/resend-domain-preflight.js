export const RESEND_API_KEY_ENV_BY_DOMAIN = Object.freeze({
  "reise-stories.de": "RESEND_API_KEY_REISE_STORIES_DE",
  "vip-studios.de": "RESEND_API_KEY_VIP_STUDIOS_DE",
  "goklever.de": "RESEND_API_KEY_GOKLEVER_DE"
});

export const RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN = Object.freeze({
  "reise-stories.de": "RESEND_DOMAIN_READ_API_KEY_REISE_STORIES_DE",
  "vip-studios.de": "RESEND_DOMAIN_READ_API_KEY_VIP_STUDIOS_DE",
  "goklever.de": "RESEND_DOMAIN_READ_API_KEY_GOKLEVER_DE"
});

export const RESEND_PREFLIGHT_POLICY_VERSION = "resend-send-only-preflight-v1";

export async function readResendDomainPreflight(domain, { env, get, timeoutMs }) {
  const normalizedDomain = String(domain || "").trim().toLowerCase();
  const sendApiKeyEnvName = RESEND_API_KEY_ENV_BY_DOMAIN[normalizedDomain] || "";
  const domainReadApiKeyEnvName = RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN[normalizedDomain] || "";
  const sendApiKey = sendApiKeyEnvName ? env[sendApiKeyEnvName] : "";
  const domainReadApiKey = domainReadApiKeyEnvName ? env[domainReadApiKeyEnvName] : "";
  const apiKeyEnvName = domainReadApiKey ? domainReadApiKeyEnvName : sendApiKeyEnvName;
  const apiKey = domainReadApiKey || sendApiKey;
  const credentialScope = domainReadApiKey ? "domain_read" : sendApiKey ? "send_key_fallback" : null;
  const base = {
    provider: "resend",
    policy_version: RESEND_PREFLIGHT_POLICY_VERSION,
    domain: normalizedDomain,
    api_key_env_name: apiKeyEnvName || null,
    api_key_configured: Boolean(apiKey),
    credential_scope: credentialScope,
    send_api_key_env_name: sendApiKeyEnvName || null,
    send_api_key_configured: Boolean(sendApiKey),
    domain_read_api_key_env_name: domainReadApiKeyEnvName || null,
    domain_read_api_key_configured: Boolean(domainReadApiKey),
    domain_read_required: false,
    provider_readback_attempted: false,
    provider_http_status: null,
    domain_registered: null,
    domain_status: null,
    domain_verification_confirmed: false,
    region: null,
    capabilities: null,
    // Eligibility is not verification, submission, delivery or action authorization.
    ready_for_live_send: false,
    readiness_basis: null,
    send_confirmation_required: true,
    warning: null,
    error: null
  };
  if (!sendApiKeyEnvName) return { ...base, error: "Versanddomain ist nicht erlaubt." };
  if (!sendApiKey) {
    return { ...base, error: "Domain-spezifischer Resend-Send-Key ist nicht konfiguriert." };
  }

  const unknownDomain = (readback, warning) => ({
    ...base,
    ...readback,
    ready_for_live_send: true,
    readiness_basis: "send_key_configured_provider_enforces_domain_on_send",
    warning
  });
  try {
    const response = await get("https://api.resend.com/domains", {
      timeout: timeoutMs,
      params: { limit: 100 },
      headers: { Authorization: `Bearer ${apiKey}` }
    });
    const readback = { provider_readback_attempted: true, provider_http_status: response.status };
    if (!Array.isArray(response?.data?.data)) {
      return unknownDomain(readback, "Optionale Domain-Diagnose liefert keine auswertbare Domainliste.");
    }
    const match = response.data.data.find(
      (item) => String(item?.name || "").trim().toLowerCase() === normalizedDomain
    );
    if (!match && response.data.has_more === true) {
      return unknownDomain(readback, "Optionale Domainliste ist unvollstaendig; Registrierung bleibt unbekannt.");
    }
    const status = String(match?.status || "").trim().toLowerCase() || null;
    const capabilities = match?.capabilities && typeof match.capabilities === "object"
      ? {
          sending: String(match.capabilities.sending || "").trim().toLowerCase() || null,
          receiving: String(match.capabilities.receiving || "").trim().toLowerCase() || null
        }
      : null;
    const ready = Boolean(match) && status === "verified" &&
      (!capabilities || capabilities.sending !== "disabled");
    return {
      ...base,
      ...readback,
      domain_registered: Boolean(match),
      domain_status: status,
      domain_verification_confirmed: status === "verified",
      region: match?.region || null,
      capabilities,
      ready_for_live_send: ready,
      readiness_basis: ready ? "provider_domain_verified" : "provider_domain_not_ready",
      error: ready ? null : "Provider-Domain-Diagnose bestaetigt keine versandbereite Domain."
    };
  } catch (caught) {
    const status = Number(caught?.response?.status || 0) || null;
    const readback = { provider_readback_attempted: true, provider_http_status: status };
    const providerMessage = caught?.response?.data?.message || caught?.response?.data?.error;
    let message = String(providerMessage || caught?.message || "Unbekannter Fehler");
    for (const secret of [sendApiKey, domainReadApiKey].filter(Boolean).sort((a, b) => b.length - a.length)) {
      message = message.replaceAll(secret, "[REDACTED]");
    }
    const diagnosticError = `Resend-Domain-Diagnose fehlgeschlagen${status ? ` (HTTP ${status})` : ""}: ${message.slice(0, 500)}`;
    const restrictedToSending = [401, 403].includes(status) &&
      /restricted to only send emails/i.test(message);
    // A known send-only restriction is not failed authentication or an unverified domain.
    if (!domainReadApiKey && [401, 403].includes(status) && !restrictedToSending) {
      return { ...base, ...readback, error: diagnosticError, readiness_basis: "send_key_authentication_failed" };
    }
    return unknownDomain(readback, restrictedToSending
      ? "Send-only-Key erlaubt keine Domain-Diagnose. Keine zusaetzlichen Leserechte erforderlich; Resend validiert die Domain beim Versand."
      : diagnosticError);
  }
}
