import assert from "node:assert/strict";
import {
  RESEND_API_KEY_ENV_BY_DOMAIN,
  RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN,
  RESEND_PREFLIGHT_POLICY_VERSION,
  readResendDomainPreflight
} from "../lib/resend-domain-preflight.js";

let cases = 0;
for (const [domain, sendEnv] of Object.entries(RESEND_API_KEY_ENV_BY_DOMAIN)) {
  const readEnv = RESEND_DOMAIN_READ_API_KEY_ENV_BY_DOMAIN[domain];
  const sendKey = `fake-send-key-${domain}`;
  const readKey = `fake-diagnostic-key-${domain}`;
  async function run({ env = { [sendEnv]: sendKey }, data, failure, expectedKey = sendKey }) {
    let calls = 0;
    let request;
    const result = await readResendDomainPreflight(domain, {
      env,
      timeoutMs: 1200,
      get: async (url, config) => {
        calls += 1;
        request = { url, config };
        if (failure) throw failure;
        return { status: 200, data };
      }
    });
    if (calls) {
      assert.equal(calls, 1);
      assert.equal(request.url, "https://api.resend.com/domains");
      assert.equal(request.config.headers.Authorization, `Bearer ${expectedKey}`);
      assert.equal(request.config.timeout, 1200);
      assert.deepEqual(request.config.params, { limit: 100 });
    }
    assert.equal(result.policy_version, RESEND_PREFLIGHT_POLICY_VERSION);
    assert.equal(result.domain_read_required, false);
    assert.equal(result.send_confirmation_required, true);
    assert.equal(JSON.stringify(result).includes(sendKey), false);
    assert.equal(JSON.stringify(result).includes(readKey), false);
    cases += 1;
    return { result, calls };
  }
  function httpError(status, message) {
    return { response: { status, data: { message } } };
  }
  for (const status of [401, 403]) {
    const { result } = await run({ failure: httpError(status, "This API key is restricted to only send emails") });
    assert.equal(result.ready_for_live_send, true);
    assert.equal(result.domain_registered, null);
    assert.equal(result.domain_verification_confirmed, false);
    assert.equal(result.error, null);
    assert.ok(result.warning);
    assert.equal(result.readiness_basis, "send_key_configured_provider_enforces_domain_on_send");
  }
  for (const status of [401, 403]) {
    const { result } = await run({ failure: httpError(status, `Invalid API key: ${sendKey}`) });
    assert.equal(result.ready_for_live_send, false);
    assert.equal(result.domain_registered, null);
    assert.equal(result.readiness_basis, "send_key_authentication_failed");
    assert.ok(result.error);
    assert.ok(result.error.includes("[REDACTED]"));
  }
  for (const env of [{}, { [readEnv]: readKey }]) {
    const { result, calls } = await run({ env });
    assert.equal(result.ready_for_live_send, false);
    assert.equal(result.send_api_key_configured, false);
    assert.equal(calls, 0);
    assert.ok(result.error);
  }
  const verified = { name: domain, status: "verified", region: "eu-west-1", capabilities: { sending: "enabled", receiving: "disabled" } };
  for (const env of [{ [sendEnv]: sendKey }, { [sendEnv]: sendKey, [readEnv]: readKey }]) {
    const { result } = await run({ env, data: { data: [verified], has_more: false }, expectedKey: env[readEnv] || sendKey });
    assert.equal(result.ready_for_live_send, true);
    assert.equal(result.domain_registered, true);
    assert.equal(result.domain_verification_confirmed, true);
    assert.equal(result.readiness_basis, "provider_domain_verified");
    assert.equal(result.warning, null);
  }
  for (const record of [
    { ...verified, status: "pending" },
    { ...verified, status: "failed" },
    { ...verified, status: "not_started" },
    { ...verified, status: "temporary_failure" },
    { ...verified, capabilities: { sending: "disabled" } }
  ]) {
    const { result } = await run({ data: { data: [record], has_more: false } });
    assert.equal(result.ready_for_live_send, false);
    assert.equal(result.readiness_basis, "provider_domain_not_ready");
    assert.ok(result.error);
  }
  const missing = await run({ data: { data: [], has_more: false } });
  assert.equal(missing.result.ready_for_live_send, false);
  assert.equal(missing.result.domain_registered, false);
  for (const data of [{ data: [], has_more: true }, { unexpected: "invalid-response" }]) {
    const { result } = await run({ data });
    assert.equal(result.ready_for_live_send, true);
    assert.equal(result.domain_registered, null);
    assert.equal(result.domain_verification_confirmed, false);
    assert.ok(result.warning);
  }
  for (const failure of [httpError(500, "Provider unavailable"), new Error("Network timeout")]) {
    const { result } = await run({ failure });
    assert.equal(result.ready_for_live_send, true);
    assert.equal(result.domain_registered, null);
    assert.ok(result.warning);
  }
  for (const failure of [httpError(401, `Invalid diagnostic key: ${readKey}`), httpError(403, "Forbidden"), new Error("Network timeout")]) {
    const { result } = await run({ env: { [sendEnv]: sendKey, [readEnv]: readKey }, failure, expectedKey: readKey });
    assert.equal(result.ready_for_live_send, true);
    assert.equal(result.domain_registered, null);
    assert.ok(result.warning);
    assert.equal(result.error, null);
  }
}
const unsupported = await readResendDomainPreflight("untrusted.example", {
  env: { RESEND_API_KEY: "fake-global-key" },
  get: () => assert.fail("Unsupported domain must not use credentials or HTTP"),
  timeoutMs: 1200
});
assert.equal(unsupported.ready_for_live_send, false);
assert.equal(unsupported.api_key_configured, false);
assert.ok(unsupported.error);
cases += 1;
console.log(JSON.stringify({ passed: true, cases, domains: Object.keys(RESEND_API_KEY_ENV_BY_DOMAIN), sends: 0, moves: 0 }, null, 2));
