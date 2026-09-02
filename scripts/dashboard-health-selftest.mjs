import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import {
  buildGoogleAdsComparison,
  dashboardControlPollMessage,
  dashboardRefreshPollMessage,
  decodeAndVerifyDashboardTelemetry,
  verifyDashboardControlPoll,
  verifyDashboardRefreshPoll
} from "../lib/dashboard-health.js";

const now = Date.now();
const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const telemetry = {
  generatedAt: new Date(now).toISOString(),
  nonce: "dashboard-selftest-123",
  codex: { status: "healthy" },
  finance: { gatewayConnected: true },
  resources: {
    plan: { usedPercent: 48 },
    forecast: { status: "critical", projectedUsedPercent: 157 }
  }
};
const payload = Buffer.from(JSON.stringify(telemetry));
const signature = sign(null, payload, privateKey);
const decoded = decodeAndVerifyDashboardTelemetry({
  payloadBase64: payload.toString("base64"),
  signatureBase64: signature.toString("base64"),
  publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
  now
});
assert.equal(decoded.nonce, telemetry.nonce);
assert.equal(decoded.resources.plan.usedPercent, 48);
assert.equal(decoded.resources.forecast.projectedUsedPercent, 157);

const pollTimestamp = new Date(now).toISOString();
const pollNonce = "dashboard-poll-selftest-123";
const pollSignature = sign(
  null,
  dashboardRefreshPollMessage({ timestamp: pollTimestamp, nonce: pollNonce }),
  privateKey
);
const verifiedPoll = verifyDashboardRefreshPoll({
  timestamp: pollTimestamp,
  nonce: pollNonce,
  signatureBase64: pollSignature.toString("base64"),
  publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
  now
});
assert.equal(verifiedPoll.nonce, pollNonce);
assert.throws(() =>
  verifyDashboardRefreshPoll({
    timestamp: pollTimestamp,
    nonce: `${pollNonce}x`,
    signatureBase64: pollSignature.toString("base64"),
    publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
    now
  })
);

const controlNonce = "dashboard-control-selftest-123";
const controlSignature = sign(
  null,
  dashboardControlPollMessage({ timestamp: pollTimestamp, nonce: controlNonce }),
  privateKey
);
const verifiedControlPoll = verifyDashboardControlPoll({
  timestamp: pollTimestamp,
  nonce: controlNonce,
  signatureBase64: controlSignature.toString("base64"),
  publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
  now
});
assert.equal(verifiedControlPoll.nonce, controlNonce);
assert.throws(() =>
  verifyDashboardControlPoll({
    timestamp: pollTimestamp,
    nonce: `${controlNonce}x`,
    signatureBase64: controlSignature.toString("base64"),
    publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
    now
  })
);

assert.throws(() =>
  decodeAndVerifyDashboardTelemetry({
    payloadBase64: Buffer.from(`${payload.toString("utf8")}x`).toString("base64"),
    signatureBase64: signature.toString("base64"),
    publicKeyPem: publicKey.export({ type: "spki", format: "pem" }),
    now
  })
);

const rows = Array.from({ length: 60 }, (_, index) => ({
  segments: {
    date: new Date(Date.UTC(2026, 4, 25 + index)).toISOString().slice(0, 10)
  },
  metrics: {
    impressions: "100",
    clicks: "10",
    costMicros: "20000000",
    conversions: "2",
    conversionsValue: "50"
  }
}));
const comparison = buildGoogleAdsComparison(rows, "2026-07-23");
assert.equal(comparison.currentWeek.clicks, 70);
assert.equal(comparison.currentWeek.spend, 140);
assert.equal(comparison.currentWeek.cpl, 10);
assert.equal(comparison.currentMonth.conversions, 60);
assert.equal(comparison.weekDeltas.cpc, 0);

console.log("dashboard-health-selftest: ok");
