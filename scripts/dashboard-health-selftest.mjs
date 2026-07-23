import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import {
  buildGoogleAdsComparison,
  decodeAndVerifyDashboardTelemetry
} from "../lib/dashboard-health.js";

const now = Date.now();
const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const telemetry = {
  generatedAt: new Date(now).toISOString(),
  nonce: "dashboard-selftest-123",
  codex: { status: "healthy" },
  finance: { gatewayConnected: true }
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
