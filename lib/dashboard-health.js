import { createPublicKey, verify } from "node:crypto";

export const DASHBOARD_TELEMETRY_MAX_AGE_MS = 15 * 60 * 1000;

export function decodeAndVerifyDashboardTelemetry({
  payloadBase64,
  signatureBase64,
  publicKeyPem,
  now = Date.now()
}) {
  if (!payloadBase64 || !signatureBase64 || !publicKeyPem) {
    throw new Error("Telemetry payload, signature or public key is missing.");
  }

  const payloadBuffer = Buffer.from(payloadBase64, "base64");
  const signature = Buffer.from(signatureBase64, "base64");
  if (payloadBuffer.length === 0 || payloadBuffer.length > 256 * 1024) {
    throw new Error("Telemetry payload size is invalid.");
  }

  const verified = verify(
    null,
    payloadBuffer,
    createPublicKey(publicKeyPem),
    signature
  );
  if (!verified) throw new Error("Telemetry signature is invalid.");

  let payload;
  try {
    payload = JSON.parse(payloadBuffer.toString("utf8"));
  } catch {
    throw new Error("Telemetry payload is not valid JSON.");
  }

  const generatedAtMs = Date.parse(payload?.generatedAt || "");
  if (!Number.isFinite(generatedAtMs)) {
    throw new Error("Telemetry generatedAt is invalid.");
  }
  if (generatedAtMs > now + 60_000 || now - generatedAtMs > DASHBOARD_TELEMETRY_MAX_AGE_MS) {
    throw new Error("Telemetry payload is outside the accepted time window.");
  }
  if (!/^[a-zA-Z0-9_-]{12,100}$/.test(String(payload?.nonce || ""))) {
    throw new Error("Telemetry nonce is invalid.");
  }

  return payload;
}

export function dashboardTelemetryFresh(telemetry, now = Date.now()) {
  const generatedAtMs = Date.parse(telemetry?.generatedAt || "");
  return (
    Number.isFinite(generatedAtMs) &&
    generatedAtMs <= now + 60_000 &&
    now - generatedAtMs <= DASHBOARD_TELEMETRY_MAX_AGE_MS
  );
}

export function dateKeyShift(dateKey, days) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dateKey || ""));
  if (!match) throw new Error(`Invalid date key: ${dateKey}`);
  const value = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function metricNumber(value) {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
}

export function aggregateGoogleAdsRows(rows, startDate, endDate) {
  const aggregate = {
    impressions: 0,
    clicks: 0,
    spend: 0,
    conversions: 0,
    conversionValue: 0
  };

  for (const row of rows || []) {
    const date = row?.segments?.date;
    if (!date || date < startDate || date > endDate) continue;
    const metrics = row.metrics || {};
    aggregate.impressions += metricNumber(metrics.impressions);
    aggregate.clicks += metricNumber(metrics.clicks);
    aggregate.spend += metricNumber(metrics.costMicros) / 1_000_000;
    aggregate.conversions += metricNumber(metrics.conversions);
    aggregate.conversionValue += metricNumber(metrics.conversionsValue);
  }

  return {
    ...aggregate,
    ctr: aggregate.impressions > 0 ? aggregate.clicks / aggregate.impressions : null,
    cpc: aggregate.clicks > 0 ? aggregate.spend / aggregate.clicks : null,
    conversionRate: aggregate.clicks > 0 ? aggregate.conversions / aggregate.clicks : null,
    cpl: aggregate.conversions > 0 ? aggregate.spend / aggregate.conversions : null
  };
}

export function percentDelta(current, previous) {
  const currentNumber = Number(current);
  const previousNumber = Number(previous);
  if (!Number.isFinite(currentNumber) || !Number.isFinite(previousNumber) || previousNumber === 0) {
    return null;
  }
  return (currentNumber - previousNumber) / Math.abs(previousNumber);
}

export function buildGoogleAdsComparison(rows, endDate) {
  const currentWeekStart = dateKeyShift(endDate, -6);
  const previousWeekEnd = dateKeyShift(currentWeekStart, -1);
  const previousWeekStart = dateKeyShift(previousWeekEnd, -6);
  const currentMonthStart = dateKeyShift(endDate, -29);
  const previousMonthEnd = dateKeyShift(currentMonthStart, -1);
  const previousMonthStart = dateKeyShift(previousMonthEnd, -29);

  const currentWeek = aggregateGoogleAdsRows(rows, currentWeekStart, endDate);
  const previousWeek = aggregateGoogleAdsRows(rows, previousWeekStart, previousWeekEnd);
  const currentMonth = aggregateGoogleAdsRows(rows, currentMonthStart, endDate);
  const previousMonth = aggregateGoogleAdsRows(rows, previousMonthStart, previousMonthEnd);
  const metricKeys = [
    "impressions",
    "clicks",
    "spend",
    "conversions",
    "ctr",
    "cpc",
    "conversionRate",
    "cpl"
  ];

  return {
    ranges: {
      currentWeek: [currentWeekStart, endDate],
      previousWeek: [previousWeekStart, previousWeekEnd],
      currentMonth: [currentMonthStart, endDate],
      previousMonth: [previousMonthStart, previousMonthEnd]
    },
    currentWeek,
    previousWeek,
    currentMonth,
    previousMonth,
    weekDeltas: Object.fromEntries(
      metricKeys.map((key) => [key, percentDelta(currentWeek[key], previousWeek[key])])
    ),
    monthDeltas: Object.fromEntries(
      metricKeys.map((key) => [key, percentDelta(currentMonth[key], previousMonth[key])])
    )
  };
}
