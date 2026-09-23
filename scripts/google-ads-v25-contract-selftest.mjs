import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
const adsStart = source.indexOf('server.tool(\n    "google_ads_check_config"');
const adsEnd = source.indexOf('server.tool(\n    "dataforseo_google_keyword_overview"');
const budgetLookupStart = source.indexOf("async function lookupCampaignBudgetAmountMicros");
const budgetLookupEnd = source.indexOf("async function assessGoogleAdsMutateSafety");

assert.ok(adsStart >= 0 && adsEnd > adsStart, "Google Ads tool block must be present");
assert.ok(
  budgetLookupStart >= 0 && budgetLookupEnd > budgetLookupStart,
  "Google Ads budget lookup must be present"
);
const adsTools = source.slice(adsStart, adsEnd);
const budgetLookup = source.slice(budgetLookupStart, budgetLookupEnd);

assert.match(source, /const GOOGLE_ADS_API_VERSION_DEFAULT = "v25";/);
assert.match(source, /api_version_source: GOOGLE_ADS_API_VERSION_SOURCE/);
assert.match(source, /\^v\\d\+\$/);
assert.match(source, /`\$\{GOOGLE_ADS_API_BASE\}\/\$\{GOOGLE_ADS_API_VERSION\}\$\{path\}`/);

assert.match(adsTools, /path: "\/customers:listAccessibleCustomers"/);
assert.match(adsTools, /path: `\/customers\/\$\{normalizedCustomerId\}\/googleAds:search`/);
assert.match(adsTools, /path: `\/customers\/\$\{normalizedCustomerId\}\/\$\{service\}:mutate`/);
assert.match(adsTools, /validateOnly: effectiveValidateOnly/);
assert.match(adsTools, /live_mutation_executed: !effectiveValidateOnly/);
assert.ok(!budgetLookup.includes("pageSize"), "v25 Search rejects the legacy pageSize request field");

for (const removedField of [
  "callAd",
  "callAdInfo",
  "leadFormOnly",
  "videoBrandSafetySuitability",
  "customerAcquisitionGoalSettings",
  "additionalHighLifetimeValue",
  "additionalValue"
]) {
  assert.ok(
    !adsTools.includes(removedField),
    `Google Ads tools must not depend on removed v23-v25 field ${removedField}`
  );
}

console.log("google-ads-v25-contract-selftest: ok");
