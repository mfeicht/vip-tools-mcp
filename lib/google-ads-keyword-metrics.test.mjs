import assert from "node:assert/strict";
import test from "node:test";

import {
  buildGoogleAdsKeywordHistoricalMetricsRequest,
  normalizeGoogleAdsKeywordHistoricalMetricsResponse
} from "./google-ads-keyword-metrics.js";

test("builds the Germany/German/Google Search request by default", () => {
  assert.deepEqual(
    buildGoogleAdsKeywordHistoricalMetricsRequest({
      keywords: [" entsorgung auto ", "entsorgung auto"]
    }),
    {
      keywords: ["entsorgung auto"],
      geoTargetConstants: ["geoTargetConstants/2276"],
      language: "languageConstants/1001",
      keywordPlanNetwork: "GOOGLE_SEARCH",
      includeAdultKeywords: false,
      historicalMetricsOptions: {
        includeAverageCpc: true
      }
    }
  );
});

test("maps an explicit year-month range to Google Ads enums", () => {
  const request = buildGoogleAdsKeywordHistoricalMetricsRequest({
    keywords: ["entsorgung auto"],
    yearMonthStart: "2025-09",
    yearMonthEnd: "2026-08"
  });
  assert.deepEqual(request.historicalMetricsOptions.yearMonthRange, {
    start: { year: "2025", month: "SEPTEMBER" },
    end: { year: "2026", month: "AUGUST" }
  });
});

test("rejects incomplete or reversed ranges", () => {
  assert.throws(
    () =>
      buildGoogleAdsKeywordHistoricalMetricsRequest({
        keywords: ["entsorgung auto"],
        yearMonthStart: "2025-09"
      }),
    /gemeinsam/
  );
  assert.throws(
    () =>
      buildGoogleAdsKeywordHistoricalMetricsRequest({
        keywords: ["entsorgung auto"],
        yearMonthStart: "2026-09",
        yearMonthEnd: "2025-09"
      }),
    /darf nicht nach/
  );
});

test("normalizes keyword metrics and currency micros", () => {
  assert.deepEqual(
    normalizeGoogleAdsKeywordHistoricalMetricsResponse({
      results: [
        {
          text: "entsorgung auto",
          closeVariants: ["auto entsorgung"],
          keywordMetrics: {
            avgMonthlySearches: "140",
            competition: "MEDIUM",
            competitionIndex: "52",
            averageCpcMicros: "2340000",
            lowTopOfPageBidMicros: "1200000",
            highTopOfPageBidMicros: "4100000",
            monthlySearchVolumes: [{ year: "2026", month: "AUGUST", monthlySearches: "170" }]
          }
        }
      ]
    }).results[0],
    {
      text: "entsorgung auto",
      close_variants: ["auto entsorgung"],
      metrics: {
        average_monthly_searches: 140,
        competition: "MEDIUM",
        competition_index: 52,
        average_cpc_micros: 2340000,
        average_cpc: 2.34,
        low_top_of_page_bid_micros: 1200000,
        low_top_of_page_bid: 1.2,
        high_top_of_page_bid_micros: 4100000,
        high_top_of_page_bid: 4.1,
        monthly_search_volumes: [{ year: 2026, month: "AUGUST", monthly_searches: 170 }]
      }
    }
  );
});
