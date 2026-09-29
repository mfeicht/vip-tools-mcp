import assert from "node:assert/strict";
import test from "node:test";

import {
  buildGoogleAdsCountryTargetQuery,
  buildGoogleAdsKeywordHistoricalMetricsRequest,
  buildGoogleAdsKeywordIdeasRequest,
  buildGoogleAdsLanguageTargetQuery,
  normalizeGoogleAdsCountryCodes,
  normalizeGoogleAdsLanguageCode,
  normalizeGoogleAdsKeywordHistoricalMetricsResponse
} from "./google-ads-keyword-metrics.js";

test("normalizes country and language codes and builds lookup queries", () => {
  assert.deepEqual(normalizeGoogleAdsCountryCodes(["de", "AT", "de"]), ["DE", "AT"]);
  assert.equal(normalizeGoogleAdsLanguageCode("pt-br"), "pt_BR");
  assert.match(buildGoogleAdsCountryTargetQuery(["de", "AT"]), /country_code IN \('DE', 'AT'\)/);
  assert.match(buildGoogleAdsLanguageTargetQuery("de"), /language_constant\.code = 'de'/);
});

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

test("builds keyword ideas from keywords and a landing page", () => {
  const request = buildGoogleAdsKeywordIdeasRequest({
    seedKeywords: [" entsorgung auto ", "autoverwertung"],
    pageUrl: "https://example.com/autoverwertung",
    geoTargetConstantIds: ["2040"],
    languageConstantId: "1001",
    pageSize: 250,
    includeKeywordConcepts: true
  });
  assert.deepEqual(request.keywordAndUrlSeed, {
    keywords: ["entsorgung auto", "autoverwertung"],
    url: "https://example.com/autoverwertung"
  });
  assert.deepEqual(request.geoTargetConstants, ["geoTargetConstants/2040"]);
  assert.equal(request.language, "languageConstants/1001");
  assert.equal(request.pageSize, 250);
  assert.deepEqual(request.keywordAnnotation, ["KEYWORD_CONCEPT"]);
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
      keyword_annotations: null,
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

test("normalizes metrics returned by keyword ideas", () => {
  const normalized = normalizeGoogleAdsKeywordHistoricalMetricsResponse({
    results: [
      {
        text: "auto entsorgen",
        keywordIdeaMetrics: {
          avgMonthlySearches: "90",
          competition: "LOW",
          averageCpcMicros: "1500000"
        },
        keywordAnnotations: { concepts: [{ name: "Autoentsorgung" }] }
      }
    ]
  });
  assert.equal(normalized.results[0].metrics.average_monthly_searches, 90);
  assert.equal(normalized.results[0].metrics.average_cpc, 1.5);
  assert.deepEqual(normalized.results[0].keyword_annotations, {
    concepts: [{ name: "Autoentsorgung" }]
  });
});
