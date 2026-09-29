const MONTH_ENUM_BY_NUMBER = {
  "01": "JANUARY",
  "02": "FEBRUARY",
  "03": "MARCH",
  "04": "APRIL",
  "05": "MAY",
  "06": "JUNE",
  "07": "JULY",
  "08": "AUGUST",
  "09": "SEPTEMBER",
  "10": "OCTOBER",
  "11": "NOVEMBER",
  "12": "DECEMBER"
};

function normalizeId(value, fieldName) {
  const normalized = String(value || "").replace(/\D/g, "");
  if (!normalized) throw new Error(`${fieldName} darf nicht leer sein.`);
  return normalized;
}

function parseYearMonth(value, fieldName) {
  const match = /^(\d{4})-(0[1-9]|1[0-2])$/.exec(String(value || ""));
  if (!match) throw new Error(`${fieldName} muss das Format YYYY-MM haben.`);
  return {
    year: match[1],
    month: MONTH_ENUM_BY_NUMBER[match[2]]
  };
}

function nullableNumber(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function microsToCurrencyUnits(value) {
  const micros = nullableNumber(value);
  return micros === null ? null : micros / 1_000_000;
}

export function buildGoogleAdsKeywordHistoricalMetricsRequest({
  keywords,
  geoTargetConstantIds = ["2276"],
  languageConstantId = "1001",
  keywordPlanNetwork = "GOOGLE_SEARCH",
  includeAdultKeywords = false,
  includeAverageCpc = true,
  yearMonthStart,
  yearMonthEnd
}) {
  const normalizedKeywords = [...new Set((keywords || []).map((keyword) => String(keyword).trim()).filter(Boolean))];
  if (!normalizedKeywords.length) throw new Error("keywords darf nicht leer sein.");

  if (Boolean(yearMonthStart) !== Boolean(yearMonthEnd)) {
    throw new Error("year_month_start und year_month_end muessen gemeinsam gesetzt werden.");
  }

  let yearMonthRange;
  if (yearMonthStart && yearMonthEnd) {
    const start = parseYearMonth(yearMonthStart, "year_month_start");
    const end = parseYearMonth(yearMonthEnd, "year_month_end");
    if (Number(yearMonthStart.replace("-", "")) > Number(yearMonthEnd.replace("-", ""))) {
      throw new Error("year_month_start darf nicht nach year_month_end liegen.");
    }
    yearMonthRange = { start, end };
  }

  return {
    keywords: normalizedKeywords,
    geoTargetConstants: (geoTargetConstantIds || []).map(
      (id) => `geoTargetConstants/${normalizeId(id, "geo_target_constant_id")}`
    ),
    language: `languageConstants/${normalizeId(languageConstantId, "language_constant_id")}`,
    keywordPlanNetwork,
    includeAdultKeywords: Boolean(includeAdultKeywords),
    historicalMetricsOptions: {
      includeAverageCpc: Boolean(includeAverageCpc),
      ...(yearMonthRange ? { yearMonthRange } : {})
    }
  };
}

export function normalizeGoogleAdsKeywordHistoricalMetricsResponse(response = {}) {
  return {
    aggregate_metric_results: response.aggregateMetricResults || null,
    results: (response.results || []).map((result) => {
      const metrics = result.keywordMetrics || {};
      return {
        text: result.text || null,
        close_variants: result.closeVariants || [],
        metrics: {
          average_monthly_searches: nullableNumber(metrics.avgMonthlySearches),
          competition: metrics.competition || null,
          competition_index: nullableNumber(metrics.competitionIndex),
          average_cpc_micros: nullableNumber(metrics.averageCpcMicros),
          average_cpc: microsToCurrencyUnits(metrics.averageCpcMicros),
          low_top_of_page_bid_micros: nullableNumber(metrics.lowTopOfPageBidMicros),
          low_top_of_page_bid: microsToCurrencyUnits(metrics.lowTopOfPageBidMicros),
          high_top_of_page_bid_micros: nullableNumber(metrics.highTopOfPageBidMicros),
          high_top_of_page_bid: microsToCurrencyUnits(metrics.highTopOfPageBidMicros),
          monthly_search_volumes: (metrics.monthlySearchVolumes || []).map((month) => ({
            year: nullableNumber(month.year),
            month: month.month || null,
            monthly_searches: nullableNumber(month.monthlySearches)
          }))
        }
      };
    })
  };
}
