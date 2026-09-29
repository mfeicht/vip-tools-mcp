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

export function normalizeGoogleAdsCountryCodes(values) {
  const normalized = [...new Set((values || []).map((value) => String(value).trim().toUpperCase()).filter(Boolean))];
  for (const code of normalized) {
    if (!/^[A-Z]{2}$/.test(code)) {
      throw new Error(`country_code muss ein ISO-3166-1-Alpha-2-Code sein: ${code}`);
    }
  }
  return normalized;
}

export function normalizeGoogleAdsLanguageCode(value) {
  const raw = String(value || "").trim().replace(/-/g, "_");
  if (!/^[A-Za-z]{2,3}(?:_[A-Za-z]{2,4})?$/.test(raw)) {
    throw new Error(`language_code ist ungueltig: ${raw || "leer"}`);
  }
  const [base, region] = raw.split("_");
  return region ? `${base.toLowerCase()}_${region.toUpperCase()}` : base.toLowerCase();
}

export function buildGoogleAdsCountryTargetQuery(countryCodes) {
  const normalized = normalizeGoogleAdsCountryCodes(countryCodes);
  if (!normalized.length) throw new Error("country_codes darf nicht leer sein.");
  const values = normalized.map((code) => `'${code}'`).join(", ");
  return `
    SELECT
      geo_target_constant.id,
      geo_target_constant.name,
      geo_target_constant.country_code,
      geo_target_constant.target_type,
      geo_target_constant.status
    FROM geo_target_constant
    WHERE geo_target_constant.country_code IN (${values})
      AND geo_target_constant.target_type = 'Country'
      AND geo_target_constant.status = 'ENABLED'
  `;
}

export function buildGoogleAdsLanguageTargetQuery(languageCode) {
  const normalized = normalizeGoogleAdsLanguageCode(languageCode);
  return `
    SELECT
      language_constant.id,
      language_constant.code,
      language_constant.name,
      language_constant.targetable
    FROM language_constant
    WHERE language_constant.code = '${normalized}'
      AND language_constant.targetable = TRUE
    LIMIT 1
  `;
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

export function buildGoogleAdsKeywordIdeasRequest({
  seedKeywords = [],
  pageUrl,
  geoTargetConstantIds = ["2276"],
  languageConstantId = "1001",
  keywordPlanNetwork = "GOOGLE_SEARCH",
  includeAdultKeywords = false,
  includeAverageCpc = true,
  includeKeywordConcepts = false,
  yearMonthStart,
  yearMonthEnd,
  pageSize = 500,
  pageToken
}) {
  const normalizedSeedKeywords = [
    ...new Set((seedKeywords || []).map((keyword) => String(keyword).trim()).filter(Boolean))
  ];
  const normalizedPageUrl = String(pageUrl || "").trim();
  if (!normalizedSeedKeywords.length && !normalizedPageUrl) {
    throw new Error("Mindestens seed_keywords oder page_url ist erforderlich.");
  }

  const metricsRequest = buildGoogleAdsKeywordHistoricalMetricsRequest({
    keywords: normalizedSeedKeywords.length ? normalizedSeedKeywords : ["seed-placeholder"],
    geoTargetConstantIds,
    languageConstantId,
    keywordPlanNetwork,
    includeAdultKeywords,
    includeAverageCpc,
    yearMonthStart,
    yearMonthEnd
  });
  const {
    keywords: _unusedKeywords,
    ...targeting
  } = metricsRequest;

  let seed;
  if (normalizedSeedKeywords.length && normalizedPageUrl) {
    seed = { keywordAndUrlSeed: { keywords: normalizedSeedKeywords, url: normalizedPageUrl } };
  } else if (normalizedSeedKeywords.length) {
    seed = { keywordSeed: { keywords: normalizedSeedKeywords } };
  } else {
    seed = { urlSeed: { url: normalizedPageUrl } };
  }

  return {
    ...targeting,
    ...seed,
    ...(includeKeywordConcepts ? { keywordAnnotation: ["KEYWORD_CONCEPT"] } : {}),
    pageSize,
    ...(pageToken ? { pageToken } : {})
  };
}

export function normalizeGoogleAdsKeywordHistoricalMetricsResponse(response = {}) {
  return {
    aggregate_metric_results: response.aggregateMetricResults || null,
    results: (response.results || []).map((result) => {
      const metrics = result.keywordMetrics || result.keywordIdeaMetrics || {};
      return {
        text: result.text || null,
        close_variants: result.closeVariants || [],
        keyword_annotations: result.keywordAnnotations || null,
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
