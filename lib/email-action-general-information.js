const STANDARD_REQUEST_TYPES = new Set(["guest_article", "link_insertion", "link_purchase"]);
const PROHIBITED_INBOUND = /\b(?:casino|gambling|betting|sportsbook|wagering|cbd|crypto(?:currency)?|web3|nft|porn(?:ography)?|adult|malware|phishing|scam|gluecksspiel|gluecksspiele|glücksspiel|wetten|wettanbieter)\b/iu;
const SPECIAL_TERMS = /\b(?:discount|rebate|coupon|promo(?:tion)?|paypal|payoneer|bank transfer|rabatt|nachlass|gutschein|sonderpreis|sonderrabatt)\b/iu;
const UNSOURCED_CURRENCY = /\b(?:usd|dollars?)\b|\$/iu;
const INBOUND_PRICE_AMOUNT = /(?:[$€]\s*\d|\b\d+(?:[.,]\d+)?\s*(?:€|eur|usd|dollars?|euros?)(?=$|[\s.,;!?)]))/iu;
const PLACEMENT_COMMITMENT = /\b(?:your (?:article|post|link|placement) (?:is|has been) (?:accepted|approved|booked|reserved)|we (?:have )?(?:accepted|approved|booked|reserved) your|we (?:will|can guarantee to) publish your|guaranteed publication|ihr (?:artikel|beitrag|link) (?:ist|wurde) (?:angenommen|freigegeben|gebucht)|wir (?:haben|werden) (?:ihren|ihre) (?:artikel|beitrag|link) (?:angenommen|veröffentlichen))\b/iu;
const URL_PATTERN = /\b(?:https?:\/\/|www\.)[^\s<>"')]+/giu;

export function validateGeneralInformationReply({
  answerScope,
  requestType,
  industryRisk,
  inboundSubject,
  inboundBody,
  inboundBodyTruncated,
  replyBody,
  discountStage,
  proposedPriceEur,
  negotiationRoundsCompleted,
  negotiationRoundsFailed,
  includedLinkCount,
  previousOfferAmountsEur
}) {
  if (answerScope !== "general_information_only") {
    throw new Error("Allgemeiner Auskunftsmodus wurde nicht explizit gewaehlt.");
  }
  if (!STANDARD_REQUEST_TYPES.has(requestType) || industryRisk !== "uncertain") {
    throw new Error("Allgemeine Auskunft ist nur fuer Standard-Link-/Gastbeitragsanfragen mit unbekannter Branche zulaessig.");
  }
  if (!inboundBody || inboundBodyTruncated) {
    throw new Error("Eingangstext fehlt oder ist abgeschnitten; sichere Standardauskunft gesperrt.");
  }
  const inbound = `${inboundSubject || ""}\n${inboundBody}`;
  if (PROHIBITED_INBOUND.test(inbound)) {
    throw new Error("Eingang nennt eine ausgeschlossene oder riskante Branche; keine allgemeine externe Auskunft.");
  }
  for (const match of inbound.matchAll(URL_PATTERN)) {
    const raw = match[0].replace(/[.,;:!?]+$/u, "");
    let host;
    try {
      host = new URL(raw.startsWith("www.") ? `https://${raw}` : raw).hostname.toLowerCase();
    } catch {
      throw new Error("Eingang enthaelt eine nicht pruefbare URL.");
    }
    if (host !== "reise-stories.de" && host !== "www.reise-stories.de") {
      throw new Error("Eingang enthaelt eine fremde Ziel-URL; allgemeine externe Auskunft gesperrt.");
    }
  }
  if (
    discountStage != null ||
    proposedPriceEur != null ||
    Number(negotiationRoundsCompleted || 0) !== 0 ||
    Number(negotiationRoundsFailed || 0) !== 0 ||
    Number(includedLinkCount || 1) !== 1 ||
    (previousOfferAmountsEur || []).length !== 0 ||
    SPECIAL_TERMS.test(inbound) ||
    INBOUND_PRICE_AMOUNT.test(inbound) ||
    SPECIAL_TERMS.test(replyBody) ||
    UNSOURCED_CURRENCY.test(replyBody)
  ) {
    throw new Error("Rabatt-, Zahlungs-, Umrechnungs- oder Sonderkonditionsantwort braucht den bisherigen Pruefweg.");
  }
  if (PLACEMENT_COMMITMENT.test(replyBody)) {
    throw new Error("Allgemeine Auskunft darf keine konkrete Platzierung zusagen.");
  }
  return true;
}

export function appendGeneralInformationQualification(replyBody, language) {
  const qualification = language === "de"
    ? "Bevor wir eine konkrete Platzierung bestätigen können, benötigen wir Thema und Ziel-URL zur redaktionellen Prüfung."
    : "Before we can confirm a specific placement, please send us the topic and target URL for editorial review.";
  const body = String(replyBody || "").trim();
  const closing = body.match(/\n\n((?:Best regards|Kind regards|Warm regards|Sincerely|Cheers|Viele Grüße|Herzliche Grüße|Mit besten Grüßen|Mit freundlichen Grüßen)[^\n]*(?:\n[^\n]+)?)$/iu);
  if (closing) {
    return `${body.slice(0, closing.index).trimEnd()}\n\n${qualification}\n\n${closing[1]}`;
  }
  return `${body}\n\n${qualification}`;
}
