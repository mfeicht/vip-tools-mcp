import assert from "node:assert/strict";
import test from "node:test";
import {
  appendGeneralInformationQualification,
  validateGeneralInformationReply
} from "../lib/email-action-general-information.js";

const standard = {
  answerScope: "general_information_only",
  requestType: "guest_article",
  industryRisk: "uncertain",
  inboundSubject: "Guest post pricing",
  inboundBody: "Could you share the guest post price for reise-stories.de?",
  inboundBodyTruncated: false,
  replyBody: "The current Guest Article price is €365 plus VAT.\n\nBest regards",
  discountStage: null,
  proposedPriceEur: null,
  negotiationRoundsCompleted: 0,
  negotiationRoundsFailed: 0,
  includedLinkCount: 1,
  previousOfferAmountsEur: []
};

test("ordinary unknown-industry pricing inquiry can receive conditional information", () => {
  assert.equal(validateGeneralInformationReply(standard), true);
  assert.equal(validateGeneralInformationReply({ ...standard, requestType: "link_insertion" }), true);
  assert.equal(validateGeneralInformationReply({ ...standard, requestType: "link_purchase" }), true);
  assert.equal(validateGeneralInformationReply({ ...standard, inboundBody: "Can you quote your price in USD?" }), true);
  const body = appendGeneralInformationQualification(standard.replyBody, "en");
  assert.match(body, /editorial review\.\n\nBest regards$/u);
  assert.ok(body.indexOf("editorial review") < body.indexOf("Best regards"));
  assert.match(appendGeneralInformationQualification("Hallo\n\nViele Grüße", "de"), /redaktionellen Prüfung\.\n\nViele Grüße$/u);
  assert.match(
    appendGeneralInformationQualification("Hallo\n\nMit besten Grüßen\nReise-Stories", "de"),
    /redaktionellen Prüfung\.\n\nMit besten Grüßen\nReise-Stories$/u
  );
});

test("explicit prohibited or concrete foreign targets do not use the general-information lane", () => {
  for (const inboundBody of [
    "We want casino links on your site.",
    "Can we publish crypto content?",
    "Please link https://example.com/travel from an existing article."
  ]) {
    assert.throws(() => validateGeneralInformationReply({ ...standard, inboundBody }));
  }
});

test("discounts, payment requests, promises and nonstandard requests remain gated", () => {
  const changes = [
    { requestType: "discount_negotiation" },
    { requestType: "press_release" },
    { industryRisk: "prohibited" },
    { industryRisk: "safe" },
    { inboundBodyTruncated: true },
    { discountStage: "initial" },
    { includedLinkCount: 2 },
    { proposedPriceEur: 100 },
    { inboundBody: "Can you take PayPal?" },
    { inboundBody: "I can pay $35 for a guest post." },
    { inboundBody: "Can you do 100 EUR?" },
    { replyBody: "We can offer this for $300." },
    { replyBody: "We approved your article." },
    { replyBody: "Your article has been booked." }
  ];
  for (const change of changes) {
    assert.throws(() => validateGeneralInformationReply({ ...standard, ...change }), JSON.stringify(change));
  }
});
