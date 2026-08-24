const MONTHS = {
  jan: 1,
  january: 1,
  januar: 1,
  feb: 2,
  february: 2,
  februar: 2,
  mar: 3,
  march: 3,
  maerz: 3,
  marz: 3,
  mrz: 3,
  apr: 4,
  april: 4,
  may: 5,
  mai: 5,
  jun: 6,
  june: 6,
  juni: 6,
  jul: 7,
  july: 7,
  juli: 7,
  aug: 8,
  august: 8,
  sep: 9,
  sept: 9,
  september: 9,
  oct: 10,
  october: 10,
  okt: 10,
  oktober: 10,
  nov: 11,
  november: 11,
  dec: 12,
  december: 12,
  dez: 12,
  dezember: 12
};

const AMOUNT_TOKEN = /(?:\(\s*)?-?\s*(?:\d{1,3}(?:[.\s]\d{3})+|\d+)[.,]\d{2}\s*(?:\))?/g;
const INVOICE_WORD = /\b(?:invoice|rechnung|gutschrift|credit\s+note)\b/i;
const NON_INVOICE_WORD = /\b(?:booking\s+confirmation|reservation\s+confirmation|buchungs(?:\/listing)?\s+best[aä]tigung|reisebest[aä]tigung|ticketbest[aä]tigung)\b/i;

export function normalizeAccountingInvoiceText(value) {
  return String(value || "")
    .replace(/\u00ad/g, "")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "")
    .replace(/\u00a0/g, " ")
    .replace(/[‐‑‒–—−]/g, "-")
    .replace(/\r\n?/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .trim();
}

export function getNormalizedAccountingInvoiceLines(value) {
  return normalizeAccountingInvoiceText(value)
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function normalizeYear(value) {
  const year = Number(value);
  if (!Number.isInteger(year)) return null;
  if (String(value).length === 2) return 2000 + year;
  return year;
}

function validDate(day, month, year) {
  if (![day, month, year].every(Number.isInteger)) return false;
  if (year < 2000 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) return false;
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function formatDate(day, month, year) {
  return validDate(day, month, year)
    ? `${String(day).padStart(2, "0")}.${String(month).padStart(2, "0")}.${year}`
    : null;
}

function parseDateCandidate(value) {
  const text = String(value || "").toLowerCase().replace(/ä/g, "ae");
  let match = text.match(/\b(\d{1,2})[.]\s*(\d{1,2})[.]\s*(\d{2,4})\b/);
  if (match) return formatDate(Number(match[1]), Number(match[2]), normalizeYear(match[3]));

  match = text.match(/\b(\d{4})-(\d{2})-(\d{2})\b/);
  if (match) return formatDate(Number(match[3]), Number(match[2]), Number(match[1]));

  match = text.match(/\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b/);
  if (match) return formatDate(Number(match[2]), Number(match[1]), normalizeYear(match[3]));

  match = text.match(/\b(\d{1,2})[.\s/-]+([a-z]+)[.\s/-]+(\d{2,4})\b/);
  if (match && MONTHS[match[2]]) return formatDate(Number(match[1]), MONTHS[match[2]], normalizeYear(match[3]));

  match = text.match(/\b([a-z]+)\s+(\d{1,2}),?\s+(\d{2,4})\b/);
  if (match && MONTHS[match[1]]) return formatDate(Number(match[2]), MONTHS[match[1]], normalizeYear(match[3]));
  return null;
}

function monthKeyFromDate(value) {
  const match = String(value || "").match(/^\d{2}[.](\d{2})[.](\d{4})$/);
  return match ? `${match[1]}/${match[2].slice(-2)}` : null;
}

function extractInvoiceDate(lines) {
  const priorityLabels = [
    /\b(?:rechnungsdatum|datum\s+der\s+rechnung|belegdatum)\b/i,
    /\b(?:invoice\s+date|date\s+of\s+issue|billing\s+date|issue\s+date)\b/i,
    /\b(?:rechnung(?:s)?[.\s-]*(?:nr|nummer)[^\n]{0,60})\b/i
  ];
  for (const label of priorityLabels) {
    for (let index = 0; index < lines.length; index += 1) {
      if (!label.test(lines[index])) continue;
      for (let offset = 0; offset <= 2 && index + offset < lines.length; offset += 1) {
        const parsed = parseDateCandidate(lines.slice(index, index + offset + 1).join(" "));
        if (parsed) return parsed;
      }
    }
  }

  for (const line of lines) {
    if (!/\b(?:datum|date)\b/i.test(line)) continue;
    const parsed = parseDateCandidate(line);
    if (parsed) return parsed;
  }

  for (const line of lines) {
    if (!/\b(?:leistungszeitraum|service\s+period)\b/i.test(line)) continue;
    const matches = [...line.matchAll(/\b\d{1,2}[.]\d{1,2}[.]\d{2,4}\b/g)];
    const parsed = parseDateCandidate(matches.at(-1)?.[0]);
    if (parsed) return parsed;
  }
  return null;
}

export function parseAccountingAmountToken(value) {
  const original = String(value || "");
  if (!/\d/.test(original)) return null;
  const negativeParentheses = /^\s*\(.*\)\s*$/.test(original);
  let normalized = original.replace(/\s/g, "").replace(/[^\d.,-]/g, "");
  const lastComma = normalized.lastIndexOf(",");
  const lastDot = normalized.lastIndexOf(".");
  if (lastComma >= 0 && lastDot >= 0) {
    const decimal = lastComma > lastDot ? "," : ".";
    const thousands = decimal === "," ? "." : ",";
    normalized = normalized.replaceAll(thousands, "").replace(decimal, ".");
  } else if (lastComma >= 0) {
    normalized = /,\d{2}$/.test(normalized)
      ? normalized.replaceAll(".", "").replace(/,(?=[^,]*$)/, ".").replaceAll(",", "")
      : normalized.replaceAll(",", "");
  } else if (lastDot >= 0) {
    if (/\.\d{2}$/.test(normalized)) {
      const integer = normalized.slice(0, lastDot).replaceAll(".", "").replaceAll(",", "");
      normalized = `${integer}.${normalized.slice(lastDot + 1)}`;
    } else {
      normalized = normalized.replaceAll(".", "");
    }
  }
  let amount = Number(normalized);
  if (!Number.isFinite(amount)) return null;
  if (negativeParentheses && amount > 0) amount *= -1;
  return Math.round(amount * 100) / 100;
}

function amountTokens(line) {
  return [...String(line || "").matchAll(AMOUNT_TOKEN)]
    .map((match) => ({ raw: match[0], value: parseAccountingAmountToken(match[0]), index: match.index || 0 }))
    .filter((item) => item.value !== null);
}

function detectCurrency(value) {
  const text = String(value || "");
  const eur = /(?:€|\bEUR\b)/i.test(text);
  const usd = /(?:\$|\bUSD\b)/i.test(text);
  if (eur && usd) return "MIXED";
  if (eur) return "EUR";
  if (usd) return "USD";
  return null;
}

function candidateFromLine(lines, index, priority, take = "last") {
  const own = amountTokens(lines[index]);
  if (own.length) {
    const token = take === "first" ? own[0] : own.at(-1);
    return { value: token.value, priority, line: lines[index], currency: detectCurrency(lines[index]) };
  }
  for (let offset = 1; offset <= 2 && index + offset < lines.length; offset += 1) {
    const nextLine = lines[index + offset];
    if (/\b(?:subtotal|total|netto|brutto|mwst|vat|tax|ust|summe|betrag)\b/i.test(nextLine)) break;
    const next = amountTokens(nextLine);
    if (!next.length) continue;
    const token = take === "first" ? next[0] : next.at(-1);
    return { value: token.value, priority: priority - offset, line: nextLine, currency: detectCurrency(nextLine) };
  }
  return null;
}

function chooseCandidate(candidates) {
  if (!candidates.length) return { value: null, ambiguous: false, candidate: null };
  const bestPriority = Math.max(...candidates.map((candidate) => candidate.priority));
  const best = candidates.filter((candidate) => candidate.priority === bestPriority);
  const distinct = [...new Set(best.map((candidate) => candidate.value))];
  return distinct.length === 1
    ? { value: distinct[0], ambiguous: false, candidate: best[0] }
    : { value: null, ambiguous: true, candidate: null };
}

function extractAmounts(lines, text) {
  const net = [];
  const gross = [];
  const vat = [];
  const vatRateTotals = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    let candidate;
    if (/\bgesamtnettobetrag\b/i.test(line)) candidate = candidateFromLine(lines, index, 130);
    else if (/\b(?:nettobetrag|net\s+amount|net\s+total)\b/i.test(line)) candidate = candidateFromLine(lines, index, 125);
    else if (/\b(?:subtotal|zwischensumme)\b/i.test(line)) candidate = candidateFromLine(lines, index, 110);
    if (candidate) net.push(candidate);

    if (/\b(?:gesamtbetrag\s+(?:ust|mwst|vat)|gesamt\s+(?:ust|mwst|vat))\b/i.test(line)) {
      candidate = candidateFromLine(lines, index, 135);
      if (candidate) vat.push(candidate);
    } else if (/\b(?:mwst|mehrwertsteuer|umsatzsteuer|vat|sales\s+tax|tax\s+amount|ust[.:]?)\b/i.test(line)) {
      candidate = candidateFromLine(lines, index, 115);
      if (candidate) vat.push(candidate);
      if (/\b\d{1,2}(?:[.,]\d{1,2})?\s*%\s*(?:mwst|ust|vat)|\b(?:mwst|ust|vat)\s*\d{1,2}(?:[.,]\d{1,2})?\s*%/i.test(line)) {
        if (candidate) vatRateTotals.push(candidate);
      }
    }

    if (/\b(?:rechnungsbetrag|bruttobetrag|gesamtsumme|grand\s+total|balance\s+due)\b/i.test(line)) {
      candidate = candidateFromLine(lines, index, 135);
    } else if (/\bamount\s+due\b/i.test(line)) {
      candidate = candidateFromLine(lines, index, 120);
    } else if (/\bgesamtbetrag\b(?!\s*(?:ust|mwst|vat|steuer|tax))/i.test(line)) {
      candidate = candidateFromLine(lines, index, 125);
    } else if (/^\s*(?:total|gesamt)\b/i.test(line) && !/\b(?:tax|vat|ust|mwst|excl|before)\b/i.test(line)) {
      candidate = candidateFromLine(lines, index, 110);
    } else {
      candidate = null;
    }
    if (candidate) gross.push(candidate);
  }

  let netChoice = chooseCandidate(net);
  let grossChoice = chooseCandidate(gross);
  let vatChoice = chooseCandidate(vat);
  if (vatRateTotals.length > 1 && !vat.some((candidate) => candidate.priority >= 130)) {
    const uniqueLines = [...new Map(vatRateTotals.map((candidate) => [candidate.line, candidate])).values()];
    vatChoice = {
      value: Math.round(uniqueLines.reduce((sum, candidate) => sum + candidate.value, 0) * 100) / 100,
      ambiguous: false,
      candidate: uniqueLines[0]
    };
  }

  const reverseCharge = /\b(?:reverse\s*charge|steuerschuldnerschaft|article\s+196|art\.?\s*196)\b/i.test(text);
  let netAmount = netChoice.value;
  let vatAmount = vatChoice.value;
  let grossAmount = grossChoice.value;
  if (vatAmount === null && reverseCharge) vatAmount = 0;
  if (grossAmount !== null && vatAmount !== null && netAmount === null) {
    netAmount = Math.round((grossAmount - vatAmount) * 100) / 100;
  }
  if (netAmount !== null && vatAmount !== null && grossAmount === null) {
    grossAmount = Math.round((netAmount + vatAmount) * 100) / 100;
  }
  if (netAmount !== null && grossAmount !== null && vatAmount === null) {
    vatAmount = Math.round((grossAmount - netAmount) * 100) / 100;
  }

  const currency = grossChoice.candidate?.currency || netChoice.candidate?.currency || vatChoice.candidate?.currency || detectCurrency(text);
  return {
    net_amount: netAmount,
    vat_amount: vatAmount,
    gross_amount: grossAmount,
    currency_code: currency,
    amount_ambiguous: netChoice.ambiguous || vatChoice.ambiguous || grossChoice.ambiguous,
    reverse_charge: reverseCharge
  };
}

function extractInvoiceNumber(lines) {
  for (const line of lines) {
    const match = line.match(/\b(?:invoice|rechnungs?)[.\s-]*(?:number|nummer|nr|#)\s*[:.]?\s*([A-Z0-9][A-Z0-9/ ._-]{2,40})/i);
    if (!match) continue;
    return match[1].trim().replace(/\s{2,}/g, " ").replace(/[.,;:]$/, "") || null;
  }
  return null;
}

function extractRecipient(lines, text) {
  const looksLikeVipIssuedInvoice = lines.some((line) => /^VIP-Studios\s*\|\s*Feichtmeyer,/i.test(line)) &&
    lines.some((line) => /^Firmen-ID:/i.test(line));
  const knownVipRecipient = !looksLikeVipIssuedInvoice && /\b(?:VIP[- ]?Studios|Moritz\s+Feichtmeyer)\b/i.test(text);
  return {
    recipient_name: knownVipRecipient ? "VIP-Studios / Moritz Feichtmeyer" : null,
    recipient_address: [],
    recipient_firm_id: null,
    recipient_vat_id: null,
    recipient_confidence: knownVipRecipient ? "high" : "low",
    recipient_source: knownVipRecipient ? "known_accounting_recipient_identity" : "not_found",
    payment_organization: null,
    payment_account_alias: null
  };
}

function extractSupplier(lines, text) {
  const issuedBy = lines
    .map((line) => line.match(/\b(?:rechnung\s+ausgestellt\s+von|invoice\s+(?:issued\s+)?by)\s+(.+?)(?:\s+im\s+namen|$)/i))
    .find(Boolean);
  if (issuedBy?.[1]) {
    return { supplier_name: issuedBy[1].trim(), supplier_confidence: "high", supplier_source: "issued_by_label" };
  }
  const suppliers = [
    [/\bApple\s+Distribution\s+International\b/i, "Apple Distribution International Ltd."],
    [/\bOpenAI\b/i, "OpenAI"],
    [/\bGoogle\s+Ireland\b|\bGoogle\s+Ads\b/i, "Google Ireland / Google Ads"],
    [/\bDeutsche\s+Telekom\b|\bTelekom\s+Deutschland\b/i, "Telekom Deutschland GmbH"],
    [/\bn8n\b/i, "n8n"],
    [/\bUber\s+B[.]?V[.]?\b/i, "Uber B.V."],
    [/\bStripe\b/i, "Stripe"],
    [/\bMicrosoft\b/i, "Microsoft"],
    [/\bAmazon\b/i, "Amazon"]
  ];
  const found = suppliers.find(([pattern]) => pattern.test(text));
  return found
    ? { supplier_name: found[1], supplier_confidence: "high", supplier_source: "known_supplier_identity" }
    : { supplier_name: null, supplier_confidence: "low", supplier_source: "not_found" };
}

function amountsPlausible({ net_amount: net, vat_amount: vat, gross_amount: gross }) {
  if (![net, vat, gross].every((value) => Number.isFinite(value))) return false;
  if (net < 0 || vat < 0 || gross <= 0) return false;
  return Math.abs(net + vat - gross) <= 0.02 && vat <= net * 0.3 + 0.02;
}

export function extractAccountingInvoice(rawText) {
  const text = normalizeAccountingInvoiceText(rawText);
  const lines = getNormalizedAccountingInvoiceLines(text);
  const recipient = extractRecipient(lines, text);
  const supplier = extractSupplier(lines, text);
  const invoiceNumber = extractInvoiceNumber(lines);
  const invoiceDate = extractInvoiceDate(lines);
  const amounts = extractAmounts(lines, text);
  const strongNonInvoice = NON_INVOICE_WORD.test(text) && !INVOICE_WORD.test(text);
  const invoiceCharacter = !strongNonInvoice && Boolean(
    INVOICE_WORD.test(text) && (invoiceNumber || invoiceDate || amounts.gross_amount !== null)
  );
  const multipleDocuments = /\bcredit\s+note\b/i.test(text) && /\binvoice\b/i.test(text);

  if (!invoiceCharacter) {
    return {
      ...recipient,
      ...supplier,
      kind: strongNonInvoice ? "non_invoice" : "unknown",
      status: strongNonInvoice ? "not_invoice" : "not_extracted",
      invoice_character: false,
      invoice_number: invoiceNumber,
      invoice_date: invoiceDate,
      month_key: invoiceDate ? monthKeyFromDate(invoiceDate) : null,
      net_amount: null,
      vat_amount: null,
      gross_amount: null,
      currency_code: amounts.currency_code,
      confidence: strongNonInvoice ? "high" : "low",
      amount_ambiguous: amounts.amount_ambiguous,
      multiple_documents: multipleDocuments,
      classification_reasons: strongNonInvoice ? ["booking_or_reservation_confirmation"] : ["invoice_character_not_proven"]
    };
  }

  const plausible = amountsPlausible(amounts);
  const complete = plausible && !amounts.amount_ambiguous;
  return {
    ...recipient,
    ...supplier,
    kind: "invoice",
    status: complete ? "extracted" : amounts.gross_amount !== null || amounts.net_amount !== null ? "partial" : "not_extracted",
    invoice_character: true,
    invoice_number: invoiceNumber,
    invoice_date: invoiceDate,
    month_key: invoiceDate ? monthKeyFromDate(invoiceDate) : null,
    net_amount: amounts.net_amount,
    vat_amount: amounts.vat_amount,
    gross_amount: amounts.gross_amount,
    currency_code: amounts.currency_code,
    confidence: complete && invoiceNumber && invoiceDate ? "high" : "medium",
    amount_ambiguous: amounts.amount_ambiguous,
    multiple_documents: multipleDocuments,
    reverse_charge: amounts.reverse_charge,
    classification_reasons: [
      invoiceNumber ? "invoice_number_present" : "invoice_number_missing",
      invoiceDate ? "invoice_date_present" : "invoice_date_missing",
      plausible ? "amounts_plausible" : "amounts_incomplete_or_implausible",
      amounts.amount_ambiguous ? "amount_candidates_conflict" : "amount_candidates_consistent",
      multipleDocuments ? "multiple_document_types_detected" : "single_document_type"
    ]
  };
}
