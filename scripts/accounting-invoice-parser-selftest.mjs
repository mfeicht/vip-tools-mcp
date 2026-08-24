import assert from "node:assert/strict";

import { extractAccountingInvoice } from "../lib/accounting-invoice-parser.js";

const german = extractAccountingInvoice(`
Rechnung
Rechnung ausgestellt von Uber B.V. im Namen von Test
VIP-Studios / Moritz Feichtmeyer
Rechnungsnummer 72263845
Rechnungsdatum: 24.04.2026
Nettobetrag 358,20 EUR
19% MwSt 68,06 EUR
Rechnungsbetrag 426,26 EUR
`);
assert.equal(german.status, "extracted");
assert.equal(german.invoice_number, "72263845");
assert.equal(german.invoice_date, "24.04.2026");
assert.equal(german.net_amount, 358.2);
assert.equal(german.vat_amount, 68.06);
assert.equal(german.gross_amount, 426.26);
assert.equal(german.currency_code, "EUR");
assert.equal(german.supplier_name, "Uber B.V.");

const english = extractAccountingInvoice(`
Invoice
OpenAI
VIP-Studios / Moritz Feichtmeyer
Invoice number 6FFC131F-0015
Date of issue April 30, 2026
Subtotal €83.05
Total €83.05
Amount due €83.05
Tax to be paid on reverse charge basis under Article 196
`);
assert.equal(english.status, "extracted");
assert.equal(english.invoice_date, "30.04.2026");
assert.equal(english.net_amount, 83.05);
assert.equal(english.vat_amount, 0);
assert.equal(english.gross_amount, 83.05);
assert.equal(english.currency_code, "EUR");

const shortDate = extractAccountingInvoice(`
INVOICE
n8n GmbH
VIP-Studios / Moritz Feichtmeyer
Invoice #: DE2026-7433
Invoice date: 4/7/26
DESCRIPTION QUANTITY PRICE TAX TOTAL
Total 38.50 0.00 EUR 38.50
VAT reverse charged to customer based on article 196 Directive 2006/112/EC
`);
assert.equal(shortDate.status, "extracted");
assert.equal(shortDate.invoice_date, "07.04.2026");
assert.equal(shortDate.net_amount, 38.5);
assert.equal(shortDate.vat_amount, 0);
assert.equal(shortDate.gross_amount, 38.5);

const mixedVat = extractAccountingInvoice(`
Rechnung
Uber B.V.
VIP-Studios / Moritz Feichtmeyer
Rechnung.-Nr.: 2026-03971 München, 24.06.2026
Bruttobetrag 761,55 EUR
0,00 % MwSt: 0,00 EUR
7,00 % MwSt: 17,15 EUR
19,00 % MwSt: 51,01 EUR
Nettobetrag: 693,39 EUR
`);
assert.equal(mixedVat.status, "extracted");
assert.equal(mixedVat.invoice_date, "24.06.2026");
assert.equal(mixedVat.net_amount, 693.39);
assert.equal(mixedVat.vat_amount, 68.16);
assert.equal(mixedVat.gross_amount, 761.55);

const grossAndVat = extractAccountingInvoice(`
Mobilfunk-Rechnung für Mai 2026
Telekom Deutschland GmbH
VIP-Studios / Moritz Feichtmeyer
Rechnungsnummer 34 6018 1300 0881
Datum 15.06.2026
Rechnungsbetrag 60,39 EUR
(davon +19 % USt. auf 48,24 EUR = 9,16 EUR)
`);
assert.equal(grossAndVat.status, "extracted");
assert.equal(grossAndVat.net_amount, 51.23);
assert.equal(grossAndVat.vat_amount, 9.16);
assert.equal(grossAndVat.gross_amount, 60.39);

const booking = extractAccountingInvoice(`
My Private Travel Manager - Buchungs/Listing Bestätigung
VIP-Studios / Moritz Feichtmeyer
Datum: 5. August 2026
Gesamt Preis 36,00 EUR
Summe Steuern/Gebühren 58,55 EUR
Gesamt-Ticketpreis 97,17 EUR
`);
assert.equal(booking.status, "not_invoice");
assert.equal(booking.invoice_character, false);
assert.equal(booking.net_amount, null);
assert.equal(booking.gross_amount, null);

const creditNoteBundle = extractAccountingInvoice(`
INVOICE
Google Ireland Limited
VIP-Studios / Moritz Feichtmeyer
Invoice number GCSAS0000851809Y
Invoice date May 8, 2026
TOTAL EUR 19.88
Credit note
Credit note date May 9, 2026
TOTAL EUR (0.72)
`);
assert.equal(creditNoteBundle.multiple_documents, true);

console.log("accounting invoice parser selftest: ok");
