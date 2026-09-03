import assert from "node:assert/strict";
import {
  composeHtmlWithSignature,
  stripTrailingIdentityFromText
} from "../lib/email-signature-template.js";

const contentHtml = [
  "<html><head><style>.reply{font-size:14px}</style></head><body>",
  '<div class="reply">Hallo Moritz,<br><br>Das ist der Text.<br><br>Viele Gruesse<br>Reise-Stories</div>',
  "</body></html>"
].join("");
const signatureHtml = [
  "<html><head><style>.signature{color:#222}</style></head><body>",
  "<div>TEXT</div>",
  '<div class="signature">Editorial Team<br>Reise-Stories<br><img src="cid:banner"></div>',
  "</body></html>"
].join("");

const composed = composeHtmlWithSignature({
  contentHtml,
  signatureHtml,
  marker: "TEXT",
  trailingIdentities: ["Reise-Stories"]
});

assert.equal(composed.marker_count, 1);
assert.equal(composed.trailing_identity_removed, true);
assert.equal((composed.html.match(/Reise-Stories/g) || []).length, 1);
assert.match(composed.html, /Viele Gruesse/);
assert.match(composed.html, /cid:banner/);
assert.match(composed.html, /\.reply\{font-size:14px\}/);
assert.doesNotMatch(composed.html, /TEXT/);
assert.equal(
  stripTrailingIdentityFromText("Hello\n\nBest regards\nReise-Stories\n", ["Reise-Stories"]),
  "Hello\n\nBest regards"
);

process.stdout.write("email signature template self-test: ok\n");
