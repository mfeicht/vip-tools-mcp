export function selectBoundedEmailText({ plainText = "", htmlText = "", offsetChars = 0, limitChars = 8000 }) {
  if (!Number.isInteger(offsetChars) || offsetChars < 0 ||
      !Number.isInteger(limitChars) || limitChars < 1) {
    throw new Error("Invalid email text window");
  }
  const plain = String(plainText || "").trim();
  const html = String(htmlText || "").trim();
  const text = plain || html;
  const source = plain ? "text/plain" : html ? "text/html" : "none";
  const start = Math.min(offsetChars, text.length);
  const end = Math.min(start + limitChars, text.length);
  return {
    text: text.slice(start, end),
    source,
    total_chars: text.length,
    offset_chars: start,
    end_chars: end,
    has_more: end < text.length,
    coverage_status: !text ? "no_text" : start === 0 && end === text.length ? "complete" : "partial"
  };
}
