import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const port = process.env.EVIDENCE_GATE_TEST_PORT || "3007";
process.env.PORT = port;

const client = new Client({ name: "vip-evidence-gate-selftest", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));

async function call(arguments_) {
  return client.callTool({ name: "asana_comment", arguments: arguments_ });
}

function text(result) {
  return (result.content || []).map((item) => item.text || "").join("\n");
}

await import("../server.js");
await new Promise((resolve) => setTimeout(resolve, 300));
await client.connect(transport);

try {
  const missing = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "completion",
    greeting: "Abschluss",
    dry_run: true
  });

  const missingStatus = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "status",
    greeting: "Technischer Zwischenstand",
    dry_run: true
  });

  const pureQuestion = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "question",
    greeting: "Rueckfrage",
    sections: [{ title: "Benoetigte Angabe", bullets: ["Bitte die gewuenschte Region nennen."] }],
    dry_run: true
  });

  const valid = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "completion",
    greeting: "Abschluss",
    sections: [{ title: "Ergebnis", bullets: ["Aufgabe anhand der sichtbaren Testeingabe abgeschlossen."] }],
    evidence: {
      summary: "Der Kommentar enthaelt keine externen materialen Tatsachen; die Ausfuehrung basiert nur auf der sichtbaren Testeingabe.",
      no_external_factual_claims: true
    },
    dry_run: true
  });

  const mislabeledResult = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "status",
    greeting: "Ergebnis erfolgreich aktualisiert",
    dry_run: true
  });

  const unresolved = await call({
    agent_id: "vip-ai-content",
    task_gid: "1234567890",
    comment_kind: "completion",
    greeting: "Abschluss",
    evidence: {
      summary: "Eine externe Aussage wurde noch nicht abschliessend gegen die Zielquelle verifiziert.",
      sources: ["Testquelle"],
      verified_claims: ["Nur der Testpfad wurde erreicht."],
      unresolved: ["Externe Kernaussage ist ungeklaert."]
    },
    dry_run: true
  });

  const validText = text(valid);
  const report = {
    missing_evidence_blocked: Boolean(missing.isError),
    missing_status_evidence_blocked: Boolean(missingStatus.isError),
    pure_question_without_claims_passed: !pureQuestion.isError,
    valid_evidence_passed: !valid.isError && /"evidence_gate_status"\s*:\s*"ok"/.test(validText),
    valid_comment_has_marker: validText.includes("Evidenz / Verifikation"),
    mislabeled_result_blocked: Boolean(mislabeledResult.isError),
    unresolved_completion_blocked: Boolean(unresolved.isError)
  };

  console.log(JSON.stringify(report));
  process.exit(Object.values(report).every(Boolean) ? 0 : 1);
} finally {
  await client.close().catch(() => {});
}
