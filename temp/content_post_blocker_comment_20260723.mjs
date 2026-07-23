import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const client = new Client({ name: "vip-ai-content-standard-comment", version: "1.0.0" });
const transport = new StreamableHTTPClientTransport(new URL("https://vip-tools-mcp.onrender.com/mcp"));
await client.connect(transport);
const result = await client.callTool({
  name: "asana_comment",
  arguments: {
    agent_id: "vip-ai-content",
    task_gid: "1216781796608871",
    comment_kind: "handoff",
    greeting: "VIP AI-Content Standardlauf – Story-Fortsetzung Redaktionsplan Zeile 8819",
    sections: [
      {
        title: "Aufgabe / Zeile / URL / Keyword",
        paragraphs: [
          "Redaktionsplan!8819, Seitentyp Story, Fokus-Keyword: thai visa. Ziel-URL: https://reise-stories.de/thai-visa-thailand-reisende-2026/."
        ]
      },
      {
        title: "Geänderte Inhalte",
        paragraphs: [
          "Den vorbereiteten sauberen Duplikatpost 149474 für den Reparatur-Readback wiederhergestellt, unter dem finalen Slug geprüft und anschließend wegen des roten Titelbild-Credit-Wrapper-Gates wieder auf draft gesetzt. Der alte Post 149469 bleibt als draft unter dem Alt-Slug thai-visa-thailand-reisende-2026-alt."
        ]
      },
      {
        title: "Readbacks",
        bullets: [
          "WP-REST: Post 149474 ist draft, slug thai-visa-thailand-reisende-2026, Autor-ID 1/vipadmin, Featured Media 149470, Yoast-Titel/Description/OG-Bild gesetzt; Post 149469 ist draft unter dem Alt-Slug.",
          "Ein sparsamer Public-Readback mit Cache-Buster während der QA-Publikation bestätigte Canonical/H1, korrekte og/twitter-Titel und Beschreibungen, /author/vipadmin/, keine {text}-Platzhalter sowie vorhandene Das-Wichtigste-, Empfehlungen- und DETAILS-ZUR-STORY-Marker.",
          "Das harte Readback fand den Titelbild-Credit als nackten Text am Ende des Content-Blocks statt im vorgeschriebenen Thrive-Wrapper. rs_content_qa_gate und der dokumentierte Thrive-Wrapper-Reparaturpfad scheiterten vor dem Seitenzugriff an Chrome Target page/context closed bzw. SIGABRT."
        ]
      },
      {
        title: "Offener Punkt / nächster Schritt",
        paragraphs: [
          "Technischer Blocker: approved Admin-/Thrive-Browser-Runtime startet nicht. Nach Wiederherstellung des Editorpfads den Titelbild-Credit im Original-Wrapper speichern, Public-QA mit Cache-Buster erneut vollständig ausführen und erst dann Redaktionsplan A/B/F/G/H/I schreiben und die Routine abschließen. Keine Sheet-Änderung und kein Routineabschluss in diesem Lauf."
        ]
      }
    ],
    evidence: {
      summary: "Die Thai-Visa-Story wurde aktiv im sicheren Ersatzpfad geprüft; der finale Public-Head ist inhaltlich plausibel, aber der harte Thrive-Credit-Wrapper-Gate bleibt wegen nicht startender Browser-Runtime offen.",
      sources: [
        "Asana-Aufgabe 1216781796608871 und Redaktionsplan!A8819:O8819",
        "WP-REST-Readbacks der Posts 149469 und 149474",
        "Public HTML Cache-Buster-Readback: https://reise-stories.de/thai-visa-thailand-reisende-2026/?rsqa=content-20260723-1645-b",
        "Lokaler QA-Readback: VIP-AI-Workspace/10-Agenten/VIP-AI-Content/temp/2026-07-23__vip-ai-content__asana-1216781796608871__thai-visa-final__public__v01.html"
      ],
      verified_claims: [
        "Post 149474 steht aktuell auf draft; der Redaktionsplan wurde nicht geschrieben.",
        "Der Public-Head enthielt während der QA keine Story - Vorlage- oder {text}-Social-Meta-Platzhalter.",
        "Der Titelbild-Credit-Wrapper wurde nicht erfolgreich repariert, weil der Editorpfad vor dem Seitenzugriff mit SIGABRT abbrach."
      ],
      inferences: [
        "Die inhaltliche Story-Hydrierung des sauberen Duplikats ist für eine erneute Wrapper-Reparatur wiederverwendbar."
      ],
      unresolved: [
        "Admin-/Thrive-Editor-Runtime und der vollständige rs_content_qa_gate-Readback bleiben technisch offen.",
        "Die finale öffentliche URL bleibt bis zum grünen Credit-Wrapper-Gate bewusst offline."
      ]
    },
    mention_user_gid: "1108801330389276",
    mention_text: "Moritz",
    effort_note: "(Verbrauch Token: nicht verfügbar, Zeitaufwand AI: ca. 32 Min., Zeitaufwand Mitarbeiter: 0 Min.)",
    verify_after: true
  }
});
console.log(JSON.stringify(result, null, 2));
await transport.close();
