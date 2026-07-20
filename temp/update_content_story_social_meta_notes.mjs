import "dotenv/config";
import axios from "axios";
import fs from "fs";

function printSafeError(error) {
  console.error(JSON.stringify({
    ok: false,
    code: error?.code || null,
    status: error?.response?.status || null,
    message: error?.message || String(error),
  }, null, 2));
}

process.on("uncaughtException", (error) => {
  printSafeError(error);
  process.exit(1);
});

process.on("unhandledRejection", (error) => {
  printSafeError(error);
  process.exit(1);
});

const taskGid = "1216565691721832";
const updateKey = "rs-story-public-social-meta-theme-hook-2026-07-19";
const agent = JSON.parse(fs.readFileSync("./agents/vip-ai-content.json", "utf8"));
const token = process.env[agent.asanaTokenEnv];

if (!token) throw new Error(`Token fehlt: ${agent.asanaTokenEnv}`);

const asana = axios.create({
  baseURL: "https://app.asana.com/api/1.0",
  headers: {
    Authorization: `Bearer ${token}`,
    "Asana-Enable": "new_rich_text",
  },
});

const replacement = `Update-Key: ${updateKey}
Ergaenzung 2026-07-19: Public-Head-Social-Meta nach frischem Duplikat
Pflicht-Readback vor Abschluss: Bei jeder Story muss der komplette aktuelle oeffentliche HTML-Head der final publizierten oder final previewbaren URL mit Cachebuster geprueft werden. Nicht ausreichend sind Thrive-Editor-Ansicht, eingeloggt sichtbare Vorschau, WP-Rohinhalt, Yoast-REST/yoast_head_json oder eine alte gespeicherte Head-Datei aus einem frueheren Zustand.

Harte Fehler: In og:title, og:description, twitter:title oder twitter:description duerfen keine Template-Werte oder Platzhalter stehen. Dazu zaehlen insbesondere \`Story - Vorlage\`, \`{text6}\` und alle anderen \`{text...}\`-Platzhalter wie \`{text1}\`, \`{text12}\` usw.

Spezifischer bekannter Fingerprint: Wenn vor dem Yoast-Block ein nicht-Yoast/iworks-OG-Block steht mit \`og:title = Story - Vorlage\`, \`og:description = {text6}\`, \`twitter:card = summary\`, \`twitter:title = Story - Vorlage\` oder \`twitter:description = {text6}\`, obwohl WP-Titel, Excerpt, Yoast/Schema/Featured Image sauber sind, ist das ein vorgelagerter OG-Plugin-Transient-/Hook-Fehler. Dieser Zustand darf nie als fertig gelten.

Reparaturfolge: Ein rotes QA-Gate ist ein Reparatur-Trigger, keine Endstation. Vor technischer Eskalation immer zuerst den bewaehrten Standardweg reparieren: echtes Duplikat der Vorlage 147947, nur vorhandene Thrive-Komponenten in-place befuellen, WP-Titel und Excerpt final setzen, Yoast SEO-Title/-Description sowie Yoast OG-/Twitter-Title/-Description und Featured-/OG-/Twitter-Bild final setzen, normaler Thrive-Save/Hydrierung, dann frischen Preview-/Live-Head mit Cachebuster erneut pruefen.

Wichtige Klarstellung: \`thrive_head_scripts\` ist kein Fix fuer einen vorgelagerten falschen Social-Meta-Block. Spaet injizierte korrekte Tags duerfen nicht als Loesung gewertet werden, wenn davor weiterhin falsche og/twitter-Tags stehen. Massgeblich ist der erste/gesamte aktuelle Head-Readback.

Nur wenn der frische Head nach sauberem Standardablauf weiterhin denselben nicht editierbaren vorgelagerten Social-Meta-Block zeigt oder ein echter Server-/WAF-/Rechteblocker vorliegt, gilt es als technischer Theme-/Plugin-/Server-Hook-Blocker. Dann nicht veroeffentlichen, Redaktionsplan nicht auf Online setzen und Asana nicht erledigt schliessen; falls der Beitrag versehentlich live ist, sofort wieder auf Entwurf setzen. Konkrete Evidenz, naechsten Reparaturschritt und technischen Klaerungsbedarf dokumentieren. Endgueltige technische Loesung braucht Admin-/Server-Hook: Duplicate-Original entfernen und postbezogenen iworks-OG-Transient nach Duplikat/Hydrate/Save/Publish loeschen oder den vorgelagerten OG-Generator fuer Storys deaktivieren.`;

const current = await asana.get(`/tasks/${taskGid}`, {
  params: { opt_fields: "gid,name,notes" },
});

const task = current.data.data;
const originalNotes = task.notes || "";
const blockPattern = new RegExp(`\\n\\nUpdate-Key: ${updateKey}[\\s\\S]*?(?=\\n\\nUpdate-Key: |$)`);
let nextNotes;

if (blockPattern.test(originalNotes)) {
  nextNotes = originalNotes.replace(blockPattern, `\n\n${replacement}`);
} else {
  nextNotes = `${originalNotes.replace(/\s+$/u, "")}\n\n${replacement}`;
}

if (nextNotes === originalNotes) {
  console.log(JSON.stringify({ ok: true, changed: false, task: task.name }, null, 2));
  process.exit(0);
}

await asana.put(`/tasks/${taskGid}`, {
  data: { notes: nextNotes },
});

const readback = await asana.get(`/tasks/${taskGid}`, {
  params: { opt_fields: "gid,name,notes" },
});

const notes = readback.data.data.notes || "";
const checks = {
  hasUpdateKey: notes.includes(`Update-Key: ${updateKey}`),
  hasAnyTextPlaceholderRule: notes.includes("alle anderen `{text...}`-Platzhalter"),
  hasStaleHeadWarning: notes.includes("alte gespeicherte Head-Datei"),
  hasThriveHeadScriptsWarning: notes.includes("`thrive_head_scripts` ist kein Fix"),
  hasRepairTrigger: notes.includes("Ein rotes QA-Gate ist ein Reparatur-Trigger"),
  hasIworksFingerprint: notes.includes("nicht-Yoast/iworks-OG-Block"),
  hasDraftFallback: notes.includes("sofort wieder auf Entwurf setzen"),
  hasAdminHookFix: notes.includes("Admin-/Server-Hook"),
};

if (Object.values(checks).some((value) => !value)) {
  throw new Error(`Asana-Readback unvollstaendig: ${JSON.stringify(checks)}`);
}

console.log(JSON.stringify({
  ok: true,
  changed: true,
  task: readback.data.data.name,
  taskGid,
  checks,
}, null, 2));
