const fs = require("fs");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const LEGACY_SOURCES = [
  {
    circuit: "EYBL",
    file: path.join(rootDir, "nike_all_event_player_stats_clean.csv"),
    include: (row) => {
      const eventName = String(row.event_name || "");
      return /eybl/i.test(eventName) && !/eycl|scholastic/i.test(eventName);
    },
  },
  {
    circuit: "Nike Other",
    file: path.join(rootDir, "nike_all_event_player_stats_clean.csv"),
    include: (row) => {
      const eventName = String(row.event_name || "");
      return /nike/i.test(eventName) && (!/eybl/i.test(eventName) || /eycl|scholastic/i.test(eventName));
    },
  },
  { circuit: "3SSB", file: path.join(rootDir, "adidas_3ssb_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "UAA", file: path.join(rootDir, "uaa_under_armour_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "Puma", file: path.join(rootDir, "puma_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "OTE", file: path.join(rootDir, "ote_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "Grind Session", file: path.join(rootDir, "grind_session_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "NBPA 100", file: path.join(rootDir, "nbpa_100_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "Hoophall", file: path.join(rootDir, "hoophall_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "Montverde", file: path.join(rootDir, "montverde_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "EPL", file: path.join(rootDir, "epl_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "General HS", file: path.join(rootDir, "showcases_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "General HS", file: path.join(rootDir, "general_hs_all_event_player_stats_clean.csv"), include: () => true },
];
const YEARLY_MANIFEST_FILE = path.join(__dirname, "yearly_event_exports", "cerebro_yearly_manifest.csv");
const RAW_ROW_KEY_COLUMNS = [
  "event_name",
  "event_url",
  "event_total_players",
  "page_index",
  "Rank",
  "Player",
  "Team",
  "POS",
  "Class",
  "HT",
  "WT",
  "Games",
  "MIN/G",
  "RAM",
  "C-RAM",
  "USG%",
  "PSP",
  "PTS/G",
  "FG%",
  "3PE",
  "3PM/G",
  "3PT%",
  "FGS",
  "AST/G",
  "TOV",
  "ATR",
  "REB/G",
  "BLK/G",
  "DSI",
  "STL/G",
  "PF/G",
];

const outputFile = path.join(__dirname, "data", "vendor", "grassroots_all_seasons.js");
const circuitOrder = new Map([
  ["EYBL", 0],
  ["Nike Other", 1],
  ["3SSB", 2],
  ["UAA", 3],
  ["Puma", 4],
  ["OTE", 5],
  ["Grind Session", 6],
  ["NBPA 100", 7],
  ["Hoophall", 8],
  ["Montverde", 9],
  ["EPL", 10],
  ["General HS", 11],
]);

const GRASSROOTS_HS_CIRCUITS = new Set(["General HS", "Hoophall", "Grind Session", "OTE", "EPL", "Montverde"].map((value) => normalizeKey(value)));
const GRASSROOTS_AAU_CIRCUITS = new Set(["EYBL", "3SSB", "Nike Other", "UAA", "NBPA 100", "Puma"].map((value) => normalizeKey(value)));

const GRASSROOTS_PHASE_SUFFIX_PATTERNS = [
  /\s*-\s*session\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*chapter\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*live\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*(?:playoffs?|pre-season|non-conference|power play-in|elevation conference|championships?|final|peach jam|peach invitational tournament|the eight|earn your stripes invitational|palmetto road championship)\b.*$/i,
  /\s*\((?:playoffs?|pre-season|non-conference|power play-in|elevation conference|championships?|final|live|december|november|october)\)\s*$/i,
];

function parseCSV(text) {
  const rows = [];
  let current = "";
  let row = [];
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (char === "\"") {
      if (inQuotes && next === "\"") {
        current += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (!inQuotes && char === ",") {
      row.push(current);
      current = "";
      continue;
    }
    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(current);
      current = "";
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
      continue;
    }
    current += char;
  }

  if (current !== "" || row.length) {
    row.push(current);
    if (row.some((cell) => cell !== "")) rows.push(row);
  }

  if (!rows.length) return [];
  const header = rows[0].map((cell) => cell.trim());
  return rows.slice(1).map((cells) => {
    const out = {};
    header.forEach((column, index) => {
      out[column] = cells[index] ?? "";
    });
    return out;
  });
}

function csvEscape(value) {
  if (value == null) return "";
  const text = typeof value === "number" && Number.isFinite(value) ? String(value) : String(value);
  if (!/[",\n\r]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function toNumber(value) {
  const text = String(value ?? "").trim().replace(/%$/, "");
  if (!text || /^N\/A$/i.test(text)) return "";
  const numeric = Number(text);
  return Number.isFinite(numeric) ? numeric : "";
}

function toPercentNumber(value) {
  const text = String(value ?? "").trim().replace(/%$/, "");
  if (!text || /^N\/A$/i.test(text)) return "";
  const numeric = Number(text);
  return Number.isFinite(numeric) ? numeric : "";
}

function parseHeight(value) {
  const text = String(value ?? "").trim();
  if (!text || /^N\/A$/i.test(text)) return "";
  const match = text.match(/^(\d+)\s*[-' ]\s*(\d{1,2})$/);
  if (!match) return "";
  return Number(match[1]) * 12 + Number(match[2]);
}

function parseSeason(eventName) {
  const text = String(eventName ?? "");
  const match = text.match(/\b(\d{4})\b/);
  return match ? Number(match[1]) : "";
}

function round(value, digits = 1) {
  if (!Number.isFinite(value)) return "";
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function roundGrassrootsCount(value) {
  if (!Number.isFinite(value)) return "";
  return Math.max(0, Math.round(value));
}

function ratio(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) return "";
  return round((numerator / denominator) * 100, 1);
}

function perGameTotal(value, gp) {
  if (!Number.isFinite(value) || !Number.isFinite(gp) || gp <= 0) return "";
  return round(value * gp, 1);
}

function normalizeKey(value) {
  return String(value ?? "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function normalizeGrassrootsTeamKey(value) {
  return normalizeKey(
    String(value ?? "")
      .replace(/,\s*[A-Z]{2}$/i, "")
      .replace(/\s*\(([A-Z]{2})\)$/i, "")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function normalizePosLabel(value) {
  const text = String(value ?? "").toUpperCase();
  if (/\bPOINT GUARD\b/.test(text)) return "PG";
  if (/\bPG\b|PURE PG/.test(text)) return "PG";
  if (/\bSHOOTING GUARD\b/.test(text)) return "SG";
  if (/\bSG\b|SCORING PG|COMBO G/.test(text)) return "SG";
  if (/\bSMALL FORWARD\b/.test(text)) return "SF";
  if (/\bSF\b|WING/.test(text)) return "SF";
  if (/\bPOWER FORWARD\b/.test(text)) return "PF";
  if (/\bPF\b|STRETCH 4/.test(text)) return "PF";
  if (/^\s*(G\/F|F\/G|GUARD\/FORWARD|FORWARD\/GUARD)\s*$/.test(text)) return "G/F";
  if (/^\s*GUARD\s*$/.test(text)) return "G";
  if (/^\s*FORWARD\s*$/.test(text)) return "F";
  if (/^\s*CENTER\s*$/.test(text)) return "C";
  if (/\bC\b/.test(text)) return "C";
  return String(value ?? "").trim();
}

function getGrassrootsSettingForCircuit(circuit) {
  const key = normalizeKey(circuit);
  if (!key) return "";
  if (GRASSROOTS_AAU_CIRCUITS.has(key)) return "AAU";
  if (GRASSROOTS_HS_CIRCUITS.has(key)) return "HS";
  if (/(eybl|3ssb|nike|nbpa|puma|uaa)/i.test(key)) return "AAU";
  return "HS";
}

function parseAgeRange(eventName, teamName) {
  const candidates = [eventName, teamName];
  for (const candidate of candidates) {
    const text = String(candidate ?? "");
    const match = text.match(/\b(\d{1,2}U)\b/i);
    if (match) return match[1].toUpperCase();
  }
  return "17U";
}

function inferGrassrootsClassYear(rawClass, season, ageRange) {
  const numericClass = Number(rawClass);
  if (Number.isFinite(numericClass) && numericClass >= 1000) return numericClass;
  const numericSeason = Number(season);
  const match = String(ageRange ?? "").match(/\b(\d{1,2})U\b/i);
  const ageValue = match ? Number(match[1]) : Number.NaN;
  if (!Number.isFinite(numericSeason) || !Number.isFinite(ageValue) || ageValue <= 0) return "";
  return numericSeason + Math.max(0, 18 - ageValue);
}

function ageSortValue(ageRange) {
  const match = String(ageRange ?? "").match(/\b(\d{1,2})U\b/i);
  return match ? Number(match[1]) : 0;
}

function bucketNumber(value, step) {
  if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return "";
  return Math.round(value / step) * step;
}

function mergeBucketKey(row) {
  if (row.circuit === "General HS") {
    const player = normalizeKey(row.player_name);
    const height = bucketNumber(row.height_in, 2);
    const weight = bucketNumber(row.weight_lb, 5);
    const pos = normalizeKey(row.pos);
    return [row.season, row.age_range, player, height, weight, pos].join("|");
  }
  return [
    row.season,
    row.age_range,
    normalizeKey(row.team_name),
    normalizeKey(row.player_name),
  ].join("|");
}

function firstNonEmpty(group, column) {
  for (const row of group) {
    const value = row[column];
    if (Number.isFinite(value)) return value;
    if (String(value ?? "").trim()) return value;
  }
  return "";
}

function uniqueJoined(group, column, sortValues = false) {
  const values = [];
  const seen = new Set();
  group.forEach((row) => {
    const value = String(row[column] ?? "").trim();
    if (!value || seen.has(value)) return;
    seen.add(value);
    values.push(value);
  });
  if (sortValues) {
    values.sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" }));
  }
  return values.join(" / ");
}

function circuitSortValue(circuitText) {
  return String(circuitText ?? "")
    .split(" / ")
    .map((part) => circuitOrder.get(part.trim()) ?? 99)
    .reduce((min, value) => Math.min(min, value), 99);
}

const GRASSROOTS_NAME_SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "v"]);

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getGrassrootsNameLastToken(name) {
  const tokens = String(name ?? "").trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return "";
  const tail = tokens[tokens.length - 1].replace(/[^\p{L}\p{N}.-]+/gu, "");
  if (tokens.length > 1 && GRASSROOTS_NAME_SUFFIXES.has(tail.toLowerCase())) {
    return tokens[tokens.length - 2].replace(/[^\p{L}\p{N}.-]+/gu, "");
  }
  return tail;
}

function getGrassrootsStatePrefix(text) {
  const normalized = String(text ?? "").trim();
  if (!normalized) return "";
  const beforeClass = normalized.split(/\b(?:Class|DIV|Division)\b/i)[0].trim();
  const beforeParen = beforeClass.split(/\s*\(/)[0].trim();
  const beforeDash = beforeParen.split(/\s+-\s+/)[0].trim();
  return beforeDash.replace(/\s+/g, " ").trim();
}

function stripGrassrootsEventDecorations(text) {
  return String(text ?? "")
    .replace(/^\s*\d{4}(?:-\d{2})?\s*/g, "")
    .replace(/\s*\((?:boys?|girls?|men|women)\)\s*/gi, " ")
    .replace(/\b(?:boys?|girls?)\b/gi, " ")
    .replace(/\b\d{4}(?:-\d{2})?\b/g, " ")
    .replace(/^\s*\(\s*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\s*\)\s*/i, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeGrassrootsEventInfo(eventName) {
  const raw = String(eventName ?? "").trim();
  let specific = stripGrassrootsEventDecorations(raw);

  let changed = true;
  while (changed) {
    changed = false;
    for (const pattern of GRASSROOTS_PHASE_SUFFIX_PATTERNS) {
      const next = specific.replace(pattern, "").trim();
      if (next !== specific) {
        specific = next;
        changed = true;
      }
    }
  }
  specific = specific.replace(/\s+/g, " ").trim();

  const circuit = classifyGrassrootsCircuit(specific || raw) || "General HS";
  const statePrefix = getGrassrootsStatePrefix(specific);
  const eventGroup = statePrefix && /\b(?:Class|DIV|Division)\b/i.test(specific)
    ? `${statePrefix} All Classes`
    : specific;

  return {
    specific: specific || raw,
    family: eventGroup,
    circuit,
  };
}

function buildGrassrootsPlayerClusterKey(row) {
  const lastName = getGrassrootsNameLastToken(row.player_name || row.player);
  const heightKey = Number.isFinite(row.height_in) ? Math.round(row.height_in / 2) * 2 : "";
  const weightKey = Number.isFinite(row.weight_lb) ? Math.round(row.weight_lb / 10) * 10 : "";
  return [normalizeKey(lastName), heightKey, weightKey].join("|");
}

function buildGrassrootsTeamClusterKey(row) {
  return normalizeGrassrootsTeamKey(row.team_name || row.team_full || "");
}

function pickPreferredText(values) {
  return Array.from(new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean)))
    .sort((left, right) => {
      const lengthDiff = right.length - left.length;
      if (lengthDiff) return lengthDiff;
      return left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" });
    })[0] || "";
}

function buildGrassrootsStatSignature(row) {
  return [
    row.season,
    row.age_range,
    row.level,
    row.circuit,
    row.gp,
    row.min,
    row.mpg,
    row.pts,
    row.pts_pg,
    row.fgm,
    row.fga,
    row.fg_pct,
    row["2pm"],
    row["2pa"],
    row["2p_pct"],
    row.tpm,
    row.tpa,
    row.tpm_pg,
    row.tpa_pg,
    row.ftm,
    row.ftm_pg,
    row.ftm_fga,
    row.trb,
    row.ast,
    row.tov,
    row.stl,
    row.blk,
    row.pf,
    row.ram,
    row.c_ram,
    row.usg_pct,
    row.psp,
    row.atr,
    row.dsi,
  ].map((value) => String(value ?? "")).join("\u0001");
}

function aggregateGrassrootsRowGroup(groupRows) {
  const rows = groupRows.slice();
  if (!rows.length) return {};
  const latest = rows[0];
  const eventNames = rows.map((row) => row.event_name);
  const eventGroups = rows.map((row) => row.event_group);
  const eventSources = rows.map((row) => row.event_raw_name || row.event_name);
  const playerNames = rows.map((row) => row.player_name);
  const teamNames = rows.map((row) => row.team_name);
  const positions = rows.map((row) => row.pos).filter(Boolean);
  const uniqueEventNames = Array.from(new Set(eventNames.filter(Boolean)));
  const uniqueEventGroups = Array.from(new Set(eventGroups.filter(Boolean)));
  const uniqueEventSources = Array.from(new Set(eventSources.filter(Boolean)));
  const uniquePlayerNames = Array.from(new Set(playerNames.filter(Boolean)));
  const uniqueTeamNames = Array.from(new Set(teamNames.filter(Boolean)));
  const uniquePositions = Array.from(new Set(positions.map((value) => normalizePosLabel(value)).filter(Boolean)));

  const sumColumns = [
    "gp",
    "min",
    "pts",
    "fgm",
    "fga",
    "2pm",
    "2pa",
    "tpm",
    "tpa",
    "ftm",
    "trb",
    "ast",
    "tov",
    "stl",
    "blk",
    "pf",
    "stocks",
  ];
  const aggregate = latest ? Object.create(latest) : {};
  sumColumns.forEach((column) => {
    const total = rows.reduce((sum, row) => sum + (Number.isFinite(row[column]) ? row[column] : 0), 0);
    aggregate[column] = column === "min" ? round(total, 1) : roundGrassrootsCount(total);
  });

  aggregate.event_name = pickPreferredText(uniqueEventNames) || latest.event_name || "";
  aggregate.event_group = pickPreferredText(uniqueEventGroups) || aggregate.event_name || "";
  aggregate.event_raw_name = uniqueEventSources.join(" / ");
  aggregate.player_name = pickPreferredText(uniquePlayerNames) || latest.player_name || "";
  aggregate.player = aggregate.player_name;
  aggregate.player_aliases = uniquePlayerNames.length > 1 ? uniquePlayerNames.join(" / ") : "";
  aggregate.team_name = uniqueTeamNames.join(" / ") || latest.team_name || "";
  aggregate.team_full = aggregate.team_name;
  aggregate.team_aliases = uniqueTeamNames.length > 1 ? uniqueTeamNames.join(" / ") : "";
  aggregate.pos = uniquePositions.length ? uniquePositions.join(" / ") : latest.pos || "";
  aggregate.pos_text = aggregate.pos;
  aggregate.event_aliases = uniqueEventSources.length > 1 ? uniqueEventSources.join(" / ") : "";
  aggregate.player_search_text = uniquePlayerNames.join(" / ");
  aggregate.team_search_text = uniqueTeamNames.join(" / ");
  aggregate.event_total_players = Math.max(...rows.map((row) => Number.isFinite(row.event_total_players) ? row.event_total_players : 0), 0) || "";
  aggregate.page_index = latest.page_index || "";
  aggregate.rank = rows
    .map((row) => Number(row.rank))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right)[0] ?? "";
  aggregate.gp = Math.round(aggregate.gp);
  aggregate.pts_pg = aggregate.gp > 0 ? round(aggregate.pts / aggregate.gp, 2) : "";
  aggregate.mpg = aggregate.gp > 0 ? round(aggregate.min / aggregate.gp, 1) : "";
  aggregate.tpm_pg = aggregate.gp > 0 ? round(aggregate.tpm / aggregate.gp, 1) : "";
  aggregate.tpa_pg = aggregate.gp > 0 ? round(aggregate.tpa / aggregate.gp, 1) : "";
  aggregate.ftm_pg = aggregate.gp > 0 ? round(aggregate.ftm / aggregate.gp, 1) : "";
  aggregate.trb_pg = aggregate.gp > 0 ? round(aggregate.trb / aggregate.gp, 1) : "";
  aggregate.ast_pg = aggregate.gp > 0 ? round(aggregate.ast / aggregate.gp, 1) : "";
  aggregate.tov_pg = aggregate.gp > 0 ? round(aggregate.tov / aggregate.gp, 1) : "";
  aggregate.stl_pg = aggregate.gp > 0 ? round(aggregate.stl / aggregate.gp, 1) : "";
  aggregate.blk_pg = aggregate.gp > 0 ? round(aggregate.blk / aggregate.gp, 1) : "";
  aggregate.pf_pg = aggregate.gp > 0 ? round(aggregate.pf / aggregate.gp, 1) : "";
  aggregate.stocks_pg = aggregate.gp > 0 ? round(aggregate.stocks / aggregate.gp, 1) : "";
  aggregate.fg_pct = aggregate.fga > 0 ? round((aggregate.fgm / aggregate.fga) * 100, 1) : "";
  aggregate["2p_pct"] = aggregate["2pa"] > 0 ? round((aggregate["2pm"] / aggregate["2pa"]) * 100, 1) : "";
  aggregate.tp_pct = aggregate.tpa > 0 ? round((aggregate.tpm / aggregate.tpa) * 100, 1) : "";
  aggregate.three_pr = aggregate.fga > 0 ? round(aggregate.tpa / aggregate.fga, 3) : "";
  aggregate.ftm_fga = aggregate.fga > 0 ? round(aggregate.ftm / aggregate.fga, 3) : "";
  aggregate.three_pr_plus_ftm_fga = Number.isFinite(aggregate.three_pr) && Number.isFinite(aggregate.ftm_fga)
    ? round(aggregate.three_pr + aggregate.ftm_fga, 3)
    : "";
  aggregate.three_pe = aggregate.tpa;
  aggregate.fgs = aggregate.fgm;
  aggregate.rank = aggregate.rank === "" ? null : aggregate.rank;
  return aggregate;
}

function mergeGrassrootsRows(rawRows) {
  const grouped = new Map();
  rawRows.forEach((row) => {
    const groupKey = [
      row.season,
      normalizeKey(row.event_name),
      normalizeKey(row.player_cluster_key || buildGrassrootsPlayerClusterKey(row)),
      normalizeGrassrootsTeamKey(row.team_name),
    ].join("|");
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push(row);
  });

  const merged = [];
  grouped.forEach((groupRows) => {
    const deduped = [];
    const seen = new Set();
    groupRows.forEach((row) => {
      const signature = buildGrassrootsStatSignature(row);
      if (seen.has(signature)) return;
      seen.add(signature);
      deduped.push(row);
    });
    merged.push(deduped.length === 1 ? deduped[0] : aggregateGrassrootsRowGroup(deduped));
  });
  return merged;
}

function classifyGrassrootsCircuit(eventName) {
  const text = String(eventName ?? "");
  if (/\bNike\b/i.test(text)) {
    if (/\bEYBL\b/i.test(text) && !/eycl|scholastic/i.test(text)) return "EYBL";
    return "Nike Other";
  }
  if (/3SSB|adidas/i.test(text)) return "3SSB";
  if (/nxt\s*pro/i.test(text)) return "Puma";
  if (/\bUAA\b|under armour/i.test(text)) return "UAA";
  if (/puma/i.test(text)) return "Puma";
  if (/overtime elite|\bote\b/i.test(text)) return "OTE";
  if (/grind session/i.test(text)) return "Grind Session";
  if (/nbpa.*100|top 100 camp/i.test(text)) return "NBPA 100";
  if (/hoophall|hoop hall/i.test(text)) return "Hoophall";
  if (/montverde/i.test(text)) return "Montverde";
  if (/elite prep league|\bepl\b/i.test(text)) return "EPL";
  return "";
}

function inferCircuit(sourceCircuit, row) {
  return classifyGrassrootsCircuit(row.event_name) || sourceCircuit || "General HS";
}

function loadManifestSourceFiles() {
  if (!fs.existsSync(YEARLY_MANIFEST_FILE)) return [];
  const manifestRows = parseCSV(fs.readFileSync(YEARLY_MANIFEST_FILE, "utf8"));
  const files = [];
  const seen = new Set();
  manifestRows.forEach((row) => {
    const file = String(row.output_path ?? "").trim();
    if (!file) return;
    const normalized = path.normalize(file);
    if (seen.has(normalized)) return;
    seen.add(normalized);
    if (fs.existsSync(normalized)) files.push(normalized);
  });
  return files;
}

function buildGrassrootsRowKey(row, circuit) {
  return [circuit, ...RAW_ROW_KEY_COLUMNS.map((column) => String(row[column] ?? ""))].join("\u0001");
}

function loadGrassrootsRowsFromFile(file, circuitResolver, seenRowKeys, filterFn = () => true) {
  if (!fs.existsSync(file)) return 0;
  const parsed = parseCSV(fs.readFileSync(file, "utf8"));
  let added = 0;
  parsed.forEach((row) => {
    if (!filterFn(row)) return;
    const circuit = circuitResolver(row);
    if (!circuit) return;
    const rowKey = buildGrassrootsRowKey(row, circuit);
    if (seenRowKeys.has(rowKey)) return;
    seenRowKeys.add(rowKey);
    rawRows.push(mapRow(row, circuit));
    added += 1;
  });
  return added;
}

function mapRow(row, circuit) {
  const rawEventName = row.event_name || "";
  const eventInfo = normalizeGrassrootsEventInfo(rawEventName);
  const gp = toNumber(row.Games);
  const mpg = toNumber(row["MIN/G"]);
  const ptsPg = toNumber(row["PTS/G"]);
  const fgPct = toPercentNumber(row["FG%"]);
  const tpmPg = toNumber(row["3PM/G"]);
  const tpPct = toPercentNumber(row["3PT%"]);
  const gpCount = Number.isFinite(gp) ? roundGrassrootsCount(gp) : 0;
  const pts = Number.isFinite(ptsPg) ? roundGrassrootsCount(ptsPg * gpCount) : 0;
  let tpm = Number.isFinite(tpmPg) ? roundGrassrootsCount(tpmPg * gpCount) : 0;
  if (pts >= 0 && tpm * 3 > pts) tpm = Math.floor(pts / 3);
  let twoPm = Number.isFinite(pts) ? Math.max(0, Math.floor((pts - (3 * tpm)) / 2)) : 0;
  const ftm = pts - (2 * twoPm) - (3 * tpm);
  let fgm = twoPm + tpm;
  let tpa = Number.isFinite(tpPct) && tpPct > 0 ? roundGrassrootsCount(tpm / (tpPct / 100)) : tpm;
  if (tpa < 0) tpa = 0;
  let fga = Number.isFinite(fgPct) && fgPct > 0 ? roundGrassrootsCount(fgm / (fgPct / 100)) : fgm;
  if (fga < fgm) fga = fgm;
  if (tpa > fga) fga = tpa;
  const twoPa = Math.max(0, fga - tpa);
  const twoPct = twoPa > 0 ? round((twoPm / twoPa) * 100, 1) : "";
  const ftmPg = gpCount > 0 ? round(ftm / gpCount, 1) : "";
  const ftmFga = fga > 0 ? round(ftm / fga, 3) : "";
  const threePr = fga > 0 ? round(tpa / fga, 3) : "";
  const threePrPlusFtmFga = Number.isFinite(threePr) && Number.isFinite(ftmFga) ? round(threePr + ftmFga, 3) : "";
  const trbPg = toNumber(row["REB/G"]);
  const astPg = toNumber(row["AST/G"]);
  const tovPg = toNumber(row.TOV);
  const stlPg = toNumber(row["STL/G"]);
  const blkPg = toNumber(row["BLK/G"]);
  const pfPg = toNumber(row["PF/G"]);
  const trb = roundGrassrootsCount(perGameTotal(trbPg, gpCount));
  const ast = roundGrassrootsCount(perGameTotal(astPg, gpCount));
  const tov = roundGrassrootsCount(perGameTotal(tovPg, gpCount));
  const stl = roundGrassrootsCount(perGameTotal(stlPg, gpCount));
  const blk = roundGrassrootsCount(perGameTotal(blkPg, gpCount));
  const pf = roundGrassrootsCount(perGameTotal(pfPg, gpCount));
  const stocks = Number.isFinite(stl) && Number.isFinite(blk) ? roundGrassrootsCount(stl + blk) : "";
  const stocksPg = Number.isFinite(stlPg) && Number.isFinite(blkPg) ? round(stlPg + blkPg, 1) : "";
  const min = Number.isFinite(mpg) && Number.isFinite(gp) ? round(mpg * gp, 1) : "";
  const heightIn = parseHeight(row.HT);
  const weightLb = toNumber(row.WT);
  const season = parseSeason(rawEventName);
  const ageRange = parseAgeRange(rawEventName, row.Team);
  const classYear = inferGrassrootsClassYear(row.Class, season, ageRange);
  const rank = toNumber(row.Rank);
  const teamSearchText = String(row.Team || "").trim();
  const playerSearchText = String(row.Player || "").trim();
  const setting = getGrassrootsSettingForCircuit(circuit);

  return {
    season,
    age_range: ageRange,
    circuit,
    setting,
    event_name: eventInfo.specific || rawEventName || "",
    event_group: eventInfo.family || eventInfo.specific || rawEventName || "",
    event_raw_name: rawEventName,
    event_aliases: "",
    player_search_text: playerSearchText,
    team_search_text: teamSearchText,
    player_aliases: "",
    team_aliases: "",
    event_url: row.event_url || "",
    event_total_players: toNumber(row.event_total_players),
    page_index: toNumber(row.page_index),
    rank,
    player_name: row.Player || "",
    team_name: row.Team || "",
    pos: row.POS || "",
    class_year: classYear,
    height_in: heightIn,
    weight_lb: weightLb,
    gp,
    min,
    mpg,
    ram: toNumber(row.RAM),
    c_ram: toNumber(row["C-RAM"]),
    usg_pct: toNumber(row["USG%"]),
    psp: toNumber(row.PSP),
    pts,
    pts_pg: ptsPg,
    fgs: fgm,
    fgm,
    fga,
    fg_pct: fga > 0 ? round((fgm / fga) * 100, 1) : fgPct,
    "2pm": twoPm,
    "2pa": twoPa,
    "2p_pct": twoPct,
    tpm,
    tpa,
    tpm_pg: tpmPg,
    tpa_pg: gpCount > 0 ? round(tpa / gpCount, 1) : "",
    ftm,
    ftm_pg: ftmPg,
    ftm_fga: ftmFga,
    three_pr: threePr,
    three_pr_plus_ftm_fga: threePrPlusFtmFga,
    tp_pct: tpa > 0 ? round((tpm / tpa) * 100, 1) : tpPct,
    three_pe: toNumber(row["3PE"]),
    ast,
    ast_pg: astPg,
    tov,
    tov_pg: tovPg,
    atr: toNumber(row.ATR),
    trb,
    trb_pg: trbPg,
    blk,
    blk_pg: blkPg,
    dsi: toNumber(row.DSI),
    stl,
    stl_pg: stlPg,
    pf,
    pf_pg: pfPg,
    stocks,
    stocks_pg: stocksPg,
  };
}

  const rawRows = [];
const seenRowKeys = new Set();
const manifestSourceFiles = loadManifestSourceFiles();

if (manifestSourceFiles.length) {
  manifestSourceFiles.forEach((file) => {
    loadGrassrootsRowsFromFile(file, (row) => inferCircuit("", row), seenRowKeys);
  });
} else {
  console.warn("No yearly manifest found; falling back to legacy grassroots sources.");
}

LEGACY_SOURCES.forEach(({ circuit, file, include }) => {
  loadGrassrootsRowsFromFile(file, (row) => circuit || inferCircuit("", row), seenRowKeys, include);
});

const rows = mergeGrassrootsRows(rawRows);

rows.sort((left, right) => {
  const yearDiff = Number(right.season || 0) - Number(left.season || 0);
  if (yearDiff) return yearDiff;
  const ageDiff = ageSortValue(right.age_range) - ageSortValue(left.age_range);
  if (ageDiff) return ageDiff;
  const circuitDiff = circuitSortValue(left.circuit) - circuitSortValue(right.circuit);
  if (circuitDiff) return circuitDiff;
  const rankDiff = Number(left.rank || 0) - Number(right.rank || 0);
  if (rankDiff) return rankDiff;
  const teamDiff = String(left.team_name).localeCompare(String(right.team_name), undefined, { numeric: true, sensitivity: "base" });
  if (teamDiff) return teamDiff;
  return String(left.player_name).localeCompare(String(right.player_name), undefined, { numeric: true, sensitivity: "base" });
});

const aliasNoteRows = rows.filter((row) => row.event_aliases || row.player_aliases || row.team_aliases);
console.log(`Merged ${rawRows.length} raw rows into ${rows.length} grouped rows (${aliasNoteRows.length} alias-note rows).`);

  const columns = [
  "season",
  "age_range",
  "level",
  "circuit",
  "setting",
  "event_name",
  "event_group",
  "event_raw_name",
  "event_aliases",
  "player_search_text",
  "team_search_text",
  "player_aliases",
  "team_aliases",
  "event_url",
  "event_total_players",
  "page_index",
  "rank",
  "player_name",
  "team_name",
  "pos",
  "class_year",
  "height_in",
  "weight_lb",
  "gp",
  "min",
  "mpg",
  "ram",
  "c_ram",
  "usg_pct",
  "psp",
  "pts",
  "pts_pg",
  "fgs",
  "fgm",
  "fga",
  "fg_pct",
  "2pm",
  "2pa",
  "2p_pct",
  "tpm",
  "tpa",
  "tpm_pg",
  "tpa_pg",
  "ftm",
  "ftm_pg",
  "ftm_fga",
  "three_pr",
  "three_pr_plus_ftm_fga",
  "tp_pct",
  "three_pe",
  "ast",
  "ast_pg",
  "tov",
  "tov_pg",
  "atr",
  "trb",
  "trb_pg",
  "blk",
  "blk_pg",
  "dsi",
  "stl",
  "stl_pg",
  "pf",
  "pf_pg",
  "blk_pf",
  "stocks_pf",
  "stocks",
  "stocks_pg",
];

const csvText = [
  columns.join(","),
  ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(",")),
].join("\n");

fs.writeFileSync(outputFile, `window.GRASSROOTS_ALL_CSV = ${JSON.stringify(csvText)};\n`, "utf8");

const circuitCounts = rows.reduce((acc, row) => {
  acc[row.circuit] = (acc[row.circuit] || 0) + 1;
  return acc;
}, {});

console.log(`Wrote ${outputFile}`);
console.log(`Rows: ${rows.length}`);
console.log(JSON.stringify(circuitCounts));
