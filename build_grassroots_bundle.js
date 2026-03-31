const fs = require("fs");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const YEARLY_MANIFEST_FILE = path.join(__dirname, "yearly_event_exports", "cerebro_yearly_manifest_all.csv");
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

const yearManifestFile = path.join(__dirname, "data", "vendor", "grassroots_year_manifest.js");
const yearChunkDir = path.join(__dirname, "data", "vendor", "grassroots_year_chunks");
const circuitOrder = new Map([
  ["EYBL", 0],
  ["Nike Scholastic", 1],
  ["Nike EYCL", 2],
  ["Nike Extravaganza", 3],
  ["Nike Global Challenge", 4],
  ["Nike Other", 5],
  ["3SSB", 6],
  ["UAA", 7],
  ["Puma", 8],
  ["Other Amateur", 9],
  ["OTE", 10],
  ["Grind Session", 11],
  ["NBPA 100", 12],
  ["Hoophall", 13],
  ["Montverde", 14],
  ["EPL", 15],
  ["General HS", 16],
]);

const GRASSROOTS_HS_CIRCUITS = new Set(["General HS", "Hoophall", "Grind Session", "OTE", "EPL", "Montverde", "Nike Scholastic"].map((value) => normalizeKey(value)));
const GRASSROOTS_AAU_CIRCUITS = new Set(["EYBL", "3SSB", "Nike EYCL", "Nike Extravaganza", "Nike Global Challenge", "Nike Other", "UAA", "NBPA 100", "Puma", "Other Amateur"].map((value) => normalizeKey(value)));
const GRASSROOTS_STATE_ABBREVIATIONS = {
  alabama: "AL",
  alaska: "AK",
  arizona: "AZ",
  arkansas: "AR",
  california: "CA",
  colorado: "CO",
  connecticut: "CT",
  delaware: "DE",
  florida: "FL",
  georgia: "GA",
  hawaii: "HI",
  idaho: "ID",
  illinois: "IL",
  indiana: "IN",
  iowa: "IA",
  kansas: "KS",
  kentucky: "KY",
  louisiana: "LA",
  maine: "ME",
  maryland: "MD",
  massachusetts: "MA",
  michigan: "MI",
  minnesota: "MN",
  mississippi: "MS",
  missouri: "MO",
  montana: "MT",
  nebraska: "NE",
  nevada: "NV",
  "new hampshire": "NH",
  "new jersey": "NJ",
  "new mexico": "NM",
  "new york": "NY",
  "north carolina": "NC",
  "north dakota": "ND",
  ohio: "OH",
  oklahoma: "OK",
  oregon: "OR",
  pennsylvania: "PA",
  "rhode island": "RI",
  "south carolina": "SC",
  "south dakota": "SD",
  tennessee: "TN",
  texas: "TX",
  utah: "UT",
  vermont: "VT",
  virginia: "VA",
  washington: "WA",
  "west virginia": "WV",
  wisconsin: "WI",
  wyoming: "WY",
  district: "DC",
};

const GRASSROOTS_STATE_TEXT_HINTS = [
  [/montverde academy/i, "FL"],
  [/img academy/i, "FL"],
  [/brewster academy/i, "NH"],
  [/long island lutheran/i, "NY"],
  [/spire institute/i, "OH"],
  [/wasatch academy/i, "UT"],
  [/sunrise christian/i, "KS"],
  [/link academy/i, "MO"],
  [/dream city christian/i, "AZ"],
  [/cia bella vista/i, "AZ"],
  [/faith family/i, "TX"],
  [/la lumiere/i, "IN"],
  [/oak hill/i, "VA"],
  [/prolific prep/i, "CA"],
  [/dme academy/i, "FL"],
  [/combine academy/i, "NC"],
  [/az compass/i, "AZ"],
  [/long island/i, "NY"],
  [/mount vernon/i, "WA"],
];

const GRASSROOTS_PHASE_SUFFIX_PATTERNS = [
  /\s*-\s*session\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*chapter\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*live\s+[ivx0-9]+\s*(?:\([^)]*\))?\s*$/i,
  /\s*-\s*(?:playoffs?|pre-season|non-conference|power play-in|elevation conference|championships?|final|peach jam|peach invitational tournament|the eight|earn your stripes invitational|palmetto road championship)\b.*$/i,
  /\s*\((?:playoffs?|pre-season|non-conference|power play-in|elevation conference|championships?|final|live|december|november|october)\)\s*$/i,
];

const GRASSROOTS_NAME_SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "v"]);

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

function getStringValue(value) {
  return value == null ? "" : String(value);
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

function roundGrassrootsPercent(value, digits = 1) {
  if (!Number.isFinite(value)) return "";
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function deriveGrassrootsShotTotals({ gp, ptsPg, fgPct, tpPct, tpmPg, forceNoThree = false }) {
  const games = Number.isFinite(gp) && gp > 0 ? Math.max(1, Math.round(gp)) : 0;
  const points = Number.isFinite(ptsPg) && games > 0 ? Math.max(0, Math.round(ptsPg * games)) : 0;
  const expectedThree = !forceNoThree && Number.isFinite(tpmPg) && games > 0 ? Math.max(0, Math.round(tpmPg * games)) : 0;
  const fgRatio = Number.isFinite(fgPct) && fgPct > 0 ? fgPct / 100 : Number.NaN;
  const tpRatio = Number.isFinite(tpPct) && tpPct > 0 ? tpPct / 100 : Number.NaN;

  const tpmCandidates = [];
  const seen = new Set();
  const maxThree = points > 0 ? Math.floor(points / 3) : expectedThree;
  const pushCandidate = (value) => {
    if (!Number.isFinite(value)) return;
    const candidate = Math.max(0, Math.min(maxThree, Math.round(value)));
    if (seen.has(candidate)) return;
    seen.add(candidate);
    tpmCandidates.push(candidate);
  };

  pushCandidate(expectedThree);
  if (!forceNoThree) {
    for (let delta = 1; delta <= 2; delta += 1) {
      pushCandidate(expectedThree - delta);
      pushCandidate(expectedThree + delta);
    }
  }
  if (!tpmCandidates.length) pushCandidate(0);
  tpmCandidates.sort((left, right) => left - right);

  let best = null;
  tpmCandidates.forEach((tpmCandidate) => {
    const maxFgm = points > 0 ? Math.floor((points - tpmCandidate) / 2) : tpmCandidate;
    for (let fgm = tpmCandidate; fgm <= Math.max(tpmCandidate, maxFgm); fgm += 1) {
      const ftm = points - (2 * fgm) - tpmCandidate;
      if (ftm < 0) continue;
      const twoPm = fgm - tpmCandidate;
      if (twoPm < 0) continue;
      const fga = Number.isFinite(fgRatio) ? Math.max(fgm, Math.round(fgm / fgRatio)) : fgm;
      const tpa = Number.isFinite(tpRatio) ? Math.max(tpmCandidate, Math.round(tpmCandidate / tpRatio)) : tpmCandidate;
      if (tpa > fga) continue;
      const twoPa = fga - tpa;
      if (twoPa < twoPm) continue;
      const calcFgPct = fga > 0 ? round((fgm / fga) * 100, 1) : 0;
      const calcTpPct = tpa > 0 ? round((tpmCandidate / tpa) * 100, 1) : 0;
      const fgErr = Number.isFinite(fgPct) && fgPct > 0 ? Math.abs(calcFgPct - fgPct) : 0;
      const tpErr = Number.isFinite(tpPct) && tpPct > 0 ? Math.abs(calcTpPct - tpPct) : 0;
      const expectedFgm = Number.isFinite(fgPct) && fgPct > 0
        ? Math.max(tpmCandidate, Math.round(points * (fgPct / 100) / 2))
        : Math.max(tpmCandidate, Math.round(points / 4));
      const score = (Math.abs(tpmCandidate - expectedThree) * 1000)
        + (fgErr * 25)
        + (tpErr * 25)
        + Math.abs(fgm - expectedFgm);
      if (!best || score < best.score || (score === best.score && ftm < best.ftm) || (score === best.score && ftm === best.ftm && fgm > best.fgm)) {
        best = { pts: (2 * twoPm) + (3 * tpmCandidate) + ftm, tpm: tpmCandidate, twoPm, twoPa, ftm, fgm, fga, tpa, score };
      }
    }
  });

  if (!best) {
    const fallbackTpm = expectedThree;
    const fallbackTwoPm = Number.isFinite(points) ? Math.max(0, Math.floor((points - (3 * fallbackTpm)) / 2)) : 0;
    const fallbackFtm = Number.isFinite(points) ? Math.max(0, points - (2 * fallbackTwoPm) - (3 * fallbackTpm)) : 0;
    const fallbackFgm = fallbackTwoPm + fallbackTpm;
    const fallbackFga = Number.isFinite(fgRatio) ? Math.max(fallbackFgm, Math.round(fallbackFgm / fgRatio)) : fallbackFgm;
    const fallbackTpa = Number.isFinite(tpRatio) ? Math.max(fallbackTpm, Math.round(fallbackTpm / tpRatio)) : fallbackTpm;
    const fallbackTwoPa = Math.max(0, fallbackFga - fallbackTpa);
    return {
      pts: (2 * fallbackTwoPm) + (3 * fallbackTpm) + fallbackFtm,
      tpm: fallbackTpm,
      twoPm: fallbackTwoPm,
      twoPa: fallbackTwoPa,
      ftm: fallbackFtm,
      fgm: fallbackFgm,
      fga: Math.max(fallbackFgm, fallbackFga),
      tpa: Math.max(fallbackTpm, fallbackTpa),
    };
  }

  return best;
}

function deriveGrassrootsPercentileWeight(gp, min) {
  const games = Number.isFinite(gp) && gp > 0 ? gp : 1;
  const minutes = Number.isFinite(min) && min > 0 ? min : 1;
  return round(games * minutes, 1);
}

function buildGrassrootsCareerKey(row) {
  const playerName = normalizeGrassrootsNameKey(row.player_name || row.player);
  const lastName = getGrassrootsNameLastToken(playerName);
  const heightKey = Number.isFinite(row.height_in) ? Math.round(row.height_in / 2) * 2 : "";
  const weightKey = Number.isFinite(row.weight_lb) ? Math.round(row.weight_lb / 10) * 10 : "";
  const classKey = String(row.class_year || "").trim();
  return [normalizeKey(lastName || playerName), heightKey, weightKey, classKey].join("|");
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

function normalizeGrassrootsNameKey(value) {
  return normalizeKey(value).replace(/\b(jr|sr|ii|iii|iv|v)\b/g, " ").replace(/\s+/g, " ").trim();
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

function normalizeGrassrootsDisplayName(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .replace(/\b(jr|sr|ii|iii|iv|v)\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanGrassrootsTeamDisplayName(value) {
  return String(value ?? "")
    .replace(/\s*\(\s*(?:\d{1,2}U|[A-Z]{2})\s*\)\s*$/i, "")
    .replace(/\s*\b(15U|16U|17U)\b\s*$/i, "")
    .replace(/\s+\(\s*\)\s*$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function inferGrassrootsStateFromText(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  const parenMatch = text.match(/\(([A-Z]{2})\)\s*$/);
  if (parenMatch && isLikelyStateAbbreviation(parenMatch[1])) return parenMatch[1].toUpperCase();
  const normalized = normalizeKey(text);
  for (const [pattern, abbr] of GRASSROOTS_STATE_TEXT_HINTS) {
    if (pattern.test(text) || pattern.test(normalized)) return abbr;
  }
  for (const [stateName, abbr] of Object.entries(GRASSROOTS_STATE_ABBREVIATIONS)) {
    if (normalized === stateName || normalized.startsWith(`${stateName} `) || normalized.includes(` ${stateName} `)) {
      return abbr;
    }
  }
  return "";
}

function isLikelyStateAbbreviation(value) {
  const upper = String(value ?? "").trim().toUpperCase();
  return Object.values(GRASSROOTS_STATE_ABBREVIATIONS).includes(upper);
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

function isGrassrootsEventSessionVariant(text) {
  return /\b(?:session|chapter|live)\b/i.test(String(text ?? ""));
}

function buildGrassrootsEventGroupLabel(text) {
  const normalized = String(text ?? "").trim();
  if (!normalized) return "";
  const classMatch = normalized.match(/^(.+?)(?:\s*-\s*)?(?:class|div(?:ision)?)\s+([0-9]+[A-Z]?|[IVX]+)\b/i);
  if (classMatch) {
    const prefix = classMatch[1]
      .replace(/\s*-\s*$/g, "")
      .replace(/\s*[\(\[\{]+$/g, "")
      .trim();
    if (prefix) return `${prefix} All Classes`;
  }
  return normalized;
}

function normalizeGrassrootsEventInfo(eventName) {
  const raw = String(eventName ?? "").trim();
  const specific = stripGrassrootsEventDecorations(raw) || raw;
  let familySource = specific;
  let changed = true;
  while (changed) {
    changed = false;
    for (const pattern of GRASSROOTS_PHASE_SUFFIX_PATTERNS) {
      const next = familySource.replace(pattern, "").trim();
      if (next !== familySource) {
        familySource = next;
        changed = true;
      }
    }
  }
  familySource = familySource.replace(/\s+/g, " ").trim();
  const mergeKey = normalizeKey(familySource || specific || raw);

  return {
    specific,
    family: buildGrassrootsEventGroupLabel(familySource || specific || raw),
    mergeKey,
    isVariant: isGrassrootsEventSessionVariant(raw) || normalizeKey(specific) !== mergeKey,
  };
}

function buildGrassrootsPlayerClusterKey(row) {
  const playerName = normalizeGrassrootsNameKey(row.player_name || row.player);
  const lastName = getGrassrootsNameLastToken(playerName);
  const heightKey = Number.isFinite(row.height_in) ? Math.round(row.height_in / 2) * 2 : "";
  const weightKey = Number.isFinite(row.weight_lb) ? Math.round(row.weight_lb / 10) * 10 : "";
  const classKey = getStringValue(row.class_year).trim();
  const ageKey = getStringValue(row.age_range || row.level || "").trim();
  return [normalizeKey(lastName || playerName), heightKey, weightKey, classKey, normalizeKey(ageKey)].join("|");
}

function buildGrassrootsTeamClusterKey(row) {
  return normalizeGrassrootsTeamKey(row.team_full || row.team_name || "");
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
    row.state,
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
  const eventMergeKeys = rows.map((row) => row.event_merge_key || row.event_name);
  const playerNames = rows.map((row) => row.player_name);
  const teamNames = rows.map((row) => row.team_name);
  const teamFullNames = rows.map((row) => row.team_full || row.team_name);
  const stateValues = rows.map((row) => row.state).filter(Boolean);
  const positions = rows.map((row) => row.pos).filter(Boolean);
  const uniqueEventNames = Array.from(new Set(eventNames.filter(Boolean)));
  const uniqueEventGroups = Array.from(new Set(eventGroups.filter(Boolean)));
  const uniqueEventSources = Array.from(new Set(eventSources.filter(Boolean)));
  const uniqueEventMergeKeys = Array.from(new Set(eventMergeKeys.filter(Boolean)));
  const uniquePlayerNames = Array.from(new Set(playerNames.filter(Boolean)));
  const uniqueTeamNames = Array.from(new Set(teamNames.filter(Boolean)));
  const uniqueTeamFullNames = Array.from(new Set(teamFullNames.filter(Boolean)));
  const uniqueStates = Array.from(new Set(stateValues.filter(Boolean)));
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

  aggregate["2pm"] = roundGrassrootsCount(aggregate["2pm"]);
  aggregate["2pa"] = roundGrassrootsCount(aggregate["2pa"]);
  aggregate.tpm = roundGrassrootsCount(aggregate.tpm);
  aggregate.tpa = roundGrassrootsCount(aggregate.tpa);
  aggregate.pts = roundGrassrootsCount(aggregate.pts);
  aggregate.fgm = roundGrassrootsCount(aggregate["2pm"] + aggregate.tpm);
  aggregate.fga = roundGrassrootsCount(aggregate["2pa"] + aggregate.tpa);
  aggregate.ftm = roundGrassrootsCount(Math.max(0, aggregate.pts - (2 * aggregate["2pm"]) - (3 * aggregate.tpm)));
  aggregate.fgs = aggregate.fgm;

  const preferredEventName = pickPreferredText(uniqueEventNames) || latest.event_name || "";
  const preferredEventGroup = pickPreferredText(uniqueEventGroups) || latest.event_group || preferredEventName;
  const aliasValues = Array.from(new Set([
    ...uniqueEventNames,
    ...uniqueEventGroups,
    ...uniqueEventSources,
  ].map((value) => String(value ?? "").trim()).filter(Boolean)));
  aggregate.event_name = preferredEventName || preferredEventGroup;
  aggregate.event_group = preferredEventGroup || preferredEventName;
  aggregate.event_raw_name = Array.from(new Set(uniqueEventSources.map((value) => String(value ?? "").trim()).filter(Boolean))).join(" / ");
  aggregate.player_name = pickPreferredText(uniquePlayerNames) || latest.player_name || "";
  aggregate.player = aggregate.player_name;
  aggregate.player_aliases = uniquePlayerNames.length > 1 ? Array.from(new Set(uniquePlayerNames)).join(" / ") : "";
  aggregate.team_name = Array.from(new Set(uniqueTeamNames.map((value) => cleanGrassrootsTeamDisplayName(value)).filter(Boolean))).join(" / ") || latest.team_name || "";
  aggregate.team_full = Array.from(new Set(uniqueTeamFullNames.map((value) => String(value ?? "").trim()).filter(Boolean))).join(" / ") || latest.team_full || latest.team_name || "";
  aggregate.team_aliases = uniqueTeamFullNames.length > 1 ? Array.from(new Set(uniqueTeamFullNames)).join(" / ") : "";
  aggregate.pos = uniquePositions.length ? uniquePositions.join(" / ") : latest.pos || "";
  aggregate.pos_text = aggregate.pos;
  aggregate.event_aliases = aliasValues.join(" / ");
  aggregate.event_merge_key = uniqueEventMergeKeys[0] || latest.event_merge_key || aggregate.event_name || "";
  aggregate.player_search_text = uniquePlayerNames.join(" / ");
  aggregate.team_search_text = uniqueTeamFullNames.join(" / ");
  aggregate.career_player_key = Array.from(new Set(rows.map((row) => String(row.career_player_key || buildGrassrootsCareerKey(row) || "").trim()).filter(Boolean)))[0] || "";
  aggregate.event_total_players = Math.max(...rows.map((row) => Number.isFinite(row.event_total_players) ? row.event_total_players : 0), 0) || "";
  aggregate.page_index = latest.page_index || "";
  aggregate.rank = rows
    .map((row) => Number(row.rank))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right)[0] ?? "";
  aggregate.state = uniqueStates.join(" / ");
  aggregate.gp = Math.round(aggregate.gp);
  aggregate.percentile_weight = deriveGrassrootsPercentileWeight(aggregate.gp, aggregate.min);
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
  aggregate.three_pr = aggregate.fga > 0 ? round((aggregate.tpa / aggregate.fga) * 100, 1) : "";
  aggregate.ftm_fga = aggregate.fga > 0 ? round((aggregate.ftm / aggregate.fga) * 100, 1) : "";
  aggregate.three_pr_plus_ftm_fga = Number.isFinite(aggregate.three_pr) && Number.isFinite(aggregate.ftm_fga)
    ? round(aggregate.three_pr + aggregate.ftm_fga, 1)
    : "";
  aggregate.ast_to = aggregate.tov > 0 ? round(aggregate.ast / aggregate.tov, 2) : "";
  aggregate.blk_pf = aggregate.pf > 0 ? round(aggregate.blk / aggregate.pf, 2) : "";
  aggregate.stocks_pf = aggregate.pf > 0 ? round(aggregate.stocks / aggregate.pf, 2) : "";
  aggregate.tov_per40 = aggregate.min > 0 ? round((aggregate.tov / aggregate.min) * 40, 1) : "";
  aggregate.pf_per40 = aggregate.min > 0 ? round((aggregate.pf / aggregate.min) * 40, 1) : "";
  aggregate.three_pe = aggregate.tpa;
  aggregate.fgs = aggregate.fgm;
  aggregate.rank = aggregate.rank === "" ? null : aggregate.rank;
  return aggregate;
}

function sumGrassrootsGames(rows) {
  return rows.reduce((sum, row) => sum + (Number.isFinite(row.gp) ? row.gp : 0), 0);
}

function mergeGrassrootsRows(rawRows) {
  const grouped = new Map();
  rawRows.forEach((row) => {
    const groupKey = [
      row.season,
      normalizeKey(row.event_merge_key || row.event_name),
      normalizeKey(row.player_cluster_key || buildGrassrootsPlayerClusterKey(row)),
      normalizeGrassrootsTeamKey(row.team_full || row.team_name),
    ].join("|");
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push(row);
  });

  const merged = [];
  grouped.forEach((groupRows) => {
    const coreRows = groupRows.filter((row) => !row.event_variant);
    const variantRows = groupRows.filter((row) => row.event_variant);
    const preferredRows = coreRows.length && variantRows.length
      ? (sumGrassrootsGames(coreRows) >= sumGrassrootsGames(variantRows) ? coreRows : variantRows)
      : groupRows;
    const deduped = [];
    const seen = new Set();
    preferredRows.forEach((row) => {
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
  if (/\bEYCL\b/i.test(text)) return "Nike EYCL";
  if (/scholastic/i.test(text)) return "Nike Scholastic";
  if (/extravaganza/i.test(text)) return "Nike Extravaganza";
  if (/global challenge/i.test(text)) return "Nike Global Challenge";
  if (/\bEYBL\b/i.test(text)) return "EYBL";
  if (/\bNike\b/i.test(text)) return "Nike Other";
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
  if (/17u/i.test(text) && /general hs/i.test(text)) return "Other Amateur";
  return "";
}

function inferCircuit(sourceCircuit, row) {
  const circuit = classifyGrassrootsCircuit(row.event_name) || sourceCircuit || "General HS";
  if (circuit === "General HS" && /17u/i.test(String(row.Team || ""))) return "Other Amateur";
  return circuit;
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
  const rawTeamName = String(row.Team || "").trim();
  const teamDisplayName = cleanGrassrootsTeamDisplayName(rawTeamName);
  const state = inferGrassrootsStateFromText(rawTeamName) || inferGrassrootsStateFromText(eventInfo.family || rawEventName);
  const gp = toNumber(row.Games);
  const mpg = toNumber(row["MIN/G"]);
  const ptsPg = toNumber(row["PTS/G"]);
  const fgPct = toPercentNumber(row["FG%"]);
  const tpmPg = toNumber(row["3PM/G"]);
  const tpPct = toPercentNumber(row["3PT%"]);
  const gpCount = Number.isFinite(gp) ? roundGrassrootsCount(gp) : 0;
  const hasLoggedThreePm = Number.isFinite(tpmPg) && tpmPg > 0;
  const shotTotals = deriveGrassrootsShotTotals({ gp: gpCount, ptsPg, fgPct, tpPct, tpmPg, forceNoThree: !hasLoggedThreePm });
  const pts = shotTotals.pts;
  const tpm = hasLoggedThreePm ? shotTotals.tpm : 0;
  const twoPm = shotTotals.twoPm;
  const ftm = shotTotals.ftm;
  const fgm = shotTotals.fgm;
  const fga = shotTotals.fga;
  const tpa = shotTotals.tpa;
  const twoPa = shotTotals.twoPa;
  const twoPct = twoPa > 0 ? round((twoPm / twoPa) * 100, 1) : "";
  const ftmPg = gpCount > 0 ? round(ftm / gpCount, 1) : "";
  const ftmFga = fga > 0 ? round((ftm / fga) * 100, 1) : "";
  const threePr = fga > 0 ? round((tpa / fga) * 100, 1) : "";
  const threePrPlusFtmFga = Number.isFinite(threePr) && Number.isFinite(ftmFga) ? round(threePr + ftmFga, 1) : "";
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
  const percentileWeight = deriveGrassrootsPercentileWeight(gpCount, min);
  const heightIn = parseHeight(row.HT);
  const weightLb = toNumber(row.WT);
  const season = parseSeason(rawEventName);
  const ageRange = parseAgeRange(rawEventName, row.Team);
  const classYear = inferGrassrootsClassYear(row.Class, season, ageRange);
  const rank = toNumber(row.Rank);
  const playerClusterKey = buildGrassrootsPlayerClusterKey({
    player_name: row.Player || "",
    height_in: heightIn,
    weight_lb: weightLb,
    class_year: classYear,
    state,
    age_range: ageRange,
    level: eventInfo.family || "",
  });
  const careerPlayerKey = buildGrassrootsCareerKey({
    player_name: row.Player || "",
    height_in: heightIn,
    weight_lb: weightLb,
    state,
    team_state: state,
    pos: row.POS || "",
  });
  const playerSearchText = String(row.Player || "").trim();
  const teamSearchText = rawTeamName;
  const circuitLabel = (circuit === "General HS" && /17u/i.test(rawTeamName)) ? "Other Amateur" : circuit;
  const setting = getGrassrootsSettingForCircuit(circuit);
  const eventDisplay = eventInfo.specific || rawEventName || "";
  const eventGroup = eventInfo.family || eventDisplay || rawEventName || "";
  const eventAlias = Array.from(new Set([eventDisplay, eventGroup].map((value) => String(value ?? "").trim()).filter(Boolean))).join(" / ");
  const teamRawName = rawTeamName;

  return {
    season,
    age_range: ageRange,
    circuit: circuitLabel,
    setting,
    event_name: eventDisplay,
    event_group: eventGroup,
    event_raw_name: eventDisplay,
    event_aliases: eventAlias,
    event_merge_key: eventInfo.mergeKey || normalizeKey(eventDisplay),
    event_variant: eventInfo.isVariant,
    player_search_text: playerSearchText,
    team_search_text: teamSearchText,
    player_aliases: "",
    team_aliases: "",
    player_cluster_key: playerClusterKey,
    event_url: row.event_url || "",
    event_total_players: toNumber(row.event_total_players),
    page_index: toNumber(row.page_index),
    rank,
    player_name: row.Player || "",
    team_name: teamDisplayName || rawTeamName,
    team_full: teamRawName,
    state,
    team_state: state,
    career_player_key: careerPlayerKey,
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
    ast_to: tov > 0 ? round(ast / tov, 2) : "",
    blk_pf: pf > 0 ? round(blk / pf, 2) : "",
    stocks_pf: pf > 0 ? round(stocks / pf, 2) : "",
    pf_per40: Number.isFinite(min) && min > 0 ? round((pf / min) * 40, 1) : "",
    tov_per40: Number.isFinite(min) && min > 0 ? round((tov / min) * 40, 1) : "",
    percentile_weight: percentileWeight,
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
  console.warn("No yearly manifest found; grassroots build will be empty.");
}

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
  "team_full",
  "state",
  "team_state",
  "career_player_key",
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
  "percentile_weight",
];

const rowsBySeason = new Map();
rows.forEach((row) => {
  const season = String(row.season ?? "").trim();
  if (!season) return;
  if (!rowsBySeason.has(season)) rowsBySeason.set(season, []);
  rowsBySeason.get(season).push(row);
});

const seasons = Array.from(rowsBySeason.keys()).sort((left, right) => Number(left) - Number(right));
const latestSeason = seasons[seasons.length - 1] || "";
fs.mkdirSync(yearChunkDir, { recursive: true });

const yearManifest = {
  years: seasons,
  latestYear: latestSeason,
  initialYears: latestSeason ? [latestSeason] : [],
  rowCounts: Object.fromEntries(seasons.map((season) => [season, rowsBySeason.get(season).length])),
};

fs.writeFileSync(
  yearManifestFile,
  [
    "// Generated by build_grassroots_bundle.js",
    `// ${new Date().toISOString()}`,
    `window.GRASSROOTS_YEAR_MANIFEST = ${JSON.stringify(yearManifest, null, 2)};`,
    "",
  ].join("\n"),
  "utf8"
);

seasons.forEach((season) => {
  const seasonRows = rowsBySeason.get(season) || [];
  const csvText = [
    columns.join(","),
    ...seasonRows.map((row) => columns.map((column) => csvEscape(row[column])).join(",")),
  ].join("\n");
  const output = [
    "// Generated by build_grassroots_bundle.js",
    `// ${new Date().toISOString()}`,
    `window.GRASSROOTS_YEAR_CSV_CHUNKS = window.GRASSROOTS_YEAR_CSV_CHUNKS || {};`,
    `window.GRASSROOTS_YEAR_CSV_CHUNKS[${JSON.stringify(season)}] = ${JSON.stringify(csvText)};`,
    "",
  ].join("\n");
  fs.writeFileSync(path.join(yearChunkDir, `${season}.js`), output, "utf8");
});

const circuitCounts = rows.reduce((acc, row) => {
  acc[row.circuit] = (acc[row.circuit] || 0) + 1;
  return acc;
}, {});

console.log(`Wrote ${yearManifestFile}`);
console.log(`Wrote ${yearChunkDir}`);
console.log(`Rows: ${rows.length}`);
console.log(JSON.stringify(circuitCounts));
