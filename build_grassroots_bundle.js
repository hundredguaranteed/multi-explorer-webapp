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
const scopeBundleDir = path.join(__dirname, "data", "vendor", "grassroots_scope_bundles");
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
const GRASSROOTS_ADJ_BPM_LG_2P = 0.51;
const GRASSROOTS_ADJ_BPM_LG_FT = 0.77;
const GRASSROOTS_ADJ_BPM_BAD_3P = 0.32;
const GRASSROOTS_ADJ_BPM_POSITION_IN = {
  PG: 74,
  SG: 76,
  SF: 78,
  PF: 80,
  C: 83,
};
const GRASSROOTS_ADJ_BPM_ROLE_ADJ = {
  PG: -0.5,
  "PG/SG": -0.375,
  SG: -0.25,
  SF: 0,
  "SF/PF": 0.15,
  PF: 0.3,
  C: 0.6,
};

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

function sanitizeGrassrootsPosValue(value) {
  const text = String(value ?? "").trim().toUpperCase();
  if (!text) return "";
  if (isGrassrootsPosPlaceholder(text)) return "";
  return normalizePosLabel(text);
}

function csvEscapeGrassrootsValue(column, value) {
  if (column === "pos" || column === "pos_text") {
    return csvEscape(sanitizeGrassrootsPosValue(value));
  }
  return csvEscape(value);
}

function isGrassrootsPosPlaceholder(value) {
  const text = String(value ?? "").trim().toUpperCase();
  return Boolean(text) && text.replace(/[^A-Z]/g, "") === "NA";
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
  const match = text.match(/\b(20\d{2})(?:\s*[-/]\s*(\d{2}|20\d{2}))?\b/);
  if (match) {
    const startYear = Number(match[1]);
    const endYear = match[2];
    if (!endYear) return startYear;
    if (endYear.length === 4) return Number(endYear);
    const inferredEnd = Number(`${match[1].slice(0, 2)}${endYear}`);
    return Number.isFinite(inferredEnd) ? inferredEnd : startYear;
  }
  const years = text.match(/\b20\d{2}\b/g);
  return years?.length ? Number(years[years.length - 1]) : "";
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

function firstFinite(...values) {
  for (const value of values) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric;
  }
  return Number.NaN;
}

function getMinutesValue(row) {
  if (typeof row?.min === "number" && Number.isFinite(row.min)) return row.min;
  if (typeof row?.mpg === "number" && typeof row?.gp === "number" && Number.isFinite(row.mpg) && Number.isFinite(row.gp)) {
    return row.mpg * row.gp;
  }
  return 0;
}

function compareFilterValues(left, right) {
  return String(left ?? "").localeCompare(String(right ?? ""), undefined, { numeric: true, sensitivity: "base" });
}

function compareYears(left, right) {
  const leftYear = Number(String(left ?? "").match(/\d{4}/)?.[0] || 0);
  const rightYear = Number(String(right ?? "").match(/\d{4}/)?.[0] || 0);
  if (leftYear !== rightYear) return rightYear - leftYear;
  return compareFilterValues(right, left);
}

function normalizeGrassrootsPercentRate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return Number.NaN;
  return numeric > 1.5 ? (numeric / 100) : numeric;
}

function normalizeClassValue(value) {
  const text = getStringValue(value).trim();
  if (!text) return "";
  const lower = text.toLowerCase();
  if (lower.startsWith("fr")) return "Fr";
  if (lower.startsWith("so")) return "So";
  if (lower.startsWith("jr")) return "Jr";
  if (lower.startsWith("sr")) return "Sr";
  if (lower.startsWith("gr")) return "Gr";
  return text;
}

function getGrassrootsAdjBpmPositionInfo(heightIn, positionText) {
  const position = normalizePosLabel(positionText);
  const canonicalIn = GRASSROOTS_ADJ_BPM_POSITION_IN[position];
  const roleAdj = GRASSROOTS_ADJ_BPM_ROLE_ADJ[position];
  const hasHeight = Number.isFinite(heightIn);
  const hasPosition = Boolean(position);

  if (hasHeight && hasPosition && Number.isFinite(canonicalIn) && Number.isFinite(roleAdj)) {
    return {
      effectiveIn: (heightIn * 0.65) + (canonicalIn * 0.35),
      roleAdj,
    };
  }

  if (hasHeight && !hasPosition) {
    if (heightIn <= 72) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ.PG };
    if (heightIn <= 74) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ["PG/SG"] };
    if (heightIn <= 76) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ.SG };
    if (heightIn <= 78) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ.SF };
    if (heightIn <= 80) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ["SF/PF"] };
    if (heightIn <= 82) return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ.PF };
    return { effectiveIn: heightIn, roleAdj: GRASSROOTS_ADJ_BPM_ROLE_ADJ.C };
  }

  if (hasPosition) {
    return {
      effectiveIn: Number.isFinite(canonicalIn) ? canonicalIn : 78,
      roleAdj: Number.isFinite(roleAdj) ? roleAdj : 0,
    };
  }

  return { effectiveIn: 78, roleAdj: 0 };
}

function getGrassrootsAdjBpmReboundSplit(positionText) {
  const position = normalizePosLabel(positionText);
  if (["PG", "SG", "G"].includes(position)) return 0.25;
  if (position === "G/F") return 0.30;
  if (position === "SF") return 0.31;
  if (position === "F") return 0.32;
  if (position === "PF") return 0.35;
  if (position === "C") return 0.40;
  return 0.30;
}

function calculateGrassrootsAdjBpm(row) {
  const minutes = getMinutesValue(row);
  const scale = Number.isFinite(minutes) && minutes > 0 ? (40 / minutes) : Number.NaN;
  const pts = firstFinite(row.pts_per40, Number.isFinite(row.pts) && Number.isFinite(scale) ? row.pts * scale : Number.NaN, Number.NaN);
  const twoPa = firstFinite(row.two_pa_per40, Number.isFinite(row["2pa"]) && Number.isFinite(scale) ? row["2pa"] * scale : Number.NaN, Number.NaN);
  const threePa = firstFinite(row.three_pa_per40, Number.isFinite(row.tpa) && Number.isFinite(scale) ? row.tpa * scale : Number.NaN, Number.NaN);
  const ast = firstFinite(row.ast_per40, Number.isFinite(row.ast) && Number.isFinite(scale) ? row.ast * scale : Number.NaN, Number.NaN);
  const tov = firstFinite(row.tov_per40, Number.isFinite(row.tov) && Number.isFinite(scale) ? row.tov * scale : Number.NaN, Number.NaN);
  const stl = firstFinite(row.stl_per40, Number.isFinite(row.stl) && Number.isFinite(scale) ? row.stl * scale : Number.NaN, Number.NaN);
  const blk = firstFinite(row.blk_per40, Number.isFinite(row.blk) && Number.isFinite(scale) ? row.blk * scale : Number.NaN, Number.NaN);
  const pf = firstFinite(row.pf_per40, Number.isFinite(row.pf) && Number.isFinite(scale) ? row.pf * scale : Number.NaN, Number.NaN);
  const trb = firstFinite(row.trb_per40, Number.isFinite(row.trb) && Number.isFinite(scale) ? row.trb * scale : Number.NaN, Number.NaN);
  const positionInfo = getGrassrootsAdjBpmPositionInfo(firstFinite(row.height_in, row.inches, Number.NaN), row.pos || row.pos_text);
  const reboundShare = getGrassrootsAdjBpmReboundSplit(row.pos || row.pos_text);
  const orb = Number.isFinite(trb) ? trb * reboundShare : 0;
  const drb = Number.isFinite(trb) ? Math.max(0, trb - orb) : 0;
  const fga = Number.isFinite(twoPa) && Number.isFinite(threePa) ? (twoPa + threePa) : Number.NaN;
  const twoPpct = normalizeGrassrootsPercentRate(firstFinite(row["2p_pct"], Number.NaN));
  const threePpct = normalizeGrassrootsPercentRate(firstFinite(row.tp_pct, row["3p_pct"], Number.NaN));
  const ftPct = normalizeGrassrootsPercentRate(firstFinite(row.ft_pct, Number.NaN));
  const ftmPer40 = Number.isFinite(row.ftm) && Number.isFinite(scale) ? row.ftm * scale : Number.NaN;
  const ftAttempts = Number.isFinite(ftPct) && Number.isFinite(ftmPer40) && ftPct > 0 ? (ftmPer40 / ftPct) : Number.NaN;
  const eff2p = Number.isFinite(twoPpct) && Number.isFinite(twoPa)
    ? ((twoPpct - GRASSROOTS_ADJ_BPM_LG_2P) * twoPa * 1.9)
    : 0;
  const effFt = Number.isFinite(ftPct) && Number.isFinite(ftAttempts)
    ? ((ftPct - GRASSROOTS_ADJ_BPM_LG_FT) * ftAttempts * 0.75)
    : 0;
  const eff3p = Number.isFinite(threePpct) && Number.isFinite(threePa)
    ? (-Math.max(0, GRASSROOTS_ADJ_BPM_BAD_3P - threePpct) * threePa * 1.3)
    : 0;

  if (![pts, ast, tov, fga].every((value) => Number.isFinite(value))) return "";

  const obpm = -5.5
    + (pts * 0.22)
    + (ast * 0.68)
    - (tov * 0.42)
    - (fga * 0.10)
    + eff2p
    + effFt
    + eff3p;

  const dbpm = -0.8
    + ((positionInfo.effectiveIn - 76) * 0.33)
    + positionInfo.roleAdj
    + (Number.isFinite(stl) ? (stl * 0.36) : 0)
    + (Number.isFinite(blk) ? (blk * 0.30) : 0)
    + (Number.isFinite(drb) ? (drb * 0.10) : 0)
    + (Number.isFinite(orb) ? (orb * 0.08) : 0)
    - (Number.isFinite(pf) ? (pf * 0.10) : 0);

  return round((obpm + dbpm), 1);
}

function sortGrassrootsDisplayValues(values, preferredOrder = []) {
  const orderIndex = new Map(preferredOrder.map((value, index) => [normalizeKey(value), index]));
  return Array.from(new Set(Array.from(values).map((value) => String(value ?? "").trim()).filter(Boolean))).sort((left, right) => {
    const leftIndex = orderIndex.has(normalizeKey(left)) ? orderIndex.get(normalizeKey(left)) : Number.POSITIVE_INFINITY;
    const rightIndex = orderIndex.has(normalizeKey(right)) ? orderIndex.get(normalizeKey(right)) : Number.POSITIVE_INFINITY;
    if (leftIndex !== rightIndex) return leftIndex - rightIndex;
    return compareFilterValues(left, right);
  });
}

function sortGrassrootsEventValues(values) {
  return Array.from(new Set(Array.from(values).map((value) => String(value ?? "").trim()).filter(Boolean))).sort((left, right) => {
    const leftAllClasses = /\ball classes\b/i.test(left);
    const rightAllClasses = /\ball classes\b/i.test(right);
    const leftSeason = Number(String(left).match(/\d{4}/)?.[0] || 0);
    const rightSeason = Number(String(right).match(/\d{4}/)?.[0] || 0);
    if (leftSeason !== rightSeason) return rightSeason - leftSeason;
    if (leftAllClasses !== rightAllClasses) return leftAllClasses ? -1 : 1;
    return compareFilterValues(left, right);
  });
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
  return normalizeKey(playerName);
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
  const text = String(value ?? "").trim().toUpperCase();
  if (!text) return "";
  const compact = text.replace(/[^A-Z]/g, "");
  if (!compact || compact === "NA") return "";
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
  if (Number.isFinite(numericClass) && numericClass >= 1000) {
    if (numericClass === 2034) return 2024;
    if (numericClass === 2035) return 2025;
    if (numericClass === 2206) return 2026;
    return numericClass;
  }
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
  const heightKey = Number.isFinite(row.height_in) ? Math.round(row.height_in / 2) * 2 : "";
  const weightKey = Number.isFinite(row.weight_lb) ? Math.round(row.weight_lb / 10) * 10 : "";
  const classKey = getStringValue(row.class_year).trim();
  const ageKey = getStringValue(row.age_range || row.level || "").trim();
  return [normalizeKey(playerName), heightKey, weightKey, classKey, normalizeKey(ageKey)].join("|");
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
  const ageRangeValues = rows.map((row) => row.age_range).filter(Boolean);
  const settingValues = rows.map((row) => row.setting).filter(Boolean);
  const eventUrls = rows.map((row) => row.event_url).filter(Boolean);
  const circuitValues = rows.map((row) => row.circuit).filter(Boolean);
  const stateValues = rows.map((row) => row.state).filter(Boolean);
  const positions = rows.map((row) => row.pos).filter(Boolean);
  const uniqueEventNames = Array.from(new Set(eventNames.filter(Boolean)));
  const uniqueEventGroups = Array.from(new Set(eventGroups.filter(Boolean)));
  const uniqueEventSources = Array.from(new Set(eventSources.filter(Boolean)));
  const uniqueEventMergeKeys = Array.from(new Set(eventMergeKeys.filter(Boolean)));
  const uniquePlayerNames = Array.from(new Set(playerNames.filter(Boolean)));
  const uniqueTeamNames = Array.from(new Set(teamNames.filter(Boolean)));
  const uniqueTeamFullNames = Array.from(new Set(teamFullNames.filter(Boolean)));
  const uniqueAgeRanges = Array.from(new Set(ageRangeValues.filter(Boolean)));
  const uniqueSettings = Array.from(new Set(settingValues.filter(Boolean)));
  const uniqueEventUrls = Array.from(new Set(eventUrls.filter(Boolean)));
  const uniqueCircuits = Array.from(new Set(circuitValues.filter(Boolean)));
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
  const aggregate = latest ? { ...latest } : {};
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
  aggregate.age_range = sortGrassrootsDisplayValues(uniqueAgeRanges, ["17U", "16U", "15U"]).join(" / ") || latest.age_range || "";
  aggregate.level = sortGrassrootsDisplayValues(uniqueAgeRanges, ["17U", "16U", "15U"]).join(" / ") || aggregate.level || aggregate.age_range || "";
  aggregate.setting = sortGrassrootsDisplayValues(uniqueSettings, ["HS", "AAU"]).join(" / ") || latest.setting || "";
  aggregate.event_url = uniqueEventUrls.join(" / ") || latest.event_url || "";
  aggregate.circuit = sortGrassrootsDisplayValues(uniqueCircuits, Array.from(circuitOrder.keys())).join(" / ") || latest.circuit || "";
  const heightValues = rows.map((row) => (Number.isFinite(row.height_in) ? row.height_in : Number.isFinite(row.inches) ? row.inches : Number.NaN)).filter(Number.isFinite);
  const weightValues = rows.map((row) => (Number.isFinite(row.weight_lb) ? row.weight_lb : Number.isFinite(row.weight) ? row.weight : Number.NaN)).filter(Number.isFinite);
  if (!Number.isFinite(aggregate.height_in) && heightValues.length) aggregate.height_in = heightValues[0];
  if (!Number.isFinite(aggregate.weight_lb) && weightValues.length) aggregate.weight_lb = weightValues[0];
  if (!getStringValue(aggregate.level).trim()) aggregate.level = aggregate.age_range || "";
  aggregate.pos = uniquePositions.length ? uniquePositions.join(" / ") : normalizePosLabel(latest.pos || latest.pos_text || "") || "";
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
  const usgRows = rows
    .map((row) => ({
      value: Number(row.usg_pct),
      weight: Math.max(getMinutesValue(row), 1),
    }))
    .filter((item) => Number.isFinite(item.value) && item.weight > 0);
  if (usgRows.length) {
    const usgWeight = usgRows.reduce((sum, item) => sum + item.weight, 0);
    if (usgWeight > 0) {
      const usgWeighted = usgRows.reduce((sum, item) => sum + (item.value * item.weight), 0);
      aggregate.usg_pct = round(usgWeighted / usgWeight, 3);
    }
  }
  aggregate.gp = Math.round(aggregate.gp);
  aggregate.percentile_weight = deriveGrassrootsPercentileWeight(aggregate.gp, aggregate.min);
  aggregate.pts_pg = aggregate.gp > 0 ? round(aggregate.pts / aggregate.gp, 1) : "";
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
  aggregate.ast_stl_pg = aggregate.gp > 0 ? round((aggregate.ast + aggregate.stl) / aggregate.gp, 1) : "";
  aggregate.ast_stl_per40 = aggregate.min > 0 ? round(((aggregate.ast + aggregate.stl) / aggregate.min) * 40, 1) : "";
  aggregate.three_pa_per40 = aggregate.min > 0 ? round((aggregate.tpa / aggregate.min) * 40, 1) : "";
  aggregate.tov_per40 = aggregate.min > 0 ? round((aggregate.tov / aggregate.min) * 40, 1) : "";
  aggregate.pf_per40 = aggregate.min > 0 ? round((aggregate.pf / aggregate.min) * 40, 1) : "";
  aggregate.three_pe = aggregate.tpa;
  aggregate.fgs = aggregate.fgm;
  const aggregateClassYear = Number(aggregate.class_year);
  if (Number.isFinite(aggregateClassYear) && aggregateClassYear >= 1000) {
    aggregate.class_year = aggregateClassYear;
  } else {
    const inferredClassYear = rows
      .map((row) => Number(row.class_year))
      .find((value) => Number.isFinite(value) && value >= 1000);
    aggregate.class_year = Number.isFinite(inferredClassYear) ? inferredClassYear : "";
  }
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

function buildGrassrootsExactDuplicateKey(row) {
  return [
    row.season,
    normalizeKey(row.event_merge_key || row.event_name || row.event_group || row.event_raw_name || ""),
    normalizeGrassrootsTeamKey(row.team_full || row.team_name || ""),
    normalizeKey(row.circuit || ""),
    normalizeKey(row.setting || ""),
    normalizeKey(row.pos || row.pos_text || ""),
    String(row.class_year ?? ""),
    String(row.height_in ?? ""),
    String(row.weight_lb ?? ""),
    String(row.gp ?? ""),
    String(row.min ?? ""),
    String(row.pts ?? ""),
    String(row.fgm ?? ""),
    String(row.fga ?? ""),
    String(row["2pm"] ?? ""),
    String(row["2pa"] ?? ""),
    String(row.tpm ?? ""),
    String(row.tpa ?? ""),
    String(row.ftm ?? ""),
    String(row.trb ?? ""),
    String(row.ast ?? ""),
    String(row.tov ?? ""),
    String(row.stl ?? ""),
    String(row.blk ?? ""),
    String(row.pf ?? ""),
    String(row.stocks ?? ""),
  ].join("|");
}

function grassrootsDuplicateRowScore(row) {
  const fields = Object.entries(row || {}).reduce((sum, [column, value]) => {
    if (column.startsWith("_")) return sum;
    if (value == null || value === "") return sum;
    if (typeof value === "number" && !Number.isFinite(value)) return sum;
    return sum + 1;
  }, 0);
  const playerScore = String(row.player_name || row.player || "").length;
  const teamScore = String(row.team_full || row.team_name || "").length;
  const eventScore = String(row.event_name || row.event_group || "").length;
  return (fields * 10) + playerScore + teamScore + eventScore;
}

function buildGrassrootsAttributeProfileKeys(row) {
  const player = normalizeGrassrootsNameKey(row.player_name || row.player);
  if (!player) return [];
  const season = getStringValue(row.season).trim();
  const age = getStringValue(row.age_range || row.level || "").trim();
  const classYear = getStringValue(row.class_year).trim();
  return Array.from(new Set([
    [player, season, age, classYear].join("|"),
    [player, season, age].join("|"),
    [player, classYear].join("|"),
    player,
  ].filter(Boolean)));
}

function backfillGrassrootsPlayerAttributes(rows) {
  const profiles = new Map();

  rows.forEach((row) => {
    const score = grassrootsDuplicateRowScore(row);
    const height = Number.isFinite(row.height_in) ? row.height_in : Number.isFinite(row.inches) ? row.inches : Number.NaN;
    const weight = Number.isFinite(row.weight_lb) ? row.weight_lb : Number.isFinite(row.weight) ? row.weight : Number.NaN;
    const pos = normalizePosLabel(row.pos || row.pos_text);
    buildGrassrootsAttributeProfileKeys(row).forEach((key) => {
      const current = profiles.get(key) || {
        heightScore: Number.NEGATIVE_INFINITY,
        weightScore: Number.NEGATIVE_INFINITY,
        posScore: Number.NEGATIVE_INFINITY,
      };
      if (Number.isFinite(height) && score > current.heightScore) {
        current.heightScore = score;
        current.heightRow = row;
      }
      if (Number.isFinite(weight) && score > current.weightScore) {
        current.weightScore = score;
        current.weightRow = row;
      }
      if (pos && score > current.posScore) {
        current.posScore = score;
        current.posRow = row;
      }
      profiles.set(key, current);
    });
    row.pos = pos;
    row.pos_text = pos;
  });

  rows.forEach((row) => {
    const keys = buildGrassrootsAttributeProfileKeys(row);
    if (!keys.length) return;

    if (!Number.isFinite(row.height_in)) {
      for (const key of keys) {
        const source = profiles.get(key)?.heightRow;
        const height = Number.isFinite(source?.height_in) ? source.height_in : Number.isFinite(source?.inches) ? source.inches : Number.NaN;
        if (Number.isFinite(height)) {
          row.height_in = height;
          break;
        }
      }
    }

    if (!Number.isFinite(row.weight_lb)) {
      for (const key of keys) {
        const source = profiles.get(key)?.weightRow;
        const weight = Number.isFinite(source?.weight_lb) ? source.weight_lb : Number.isFinite(source?.weight) ? source.weight : Number.NaN;
        if (Number.isFinite(weight)) {
          row.weight_lb = weight;
          break;
        }
      }
    }

    if (!normalizePosLabel(row.pos || row.pos_text)) {
      for (const key of keys) {
        const source = profiles.get(key)?.posRow;
        const pos = normalizePosLabel(source?.pos || source?.pos_text);
        if (pos) {
          row.pos = pos;
          row.pos_text = pos;
          break;
        }
      }
    }
  });
}

function pickPreferredGrassrootsDuplicateRow(rows) {
  return rows
    .slice()
    .sort((left, right) => grassrootsDuplicateRowScore(right) - grassrootsDuplicateRowScore(left))[0] || rows[0] || {};
}

function dedupeGrassrootsExactDuplicateRows(rows) {
  const grouped = new Map();
  rows.forEach((row) => {
    const key = buildGrassrootsExactDuplicateKey(row);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  let removed = 0;
  const deduped = Array.from(grouped.values()).map((groupRows) => {
    if (groupRows.length <= 1) return groupRows[0];
    removed += groupRows.length - 1;
    return pickPreferredGrassrootsDuplicateRow(groupRows);
  });

  if (removed > 0) {
    console.log(`Removed ${removed} exact duplicate grassroots rows.`);
  }
  return deduped;
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

function getGrassrootsScopeSpec(scope) {
  if (scope === "career_hs") {
    return {
      scope,
      setting: "HS",
      season: "All Years",
      filter: (row) => getGrassrootsSettingForCircuit(row.circuit) === "HS",
      key: (row) => row.career_player_key || buildGrassrootsCareerKey(row),
    };
  }
  if (scope === "career_aau") {
    return {
      scope,
      setting: "AAU",
      season: "All Years",
      filter: (row) => getGrassrootsSettingForCircuit(row.circuit) === "AAU",
      key: (row) => row.career_player_key || buildGrassrootsCareerKey(row),
    };
  }
  if (scope === "single_year") {
    return {
      scope,
      setting: "Single Year",
      season: null,
      filter: () => true,
      key: (row) => [row.season, row.career_player_key || buildGrassrootsCareerKey(row)].join("|"),
    };
  }
  return {
    scope: "career_overall",
    setting: "Overall",
    season: "All Years",
    filter: () => true,
    key: (row) => row.career_player_key || buildGrassrootsCareerKey(row),
  };
}

function finalizeGrassrootsScopeAggregate(aggregate, groupRows, scopeSpec) {
  const rows = Array.isArray(groupRows) ? groupRows : [];
  if (!rows.length) return aggregate;

  const latest = rows[0] || {};
  const numericColumns = new Set();
  rows.forEach((row) => {
    Object.entries(row).forEach(([column, value]) => {
      if (typeof value === "number" && Number.isFinite(value)) numericColumns.add(column);
    });
  });

  const latestColumns = new Set([
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
    "pos_text",
    "class_year",
    "height_in",
    "weight_lb",
    "gp",
    "min",
    "mpg",
    "percentile_weight",
  ]);

  const sumColumns = new Set([
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
  ]);

  numericColumns.forEach((column) => {
    if (sumColumns.has(column) || latestColumns.has(column)) return;
    if (/(_pg$|_per40$|_pr$|_pf$|_to$|_bpm$|_pe$|_weight$|_percentile$|_rate$)/i.test(column)) return;
    let totalWeight = 0;
    let weightedSum = 0;
    let hasValue = false;
    rows.forEach((row) => {
      const value = Number(row[column]);
      if (!Number.isFinite(value)) return;
      hasValue = true;
      const weight = Math.max(getMinutesValue(row), 1);
      totalWeight += weight;
      weightedSum += value * weight;
    });
    if (hasValue && totalWeight > 0) aggregate[column] = round(weightedSum / totalWeight, 3);
  });

  aggregate.season = latest.season || "";
  aggregate.setting = scopeSpec.setting;
  aggregate.rank = null;
  if (scopeSpec.scope !== "single_year") {
    const aggregateClassYear = Number(aggregate.class_year);
    if (Number.isFinite(aggregateClassYear) && aggregateClassYear >= 1000) {
      aggregate.class_year = aggregateClassYear;
    } else {
      const inferredClassYear = rows
        .map((row) => Number(row.class_year))
        .find((value) => Number.isFinite(value) && value >= 1000);
      aggregate.class_year = Number.isFinite(inferredClassYear) ? inferredClassYear : "";
    }
  }
  aggregate.pts_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.pts / aggregate.min) * 40, 1) : "";
  aggregate.trb_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.trb / aggregate.min) * 40, 1) : "";
  aggregate.ast_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.ast / aggregate.min) * 40, 1) : "";
  aggregate.tov_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.tov / aggregate.min) * 40, 1) : "";
  aggregate.stl_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.stl / aggregate.min) * 40, 1) : "";
  aggregate.blk_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.blk / aggregate.min) * 40, 1) : "";
  aggregate.pf_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.pf / aggregate.min) * 40, 1) : "";
  aggregate.stocks_per40 = Number.isFinite(aggregate.min) && aggregate.min > 0 ? round((aggregate.stocks / aggregate.min) * 40, 1) : "";
  aggregate.gp = roundGrassrootsCount(aggregate.gp);
  aggregate.min = Number.isFinite(aggregate.min) ? round(aggregate.min, 1) : "";
  aggregate.mpg = aggregate.gp > 0 ? round(aggregate.min / aggregate.gp, 1) : "";
  aggregate.pts_pg = aggregate.gp > 0 ? round(aggregate.pts / aggregate.gp, 1) : "";
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
  aggregate.ast_stl_pg = aggregate.gp > 0 ? round((aggregate.ast + aggregate.stl) / aggregate.gp, 1) : "";
  aggregate.ast_stl_per40 = aggregate.min > 0 ? round(((aggregate.ast + aggregate.stl) / aggregate.min) * 40, 1) : "";
  aggregate.three_pa_per40 = aggregate.min > 0 ? round((aggregate.tpa / aggregate.min) * 40, 1) : "";
  aggregate.three_pe = aggregate.tpa;
  aggregate.adj_bpm = calculateGrassrootsAdjBpm(aggregate);
  aggregate.percentile_weight = deriveGrassrootsPercentileWeight(aggregate.gp, aggregate.min);
  aggregate.fgs = aggregate.fgm;
  if (scopeSpec.scope === "single_year") {
    aggregate.event_aliases = "";
    aggregate.player_aliases = "";
    aggregate.team_aliases = "";
  }
  aggregate._careerAggregate = true;
  aggregate._searchCacheKey = "";
  aggregate._searchHaystack = "";
  aggregate._colorBucketCacheKey = "";
  aggregate._colorBucketValue = "";
  return aggregate;
}

function buildGrassrootsScopeRows(sourceRows, scope) {
  const scopeSpec = getGrassrootsScopeSpec(scope);
  const grouped = new Map();
  sourceRows.forEach((row) => {
    if (!scopeSpec.filter(row)) return;
    const key = scopeSpec.key(row);
    if (!key) return;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  const rows = Array.from(grouped.values()).map((groupRows) => {
    const aggregate = aggregateGrassrootsRowGroup(groupRows);
    return finalizeGrassrootsScopeAggregate(aggregate, groupRows, scopeSpec);
  });

const mergedByPlayer = new Map();
  rows.forEach((row) => {
    const playerKey = normalizeKey(row.career_player_key || row.player_name || row.player || "");
    if (!playerKey) {
      const fallbackKey = `row|${mergedByPlayer.size}`;
      mergedByPlayer.set(fallbackKey, [row]);
      return;
    }
    const seasonKey = scope === "single_year" ? normalizeKey(row.season || "") : "";
    const key = `${seasonKey}|${playerKey}`;
    if (!mergedByPlayer.has(key)) mergedByPlayer.set(key, []);
    mergedByPlayer.get(key).push(row);
  });

  const mergedRows = Array.from(mergedByPlayer.values()).map((groupRows) => {
    if (groupRows.length <= 1) return groupRows[0];
    return aggregateGrassrootsRowGroup(groupRows);
  });

  mergedRows.sort((left, right) => {
    const leftSeason = Number(String(left.season ?? "").match(/\d{4}/)?.[0] || 0);
    const rightSeason = Number(String(right.season ?? "").match(/\d{4}/)?.[0] || 0);
    if (leftSeason !== rightSeason) return rightSeason - leftSeason;
    const leftAge = ageSortValue(left.age_range);
    const rightAge = ageSortValue(right.age_range);
    if (leftAge !== rightAge) return rightAge - leftAge;
    const circuitDiff = circuitSortValue(left.circuit) - circuitSortValue(right.circuit);
    if (circuitDiff) return circuitDiff;
    const rankDiff = Number(left.rank || 0) - Number(right.rank || 0);
    if (rankDiff) return rankDiff;
    const teamDiff = String(left.team_name || "").localeCompare(String(right.team_name || ""), undefined, { numeric: true, sensitivity: "base" });
    if (teamDiff) return teamDiff;
    return String(left.player_name || "").localeCompare(String(right.player_name || ""), undefined, { numeric: true, sensitivity: "base" });
  });

  return mergedRows;
}

function writeGrassrootsScopeBundle(scope, rows) {
  fs.mkdirSync(scopeBundleDir, { recursive: true });
  const bundleColumns = getGrassrootsScopeCsvColumns(scope);
  const csvText = [
    bundleColumns.join(","),
    ...rows.map((row) => {
      const csvRow = { ...row };
      if (isGrassrootsPosPlaceholder(csvRow.pos)) csvRow.pos = "";
      if (isGrassrootsPosPlaceholder(csvRow.pos_text)) csvRow.pos_text = "";
      return bundleColumns.map((column) => csvEscapeGrassrootsValue(column, csvRow[column])).join(",");
    }),
  ].join("\n");
  const output = [
    "// Generated by build_grassroots_bundle.js",
    `// ${new Date().toISOString()}`,
    "(function(root) {",
    "  root.GRASSROOTS_SCOPE_CSV_BUNDLES = root.GRASSROOTS_SCOPE_CSV_BUNDLES || {};",
    `  root.GRASSROOTS_SCOPE_CSV_BUNDLES[${JSON.stringify(scope)}] = ${JSON.stringify(csvText)};`,
    "})(typeof self !== \"undefined\" ? self : window);",
    "",
  ].join("\n");
  fs.writeFileSync(path.join(scopeBundleDir, `${scope}.js`), output, "utf8");
}

function getGrassrootsScopeCsvColumns(scope) {
  if (!["career_overall", "career_hs", "career_aau", "single_year"].includes(scope)) {
    return columns;
  }
  const omitted = new Set([
    "player_search_text",
    "player_aliases",
    "team_full",
    "team_search_text",
    "team_aliases",
    "player_cluster_key",
    "event_group",
    "event_raw_name",
    "event_aliases",
    "career_player_key",
  ]);
  return columns.filter((column) => !omitted.has(column));
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
  const pos = normalizePosLabel(row.POS);
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
    pos,
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
    level: ageRange,
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
    pos,
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
    three_pa_per40: Number.isFinite(min) && min > 0 ? round((tpa / min) * 40, 1) : "",
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
    ast_stl_pg: gpCount > 0 ? round((ast + stl) / gpCount, 1) : "",
    pts_per40: Number.isFinite(min) && min > 0 ? round((pts / min) * 40, 1) : "",
    trb_per40: Number.isFinite(min) && min > 0 ? round((trb / min) * 40, 1) : "",
    ast_per40: Number.isFinite(min) && min > 0 ? round((ast / min) * 40, 1) : "",
    ast_stl_per40: Number.isFinite(min) && min > 0 ? round(((ast + stl) / min) * 40, 1) : "",
    tov_per40: Number.isFinite(min) && min > 0 ? round((tov / min) * 40, 1) : "",
    stl_per40: Number.isFinite(min) && min > 0 ? round((stl / min) * 40, 1) : "",
    blk_per40: Number.isFinite(min) && min > 0 ? round((blk / min) * 40, 1) : "",
    ast_to: tov > 0 ? round(ast / tov, 2) : "",
    blk_pf: pf > 0 ? round(blk / pf, 2) : "",
    stocks_pf: pf > 0 ? round(stocks / pf, 2) : "",
    pf_per40: Number.isFinite(min) && min > 0 ? round((pf / min) * 40, 1) : "",
    stocks_per40: Number.isFinite(min) && min > 0 ? round((stocks / min) * 40, 1) : "",
    adj_bpm: calculateGrassrootsAdjBpm({
      pts_per40: Number.isFinite(min) && min > 0 ? round((pts / min) * 40, 1) : "",
      two_pa_per40: Number.isFinite(min) && min > 0 ? round((twoPa / min) * 40, 1) : "",
      three_pa_per40: Number.isFinite(min) && min > 0 ? round((tpa / min) * 40, 1) : "",
      ast_per40: Number.isFinite(min) && min > 0 ? round((ast / min) * 40, 1) : "",
      tov_per40: Number.isFinite(min) && min > 0 ? round((tov / min) * 40, 1) : "",
      stl_per40: Number.isFinite(min) && min > 0 ? round((stl / min) * 40, 1) : "",
      blk_per40: Number.isFinite(min) && min > 0 ? round((blk / min) * 40, 1) : "",
      pf_per40: Number.isFinite(min) && min > 0 ? round((pf / min) * 40, 1) : "",
      trb_per40: Number.isFinite(min) && min > 0 ? round((trb / min) * 40, 1) : "",
      height_in: heightIn,
      pos,
      pos_text: pos,
      two_p_pct: twoPct,
      tp_pct: tpPct,
      ft_pct: toNumber(row["FT%"]),
      ftm,
      gp: gpCount,
      min,
      pts,
      tpa,
      two_pa: twoPa,
      stl,
      blk,
      trb,
      pf,
    }),
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

backfillGrassrootsPlayerAttributes(rawRows);

const dedupedRawRows = dedupeGrassrootsExactDuplicateRows(rawRows);
const rows = mergeGrassrootsRows(dedupedRawRows);

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
  "ast_stl_pg",
  "tov",
  "tov_pg",
  "ast_to",
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
  "pts_per40",
  "trb_per40",
  "ast_per40",
  "tov_per40",
  "stl_per40",
  "blk_per40",
  "pf_per40",
  "stocks_per40",
  "ast_stl_per40",
  "three_pa_per40",
  "adj_bpm",
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

const scopeDefinitions = [
  "career_overall",
  "career_hs",
  "career_aau",
  "single_year",
];

scopeDefinitions.forEach((scope) => {
  const scopeRows = buildGrassrootsScopeRows(rows, scope);
  writeGrassrootsScopeBundle(scope, scopeRows);
  console.log(`Wrote grassroots scope bundle ${scope} (${scopeRows.length} rows)`);
});

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
    ...seasonRows.map((row) => {
      const csvRow = { ...row };
      if (isGrassrootsPosPlaceholder(csvRow.pos)) csvRow.pos = "";
      return columns.map((column) => csvEscapeGrassrootsValue(column, csvRow[column])).join(",");
    }),
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
