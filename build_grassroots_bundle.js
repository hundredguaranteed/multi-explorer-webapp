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
    rows.push(mapRow(row, circuit));
    added += 1;
  });
  return added;
}

function mapRow(row, circuit) {
  const gp = toNumber(row.Games);
  const mpg = toNumber(row["MIN/G"]);
  const ptsPg = toNumber(row["PTS/G"]);
  const fgPct = toPercentNumber(row["FG%"]);
  const tpmPg = toNumber(row["3PM/G"]);
  const tpPct = toPercentNumber(row["3PT%"]);
  const fgm = toNumber(row.FGS);
  const fga = Number.isFinite(fgm) && Number.isFinite(fgPct)
    ? (fgPct > 0 ? round(fgm / (fgPct / 100), 1) : (fgm === 0 ? 0 : ""))
    : "";
  const tpm = Number.isFinite(tpmPg) && Number.isFinite(gp)
    ? round(tpmPg * gp, 1)
    : (Number.isFinite(tpmPg) && tpmPg === 0 ? 0 : "");
  const tpa = Number.isFinite(tpm) && Number.isFinite(tpPct)
    ? (tpPct > 0 ? round(tpm / (tpPct / 100), 1) : (tpm === 0 ? 0 : ""))
    : (Number.isFinite(tpm) && tpm === 0 ? 0 : "");
  const twoPm = Number.isFinite(fgm) && Number.isFinite(tpm) ? round(fgm - tpm, 1) : "";
  const twoPa = Number.isFinite(fga) && Number.isFinite(tpa) ? round(fga - tpa, 1) : "";
  const twoPct = Number.isFinite(twoPm) && Number.isFinite(twoPa) && twoPa > 0 ? round((twoPm / twoPa) * 100, 1) : "";
  const pts = Number.isFinite(ptsPg) && Number.isFinite(gp) ? round(ptsPg * gp, 1) : (Number.isFinite(ptsPg) && ptsPg === 0 ? 0 : "");
  const ftmRaw = Number.isFinite(pts) && Number.isFinite(fgm) && Number.isFinite(tpm) ? round(pts - (2 * fgm) - tpm, 1) : "";
  const ftm = Number.isFinite(ftmRaw) ? Math.max(0, ftmRaw) : "";
  const ftmPg = Number.isFinite(ftm) && Number.isFinite(gp) ? round(ftm / gp, 1) : "";
  const ftmFga = Number.isFinite(ftm) && Number.isFinite(fga) && fga > 0 ? round(ftm / fga, 3) : "";
  const threePr = Number.isFinite(tpa) && Number.isFinite(fga) && fga > 0 ? round(tpa / fga, 3) : "";
  const threePrPlusFtmFga = Number.isFinite(threePr) && Number.isFinite(ftmFga) ? round(threePr + ftmFga, 3) : "";
  const trbPg = toNumber(row["REB/G"]);
  const astPg = toNumber(row["AST/G"]);
  const tovPg = toNumber(row.TOV);
  const stlPg = toNumber(row["STL/G"]);
  const blkPg = toNumber(row["BLK/G"]);
  const pfPg = toNumber(row["PF/G"]);
  const trb = perGameTotal(trbPg, gp);
  const ast = perGameTotal(astPg, gp);
  const tov = perGameTotal(tovPg, gp);
  const stl = perGameTotal(stlPg, gp);
  const blk = perGameTotal(blkPg, gp);
  const pf = perGameTotal(pfPg, gp);
  const stocks = Number.isFinite(stl) && Number.isFinite(blk) ? round(stl + blk, 1) : "";
  const stocksPg = Number.isFinite(stlPg) && Number.isFinite(blkPg) ? round(stlPg + blkPg, 1) : "";
  const min = Number.isFinite(mpg) && Number.isFinite(gp) ? round(mpg * gp, 1) : "";
  const heightIn = parseHeight(row.HT);
  const weightLb = toNumber(row.WT);
  const season = parseSeason(row.event_name);
  const ageRange = parseAgeRange(row.event_name, row.Team);
  const classYear = inferGrassrootsClassYear(row.Class, season, ageRange);
  const rank = toNumber(row.Rank);

  return {
    season,
    age_range: ageRange,
    circuit,
    event_name: row.event_name || "",
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
    fg_pct: fgPct,
    "2pm": twoPm,
    "2pa": twoPa,
    "2p_pct": twoPct,
    tpm,
    tpa,
    tpm_pg: tpmPg,
    tpa_pg: Number.isFinite(tpa) && Number.isFinite(gp) ? round(tpa / gp, 1) : "",
    ftm,
    ftm_pg: ftmPg,
    ftm_fga: ftmFga,
    three_pr: threePr,
    three_pr_plus_ftm_fga: threePrPlusFtmFga,
    tp_pct: tpPct,
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

  const rows = [];
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

  const columns = [
  "season",
  "age_range",
  "level",
  "circuit",
  "event_name",
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
