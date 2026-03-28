const fs = require("fs");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const sources = [
  { circuit: "3SSB", file: path.join(rootDir, "adidas_3ssb_all_event_player_stats_clean.csv"), include: () => true },
  { circuit: "UAA", file: path.join(rootDir, "uaa_under_armour_all_event_player_stats_clean.csv"), include: () => true },
  {
    circuit: "EYBL",
    file: path.join(rootDir, "nike_all_event_player_stats_clean.csv"),
    include: (row) => {
      const eventName = String(row.event_name || "");
      return /eybl/i.test(eventName) && !/eycl|scholastic/i.test(eventName);
    },
  },
];

const outputFile = path.join(__dirname, "data", "vendor", "grassroots_all_seasons.js");
const circuitOrder = new Map([["EYBL", 0], ["3SSB", 1], ["UAA", 2]]);

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

function ageSortValue(ageRange) {
  const match = String(ageRange ?? "").match(/\b(\d{1,2})U\b/i);
  return match ? Number(match[1]) : 0;
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

function aggregateRows(rows) {
  const groups = new Map();
  rows.forEach((row) => {
    const key = [
      row.season,
      row.age_range,
      normalizeKey(row.team_name),
      normalizeKey(row.player_name),
    ].join("|");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  });
  return Array.from(groups.values()).map(aggregateGroup);
}

function aggregateGroup(group) {
  const leader = group.slice().sort((left, right) => {
    const leftWeight = Number.isFinite(left.min) ? left.min : Number.isFinite(left.gp) ? left.gp : 0;
    const rightWeight = Number.isFinite(right.min) ? right.min : Number.isFinite(right.gp) ? right.gp : 0;
    if (rightWeight !== leftWeight) return rightWeight - leftWeight;
    return Number(right.gp || 0) - Number(left.gp || 0);
  })[0] || {};
  const output = { ...leader };
  const sumColumns = ["gp", "min", "pts", "fgs", "fgm", "fga", "2pm", "2pa", "tpm", "tpa", "ast", "tov", "trb", "blk", "stl", "pf", "stocks"];
  const weightedColumns = ["ram", "c_ram", "usg_pct", "psp", "atr", "dsi", "three_pe"];
  const averageColumns = ["class_year", "height_in", "weight_lb", "page_index"];
  const weights = group.map((row) => {
    const minutes = Number.isFinite(row.min) ? row.min : "";
    if (Number.isFinite(minutes) && minutes > 0) return minutes;
    if (Number.isFinite(row.gp) && row.gp > 0) return row.gp;
    return 1;
  });

  sumColumns.forEach((column) => {
    const total = group.reduce((sum, row) => sum + (Number.isFinite(row[column]) ? row[column] : 0), 0);
    output[column] = total > 0 ? round(total, 1) : "";
  });

  weightedColumns.forEach((column) => {
    let weightedSum = 0;
    let totalWeight = 0;
    group.forEach((row, index) => {
      if (!Number.isFinite(row[column])) return;
      weightedSum += row[column] * weights[index];
      totalWeight += weights[index];
    });
    output[column] = totalWeight > 0 ? round(weightedSum / totalWeight, 1) : "";
  });

  averageColumns.forEach((column) => {
    output[column] = firstNonEmpty(group, column);
  });

  output.season = firstNonEmpty(group, "season");
  output.age_range = firstNonEmpty(group, "age_range") || "17U";
  output.level = output.age_range || "17U";
  output.circuit = uniqueJoined(group, "circuit");
  output.player_name = firstNonEmpty(group, "player_name");
  output.team_name = firstNonEmpty(group, "team_name");
  output.pos = firstNonEmpty(group, "pos");
  output.event_name = uniqueJoined(group, "event_name", true);
  output.event_url = firstNonEmpty(group, "event_url");
  output.event_total_players = group.reduce((max, row) => {
    const value = Number.isFinite(row.event_total_players) ? row.event_total_players : 0;
    return value > max ? value : max;
  }, 0) || "";
  output.page_index = firstNonEmpty(group, "page_index");
  output.rank = null;
  output.min = output.min || "";
  output.mpg = Number.isFinite(output.min) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.min / output.gp, 1) : "";
  output.pts_pg = Number.isFinite(output.pts) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.pts / output.gp, 1) : "";
  output.trb_pg = Number.isFinite(output.trb) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.trb / output.gp, 1) : "";
  output.ast_pg = Number.isFinite(output.ast) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.ast / output.gp, 1) : "";
  output.tov_pg = Number.isFinite(output.tov) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.tov / output.gp, 1) : "";
  output.stl_pg = Number.isFinite(output.stl) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.stl / output.gp, 1) : "";
  output.blk_pg = Number.isFinite(output.blk) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.blk / output.gp, 1) : "";
  output.pf_pg = Number.isFinite(output.pf) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.pf / output.gp, 1) : "";
  output.stocks_pg = Number.isFinite(output.stocks) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.stocks / output.gp, 1) : "";
  output.fg_pct = Number.isFinite(output.fgm) && Number.isFinite(output.fga) && output.fga > 0 ? ratio(output.fgm, output.fga) : "";
  output["2p_pct"] = Number.isFinite(output["2pm"]) && Number.isFinite(output["2pa"]) && output["2pa"] > 0 ? ratio(output["2pm"], output["2pa"]) : "";
  output.tp_pct = Number.isFinite(output.tpm) && Number.isFinite(output.tpa) && output.tpa > 0 ? ratio(output.tpm, output.tpa) : "";
  output.three_pr = Number.isFinite(output.tpa) && Number.isFinite(output.fga) && output.fga > 0 ? round(output.tpa / output.fga, 3) : "";
  output.tpm_pg = Number.isFinite(output.tpm) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.tpm / output.gp, 1) : "";
  output.tpa_pg = Number.isFinite(output.tpa) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.tpa / output.gp, 1) : "";
  output.ftm = Number.isFinite(output.pts) && Number.isFinite(output["2pm"]) && Number.isFinite(output.tpm)
    ? round(output.pts - (output["2pm"] * 2) - (output.tpm * 3), 1)
    : "";
  output.ftm_pg = Number.isFinite(output.ftm) && Number.isFinite(output.gp) && output.gp > 0 ? round(output.ftm / output.gp, 1) : "";
  output.ftm_fga = Number.isFinite(output.ftm) && Number.isFinite(output.fga) && output.fga > 0 ? round(output.ftm / output.fga, 2) : "";
  output.three_pr_plus_ftm_fga = Number.isFinite(output.three_pr) && Number.isFinite(output.ftm_fga)
    ? round(output.three_pr + output.ftm_fga, 3)
    : "";
  output.atr = Number.isFinite(output.ast) && Number.isFinite(output.tov) && output.tov > 0 ? round(output.ast / output.tov, 2) : "";
  output.blk_pf = Number.isFinite(output.blk) && Number.isFinite(output.pf) && output.pf > 0 ? round(output.blk / output.pf, 2) : "";
  output.stocks_pf = Number.isFinite(output.stocks) && Number.isFinite(output.pf) && output.pf > 0 ? round(output.stocks / output.pf, 2) : "";
  return output;
}

function mapRow(row, circuit) {
  const gp = toNumber(row.Games);
  const mpg = toNumber(row["MIN/G"]);
  const ptsPg = toNumber(row["PTS/G"]);
  const fgPct = toPercentNumber(row["FG%"]);
  const tpmPg = toNumber(row["3PM/G"]);
  const tpPct = toPercentNumber(row["3PT%"]);
  const fgm = toNumber(row.FGS);
  const fga = Number.isFinite(fgm) && Number.isFinite(fgPct) && fgPct > 0 ? round(fgm / (fgPct / 100), 1) : "";
  const tpm = Number.isFinite(tpmPg) && Number.isFinite(gp) ? round(tpmPg * gp, 1) : "";
  const tpa = Number.isFinite(tpm) && Number.isFinite(tpPct) && tpPct > 0 ? round(tpm / (tpPct / 100), 1) : "";
  const twoPm = Number.isFinite(fgm) && Number.isFinite(tpm) ? round(fgm - tpm, 1) : "";
  const twoPa = Number.isFinite(fga) && Number.isFinite(tpa) ? round(fga - tpa, 1) : "";
  const twoPct = Number.isFinite(twoPm) && Number.isFinite(twoPa) && twoPa > 0 ? round((twoPm / twoPa) * 100, 1) : "";
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
  const pts = Number.isFinite(ptsPg) && Number.isFinite(gp) ? round(ptsPg * gp, 1) : "";
  const min = Number.isFinite(mpg) && Number.isFinite(gp) ? round(mpg * gp, 1) : "";
  const heightIn = parseHeight(row.HT);
  const weightLb = toNumber(row.WT);
  const classYear = toNumber(row.Class);
  const rank = toNumber(row.Rank);

  return {
    season: parseSeason(row.event_name),
    age_range: parseAgeRange(row.event_name, row.Team),
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
sources.forEach(({ circuit, file, include }) => {
  const csv = fs.readFileSync(file, "utf8");
  const parsed = parseCSV(csv);
  parsed.forEach((row) => {
    if (!include(row)) return;
    rows.push(mapRow(row, circuit));
  });
});

const aggregatedRows = aggregateRows(rows);

aggregatedRows.sort((left, right) => {
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
  ...aggregatedRows.map((row) => columns.map((column) => csvEscape(row[column])).join(",")),
].join("\n");

fs.writeFileSync(outputFile, `window.GRASSROOTS_ALL_CSV = ${JSON.stringify(csvText)};\n`, "utf8");

const circuitCounts = aggregatedRows.reduce((acc, row) => {
  acc[row.circuit] = (acc[row.circuit] || 0) + 1;
  return acc;
}, {});

console.log(`Wrote ${outputFile}`);
console.log(`Rows: ${aggregatedRows.length}`);
console.log(JSON.stringify(circuitCounts));
