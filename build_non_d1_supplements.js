const fs = require("fs");
const path = require("path");
const vm = require("vm");

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const VENDOR_DIR = path.join(DATA_DIR, "vendor");
const RIM_ROOT = path.resolve(ROOT, "..", "Rim Data");
const OUTPUT_ROOT = path.join(VENDOR_DIR, "rim_supplements");
const OUTPUT_DIRS = {
  d2: path.join(OUTPUT_ROOT, "d2"),
  naia: path.join(OUTPUT_ROOT, "naia"),
  juco: path.join(OUTPUT_ROOT, "juco"),
  fiba: path.join(OUTPUT_ROOT, "fiba"),
};
const SHOT_PROFILE_COLUMNS = ["rim_made", "rim_att", "rim_pct", "mid_made", "mid_att", "mid_pct"];

const TEAM_STATE_TOKENS = new Set([
  "al", "ak", "ar", "az", "ca", "cal", "calif", "co", "colo", "ct", "de", "fl", "fla", "ga", "hi", "ia", "id", "il", "ill", "in", "ind",
  "ks", "kan", "ky", "la", "ma", "md", "me", "mi", "mich", "mn", "mo", "ms", "mt", "nc", "nd", "ne", "nev", "nh", "nj", "nm", "ny", "oh",
  "ok", "okla", "or", "ore", "pa", "ri", "sc", "sd", "tn", "tenn", "tx", "ut", "va", "vt", "wa", "wash", "wi", "wisc", "wv", "wy",
  "ark", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maryland", "michigan", "missouri", "montana", "nebraska", "ohio",
  "oklahoma", "oregon", "tennessee", "texas", "virginia", "washington", "wisconsin",
]);
const MASCOT_TOKENS = new Set([
  "aces", "aggies", "anteaters", "aztecs", "bears", "bearcats", "beavers", "bison", "bisons", "blazers", "blue", "bluebirds",
  "bluejays", "bobcats", "bonnies", "braves", "broncos", "bruins", "bulldogs", "bulls", "buccaneers", "buckeyes", "camels",
  "cardinal", "cardinals", "catamounts", "chargers", "chiefs", "chippewas", "colonials", "comets", "conference", "cougars",
  "cowboys", "crimson", "cyclones", "demons", "devils", "dolphins", "dons", "dragons", "dukes", "eagles", "explorers",
  "falcons", "fightin", "fighting", "firebirds", "flames", "flash", "foxes", "frogs", "gators", "gaels", "golden",
  "governors", "grif", "griffins", "grizzlies", "hawks", "huskies", "hurricane", "hurricanes", "islanders", "jacks",
  "jackrabbits", "jaguars", "jayhawks", "keydets", "knights", "lancers", "leopards", "lions", "lobos", "marauders", "matadors", "miners", "minutemen", "monarchs",
  "mountaineers", "musketeers", "mustangs", "norse", "oilers", "orange", "ospreys", "owls", "panthers", "patriots",
  "pelicans", "penguins", "pioneers", "pirates", "pride", "purple", "racers", "raiders", "rams", "ravens", "red", "redbirds",
  "redhawks", "redstorm", "rebels", "revolutionaries", "roadrunners", "rockets", "royals", "saints", "seminoles", "shockers",
  "skyhawks", "spartans", "spiders", "storm", "sun", "sycamores", "tars", "terriers", "thunderbirds", "tigers", "titans",
  "tar", "heels", "thundering", "trojans", "tribe", "tulips", "vandals", "vikings", "wildcats", "wolves", "wolfpack", "wolverines",
  "49ers", "badgers", "bearkats", "bengals", "billikens", "boilermakers", "broncs", "buffaloes", "cavaliers", "chanticleers",
  "commodores", "crusaders", "deacons", "ducks", "flyers", "friars", "gamecocks", "gauchos", "hatters", "hawkeyes", "hens",
  "hilltoppers", "hoosiers", "hokies", "hoyas", "jackets", "jaspers", "kangaroos", "leathernecks", "lumberjacks", "mastodons",
  "midshipmen", "mocs", "pack", "paladins", "peacocks", "phoenix", "pilots", "rattlers", "retrievers", "runnin", "salukis",
  "screaming", "seahawks", "seawolves", "sharks", "stags", "terrapins", "texans", "tommies", "toreros", "trailblazers", "tritons",
  "utes", "vaqueros", "volunteers", "wave", "waves", "warhawks", "warriors", "zips", "illini", "irish", "herd", "antelopes",
  "cajuns", "colonels", "cornhuskers", "coyotes", "danes", "greyhounds", "highlanders", "hornets", "lakers", "longhorns",
  "mavericks", "privateers", "quakers", "razorbacks", "ramblers", "sooners", "avengers",
]);
const MASCOT_PREFIX_TOKENS = new Set(["big", "black", "fighting", "golden", "great", "lady", "little", "mighty", "purple", "ragin", "red", "scarlet", "silver"]);
const TEAM_SUFFIX_PATTERNS = [
  /\bFightin(?:g)?\s+Blue\s+Hens?\b$/i,
  /\bBlack\s+Knights?\b$/i,
  /\bNittany\s+Lions?\b$/i,
  /\bMountain\s+Hawks?\b$/i,
  /\bHorned\s+Frogs?\b$/i,
  /\bYellow\s+Jackets?\b$/i,
  /\bBlue\s+Devils?\b$/i,
  /\bBlue\s+Hose\b$/i,
  /\bBig\s+Green\b$/i,
  /\bGreen\s+Wave\b$/i,
  /\bMean\s+Green\b$/i,
  /\bRunnin['’]?\s+Bulldogs?\b$/i,
  /\bScreaming\s+Eagles?\b$/i,
  /\bGolden\s+Gophers?\b$/i,
  /\bGolden\s+Flashes?\b$/i,
  /\bGolden\s+Eagles?\b$/i,
  /\bRed\s+Storm\b$/i,
  /\bWolf\s+Pack\b$/i,
  /\bCrimson\s+Tide\b$/i,
  /\bDemon\s+Deacons?\b$/i,
];
const ACADEMIC_SUFFIX_PATTERNS = [
  /\bCommunity\s+and\s+Technical\s+College\b$/i,
  /\bTechnical\s+and\s+Community\s+College\b$/i,
  /\bCommunity\s+and\s+Technical\b$/i,
  /\bTechnical\s+and\s+Community\b$/i,
  /\bTechnical\s*&\s*CC\b$/i,
  /\bTechnical\s+CC\b$/i,
  /\bCommunity\s+College\b$/i,
  /\bJunior\s+College\b$/i,
  /\bTechnical\s+College\b$/i,
  /\bState\s+College\b$/i,
  /\bCollege\s+of\s+Science\b$/i,
  /\bCollege\b$/i,
  /\bCC\b$/i,
];

const FIBA_FOLDER_MAP = [
  ["U16 Americup Rim Data", "u16_americup"],
  ["U17 World Cup Rim Data", "u17_world_cup"],
  ["U18 Americup Rim Data", "u18_americup"],
  ["U18 Euro A Rim Data", "u18_euro_a"],
  ["U18 Euro B Rim Data", "u18_euro_b"],
  ["U19 World Cup Rim Data", "u19_world_cup"],
  ["U20 Euro A RIm Data", "u20_euro_a"],
  ["U20 Euro B Rim Data", "u20_euro_b"],
];

const DATASET_SOURCES = {
  d2: {
    inputFile: path.join(DATA_DIR, "d2_all_seasons.js"),
    globalName: "D2_ALL_CSV",
    playerColumn: "player",
    teamColumn: "team_name",
    seasonParser: (fileName) => {
      const match = fileName.match(/(\d{4})-(\d{4})/);
      return match ? `${match[1]}-${String(match[2]).slice(-2)}` : "";
    },
    rimDir: path.join(RIM_ROOT, "D2 Rim Stats"),
    twoAtt: (row) => firstFinite(toNumber(row.two_pa), subtractIfFinite(toNumber(row.fga), toNumber(row["3pa"]))),
    twoMade: (row) => firstFinite(toNumber(row.two_pm), subtractIfFinite(toNumber(row.fgm), toNumber(row["3pm"]))),
    rowKey: (row) => makeRuntimeKey("d2", row.season, row.team_name, row.player),
  },
  naia: {
    inputFile: path.join(VENDOR_DIR, "naia_all_seasons.js"),
    globalName: "NAIA_ALL_CSV",
    playerColumn: "player_name",
    teamColumn: "team_name",
    seasonParser: (fileName) => {
      const match = fileName.match(/(\d{4})-(\d{4})/);
      return match ? `${match[1]}-${String(match[2]).slice(-2)}` : "";
    },
    rimDir: path.join(RIM_ROOT, "NAIA Rim Data"),
    twoAtt: (row) => firstFinite(toNumber(row["2pa"]), toNumber(row.two_pa), subtractIfFinite(toNumber(row.fga), toNumber(row.tpa))),
    twoMade: (row) => firstFinite(toNumber(row["2pm"]), toNumber(row.two_pm), subtractIfFinite(toNumber(row.fgm), toNumber(row.tpm))),
    rowKey: (row) => makeRuntimeKey("naia", row.season, row.team_name, row.player_name),
  },
  juco: {
    inputFile: path.join(VENDOR_DIR, "juco_all_seasons.js"),
    globalName: "NJCAA_ALL_CSV",
    playerColumn: "player_name",
    teamColumn: "team_name",
    seasonParser: (fileName) => {
      const match = fileName.match(/(\d{4})-(\d{4})/);
      return match ? `${match[1]}-${String(match[2]).slice(-2)}` : "";
    },
    rimDir: path.join(RIM_ROOT, "JUCO Rim Data"),
    twoAtt: (row) => firstFinite(toNumber(row["2pa"]), toNumber(row.two_pa), subtractIfFinite(toNumber(row.fga), toNumber(row.tpa))),
    twoMade: (row) => firstFinite(toNumber(row["2pm"]), toNumber(row.two_pm), subtractIfFinite(toNumber(row.fgm), toNumber(row.tpm))),
    rowKey: (row) => makeRuntimeKey("juco", row.season, row.team_name, row.player_name),
  },
  fiba: {
    inputFile: path.join(DATA_DIR, "fiba_all_seasons.js"),
    globalName: "FIBA_ALL_CSV",
    playerColumn: "player_name",
    teamColumn: "team_name",
    seasonParser: (fileName) => {
      const match = fileName.match(/National Teams\s+(\d{4})/i);
      return match ? match[1] : "";
    },
    rimDirs: FIBA_FOLDER_MAP.map(([folder, competitionKey]) => ({
      dir: path.join(RIM_ROOT, folder),
      competitionKey,
    })),
    twoAtt: (row) => firstFinite(toNumber(row["2pa"]), toNumber(row.two_pa), subtractIfFinite(toNumber(row.fga), toNumber(row["3pa"]))),
    twoMade: (row) => firstFinite(toNumber(row["2pm"]), toNumber(row.two_pm), subtractIfFinite(toNumber(row.fgm), toNumber(row["3pm"]))),
    rowKey: (row) => makeRuntimeKey("fiba", row.season, row.team_code || row.team_name, row.player_name, row.competition_key),
  },
};

function main() {
  const output = { rim: { d2: {}, naia: {}, juco: {}, fiba: {} } };
  const summary = {};
  const sourceRows = {};

  Object.entries(DATASET_SOURCES).forEach(([datasetId, config]) => {
    const rows = loadCsvRows(config.inputFile, config.globalName);
    sourceRows[datasetId] = rows;
    const index = buildRowIndex(rows, datasetId, config);
    const availableSeasons = Array.from(new Set(rows.map((row) => getStringValue(row.season).trim()).filter(Boolean))).sort(compareSeasons);
    const stats = datasetId === "fiba"
      ? processFibaRimRows(index, output.rim.fiba, config)
      : processCollegeRimRows(index, output.rim[datasetId], config, datasetId);
    stats.availableSeasons = availableSeasons;
    summary[datasetId] = stats;
  });

  Object.keys(OUTPUT_DIRS).forEach((datasetId) => {
    const stats = summary[datasetId];
    const dirPath = OUTPUT_DIRS[datasetId];
    const seasons = summary[datasetId].availableSeasons || summary[datasetId].seasons;
    fs.mkdirSync(dirPath, { recursive: true });
    seasons.forEach((season) => {
      const seasonPayload = output.rim[datasetId][season] || {};
      const banner = [
        "// Generated by build_non_d1_supplements.js",
        `// ${new Date().toISOString()}`,
        `// ${datasetId} ${season}: ${Object.keys(seasonPayload).length} matched rows`,
        "",
      ].join("\n");
      const payload = [
        "window.NON_D1_SUPPLEMENTS = window.NON_D1_SUPPLEMENTS || { rim: {} };",
        "window.NON_D1_SUPPLEMENTS.rim = window.NON_D1_SUPPLEMENTS.rim || {};",
        `window.NON_D1_SUPPLEMENTS.rim.${datasetId} = window.NON_D1_SUPPLEMENTS.rim.${datasetId} || {};`,
        `window.NON_D1_SUPPLEMENTS.rim.${datasetId}[${JSON.stringify(season)}] = ${JSON.stringify(seasonPayload)};`,
        "",
      ].join("\n");
      const fileName = `${sanitizeSeasonFileName(season)}.js`;
      fs.writeFileSync(path.join(dirPath, fileName), `${banner}${payload}`, "utf8");
    });
    stats.fileCount = seasons.length;
  });

  Object.entries(DATASET_SOURCES).forEach(([datasetId, config]) => {
    writeMergedDatasetBundle(datasetId, config, sourceRows[datasetId], output.rim[datasetId]);
  });

  Object.entries(summary).forEach(([datasetId, stats]) => {
    console.log(`${datasetId}: matched ${stats.matched}/${stats.total} rim rows (${stats.skippedAmbiguous} ambiguous, ${stats.skippedMissing} missing)`);
  });
  Object.entries(OUTPUT_DIRS).forEach(([datasetId, dirPath]) => {
    console.log(`wrote ${datasetId} supplement shards -> ${path.relative(ROOT, dirPath)} (${summary[datasetId].fileCount} files)`);
  });
  Object.entries(DATASET_SOURCES).forEach(([datasetId, config]) => {
    console.log(`rewrote ${datasetId} base bundle -> ${path.relative(ROOT, config.inputFile)}`);
  });
}

function processCollegeRimRows(index, out, config, datasetId) {
  const stats = { total: 0, matched: 0, skippedAmbiguous: 0, skippedMissing: 0, seasons: [] };
  const files = fs.readdirSync(config.rimDir).filter((fileName) => fileName.toLowerCase().endsWith(".csv"));
  files.forEach((fileName) => {
    const season = config.seasonParser(fileName);
    if (!season) return;
    if (!stats.seasons.includes(season)) stats.seasons.push(season);
    const rimRows = parseCSV(fs.readFileSync(path.join(config.rimDir, fileName), "utf8"));
    rimRows.forEach((rimRow) => {
      stats.total += 1;
      const match = resolveRimMatch(index, datasetId, season, rimRow.Player, rimRow.Team);
      if (!match) {
        stats.skippedMissing += 1;
        return;
      }
      if (match.ambiguous) {
        stats.skippedAmbiguous += 1;
        return;
      }
      const supplement = buildRimSupplement(match.row, rimRow, config);
      if (!supplement) return;
      if (!out[season]) out[season] = {};
      out[season][config.rowKey(match.row)] = supplement;
      stats.matched += 1;
    });
  });
  stats.seasons.sort(compareSeasons);
  return stats;
}

function processFibaRimRows(index, out, config) {
  const stats = { total: 0, matched: 0, skippedAmbiguous: 0, skippedMissing: 0, seasons: [] };
  config.rimDirs.forEach(({ dir, competitionKey }) => {
    const files = fs.readdirSync(dir).filter((fileName) => fileName.toLowerCase().endsWith(".csv"));
    files.forEach((fileName) => {
      const season = config.seasonParser(fileName);
      if (!season) return;
      if (!stats.seasons.includes(season)) stats.seasons.push(season);
      const rimRows = parseCSV(fs.readFileSync(path.join(dir, fileName), "utf8"));
      rimRows.forEach((rimRow) => {
        stats.total += 1;
        const match = resolveRimMatch(index, "fiba", season, rimRow.Player, rimRow.Team, competitionKey);
        if (!match) {
          stats.skippedMissing += 1;
          return;
        }
        if (match.ambiguous) {
          stats.skippedAmbiguous += 1;
          return;
        }
        const supplement = buildRimSupplement(match.row, rimRow, config);
        if (!supplement) return;
        if (!out[season]) out[season] = {};
        out[season][config.rowKey(match.row)] = supplement;
        stats.matched += 1;
      });
    });
  });
  stats.seasons.sort(compareSeasons);
  return stats;
}

function loadCsvRows(filePath, globalName) {
  const context = { window: {} };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(filePath, "utf8"), context);
  const raw = context.window[globalName] ?? "";
  const csvText = Array.isArray(raw) ? raw.join("\n") : String(raw);
  return parseCSV(csvText);
}

function writeMergedDatasetBundle(datasetId, config, rows, seasonBuckets) {
  if (!Array.isArray(rows) || !rows.length) return;
  rows.forEach((row) => {
    const season = getStringValue(row.season).trim();
    const supplement = seasonBuckets?.[season]?.[config.rowKey(row)] || null;
    SHOT_PROFILE_COLUMNS.forEach((column) => {
      if (!Object.prototype.hasOwnProperty.call(row, column)) row[column] = "";
      if (supplement && supplement[column] !== undefined && supplement[column] !== "") {
        row[column] = supplement[column];
      }
    });
  });
  const columns = [
    ...Object.keys(rows[0]),
    ...SHOT_PROFILE_COLUMNS.filter((column) => !Object.prototype.hasOwnProperty.call(rows[0], column)),
  ];
  const csvText = stringifyCSV(rows, columns);
  fs.writeFileSync(config.inputFile, `window.${config.globalName} = ${JSON.stringify(csvText)};\n`, "utf8");
}

function stringifyCSV(rows, columns) {
  const lines = [columns.join(",")];
  rows.forEach((row) => {
    lines.push(columns.map((column) => escapeCsvValue(row[column])).join(","));
  });
  return lines.join("\n");
}

function escapeCsvValue(value) {
  const text = getStringValue(value);
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, "\"\"")}"`;
  return text;
}

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
  const startIndex = rows[0].length === 2 && /^sep=$/i.test(rows[0][0]) ? 1 : 0;
  const header = rows[startIndex].map((cell) => cell.trim().replace(/^"|"$/g, ""));
  return rows.slice(startIndex + 1).map((cells) => {
    const out = {};
    header.forEach((column, index) => {
      out[column] = cells[index] ?? "";
    });
    return out;
  });
}

function sanitizeSeasonFileName(season) {
  return String(season).replace(/[^0-9A-Za-z_-]+/g, "_");
}

function compareSeasons(left, right) {
  const leftYear = extractLeadingYear(left);
  const rightYear = extractLeadingYear(right);
  if (leftYear !== rightYear) return leftYear - rightYear;
  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
}

function extractLeadingYear(value) {
  const match = String(value).match(/\d{4}/);
  return match ? Number(match[0]) : 0;
}

function buildRowIndex(rows, datasetId, config) {
  const index = new Map();
  rows.forEach((row) => {
    const season = getStringValue(row.season).trim();
    const nameKeys = buildNameKeys(row[config.playerColumn]);
    if (!season || !nameKeys.length) return;
    const prefix = datasetId === "fiba"
      ? `${season}|${getStringValue(row.competition_key).trim()}|`
      : `${season}|`;
    const candidate = {
      row,
      teamKeys: datasetId === "fiba"
        ? buildFibaTeamKeys(row.team_name, row.team_code, row.nationality)
        : buildSchoolKeys(row[config.teamColumn]),
    };
    nameKeys.forEach((nameKey) => {
      const key = `${prefix}${nameKey}`;
      if (!index.has(key)) index.set(key, []);
      index.get(key).push(candidate);
    });
  });
  return index;
}

function resolveRimMatch(index, datasetId, season, playerName, teamName, competitionKey = "") {
  const prefix = datasetId === "fiba" ? `${season}|${competitionKey}|` : `${season}|`;
  const candidates = new Map();
  buildNameKeys(playerName).forEach((nameKey) => {
    (index.get(`${prefix}${nameKey}`) || []).forEach((candidate) => {
      const key = JSON.stringify(candidate.row);
      if (!candidates.has(key)) candidates.set(key, candidate);
    });
  });
  if (!candidates.size) return null;

  const rimTeamKeys = datasetId === "fiba" ? buildFibaTeamKeys(teamName) : buildSchoolKeys(teamName);
  const scored = Array.from(candidates.values())
    .map((candidate) => ({
      row: candidate.row,
      score: scoreTeamKeySets(candidate.teamKeys, rimTeamKeys),
    }))
    .sort((left, right) => right.score - left.score);

  if (scored.length === 1) return { row: scored[0].row, ambiguous: false };
  if (scored[0].score > scored[1].score) return { row: scored[0].row, ambiguous: false };
  if (scored[0].score > 0 && scored[1].score === 0) return { row: scored[0].row, ambiguous: false };
  return { row: null, ambiguous: true };
}

function buildRimSupplement(row, rimRow, config) {
  const rimGp = toNumber(rimRow.GP);
  const targetGp = firstFinite(toNumber(row.gp), rimGp);
  const rimAttPerGame = firstFinite(toNumber(rimRow["2 FG Att"]), toNumber(rimRow["FG Att"]));
  const rimMadePerGame = firstFinite(toNumber(rimRow["2 FG Made"]), toNumber(rimRow["FG Made"]));
  const twoAtt = config.twoAtt(row);
  const twoMade = config.twoMade(row);

  if (!Number.isFinite(targetGp) || targetGp <= 0 || !Number.isFinite(rimAttPerGame) || !Number.isFinite(rimMadePerGame)) return null;

  let rimAtt = rimAttPerGame * targetGp;
  let rimMade = rimMadePerGame * targetGp;

  if (Number.isFinite(twoAtt) && rimAtt > twoAtt) {
    const factor = twoAtt > 0 ? twoAtt / rimAtt : 0;
    rimAtt *= factor;
    rimMade *= factor;
  }
  if (rimMade > rimAtt) rimMade = rimAtt;
  if (Number.isFinite(twoMade) && rimMade > twoMade) rimMade = twoMade;

  const midAtt = Number.isFinite(twoAtt) ? Math.max(0, twoAtt - rimAtt) : Number.NaN;
  let midMade = Number.isFinite(twoMade) ? Math.max(0, twoMade - rimMade) : Number.NaN;
  if (Number.isFinite(midAtt) && Number.isFinite(midMade) && midMade > midAtt) midMade = midAtt;

  return {
    rim_made: roundNumber(rimMade, 3),
    rim_att: roundNumber(rimAtt, 3),
    rim_pct: roundNumber(zeroSafePercent(rimMade, rimAtt), 1),
    mid_made: Number.isFinite(midMade) ? roundNumber(midMade, 3) : "",
    mid_att: Number.isFinite(midAtt) ? roundNumber(midAtt, 3) : "",
    mid_pct: Number.isFinite(midMade) && Number.isFinite(midAtt) ? roundNumber(zeroSafePercent(midMade, midAtt), 1) : "",
    rim_source_gp: Number.isFinite(rimGp) ? roundNumber(rimGp, 3) : "",
  };
}

function buildNameKeys(value) {
  const strictKey = normalizeNameKey(value);
  const looseKey = normalizeLooseNameKey(value);
  const squeezedKey = strictKey.replace(/\s+/g, "");
  return Array.from(new Set([strictKey, looseKey, squeezedKey].filter(Boolean)));
}

function normalizeKey(value) {
  return getStringValue(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\ba\s*&\s*m\b/g, "a m")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9 ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeNameKey(value) {
  return normalizeKey(value).replace(/\b(jr|sr|ii|iii|iv|v)\b/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeLooseNameKey(value) {
  return normalizeNameKey(value).replace(/\b[a-z]\b/g, " ").replace(/\s+/g, " ").trim();
}

function buildSchoolKeys(value) {
  const raw = getStringValue(value).trim();
  if (!raw) return [];
  const keys = new Set();
  const push = (candidate) => {
    const key = normalizeKey(candidate);
    if (key) keys.add(key);
  };
  push(raw);
  push(raw.replace(/\([^)]*\)/g, " "));
  push(simplifySchoolName(raw));
  const strippedState = stripStateTokens(raw);
  if (strippedState !== raw) push(strippedState);
  return Array.from(keys);
}

function buildFibaTeamKeys(...values) {
  const keys = new Set();
  values.forEach((value) => {
    const raw = getStringValue(value).trim();
    if (!raw) return;
    const normalized = normalizeKey(raw);
    if (normalized) keys.add(normalized);
    const upper = raw.toUpperCase().trim();
    if (/^[A-Z]{3}$/.test(upper)) keys.add(normalizeKey(expandFibaCode(upper)));
  });
  return Array.from(keys);
}

function expandFibaCode(value) {
  return {
    USA: "United States of America",
    GBR: "Great Britain",
    CZE: "Czech Republic",
    KOR: "South Korea",
    TUR: "Turkey",
    NED: "Netherlands",
    RSA: "South Africa",
    UAE: "United Arab Emirates",
  }[value] || value;
}

function simplifySchoolName(value) {
  const raw = getStringValue(value).trim();
  if (!raw) return raw;
  let cleaned = raw
    .replace(/&amp;/gi, "&")
    .replace(/([A-Za-z])-\(([^)]*)\)/g, "$1 $2")
    .replace(/-\s+\(/g, " (")
    .replace(/\(([^)]*)\)/g, " $1 ")
    .replace(/\s+/g, " ")
    .trim();
  TEAM_SUFFIX_PATTERNS.forEach((pattern) => {
    cleaned = cleaned.replace(pattern, " ").replace(/\s+/g, " ").trim();
  });
  ACADEMIC_SUFFIX_PATTERNS.forEach((pattern) => {
    while (pattern.test(cleaned)) {
      cleaned = cleaned.replace(pattern, " ").replace(/\s+/g, " ").trim();
    }
  });
  cleaned = cleaned
    .replace(/\bInstitute of Technology\b/gi, " ")
    .replace(/\bUniversity\b/gi, " ")
    .replace(/\bInstitutes?\b/gi, " ")
    .replace(/^(?:college|cc|community college)\s+of\s+/i, "")
    .replace(/^of\s+/i, "")
    .replace(/\s+(?:of|and|&|-)\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
  const tokens = cleaned.split(" ").filter(Boolean);
  while (tokens.length) {
    const last = normalizeTeamToken(tokens[tokens.length - 1]);
    if (!MASCOT_TOKENS.has(last)) break;
    tokens.pop();
    while (tokens.length && MASCOT_PREFIX_TOKENS.has(normalizeTeamToken(tokens[tokens.length - 1]))) {
      tokens.pop();
    }
  }
  return cleanupTeamEdgeWords(tokens.join(" ").replace(/\s+/g, " ").trim()) || cleaned || raw;
}

function normalizeTeamToken(value) {
  return getStringValue(value).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function stripStateTokens(value) {
  const tokens = normalizeKey(value).split(" ").filter(Boolean);
  return tokens.filter((token) => !TEAM_STATE_TOKENS.has(token)).join(" ");
}

function cleanupTeamEdgeWords(value) {
  return getStringValue(value)
    .replace(/&amp;/gi, "&")
    .replace(/^\s*(?:of|and|the|at)\s+/i, "")
    .replace(/\s+(?:of|and|the|at)\s*$/i, "")
    .replace(/^\s*[&-]\s*/, "")
    .replace(/\s*[&-]\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function scoreTeamKeySets(leftKeys, rightKeys) {
  let best = 0;
  leftKeys.forEach((left) => {
    rightKeys.forEach((right) => {
      const score = teamMatchScore(left, right);
      if (score > best) best = score;
    });
  });
  return best;
}

function teamMatchScore(left, right) {
  if (!left || !right) return 0;
  if (left === right) return 1;
  const leftTokens = new Set(left.split(" ").filter(Boolean));
  const rightTokens = new Set(right.split(" ").filter(Boolean));
  let overlap = 0;
  rightTokens.forEach((token) => {
    if (leftTokens.has(token)) overlap += 1;
  });
  if (!overlap) return 0;
  const minSize = Math.min(leftTokens.size, rightTokens.size);
  const maxSize = Math.max(leftTokens.size, rightTokens.size);
  if (overlap === minSize && minSize >= 2) return 0.82;
  return overlap / maxSize;
}

function makeRuntimeKey(datasetId, season, team, player, competitionKey = "") {
  if (datasetId === "fiba") return `fiba|${getStringValue(season).trim()}|${getStringValue(competitionKey).trim()}|${normalizeKey(team)}|${normalizeNameKey(player)}`;
  return `${datasetId}|${getStringValue(season).trim()}|${normalizeKey(team)}|${normalizeNameKey(player)}`;
}

function subtractIfFinite(left, right) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return Number.NaN;
  return left - right;
}

function firstFinite(...values) {
  return values.find((value) => Number.isFinite(value));
}

function zeroSafePercent(made, attempts) {
  if (!Number.isFinite(attempts) || attempts <= 0 || !Number.isFinite(made)) return 0;
  return (made / attempts) * 100;
}

function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : Number.NaN;
}

function roundNumber(value, digits = 3) {
  if (!Number.isFinite(value)) return "";
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function getStringValue(value) {
  return value == null ? "" : String(value);
}

main();
