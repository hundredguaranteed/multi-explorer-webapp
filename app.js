const LOAD_STEP = 40;
const SEARCH_RENDER_DEBOUNCE_MS = 120;
const HOME_ID = "home";
const HOME_PAGES = [
  { id: "d1", label: "D1" },
  { id: "d2", label: "D2" },
  { id: "naia", label: "NAIA" },
  { id: "juco", label: "JUCO" },
  { id: "fiba", label: "FIBA" },
  { id: "nba", label: "NBA" },
  { id: "nba_companion", label: "NBA Companion" },
];
const D1_HM_CONFS = new Set(["ACC", "SEC", "B10", "B12", "BE", "P12", "P10"]);
const D1_HMM_CONFS = new Set(["WCC", "A10", "MWC"]);
const MINUTES_DEFAULT = 400;
const TABLE_FRAME_LIMIT = 2580;
const COLOR_SCALE_MAX_ROWS = 8000;
const STATUS_ANNOTATIONS_SCRIPT = "data/vendor/status_annotations.js";
const SHARED_SINGLE_FILTERS = [
  {
    id: "view_mode",
    label: "View",
    defaultValue: "player",
    options: [
      { value: "player", label: "Player" },
      { value: "career", label: "Career" },
    ],
  },
  {
    id: "color_mode",
    label: "Color mode",
    defaultValue: "overall",
    options: [
      { value: "year", label: "PCT by year" },
      { value: "year_position", label: "PCT by year+pos" },
      { value: "position", label: "PCT by pos" },
      { value: "competition", label: "PCT by comp" },
      { value: "competition_position", label: "PCT by comp+pos" },
      { value: "overall", label: "PCT overall" },
    ],
  },
];
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
  "mavericks", "privateers", "quakers", "razorbacks", "ramblers", "sooners", "avengers"
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
const JUCO_TEAM_STOPWORDS = new Set(["college", "community", "technical", "tech", "institute", "institutes", "school", "jr", "junior", "and", "of", "the", "at"]);
const TEAM_DISPLAY_OVERRIDES = {
  "asa brooklyn": "ASA Brooklyn",
  "queens of charlotte": "Queens",
  "mississippi valley state delta": "Mississippi Valley State",
  "texas austin": "Texas",
  "indiana indianapolis": "Indianapolis",
};
const TEAM_STATE_TOKENS = new Set([
  "al", "ak", "ar", "az", "ca", "cal", "calif", "co", "colo", "ct", "de", "fl", "fla", "ga", "hi", "ia", "id", "il", "ill", "in", "ind",
  "ks", "kan", "ky", "la", "ma", "md", "me", "mi", "mich", "mn", "mo", "ms", "mt", "nc", "nd", "ne", "nev", "nh", "nj", "nm", "ny", "oh",
  "ok", "okla", "or", "ore", "pa", "ri", "sc", "sd", "tn", "tenn", "tx", "ut", "va", "vt", "wa", "wash", "wi", "wisc", "wv", "wy",
  "ark", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maryland", "michigan", "missouri", "montana", "nebraska", "ohio",
  "oklahoma", "oregon", "tennessee", "texas", "virginia", "washington", "wisconsin"
]);
const TRAILING_LOCATION_TOKENS = new Set([...TEAM_STATE_TOKENS, "california", "florida", "georgia", "illinois"]);
const NAIA_DIVISION_STOPWORDS = new Set(["university", "college", "state", "st", "saint", "mt", "mount", "fort", "ft", "of", "the", "and", "u", "at"]);
const NAIA_DIVISION_STOPWORDS_EXT = new Set([
  ...NAIA_DIVISION_STOPWORDS,
  "christian", "baptist", "international", "tech", "technical", "institute", "institutes", "agricultural", "school", "campus", "univ", "academy",
]);
const NAIA_STATE_TOKENS = new Set([
  "al", "ak", "az", "ar", "ca", "co", "ct", "dc", "de", "fl", "ga", "hi", "ia", "id", "il", "in", "ks", "ky", "la", "ma",
  "md", "me", "mi", "mn", "mo", "ms", "mt", "nc", "nd", "ne", "nh", "nj", "nm", "nv", "ny", "oh", "ok", "or", "pa", "ri",
  "sc", "sd", "tn", "tx", "ut", "va", "vt", "wa", "wi", "wv", "wy", "ala", "ariz", "calif", "colo", "conn", "del", "fla",
  "ill", "ind", "kan", "kans", "mass", "mich", "minn", "miss", "mont", "neb", "nev", "okla", "ore", "penn", "tenn", "tex",
  "wash", "wisc", "wyo",
]);
const NAIA_DIVISION_ALIASES = {
  "benedictine mesa": "benedictine az",
  "ciu": "columbia international",
  "ciu sc": "columbia international",
  "cumberlands": "cumberland ky",
  "cumberlands ky": "cumberland ky",
  "embry riddle": "embry riddle prescott",
  "embry riddle az": "embry riddle prescott",
  "haskell indian nations university": "haskell",
  "iu kokomo": "indiana kokomo",
  "iu east": "indiana east",
  "iu northwest": "indiana northwest",
  "iu southeast": "indiana southeast",
  "indiana south bend": "iu south bend",
  "indiana south bend in": "iu south bend",
  "jessup": "william jessup",
  "calumet coll of st joseph": "calumet",
  "calumet st joseph": "calumet",
  "lindenwood belleville": "NAIA I",
  "loyola la": "loyola no",
  "new college of florida": "new college fl",
  "northern": "northern new mexico",
  "ouaz": "ottawa az",
  "martin methodist": "ut southern",
  "oklahoma panhandle state": "panhandle state ok",
  "ottawa kan": "ottawa ks",
  "ottawa kansas": "ottawa ks",
  "robert morris": "robert morris chicago",
  "sagu": "NAIA I",
  "st louis pharmacy": "health sciences pharmacy mo",
  "southwestern": "southwestern college ks",
  "st andrews": "st andrews presbyterian",
  "st andrews nc": "st andrews presbyterian",
  "tennessee southern": "ut southern",
  "thomas": "thomas ga",
  "uhsp": "health sciences pharmacy mo",
  "union commonwealth": "union ky",
  "wvu tech": "west virginia tech",
  "wvu tech wv": "west virginia tech",
};
const COMMON_ABBREV_NAME_TOKENS = new Set(["aj", "bj", "cj", "dj", "ej", "gj", "jc", "jd", "jj", "jk", "jl", "jm", "jp", "jr", "jt", "kj", "mj", "oj", "pj", "rj", "tj"]);

let naiaDivisionLookup = null;
let naiaDivisionTeamLookup = null;
let naiaDivisionHistory = null;

const D1_PLAYTYPE_DEFS = [
  { id: "iso", label: "Isolation", source: "Isolation", short: "Iso" },
  { id: "pnr_bh", label: "PnR BH", source: "PnR_BH", short: "PnR BH" },
  { id: "post_up", label: "Post Up", source: "Post_Up", short: "Post" },
  { id: "pnr_roll", label: "Roll", source: "PnR_Roll", short: "Roll" },
  { id: "spot_up", label: "Spot Up", source: "Spot_Up", short: "Spot" },
  { id: "off_screen", label: "Off Screen", source: "Off_Screen", short: "Screen" },
  { id: "hand_off", label: "Hand Off", source: "Hand_Off", short: "Handoff" },
  { id: "cut", label: "Cut", source: "Cut", short: "Cut" },
  { id: "off_reb", label: "Off Reb", source: "Off_Reb", short: "OReb" },
];

const D1_PLAYTYPE_METRICS = [
  { source: "Poss", suffix: "poss", label: "Poss", defaultVisible: false },
  { source: "%Time", suffix: "freq", label: "Freq", defaultVisible: true },
  { source: "PPP", suffix: "ppp", label: "PPP", defaultVisible: true },
  { source: "FG Att", suffix: "fg_att", label: "FGA", defaultVisible: false },
  { source: "eFG%", suffix: "efg_pct", label: "eFG%", defaultVisible: false },
  { source: "TO%", suffix: "tov_pct", label: "TO%", defaultVisible: true },
  { source: "FTA/FGA", suffix: "ftr", label: "FTr", defaultVisible: true },
  { source: "2 FG Att", suffix: "two_fg_att", label: "2PA", defaultVisible: false },
  { source: "2 FG%", suffix: "two_fg_pct", label: "2P%", defaultVisible: false },
  { source: "3FG Att", suffix: "three_fg_att", label: "3PA", defaultVisible: false },
  { source: "3 FG%", suffix: "three_fg_pct", label: "3P%", defaultVisible: false },
  { source: "3PA/FGA", suffix: "three_pr", label: "3Pr", defaultVisible: false },
];
const D1_TRUE_PLAYTYPE_IDS = ["iso", "pnr_bh", "post_up", "pnr_roll", "spot_up", "off_screen", "hand_off", "cut", "off_reb", "transition"];

const D1_SUMMARY_COLUMNS = [
  { key: "halfcourt_freq", label: "HC Freq", defaultVisible: true },
  { key: "halfcourt_ppp", label: "HC PPP", defaultVisible: true },
  { key: "halfcourt_two_pa", label: "HC 2PA", defaultVisible: false },
  { key: "halfcourt_two_p_pct", label: "HC 2P%", defaultVisible: false },
  { key: "transition_freq", label: "Trans Freq", defaultVisible: true },
  { key: "transition_ppp", label: "Trans PPP", defaultVisible: true },
  { key: "transition_two_fg_att", label: "Trans 2PA", defaultVisible: false },
  { key: "transition_two_fg_pct", label: "Trans 2P%", defaultVisible: false },
];

const D1_DRIVE_COLUMNS = [
  { key: "drive_freq", label: "Drive Freq", defaultVisible: true },
  { key: "hc_drive_freq", label: "HC Drive Freq", defaultVisible: false },
  { key: "drive_poss", label: "Drive Poss", defaultVisible: false },
  { key: "drive_ppp", label: "Drive PPP", defaultVisible: true },
  { key: "drive_tov_pct", label: "Drive TO%", defaultVisible: true },
  { key: "drive_ftr", label: "Drive FTr", defaultVisible: true },
  { key: "drive_fg_pct", label: "Drive FG%", defaultVisible: false },
  { key: "drive_two_p_pct", label: "Drive 2P%", defaultVisible: false },
  { key: "drive_fga", label: "Drive FGA", defaultVisible: false },
  { key: "drive_two_pa", label: "Drive 2PA", defaultVisible: false },
  { key: "drive_plus1_pct", label: "Drive +1%", defaultVisible: false },
];

const D1_SHOT_PROFILE_COLUMNS = [
  { key: "dunk_made", label: "Dunk Made", defaultVisible: false },
  { key: "dunk_att", label: "Dunk Att", defaultVisible: false },
  { key: "dunk_pct", label: "Dunk%", defaultVisible: true },
  { key: "dunk_ast_pct", label: "Dunk Ast%", defaultVisible: false },
  { key: "rim_made", label: "Rim Made", defaultVisible: true },
  { key: "rim_att", label: "Rim Att", defaultVisible: true },
  { key: "rim_pct", label: "Rim%", defaultVisible: true },
  { key: "rim_ast_pct", label: "Rim Ast%", defaultVisible: true },
  { key: "mid_made", label: "Mid Made", defaultVisible: false },
  { key: "mid_att", label: "Mid Att", defaultVisible: false },
  { key: "mid_pct", label: "Mid%", defaultVisible: true },
  { key: "mid_ast_pct", label: "Mid Ast%", defaultVisible: true },
  { key: "two_p_made", label: "2P Made", defaultVisible: false },
  { key: "two_p_att", label: "2P Att", defaultVisible: false },
  { key: "two_p_pct", label: "2P%", defaultVisible: true },
  { key: "two_p_ast_pct", label: "2P Ast%", defaultVisible: true },
  { key: "three_p_made", label: "3P Made", defaultVisible: false },
  { key: "three_p_att", label: "3P Att", defaultVisible: false },
  { key: "three_p_pct", label: "3P%", defaultVisible: true },
  { key: "three_p_ast_pct", label: "3P Ast%", defaultVisible: true },
  { key: "ftm", label: "FTM", defaultVisible: false },
  { key: "fta", label: "FTA", defaultVisible: false },
  { key: "ft_pct", label: "FT%", defaultVisible: true },
  { key: "ftr", label: "FTr", defaultVisible: true },
  { key: "three_pr", label: "3Pr", defaultVisible: true },
  { key: "three_p_per100", label: "3PA/100", defaultVisible: true },
];

const LOWER_TIER_SHOT_PROFILE_COLUMNS = [
  { key: "rim_made", label: "Rim Made", defaultVisible: false },
  { key: "rim_att", label: "Rim Att", defaultVisible: false },
  { key: "rim_pct", label: "Rim%", defaultVisible: true },
  { key: "mid_made", label: "Mid Made", defaultVisible: false },
  { key: "mid_att", label: "Mid Att", defaultVisible: false },
  { key: "mid_pct", label: "Mid%", defaultVisible: true },
];
const LOWER_TIER_SHOT_PROFILE_LABELS = LOWER_TIER_SHOT_PROFILE_COLUMNS.reduce((labels, item) => {
  labels[item.key] = item.label;
  return labels;
}, {});
const D2_PDF_NBA_OVERRIDE_ROWS = [
  { season: "2013-14", team: "UCCS", player: "Derrick White", nba_name: "Derrick White", first_nba_year: 2017 },
  { season: "2014-15", team: "UCCS", player: "Derrick White", nba_name: "Derrick White", first_nba_year: 2017 },
  { season: "2014-15", team: "Lewis", player: "Max Strus", nba_name: "Max Strus", first_nba_year: 2019 },
  { season: "2015-16", team: "Lewis", player: "Max Strus", nba_name: "Max Strus", first_nba_year: 2019 },
  { season: "2013-14", team: "UIndy", player: "Jordan Loyd", nba_name: "Jordan Loyd", first_nba_year: 2018 },
  { season: "2014-15", team: "UIndy", player: "Jordan Loyd", nba_name: "Jordan Loyd", first_nba_year: 2018 },
  { season: "2015-16", team: "UIndy", player: "Jordan Loyd", nba_name: "Jordan Loyd", first_nba_year: 2018 },
  { season: "2014-15", team: "Lincoln Memorial", player: "Emmanuel Terry", nba_name: "Emmanuel Terry", first_nba_year: 2018 },
  { season: "2015-16", team: "Lincoln Memorial", player: "Emmanuel Terry", nba_name: "Emmanuel Terry", first_nba_year: 2018 },
  { season: "2017-18", team: "Lincoln Memorial", player: "Emmanuel Terry", nba_name: "Emmanuel Terry", first_nba_year: 2018 },
  { season: "2016-17", team: "Wheeling", player: "Haywood Highsmith", nba_name: "Haywood Highsmith", first_nba_year: 2018 },
  { season: "2017-18", team: "Wheeling", player: "Haywood Highsmith", nba_name: "Haywood Highsmith", first_nba_year: 2018 },
  { season: "2018-19", team: "Northwest Mo. St.", player: "Trevor Hudgins", nba_name: "Trevor Hudgins", first_nba_year: 2022 },
  { season: "2019-20", team: "Northwest Mo. St.", player: "Trevor Hudgins", nba_name: "Trevor Hudgins", first_nba_year: 2022 },
  { season: "2020-21", team: "Northwest Mo. St.", player: "Trevor Hudgins", nba_name: "Trevor Hudgins", first_nba_year: 2022 },
  { season: "2021-22", team: "Northwest Mo. St.", player: "Trevor Hudgins", nba_name: "Trevor Hudgins", first_nba_year: 2022 },
  { season: "2018-19", team: "Truman St.", player: "Brodric Thomas", nba_name: "Brodric Thomas", first_nba_year: 2020 },
  { season: "2019-20", team: "Truman St.", player: "Brodric Thomas", nba_name: "Brodric Thomas", first_nba_year: 2020 },
  { season: "2021-22", team: "Sonoma St.", player: "Jaylen Wells", nba_name: "Jaylen Wells", first_nba_year: 2024 },
  { season: "2022-23", team: "Sonoma St.", player: "Jaylen Wells", nba_name: "Jaylen Wells", first_nba_year: 2024 },
];

const D1_ADVANCED_COLUMNS = [
  { key: "min_per", label: "Min%", defaultVisible: false },
  { key: "ortg", label: "ORtg", defaultVisible: false },
  { key: "drtg", label: "DRtg", defaultVisible: false },
  { key: "per", label: "PER", defaultVisible: false },
  { key: "porpag", label: "PRPG", defaultVisible: true },
  { key: "dporpag", label: "dPRPG", defaultVisible: true },
  { key: "fic", label: "FIC", defaultVisible: false },
  { key: "ppr", label: "PPR", defaultVisible: false },
  { key: "bpm", label: "BPM", defaultVisible: true },
  { key: "obpm", label: "OBPM", defaultVisible: false },
  { key: "dbpm", label: "DBPM", defaultVisible: false },
  { key: "orb_pct", label: "ORB%", defaultVisible: true },
  { key: "drb_pct", label: "DRB%", defaultVisible: true },
  { key: "usg_pct", label: "USG%", defaultVisible: true },
  { key: "ast_pct", label: "AST%", defaultVisible: true },
  { key: "ast_to", label: "AST/TO", defaultVisible: true },
  { key: "tov_pct_adv", label: "TOV%", defaultVisible: true },
  { key: "stl_pct", label: "STL%", defaultVisible: true },
  { key: "blk_pct", label: "BLK%", defaultVisible: true },
];

function getD1PlaytypeMetrics(playtypeId) {
  if (["cut", "post_up"].includes(playtypeId)) {
    return D1_PLAYTYPE_METRICS.filter((metric) => !["fg_att", "three_fg_att", "three_fg_pct", "three_pr"].includes(metric.suffix));
  }
  return D1_PLAYTYPE_METRICS;
}

function getD1TransitionGroup() {
  const metricColumns = [
    { key: "transition_poss", label: "Trans Poss", defaultVisible: false },
    { key: "transition_freq", label: "Trans Freq", defaultVisible: true },
    { key: "transition_ppp", label: "Trans PPP", defaultVisible: true },
    { key: "transition_fg_att", label: "Trans FGA", defaultVisible: false },
    { key: "transition_efg_pct", label: "Trans eFG%", defaultVisible: false },
    { key: "transition_tov_pct", label: "Trans TO%", defaultVisible: true },
    { key: "transition_ftr", label: "Trans FTr", defaultVisible: true },
    { key: "transition_two_fg_att", label: "Trans 2PA", defaultVisible: false },
    { key: "transition_two_fg_pct", label: "Trans 2P%", defaultVisible: false },
    { key: "transition_three_fg_att", label: "Trans 3PA", defaultVisible: false },
    { key: "transition_three_fg_pct", label: "Trans 3P%", defaultVisible: false },
    { key: "transition_three_pr", label: "Trans 3Pr", defaultVisible: false },
  ];
  return {
    group: {
      id: "transition",
      label: "Transition",
      columns: metricColumns.map((item) => item.key),
      defaultColumns: metricColumns.filter((item) => item.defaultVisible).map((item) => item.key),
    },
    labels: Object.fromEntries(metricColumns.map((item) => [item.key, item.label])),
  };
}

function withSharedSingleFilters(filters = []) {
  return [
    ...SHARED_SINGLE_FILTERS.map((filter) => ({ ...filter, options: filter.options ? filter.options.map((option) => ({ ...option })) : undefined })),
    ...filters,
  ];
}

function withSingleFilterDefault(filters = [], filterId, defaultValue) {
  return (filters || []).map((filter) => (filter.id === filterId ? { ...filter, defaultValue } : filter));
}

function buildD1Config() {
  const demoColumns = ["conference", "pos", "class_year", "height_in", "weight_lb", "bmi", "age", "dob", "gp", "min", "mpg", "draft_pick"];
  const labels = {
    rank: "",
    season: "Year",
    player_name: "Player",
    team_name: "Team",
    conference: "Conf",
    coach: "Coach",
    pos: "Pos",
    class_year: "Class",
    height_in: "HT",
    weight_lb: "WT",
    bmi: "BMI",
    age: "Age",
    dob: "DOB",
    gp: "GP",
    min: "Min",
    mpg: "MPG",
    total_poss: "Poss",
    draft_pick: "Pick",
  };

  const groups = [
    {
      id: "advanced",
      label: "Advanced",
      columns: D1_ADVANCED_COLUMNS.map((item) => item.key),
      defaultColumns: D1_ADVANCED_COLUMNS.filter((item) => item.defaultVisible).map((item) => item.key),
    },
    {
      id: "shot_profile",
      label: "Shot Profile",
      columns: D1_SHOT_PROFILE_COLUMNS.map((item) => item.key),
      defaultColumns: D1_SHOT_PROFILE_COLUMNS.filter((item) => item.defaultVisible).map((item) => item.key),
    },
    {
      id: "summary",
      label: "Playtype Summary",
      columns: D1_SUMMARY_COLUMNS.map((item) => item.key),
      defaultColumns: D1_SUMMARY_COLUMNS.filter((item) => item.defaultVisible).map((item) => item.key),
    },
  ];
  const playtypeGroups = [];
  const playtypeAnalysisColumns = [];

  D1_SUMMARY_COLUMNS.forEach((item) => {
    labels[item.key] = item.label;
  });
  D1_SHOT_PROFILE_COLUMNS.forEach((item) => {
    labels[item.key] = item.label;
  });
  D1_ADVANCED_COLUMNS.forEach((item) => {
    labels[item.key] = item.label;
  });
  labels.nba_career_epm = "NBA EPM";
  D1_DRIVE_COLUMNS.forEach((item) => {
    labels[item.key] = item.label;
  });
  Object.assign(labels, {
    runner_freq: "Runner Freq",
    runner_ppp: "Runner PPP",
    runner_ftr: "Runner FTr",
    runner_plus1_pct: "Runner +1%",
    runner_two_fg_att: "Runner 2PA",
    runner_two_fg_pct: "Runner 2P%",
  });
  Object.assign(labels, getD1TransitionGroup().labels);

  D1_PLAYTYPE_DEFS.forEach((playtype) => {
    const metrics = getD1PlaytypeMetrics(playtype.id);
    const columns = metrics.map((metric) => `${playtype.id}_${metric.suffix}`);
    const defaultColumns = metrics.filter((metric) => metric.defaultVisible).map((metric) => `${playtype.id}_${metric.suffix}`);
    playtypeGroups.push({
      id: playtype.id,
      label: playtype.label,
      columns,
      defaultColumns,
    });
    if (D1_TRUE_PLAYTYPE_IDS.includes(playtype.id)) {
      playtypeAnalysisColumns.push(...defaultColumns);
    }

    metrics.forEach((metric) => {
      labels[`${playtype.id}_${metric.suffix}`] = `${playtype.short} ${metric.label}`;
    });
  });

  const transitionGroup = getD1TransitionGroup();
  playtypeGroups.push(transitionGroup.group);
  playtypeAnalysisColumns.push(...transitionGroup.group.defaultColumns);
  groups.push({
    id: "playtype_analysis",
    label: "Playtype Analysis",
    columns: [...new Set(playtypeAnalysisColumns)],
    defaultColumns: [...new Set(playtypeAnalysisColumns)],
  });
  groups.push(...playtypeGroups);
  groups.push(
    {
      id: "scoring_data",
      label: "Scoring Data",
      columns: [
        ...D1_DRIVE_COLUMNS.map((item) => item.key),
        "runner_freq",
        "runner_ppp",
        "runner_ftr",
        "runner_plus1_pct",
        "runner_two_fg_att",
        "runner_two_fg_pct",
      ],
      defaultColumns: [
        ...D1_DRIVE_COLUMNS.filter((item) => item.defaultVisible).map((item) => item.key),
        "runner_freq",
        "runner_ppp",
        "runner_ftr",
      ],
    },
  );
  const d1AdvancedGroup = groups.find((group) => group.id === "advanced");
  if (d1AdvancedGroup && !d1AdvancedGroup.columns.includes("nba_career_epm")) {
    d1AdvancedGroup.columns.push("nba_career_epm");
  }

  const defaultVisible = groups
    .filter((group) => ["summary", "shot_profile", "advanced"].includes(group.id))
    .flatMap((group) => group.defaultColumns);

  return {
    id: "d1",
    navLabel: "D1",
    title: "D1",
    subtitle: "",
    dataScript: "data/d1_enriched_all_seasons.js",
    deferredExtraScripts: [
      "data/vendor/d1_frontend_data.js",
      "data/vendor/d1_drives.js",
      "data/vendor/d1_extras.js",
      "data/vendor/d1_rosters.js",
      "data/vendor/d1_coachdict.js",
    ],
    deferSupplementHydration: true,
    autoHydrateDeferred: false,
    globalName: "D1_ENRICHED_ALL_CSV",
    precomputed: false,
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player_name", "team_name"],
    searchColumns: ["player_name", "team_name", "conference", "coach", "team_search_text", "coach_search_text"],
    sortBy: "min",
    sortDir: "desc",
    demoColumns,
    demoFilterColumns: ["age", "height_in", "weight_lb", "bmi", "dob", "gp", "min", "mpg", "draft_pick"],
    groups,
    singleFilters: withSharedSingleFilters([
      { id: "conference_bucket", label: "Conference" },
      {
        id: "status_path",
        label: "Status",
        options: [
          { value: "all", label: "All" },
          { value: "nba", label: "NBA" },
          { value: "former_juco", label: "Former JUCO" },
          { value: "former_d2", label: "Former D2" },
          { value: "former_naia", label: "Former NAIA" },
        ],
      },
    ]),
    multiFilters: [
      { id: "pos", label: "Pos", column: "pos", sort: ["PG", "SG", "SF", "PF", "C"] },
      { id: "class_year", label: "Class", column: "class_year", sort: ["Fr", "So", "Jr", "Sr", "Gr"] },
    ],
    defaultVisible: [...new Set([...demoColumns.filter((column) => !["conference", "bmi", "draft_pick", "weight_lb", "mpg", "dob"].includes(column)), ...defaultVisible])],
    labels,
  };
}

function buildLowerTierShotProfileGroup() {
  return {
    id: "shot_profile",
    label: "Shot Profile",
    columns: LOWER_TIER_SHOT_PROFILE_COLUMNS.map((item) => item.key),
    defaultColumns: LOWER_TIER_SHOT_PROFILE_COLUMNS.filter((item) => item.defaultVisible).map((item) => item.key),
  };
}

const NBA_COMPANION_GROUPS = [
  {
    id: "summary",
    label: "Summary",
    columns: ["ncaa_gp", "ncaa_min", "ncaa_mpg", "ncaa_height_in", "nba_height_in", "ncaa_weight_lb", "nba_weight_lb", "ncaa_bmi", "nba_bmi", "ncaa_porpag", "ncaa_dporpag", "ncaa_bpm", "nba_gp", "nba_mp", "nba_mpg", "nba_epm", "nba_oepm", "nba_depm", "nba_ewins"],
    defaultColumns: ["ncaa_gp", "ncaa_min", "ncaa_porpag", "ncaa_dporpag", "ncaa_bpm", "nba_gp", "nba_mp", "nba_epm", "nba_oepm", "nba_depm", "nba_ewins"],
  },
  {
    id: "advanced",
    label: "Advanced",
    columns: ["ncaa_orb_pct", "nba_orb_pct", "ncaa_drb_pct", "nba_drb_pct", "ncaa_usg_pct", "nba_usg_pct", "ncaa_ast_pct", "nba_ast_pct", "ncaa_ast_to", "nba_ast_to", "ncaa_tov_pct", "nba_tov_pct", "ncaa_stl_pct", "nba_stl_pct", "ncaa_blk_pct", "nba_blk_pct", "ncaa_ts_pct", "nba_ts_pct", "ncaa_efg_pct", "nba_efg_pct"],
    defaultColumns: ["ncaa_usg_pct", "nba_usg_pct", "ncaa_ast_pct", "nba_ast_pct", "ncaa_ast_to", "nba_ast_to", "ncaa_tov_pct", "nba_tov_pct", "ncaa_stl_pct", "nba_stl_pct", "ncaa_blk_pct", "nba_blk_pct", "ncaa_ts_pct", "nba_ts_pct", "ncaa_efg_pct", "nba_efg_pct"],
  },
  {
    id: "shooting",
    label: "Shot Profile",
    columns: ["ncaa_rim_pct", "nba_rim_pct", "ncaa_rim_ast_pct", "nba_rim_ast_pct", "ncaa_mid_pct", "nba_mid_pct", "ncaa_mid_ast_pct", "nba_mid_ast_pct", "ncaa_two_p_pct", "nba_two_p_pct", "ncaa_two_p_ast_pct", "nba_two_p_ast_pct", "ncaa_three_p_pct", "nba_three_p_pct", "ncaa_three_p_ast_pct", "nba_three_p_ast_pct", "ncaa_ft_pct", "nba_ft_pct", "ncaa_ftr", "nba_ftr", "ncaa_three_pr", "nba_three_pr"],
    defaultColumns: ["ncaa_rim_pct", "nba_rim_pct", "ncaa_rim_ast_pct", "nba_rim_ast_pct", "ncaa_two_p_pct", "nba_two_p_pct", "ncaa_two_p_ast_pct", "nba_two_p_ast_pct", "ncaa_three_p_pct", "nba_three_p_pct", "ncaa_three_p_ast_pct", "nba_three_p_ast_pct", "ncaa_ft_pct", "nba_ft_pct", "ncaa_ftr", "nba_ftr", "ncaa_three_pr", "nba_three_pr"],
  },
];

const NBA_COMPANION_LABELS = {
  ncaa_gp: "NCAA GP",
  ncaa_min: "NCAA Min",
  ncaa_mpg: "NCAA MPG",
  ncaa_height_in: "NCAA HT",
  nba_height_in: "NBA HT",
  ncaa_weight_lb: "NCAA WT",
  nba_weight_lb: "NBA WT",
  ncaa_bmi: "NCAA BMI",
  nba_bmi: "NBA BMI",
  ncaa_porpag: "NCAA PRPG",
  ncaa_dporpag: "NCAA dPRPG",
  ncaa_bpm: "NCAA BPM",
  nba_gp: "NBA GP",
  nba_mp: "NBA MP",
  nba_mpg: "NBA MPG",
  nba_epm: "NBA EPM",
  nba_oepm: "NBA oEPM",
  nba_depm: "NBA dEPM",
  nba_ewins: "NBA EW",
  ncaa_orb_pct: "NCAA ORB%",
  nba_orb_pct: "NBA ORB%",
  ncaa_drb_pct: "NCAA DRB%",
  nba_drb_pct: "NBA DRB%",
  ncaa_usg_pct: "NCAA USG%",
  nba_usg_pct: "NBA USG%",
  ncaa_ast_pct: "NCAA AST%",
  nba_ast_pct: "NBA AST%",
  ncaa_ast_to: "NCAA AST/TO",
  nba_ast_to: "NBA AST/TO",
  ncaa_tov_pct: "NCAA TOV%",
  nba_tov_pct: "NBA TOV%",
  ncaa_stl_pct: "NCAA STL%",
  nba_stl_pct: "NBA STL%",
  ncaa_blk_pct: "NCAA BLK%",
  nba_blk_pct: "NBA BLK%",
  ncaa_ts_pct: "NCAA TS%",
  nba_ts_pct: "NBA TS%",
  ncaa_efg_pct: "NCAA eFG%",
  nba_efg_pct: "NBA eFG%",
  ncaa_rim_pct: "NCAA Rim%",
  nba_rim_pct: "NBA Rim%",
  ncaa_rim_ast_pct: "NCAA Rim Ast%",
  nba_rim_ast_pct: "NBA Rim Ast%",
  ncaa_mid_pct: "NCAA Mid%",
  nba_mid_pct: "NBA Mid%",
  ncaa_mid_ast_pct: "NCAA Mid Ast%",
  nba_mid_ast_pct: "NBA Mid Ast%",
  ncaa_two_p_pct: "NCAA 2P%",
  nba_two_p_pct: "NBA 2P%",
  ncaa_two_p_ast_pct: "NCAA 2P Ast%",
  nba_two_p_ast_pct: "NBA 2P Ast%",
  ncaa_three_p_pct: "NCAA 3P%",
  nba_three_p_pct: "NBA 3P%",
  ncaa_three_p_ast_pct: "NCAA 3P Ast%",
  nba_three_p_ast_pct: "NBA 3P Ast%",
  ncaa_ft_pct: "NCAA FT%",
  nba_ft_pct: "NBA FT%",
  ncaa_ftr: "NCAA FTr",
  nba_ftr: "NBA FTr",
  ncaa_three_pr: "NCAA 3Pr",
  nba_three_pr: "NBA 3Pr",
};

const NBA_COMPANION_COPY_MAP = [
  ["ncaa_gp", "gp"],
  ["ncaa_min", "min"],
  ["ncaa_mpg", "mpg"],
  ["ncaa_height_in", "height_in"],
  ["ncaa_weight_lb", "weight_lb"],
  ["ncaa_bmi", "bmi"],
  ["ncaa_porpag", "porpag"],
  ["ncaa_dporpag", "dporpag"],
  ["ncaa_bpm", "bpm"],
  ["ncaa_orb_pct", "orb_pct"],
  ["ncaa_drb_pct", "drb_pct"],
  ["ncaa_usg_pct", "usg_pct"],
  ["ncaa_ast_pct", "ast_pct"],
  ["ncaa_ast_to", "ast_to"],
  ["ncaa_tov_pct", "tov_pct_adv"],
  ["ncaa_stl_pct", "stl_pct"],
  ["ncaa_blk_pct", "blk_pct"],
  ["ncaa_ts_pct", "ts_pct"],
  ["ncaa_efg_pct", "efg_pct"],
  ["ncaa_rim_pct", "rim_pct"],
  ["ncaa_rim_ast_pct", "rim_ast_pct"],
  ["ncaa_mid_pct", "mid_pct"],
  ["ncaa_mid_ast_pct", "mid_ast_pct"],
  ["ncaa_two_p_pct", "two_p_pct"],
  ["ncaa_two_p_ast_pct", "two_p_ast_pct"],
  ["ncaa_three_p_pct", "three_p_pct"],
  ["ncaa_three_p_ast_pct", "three_p_ast_pct"],
  ["ncaa_ft_pct", "ft_pct"],
  ["ncaa_ftr", "ftr"],
  ["ncaa_three_pr", "three_pr"],
];

const NBA_COMPANION_NBA_COPY_MAP = [
  ["nba_gp", "gp"],
  ["nba_mp", "mp"],
  ["nba_mpg", "mpg"],
  ["nba_height_in", "inches"],
  ["nba_weight_lb", "weight"],
  ["nba_bmi", "bmi"],
  ["nba_epm", "tot"],
  ["nba_oepm", "off"],
  ["nba_depm", "def"],
  ["nba_ewins", "ewins"],
  ["nba_orb_pct", "orbpct"],
  ["nba_drb_pct", "drbpct"],
  ["nba_usg_pct", "usg"],
  ["nba_ast_pct", "astpct"],
  ["nba_ast_to", "ast_to"],
  ["nba_tov_pct", "topct"],
  ["nba_stl_pct", "stlpct"],
  ["nba_blk_pct", "blkpct"],
  ["nba_ts_pct", "tspct"],
  ["nba_efg_pct", "efg"],
  ["nba_rim_pct", "fgpct_rim"],
  ["nba_rim_ast_pct", "rim_ast_pct"],
  ["nba_mid_pct", "fgpct_mid"],
  ["nba_mid_ast_pct", "mid_ast_pct"],
  ["nba_two_p_pct", "fg2pct"],
  ["nba_two_p_ast_pct", "two_ast_pct"],
  ["nba_three_p_pct", "fg3pct"],
  ["nba_three_p_ast_pct", "three_ast_pct"],
  ["nba_ft_pct", "ftpct"],
  ["nba_ftr", "ftr"],
  ["nba_three_pr", "three_pr"],
];

function buildNbaCompanionConfig() {
  const demoColumns = ["conference", "pos", "class_year", "height_in", "weight_lb", "bmi", "age", "dob", "gp", "min", "mpg", "draft_pick"];
  return {
    id: "nba_companion",
    navLabel: "NBA Companion",
    title: "NBA Companion",
    subtitle: "",
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player_name", "team_name"],
    searchColumns: ["player_name", "team_name", "conference", "coach", "team_search_text", "coach_search_text"],
    sortBy: "nba_epm",
    sortDir: "desc",
    demoColumns,
    demoFilterColumns: ["age", "height_in", "weight_lb", "bmi", "dob", "gp", "min", "mpg", "draft_pick"],
    groups: NBA_COMPANION_GROUPS,
    singleFilters: withSharedSingleFilters([{ id: "conference_bucket", label: "Conference" }]),
    multiFilters: [
      { id: "pos", label: "Pos", column: "pos", sort: ["PG", "SG", "SF", "PF", "C"] },
      { id: "class_year", label: "Class", column: "class_year", sort: ["Fr", "So", "Jr", "Sr", "Gr"] },
    ],
    defaultVisible: [...new Set([...demoColumns.filter((column) => !["conference", "bmi", "draft_pick", "weight_lb", "mpg", "dob"].includes(column)), ...NBA_COMPANION_GROUPS.flatMap((group) => group.defaultColumns)])],
    labels: {
      rank: "",
      season: "Year",
      player_name: "Player",
      team_name: "Team",
      conference: "Conf",
      coach: "Coach",
      pos: "Pos",
      class_year: "Class",
      height_in: "HT",
      weight_lb: "WT",
      bmi: "BMI",
      age: "Age",
      dob: "DOB",
      gp: "GP",
      min: "Min",
      mpg: "MPG",
      draft_pick: "Pick",
      ...NBA_COMPANION_LABELS,
    },
  };
}

const DATASETS = {
  d1: buildD1Config(),
  d2: {
    id: "d2",
    navLabel: "D2",
    title: "D2",
    subtitle: "",
    dataScript: "data/d2_all_seasons.js",
    extraScripts: ["data/vendor/d2_bio_lookup.js"],
    globalName: "D2_ALL_CSV",
    yearColumn: "season",
    playerColumn: "player",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player", "team_name"],
    searchColumns: ["player", "team_name", "team_search_text"],
    sortBy: "pts_per40",
    sortDir: "desc",
    demoColumns: ["pos", "class_year", "height_in", "weight_lb", "gp", "min", "mpg"],
    demoFilterColumns: [],
    groups: [
      { id: "summary", label: "Summary", columns: ["gp", "min", "mpg", "adjoe", "adrtg", "porpag", "dporpag", "per", "fic", "ppr", "nba_career_epm"], defaultColumns: ["gp", "min", "mpg", "adjoe", "adrtg", "porpag", "dporpag"] },
      { id: "advanced", label: "Advanced", columns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"], defaultColumns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"] },
      { id: "shooting", label: "Shooting", columns: ["fg_pct", "two_pm", "two_pa", "two_p_pct", "3pm", "3pa", "3p_pct", "ftm", "fta", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr", "three_pa_per100"], defaultColumns: ["fg_pct", "two_p_pct", "3p_pct", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr", "three_pa_per100"] },
      buildLowerTierShotProfileGroup(),
      { id: "per40", label: "Per 40", columns: ["pts_per40", "trb_per40", "ast_per40", "tov_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"], defaultColumns: ["pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"] },
      { id: "per_game", label: "Per Game", columns: ["pts_pg", "trb_pg", "ast_pg", "tov_pg", "stl_pg", "blk_pg", "stocks_pg", "two_pa_pg", "three_pa_pg"], defaultColumns: ["pts_pg", "trb_pg", "ast_pg", "stl_pg"] },
      { id: "d1_peak", label: "D1 Peak", columns: ["d1_peak_prpg", "d1_peak_dprpg", "d1_peak_bpm"], defaultColumns: [] },
    ],
    singleFilters: withSharedSingleFilters([{ id: "status_path", label: "Status", options: [{ value: "all", label: "All" }, { value: "d1", label: "D1" }, { value: "formerd1", label: "Former D1" }, { value: "nba", label: "NBA" }] }]),
    multiFilters: [
      { id: "pos", label: "Pos", column: "pos", sort: ["PG", "SG", "SF", "PF", "C"] },
      { id: "class_year", label: "Class", column: "class_year", sort: ["Fr", "So", "Jr", "Sr", "Gr"] },
    ],
    defaultVisible: ["rank", "season", "player", "team_name", "class_year", "gp", "min", "adjoe", "adrtg", "porpag", "dporpag", "min_per", "usg_pct", "fg_pct", "two_p_pct", "3p_pct", "efg_pct", "ts_pct", "ft_pct", "ftr", "three_pr", "rim_pct", "mid_pct", "three_pa_per100", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct", "pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"],
    labels: {
      rank: "",
      season: "Year",
      player: "Player",
      team_name: "Team",
      pos: "Pos",
      class_year: "Class",
      height_in: "HT",
      weight_lb: "WT",
      gp: "GP",
      min: "MIN",
      mpg: "MPG",
      min_per: "Min%",
      pts_pg: "PTS/G",
      trb_pg: "TRB/G",
      ast_pg: "AST/G",
      tov_pg: "TOV/G",
      stl_pg: "STL/G",
      blk_pg: "BLK/G",
      stocks_pg: "Stocks/G",
      two_pa_pg: "2PA/G",
      three_pa_pg: "3PA/G",
      pts_per40: "PTS/40",
      trb_per40: "TRB/40",
      ast_per40: "AST/40",
      tov_per40: "TOV/40",
      stl_per40: "STL/40",
      blk_per40: "BLK/40",
      stocks_per40: "Stocks/40",
      two_pm: "2PM",
      two_pa: "2PA",
      "3pm": "3PM",
      "3pa": "3PA",
      ftm: "FTM",
      fta: "FTA",
      two_pa_per40: "2PA/40",
      three_pa_per40: "3PA/40",
      fg_pct: "FG%",
      two_p_pct: "2P%",
      "3p_pct": "3P%",
      ft_pct: "FT%",
      efg_pct: "eFG%",
      ts_pct: "TS%",
      adjoe: "AdjO",
      adrtg: "AdjD",
      porpag: "PRPG",
      dporpag: "dPRPG",
      per: "PER",
      fic: "FIC",
      ppr: "PPR",
      nba_career_epm: "NBA EPM",
      orb_pct: "ORB%",
      drb_pct: "DRB%",
      trb_pct: "TRB%",
      ast_pct: "AST%",
      tov_pct: "TOV%",
      stl_pct: "STL%",
      blk_pct: "BLK%",
      usg_pct: "USG%",
      ast_to: "AST/TO",
      ftr: "FTr",
      three_pr: "3Pr",
      three_pa_per100: "3PA/100",
      d1_peak_prpg: "D1 Peak PRPG",
      d1_peak_dprpg: "D1 Peak dPRPG",
      d1_peak_bpm: "D1 Peak BPM",
      ...LOWER_TIER_SHOT_PROFILE_LABELS,
    },
  },
  naia: {
    id: "naia",
    navLabel: "NAIA",
    title: "NAIA",
    subtitle: "",
    dataScript: "data/vendor/naia_all_seasons.js",
    extraScripts: ["data/vendor/naia_massey_team_ratings.js", "data/vendor/naia_divisions.js"],
    globalName: "NAIA_ALL_CSV",
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player_name", "team_name"],
    searchColumns: ["player_name", "team_name", "conference", "division", "team_search_text"],
    sortBy: "min",
    sortDir: "desc",
    demoColumns: ["division", "conference", "age", "height_in", "weight_lb", "bmi", "dob", "gp", "min", "mpg"],
    demoFilterColumns: [],
    groups: [
      { id: "summary", label: "Summary", columns: ["gp", "min", "mpg", "adjoe", "adrtg", "porpag", "dporpag", "per", "fic", "ppr", "nba_career_epm"], defaultColumns: ["gp", "min", "mpg", "adjoe", "adrtg", "porpag", "dporpag"] },
      { id: "advanced", label: "Advanced", columns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"], defaultColumns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"] },
      { id: "shooting", label: "Shooting", columns: ["fg_pct", "2pm", "2pa", "2p_pct", "tpm", "tpa", "tp_pct", "ftm", "fta", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr"], defaultColumns: ["fg_pct", "2p_pct", "tp_pct", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr"] },
      buildLowerTierShotProfileGroup(),
      { id: "per40", label: "Per 40", columns: ["pts_per40", "trb_per40", "ast_per40", "tov_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"], defaultColumns: ["pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"] },
      { id: "per_game", label: "Per Game", columns: ["pts_pg", "trb_pg", "ast_pg", "tov_pg", "stl_pg", "blk_pg", "stocks_pg", "two_pa_pg", "three_pa_pg"], defaultColumns: ["pts_pg", "trb_pg", "ast_pg", "stl_pg"] },
      { id: "d1_peak", label: "D1 Peak", columns: ["d1_peak_prpg", "d1_peak_dprpg", "d1_peak_bpm"], defaultColumns: [] },
    ],
    singleFilters: withSharedSingleFilters([
      { id: "division", label: "Division", column: "division" },
      { id: "conference", label: "Conference", column: "conference" },
      { id: "status_path", label: "Status", options: [{ value: "all", label: "All" }, { value: "d1", label: "D1" }, { value: "formerd1", label: "Former D1" }] },
    ]),
    minYear: 2011,
    defaultVisible: ["rank", "season", "player_name", "team_name", "division", "age", "height_in", "gp", "min", "adjoe", "adrtg", "porpag", "dporpag", "min_per", "usg_pct", "fg_pct", "2p_pct", "tp_pct", "efg_pct", "ts_pct", "ft_pct", "ftr", "three_pr", "rim_pct", "mid_pct", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct", "pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"],
    labels: {
      rank: "",
      season: "Year",
      player_name: "Player",
      team_name: "Team",
      division: "Div",
      conference: "Conf",
      age: "Age",
      height_in: "HT",
      weight_lb: "WT",
      bmi: "BMI",
      dob: "DOB",
      gp: "GP",
      min: "MIN",
      mpg: "MPG",
      min_per: "Min%",
      pts_pg: "PTS/G",
      trb_pg: "TRB/G",
      ast_pg: "AST/G",
      tov_pg: "TOV/G",
      stl_pg: "STL/G",
      blk_pg: "BLK/G",
      stocks_pg: "Stocks/G",
      two_pa_pg: "2PA/G",
      three_pa_pg: "3PA/G",
      pts_per40: "PTS/40",
      trb_per40: "TRB/40",
      ast_per40: "AST/40",
      tov_per40: "TOV/40",
      stl_per40: "STL/40",
      blk_per40: "BLK/40",
      stocks_per40: "Stocks/40",
      "2pm": "2PM",
      "2pa": "2PA",
      tpm: "3PM",
      tpa: "3PA",
      ftm: "FTM",
      fta: "FTA",
      two_pa_per40: "2PA/40",
      three_pa_per40: "3PA/40",
      fg_pct: "FG%",
      tp_pct: "3P%",
      ft_pct: "FT%",
      "2p_pct": "2P%",
      ts_pct: "TS%",
      efg_pct: "eFG%",
      adjoe: "AdjO",
      adrtg: "AdjD",
      porpag: "PRPG",
      dporpag: "dPRPG",
      per: "PER",
      fic: "FIC",
      ppr: "PPR",
      nba_career_epm: "NBA EPM",
      orb_pct: "ORB%",
      drb_pct: "DRB%",
      trb_pct: "TRB%",
      ast_pct: "AST%",
      stl_pct: "STL%",
      blk_pct: "BLK%",
      tov_pct: "TOV%",
      usg_pct: "USG%",
      ast_to: "AST/TO",
      ftr: "FTr",
      three_pr: "3Pr",
      d1_peak_prpg: "D1 Peak PRPG",
      d1_peak_dprpg: "D1 Peak dPRPG",
      d1_peak_bpm: "D1 Peak BPM",
      ...LOWER_TIER_SHOT_PROFILE_LABELS,
    },
  },
  juco: {
    id: "juco",
    navLabel: "JUCO",
    title: "JUCO",
    subtitle: "NJCAA only",
    dataScript: "data/vendor/juco_all_seasons.js",
    globalName: "NJCAA_ALL_CSV",
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player_name", "team_name"],
    searchColumns: ["player_name", "team_name", "level", "region", "team_search_text"],
    sortBy: "min",
    sortDir: "desc",
    demoColumns: ["level", "region", "age", "height_in", "weight_lb", "bmi", "dob", "gp", "min", "mpg"],
    demoFilterColumns: ["age", "height_in", "weight_lb", "bmi", "gp", "min", "mpg", "dob"],
    groups: [
      { id: "summary", label: "Summary", columns: ["adjoe", "adrtg", "porpag", "dporpag", "per", "fic", "ppr", "nba_career_epm"], defaultColumns: ["adjoe", "adrtg", "porpag", "dporpag"] },
      { id: "advanced", label: "Advanced", columns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"], defaultColumns: ["min_per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"] },
      { id: "shooting", label: "Shooting", columns: ["fg_pct", "2pm", "2pa", "2p_pct", "tpm", "tpa", "tp_pct", "ftm", "fta", "ft_pct", "ts_pct", "efg_pct", "ftr", "three_pr"], defaultColumns: ["fg_pct", "2p_pct", "tp_pct", "efg_pct", "ts_pct", "ft_pct", "ftr", "three_pr"] },
      buildLowerTierShotProfileGroup(),
      { id: "per40", label: "Per 40", columns: ["pts_per40", "trb_per40", "ast_per40", "tov_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"], defaultColumns: ["pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"] },
      { id: "per_game", label: "Per Game", columns: ["pts_pg", "trb_pg", "ast_pg", "tov_pg", "stl_pg", "blk_pg", "stocks_pg", "two_pa_pg", "three_pa_pg"], defaultColumns: ["pts_pg", "trb_pg", "ast_pg", "stl_pg"] },
      { id: "projection", label: "NCAA Projection", columns: ["ncaa_spi_peak", "ncaa_spi_pct_peak", "ncaa_off_impact_peak", "ncaa_off_impact_pct_peak", "ncaa_def_impact_peak", "ncaa_def_impact_pct_peak", "ncaa_wins_added_peak", "ncaa_wins_added_pct_peak"], defaultColumns: ["ncaa_spi_peak"] },
      { id: "d1_peak", label: "D1 Peak", columns: ["d1_peak_prpg", "d1_peak_dprpg", "d1_peak_bpm"], defaultColumns: [] },
    ],
    singleFilters: withSharedSingleFilters([
      { id: "level", label: "Division", column: "level" },
      { id: "region", label: "Region", column: "region" },
      { id: "status_path", label: "Status", options: [{ value: "all", label: "All" }, { value: "d1", label: "D1" }, { value: "formerd1", label: "Former D1" }, { value: "nba", label: "NBA" }] },
    ]),
    defaultVisible: ["rank", "season", "player_name", "team_name", "gp", "min", "adjoe", "adrtg", "porpag", "dporpag", "usg_pct", "min_per", "fg_pct", "2p_pct", "tp_pct", "efg_pct", "ts_pct", "ft_pct", "ftr", "three_pr", "rim_pct", "mid_pct", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct", "pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40"],
    labels: {
      rank: "",
      season: "Year",
      player_name: "Player",
      team_name: "Team",
      level: "Div",
      region: "Region",
      age: "Age",
      gp: "GP",
      min: "MIN",
      mpg: "MPG",
      min_per: "Min%",
      height_in: "HT",
      weight_lb: "WT",
      bmi: "BMI",
      dob: "DOB",
      pts_pg: "PTS/G",
      trb_pg: "TRB/G",
      ast_pg: "AST/G",
      stl_pg: "STL/G",
      blk_pg: "BLK/G",
      tov_pg: "TOV/G",
      stocks_pg: "Stocks/G",
      two_pa_pg: "2PA/G",
      three_pa_pg: "3PA/G",
      pts_per40: "PTS/40",
      trb_per40: "TRB/40",
      ast_per40: "AST/40",
      tov_per40: "TOV/40",
      stl_per40: "STL/40",
      blk_per40: "BLK/40",
      stocks_per40: "Stocks/40",
      "2pm": "2PM",
      "2pa": "2PA",
      tpm: "3PM",
      tpa: "3PA",
      ftm: "FTM",
      fta: "FTA",
      two_pa_per40: "2PA/40",
      three_pa_per40: "3PA/40",
      fg_pct: "FG%",
      tp_pct: "3P%",
      ft_pct: "FT%",
      "2p_pct": "2P%",
      ts_pct: "TS%",
      efg_pct: "eFG%",
      adjoe: "AdjO",
      adrtg: "AdjD",
      porpag: "PRPG",
      dporpag: "dPRPG",
      per: "PER",
      fic: "FIC",
      ppr: "PPR",
      nba_career_epm: "NBA EPM",
      orb_pct: "ORB%",
      drb_pct: "DRB%",
      trb_pct: "TRB%",
      ast_pct: "AST%",
      stl_pct: "STL%",
      blk_pct: "BLK%",
      tov_pct: "TOV%",
      usg_pct: "USG%",
      ast_to: "AST/TO",
      ftr: "FTr",
      three_pr: "3Pr",
      three_pa_per100: "3PA/100",
      ncaa_spi_peak: "NCAA SPI Peak",
      ncaa_spi_pct_peak: "NCAA SPI %",
      ncaa_off_impact_peak: "NCAA Off Peak",
      ncaa_off_impact_pct_peak: "NCAA Off %",
      ncaa_def_impact_peak: "NCAA Def Peak",
      ncaa_def_impact_pct_peak: "NCAA Def %",
      ncaa_wins_added_peak: "NCAA Wins Peak",
      ncaa_wins_added_pct_peak: "NCAA Wins %",
      d1_peak_prpg: "D1 Peak PRPG",
      d1_peak_dprpg: "D1 Peak dPRPG",
      d1_peak_bpm: "D1 Peak BPM",
      ...LOWER_TIER_SHOT_PROFILE_LABELS,
    },
  },
  fiba: {
    id: "fiba",
    navLabel: "FIBA",
    title: "FIBA",
    subtitle: "",
    dataScript: "data/fiba_all_seasons.js",
    globalName: "FIBA_ALL_CSV",
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_name",
    lockedColumns: ["rank", "season", "player_name", "team_name"],
    searchColumns: ["player_name", "team_name", "competition_label", "team_search_text"],
    sortBy: "pts_per40",
    sortDir: "desc",
    defaultAllYears: true,
    minYear: 1995,
    minuteDefault: 120,
    demoColumns: ["competition_label", "pos", "height_in", "age", "dob", "gp", "min", "mpg"],
    demoFilterColumns: ["height_in", "age", "dob", "gp", "min", "mpg"],
    groups: [
      { id: "summary", label: "Summary", columns: ["gp", "min", "mpg", "eff_pg", "plus_minus_pg"], defaultColumns: ["min", "mpg", "eff_pg", "plus_minus_pg"] },
      { id: "advanced", label: "Advanced", columns: ["min_per", "adjoe", "adrtg", "porpag", "dporpag", "per", "rgm_per", "fic", "ppr", "nba_career_epm", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"], defaultColumns: ["min_per", "adjoe", "adrtg", "porpag", "dporpag", "per", "orb_pct", "drb_pct", "trb_pct", "usg_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct"] },
      { id: "shooting", label: "Shooting", columns: ["fg_pct", "2pm", "2pa", "2p_pct", "3pm", "3pa", "tp_pct", "ftm", "fta", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr"], defaultColumns: ["fg_pct", "2p_pct", "tp_pct", "ft_pct", "efg_pct", "ts_pct", "ftr", "three_pr"] },
      buildLowerTierShotProfileGroup(),
      { id: "per40", label: "Per 40", columns: ["pts_per40", "trb_per40", "ast_per40", "tov_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"], defaultColumns: ["pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"] },
      { id: "per_game", label: "Per Game", columns: ["pts_pg", "trb_pg", "ast_pg", "tov_pg", "stl_pg", "blk_pg", "stocks_pg", "two_pa_pg", "three_pa_pg"], defaultColumns: ["pts_pg", "trb_pg", "ast_pg", "stl_pg", "blk_pg"] },
    ],
    singleFilters: withSingleFilterDefault(withSharedSingleFilters([
      { id: "competition_label", label: "Competition", column: "competition_label" },
      { id: "status_path", label: "Status", options: [{ value: "all", label: "All" }, { value: "d1", label: "D1" }, { value: "nba", label: "NBA" }] },
    ]), "color_mode", "competition_position"),
    multiFilters: [{ id: "pos", label: "Pos", column: "pos", sort: ["PG", "SG", "SF", "PF", "C", "G", "F", "C/F", "F/C"] }],
    defaultVisible: ["rank", "season", "player_name", "team_name", "competition_label", "pos", "min", "mpg", "eff_pg", "plus_minus_pg", "min_per", "adjoe", "adrtg", "porpag", "dporpag", "per", "fg_pct", "2p_pct", "tp_pct", "efg_pct", "ts_pct", "ft_pct", "ftr", "three_pr", "rim_pct", "mid_pct", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "ast_to", "tov_pct", "stl_pct", "blk_pct", "pts_per40", "trb_per40", "ast_per40", "stl_per40", "blk_per40", "stocks_per40", "two_pa_per40", "three_pa_per40"],
    labels: {
      rank: "",
      season: "Year",
      player_name: "Player",
      team_name: "Team",
      competition_label: "Comp",
      nationality: "Country",
      pos: "Pos",
      height_in: "HT",
      age: "Age",
      dob: "DOB",
      gp: "GP",
      min: "MIN",
      mpg: "MPG",
      min_per: "Min%",
      adjoe: "AdjO",
      adrtg: "AdjD",
      porpag: "PRPG",
      dporpag: "dPRPG",
      per: "PER",
      rgm_per: "RGM PER",
      fic: "FIC",
      ppr: "PPR",
      nba_career_epm: "NBA EPM",
      eff_pg: "Eff/G",
      plus_minus_pg: "+/-/G",
      pts_pg: "PTS/G",
      trb_pg: "TRB/G",
      ast_pg: "AST/G",
      tov_pg: "TOV/G",
      stl_pg: "STL/G",
      blk_pg: "BLK/G",
      stocks_pg: "Stocks/G",
      two_pa_pg: "2PA/G",
      three_pa_pg: "3PA/G",
      pts_per40: "PTS/40",
      trb_per40: "TRB/40",
      ast_per40: "AST/40",
      tov_per40: "TOV/40",
      stl_per40: "STL/40",
      blk_per40: "BLK/40",
      stocks_per40: "Stocks/40",
      "2pm": "2PM",
      "2pa": "2PA",
      "3pm": "3PM",
      "3pa": "3PA",
      ftm: "FTM",
      fta: "FTA",
      two_pa_per40: "2PA/40",
      three_pa_per40: "3PA/40",
      fg_pct: "FG%",
      "2p_pct": "2P%",
      tp_pct: "3P%",
      efg_pct: "eFG%",
      ts_pct: "TS%",
      ft_pct: "FT%",
      ftr: "FTr",
      three_pr: "3Pr",
      orb_pct: "ORB%",
      drb_pct: "DRB%",
      trb_pct: "TRB%",
      usg_pct: "USG%",
      ast_pct: "AST%",
      ast_to: "AST/TO",
      tov_pct: "TOV%",
      stl_pct: "STL%",
      blk_pct: "BLK%",
      ...LOWER_TIER_SHOT_PROFILE_LABELS,
    },
  },
  nba: {
    id: "nba",
    navLabel: "NBA",
    title: "NBA",
    subtitle: "",
    dataScript: "data/nba_all_seasons.js",
    globalName: "NBA_ALL_CSV",
    yearColumn: "season",
    playerColumn: "player_name",
    teamColumn: "team_alias",
    lockedColumns: ["rank", "season", "player_name", "team_alias"],
    searchColumns: ["player_name", "team_alias", "team_alias_all", "team_search_text"],
    sortBy: "tot",
    sortDir: "desc",
    demoColumns: ["pos_text", "age", "inches", "weight", "gp", "mp", "mpg", "rookie_year"],
    demoFilterColumns: ["age", "inches", "weight", "bmi", "wingspan", "exp", "gp", "mp", "mpg", "rookie_year"],
    groups: [
      { id: "summary", label: "Summary", columns: ["off", "def", "tot", "ewins", "per", "porpag", "fic"] },
      { id: "shot_profile", label: "Shot Profile", columns: ["fgpct_rim", "rim_ast_pct", "fga_rim_75", "fgpct_mid", "mid_ast_pct", "fga_mid_75", "fg2pct", "two_ast_pct", "fg3pct", "three_ast_pct", "fg3a_75", "ftpct", "three_p_per100", "three_pr", "ftr"] },
      { id: "advanced", label: "Advanced", columns: ["orbpct", "drbpct", "usg", "astpct", "ast_to", "topct", "stlpct", "blkpct", "tspct", "efg"] },
    ],
    singleFilters: withSharedSingleFilters(),
    multiFilters: [{ id: "pos_text", label: "Pos", column: "pos_text", sort: ["PG", "SG", "SF", "PF", "C"] }],
    defaultVisible: ["rank", "season", "player_name", "team_alias", "pos_text", "age", "gp", "mp", "off", "def", "tot", "ewins", "per", "porpag", "fic", "fgpct_rim", "fgpct_mid", "fg2pct", "fg3pct", "ftpct", "three_p_per100", "three_pr", "ftr", "usg", "orbpct", "drbpct", "astpct", "ast_to", "topct", "stlpct", "blkpct", "tspct", "efg"],
    labels: {
      rank: "",
      season: "Year",
      player_name: "Player",
      team_alias: "Team",
      pos_text: "Pos",
      age: "Age",
      gp: "GP",
      mp: "MP",
      mpg: "MPG",
      rookie_year: "Draft",
      inches: "HT",
      weight: "WT",
      off: "oEPM",
      def: "dEPM",
      tot: "EPM",
      ewins: "eWins",
      usg: "USG%",
      tspct: "TS%",
      efg: "eFG%",
      fgpct_rim: "Rim FG%",
      fgpct_mid: "Mid FG%",
      fg2pct: "2P%",
      fg3pct: "3P%",
      ftpct: "FT%",
      fga_rim_75: "Rim Att/40",
      fga_mid_75: "Mid Att/40",
      fg3a_75: "3PA/75",
      three_p_per100: "3PA/100",
      three_pr: "3Pr",
      ftr: "FTr",
      per: "PER",
      porpag: "PRPG",
      fic: "FIC",
      ppr: "PPR",
      orbpct: "ORB%",
      drbpct: "DRB%",
      astpct: "AST%",
      topct: "TOV%",
      stlpct: "STL%",
      blkpct: "BLK%",
      ast_to: "AST/TO",
      rim_ast_pct: "Rim Ast%",
      mid_ast_pct: "Mid Ast%",
      two_ast_pct: "2P Ast%",
      three_ast_pct: "3P Ast%",
    },
  },
  nba_companion: buildNbaCompanionConfig(),
};

const elements = {
  pageTitle: document.getElementById("pageTitle"),
  pageSubtitle: document.getElementById("pageSubtitle"),
  leagueNav: document.getElementById("leagueNav"),
  statusPill: document.getElementById("statusPill"),
  filtersSummary: document.getElementById("filtersSummary"),
  yearPills: document.getElementById("yearPills"),
  searchInput: document.getElementById("searchInput"),
  teamSelect: document.getElementById("teamSelect"),
  viewModeFilters: document.getElementById("viewModeFilters"),
  singleSelectFilters: document.getElementById("singleSelectFilters"),
  multiSelectFilters: document.getElementById("multiSelectFilters"),
  demoControls: document.getElementById("demoControls"),
  demoToggles: document.getElementById("demoToggles"),
  demoRangeFilters: document.getElementById("demoRangeFilters"),
  statGroups: document.getElementById("statGroups"),
  yearQuickSelect: document.getElementById("yearQuickSelect"),
  finderQuery: document.getElementById("finderQuery"),
  finderTitle: document.getElementById("finderTitle"),
  resultsCount: document.getElementById("resultsCount"),
  resultsSubtitle: document.getElementById("resultsSubtitle"),
  loadMoreBtn: document.getElementById("loadMoreBtn"),
  tableWrapper: document.getElementById("tableWrapper"),
  statsTable: document.getElementById("statsTable"),
  statsTableHead: document.getElementById("statsTableHead"),
  statsTableBody: document.getElementById("statsTableBody"),
  queryPanel: document.getElementById("queryPanel"),
  homeContent: document.getElementById("homeContent"),
  tableContent: document.getElementById("tableContent"),
  selectAllYearsBtn: document.getElementById("selectAllYearsBtn"),
  selectLatestYearBtn: document.getElementById("selectLatestYearBtn"),
  clearYearsBtn: document.getElementById("clearYearsBtn"),
  demoDefaultsBtn: document.getElementById("demoDefaultsBtn"),
  demoAllBtn: document.getElementById("demoAllBtn"),
  demoClearBtn: document.getElementById("demoClearBtn"),
  statDefaultsBtn: document.getElementById("statDefaultsBtn"),
  statAllBtn: document.getElementById("statAllBtn"),
  statNoneBtn: document.getElementById("statNoneBtn"),
  statClearBtn: document.getElementById("statClearBtn"),
  tableLegend: document.getElementById("tableLegend"),
  statGroupTemplate: document.getElementById("statGroupTemplate"),
};

const appState = {
  currentId: null,
  scriptLoads: new Map(),
  statusLoads: new Map(),
  hydrationLoads: new Map(),
  scheduledTasks: new Map(),
  datasetCache: {},
  uiState: {},
  searchRenderTimer: 0,
};

renderNav();
wireGlobalEvents();
handleRoute();
window.addEventListener("hashchange", handleRoute);

function renderNav() {
  elements.leagueNav.innerHTML = Object.values(DATASETS)
    .map(
      (dataset) => `
        <a class="league-link" href="#${dataset.id}" data-id="${dataset.id}">
          <span class="league-tag">${escapeHtml(dataset.navLabel)}</span>
          <span>${escapeHtml(dataset.title.replace(" Explorer", ""))}</span>
        </a>
      `
    )
    .join("");
}

function wireGlobalEvents() {
  elements.searchInput.addEventListener("input", () => {
    const state = getCurrentUiState();
    const dataset = getCurrentDataset();
    if (!state || !dataset) return;
    const changed = applySearchInputValue(dataset, state, elements.searchInput.value);
    if (!changed) return;
    if (changed.filtersChanged) {
      renderCurrentDataset();
      return;
    }
    scheduleSearchResultsRender(dataset, state);
  });

  elements.teamSelect.addEventListener("change", () => {
    const state = getCurrentUiState();
    if (!state) return;
    state.team = elements.teamSelect.value;
    renderCurrentDataset();
  });

  elements.selectAllYearsBtn.addEventListener("click", () => {
    const state = getCurrentUiState();
    const dataset = getCurrentDataset();
    if (!state || !dataset) return;
    state.years = new Set(dataset.meta.years);
    renderCurrentDataset();
  });

  elements.selectLatestYearBtn.addEventListener("click", () => {
    const state = getCurrentUiState();
    const dataset = getCurrentDataset();
    if (!state || !dataset) return;
    state.years = new Set(dataset.meta.latestYear ? [dataset.meta.latestYear] : []);
    renderCurrentDataset();
  });

  elements.clearYearsBtn.addEventListener("click", () => {
    const state = getCurrentUiState();
    if (!state) return;
    state.years = new Set();
    renderCurrentDataset();
  });

  elements.demoDefaultsBtn?.addEventListener("click", () => applyVisibilityMode("demo", "default"));
  elements.demoAllBtn?.addEventListener("click", () => applyVisibilityMode("demo", "all"));
  elements.demoClearBtn?.addEventListener("click", () => applyVisibilityMode("demo", "clear"));
  elements.statDefaultsBtn?.addEventListener("click", () => applyVisibilityMode("stats", "default"));
  elements.statAllBtn?.addEventListener("click", () => applyVisibilityMode("stats", "all"));
  elements.statNoneBtn?.addEventListener("click", () => applyVisibilityMode("stats", "none"));
  elements.statClearBtn?.addEventListener("click", () => applyVisibilityMode("stats", "clear"));

  elements.loadMoreBtn.addEventListener("click", async () => {
    const state = getCurrentUiState();
    const dataset = getCurrentDataset();
    if (!state || !dataset) return;
    state.visibleCount += LOAD_STEP;
    const visibleDeferredColumns = getVisibleDeferredColumns(dataset, state);
    if (visibleDeferredColumns.length) {
      await ensureDeferredColumnsReady(dataset, state, visibleDeferredColumns, { scope: "visible" });
      if (appState.currentId !== dataset.id) return;
    }
    const filtered = getFilteredRows(dataset, state);
    state.visibleCount = Math.min(state.visibleCount, filtered.length);
    renderTable(dataset, state, filtered);
    updateSummary(dataset, state, filtered);
  });
}

function getRouteId() {
  const raw = window.location.hash.replace(/^#/, "").trim().toLowerCase();
  if (raw === HOME_ID || !raw) return HOME_ID;
  return DATASETS[raw] ? raw : HOME_ID;
}

async function handleRoute() {
  const datasetId = getRouteId();
  appState.currentId = datasetId;
  updateNavActive(datasetId);

  if (datasetId === HOME_ID) {
    renderHomePage();
    return;
  }

  const config = DATASETS[datasetId];
  elements.queryPanel.hidden = false;
  elements.queryPanel.style.display = "";
  elements.homeContent.hidden = true;
  elements.homeContent.style.display = "none";
  elements.tableContent.hidden = false;
  elements.tableContent.style.display = "";
  elements.pageTitle.textContent = "100guaranteed";
  elements.pageSubtitle.textContent = config.subtitle || "";
  elements.statusPill.textContent = `Loading ${config.navLabel}`;
  elements.resultsCount.textContent = "Loading dataset...";
  elements.resultsSubtitle.textContent = "";
  elements.filtersSummary.textContent = "";
  if (config.singleFilters?.some((filter) => filter.id === "status_path")) {
    loadPrecomputedStatusAnnotations().catch(() => {});
  }

  try {
    const dataset = await ensureDatasetLoaded(datasetId);
    if (appState.currentId !== datasetId) return;

    if (!appState.uiState[datasetId]) {
      appState.uiState[datasetId] = createInitialUiState(dataset);
    }

    const state = appState.uiState[datasetId];
    const shouldBlockForHydration = requiresHydratedDataset(datasetId, state);
    if (shouldBlockForHydration && dataset._hydrationPending) {
      elements.statusPill.textContent = `Loading ${config.navLabel} corrections`;
      if (isSupplementDeferredDataset(dataset)) {
        const scope = state?.extraSelects?.view_mode === "career" || usesDeferredColumnForSortOrFilter(dataset, state)
          ? "full"
          : "visible";
        await ensureDeferredSupplementScripts(dataset, state, { scope });
      } else {
        await ensureDatasetHydrated(datasetId);
      }
      if (appState.currentId !== datasetId) return;
      resetUiCaches(appState.uiState[datasetId]);
    }

    const needsStatus = dataset.singleFilters?.some((filter) => filter.id === "status_path");
    const selectedStatus = getStringValue(state?.extraSelects?.status_path);
    const shouldBlockForStatus = needsStatus && selectedStatus && selectedStatus !== "all" && !dataset._statusAnnotated;

    if (shouldBlockForStatus) {
      elements.statusPill.textContent = `Loading ${config.navLabel} status`;
      await ensureStatusAnnotations(datasetId);
      if (appState.currentId !== datasetId) return;
      resetUiCaches(appState.uiState[datasetId]);
    }

    renderCurrentDataset();
    scheduleStatusAnnotations(datasetId);
    elements.statusPill.textContent = `${dataset.navLabel} ready`;
  } catch (error) {
    if (appState.currentId !== datasetId) return;
    elements.statusPill.textContent = `${config.navLabel} load failed`;
    elements.resultsCount.textContent = "Dataset failed to load.";
    elements.resultsSubtitle.textContent = getStringValue(error?.message || error);
  }
}

function updateNavActive(datasetId) {
  elements.leagueNav.querySelectorAll(".league-link").forEach((link) => {
    link.classList.toggle("is-active", link.dataset.id === datasetId);
  });
}

function getCurrentDataset() {
  return appState.currentId && appState.currentId !== HOME_ID ? appState.datasetCache[appState.currentId] : null;
}

function getCurrentUiState() {
  return appState.currentId && appState.currentId !== HOME_ID ? appState.uiState[appState.currentId] : null;
}

function renderHomePage() {
  elements.queryPanel.hidden = true;
  elements.queryPanel.style.display = "none";
  elements.tableContent.hidden = true;
  elements.tableContent.style.display = "none";
  elements.homeContent.hidden = false;
  elements.homeContent.style.display = "block";
  elements.pageTitle.textContent = "100guaranteed";
  elements.pageSubtitle.textContent = "";
  elements.statusPill.textContent = "Home";
  elements.filtersSummary.textContent = "";
  elements.resultsCount.textContent = "";
  elements.resultsSubtitle.textContent = "";
  elements.homeContent.innerHTML = `
    <div class="home-card">
      <h2>Stat Explorers</h2>
      <div class="home-list">
        ${HOME_PAGES.map((page) => `<div class="home-item"><a href="#${page.id}">${escapeHtml(page.label)}</a></div>`).join("")}
      </div>
    </div>
  `;
}

function supportsDeferredHydration(dataset) {
  return Boolean(
    dataset?.deferSupplementHydration
      && ((Array.isArray(dataset?.deferredExtraScripts) && dataset.deferredExtraScripts.length)
        || dataset?.deferredScriptTemplate)
  );
}

function getDeferredColumns(dataset) {
  return Array.isArray(dataset?.deferredColumns) ? dataset.deferredColumns : [];
}

function hasActiveRangeFilter(filter) {
  return Boolean(getStringValue(filter?.min).trim() || getStringValue(filter?.max).trim());
}

function inputHasValue(value) {
  return getStringValue(value).trim() !== "";
}

function sanitizeDeferredSeasonKey(season) {
  return getStringValue(season).replace(/[^0-9A-Za-z_-]+/g, "_");
}

function getDeferredSupplementYears(dataset, state) {
  const selectedYears = Array.from(state?.years || []).filter(Boolean);
  if (selectedYears.length) return selectedYears.sort(compareYears);
  return Array.from(dataset?.meta?.years || []).filter(Boolean).sort(compareYears);
}

function getMissingDeferredSupplementYears(dataset, state) {
  if (!isSupplementDeferredDataset(dataset)) return [];
  const loadedYears = dataset._loadedSupplementYears || new Set();
  return getDeferredSupplementYears(dataset, state).filter((season) => !loadedYears.has(getStringValue(season)));
}

function getVisibleDeferredColumns(dataset, state) {
  return getDeferredColumns(dataset).filter((column) => state?.visibleColumns?.[column]);
}

function usesDeferredColumnForSortOrFilter(dataset, state) {
  if (!supportsDeferredHydration(dataset)) return false;
  if (isDeferredSupplementColumn(dataset, state?.sortBy)) return true;
  return getDeferredColumns(dataset).some((column) => hasActiveRangeFilter(state?.numericFilters?.[column]));
}

function getDeferredSupplementYearsForRows(rows, limit = LOAD_STEP) {
  const years = new Set();
  rows.slice(0, Math.max(limit, LOAD_STEP)).forEach((row) => {
    const season = getStringValue(row?.season).trim();
    if (season) years.add(season);
  });
  return Array.from(years).sort(compareYears);
}

function getImmediateDeferredSupplementYears(dataset, state) {
  if (!isSupplementDeferredDataset(dataset)) return [];
  if (state?.extraSelects?.view_mode === "career" || usesDeferredColumnForSortOrFilter(dataset, state)) {
    return getDeferredSupplementYears(dataset, state);
  }
  const rows = getFilteredRows(dataset, state);
  const visibleYears = getDeferredSupplementYearsForRows(rows, state?.visibleCount || LOAD_STEP);
  if (visibleYears.length) return visibleYears;
  return getDeferredSupplementYears(dataset, state).slice(0, 1);
}

function getMissingDeferredSupplementYearsForScope(dataset, state, scope = "full") {
  if (!isSupplementDeferredDataset(dataset)) return [];
  const targetYears = scope === "visible"
    ? getImmediateDeferredSupplementYears(dataset, state)
    : getDeferredSupplementYears(dataset, state);
  const loadedYears = dataset._loadedSupplementYears || new Set();
  return targetYears.filter((season) => !loadedYears.has(getStringValue(season)));
}

function usesDeferredColumns(dataset, state) {
  if (!supportsDeferredHydration(dataset)) return false;
  return getDeferredColumns(dataset).some((column) => state?.visibleColumns?.[column] || hasActiveRangeFilter(state?.numericFilters?.[column]));
}

function requiresHydratedDataset(datasetId, state) {
  const dataset = appState.datasetCache[datasetId] || DATASETS[datasetId];
  if (isSupplementDeferredDataset(dataset)) {
    const scope = state?.extraSelects?.view_mode === "career" || usesDeferredColumnForSortOrFilter(dataset, state)
      ? "full"
      : "visible";
    return usesDeferredColumns(dataset, state) && getMissingDeferredSupplementYearsForScope(dataset, state, scope).length > 0;
  }
  return usesDeferredColumns(dataset, state);
}

function scheduleBackgroundTask(taskKey, callback, delayMs = 1200) {
  if (!taskKey || typeof callback !== "function" || appState.scheduledTasks.has(taskKey)) return;
  const finalize = async () => {
    appState.scheduledTasks.delete(taskKey);
    await callback();
  };
  if (typeof window.requestIdleCallback === "function") {
    const handle = window.requestIdleCallback(() => {
      finalize().catch(() => {});
    });
    appState.scheduledTasks.set(taskKey, { type: "idle", handle });
    return;
  }
  const handle = window.setTimeout(() => {
    finalize().catch(() => {});
  }, delayMs);
  appState.scheduledTasks.set(taskKey, { type: "timeout", handle });
}

function scheduleDeferredHydration(datasetId) {
  const dataset = appState.datasetCache[datasetId];
  if (!dataset?._hydrationPending || !supportsDeferredHydration(dataset) || dataset.autoHydrateDeferred === false) return;
  scheduleBackgroundTask(`hydrate:${datasetId}`, async () => {
    if (appState.currentId !== datasetId) return;
    try {
      elements.statusPill.textContent = `Loading ${dataset.navLabel} corrections`;
      await ensureDatasetHydrated(datasetId);
      if (appState.currentId !== datasetId) return;
      resetUiCaches(appState.uiState[datasetId]);
      renderCurrentDataset();
      elements.statusPill.textContent = `${dataset.navLabel} ready`;
    } catch (error) {
      if (appState.currentId !== datasetId) return;
      elements.statusPill.textContent = `${dataset.navLabel} correction failed`;
      elements.resultsSubtitle.textContent = getStringValue(error?.message || error);
    }
  });
}

function scheduleStatusAnnotations(datasetId) {
  const dataset = appState.datasetCache[datasetId];
  const state = appState.uiState[datasetId];
  if (!dataset || dataset._statusAnnotated || !dataset.singleFilters?.some((filter) => filter.id === "status_path")) return;
  if (getStringValue(state?.extraSelects?.status_path) && getStringValue(state?.extraSelects?.status_path) !== "all") return;
  scheduleBackgroundTask(`status:${datasetId}`, async () => {
    if (appState.currentId !== datasetId) return;
    try {
      elements.statusPill.textContent = `Loading ${dataset.navLabel} status`;
      await ensureStatusAnnotations(datasetId);
      if (appState.currentId !== datasetId) return;
      resetUiCaches(appState.uiState[datasetId]);
      renderCurrentDataset();
      elements.statusPill.textContent = `${dataset.navLabel} ready`;
    } catch (error) {
      if (appState.currentId !== datasetId) return;
      elements.statusPill.textContent = `${dataset.navLabel} status failed`;
      elements.resultsSubtitle.textContent = getStringValue(error?.message || error);
    }
  }, 250);
}

function buildDeferredSupplementScriptPath(dataset, season) {
  if (!dataset?.deferredScriptTemplate) return "";
  return dataset.deferredScriptTemplate.replace("{season}", sanitizeDeferredSeasonKey(season));
}

async function ensureDeferredSupplementScripts(dataset, state, options = {}) {
  if (!isSupplementDeferredDataset(dataset)) return dataset;
  const scope = options.scope === "visible" ? "visible" : "full";
  const requestedYears = Array.isArray(options.years) ? options.years.filter(Boolean) : null;
  const loadedYears = dataset._loadedSupplementYears || new Set();
  const missingYears = (requestedYears || getMissingDeferredSupplementYearsForScope(dataset, state, scope))
    .filter((season) => !loadedYears.has(getStringValue(season)));
  if (!missingYears.length) return dataset;
  if (!dataset._loadedSupplementYears) dataset._loadedSupplementYears = new Set();
  const scriptPaths = missingYears
    .map((season) => ({ season, src: buildDeferredSupplementScriptPath(dataset, season) }))
    .filter((entry) => entry.src);
  for (const entry of scriptPaths) {
    await loadScriptOnce(entry.src);
    dataset._loadedSupplementYears.add(getStringValue(entry.season));
    await new Promise((resolve) => window.setTimeout(resolve, 0));
  }
  dataset._hydrationPending = getMissingDeferredSupplementYears(dataset, state).length > 0;
  dataset._supplementScriptsLoaded = dataset._loadedSupplementYears.size > 0;
  return dataset;
}

async function ensureDeferredColumnsReady(dataset, state, columns, options = {}) {
  if (!supportsDeferredHydration(dataset)) return false;
  const deferred = new Set(getDeferredColumns(dataset));
  if (!(columns || []).some((column) => deferred.has(column))) return false;
  elements.statusPill.textContent = `Loading ${dataset.navLabel} shot profile`;
  if (isSupplementDeferredDataset(dataset)) {
    const scope = options.scope === "visible" && !usesDeferredColumnForSortOrFilter(dataset, state) && state?.extraSelects?.view_mode !== "career"
      ? "visible"
      : "full";
    await ensureDeferredSupplementScripts(dataset, state, { scope });
    if (appState.currentId !== dataset.id) return true;
    resetUiCaches(state);
    return true;
  }
  await ensureDatasetHydrated(dataset.id);
  if (appState.currentId !== dataset.id) return true;
  resetUiCaches(state);
  return true;
}

function invalidateDatasetDerivedCaches(datasetId) {
  const ids = datasetId === "d1"
    ? ["d1", "d2", "naia", "juco", "fiba", "nba_companion"]
    : [datasetId];
  ids.forEach((id) => {
    const dataset = appState.datasetCache[id];
    if (!dataset) return;
    dataset._statusAnnotated = false;
    dataset._linkedStatusMetricsAnnotated = false;
    dataset._statusGroups = null;
    if (id === "nba_companion") delete appState.datasetCache[id];
  });
  appState.statusLoads.clear();
}

function reapplyDatasetPostProcessing(rows, config) {
  if (!rows?.length || !config) return rows;
  if (config.minYear) {
    rows = rows.filter((row) => {
      const season = Number(row[config.yearColumn]);
      return Number.isFinite(season) ? season >= config.minYear : true;
    });
  }
  rows = dedupeDatasetRows(rows, config.id);
  if (config.id === "d1") annotateTrustedD1Rows(rows);
  inferMissingClassYears(rows, config);
  normalizeRepeatedSeniorSeasons(rows, config);
  rows.forEach((row) => populateImpactMetrics(row));
  applyCalculatedRatings(rows, config.id);
  rows.forEach((row) => populateImpactMetrics(row));
  applyPerNormalization(rows, config.id);
  return rows;
}

async function ensureDatasetHydrated(datasetId) {
  const dataset = appState.datasetCache[datasetId];
  if (!dataset || dataset._hydrated || !supportsDeferredHydration(dataset)) return dataset;
  if (appState.hydrationLoads.has(datasetId)) {
    await appState.hydrationLoads.get(datasetId);
    return appState.datasetCache[datasetId];
  }

  const promise = (async () => {
    await Promise.all((dataset.deferredExtraScripts || []).map((src) => loadScriptOnce(src)));
    if (dataset.deferredHydrationMode === "supplement") {
      dataset._supplementScriptsLoaded = true;
    } else if (!dataset.precomputed) {
      enrichDatasetRows(dataset.rows, datasetId);
      dataset.rows.forEach((row) => normalizePercentLikeColumns(row, datasetId));
      dataset.rows = reapplyDatasetPostProcessing(dataset.rows, dataset);
      dataset.meta = buildDatasetMeta(dataset.rows, dataset);
      invalidateDatasetDerivedCaches(datasetId);
    }
    dataset._hydrated = true;
    dataset._hydrationPending = false;
    return dataset;
  })();

  appState.hydrationLoads.set(datasetId, promise);
  try {
    await promise;
  } finally {
    appState.hydrationLoads.delete(datasetId);
  }
  return appState.datasetCache[datasetId];
}

async function ensureDatasetLoaded(datasetId, options = {}) {
  if (appState.datasetCache[datasetId]) {
    if (options.requireHydrated) await ensureDatasetHydrated(datasetId);
    return appState.datasetCache[datasetId];
  }

  if (datasetId === "nba_companion") {
    return ensureNbaCompanionDataset();
  }

  const config = DATASETS[datasetId];
  if (config.extraScripts?.length) {
    await Promise.all(config.extraScripts.map((src) => loadScriptOnce(src)));
  }
  if (options.requireHydrated && supportsDeferredHydration(config)) {
    await Promise.all((config.deferredExtraScripts || []).map((src) => loadScriptOnce(src)));
  }
  await loadScriptOnce(config.dataScript);
  const rawPayload = window[config.globalName] ?? "";
  const csvText = Array.isArray(rawPayload) ? rawPayload.join("\n") : String(rawPayload);
  const rows = parseCSV(csvText);
  let normalized = normalizeRows(rows, datasetId);
  if (config.minYear) {
    normalized = normalized.filter((row) => {
      const season = Number(row[config.yearColumn]);
      return Number.isFinite(season) ? season >= config.minYear : true;
    });
  }
  const deferredHydrationMode = supportsDeferredHydration(config) && !options.requireHydrated
    ? (config.deferredHydrationMode || "full")
    : "";
  const deferHydration = Boolean(deferredHydrationMode);
  if (!config.precomputed && deferredHydrationMode !== "full") {
    enrichDatasetRows(normalized, datasetId);
    normalized.forEach((row) => {
      normalizePercentLikeColumns(row, datasetId);
    });
  }
  normalized = reapplyDatasetPostProcessing(normalized, config);
  const meta = buildDatasetMeta(normalized, config);
  const dataset = { ...config, rows: normalized, meta, _hydrated: !deferHydration, _hydrationPending: deferHydration };
  appState.datasetCache[datasetId] = dataset;
  if (options.requireHydrated) {
    await ensureDatasetHydrated(datasetId);
  } else if (!deferHydration) {
    dataset._hydrated = true;
    dataset._hydrationPending = false;
  }
  return dataset;
}

async function ensureNbaCompanionDataset() {
  if (appState.datasetCache.nba_companion) {
    return appState.datasetCache.nba_companion;
  }

  const config = DATASETS.nba_companion;
  const [d1Dataset, nbaDataset] = await Promise.all([ensureDatasetLoaded("d1"), ensureDatasetLoaded("nba")]);
  const graph = buildStatusGraph([d1Dataset, nbaDataset]);
  const rows = buildNbaCompanionRows(d1Dataset, nbaDataset, graph);
  const meta = buildDatasetMeta(rows, config);
  const dataset = { ...config, rows, meta };
  appState.datasetCache.nba_companion = dataset;
  return dataset;
}

function buildNbaCompanionRows(d1Dataset, nbaDataset, graph) {
  const d1Groups = getStatusGroups(d1Dataset);
  const nbaGroups = getStatusGroups(nbaDataset);
  const nbaCareerByNodeId = new Map();
  const nbaCareerByD1NodeId = new Map();
  nbaGroups.forEach((group) => {
    nbaCareerByNodeId.set(group.nodeId, aggregateCareerRows(nbaDataset, group.rows));
  });
  d1Groups.forEach((group) => {
    const reachableNbaGroups = getReachableStatusGroups(group, graph, "nba", "forward");
    const bestCareer = reachableNbaGroups
      .map((targetGroup) => nbaCareerByNodeId.get(targetGroup.nodeId))
      .filter(Boolean)
      .sort((left, right) => companionNbaCareerScore(right) - companionNbaCareerScore(left))[0];
    if (bestCareer) nbaCareerByD1NodeId.set(group.nodeId, bestCareer);
  });

  return d1Groups.flatMap((group) => {
    const nbaCareer = nbaCareerByD1NodeId.get(group.nodeId);
    if (!nbaCareer) return [];
    return (group.rows || []).map((row) => {
      const out = { ...row };
      NBA_COMPANION_COPY_MAP.forEach(([target, source]) => {
        out[target] = row[source];
      });
      NBA_COMPANION_NBA_COPY_MAP.forEach(([target, source]) => {
        out[target] = nbaCareer[source];
      });
      populateNbaCompanionNcaaShooting(out, row);
      return out;
    });
  });
}

function populateNbaCompanionNcaaShooting(out, row) {
  const twoPm = firstFinite(row?.two_p_made, row?.two_pm, Number.NaN);
  const twoPa = firstFinite(row?.two_p_att, row?.two_pa, Number.NaN);
  const threePm = firstFinite(row?.three_p_made, row?.three_pm, Number.NaN);
  const threePa = firstFinite(row?.three_p_att, row?.three_pa, Number.NaN);
  const fgm = firstFinite(row?.fgm, addIfFinite(twoPm, threePm), Number.NaN);
  const fga = firstFinite(row?.fga, addIfFinite(twoPa, threePa), Number.NaN);
  const ftm = firstFinite(row?.ftm, Number.NaN);
  const fta = firstFinite(row?.fta, Number.NaN);
  const points = firstFinite(row?.pts, weightedPointTotal(twoPm, threePm, ftm), Number.NaN);
  if (!Number.isFinite(out.ncaa_efg_pct)) {
    const efgPct = zeroSafeEfgPct(fgm, threePm, fga);
    if (efgPct !== "") out.ncaa_efg_pct = efgPct;
  }
  if (!Number.isFinite(out.ncaa_ts_pct)) {
    const tsPct = zeroSafeTsPct(points, fga, fta);
    if (tsPct !== "") out.ncaa_ts_pct = tsPct;
  }
}

function companionNbaCareerScore(row) {
  return firstFinite(row?.mp, 0) + (firstFinite(row?.ewins, 0) * 100);
}

function loadScriptOnce(src) {
  if (appState.scriptLoads.has(src)) {
    return appState.scriptLoads.get(src);
  }

  const promise = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = new URL(src, window.location.href).href;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });

  appState.scriptLoads.set(src, promise);
  return promise;
}

function enrichDatasetRows(rows, datasetId) {
  if (datasetId === "d1") enrichD1Rows(rows);
  if (datasetId === "naia") enrichNaiaRows(rows);
  if (datasetId === "d1") backfillTeamSeasonFields(rows, ["conference", "coach"]);
  if (datasetId === "naia") backfillTeamSeasonFields(rows, ["conference", "division"]);
  if (datasetId === "naia") backfillNaiaDivisionFromConference(rows);
  if (datasetId === "juco") backfillTeamSeasonFields(rows, ["conference", "region"]);
  finalizeCategoricalFields(rows, datasetId);
}

async function ensureStatusAnnotations(datasetId) {
  if (datasetId === "nba") return;
  if (appState.statusLoads.has(datasetId)) {
    await appState.statusLoads.get(datasetId);
    return;
  }

  const promise = (async () => {
    const source = appState.datasetCache[datasetId] || await ensureDatasetLoaded(datasetId);
    const precomputed = await loadPrecomputedStatusAnnotations();
    if (applyPrecomputedStatusAnnotations(source, precomputed)) {
      source.meta = buildDatasetMeta(source.rows, source);
      return;
    }
    const requiredIds = datasetId === "d1"
      ? ["d1", "d2", "naia", "juco", "nba"]
      : [datasetId, "d1", "nba"];
    const datasets = await Promise.all(requiredIds.map((id) => ensureDatasetLoaded(id)));
    const graph = buildStatusGraph(datasets);
    const fallbackSource = datasets.find((dataset) => dataset.id === datasetId);
    annotateStatusFlags(fallbackSource, graph);
    annotateLinkedStatusMetrics(fallbackSource, graph);
    fallbackSource.meta = buildDatasetMeta(fallbackSource.rows, fallbackSource);
  })();

  appState.statusLoads.set(datasetId, promise);
  await promise;
}

async function loadPrecomputedStatusAnnotations() {
  if (window.STATUS_ANNOTATIONS) return window.STATUS_ANNOTATIONS;
  try {
    await loadScriptOnce(STATUS_ANNOTATIONS_SCRIPT);
  } catch (error) {
    return null;
  }
  return window.STATUS_ANNOTATIONS || null;
}

function applyPrecomputedStatusAnnotations(sourceDataset, bundle) {
  if (!sourceDataset?.rows?.length || !bundle?.datasets?.[sourceDataset.id]?.entries) return false;
  const datasetBundle = bundle.datasets[sourceDataset.id];
  const sourceGroups = getStatusGroups(sourceDataset);
  if (Number.isFinite(datasetBundle.groupCount) && datasetBundle.groupCount !== sourceGroups.length) return false;

  const entries = datasetBundle.entries || {};
  sourceGroups.forEach((group) => {
    const entry = entries[getStatusAnnotationGroupKey(group)] || null;
    const flags = decodeStatusAnnotationFlags(entry?.b, sourceDataset.id);
    group.rows.forEach((row) => {
      row._statusFlags = { ...flags };
      if (Number.isFinite(entry?.p)) row.d1_peak_prpg = entry.p;
      if (Number.isFinite(entry?.q)) row.d1_peak_dprpg = entry.q;
      if (Number.isFinite(entry?.r)) row.d1_peak_bpm = entry.r;
      if (Number.isFinite(entry?.n)) row.nba_career_epm = entry.n;
      if (sourceDataset.id === "fiba" && entry?.x) {
        row.player_name = entry.x;
        row.player = entry.x;
        row._searchCacheKey = "";
        row._searchHaystack = "";
      }
    });
  });

  sourceDataset._statusAnnotated = true;
  sourceDataset._linkedStatusMetricsAnnotated = true;
  return true;
}

function decodeStatusAnnotationFlags(bitmask, datasetId) {
  const bits = Number(bitmask) || 0;
  if (datasetId === "d1") {
    return {
      nba: Boolean(bits & 1),
      former_juco: Boolean(bits & 8),
      former_d2: Boolean(bits & 16),
      former_naia: Boolean(bits & 32),
    };
  }
  if (datasetId === "fiba") {
    return {
      d1: Boolean(bits & 2),
      formerd1: Boolean(bits & 4),
      nba: Boolean(bits & 1),
    };
  }
  return {
    d1: Boolean(bits & 2),
    formerd1: Boolean(bits & 4),
    nba: Boolean(bits & 1),
  };
}

function getStatusAnnotationGroupKey(group) {
  if (!group) return "";
  const ids = Array.from(new Set(group.explicitIds || [])).filter(Boolean).sort().join(",");
  const dobs = Array.from(new Set(group.dobs || [])).filter(Boolean).sort().join(",");
  const heights = Array.from(new Set((group.heights || []).filter(Number.isFinite))).sort((left, right) => left - right).join(",");
  const years = Array.from(new Set((group.years || []).filter(Number.isFinite))).sort((left, right) => left - right).join(",");
  return [
    getStringValue(group.nameKey).trim(),
    ids,
    dobs,
    heights,
    years,
  ].join("|");
}

function annotateStatusFlags(sourceDataset, graph) {
  if (sourceDataset._statusAnnotated) return;
  const sourceGroups = getStatusGroups(sourceDataset);

  sourceGroups.forEach((group) => {
    const manualD2NbaOverride = getManualD2NbaOverrideForGroup(group);
    const flags = sourceDataset.id === "d1"
      ? {
        nba: hasStatusPath(group, graph, "nba", "forward"),
        former_juco: hasStatusPath(group, graph, "juco", "backward"),
        former_d2: hasStatusPath(group, graph, "d2", "backward"),
        former_naia: hasStatusPath(group, graph, "naia", "backward"),
      }
      : sourceDataset.id === "fiba"
        ? {
          d1: hasAnyStatusPath(group, graph, "d1"),
          formerd1: false,
          nba: hasAnyStatusPath(group, graph, "nba"),
        }
        : {
          d1: hasStatusPath(group, graph, "d1", "forward"),
          formerd1: hasStatusPath(group, graph, "d1", "backward"),
          nba: hasStatusPath(group, graph, "nba", "forward"),
        };
    if (sourceDataset.id !== "d1" && sourceDataset.id !== "fiba" && group.rows.some((row) => rowHasExplicitProjectedD1Data(row))) {
      flags.d1 = true;
    }
    if (manualD2NbaOverride) flags.nba = true;
    group.rows.forEach((row) => {
      row._statusFlags = { ...flags };
    });
  });

  sourceDataset._statusAnnotated = true;
}

function getStatusGroups(dataset) {
  if (dataset._statusGroups) return dataset._statusGroups;
  const grouped = new Map();
  getStatusSourceRows(dataset).forEach((row) => {
    const key = getCareerGroupKey(dataset, row);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });
  dataset._statusGroups = mergeCareerRowGroups(dataset, Array.from(grouped.values())).map((rows, index) => ({
    ...buildCareerGroupMeta(dataset, rows),
    datasetId: dataset.id,
    nodeId: `${dataset.id}:${index}`,
    nameKey: normalizeNameKey(getPreferredStatusName(rows)),
    nameKeys: buildStatusNameKeys(getPreferredStatusName(rows)),
  }));
  return dataset._statusGroups;
}

function getStatusSourceRows(dataset) {
  if (!dataset?.rows) return [];
  const sourceRows = dataset.id === "d1"
    ? dataset.rows.filter((row) => row._trustedD1 !== false)
    : dataset.rows.slice();
  if (dataset.id === "fiba") return sourceRows;
  const qualified = sourceRows.filter((row) => getMinutesValue(row) >= MINUTES_DEFAULT || rowHasExplicitProjectedD1Data(row));
  return qualified.length ? qualified : sourceRows;
}

function getPreferredStatusName(rows) {
  return (rows || [])
    .map((row) => getStringValue(row?.player_name || row?.player).trim())
    .sort((left, right) => right.length - left.length)[0] || "";
}

function buildStatusGraph(datasets) {
  const nodes = new Map();
  const nameBuckets = new Map();
  (datasets || []).forEach((dataset) => {
    getStatusGroups(dataset).forEach((group) => {
      nodes.set(group.nodeId, group);
      (group.nameKeys || [group.nameKey]).forEach((nameKey) => {
        if (!nameKey) return;
        if (!nameBuckets.has(nameKey)) nameBuckets.set(nameKey, []);
        nameBuckets.get(nameKey).push(group);
      });
    });
  });

  const forward = new Map();
  const backward = new Map();
  nodes.forEach((_, nodeId) => {
    forward.set(nodeId, new Set());
    backward.set(nodeId, new Set());
  });

  const seenPairs = new Set();
  nameBuckets.forEach((groups) => {
    for (let i = 0; i < groups.length; i += 1) {
      for (let j = i + 1; j < groups.length; j += 1) {
        const left = groups[i];
        const right = groups[j];
        const pairKey = [left.nodeId, right.nodeId].sort().join("|");
        if (seenPairs.has(pairKey)) continue;
        seenPairs.add(pairKey);
        if (!statusGroupsMatch(left, right)) continue;
        if (isValidStatusTransition(left, right)) {
          forward.get(left.nodeId).add(right.nodeId);
          backward.get(right.nodeId).add(left.nodeId);
        }
        if (isValidStatusTransition(right, left)) {
          forward.get(right.nodeId).add(left.nodeId);
          backward.get(left.nodeId).add(right.nodeId);
        }
      }
    }
  });

  addExplicitProjectedD1StatusEdges(datasets, nodes, forward, backward);
  return { nodes, forward, backward };
}

function addExplicitProjectedD1StatusEdges(datasets, nodes, forward, backward) {
  const d1Dataset = (datasets || []).find((dataset) => dataset?.id === "d1");
  if (!d1Dataset) return;
  const d1Groups = getStatusGroups(d1Dataset);
  if (!d1Groups.length) return;
  const d1GroupIndex = buildExplicitProjectedD1GroupIndex(d1Groups);

  (datasets || []).forEach((dataset) => {
    if (!dataset || dataset.id === "d1" || dataset.id === "nba" || dataset.id === "nba_companion") return;
    getStatusGroups(dataset).forEach((group) => {
      getExplicitProjectedD1GroupsForStatusGroup(group, d1GroupIndex).forEach((targetGroup) => {
        if (!group?.nodeId || !targetGroup?.nodeId || group.nodeId === targetGroup.nodeId) return;
        if (!nodes.has(group.nodeId) || !nodes.has(targetGroup.nodeId)) return;
        forward.get(group.nodeId)?.add(targetGroup.nodeId);
        backward.get(targetGroup.nodeId)?.add(group.nodeId);
      });
    });
  });
}

function buildExplicitProjectedD1GroupIndex(d1Groups) {
  const byNameKey = new Map();
  const entries = (d1Groups || []).map((group) => ({
    group,
    teamKeys: buildExplicitProjectedD1TeamKeys(group.rows || []),
  }));
  entries.forEach((entry) => {
    (entry.group?.nameKeys || [entry.group?.nameKey]).forEach((nameKey) => {
      if (!nameKey) return;
      if (!byNameKey.has(nameKey)) byNameKey.set(nameKey, []);
      byNameKey.get(nameKey).push(entry);
    });
  });
  return { byNameKey, entries };
}

function buildExplicitProjectedD1TeamKeys(rows) {
  const keys = new Set();
  (rows || []).forEach((row) => {
    buildTeamVariants(row?.team_full || row?.team_name).forEach((variant) => {
      if (variant) keys.add(variant);
    });
  });
  return Array.from(keys);
}

function getExplicitProjectedD1GroupsForStatusGroup(group, d1GroupIndex) {
  const matches = new Map();
  const candidateMap = new Map();
  (group?.nameKeys || [group?.nameKey]).forEach((nameKey) => {
    (d1GroupIndex?.byNameKey?.get(nameKey) || []).forEach((entry) => {
      if (entry?.group?.nodeId) candidateMap.set(entry.group.nodeId, entry);
    });
  });
  const candidates = Array.from(candidateMap.values());
  if (!candidates.length) return [];

  (group?.rows || []).forEach((row) => {
    getExplicitProjectedD1SeasonSpecs(row).forEach((spec) => {
      const targetGroup = findExplicitProjectedD1Group(candidates, row, spec)?.group || null;
      if (targetGroup?.nodeId) matches.set(targetGroup.nodeId, targetGroup);
    });
  });
  return Array.from(matches.values());
}

function getExplicitProjectedD1SeasonSpecs(row) {
  const baseSeason = normalizeSeasonValue(row?.season);
  if (!Number.isFinite(baseSeason)) return [];
  const specs = [];
  for (let index = 1; index <= 6; index += 1) {
    const team = getStringValue(row?.[`ncaa_team_y${index}`]).trim();
    const eligibility = getStringValue(row?.[`ncaa_eligibility_y${index}`]).trim();
    const heightText = getStringValue(row?.[`ncaa_height_y${index}`]).trim();
    const hasNumericEvidence = Number.isFinite(firstFinite(
      row?.[`ncaa_spi_y${index}`],
      row?.[`ncaa_rank_y${index}`],
      row?.[`ncaa_off_impact_y${index}`],
      row?.[`ncaa_def_impact_y${index}`],
      row?.[`ncaa_wins_added_y${index}`],
      Number.NaN,
    ));
    if (!hasNumericEvidence && !team && !eligibility && !heightText) continue;
    specs.push({
      team,
      heightIn: parseHeightToInches(heightText),
    });
  }
  return specs;
}

function rowHasExplicitProjectedD1Data(row) {
  return getExplicitProjectedD1SeasonSpecs(row).length > 0;
}

function findExplicitProjectedD1Group(candidates, sourceRow, spec) {
  if (!candidates?.length || !sourceRow) return null;
  const compatible = candidates.filter((entry) => explicitProjectedD1GroupCompatible(entry?.group, sourceRow, spec));
  if (!compatible.length) return null;

  const team = getStringValue(spec?.team).trim();
  if (!team) {
    if (compatible.length === 1) return compatible[0];
    return null;
  }

  const teamVariants = buildTeamVariants(team);
  const scored = compatible
    .map((entry) => ({
      entry,
      teamScore: Math.max(
        ...teamVariants.map((variant) => Math.max(...entry.teamKeys.map((candidateTeam) => teamMatchScore(variant, candidateTeam)), 0)),
        0,
      ),
    }))
    .sort((left, right) => right.teamScore - left.teamScore);
  const best = scored[0];
  const second = scored[1];
  if (!best || best.teamScore < 50) return null;
  if (second && (best.teamScore - second.teamScore) < 12 && best.teamScore < 90) return null;
  return best.entry;
}

function explicitProjectedD1GroupCompatible(targetGroup, sourceRow, spec) {
  if (!targetGroup) return false;
  const sourceDob = getStringValue(sourceRow?.dob).trim();
  if (sourceDob && targetGroup.dobs?.length && !targetGroup.dobs.includes(sourceDob)) return false;

  if (targetGroup.heights?.length) {
    const anchorHeights = [
      firstFinite(sourceRow?.height_in, sourceRow?.inches, Number.NaN),
      spec?.heightIn,
    ].filter(Number.isFinite);
    if (anchorHeights.length && !targetGroup.heights.some((targetHeight) => anchorHeights.some((height) => Math.abs(height - targetHeight) <= 1))) {
      return false;
    }
  }
  return true;
}

function hasStatusPath(sourceGroup, graph, targetDatasetId, direction) {
  if (!sourceGroup?.nodeId || !graph?.nodes?.size) return false;
  const seen = new Set();
  const stack = [sourceGroup.nodeId];
  const edgeMap = direction === "backward" ? graph.backward : graph.forward;

  while (stack.length) {
    const nodeId = stack.pop();
    if (!nodeId || seen.has(nodeId)) continue;
    seen.add(nodeId);
    const nextIds = edgeMap.get(nodeId);
    if (!nextIds) continue;
    for (const nextId of nextIds) {
      if (seen.has(nextId)) continue;
      const nextGroup = graph.nodes.get(nextId);
      if (!nextGroup) continue;
      if (nextGroup.datasetId === targetDatasetId) return true;
      stack.push(nextId);
    }
  }

  return false;
}

function hasAnyStatusPath(sourceGroup, graph, targetDatasetId) {
  return hasStatusPath(sourceGroup, graph, targetDatasetId, "forward")
    || hasStatusPath(sourceGroup, graph, targetDatasetId, "backward");
}

function getReachableStatusGroups(sourceGroup, graph, targetDatasetId, direction) {
  if (!sourceGroup?.nodeId || !graph?.nodes?.size) return [];
  const seen = new Set([sourceGroup.nodeId]);
  const stack = [sourceGroup.nodeId];
  const edgeMap = direction === "backward" ? graph.backward : graph.forward;
  const matches = new Map();

  while (stack.length) {
    const nodeId = stack.pop();
    const nextIds = edgeMap.get(nodeId);
    if (!nextIds) continue;
    for (const nextId of nextIds) {
      if (seen.has(nextId)) continue;
      seen.add(nextId);
      const nextGroup = graph.nodes.get(nextId);
      if (!nextGroup) continue;
      if (nextGroup.datasetId === targetDatasetId) matches.set(nextId, nextGroup);
      stack.push(nextId);
    }
  }

  return Array.from(matches.values());
}

function annotateLinkedStatusMetrics(sourceDataset, graph) {
  if (!sourceDataset?.rows?.length || sourceDataset._linkedStatusMetricsAnnotated) return;
  const nbaDataset = appState.datasetCache?.nba;
  const sourceGroups = getStatusGroups(sourceDataset);
  sourceGroups.forEach((group) => {
    const manualD2NbaOverride = getManualD2NbaOverrideForGroup(group);
    const d1Groups = getReachableStatusGroups(group, graph, "d1", "forward");
    const d1Rows = d1Groups.flatMap((targetGroup) => targetGroup.rows || []);
    const d1PeakPrpg = d1Rows.map((row) => row.porpag).filter(Number.isFinite);
    const d1PeakDprpg = d1Rows.map((row) => row.dporpag).filter(Number.isFinite);
    const d1PeakBpm = d1Rows.map((row) => row.bpm).filter(Number.isFinite);
    const linkedNbaCareer = nbaDataset
      ? getReachableStatusGroups(group, graph, "nba", "forward")
        .map((targetGroup) => aggregateCareerRows(nbaDataset, targetGroup.rows || []))
        .filter(Boolean)
        .sort((left, right) => companionNbaCareerScore(right) - companionNbaCareerScore(left))[0]
      : null;
    const nbaCareer = linkedNbaCareer || findManualNbaCareer(manualD2NbaOverride, nbaDataset);
    group.rows.forEach((row) => {
      if (d1PeakPrpg.length) row.d1_peak_prpg = roundNumber(Math.max(...d1PeakPrpg), 3);
      if (d1PeakDprpg.length) row.d1_peak_dprpg = roundNumber(Math.max(...d1PeakDprpg), 3);
      if (d1PeakBpm.length) row.d1_peak_bpm = roundNumber(Math.max(...d1PeakBpm), 1);
      if (Number.isFinite(nbaCareer?.tot)) row.nba_career_epm = roundNumber(nbaCareer.tot, 1);
      if (sourceDataset.id === "fiba" && nbaCareer?.player_name) {
        row.player_name = nbaCareer.player_name;
        row.player = nbaCareer.player_name;
        row._searchCacheKey = "";
        row._searchHaystack = "";
      }
    });
  });
  sourceDataset._linkedStatusMetricsAnnotated = true;
}

function statusGroupsMatch(left, right) {
  if (!left || !right || left.datasetId === right.datasetId) return false;
  const leftKeys = new Set(left.nameKeys || [left.nameKey]);
  const sharedKey = (right.nameKeys || [right.nameKey]).find((key) => leftKeys.has(key));
  if (!sharedKey) return false;
  if (left.dobs.length && right.dobs.length && !left.dobs.some((dob) => right.dobs.includes(dob))) return false;
  if (left.heights.length && right.heights.length) {
    const tolerance = statusHeightTolerance(left.datasetId, right.datasetId);
    const compatibleHeight = left.heights.some((height) => right.heights.some((other) => Math.abs(height - other) <= tolerance));
    if (!compatibleHeight) return false;
  }
  const sameStrictName = left.nameKey && right.nameKey && left.nameKey === right.nameKey;
  const hasIdentityAnchor = left.dobs.length || right.dobs.length || left.heights.length || right.heights.length;
  if (!sameStrictName && !hasIdentityAnchor) return false;
  return true;
}

function isValidStatusTransition(sourceGroup, targetGroup) {
  if (!sourceGroup || !targetGroup || sourceGroup.datasetId === targetGroup.datasetId) return false;
  return getStatusTransitionPairs(sourceGroup, targetGroup).length > 0;
}

function buildStatusNameKeys(value) {
  const strictKey = normalizeNameKey(value);
  const looseKey = normalizeLooseNameKey(value);
  const keys = new Set([strictKey, looseKey].filter(Boolean));
  const tokens = strictKey.split(" ").filter(Boolean);
  if (tokens.length >= 3) {
    keys.add(tokens.slice(0, -1).join(" "));
    keys.add(tokens.slice(0, 2).join(" "));
    keys.add([tokens[0], tokens[tokens.length - 1]].join(" "));
  }
  if (tokens.length >= 4) {
    keys.add([tokens[0], tokens[1], tokens[tokens.length - 1]].join(" "));
  }
  return Array.from(keys).filter(Boolean);
}

function getStatusTransitionPairs(sourceGroup, targetGroup) {
  if (!sourceGroup || !targetGroup) return [];
  const pairs = [];
  sourceGroup.rows.forEach((sourceRow) => {
    const sourceYear = extractLeadingYear(sourceRow.season);
    if (!Number.isFinite(sourceYear) || sourceYear <= 0) return;
    targetGroup.rows.forEach((targetRow) => {
      const targetYear = extractLeadingYear(targetRow.season);
      if (!Number.isFinite(targetYear) || targetYear <= sourceYear) return;
      if (isUnanchoredLowerTierLink(sourceGroup, targetGroup)) return;
      if (!statusRowsCompatible(sourceRow, targetRow, sourceGroup.datasetId, targetGroup.datasetId)) return;
      if (isAmbiguousLowerTierPromotion(sourceGroup, targetGroup, sourceYear)) return;
      const gap = targetYear - sourceYear;
      if (gap < 1 || gap > maxStatusGap(sourceGroup, targetGroup, sourceYear)) return;
      if (targetGroup.datasetId === "nba" && !["d1", "d2", "fiba"].includes(sourceGroup.datasetId)) return;
      if (isLowerTierFreshmanJump(sourceGroup, targetGroup, targetRow)) return;
      if (isPre21NbaBlock(sourceGroup, targetGroup, targetRow)) return;
      if (isGraduationBlockedDrop(sourceRow, sourceGroup, targetGroup)) return;
      pairs.push({ sourceRow, targetRow, gap });
    });
  });
  return pairs;
}

function isUnanchoredLowerTierLink(sourceGroup, targetGroup) {
  if (!sourceGroup || !targetGroup) return false;
  const sourceTier = datasetTier(sourceGroup.datasetId);
  const targetTier = datasetTier(targetGroup.datasetId);
  if (sourceTier === targetTier) return false;
  const lowerGroup = sourceTier < targetTier ? sourceGroup : targetGroup;
  const higherGroup = sourceTier < targetTier ? targetGroup : sourceGroup;
  if (!["d2", "juco", "naia"].includes(lowerGroup.datasetId)) return false;
  if (!["d1", "nba"].includes(higherGroup.datasetId)) return false;
  const hasIdentityAnchor = (lowerGroup.dobs.length && higherGroup.dobs.length)
    || (lowerGroup.heights.length && higherGroup.heights.length);
  if (hasIdentityAnchor) return false;
  return higherGroup.years.length > 1;
}

function isAmbiguousLowerTierPromotion(sourceGroup, targetGroup, sourceYear) {
  if (!sourceGroup || !targetGroup) return false;
  if (!["d2", "juco", "naia"].includes(sourceGroup.datasetId)) return false;
  if (!["d1", "nba"].includes(targetGroup.datasetId)) return false;
  if (targetGroup.minYear >= sourceYear) return false;
  const hasIdentityAnchor = (sourceGroup.dobs.length && targetGroup.dobs.length)
    || (sourceGroup.heights.length && targetGroup.heights.length);
  return !hasIdentityAnchor;
}

function isLowerTierFreshmanJump(sourceGroup, targetGroup, targetRow) {
  if (!sourceGroup || !targetGroup) return false;
  if (!["d2", "juco", "naia"].includes(sourceGroup.datasetId)) return false;
  if (targetGroup.datasetId !== "d1") return false;
  return normalizeClassValue(targetRow?.class_year) === "Fr";
}

function getStatusAgeValue(row) {
  const directAge = firstFinite(row?.age, Number.NaN);
  if (Number.isFinite(directAge)) return directAge;
  const season = extractLeadingYear(row?.season);
  const dob = getStringValue(row?.dob).trim();
  if (!season || !dob) return Number.NaN;
  const parsedDob = new Date(dob);
  if (Number.isNaN(parsedDob.getTime())) return Number.NaN;
  const ref = new Date(Number(season), 5, 23);
  return (ref - parsedDob) / (1000 * 60 * 60 * 24 * 365.25);
}

function isPre21NbaBlock(sourceGroup, targetGroup, targetRow) {
  if (!sourceGroup || !targetGroup) return false;
  if (!["d2", "juco", "naia"].includes(sourceGroup.datasetId)) return false;
  if (targetGroup.datasetId !== "nba") return false;
  const age = getStatusAgeValue(targetRow);
  return Number.isFinite(age) && age < 21;
}

function statusRowsCompatible(sourceRow, targetRow, sourceDatasetId = "", targetDatasetId = "") {
  const sourceDob = getStringValue(sourceRow?.dob).trim();
  const targetDob = getStringValue(targetRow?.dob).trim();
  if (sourceDob && targetDob && sourceDob !== targetDob) return false;
  const sourceHeight = firstFinite(sourceRow?.height_in, sourceRow?.inches, Number.NaN);
  const targetHeight = firstFinite(targetRow?.height_in, targetRow?.inches, Number.NaN);
  if (Number.isFinite(sourceHeight) && Number.isFinite(targetHeight)) {
    const tolerance = statusHeightTolerance(sourceDatasetId, targetDatasetId);
    if (Math.abs(sourceHeight - targetHeight) > tolerance) return false;
  }
  return true;
}

function statusHeightTolerance(leftDatasetId, rightDatasetId) {
  return leftDatasetId === "fiba" || rightDatasetId === "fiba" ? 3 : 1;
}

function maxStatusGap(sourceGroup, targetGroup, sourceYear) {
  if (sourceGroup.datasetId === "d1" && targetGroup.datasetId === "nba") return 2;
  if (sourceGroup.datasetId === "fiba" && targetGroup.datasetId === "d1") return 8;
  if (sourceGroup.datasetId === "fiba" && targetGroup.datasetId === "nba") return 12;
  if (sourceGroup.datasetId === "d2" && targetGroup.datasetId === "d1") {
    return sourceYear <= 2020 ? 2 : 1;
  }
  return 1;
}

function isGraduationBlockedDrop(sourceRow, sourceGroup, targetGroup) {
  const sourceTier = datasetTier(sourceGroup.datasetId);
  const targetTier = datasetTier(targetGroup.datasetId);
  if (!(targetTier < sourceTier)) return false;
  const sourceClass = normalizeClassValue(sourceRow?.class_year);
  const sourceMinutes = getMinutesValue(sourceRow);
  return ["Sr", "Gr"].includes(sourceClass) && sourceMinutes >= MINUTES_DEFAULT;
}

function datasetTier(datasetId) {
  if (datasetId === "juco") return 1;
  if (datasetId === "naia") return 2;
  if (datasetId === "d2") return 3;
  if (datasetId === "d1") return 4;
  if (datasetId === "nba") return 5;
  return 0;
}

function enrichD1Rows(rows) {
  const rawRows = typeof playersData !== "undefined" ? playersData : (window.playersData || []);
  const runnerMetrics = typeof RUNNER_METRICS !== "undefined" ? RUNNER_METRICS : (window.RUNNER_METRICS || []);
  const drivesHeaders = typeof DRIVES_HEADERS !== "undefined" ? DRIVES_HEADERS : (window.DRIVES_HEADERS || []);
  const drivesData = typeof DRIVES_DATA !== "undefined" ? DRIVES_DATA : (window.DRIVES_DATA || {});
  const advHeaders = typeof ADV_HEADERS !== "undefined" ? ADV_HEADERS : (window.ADV_HEADERS || []);
  const advData = typeof ADV_DATA !== "undefined" ? ADV_DATA : (window.ADV_DATA || {});
  const coachDict = typeof COACH_DICT !== "undefined" ? COACH_DICT : (window.COACH_DICT || {});
  const rosterData = typeof ROSTER_DATA !== "undefined" ? ROSTER_DATA : (window.ROSTER_DATA || {});

  const rawIndex = buildD1CandidateMap(rawRows.map((values) => ({
    season: values[2],
    player: values[0],
    team: values[1],
    poss: Number(values[4]),
    values,
  })));
  const advIndex = Object.fromEntries(advHeaders.map((header, index) => [header, index]));
  const advEntries = Object.entries(advData).map(([key, values]) => {
    const [season = "", team = "", player = ""] = key.split("|");
    return { season, player, team, values };
  });
  const advMap = buildD1CandidateMap(advEntries);
  const driveEntries = Object.entries(drivesData).map(([key, values]) => {
    const [season = "", team = "", player = ""] = key.split("|");
    return { season, player, team, values };
  });
  const driveMap = buildD1CandidateMap(driveEntries);
  const rosterEntries = Object.entries(rosterData).map(([key, values]) => {
    const [season = "", team = "", player = ""] = key.split("|");
    return { season, player, team, values };
  });
  const rosterMap = buildD1CandidateMap(rosterEntries);
  const runnerIndex = Object.fromEntries(runnerMetrics.map((metric, index) => [metric, index]));
  const driveIndex = Object.fromEntries(drivesHeaders.map((metric, index) => [metric, index]));
  const rawMatches = assignD1CandidateMatches(rows, rawIndex);
  const advMatches = assignD1CandidateMatches(rows, advMap);
  const driveMatches = assignD1CandidateMatches(rows, driveMap);
  const rosterMatches = assignD1CandidateMatches(rows, rosterMap);

  rows.forEach((row) => {
    const rawMatch = rawMatches.get(row) || null;
    const advMatch = advMatches.get(row) || null;
    const driveMatch = driveMatches.get(row) || null;
    const rosterMatch = rosterMatches.get(row) || null;

    if (rawMatch?.values) {
      row._rawTeamScore = rawMatch._teamScore;
      const rawValues = rawMatch.values;
      if (Number.isFinite(Number(rawValues[3]))) row.gp = Number(rawValues[3]);
      if (Number.isFinite(Number(rawValues[4]))) row.total_poss = Number(rawValues[4]);
      if (rawValues[7]) row.class_year = normalizeClassValue(rawValues[7]);
      if (!Number.isFinite(row.height_in)) {
        const height = parseHeightToInches(rawValues[8]);
        if (Number.isFinite(height)) row.height_in = height;
      }
      if (Number.isFinite(Number(rawValues[6]))) row.draft_pick = Number(rawValues[6]);

      const runnerBase = 229;
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["%Time"], "runner_freq");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex.PPP, "runner_ppp");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["FTA/FGA"], "runner_ftr");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["FG Att"], "runner_fg_att");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["FG%"], "runner_fg_pct");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex.SSQ, "runner_ssq");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex.SSM, "runner_ssm");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["+1%"], "runner_plus1_pct");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["2 FG Att"], "runner_two_fg_att");
      assignD1RawMetric(row, rawValues, runnerBase, runnerIndex["2 FG%"], "runner_two_fg_pct");
    }

    if (advMatch?.values) {
      row._advTeamScore = advMatch._teamScore;
      const advValues = advMatch.values;
      if (advValues[advIndex.pid] != null && advValues[advIndex.pid] !== "") {
        row.player_id = getStringValue(advValues[advIndex.pid]);
      }
      assignIfFinite(row, "gp", advValues[advIndex.GP]);
      assignIfFinite(row, "min_per", advValues[advIndex.Min_per]);
      assignIfFinite(row, "mpg", advValues[advIndex.mp]);
      if (!Number.isFinite(row.min) && Number.isFinite(row.mpg) && Number.isFinite(row.gp) && row.gp > 0) {
        row.min = roundNumber(row.mpg * row.gp, 1);
      }
      const gp = firstFinite(row.gp, Number.NaN);
      [
        ["ortg", "ORtg"], ["drtg", "drtg"], ["adjoe", "adjoe"], ["adrtg", "adrtg"], ["porpag", "porpag"], ["dporpag", "dporpag"],
        ["bpm", "bpm"], ["obpm", "obpm"], ["dbpm", "dbpm"], ["orb_pct", "ORB_per"], ["drb_pct", "DRB_per"], ["ast_pct", "AST_per"],
        ["tov_pct_adv", "TO_per"], ["stl_pct", "stl_per"], ["blk_pct", "blk_per"], ["usg_pct", "usg"], ["ast_to", "ast/tov"],
        ["ft_pct", "FT_per"], ["two_p_pct_adv", "twoP_per"], ["three_p_pct_adv", "TP_per"], ["three_p_per100", "3p/100?"]
      ].forEach(([target, source]) => assignIfFinite(row, target, advValues[advIndex[source]]));
      [
        ["pts", "pts"], ["orb", "oreb"], ["drb", "dreb"], ["trb", "treb"], ["ast", "ast"], ["stl", "stl"], ["blk", "blk"],
      ].forEach(([target, source]) => {
        const perGameValue = Number(advValues[advIndex[source]]);
        if (Number.isFinite(perGameValue) && Number.isFinite(gp)) {
          row[target] = roundNumber(perGameValue * gp, 3);
        }
      });
      [
        ["fta", "FTA"], ["ftm", "FTM"], ["two_pa", "twoPA"], ["two_pm", "twoPM"], ["three_pa", "TPA"], ["three_pm", "TPM"],
      ].forEach(([target, source]) => assignIfFinite(row, target, advValues[advIndex[source]]));
      assignD1AdvShotProfile(row, advValues, advIndex);
      if (!Number.isFinite(row.fga) && Number.isFinite(row.two_pa) && Number.isFinite(row.three_pa)) row.fga = row.two_pa + row.three_pa;
      if (!Number.isFinite(row.fgm) && Number.isFinite(row.two_pm) && Number.isFinite(row.three_pm)) row.fgm = row.two_pm + row.three_pm;
      if (!Number.isFinite(row.tov) && Number.isFinite(row.ast) && Number.isFinite(row.ast_to) && row.ast_to > 0) {
        row.tov = roundNumber(row.ast / row.ast_to, 3);
      }
      if (!Number.isFinite(row.tov) && Number.isFinite(row.total_poss) && Number.isFinite(row.tov_pct_adv)) {
        row.tov = roundNumber(row.total_poss * (row.tov_pct_adv / 100), 3);
      }
      row.conference = getStringValue(advValues[advIndex.conf]) || row.conference;
      row.class_year = normalizeClassValue(advValues[advIndex.yr]) || row.class_year;
      const height = parseHeightToInches(advValues[advIndex.ht]);
      if (Number.isFinite(height)) row.height_in = height;
      row.dob = getStringValue(advValues[advIndex.birthday]) || row.dob;
      row.pos = normalizePosLabel(advValues[advIndex.role]) || row.pos;
    }

    if (driveMatch?.values) {
      const driveValues = driveMatch.values;
      const drivePoss = Number(driveValues[driveIndex.Poss]);
      const drivePoints = Number(driveValues[driveIndex.Points]);
      const driveTov = Number(driveValues[driveIndex.TO]);
      const driveFta = Number(driveValues[driveIndex.FTA]);
      const driveFgm = Number(driveValues[driveIndex.FGM]);
      const driveFga = Number(driveValues[driveIndex.FGA]);
      const driveTwoPm = Number(driveValues[driveIndex["2FGM"]]);
      const driveTwoPa = Number(driveValues[driveIndex["2FGA"]]);
      const drivePlus1 = Number(driveValues[driveIndex["+1"]]);
      const totalPoss = firstFinite(row.total_poss, rawMatch?.poss, Number.NaN);
      if (Number.isFinite(drivePoss) && Number.isFinite(totalPoss) && totalPoss > 0) {
        row.drive_freq = roundNumber((drivePoss / totalPoss) * 100, 1);
        const transitionPoss = firstFinite(row.transition_poss, Number.isFinite(row.transition_freq) ? (totalPoss * row.transition_freq) / 100 : Number.NaN, 0);
        const halfcourtPoss = totalPoss - transitionPoss;
        if (halfcourtPoss > 0) row.hc_drive_freq = roundNumber((drivePoss / halfcourtPoss) * 100, 1);
      }
      if (Number.isFinite(drivePoss) && drivePoss > 0 && Number.isFinite(drivePoints)) {
        row.drive_ppp = roundNumber(drivePoints / drivePoss, 3);
      }
      if (Number.isFinite(drivePoss)) row.drive_poss = drivePoss;
      if (Number.isFinite(driveFga)) row.drive_fga = driveFga;
      if (Number.isFinite(driveTwoPa)) row.drive_two_pa = driveTwoPa;
      row.drive_fg_pct = percentIfPossible(driveFgm, driveFga);
      row.drive_two_p_pct = percentIfPossible(driveTwoPm, driveTwoPa);
      row.drive_tov_pct = percentOfPossessions(driveTov, drivePoss);
      row.drive_ftr = ratioIfPossible(driveFta, driveFga);
      row.drive_plus1_pct = percentOfPossessions(drivePlus1, drivePoss);
    }

    if (rosterMatch?.values && !Number.isFinite(row.weight_lb)) {
      assignIfFinite(row, "weight_lb", rosterMatch.values.weight);
    }

    const coachName = getD1CoachName(row.season, rawMatch?.team || row.team_full || row.team_name, coachDict);
    if (coachName) row.coach = coachName;
    refreshDerivedBiography(row);
  });
  resolveD1SharedNameAssignments(rows);
}

function buildTeamVariants(value) {
  const raw = normalizeKey(value);
  const simplified = normalizeKey(simplifySchoolName(value, "d1"));
  const variants = new Set([raw, simplified]);
  const parts = simplified.split(" ").filter(Boolean);
  if (parts.length > 1) variants.add(parts.slice(0, -1).join(" "));
  return Array.from(variants).filter(Boolean);
}

function enrichNaiaRows(rows) {
  const csv = window.MASSEY_TEAM_CSV || "";
  const seasonMap = new Map();
  if (csv) {
    parseCSV(String(csv)).forEach((entry) => {
      const season = normalizeSeasonValue(entry.season);
      if (!season) return;
      buildSchoolKeys(entry.team, "naia").forEach((teamKey) => {
        seasonMap.set(`${season}|${teamKey}`, entry);
      });
    });
  }
  const divisionMap = window.NAIA_DIVISION_MAP || {};
  const divisionTeamMap = window.NAIA_DIVISION_TEAM_MAP || {};
  rows.forEach((row) => {
    const season = getStringValue(row.season);
    const candidates = buildSchoolKeys(row.team_full || row.team_name, "naia");
    const entry = candidates.map((key) => seasonMap.get(`${season}|${key}`)).find(Boolean);
    if (entry?.conference && !row.conference) row.conference = entry.conference;
    if (entry?.division && !row.division) row.division = normalizeNaiaDivision(entry.division);
    if (row.division) return;
    const resolved = resolveNaiaDivisionName(row.team_full || row.team_name, row.season);
    if (resolved) {
      row.division = normalizeNaiaDivision(resolved);
      return;
    }
    const legacySeason = normalizeLegacySeason(row.season);
    for (const candidate of candidates) {
      const seasonKey = `${legacySeason}|${candidate}`;
      if (divisionMap[seasonKey]) {
        row.division = normalizeNaiaDivision(divisionMap[seasonKey]);
        return;
      }
      if (divisionTeamMap[candidate]) {
        row.division = normalizeNaiaDivision(divisionTeamMap[candidate]);
        return;
      }
    }
  });
}

function backfillNaiaDivisionFromConference(rows) {
  const confSeasonMap = new Map();
  const confMap = new Map();

  rows.forEach((row) => {
    const conference = getStringValue(row.conference).trim();
    const division = normalizeNaiaDivision(row.division);
    if (!conference || !division) return;
    const confKey = normalizeKey(conference);
    const season = getStringValue(row.season);
    incrementValueCounter(confSeasonMap, `${season}|${confKey}`, division);
    incrementValueCounter(confMap, confKey, division);
  });

  rows.forEach((row) => {
    const normalized = normalizeNaiaDivision(row.division);
    if (normalized) {
      row.division = normalized;
      return;
    }
    const conference = getStringValue(row.conference).trim();
    if (!conference) return;
    const confKey = normalizeKey(conference);
    const season = getStringValue(row.season);
    const seasonGuess = normalizeNaiaDivision(pickBestCounterValue(confSeasonMap.get(`${season}|${confKey}`), false));
    const confGuess = normalizeNaiaDivision(pickBestCounterValue(confMap.get(confKey), true));
    const textGuess = normalizeNaiaDivision(inferNaiaDivisionFromText(conference));
    row.division = seasonGuess || confGuess || textGuess || row.division;
  });
}

function backfillTeamSeasonFields(rows, fields) {
  const seasonMaps = Object.fromEntries(fields.map((field) => [field, new Map()]));
  const teamMaps = Object.fromEntries(fields.map((field) => [field, new Map()]));

  rows.forEach((row) => {
    const season = getStringValue(row.season);
    const teamKey = normalizeKey(row.team_full || row.team_name);
    if (!teamKey) return;
    fields.forEach((field) => {
      const value = getStringValue(row[field]).trim();
      if (!value) return;
      incrementValueCounter(seasonMaps[field], `${season}|${teamKey}`, value);
      incrementValueCounter(teamMaps[field], teamKey, value);
    });
  });

  rows.forEach((row) => {
    const season = getStringValue(row.season);
    const teamKey = normalizeKey(row.team_full || row.team_name);
    if (!teamKey) return;
    fields.forEach((field) => {
      if (getStringValue(row[field]).trim()) return;
      const seasonValue = pickBestCounterValue(seasonMaps[field].get(`${season}|${teamKey}`), false);
      const teamValue = pickBestCounterValue(teamMaps[field].get(teamKey), true);
      if (seasonValue) row[field] = seasonValue;
      else if (teamValue) row[field] = teamValue;
    });
  });
}

function finalizeCategoricalFields(rows, datasetId) {
  rows.forEach((row) => {
    if (datasetId === "d1") {
      const conf = getStringValue(row.conference);
      if (!conf) row.conference = "Ind";
      else if (conf.toLowerCase() === "ind") row.conference = "Ind";
    }
    if (datasetId === "naia") {
      if (!getStringValue(row.conference)) row.conference = "Independent";
    }
    if (datasetId === "juco") {
      if (!getStringValue(row.region)) row.region = "Unknown";
    }
    row.team_search_text = Array.from(new Set([row.team_name, row.team_full, row.team_alias, row.team_alias_all].map((value) => getStringValue(value).trim()).filter(Boolean))).join(" ");
    row.coach_search_text = getStringValue(row.coach).trim();
  });
}

function incrementValueCounter(map, key, value) {
  if (!key || !value) return;
  if (!map.has(key)) map.set(key, new Map());
  const counter = map.get(key);
  counter.set(value, (counter.get(value) || 0) + 1);
}

function pickBestCounterValue(counter, requireDominant) {
  if (!counter || !counter.size) return "";
  const values = Array.from(counter.entries()).sort((left, right) => right[1] - left[1]);
  if (!values.length) return "";
  if (!requireDominant || values.length === 1) return values[0][0];
  if (values[0][1] >= (values[1]?.[1] || 0) * 2) return values[0][0];
  return "";
}

function normalizeNaiaDivision(value) {
  return inferNaiaDivisionFromText(value);
}

function inferNaiaDivisionFromText(value) {
  const text = getStringValue(value).toUpperCase();
  if (!text) return "";
  if (/\bDII\b|\bDIVISION\s*II\b|\bNAIA\s*II\b/.test(text)) return "DII";
  if (/\bDI\b|\bDIVISION\s*I\b|\bNAIA\s*I\b/.test(text)) return "DI";
  return "";
}

function simplifyKeyWithStopwords(key, stopwords) {
  if (!key) return "";
  return key.split(" ").filter((word) => word && !stopwords.has(word)).join(" ");
}

function stripNaiaStateTokens(key) {
  if (!key) return "";
  return key.split(" ").filter((word) => word && !NAIA_STATE_TOKENS.has(word)).join(" ");
}

function seasonEndYear(value) {
  const season = Number(normalizeSeasonValue(value));
  return Number.isFinite(season) ? season : null;
}

function buildNaiaDivisionLookups() {
  if (naiaDivisionLookup && naiaDivisionTeamLookup && naiaDivisionHistory) return;
  const divisionMap = window.NAIA_DIVISION_MAP || {};
  const divisionTeamMap = window.NAIA_DIVISION_TEAM_MAP || {};
  const seasonCandidates = new Map();
  const teamCandidates = new Map();
  const history = new Map();

  const addCandidate = (map, key, division) => {
    if (!key || !division) return;
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(division);
  };

  Object.keys(divisionMap).forEach((fullKey) => {
    if (!fullKey.includes("|")) return;
    const [season, rawKey] = fullKey.split("|");
    const division = normalizeNaiaDivision(divisionMap[fullKey]);
    if (!season || !rawKey || !division) return;
    const variants = [
      rawKey,
      stripNaiaStateTokens(rawKey),
      simplifyKeyWithStopwords(rawKey, NAIA_DIVISION_STOPWORDS),
      simplifyKeyWithStopwords(rawKey, NAIA_DIVISION_STOPWORDS_EXT),
      simplifyKeyWithStopwords(stripNaiaStateTokens(rawKey), NAIA_DIVISION_STOPWORDS),
      simplifyKeyWithStopwords(stripNaiaStateTokens(rawKey), NAIA_DIVISION_STOPWORDS_EXT),
    ].filter(Boolean);
    variants.forEach((variant) => addCandidate(seasonCandidates, `${normalizeSeasonValue(season)}|${variant}`, division));
    const endYear = seasonEndYear(season);
    if (!endYear) return;
    if (!history.has(rawKey)) history.set(rawKey, []);
    history.get(rawKey).push({ year: endYear, division });
  });

  Object.keys(divisionTeamMap).forEach((rawKey) => {
    const division = normalizeNaiaDivision(divisionTeamMap[rawKey]);
    if (!rawKey || !division) return;
    [
      rawKey,
      stripNaiaStateTokens(rawKey),
      simplifyKeyWithStopwords(rawKey, NAIA_DIVISION_STOPWORDS),
      simplifyKeyWithStopwords(rawKey, NAIA_DIVISION_STOPWORDS_EXT),
      simplifyKeyWithStopwords(stripNaiaStateTokens(rawKey), NAIA_DIVISION_STOPWORDS),
      simplifyKeyWithStopwords(stripNaiaStateTokens(rawKey), NAIA_DIVISION_STOPWORDS_EXT),
    ].filter(Boolean).forEach((variant) => addCandidate(teamCandidates, variant, division));
  });

  const seasonLookup = {};
  seasonCandidates.forEach((set, key) => {
    if (set.size === 1) seasonLookup[key] = Array.from(set)[0];
  });
  const teamLookup = {};
  teamCandidates.forEach((set, key) => {
    if (set.size === 1) teamLookup[key] = Array.from(set)[0];
  });
  history.forEach((list) => list.sort((left, right) => left.year - right.year));

  naiaDivisionLookup = seasonLookup;
  naiaDivisionTeamLookup = teamLookup;
  naiaDivisionHistory = history;
}

function getNaiaDivisionCandidates(teamName) {
  const raw = normalizeKey(teamName);
  if (!raw) return [];
  const noParen = normalizeKey(getStringValue(teamName).replace(/\([^)]*\)/g, " "));
  const candidates = [];
  const push = (value) => {
    if (value && !candidates.includes(value)) candidates.push(value);
  };
  push(raw);
  push(noParen);
  [...candidates].forEach((candidate) => {
    const noState = stripNaiaStateTokens(candidate);
    push(noState);
    push(simplifyKeyWithStopwords(candidate, NAIA_DIVISION_STOPWORDS));
    push(simplifyKeyWithStopwords(candidate, NAIA_DIVISION_STOPWORDS_EXT));
    push(simplifyKeyWithStopwords(noState, NAIA_DIVISION_STOPWORDS));
    push(simplifyKeyWithStopwords(noState, NAIA_DIVISION_STOPWORDS_EXT));
  });
  return candidates;
}

function resolveNaiaDivisionName(teamName, season) {
  buildNaiaDivisionLookups();
  const candidates = getNaiaDivisionCandidates(teamName);
  const endYear = seasonEndYear(season);

  const resolveFromHistory = (key) => {
    if (!naiaDivisionHistory || !endYear) return "";
    const list = naiaDivisionHistory.get(key);
    if (!list?.length) return "";
    let latest = "";
    for (let index = 0; index < list.length; index += 1) {
      if (list[index].year <= endYear) latest = list[index].division;
      else break;
    }
    return latest || list[0].division || "";
  };

  for (const candidate of candidates) {
    const alias = NAIA_DIVISION_ALIASES[candidate];
    if (alias) {
      const aliasDivision = inferNaiaDivisionFromText(alias);
      if (aliasDivision) return aliasDivision;
      const aliasSeasonKey = `${normalizeSeasonValue(season)}|${alias}`;
      if (naiaDivisionLookup?.[aliasSeasonKey]) return naiaDivisionLookup[aliasSeasonKey];
      if (naiaDivisionTeamLookup?.[alias]) return naiaDivisionTeamLookup[alias];
      const aliasHistory = resolveFromHistory(alias);
      if (aliasHistory) return aliasHistory;
    }

    const seasonKey = `${normalizeSeasonValue(season)}|${candidate}`;
    if (naiaDivisionLookup?.[seasonKey]) return naiaDivisionLookup[seasonKey];
    if (naiaDivisionTeamLookup?.[candidate]) return naiaDivisionTeamLookup[candidate];
    const historyDivision = resolveFromHistory(candidate);
    if (historyDivision) return historyDivision;
  }
  return "";
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

function normalizeTeamToken(value) {
  return getStringValue(value).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function buildSchoolKeys(value, datasetId) {
  const raw = getStringValue(value).trim();
  if (!raw) return [];
  const keys = new Set();
  const push = (candidate) => {
    const key = normalizeKey(candidate);
    if (key) keys.add(key);
  };
  push(raw);
  push(raw.replace(/\([^)]*\)/g, " "));
  push(simplifySchoolName(raw, datasetId));
  const strippedState = stripStateTokens(raw);
  if (strippedState !== raw) push(strippedState);
  return Array.from(keys);
}

function stripStateTokens(value) {
  const tokens = normalizeKey(value).split(" ").filter(Boolean);
  const cleaned = tokens.filter((token) => !TEAM_STATE_TOKENS.has(token));
  return cleaned.join(" ");
}

function teamMatchScore(left, right) {
  if (!left || !right) return 0;
  if (left === right) return 100;
  const leftTokens = new Set(left.split(" ").filter(Boolean));
  const rightTokens = new Set(right.split(" ").filter(Boolean));
  let overlap = 0;
  rightTokens.forEach((token) => {
    if (leftTokens.has(token)) overlap += 1;
  });
  if (!overlap) return 0;
  const minSize = Math.min(leftTokens.size, rightTokens.size);
  const maxSize = Math.max(leftTokens.size, rightTokens.size);
  if (overlap === minSize && minSize >= 2) return 82;
  return Math.round((overlap / maxSize) * 70);
}

function buildD1CandidateMap(entries) {
  const map = new Map();
  let counter = 0;
  entries.forEach((entry) => {
    if (!entry._candidateId) entry._candidateId = `d1cand_${counter += 1}`;
    const season = getStringValue(normalizeSeasonValue(entry.season));
    const name = normalizeNameKey(entry.player);
    if (!season || !name) return;
    const key = `${season}|${name}`;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(entry);
  });
  return map;
}

function collectD1Candidates(index, seasonValue, playerName) {
  const season = getStringValue(normalizeSeasonValue(seasonValue));
  if (!season) return [];
  const nameKeys = Array.from(new Set([normalizeNameKey(playerName), normalizeLooseNameKey(playerName)].filter(Boolean)));
  const unique = new Map();
  nameKeys.forEach((nameKey) => {
    (index.get(`${season}|${nameKey}`) || []).forEach((entry) => {
      if (!entry?._candidateId) return;
      unique.set(entry._candidateId, entry);
    });
  });
  return Array.from(unique.values());
}

function scoreD1CandidateMatch(candidate, teamVariants, possTarget) {
  const candidateTeam = normalizeKey(candidate?.team);
  const teamScore = Math.max(...teamVariants.map((team) => teamMatchScore(team, candidateTeam)), 0);
  const possScore = Number.isFinite(possTarget) && Number.isFinite(candidate?.poss) ? Math.max(0, 12 - Math.abs(possTarget - candidate.poss) / 20) : 0;
  return {
    candidate,
    score: (teamScore * 2) + possScore,
    teamScore,
    possScore,
  };
}

function isUsableD1CandidateScore(match) {
  return Boolean(match?.candidate) && match.score >= 50 && match.teamScore >= 50;
}

function findBestD1Candidate(index, seasonValue, playerName, teamVariants, possTarget) {
  const candidates = collectD1Candidates(index, seasonValue, playerName);
  if (!candidates.length) return null;
  let best = null;
  let bestScore = -Infinity;
  let bestTeamScore = -Infinity;
  let secondBestScore = -Infinity;
  candidates.forEach((candidate) => {
    const { score, teamScore } = scoreD1CandidateMatch(candidate, teamVariants, possTarget);
    if (score > secondBestScore) secondBestScore = score;
    if (score > bestScore) {
      secondBestScore = bestScore;
      bestScore = score;
      bestTeamScore = teamScore;
      best = candidate;
    }
  });
  if (!best || bestScore < 50 || bestTeamScore < 50) return null;
  if (secondBestScore > -Infinity && (bestScore - secondBestScore) < 12 && bestTeamScore < 90) return null;
  return { ...best, _matchScore: bestScore, _teamScore: bestTeamScore };
}

function assignD1CandidateMatches(rows, index) {
  const matches = new Map();
  const grouped = new Map();

  rows.forEach((row) => {
    const season = getStringValue(normalizeSeasonValue(row.season));
    const nameKey = normalizeNameKey(row.player_name);
    if (!season || !nameKey) return;
    const key = `${season}|${nameKey}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  grouped.forEach((group) => {
    if (group.length === 1) {
      const row = group[0];
      const teamVariants = buildTeamVariants(row.team_full || row.team_name);
      const possTarget = firstFinite(row.total_poss, row.min, Number.NaN);
      matches.set(row, findBestD1Candidate(index, row.season, row.player_name, teamVariants, possTarget));
      return;
    }

    const candidateMap = new Map();
    group.forEach((row) => {
      collectD1Candidates(index, row.season, row.player_name).forEach((candidate) => {
        candidateMap.set(candidate._candidateId, candidate);
      });
    });
    const candidates = Array.from(candidateMap.values());
    if (!candidates.length) {
      group.forEach((row) => matches.set(row, null));
      return;
    }

    const rowOptions = group.map((row) => {
      const teamVariants = buildTeamVariants(row.team_full || row.team_name);
      const possTarget = firstFinite(row.total_poss, row.min, Number.NaN);
      return {
        row,
        options: candidates
          .map((candidate) => scoreD1CandidateMatch(candidate, teamVariants, possTarget))
          .filter(isUsableD1CandidateScore)
          .sort((left, right) => right.score - left.score),
      };
    }).sort((left, right) => left.options.length - right.options.length);

    let bestScore = -Infinity;
    let bestCount = -Infinity;
    let bestMap = new Map();

    function search(rowIndex, usedIds, assigned, score, count) {
      if (rowIndex >= rowOptions.length) {
        if (score > bestScore || (Math.abs(score - bestScore) < 0.001 && count > bestCount)) {
          bestScore = score;
          bestCount = count;
          bestMap = new Map(assigned);
        }
        return;
      }

      const current = rowOptions[rowIndex];
      search(rowIndex + 1, usedIds, assigned, score, count);
      current.options.forEach((option) => {
        const candidateId = option.candidate._candidateId;
        if (usedIds.has(candidateId)) return;
        usedIds.add(candidateId);
        assigned.set(current.row, { ...option.candidate, _matchScore: option.score, _teamScore: option.teamScore });
        search(rowIndex + 1, usedIds, assigned, score + option.score, count + 1);
        assigned.delete(current.row);
        usedIds.delete(candidateId);
      });
    }

    search(0, new Set(), new Map(), 0, 0);
    group.forEach((row) => matches.set(row, bestMap.get(row) || null));
  });

  return matches;
}

function resolveD1SharedNameAssignments(rows) {
  const grouped = new Map();
  rows.forEach((row) => {
    const season = getStringValue(row.season);
    const nameKey = normalizeNameKey(row.player_name);
    if (!season || !nameKey) return;
    const key = `${season}|${nameKey}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  grouped.forEach((group) => {
    if (group.length < 2) return;
    dedupeSharedField(group, "draft_pick", "_rawTeamScore");
    dedupeSharedField(group, "player_id", "_advTeamScore");
  });
}

function dedupeSharedField(rows, field, scoreField) {
  const values = new Map();
  rows.forEach((row) => {
    if (field === "draft_pick") {
      if (!Number.isFinite(row.draft_pick) || row._draftPickBlank) return;
      const key = String(row.draft_pick);
      if (!values.has(key)) values.set(key, []);
      values.get(key).push(row);
      return;
    }
    const key = getStringValue(row[field]).trim();
    if (!key) return;
    if (!values.has(key)) values.set(key, []);
    values.get(key).push(row);
  });

  values.forEach((dupes) => {
    if (dupes.length < 2) return;
    const best = dupes.slice().sort((left, right) => {
      const leftScore = firstFinite(left[scoreField], left._rawTeamScore, left._advTeamScore, 0);
      const rightScore = firstFinite(right[scoreField], right._rawTeamScore, right._advTeamScore, 0);
      if (rightScore !== leftScore) return rightScore - leftScore;
      return duplicateRowScore(right) - duplicateRowScore(left);
    })[0];
    dupes.forEach((row) => {
      if (row === best) return;
      if (field === "draft_pick") {
        row._draftPickBlank = true;
        row.draft_pick = 61;
      } else {
        row[field] = "";
      }
    });
  });
}

function normalizeNameKey(value) {
  return normalizeKey(value).replace(/\b(jr|sr|ii|iii|iv|v)\b/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeLooseNameKey(value) {
  return normalizeNameKey(value).replace(/\b[a-z]\b/g, " ").replace(/\s+/g, " ").trim();
}

let manualD2NbaOverrideIndex = null;

function getManualD2NbaOverrideIndex() {
  if (manualD2NbaOverrideIndex) return manualD2NbaOverrideIndex;
  const index = {};
  D2_PDF_NBA_OVERRIDE_ROWS.forEach((entry) => {
    index[makeManualD2NbaOverrideKey(entry.season, entry.team, entry.player)] = entry;
  });
  manualD2NbaOverrideIndex = index;
  return manualD2NbaOverrideIndex;
}

function makeManualD2NbaOverrideKey(season, team, player) {
  return `d2|${getStringValue(season).trim()}|${normalizeKey(team)}|${normalizeNameKey(player)}`;
}

function getManualD2NbaOverrideForRow(row) {
  if (!row) return null;
  return getManualD2NbaOverrideIndex()[makeManualD2NbaOverrideKey(row.season, row.team_name || row.team_full, row.player_name || row.player)] || null;
}

function getManualD2NbaOverrideForGroup(group) {
  if (!group || group.datasetId !== "d2") return null;
  return (group.rows || []).map((row) => getManualD2NbaOverrideForRow(row)).find(Boolean) || null;
}

function isSupplementDeferredDataset(dataset) {
  return dataset?.deferredHydrationMode === "supplement";
}

function isDeferredSupplementColumn(dataset, column) {
  return Boolean(column && isSupplementDeferredDataset(dataset) && getDeferredColumns(dataset).includes(column));
}

function getRowColumnValue(dataset, row, column) {
  if (!row || !column) return row?.[column];
  const direct = row[column];
  return direct;
}

function findManualNbaCareer(override, nbaDataset) {
  if (!override?.nba_name || !nbaDataset?.rows?.length) return null;
  const targetName = normalizeNameKey(override.nba_name);
  const firstYear = Number(override.first_nba_year);
  const rows = nbaDataset.rows.filter((row) => {
    if (normalizeNameKey(row.player_name) !== targetName) return false;
    if (Number.isFinite(firstYear) && Number.isFinite(row.rookie_year) && row.rookie_year < firstYear - 1) return false;
    return true;
  });
  if (!rows.length) return null;
  const merged = mergeCareerRowGroups(nbaDataset, rows.map((row) => [row]));
  return merged
    .map((groupRows) => aggregateCareerRows(nbaDataset, groupRows))
    .filter(Boolean)
    .sort((left, right) => companionNbaCareerScore(right) - companionNbaCareerScore(left))[0] || null;
}

function assignD1RawMetric(row, values, baseIndex, metricIndex, target) {
  if (!Array.isArray(values) || !Number.isInteger(metricIndex) || metricIndex < 0) return;
  assignIfFinite(row, target, values[baseIndex + metricIndex]);
}

function assignIfFinite(row, target, value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) row[target] = numeric;
}

function assignD1AdvShotProfile(row, advValues, advIndex) {
  assignIfFinite(row, "rim_made", advValues[advIndex.rimmade]);
  assignIfFinite(row, "rim_att", advValues[advIndex["rimmade+rimmiss"]]);
  assignIfFinite(row, "rim_pct", advValues[advIndex["rimmade/(rimmade+rimmiss)"]]);
  assignIfFinite(row, "mid_made", advValues[advIndex.midmade]);
  assignIfFinite(row, "mid_att", advValues[advIndex["midmade+midmiss"]]);
  assignIfFinite(row, "mid_pct", advValues[advIndex["midmade/(midmade+midmiss)"]]);
  assignIfFinite(row, "dunk_made", advValues[advIndex.dunksmade]);
  assignIfFinite(row, "dunk_att", advValues[advIndex["dunksmiss+dunksmade"]]);
  assignIfFinite(row, "dunk_pct", advValues[advIndex["dunksmade/(dunksmade+dunksmiss)"]]);
  assignIfFinite(row, "two_p_made", advValues[advIndex.twoPM]);
  assignIfFinite(row, "two_p_att", advValues[advIndex.twoPA]);
  assignIfFinite(row, "two_p_pct", advValues[advIndex.twoP_per]);
  assignIfFinite(row, "three_p_made", advValues[advIndex.TPM]);
  assignIfFinite(row, "three_p_att", advValues[advIndex.TPA]);
  assignIfFinite(row, "three_p_pct", advValues[advIndex.TP_per]);
  assignIfFinite(row, "ft_pct", advValues[advIndex.FT_per]);
}

function getD1CoachName(seasonValue, teamValue, coachDict) {
  const yearMap = coachDict?.[getStringValue(seasonValue)];
  if (!yearMap) return "";
  const teamVariants = buildTeamVariants(teamValue);
  let bestCoach = "";
  let bestScore = 0;
  Object.entries(yearMap).forEach(([team, coach]) => {
    const candidate = normalizeKey(team);
    const score = Math.max(...teamVariants.map((variant) => teamMatchScore(variant, candidate)), 0);
    if (score > bestScore) {
      bestScore = score;
      bestCoach = coach;
    }
  });
  return bestScore >= 45 ? bestCoach : "";
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

function classToOrdinal(value) {
  const normalized = normalizeClassValue(value);
  if (normalized === "Fr") return 1;
  if (normalized === "So") return 2;
  if (normalized === "Jr") return 3;
  if (normalized === "Sr") return 4;
  if (normalized === "Gr") return 5;
  return 0;
}

function ordinalToClass(value) {
  const ordinal = Math.max(1, Math.min(5, Math.round(Number(value) || 0)));
  if (ordinal === 1) return "Fr";
  if (ordinal === 2) return "So";
  if (ordinal === 3) return "Jr";
  if (ordinal === 4) return "Sr";
  return "Gr";
}

function normalizePosLabel(value) {
  const text = getStringValue(value).toUpperCase();
  if (/\bPOINT GUARD\b/.test(text)) return "PG";
  if (/\bPG\b|PURE PG/.test(text)) return "PG";
  if (/\bSHOOTING GUARD\b/.test(text)) return "SG";
  if (/\bSG\b|SCORING PG|COMBO G/.test(text)) return "SG";
  if (/\bSMALL FORWARD\b/.test(text)) return "SF";
  if (/\bSF\b|WING/.test(text)) return "SF";
  if (/\bPOWER FORWARD\b/.test(text)) return "PF";
  if (/\bPF\b|STRETCH 4/.test(text)) return "PF";
  if (/^\s*GUARD\s*$/.test(text)) return "G";
  if (/^\s*FORWARD\s*$/.test(text)) return "F";
  if (/^\s*CENTER\s*$/.test(text)) return "C";
  if (/\bC\b/.test(text)) return "C";
  return getStringValue(value);
}

function refreshDerivedBiography(row) {
  if (!Number.isFinite(row.bmi) && Number.isFinite(row.height_in) && Number.isFinite(row.weight_lb) && row.height_in > 0) {
    row.bmi = roundNumber((row.weight_lb / (row.height_in * row.height_in)) * 703, 1);
  }
  if (!Number.isFinite(row.age) && row.dob && Number.isFinite(row.season)) {
    const dob = new Date(row.dob);
    if (!Number.isNaN(dob.getTime())) {
      const ref = new Date(Number(row.season), 5, 23);
      row.age = roundNumber((ref - dob) / (1000 * 60 * 60 * 24 * 365.25), 1);
    }
  }
}

function inferMissingClassYears(rows, config) {
  if (!rows?.length || !config?.yearColumn) return;
  const miniDataset = { id: config.id, yearColumn: config.yearColumn };
  const grouped = new Map();
  rows.forEach((row) => {
    const key = getCareerGroupKey(miniDataset, row);
    if (!key) return;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  mergeCareerRowGroups(miniDataset, Array.from(grouped.values())).forEach((careerRows) => {
    const sorted = careerRows
      .slice()
      .sort((left, right) => extractLeadingYear(left[config.yearColumn]) - extractLeadingYear(right[config.yearColumn]));
    const anchors = sorted
      .map((row, index) => ({
        index,
        year: extractLeadingYear(row[config.yearColumn]),
        ordinal: classToOrdinal(row.class_year),
      }))
      .filter((item) => item.ordinal);

    sorted.forEach((row, index) => {
      const normalized = normalizeClassValue(row.class_year);
      if (normalized) {
        row.class_year = normalized;
        return;
      }
      const rowYear = extractLeadingYear(row[config.yearColumn]);
      let best = null;
      anchors.forEach((anchor) => {
        const offset = rowYear && anchor.year ? rowYear - anchor.year : index - anchor.index;
        const candidate = anchor.ordinal + offset;
        if (candidate < 1 || candidate > 5) return;
        const distance = Math.abs(offset);
        if (!best || distance < best.distance) {
          best = { distance, candidate };
        }
      });
      row.class_year = ordinalToClass(best ? best.candidate : Math.min(5, index + 1));
    });
  });
}

function normalizeRepeatedSeniorSeasons(rows, config) {
  if (!rows?.length || !config?.yearColumn) return;
  const miniDataset = { id: config.id, yearColumn: config.yearColumn };
  const grouped = new Map();
  rows.forEach((row) => {
    const key = getCareerGroupKey(miniDataset, row);
    if (!key) return;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  mergeCareerRowGroups(miniDataset, Array.from(grouped.values())).forEach((careerRows) => {
    const seniorRows = careerRows
      .filter((row) => ["Sr", "Gr"].includes(normalizeClassValue(row.class_year)))
      .sort((left, right) => extractLeadingYear(left[config.yearColumn]) - extractLeadingYear(right[config.yearColumn]));
    if (!seniorRows.length) return;
    seniorRows.forEach((row, index) => {
      const season = extractLeadingYear(row[config.yearColumn]);
      row.class_year = index === 0 || season <= 2021 ? "Sr" : "Gr";
    });
  });
}

function normalizeLegacySeason(value) {
  const season = Number(normalizeSeasonValue(value));
  if (!Number.isFinite(season)) return "";
  const start = season - 1;
  return `${start}-${String(season).slice(-2)}`;
}

function createInitialUiState(dataset) {
  const visibleColumns = {};
  dataset.meta.allColumns.forEach((column) => {
    visibleColumns[column] = dataset.defaultVisible.includes(column);
  });
  getLockedColumns(dataset).forEach((column) => {
    if (column in visibleColumns) visibleColumns[column] = true;
  });

  const numericFilters = {};
  dataset.meta.statColumns.filter((column) => dataset.meta.numericColumnSet.has(column)).forEach((column) => {
    numericFilters[column] = { min: "", max: "" };
  });

  const demoFilters = {};
  dataset.meta.demoFilterMeta.forEach((item) => {
    demoFilters[item.column] = { min: "", max: "" };
  });
  if (dataset.meta.minuteFilterColumn && demoFilters[dataset.meta.minuteFilterColumn]) {
    demoFilters[dataset.meta.minuteFilterColumn].min = String(getDatasetMinuteThreshold(dataset));
  }

  const state = {
    search: "",
    team: "all",
    years: new Set(dataset.defaultAllYears ? dataset.meta.years : (dataset.meta.latestYear ? [dataset.meta.latestYear] : dataset.meta.years)),
    sortBy: dataset.sortBy,
    sortDir: dataset.sortDir,
    sortBlankMode: "last",
    extraSelects: Object.fromEntries((dataset.singleFilters || []).map((filter) => [filter.id, filter.defaultValue ?? "all"])),
    multiSelects: Object.fromEntries((dataset.multiFilters || []).map((filter) => [filter.id, new Set()])),
    visibleColumns,
    demoFilters,
    numericFilters,
    groupCycles: Object.fromEntries(dataset.meta.groups.map((group) => [group.id, 0])),
    visibleCount: LOAD_STEP,
    _careerCache: null,
    _renderCache: null,
    _visibleSupplementLoadKey: "",
  };
  if (dataset.meta.latestYear && dataset.meta.years.length > 1 && !getFilteredRows(dataset, state).length) {
    state.years = new Set(dataset.meta.years);
  }
  return state;
}

function resetUiCaches(state) {
  if (!state) return;
  state._careerCache = null;
  state._renderCache = null;
}

function scheduleSearchResultsRender(dataset, state) {
  if (appState.searchRenderTimer) window.clearTimeout(appState.searchRenderTimer);
  const datasetId = dataset?.id;
  appState.searchRenderTimer = window.setTimeout(() => {
    appState.searchRenderTimer = 0;
    if (appState.currentId !== datasetId) return;
    renderResultsOnly(dataset, state);
  }, SEARCH_RENDER_DEBOUNCE_MS);
}

function applySearchInputValue(dataset, state, rawValue) {
  const parsed = parseSearchDirectives(dataset, rawValue, state);
  const nextSearch = parsed.search;
  const currentSearch = getStringValue(state.search);
  const filtersChanged = parsed.filtersChanged;
  const searchChanged = nextSearch !== currentSearch;
  state.search = nextSearch;
  return searchChanged || filtersChanged ? { searchChanged, filtersChanged } : null;
}

function parseSearchDirectives(dataset, rawValue, state) {
  let filtersChanged = false;
  let consumedDirective = false;
  let search = getStringValue(rawValue);
  const statusFilter = (dataset?.singleFilters || []).find((filter) => filter.id === "status_path");
  if (statusFilter) {
    const statusLookup = new Map((statusFilter.options || []).map((option) => {
      const aliases = new Set([
        normalizeDirectiveValue(option.value),
        normalizeDirectiveValue(option.label),
      ]);
      if (option.value === "formerd1") aliases.add("former_d1");
      if (option.value === "former_juco") aliases.add("juco");
      return [Array.from(aliases), option.value];
    }).flatMap(([aliases, value]) => aliases.map((alias) => [alias, value])));
    search = search.replace(/(^|\s)status:([^\s,;]+)/gi, (match, prefix, rawStatus) => {
      consumedDirective = true;
      const normalized = normalizeDirectiveValue(rawStatus);
      const nextValue = statusLookup.get(normalized);
      if (nextValue && state.extraSelects.status_path !== nextValue) {
        state.extraSelects.status_path = nextValue;
        filtersChanged = true;
      }
      return prefix;
    });
  }
  return { search: search.replace(/\s{2,}/g, " ").trim(), filtersChanged: filtersChanged || consumedDirective };
}

function normalizeDirectiveValue(value) {
  return getStringValue(value).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

async function applyVisibilityMode(section, mode) {
  const dataset = getCurrentDataset();
  const state = getCurrentUiState();
  if (!dataset || !state) return;

  const columns = section === "demo" ? getDemoControlColumns(dataset) : dataset.meta.statColumns;

  if (section === "stats" && mode === "clear") {
    columns.forEach((column) => {
      if (state.numericFilters[column]) state.numericFilters[column] = { min: "", max: "" };
    });
    renderResultsOnly(dataset, state);
    return;
  }

  if (section === "stats" && mode !== "none") {
    const nextVisibleColumns = columns.filter((column) => {
      if (!(column in state.visibleColumns) || getLockedColumns(dataset).includes(column)) return false;
      if (mode === "all") return !state.visibleColumns[column];
      return dataset.defaultVisible.includes(column) && !state.visibleColumns[column];
    });
    if (nextVisibleColumns.length) {
      await ensureDeferredColumnsReady(dataset, state, nextVisibleColumns, { scope: "visible" });
    }
  }

  columns.forEach((column) => {
    if (!(column in state.visibleColumns)) return;
    if (getLockedColumns(dataset).includes(column)) return;
    if (mode === "default") {
      state.visibleColumns[column] = dataset.defaultVisible.includes(column);
    } else if (mode === "all") {
      state.visibleColumns[column] = true;
    } else if (mode === "none") {
      state.visibleColumns[column] = false;
    } else {
      state.visibleColumns[column] = false;
    }

    if (section === "stats" && mode === "none" && state.numericFilters[column]) {
      state.numericFilters[column] = { min: "", max: "" };
    }
  });

  renderCurrentDataset();
}

function renderCurrentDataset() {
  const dataset = getCurrentDataset();
  const state = getCurrentUiState();
  if (!dataset || !state) return;

  renderFilters(dataset, state);
  renderResultsOnly(dataset, state);
  elements.statusPill.textContent = `${dataset.navLabel} ready`;
}

function maybeScheduleVisibleDeferredSupplementLoad(dataset, state) {
  if (!isSupplementDeferredDataset(dataset)) return;
  if (!getVisibleDeferredColumns(dataset, state).length) return;
  if (state?.extraSelects?.view_mode === "career" || usesDeferredColumnForSortOrFilter(dataset, state)) return;
  const missingYears = getMissingDeferredSupplementYearsForScope(dataset, state, "visible");
  if (!missingYears.length) return;
  const key = [
    dataset.id,
    Array.from(state?.years || []).sort(compareYears).join("|"),
    state.visibleCount,
    getStringValue(state.team),
    state.search.trim().toLowerCase(),
    missingYears.join("|"),
  ].join("||");
  if (state._visibleSupplementLoadKey === key) return;
  state._visibleSupplementLoadKey = key;
  Promise.resolve().then(async () => {
    try {
      if (appState.currentId === dataset.id) {
        elements.statusPill.textContent = `Loading ${dataset.navLabel} shot profile`;
      }
      await ensureDeferredSupplementScripts(dataset, state, { scope: "visible", years: missingYears });
      if (appState.currentId !== dataset.id) return;
      resetUiCaches(state);
      renderResultsOnly(dataset, state);
      elements.statusPill.textContent = `${dataset.navLabel} ready`;
    } catch (error) {
      if (appState.currentId !== dataset.id) return;
      elements.statusPill.textContent = `${dataset.navLabel} shot profile failed`;
      elements.resultsSubtitle.textContent = getStringValue(error?.message || error);
    } finally {
      if (state._visibleSupplementLoadKey === key) state._visibleSupplementLoadKey = "";
    }
  });
}

function renderResultsOnly(dataset = getCurrentDataset(), state = getCurrentUiState()) {
  if (!dataset || !state) return;
  maybeScheduleVisibleDeferredSupplementLoad(dataset, state);
  const filtered = getFilteredRows(dataset, state);
  const visibleColumns = getVisibleColumns(dataset, state);
  const colorScale = getColorScale(dataset, state, visibleColumns);
  renderTable(dataset, state, filtered, { visibleColumns, colorScale });
  renderFinderBar(dataset, state);
  updateSummary(dataset, state, filtered);
  renderTableLegend(dataset, state);
}

function renderFilters(dataset, state) {
  renderYearPills(dataset, state);
  renderTeamSelect(dataset, state);
  renderExtraFilters(dataset, state);
  renderDemoRangeFilters(dataset, state);
  renderStatGroups(dataset, state);
  elements.searchInput.value = state.search;
  const searchLabel = document.querySelector('label[for="searchInput"]');
  if (searchLabel) searchLabel.textContent = dataset.id === "d1" ? "Player / Team / Coach" : "Player / Team";
  elements.searchInput.placeholder = dataset.id === "d1" ? "Use && for multiple terms" : "Start typing";
}

function renderYearPills(dataset, state) {
  elements.yearPills.innerHTML = dataset.meta.years
    .map((year) => {
      const active = state.years.has(year) ? "is-active" : "";
      return `<button class="pill-toggle ${active}" data-year="${escapeHtml(year)}" type="button">${escapeHtml(year)}</button>`;
    })
    .join("");

  elements.yearPills.querySelectorAll("[data-year]").forEach((button) => {
    button.addEventListener("click", () => {
      const year = button.dataset.year;
      if (!year) return;
      if (state.years.has(year)) {
        state.years.delete(year);
      } else {
        state.years.add(year);
      }
      renderCurrentDataset();
    });
  });
}

function renderTeamSelect(dataset, state) {
  const current = state.team;
  const teamRows = getFilterContextRows(dataset, state, { ignoreTeam: true, ignoreSearch: true, ignoreNumericFilters: true, ignoreDemoFilters: true, skipSort: true });
  const teams = Array.from(new Set(teamRows.map((row) => getStringValue(row[dataset.teamColumn])).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  const options = ['<option value="all">All teams</option>'].concat(
    teams.map((team) => {
      const selected = current === team ? " selected" : "";
      return `<option value="${escapeAttribute(team)}"${selected}>${escapeHtml(team)}</option>`;
    })
  );
  elements.teamSelect.innerHTML = options.join("");
}

function renderExtraFilters(dataset, state) {
  const viewModeFilter = (dataset.singleFilters || []).find((filter) => filter.id === "view_mode");
  if (elements.viewModeFilters) {
    if (viewModeFilter) {
      const selected = state.extraSelects[viewModeFilter.id];
      const pills = (viewModeFilter.options || []).map((option) => `<button class="pill-toggle ${selected === option.value ? "is-active" : ""}" type="button" data-view-mode="${escapeAttribute(option.value)}">${escapeHtml(option.label)}</button>`).join("");
      elements.viewModeFilters.innerHTML = `<div class="multi-pill-group"><div class="pill-grid">${pills}</div></div>`;
    } else {
      elements.viewModeFilters.innerHTML = "";
    }
  }

  elements.singleSelectFilters.innerHTML = (dataset.singleFilters || [])
    .map((filter) => {
      if (filter.id === "view_mode") return "";
      const options = getSingleFilterOptions(dataset, filter, state)
        .map((option) => `<option value="${escapeAttribute(option.value)}"${state.extraSelects[filter.id] === option.value ? " selected" : ""}>${escapeHtml(option.label)}</option>`)
        .join("");
      return `<div class="field-stack field-stack--compact field-stack--inline"><label class="field-label field-label--inline" for="single-${escapeAttribute(filter.id)}">${escapeHtml(filter.label)}</label><select class="form-control" id="single-${escapeAttribute(filter.id)}" data-single-filter="${escapeAttribute(filter.id)}">${options}</select></div>`;
    })
    .join("");

  elements.multiSelectFilters.innerHTML = (dataset.multiFilters || [])
    .map((filter) => {
      const selected = state.multiSelects[filter.id] || new Set();
      const pills = getMultiFilterOptions(dataset, filter, state)
        .map((option) => `<button class="pill-toggle ${selected.has(option) ? "is-active" : ""}" type="button" data-multi-filter="${escapeAttribute(filter.id)}" data-multi-value="${escapeAttribute(option)}">${escapeHtml(option)}</button>`)
        .join("");
      return `<div class="multi-pill-group"><div class="multi-pill-group__label">${escapeHtml(filter.label)}</div><div class="pill-grid">${pills}</div></div>`;
    })
    .join("");

  const viewModeRoot = elements.viewModeFilters || elements.singleSelectFilters;
  viewModeRoot.querySelectorAll("[data-view-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      state.extraSelects.view_mode = button.dataset.viewMode;
      state.team = "all";
      if (button.dataset.viewMode === "career") state.years = new Set(dataset.meta.years);
      renderCurrentDataset();
    });
  });

  elements.singleSelectFilters.querySelectorAll("[data-single-filter]").forEach((select) => {
    select.addEventListener("change", async () => {
      const filterId = select.dataset.singleFilter;
      state.extraSelects[filterId] = select.value;
      if (filterId === "view_mode") {
        state.team = "all";
        if (select.value === "career") {
          state.years = new Set(dataset.meta.years);
        }
      }
      if (filterId === "status_path" && select.value !== "all") {
        if (!dataset._statusAnnotated) {
          elements.statusPill.textContent = `Loading ${dataset.navLabel} status`;
          await ensureStatusAnnotations(dataset.id);
          if (appState.currentId !== dataset.id) return;
          resetUiCaches(state);
        }
      }
      renderCurrentDataset();
    });
  });

  elements.multiSelectFilters.querySelectorAll("[data-multi-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      const filterId = button.dataset.multiFilter;
      const value = button.dataset.multiValue;
      if (!filterId || !value) return;
      const selected = state.multiSelects[filterId] || new Set();
      if (selected.has(value)) selected.delete(value);
      else selected.add(value);
      state.multiSelects[filterId] = selected;
      renderCurrentDataset();
    });
  });
}

function getSingleFilterOptions(dataset, filter, state) {
  if (filter.options) return filter.options;
  if (filter.id === "conference_bucket" && dataset.id === "d1") {
    const conferences = Array.from(new Set(dataset.rows.map((row) => getStringValue(row.conference)).filter(Boolean))).sort(compareFilterValues);
    return [
      { value: "all", label: "All" },
      { value: "hm", label: "HM" },
      { value: "hmm", label: "HMM" },
      { value: "hm_hmm", label: "HM+HMM" },
      { value: "low_major", label: "Low Major" },
      ...conferences.map((conference) => ({ value: `conf:${conference}`, label: conference })),
    ];
  }
  const values = Array.from(new Set(dataset.rows.map((row) => getStringValue(row[filter.column])).filter((value) => value && value !== "Unknown"))).sort(compareFilterValues);
  return [{ value: "all", label: "All" }, ...values.map((value) => ({ value, label: value }))];
}

function getMultiFilterOptions(dataset, filter, state) {
  const values = Array.from(new Set(dataset.rows.map((row) => getStringValue(row[filter.column])).filter(Boolean)));
  if (Array.isArray(filter.sort)) {
    return filter.sort.slice();
  }
  return values.sort(compareFilterValues);
}

function compareFilterValues(left, right) {
  return getStringValue(left).localeCompare(getStringValue(right), undefined, { numeric: true, sensitivity: "base" });
}

function renderDemoToggles(dataset, state) {
  elements.demoToggles.innerHTML = "";
  elements.demoToggles.hidden = true;
}

function renderDemoRangeFilters(dataset, state) {
  const filterMeta = new Map(dataset.meta.demoFilterMeta.map((item) => [item.column, item]));
  const columns = getDemoControlColumns(dataset);
  if (elements.demoControls) elements.demoControls.hidden = !columns.length;
  if (!columns.length) {
    elements.demoRangeFilters.innerHTML = "";
    return;
  }
  elements.demoRangeFilters.innerHTML = columns
    .map((column) => {
      const type = filterMeta.get(column)?.type || "";
      const filter = state.demoFilters[column] || { min: "", max: "" };
      const inputMode = type === "date" ? "numeric" : "decimal";
      const active = state.visibleColumns[column] ? "is-active" : "";
      const disabled = filterMeta.has(column) ? "" : " disabled";
      return `
        <div class="demo-range-item" data-demo-range="${escapeAttribute(column)}">
          <button class="demo-toggle-button demo-range-label ${active}" type="button" data-demo-column="${escapeAttribute(column)}">${escapeHtml(displayLabel(dataset, column))}</button>
          <input class="demo-range-input" type="text" inputmode="${inputMode}" autocomplete="off" placeholder="Min" value="${escapeAttribute(filter.min)}" data-demo-min="${escapeAttribute(column)}"${disabled}>
          <input class="demo-range-input" type="text" inputmode="${inputMode}" autocomplete="off" placeholder="Max" value="${escapeAttribute(filter.max)}" data-demo-max="${escapeAttribute(column)}"${disabled}>
        </div>
      `;
    })
    .join("");

  elements.demoRangeFilters.querySelectorAll("[data-demo-column]").forEach((button) => {
    button.addEventListener("click", () => {
      const column = button.dataset.demoColumn;
      if (!column) return;
      if (getLockedColumns(dataset).includes(column)) return;
      state.visibleColumns[column] = !state.visibleColumns[column];
      renderCurrentDataset();
    });
  });

  elements.demoRangeFilters.querySelectorAll("[data-demo-min]").forEach((input) => {
    input.addEventListener("input", () => {
      const column = input.dataset.demoMin;
      if (!column || !state.demoFilters[column]) return;
      state.demoFilters[column].min = input.value;
      renderResultsOnly(dataset, state);
    });
  });

  elements.demoRangeFilters.querySelectorAll("[data-demo-max]").forEach((input) => {
    input.addEventListener("input", () => {
      const column = input.dataset.demoMax;
      if (!column || !state.demoFilters[column]) return;
      state.demoFilters[column].max = input.value;
      renderResultsOnly(dataset, state);
    });
  });
}

function getLockedColumns(dataset) {
  return dataset?.lockedColumns || [];
}

function getDemoControlColumns(dataset) {
  return (dataset?.meta?.demoFilterColumns || []).filter((column) => !getLockedColumns(dataset).includes(column));
}

function renderStatGroups(dataset, state) {
  elements.statGroups.innerHTML = "";

  dataset.meta.groups.forEach((group) => {
    if (dataset.id === "d1" && group.id === "playtype_analysis") return;
    const template = elements.statGroupTemplate.content.cloneNode(true);
    const section = template.querySelector(".stat-group");
    const header = template.querySelector(".stat-group__header");
    const body = template.querySelector(".stat-group__body");
    const groupState = getGroupSelectionState(group, state);
    header.innerHTML = `<button class="group-cycle-button ${group.columns.some((column) => state.visibleColumns[column]) ? "is-active" : ""}" type="button" data-group-cycle="${escapeAttribute(group.id)}">${escapeHtml(group.label)} <span class="cycle-note">(${escapeHtml(groupState)})</span></button>`;

    group.columns.forEach((column) => {
      const row = document.createElement("div");
      row.className = "filter-row";

      const labelButton = document.createElement("button");
      labelButton.type = "button";
      labelButton.className = `pill-toggle filter-row__label ${state.visibleColumns[column] ? "is-active" : ""}`;
      labelButton.dataset.statColumn = column;
      labelButton.textContent = displayLabel(dataset, column);

      const minInput = document.createElement("input");
      minInput.type = "text";
      minInput.inputMode = "decimal";
      minInput.autocomplete = "off";
      minInput.className = "filter-input";
      minInput.placeholder = "Min";
      minInput.value = state.numericFilters[column]?.min ?? "";

      const maxInput = document.createElement("input");
      maxInput.type = "text";
      maxInput.inputMode = "decimal";
      maxInput.autocomplete = "off";
      maxInput.className = "filter-input";
      maxInput.placeholder = "Max";
      maxInput.value = state.numericFilters[column]?.max ?? "";

      if (!dataset.meta.numericColumns.includes(column)) {
        minInput.disabled = true;
        maxInput.disabled = true;
      }

      labelButton.addEventListener("click", async () => {
        if (!state.visibleColumns[column]) {
          await ensureDeferredColumnsReady(dataset, state, [column], { scope: "visible" });
          if (appState.currentId !== dataset.id) return;
        }
        state.visibleColumns[column] = !state.visibleColumns[column];
        renderCurrentDataset();
      });

      minInput.addEventListener("input", async () => {
        if (!state.numericFilters[column]) return;
        if (inputHasValue(minInput.value)) {
          await ensureDeferredColumnsReady(dataset, state, [column]);
          if (appState.currentId !== dataset.id) return;
        }
        state.numericFilters[column].min = minInput.value;
        renderResultsOnly(dataset, state);
      });

      maxInput.addEventListener("input", async () => {
        if (!state.numericFilters[column]) return;
        if (inputHasValue(maxInput.value)) {
          await ensureDeferredColumnsReady(dataset, state, [column]);
          if (appState.currentId !== dataset.id) return;
        }
        state.numericFilters[column].max = maxInput.value;
        renderResultsOnly(dataset, state);
      });

      row.append(labelButton, minInput, maxInput);
      body.appendChild(row);
    });

    elements.statGroups.appendChild(section);
  });

  elements.statGroups.querySelectorAll("[data-group-cycle]").forEach((button) => {
    button.addEventListener("click", async () => {
      const groupId = button.dataset.groupCycle;
      const group = dataset.meta.groups.find((item) => item.id === groupId);
      if (!group) return;
      await cycleGroupVisibility(dataset, state, group);
    });
  });
}

function getGroupSelectionState(group, state) {
  const visible = group.columns.filter((column) => state.visibleColumns[column]);
  if (!visible.length) return "none";
  if (visible.length === group.columns.length) return "all";
  if (group.columns.every((column) => Boolean(state.visibleColumns[column]) === group.defaultColumns.includes(column))) return "default";
  return "custom";
}

async function cycleGroupVisibility(dataset, state, group) {
  const cycleActions = ["none", "all", "default"];
  const actionIndex = state.groupCycles[group.id] ?? 0;
  const action = cycleActions[actionIndex];

  if (action !== "none") {
    const targetColumns = group.columns.filter((column) => {
      if (action === "all") return !state.visibleColumns[column];
      return group.defaultColumns.includes(column) && !state.visibleColumns[column];
    });
    if (targetColumns.length) {
      await ensureDeferredColumnsReady(dataset, state, targetColumns, { scope: "visible" });
      if (appState.currentId !== dataset.id) return;
    }
  }

  group.columns.forEach((column) => {
    if (!(column in state.visibleColumns)) return;
    if (action === "all") {
      state.visibleColumns[column] = true;
    } else if (action === "default") {
      state.visibleColumns[column] = group.defaultColumns.includes(column);
    } else {
      state.visibleColumns[column] = false;
      if (state.numericFilters[column]) state.numericFilters[column] = { min: "", max: "" };
    }
  });

  state.groupCycles[group.id] = (actionIndex + 1) % cycleActions.length;
  renderCurrentDataset();
}

function renderTableLegend(dataset, state) {
  if (!elements.tableLegend) return;
  const footerGroups = dataset.id === "d1"
    ? ["summary", "playtype_analysis", "advanced", "shot_profile", "drive", "runner"]
      .map((groupId) => (dataset.meta.groups || []).find((group) => group.id === groupId))
      .filter(Boolean)
    : (dataset.meta.groups || []);
  const sep = '<span class="table-legend__sep">&bull;</span>';
  const links = [
    `<button class="link-button" type="button" data-stat-mode="default">Default</button>`,
    sep,
    `<button class="link-button" type="button" data-stat-mode="all">All</button>`,
    sep,
    `<button class="link-button" type="button" data-stat-mode="none">None</button>`,
  ];
  footerGroups.forEach((group) => {
    links.push(sep);
    links.push(`<button class="link-button" type="button" data-stat-group="${escapeAttribute(group.id)}">${escapeHtml(group.label)}</button>`);
  });
  links.push(sep);
  links.push(`<button class="link-button" type="button" data-reset-filters="true">Reset</button>`);
  elements.tableLegend.innerHTML = `<span class="table-legend__title">Show:</span> <span class="table-legend__links">${links.join("")}</span>`;

  elements.tableLegend.querySelectorAll("[data-stat-mode]").forEach((button) => {
    button.addEventListener("click", async () => {
      const mode = button.dataset.statMode;
      if (!mode) return;
      await applyVisibilityMode("stats", mode);
    });
  });

  elements.tableLegend.querySelectorAll("[data-stat-group]").forEach((button) => {
    button.addEventListener("click", async () => {
      const groupId = button.dataset.statGroup;
      const group = dataset.meta.groups.find((item) => item.id === groupId);
      if (!group) return;
      const groupState = getGroupSelectionState(group, state);
      const nextMode = groupState === "none" ? "default" : (groupState === "default" ? "all" : "none");
      if (nextMode !== "none") {
        const targetColumns = group.columns.filter((column) => {
          if (nextMode === "all") return !state.visibleColumns[column];
          return group.defaultColumns.includes(column) && !state.visibleColumns[column];
        });
        if (targetColumns.length) {
          await ensureDeferredColumnsReady(dataset, state, targetColumns, { scope: "visible" });
          if (appState.currentId !== dataset.id) return;
        }
      }
      group.columns.forEach((column) => {
        if (!(column in state.visibleColumns)) return;
        if (nextMode === "all") state.visibleColumns[column] = true;
        else if (nextMode === "default") state.visibleColumns[column] = group.defaultColumns.includes(column);
        else state.visibleColumns[column] = false;
        if (nextMode === "none" && state.numericFilters[column]) state.numericFilters[column] = { min: "", max: "" };
      });
      renderCurrentDataset();
    });
  });

  elements.tableLegend.querySelectorAll("[data-reset-filters]").forEach((button) => {
    button.addEventListener("click", () => {
      appState.uiState[dataset.id] = createInitialUiState(dataset);
      renderCurrentDataset();
    });
  });
}

function getFilteredRows(dataset, state) {
  const cache = getRenderCache(state);
  const key = [
    getDisplayRowsCacheKey(state),
    getStringValue(state.team),
    state.search.trim().toLowerCase(),
    getStringValue(state.sortBy),
    getStringValue(state.sortDir),
    getStringValue(state.sortBlankMode),
    serializeSingleFilterState(dataset, state),
    serializeMultiFilterState(dataset, state),
    serializeRangeFilters(dataset.meta.demoFilterMeta.map((item) => item.column), state.demoFilters),
    serializeRangeFilters(dataset.meta.numericColumns || [], state.numericFilters),
  ].join("||");
  if (cache.filteredRowsKey === key) return cache.filteredRows;
  const filtered = getFilterContextRows(dataset, state, { rows: getDisplayRows(dataset, state) });
  cache.filteredRowsKey = key;
  cache.filteredRows = filtered;
  return filtered;
}

function getFilterContextRows(dataset, state, options = {}) {
  const searchTerms = parseSearchTerms(state.search);
  const sourceRows = options.rows || getDisplayRows(dataset, state);
  const applyYearFilter = !options.ignoreYears && state.extraSelects.view_mode !== "career";

  const filtered = sourceRows.filter((row) => {
    if (applyYearFilter) {
      if (!state.years.size) return false;
      const yearValue = getStringValue(row[dataset.yearColumn]);
      if (state.years.size && !state.years.has(yearValue)) return false;
    }
    if (!options.ignoreTeam && state.team !== "all" && getStringValue(row[dataset.teamColumn]) !== state.team) return false;

    for (const filter of dataset.singleFilters || []) {
      if (options.ignoreSingleFilterId && filter.id === options.ignoreSingleFilterId) continue;
      const selected = state.extraSelects[filter.id];
      if (!selected || selected === "all") continue;
      if (filter.id === "conference_bucket" && dataset.id === "d1") {
        const conf = getStringValue(row.conference).toUpperCase();
        if (selected === "hm" && !D1_HM_CONFS.has(conf)) return false;
        if (selected === "hmm" && !D1_HMM_CONFS.has(conf)) return false;
        if (selected === "hm_hmm" && !D1_HM_CONFS.has(conf) && !D1_HMM_CONFS.has(conf)) return false;
        if (selected === "low_major" && (D1_HM_CONFS.has(conf) || D1_HMM_CONFS.has(conf))) return false;
        if (selected.startsWith("conf:") && conf !== selected.slice(5).toUpperCase()) return false;
        continue;
      }
      if (filter.id === "status_path") {
        if (!row._statusFlags?.[selected]) return false;
        continue;
      }
      if (!filter.column) continue;
      if (getStringValue(row[filter.column]) !== selected) return false;
    }

    for (const filter of dataset.multiFilters || []) {
      if (options.ignoreMultiFilterId && filter.id === options.ignoreMultiFilterId) continue;
      const selected = state.multiSelects[filter.id];
      if (!selected || !selected.size) continue;
      if (!selected.has(getStringValue(row[filter.column]))) return false;
    }

    if (!options.ignoreSearch && searchTerms.length) {
      const haystack = getRowSearchHaystack(dataset, row);
      const matches = searchTerms.every((term) => haystack.includes(term));
      if (!matches) return false;
    }

    if (!options.ignoreDemoFilters) {
      for (const item of dataset.meta.demoFilterMeta) {
        const filter = state.demoFilters[item.column];
        if (!filter) continue;
        if (!matchesDemoFilter(row[item.column], filter, item.type)) return false;
      }
    }

    if (!options.ignoreNumericFilters) {
      for (const [column, filter] of Object.entries(state.numericFilters)) {
        const hasMin = filter?.min !== "";
        const hasMax = filter?.max !== "";
        if (!hasMin && !hasMax) continue;
        const rawValue = getRowColumnValue(dataset, row, column);
        const numericValue = typeof rawValue === "number"
          ? rawValue
          : (getStringValue(rawValue).trim() === "" ? Number.NaN : Number(rawValue));
        if (!Number.isFinite(numericValue)) return false;
        if (hasMin && numericValue < Number(filter.min)) return false;
        if (hasMax && numericValue > Number(filter.max)) return false;
      }
    }

    return true;
  });

  return options.skipSort ? filtered : sortRows(filtered, state.sortBy, state.sortDir, dataset, state.sortBlankMode);
}

function parseSearchTerms(value) {
  return getStringValue(value)
    .split(/&&|,|;/)
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);
}

function getRowSearchHaystack(dataset, row) {
  if (!row) return "";
  if (!dataset?._searchCacheColumnsKey) dataset._searchCacheColumnsKey = (dataset?.searchColumns || []).join("|");
  const searchKey = dataset?._searchCacheColumnsKey || "";
  if (row._searchCacheKey !== searchKey) {
    row._searchCacheKey = searchKey;
    row._searchHaystack = (dataset?.searchColumns || []).map((column) => getStringValue(row[column]).toLowerCase()).join(" ");
  }
  return row._searchHaystack || "";
}

function getRenderCache(state) {
  if (!state._renderCache) state._renderCache = {};
  return state._renderCache;
}

function getDisplayRowsCacheKey(state) {
  const yearsKey = Array.from(state?.years || []).sort(compareYears).join("|");
  const viewMode = getStringValue(state?.extraSelects?.view_mode || "season");
  return `${viewMode}|${yearsKey}`;
}

function serializeSingleFilterState(dataset, state) {
  return (dataset.singleFilters || [])
    .map((filter) => `${filter.id}:${getStringValue(state.extraSelects?.[filter.id])}`)
    .join("|");
}

function serializeMultiFilterState(dataset, state) {
  return (dataset.multiFilters || [])
    .map((filter) => {
      const selected = Array.from(state.multiSelects?.[filter.id] || []).sort();
      return `${filter.id}:${selected.join(",")}`;
    })
    .join("|");
}

function serializeRangeFilters(columns, filters) {
  return columns
    .map((column) => {
      const filter = filters?.[column];
      return filter ? `${column}:${getStringValue(filter.min)}:${getStringValue(filter.max)}` : `${column}::`;
    })
    .join("|");
}

function getVisibleColumns(dataset, state) {
  const cache = getRenderCache(state);
  const key = dataset.meta.allColumns.map((column) => (state.visibleColumns[column] ? column : "")).join("|");
  if (cache.visibleColumnsKey === key) return cache.visibleColumns;
  const visibleColumns = dataset.meta.allColumns.filter((column) => state.visibleColumns[column]);
  cache.visibleColumnsKey = key;
  cache.visibleColumns = visibleColumns;
  return visibleColumns;
}

function getDisplayRows(dataset, state) {
  const cache = getRenderCache(state);
  const key = getDisplayRowsCacheKey(state);
  if (cache.displayRowsKey === key) return cache.displayRows;
  const rows = state.extraSelects.view_mode === "career" ? buildCareerRows(dataset, state) : dataset.rows;
  cache.displayRowsKey = key;
  cache.displayRows = rows;
  return rows;
}

function buildCareerRows(dataset, state) {
  const yearsKey = Array.from(state.years).sort(compareYears).join("|");
  const cacheKey = `${dataset.id}|${yearsKey}`;
  if (state._careerCache?.key === cacheKey) {
    return state._careerCache.rows;
  }

  const scopedRows = dataset.rows.filter((row) => !state.years.size || state.years.has(getStringValue(row[dataset.yearColumn])));
  const grouped = new Map();
  scopedRows.forEach((row) => {
    const key = getCareerGroupKey(dataset, row);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  const mergedGroups = mergeCareerRowGroups(dataset, Array.from(grouped.values()));
  const careerRows = mergedGroups.map((rows) => (rows.length <= 1 ? rows[0] : aggregateCareerRows(dataset, rows)));
  applyCalculatedRatings(careerRows, dataset.id);
  applyPerNormalization(careerRows, dataset.id);
  state._careerCache = { key: cacheKey, rows: careerRows };
  return careerRows;
}

function getCareerGroupKey(dataset, row) {
  const explicitId = getStringValue(row.player_id || row.pid || row.id).trim();
  if (explicitId) return `${dataset.id}|id|${explicitId}`;
  const player = normalizeKey(row.player_name || row.player);
  const dob = getStringValue(row.dob).trim();
  const height = firstFinite(row.height_in, row.inches, "");
  const rookie = Number.isFinite(row.rookie_year) ? row.rookie_year : "";
  const era = Math.floor(extractLeadingYear(row.season) / 8);
  if (dob) return `${dataset.id}|dob|${player}|${dob}|${height}`;
  if (rookie !== "") return `${dataset.id}|rookie|${player}|${rookie}|${height}`;
  return `${dataset.id}|era|${player}|${height}|${era}`;
}

function aggregateCareerRows(dataset, rows) {
  if (rows.length <= 1) {
    const latest = rows[0] || {};
    const aggregate = latest ? Object.create(latest) : {};
    aggregate.rank = null;
    aggregate._careerAggregate = true;
    aggregate._searchCacheKey = "";
    aggregate._searchHaystack = "";
    aggregate._colorBucketCacheKey = "";
    aggregate._colorBucketValue = "";
    return aggregate;
  }
  let latest = rows[0] || {};
  const minuteWeights = new Array(rows.length);
  const defaultWeights = new Array(rows.length);
  const teamSearchValues = new Set();
  const coachSearchValues = new Set();
  const competitionLabels = new Set();
  rows.forEach((row, index) => {
    if (compareYears(getStringValue(row[dataset.yearColumn]), getStringValue(latest[dataset.yearColumn])) < 0) {
      latest = row;
    }
    const teamText = getStringValue(row.team_search_text || row[dataset.teamColumn]).trim();
    if (teamText) teamSearchValues.add(teamText);
    const coachText = getStringValue(row.coach_search_text || row.coach).trim();
    if (coachText) coachSearchValues.add(coachText);
    if (dataset.id === "fiba") {
      const competitionLabel = getStringValue(row.competition_label).trim();
      if (competitionLabel) competitionLabels.add(competitionLabel);
    }
    const minutesWeight = Math.max(getMinutesValue(row), 1);
    minuteWeights[index] = minutesWeight;
    defaultWeights[index] = Math.max(firstFinite(row.total_poss, estimatedPossessions(row), getMinutesValue(row), row.gp, 1), 1);
  });
  const plans = getCareerAggregationPlans(dataset);
  const aggregate = latest ? Object.create(latest) : {};

  plans.forEach((plan) => {
    const { column, mode, weightMode } = plan;
    let hasValue = false;
    let total = 0;
    let best = mode === "min" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
    let totalWeight = 0;
    let weightedSum = 0;
    for (let index = 0; index < rows.length; index += 1) {
      const value = getRowColumnValue(dataset, rows[index], column);
      if (typeof value !== "number" || !Number.isFinite(value)) continue;
      hasValue = true;
      if (mode === "sum") {
        total += value;
        continue;
      }
      if (mode === "min") {
        if (value < best) best = value;
        continue;
      }
      if (mode === "max") {
        if (value > best) best = value;
        continue;
      }
      const weight = weightMode === "minutes" ? minuteWeights[index] : defaultWeights[index];
      totalWeight += weight;
      weightedSum += value * weight;
    }
    if (!hasValue) return;
    if (mode === "sum") {
      aggregate[column] = roundNumber(total, 3);
      return;
    }
    if (mode === "min" || mode === "max") {
      aggregate[column] = best;
      return;
    }
    if (totalWeight > 0) {
      aggregate[column] = roundNumber(weightedSum / totalWeight, 3);
    }
  });

  [
    dataset.yearColumn,
    dataset.teamColumn,
    "team_full",
    "conference",
    "division",
    "region",
    "level",
    "coach",
    "player_name",
    "player",
    "pos",
    "pos_text",
    "class_year",
    "height_in",
    "inches",
    "weight_lb",
    "weight",
    "bmi",
    "age",
    "dob",
    "rookie_year",
    "draft_pick",
    "exp",
  ].forEach((column) => {
    if (Object.prototype.hasOwnProperty.call(latest, column)) aggregate[column] = latest[column];
  });

  aggregate[dataset.yearColumn] = latest[dataset.yearColumn];
  aggregate[dataset.teamColumn] = latest[dataset.teamColumn];
  aggregate.team_search_text = Array.from(teamSearchValues).join(" ");
  aggregate.coach_search_text = Array.from(coachSearchValues).join(" ");
  if (dataset.id === "fiba") {
    aggregate.competition_label = Array.from(competitionLabels).join(" / ");
  }
  aggregate.rank = null;
  aggregate._careerAggregate = true;
  aggregate._searchCacheKey = "";
  aggregate._searchHaystack = "";
  aggregate._colorBucketCacheKey = "";
  aggregate._colorBucketValue = "";
  return enhanceRowForDataset(aggregate, dataset.id);
}

function mergeCareerRowGroups(dataset, groups) {
  const buckets = new Map();
  groups.forEach((rows) => {
    const sample = rows[0] || {};
    const nameKey = normalizeNameKey(sample.player_name || sample.player);
    if (!nameKey) return;
    if (!buckets.has(nameKey)) buckets.set(nameKey, []);
    buckets.get(nameKey).push(buildCareerGroupMeta(dataset, rows));
  });

  const merged = [];
  buckets.forEach((bucket) => {
    bucket.sort((left, right) => left.minYear - right.minYear);
    const current = [];
    bucket.forEach((candidate) => {
      const target = current.find((group) => canMergeCareerGroups(group, candidate));
      if (target) {
        target.rows.push(...candidate.rows);
        target.years = Array.from(new Set(target.years.concat(candidate.years))).sort((a, b) => a - b);
        target.minYear = Math.min(target.minYear, candidate.minYear);
        target.maxYear = Math.max(target.maxYear, candidate.maxYear);
        target.dobs = Array.from(new Set(target.dobs.concat(candidate.dobs)));
        target.heights = Array.from(new Set(target.heights.concat(candidate.heights)));
      } else {
        current.push(candidate);
      }
    });
    current.forEach((group) => merged.push(group.rows));
  });
  return merged.length ? merged : groups;
}

function buildCareerGroupMeta(dataset, rows) {
  let latestRow = rows[0] || {};
  let earliestRow = rows[0] || {};
  const years = [];
  const dobs = new Set();
  const heights = new Set();
  const explicitIds = new Set();
  rows.forEach((row) => {
    if (compareYears(getStringValue(row[dataset.yearColumn]), getStringValue(latestRow[dataset.yearColumn])) < 0) {
      latestRow = row;
    }
    if (compareYears(getStringValue(row[dataset.yearColumn]), getStringValue(earliestRow[dataset.yearColumn])) > 0) {
      earliestRow = row;
    }
    const year = extractLeadingYear(row[dataset.yearColumn]);
    if (Number.isFinite(year)) years.push(year);
    const dob = getStringValue(row.dob).trim();
    if (dob) dobs.add(dob);
    const height = firstFinite(row.height_in, row.inches, Number.NaN);
    if (Number.isFinite(height)) heights.add(height);
    const explicitId = getStringValue(row.player_id || row.pid || row.id).trim();
    if (explicitId) explicitIds.add(explicitId);
  });
  years.sort((a, b) => a - b);
  return {
    rows: rows.slice(),
    years,
    minYear: years[0] || 0,
    maxYear: years[years.length - 1] || 0,
    dobs: Array.from(dobs),
    heights: Array.from(heights),
    explicitIds: Array.from(explicitIds),
    latestClass: normalizeClassValue(latestRow.class_year),
    earliestClass: normalizeClassValue(earliestRow.class_year),
    latestMinutes: getMinutesValue(latestRow),
    earliestMinutes: getMinutesValue(earliestRow),
  };
}

function canMergeCareerGroups(left, right) {
  if (left.explicitIds.length && right.explicitIds.length) {
    return left.explicitIds.some((id) => right.explicitIds.includes(id));
  }
  if (left.dobs.length && right.dobs.length && !left.dobs.some((dob) => right.dobs.includes(dob))) return false;
  if (left.heights.length && right.heights.length) {
    const compatibleHeight = left.heights.some((height) => right.heights.some((other) => Math.abs(height - other) <= 1));
    if (!compatibleHeight) return false;
  }
  if (left.maxYear >= right.minYear && right.maxYear >= left.minYear) return false;
  const gap = left.maxYear < right.minYear ? right.minYear - left.maxYear : left.minYear - right.maxYear;
  const hasExtraIdentity = (left.dobs.length && right.dobs.length) || (left.heights.length && right.heights.length);
  return gap <= (hasExtraIdentity ? 2 : 1);
}

function stripCompanionPrefix(column) {
  return getStringValue(column).replace(/^(?:ncaa|nba)_/i, "");
}

function shouldUseLatestCareerValue(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /^(season|age|height_in|inches|weight|weight_lb|bmi|dob|rookie_year|draft_pick|exp)$/i.test(baseColumn);
}

function shouldMinCareerColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /(^draft_pick$|^rookie_year$)/i.test(baseColumn);
}

function shouldMaxCareerColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /peak|_max$|_high$|percentile$/i.test(baseColumn);
}

function shouldSumCareerColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /(^gp$|^g$|^gs$|^min$|^mp$|^pts$|^orb$|^drb$|^trb$|^reb$|^ast$|^stl$|^blk$|^stocks$|^tov$|^pf$|^fga$|^fgm$|^fta$|^ftm$|^tpa$|^tpm$|^2pa$|^2pm$|^3pa$|^3pm$|^two_pa$|^two_pm$|^three_pa$|^three_pm$|^total_poss$|^transition_poss$|^team_poss$|^opp_poss$|^fga_rim_75$|^fga_mid_75$|^fg3a_75$|^fta_75$|^ewins$|^ws$)/i.test(baseColumn)
    || /(_poss$|_pts$|_points$|_att$|_made$|_miss$|_fga$|_fta$|_ftm$|_fgm$|_pa$|_pm$|_reb$|_ast$|_stl$|_blk$|_tov$|_stocks$|_wins$|_losses$|_games$|_minutes$|_possessions$)/i.test(baseColumn);
}

function getCareerAggregationPlans(dataset) {
  if (dataset?._careerAggregationPlans) return dataset._careerAggregationPlans;
  const plans = (dataset?.meta?.numericColumns || [])
    .filter((column) => !shouldUseLatestCareerValue(column))
    .map((column) => {
      if (shouldMinCareerColumn(column)) return { column, mode: "min", weightMode: "" };
      if (shouldMaxCareerColumn(column)) return { column, mode: "max", weightMode: "" };
      if (shouldSumCareerColumn(column)) return { column, mode: "sum", weightMode: "" };
      return { column, mode: "weighted", weightMode: getCareerWeightMode(column) };
    });
  dataset._careerAggregationPlans = plans;
  return plans;
}

function getCareerWeightMode(column) {
  const baseColumn = stripCompanionPrefix(column);
  if (/(_ppp$|pct|rate|freq|share|three_pr|ftr|ast_to|per$|ppr|fic|bpm|epm|porpag|adjoe|ortg|drtg|usg|off$|def$|tot$)/i.test(baseColumn)) {
    return "minutes";
  }
  if (/(_per40$|_pg$|_per100$|_75$)/i.test(baseColumn)) {
    return "minutes";
  }
  return "default";
}

function getCareerWeight(column, row) {
  if (getCareerWeightMode(column) === "minutes") {
    return Math.max(getMinutesValue(row), 1);
  }
  return Math.max(firstFinite(row.total_poss, estimatedPossessions(row), getMinutesValue(row), row.gp, 1), 1);
}

function sortRows(rows, sortBy, sortDir, dataset, blankMode = "last") {
  if (!sortBy) return rows.slice();
  const direction = sortDir === "asc" ? 1 : -1;
  const isNumeric = dataset.meta.numericColumns.includes(sortBy);

  return rows.slice().sort((left, right) => {
    const a = getRowColumnValue(dataset, left, sortBy);
    const b = getRowColumnValue(dataset, right, sortBy);
    const aBlank = isBlankSortValue(a, sortBy, left);
    const bBlank = isBlankSortValue(b, sortBy, right);
    if (aBlank && bBlank) return 0;
    if (aBlank) return blankMode === "first" ? -1 : 1;
    if (bBlank) return blankMode === "first" ? 1 : -1;
    if (isNumeric) return (a - b) * direction;
    return getStringValue(a).localeCompare(getStringValue(b), undefined, { numeric: true, sensitivity: "base" }) * direction;
  });
}

function isBlankSortValue(value, column, row) {
  if (column === "draft_pick" && row?._draftPickBlank) return true;
  return value == null || value === "" || (typeof value === "number" && Number.isNaN(value));
}

function renderTable(dataset, state, filtered, renderContext = {}) {
  const visibleColumns = renderContext.visibleColumns || getVisibleColumns(dataset, state);
  const rowsToRender = filtered.slice(0, state.visibleCount);
  const colorScale = renderContext.colorScale || getColorScale(dataset, state, visibleColumns);
  const columnWidths = visibleColumns.map((column) => getColumnWidth(column, dataset));
  const totalWidth = columnWidths.reduce((sum, width) => sum + width, 0);
  const frameWidth = Math.min(totalWidth || 320, TABLE_FRAME_LIMIT);
  elements.tableWrapper.style.setProperty("--table-frame-width", `${frameWidth}px`);
  elements.statsTable.style.setProperty("--table-total-width", `${Math.max(totalWidth, 320)}px`);
  const colgroup = `<colgroup>${visibleColumns.map((column, index) => `<col style="width:${columnWidths[index]}px">`).join("")}</colgroup>`;
  elements.statsTable.innerHTML = `${colgroup}<thead id="statsTableHead"><tr>${visibleColumns.map((column) => renderHeaderCell(dataset, state, column)).join("")}</tr></thead><tbody id="statsTableBody"></tbody>`;
  elements.statsTableHead = document.getElementById("statsTableHead");
  elements.statsTableBody = document.getElementById("statsTableBody");

  if (!rowsToRender.length) {
    elements.statsTableBody.innerHTML = `<tr class="empty-state"><td colspan="${Math.max(visibleColumns.length, 1)}">No rows matched the current filters.</td></tr>`;
  } else {
    elements.statsTableBody.innerHTML = rowsToRender
      .map((row, index) => `<tr>${visibleColumns.map((column) => renderBodyCell(dataset, state, column, row, index, colorScale)).join("")}</tr>`)
      .join("");
  }

  elements.statsTableHead.querySelectorAll("[data-sort]").forEach((header) => {
    header.addEventListener("click", async () => {
      const column = header.dataset.sort;
      if (!column) return;
      if (isDeferredSupplementColumn(dataset, column)) {
        await ensureDeferredColumnsReady(dataset, state, [column], { scope: "full" });
        if (appState.currentId !== dataset.id) return;
      }
      if (state.sortBy === column) {
        if (state.sortDir === "desc" && state.sortBlankMode !== "first") {
          state.sortDir = "asc";
          state.sortBlankMode = "last";
        } else if (state.sortDir === "asc") {
          state.sortDir = "desc";
          state.sortBlankMode = "first";
        } else {
          state.sortDir = "desc";
          state.sortBlankMode = "last";
        }
      } else {
        state.sortBy = column;
        state.sortDir = "desc";
        state.sortBlankMode = "last";
      }
      renderResultsOnly(dataset, state);
    });
  });

  updateLoadMoreButton(filtered.length, rowsToRender.length);
}

function getColumnWidth(column, dataset) {
  const baseColumn = stripCompanionPrefix(column);
  if (column === "rank") return 18;
  if (baseColumn === dataset.yearColumn || baseColumn === "season") return 48;
  if (/player/i.test(baseColumn)) return 148;
  if (baseColumn === dataset.teamColumn || /team/i.test(baseColumn)) return 88;
  if (baseColumn === "competition_label") return 94;
  if (/^nationality$|^team_code$/.test(baseColumn)) return 52;
  if (baseColumn === "coach") return 96;
  if (/^conference$|^region$/.test(baseColumn)) return 60;
  if (/^division$|^level$/.test(baseColumn)) return 44;
  if (/^pos$|^pos_text$|class/.test(baseColumn)) return 38;
  if (/height|inches|weight|bmi|rookie_year|draft_pick|dob/.test(baseColumn)) return 50;
  if (/^(gp|g|gs|age|min|mp|mpg|min_per|total_poss|fga_rim_75|fga_mid_75|fg3a_75|drive_poss)$/.test(baseColumn)) return 42;
  if (/^three_pa_per100$|^three_p_per100$|^three_pa_per40$|^two_pa_per40$|_per40$|_pg$/.test(baseColumn)) return 48;
  if (/ppp$|^per$|^ppr$|^fic$|bpm|epm|ewins|^off$|^def$|^tot$|usg|porpag|dporpag|ortg|drtg|ast_to|three_pr|ftr/.test(baseColumn)) return 46;
  if (looksPercentColumn(baseColumn) || isPercentRatioColumn(baseColumn)) return 46;
  return 44;
}

function getColorPopulation(dataset, state) {
  const cache = getRenderCache(state);
  const scoped = getDisplayRows(dataset, state);
  const key = `${cache.displayRowsKey || getDisplayRowsCacheKey(state)}|${getQualifiedMinuteThreshold(dataset, state)}`;
  if (cache.colorRowsKey === key) return cache.colorRows;
  const qualified = scoped.filter((row) => getMinutesValue(row) >= getQualifiedMinuteThreshold(dataset, state));
  const rows = qualified.length >= 10 ? qualified : scoped;
  cache.colorRowsKey = key;
  cache.colorRows = sampleColorRows(rows);
  return cache.colorRows;
}

function sampleColorRows(rows, limit = COLOR_SCALE_MAX_ROWS) {
  if (!Array.isArray(rows) || rows.length <= limit) return rows;
  const targetSize = Math.max(1, Math.min(limit, rows.length));
  const step = rows.length / targetSize;
  const sampled = [];
  for (let index = 0; index < targetSize; index += 1) {
    sampled.push(rows[Math.floor(index * step)]);
  }
  return sampled;
}

function getDatasetMinuteThreshold(dataset) {
  const value = Number(dataset?.minuteDefault);
  return Number.isFinite(value) && value > 0 ? value : MINUTES_DEFAULT;
}

function getQualifiedMinuteThreshold(dataset, state) {
  const fallback = getDatasetMinuteThreshold(dataset);
  const minuteColumn = dataset?.meta?.minuteFilterColumn;
  const rawValue = minuteColumn ? state?.demoFilters?.[minuteColumn]?.min : "";
  const parsed = Number(rawValue);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function getMinutesValue(row) {
  if (typeof row.mp === "number" && Number.isFinite(row.mp)) return row.mp;
  if (typeof row.min === "number" && Number.isFinite(row.min)) return row.min;
  if (typeof row.mpg === "number" && typeof row.gp === "number" && Number.isFinite(row.mpg) && Number.isFinite(row.gp)) {
    return row.mpg * row.gp;
  }
  return 0;
}

function renderHeaderCell(dataset, state, column) {
  const classes = ["is-sortable"];
  const label = displayLabel(dataset, column);
  if (isWrapColumn(dataset, column)) classes.push("cell-wrap");
  if (isD1PlaytypeColumn(column) || /^ncaa_|^nba_/i.test(column) || label.length >= 11) classes.push("cell-header-small");
  return `<th class="${classes.join(" ")}" data-sort="${escapeAttribute(column)}">${escapeHtml(label)}</th>`;
}

function updateLoadMoreButton(totalRows, renderedRows) {
  elements.loadMoreBtn.hidden = renderedRows >= totalRows;
}

function renderBodyCell(dataset, state, column, row, index, colorScale) {
  const rawValue = column === "rank" && (row.rank == null || state?.extraSelects?.view_mode === "career")
    ? index + 1
    : getRowColumnValue(dataset, row, column);
  const display = formatValue(dataset, column, rawValue, row);
  const classes = [];
  if (isLeftAligned(dataset, column)) classes.push("cell-left");
  if (isWrapColumn(dataset, column)) classes.push("cell-wrap");
  if (typeof rawValue === "number" && rawValue < 0) classes.push("negative");
  const style = getCellStyle(dataset, state, column, rawValue, colorScale, row);
  const styleAttribute = style ? ` style="${escapeAttribute(style)}"` : "";
  const titleAttribute = rawValue != null && rawValue !== "" ? ` title="${escapeAttribute(display)}"` : "";
  return `<td class="${classes.join(" ")}"${styleAttribute}${titleAttribute}>${escapeHtml(display)}</td>`;
}

function buildColumnScales(dataset, state, visibleColumns, rows) {
  const scales = {};
  visibleColumns.forEach((column) => {
    if (!shouldColorColumn(dataset, column)) return;
    const buckets = new Map();
    rows.forEach((row) => {
      const value = getRowColumnValue(dataset, row, column);
      if (typeof value !== "number" || !Number.isFinite(value)) return;
      if (shouldSkipPercentileColor(row, column)) return;
      const bucketKey = getCachedColorBucketKey(state, row);
      if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
      buckets.get(bucketKey).push(value);
    });
    const sortedBuckets = {};
    buckets.forEach((values, key) => {
      if (values.length > 1) sortedBuckets[key] = values.sort((left, right) => left - right);
    });
    if (Object.keys(sortedBuckets).length) scales[column] = sortedBuckets;
  });
  return scales;
}

function getColorScale(dataset, state, visibleColumns) {
  const colorColumns = visibleColumns.filter((column) => shouldColorColumn(dataset, column));
  if (!colorColumns.length) return {};
  const cache = getRenderCache(state);
  const colorRows = getColorPopulation(dataset, state);
  const key = [
    cache.colorRowsKey || `${getDisplayRowsCacheKey(state)}|${getQualifiedMinuteThreshold(dataset, state)}`,
    getStringValue(state.extraSelects?.color_mode || "year"),
    colorColumns.join("|"),
  ].join("||");
  if (cache.colorScaleKey === key) return cache.colorScale;
  const colorScale = buildColumnScales(dataset, state, colorColumns, colorRows);
  cache.colorScaleKey = key;
  cache.colorScale = colorScale;
  return colorScale;
}

function shouldColorColumn(dataset, column) {
  const baseColumn = stripCompanionPrefix(column);
  if (/^(gp|g|gs)$/i.test(baseColumn)) return false;
  return dataset.meta.statColumnSet.has(column) && dataset.meta.numericColumnSet.has(column);
}

function isInverseColorColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /(^tov$|_tov$|tov_|topct$|tov_pct$|fg_miss$|two_fg_miss$|three_fg_miss$|score_pct$)/i.test(baseColumn);
}

function isInverseShotAssistColorColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /(^dunk_ast_pct$|^rim_ast_pct$|^mid_ast_pct$|^two_p_ast_pct$|^three_p_ast_pct$|^two_ast_pct$|^three_ast_pct$|^smr_ast_pct$|^lmr_ast_pct$|^c3_ast_pct$|^ab3_ast_pct$)/i.test(baseColumn);
}

function getColorBucketKey(state, row) {
  const mode = state?.extraSelects?.color_mode || "year";
  if (mode === "overall" || state?.extraSelects?.view_mode === "career") return "all";
  if (mode === "position") {
    const pos = getStringValue(row?.pos || row?.pos_text).trim();
    return pos || "all";
  }
  const competition = getStringValue(row?.competition_label || row?.competition_key).trim();
  if (mode === "competition") {
    return competition || "all";
  }
  const season = getStringValue(row?.season) || "all";
  if (mode === "competition_position") {
    const pos = getStringValue(row?.pos || row?.pos_text).trim();
    if (competition && pos) return `${competition}|${pos}`;
    return competition || pos || "all";
  }
  if (mode === "year_position") {
    const pos = getStringValue(row?.pos || row?.pos_text).trim();
    return pos ? `${season}|${pos}` : season;
  }
  return season;
}

function getCachedColorBucketKey(state, row) {
  if (!row) return "all";
  const mode = state?.extraSelects?.color_mode || "year";
  const viewMode = state?.extraSelects?.view_mode || "player";
  const cacheKey = `${viewMode}|${mode}`;
  if (row._colorBucketCacheKey !== cacheKey) {
    row._colorBucketCacheKey = cacheKey;
    row._colorBucketValue = getColorBucketKey(state, row);
  }
  return row._colorBucketValue || "all";
}

function getCellStyle(dataset, state, column, value, colorScale, row) {
  if (!shouldColorColumn(dataset, column)) return "";
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  if (isZeroAttemptThreePointRow(row, column)) {
    return "background-color: rgb(226, 196, 184); color: #111111;";
  }
  const bucketKey = getCachedColorBucketKey(state, row);
  const sorted = colorScale[column]?.[bucketKey] || colorScale[column]?.all;
  if (!sorted || sorted.length < 2) return "";
  let pct = percentileFromSorted(sorted, value);
  if (pct == null) return "";
  if (isInverseColorColumn(column)) pct = 1 - pct;
  if (isInverseShotAssistColorColumn(column)) pct = 1 - pct;
  const style = colorFromPercentile(pct);
  return `background-color: ${style.bg}; color: ${style.color};`;
}

function shouldSkipPercentileColor(row, column) {
  if (isZeroAttemptThreePointRow(row, column)) return true;
  if (isZeroAttemptTwoPointRow(row, column)) return true;
  if (isZeroAttemptFreeThrowRow(row, column)) return true;
  if (isZeroAttemptEfgRow(row, column)) return true;
  return false;
}

function isZeroAttemptThreePointRow(row, column) {
  if (!/(^3p_pct$|^tp_pct$|^three_p_pct$|^fg3pct$|^three_fg_pct$)/i.test(column)) return false;
  const attempts = firstFinite(row["3pa"], row.tpa, row.three_pa, Number.NaN);
  return Number.isFinite(attempts) && attempts <= 0;
}

function isZeroAttemptTwoPointRow(row, column) {
  if (!/(^2p_pct$|^two_p_pct$|^fg2pct$|^two_fg_pct$)/i.test(column)) return false;
  const attempts = firstFinite(row["2pa"], row.two_pa, row["2pa_total"], Number.NaN);
  return Number.isFinite(attempts) && attempts <= 0;
}

function isZeroAttemptFreeThrowRow(row, column) {
  if (!/(^ft_pct$|^ftpct$)/i.test(column)) return false;
  const attempts = firstFinite(row.fta, Number.NaN);
  return Number.isFinite(attempts) && attempts <= 0;
}

function isZeroAttemptEfgRow(row, column) {
  if (!/(^efg$|^efg_pct$)/i.test(column)) return false;
  const attempts = firstFinite(row.fga, Number.NaN);
  return Number.isFinite(attempts) && attempts <= 0;
}

function percentileFromSorted(arr, value) {
  let lo = 0;
  let hi = arr.length - 1;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (arr[mid] >= value) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  let first = lo;
  lo = 0;
  hi = arr.length - 1;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (arr[mid] <= value) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }
  const last = lo;
  return ((first + last) / 2) / (arr.length - 1);
}

function colorFromPercentile(pct) {
  const white = "#ffffff";
  const lowColor = "#cd7c6d";
  const lowMid = "#ead2cb";
  const lightGreen = "#d9e8c8";
  const highColor = "#78a85b";
  if (pct >= 0.35 && pct <= 0.65) return { bg: white, color: "#111111" };
  if (pct < 0.35) {
    const lowPct = subtlePercentileGradient((0.35 - pct) / 0.35);
    if (lowPct <= 0.8) return mixColors(white, lowMid, lowPct / 0.8);
    return mixColors(lowMid, lowColor, (lowPct - 0.8) / 0.2);
  }
  const highPct = subtlePercentileGradient((pct - 0.65) / 0.35);
  if (highPct <= 0.8) return mixColors(white, lightGreen, highPct / 0.8);
  return mixColors(lightGreen, highColor, (highPct - 0.8) / 0.2);
}

function subtlePercentileGradient(value) {
  return Math.pow(Math.min(1, Math.max(0, value)), 1.55);
}

function smoothstep(value) {
  const t = Math.min(1, Math.max(0, value));
  return t * t * (3 - (2 * t));
}

function mixColors(startHex, endHex, t) {
  const start = hexToRgb(startHex);
  const end = hexToRgb(endHex);
  const clamp = Math.min(1, Math.max(0, t));
  const r = Math.round(start.r + (end.r - start.r) * clamp);
  const g = Math.round(start.g + (end.g - start.g) * clamp);
  const b = Math.round(start.b + (end.b - start.b) * clamp);
  return { bg: `rgb(${r}, ${g}, ${b})`, color: "#111111" };
}

function hexToRgb(hex) {
  const value = hex.replace("#", "");
  return {
    r: parseInt(value.slice(0, 2), 16),
    g: parseInt(value.slice(2, 4), 16),
    b: parseInt(value.slice(4, 6), 16),
  };
}

function updateSummary(dataset, state, filtered) {
  const selectedYears = state.years.size ? Array.from(state.years).sort((a, b) => compareYears(b, a)).join(" / ") : "none";
  const search = state.search.trim();
  elements.filtersSummary.textContent = `Years: ${selectedYears} | Team: ${state.team === "all" ? "all" : state.team} | Search: ${search || "none"}`;
  elements.resultsCount.textContent = "";
  elements.resultsSubtitle.textContent = filtered.length ? `Showing ${Math.min(filtered.length, state.visibleCount).toLocaleString()} of ${filtered.length.toLocaleString()}` : "";
}

function renderFinderBar(dataset, state) {
  if (!elements.finderTitle || !elements.finderQuery || !elements.yearQuickSelect) return;
  const selectedYears = Array.from(state.years).sort(compareYears);
  const singleYear = selectedYears.length === 1 ? selectedYears[0] : "all";
  elements.yearQuickSelect.innerHTML = [
    '<option value="all">All years</option>',
    ...dataset.meta.years.map((year) => `<option value="${escapeAttribute(year)}"${singleYear === year ? " selected" : ""}>${escapeHtml(year)}</option>`),
  ].join("");
  const yearLabel = singleYear === "all" ? "All Years" : singleYear;
  elements.finderTitle.textContent = `${yearLabel} ${dataset.navLabel} Player Finder`;
  elements.finderQuery.textContent = buildFinderQueryText(dataset, state);

  elements.yearQuickSelect.onchange = () => {
    const value = elements.yearQuickSelect.value;
    state.years = value === "all" ? new Set(dataset.meta.years) : new Set([value]);
    renderCurrentDataset();
  };
}

function buildFinderQueryText(dataset, state) {
  const parts = [];
  if (state.search.trim()) parts.push(`Search: ${state.search.trim()}`);
  if (state.team !== "all") parts.push(`Team: ${state.team}`);

  (dataset.singleFilters || []).forEach((filter) => {
    if (["view_mode", "color_mode"].includes(filter.id)) return;
    const value = state.extraSelects[filter.id];
    if (!value || value === "all") return;
    const label = getSingleFilterOptions(dataset, filter, state).find((option) => option.value === value)?.label || value;
    parts.push(`${filter.label}: ${label}`);
  });

  (dataset.multiFilters || []).forEach((filter) => {
    const values = Array.from(state.multiSelects[filter.id] || []);
    if (!values.length) return;
    parts.push(`${filter.label}: ${values.join("/")}`);
  });

  Object.entries(state.demoFilters || {}).forEach(([column, filter]) => {
    if (!filter?.min && !filter?.max) return;
    parts.push(formatFilterRange(displayLabel(dataset, column), filter.min, filter.max));
  });

  Object.entries(state.numericFilters || {}).forEach(([column, filter]) => {
    if (!filter?.min && !filter?.max) return;
    parts.push(formatFilterRange(displayLabel(dataset, column), filter.min, filter.max));
  });

  if (!parts.length) return "No quality filter.";
  return parts.join("; ");
}

function formatFilterRange(label, minValue, maxValue) {
  if (minValue && maxValue) return `${label} ${minValue}-${maxValue}`;
  if (minValue) return `${label} >= ${minValue}`;
  return `${label} <= ${maxValue}`;
}

function normalizeRows(rows, datasetId) {
  if (!rows.length) return [];
  const columns = Object.keys(rows[0]).filter((column) => column && !column.startsWith("_"));
  const types = inferTypes(rows, columns);

  return rows.map((row) => {
    const out = {};
    columns.forEach((column) => {
      out[column] = coerceValue(row[column], types[column]);
    });
    return enhanceRowForDataset(out, datasetId);
  });
}

function dedupeDatasetRows(rows, datasetId) {
  const grouped = new Map();
  rows.forEach((row) => {
    const season = getStringValue(row.season);
    const playerKey = normalizeNameKey(row.player_name || row.player);
    const teamKey = normalizeKey(row.team_name || row.team_full || row.team_alias || row.team || "");
    const key = `${datasetId}|${season}|${teamKey}|${playerKey}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  });

  return Array.from(grouped.values()).map((group) => {
    if (group.length === 1) return group[0];
    return group.slice().sort((left, right) => duplicateRowScore(right) - duplicateRowScore(left))[0];
  });
}

function duplicateRowScore(row) {
  const minutes = Math.max(getMinutesValue(row), 0);
  const gp = Math.max(firstFinite(row.gp, row.g, 0), 0);
  const nonEmpty = Object.entries(row || {}).reduce((sum, [column, value]) => {
    if (column.startsWith("_")) return sum;
    if (value == null || value === "") return sum;
    if (typeof value === "number" && !Number.isFinite(value)) return sum;
    return sum + 1;
  }, 0);
  const playerIdBonus = getStringValue(row.player_id || row.pid || row.id).trim() ? 10 : 0;
  return (minutes * 100) + (gp * 10) + nonEmpty + playerIdBonus;
}

function annotateTrustedD1Rows(rows) {
  const teamSeasonMap = new Map();
  rows.forEach((row) => {
    const key = `${getStringValue(row.season)}|${normalizeKey(row.team_name || row.team_full)}`;
    if (!teamSeasonMap.has(key)) teamSeasonMap.set(key, []);
    teamSeasonMap.get(key).push(row);
  });

  teamSeasonMap.forEach((teamRows) => {
    const confCounts = new Map();
    teamRows.forEach((row) => {
      const conf = getStringValue(row.conference).trim();
      if (!conf) return;
      confCounts.set(conf, (confCounts.get(conf) || 0) + 1);
    });
    const total = teamRows.length || 1;
    const dominant = Math.max(0, ...confCounts.values());
    const trusted = confCounts.size > 0 && confCounts.size <= 2 && (dominant / total) >= 0.6;
    teamRows.forEach((row) => {
      row._trustedD1 = trusted;
    });
  });
}

function enhanceRowForDataset(row, datasetId) {
  const out = { ...row };
  enhanceCommonRow(out, datasetId);
  if (datasetId === "d1") enhanceD1Row(out);
  if (datasetId === "d2") enhanceD2Row(out);
  if (datasetId === "naia") enhanceCollegeRow(out, datasetId);
  if (datasetId === "juco") enhanceCollegeRow(out, datasetId);
  if (datasetId === "fiba") enhanceFibaRow(out);
  if (datasetId === "nba") enhanceNbaRow(out);
  normalizePercentLikeColumns(out, datasetId);
  return out;
}

function enhanceFibaRow(row) {
  if (typeof row.competition_label === "string") {
    row.competition_label = abbreviateFibaCompetition(row.competition_label);
  }
  enhanceCollegeRow(row, "fiba");
  const teamCode = getStringValue(row.team_code).trim().toUpperCase();
  if (teamCode) row.team_name = teamCode;
  row.nationality = normalizeFibaCountryLabel(row.nationality || teamCode);
}

function enhanceCommonRow(row, datasetId) {
  if (row.season != null) {
    const seasonValue = normalizeSeasonValue(row.season);
    if (seasonValue !== "") row.season = seasonValue;
  }

  if (typeof row.player === "string" && !row.player_name) row.player_name = row.player;
  if (typeof row.team_alias === "string" && !row.team_name) row.team_name = row.team_alias;
  if (typeof row.player_name === "string") row.player_name = normalizeDisplayName(row.player_name);
  if (typeof row.player === "string") row.player = normalizeDisplayName(row.player);
  if (typeof row.coach === "string") row.coach = normalizeDisplayName(row.coach);
  if (typeof row.pos === "string") row.pos = normalizePosLabel(row.pos);
  if (typeof row.pos_text === "string") row.pos_text = normalizePosLabel(row.pos_text);

  if (typeof row.team_name === "string") {
    row.team_full = row.team_full || row.team_name;
    row.team_name = simplifySchoolName(row.team_name, datasetId);
  }

  if (!Number.isFinite(row.height_in) && Number.isFinite(row.inches)) row.height_in = row.inches;
  if (!Number.isFinite(row.inches) && Number.isFinite(row.height_in)) row.inches = row.height_in;
  if (!Number.isFinite(row.height_in) && typeof row.height === "string") {
    const parsed = parseHeightToInches(row.height);
    if (Number.isFinite(parsed)) {
      row.height_in = parsed;
      row.inches = parsed;
    }
  }

  if (!Number.isFinite(row.weight_lb) && Number.isFinite(row.weight)) row.weight_lb = row.weight;
  if (!Number.isFinite(row.weight) && Number.isFinite(row.weight_lb)) row.weight = row.weight_lb;

  if (!Number.isFinite(row.mpg) && Number.isFinite(row.min_per_g)) row.mpg = row.min_per_g;
  if (!Number.isFinite(row.mpg) && Number.isFinite(row.min) && Number.isFinite(row.gp) && row.gp > 0) {
    row.mpg = roundNumber(row.min / row.gp, 2);
  }
  if (!Number.isFinite(row.min_per) && Number.isFinite(row.mpg) && row.mpg > 0) {
    row.min_per = roundNumber(Math.min(100, (row.mpg / 40) * 100), 1);
  }

  if (!Number.isFinite(row.exp) && Number.isFinite(row.season) && Number.isFinite(row.rookie_year)) {
    row.exp = Math.max(0, row.season - row.rookie_year);
  }
  normalizeDraftPickValue(row);

  if (!Number.isFinite(row.bmi) && Number.isFinite(row.height_in) && Number.isFinite(row.weight_lb) && row.height_in > 0) {
    row.bmi = roundNumber((row.weight_lb / (row.height_in * row.height_in)) * 703, 1);
  }

  if (!Number.isFinite(row.age) && typeof row.dob === "string" && Number.isFinite(row.season)) {
    const dob = new Date(row.dob);
    if (!Number.isNaN(dob.getTime())) {
      const ref = new Date(Number(row.season), 5, 23);
      row.age = roundNumber((ref - dob) / (1000 * 60 * 60 * 24 * 365.25), 1);
    }
  }

  if (!Number.isFinite(row.stocks) && Number.isFinite(row.stl) && Number.isFinite(row.blk)) {
    row.stocks = row.stl + row.blk;
  }

  row.team_search_text = Array.from(new Set([row.team_name, row.team_full, row.team_alias, row.team_alias_all].map((value) => getStringValue(value).trim()).filter(Boolean))).join(" ");
  row.coach_search_text = getStringValue(row.coach).trim();
}

function enhanceD1Row(row) {
  normalizeD1PlaytypeColumns(row);
  normalizeD1TruePlaytypeFrequencies(row);
  if (Number.isFinite(row.transition_freq)) {
    row.halfcourt_freq = roundNumber(100 - row.transition_freq, 1);
  } else if (Number.isFinite(row.total_poss) && Number.isFinite(row.transition_poss) && row.total_poss > 0) {
    row.transition_freq = roundNumber((row.transition_poss / row.total_poss) * 100, 1);
    row.halfcourt_freq = roundNumber(100 - row.transition_freq, 1);
  }
  if (row.mid_made == null && Number.isFinite(row.long2_made)) row.mid_made = row.long2_made;
  if (row.mid_att == null && Number.isFinite(row.long2_att)) row.mid_att = row.long2_att;
  if (row.mid_pct == null && Number.isFinite(row.long2_pct)) row.mid_pct = row.long2_pct;
  populateDerivedShooting(row, {
    threeMadeKeys: ["three_p_made", "three_pm"],
    threeAttKeys: ["three_p_att", "three_pa"],
    twoMadeKeys: ["two_p_made", "two_pm"],
    twoAttKeys: ["two_p_att", "two_pa"],
    twoPctKeys: ["two_p_pct"],
    threePctKeys: ["three_p_pct"],
    efgKeys: ["efg_pct"],
  });
  if (!Number.isFinite(row.dunk_made)) row.dunk_made = 0;
  if (!Number.isFinite(row.dunk_att)) row.dunk_att = 0;
  if (!Number.isFinite(row.dunk_pct)) row.dunk_pct = zeroSafePercent(row.dunk_made, row.dunk_att);
  if (!Number.isFinite(row.tov) && Number.isFinite(row.total_poss) && Number.isFinite(row.tov_pct_adv)) {
    row.tov = roundNumber((row.total_poss * row.tov_pct_adv) / 100, 1);
  }
  row.ftr = firstFinite(row.ftr, ratioIfPossible(row.fta, row.fga), Number.NaN);
  row.three_pr = firstFinite(row.three_pr, ratioIfPossible(firstFinite(row.three_p_att, row.three_pa, Number.NaN), row.fga), Number.NaN);
  populateAstTo(row);
  fillMissingRateStats(row, ["orb_pct", "drb_pct", "ast_pct", "stl_pct", "blk_pct", "usg_pct"]);
  scalePercentRatioColumns(row);
  populateImpactMetrics(row);
}

function enhanceD2Row(row) {
  applyD2BioLookup(row);
  scaleRateColumns(row, ["fg_pct", "3p_pct", "ft_pct", "efg_pct", "ts_pct", "tpa_rate", "fta_rate", "usg_pct", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "tov_pct", "stl_pct", "blk_pct"], "d2");
  populateDerivedShooting(row, {
    threeMadeKeys: ["3pm"],
    threeAttKeys: ["3pa"],
    twoMadeKeys: ["two_pm"],
    twoAttKeys: ["two_pa"],
    twoPctKeys: ["two_p_pct"],
    threePctKeys: ["3p_pct"],
    efgKeys: ["efg_pct"],
  });
  row.three_pa_per40 = per40Value(row["3pa"], row.min);
  row.two_pa_per40 = per40Value(row.two_pa, row.min);
  row.stocks_per40 = per40Value(row.stocks, row.min);
  row.pts_pg = perGameValue(row.pts, row.gp);
  row.trb_pg = perGameValue(row.trb, row.gp);
  row.ast_pg = perGameValue(row.ast, row.gp);
  row.tov_pg = perGameValue(row.tov, row.gp);
  row.stl_pg = perGameValue(row.stl, row.gp);
  row.blk_pg = perGameValue(row.blk, row.gp);
  row.stocks_pg = perGameValue(row.stocks, row.gp);
  row.two_pa_pg = perGameValue(row.two_pa, row.gp);
  row.three_pa_pg = perGameValue(row["3pa"], row.gp);
  row.ftr = ratioIfPossible(row.fta, row.fga);
  row.three_pr = ratioIfPossible(row["3pa"], row.fga);
  row.three_pa_per100 = d2Per100Value(row["3pa"], row);
  row.ortg = ortgEstimate(row);
  populateAstTo(row);
  fillMissingRateStats(row, ["orb_pct", "drb_pct", "trb_pct", "ast_pct", "tov_pct", "stl_pct", "blk_pct", "usg_pct"]);
  scalePercentRatioColumns(row);
  populateImpactMetrics(row);
}

function enhanceCollegeRow(row, datasetId) {
  if (datasetId === "juco") {
    row.region = getStringValue(row.region).replace(/\.0$/, "");
    row.level = normalizeJucoDivision(row.level);
  }
  scaleRateColumns(row, ["fg_pct", "2p_pct", "tp_pct", "ft_pct", "efg_pct", "ts_pct", "orb_pct", "drb_pct", "trb_pct", "ast_pct", "stl_pct", "blk_pct", "tov_pct", "usg_pct"], datasetId);
  populateDerivedShooting(row, {
    threeMadeKeys: ["tpm", "3pm", "three_pm"],
    threeAttKeys: ["tpa", "3pa", "three_pa"],
    twoMadeKeys: ["2pm", "two_pm"],
    twoAttKeys: ["2pa", "two_pa", "2pa_total"],
    twoPctKeys: ["2p_pct", "two_p_pct"],
    threePctKeys: ["tp_pct", "3p_pct", "three_p_pct"],
    efgKeys: ["efg_pct"],
  });
  ["pts", "trb", "ast", "tov", "stl", "blk", "stocks"].forEach((column) => {
    const perGame = perGameValue(row[column], row.gp);
    if (perGame !== "") row[`${column}_pg`] = perGame;
    const per40 = per40Value(row[column], row.min);
    if (per40 !== "") row[`${column}_per40`] = per40;
  });
  row.pts_per40 = row.pts_per40 ?? per40Value(row.pts, row.min);
  row.trb_per40 = row.trb_per40 ?? per40Value(row.trb, row.min);
  row.ast_per40 = row.ast_per40 ?? per40Value(row.ast, row.min);
  row.tov_per40 = row.tov_per40 ?? per40Value(row.tov, row.min);
  row.stl_per40 = row.stl_per40 ?? per40Value(row.stl, row.min);
  row.blk_per40 = row.blk_per40 ?? per40Value(row.blk, row.min);
  row.stocks_per40 = row.stocks_per40 ?? per40Value(row.stocks, row.min);
  row.two_pa_pg = perGameValue(row["2pa"] ?? row.two_pa ?? row["2pa_total"], row.gp);
  row.three_pa_pg = perGameValue(row.tpa ?? row["3pa"] ?? row.three_pa, row.gp);
  row.three_pa_per40 = per40Value(row.tpa ?? row["3pa"] ?? row.three_pa, row.min);
  row.two_pa_per40 = per40Value(row["2pa"] ?? row.two_pa ?? row["2pa_total"], row.min);
  row.ftr = ratioIfPossible(row.fta, row.fga);
  row.three_pr = ratioIfPossible(row.tpa ?? row["3pa"] ?? row.three_pa, row.fga);
  row.three_pa_per100 = possPer100Value(row.tpa ?? row["3pa"] ?? row.three_pa, row);
  row.ortg = row.ortg ?? ortgEstimate(row);
  populateAstTo(row);
  fillMissingRateStats(row, ["orb_pct", "drb_pct", "trb_pct", "ast_pct", "tov_pct", "stl_pct", "blk_pct", "usg_pct"]);
  scalePercentRatioColumns(row);
  populateImpactMetrics(row);
}

function enhanceNbaRow(row) {
  scaleRateColumns(row, ["usg", "tspct", "efg", "fgpct_rim", "fgpct_mid", "fg2pct", "fg3pct", "ftpct", "orbpct", "drbpct", "astpct", "topct", "stlpct", "blkpct", "rim_ast_pct", "mid_ast_pct", "two_ast_pct", "three_ast_pct"], "nba");
  if (!Number.isFinite(row.ftr)) row.ftr = ratioIfPossible(row.fta_75, row.fga_75);
  row.three_pr = ratioIfPossible(firstFinite(row.fg3a_75, row.three_pa, row["3pa"], Number.NaN), firstFinite(row.fga_75, row.fga, Number.NaN));
  populateAstTo(row);
  scalePercentRatioColumns(row);
  populateImpactMetrics(row);
}

function getD2BioLookup() {
  return typeof D2_BIO_LOOKUP !== "undefined" ? D2_BIO_LOOKUP : (window.D2_BIO_LOOKUP || {});
}

function applyD2BioLookup(row) {
  const lookup = getD2BioLookup();
  const seasonKey = getStringValue(normalizeLegacySeason(row.season));
  const nameKey = normalizeNameKey(row.player_name || row.player);
  const teamKeys = Array.from(new Set([normalizeKey(row.team_name), normalizeKey(row.team_full)].filter(Boolean)));
  const match = teamKeys.map((teamKey) => lookup[`${seasonKey}|${teamKey}|${nameKey}`]).find(Boolean);
  if (!match) return;
  if (!row.pos && match.pos) row.pos = normalizePosLabel(match.pos);
  if (!row.class_year && match.class_year) row.class_year = normalizeClassValue(match.class_year);
  if (!Number.isFinite(row.height_in) && match.height) {
    const parsed = parseHeightToInches(match.height);
    if (Number.isFinite(parsed)) {
      row.height_in = parsed;
      row.inches = parsed;
    }
  }
  refreshDerivedBiography(row);
}

function fillMissingRateStats(row, columns) {
  if (getMinutesValue(row) <= 0) return;
  columns.forEach((column) => {
    if (!Number.isFinite(row[column]) && canImputeZeroRate(row, column)) row[column] = 0;
  });
}

function canImputeZeroRate(row, column) {
  if (/^orb_pct$|^orbpct$/i.test(column)) return Number.isFinite(row.orb);
  if (/^drb_pct$|^drbpct$/i.test(column)) return Number.isFinite(row.drb);
  if (/^trb_pct$|^trbpct$/i.test(column)) return Number.isFinite(row.trb) || (Number.isFinite(row.orb) && Number.isFinite(row.drb));
  if (/^ast_pct$|^astpct$/i.test(column)) return Number.isFinite(row.ast);
  if (/^tov_pct$|^topct$/i.test(column)) return Number.isFinite(row.tov);
  if (/^stl_pct$|^stlpct$/i.test(column)) return Number.isFinite(row.stl);
  if (/^blk_pct$|^blkpct$/i.test(column)) return Number.isFinite(row.blk);
  if (/^usg_pct$|^usg$/i.test(column)) {
    return [row.pts, row.fga, row.fta, row.tov, row.total_poss].some((value) => Number.isFinite(value));
  }
  return true;
}

function populateAstTo(row) {
  if (Number.isFinite(row.ast_to)) return;
  if (!Number.isFinite(row.ast) || !Number.isFinite(row.tov) || row.tov <= 0) return;
  row.ast_to = roundNumber(row.ast / row.tov, 2);
}

function populateImpactMetrics(row) {
  const ppr = calculatePpr(row);
  if (ppr !== "") row.ppr = ppr;
  const fic = calculateFic(row);
  if (fic !== "") row.fic = fic;
}

function normalizeSeasonValue(value) {
  const text = getStringValue(value).trim();
  if (!text) return "";
  const match = text.match(/(\d{4})\s*[-/]\s*(\d{2,4})/);
  if (match) {
    const endPart = match[2];
    return Number(endPart.length === 2 ? `20${endPart}` : endPart);
  }
  const years = text.match(/\d{4}/g);
  if (years?.length) return Number(years[years.length - 1]);
  const numeric = Number(text);
  return Number.isNaN(numeric) ? text : numeric;
}

function simplifySchoolName(value, datasetId) {
  const raw = getStringValue(value).trim();
  if (!raw || datasetId === "nba") return raw;
  let cleaned = raw
    .replace(/&amp;/gi, "&")
    .replace(/([A-Za-z])-\(([^)]*)\)/g, "$1 $2")
    .replace(/-\s+\(/g, " (")
    .replace(/\(([^)]*)\)/g, " $1 ")
    .replace(/\bN\.J\.I\.T\.\b/gi, "NJIT")
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
  if (datasetId === "juco") cleaned = simplifyStopwordTeamName(cleaned, JUCO_TEAM_STOPWORDS);
  if (datasetId === "naia" || datasetId === "juco") cleaned = stripTrailingLocationSuffix(cleaned);
  const tokens = cleaned.split(" ").filter(Boolean);
  while (tokens.length) {
    const last = normalizeTeamToken(tokens[tokens.length - 1]);
    if (!MASCOT_TOKENS.has(last)) break;
    tokens.pop();
    while (tokens.length && MASCOT_PREFIX_TOKENS.has(normalizeTeamToken(tokens[tokens.length - 1]))) {
      tokens.pop();
    }
  }
  const simplified = cleanupTeamEdgeWords(tokens.join(" ").replace(/\s+/g, " ").trim());
  const override = TEAM_DISPLAY_OVERRIDES[normalizeKey(simplified)] || TEAM_DISPLAY_OVERRIDES[normalizeKey(cleaned)] || TEAM_DISPLAY_OVERRIDES[normalizeKey(raw)];
  return override || simplified || cleaned || raw;
}

function simplifyStopwordTeamName(value, stopwords) {
  const cleaned = getStringValue(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const parts = cleaned.split(" ").filter((word) => word && !stopwords.has(word));
  if (!parts.length) return cleanupTeamEdgeWords(value);
  return cleanupTeamEdgeWords(parts.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(" "));
}

function stripTrailingLocationSuffix(value) {
  const tokens = getStringValue(value).trim().split(/\s+/).filter(Boolean);
  while (tokens.length > 1) {
    const last = normalizeTeamToken(tokens[tokens.length - 1]);
    if (!TRAILING_LOCATION_TOKENS.has(last)) break;
    tokens.pop();
  }
  return tokens.join(" ");
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

function datasetPercentColumnsStoredAsRatios(datasetId = "") {
  return datasetId === "d2" || datasetId === "nba";
}

function scaleRateColumns(row, columns, datasetId = "") {
  if (!datasetPercentColumnsStoredAsRatios(datasetId)) return;
  columns.forEach((column) => {
    if (typeof row[column] === "number" && Number.isFinite(row[column]) && Math.abs(row[column]) <= 1) {
      row[column] = row[column] * 100;
    }
  });
}

function scalePercentRatioColumns(row) {
  Object.keys(row || {}).forEach((column) => {
    if (!/(^ftr$|_ftr$|^three_pr$|_three_pr$)/i.test(column)) return;
    if (typeof row[column] !== "number" || !Number.isFinite(row[column])) return;
    if (Math.abs(row[column]) <= 1.5) row[column] = row[column] * 100;
  });
}

function normalizePercentLikeColumns(row, datasetId = "") {
  const scaleStandardPercentColumns = datasetPercentColumnsStoredAsRatios(datasetId);
  Object.keys(row || {}).forEach((column) => {
    if (typeof row[column] !== "number" || !Number.isFinite(row[column])) return;
    if (isPercentRatioColumn(column)) {
      if (Math.abs(row[column]) <= 1.5) row[column] = row[column] * 100;
      return;
    }
    if (!scaleStandardPercentColumns) return;
    if (!(looksPercentColumn(column) || /^min_per$/i.test(column))) return;
    if (Math.abs(row[column]) <= 1) row[column] = row[column] * 100;
  });
}

function abbreviateFibaCompetition(value) {
  const text = getStringValue(value).trim();
  if (!text) return "";
  let formatted = text
    .replace(/^FIBA\s+/i, "")
    .replace(/Basketball World Cup/gi, "WC")
    .replace(/World Cup/gi, "WC")
    .replace(/\bUnder\s*/gi, "U")
    .replace(/\s+/g, " ")
    .trim();
  if (/EuroBasket Division B/i.test(formatted)) {
    formatted = formatted.replace(/EuroBasket Division B/gi, "Euros B");
  } else if (/EuroBasket/i.test(formatted)) {
    formatted = formatted.replace(/EuroBasket/gi, "Euros A");
  }
  return formatted;
}

function normalizeFibaCountryLabel(value) {
  const text = getStringValue(value).trim();
  if (!text) return "";
  const upper = text.toUpperCase();
  if (/^[A-Z]{3}$/.test(upper)) return upper;
  const key = normalizeKey(text);
  return {
    "united states of america": "USA",
    "united states": "USA",
    usa: "USA",
    "great britain": "GBR",
    britain: "GBR",
    england: "GBR",
    scotland: "GBR",
    wales: "GBR",
    turkey: "TUR",
    turkiye: "TUR",
    "czech republic": "CZE",
    czechia: "CZE",
    korea: "KOR",
    "south korea": "KOR",
    "republic of korea": "KOR",
    "dominican republic": "DOM",
    "puerto rico": "PUR",
    "new zealand": "NZL",
    "saudi arabia": "KSA",
    "united arab emirates": "UAE",
    "bosnia and herzegovina": "BIH",
    "north macedonia": "MKD",
    "cape verde": "CPV",
    "ivory coast": "CIV",
    "cote divoire": "CIV",
  }[key] || upper;
}

function normalizeDisplayName(value) {
  const text = getStringValue(value).trim();
  if (!text) return "";
  const letters = text.replace(/[^A-Za-z]/g, "");
  if (!letters) return text;
  const upperRatio = letters.split("").filter((char) => char === char.toUpperCase()).length / letters.length;
  const needsCleanup = upperRatio >= 0.7 || text.split(/\s+/).some((part) => {
    const onlyLetters = part.replace(/[^A-Za-z]/g, "");
    return onlyLetters.length > 1 && onlyLetters === onlyLetters.toUpperCase();
  });
  const cleaned = !needsCleanup ? text.replace(/\s+/g, " ").trim() : text
    .split(/(\s+|-|')/)
    .map((part) => {
      const onlyLetters = part.replace(/[^A-Za-z]/g, "");
      if (!onlyLetters) return part;
      if (onlyLetters.length === 1) return part.toUpperCase();
      const lower = part.toLowerCase();
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join("")
    .replace(/\bMc([a-z])/g, (_, letter) => `Mc${letter.toUpperCase()}`)
    .replace(/\bO'([a-z])/g, (_, letter) => `O'${letter.toUpperCase()}`)
    .replace(/\s+/g, " ")
    .trim();
  return cleaned
    .split(/\s+/)
    .map((token) => normalizeDisplayNameToken(token))
    .join(" ")
    .replace(/\s+/g, " ")
    .replace(/\bJR\.?$/i, "Jr.")
    .replace(/\bSR\.?$/i, "Sr.")
    .trim();
}

function normalizeDisplayNameToken(token) {
  const bare = token.replace(/[^A-Za-z]/g, "");
  if (!bare) return token;
  if (/^(ii|iii|iv|v|vi|vii|viii|ix|x)$/i.test(bare)) return token.replace(bare, bare.toUpperCase());
  if (/^[A-Za-z]\.?[A-Za-z]\.?$/.test(token)) return bare.toUpperCase();
  if (/^[A-Za-z]{2}$/.test(bare) && COMMON_ABBREV_NAME_TOKENS.has(bare.toLowerCase())) return token.replace(bare, bare.toUpperCase());
  return token;
}

function normalizeDraftPickValue(row) {
  if (!Object.prototype.hasOwnProperty.call(row, "draft_pick")) return;
  const pick = Number(row.draft_pick);
  if (!Number.isFinite(pick) || pick < 1 || pick > 60) {
    row._draftPickBlank = true;
    row.draft_pick = 61;
    return;
  }
  row._draftPickBlank = false;
  row.draft_pick = Math.round(pick);
}

function normalizeD1PlaytypeColumns(row) {
  if (row._careerAggregate) return;
  const gp = firstFinite(row.gp, Number.NaN);
  if (!Number.isFinite(gp) || gp <= 0) return;
  Object.keys(row).forEach((column) => {
    if (typeof row[column] !== "number" || !Number.isFinite(row[column])) return;
    if (/(_fg_att$|_two_fg_att$|_three_fg_att$)/.test(column)) {
      const possColumn = column.replace(/(_fg_att|_two_fg_att|_three_fg_att)$/, "_poss");
      if (Number.isFinite(row[possColumn]) && row[column] > (row[possColumn] * 5)) return;
      row[column] = roundNumber(row[column] * gp, 1);
    }
  });
}

function normalizeD1TruePlaytypeFrequencies(row) {
  const possColumns = [
    "iso_poss",
    "pnr_bh_poss",
    "post_up_poss",
    "pnr_roll_poss",
    "spot_up_poss",
    "off_screen_poss",
    "hand_off_poss",
    "cut_poss",
    "off_reb_poss",
    "transition_poss",
  ];
  const total = possColumns.reduce((sum, column) => sum + (Number.isFinite(row[column]) ? row[column] : 0), 0);
  if (!(total > 0)) return;
  possColumns.forEach((column) => {
    const value = Number.isFinite(row[column]) ? row[column] : 0;
    row[column.replace(/_poss$/, "_freq")] = roundNumber((value / total) * 100, 1);
  });
}

function normalizeJucoDivision(value) {
  const text = getStringValue(value).toUpperCase().trim();
  if (!text) return "";
  if (/\bDIII\b|\bNJCAA\s*DIII\b|\bIII\b|\bD3\b|\bDIVISION\s*III\b/.test(text)) return "III";
  if (/\bDII\b|\bNJCAA\s*DII\b|\bII\b|\bD2\b|\bDIVISION\s*II\b/.test(text)) return "II";
  if (/\bDI\b|\bNJCAA\s*DI\b|\bI\b|\bD1\b|\bDIVISION\s*I\b/.test(text)) return "I";
  return getStringValue(value).trim();
}

function populateDerivedShooting(row, config) {
  let threePm = firstFinite(...config.threeMadeKeys.map((key) => row[key]), Number.NaN);
  let threePa = firstFinite(...config.threeAttKeys.map((key) => row[key]), Number.NaN);
  let twoPm = firstFinite(...config.twoMadeKeys.map((key) => row[key]), Number.NaN);
  let twoPa = firstFinite(...config.twoAttKeys.map((key) => row[key]), Number.NaN);
  const fgm = firstFinite(row.fgm, addIfFinite(twoPm, threePm), Number.NaN);
  const fga = firstFinite(row.fga, addIfFinite(twoPa, threePa), Number.NaN);
  if (!Number.isFinite(threePm) && Number.isFinite(fga)) threePm = 0;
  if (!Number.isFinite(threePa) && Number.isFinite(fga)) threePa = 0;
  if (!Number.isFinite(twoPm)) twoPm = firstFinite(subtractIfFinite(fgm, threePm), Number.NaN);
  if (!Number.isFinite(twoPa)) twoPa = firstFinite(subtractIfFinite(fga, threePa), Number.NaN);

  if (Number.isFinite(threePm)) {
    config.threeMadeKeys.forEach((key) => {
      if (row[key] == null || row[key] === "") row[key] = threePm;
    });
  }
  if (Number.isFinite(threePa)) {
    config.threeAttKeys.forEach((key) => {
      if (row[key] == null || row[key] === "") row[key] = threePa;
    });
  }
  if (Number.isFinite(twoPm)) {
    row.two_pm = twoPm;
    config.twoMadeKeys.forEach((key) => {
      if (row[key] == null || row[key] === "") row[key] = twoPm;
    });
  }
  if (Number.isFinite(twoPa)) {
    row.two_pa = twoPa;
    config.twoAttKeys.forEach((key) => {
      if (row[key] == null || row[key] === "") row[key] = twoPa;
    });
  }
  if (Number.isFinite(fgm) && !Number.isFinite(row.fgm)) row.fgm = fgm;
  if (Number.isFinite(fga) && !Number.isFinite(row.fga)) row.fga = fga;

  const twoPct = zeroSafePercent(twoPm, twoPa);
  const threePct = zeroSafePercent(threePm, threePa);
  const efgPct = zeroSafeEfgPct(fgm, threePm, fga);
  const ftPct = zeroSafePercent(row.ftm, row.fta);
  const points = firstFinite(row.pts, weightedPointTotal(twoPm, threePm, row.ftm), Number.NaN);
  const tsPct = zeroSafeTsPct(points, fga, row.fta);
  config.twoPctKeys.forEach((key) => {
    if ((row[key] == null || row[key] === "") && twoPct !== "") row[key] = twoPct;
  });
  config.threePctKeys.forEach((key) => {
    if ((row[key] == null || row[key] === "") && threePct !== "") row[key] = threePct;
  });
  config.efgKeys.forEach((key) => {
    if ((row[key] == null || row[key] === "") && efgPct !== "") row[key] = efgPct;
  });
  if ((row.ft_pct == null || row.ft_pct === "") && ftPct !== "") row.ft_pct = ftPct;
  if ((row.ts_pct == null || row.ts_pct === "") && tsPct !== "") row.ts_pct = tsPct;
}

function subtractIfFinite(left, right) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return "";
  return left - right;
}

function addIfFinite(left, right) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return "";
  return left + right;
}

function percentIfPossible(made, att) {
  if (!Number.isFinite(made) || !Number.isFinite(att) || att <= 0) return "";
  return roundNumber((made / att) * 100, 1);
}

function zeroSafePercent(made, att) {
  if (!Number.isFinite(att)) return "";
  if (att <= 0) return 0;
  if (!Number.isFinite(made)) return "";
  return roundNumber((made / att) * 100, 1);
}

function zeroSafeEfgPct(fgm, threePm, fga) {
  if (!Number.isFinite(fga)) return "";
  if (fga <= 0) return 0;
  if (!Number.isFinite(fgm)) return "";
  const threes = Number.isFinite(threePm) ? threePm : 0;
  return roundNumber(((fgm + (0.5 * threes)) / fga) * 100, 1);
}

function zeroSafeTsPct(points, fga, fta) {
  if (!Number.isFinite(fga) || !Number.isFinite(fta)) return "";
  const denom = 2 * (fga + (0.44 * fta));
  if (denom <= 0) return 0;
  if (!Number.isFinite(points)) return "";
  return roundNumber((points / denom) * 100, 1);
}

function weightedPointTotal(twoPm, threePm, ftm) {
  if (![twoPm, threePm, ftm].some((value) => Number.isFinite(value))) return Number.NaN;
  return (Number.isFinite(twoPm) ? (twoPm * 2) : 0)
    + (Number.isFinite(threePm) ? (threePm * 3) : 0)
    + (Number.isFinite(ftm) ? ftm : 0);
}

function percentOfPossessions(value, poss) {
  if (!Number.isFinite(value) || !Number.isFinite(poss) || poss <= 0) return "";
  return roundNumber((value / poss) * 100, 1);
}

function ratioIfPossible(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) return "";
  return roundNumber(numerator / denominator, 3);
}

function per40Value(value, minutes) {
  if (!Number.isFinite(value) || !Number.isFinite(minutes) || minutes <= 0) return "";
  return roundNumber((value / minutes) * 40, 1);
}

function d2Per100Value(value, row) {
  if (!Number.isFinite(value) || !Number.isFinite(row.min) || row.min <= 0) return "";
  if (!Number.isFinite(row.team_minutes) || !Number.isFinite(row.team_poss) || row.team_poss <= 0) return "";
  return roundNumber((value / row.min) * ((row.team_minutes / 5) / row.team_poss) * 100, 1);
}

function possPer100Value(value, row) {
  if (!Number.isFinite(value)) return "";
  const poss = estimatedPossessions(row);
  if (!Number.isFinite(poss) || poss <= 0) return "";
  return roundNumber((value / poss) * 100, 1);
}

function estimatedPossessions(row) {
  if (Number.isFinite(row.total_poss)) return row.total_poss;
  const fga = firstFinite(row.fga, Number.isFinite(row.two_pa) && Number.isFinite(row.three_pa) ? row.two_pa + row.three_pa : Number.NaN);
  const fta = firstFinite(row.fta, Number.NaN);
  const tov = firstFinite(row.tov, Number.NaN);
  if (!Number.isFinite(fga) || !Number.isFinite(fta) || !Number.isFinite(tov)) return Number.NaN;
  return fga + (0.44 * fta) + tov;
}

function ortgEstimate(row) {
  const poss = estimatedPossessions(row);
  if (!Number.isFinite(poss) || poss <= 0 || !Number.isFinite(row.pts)) return "";
  return roundNumber((row.pts / poss) * 100, 1);
}

function applyCalculatedRatings(rows, datasetId) {
  const seasonBuckets = new Map();
  rows.forEach((row) => {
    if (!Number.isFinite(row.ortg)) {
      const estimated = ortgEstimate(row);
      if (Number.isFinite(estimated)) row.ortg = estimated;
    }
    const ortg = firstFinite(row.ortg, Number.NaN);
    if (!Number.isFinite(ortg)) return;
    const season = getStringValue(row.season) || datasetId || "all";
    const weight = Math.max(getMinutesValue(row), 1);
    if (!seasonBuckets.has(season)) seasonBuckets.set(season, { total: 0, weight: 0 });
    const bucket = seasonBuckets.get(season);
    bucket.total += ortg * weight;
    bucket.weight += weight;
  });

  rows.forEach((row) => {
    const season = getStringValue(row.season) || datasetId || "all";
    const bucket = seasonBuckets.get(season);
    const avgEff = bucket?.weight ? bucket.total / bucket.weight : 104.9;
    const nationalAvgEff = 104.9;
    const usageValue = firstFinite(row.usg_pct, row.usg, Number.NaN);
    const usage = Number.isFinite(usageValue) ? (Math.abs(usageValue) <= 1.5 ? usageValue * 100 : usageValue) : Number.NaN;
    const ortg = firstFinite(row.ortg, Number.NaN);
    const oppDe = avgEff;
    const mpg = firstFinite(row.mpg, Number.isFinite(row.min) && Number.isFinite(row.gp) && row.gp > 0 ? row.min / row.gp : Number.NaN);
    const minuteSharePct = firstFinite(
      Number.isFinite(row.min_per) ? (Math.abs(row.min_per) <= 1.5 ? row.min_per * 100 : row.min_per) : Number.NaN,
      Number.isFinite(mpg) ? (mpg / 40) * 100 : Number.NaN,
      Number.NaN
    );
    const minutePct = firstFinite(
      Number.isFinite(row.min_per) ? (Math.abs(row.min_per) <= 1.5 ? row.min_per : row.min_per / 100) : Number.NaN,
      Number.isFinite(mpg) ? mpg / 40 : Number.NaN,
      Number.NaN
    );

    if (datasetId !== "d1" && !Number.isFinite(row.adjoe) && Number.isFinite(ortg) && Number.isFinite(usage) && Number.isFinite(oppDe) && oppDe > 0) {
      row.adjoe = roundNumber(calculateAdjoe(ortg, avgEff, oppDe, usage), 1);
    }
    if (datasetId !== "d1" && !Number.isFinite(row.adrtg) && Number.isFinite(row.drtg)) {
      row.adrtg = roundNumber(row.drtg, 1);
    }
    if (datasetId !== "d1" && !Number.isFinite(row.porpag) && Number.isFinite(usage) && Number.isFinite(minutePct)) {
      const adjoe = firstFinite(row.adjoe, Number.NaN);
      if (Number.isFinite(adjoe)) {
        row.porpag = roundNumber(((adjoe + (nationalAvgEff - avgEff) - 88) * minutePct * 69.4) / 500, 3);
      }
    }
    if (datasetId !== "nba" && !Number.isFinite(row.dporpag)) {
      const adrtg = firstFinite(row.adrtg, row.drtg, Number.NaN);
      if (Number.isFinite(adrtg) && Number.isFinite(minuteSharePct)) {
        row.dporpag = roundNumber(calculateDporpag(adrtg, minuteSharePct, nationalAvgEff), 3);
      }
    }
  });
}

function calculateAdjoe(ortg, avgEff, oppDe, usage) {
  const oAdj = avgEff / oppDe;
  const uFactor = (usage * -0.144) + 13.023;
  if (!Number.isFinite(uFactor) || uFactor === 0) return Number.NaN;
  const altZscore = (ortg - avgEff) / uFactor;
  const xOrtg = avgEff + (altZscore * 10.143);
  const usageAdjustment = usage > 20 ? ((usage - 20) * 1.25) : ((usage - 20) * 1.5);
  return (xOrtg + usageAdjustment) * oAdj;
}

function calculateDporpag(adrtg, minuteSharePct, baselineEff = 104.9) {
  const minutePct = Math.abs(minuteSharePct) <= 1.5 ? minuteSharePct * 100 : minuteSharePct;
  const effGap = baselineEff - adrtg;
  return 0.281674445 + (0.031016033 * minutePct) + (0.006464185 * effGap) + (0.001213477 * minutePct * effGap);
}

function firstFinite(...values) {
  return values.find((value) => Number.isFinite(value));
}

function applyPerNormalization(rows, datasetId = "") {
  const buckets = new Map();
  const minuteThreshold = getDatasetMinuteThreshold(DATASETS[datasetId] || {});
  rows.forEach((row) => {
    if (datasetId === "d1" && !row._careerAggregate && Number.isFinite(row.per)) return;
    const base = perBaseValue(row);
    const minutes = getMinutesValue(row);
    if (!Number.isFinite(base) || !Number.isFinite(minutes) || minutes <= 0) return;
    row._perBase = base;
    const season = getStringValue(row.season) || "all";
    if (!buckets.has(season)) {
      buckets.set(season, {
        totalCount: 0,
        totalSum: 0,
        qualifiedCount: 0,
        qualifiedSum: 0,
      });
    }
    const bucket = buckets.get(season);
    bucket.totalCount += 1;
    bucket.totalSum += base;
    if (minutes >= minuteThreshold) {
      bucket.qualifiedCount += 1;
      bucket.qualifiedSum += base;
    }
  });
  rows.forEach((row) => {
    if (datasetId === "d1" && !row._careerAggregate && Number.isFinite(row.per)) return;
    if (!Number.isFinite(row._perBase)) return;
    const season = getStringValue(row.season) || "all";
    const bucket = buckets.get(season);
    const avg = bucket
      ? (bucket.qualifiedCount >= 10
        ? bucket.qualifiedSum / bucket.qualifiedCount
        : (bucket.totalCount ? bucket.totalSum / bucket.totalCount : 0))
      : 0;
    row.per = avg ? roundNumber((row._perBase / avg) * 15, 1) : "";
    delete row._perBase;
  });
}

function perBaseValue(row) {
  const minutes = getMinutesValue(row);
  const fic = calculateFic(row);
  if (!Number.isFinite(minutes) || minutes <= 0 || !Number.isFinite(fic)) return Number.NaN;
  return (fic / minutes) * 40;
}

function calculatePpr(row) {
  const minutes = getMinutesValue(row);
  const ast = firstFinite(row.ast, 0);
  const tov = firstFinite(row.tov, resolveTurnoversValue(row), 0);
  if (!Number.isFinite(minutes) || minutes <= 0 || !Number.isFinite(ast) || !Number.isFinite(tov)) return "";
  return roundNumber((((ast * (2 / 3)) - tov) * 40) / minutes, 1);
}

function calculateFic(row) {
  const pts = firstFinite(row.pts, Number.NaN);
  const orb = firstFinite(row.orb, 0);
  const drb = firstFinite(row.drb, Number.isFinite(row.trb) ? Math.max(0, row.trb - orb) : 0);
  const ast = firstFinite(row.ast, 0);
  const stl = firstFinite(row.stl, 0);
  const blk = firstFinite(row.blk, 0);
  const fga = firstFinite(row.fga, Number.NaN);
  const fta = firstFinite(row.fta, Number.NaN);
  const tov = firstFinite(row.tov, resolveTurnoversValue(row), 0);
  const pf = firstFinite(row.pf, 0);
  if (![pts, ast, stl, blk, fga, fta, tov].every((value) => Number.isFinite(value))) return "";
  return roundNumber(pts + orb + (0.75 * drb) + ast + stl + blk - (0.75 * fga) - (0.375 * fta) - tov - (0.5 * pf), 1);
}

function resolveTurnoversValue(row) {
  const ast = firstFinite(row.ast, Number.NaN);
  const astTo = firstFinite(row.ast_to, Number.NaN);
  if (Number.isFinite(ast) && Number.isFinite(astTo) && astTo > 0) {
    return ast / astTo;
  }
  const poss = firstFinite(row.total_poss, estimatedPossessions(row), Number.NaN);
  const tovPct = firstFinite(row.tov_pct_adv, row.tov_pct, row.topct, Number.NaN);
  if (Number.isFinite(poss) && Number.isFinite(tovPct)) {
    const pct = Math.abs(tovPct) <= 1.5 ? tovPct : tovPct / 100;
    return poss * pct;
  }
  return Number.NaN;
}

function parseHeightToInches(value) {
  if (!value) return Number.NaN;
  const match = String(value).trim().match(/^(\d+)\s*[-']\s*(\d+)$/);
  if (!match) return Number.NaN;
  return Number(match[1]) * 12 + Number(match[2]);
}

function perGameValue(value, gp) {
  if (!Number.isFinite(value) || !Number.isFinite(gp) || gp <= 0) return "";
  return roundNumber(value / gp, 2);
}

function roundNumber(value, digits = 3) {
  if (!Number.isFinite(value)) return "";
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function buildDatasetMeta(rows, config) {
  const columnSet = new Set();
  rows.forEach((row) => {
    Object.keys(row || {}).forEach((column) => columnSet.add(column));
  });
  const columns = Array.from(columnSet);
  const deferredColumns = Array.isArray(config?.deferredColumns) ? config.deferredColumns : [];
  const available = new Set([...columns, ...deferredColumns]);
  const demoColumns = [...new Set([...(config.lockedColumns || []), ...(config.demoColumns || [])])]
    .filter((column) => column === "rank" || available.has(column));
  const groups = config.groups
    .map((group) => {
      const columnsForGroup = group.columns.filter((column) => available.has(column));
      const defaultColumns = (group.defaultColumns || group.columns.filter((column) => config.defaultVisible.includes(column)))
        .filter((column) => available.has(column));
      return { ...group, columns: columnsForGroup, defaultColumns };
    })
    .filter((group) => group.columns.length);
  const statColumns = groups.flatMap((group) => group.columns);
  const allColumns = [...new Set([...demoColumns, ...statColumns])];
  const numericColumns = [...new Set([
    ...columns.filter((column) => rows.some((row) => typeof row[column] === "number" && !Number.isNaN(row[column]))),
    ...deferredColumns,
  ])];
  const teams = Array.from(new Set(rows.map((row) => getStringValue(row[config.teamColumn])).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  const years = Array.from(new Set(rows.map((row) => getStringValue(row[config.yearColumn])).filter(Boolean)))
    .filter((year) => !config.minYear || extractLeadingYear(year) >= config.minYear)
    .sort(compareYears);
  const demoFilterMeta = (config.demoFilterColumns || [])
    .filter((column) => available.has(column))
    .map((column) => ({ column, type: inferDemoFilterType(column, rows, numericColumns) }))
    .filter((item) => item.type);
  return {
    allColumns,
    demoColumns,
    demoColumnSet: new Set(demoColumns),
    demoFilterColumns: demoFilterMeta.map((item) => item.column),
    demoFilterMeta,
    groups,
    numericColumns,
    numericColumnSet: new Set(numericColumns),
    statColumns,
    statColumnSet: new Set(statColumns),
    teams,
    years,
    latestYear: years[0] || "",
    minuteFilterColumn: demoFilterMeta.find((item) => item.column === "min" || item.column === "mp")?.column || "",
  };
}

function inferDemoFilterType(column, rows, numericColumns) {
  if (/dob|birth/i.test(column)) return "date";
  if (/age|height|weight|inches|wingspan|bmi|gp|games|min$|^min|mpg|^mp$|poss|draft|pick|rookie/i.test(column)) return "number";
  if (numericColumns.includes(column)) return "number";
  const sample = rows.map((row) => getStringValue(row[column]).trim()).find(Boolean);
  return parseDateInput(sample) != null ? "date" : "";
}

function matchesDemoFilter(value, filter, type) {
  if (!filter) return true;
  if (type === "date") {
    const comparable = parseDateInput(value);
    if (comparable == null) return !(filter.min || filter.max);
    const min = parseDateInput(filter.min, "min");
    const max = parseDateInput(filter.max, "max");
    if (min != null && comparable < min) return false;
    if (max != null && comparable > max) return false;
    return true;
  }

  if (value == null || Number.isNaN(value)) return !(filter.min || filter.max);
  if (filter.min !== "" && Number(value) < Number(filter.min)) return false;
  if (filter.max !== "" && Number(value) > Number(filter.max)) return false;
  return true;
}

function parseDateInput(value, bound = "exact") {
  const text = getStringValue(value).trim();
  if (!text) return null;
  if (/^\d{4}$/.test(text)) {
    const year = Number(text);
    if (bound === "max") return Date.UTC(year, 11, 31);
    return Date.UTC(year, 0, 1);
  }
  const parsed = Date.parse(text);
  return Number.isNaN(parsed) ? null : parsed;
}

function inferTypes(rows, columns) {
  const types = {};
  columns.forEach((column) => {
    let numericCount = 0;
    let sampleCount = 0;
    for (const row of rows) {
      const value = getStringValue(row[column]).trim();
      if (!value) continue;
      sampleCount += 1;
      if (isNumericString(value)) numericCount += 1;
      if (sampleCount >= 50) break;
    }
    types[column] = sampleCount > 0 && numericCount / sampleCount >= 0.75 ? "number" : "string";
  });
  return types;
}

function coerceValue(value, type) {
  const text = getStringValue(value).trim();
  if (!text) return null;
  if (type === "number" && isNumericString(text)) {
    const number = Number(text);
    return Number.isNaN(number) ? null : number;
  }
  return text;
}

function parseCSV(text) {
  const rows = [];
  if (!text) return rows;
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

function isNumericString(value) {
  return /^-?\d+(?:\.\d+)?$/.test(value);
}

function compareYears(left, right) {
  const leftYear = extractLeadingYear(left);
  const rightYear = extractLeadingYear(right);
  if (leftYear !== rightYear) return rightYear - leftYear;
  return right.localeCompare(left, undefined, { numeric: true, sensitivity: "base" });
}

function extractLeadingYear(value) {
  const match = getStringValue(value).match(/\d{4}/);
  return match ? Number(match[0]) : 0;
}

function displayLabel(dataset, column) {
  if (!column) return "";
  let label = "";
  if (dataset?.labels && Object.prototype.hasOwnProperty.call(dataset.labels, column)) label = dataset.labels[column];
  else if (column === "bmi") label = "BMI";
  else if (column === "wingspan") label = "WS";
  else if (column === "exp") label = "Exp";
  else label = column.replace(/_/g, " ").replace(/\b\w/g, (match) => match.toUpperCase());
  return label.replace(/\bFTr\b/g, "FTR");
}

function formatValue(dataset, column, value, row) {
  if (value == null) return "";
  if (typeof value !== "number") return String(value);
  if (!Number.isFinite(value)) return "";
  if (column === "draft_pick" && row?._draftPickBlank) return "";
  if (column === "rank") return String(value);
  if (/^(season|year|rookie_year)$/i.test(column) && Number.isInteger(value)) return String(value);
  if (column === "age") return value.toFixed(1);
  if (/^(gp|g|gs|min|mp|total_poss)$/i.test(column)) return String(Math.round(value));
  if (/_poss$/i.test(column)) return value.toFixed(1);
  if (/(_att$|_made$|_miss$)/i.test(column)) return String(Math.round(value));
  if (column === "ast_to") return value.toFixed(2);
  if (/ppp$/i.test(column)) return value.toFixed(3);
  if (/^(fic|ppr)$/i.test(column)) return value.toFixed(1);
  if (isRatioDisplayPercentColumn(dataset, column)) return (Math.abs(value) <= 1.5 ? value * 100 : value).toFixed(1);
  if (isPercentRatioColumn(column)) return value.toFixed(1);

  if (looksPercentColumn(column)) {
    return value.toFixed(1);
  }

  if (Number.isInteger(value)) return String(value);
  return value.toFixed(1);
}

function looksPercentColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /pct|percentile|rate|usg$|freq$|share$|^efg$|^tspct$|^ftpct$|^fg2pct$|^fg3pct$|^fgpct_rim$|^fgpct_mid$|^orbpct$|^drbpct$|^astpct$|^topct$|^stlpct$|^blkpct$/.test(baseColumn);
}

function isRatioDisplayPercentColumn(dataset, column) {
  const datasetId = getStringValue(dataset?.id).toLowerCase();
  const baseColumn = stripCompanionPrefix(column);
  const d1RatioColumns = new Set(["fg_pct", "dunk_pct", "rim_pct", "mid_pct", "two_p_pct", "three_p_pct", "ft_pct"]);
  if (datasetId === "d1") return d1RatioColumns.has(baseColumn);
  if (datasetId === "nba_companion" && /^ncaa_/i.test(column)) return d1RatioColumns.has(baseColumn);
  return false;
}

function isPercentRatioColumn(column) {
  const baseColumn = stripCompanionPrefix(column);
  return /(^ftr$|_ftr$|^three_pr$|_three_pr$)/.test(baseColumn);
}

function isLeftAligned(dataset, column) {
  return false;
}

function isWrapColumn(dataset, column) {
  return column === dataset?.playerColumn || column === dataset?.teamColumn || column === "coach" || column === "competition_label";
}

function isD1PlaytypeColumn(column) {
  return D1_PLAYTYPE_DEFS.some((playtype) => column.startsWith(`${playtype.id}_`));
}

function getStringValue(value) {
  return value == null ? "" : String(value);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replace(/"/g, "&quot;");
}
