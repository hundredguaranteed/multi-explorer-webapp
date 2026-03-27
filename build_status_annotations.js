const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { pathToFileURL } = require("url");

const repoDir = __dirname;
const appPath = path.join(repoDir, "app.js");
const outputPath = path.join(repoDir, "data", "vendor", "status_annotations.js");
const routes = ["d1", "d2", "naia", "juco", "fiba"];

function createStubElement() {
  return {
    hidden: false,
    style: {},
    innerHTML: "",
    textContent: "",
    value: "",
    dataset: {},
    addEventListener() {},
    removeEventListener() {},
    appendChild() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
    cloneNode() { return createStubElement(); },
    get content() {
      return {
        cloneNode() {
          return {
            querySelector() { return createStubElement(); },
          };
        },
      };
    },
  };
}

function loadAppSource() {
  const source = fs.readFileSync(appPath, "utf8");
  return source
    .replace(/\brenderNav\(\);\s*/g, "")
    .replace(/\bwireGlobalEvents\(\);\s*/g, "")
    .replace(/\bhandleRoute\(\);\s*/g, "")
    .replace(/\bwindow\.addEventListener\("hashchange", handleRoute\);\s*/g, "");
}

function createContext() {
  const document = {
    getElementById() { return createStubElement(); },
    createElement() { return createStubElement(); },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    head: { appendChild() {} },
  };
  const window = {
    document,
    location: {
      href: pathToFileURL(path.join(repoDir, "index.html")).href,
      hash: "",
    },
    setTimeout,
    clearTimeout,
    requestIdleCallback(callback) {
      return setTimeout(() => callback({ didTimeout: false, timeRemaining: () => 50 }), 0);
    },
    cancelIdleCallback(handle) {
      clearTimeout(handle);
    },
    addEventListener() {},
    removeEventListener() {},
    console,
    URL,
    STATUS_ANNOTATIONS: null,
  };
  const context = vm.createContext({
    console,
    setTimeout,
    clearTimeout,
    URL,
    window,
    document,
    performance: { now: () => Date.now() },
  });
  context.globalThis = context;
  context.window.window = context.window;
  context.window.globalThis = context;
  return context;
}

function encodeFlags(flags = {}) {
  let bits = 0;
  if (flags.nba) bits |= 1;
  if (flags.d1) bits |= 2;
  if (flags.formerd1) bits |= 4;
  if (flags.former_juco) bits |= 8;
  if (flags.former_d2) bits |= 16;
  if (flags.former_naia) bits |= 32;
  return bits;
}

async function main() {
  const source = loadAppSource();
  const context = createContext();
  vm.runInContext(source, context, { filename: "app.js" });

  const scriptLoads = new Map();
  context.loadScriptOnce = async function loadScriptOnce(src) {
    if (scriptLoads.has(src)) return scriptLoads.get(src);
    const promise = (async () => {
      const scriptPath = path.resolve(repoDir, src);
      const code = fs.readFileSync(scriptPath, "utf8");
      vm.runInContext(code, context, { filename: src });
    })();
    scriptLoads.set(src, promise);
    try {
      await promise;
    } catch (error) {
      scriptLoads.delete(src);
      throw error;
    }
    return promise;
  };

  vm.runInContext("window.STATUS_ANNOTATIONS = null; loadPrecomputedStatusAnnotations = async () => null; applyPrecomputedStatusAnnotations = () => false;", context);

  const ensureDatasetLoaded = vm.runInContext("ensureDatasetLoaded", context);
  const ensureStatusAnnotations = vm.runInContext("ensureStatusAnnotations", context);
  const getStatusGroups = vm.runInContext("getStatusGroups", context);
  const getStatusAnnotationGroupKey = vm.runInContext("getStatusAnnotationGroupKey", context);
  const appState = vm.runInContext("appState", context);

  const bundle = {
    version: 1,
    generatedAt: new Date().toISOString(),
    datasets: {},
  };

  for (const datasetId of routes) {
    console.log(`Building ${datasetId} status annotations`);
    const dataset = await ensureDatasetLoaded(datasetId);
    await ensureStatusAnnotations(datasetId);
    const groups = getStatusGroups(dataset);
    const entries = {};

    groups.forEach((group) => {
      const row = group.rows?.[0] || {};
      const bitmask = encodeFlags(row._statusFlags);
      const entry = {};
      if (bitmask) entry.b = bitmask;
      if (Number.isFinite(row.d1_peak_prpg)) entry.p = row.d1_peak_prpg;
      if (Number.isFinite(row.d1_peak_dprpg)) entry.q = row.d1_peak_dprpg;
      if (Number.isFinite(row.d1_peak_bpm)) entry.r = row.d1_peak_bpm;
      if (Number.isFinite(row.nba_career_epm)) entry.n = row.nba_career_epm;
      if (datasetId === "fiba" && (bitmask & 1) && row.player_name) entry.x = row.player_name;
      if (!Object.keys(entry).length) return;
      entries[getStatusAnnotationGroupKey(group)] = entry;
    });

    bundle.datasets[datasetId] = {
      rowCount: dataset.rows.length,
      groupCount: groups.length,
      entries,
    };
  }

  fs.writeFileSync(outputPath, `window.STATUS_ANNOTATIONS = ${JSON.stringify(bundle)};\n`, "utf8");
  console.log(`Wrote ${outputPath}`);
  console.log(`Datasets loaded: ${Object.keys(appState.datasetCache).join(", ")}`);
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
