/* Build-time only: node scripts/generate-listing-time-zones.cjs /path/to/US.txt /path/to/node_modules */
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const ts = require("typescript");
const root = path.resolve(__dirname, "..");
const sourcePath = process.argv[2];
const modulesPath = process.argv[3];
if (!sourcePath || !modulesPath) throw new Error("Expected GeoNames US.txt and generator node_modules paths");
const { find } = require(path.join(path.resolve(modulesPath), "geo-tz/dist/find-now.js"));
const geoVersion = require(path.join(path.resolve(modulesPath), "geo-tz/package.json")).version;
if (geoVersion !== "8.1.9") throw new Error("Review boundary changes before changing the pinned geo-tz version");
const sourceBytes = fs.readFileSync(sourcePath);
const sourceSha256 = crypto.createHash("sha256").update(sourceBytes).digest("hex");
if (sourceSha256 !== "a2e9aa82edb6037deb1c5524c74cd7c948d337fb43c06a216d470989e901647f") {
  throw new Error("Review changed postal data, source date, and checksum before regenerating");
}
const sourceTs = fs.readFileSync(path.join(root, "src/lib/listingTimeZone.ts"), "utf8");
const normalizerMatch = sourceTs.match(/function normalizeListingCity\([\s\S]+?\n}/);
if (!normalizerMatch) throw new Error("City normalizer not found");
const normalizeCity = new Function(ts.transpileModule(normalizerMatch[0] + "\nreturn normalizeListingCity;", {
  compilerOptions: { target: ts.ScriptTarget.ES2020 },
}).outputText)();
const states = new Set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split(" "));
const cities = new Map();
const zips = new Map();
const pointCache = new Map();
const allowedZones = new Set(["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Phoenix", "America/Anchorage", "America/Adak", "Pacific/Honolulu"]);
// Sample at 5 km and 15 km, not only ZIP centers, to hold obvious boundary locations.
function pointZones(lat, lon) {
  const key = lat + "," + lon;
  if (pointCache.has(key)) return pointCache.get(key);
  const zones = new Set(find(lat, lon));
  for (const km of [5, 15]) {
    for (let bearing = 0; bearing < 360; bearing += 45) {
      const angle = bearing * Math.PI / 180;
      const sampleLat = lat + km * Math.cos(angle) / 111.32;
      const sampleLon = lon + km * Math.sin(angle) / (111.32 * Math.cos(lat * Math.PI / 180));
      // Coastal sea zones do not describe a listing; center points still must be on land.
      for (const zone of find(sampleLat, sampleLon)) if (!zone.startsWith("Etc/")) zones.add(zone);
    }
  }
  const result = [...zones];
  pointCache.set(key, result);
  return result;
}
let rows = 0;
for (const row of sourceBytes.toString("utf8").trim().split(/\r?\n/)) {
  const fields = row.split("\t");
  const [country, zip, rawCity, , state] = fields;
  if (country !== "US" || !states.has(state) || !/^\d{5}$/.test(zip)) continue;
  rows += 1;
  const key = state + "|" + normalizeCity(rawCity);
  const entry = cities.get(key) || { zones: new Set(), hasAccuratePoint: false };
  const lat = Number(fields[9]);
  const lon = Number(fields[10]);
  if (fields[9] && fields[10] && Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180) {
    if (Number(fields[11]) >= 4) entry.hasAccuratePoint = true;
    for (const zone of pointZones(lat, lon)) entry.zones.add(zone);
  }
  cities.set(key, entry);
  const zipKeys = zips.get(zip) || new Set();
  zipKeys.add(key);
  zips.set(zip, zipKeys);
}
const zones = [...allowedZones].sort();
const cityOutput = {};
for (const [key, entry] of [...cities].sort(([a], [b]) => a.localeCompare(b, "en"))) {
  const found = [...entry.zones];
  cityOutput[key] = !entry.hasAccuratePoint ? -2 : found.length === 1 && allowedZones.has(found[0]) ? zones.indexOf(found[0]) : -1;
}
const output = {
  metadata: {
    schemaVersion: 1,
    source: "https://download.geonames.org/export/zip/US.zip",
    sourceDate: "2026-10-03",
    sourceSha256,
    geoTzVersion: geoVersion,
    boundaryDataset: "timezones-now",
    boundarySampleKm: [5, 15],
    acceptedCoordinateAccuracy: "At least one >=4; all finite point coordinates and boundary samples must agree",
    rows,
    cityCount: cities.size,
    resolvedCityCount: Object.values(cityOutput).filter((value) => value >= 0).length,
    licenses: ["GeoNames CC BY 4.0", "geo-tz MIT", "timezone-boundary-builder ODbL 1.0"],
  },
  zones,
  cities: cityOutput,
  zips: Object.fromEntries([...zips].sort(([a], [b]) => a.localeCompare(b, "en")).map(([zip, keys]) => [zip, [...keys].sort()])),
};
fs.mkdirSync(path.join(root, "src/config"), { recursive: true });
fs.writeFileSync(path.join(root, "src/config/us-city-timezones.json"), JSON.stringify(output) + "\n");
const gsSource = sourceTs.replace(/^import listingTimeZoneData[^\n]+\n/m, "")
  .replace(/const LISTING_TIME_ZONE_DATA = listingTimeZoneData as \{[\s\S]+?\n};/, "const LISTING_TIME_ZONE_DATA = " + JSON.stringify(output) + ";")
  .replace(/^export /gm, "");
const gs = ts.transpileModule(gsSource, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.None } }).outputText;
fs.writeFileSync(path.join(root, "apps-script/listing-time-zone.gs"),
  "// GENERATED. See scripts/generate-listing-time-zones.cjs and config/listing-timezones-provenance.md.\n" + gs +
  "\nfunction resolveListingTimeZone_(input) { return resolveListingTimeZone(input); }\n");
console.log(JSON.stringify(output.metadata, null, 2));
