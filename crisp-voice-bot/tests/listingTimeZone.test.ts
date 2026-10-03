import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import test from "node:test";
import { resolveListingTimeZone, type ListingTimeZoneInput } from "../src/lib/listingTimeZone";
import data from "../src/config/us-city-timezones.json";

const cases: [string, string, string][] = [
  ["Pensacola", "FL", "America/Chicago"], ["Orlando", "FL", "America/New_York"],
  ["Miami", "FL", "America/New_York"], ["Indianapolis", "IN", "America/New_York"],
  ["Fort Worth", "TX", "America/Chicago"],
  ["Nashville", "TN", "America/Chicago"], ["Knoxville", "TN", "America/New_York"],
  ["El Paso", "TX", "America/Denver"], ["Dallas", "TX", "America/Chicago"],
  ["Boise", "ID", "America/Denver"], ["Coeur d'Alene", "ID", "America/Los_Angeles"],
  ["Phoenix", "AZ", "America/Phoenix"], ["Kayenta", "AZ", "America/Denver"],
  ["Las Vegas", "NV", "America/Los_Angeles"],
  ["Louisville", "KY", "America/New_York"], ["Bowling Green", "KY", "America/Chicago"],
  ["Gary", "IN", "America/Chicago"], ["Fort Wayne", "IN", "America/New_York"],
  ["Detroit", "MI", "America/New_York"], ["Iron Mountain", "MI", "America/Chicago"],
  ["North Platte", "NE", "America/Chicago"], ["Scottsbluff", "NE", "America/Denver"],
  ["Sioux Falls", "SD", "America/Chicago"], ["Rapid City", "SD", "America/Denver"],
  ["Bismarck", "ND", "America/Chicago"], ["Dickinson", "ND", "America/Denver"],
  ["Goodland", "KS", "America/Denver"], ["Wichita", "KS", "America/Chicago"],
  ["Ontario", "OR", "America/Denver"], ["Portland", "OR", "America/Los_Angeles"],
];

test("offline city consensus resolves both sides of split-state boundaries", () => {
  for (const [city, state, timeZone] of cases) {
    assert.deepEqual(resolveListingTimeZone({ city, state }), { timeZone, reason: "", source: "city_consensus" }, city + ", " + state);
  }
  assert.ok(data.metadata.cityCount > 29000);
  for (const state of ["AL", "AK", "AZ", "FL", "ID", "IN", "KS", "KY", "MI", "ND", "NE", "NV", "OK", "OR", "SD", "TN", "TX"]) {
    assert.ok(Object.keys(data.cities).filter((key) => key.startsWith(state + "|")).length > 50, state);
  }
});

test("full addresses and state names resolve without a phone or external geocoder", () => {
  for (const streetAddress of ["123 Main St, El Paso, TX 79901", "123 Main St El Paso TX 79901", "123 Main St, El Paso, Texas 79901-1234, USA"]) {
    assert.equal(resolveListingTimeZone({ streetAddress }).timeZone, "America/Denver");
  }
  assert.equal(resolveListingTimeZone({ streetAddress: "123 Washington St", city: "Atlanta", state: "GA" }).timeZone, "America/New_York");
  assert.equal(resolveListingTimeZone({ streetAddress: "123 Main St NE", city: "Rio Rancho", state: "NM" }).timeZone, "America/Denver");
  assert.equal(resolveListingTimeZone({ streetAddress: "123 Main Ct", city: "Orlando", state: "FL" }).timeZone, "America/New_York");
  assert.equal(resolveListingTimeZone({ city: "  st. augustine ", state: "Florida" }).timeZone, "America/New_York");
  assert.equal(resolveListingTimeZone({ streetAddress: "123 Main St 79901", city: "El Paso" }).source, "zip_city_consensus");
});

test("ZIP identifies a consensus city, never overrides a boundary or uncertain city", () => {
  assert.deepEqual(resolveListingTimeZone({ streetAddress: "123 Main St TX 79901" }), {
    timeZone: "America/Denver", reason: "", source: "zip_city_consensus",
  });
  assert.equal(resolveListingTimeZone({ city: "Tuba City", state: "AZ", streetAddress: "123 Main St AZ 86045" }).timeZone, "");
  assert.equal(resolveListingTimeZone({ city: "West Wendover", state: "NV" }).reason, "ambiguous_listing_city_timezone");
  assert.equal(resolveListingTimeZone({ city: "Adak", state: "AK" }).reason, "insufficient_listing_city_coordinates");
});

test("missing, conflicting, invalid, and unknown split-state evidence fails closed", () => {
  const checks: [ListingTimeZoneInput, string][] = [
    [{}, "missing_listing_state"],
    [{ state: "TX" }, "missing_listing_city_in_split_state"],
    [{ city: "Invented city", state: "FL" }, "unknown_listing_city_in_split_state"],
    [{ state: "XX" }, "invalid_listing_state"],
    [{ city: "Dallas", state: "TX", streetAddress: "123 Main St Orlando FL 32801" }, "listing_state_conflict"],
    [{ city: "Dallas", state: "TX", streetAddress: "123 Main St El Paso TX 79901" }, "listing_city_conflict"],
    [{ city: "Dallas", state: "TX", streetAddress: "123 Main St TX 79901" }, "listing_zip_conflict"],
    [{ city: "Dallas", state: "TX", streetAddress: "123 Main St TX 00000" }, "unknown_listing_zip"],
  ];
  for (const [input, reason] of checks) assert.deepEqual(resolveListingTimeZone(input), { timeZone: "", reason, source: "unresolved" });
  for (const state of ["AL", "AK", "AZ", "FL", "ID", "IN", "KS", "KY", "MI", "ND", "NE", "NV", "OK", "OR", "SD", "TN", "TX"]) {
    assert.equal(resolveListingTimeZone({ state }).timeZone, "", state);
  }
  assert.equal(resolveListingTimeZone({ state: "CA" }).timeZone, "America/Los_Angeles");
});

test("IANA city zones preserve DST and Arizona reservation differences", () => {
  const hour = (city: string, state: string, date: string) => new Intl.DateTimeFormat("en-US", {
    timeZone: resolveListingTimeZone({ city, state }).timeZone, hour: "numeric", hourCycle: "h23",
  }).format(new Date(date));
  assert.equal(hour("Pensacola", "FL", "2026-07-15T15:00:00Z"), "10");
  assert.equal(hour("Pensacola", "FL", "2026-12-15T15:00:00Z"), "09");
  assert.equal(hour("Phoenix", "AZ", "2026-07-15T15:00:00Z"), "08");
  assert.equal(hour("Kayenta", "AZ", "2026-07-15T15:00:00Z"), "09");
});

test("Apps Script and TypeScript share exact data and decisions for every postal city", () => {
  const context = vm.createContext({});
  vm.runInContext(fs.readFileSync(path.resolve(__dirname, "../apps-script/listing-time-zone.gs"), "utf8"), context);
  assert.deepEqual(JSON.parse(vm.runInContext("JSON.stringify(LISTING_TIME_ZONE_DATA)", context)), data);
  const resolveGs = context.resolveListingTimeZone_ as typeof resolveListingTimeZone;
  for (const key of Object.keys(data.cities)) {
    const [state, city] = key.split("|");
    const input = { city, state };
    assert.deepEqual(JSON.parse(JSON.stringify(resolveGs(input))), resolveListingTimeZone(input), key);
  }
  for (const input of [{}, { state: "CA" }, { state: "FL" }, { streetAddress: "123 Main St, El Paso, TX 79901" }]) {
    assert.deepEqual(JSON.parse(JSON.stringify(resolveGs(input))), resolveListingTimeZone(input));
  }
});
