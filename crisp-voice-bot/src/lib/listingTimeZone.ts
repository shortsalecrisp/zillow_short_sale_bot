import listingTimeZoneData from "../config/us-city-timezones.json";

export interface ListingTimeZoneInput {
  streetAddress?: string;
  city?: string;
  state?: string;
}

export interface ListingTimeZoneResolution {
  timeZone: string;
  reason: string;
  source: "city_consensus" | "zip_city_consensus" | "single_zone_state" | "unresolved";
}

const LISTING_TIME_ZONE_DATA = listingTimeZoneData as {
  zones: string[];
  cities: Record<string, number>;
  zips: Record<string, string[]>;
};

const LISTING_STATE_NAMES: Record<string, string> = {
  ALABAMA: "AL", ALASKA: "AK", ARIZONA: "AZ", ARKANSAS: "AR", CALIFORNIA: "CA",
  COLORADO: "CO", CONNECTICUT: "CT", DELAWARE: "DE", "DISTRICT OF COLUMBIA": "DC",
  FLORIDA: "FL", GEORGIA: "GA", HAWAII: "HI", IDAHO: "ID", ILLINOIS: "IL",
  INDIANA: "IN", IOWA: "IA", KANSAS: "KS", KENTUCKY: "KY", LOUISIANA: "LA",
  MAINE: "ME", MARYLAND: "MD", MASSACHUSETTS: "MA", MICHIGAN: "MI", MINNESOTA: "MN",
  MISSISSIPPI: "MS", MISSOURI: "MO", MONTANA: "MT", NEBRASKA: "NE", NEVADA: "NV",
  "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
  "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", OHIO: "OH", OKLAHOMA: "OK", OREGON: "OR",
  PENNSYLVANIA: "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
  TENNESSEE: "TN", TEXAS: "TX", UTAH: "UT", VERMONT: "VT", VIRGINIA: "VA",
  WASHINGTON: "WA", "WEST VIRGINIA": "WV", WISCONSIN: "WI", WYOMING: "WY",
};

// Exclude split states and local-observance exceptions, including AL, AZ, NV and OK.
const LISTING_SINGLE_ZONE_STATES: Record<string, string> = {
  AR: "America/Chicago", CA: "America/Los_Angeles", CO: "America/Denver",
  CT: "America/New_York", DE: "America/New_York", DC: "America/New_York",
  GA: "America/New_York", HI: "Pacific/Honolulu", IL: "America/Chicago",
  IA: "America/Chicago", LA: "America/Chicago", ME: "America/New_York",
  MD: "America/New_York", MA: "America/New_York", MN: "America/Chicago",
  MS: "America/Chicago", MO: "America/Chicago", MT: "America/Denver",
  NH: "America/New_York", NJ: "America/New_York", NM: "America/Denver",
  NY: "America/New_York", NC: "America/New_York", OH: "America/New_York",
  PA: "America/New_York", RI: "America/New_York", SC: "America/New_York",
  UT: "America/Denver", VT: "America/New_York", VA: "America/New_York",
  WA: "America/Los_Angeles", WV: "America/New_York", WI: "America/Chicago", WY: "America/Denver",
};

function normalizeListingCity(value: string): string {
  return String(value || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "")
    .toUpperCase().replace(/[.'\u2019]/g, "").replace(/[^A-Z0-9 ]/g, " ")
    .replace(/\s+/g, " ").trim()
    .replace(/^ST /, "SAINT ").replace(/^FT /, "FORT ").replace(/^MT /, "MOUNT ");
}

function normalizeListingState(value: string): string {
  const normalized = String(value || "").toUpperCase().replace(/\./g, "").trim();
  return LISTING_STATE_NAMES[normalized] ||
    (Object.values(LISTING_STATE_NAMES).includes(normalized) ? normalized : "");
}

function listingTimeZoneUnresolved(reason: string): ListingTimeZoneResolution {
  return { timeZone: "", reason, source: "unresolved" };
}

function findListingCitySuffix(addressPrefix: string, state: string): string {
  const tokens = normalizeListingCity(addressPrefix).split(" ");
  for (let length = Math.min(8, tokens.length); length >= 1; length -= 1) {
    const candidate = normalizeListingCity(tokens.slice(-length).join(" "));
    if (Object.prototype.hasOwnProperty.call(LISTING_TIME_ZONE_DATA.cities, state + "|" + candidate)) return candidate;
  }
  return "";
}

/** Postal-city consensus, not street geocoding. See config/listing-timezones-provenance.md. */
export function resolveListingTimeZone(input: ListingTimeZoneInput): ListingTimeZoneResolution {
  const address = String(input.streetAddress || "").trim().replace(/,?\s+(?:USA|UNITED STATES)\s*$/i, "");
  const stateNames = Object.keys(LISTING_STATE_NAMES).sort((a, b) => b.length - a.length).join("|");
  const candidateSuffix = address.match(new RegExp("(?:^|[\\s,])(" + stateNames + "|[A-Z]{2})(?:\\s+(\\d{5})(?:-\\d{4})?)?\\s*$", "i"));
  const candidateState = candidateSuffix ? normalizeListingState(candidateSuffix[1] || "") : "";
  const candidatePrefix = candidateSuffix ? address.slice(0, candidateSuffix.index) : "";
  // CT (Court) and NE (Northeast) alone are street suffixes, not state evidence.
  const suffix = candidateState && candidateSuffix && (candidateSuffix[2] || /,\s*$/.test(candidatePrefix) ||
    candidateSuffix[1]!.length > 2 || findListingCitySuffix(candidatePrefix, candidateState)) ? candidateSuffix : null;
  const addressState = suffix ? normalizeListingState(suffix[1] || "") : "";
  const explicitState = normalizeListingState(input.state || "");
  if (String(input.state || "").trim() && !explicitState) return listingTimeZoneUnresolved("invalid_listing_state");
  if (explicitState && addressState && explicitState !== addressState) return listingTimeZoneUnresolved("listing_state_conflict");
  let state = explicitState || addressState;
  let city = normalizeListingCity(input.city || "");
  const zip = suffix?.[2] || address.match(/(?:\s|,)(\d{5})(?:-\d{4})?\s*$/)?.[1] || "";
  const zipCities = zip ? LISTING_TIME_ZONE_DATA.zips[zip] : undefined;
  if (zip && !zipCities) return listingTimeZoneUnresolved("unknown_listing_zip");

  // A known city suffix supports full addresses in E without mistaking the street for a city.
  if (suffix && addressState && state) {
    const addressCity = findListingCitySuffix(address.slice(0, suffix.index), state);
    if (city && addressCity && city !== addressCity) return listingTimeZoneUnresolved("listing_city_conflict");
    city = city || addressCity;
  }

  let source: ListingTimeZoneResolution["source"] = "city_consensus";
  if (zipCities) {
    const matching = zipCities.filter((key) => (!state || key.startsWith(state + "|")) && (!city || key.split("|")[1] === city));
    if (!matching.length) return listingTimeZoneUnresolved("listing_zip_conflict");
    if (!city || !state) {
      if (matching.length !== 1) return listingTimeZoneUnresolved("ambiguous_listing_zip_city");
      const parts = matching[0]!.split("|");
      state = parts[0]!;
      city = parts[1]!;
      source = "zip_city_consensus";
    }
  }
  if (!state) return listingTimeZoneUnresolved("missing_listing_state");
  const key = state + "|" + city;
  const zoneIndex = city ? LISTING_TIME_ZONE_DATA.cities[key] : undefined;
  const singleZone = LISTING_SINGLE_ZONE_STATES[state];
  if ((zoneIndex === undefined || zoneIndex < 0) && singleZone) {
    return { timeZone: singleZone, reason: "", source: "single_zone_state" };
  }
  if (zoneIndex === -1) return listingTimeZoneUnresolved("ambiguous_listing_city_timezone");
  if (zoneIndex === -2) return listingTimeZoneUnresolved("insufficient_listing_city_coordinates");
  if (zoneIndex !== undefined && zoneIndex >= 0) {
    return { timeZone: LISTING_TIME_ZONE_DATA.zones[zoneIndex]!, reason: "", source };
  }
  return listingTimeZoneUnresolved(city ? "unknown_listing_city_in_split_state" : "missing_listing_city_in_split_state");
}
