const US_STATE_ABBREVIATIONS = new Set([
  "AL",
  "AK",
  "AZ",
  "AR",
  "CA",
  "CO",
  "CT",
  "DE",
  "FL",
  "GA",
  "HI",
  "ID",
  "IL",
  "IN",
  "IA",
  "KS",
  "KY",
  "LA",
  "ME",
  "MD",
  "MA",
  "MI",
  "MN",
  "MS",
  "MO",
  "MT",
  "NE",
  "NV",
  "NH",
  "NJ",
  "NM",
  "NY",
  "NC",
  "ND",
  "OH",
  "OK",
  "OR",
  "PA",
  "RI",
  "SC",
  "SD",
  "TN",
  "TX",
  "UT",
  "VT",
  "VA",
  "WA",
  "WV",
  "WI",
  "WY",
  "DC",
]);

const STREET_SUFFIXES: Record<string, string> = {
  ave: "Avenue",
  avenue: "Avenue",
  blvd: "Boulevard",
  boulevard: "Boulevard",
  cir: "Circle",
  circle: "Circle",
  ct: "Court",
  court: "Court",
  dr: "Drive",
  drive: "Drive",
  ln: "Lane",
  lane: "Lane",
  pkwy: "Parkway",
  parkway: "Parkway",
  pl: "Place",
  place: "Place",
  rd: "Road",
  road: "Road",
  st: "Street",
  street: "Street",
  ter: "Terrace",
  terrace: "Terrace",
  trl: "Trail",
  trail: "Trail",
  way: "Way",
};

type FormatWhisperMessageInput = {
  agentName: string;
  listingAddress: string;
};

function fallbackMessage(agentName: string): string {
  return `Live transfer from ${agentName.trim() || "Unknown"}`;
}

function parseState(addressParts: string[]): string | undefined {
  for (let index = addressParts.length - 1; index >= 0; index -= 1) {
    const matches = addressParts[index].toUpperCase().match(/\b[A-Z]{2}\b/g) ?? [];
    const state = matches.find((match) => US_STATE_ABBREVIATIONS.has(match));

    if (state) {
      return state;
    }
  }

  return undefined;
}

function normalizeStreetName(streetLine: string): string | undefined {
  const withoutStreetNumber = streetLine
    .trim()
    .replace(/^\d+[A-Z]?(?:[-/]\d+[A-Z]?)?\s+/i, "")
    .replace(/\s+/g, " ");

  if (!withoutStreetNumber) {
    return undefined;
  }

  const words = withoutStreetNumber.split(" ");
  const finalWord = words[words.length - 1].replace(/\.$/, "");
  const normalizedSuffix = STREET_SUFFIXES[finalWord.toLowerCase()];

  if (normalizedSuffix) {
    words[words.length - 1] = normalizedSuffix;
  }

  return words.join(" ");
}

export function formatWhisperMessage({ agentName, listingAddress }: FormatWhisperMessageInput): string {
  const addressParts = listingAddress
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);

  const streetName = addressParts[0] ? normalizeStreetName(addressParts[0]) : undefined;
  const state = parseState(addressParts);

  if (!streetName || !state) {
    return fallbackMessage(agentName);
  }

  return `Live transfer from ${agentName.trim() || "Unknown"} about ${streetName} in ${state}`;
}
