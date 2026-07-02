#!/usr/bin/env python3
"""Free-source short sale listing discovery pilot.

This script is intentionally separate from the production Zillow verifier path.
It searches free public web results, keeps only net-new listings that pass the
strict short-sale rule, and writes review candidates to a pilot Google Sheet tab.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import html
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable


SPREADSHEET_ID = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"
MAIN_TAB = "Sheet1"
PILOT_TAB = "Lead Source Pilot"

PILOT_HEADERS = [
    "first_name",
    "last_name",
    "phone",
    "email",
    "listing_address",
    "city",
    "state",
    "first_seen_at",
    "synthetic_zpid",
    "source",
    "source_query",
    "source_url",
    "status",
    "failure_reason",
    "promotion_status",
    "promotion_notes",
    "import_ready",
    "zip",
    "broker_name",
    "short_sale_evidence_type",
    "qualification_evidence",
    "disqualifying_terms",
    "duplicate_key",
    "matched_main_row",
    "possible_existing_agent_rows",
    "pending_queue_source",
    "pending_queue_address",
    "pending_queue_listing_json",
    "description_excerpt",
    "raw_title",
]

STATE_QUERY_TERMS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}
FORCE_ALL_STATES = os.getenv("FREE_SOURCE_PILOT_FORCE_ALL_STATES", "false").lower() == "true"
DEFAULT_EXCLUDED_STATES = set() if FORCE_ALL_STATES else {
    state.strip().upper()
    for state in os.getenv("FREE_SOURCE_PILOT_EXCLUDED_STATES", "").split(",")
    if state.strip()
}
DEFAULT_STATES = [
    state for state in STATE_QUERY_TERMS if state not in DEFAULT_EXCLUDED_STATES
]

ALL_SOURCE_QUERIES = [
    (
        "idx_broker_pages",
        '"{state}" ("Special Listing Conditions: Short Sale" OR "Is Short Sale: Yes" OR "Potential Short Sale") "For Sale" -zillow -trulia -realtor.com -redfin.com',
    ),
    (
        "realtor.com",
        'site:realtor.com/realestateandhomes-detail "{state}" "Short Sale"',
    ),
    (
        "redfin.com",
        'site:redfin.com "{state}" "Short Sale" "For Sale"',
    ),
    (
        "homes.com",
        'site:homes.com/property "{state}" "Short Sale"',
    ),
]
ALL_SOURCE_QUERY_MAP = dict(ALL_SOURCE_QUERIES)
DEFAULT_SOURCE_BUCKETS = ("idx_broker_pages", "realtor.com")


def configured_source_queries() -> list[tuple[str, str]]:
    raw = os.getenv("FREE_SOURCE_PILOT_SOURCE_BUCKETS", ",".join(DEFAULT_SOURCE_BUCKETS))
    buckets = []
    seen = set()
    for bucket in raw.split(","):
        source = bucket.strip()
        if not source or source in seen or source not in ALL_SOURCE_QUERY_MAP:
            continue
        buckets.append(source)
        seen.add(source)
    if not buckets:
        buckets = list(DEFAULT_SOURCE_BUCKETS)
    return [(source, ALL_SOURCE_QUERY_MAP[source]) for source in buckets]


SOURCE_QUERIES = configured_source_queries()

SEARCH_ENGINE = os.getenv("FREE_SOURCE_PILOT_SEARCH_ENGINE", "auto").lower()
CSE_API_KEY = os.getenv("CS_API_KEY") or os.getenv("GOOGLE_API_KEY")
CSE_CX = os.getenv("CS_CX") or os.getenv("GOOGLE_CX")
CSE_DATE_RESTRICT = os.getenv("FREE_SOURCE_PILOT_DATE_RESTRICT", "d1").strip()
CONTACT_RESEARCH_RESULTS = int(os.getenv("FREE_SOURCE_PILOT_CONTACT_RESEARCH_RESULTS", "3"))
HEADLESS_FALLBACK = os.getenv("FREE_SOURCE_PILOT_HEADLESS_FALLBACK", "true").lower() == "true"
HEADLESS_BUDGET = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_BUDGET", "12")))
HEADLESS_DOMAIN_BUDGET = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_DOMAIN_BUDGET", "4")))
HEADLESS_NAV_TIMEOUT_MS = max(1000, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_NAV_TIMEOUT_MS", "12000")))
HEADLESS_WAIT_MS = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_WAIT_MS", "900")))
HEADLESS_DOMAINS = {
    domain.strip().lower()
    for domain in os.getenv("FREE_SOURCE_PILOT_HEADLESS_DOMAINS", "realtor.com,redfin.com,homes.com").split(",")
    if domain.strip()
}
_headless_used_total = 0
_headless_used_by_domain: dict[str, int] = {}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

SHORT_SALE_LISTING_RE = re.compile(
    r"\b(?:short\s+sale|short-sale)\b",
    re.IGNORECASE,
)

LISTING_EVIDENCE_LABEL_RE = re.compile(
    r"\b(?:"
    r"special\s+listing\s+conditions?|specialListingConditions|"
    r"what'?s\s+special|description|remarks|public\s+remarks|"
    r"about\s+this\s+home|property\s+description|listing\s+description|"
    r"property\s+overview|overview"
    r")\b",
    re.IGNORECASE,
)

SHORT_SALE_SALE_CONTEXT_RE = re.compile(
    r"\bshort\s*-?\s*sale\b.{0,180}\b(?:"
    r"subject\s+to|lender|bank|approval|third[-\s]?party|"
    r"property|home|house|seller|offer"
    r")\b|"
    r"\b(?:subject\s+to|lender|bank|approval|third[-\s]?party|"
    r"property|home|house|seller|offer"
    r")\b.{0,180}\bshort\s*-?\s*sale\b",
    re.IGNORECASE,
)

CURRENT_MARKET_STATUS_RE = re.compile(
    r"\b(?:"
    r"(?:source\s+listing\s+status|listing\s+status|mls\s+status|status)\s*[:#-]?\s*"
    r"(?:active(?:\s+under\s+contract)?|pending|under\s+agreement|under\s+contract|contingent|coming\s+soon)\b|"
    r"STATUS\s+(?:Active(?:\s+Under\s+Contract)?|Pending|Under\s+Agreement|Under\s+Contract|Contingent|Coming\s+Soon)\b|"
    r"Share\s+Active\b|"
    r"currently\s+listed\s+for\s+sale\b|"
    r"homeStatus[\"']?\s*[:=]\s*[\"']?FOR_SALE[\"']?|"
    r"listingStatus[\"']?\s*[:=]\s*[\"']?(?:ACTIVE|PENDING|CONTINGENT|COMING_SOON)[\"']?|"
    r"MLS#\s*\d+.{0,120}\bActive\b"
    r")\b",
    re.IGNORECASE,
)

NON_CURRENT_STATUS_RE = re.compile(
    r"\b(?:"
    r"Off\s+Market|"
    r"Last\s+Sold\s+Price|"
    r"Listing\s+removed|"
    r"Share\s+Closed|"
    r"LISTING\s+CLOSED\b|"
    r"Sold\s*-\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2}/\d{1,2}/\d{2,4})|"
    r"Sold\s+Listed\s+by|"
    r"Sold\s+For\b|"
    r"Sold\s+Date\b|"
    r"Sold\b.{0,80}\bLast\s+updated\b|"
    r"(?:Source\s+Listing\s+Status|Listing\s+Status|Mls\s+Status|Status)\s*[:#-]?\s*"
    r"(?:Temporarily\s+Withdrawn|Withdrawn|Expired|Canceled|Cancelled|Closed|Sold)\b"
    r")",
    re.IGNORECASE,
)

DISQUALIFY_PATTERNS = [
    re.compile(r"\bis\s+short\s+sale\s*[:=]?\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s*[:=]\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\b(?:potential\s+)?short\s+sale\s+(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\b(?:financial\s+status|contract\s+information|special\s+listing\s+conditions?)\s*[-:]?\s*(?:potential\s+)?short\s+sale\s+(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\bisShortSale[\"']?\s*[:=]\s*[\"']?false[\"']?\b", re.IGNORECASE),
    re.compile(r"\bapproved\s+short\s+sale\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s+approved\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\b.{0,80}\bapproved\s+price\b", re.IGNORECASE),
    re.compile(r"\bapproved\s+price\b.{0,80}\bshort\s+sale\b", re.IGNORECASE),
    re.compile(r"\balready\s+approved\b", re.IGNORECASE),
    re.compile(r"\blender\s+approved\b", re.IGNORECASE),
    re.compile(
        r"\b(?:already|currently)\s+(?:working|work)\s+with\s+(?:a\s+|an\s+|the\s+)?"
        r"(?:short\s+sale\s+)?(?:specialist|attorney|negotiator|processor)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:short\s+sale\s+)?(?:specialist|attorney|negotiator|processor)\b.{0,80}"
        r"\b(?:assisting|handling|assigned|involved|processing|negotiating)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:specialist|attorney|negotiator|processor)\s+(?:is\s+)?(?:already\s+)?"
        r"(?:handling|working|assigned)\b",
        re.IGNORECASE,
    ),
]

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}(?!\d)"
)
PERSON_TOKEN_RE = re.compile(r"[A-Z][A-Za-z.'-]{1,30}")
BUSINESS_NAME_RE = re.compile(
    r"\b(?:"
    r"realty|realtor|real\s+estate|properties|property|brokerage|llc|inc|corp|"
    r"company|group|team|associates|homes?|mortgage|bank|trust|services|"
    r"partners|title|insurance|regional|mls|re/max|remax|coldwell|century|sotheby|compass|"
    r"redfin|zillow|berkshire"
    r")\b",
    re.IGNORECASE,
)
GENERIC_NAME_TOKENS = {
    "agent",
    "agents",
    "brokered",
    "broker",
    "brokers",
    "listing",
    "listed",
    "shown",
    "by",
    "office",
    "call",
    "phone",
    "mobile",
    "cell",
    "email",
    "mls",
    "central",
    "northern",
    "regional",
    "southern",
    "dre",
    "license",
    "usa",
}
AGENT_LABEL_CONTEXT_RE = re.compile(
    r"\b(?:listing\s+agent(?:s|\(s\))?|brokered\s+by|shown\s+by|listed\s+by|"
    r"listing\s+courtesy\s+of|courtesy\s+of|presented\s+by|agent\s*[:\-])\s*[:\-]?\s*(.{2,180})",
    re.IGNORECASE,
)
STREET_SUFFIX_RE = (
    r"(?:avenue|ave|street|st|road|rd|drive|dr|lane|ln|boulevard|blvd|court|ct|mews|"
    r"circle|cir|way|place|pl|loop|trail|trl|parkway|pkwy|terrace|ter|highway|hwy|"
    r"route|rte|pass|path|point|pt|run|row)"
)


@dataclass
class SearchResult:
    source: str
    query: str
    url: str
    title: str
    snippet: str


@dataclass
class Candidate:
    source: str
    query: str
    url: str
    title: str
    text: str
    fields: dict[str, str]


@dataclass
class Qualification:
    status: str
    failure_reason: str
    short_sale_evidence_type: str
    evidence: str
    disqualifying_terms: str


@dataclass
class ExistingIndex:
    address_keys: dict[str, int]
    phone_keys: dict[str, int]
    agent_keys: dict[str, list[int]]


def log_event(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_key(value: str) -> str:
    value = html.unescape(value or "").lower()
    value = re.sub(r"\b(?:unit|apt|apartment|suite|ste|#)\b", " ", value)
    value = re.sub(r"\b(?:avenue|ave)\b", " avenue ", value)
    value = re.sub(r"\b(?:street|st)\b", " street ", value)
    value = re.sub(r"\b(?:road|rd)\b", " road ", value)
    value = re.sub(r"\b(?:drive|dr)\b", " drive ", value)
    value = re.sub(r"\b(?:lane|ln)\b", " lane ", value)
    value = re.sub(r"\b(?:boulevard|blvd)\b", " boulevard ", value)
    value = re.sub(r"\b(?:court|ct)\b", " court ", value)
    value = re.sub(r"\b(?:circle|cir)\b", " circle ", value)
    value = re.sub(r"\b(?:parkway|pkwy)\b", " parkway ", value)
    value = re.sub(r"\b(?:terrace|ter)\b", " terrace ", value)
    value = re.sub(r"\b(?:highway|hwy)\b", " highway ", value)
    value = re.sub(r"\b(?:route|rte)\b", " route ", value)
    value = re.sub(
        r"\b([nsew])\s+(.+?\b(?:street|road|avenue|drive|lane|court|circle|boulevard|parkway|terrace|highway|route)\b)\s+\1\b",
        r"\1 \2",
        value,
    )
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_space(value)


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def address_key(address: str, city: str, state: str) -> str:
    parts = [normalize_key(clean_listing_address(address, city, state)), normalize_key(city), normalize_key(state)]
    return "|".join(part for part in parts if part)


def stable_synthetic_zpid(source: str, url: str, address: str, city: str, state: str) -> str:
    raw = "|".join(
        [
            normalize_key(source),
            normalize_key(url),
            address_key(address, city, state),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"free-{digest}"


def agent_key(agent: str, state: str, phone: str = "", email: str = "") -> str:
    phone_key = normalize_phone(phone)
    email_key = normalize_key(email)
    return "|".join(
        part
        for part in [normalize_key(agent), normalize_key(state), phone_key or email_key]
        if part
    )


def split_agent_name(full_name: str) -> tuple[str, str]:
    parts = normalize_space(full_name).split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def current_listing_status(text: str) -> tuple[str, str]:
    compact = normalize_space(html.unescape(text or ""))
    non_current_match = NON_CURRENT_STATUS_RE.search(compact)
    if non_current_match:
        return "not_current", non_current_match.group(0)
    current_match = CURRENT_MARKET_STATUS_RE.search(compact)
    if current_match:
        return "current", current_match.group(0)
    return "unknown", ""


def qualification_for_text(text: str) -> Qualification:
    text = html.unescape(text or "")
    compact = normalize_space(text)
    disqualified = []
    for pattern in DISQUALIFY_PATTERNS:
        match = pattern.search(compact)
        if match:
            disqualified.append(match.group(0))

    short_sale_match = SHORT_SALE_LISTING_RE.search(compact)
    listing_status, listing_status_evidence = current_listing_status(compact)

    if not short_sale_match:
        return Qualification(
            "rejected",
            "missing_listing_text_short_sale",
            "",
            "",
            "; ".join(disqualified),
        )
    if disqualified:
        return Qualification(
            "rejected",
            "disqualifying_short_sale_text",
            extract_short_sale_evidence_type(compact),
            excerpt_around(compact, short_sale_match.start(), short_sale_match.end()),
            "; ".join(disqualified),
        )
    verified_match = verified_short_sale_match(compact)
    if not verified_match:
        return Qualification(
            "rejected",
            "short_sale_not_in_listing_evidence",
            "",
            excerpt_around(compact, short_sale_match.start(), short_sale_match.end()),
            "; ".join(disqualified),
        )
    if listing_status != "current":
        return Qualification(
            "rejected",
            "not_current_listing" if listing_status == "not_current" else "missing_current_listing_status",
            extract_short_sale_evidence_type(compact),
            excerpt_around(compact, verified_match.start(), verified_match.end()),
            listing_status_evidence,
        )

    return Qualification(
        "qualified",
        "",
        extract_short_sale_evidence_type(compact),
        excerpt_around(compact, verified_match.start(), verified_match.end()),
        "",
    )


def verified_short_sale_match(text: str) -> re.Match[str] | None:
    short_sale_match = SHORT_SALE_LISTING_RE.search(text)
    if not short_sale_match:
        return None

    for label_match in LISTING_EVIDENCE_LABEL_RE.finditer(text):
        section = text[label_match.start() : min(len(text), label_match.end() + 1400)]
        section_match = SHORT_SALE_LISTING_RE.search(section)
        if section_match:
            start = label_match.start() + section_match.start()
            end = label_match.start() + section_match.end()
            return SHORT_SALE_LISTING_RE.search(text, start, end)

    contextual_match = SHORT_SALE_SALE_CONTEXT_RE.search(text)
    if contextual_match:
        return contextual_match

    return None


def extract_short_sale_evidence_type(text: str) -> str:
    if re.search(r"(?:special\s+listing\s+conditions?|specialListingConditions)", text, re.I):
        return "special_listing_conditions_or_field"
    if re.search(r"\b(?:description|remarks|what'?s special|about this home|public remarks)\b", text, re.I):
        return "listing_description_or_remarks"
    return "listing_text"


def excerpt_around(text: str, start: int, end: int, width: int = 240) -> str:
    left = max(0, start - width // 2)
    right = min(len(text), end + width // 2)
    return normalize_space(text[left:right])


def build_existing_index(rows: list[list[str]]) -> ExistingIndex:
    address_keys: dict[str, int] = {}
    phone_keys: dict[str, int] = {}
    agent_keys: dict[str, list[int]] = {}
    for idx, row in enumerate(rows[1:], start=2):
        padded = row + [""] * 8
        agent = padded[0]
        phone = padded[2]
        email = padded[3]
        address = padded[4]
        city = padded[5]
        state = padded[6]
        akey = address_key(address, city, state)
        if akey:
            address_keys.setdefault(akey, idx)
        phone_key = normalize_phone(phone)
        if phone_key:
            phone_keys.setdefault(phone_key, idx)
        gkey = agent_key(agent, state, phone, email)
        if gkey:
            agent_keys.setdefault(gkey, []).append(idx)
    return ExistingIndex(address_keys, phone_keys, agent_keys)


def duplicate_status(candidate: Candidate, existing: ExistingIndex) -> tuple[str, str, str]:
    fields = candidate.fields
    akey = address_key(
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
    )
    if akey and akey in existing.address_keys:
        return "duplicate_listing", akey, str(existing.address_keys[akey])

    phone_key = normalize_phone(fields.get("phone", ""))
    if phone_key and phone_key in existing.phone_keys:
        return "duplicate_agent_phone", phone_key, str(existing.phone_keys[phone_key])

    gkey = agent_key(
        fields.get("agent_name", ""),
        fields.get("state", ""),
        fields.get("phone", ""),
        fields.get("email", ""),
    )
    if gkey and gkey in existing.agent_keys:
        return "possible_existing_agent", gkey, ",".join(map(str, existing.agent_keys[gkey]))

    return "", akey, ""


def duplicate_listing_status(candidate: Candidate, existing: ExistingIndex) -> tuple[str, str, str]:
    fields = candidate.fields
    akey = address_key(
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
    )
    if akey and akey in existing.address_keys:
        return "duplicate_listing", akey, str(existing.address_keys[akey])
    return "", akey, ""


def is_valid_email(value: str) -> bool:
    compact = normalize_space(value)
    match = EMAIL_RE.fullmatch(compact)
    return bool(match)


def has_complete_agent_contact(candidate: Candidate) -> bool:
    fields = candidate.fields
    agent_name = clean_agent_name(fields.get("agent_name", ""))
    first_name, last_name = split_agent_name(agent_name)
    return bool(
        first_name
        and last_name
        and normalize_phone(fields.get("phone", ""))
        and is_valid_email(fields.get("email", ""))
    )


def merge_contact_hints(candidate: Candidate, text: str) -> None:
    fields = candidate.fields
    agent_name = extract_agent_name(text)
    if agent_name and not clean_agent_name(fields.get("agent_name", "")):
        fields["agent_name"] = agent_name
    elif agent_name and len(agent_name.split()) >= 2:
        fields["agent_name"] = clean_agent_name(fields.get("agent_name", "")) or agent_name
    if not normalize_phone(fields.get("phone", "")):
        phone_match = PHONE_RE.search(text)
        if phone_match:
            fields["phone"] = phone_match.group(0)
    if not is_valid_email(fields.get("email", "")):
        email_match = EMAIL_RE.search(text)
        if email_match:
            fields["email"] = email_match.group(0)


def research_candidate_contact(candidate: Candidate) -> None:
    fields = candidate.fields
    fields["agent_name"] = clean_agent_name(fields.get("agent_name", ""))
    if has_complete_agent_contact(candidate):
        return

    address = fields.get("listing_address", "")
    city = fields.get("city", "")
    state = fields.get("state", "")
    agent_name = clean_agent_name(fields.get("agent_name", ""))
    queries: list[str] = []
    if address and city and state:
        queries.append(f'"{address}" "{city}" "{state}" "listing agent"')
        queries.append(f'"{address}" "{city}" "{state}" realtor phone email')
    if agent_name:
        queries.append(f'"{agent_name}" realtor "{state}" phone email')

    seen_urls: set[str] = set()
    fetched_pages = 0
    for query in queries:
        try:
            _, results = search_web(query, "contact_research", CONTACT_RESEARCH_RESULTS)
        except Exception as exc:  # noqa: BLE001
            log_event("pilot_contact_research_failed", url=candidate.url, query=query, error=str(exc)[:220])
            continue
        for result in results:
            if result.url in seen_urls or is_ad_or_tracking_url(result.url):
                continue
            seen_urls.add(result.url)
            merge_contact_hints(candidate, " ".join([result.title, result.snippet]))
            if has_complete_agent_contact(candidate):
                return
            if fetched_pages >= 2:
                continue
            try:
                fetched = fetch_url(result.url, allow_headless=False)
                fetched_pages += 1
            except Exception:
                continue
            merge_contact_hints(candidate, strip_html(fetched))
            if has_complete_agent_contact(candidate):
                return


def registered_domain(url: str) -> str:
    host = urllib.parse.urlparse(url).netloc.lower()
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def headless_budget_available(url: str) -> tuple[bool, str]:
    if not HEADLESS_FALLBACK:
        return False, "disabled"
    domain = registered_domain(url)
    if domain not in HEADLESS_DOMAINS:
        return False, "domain_not_allowed"
    if _headless_used_total >= HEADLESS_BUDGET:
        return False, "run_budget_exhausted"
    if _headless_used_by_domain.get(domain, 0) >= HEADLESS_DOMAIN_BUDGET:
        return False, "domain_budget_exhausted"
    return True, domain


def fetch_url_headless(url: str) -> str:
    global _headless_used_total
    domain = registered_domain(url)
    _headless_used_total += 1
    _headless_used_by_domain[domain] = _headless_used_by_domain.get(domain, 0) + 1
    log_event(
        "pilot_headless_fetch_start",
        url=url,
        domain=domain,
        used_total=_headless_used_total,
        used_domain=_headless_used_by_domain[domain],
    )
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        log_event("pilot_headless_fetch_failed", url=url, domain=domain, error=f"playwright_missing:{exc}")
        return ""

    browser = None
    context = None
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--disable-extensions",
                ],
                timeout=HEADLESS_NAV_TIMEOUT_MS,
            )
            context = browser.new_context(
                user_agent=USER_AGENT,
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                viewport={"width": 1280, "height": 720},
            )
            page = context.new_page()
            page.set_default_timeout(HEADLESS_NAV_TIMEOUT_MS)
            page.set_default_navigation_timeout(HEADLESS_NAV_TIMEOUT_MS)

            def route_handler(route) -> None:
                try:
                    if route.request.resource_type in {"image", "media", "font", "stylesheet"}:
                        route.abort()
                    else:
                        route.continue_()
                except Exception:
                    pass

            page.route("**/*", route_handler)
            response = page.goto(url, wait_until="domcontentloaded", timeout=HEADLESS_NAV_TIMEOUT_MS)
            status = response.status if response else 0
            if status in {403, 429, 451}:
                log_event("pilot_headless_fetch_blocked", url=url, domain=domain, status=status)
                return ""
            if HEADLESS_WAIT_MS:
                page.wait_for_timeout(HEADLESS_WAIT_MS)
            content = page.content()
            try:
                visible_text = page.locator("body").inner_text(timeout=3000)
            except Exception:
                visible_text = ""
            combined = "\n".join(part for part in [content, visible_text] if part)
            log_event(
                "pilot_headless_fetch_done",
                url=url,
                domain=domain,
                status=status,
                bytes=len(combined.encode("utf-8")),
            )
            return combined
    except Exception as exc:  # noqa: BLE001
        log_event("pilot_headless_fetch_failed", url=url, domain=domain, error=str(exc)[:500])
        return ""
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass


def fetch_url(url: str, timeout: int = 20, allow_headless: bool = True) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(2_000_000)
            encoding = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(encoding, errors="ignore")
    except urllib.error.HTTPError as exc:
        if allow_headless and exc.code in {403, 429, 451}:
            allowed, reason_or_domain = headless_budget_available(url)
            if allowed:
                rendered = fetch_url_headless(url)
                if rendered.strip():
                    return rendered
            else:
                log_event(
                    "pilot_headless_fetch_skipped",
                    url=url,
                    domain=registered_domain(url),
                    reason=reason_or_domain,
                    status=exc.code,
                )
        raise


def strip_html(markup: str) -> str:
    markup = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", markup)
    markup = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", markup)
    markup = re.sub(r"(?is)<[^>]+>", " ", markup)
    return normalize_space(html.unescape(markup))


def parse_address_parts(value: str) -> dict[str, str]:
    compact = normalize_space(value)
    if not compact:
        return {}
    compact = re.sub(r"\bnull\b", " ", compact, flags=re.I)
    compact = normalize_space(compact).strip(" ,")
    compact = re.split(
        r"\s+(?:for\s+)?\$\d[\d,]*(?:\.\d+)?|\s+\((?:for sale|active|pending)\)",
        compact,
        maxsplit=1,
        flags=re.I,
    )[0]
    patterns = [
        rf"^(?P<listing_address>\d{{1,6}}\s+.+?\b{STREET_SUFFIX_RE}\b)(?:\s+in\s+|,?\s+)"
        r"(?P<city>[A-Z][A-Za-z .'-]{2,40}),\s*(?P<state>[A-Z]{2}),?\s*(?P<zip>\d{5}(?:-\d{4})?)?$",
        r"^(?P<listing_address>\d{1,6}\s+.+?),\s*"
        r"(?P<city>[A-Z][A-Za-z .'-]{2,40}),\s*(?P<state>[A-Z]{2}),?\s*(?P<zip>\d{5}(?:-\d{4})?)?$",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact, re.I)
        if match:
            return {key: normalize_space(value or "") for key, value in match.groupdict(default="").items()}
    return {}


def clean_listing_address(address: str, city: str = "", state: str = "", zip_code: str = "") -> str:
    compact = normalize_space(html.unescape(address or ""))
    if not compact:
        return ""
    compact = re.sub(r"\bnull\b", " ", compact, flags=re.I)
    compact = normalize_space(compact).strip(" ,")
    parsed = parse_address_parts(compact)
    if parsed.get("listing_address"):
        compact = parsed["listing_address"]

    city = normalize_space(city)
    state = normalize_space(state).upper()
    zip_code = normalize_space(zip_code)
    if city and state:
        compact = re.sub(
            rf",?\s+{re.escape(city)}\s*,?\s+{re.escape(state)}(?:\s+{re.escape(zip_code)})?$",
            "",
            compact,
            flags=re.I,
        )
    elif state:
        compact = re.sub(rf",?\s+{re.escape(state)}(?:\s+{re.escape(zip_code)})?$", "", compact, flags=re.I)
    compact = re.sub(r"\s*,\s*$", "", normalize_space(compact))
    return compact.strip(" ,")


def apply_address_parts(fields: dict[str, str], parts: dict[str, str], replace_bad_address: bool = False) -> None:
    if not parts:
        return
    current_address = fields.get("listing_address", "")
    if replace_bad_address and not looks_like_listing_address(current_address):
        fields["listing_address"] = parts.get("listing_address", "")
    else:
        fields.setdefault("listing_address", parts.get("listing_address", ""))
    for key in ["city", "state", "zip"]:
        if parts.get(key):
            fields.setdefault(key, parts[key])


def normalize_candidate_address_fields(fields: dict[str, str]) -> None:
    cleaned = clean_listing_address(
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
        fields.get("zip", ""),
    )
    if cleaned:
        fields["listing_address"] = cleaned


def ddg_search(query: str, source: str, limit: int) -> list[SearchResult]:
    url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query, "kl": "us-en"})
    body = fetch_url(url, allow_headless=False)
    results: list[SearchResult] = []
    blocks = re.split(r'(?=<a[^>]+class="result__a")', body)
    for block in blocks:
        if len(results) >= limit:
            break
        link_match = re.search(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', block, re.I | re.S)
        if not link_match:
            continue
        href = html.unescape(link_match.group(1))
        parsed = urllib.parse.urlparse(href)
        if parsed.netloc.endswith("duckduckgo.com"):
            qs = urllib.parse.parse_qs(parsed.query)
            href = qs.get("uddg", [href])[0]
        if is_ad_or_tracking_url(href):
            continue
        title = strip_html(link_match.group(2))
        snippet_match = re.search(r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>', block, re.I | re.S)
        snippet = strip_html(snippet_match.group(1)) if snippet_match else ""
        if href.startswith("http"):
            results.append(SearchResult(source, query, href, title, snippet))
    return results


def cse_search(query: str, source: str, limit: int) -> list[SearchResult]:
    if not CSE_API_KEY or not CSE_CX:
        return []
    results: list[SearchResult] = []
    start = 1
    while len(results) < limit and start <= 91:
        num = min(10, limit - len(results))
        request_params = {
            "q": query,
            "key": CSE_API_KEY,
            "cx": CSE_CX,
            "num": num,
            "start": start,
        }
        if CSE_DATE_RESTRICT:
            request_params["dateRestrict"] = CSE_DATE_RESTRICT
        params = urllib.parse.urlencode(request_params)
        req = urllib.request.Request(
            "https://www.googleapis.com/customsearch/v1?" + params,
            headers={"User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = data.get("items", [])
        for item in items:
            href = item.get("link") or ""
            if not href.startswith("http") or is_ad_or_tracking_url(href):
                continue
            results.append(
                SearchResult(
                    source,
                    query,
                    href,
                    normalize_space(item.get("title") or ""),
                    normalize_space(item.get("snippet") or ""),
                )
            )
            if len(results) >= limit:
                break
        if len(items) < num:
            break
        start += len(items)
    return results


def search_web(query: str, source: str, limit: int) -> tuple[str, list[SearchResult]]:
    engines: list[str]
    if SEARCH_ENGINE in {"cse", "google", "google_cse"}:
        engines = ["cse"]
    elif SEARCH_ENGINE in {"ddg", "duckduckgo"}:
        engines = ["ddg"]
    else:
        engines = ["cse", "ddg"] if CSE_API_KEY and CSE_CX else ["ddg"]

    last_error = ""
    for engine in engines:
        try:
            if engine == "cse":
                results = cse_search(query, source, limit)
            else:
                results = ddg_search(query, source, limit)
            return engine, results
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            log_event("search_engine_failed", engine=engine, source=source, query=query, error=last_error)
    raise RuntimeError(last_error or "all search engines failed")


def source_result_allowed(result: SearchResult) -> tuple[bool, str]:
    parsed = urllib.parse.urlparse(result.url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    title = normalize_space(result.title)
    if result.source == "realtor.com":
        if host.endswith("realtor.com") and "/realestateandhomes-detail/" in path:
            return True, ""
        return False, "not_realtor_detail"
    if result.source == "redfin.com":
        if host.endswith("redfin.com") and "/home/" in path:
            return True, ""
        return False, "not_redfin_detail"
    if result.source == "homes.com":
        if host.endswith("homes.com") and "/property/" in path:
            return True, ""
        return False, "not_homes_detail"
    if result.source == "idx_broker_pages":
        if re.search(r"/(?:search|blog|buying|selling|guides?|resources?|category|tag)(?:/|$)", path):
            return False, "not_idx_listing_detail"
        if re.search(r"\b(?:\d+\+\s+listings|homes?\s+for\s+sale|search\s+homes|buying\s+a|tips)\b", title, re.I):
            return False, "not_idx_listing_detail"
    return True, ""


def looks_like_listing_address(address: str) -> bool:
    compact = normalize_space(address)
    if not compact or not re.search(r"\d", compact):
        return False
    return not re.search(
        r"\b(?:blog|buying|foreclosure|short\s+sale|homes?\s+for\s+sale|listings?|page|search|vintage|fixer[-\s]?upper|viewing\s+listing|mls\s*#|for\s+\$)\b",
        compact,
        re.IGNORECASE,
    )


def looks_like_person_name(value: str) -> bool:
    compact = normalize_space(value).strip(" .:-")
    if not compact or BUSINESS_NAME_RE.search(compact) or re.search(r"\d", compact):
        return False
    tokens = [token.strip(" .'") for token in compact.split()]
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    return not any(token.lower().strip(".") in GENERIC_NAME_TOKENS for token in tokens)


def clean_agent_name(value: str) -> str:
    compact = normalize_space(html.unescape(value or ""))
    if not compact:
        return ""
    compact = re.sub(
        r"(?i)^(?:by|agent|agents|listing\s+agent(?:s|\(s\))?|brokered\s+by|"
        r"shown\s+by|listed\s+by|listing\s+courtesy\s+of|courtesy\s+of|"
        r"presented\s+by)\s*[:\-]?\s*",
        "",
        compact,
    )
    compact = re.split(
        r"(?i)\b(?:call|phone|cell|mobile|email|license|lic\.?|dre|brokerage|broker|"
        r"brokered\s+by|shown\s+by|listed\s+by|listing\s+office|office|mls|fax|"
        r"website|provided\s+by|realty|realtor|"
        r"real\s+estate|properties|brokerage|llc|inc|corp|company|group|team|"
        r"associates|homes?|mortgage|bank|trust|services|partners|title|insurance)\b|"
        r"[|•;,]|\.\s+(?=[{\"A-Z])",
        compact,
        maxsplit=1,
    )[0]
    compact = normalize_space(re.split(r"(?i)\s+(?:and|&|with)\s+", compact, maxsplit=1)[0]).strip(" .:-")
    if looks_like_person_name(compact):
        return compact

    tokens = PERSON_TOKEN_RE.findall(compact)
    for start in range(len(tokens)):
        for length in range(min(4, len(tokens) - start), 1, -1):
            candidate = " ".join(tokens[start : start + length]).strip(" .:-")
            if looks_like_person_name(candidate):
                return candidate
    return ""


def extract_agent_name(text: str) -> str:
    for match in AGENT_LABEL_CONTEXT_RE.finditer(text):
        name = clean_agent_name(match.group(1))
        if name:
            return name
    return ""


def first_contact_phone_match(text: str) -> re.Match[str] | None:
    for match in PHONE_RE.finditer(text):
        context = text[max(0, match.start() - 20) : min(len(text), match.end() + 20)].lower()
        if re.search(r"[a-z/_-]?\d{10}[a-z0-9_-]*\.(?:jpg|jpeg|png|webp)\b", context):
            continue
        return match
    return None


def jsonld_type_names(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value.lower()}
    if isinstance(value, list):
        return {str(item).lower() for item in value}
    return set()


def required_review_field_failure(candidate: Candidate, qualification: Qualification) -> str:
    agent_name = clean_agent_name(candidate.fields.get("agent_name", ""))
    if agent_name:
        candidate.fields["agent_name"] = agent_name
    if not looks_like_listing_address(candidate.fields.get("listing_address", "")):
        return "missing_listing_detail_address"
    if qualification.status != "qualified" or not normalize_space(qualification.evidence):
        return "missing_short_sale_confirmation"
    return ""


def candidate_matches_requested_state(candidate: Candidate, requested_state: str) -> bool:
    state = normalize_space(candidate.fields.get("state", "")).upper()
    return bool(state) and state == requested_state.upper()


def is_ad_or_tracking_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return (
        host.endswith("duckduckgo.com")
        or "bing.com" in host and "/aclick" in path
        or "doubleclick.net" in host
        or "googleadservices.com" in host
    )


def extract_jsonld_text(markup: str) -> tuple[str, dict[str, str]]:
    fields: dict[str, str] = {}
    pieces: list[str] = []
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        markup,
        re.I | re.S,
    ):
        raw = html.unescape(match.group(1)).strip()
        pieces.append(raw)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in iter_json_objects(data):
            if not isinstance(obj, dict):
                continue
            address = obj.get("address")
            if isinstance(address, dict):
                fields.setdefault("listing_address", str(address.get("streetAddress") or ""))
                fields.setdefault("city", str(address.get("addressLocality") or ""))
                fields.setdefault("state", str(address.get("addressRegion") or ""))
                fields.setdefault("zip", str(address.get("postalCode") or ""))
            elif isinstance(address, str):
                fields.setdefault("listing_address", address)
            name = obj.get("name")
            if isinstance(name, str) and not fields.get("raw_name"):
                fields["raw_name"] = name
            type_names = jsonld_type_names(obj.get("@type"))
            if type_names.intersection({"person", "realestateagent", "real estate agent"}):
                agent_name = clean_agent_name(str(name or ""))
                if agent_name:
                    fields.setdefault("agent_name", agent_name)
            if isinstance(name, str):
                apply_address_parts(fields, parse_address_parts(name), replace_bad_address=True)
            description = obj.get("description")
            if isinstance(description, str):
                pieces.append(description)
    return "\n".join(pieces), fields


def decode_json_string(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except json.JSONDecodeError:
        return html.unescape(value.replace(r"\/", "/"))


def extract_embedded_listing_text(markup: str) -> str:
    pieces: list[str] = []
    if re.search(r'"is_short_sale"\s*:\s*true', markup, re.I):
        pieces.append("Special Listing Conditions: Short Sale.")
    status_flags = [
        (r'"is_pending"\s*:\s*true', "Status: Pending."),
        (r'"is_contingent"\s*:\s*true', "Status: Contingent."),
        (r'"is_coming_soon"\s*:\s*true', "Status: Coming Soon."),
    ]
    for pattern, text in status_flags:
        if re.search(pattern, markup, re.I):
            pieces.append(text)
    for match in re.finditer(r'"description"\s*:\s*"((?:\\.|[^"\\]){20,5000})"', markup, re.I):
        decoded = normalize_space(decode_json_string(match.group(1)))
        if decoded:
            pieces.append(f"Property description: {decoded}")
    for match in re.finditer(r'"text"\s*:\s*"((?:\\.|[^"\\]){20,5000})"', markup, re.I):
        decoded = normalize_space(decode_json_string(match.group(1)))
        if decoded:
            pieces.append(f"Property description: {decoded}")
    for match in re.finditer(r'"number"\s*:\s*"((?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4})"', markup, re.I):
        pieces.append(f"Phone: {decode_json_string(match.group(1))}")
    return " ".join(pieces)


def iter_json_objects(data: Any) -> Iterable[Any]:
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from iter_json_objects(value)
    elif isinstance(data, list):
        for item in data:
            yield from iter_json_objects(item)


def infer_fields(result: SearchResult, markup: str) -> Candidate:
    json_text, json_fields = extract_jsonld_text(markup)
    embedded_listing_text = extract_embedded_listing_text(markup)
    page_text = strip_html(markup)
    combined = normalize_space(" ".join([result.title, result.snippet, json_text, embedded_listing_text, page_text]))

    fields = dict(json_fields)
    title_parts = [part.strip() for part in re.split(r"\s*[|–-]\s*", result.title) if part.strip()]
    if title_parts and not fields.get("listing_address"):
        fields["listing_address"] = title_parts[0]
    if not looks_like_listing_address(fields.get("listing_address", "")):
        apply_address_parts(fields, parse_address_parts(result.title), replace_bad_address=True)
    if not looks_like_listing_address(fields.get("listing_address", "")) and fields.get("raw_name"):
        apply_address_parts(fields, parse_address_parts(fields["raw_name"]), replace_bad_address=True)

    fields.setdefault("source_url", result.url)
    fields.setdefault("agent_name", extract_agent_name(combined))
    fields.setdefault("broker_name", extract_labeled_value(combined, ["Broker", "Brokerage", "Listing Office"]))

    phone_match = first_contact_phone_match(combined)
    email_match = EMAIL_RE.search(combined)
    fields.setdefault("phone", phone_match.group(0) if phone_match else "")
    fields.setdefault("email", email_match.group(0) if email_match else "")

    if not fields.get("city") or not fields.get("state"):
        city_state = re.search(r"\b([A-Z][A-Za-z .'-]{2,40}),\s*([A-Z]{2})\s*(\d{5})?\b", result.title + " " + result.snippet)
        if city_state:
            fields.setdefault("city", normalize_space(city_state.group(1)))
            fields.setdefault("state", city_state.group(2))
            fields.setdefault("zip", city_state.group(3) or "")

    normalize_candidate_address_fields(fields)
    return Candidate(result.source, result.query, result.url, result.title, combined, fields)


def extract_labeled_value(text: str, labels: list[str]) -> str:
    for label in labels:
        match = re.search(rf"{re.escape(label)}\s*[:\-]\s*([^|•\n\r,]{{2,80}})", text, re.I)
        if match:
            return normalize_space(match.group(1))
    return ""


def canonical_queue_payload(candidate: Candidate, qualification: Qualification, synthetic_zpid: str) -> dict[str, str]:
    fields = candidate.fields
    address = fields.get("listing_address", "")
    source = f"free-source-pilot:{candidate.source}"
    payload = {
        "zpid": synthetic_zpid,
        "address": address,
        "street": address,
        "city": fields.get("city", ""),
        "state": fields.get("state", ""),
        "zip": fields.get("zip", ""),
        "source": source,
        "search_source": source,
        "agentName": fields.get("agent_name", ""),
        "brokerName": fields.get("broker_name", ""),
        "brokerageName": fields.get("broker_name", ""),
        "phone": fields.get("phone", ""),
        "email": fields.get("email", ""),
        "url": candidate.url,
        "detailUrl": candidate.url,
        "propertyUrl": candidate.url,
        "homeStatus": "FOR_SALE",
        "specialListingConditions": "Short Sale",
        "listing_description": candidate.text[:8_000],
        "description": candidate.text[:8_000],
        "listingText": candidate.text[:8_000],
        "sourceQuery": candidate.query,
        "sourceTitle": candidate.title,
        "qualificationEvidence": qualification.evidence,
    }
    return {key: str(value) for key, value in payload.items() if str(value or "").strip()}


def candidate_to_row(
    candidate: Candidate,
    qualification: Qualification,
    duplicate_key: str,
    matched: str,
    agent_rows: str,
) -> list[str]:
    fields = candidate.fields
    fields["agent_name"] = clean_agent_name(fields.get("agent_name", ""))
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    first_name, last_name = split_agent_name(fields.get("agent_name", ""))
    synthetic_zpid = stable_synthetic_zpid(
        candidate.source,
        candidate.url,
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
    )
    payload = canonical_queue_payload(candidate, qualification, synthetic_zpid)
    queue_source = payload.get("source", "")
    queue_address = payload.get("address", "")
    has_phone = bool(normalize_phone(fields.get("phone", "")))
    has_email = is_valid_email(fields.get("email", ""))
    has_contact = bool(first_name and last_name and has_phone and has_email)
    import_ready = "yes" if qualification.status == "qualified" and not matched and not agent_rows and has_contact else "review"
    promotion_status = "pilot_review"
    promotion_notes = (
        "Net-new listing candidate; review then promote to PendingQueue using pending_queue_listing_json."
        if not matched and not agent_rows and has_contact
        else "Qualified net-new short sale listing; agent contact is missing or partial and can be reviewed later."
        if not has_contact
        else "Possible existing main-sheet match; review before promotion."
    )
    return [
        first_name,
        last_name,
        fields.get("phone", ""),
        fields.get("email", ""),
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
        now,
        synthetic_zpid,
        candidate.source,
        candidate.query,
        candidate.url,
        qualification.status,
        qualification.failure_reason,
        promotion_status,
        promotion_notes,
        import_ready,
        fields.get("zip", ""),
        fields.get("broker_name", ""),
        qualification.short_sale_evidence_type,
        qualification.evidence,
        qualification.disqualifying_terms,
        duplicate_key,
        matched,
        agent_rows,
        queue_source,
        queue_address,
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        candidate.text[:900],
        candidate.title,
    ]


def load_service_account_info(path: str | None) -> dict[str, Any]:
    if os.getenv("GOOGLE_SVC_JSON"):
        return json.loads(os.environ["GOOGLE_SVC_JSON"])
    if os.getenv("GCP_SERVICE_ACCOUNT_JSON"):
        return json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    for candidate in ("service_account.json",):
        if os.path.exists(candidate):
            with open(candidate, "r", encoding="utf-8") as fh:
                return json.load(fh)
    raise SystemExit("No service account JSON found. Pass --service-account or set GOOGLE_SVC_JSON.")


def sheets_client(service_account: dict[str, Any]):
    try:
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import Request

        creds = Credentials.from_service_account_info(
            service_account,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        creds.refresh(Request())
        return creds.token
    except ImportError:
        return sheets_token_via_openssl(service_account)


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def sheets_token_via_openssl(service_account: dict[str, Any]) -> str:
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    claims = {
        "iss": service_account["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }
    signing_input = ".".join(
        [
            base64url(json.dumps(header, separators=(",", ":")).encode("utf-8")),
            base64url(json.dumps(claims, separators=(",", ":")).encode("utf-8")),
        ]
    )
    key_path = None
    try:
        import tempfile

        with tempfile.NamedTemporaryFile("w", delete=False) as key_file:
            key_file.write(service_account["private_key"])
            key_path = key_file.name
        signature = subprocess.check_output(
            ["openssl", "dgst", "-sha256", "-sign", key_path],
            input=signing_input.encode("ascii"),
        )
    finally:
        if key_path:
            try:
                os.unlink(key_path)
            except OSError:
                pass
    assertion = signing_input + "." + base64url(signature)
    body = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        token_data = json.loads(resp.read().decode("utf-8"))
    return token_data["access_token"]


def sheets_request(token: str, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = "https://sheets.googleapis.com/v4/spreadsheets/" + path
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8")) if raw else {}


def ensure_tab(token: str, spreadsheet_id: str, tab_name: str) -> None:
    meta = sheets_request(token, "GET", f"{spreadsheet_id}?fields=sheets.properties.title")
    titles = {sheet["properties"]["title"] for sheet in meta.get("sheets", [])}
    if tab_name not in titles:
        sheets_request(
            token,
            "POST",
            f"{spreadsheet_id}:batchUpdate",
            {"requests": [{"addSheet": {"properties": {"title": tab_name, "gridProperties": {"columnCount": len(PILOT_HEADERS)}}}}]},
        )
    header_range = f"{tab_name}!A1:{column_letter(len(PILOT_HEADERS))}1"
    values = get_values(token, spreadsheet_id, header_range)
    if not values or values[0] != PILOT_HEADERS:
        update_values(token, spreadsheet_id, header_range, [PILOT_HEADERS])


def column_letter(count: int) -> str:
    value = count
    letters = ""
    while value:
        value, rem = divmod(value - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def get_values(token: str, spreadsheet_id: str, range_name: str) -> list[list[str]]:
    encoded = urllib.parse.quote(range_name, safe="")
    data = sheets_request(token, "GET", f"{spreadsheet_id}/values/{encoded}?majorDimension=ROWS")
    return data.get("values", [])


def update_values(token: str, spreadsheet_id: str, range_name: str, values: list[list[str]]) -> None:
    encoded = urllib.parse.quote(range_name, safe="")
    sheets_request(
        token,
        "PUT",
        f"{spreadsheet_id}/values/{encoded}?valueInputOption=RAW",
        {"values": values},
    )


def append_values(token: str, spreadsheet_id: str, range_name: str, values: list[list[str]]) -> None:
    if not values:
        return
    encoded = urllib.parse.quote(range_name, safe="")
    sheets_request(
        token,
        "POST",
        f"{spreadsheet_id}/values/{encoded}:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS",
        {"values": values},
    )


def run(args: argparse.Namespace) -> None:
    service_account = load_service_account_info(args.service_account)
    token = sheets_client(service_account)
    ensure_tab(token, args.spreadsheet_id, args.pilot_tab)

    main_rows = get_values(token, args.spreadsheet_id, f"{args.main_tab}!A:AQ")
    existing = build_existing_index(main_rows)
    pilot_rows = get_values(token, args.spreadsheet_id, f"{args.pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}")
    already_seen_urls = {row[11] for row in pilot_rows[1:] if len(row) > 11 and row[11]}
    pilot_seen_addresses = {
        address_key(row[4], row[5], row[6])
        for row in pilot_rows[1:]
        if len(row) > 6 and address_key(row[4], row[5], row[6])
    }
    stats = {
        "searched": 0,
        "results": 0,
        "fetched": 0,
        "qualified": 0,
        "duplicates": 0,
        "rejected": 0,
        "fetch_failed": 0,
        "rows_written": 0,
    }
    log_event(
        "pilot_run_start",
        states=args.states,
        source_count=len(SOURCE_QUERIES),
        source_buckets=[source for source, _ in SOURCE_QUERIES],
        results_per_query=args.results_per_query,
        search_engine=SEARCH_ENGINE,
        cse_date_restrict=CSE_DATE_RESTRICT,
        cse_configured=bool(CSE_API_KEY and CSE_CX),
        dry_run=args.dry_run,
    )

    for state in args.states:
        state_query_term = STATE_QUERY_TERMS.get(state.upper(), state)
        for source, template in SOURCE_QUERIES:
            query = template.format(state=state_query_term)
            stats["searched"] += 1
            log_event("pilot_query_start", state=state, source=source, query=query)
            try:
                engine, results = search_web(query, source, args.results_per_query)
            except Exception as exc:  # noqa: BLE001
                log_event("pilot_query_failed", state=state, source=source, query=query, error=str(exc))
                continue
            stats["results"] += len(results)
            log_event(
                "pilot_query_results",
                state=state,
                source=source,
                engine=engine,
                query=query,
                result_count=len(results),
            )
            time.sleep(args.sleep_seconds)
            query_rows: list[list[str]] = []
            query_stats = {"fetched": 0, "qualified": 0, "duplicates": 0, "rejected": 0, "fetch_failed": 0}
            for result in results:
                if result.url in already_seen_urls:
                    stats["duplicates"] += 1
                    query_stats["duplicates"] += 1
                    log_event("pilot_duplicate_url", state=state, source=source, url=result.url)
                    continue
                allowed, reason = source_result_allowed(result)
                if not allowed:
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    log_event("pilot_result_skipped", state=state, source=source, url=result.url, reason=reason)
                    continue
                try:
                    markup = fetch_url(result.url)
                    stats["fetched"] += 1
                    query_stats["fetched"] += 1
                except Exception as exc:  # noqa: BLE001
                    stats["fetch_failed"] += 1
                    query_stats["fetch_failed"] += 1
                    log_event("pilot_fetch_failed", state=state, source=source, url=result.url, error=str(exc))
                    continue
                candidate = infer_fields(result, markup)
                if not looks_like_listing_address(candidate.fields.get("listing_address", "")):
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    log_event(
                        "pilot_candidate_rejected",
                        state=state,
                        source=source,
                        url=result.url,
                        reason="missing_listing_detail_address",
                        evidence=candidate.fields.get("listing_address", "")[:220],
                    )
                    continue
                if not candidate_matches_requested_state(candidate, state):
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    log_event(
                        "pilot_candidate_rejected",
                        state=state,
                        source=source,
                        url=result.url,
                        reason="listing_state_mismatch",
                        evidence=candidate.fields.get("state", "")[:40],
                    )
                    continue
                qualification = qualification_for_text(candidate.text)
                if qualification.status != "qualified":
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    log_event(
                        "pilot_candidate_rejected",
                        state=state,
                        source=source,
                        url=result.url,
                        reason=qualification.failure_reason,
                        evidence=qualification.evidence[:220],
                    )
                    if args.include_rejected:
                        log_event(
                            "pilot_rejected_row_not_written",
                            state=state,
                            source=source,
                            url=result.url,
                            reason="missing_short_sale_confirmation",
                        )
                    continue
                listing_dup_status, listing_dup_key, listing_matched = duplicate_listing_status(candidate, existing)
                if listing_dup_status:
                    stats["duplicates"] += 1
                    query_stats["duplicates"] += 1
                    log_event(
                        "pilot_candidate_duplicate",
                        state=state,
                        source=source,
                        url=result.url,
                        duplicate_status=listing_dup_status,
                        duplicate_key=listing_dup_key,
                        matched=listing_matched,
                    )
                    continue
                if listing_dup_key and listing_dup_key in pilot_seen_addresses:
                    stats["duplicates"] += 1
                    query_stats["duplicates"] += 1
                    log_event(
                        "pilot_candidate_duplicate",
                        state=state,
                        source=source,
                        url=result.url,
                        duplicate_status="pilot_listing",
                        duplicate_key=listing_dup_key,
                    )
                    continue
                required_failure = required_review_field_failure(candidate, qualification)
                if required_failure:
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    log_event(
                        "pilot_candidate_rejected",
                        state=state,
                        source=source,
                        url=result.url,
                        reason=required_failure,
                        evidence=candidate.fields.get("agent_name", "")[:220]
                        or candidate.fields.get("listing_address", "")[:220]
                        or qualification.evidence[:220],
                    )
                    continue
                dup_status, dup_key, matched = duplicate_status(candidate, existing)
                if dup_status == "duplicate_listing":
                    stats["duplicates"] += 1
                    query_stats["duplicates"] += 1
                    log_event(
                        "pilot_candidate_duplicate",
                        state=state,
                        source=source,
                        url=result.url,
                        duplicate_status=dup_status,
                        duplicate_key=dup_key,
                        matched=matched,
                    )
                    continue
                phone_key = normalize_phone(candidate.fields.get("phone", ""))
                matched_main_row = matched if dup_status == "duplicate_agent_phone" else ""
                agent_rows = matched if dup_status == "possible_existing_agent" else ""
                stats["qualified"] += 1
                query_stats["qualified"] += 1
                query_rows.append(candidate_to_row(candidate, qualification, dup_key, matched_main_row, agent_rows))
                log_event(
                    "pilot_candidate_qualified",
                    state=state,
                    source=source,
                    url=result.url,
                    address=candidate.fields.get("listing_address", ""),
                    agent=candidate.fields.get("agent_name", ""),
                    has_phone=bool(phone_key),
                    has_email=is_valid_email(candidate.fields.get("email", "")),
                    duplicate_status=dup_status,
                    matched=matched,
                )
                already_seen_urls.add(result.url)
                if listing_dup_key:
                    pilot_seen_addresses.add(listing_dup_key)
                time.sleep(args.sleep_seconds)

            if query_rows and not args.dry_run:
                append_values(
                    token,
                    args.spreadsheet_id,
                    f"{args.pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}",
                    query_rows,
                )
            stats["rows_written"] += 0 if args.dry_run else len(query_rows)
            log_event(
                "pilot_query_done",
                state=state,
                source=source,
                rows_written=0 if args.dry_run else len(query_rows),
                **query_stats,
            )

    log_event("pilot_run_done", stats=stats, dry_run=args.dry_run)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Free-source short sale listing pilot")
    parser.add_argument("--spreadsheet-id", default=os.getenv("GSHEET_ID", SPREADSHEET_ID))
    parser.add_argument("--main-tab", default=os.getenv("GSHEET_TAB", MAIN_TAB))
    parser.add_argument("--pilot-tab", default=os.getenv("PILOT_TAB", PILOT_TAB))
    parser.add_argument("--service-account", default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    parser.add_argument("--states", nargs="+", default=DEFAULT_STATES)
    parser.add_argument("--results-per-query", type=int, default=10)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--include-rejected", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
