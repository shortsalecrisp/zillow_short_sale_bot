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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable


SPREADSHEET_ID = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"
MAIN_TAB = "Sheet1"
PILOT_TAB = "Lead Source Pilot"

PILOT_HEADERS = [
    "agent_name",
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
DEFAULT_STATES = list(STATE_QUERY_TERMS.keys())

SOURCE_QUERIES = [
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
    (
        "idx_broker_pages",
        '"{state}" "Special Listing Conditions" "Short Sale" "For Sale" -zillow -trulia -realtor.com -redfin.com',
    ),
]

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

ACTIVE_RE = re.compile(
    r"\b(?:for sale|active|homeStatus[\"']?\s*[:=]\s*[\"']?FOR_SALE|listingStatus[\"']?\s*[:=]\s*[\"']?ACTIVE)\b",
    re.IGNORECASE,
)

INACTIVE_RE = re.compile(r"\b(?:sold|off market|contingent|pending)\b", re.IGNORECASE)

DISQUALIFY_PATTERNS = [
    re.compile(r"\bapproved\s+short\s+sale\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s+approved\b", re.IGNORECASE),
    re.compile(r"\balready\s+approved\b", re.IGNORECASE),
    re.compile(r"\blender\s+approved\b", re.IGNORECASE),
    re.compile(
        r"\b(?:already|currently)\s+(?:working|work)\s+with\s+(?:a\s+|an\s+|the\s+)?"
        r"(?:short\s+sale\s+)?(?:specialist|attorney|negotiator|processor)\b",
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
    r"(?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}"
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


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_key(value: str) -> str:
    value = html.unescape(value or "").lower()
    value = re.sub(r"\b(?:unit|apt|apartment|suite|ste|#)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_space(value)


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def address_key(address: str, city: str, state: str) -> str:
    parts = [normalize_key(address), normalize_key(city), normalize_key(state)]
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


def qualification_for_text(text: str) -> Qualification:
    text = html.unescape(text or "")
    compact = normalize_space(text)
    disqualified = []
    for pattern in DISQUALIFY_PATTERNS:
        match = pattern.search(compact)
        if match:
            disqualified.append(match.group(0))

    short_sale_match = SHORT_SALE_LISTING_RE.search(compact)
    active_match = ACTIVE_RE.search(compact)
    inactive_match = INACTIVE_RE.search(compact)

    if not short_sale_match:
        return Qualification(
            "rejected",
            "missing_listing_text_short_sale",
            "",
            "",
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
    if disqualified:
        return Qualification(
            "rejected",
            "disqualifying_short_sale_text",
            extract_short_sale_evidence_type(compact),
            excerpt_around(compact, verified_match.start(), verified_match.end()),
            "; ".join(disqualified),
        )
    if inactive_match and not active_match:
        return Qualification(
            "rejected",
            "not_active_for_sale",
            extract_short_sale_evidence_type(compact),
            excerpt_around(compact, verified_match.start(), verified_match.end()),
            inactive_match.group(0),
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


def fetch_url(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(2_000_000)
        encoding = resp.headers.get_content_charset() or "utf-8"
    return raw.decode(encoding, errors="ignore")


def strip_html(markup: str) -> str:
    markup = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", markup)
    markup = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", markup)
    markup = re.sub(r"(?is)<[^>]+>", " ", markup)
    return normalize_space(html.unescape(markup))


def ddg_search(query: str, source: str, limit: int) -> list[SearchResult]:
    url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query, "kl": "us-en"})
    body = fetch_url(url)
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
            description = obj.get("description")
            if isinstance(description, str):
                pieces.append(description)
    return "\n".join(pieces), fields


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
    page_text = strip_html(markup)
    combined = normalize_space(" ".join([result.title, result.snippet, json_text, page_text]))

    fields = dict(json_fields)
    title_parts = [part.strip() for part in re.split(r"\s*[|–-]\s*", result.title) if part.strip()]
    if title_parts and not fields.get("listing_address"):
        fields["listing_address"] = title_parts[0]

    fields.setdefault("source_url", result.url)
    fields.setdefault("agent_name", extract_labeled_value(combined, ["Listing Agent", "Listed by", "Agent"]))
    fields.setdefault("broker_name", extract_labeled_value(combined, ["Broker", "Brokerage", "Listing Office"]))

    phone_match = PHONE_RE.search(combined)
    email_match = EMAIL_RE.search(combined)
    fields.setdefault("phone", phone_match.group(0) if phone_match else "")
    fields.setdefault("email", email_match.group(0) if email_match else "")

    if not fields.get("city") or not fields.get("state"):
        city_state = re.search(r"\b([A-Z][A-Za-z .'-]{2,40}),\s*([A-Z]{2})\s*(\d{5})?\b", result.title + " " + result.snippet)
        if city_state:
            fields.setdefault("city", normalize_space(city_state.group(1)))
            fields.setdefault("state", city_state.group(2))
            fields.setdefault("zip", city_state.group(3) or "")

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
    import_ready = "yes" if qualification.status == "qualified" and not matched and not agent_rows and has_phone else "review"
    promotion_status = "pilot_review"
    promotion_notes = (
        "Net-new listing candidate; review then promote to PendingQueue using pending_queue_listing_json."
        if not matched and not agent_rows and has_phone
        else "No phone found; cannot prove this is a new agent for one-touch outreach."
        if not has_phone
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
    pilot_seen_phones = {normalize_phone(row[2]) for row in pilot_rows[1:] if len(row) > 2 and normalize_phone(row[2])}

    output_rows: list[list[str]] = []
    stats = {"searched": 0, "fetched": 0, "qualified": 0, "duplicates": 0, "rejected": 0}

    for state in args.states:
        state_query_term = STATE_QUERY_TERMS.get(state.upper(), state)
        for source, template in SOURCE_QUERIES:
            query = template.format(state=state_query_term)
            stats["searched"] += 1
            try:
                results = ddg_search(query, source, args.results_per_query)
            except Exception as exc:  # noqa: BLE001
                print(f"search_failed source={source} state={state} error={exc}")
                continue
            time.sleep(args.sleep_seconds)
            for result in results:
                if result.url in already_seen_urls:
                    continue
                try:
                    markup = fetch_url(result.url)
                    stats["fetched"] += 1
                except Exception as exc:  # noqa: BLE001
                    print(f"fetch_failed url={result.url} error={exc}")
                    continue
                candidate = infer_fields(result, markup)
                qualification = qualification_for_text(candidate.text)
                if qualification.status != "qualified":
                    stats["rejected"] += 1
                    if args.include_rejected:
                        output_rows.append(candidate_to_row(candidate, qualification, "", "", ""))
                    continue
                dup_status, dup_key, matched = duplicate_status(candidate, existing)
                if dup_status in {"duplicate_listing", "duplicate_agent_phone"}:
                    stats["duplicates"] += 1
                    continue
                phone_key = normalize_phone(candidate.fields.get("phone", ""))
                if phone_key and phone_key in pilot_seen_phones:
                    stats["duplicates"] += 1
                    continue
                agent_rows = matched if dup_status == "possible_existing_agent" else ""
                stats["qualified"] += 1
                output_rows.append(candidate_to_row(candidate, qualification, dup_key, "", agent_rows))
                already_seen_urls.add(result.url)
                if phone_key:
                    pilot_seen_phones.add(phone_key)
                time.sleep(args.sleep_seconds)

    if not args.dry_run:
        append_values(token, args.spreadsheet_id, f"{args.pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}", output_rows)

    print(json.dumps({"stats": stats, "rows_to_write": len(output_rows), "dry_run": args.dry_run}, indent=2))


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
