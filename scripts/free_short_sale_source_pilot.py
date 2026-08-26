#!/usr/bin/env python3
"""Free-source short sale listing discovery pilot.

This script is intentionally separate from the production Zillow verifier path.
It searches free public web results, keeps only net-new listings that pass the
strict short-sale rule, and writes review candidates to a pilot Google Sheet tab.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime as dt
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Iterable
from zoneinfo import ZoneInfo

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from sheet_safety import (  # noqa: E402
    safe_source_reference,
    sanitize_external_links_for_sheet,
    sanitize_payload_for_sheet_json,
)


SPREADSHEET_ID = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"
MAIN_TAB = "Sheet1"
PILOT_TAB = "Lead Source Pilot"
RUN_RECEIPT_TAB = os.getenv("FREE_SOURCE_PILOT_RUN_RECEIPT_TAB", "Pilot Run Receipts")
SOURCE_EVIDENCE_TAB = os.getenv(
    "FREE_SOURCE_PILOT_SOURCE_EVIDENCE_TAB",
    "Pilot Source Evidence",
)
POST_SOURCE_AUDIT_GRACE_MINUTES = int(
    os.getenv("FREE_SOURCE_PILOT_POST_SOURCE_AUDIT_GRACE_MINUTES", "30")
)


def validate_run_receipt_tab(main_tab: str, pilot_tab: str) -> None:
    """Fail closed before any Sheet mutation if receipts could touch lead data."""
    receipt_name = normalize_space(RUN_RECEIPT_TAB)
    protected_names = {
        normalize_space(main_tab).casefold(),
        normalize_space(pilot_tab).casefold(),
        "sheet1",
        "lead source pilot",
    }
    if not receipt_name or receipt_name.casefold() in protected_names:
        raise RuntimeError(
            "FREE_SOURCE_PILOT_RUN_RECEIPT_TAB must name a dedicated tab distinct "
            "from Sheet1 and Lead Source Pilot"
        )


def validate_source_evidence_tab(main_tab: str, pilot_tab: str) -> None:
    """Keep exact-source evidence isolated from lead and run-receipt rows."""
    evidence_name = normalize_space(SOURCE_EVIDENCE_TAB)
    protected_names = {
        normalize_space(main_tab).casefold(),
        normalize_space(pilot_tab).casefold(),
        normalize_space(RUN_RECEIPT_TAB).casefold(),
        "sheet1",
        "lead source pilot",
        "pilot run receipts",
    }
    if not evidence_name or evidence_name.casefold() in protected_names:
        raise RuntimeError(
            "FREE_SOURCE_PILOT_SOURCE_EVIDENCE_TAB must name a dedicated tab distinct "
            "from Sheet1, Lead Source Pilot, and Pilot Run Receipts"
        )
RUN_RECEIPT_HEADERS = [
    "schedule_slot_id",
    "run_receipt_id",
    "run_date",
    "run_mode",
    "status",
    "observed_at",
    "pipeline_complete",
    "detail",
]
SOURCE_EVIDENCE_HEADERS = [
    "receipt_id",
    "captured_at",
    "stable_id",
    "source_reference",
    "encoded_source_url",
    "listing_identity_group",
    "qualification_hash",
    "evidence_state",
]
MAX_SOURCE_QUERY_RECOVERY = 10
MAX_SOURCE_QUERY_RECOVERY_EXPERIMENT = 20
SOURCE_QUERY_RECOVERY_EXPERIMENT_START_DATE = "2026-08-23"
SOURCE_QUERY_RECOVERY_EXPERIMENT_DAYS = 7
RECOVERY_PENDING_PREFIX = "recovery_pending_v1="
RECOVERY_ATTEMPT_PREFIX = "recovery_attempt_v1="
RECOVERY_COMPLETED_PREFIX = "recovery_completed_v1="
RECOVERY_EXHAUSTED_PREFIX = "recovery_exhausted_v1="
RUN_RECEIPT_STALE_MINUTES = int(os.getenv("FREE_SOURCE_PILOT_RUN_RECEIPT_STALE_MINUTES", "70"))

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
        '"{state}" ("Special Listing Conditions: Short Sale" OR "Is Short Sale: Yes") '
        '("For Sale" OR "Pending" OR "Active") '
        '-site:zillow.com -site:trulia.com -site:realtor.com -site:redfin.com -site:homes.com',
    ),
    (
        "idx_broker_remarks",
        '"{state}" "short sale" '
        '("Public Remarks" OR "Property Description" OR "What\'s Special" OR "About This Home") '
        '("Active" OR "Pending" OR "Under Contract") '
        '-site:zillow.com -site:trulia.com -site:realtor.com -site:redfin.com -site:homes.com',
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
DEFAULT_SOURCE_BUCKETS = ("idx_broker_pages", "idx_broker_remarks")
CSE_DATE_RESTRICT = os.getenv("FREE_SOURCE_PILOT_DATE_RESTRICT", "d1").strip()
SOURCE_PLAN = os.getenv("FREE_SOURCE_PILOT_SOURCE_PLAN", "idx_permanent_90_10").strip().lower()
SHADOW_MODE = os.getenv("FREE_SOURCE_PILOT_SHADOW_MODE", "true").lower() == "true"
SHADOW_REVIEW_TARGET = max(1, int(os.getenv("FREE_SOURCE_PILOT_SHADOW_REVIEW_TARGET", "10")))
SHADOW_REVIEW_DAYS = max(1, int(os.getenv("FREE_SOURCE_PILOT_SHADOW_REVIEW_DAYS", "7")))
DEFAULT_DAILY_SOURCE_BUCKETS = ("idx_broker_pages", "idx_broker_remarks")
DEFAULT_ROTATING_SOURCE_BUCKETS = ("homes.com", "realtor.com", "redfin.com")
DEFAULT_ROTATION_ANCHOR_DATE = "2026-07-06"
ROTATION_TZ = os.getenv("FREE_SOURCE_PILOT_ROTATION_TZ", "America/New_York")
DAILY_DATE_RESTRICT = os.getenv("FREE_SOURCE_PILOT_DAILY_DATE_RESTRICT", "w1").strip()
ROTATING_DATE_RESTRICT = os.getenv("FREE_SOURCE_PILOT_ROTATING_DATE_RESTRICT", "w1").strip()
DIRECT_MONITOR_ENABLED = os.getenv("FREE_SOURCE_PILOT_DIRECT_MONITOR_ENABLED", "false").lower() == "true"
DIRECT_MONITOR_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_DIRECT_MONITOR_REBALANCE_START_DATE",
    "2026-08-07",
).strip()
DIRECT_MONITOR_DAYS = max(1, int(os.getenv("FREE_SOURCE_PILOT_DIRECT_MONITOR_DAYS", "7")))
DIRECT_MONITOR_MAX_URLS = min(
    50,
    max(1, int(os.getenv("FREE_SOURCE_PILOT_DIRECT_MONITOR_MAX_URLS", "50"))),
)
DIRECT_MONITOR_FAMILY_LIMITS = {
    "momentum": max(
        0,
        int(os.getenv("FREE_SOURCE_PILOT_DIRECT_MONITOR_MOMENTUM_URLS", "40")),
    ),
    "coldwell": max(
        0,
        int(os.getenv("FREE_SOURCE_PILOT_DIRECT_MONITOR_COLDWELL_URLS", "10")),
    ),
}
DIRECT_MONITOR_FEEDS = {
    "momentum": (
        "https://movewithmomentum.com/sitemap-idx-stellar-1.xml",
        "https://movewithmomentum.com/sitemap-idx-floridakeys.xml",
    ),
    "coldwell": (
        "https://www.coldwellbanker.com/xml-sitemap/states/sitemapindex-listings-new-day.xml",
    ),
}


@dataclass(frozen=True)
class SourceQuery:
    source: str
    template: str
    date_restrict: str


@dataclass(frozen=True)
class SearchPlanEntry:
    state: str
    source_query: SourceQuery
    result_start: int = 1


def configured_bucket_names(env_name: str, fallback: tuple[str, ...]) -> list[str]:
    raw = os.getenv(env_name, ",".join(fallback))
    buckets = []
    seen = set()
    for bucket in raw.split(","):
        source = bucket.strip()
        if not source or source in seen or source not in ALL_SOURCE_QUERY_MAP:
            continue
        buckets.append(source)
        seen.add(source)
    if not buckets:
        buckets = [source for source in fallback if source in ALL_SOURCE_QUERY_MAP]
    return buckets


def parse_run_date(value: str | None) -> dt.date:
    if value:
        return dt.date.fromisoformat(value)
    try:
        return dt.datetime.now(ZoneInfo(ROTATION_TZ)).date()
    except Exception:
        return dt.datetime.now().date()


def rotating_source_for_date(run_date: dt.date, sources: list[str]) -> str:
    if not sources:
        return ""
    anchor = parse_run_date(os.getenv("FREE_SOURCE_PILOT_ROTATION_ANCHOR_DATE", DEFAULT_ROTATION_ANCHOR_DATE))
    return sources[(run_date - anchor).days % len(sources)]


def configured_source_queries(run_date: dt.date | None = None) -> list[SourceQuery]:
    if SOURCE_PLAN == "idx_permanent_90_10":
        return [
            SourceQuery("idx_broker_remarks", ALL_SOURCE_QUERY_MAP["idx_broker_remarks"], DAILY_DATE_RESTRICT),
            SourceQuery("idx_broker_pages", ALL_SOURCE_QUERY_MAP["idx_broker_pages"], DAILY_DATE_RESTRICT),
        ]

    if SOURCE_PLAN in {"idx_dual_shadow", "idx_dual_daily"}:
        daily_sources = configured_bucket_names(
            "FREE_SOURCE_PILOT_DAILY_SOURCE_BUCKETS",
            DEFAULT_DAILY_SOURCE_BUCKETS,
        )
        return [
            SourceQuery(source, ALL_SOURCE_QUERY_MAP[source], DAILY_DATE_RESTRICT)
            for source in daily_sources
        ]

    if SOURCE_PLAN in {
        "idx_daily_rotating_weekly",
        "daily_idx_rotating_weekly",
        "homes_daily_rotating_weekly",
        "daily_homes_rotating_weekly",
    }:
        resolved_run_date = run_date or parse_run_date(os.getenv("FREE_SOURCE_PILOT_RUN_DATE"))
        daily_sources = configured_bucket_names("FREE_SOURCE_PILOT_DAILY_SOURCE_BUCKETS", ("idx_broker_pages",))
        rotating_sources = configured_bucket_names("FREE_SOURCE_PILOT_ROTATING_SOURCE_BUCKETS", DEFAULT_ROTATING_SOURCE_BUCKETS)
        selected: list[SourceQuery] = []
        seen = set()
        for source in daily_sources:
            selected.append(SourceQuery(source, ALL_SOURCE_QUERY_MAP[source], DAILY_DATE_RESTRICT))
            seen.add(source)
        first_rotating = rotating_source_for_date(resolved_run_date, rotating_sources)
        if first_rotating:
            start_index = rotating_sources.index(first_rotating)
            for offset in range(len(rotating_sources)):
                source = rotating_sources[(start_index + offset) % len(rotating_sources)]
                if source not in seen:
                    selected.append(SourceQuery(source, ALL_SOURCE_QUERY_MAP[source], ROTATING_DATE_RESTRICT))
                    break
        return selected

    buckets = configured_bucket_names("FREE_SOURCE_PILOT_SOURCE_BUCKETS", DEFAULT_SOURCE_BUCKETS)
    return [SourceQuery(source, ALL_SOURCE_QUERY_MAP[source], CSE_DATE_RESTRICT) for source in buckets]


def permanent_baseline_states(states: list[str], run_date: dt.date) -> list[str]:
    ordered_states = list(dict.fromkeys(normalize_space(state).upper() for state in states if normalize_space(state)))
    if not ordered_states:
        return []
    daily_count = max(1, (len(ordered_states) + 4) // 5)
    anchor = parse_run_date(os.getenv("FREE_SOURCE_PILOT_ROTATION_ANCHOR_DATE", DEFAULT_ROTATION_ANCHOR_DATE))
    start = ((run_date - anchor).days % 5) * daily_count
    return [ordered_states[(start + offset) % len(ordered_states)] for offset in range(daily_count)]


def configured_search_plan(
    states: list[str],
    run_date: dt.date,
    source_queries: list[SourceQuery] | None = None,
) -> list[SearchPlanEntry]:
    queries = source_queries if source_queries is not None else configured_source_queries(run_date)
    if SOURCE_PLAN != "idx_permanent_90_10":
        return [
            SearchPlanEntry(state, source_query)
            for state in states
            for source_query in queries
        ]

    by_source = {query.source: query for query in queries}
    remarks = by_source.get("idx_broker_remarks")
    pages = by_source.get("idx_broker_pages")
    if not remarks or not pages:
        return [
            SearchPlanEntry(state, source_query)
            for state in states
            for source_query in queries
        ]

    baseline_states = set(permanent_baseline_states(states, run_date))
    plan: list[SearchPlanEntry] = []
    for state in states:
        plan.append(SearchPlanEntry(state, remarks, 1))
        if normalize_space(state).upper() in baseline_states:
            plan.append(SearchPlanEntry(state, pages, 1))
        else:
            plan.append(SearchPlanEntry(state, remarks, 11))
    return plan


SOURCE_QUERIES = configured_source_queries()

SEARCH_ENGINE = os.getenv("FREE_SOURCE_PILOT_SEARCH_ENGINE", "auto").lower()
CSE_API_KEY = os.getenv("CS_API_KEY") or os.getenv("GOOGLE_API_KEY")
CSE_CX = os.getenv("CS_CX") or os.getenv("GOOGLE_CX")
ALLOW_DDG_FALLBACK = os.getenv("FREE_SOURCE_PILOT_ALLOW_DDG_FALLBACK", "false").lower() == "true"
CONTACT_RESEARCH_RESULTS = int(os.getenv("FREE_SOURCE_PILOT_CONTACT_RESEARCH_RESULTS", "3"))
PROMOTION_ENABLED = os.getenv("FREE_SOURCE_PILOT_PROMOTION_ENABLED", "false").lower() == "true"
PROMOTION_DAILY_CAP = max(0, int(os.getenv("FREE_SOURCE_PILOT_PROMOTION_DAILY_CAP", "10")))
PROMOTION_DRY_RUN = os.getenv("FREE_SOURCE_PILOT_PROMOTION_DRY_RUN", "false").lower() == "true"
AGENT_SHADOW_CONSENSUS_CAP = max(
    0,
    int(os.getenv("FREE_SOURCE_PILOT_AGENT_SHADOW_CONSENSUS_CAP", "10")),
)
LINK_AUDIT_START_DATE = os.getenv("FREE_SOURCE_PILOT_LINK_AUDIT_START_DATE", "2026-08-01").strip()
LINK_AUDIT_DAYS = max(1, int(os.getenv("FREE_SOURCE_PILOT_LINK_AUDIT_DAYS", "3")))
BROKERAGE_SUFFIX_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_BROKERAGE_SUFFIX_SHADOW_START_DATE",
    "2026-08-01",
).strip()
BROKERAGE_SUFFIX_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_BROKERAGE_SUFFIX_SHADOW_DAYS", "7")),
)
AGENT_ADDRESS_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_AGENT_ADDRESS_SHADOW_START_DATE",
    "2026-08-12",
).strip()
AGENT_ADDRESS_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_AGENT_ADDRESS_SHADOW_DAYS", "7")),
)
AGENT_ADDRESS_SHADOW_MAX_CANDIDATES = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_AGENT_ADDRESS_SHADOW_MAX_CANDIDATES", "10")),
)
QUERY_EXCLUSION_EXPERIMENT_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_QUERY_EXCLUSION_START_DATE",
    "2026-08-15",
).strip()
QUERY_EXCLUSION_EXPERIMENT_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_QUERY_EXCLUSION_DAYS", "7")),
)
QUERY_EXCLUSION_BASELINE_PER_BUCKET = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_QUERY_EXCLUSION_BASELINE_PER_BUCKET", "5")),
)
QUERY_EXCLUSION_DOMAINS = tuple(
    domain.strip().lower()
    for domain in os.getenv(
        "FREE_SOURCE_PILOT_QUERY_EXCLUSION_DOMAINS",
        "edinarealty.com,ikeyrealty.com",
    ).split(",")
    if domain.strip()
)
CANONICAL_ID_AUDIT_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_CANONICAL_ID_AUDIT_START_DATE",
    "2026-08-14",
).strip()
CANONICAL_ID_AUDIT_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_CANONICAL_ID_AUDIT_DAYS", "7")),
)
CANONICAL_ID_AUDIT_MAX_CANDIDATES = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_CANONICAL_ID_AUDIT_MAX_CANDIDATES", "10")),
)
CANONICAL_VERIFIER_EVIDENCE_HEADER = "contact_verification_note"
SOURCE_DURABILITY_AUDIT_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_START_DATE",
    "2026-08-20",
).strip()
SOURCE_DURABILITY_AUDIT_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_DAYS", "14")),
)
SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES", "10")),
)
SOURCE_DURABILITY_AUDIT_MIN_REVIEWABLE = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_MIN_REVIEWABLE", "9")),
)
SOURCE_DURABILITY_AUDIT_MIN_AGE_HOURS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_MIN_AGE_HOURS", "24")),
)
SOURCE_DURABILITY_AUDIT_STATE_PATH = os.getenv(
    "FREE_SOURCE_PILOT_SOURCE_DURABILITY_AUDIT_STATE_PATH",
    "/tmp/free_source_pilot_source_durability_audit.json",
).strip()
ROUTE_ALIAS_DEDUPE_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_ROUTE_ALIAS_DEDUPE_SHADOW_START_DATE",
    "2026-08-17",
).strip()
ROUTE_ALIAS_DEDUPE_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_ROUTE_ALIAS_DEDUPE_SHADOW_DAYS", "7")),
)
ROUTE_ALIAS_DEDUPE_SHADOW_MAX_CANDIDATES = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_ROUTE_ALIAS_DEDUPE_SHADOW_MAX_CANDIDATES", "50")),
)
DELIVERY_RECEIPT_AUDIT_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_DELIVERY_RECEIPT_AUDIT_START_DATE",
    "2026-08-14",
).strip()
DELIVERY_RECEIPT_AUDIT_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_DELIVERY_RECEIPT_AUDIT_DAYS", "7")),
)
DELIVERY_RECEIPT_MATURE_TARGET = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_DELIVERY_RECEIPT_MATURE_TARGET", "6")),
)
DELIVERY_RECEIPT_NEW_TARGET = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_DELIVERY_RECEIPT_NEW_TARGET", "4")),
)
QUALIFICATION_PRECEDENCE_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_QUALIFICATION_PRECEDENCE_SHADOW_START_DATE",
    "2026-08-02",
).strip()
QUALIFICATION_PRECEDENCE_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_QUALIFICATION_PRECEDENCE_SHADOW_DAYS", "7")),
)
DESCRIPTION_BLOCK_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_DESCRIPTION_BLOCK_SHADOW_START_DATE",
    "2026-08-04",
).strip()
DESCRIPTION_BLOCK_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_DESCRIPTION_BLOCK_SHADOW_DAYS", "7")),
)
DESCRIPTION_BLOCK_SHADOW_MAX_PER_RUN = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_DESCRIPTION_BLOCK_SHADOW_MAX_PER_RUN", "100")),
)
SITE_CHROME_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_SITE_CHROME_SHADOW_START_DATE",
    "2026-08-21",
).strip()
SITE_CHROME_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SITE_CHROME_SHADOW_DAYS", "7")),
)
SITE_CHROME_SHADOW_MAX_PER_RUN = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_SITE_CHROME_SHADOW_MAX_PER_RUN", "10")),
)
SITE_CHROME_SHADOW_DOMAINS = {
    domain.strip().lower()
    for domain in os.getenv(
        "FREE_SOURCE_PILOT_SITE_CHROME_SHADOW_DOMAINS",
        "bishopcountry.com",
    ).split(",")
    if domain.strip()
}
COMPOUND_NEGATIVE_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_COMPOUND_NEGATIVE_SHADOW_START_DATE",
    "2026-08-09",
).strip()
COMPOUND_NEGATIVE_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_COMPOUND_NEGATIVE_SHADOW_DAYS", "7")),
)
COMPOUND_NEGATIVE_SHADOW_MAX_PER_RUN = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_COMPOUND_NEGATIVE_SHADOW_MAX_PER_RUN", "100")),
)
FUTURE_NEGOTIATOR_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_FUTURE_NEGOTIATOR_SHADOW_START_DATE",
    "2026-08-06",
).strip()
FUTURE_NEGOTIATOR_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_FUTURE_NEGOTIATOR_SHADOW_DAYS", "7")),
)
FOLLOWUP_HOLD_SHADOW_START_DATE = os.getenv(
    "FREE_SOURCE_PILOT_FOLLOWUP_HOLD_SHADOW_START_DATE",
    "2026-08-06",
).strip()
FOLLOWUP_HOLD_SHADOW_DAYS = max(
    1,
    int(os.getenv("FREE_SOURCE_PILOT_FOLLOWUP_HOLD_SHADOW_DAYS", "7")),
)
HEADLESS_FALLBACK = os.getenv("FREE_SOURCE_PILOT_HEADLESS_FALLBACK", "true").lower() == "true"
HEADLESS_BUDGET = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_BUDGET", "12")))
HEADLESS_DOMAIN_BUDGET = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_DOMAIN_BUDGET", "4")))
HEADLESS_NAV_TIMEOUT_MS = max(1000, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_NAV_TIMEOUT_MS", "12000")))
HEADLESS_WAIT_MS = max(0, int(os.getenv("FREE_SOURCE_PILOT_HEADLESS_WAIT_MS", "900")))
HEADLESS_DOMAINS = {
    domain.strip().lower()
    for domain in os.getenv("FREE_SOURCE_PILOT_HEADLESS_DOMAINS", "*").split(",")
    if domain.strip()
}
_headless_used_total = 0
_headless_used_by_domain: dict[str, int] = {}
_site_chrome_prewrite_seen: set[str] = set()
_site_chrome_prewrite_count = 0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

SHORT_SALE_LISTING_RE = re.compile(
    r"\b(?:short\s+sale|short-sale)\b",
    re.IGNORECASE,
)

DESCRIPTION_EVIDENCE_LABEL_RE = re.compile(
    r"\b(?:"
    r"what'?s\s+special|description|remarks|public\s+remarks|"
    r"about\s+this\s+home|property\s+description|listing\s+description|"
    r"property\s+overview|overview"
    r")\b",
    re.IGNORECASE,
)

STRICT_DESCRIPTION_EVIDENCE_LABEL_RE = re.compile(
    r"\b(?:"
    r"what'?s\s+special|description|remarks|public\s+remarks|"
    r"about\s+this\s+home|property\s+description|listing\s+description"
    r")\b",
    re.IGNORECASE,
)

DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE = re.compile(
    r"(?:legal|meta|seo|image|special\s+conditions?|special\s+listing\s+conditions?|potential\s+short\s+sale)\s*$",
    re.IGNORECASE,
)

DESCRIPTION_SECTION_STOP_RE = re.compile(
    r"\b(?:"
    r"location|school\s+information|community|heating(?:\s*&\s*cooling)?|utilities|"
    r"financial\s+considerations|disclosures(?:\s+and\s+reports)?|interior|exterior|"
    r"features|parking|lot\s+features|listing\s+agent|home\s+details|property\s+details|"
    r"listing\s+details|quickly\s+find|map|facts\s*&\s*features|tax\s+info|"
    r"special\s+(?:listing\s+)?conditions?|short\s+sale\s+status|is\s+short\s+sale|"
    r"financial\s+status|contract\s+information|"
    r"condo/co-op/association|mls\s+data"
    r")\b",
    re.IGNORECASE,
)

DESCRIPTION_BLOCK_NAVIGATION_STOP_RE = re.compile(
    r"\b(?:skip\s+to\s+content|home\s+advanced[- ]search|login\s+contact|search\s+sell\s+agents)\b",
    re.IGNORECASE,
)

DESCRIPTION_BLOCK_TAXONOMY_NOISE_RE = re.compile(
    r"\b(?:amenities?|foreclosure\s+property|home\s+advanced[- ]search)\b"
    r".{0,180}\bshort\s+sale\b.{0,140}"
    r"\b(?:new\s+construction|featured\s+listing|buy\s+a\s+house|get\s+prequalified|lease\s+to\s+own)\b",
    re.IGNORECASE,
)

SITE_CHROME_SHORT_SALE_CARD_RE = re.compile(
    r"\bproperty\s+description\b\s*(?:\.{3}|\u2026)\s*"
    r"(?P<label>\bshort\s+sale\b)\s*[.!]?\s*\$\s*[\d,]+",
    re.IGNORECASE,
)

SITE_CHROME_SHORT_SALE_NAVIGATION_RE = re.compile(
    r"\b(?:financing\s+options?\s+)?(?P<label>short[-\s]+sale\s+options?)"
    r"(?:\s*[|:;,.\-/–—\u2022]\s*|\s+)"
    r"(?:select(?:ing)?|choos(?:e|ing))\s+(?:your\s+)?(?:an?\s+)?(?:real\s+estate\s+)?agent\b",
    re.IGNORECASE,
)

SHORT_SALE_NEGATION_RE = re.compile(
    r"\b(?:not|never|no|no\s+longer|isn['’]?t|is\s+not)\s+(?:an?\s+)?short[-\s]+sale\b|"
    r"\bshort[-\s]+sale\s*[:=-]?\s*(?:no|false)\b|"
    r"\b(?:will|would|does|do|did)?\s*not\s+(?:be\s+|consider\s+|pursue\s+|accept\s+|allow\s+|seek\s+)?(?:an?\s+)?short[-\s]+sale\b|"
    r"\bshort[-\s]+sale\s+(?:is|was|will\s+be)?\s*not\s+(?:applicable|available|eligible|possible|considered|an?\s+option)\b",
    re.IGNORECASE,
)

TRUSTED_LISTING_DESCRIPTION_SOURCES = {
    "jsonld_listing_object",
    "visible_listing_description",
}

FUTURE_NEGOTIATOR_INVOLVEMENT_RE = re.compile(
    r"\b(?:in\s+the\s+process\s+of\s+)?being\s+assigned\s+"
    r"(?:an?\s+)?(?:bank\s+|lender\s+)?negotiator\b|"
    r"\b(?:bank\s+|lender\s+)?negotiator\s+(?:is\s+being|will\s+be)\s+assigned\b|"
    r"\bwill\s+(?:be\s+)?assign(?:ed)?\s+(?:an?\s+)?(?:bank\s+|lender\s+)?negotiator\b",
    re.IGNORECASE,
)

CURRENT_MARKET_STATUS_RE = re.compile(
    r"\b(?:"
    r"(?:source\s+listing\s+status|listing\s+status|mls\s+status|status)\s*[:#-]?\s*"
    r"(?:active(?:\s+under\s+contract)?|pending(?:\s+lender\s+approval)?|under\s+contract|for\s+sale)\b|"
    r"STATUS\s+(?:Active(?:\s+Under\s+Contract)?|Pending(?:\s+Lender\s+Approval)?|Under\s+Contract|For\s+Sale)\b|"
    r"Share\s+Active\b|"
    r"\bFor\s+Sale\b|"
    r"currently\s+listed\s+for\s+sale\b|"
    r"homeStatus[\"']?\s*[:=]\s*[\"']?FOR_SALE[\"']?|"
    r"listingStatus[\"']?\s*[:=]\s*[\"']?(?:ACTIVE|PENDING|UNDER_CONTRACT|FOR_SALE)[\"']?|"
    r"MLS#\s*\d+.{0,120}\bActive\b"
    r")\b",
    re.IGNORECASE,
)

COMING_SOON_STATUS_RE = re.compile(
    r"\b(?:"
    r"(?:source\s+listing\s+status|listing\s+status|mls\s+status|status)\s*[:#-]?\s*coming\s+soon|"
    r"listingStatus[\"']?\s*[:=]\s*[\"']?COMING_SOON[\"']?|"
    r"is_coming_soon[\"']?\s*[:=]\s*(?:true|1)"
    r")\b",
    re.IGNORECASE,
)

UNSUPPORTED_LISTING_STATUS_RE = re.compile(
    r"\b(?:"
    r"(?:source\s+listing\s+status|listing\s+status|mls\s+status|status)\s*[:#-]?\s*"
    r"(?:contingent|under\s+agreement)|"
    r"listingStatus[\"']?\s*[:=]\s*[\"']?(?:CONTINGENT|UNDER_AGREEMENT)[\"']?"
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

STRUCTURED_SHORT_SALE_NEGATIVE_PATTERNS = [
    re.compile(r"\bis\s+short\s+sale\s*\??\s*[:=]?\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s+status\s*\??\s*[:=]?\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s*\??\s*[:=]\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\b(?:potential\s+)?short\s+sale\s*\??\s*[:=]?\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\b(?:financial\s+status|contract\s+information|special\s+listing\s+conditions?)\s*[-:]?\s*(?:potential\s+)?short\s+sale\s*\??\s*[:=]?\s*(?:no|false)\b", re.IGNORECASE),
    re.compile(r"\bisShortSale[\"']?\s*[:=]\s*[\"']?false[\"']?\b", re.IGNORECASE),
]
COMPOUND_SHORT_SALE_NEGATIVE_RE = re.compile(
    r"\b(?P<label>"
    r"(?:(?:foreclosure|pre-foreclosure)\s*/\s*)?(?:potential\s+)?short\s+sale(?:\s+status)?|"
    r"is\s+short\s+sale"
    r")\s*\??\s*[:=]\s*(?P<value>no|false)\b",
    re.IGNORECASE,
)

DISQUALIFY_PATTERNS = [
    re.compile(r"\bapproved\s+short\s+sale\b", re.IGNORECASE),
    re.compile(r"\bshort\s+sale\s+approved\b", re.IGNORECASE),
    re.compile(
        r"\bshort\s+sale\b.{0,80}\bapproved\s+at\s+(?:the\s+)?(?:current\s+)?list(?:ing)?\s+price\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bapproved\s+at\s+(?:the\s+)?(?:current\s+)?list(?:ing)?\s+price\b.{0,80}\bshort\s+sale\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bshort\s+sale\b.{0,80}\bapproved\s+price\b", re.IGNORECASE),
    re.compile(r"\bapproved\s+price\b.{0,80}\bshort\s+sale\b", re.IGNORECASE),
    re.compile(
        r"\bshort\s+sale\b.{0,80}\b(?:already\s+approved|lender\s+approved)\b|"
        r"\b(?:already\s+approved|lender\s+approved)\b.{0,80}\bshort\s+sale\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:loss\s+mitigation|short\s+sale|negotiator|negotiation|processing|processor|"
        r"attorney|specialist|representation)\s+fee\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(?:short\s+sale\s+)?(?:negotiator|negotiation)\s+fee\b", re.IGNORECASE),
    re.compile(
        r"\b(?:buyer|purchaser)\s+to\s+pay\b.{0,80}"
        r"\b(?:short\s+sale\s+)?(?:negotiator|negotiation)\s+fee\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bprofessional\s+third[-\s]?party\s+negotiation\b.{0,80}"
        r"\b(?:already|underway|under\s+way|in\s+process)\b",
        re.IGNORECASE,
    ),
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
PROMOTION_AGENT_BLOCKLIST = {
    "brokerage",
    "brokerages",
    "company",
    "corp",
    "coldwell",
    "enrg",
    "exp",
    "firm",
    "group",
    "homes",
    "inc",
    "keller",
    "llc",
    "mls",
    "network",
    "office",
    "provided",
    "properties",
    "real",
    "realty",
    "realtor",
    "realtors",
    "red",
    "stellar",
    "team",
    "williams",
}
PROMOTION_AGENT_STREET_TOKENS = {
    "avenue",
    "boulevard",
    "circle",
    "court",
    "drive",
    "highway",
    "lane",
    "loop",
    "parkway",
    "place",
    "road",
    "route",
    "street",
    "terrace",
    "trail",
    "way",
}
TRUSTED_AGENT_LABEL_CONTEXT_RE = re.compile(
    r"\b(?:listing\s+agent(?:s|\(s\))?|list\s+agent|listed\s+by)\s*[:\-]?\s*(.{2,180})",
    re.IGNORECASE,
)
SHADOW_AGENT_LABEL_PATTERNS = (
    (
        "listing_agent",
        re.compile(r"\b(?:listing\s+agent(?:s|\(s\))?|list\s+agent)\s*[:\-]?\s*(.{2,180})", re.I),
    ),
    ("listed_by", re.compile(r"\blisted\s+by\s*[:\-]?\s*(.{2,180})", re.I)),
    (
        "listing_courtesy",
        re.compile(r"\b(?:listing\s+courtesy\s+of|courtesy\s+of)\s*[:\-]?\s*(.{2,180})", re.I),
    ),
    (
        "listing_provided_by",
        re.compile(r"\blisting\s+provided\s+by\s*[:\-]?\s*(.{2,180})", re.I),
    ),
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
    street_state_keys: dict[str, int]
    phone_keys: dict[str, int]
    agent_keys: dict[str, list[int]]
    agent_name_keys: dict[str, list[int]]
    listing_records: list[dict[str, Any]]


_active_run_event_context: dict[str, Any] = {}


def set_run_event_context(**fields: Any) -> None:
    _active_run_event_context.clear()
    _active_run_event_context.update({key: value for key, value in fields.items() if value not in (None, "")})


def clear_run_event_context() -> None:
    _active_run_event_context.clear()


def log_event(event: str, **fields: Any) -> None:
    payload = {
        **_active_run_event_context,
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


_search_engine_attempt_stats: dict[str, int] = {
    "attempted": 0,
    "succeeded": 0,
    "blocked": 0,
    "failed": 0,
}


def reset_search_engine_attempt_stats() -> None:
    for key in _search_engine_attempt_stats:
        _search_engine_attempt_stats[key] = 0


def source_error_class(error: BaseException | str) -> str:
    text = str(error)
    if re.search(
        r"(?:\b(?:401|403|429|451)\b|forbidden|unauthori[sz]ed|access.?denied|"
        r"rate.?limit|quota|resource.?exhausted|daily.?limit)",
        text,
        re.I,
    ):
        return "blocked"
    return "failed"


def source_failure_reason(error: BaseException | str) -> str:
    text = str(error)
    match = re.search(r"\b(?:HTTP(?:\s+Error)?\s*)?(401|403|404|429|451|500|502|503|504)\b", text, re.I)
    if match:
        return f"http_{match.group(1)}"
    if re.search(r"timed?\s*out|timeout", text, re.I):
        return "timeout"
    if re.search(r"quota|resource.?exhausted|daily.?limit", text, re.I):
        return "quota"
    if re.search(r"rate.?limit", text, re.I):
        return "rate_limit"
    return type(error).__name__.lower() if isinstance(error, BaseException) else "unknown"


def increment_reason(stats: dict[str, Any], bucket: str, reason: str) -> None:
    reasons = stats.setdefault(bucket, {})
    reasons[reason or "unknown"] = int(reasons.get(reason or "unknown", 0)) + 1


TRACKING_QUERY_KEYS = {
    "fbclid", "gclid", "msclkid", "dclid", "_ga", "mc_cid", "mc_eid",
}


def canonical_public_listing_url(url: str) -> str:
    if not re.match(r"^https?://", normalize_space(url), re.I):
        return ""
    parsed = urllib.parse.urlsplit(url)
    query_pairs = [
        (key, value)
        for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_QUERY_KEYS
    ]
    query = urllib.parse.urlencode(sorted(query_pairs))
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), query, "")
    )


def result_url_identity(url: str) -> str:
    return canonical_public_listing_url(url)


def public_source_url_for_receipt(url: str) -> str:
    return canonical_public_listing_url(url)


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


def street_state_key(address: str, state: str) -> str:
    street = normalize_key(clean_listing_address(address, state=state))
    state_key = normalize_key(state)
    if not street or not state_key:
        return ""
    return f"{street}|{state_key}"


CANONICAL_STREET_SUFFIXES = {
    "avenue", "street", "road", "drive", "lane", "boulevard", "court",
    "circle", "way", "place", "loop", "trail", "parkway", "terrace",
    "highway", "route", "pass", "path", "point", "run", "row", "mews",
}
ADDRESS_UNIT_RE = re.compile(
    r"(?i)(?:\s*,?\s*)(?:#|unit\s*#?|apt(?:artment)?\s*#?|suite\s*#?|ste\s*#?)\s*([a-z0-9-]+)\b"
)


def canonical_address_identity(address: str, state: str) -> dict[str, str]:
    """Return a suffix-tolerant, unit-aware listing address identity."""
    cleaned = clean_listing_address(address, state=state)
    unit_matches = list(ADDRESS_UNIT_RE.finditer(cleaned))
    unit = normalize_key(unit_matches[-1].group(1)) if unit_matches else ""
    street = ADDRESS_UNIT_RE.sub(" ", cleaned)
    street_key = normalize_key(street)
    tokens = street_key.split()
    relaxed_tokens = tokens[:-1] if tokens and tokens[-1] in CANONICAL_STREET_SUFFIXES else tokens
    relaxed_street = " ".join(relaxed_tokens)
    state_key = normalize_key(state)
    base_key = f"{relaxed_street}|{state_key}" if relaxed_street and state_key else ""
    listing_key = f"{base_key}|unit:{unit or '-'}" if base_key else ""
    return {
        "street": street_key,
        "relaxed_street": relaxed_street,
        "state": state_key,
        "unit": unit,
        "base_key": base_key,
        "listing_key": listing_key,
    }


def canonical_listing_address_key(address: str, state: str) -> str:
    return canonical_address_identity(address, state).get("listing_key", "")


def listing_id_namespace(value: str) -> str:
    compact = normalize_space(value)
    if not compact:
        return ""
    if PILOT_ID_RE.fullmatch(compact):
        return "pilot"
    if compact.isdigit():
        return "zillow"
    return "source"


def listing_identity_record(
    *,
    row: int,
    address: str,
    state: str,
    stable_id: str = "",
    canonical_id: str = "",
) -> dict[str, Any]:
    identity = canonical_address_identity(address, state)
    namespace = listing_id_namespace(stable_id)
    return {
        "row": row,
        **identity,
        "stable_id": normalize_space(stable_id),
        "stable_namespace": namespace,
        "canonical_id": normalize_space(canonical_id).upper(),
        "attribution": "pilot" if namespace == "pilot" else ("zillow" if namespace == "zillow" else "sheet1"),
    }


def stable_synthetic_zpid(source: str, url: str, address: str, city: str, state: str) -> str:
    raw = canonical_listing_address_key(address, state)
    if not raw:
        raw = "|".join([normalize_key(source), normalize_key(url), address_key(address, city, state)])
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


def agent_name_key(agent: str) -> str:
    return normalize_key(clean_agent_name(agent))


def split_agent_name(full_name: str) -> tuple[str, str]:
    parts = normalize_space(full_name).split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def current_listing_status(text: str) -> tuple[str, str]:
    compact = normalize_space(html.unescape(text or ""))
    non_current_match = NON_CURRENT_STATUS_RE.search(compact)
    current_match = CURRENT_MARKET_STATUS_RE.search(compact)
    if non_current_match and current_match:
        evidence = f"{current_match.group(0)}; {non_current_match.group(0)}"
        return "conflicting", evidence[:220]
    if non_current_match:
        return "not_current", non_current_match.group(0)
    coming_soon_match = COMING_SOON_STATUS_RE.search(compact)
    if coming_soon_match:
        return "coming_soon", coming_soon_match.group(0)
    unsupported_match = UNSUPPORTED_LISTING_STATUS_RE.search(compact)
    if unsupported_match:
        return "unsupported", unsupported_match.group(0)
    if current_match:
        return "current", current_match.group(0)
    return "unknown", ""


def listing_status_failure_reason(status: str) -> str:
    if status == "not_current":
        return "not_current_listing"
    if status == "coming_soon":
        return "coming_soon_status_hold"
    if status == "conflicting":
        return "conflicting_status_hold"
    if status == "unsupported":
        return "unsupported_listing_status"
    return "missing_current_listing_status"


def qualification_for_text(text: str) -> Qualification:
    text = html.unescape(text or "")
    compact = normalize_space(text)
    verified_match = verified_short_sale_match(compact)
    disqualified = []
    # An explicit statement in the listing agent's remarks is authoritative.
    # Structured fields are often left at their default value, so a conflicting
    # "No"/false field cannot overrule those remarks. Business disqualifiers
    # such as approved price or an assigned negotiator still apply.
    disqualify_patterns = list(DISQUALIFY_PATTERNS)
    if not verified_match:
        disqualify_patterns = (
            list(STRUCTURED_SHORT_SALE_NEGATIVE_PATTERNS) + disqualify_patterns
        )
    for pattern in disqualify_patterns:
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
            listing_status_failure_reason(listing_status),
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


def qualification_for_candidate(candidate: Candidate) -> Qualification:
    """Qualify only evidence scoped to the exact fetched listing."""
    fields = candidate.fields
    description = normalize_space(fields.get("listing_description", ""))
    description_source = fields.get("listing_description_source", "")
    exact_listing_confirmed = fields.get("exact_listing_confirmed", "").lower() == "true"
    if not exact_listing_confirmed:
        return Qualification("rejected", "exact_listing_not_confirmed", "", "", "")
    if description_source not in TRUSTED_LISTING_DESCRIPTION_SOURCES or not description:
        return Qualification("rejected", "needs_description_confirmation", "", "", "")

    identity_group = fields.get("listing_identity_group", "")
    description_group = fields.get("listing_description_group", "")
    status_group = fields.get("scoped_listing_status_group", "")
    if not identity_group or len({identity_group, description_group, status_group}) != 1:
        return Qualification("rejected", "listing_evidence_not_bound", "", "", "")

    navigation_match = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.search(description)
    description_without_navigation = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.sub(" ", description)
    if navigation_match and not SHORT_SALE_LISTING_RE.search(description_without_navigation):
        return Qualification(
            "rejected",
            "short_sale_not_in_listing_evidence",
            "",
            excerpt_around(description, navigation_match.start("label"), navigation_match.end("label")),
            "site_chrome_short_sale_navigation_only",
        )

    short_sale_match = SHORT_SALE_LISTING_RE.search(description)
    if not short_sale_match:
        return Qualification("rejected", "missing_listing_text_short_sale", "", "", "")
    negation_match = SHORT_SALE_NEGATION_RE.search(description)
    if negation_match:
        return Qualification(
            "rejected",
            "disqualifying_short_sale_text",
            "listing_description_or_remarks",
            excerpt_around(description, short_sale_match.start(), short_sale_match.end()),
            negation_match.group(0),
        )
    disqualified = []
    for pattern in DISQUALIFY_PATTERNS:
        match = pattern.search(description)
        if match:
            disqualified.append(match.group(0))
    if disqualified:
        return Qualification(
            "rejected",
            "disqualifying_short_sale_text",
            "listing_description_or_remarks",
            excerpt_around(description, short_sale_match.start(), short_sale_match.end()),
            "; ".join(disqualified),
        )

    listing_status = fields.get("scoped_listing_status", "unknown")
    listing_status_evidence = fields.get("scoped_listing_status_evidence", "")
    if listing_status != "current":
        return Qualification(
            "rejected",
            listing_status_failure_reason(listing_status),
            "listing_description_or_remarks",
            excerpt_around(description, short_sale_match.start(), short_sale_match.end()),
            listing_status_evidence,
        )
    return Qualification(
        "qualified",
        "",
        "listing_description_or_remarks",
        excerpt_around(description, short_sale_match.start(), short_sale_match.end()),
        "",
    )


def verified_short_sale_match(text: str) -> re.Match[str] | None:
    short_sale_match = SHORT_SALE_LISTING_RE.search(text)
    if not short_sale_match:
        return None

    for label_match in DESCRIPTION_EVIDENCE_LABEL_RE.finditer(text):
        prefix = text[max(0, label_match.start() - 40) : label_match.start()]
        if DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE.search(prefix):
            continue
        section = text[label_match.start() : min(len(text), label_match.end() + 900)]
        stop_match = DESCRIPTION_SECTION_STOP_RE.search(section, max(20, label_match.end() - label_match.start()))
        if stop_match:
            section = section[: stop_match.start()]
        section_match = SHORT_SALE_LISTING_RE.search(section)
        if section_match:
            start = label_match.start() + section_match.start()
            end = label_match.start() + section_match.end()
            return SHORT_SALE_LISTING_RE.search(text, start, end)

    return None


def extract_short_sale_evidence_type(text: str) -> str:
    if re.search(r"\b(?:description|remarks|what'?s special|about this home|public remarks)\b", text, re.I):
        return "listing_description_or_remarks"
    if re.search(r"(?:special\s+listing\s+conditions?|specialListingConditions)", text, re.I):
        return "special_listing_conditions_or_field"
    return "listing_text"


def excerpt_around(text: str, start: int, end: int, width: int = 240) -> str:
    left = max(0, start - width // 2)
    right = min(len(text), end + width // 2)
    return normalize_space(text[left:right])


def build_existing_index(rows: list[list[str]]) -> ExistingIndex:
    address_keys: dict[str, int] = {}
    street_state_keys: dict[str, int] = {}
    phone_keys: dict[str, int] = {}
    agent_keys: dict[str, list[int]] = {}
    agent_name_keys: dict[str, list[int]] = {}
    listing_records: list[dict[str, Any]] = []
    for idx, row in enumerate(rows[1:], start=2):
        padded = row + [""] * 8
        agent = normalize_space(f"{padded[0]} {padded[1]}")
        phone = padded[2]
        email = padded[3]
        address = padded[4]
        city = padded[5]
        state = padded[6]
        akey = address_key(address, city, state)
        if akey:
            address_keys.setdefault(akey, idx)
        skey = street_state_key(address, state)
        if skey:
            street_state_keys.setdefault(skey, idx)
        phone_key = normalize_phone(phone)
        if phone_key:
            phone_keys.setdefault(phone_key, idx)
        gkey = agent_key(agent, state, phone, email)
        if gkey:
            agent_keys.setdefault(gkey, []).append(idx)
        name_key = agent_name_key(agent)
        if name_key:
            agent_name_keys.setdefault(name_key, []).append(idx)
        stable_id = normalize_space(padded[27]) if len(padded) > 27 else ""
        evidence = normalize_space(padded[25]) if len(padded) > 25 else ""
        canonical_id = canonical_listing_identifier({CANONICAL_VERIFIER_EVIDENCE_HEADER: evidence})
        record = listing_identity_record(
            row=idx,
            address=address,
            state=state,
            stable_id=stable_id,
            canonical_id=canonical_id,
        )
        if record["base_key"]:
            listing_records.append(record)
    return ExistingIndex(
        address_keys,
        street_state_keys,
        phone_keys,
        agent_keys,
        agent_name_keys,
        listing_records,
    )


def duplicate_status(candidate: Candidate, existing: ExistingIndex) -> tuple[str, str, str]:
    fields = candidate.fields
    listing_status, listing_key, listing_row = duplicate_listing_status(candidate, existing)
    if listing_status:
        return listing_status, listing_key, listing_row

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

    name_key = agent_name_key(fields.get("agent_name", ""))
    if name_key and name_key in existing.agent_name_keys:
        return (
            "possible_existing_agent",
            name_key,
            ",".join(map(str, existing.agent_name_keys[name_key])),
        )

    return "", listing_key, ""


def duplicate_listing_status(candidate: Candidate, existing: ExistingIndex) -> tuple[str, str, str]:
    classification = classify_listing_identity(candidate, existing)
    if classification["status"]:
        return (
            classification["status"],
            classification["listing_key"],
            ",".join(str(row) for row in classification["matched_rows"]),
        )
    fields = candidate.fields
    akey = address_key(
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
    )
    if akey and akey in existing.address_keys:
        return "duplicate_listing", akey, str(existing.address_keys[akey])
    skey = street_state_key(fields.get("listing_address", ""), fields.get("state", ""))
    if skey and skey in existing.street_state_keys:
        return "duplicate_listing", skey, str(existing.street_state_keys[skey])
    return "", skey or akey, ""


def classify_listing_identity(candidate: Candidate, existing: ExistingIndex) -> dict[str, Any]:
    """Classify listing ownership without using agent/contact history as identity."""
    fields = candidate.fields
    candidate_identifier = canonical_listing_identifier(
        {
            **fields,
            "raw_title": candidate.title,
            "qualification_evidence": candidate.text,
        }
    )
    stable_id = normalize_space(fields.get("zpid", "") or fields.get("synthetic_zpid", ""))
    identity = listing_identity_record(
        row=0,
        address=fields.get("listing_address", ""),
        state=fields.get("state", ""),
        stable_id=stable_id,
        canonical_id=candidate_identifier,
    )
    same_unit: list[dict[str, Any]] = []
    ambiguous_unit: list[dict[str, Any]] = []
    for record in existing.listing_records:
        if not identity["base_key"] or record["base_key"] != identity["base_key"]:
            continue
        if identity["unit"] == record["unit"]:
            same_unit.append(record)
        elif not identity["unit"] or not record["unit"]:
            ambiguous_unit.append(record)

    conflicts: list[dict[str, Any]] = list(ambiguous_unit)
    matches: list[dict[str, Any]] = []
    for record in same_unit:
        canonical_conflict = bool(
            identity["canonical_id"]
            and record["canonical_id"]
            and identity["canonical_id"] != record["canonical_id"]
        )
        stable_conflict = bool(
            identity["stable_namespace"]
            and identity["stable_namespace"] != "pilot"
            and identity["stable_namespace"] == record["stable_namespace"]
            and identity["stable_id"]
            and record["stable_id"]
            and identity["stable_id"] != record["stable_id"]
        )
        (conflicts if canonical_conflict or stable_conflict else matches).append(record)

    if conflicts:
        status = "identity_conflict"
        selected = conflicts
    elif matches:
        status = "duplicate_listing"
        selected = matches
    else:
        status = ""
        selected = []
    return {
        "status": status,
        "listing_key": identity["listing_key"],
        "matched_rows": [record["row"] for record in selected],
        "matched_attributions": sorted({record["attribution"] for record in selected}),
        "unit_ambiguous": bool(ambiguous_unit),
        "canonical_identifier_conflict": any(
            identity["canonical_id"]
            and record["canonical_id"]
            and identity["canonical_id"] != record["canonical_id"]
            for record in conflicts
        ),
    }


def duplicate_status_blocks_pilot_row(status: str) -> bool:
    return status in {"duplicate_listing", "identity_conflict"}


def is_valid_email(value: str) -> bool:
    compact = normalize_space(value)
    match = EMAIL_RE.fullmatch(compact)
    return bool(match)


def format_phone(value: str) -> str:
    digits = normalize_phone(value)
    if len(digits) != 10:
        return ""
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def has_complete_agent_contact(candidate: Candidate) -> bool:
    fields = candidate.fields
    agent_name = clean_agent_name(fields.get("agent_name", ""))
    first_name, last_name = split_agent_name(agent_name)
    return bool(
        first_name
        and last_name
        and len(normalize_phone(fields.get("phone", ""))) == 10
        and is_valid_email(fields.get("email", ""))
        and fields.get("phone_contact_type") in {"direct_mobile", "agent_specific_listing"}
        and fields.get("email_contact_type") == "agent_specific_professional"
        and fields.get("agent_evidence_group") == fields.get("listing_identity_group")
        and fields.get("phone_evidence_group") == fields.get("listing_identity_group")
        and fields.get("email_evidence_group") == fields.get("listing_identity_group")
        and fields.get("phone_owner_key") == fields.get("agent_subject_key")
        and fields.get("email_owner_key") == fields.get("agent_subject_key")
    )


def is_sms_ready_agent_contact(candidate: Candidate) -> bool:
    return bool(
        has_complete_agent_contact(candidate)
        and candidate.fields.get("phone_contact_type") == "direct_mobile"
    )


def merge_contact_hints(candidate: Candidate, text: str) -> None:
    fields = candidate.fields
    details = extract_listing_agent_fields(text)
    agent_name = details.get("agent_name", "")
    current_name = clean_agent_name(fields.get("agent_name", ""))
    if agent_name and (not current_name or normalize_key(current_name) == normalize_key(agent_name)):
        fields.update(details)
    sanitize_candidate_identity(candidate)


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
    reference_match = re.search(r"(?:^|;\s*)source_domain=([^;\s]+)", url or "", re.I)
    if reference_match:
        return reference_match.group(1).strip().lower()
    host = urllib.parse.urlparse(url).netloc.lower()
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def headless_budget_available(url: str) -> tuple[bool, str]:
    if not HEADLESS_FALLBACK:
        return False, "disabled"
    domain = registered_domain(url)
    if "*" not in HEADLESS_DOMAINS and domain not in HEADLESS_DOMAINS:
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
    compact = re.sub(r"\s*\(MLS#.*?\)\s*$", "", compact, flags=re.I)
    compact = normalize_space(compact).strip(" ,")
    parsed = parse_address_parts(compact)
    if parsed.get("listing_address"):
        compact = parsed["listing_address"]

    city = normalize_space(city)
    state = normalize_space(state).upper()
    zip_code = normalize_space(zip_code)
    if city and state:
        zip_part = rf"(?:\s+{re.escape(zip_code)})?" if zip_code else ""
        compact = re.sub(
            rf",?\s+{re.escape(city)}\s*,?\s+{re.escape(state)}\.?{zip_part}$",
            "",
            compact,
            flags=re.I,
        )
    elif state:
        zip_part = rf"(?:\s+{re.escape(zip_code)})?" if zip_code else ""
        compact = re.sub(rf",?\s+{re.escape(state)}\.?{zip_part}$", "", compact, flags=re.I)
    compact = re.sub(r"\b([A-Za-z][A-Za-z.'-]*)\s+\1\b", r"\1", compact, flags=re.I)
    compact = re.sub(r"\s*,\s*$", "", normalize_space(compact))
    return compact.strip(" ,.")


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


def cse_search(
    query: str,
    source: str,
    limit: int,
    date_restrict: str | None = None,
    *,
    start_index: int = 1,
) -> list[SearchResult]:
    if not CSE_API_KEY or not CSE_CX:
        return []
    results: list[SearchResult] = []
    effective_date_restrict = CSE_DATE_RESTRICT if date_restrict is None else date_restrict
    start = max(1, start_index)
    while len(results) < limit and start <= 91:
        num = min(10, limit - len(results))
        request_params = {
            "q": query,
            "key": CSE_API_KEY,
            "cx": CSE_CX,
            "num": num,
            "start": start,
        }
        if effective_date_restrict:
            request_params["dateRestrict"] = effective_date_restrict
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


def search_web(
    query: str,
    source: str,
    limit: int,
    date_restrict: str | None = None,
    *,
    start_index: int = 1,
) -> tuple[str, list[SearchResult]]:
    engines: list[str]
    if SEARCH_ENGINE in {"cse", "google", "google_cse"}:
        if not (CSE_API_KEY and CSE_CX):
            raise RuntimeError("configured CSE search engine is missing CSE_API_KEY or CSE_CX")
        engines = ["cse"]
    elif SEARCH_ENGINE in {"ddg", "duckduckgo"}:
        engines = ["ddg"]
    else:
        engines = ["cse"] if CSE_API_KEY and CSE_CX else []
        if ALLOW_DDG_FALLBACK:
            if CSE_API_KEY and CSE_CX:
                engines.append("ddg")
            else:
                engines = ["ddg"]

    last_error = ""
    for engine in engines:
        _search_engine_attempt_stats["attempted"] += 1
        try:
            if engine == "cse":
                cse_kwargs = {"date_restrict": date_restrict}
                if start_index != 1:
                    cse_kwargs["start_index"] = start_index
                results = cse_search(query, source, limit, **cse_kwargs)
            else:
                if start_index != 1:
                    raise RuntimeError("deep-page searches require Google CSE")
                results = ddg_search(query, source, limit)
            _search_engine_attempt_stats["succeeded"] += 1
            return engine, results
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            failure_class = source_error_class(exc)
            _search_engine_attempt_stats[failure_class] += 1
            log_event(
                "search_engine_failed",
                engine=engine,
                source=source,
                query=query,
                error=last_error,
                failure_class=failure_class,
                blocked=failure_class == "blocked",
            )
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
    if result.source in {"idx_broker_pages", "idx_broker_remarks"}:
        if re.search(r"/(?:search|blog|buying|selling|guides?|resources?|category|tag)(?:/|$)", path):
            return False, "not_idx_listing_detail"
        if re.search(r"\b(?:\d+\+\s+listings|homes?\s+for\s+sale|search\s+homes|buying\s+a|tips)\b", title, re.I):
            return False, "not_idx_listing_detail"
    return True, ""


def looks_like_listing_address(address: str) -> bool:
    compact = normalize_space(address)
    if is_undisclosed_address(compact) or not compact or not re.match(r"^\d{1,6}\b", compact):
        return False
    street_text = re.sub(r"^\d{1,6}\s*", "", compact).strip(" ,")
    if not re.search(r"[A-Za-z]", street_text):
        return False
    if "," in compact and not re.search(
        r",\s*(?:#|unit|apt|apartment|suite|ste)\s*[-A-Za-z0-9]+\s*$",
        compact,
        re.I,
    ):
        return False
    return not re.search(
        r"\b(?:blog|buying|foreclosure|short\s+sale|homes?\s+for\s+sale|listings?|page|search|vintage|fixer[-\s]?upper|viewing\s+listing|mls\s*#|for\s+\$)\b",
        compact,
        re.IGNORECASE,
    )


def is_undisclosed_address(address: str) -> bool:
    return bool(re.search(r"\bundisclosed\b", normalize_space(address), re.IGNORECASE))


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
    compact = re.sub(r"(?i)^the\s+", "", compact)
    compact = re.split(
        r"(?i)\b(?:call|phone|cell|mobile|direct|main|email|license|lic\.?|dre|brokerage|broker|"
        r"brokered\s+by|shown\s+by|listed\s+by|listing\s+office|office|mls|fax|"
        r"website|provided\s+by|status|remarks|public\s+remarks|description|"
        r"property\s+description|special\s+listing\s+conditions?|realty|realtor|"
        r"real\s+estate|properties|brokerage|llc|inc|corp|company|group|team|"
        r"associates|homes?|mortgage|bank|trust|services|partners|title|insurance)\b|"
        r"[|•;,{]",
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
    return extract_listing_agent_fields(text).get("agent_name", "")


def extract_listing_agent_fields(text: str) -> dict[str, str]:
    for match in TRUSTED_AGENT_LABEL_CONTEXT_RE.finditer(text):
        context = match.group(1)
        agent_segment = re.split(
            r"(?i)\b(?:status|remarks|public\s+remarks|description|property\s+description|"
            r"special\s+listing\s+conditions?|brokerage|listing\s+office|office|contact\s+phone)\b",
            context,
            maxsplit=1,
        )[0]
        name = clean_agent_name(agent_segment)
        if not name:
            continue
        details = {
            "agent_name": name,
            "agent_name_source": "listing_agent_label",
        }
        phone_match = first_contact_phone_match(agent_segment)
        if phone_match:
            details["phone"] = format_phone(phone_match.group(0))
            details["phone_source"] = "listing_agent_label"
        email_match = EMAIL_RE.search(agent_segment)
        if email_match:
            details["email"] = email_match.group(0).lower()
            details["email_source"] = "listing_agent_label"
        return details
    return {}


def extract_bound_listing_agent_fields(text: str) -> dict[str, str]:
    """Return one unambiguous individual listing-agent label from a bound listing record."""
    matches: dict[str, dict[str, str]] = {}
    for match in TRUSTED_AGENT_LABEL_CONTEXT_RE.finditer(text):
        context = match.group(1)
        agent_segment = re.split(
            r"(?i)\b(?:status|remarks|public\s+remarks|description|property\s+description|"
            r"special\s+listing\s+conditions?|brokerage|listing\s+office|office|contact\s+phone)\b|"
            r"[|•;\n\r]",
            context,
            maxsplit=1,
        )[0]
        if BUSINESS_NAME_RE.search(agent_segment):
            continue
        name = clean_agent_name(agent_segment)
        if not name:
            continue
        details = {"agent_name": name}
        phone_match = first_contact_phone_match(agent_segment)
        if phone_match:
            details["phone"] = format_phone(phone_match.group(0))
        email_match = EMAIL_RE.search(agent_segment)
        if email_match:
            details["email"] = email_match.group(0).lower()
        key = normalize_key(name)
        existing = matches.setdefault(key, details)
        for field in ("phone", "email"):
            if details.get(field):
                existing.setdefault(field, details[field])
    return next(iter(matches.values())) if len(matches) == 1 else {}


def shadow_listing_agent_candidate(candidate: Candidate) -> dict[str, str]:
    """Extract a high-confidence agent candidate without changing live fields."""
    for label, pattern in SHADOW_AGENT_LABEL_PATTERNS:
        for match in pattern.finditer(candidate.text):
            segment = re.split(
                r"(?i)\b(?:status|remarks|public\s+remarks|description|property\s+description|"
                r"special\s+listing\s+conditions?|listing\s+office|office|contact\s+phone)\b|"
                r"[|•;\n\r]",
                match.group(1),
                maxsplit=1,
            )[0]
            if label in {"listing_courtesy", "listing_provided_by"}:
                person_of_brokerage = re.match(
                    r"^\s*(?P<person>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})"
                    r"\s+of\s+(?P<brokerage>.+)$",
                    segment,
                )
                if person_of_brokerage and BUSINESS_NAME_RE.search(person_of_brokerage.group("brokerage")):
                    segment = person_of_brokerage.group("person")
                elif BUSINESS_NAME_RE.search(segment):
                    log_event(
                        "pilot_agent_shadow_rejected",
                        url=candidate.url,
                        label=label,
                        agent="",
                        reason="courtesy_names_brokerage_not_person",
                    )
                    continue
            artifact_shadow = agent_artifact_shadow_name(segment)
            name = artifact_shadow.get("proposed_agent", "") or clean_agent_name(segment)
            if not name:
                continue
            probe = Candidate(
                source=candidate.source,
                query=candidate.query,
                url=candidate.url,
                title=candidate.title,
                text=candidate.text,
                fields={
                    "agent_name": name,
                    "agent_name_source": "listing_agent_label",
                    "listing_address": candidate.fields.get("listing_address", ""),
                    "city": candidate.fields.get("city", ""),
                    "state": candidate.fields.get("state", ""),
                    "broker_name": candidate.fields.get("broker_name", ""),
                },
            )
            safe, reason = agent_name_promotion_safety(probe, require_source=False)
            if safe:
                result = {"agent_name": name, "label": label}
                if artifact_shadow:
                    result.update(artifact_shadow)
                    result["artifact_sanitized"] = "true"
                return result
            log_event(
                "pilot_agent_shadow_rejected",
                url=candidate.url,
                label=label,
                agent=name,
                reason=reason,
            )
    return {}


def first_contact_phone_match(text: str) -> re.Match[str] | None:
    for match in PHONE_RE.finditer(text):
        context = text[max(0, match.start() - 20) : min(len(text), match.end() + 20)].lower()
        if re.search(r"[a-z/_-]?\d{10}[a-z0-9_-]*\.(?:jpg|jpeg|png|webp)\b", context):
            continue
        return match
    return None


TRUSTED_AGENT_SOURCES = {
    "jsonld_bound_listing_agent",
    "visible_listing_container",
}


def agent_name_promotion_safety(candidate: Candidate, *, require_source: bool = True) -> tuple[bool, str]:
    fields = candidate.fields
    name = clean_agent_name(fields.get("agent_name", ""))
    if not name:
        return False, "missing_listing_agent"
    if require_source and fields.get("agent_name_source", "") not in TRUSTED_AGENT_SOURCES:
        return False, "unattributed_listing_agent"
    if require_source and (
        not fields.get("agent_evidence_group")
        or fields.get("agent_evidence_group") != fields.get("listing_identity_group")
    ):
        return False, "unbound_listing_agent"
    if require_source and fields.get("agent_subject_key") != normalize_key(name):
        return False, "agent_subject_mismatch"

    raw_tokens = [token.strip(" .'()-") for token in name.split() if token.strip(" .'()-")]
    if any(len(token) >= 2 and token.isupper() for token in raw_tokens):
        return False, "agent_name_contains_acronym"

    normalized_name = normalize_key(name)
    name_tokens = set(normalized_name.split())
    if any(token in PROMOTION_AGENT_BLOCKLIST for token in name_tokens):
        return False, "agent_name_contains_feed_or_brokerage_term"

    ordered_tokens = normalized_name.split()
    if ordered_tokens and ordered_tokens[0] in PROMOTION_AGENT_STREET_TOKENS:
        return False, "agent_name_starts_with_address_token"

    state = normalize_space(fields.get("state", "")).upper()
    state_tokens = set(normalize_key(STATE_QUERY_TERMS.get(state, state)).split())
    if state.lower() in name_tokens or state_tokens and state_tokens.issubset(name_tokens):
        return False, "agent_name_contains_state"

    city_tokens = set(normalize_key(fields.get("city", "")).split())
    if city_tokens and city_tokens.issubset(name_tokens):
        return False, "agent_name_matches_city"

    address_tokens = set(normalize_key(fields.get("listing_address", "")).split())
    if len(name_tokens) >= 2 and name_tokens.issubset(address_tokens):
        return False, "agent_name_matches_address"
    overlapping_address_tokens = name_tokens.intersection(address_tokens).intersection(PROMOTION_AGENT_STREET_TOKENS)
    if overlapping_address_tokens:
        return False, "agent_name_contains_address_token"

    broker_tokens = set(normalize_key(fields.get("broker_name", "")).split())
    if broker_tokens and (name_tokens.issubset(broker_tokens) or broker_tokens.issubset(name_tokens)):
        return False, "agent_name_matches_brokerage"
    return True, ""


def sanitize_candidate_identity(candidate: Candidate) -> tuple[bool, str]:
    fields = candidate.fields
    fields["agent_name"] = clean_agent_name(fields.get("agent_name", ""))
    safe, reason = agent_name_promotion_safety(candidate)
    if not safe:
        if fields.get("agent_name"):
            fields["rejected_agent_name"] = fields["agent_name"]
        fields["agent_name_rejection_reason"] = reason
        for key in (
            "agent_name", "agent_name_source", "agent_evidence_group",
            "agent_subject_key",
            "phone", "phone_source", "phone_evidence_group", "phone_contact_type",
            "phone_owner_key",
            "email", "email_source", "email_evidence_group", "email_contact_type",
            "email_owner_key",
        ):
            fields[key] = ""
        return False, reason

    agent_group = fields.get("agent_evidence_group", "")
    if (
        fields.get("phone_source", "") not in TRUSTED_AGENT_SOURCES
        or not agent_group
        or fields.get("phone_evidence_group", "") != agent_group
    ):
        fields["phone"] = ""
        fields["phone_source"] = ""
        fields["phone_evidence_group"] = ""
        fields["phone_contact_type"] = ""
        fields["phone_owner_key"] = ""
    else:
        fields["phone"] = format_phone(fields.get("phone", ""))
        if fields.get("phone_contact_type") == "office_team_main":
            fields["contact_phone_hint"] = fields["phone"]
            fields["contact_phone_hint_type"] = fields["phone_contact_type"]
            fields["phone"] = ""
            fields["phone_source"] = ""
            fields["phone_evidence_group"] = ""
            fields["phone_contact_type"] = ""
            fields["phone_owner_key"] = ""
    if (
        fields.get("email_source", "") not in TRUSTED_AGENT_SOURCES
        or not agent_group
        or fields.get("email_evidence_group", "") != agent_group
    ):
        fields["email"] = ""
        fields["email_source"] = ""
        fields["email_evidence_group"] = ""
        fields["email_contact_type"] = ""
        fields["email_owner_key"] = ""
    elif is_valid_email(fields.get("email", "")):
        fields["email"] = fields["email"].strip().lower()
        if fields.get("email_contact_type") != "agent_specific_professional":
            fields["contact_email_hint"] = fields["email"]
            fields["contact_email_hint_type"] = fields.get("email_contact_type", "")
            fields["email"] = ""
            fields["email_source"] = ""
            fields["email_evidence_group"] = ""
            fields["email_contact_type"] = ""
            fields["email_owner_key"] = ""
    else:
        fields["email"] = ""
        fields["email_source"] = ""
        fields["email_evidence_group"] = ""
        fields["email_contact_type"] = ""
        fields["email_owner_key"] = ""
    return True, ""


def jsonld_type_names(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value.lower()}
    if isinstance(value, list):
        return {str(item).lower() for item in value}
    return set()


LISTING_JSONLD_TYPES = {
    "product",
    "house",
    "singlefamilyresidence",
    "residence",
    "apartment",
    "realestatelisting",
    "offer",
}


def required_review_field_failure(candidate: Candidate, qualification: Qualification) -> str:
    agent_name = clean_agent_name(candidate.fields.get("agent_name", ""))
    if agent_name:
        candidate.fields["agent_name"] = agent_name
    if not looks_like_listing_address(candidate.fields.get("listing_address", "")):
        return "missing_listing_detail_address"
    if qualification.status != "qualified" or not normalize_space(qualification.evidence):
        return "missing_short_sale_confirmation"
    return ""


def strict_listing_description_evidence(candidate: Candidate) -> str:
    description = normalize_space(candidate.fields.get("listing_description", ""))
    description_match = SHORT_SALE_LISTING_RE.search(description)
    if description_match:
        return excerpt_around(description, description_match.start(), description_match.end())

    text = candidate.text
    for label_match in STRICT_DESCRIPTION_EVIDENCE_LABEL_RE.finditer(text):
        prefix = text[max(0, label_match.start() - 40) : label_match.start()]
        if DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE.search(prefix):
            continue
        section = text[label_match.start() : min(len(text), label_match.end() + 900)]
        stop_match = DESCRIPTION_SECTION_STOP_RE.search(
            section,
            max(20, label_match.end() - label_match.start()),
        )
        if stop_match:
            section = section[: stop_match.start()]
        section_match = SHORT_SALE_LISTING_RE.search(section)
        if section_match:
            return excerpt_around(section, section_match.start(), section_match.end())
    return ""


def qualification_precedence_shadow(candidate: Candidate) -> dict[str, Any]:
    """Evaluate broader agent-written description labels without changing intake."""
    current_evidence = strict_listing_description_evidence(candidate)
    proposed_evidence = current_evidence
    if not proposed_evidence:
        text = candidate.text
        for label_match in DESCRIPTION_EVIDENCE_LABEL_RE.finditer(text):
            prefix = text[max(0, label_match.start() - 40) : label_match.start()]
            if DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE.search(prefix):
                continue
            section = text[label_match.start() : min(len(text), label_match.end() + 900)]
            stop_match = DESCRIPTION_SECTION_STOP_RE.search(
                section,
                max(20, label_match.end() - label_match.start()),
            )
            if stop_match:
                section = section[: stop_match.start()]
            section_match = SHORT_SALE_LISTING_RE.search(section)
            if section_match:
                proposed_evidence = excerpt_around(
                    section,
                    section_match.start(),
                    section_match.end(),
                )
                break

    shadow_text = candidate.text
    home_status = normalize_space(candidate.fields.get("home_status", ""))
    if home_status:
        shadow_text = f"{shadow_text} homeStatus: {home_status}"
    qualification = qualification_for_text(shadow_text)
    proposed_ready = bool(qualification.status == "qualified" and proposed_evidence)
    return {
        "current_description_confirmed": bool(current_evidence),
        "proposed_description_confirmed": bool(proposed_evidence),
        "proposed_ready": proposed_ready,
        "qualification_status": qualification.status,
        "failure_reason": qualification.failure_reason,
        "disqualifying_terms": qualification.disqualifying_terms,
        "proposed_evidence": proposed_evidence[:500],
        "writes": 0,
    }


def description_block_evidence(candidate: Candidate) -> str:
    """Return short-sale proof only when it remains inside a clean description block."""
    description = normalize_space(candidate.fields.get("listing_description", ""))
    if description and not DESCRIPTION_BLOCK_TAXONOMY_NOISE_RE.search(description):
        description_match = SHORT_SALE_LISTING_RE.search(description)
        if description_match:
            return excerpt_around(description, description_match.start(), description_match.end())

    text = candidate.text
    for label_match in STRICT_DESCRIPTION_EVIDENCE_LABEL_RE.finditer(text):
        prefix = text[max(0, label_match.start() - 40) : label_match.start()]
        if DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE.search(prefix):
            continue
        section = text[label_match.start() : min(len(text), label_match.end() + 900)]
        stop_offsets = []
        for stop_re in (DESCRIPTION_SECTION_STOP_RE, DESCRIPTION_BLOCK_NAVIGATION_STOP_RE):
            stop_match = stop_re.search(section, max(20, label_match.end() - label_match.start()))
            if stop_match:
                stop_offsets.append(stop_match.start())
        if stop_offsets:
            section = section[: min(stop_offsets)]
        if DESCRIPTION_BLOCK_TAXONOMY_NOISE_RE.search(section):
            continue
        section_match = SHORT_SALE_LISTING_RE.search(section)
        if section_match:
            return excerpt_around(section, section_match.start(), section_match.end())
    return ""


def description_block_shadow(candidate: Candidate) -> dict[str, Any]:
    """Compare broad description-label readiness with strict description proof."""
    broad = qualification_precedence_shadow(candidate)
    strict_evidence = description_block_evidence(candidate)
    strict_ready = bool(broad["qualification_status"] == "qualified" and strict_evidence)
    would_hold = bool(broad["proposed_ready"] and not strict_ready)
    return {
        "current_ready": bool(broad["proposed_ready"]),
        "description_block_confirmed": bool(strict_evidence),
        "proposed_ready": strict_ready,
        "would_hold": would_hold,
        "qualification_status": broad["qualification_status"],
        "failure_reason": broad["failure_reason"],
        "strict_evidence": strict_evidence[:500],
        "writes": 0,
    }


def site_chrome_exclusion_shadow(candidate: Candidate) -> dict[str, Any]:
    """Flag a targeted site-card phrase without changing intake or promotion."""
    broad = qualification_precedence_shadow(candidate)
    domain = registered_domain(candidate.url)
    platform_targeted = domain in SITE_CHROME_SHADOW_DOMAINS
    description = normalize_space(candidate.fields.get("listing_description", ""))
    navigation_match = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.search(candidate.text)
    description_without_navigation = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.sub(
        " ",
        description,
    )
    description_is_navigation = bool(
        navigation_match
        and SITE_CHROME_SHORT_SALE_NAVIGATION_RE.search(description)
        and not SHORT_SALE_LISTING_RE.search(description_without_navigation)
    )
    description_confirmed = bool(
        SHORT_SALE_LISTING_RE.search(description) and not description_is_navigation
    )
    chrome_match = SITE_CHROME_SHORT_SALE_CARD_RE.search(candidate.text) or navigation_match
    chrome_evidence = ""
    if chrome_match:
        chrome_evidence = excerpt_around(
            candidate.text,
            chrome_match.start("label"),
            chrome_match.end("label"),
        )
    would_hold = bool(
        broad["proposed_ready"]
        and platform_targeted
        and description
        and not description_confirmed
        and chrome_match
    )
    return {
        "current_ready": bool(broad["proposed_ready"]),
        "platform_targeted": platform_targeted,
        "target_domain": domain,
        "listing_description_present": bool(description),
        "listing_description_short_sale_confirmed": description_confirmed,
        "site_chrome_pattern_found": bool(chrome_match),
        "navigation_pattern_found": bool(navigation_match),
        "listing_description_is_navigation": description_is_navigation,
        "proposed_ready": bool(broad["proposed_ready"] and not would_hold),
        "would_hold": would_hold,
        "reason": (
            "site_chrome_short_sale_navigation_only"
            if would_hold and navigation_match
            else "site_chrome_short_sale_card_only" if would_hold else ""
        ),
        "evidence": chrome_evidence[:500],
        "writes": 0,
    }


def log_site_chrome_prewrite_receipt(
    candidate: Candidate,
    qualification: Qualification,
    *,
    state: str,
    source: str,
    run_date: dt.date | None = None,
) -> bool:
    """Record every target-domain candidate before qualification can write a lead row."""
    global _site_chrome_prewrite_count
    shadow = site_chrome_exclusion_shadow(candidate)
    if not shadow["platform_targeted"]:
        return False
    observed_date = run_date or dt.datetime.now(ZoneInfo("America/New_York")).date()
    if not experiment_active(
        observed_date,
        SITE_CHROME_SHADOW_START_DATE,
        SITE_CHROME_SHADOW_DAYS,
    ):
        return False
    reviewable = bool(
        candidate.fields.get("exact_listing_confirmed") == "true"
        and candidate.fields.get("listing_description")
        and candidate.fields.get("listing_identity_group")
    )
    if not reviewable:
        return False
    stable_id = stable_synthetic_zpid(
        candidate.source,
        candidate.url,
        candidate.fields.get("listing_address", ""),
        candidate.fields.get("city", ""),
        candidate.fields.get("state", ""),
    )
    candidate_ref = hashlib.sha256(stable_id.encode("utf-8")).hexdigest()[:16]
    if candidate_ref in _site_chrome_prewrite_seen:
        return False
    if _site_chrome_prewrite_count >= SITE_CHROME_SHADOW_MAX_PER_RUN:
        return False
    _site_chrome_prewrite_seen.add(candidate_ref)
    _site_chrome_prewrite_count += 1
    receipt_source_url = public_source_url_for_receipt(candidate.url)
    log_event(
        "pilot_site_chrome_prewrite_receipt",
        run_date=observed_date.isoformat(),
        state=state,
        source=source,
        candidate_ref=candidate_ref,
        exact_source_url=receipt_source_url,
        exact_source_url_reviewable=bool(receipt_source_url),
        qualification_status=qualification.status,
        qualification_reason=qualification.failure_reason,
        reviewable=reviewable,
        independent_review_required=True,
        agreement_unconfirmed=True,
        experiment_case_number_this_run=_site_chrome_prewrite_count,
        experiment_window_days=SITE_CHROME_SHADOW_DAYS,
        experiment_target_unique_properties=SITE_CHROME_SHADOW_MAX_PER_RUN,
        denominator_key_type="normalized_property_identity_hash",
        durable_receipt_surface="render_structured_logs",
        window_case_number_requires_log_aggregation=True,
        lead_data_writes=0,
        searches_added=0,
        sends=0,
        **shadow,
    )
    return True


def compound_negative_field_shadow(candidate: Candidate) -> dict[str, Any]:
    """Flag an explicit structured short-sale No/false field without writing."""
    broad = qualification_precedence_shadow(candidate)
    negative_match = COMPOUND_SHORT_SALE_NEGATIVE_RE.search(candidate.text)
    evidence = ""
    if negative_match:
        evidence = excerpt_around(
            candidate.text,
            negative_match.start("label"),
            negative_match.end("value"),
        )
    would_hold = bool(broad["proposed_ready"] and negative_match)
    return {
        "current_ready": bool(broad["proposed_ready"]),
        "explicit_negative_field_found": bool(negative_match),
        "negative_field_label": normalize_space(negative_match.group("label"))
        if negative_match
        else "",
        "negative_field_value": negative_match.group("value").lower()
        if negative_match
        else "",
        "proposed_ready": bool(broad["proposed_ready"] and not would_hold),
        "would_hold": would_hold,
        "reason": "explicit_negative_short_sale_field" if would_hold else "",
        "evidence": evidence[:500],
        "writes": 0,
    }


def future_negotiator_phrase_shadow(candidate: Candidate) -> dict[str, Any]:
    """Flag future or underway negotiator involvement without changing qualification."""
    description = normalize_space(candidate.fields.get("listing_description", ""))
    matches = list(FUTURE_NEGOTIATOR_INVOLVEMENT_RE.finditer(description)) if description else []
    evidence = ""
    if matches:
        match = matches[0]
        evidence = excerpt_around(description, match.start(), match.end())
    else:
        text = candidate.text
        for label_match in STRICT_DESCRIPTION_EVIDENCE_LABEL_RE.finditer(text):
            prefix = text[max(0, label_match.start() - 40) : label_match.start()]
            if DESCRIPTION_EVIDENCE_SKIP_PREFIX_RE.search(prefix):
                continue
            section = text[label_match.start() : min(len(text), label_match.end() + 900)]
            stop_offsets = []
            for stop_re in (DESCRIPTION_SECTION_STOP_RE, DESCRIPTION_BLOCK_NAVIGATION_STOP_RE):
                stop_match = stop_re.search(section, max(20, label_match.end() - label_match.start()))
                if stop_match:
                    stop_offsets.append(stop_match.start())
            if stop_offsets:
                section = section[: min(stop_offsets)]
            match = FUTURE_NEGOTIATOR_INVOLVEMENT_RE.search(section)
            if match:
                evidence = excerpt_around(section, match.start(), match.end())
                break
    return {
        "phrase_found": bool(evidence),
        "would_hold": bool(evidence),
        "reason": "future_negotiator_involvement" if evidence else "",
        "evidence": evidence[:500],
        "writes": 0,
    }


def has_durable_source_evidence(values: dict[str, Any]) -> bool:
    """Require an explicit durable receipt; an in-memory exact URL or hash is insufficient."""
    state = normalize_space(
        str(values.get("source_evidence_state") or values.get("sourceEvidenceState") or "")
    ).lower()
    receipt = normalize_space(
        str(values.get("source_evidence_receipt") or values.get("sourceEvidenceReceipt") or "")
    )
    return state == "durable_reopenable" and bool(receipt)


def shadow_promotion_readiness(candidate: Candidate, qualification: Qualification) -> tuple[str, bool, str]:
    safe_agent, agent_reason = sanitize_candidate_identity(candidate)
    fields = candidate.fields
    if qualification.status != "qualified":
        return "not_qualified", False, "Listing did not pass the short sale qualification rules."
    if not (
        looks_like_listing_address(fields.get("listing_address", ""))
        and normalize_space(fields.get("city", ""))
        and normalize_space(fields.get("state", ""))
    ):
        return "needs_address", False, "Street, city, and state must be confirmed before promotion."
    if not strict_listing_description_evidence(candidate):
        return (
            "needs_description_confirmation",
            False,
            "Short sale language was not confirmed in the listing agent's description or remarks.",
        )
    if not has_durable_source_evidence(candidate.fields):
        return (
            "needs_source_evidence_confirmation",
            False,
            "Exact source evidence is hash-only or lacks a durable reopenable receipt; promotion and outreach remain held.",
        )
    contact_note = (
        "Agent phone and email are attributable to the listing."
        if has_complete_agent_contact(candidate)
        else "Agent identity or contact is blank or partial; the lead verifier must confirm it after Sheet1 intake."
    )
    if not safe_agent:
        contact_note = f"Agent identity was left blank ({agent_reason}); the lead verifier must confirm it after Sheet1 intake."
    mode_note = (
        "Automatic PendingQueue promotion is disabled during the shadow rollout."
        if SHADOW_MODE
        else "Automatic PendingQueue promotion remains disabled."
    )
    return "shadow_ready", True, f"{contact_note} {mode_note}"


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


def address_appears_in_text(address: str, text: str) -> bool:
    address_tokens = normalize_key(clean_listing_address(address)).split()
    text_key = normalize_key(text)
    if not address_tokens or not text_key:
        return False
    address_key_value = " ".join(address_tokens)
    if address_key_value in text_key:
        return True
    number = next((token for token in address_tokens if token.isdigit()), "")
    ignored = {
        "street", "st", "road", "rd", "avenue", "ave", "drive", "dr", "lane", "ln",
        "court", "ct", "trail", "trl", "way", "boulevard", "blvd", "circle", "cir",
        "unit", "apt", "suite", "north", "south", "east", "west", "n", "s", "e", "w",
    }
    core = [token for token in address_tokens if token != number and token not in ignored]
    if not number or not core:
        return False
    for match in re.finditer(rf"\b{re.escape(number)}\b", text_key):
        window = text_key[max(0, match.start() - 40) : match.end() + 160]
        if all(re.search(rf"\b{re.escape(token)}\b", window) for token in core[:3]):
            return True
    return False


def jsonld_listing_status(obj: dict[str, Any]) -> tuple[str, str]:
    values: list[str] = []
    for key in ("homeStatus", "listingStatus", "status", "availability"):
        value = obj.get(key)
        if isinstance(value, str):
            values.append(value)
    offers = obj.get("offers")
    if isinstance(offers, dict):
        for key in ("availability", "status"):
            value = offers.get(key)
            if isinstance(value, str):
                values.append(value)
    evidence = normalize_space(" ".join(values))
    if not evidence:
        return "unknown", ""
    normalized_values = {
        re.sub(r"[^a-z0-9]+", "", value.rsplit("/", 1)[-1].rsplit("#", 1)[-1].lower())
        for value in values
    }
    current_values = {
        "instock", "forsale", "active", "activeundercontract", "pending",
        "pendinglenderapproval", "undercontract",
    }
    coming_soon_values = {"comingsoon"}
    unsupported_values = {"contingent", "underagreement"}
    non_current_values = {
        "inactive", "sold", "closed", "offmarket", "expired", "withdrawn", "cancelled",
        "canceled", "outofstock",
    }
    has_current = bool(normalized_values.intersection(current_values))
    has_non_current = bool(normalized_values.intersection(non_current_values))
    has_coming_soon = bool(normalized_values.intersection(coming_soon_values))
    has_unsupported = bool(normalized_values.intersection(unsupported_values))
    observed_classes = sum(bool(value) for value in (has_current, has_non_current, has_coming_soon, has_unsupported))
    if observed_classes > 1:
        return "conflicting", f"conflicting_status:{evidence[:200]}"
    if has_non_current:
        return "not_current", evidence[:220]
    if has_coming_soon:
        return "coming_soon", evidence[:220]
    if has_unsupported:
        return "unsupported", evidence[:220]
    if has_current:
        return "current", evidence[:220]
    return "unknown", evidence[:220]


def listing_evidence_group(address: str, state: str = "", zip_code: str = "") -> str:
    street = normalize_key(clean_listing_address(address, state=state, zip_code=zip_code))
    if not street:
        return ""
    return "|".join(part for part in (street, normalize_key(state), normalize_key(zip_code)) if part)


def expected_listing_fields(result: SearchResult) -> dict[str, str]:
    normalized_title = re.sub(
        r"\b([A-Z]{2})\.(?=\s*(?:[|–-]|$))",
        r"\1",
        result.title,
    )
    parts = parse_address_parts(normalized_title)
    if not parts.get("listing_address"):
        title_parts = [part.strip(" .") for part in re.split(r"\s*[|–-]\s*", normalized_title) if part.strip()]
        if title_parts:
            parts.update(parse_address_parts(title_parts[0]))
        if title_parts and not parts.get("listing_address") and looks_like_listing_address(title_parts[0]):
            parts["listing_address"] = clean_listing_address(title_parts[0])
    if not parts.get("state"):
        location = re.search(r"\b([A-Z][A-Za-z .'-]{2,40}),\s*([A-Z]{2})\s*(\d{5})?\b", result.title + " " + result.snippet)
        if location:
            parts.setdefault("city", normalize_space(location.group(1)))
            parts["state"] = location.group(2)
            parts.setdefault("zip", location.group(3) or "")
    return parts


def listing_fields_match_expected(candidate: dict[str, str], expected: dict[str, str]) -> bool:
    candidate_street = normalize_key(clean_listing_address(
        candidate.get("listing_address", ""),
        candidate.get("city", ""),
        candidate.get("state", ""),
        candidate.get("zip", ""),
    ))
    expected_street = normalize_key(clean_listing_address(
        expected.get("listing_address", ""),
        expected.get("city", ""),
        expected.get("state", ""),
        expected.get("zip", ""),
    ))
    if not candidate_street or not expected_street or candidate_street != expected_street:
        return False
    candidate_state = normalize_key(candidate.get("state", ""))
    expected_state = normalize_key(expected.get("state", ""))
    if candidate_state and expected_state and candidate_state != expected_state:
        return False
    candidate_city = normalize_key(candidate.get("city", ""))
    expected_city = normalize_key(expected.get("city", ""))
    if candidate_city and expected_city and candidate_city != expected_city:
        return False
    candidate_zip = normalize_key(candidate.get("zip", ""))
    expected_zip = normalize_key(expected.get("zip", ""))
    if expected_zip and candidate_zip != expected_zip:
        return False
    return True


def listing_url_matches_address(url: str, address: str) -> bool:
    path_key = normalize_key(urllib.parse.unquote(urllib.parse.urlparse(url).path))
    address_tokens = normalize_key(clean_listing_address(address)).split()
    number = next((token for token in address_tokens if token.isdigit()), "")
    ignored = {
        "street", "road", "avenue", "drive", "lane", "court", "trail", "way",
        "boulevard", "circle", "parkway", "terrace", "highway", "route", "north",
        "south", "east", "west", "n", "s", "e", "w",
    }
    core = [token for token in address_tokens if token != number and token not in ignored]
    return bool(number and core and number in path_key.split() and all(token in path_key.split() for token in core[:2]))


def bound_phone_contact_type(text: str, phone: str) -> str:
    digits = normalize_phone(phone)
    if not digits:
        return ""
    if digits[:3] in {"800", "833", "844", "855", "866", "877", "888"}:
        return "office_team_main"
    compact = normalize_space(text)
    for match in PHONE_RE.finditer(compact):
        if normalize_phone(match.group(0)) != digits:
            continue
        prefix = compact[max(0, match.start() - 35) : match.start()]
        if re.search(r"\b(?:mobile|cell|direct)(?:\s+(?:phone|number))?\s*[:#-]?\s*$", prefix, re.I):
            return "direct_mobile"
        if re.search(r"\b(?:office|main|team|brokerage)(?:\s+(?:phone|number))?\s*[:#-]?\s*$", prefix, re.I):
            return "office_team_main"
    return "agent_specific_listing"


def bound_email_contact_type(email_value: str, agent_name: str = "") -> str:
    local = normalize_space(email_value).lower().split("@", 1)[0]
    routing_tokens = (
        "agent", "info", "support", "office", "team", "sales", "listing", "lead",
        "contact", "admin", "hello", "home", "frontdesk", "reception", "inquiry",
        "inquiries", "marketing", "showing", "appointment", "clientservice", "concierge",
        "transaction", "closing",
    )
    if any(token in local for token in routing_tokens):
        return "team_brokerage_routing"
    name_tokens = normalize_key(agent_name).split()
    first = name_tokens[0] if name_tokens else ""
    last = name_tokens[-1] if len(name_tokens) >= 2 else ""
    local_key = normalize_key(local).replace(" ", "")
    approved_forms = {
        first + last,
        first[:1] + last if first else "",
        last + first,
    }
    if local_key and local_key in approved_forms:
        return "agent_specific_professional"
    return "professional_unverified" if is_valid_email(email_value) else ""


def bind_agent_fields(details: dict[str, str], group: str, source: str, context: str) -> dict[str, str]:
    raw_name = normalize_space(details.get("agent_name", ""))
    if BUSINESS_NAME_RE.search(raw_name):
        return {}
    name = clean_agent_name(raw_name)
    if not name or not group:
        return {}
    bound = {
        "agent_name": name,
        "agent_name_source": source,
        "agent_evidence_group": group,
        "agent_subject_key": normalize_key(name),
    }
    phone = format_phone(details.get("phone", ""))
    if phone:
        bound.update({
            "phone": phone,
            "phone_source": source,
            "phone_evidence_group": group,
            "phone_contact_type": bound_phone_contact_type(context, phone),
            "phone_owner_key": normalize_key(name),
        })
    email_value = normalize_space(details.get("email", "")).lower()
    if is_valid_email(email_value):
        bound.update({
            "email": email_value,
            "email_source": source,
            "email_evidence_group": group,
            "email_contact_type": bound_email_contact_type(email_value, name),
            "email_owner_key": normalize_key(name),
        })
    return bound


def visible_agent_subrecord(container: Any, group: str) -> dict[str, str]:
    """Bind exactly one agent from an explicit agent subrecord, never the full listing container."""
    if container is None or not hasattr(container, "find_all"):
        return {}
    bound_by_subject: dict[str, dict[str, str]] = {}
    contact_values: dict[str, dict[str, set[str]]] = {}
    for element in container.find_all(True):
        attributes = " ".join(
            str(value)
            for key, value in getattr(element, "attrs", {}).items()
            if key.lower() in {"id", "class", "itemprop", "data-testid", "aria-label"}
        ).lower()
        if not re.search(r"(?:listing[-_ ]?agent|agent[-_ ]?(?:info|contact|details|profile))", attributes):
            continue
        if re.search(r"(?:related|recommended|similar|team|office|brokerage|sidebar)", attributes):
            continue
        ancestor = getattr(element, "parent", None)
        excluded_ancestor = False
        while ancestor is not None and ancestor is not container:
            ancestor_attributes = " ".join(
                str(value)
                for key, value in getattr(ancestor, "attrs", {}).items()
                if key.lower() in {"id", "class", "itemprop", "data-testid", "aria-label"}
            ).lower()
            if re.search(r"(?:related|recommended|similar|team|office|brokerage|sidebar)", ancestor_attributes):
                excluded_ancestor = True
                break
            ancestor = getattr(ancestor, "parent", None)
        if excluded_ancestor:
            continue
        context = normalize_space(element.get_text(" ", strip=True))
        details = extract_bound_listing_agent_fields(context)
        bound = bind_agent_fields(details, group, "visible_listing_container", context)
        if bound:
            subject = bound["agent_subject_key"]
            existing = bound_by_subject.setdefault(subject, bound)
            values = contact_values.setdefault(subject, {"phone": set(), "email": set()})
            for field in ("phone", "email"):
                if bound.get(field):
                    values[field].add(bound[field])
                    existing.setdefault(field, bound[field])
    if len(bound_by_subject) != 1:
        return {}
    subject, result = next(iter(bound_by_subject.items()))
    conflicts = []
    if len(contact_values.get(subject, {}).get("phone", set())) > 1:
        conflicts.append("phone")
        for field in ("phone", "phone_source", "phone_evidence_group", "phone_contact_type", "phone_owner_key"):
            result.pop(field, None)
    if len(contact_values.get(subject, {}).get("email", set())) > 1:
        conflicts.append("email")
        for field in ("email", "email_source", "email_evidence_group", "email_contact_type", "email_owner_key"):
            result.pop(field, None)
    if conflicts:
        result["agent_contact_conflict"] = ",".join(conflicts)
    return result


def reconcile_bound_agent_fields(records: list[dict[str, str]]) -> dict[str, str]:
    """Keep one attributable subject while blanking cross-record contact conflicts."""
    bound_records = [record for record in records if record.get("agent_subject_key")]
    subjects = {record["agent_subject_key"] for record in bound_records}
    if len(subjects) != 1:
        return {}
    subject = next(iter(subjects))
    result = dict(bound_records[0])
    conflicts = {
        field
        for field in ("phone", "email")
        if len({record[field] for record in bound_records if record.get(field)}) > 1
    }
    for record in bound_records[1:]:
        for field, value in record.items():
            if value:
                result.setdefault(field, value)
    inherited_conflicts = {
        field
        for record in bound_records
        for field in record.get("agent_contact_conflict", "").split(",")
        if field
    }
    conflicts.update(inherited_conflicts)
    contact_fields = {
        "phone": ("phone", "phone_source", "phone_evidence_group", "phone_contact_type", "phone_owner_key"),
        "email": ("email", "email_source", "email_evidence_group", "email_contact_type", "email_owner_key"),
    }
    for conflict in conflicts:
        for field in contact_fields.get(conflict, ()):
            result.pop(field, None)
    if conflicts:
        result["agent_contact_conflict"] = ",".join(sorted(conflicts))
    result["agent_subject_key"] = subject
    return result


def visible_listing_evidence(markup: str, expected: dict[str, str]) -> dict[str, str]:
    """Return address, remarks, and status only when bound to one visible listing container."""
    expected_address = expected.get("listing_address", "")
    group = listing_evidence_group(expected_address, expected.get("state", ""), expected.get("zip", ""))
    candidates: list[tuple[str, str, dict[str, str]]] = []
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(markup, "html.parser")
        for element in soup.find_all(True):
            attributes = " ".join(
                str(value)
                for key, value in element.attrs.items()
                if key.lower() in {"id", "class", "itemprop", "data-testid", "aria-label"}
            ).lower()
            if re.search(r"(?:public[-_ ]?remarks|listing[-_ ]?description|property[-_ ]?description|remarks)", attributes):
                description = normalize_space(element.get_text(" ", strip=True))
                if not 20 <= len(description) <= 12_000:
                    continue
                container = element
                depth = 0
                while container.parent is not None and getattr(container.parent, "name", "") not in {"body", "html", "[document]"}:
                    container = container.parent
                    depth += 1
                    container_text = normalize_space(container.get_text(" ", strip=True))
                    container_attributes = " ".join(
                        str(value)
                        for key, value in getattr(container, "attrs", {}).items()
                        if key.lower() in {"id", "class", "itemprop", "data-testid", "aria-label"}
                    ).lower()
                    semantic_boundary = bool(
                        getattr(container, "name", "") in {"article", "section", "li", "aside"}
                        or re.search(r"(?:listing|property|home|mls|detail|card)", container_attributes)
                    )
                    main_boundary = getattr(container, "name", "") == "main"
                    if address_appears_in_text(expected_address, container_text) and (
                        depth == 1 or semantic_boundary
                    ):
                        candidates.append((description, container_text, visible_agent_subrecord(container, group)))
                        break
                    if semantic_boundary or main_boundary or depth >= 3:
                        break
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            label = normalize_space(heading.get_text(" ", strip=True))
            if not re.fullmatch(r"(?:public\s+remarks|listing\s+description|property\s+description|remarks)", label, re.I):
                continue
            sibling = heading.find_next_sibling()
            if sibling is not None:
                description = normalize_space(sibling.get_text(" ", strip=True))
                container = heading.parent
                container_text = normalize_space(container.get_text(" ", strip=True)) if container else ""
                container_attributes = " ".join(
                    str(value)
                    for key, value in getattr(container, "attrs", {}).items()
                    if key.lower() in {"id", "class", "itemprop", "data-testid", "aria-label"}
                ).lower() if container else ""
                semantic_container = bool(
                    container
                    and (
                        getattr(container, "name", "") in {"main", "article", "section", "li", "aside"}
                        or re.search(r"(?:listing|property|home|mls|detail|card)", container_attributes)
                    )
                )
                if (
                    20 <= len(description) <= 12_000
                    and semantic_container
                    and address_appears_in_text(expected_address, container_text)
                ):
                    candidates.append((description, container_text, visible_agent_subrecord(container, group)))
    except ImportError:
        pattern = re.compile(
            r"<(?P<tag>[a-z0-9]+)[^>]*(?:id|class|itemprop|data-testid|aria-label)=[\"'][^\"']*"
            r"(?:public[-_ ]?remarks|listing[-_ ]?description|property[-_ ]?description|remarks)[^\"']*[\"'][^>]*>"
            r"(?P<body>.*?)</(?P=tag)>",
            re.I | re.S,
        )
        for match in pattern.finditer(markup):
            description = strip_html(match.group("body"))
            if 20 <= len(description) <= 12_000 and address_appears_in_text(expected_address, description):
                candidates.append((description, description, {}))
    reviewable: list[tuple[str, str, str, dict[str, str]]] = []
    for description, container_text, agent_fields in candidates:
        status, status_evidence = current_listing_status(container_text)
        without_navigation = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.sub(" ", description)
        if SHORT_SALE_LISTING_RE.search(without_navigation):
            reviewable.append((description, status, status_evidence, agent_fields))
    if not reviewable:
        return {}
    listing_fingerprints = {
        (
            normalize_key(description),
            status,
            normalize_key(status_evidence),
        )
        for description, status, status_evidence, _agent_fields in reviewable
    }
    if len(listing_fingerprints) != 1:
        return {**expected, "listing_identity_ambiguous": "true"}
    description, status, status_evidence, _agent_fields = reviewable[0]
    evidence = {
        **expected,
        "exact_listing_confirmed": "true",
        "listing_identity_source": "visible_listing_container",
        "listing_identity_group": group,
        "listing_description": description,
        "listing_description_source": "visible_listing_description",
        "listing_description_group": group,
        "scoped_listing_status": status,
        "scoped_listing_status_evidence": status_evidence,
        "scoped_listing_status_source": "visible_listing_container",
        "scoped_listing_status_group": group,
    }
    evidence.update(reconcile_bound_agent_fields([item[3] for item in reviewable]))
    return evidence


def jsonld_address_fields(obj: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    address = obj.get("address")
    if isinstance(address, dict):
        fields = {
            "listing_address": str(address.get("streetAddress") or ""),
            "city": str(address.get("addressLocality") or ""),
            "state": str(address.get("addressRegion") or ""),
            "zip": str(address.get("postalCode") or ""),
        }
    elif isinstance(address, str):
        fields = parse_address_parts(address)
        fields.setdefault("listing_address", address)
    name = obj.get("name")
    if isinstance(name, str) and not looks_like_listing_address(fields.get("listing_address", "")):
        apply_address_parts(fields, parse_address_parts(name), replace_bad_address=True)
    normalize_candidate_address_fields(fields)
    return fields


def jsonld_bound_agent_fields(obj: dict[str, Any], group: str) -> dict[str, str]:
    nodes: list[dict[str, Any]] = []
    for key in ("listingAgent",):
        value = obj.get(key)
        if isinstance(value, dict):
            nodes.append(value)
        elif isinstance(value, list):
            nodes.extend(item for item in value if isinstance(item, dict))
    offers = obj.get("offers")
    if isinstance(offers, dict):
        for key in ("listingAgent",):
            value = offers.get(key)
            if isinstance(value, dict):
                nodes.append(value)
    bound_by_subject: dict[str, dict[str, str]] = {}
    contact_values: dict[str, dict[str, set[str]]] = {}
    for node in nodes:
        type_names = jsonld_type_names(node.get("@type"))
        if type_names and not type_names.intersection({"person", "realestateagent", "real estate agent"}):
            continue
        details = {
            "agent_name": str(node.get("name") or ""),
            "phone": str(node.get("mobilePhone") or node.get("telephone") or node.get("phone") or ""),
            "email": str(node.get("email") or ""),
        }
        context = " ".join(
            part for part in (
                "Mobile" if node.get("mobilePhone") else "",
                details["phone"],
                details["email"],
            ) if part
        )
        bound = bind_agent_fields(details, group, "jsonld_bound_listing_agent", context)
        if bound:
            subject = bound["agent_subject_key"]
            existing = bound_by_subject.setdefault(subject, bound)
            values = contact_values.setdefault(subject, {"phone": set(), "email": set()})
            for field in (
                "phone", "phone_source", "phone_evidence_group", "phone_contact_type", "phone_owner_key",
                "email", "email_source", "email_evidence_group", "email_contact_type", "email_owner_key",
            ):
                if bound.get(field):
                    existing.setdefault(field, bound[field])
            for field in ("phone", "email"):
                if bound.get(field):
                    values[field].add(bound[field])
    if len(bound_by_subject) != 1:
        return {}
    subject, result = next(iter(bound_by_subject.items()))
    conflicts = []
    if len(contact_values.get(subject, {}).get("phone", set())) > 1:
        conflicts.append("phone")
        for field in ("phone", "phone_source", "phone_evidence_group", "phone_contact_type", "phone_owner_key"):
            result.pop(field, None)
    if len(contact_values.get(subject, {}).get("email", set())) > 1:
        conflicts.append("email")
        for field in ("email", "email_source", "email_evidence_group", "email_contact_type", "email_owner_key"):
            result.pop(field, None)
    if conflicts:
        result["agent_contact_conflict"] = ",".join(conflicts)
    return result


def extract_jsonld_text(
    markup: str,
    expected: dict[str, str],
    source_url: str = "",
) -> tuple[str, dict[str, str]]:
    matches: list[tuple[int, dict[str, str]]] = []
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
            type_names = jsonld_type_names(obj.get("@type"))
            listing_object = bool(type_names.intersection(LISTING_JSONLD_TYPES))
            name = obj.get("name")
            description = obj.get("description")
            if isinstance(description, str):
                pieces.append(description)
            if not listing_object:
                continue
            object_fields = jsonld_address_fields(obj)
            expected_present = looks_like_listing_address(expected.get("listing_address", ""))
            if expected_present and not listing_fields_match_expected(object_fields, expected):
                continue
            if not expected_present and not listing_url_matches_address(
                source_url,
                object_fields.get("listing_address", ""),
            ):
                continue
            group = listing_evidence_group(
                object_fields.get("listing_address", ""),
                object_fields.get("state", ""),
                object_fields.get("zip", ""),
            )
            object_fields.update({
                "raw_name": str(name or ""),
                "exact_listing_confirmed": "true",
                "listing_identity_source": "jsonld_listing_object",
                "listing_identity_group": group,
            })
            object_fields.update(jsonld_bound_agent_fields(obj, group))
            if isinstance(description, str) and normalize_space(description):
                object_fields["listing_description"] = normalize_space(description)
                object_fields["listing_description_source"] = "jsonld_listing_object"
                object_fields["listing_description_group"] = group
            status, status_evidence = jsonld_listing_status(obj)
            object_fields["scoped_listing_status"] = status
            object_fields["scoped_listing_status_evidence"] = status_evidence
            object_fields["scoped_listing_status_source"] = "jsonld_listing_object"
            object_fields["scoped_listing_status_group"] = group
            score = int(bool(object_fields.get("listing_description"))) + int(status != "unknown")
            matches.append((score, object_fields))
    if not matches:
        return "\n".join(pieces), {}
    matches.sort(key=lambda item: item[0], reverse=True)
    best_score = matches[0][0]
    best = [item[1] for item in matches if item[0] == best_score]
    fingerprints = {
        (
            normalize_key(item.get("listing_description", "")),
            item.get("scoped_listing_status", ""),
            item.get("scoped_listing_status_evidence", ""),
        )
        for item in best
    }
    if len(fingerprints) > 1:
        return "\n".join(pieces), {
            **expected,
            "listing_identity_ambiguous": "true",
        }
    result = dict(best[0])
    for field in list(result):
        if field.startswith(("agent_", "phone", "email")):
            result.pop(field, None)
    result.update(reconcile_bound_agent_fields(best))
    return "\n".join(pieces), result


def decode_json_string(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except json.JSONDecodeError:
        return html.unescape(value.replace(r"\/", "/"))


def extract_embedded_listing_text(markup: str) -> tuple[str, str]:
    pieces: list[str] = []
    descriptions: list[str] = []
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
            descriptions.append(decoded)
            pieces.append(f"Property description: {decoded}")
    for match in re.finditer(r'"text"\s*:\s*"((?:\\.|[^"\\]){20,5000})"', markup, re.I):
        decoded = normalize_space(decode_json_string(match.group(1)))
        if decoded:
            descriptions.append(decoded)
            pieces.append(f"Property description: {decoded}")
    for match in re.finditer(r'"number"\s*:\s*"((?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4})"', markup, re.I):
        pieces.append(f"Phone: {decode_json_string(match.group(1))}")
    preferred_description = next(
        (description for description in descriptions if SHORT_SALE_LISTING_RE.search(description)),
        descriptions[0] if descriptions else "",
    )
    return " ".join(pieces), preferred_description


def iter_json_objects(data: Any) -> Iterable[Any]:
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from iter_json_objects(value)
    elif isinstance(data, list):
        for item in data:
            yield from iter_json_objects(item)


def infer_fields(result: SearchResult, markup: str) -> Candidate:
    expected = expected_listing_fields(result)
    json_text, json_fields = extract_jsonld_text(markup, expected, result.url)
    embedded_listing_text, embedded_description = extract_embedded_listing_text(markup)
    page_text = strip_html(markup)
    combined = normalize_space(" ".join([result.title, result.snippet, json_text, embedded_listing_text, page_text]))

    fields = dict(expected)
    fields.update(json_fields)
    if embedded_description:
        fields.setdefault("untrusted_description_hint", embedded_description)
    if not looks_like_listing_address(fields.get("listing_address", "")):
        apply_address_parts(fields, parse_address_parts(result.title), replace_bad_address=True)

    visible_fields = visible_listing_evidence(markup, expected)
    json_complete = bool(
        fields.get("listing_identity_group")
        and fields.get("listing_description_group")
        and fields.get("scoped_listing_status_group")
    )
    visible_complete = bool(visible_fields.get("listing_description"))
    if visible_complete and not json_complete:
        fields.update(visible_fields)

    fields.setdefault("source_url", result.url)

    if not fields.get("city") or not fields.get("state"):
        city_state = re.search(r"\b([A-Z][A-Za-z .'-]{2,40}),\s*([A-Z]{2})\s*(\d{5})?\b", result.title + " " + result.snippet)
        if city_state:
            fields.setdefault("city", normalize_space(city_state.group(1)))
            fields.setdefault("state", city_state.group(2))
            fields.setdefault("zip", city_state.group(3) or "")

    normalize_candidate_address_fields(fields)
    candidate = Candidate(result.source, result.query, result.url, result.title, combined, fields)
    sanitize_candidate_identity(candidate)
    return candidate


def direct_monitor_active(run_date: dt.date) -> bool:
    if not DIRECT_MONITOR_ENABLED:
        return False
    try:
        start = dt.date.fromisoformat(DIRECT_MONITOR_START_DATE)
    except ValueError:
        return False
    return start <= run_date < start + dt.timedelta(days=DIRECT_MONITOR_DAYS)


def direct_monitor_family_limits() -> dict[str, int]:
    """Return configured family caps while preserving the global URL ceiling."""
    remaining = DIRECT_MONITOR_MAX_URLS
    limits: dict[str, int] = {}
    for family in DIRECT_MONITOR_FEEDS:
        configured = DIRECT_MONITOR_FAMILY_LIMITS.get(family, 0)
        limits[family] = min(configured, remaining)
        remaining -= limits[family]
    return limits


def fetch_public_feed(url: str, timeout: int = 20, max_bytes: int = 6_000_000) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(max_bytes)
        encoding = resp.headers.get_content_charset() or "utf-8"
    return raw.decode(encoding, errors="ignore")


def sitemap_entries(markup: str) -> list[tuple[str, str]]:
    root = ET.fromstring(markup)
    entries: list[tuple[str, str]] = []
    for item in list(root):
        loc = ""
        lastmod = ""
        for child in list(item):
            tag = child.tag.rsplit("}", 1)[-1].lower()
            if tag == "loc":
                loc = normalize_space(child.text or "")
            elif tag == "lastmod":
                lastmod = normalize_space(child.text or "")
        if loc.startswith("http"):
            entries.append((loc, lastmod))
    return entries


def is_direct_listing_url(family: str, url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.rstrip("/").lower()
    if family == "momentum":
        return bool(re.search(r"^/listings/idx(?:-[a-z0-9]+)?/\d", path))
    if family == "coldwell":
        return "/lid-" in path
    return False


def collect_direct_monitor_urls(
    family: str,
    feeds: tuple[str, ...],
    *,
    run_date: dt.date,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []
    per_feed = max(1, (limit + len(feeds) - 1) // len(feeds))
    try:
        start = dt.date.fromisoformat(DIRECT_MONITOR_START_DATE)
        day_offset = max(0, (run_date - start).days)
    except ValueError:
        day_offset = 0
    selected: list[str] = []
    seen: set[str] = set()
    for feed in feeds:
        documents = [feed]
        listing_entries: list[tuple[str, str]] = []
        while documents and len(listing_entries) < (day_offset + 1) * per_feed:
            document = documents.pop(0)
            markup = fetch_public_feed(document)
            entries = sitemap_entries(markup)
            child_sitemaps = [url for url, _ in entries if url.lower().endswith(".xml")]
            if child_sitemaps:
                documents.extend(child_sitemaps[:8])
                continue
            listing_entries.extend(
                (url, lastmod)
                for url, lastmod in entries
                if is_direct_listing_url(family, url)
            )
        listing_entries.sort(key=lambda item: (item[1], item[0]), reverse=True)
        if family == "momentum" and listing_entries:
            count = min(per_feed, len(listing_entries))
            sample_indexes = [
                ((slot * len(listing_entries)) // count + day_offset) % len(listing_entries)
                for slot in range(count)
            ]
            sampled_entries = [listing_entries[index] for index in sample_indexes]
        else:
            offset = 0 if "new-day" in feed else day_offset * per_feed
            sampled_entries = listing_entries[offset : offset + per_feed]
        for url, _ in sampled_entries:
            if url not in seen:
                selected.append(url)
                seen.add(url)
            if len(selected) >= limit:
                return selected
    return selected[:limit]


def log_agent_shadow(candidate: Candidate, *, monitor_family: str = "") -> bool:
    shadow = shadow_listing_agent_candidate(candidate)
    if not shadow:
        return False
    log_event(
        "pilot_agent_shadow_candidate",
        url=candidate.url,
        source=candidate.source,
        monitor_family=monitor_family,
        current_agent=candidate.fields.get("agent_name", ""),
        shadow_agent=shadow["agent_name"],
        label=shadow["label"],
        would_change=normalize_key(shadow["agent_name"])
        != normalize_key(candidate.fields.get("agent_name", "")),
        promotion_changed=False,
    )
    record_two_source_agent_shadow(candidate, shadow=shadow)
    return True


_agent_shadow_observations: dict[str, dict[str, dict[str, Any]]] = {}
_agent_shadow_consensus_logged: set[str] = set()


def reset_agent_shadow_consensus_state() -> None:
    _agent_shadow_observations.clear()
    _agent_shadow_consensus_logged.clear()


def reset_site_chrome_prewrite_state() -> None:
    global _site_chrome_prewrite_count
    _site_chrome_prewrite_seen.clear()
    _site_chrome_prewrite_count = 0


def record_two_source_agent_shadow(
    candidate: Candidate,
    *,
    shadow: dict[str, str] | None = None,
) -> bool:
    """Log a zero-write agent fallback only after two independent domains agree."""
    if AGENT_SHADOW_CONSENSUS_CAP <= 0:
        return False
    listing_key = canonical_listing_address_key(
        candidate.fields.get("listing_address", ""),
        candidate.fields.get("state", ""),
    )
    domain = urllib.parse.urlparse(candidate.url).netloc.lower()
    domain = re.sub(r"^www\.", "", domain)
    if not listing_key or not domain:
        return False

    current_name = clean_agent_name(candidate.fields.get("agent_name", ""))
    if current_name:
        probe = Candidate(
            source=candidate.source,
            query=candidate.query,
            url=candidate.url,
            title=candidate.title,
            text=candidate.text,
            fields=dict(candidate.fields, agent_name=current_name),
        )
        safe, _ = agent_name_promotion_safety(probe)
        if not safe:
            current_name = ""
    proposed_name = current_name or (shadow or {}).get("agent_name", "")
    proposed_name = clean_agent_name(proposed_name)
    name_key = normalize_key(proposed_name)
    if not name_key:
        return False

    listing_observations = _agent_shadow_observations.setdefault(listing_key, {})
    observation = listing_observations.setdefault(
        name_key,
        {"agent_name": proposed_name, "domains": set()},
    )
    observation["domains"].add(domain)
    consensus_key = f"{listing_key}|{name_key}"
    if (
        len(observation["domains"]) < 2
        or consensus_key in _agent_shadow_consensus_logged
        or len(_agent_shadow_consensus_logged) >= AGENT_SHADOW_CONSENSUS_CAP
    ):
        return False

    _agent_shadow_consensus_logged.add(consensus_key)
    log_event(
        "pilot_agent_shadow_two_source_consensus",
        listing_key=listing_key,
        address=candidate.fields.get("listing_address", ""),
        state=candidate.fields.get("state", ""),
        shadow_agent=observation["agent_name"],
        domains=sorted(observation["domains"]),
        source_count=len(observation["domains"]),
        would_change=not bool(current_name),
        promotion_changed=False,
        writes=0,
    )
    return True


def run_direct_monitor(
    run_date: dt.date,
    already_seen_urls: set[str],
    existing: ExistingIndex,
    pilot_seen_addresses: set[str],
    *,
    sleep_seconds: float = 0.0,
) -> dict[str, Any]:
    stats = {
        "active": direct_monitor_active(run_date),
        "families_planned": 0,
        "families_succeeded": 0,
        "families_failed": 0,
        "complete": True,
        "selected": 0,
        "fetched": 0,
        "fetch_failed": 0,
        "fetch_failure_reasons": {},
        "qualified": 0,
        "net_new_qualified": 0,
        "duplicates": 0,
        "rejected": 0,
        "agent_shadow_candidates": 0,
        "rows_written": 0,
    }
    if not direct_monitor_active(run_date):
        log_event(
            "pilot_direct_monitor_skipped",
            run_date=run_date.isoformat(),
            enabled=DIRECT_MONITOR_ENABLED,
            start_date=DIRECT_MONITOR_START_DATE,
            days=DIRECT_MONITOR_DAYS,
        )
        return stats

    family_limits = direct_monitor_family_limits()
    families = [family for family in DIRECT_MONITOR_FEEDS if family_limits.get(family, 0) > 0]
    stats["families_planned"] = len(families)
    if not families:
        stats["complete"] = False
    log_event(
        "pilot_direct_monitor_start",
        run_date=run_date.isoformat(),
        families=families,
        family_limits=family_limits,
        max_urls=DIRECT_MONITOR_MAX_URLS,
        comparison_window_days=DIRECT_MONITOR_DAYS,
        hypothesis="momentum_heavy_free_monitoring_increases_net_new_qualified_yield",
        success_metric="momentum_produces_at_least_two_net_new_qualified_and_outyields_coldwell",
        stop_condition="any_write_or_promotion_or_daily_cap_breach_or_first_access_control_concern",
        shadow_only=True,
    )
    for family in families:
        family_limit = family_limits[family]
        try:
            urls = collect_direct_monitor_urls(
                family,
                DIRECT_MONITOR_FEEDS[family],
                run_date=run_date,
                limit=family_limit,
            )
        except Exception as exc:  # noqa: BLE001
            stats["families_failed"] += 1
            stats["complete"] = False
            log_event("pilot_direct_monitor_feed_failed", family=family, error=str(exc)[:500])
            continue
        stats["families_succeeded"] += 1
        for url in urls:
            if stats["selected"] >= DIRECT_MONITOR_MAX_URLS:
                break
            stats["selected"] += 1
            try:
                markup = fetch_url(url, timeout=12, allow_headless=False)
                stats["fetched"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["fetch_failed"] += 1
                increment_reason(stats, "fetch_failure_reasons", source_failure_reason(exc))
                log_event("pilot_direct_monitor_fetch_failed", family=family, url=url, error=str(exc)[:500])
                continue
            result = SearchResult(
                source=f"direct_monitor:{family}",
                query=DIRECT_MONITOR_FEEDS[family][0],
                url=url,
                title="",
                snippet="",
            )
            candidate = infer_fields(result, markup)
            if log_agent_shadow(candidate, monitor_family=family):
                stats["agent_shadow_candidates"] += 1
            qualification = qualification_for_candidate(candidate)
            log_site_chrome_prewrite_receipt(
                candidate,
                qualification,
                state=candidate.fields.get("state", ""),
                source=result.source,
                run_date=run_date,
            )
            rejection = required_review_field_failure(candidate, qualification)
            if rejection:
                stats["rejected"] += 1
                log_event(
                    "pilot_direct_monitor_candidate",
                    family=family,
                    url=url,
                    status="rejected",
                    reason=rejection if qualification.status == "qualified" else qualification.failure_reason,
                    rows_written=0,
                )
                continue
            listing_status, listing_key, matched = duplicate_listing_status(candidate, existing)
            pilot_duplicate = listing_key and listing_key in pilot_seen_addresses
            source_ref = safe_source_reference(url)
            url_duplicate = url in already_seen_urls or source_ref in already_seen_urls
            if listing_status or pilot_duplicate or url_duplicate:
                stats["duplicates"] += 1
                log_event(
                    "pilot_direct_monitor_candidate",
                    family=family,
                    url=url,
                    status="duplicate",
                    reason=listing_status or ("pilot_listing" if pilot_duplicate else "pilot_url"),
                    matched=matched,
                    rows_written=0,
                )
                continue
            stats["qualified"] += 1
            stats["net_new_qualified"] += 1
            log_event(
                "pilot_direct_monitor_candidate",
                family=family,
                url=url,
                status="net_new_qualified_shadow",
                address=candidate.fields.get("listing_address", ""),
                agent=candidate.fields.get("agent_name", ""),
                rows_written=0,
            )
            if sleep_seconds:
                time.sleep(sleep_seconds)
    log_event("pilot_direct_monitor_done", stats=stats, shadow_only=True)
    return stats


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
    listing_description = fields.get("listing_description", "") or strict_listing_description_evidence(candidate)
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
        "agentNameSource": fields.get("agent_name_source", ""),
        "agentEvidenceGroup": fields.get("agent_evidence_group", ""),
        "agentSubjectKey": fields.get("agent_subject_key", ""),
        "brokerName": fields.get("broker_name", ""),
        "brokerageName": fields.get("broker_name", ""),
        "phone": fields.get("phone", ""),
        "phoneSource": fields.get("phone_source", ""),
        "phoneEvidenceGroup": fields.get("phone_evidence_group", ""),
        "phoneContactType": fields.get("phone_contact_type", ""),
        "phoneOwnerKey": fields.get("phone_owner_key", ""),
        "email": fields.get("email", ""),
        "emailSource": fields.get("email_source", ""),
        "emailEvidenceGroup": fields.get("email_evidence_group", ""),
        "emailContactType": fields.get("email_contact_type", ""),
        "emailOwnerKey": fields.get("email_owner_key", ""),
        "contactPhoneHint": fields.get("contact_phone_hint", ""),
        "contactPhoneHintType": fields.get("contact_phone_hint_type", ""),
        "contactEmailHint": fields.get("contact_email_hint", ""),
        "contactEmailHintType": fields.get("contact_email_hint_type", ""),
        "sourceReference": safe_source_reference(candidate.url),
        "sourceEvidenceState": fields.get("source_evidence_state", "evidence_gap"),
        "sourceEvidenceReceipt": fields.get("source_evidence_receipt", ""),
        "homeStatus": "FOR_SALE",
        "specialListingConditions": "Short Sale",
        "listing_description": listing_description[:8_000],
        "description": listing_description[:8_000],
        "listingText": listing_description[:8_000],
        "listingDescriptionSource": fields.get("listing_description_source", ""),
        "listingDescriptionGroup": fields.get("listing_description_group", ""),
        "exactListingConfirmed": fields.get("exact_listing_confirmed", ""),
        "listingIdentitySource": fields.get("listing_identity_source", ""),
        "listingIdentityGroup": fields.get("listing_identity_group", ""),
        "scopedListingStatus": fields.get("scoped_listing_status", ""),
        "scopedListingStatusEvidence": fields.get("scoped_listing_status_evidence", ""),
        "scopedListingStatusSource": fields.get("scoped_listing_status_source", ""),
        "scopedListingStatusGroup": fields.get("scoped_listing_status_group", ""),
        "sourceQuery": candidate.query,
        "sourceTitle": candidate.title,
        "qualificationEvidence": qualification.evidence,
        "sourcePilotShadow": "true",
        "requiresVerifierReview": "true",
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
    promotion_status, is_shadow_ready, promotion_notes = shadow_promotion_readiness(candidate, qualification)
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
    import_ready = "yes" if is_shadow_ready else "review"
    if matched or agent_rows:
        promotion_notes += " Existing agent/contact rows are recorded for reviewer context."
    sheet_payload = sanitize_payload_for_sheet_json(payload)
    return [
        sanitize_external_links_for_sheet(first_name),
        sanitize_external_links_for_sheet(last_name),
        sanitize_external_links_for_sheet(fields.get("phone", "")),
        sanitize_external_links_for_sheet(fields.get("email", "")),
        sanitize_external_links_for_sheet(fields.get("listing_address", "")),
        sanitize_external_links_for_sheet(fields.get("city", "")),
        sanitize_external_links_for_sheet(fields.get("state", "")),
        now,
        synthetic_zpid,
        sanitize_external_links_for_sheet(candidate.source),
        sanitize_external_links_for_sheet(candidate.query),
        safe_source_reference(candidate.url),
        sanitize_external_links_for_sheet(qualification.status),
        sanitize_external_links_for_sheet(qualification.failure_reason),
        sanitize_external_links_for_sheet(promotion_status),
        sanitize_external_links_for_sheet(promotion_notes),
        import_ready,
        sanitize_external_links_for_sheet(fields.get("zip", "")),
        sanitize_external_links_for_sheet(fields.get("broker_name", "")),
        sanitize_external_links_for_sheet(qualification.short_sale_evidence_type),
        sanitize_external_links_for_sheet(qualification.evidence),
        sanitize_external_links_for_sheet(qualification.disqualifying_terms),
        sanitize_external_links_for_sheet(duplicate_key),
        sanitize_external_links_for_sheet(matched),
        sanitize_external_links_for_sheet(agent_rows),
        sanitize_external_links_for_sheet(queue_source),
        sanitize_external_links_for_sheet(queue_address),
        json.dumps(sheet_payload, separators=(",", ":"), ensure_ascii=False),
        sanitize_external_links_for_sheet(candidate.text[:900]),
        sanitize_external_links_for_sheet(candidate.title),
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


def ensure_headers_tab(
    token: str,
    spreadsheet_id: str,
    tab_name: str,
    headers: list[str],
    *,
    hidden: bool = False,
) -> None:
    meta = sheets_request(
        token,
        "GET",
        f"{spreadsheet_id}?fields=sheets.properties",
    )
    matching = next(
        (
            sheet.get("properties", {})
            for sheet in meta.get("sheets", [])
            if sheet.get("properties", {}).get("title") == tab_name
        ),
        None,
    )
    if matching is None:
        properties: dict[str, Any] = {
            "title": tab_name,
            "gridProperties": {"columnCount": len(headers)},
        }
        if hidden:
            properties["hidden"] = True
        sheets_request(
            token,
            "POST",
            f"{spreadsheet_id}:batchUpdate",
            {"requests": [{"addSheet": {"properties": properties}}]},
        )
    elif hidden and not matching.get("hidden"):
        sheets_request(
            token,
            "POST",
            f"{spreadsheet_id}:batchUpdate",
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": matching["sheetId"],
                                "hidden": True,
                            },
                            "fields": "hidden",
                        }
                    }
                ]
            },
        )
    header_range = f"{tab_name}!A1:{column_letter(len(headers))}1"
    values = get_values(token, spreadsheet_id, header_range)
    if not values or values[0] != headers:
        update_values(token, spreadsheet_id, header_range, [headers])


def _source_evidence_receipt_id(stable_id: str, exact_url: str) -> str:
    source_digest = hashlib.sha256(exact_url.encode("utf-8")).hexdigest()
    return "pse:v1:" + hashlib.sha256(
        f"{stable_id}|{source_digest}".encode("utf-8")
    ).hexdigest()[:24]


def _source_evidence_values(candidate: Candidate, captured_at: dt.datetime) -> list[str]:
    exact_url = normalize_space(candidate.url)
    if not exact_url or urllib.parse.urlparse(exact_url).scheme not in {"http", "https"}:
        raise ValueError("exact listing source URL must use http or https")
    fields = candidate.fields
    stable_id = stable_synthetic_zpid(
        candidate.source,
        exact_url,
        fields.get("listing_address", ""),
        fields.get("city", ""),
        fields.get("state", ""),
    )
    receipt_id = _source_evidence_receipt_id(stable_id, exact_url)
    qualification_basis = "|".join(
        [
            normalize_space(fields.get("listing_identity_group", "")),
            normalize_space(fields.get("scoped_listing_status", "")),
            normalize_space(fields.get("listing_description", "")),
        ]
    )
    encoded_url = base64.urlsafe_b64encode(exact_url.encode("utf-8")).decode("ascii")
    return [
        receipt_id,
        captured_at.astimezone(dt.timezone.utc).isoformat(),
        stable_id,
        safe_source_reference(exact_url),
        encoded_url,
        normalize_space(fields.get("listing_identity_group", "")),
        hashlib.sha256(qualification_basis.encode("utf-8")).hexdigest(),
        "durable_reopenable",
    ]


def resolve_source_evidence_receipt(
    token: str,
    spreadsheet_id: str,
    receipt_id: str,
) -> str:
    """Resolve and validate one durable receipt without exposing it in lead rows."""
    evidence_range = f"{SOURCE_EVIDENCE_TAB}!A:{column_letter(len(SOURCE_EVIDENCE_HEADERS))}"
    rows = get_values(token, spreadsheet_id, evidence_range)
    if not rows or rows[0] != SOURCE_EVIDENCE_HEADERS:
        raise RuntimeError("source evidence owner tab headers are missing or changed")
    matching = [row for row in rows[1:] if row and row[0] == receipt_id]
    if len(matching) != 1:
        raise RuntimeError("source evidence receipt missing or duplicated")
    stored = matching[0] + [""] * (len(SOURCE_EVIDENCE_HEADERS) - len(matching[0]))
    if stored[7] != "durable_reopenable":
        raise RuntimeError("source evidence receipt is not reopenable")
    try:
        exact_url = base64.urlsafe_b64decode(stored[4].encode("ascii")).decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError("source evidence URL encoding is invalid") from exc
    if urllib.parse.urlparse(exact_url).scheme not in {"http", "https"}:
        raise RuntimeError("source evidence URL scheme is invalid")
    if stored[3] != safe_source_reference(exact_url):
        raise RuntimeError("source evidence URL does not match its safe reference")
    if stored[0] != _source_evidence_receipt_id(stored[2], exact_url):
        raise RuntimeError("source evidence receipt integrity check failed")
    return exact_url


def persist_candidate_source_evidence(
    token: str,
    spreadsheet_id: str,
    candidate: Candidate,
    *,
    captured_at: dt.datetime | None = None,
) -> str:
    """Persist and reread an exact URL before allowing the candidate to promote."""
    try:
        evidence_values = _source_evidence_values(
            candidate,
            captured_at or dt.datetime.now(dt.timezone.utc),
        )
        receipt_id = evidence_values[0]
        ensure_headers_tab(
            token,
            spreadsheet_id,
            SOURCE_EVIDENCE_TAB,
            SOURCE_EVIDENCE_HEADERS,
            hidden=True,
        )
        evidence_range = (
            f"{SOURCE_EVIDENCE_TAB}!A:{column_letter(len(SOURCE_EVIDENCE_HEADERS))}"
        )
        rows = get_values(token, spreadsheet_id, evidence_range)
        matching = [row for row in rows[1:] if row and row[0] == receipt_id]
        if not matching:
            append_values(token, spreadsheet_id, evidence_range, [evidence_values])
            rows = get_values(token, spreadsheet_id, evidence_range)
            matching = [row for row in rows[1:] if row and row[0] == receipt_id]
        if len(matching) != 1:
            raise RuntimeError("source evidence receipt missing or duplicated after write")
        stored = matching[0] + [""] * (len(SOURCE_EVIDENCE_HEADERS) - len(matching[0]))
        immutable_indexes = (0, 2, 3, 5, 6, 7)
        if any(stored[index] != evidence_values[index] for index in immutable_indexes):
            raise RuntimeError("source evidence receipt does not match candidate")
        resolved_url = resolve_source_evidence_receipt(token, spreadsheet_id, receipt_id)
        if resolved_url != normalize_space(candidate.url):
            raise RuntimeError("source evidence URL failed exact reopen readback")
        candidate.fields["source_evidence_state"] = "durable_reopenable"
        candidate.fields["source_evidence_receipt"] = receipt_id
        log_event(
            "pilot_source_evidence_persisted",
            receipt_id=receipt_id,
            source_reference=evidence_values[3],
            evidence_tab=SOURCE_EVIDENCE_TAB,
        )
        return receipt_id
    except Exception as exc:  # noqa: BLE001
        candidate.fields["source_evidence_state"] = "evidence_gap"
        candidate.fields["source_evidence_receipt"] = ""
        log_event(
            "pilot_source_evidence_persistence_failed",
            source_reference=safe_source_reference(candidate.url),
            error_type=type(exc).__name__,
            error=str(exc)[:300],
        )
        return ""


def append_run_slot_receipt(
    token: str,
    spreadsheet_id: str,
    *,
    schedule_slot_id: str,
    run_receipt_id: str,
    run_date: dt.date,
    run_mode: str,
    status: str,
    pipeline_complete: bool,
    detail: str = "",
    observed_at: dt.datetime | None = None,
) -> None:
    timestamp = observed_at or dt.datetime.now(dt.timezone.utc)
    append_values(
        token,
        spreadsheet_id,
        f"{RUN_RECEIPT_TAB}!A:{column_letter(len(RUN_RECEIPT_HEADERS))}",
        [[
            schedule_slot_id,
            run_receipt_id,
            run_date.isoformat(),
            run_mode,
            status,
            timestamp.isoformat(),
            "true" if pipeline_complete else "false",
            normalize_space(detail)[:500],
        ]],
    )


def source_query_key(state: str, source: str, result_start: int = 1) -> str:
    key = f"{normalize_space(state).upper()}:{normalize_space(source)}"
    page = ((max(1, result_start) - 1) // 10) + 1
    return key if page == 1 else f"{key}:p{page}"


def recovery_manifest_detail(prefix: str, query_keys: Iterable[str]) -> str:
    keys = sorted({normalize_space(key) for key in query_keys if normalize_space(key)})
    return f"{prefix}{','.join(keys)}"


def source_query_recovery_limit(run_date: dt.date) -> int:
    if experiment_active(
        run_date,
        SOURCE_QUERY_RECOVERY_EXPERIMENT_START_DATE,
        SOURCE_QUERY_RECOVERY_EXPERIMENT_DAYS,
    ):
        return MAX_SOURCE_QUERY_RECOVERY_EXPERIMENT
    return MAX_SOURCE_QUERY_RECOVERY


def parse_recovery_query_keys(
    detail: str,
    prefix: str = RECOVERY_PENDING_PREFIX,
    *,
    max_recovery_queries: int = MAX_SOURCE_QUERY_RECOVERY,
) -> list[str]:
    normalized = normalize_space(detail)
    marker = normalized.find(prefix)
    if marker < 0:
        return []
    raw = normalized[marker + len(prefix):].split(";", 1)[0]
    keys = sorted({key.strip() for key in raw.split(",") if key.strip()})
    if not keys or len(keys) > max_recovery_queries:
        return []
    if any(not re.fullmatch(r"[A-Z]{2}:[a-z0-9_]+(?::p[2-9][0-9]*)?", key) for key in keys):
        return []
    return keys


def claim_run_schedule_slot(
    token: str,
    spreadsheet_id: str,
    *,
    schedule_slot_id: str,
    run_receipt_id: str,
    run_date: dt.date,
    run_mode: str,
    now: dt.datetime | None = None,
) -> tuple[bool, str, list[str]]:
    """Atomically converge concurrent attempts on one durable Sheet-backed slot winner."""
    if not schedule_slot_id:
        return True, "unslotted", []
    current = now or dt.datetime.now(dt.timezone.utc)
    ensure_headers_tab(token, spreadsheet_id, RUN_RECEIPT_TAB, RUN_RECEIPT_HEADERS)
    rows = get_values(
        token,
        spreadsheet_id,
        f"{RUN_RECEIPT_TAB}!A:{column_letter(len(RUN_RECEIPT_HEADERS))}",
    )
    if run_mode == "link_audit" and schedule_slot_id.startswith("post_verifier_audit:"):
        slot_date = schedule_slot_id.split(":", 1)[1]
        source_slot_id = f"source:{slot_date}"
        source_completed_rows = [
            row
            for row in rows[1:]
            if (
                len(row) > 6
                and row[0] == source_slot_id
                and row[4] == "completed"
                and row[6].lower() == "true"
            )
        ]
        if not source_completed_rows:
            return False, "source_slot_not_completed", []
        source_completed_times: list[dt.datetime] = []
        for row in source_completed_rows:
            try:
                completed_at = dt.datetime.fromisoformat(row[5].replace("Z", "+00:00"))
            except (IndexError, ValueError):
                continue
            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(tzinfo=dt.timezone.utc)
            source_completed_times.append(completed_at.astimezone(dt.timezone.utc))
        if not source_completed_times:
            return False, "source_terminal_time_unconfirmed", []
        current_utc = current
        if current_utc.tzinfo is None:
            current_utc = current_utc.replace(tzinfo=dt.timezone.utc)
        current_utc = current_utc.astimezone(dt.timezone.utc)
        grace_deadline = max(source_completed_times) + dt.timedelta(
            minutes=POST_SOURCE_AUDIT_GRACE_MINUTES
        )
        if current_utc < grace_deadline:
            return False, "source_grace_not_elapsed", []
    slot_rows = [row for row in rows[1:] if row and row[0] == schedule_slot_id]
    if any(len(row) > 6 and row[4] == "completed" and row[6].lower() == "true" for row in slot_rows):
        return False, "already_completed", []
    recovery_query_keys: list[str] = []
    if run_mode == "scheduled_source":
        substantive_terminal_rows = [
            row for row in slot_rows
            if len(row) > 4 and row[4] in {"completed", "completed_degraded", "failed"}
        ]
        latest_terminal = substantive_terminal_rows[-1] if substantive_terminal_rows else []
        latest_status = latest_terminal[4] if len(latest_terminal) > 4 else ""
        latest_detail = latest_terminal[7] if len(latest_terminal) > 7 else ""
        if latest_status == "completed_degraded":
            recovery_query_keys = parse_recovery_query_keys(
                latest_detail,
                max_recovery_queries=source_query_recovery_limit(run_date),
            )
            if not recovery_query_keys:
                return False, "recovery_unavailable", []
        elif latest_status == "failed" and RECOVERY_EXHAUSTED_PREFIX in latest_detail:
            return False, "recovery_exhausted", []
    append_run_slot_receipt(
        token,
        spreadsheet_id,
        schedule_slot_id=schedule_slot_id,
        run_receipt_id=run_receipt_id,
        run_date=run_date,
        run_mode=run_mode,
        status="running",
        pipeline_complete=False,
        detail=recovery_manifest_detail(RECOVERY_ATTEMPT_PREFIX, recovery_query_keys)
        if recovery_query_keys else "",
        observed_at=current,
    )
    rows = get_values(
        token,
        spreadsheet_id,
        f"{RUN_RECEIPT_TAB}!A:{column_letter(len(RUN_RECEIPT_HEADERS))}",
    )
    slot_rows = [row for row in rows[1:] if row and row[0] == schedule_slot_id]
    if any(len(row) > 6 and row[4] == "completed" and row[6].lower() == "true" for row in slot_rows):
        return False, "already_completed", []
    cutoff = current - dt.timedelta(minutes=RUN_RECEIPT_STALE_MINUTES)
    terminal_attempt_ids = {
        row[1]
        for row in slot_rows
        if len(row) > 4 and row[4] != "running" and len(row) > 1 and row[1]
    }
    contenders: list[tuple[int, str]] = []
    for row_index, row in enumerate(slot_rows):
        if len(row) < 6 or row[4] != "running":
            continue
        if len(row) > 1 and row[1] in terminal_attempt_ids:
            continue
        try:
            observed = dt.datetime.fromisoformat(row[5].replace("Z", "+00:00"))
        except ValueError:
            continue
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=dt.timezone.utc)
        if observed >= cutoff:
            contenders.append((row_index, row[1] if len(row) > 1 else ""))
    winner = min(contenders, default=(len(slot_rows), run_receipt_id))
    if winner[1] != run_receipt_id:
        append_run_slot_receipt(
            token,
            spreadsheet_id,
            schedule_slot_id=schedule_slot_id,
            run_receipt_id=run_receipt_id,
            run_date=run_date,
            run_mode=run_mode,
            status="skipped_lost_claim",
            pipeline_complete=False,
            detail=f"winner={winner[1]}",
        )
        return False, "active_attempt", []
    return True, "claimed_recovery" if recovery_query_keys else "claimed", recovery_query_keys


def batch_update_values(token: str, spreadsheet_id: str, updates: list[dict[str, Any]]) -> None:
    if not updates:
        return
    sheets_request(
        token,
        "POST",
        f"{spreadsheet_id}/values:batchUpdate",
        {"valueInputOption": "RAW", "data": updates},
    )


def row_value(row: list[str], header: str) -> str:
    try:
        idx = PILOT_HEADERS.index(header)
    except ValueError:
        return ""
    return row[idx] if len(row) > idx else ""


def pilot_row_map(row: list[str]) -> dict[str, str]:
    return {header: row_value(row, header) for header in PILOT_HEADERS}


def normalized_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalize_space(value).lower()).strip("_")


def sheet_row_maps(rows: list[list[str]]) -> list[tuple[int, dict[str, str]]]:
    """Map a Sheet response by its live header row instead of fixed columns."""
    if not rows:
        return []
    headers = [normalized_header(value) for value in rows[0]]
    mapped: list[tuple[int, dict[str, str]]] = []
    for sheet_row, row in enumerate(rows[1:], start=2):
        mapped.append(
            (
                sheet_row,
                {
                    header: str(row[index] if index < len(row) else "")
                    for index, header in enumerate(headers)
                    if header
                },
            )
        )
    return mapped


def first_mapped_value(row: dict[str, str], *headers: str) -> str:
    for header in headers:
        value = normalize_space(row.get(normalized_header(header), ""))
        if value:
            return value
    return ""


def experiment_active(run_date: dt.date, start_date: str, days: int) -> bool:
    try:
        start = dt.date.fromisoformat(start_date)
    except ValueError:
        return False
    return start <= run_date < start + dt.timedelta(days=days)


def source_durability_audit_active(run_date: dt.date, force: bool = False) -> bool:
    return force or experiment_active(
        run_date,
        SOURCE_DURABILITY_AUDIT_START_DATE,
        SOURCE_DURABILITY_AUDIT_DAYS,
    )


def load_source_durability_state(
    path: str = SOURCE_DURABILITY_AUDIT_STATE_PATH,
    *,
    missing_is_unconfirmed: bool = False,
) -> dict[str, Any]:
    empty = {"version": 1, "candidates": [], "stopped": False, "stop_reason": ""}
    if not path or not os.path.exists(path):
        if missing_is_unconfirmed:
            empty["state_unconfirmed"] = True
            log_event(
                "pilot_source_durability_state_unconfirmed",
                reason="state_missing",
                lead_data_writes=0,
                searches=0,
                sends=0,
            )
        return empty
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError, TypeError) as exc:
        empty["state_unconfirmed"] = True
        log_event(
            "pilot_source_durability_state_unconfirmed",
            reason="state_read_failed",
            error=str(exc)[:220],
            lead_data_writes=0,
            searches=0,
            sends=0,
        )
        return empty
    if not isinstance(payload, dict) or not isinstance(payload.get("candidates"), list):
        empty["state_unconfirmed"] = True
        log_event(
            "pilot_source_durability_state_unconfirmed",
            reason="state_schema_invalid",
            lead_data_writes=0,
            searches=0,
            sends=0,
        )
        return empty
    payload.setdefault("version", 1)
    payload.setdefault("stopped", False)
    payload.setdefault("stop_reason", "")
    return payload


def save_source_durability_state(
    state: dict[str, Any],
    path: str = SOURCE_DURABILITY_AUDIT_STATE_PATH,
) -> bool:
    temporary_path = f"{path}.tmp" if path else ""
    try:
        if not path:
            raise ValueError("source durability audit state path is empty")
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(temporary_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, sort_keys=True, separators=(",", ":"))
        os.replace(temporary_path, path)
    except (OSError, ValueError, TypeError) as exc:
        if temporary_path:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
        log_event(
            "pilot_source_durability_state_unconfirmed",
            reason="state_write_failed",
            error=str(exc)[:220],
            lead_data_writes=0,
            searches=0,
            sends=0,
        )
        return False
    return True


def observe_source_durability_candidate(
    state: dict[str, Any],
    candidate: Candidate,
    *,
    captured_at: dt.datetime,
    primary_eligible: bool,
) -> bool:
    """Retain exact URLs only in private audit state; never update lead data."""
    if state.get("stopped"):
        return False
    candidates = state.setdefault("candidates", [])
    address = normalize_space(candidate.fields.get("listing_address", ""))
    state_code = normalize_space(candidate.fields.get("state", "")).upper()
    address_key = street_state_key(address, state_code)
    if not address_key or not candidate.url:
        return False
    for item in candidates:
        if item.get("address_key") != address_key:
            continue
        if candidate.url != item.get("primary_url") and not item.get("alternate_url"):
            item["alternate_url"] = candidate.url
            item["alternate_source"] = candidate.source
            return True
        return False
    if not primary_eligible or len(candidates) >= SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES:
        return False
    stable_id = stable_synthetic_zpid(
        candidate.source,
        candidate.url,
        address,
        candidate.fields.get("city", ""),
        state_code,
    )
    candidates.append(
        {
            "stable_id": stable_id,
            "address": address,
            "state": state_code,
            "address_key": address_key,
            "source": candidate.source,
            "primary_url": candidate.url,
            "alternate_url": "",
            "alternate_source": "",
            "captured_at": captured_at.astimezone(dt.timezone.utc).isoformat(),
            "evaluated_at": "",
            "primary_reviewable": False,
            "alternate_reviewable": False,
            "primary_outcome": "pending_24h",
            "alternate_outcome": "not_observed",
        }
    )
    return True


def source_durability_url_reviewability(
    url: str,
    address: str,
    state_code: str,
) -> dict[str, Any]:
    if not url:
        return {
            "reviewable": False,
            "outcome": "not_observed",
            "access_control_concern": False,
        }
    try:
        markup = fetch_url(url, timeout=12, allow_headless=False)
    except urllib.error.HTTPError as exc:
        concern = exc.code in {401, 403, 429, 451}
        return {
            "reviewable": False,
            "outcome": f"http_{exc.code}",
            "access_control_concern": concern,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "reviewable": False,
            "outcome": f"fetch_failed:{type(exc).__name__}",
            "access_control_concern": False,
        }
    result = SearchResult(
        source="source_durability_audit",
        query="",
        url=url,
        title="",
        snippet="",
    )
    candidate = infer_fields(result, markup)
    qualification = qualification_for_candidate(candidate)
    address_present = candidate.fields.get("exact_listing_confirmed", "").lower() == "true"
    reviewable = qualification.status == "qualified"
    if reviewable:
        outcome = "current_short_sale_supported"
    elif not address_present:
        outcome = "exact_address_unconfirmed"
    else:
        outcome = qualification.failure_reason or "qualification_unconfirmed"
    return {
        "reviewable": reviewable,
        "outcome": outcome,
        "address_confirmed": address_present,
        "status_outcome": candidate.fields.get("scoped_listing_status", "unknown"),
        "remarks_confirmed": bool(candidate.fields.get("listing_description_group")),
        "access_control_concern": False,
    }


def run_source_durability_audit(
    *,
    run_date: dt.date,
    force: bool = False,
    now: dt.datetime | None = None,
    state_path: str = SOURCE_DURABILITY_AUDIT_STATE_PATH,
) -> dict[str, int]:
    stats = {
        "selected": 0,
        "mature": 0,
        "evaluated": 0,
        "primary_reviewable": 0,
        "alternate_observed": 0,
        "alternate_reviewable": 0,
        "pending_24h": 0,
        "access_control_concerns": 0,
        "stopped": 0,
        "supported": 0,
        "state_unconfirmed": 0,
        "state_persistence_confirmed": 1,
    }
    if not source_durability_audit_active(run_date, force=force):
        return stats
    state = load_source_durability_state(state_path, missing_is_unconfirmed=True)
    stats["state_unconfirmed"] = int(bool(state.get("state_unconfirmed")))
    candidates = state.get("candidates", [])[:SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES]
    stats["selected"] = len(candidates)
    if state.get("stopped"):
        stats["stopped"] = 1
        log_event(
            "pilot_source_durability_audit_done",
            run_date=run_date.isoformat(),
            stats=stats,
            sample_complete=False,
            stop_reason=state.get("stop_reason", "previous_stop"),
            lead_data_writes=0,
            searches=0,
            sends=0,
        )
        return stats
    current_time = now or dt.datetime.now(dt.timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=dt.timezone.utc)
    for item in candidates:
        if item.get("alternate_url"):
            stats["alternate_observed"] += 1
        if item.get("evaluated_at"):
            stats["evaluated"] += 1
            stats["primary_reviewable"] += int(bool(item.get("primary_reviewable")))
            stats["alternate_reviewable"] += int(bool(item.get("alternate_reviewable")))
            continue
        try:
            captured_at = dt.datetime.fromisoformat(str(item.get("captured_at", "")).replace("Z", "+00:00"))
        except ValueError:
            stats["pending_24h"] += 1
            continue
        if captured_at.tzinfo is None:
            captured_at = captured_at.replace(tzinfo=dt.timezone.utc)
        age_hours = (current_time - captured_at.astimezone(dt.timezone.utc)).total_seconds() / 3600
        if age_hours < SOURCE_DURABILITY_AUDIT_MIN_AGE_HOURS:
            stats["pending_24h"] += 1
            continue
        stats["mature"] += 1
        primary = source_durability_url_reviewability(
            str(item.get("primary_url", "")),
            str(item.get("address", "")),
            str(item.get("state", "")),
        )
        if primary["access_control_concern"]:
            stats["access_control_concerns"] += 1
            stats["stopped"] = 1
            state["stopped"] = True
            state["stop_reason"] = "first_access_control_concern"
            log_event(
                "pilot_source_durability_audit_stopped",
                run_date=run_date.isoformat(),
                stable_id=item.get("stable_id", ""),
                source=item.get("source", ""),
                exact_url=item.get("primary_url", ""),
                reason="first_access_control_concern",
                outcome=primary["outcome"],
                lead_data_writes=0,
                searches=0,
                sends=0,
            )
            break
        alternate = source_durability_url_reviewability(
            str(item.get("alternate_url", "")),
            str(item.get("address", "")),
            str(item.get("state", "")),
        )
        if alternate["access_control_concern"]:
            stats["access_control_concerns"] += 1
            stats["stopped"] = 1
            state["stopped"] = True
            state["stop_reason"] = "first_access_control_concern"
            log_event(
                "pilot_source_durability_audit_stopped",
                run_date=run_date.isoformat(),
                stable_id=item.get("stable_id", ""),
                source=item.get("alternate_source", ""),
                exact_url=item.get("alternate_url", ""),
                reason="first_access_control_concern",
                outcome=alternate["outcome"],
                lead_data_writes=0,
                searches=0,
                sends=0,
            )
            break
        item["evaluated_at"] = current_time.astimezone(dt.timezone.utc).isoformat()
        item["primary_reviewable"] = bool(primary["reviewable"])
        item["alternate_reviewable"] = bool(alternate["reviewable"])
        item["primary_outcome"] = primary["outcome"]
        item["alternate_outcome"] = alternate["outcome"]
        stats["evaluated"] += 1
        stats["primary_reviewable"] += int(primary["reviewable"])
        stats["alternate_reviewable"] += int(alternate["reviewable"])
        log_event(
            "pilot_source_durability_audit",
            run_date=run_date.isoformat(),
            stable_id=item.get("stable_id", ""),
            source=item.get("source", ""),
            exact_url=item.get("primary_url", ""),
            primary_reviewable=primary["reviewable"],
            primary_outcome=primary["outcome"],
            alternate_source=item.get("alternate_source", ""),
            alternate_exact_url=item.get("alternate_url", ""),
            alternate_reviewable=alternate["reviewable"],
            alternate_outcome=alternate["outcome"],
            age_hours=round(age_hours, 2),
            lead_data_writes=0,
            searches=0,
            sends=0,
        )
    sample_complete = stats["evaluated"] >= SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES
    if sample_complete and stats["primary_reviewable"] < 7:
        stats["stopped"] = 1
        state["stopped"] = True
        state["stop_reason"] = "fewer_than_7_of_10_exact_links_reviewable"
    if sample_complete and stats["primary_reviewable"] >= SOURCE_DURABILITY_AUDIT_MIN_REVIEWABLE:
        stats["supported"] = 1
    state_saved = save_source_durability_state(state, state_path)
    stats["state_persistence_confirmed"] = int(bool(state_saved))
    log_event(
        "pilot_source_durability_audit_done",
        run_date=run_date.isoformat(),
        stats=stats,
        sample_complete=sample_complete,
        success_metric="at_least_9_of_10_exact_links_reviewable_after_24h",
        comparison_window_days=SOURCE_DURABILITY_AUDIT_DAYS,
        stop_condition="first_access_control_concern_or_any_lead_data_mutation_or_fewer_than_7_of_10_reviewable",
        exact_urls_retained_in_private_audit_state=True,
        state_persistence_confirmed=state_saved,
        lead_data_writes=0,
        searches=0,
        sends=0,
    )
    return stats


def first_seen_date(value: str) -> dt.date | None:
    raw = normalize_space(value)
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(ROTATION_TZ))
    return parsed.astimezone(ZoneInfo(ROTATION_TZ)).date()


def query_exclusion_baseline_states(states: list[str], source: str) -> set[str]:
    """Choose exactly five reproducible baseline states per source bucket."""
    ranked = sorted(
        {state.upper() for state in states},
        key=lambda state: hashlib.sha256(
            f"{QUERY_EXCLUSION_EXPERIMENT_START_DATE}|{source}|{state}".encode("utf-8")
        ).hexdigest(),
    )
    return set(ranked[: min(QUERY_EXCLUSION_BASELINE_PER_BUCKET, len(ranked))])


def query_exclusion_arm(
    run_date: dt.date,
    state: str,
    source: str,
    baseline_states: dict[str, set[str]],
) -> str:
    if source not in DEFAULT_DAILY_SOURCE_BUCKETS or not experiment_active(
        run_date,
        QUERY_EXCLUSION_EXPERIMENT_START_DATE,
        QUERY_EXCLUSION_EXPERIMENT_DAYS,
    ):
        return "not_in_experiment"
    return "baseline" if state.upper() in baseline_states.get(source, set()) else "excluded"


def query_with_exclusion_experiment(template: str, state_term: str, arm: str) -> str:
    query = template.format(state=state_term)
    if arm != "excluded":
        return query
    exclusions = " ".join(f"-site:{domain}" for domain in QUERY_EXCLUSION_DOMAINS)
    return normalize_space(f"{query} {exclusions}")


def evidence_hash(value: str) -> str:
    compact = normalize_space(value)
    return hashlib.sha256(compact.encode("utf-8")).hexdigest()[:16] if compact else ""


MLS_IDENTIFIER_PATTERNS = (
    re.compile(r"(?i)\bMLS\s*(?:number|no\.?|#|id)?\s*[:#]?\s*([A-Z]{0,6}[0-9][A-Z0-9-]{4,20})\b"),
    re.compile(r"\(#([A-Z]{1,6}[0-9][A-Z0-9-]{4,20})\)"),
)


def canonical_listing_identifier(row: dict[str, str]) -> str:
    """Extract a conservative MLS-like identifier from already-stored evidence."""
    fields = (
        CANONICAL_VERIFIER_EVIDENCE_HEADER,
        "pending_queue_listing_json",
        "raw_title",
        "qualification_evidence",
        "description_excerpt",
    )
    for field in fields:
        value = normalize_space(row.get(field, ""))
        for pattern in MLS_IDENTIFIER_PATTERNS:
            match = pattern.search(value)
            if match:
                return re.sub(r"[^A-Z0-9]", "", match.group(1).upper())
    return ""


ROUTE_ALIAS_SHADOW_RE = re.compile(
    r"\b(?:county\s+(?:road|rd)|co(?:unty)?\s+(?:road|rd)|route|rte|rt)\b",
    re.IGNORECASE,
)


def route_alias_shadow_key(address: str, state: str) -> str:
    """Normalize Route address aliases for a zero-write dedupe shadow only."""
    street = clean_listing_address(address, state=state)
    street = ROUTE_ALIAS_SHADOW_RE.sub(" route ", street)
    street_key = normalize_key(street)
    state_key = normalize_key(state)
    if not street_key or not state_key:
        return ""
    return f"{street_key}|{state_key}"


def route_alias_dedupe_shadow(
    candidate_sheet_row: int,
    candidate_row: dict[str, str],
    prior_rows: list[tuple[int, dict[str, str]]],
) -> dict[str, Any]:
    """Compare a pilot row with earlier pilot rows without changing live dedupe behavior."""
    candidate_key = route_alias_shadow_key(
        candidate_row.get("listing_address", ""), candidate_row.get("state", "")
    )
    candidate_identifier = canonical_listing_identifier(candidate_row)
    collisions: list[tuple[int, str]] = []
    for prior_sheet_row, prior_row in prior_rows:
        prior_key = route_alias_shadow_key(
            prior_row.get("listing_address", ""), prior_row.get("state", "")
        )
        if candidate_key and prior_key == candidate_key:
            collisions.append((prior_sheet_row, canonical_listing_identifier(prior_row)))

    matched_row: int | None = None
    matched_identifier = ""
    exact = False
    conflict = False
    missing_identifier = False
    for prior_sheet_row, prior_identifier in collisions:
        if not candidate_identifier or not prior_identifier:
            missing_identifier = True
            if matched_row is None:
                matched_row = prior_sheet_row
                matched_identifier = prior_identifier
            continue
        if candidate_identifier != prior_identifier:
            matched_row = prior_sheet_row
            matched_identifier = prior_identifier
            conflict = True
            exact = False
            break
        if not exact:
            matched_row = prior_sheet_row
            matched_identifier = prior_identifier
            exact = True

    return {
        "candidate_row": candidate_sheet_row,
        "matched_prior_row": matched_row,
        "alias_key_hash": evidence_hash(candidate_key),
        "candidate_identifier_hash": evidence_hash(candidate_identifier),
        "prior_identifier_hash": evidence_hash(matched_identifier),
        "alias_collision": bool(collisions),
        "alias_collision_count": len(collisions),
        "reviewable": bool(candidate_identifier and matched_identifier),
        "exact_identifier_agreement": exact,
        "missing_identifier": missing_identifier and not exact and not conflict,
        "conflicting_identifier_stop": conflict,
        "raw_identifier_logged": False,
        "raw_address_logged": False,
        "raw_url_logged": False,
        "promotion_changed": False,
        "outreach_changed": False,
        "writes": 0,
    }


def parse_sheet_datetime(value: str) -> dt.datetime | None:
    raw = normalize_space(value)
    if not raw:
        return None
    candidates = (raw, raw.replace("Z", "+00:00"))
    for candidate in candidates:
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo(ROTATION_TZ))
            return parsed.astimezone(ZoneInfo(ROTATION_TZ))
        except ValueError:
            continue
    for pattern in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%m-%d-%y %H.%M"):
        try:
            return dt.datetime.strptime(raw, pattern).replace(tzinfo=ZoneInfo(ROTATION_TZ))
        except ValueError:
            continue
    return None


def delivery_receipt_evidence(main_row: dict[str, str]) -> dict[str, Any]:
    stable_id = stable_id_from_main_row(main_row)
    phone = normalize_phone(first_mapped_value(main_row, "phone", "mobile", "agent_phone"))
    initial_at = first_mapped_value(
        main_row,
        "initial_sms_at",
        "initial_sms_timestamp",
        "initial_text_sent_at",
        "initial_send_timestamp",
    )
    timestamp_basis = "initial_sms_timestamp" if initial_at else "last_contact_time"
    if not initial_at:
        initial_at = first_mapped_value(main_row, "last_contact_time")
    inbound = first_mapped_value(main_row, "last_inbound_text", "response_status")
    inbound_at = first_mapped_value(main_row, "last_inbound_at", "last_contact_time")
    message_id = first_mapped_value(main_row, "last_message_id", "inbound_message_id")
    failure_text = " ".join(
        first_mapped_value(main_row, header)
        for header in ("sms_delivery_status", "mailshake_status", "contact_verification_note")
    ).lower()
    explicit_failure = bool(re.search(r"\b(?:sms|delivery|gateway)\s*(?:failed|error|undeliverable)\b", failure_text))
    reply_backed = bool(inbound and message_id)
    if explicit_failure:
        outcome = "failed"
    elif reply_backed:
        outcome = "confirmed_by_inbound"
    else:
        outcome = "delivery_unconfirmed"
    return {
        "stable_id": stable_id,
        "phone_hash": evidence_hash(phone),
        "message_id_hash": evidence_hash(message_id),
        "initial_sms_at": initial_at,
        "timestamp_basis": timestamp_basis,
        "receipt_at": inbound_at if reply_backed else "",
        "outcome": outcome,
        "definitive": outcome in {"failed", "confirmed_by_inbound"},
        "provider_delivery_receipt_present": False,
        "evidence_basis": "inbound_message_id" if reply_backed else ("explicit_failure" if explicit_failure else "none"),
        "writes": 0,
        "sends": 0,
    }


BROKERAGE_SUFFIX_SHADOWS = (
    "provided stellar",
    "black label real estate",
    "black label",
    "real estate group",
    "real estate team",
    "real estate",
    "property group",
    "properties",
    "brokerage",
    "associates",
    "partners",
    "company",
    "realty",
    "group",
    "team",
    "homes",
    "llc",
    "inc",
)


AGENT_ARTIFACT_SUFFIX_RE = re.compile(
    r"(?i)(?:\s*[|·•-]\s*|\s+)(?:provided\s+)?"
    r"(?:stellar|equity(?:\s+real\s+estate)?|[a-z&.'-]+\s+real\s+estate|"
    r"[a-z&.'-]+\s+realty|brokerage|properties|real\s+estate)\s*$"
)


def agent_artifact_shadow_name(value: str) -> dict[str, str]:
    """Propose a person-only name from a feed artifact; never mutate live fields."""
    original = normalize_space(html.unescape(value or "")).strip(" .,:;|-·•")
    if not original:
        return {}
    suffix_shadow = brokerage_suffix_shadow_name(original)
    if suffix_shadow:
        return suffix_shadow
    match = AGENT_ARTIFACT_SUFFIX_RE.search(original)
    if not match:
        return {}
    proposed = normalize_space(original[: match.start()]).strip(" .,:;|-·•")
    if len(proposed.split()) < 2 or len(proposed.split()) > 4 or not looks_like_person_name(proposed):
        return {}
    return {
        "original_agent": original,
        "proposed_agent": proposed,
        "brokerage_suffix": normalize_space(match.group(0)).strip(" .,:;|-·•"),
    }


def conservative_agent_shadow_name(value: str) -> dict[str, str]:
    """Return a person-only shadow proposal without guessing through site/team labels."""
    original = normalize_space(html.unescape(value or "")).strip(" .,:;|-·•")
    if not original:
        return {
            "raw_agent": "",
            "proposed_agent": "",
            "agent_proposal_reason": "missing_agent",
        }
    if re.match(r"(?i)^the\s+", original):
        return {
            "raw_agent": original,
            "proposed_agent": "",
            "agent_proposal_reason": "leading_site_or_team_article",
        }
    trailing_of = re.match(r"(?is)^(.+?)\s+of$", original)
    if trailing_of:
        proposed = normalize_space(trailing_of.group(1)).strip(" .,:;|-·•")
        if looks_like_person_name(proposed):
            return {
                "raw_agent": original,
                "proposed_agent": proposed,
                "agent_proposal_reason": "terminal_feed_artifact",
            }
    artifact = agent_artifact_shadow_name(original)
    if artifact:
        return {
            "raw_agent": original,
            "proposed_agent": artifact["proposed_agent"],
            "agent_proposal_reason": "brokerage_or_feed_suffix",
        }
    if looks_like_person_name(original):
        return {
            "raw_agent": original,
            "proposed_agent": original,
            "agent_proposal_reason": "already_person_like",
        }
    return {
        "raw_agent": original,
        "proposed_agent": "",
        "agent_proposal_reason": "unsafe_or_unattributed_agent",
    }


def verifier_agent_from_main_row(main_row: dict[str, str]) -> str:
    full_name = first_mapped_value(main_row, "agent_name", "listing_agent", "listing_agent_name")
    if full_name:
        cleaned_full_name = clean_agent_name(full_name)
        if cleaned_full_name:
            return cleaned_full_name
        last_name = first_mapped_value(main_row, "last_name", "last")
        if last_name:
            return clean_agent_name(f"{full_name} {last_name}")
    return normalize_space(
        f"{first_mapped_value(main_row, 'first_name', 'first')} "
        f"{first_mapped_value(main_row, 'last_name', 'last')}"
    )


def conservative_address_shadow(pilot_row: dict[str, str]) -> dict[str, str]:
    """Prefer only stored street extensions that preserve the parsed address prefix."""
    raw_address = normalize_space(pilot_row.get("listing_address", ""))
    proposed = clean_listing_address(
        raw_address,
        pilot_row.get("city", ""),
        pilot_row.get("state", ""),
        pilot_row.get("zip", ""),
    )
    alternatives: list[tuple[str, str]] = []
    raw_payload = normalize_space(pilot_row.get("pending_queue_listing_json", ""))
    if raw_payload:
        try:
            payload = json.loads(raw_payload)
        except (TypeError, ValueError):
            payload = {}
        if isinstance(payload, dict):
            for key in ("street", "address", "listing_address", "propertyAddress"):
                value = normalize_space(str(payload.get(key, "")))
                if value:
                    alternatives.append((value, "stored_structured_extension"))
    title_parts = parse_address_parts(pilot_row.get("raw_title", ""))
    if title_parts.get("listing_address"):
        alternatives.append((title_parts["listing_address"], "stored_title_extension"))

    proposed_key = normalize_key(proposed)
    reason = "existing_pilot_address"
    for alternative, alternative_reason in alternatives:
        cleaned = clean_listing_address(
            alternative,
            pilot_row.get("city", ""),
            pilot_row.get("state", ""),
            pilot_row.get("zip", ""),
        )
        cleaned_key = normalize_key(cleaned)
        if not looks_like_listing_address(cleaned):
            continue
        is_safe_extension = bool(
            not proposed_key
            or cleaned_key == proposed_key
            or cleaned_key.startswith(f"{proposed_key} ")
        )
        if is_safe_extension and len(cleaned_key) > len(proposed_key):
            proposed = cleaned
            proposed_key = cleaned_key
            reason = alternative_reason
    return {
        "raw_address": raw_address,
        "proposed_address": proposed,
        "address_proposal_reason": reason,
    }


def agent_address_normalization_shadow(
    pilot_row: dict[str, str],
    main_row: dict[str, str],
) -> dict[str, Any]:
    """Benchmark conservative intake normalization against verifier-confirmed fields."""
    raw_agent = normalize_space(
        f"{pilot_row.get('first_name', '')} {pilot_row.get('last_name', '')}"
    )
    agent_shadow = conservative_agent_shadow_name(raw_agent)
    address_shadow = conservative_address_shadow(pilot_row)
    proposed_address = address_shadow["proposed_address"]
    verifier_agent = verifier_agent_from_main_row(main_row)
    verifier_address = first_mapped_value(
        main_row,
        "listing_address",
        "street",
        "address",
        "property_address",
    )
    proposed_agent = agent_shadow["proposed_agent"]
    agent_exact = bool(
        proposed_agent
        and verifier_agent
        and normalize_key(proposed_agent) == normalize_key(verifier_agent)
    )
    address_exact = bool(
        proposed_address
        and verifier_address
        and normalize_key(proposed_address) == normalize_key(verifier_address)
    )
    wrong_person = bool(proposed_agent and verifier_agent and not agent_exact)
    return {
        **agent_shadow,
        **address_shadow,
        "verifier_agent": verifier_agent,
        "verifier_address": verifier_address,
        "agent_exact_match": agent_exact,
        "address_exact_match": address_exact,
        "exact_agent_address_agreement": agent_exact and address_exact,
        "wrong_person_stop": wrong_person,
        "writes": 0,
    }


PILOT_ID_RE = re.compile(r"^free-[a-z0-9]{8,64}$", re.IGNORECASE)


def stable_id_from_main_row(row: dict[str, str]) -> str:
    stable_id = first_mapped_value(row, "zpid", "synthetic_zpid", "property_id")
    if stable_id:
        return stable_id
    legacy_value = first_mapped_value(row, "created_at")
    return legacy_value if PILOT_ID_RE.fullmatch(legacy_value) else ""


def brokerage_suffix_shadow_name(value: str) -> dict[str, str]:
    """Propose a person-only name without changing the parsed candidate."""
    original = normalize_space(html.unescape(value or "")).strip(" .,:;|-")
    if not original:
        return {}
    for suffix in BROKERAGE_SUFFIX_SHADOWS:
        match = re.search(rf"(?i)\s+({re.escape(suffix)})\s*$", original)
        if not match:
            continue
        proposed = normalize_space(original[: match.start()]).strip(" .,:;|-")
        if len(proposed.split()) != 2 or not looks_like_person_name(proposed):
            return {}
        return {
            "original_agent": original,
            "proposed_agent": proposed,
            "brokerage_suffix": match.group(1),
        }
    return {}


def reconcile_pilot_link(
    pilot_sheet_row: int,
    pilot_row: dict[str, str],
    main_rows: list[tuple[int, dict[str, str]]],
) -> dict[str, Any]:
    synthetic_zpid = normalize_space(pilot_row.get("synthetic_zpid", ""))
    pilot_key = street_state_key(
        pilot_row.get("listing_address", ""),
        pilot_row.get("state", ""),
    )
    id_matches: list[tuple[int, dict[str, str]]] = []
    address_matches: list[tuple[int, dict[str, str]]] = []
    exact_matches: list[tuple[int, dict[str, str]]] = []
    for main_sheet_row, main_row in main_rows:
        main_id = stable_id_from_main_row(main_row)
        main_key = street_state_key(
            first_mapped_value(main_row, "listing_address", "street", "address", "property_address"),
            first_mapped_value(main_row, "state", "listing_state"),
        )
        id_match = bool(synthetic_zpid and main_id == synthetic_zpid)
        address_match = bool(pilot_key and main_key == pilot_key)
        if id_match:
            id_matches.append((main_sheet_row, main_row))
        if address_match:
            address_matches.append((main_sheet_row, main_row))
        if id_match and address_match:
            exact_matches.append((main_sheet_row, main_row))

    if len(id_matches) > 1 or len(exact_matches) > 1:
        outcome = "multiple_matches"
    elif len(exact_matches) == 1 and len(id_matches) == 1:
        outcome = "linked"
    elif len(id_matches) == 1:
        outcome = "identity_address_mismatch"
    elif address_matches:
        outcome = "address_only"
    else:
        outcome = "missing"

    matched_main_row = exact_matches[0][0] if outcome == "linked" else None
    recorded_pointer = normalize_space(pilot_row.get("matched_main_row", ""))
    pointer_matches = not recorded_pointer or str(matched_main_row or "") == recorded_pointer
    return {
        "pilot_row": pilot_sheet_row,
        "synthetic_zpid": synthetic_zpid,
        "address": pilot_row.get("listing_address", ""),
        "state": pilot_row.get("state", ""),
        "outcome": outcome,
        "matched_main_row": matched_main_row,
        "id_match_rows": [row for row, _ in id_matches],
        "address_match_rows": [row for row, _ in address_matches],
        "recorded_main_row": recorded_pointer,
        "pointer_matches": pointer_matches,
        "follow_on_hold": outcome != "linked",
        "main_row": exact_matches[0][1] if outcome == "linked" else {},
    }


def pilot_followup_hold_reason(pilot_row: dict[str, str]) -> str:
    """Return the qualification state that should suppress future follow-up in shadow."""
    status = normalize_space(pilot_row.get("status", "")).lower()
    failure_reason = normalize_space(pilot_row.get("failure_reason", ""))
    promotion_status = normalize_space(pilot_row.get("promotion_status", "")).lower()
    import_ready = normalize_space(pilot_row.get("import_ready", "")).lower()
    disqualifying_terms = normalize_space(pilot_row.get("disqualifying_terms", ""))
    if status in {"rejected", "duplicate"}:
        return failure_reason or disqualifying_terms or f"pilot_{status}"
    if status == "review" or import_ready == "review" or promotion_status.startswith("needs_"):
        return failure_reason or disqualifying_terms or "pilot_review_required"
    if import_ready == "skip" or promotion_status.startswith("disqualified_"):
        return failure_reason or disqualifying_terms or "pilot_not_importable"
    return ""


def main_followup_hold_present(main_row: dict[str, str]) -> bool:
    override = normalize_space(main_row.get("human_override", "")).lower()
    if override in {"1", "true", "x", "yes", "hold"}:
        return True
    evidence = " ".join(
        normalize_space(main_row.get(key, ""))
        for key in ("contact_verification_note", "conversation_summary", "ai_state")
    ).lower()
    return any(
        marker in evidence
        for marker in ("follow-on hold", "no further outreach", "do not initiate further outreach")
    )


def qualification_followup_hold_shadow(
    pilot_sheet_row: int,
    pilot_row: dict[str, str],
    main_rows: list[tuple[int, dict[str, str]]],
) -> dict[str, Any]:
    """Compare pilot qualification holds with the linked Sheet1 follow-up state."""
    reason = pilot_followup_hold_reason(pilot_row)
    reconciliation = reconcile_pilot_link(pilot_sheet_row, pilot_row, main_rows)
    linked = reconciliation["outcome"] == "linked"
    main_row = reconciliation["main_row"] if linked else {}
    existing_hold = main_followup_hold_present(main_row) if linked else False
    would_hold = bool(reason and linked)
    return {
        "hold_reason": reason,
        "linkage_outcome": reconciliation["outcome"],
        "matched_main_row": reconciliation["matched_main_row"],
        "followup_already_sent": bool(normalize_space(main_row.get("followup_text_sent", ""))),
        "existing_hold": existing_hold,
        "would_hold": would_hold,
        "hold_gap": bool(would_hold and not existing_hold),
        "writes": 0,
    }


def run_linkage_and_suffix_audits(
    token: str,
    spreadsheet_id: str,
    main_tab: str,
    pilot_tab: str,
    *,
    run_date: dt.date,
    phase: str,
    force: bool = False,
) -> dict[str, int]:
    link_active = force or experiment_active(run_date, LINK_AUDIT_START_DATE, LINK_AUDIT_DAYS)
    suffix_active = phase == "post_verifier" and (
        force
        or experiment_active(
            run_date,
            BROKERAGE_SUFFIX_SHADOW_START_DATE,
            BROKERAGE_SUFFIX_SHADOW_DAYS,
        )
    )
    agent_address_shadow_active = phase == "post_verifier" and (
        force
        or experiment_active(
            run_date,
            AGENT_ADDRESS_SHADOW_START_DATE,
            AGENT_ADDRESS_SHADOW_DAYS,
        )
    )
    canonical_id_audit_active = phase == "post_verifier" and (
        force
        or experiment_active(
            run_date,
            CANONICAL_ID_AUDIT_START_DATE,
            CANONICAL_ID_AUDIT_DAYS,
        )
    )
    source_durability_active = phase == "post_verifier" and source_durability_audit_active(
        run_date
    )
    route_alias_dedupe_shadow_active = phase == "post_verifier" and (
        force
        or experiment_active(
            run_date,
            ROUTE_ALIAS_DEDUPE_SHADOW_START_DATE,
            ROUTE_ALIAS_DEDUPE_SHADOW_DAYS,
        )
    )
    delivery_receipt_audit_active = phase == "post_verifier" and (
        force
        or experiment_active(
            run_date,
            DELIVERY_RECEIPT_AUDIT_START_DATE,
            DELIVERY_RECEIPT_AUDIT_DAYS,
        )
    )
    qualification_shadow_active = force or experiment_active(
        run_date,
        QUALIFICATION_PRECEDENCE_SHADOW_START_DATE,
        QUALIFICATION_PRECEDENCE_SHADOW_DAYS,
    )
    description_block_shadow_active = phase == "post_promotion" and (
        force
        or experiment_active(
            run_date,
            DESCRIPTION_BLOCK_SHADOW_START_DATE,
            DESCRIPTION_BLOCK_SHADOW_DAYS,
        )
    )
    site_chrome_shadow_active = phase == "post_promotion" and (
        force
        or experiment_active(
            run_date,
            SITE_CHROME_SHADOW_START_DATE,
            SITE_CHROME_SHADOW_DAYS,
        )
    )
    compound_negative_shadow_active = phase == "post_promotion" and (
        force
        or experiment_active(
            run_date,
            COMPOUND_NEGATIVE_SHADOW_START_DATE,
            COMPOUND_NEGATIVE_SHADOW_DAYS,
        )
    )
    future_negotiator_shadow_active = phase == "post_promotion" and (
        force
        or experiment_active(
            run_date,
            FUTURE_NEGOTIATOR_SHADOW_START_DATE,
            FUTURE_NEGOTIATOR_SHADOW_DAYS,
        )
    )
    followup_hold_shadow_active = phase == "post_promotion" and (
        force
        or experiment_active(
            run_date,
            FOLLOWUP_HOLD_SHADOW_START_DATE,
            FOLLOWUP_HOLD_SHADOW_DAYS,
        )
    )
    stats = {
        "audit_checks_planned": sum(
            bool(value)
            for value in (
                link_active,
                suffix_active,
                agent_address_shadow_active,
                canonical_id_audit_active,
                source_durability_active,
                route_alias_dedupe_shadow_active,
                delivery_receipt_audit_active,
                qualification_shadow_active,
                description_block_shadow_active,
                site_chrome_shadow_active,
                compound_negative_shadow_active,
                future_negotiator_shadow_active,
                followup_hold_shadow_active,
            )
        ),
        "audit_evidence_unconfirmed": 0,
        "pilot_rows": 0,
        "linked": 0,
        "held": 0,
        "stale_pointers": 0,
        "suffix_candidates": 0,
        "suffix_exact_matches": 0,
        "suffix_mismatches": 0,
        "suffix_unlinked": 0,
        "suffix_stopped": 0,
        "agent_address_shadow_eligible": 0,
        "agent_address_shadow_evaluated": 0,
        "agent_address_shadow_reviewable": 0,
        "agent_address_shadow_exact": 0,
        "agent_address_shadow_unlinked": 0,
        "agent_address_shadow_wrong_person": 0,
        "agent_address_shadow_stopped": 0,
        "agent_address_shadow_supported": 0,
        "canonical_id_eligible": 0,
        "canonical_id_evaluated": 0,
        "canonical_id_reviewable": 0,
        "canonical_id_exact": 0,
        "canonical_id_missing": 0,
        "canonical_id_mismatches": 0,
        "canonical_id_stopped": 0,
        "canonical_id_supported": 0,
        "source_durability_selected": 0,
        "source_durability_mature": 0,
        "source_durability_evaluated": 0,
        "source_durability_primary_reviewable": 0,
        "source_durability_alternate_observed": 0,
        "source_durability_alternate_reviewable": 0,
        "source_durability_pending_24h": 0,
        "source_durability_access_control_concerns": 0,
        "source_durability_stopped": 0,
        "source_durability_supported": 0,
        "route_alias_shadow_eligible": 0,
        "route_alias_shadow_evaluated": 0,
        "route_alias_shadow_collisions": 0,
        "route_alias_shadow_reviewable": 0,
        "route_alias_shadow_exact": 0,
        "route_alias_shadow_missing_identifier": 0,
        "route_alias_shadow_conflicts": 0,
        "route_alias_shadow_stopped": 0,
        "route_alias_shadow_supported": 0,
        "delivery_receipt_selected": 0,
        "delivery_receipt_mature_selected": 0,
        "delivery_receipt_new_selected": 0,
        "delivery_receipt_definitive": 0,
        "delivery_receipt_reply_backed": 0,
        "delivery_receipt_failed": 0,
        "delivery_receipt_unconfirmed": 0,
        "qualification_shadow_rows": 0,
        "qualification_shadow_ready": 0,
        "qualification_shadow_disqualified": 0,
        "description_block_rows": 0,
        "description_block_ready": 0,
        "description_block_would_hold": 0,
        "site_chrome_rows": 0,
        "site_chrome_targeted": 0,
        "site_chrome_would_hold": 0,
        "compound_negative_rows": 0,
        "compound_negative_matches": 0,
        "compound_negative_would_hold": 0,
        "future_negotiator_rows": 0,
        "future_negotiator_would_hold": 0,
        "followup_hold_rows": 0,
        "followup_hold_linked": 0,
        "followup_hold_existing": 0,
        "followup_hold_gaps": 0,
        "followup_hold_unlinked": 0,
        "unconfirmed": 0,
    }
    if (
        not link_active
        and not suffix_active
        and not agent_address_shadow_active
        and not canonical_id_audit_active
        and not source_durability_active
        and not route_alias_dedupe_shadow_active
        and not delivery_receipt_audit_active
        and not qualification_shadow_active
        and not description_block_shadow_active
        and not site_chrome_shadow_active
        and not compound_negative_shadow_active
        and not future_negotiator_shadow_active
        and not followup_hold_shadow_active
    ):
        log_event(
            "pilot_review_experiments_skipped",
            phase=phase,
            run_date=run_date.isoformat(),
            reason="outside_bounded_windows",
        )
        return stats

    main_raw = get_values(token, spreadsheet_id, f"{main_tab}!A:AS")
    pilot_raw = get_values(token, spreadsheet_id, f"{pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}")
    main_headers = {normalized_header(value) for value in (main_raw[0] if main_raw else [])}
    pilot_headers = {normalized_header(value) for value in (pilot_raw[0] if pilot_raw else [])}
    id_headers = {"zpid", "synthetic_zpid", "property_id"}
    address_headers = {"listing_address", "street", "address", "property_address"}
    required_pilot_headers = {"synthetic_zpid", "listing_address", "state", "promotion_status"}
    legacy_id_header_available = "created_at" in main_headers
    if (
        not (main_headers.intersection(id_headers) or legacy_id_header_available)
        or not main_headers.intersection(address_headers)
        or "state" not in main_headers
        or not required_pilot_headers.issubset(pilot_headers)
    ):
        stats["unconfirmed"] = 1
        log_event(
            "pilot_review_experiments_unconfirmed",
            phase=phase,
            run_date=run_date.isoformat(),
            reason="required_live_headers_missing",
            main_headers=sorted(main_headers),
            pilot_headers=sorted(pilot_headers),
            writes=0,
        )
        return stats
    main_rows = sheet_row_maps(main_raw)
    all_pilot_rows = sheet_row_maps(pilot_raw)
    pilot_rows = [
        (sheet_row, row)
        for sheet_row, row in all_pilot_rows
        if row.get("promotion_status", "").strip().lower() == "promoted"
    ]
    stats["pilot_rows"] = len(pilot_rows)
    log_event(
        "pilot_review_experiments_start",
        phase=phase,
        run_date=run_date.isoformat(),
        link_active=link_active,
        suffix_active=suffix_active,
        agent_address_shadow_active=agent_address_shadow_active,
        canonical_id_audit_active=canonical_id_audit_active,
        source_durability_audit_active=source_durability_active,
        route_alias_dedupe_shadow_active=route_alias_dedupe_shadow_active,
        canonical_verifier_evidence_header_present=(
            CANONICAL_VERIFIER_EVIDENCE_HEADER in main_headers
        ),
        delivery_receipt_audit_active=delivery_receipt_audit_active,
        qualification_shadow_active=qualification_shadow_active,
        description_block_shadow_active=description_block_shadow_active,
        site_chrome_shadow_active=site_chrome_shadow_active,
        compound_negative_shadow_active=compound_negative_shadow_active,
        future_negotiator_shadow_active=future_negotiator_shadow_active,
        followup_hold_shadow_active=followup_hold_shadow_active,
        description_block_max_per_run=DESCRIPTION_BLOCK_SHADOW_MAX_PER_RUN,
        site_chrome_max_per_run=SITE_CHROME_SHADOW_MAX_PER_RUN,
        compound_negative_max_per_run=COMPOUND_NEGATIVE_SHADOW_MAX_PER_RUN,
        agent_address_shadow_max_candidates=AGENT_ADDRESS_SHADOW_MAX_CANDIDATES,
        canonical_id_audit_max_candidates=CANONICAL_ID_AUDIT_MAX_CANDIDATES,
        source_durability_audit_max_candidates=SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES,
        route_alias_dedupe_shadow_max_candidates=(
            ROUTE_ALIAS_DEDUPE_SHADOW_MAX_CANDIDATES
        ),
        delivery_receipt_mature_target=DELIVERY_RECEIPT_MATURE_TARGET,
        delivery_receipt_new_target=DELIVERY_RECEIPT_NEW_TARGET,
        forced=force,
        counts_toward_experiment=not force,
        writes=0,
    )
    main_rows_by_number = dict(main_rows)
    if source_durability_active:
        durability_stats = run_source_durability_audit(
            run_date=run_date,
        )
        for key, value in durability_stats.items():
            stats[f"source_durability_{key}"] = value
        if (
            durability_stats.get("state_unconfirmed", 0)
            or not durability_stats.get("state_persistence_confirmed", 0)
        ):
            stats["audit_evidence_unconfirmed"] = 1
    if canonical_id_audit_active:
        canonical_candidates: list[tuple[int, dict[str, str], dt.date]] = []
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if not candidate_date or candidate_date > run_date:
                continue
            if not force and not experiment_active(
                candidate_date,
                CANONICAL_ID_AUDIT_START_DATE,
                CANONICAL_ID_AUDIT_DAYS,
            ):
                continue
            if normalize_space(pilot_row.get("source", "")).lower() != "idx_broker_remarks":
                continue
            if normalize_space(pilot_row.get("status", "")).lower() != "qualified":
                continue
            if normalize_space(pilot_row.get("promotion_status", "")).lower() != "promoted":
                continue
            canonical_candidates.append((pilot_sheet_row, pilot_row, candidate_date))
        canonical_candidates.sort(key=lambda item: (item[2], item[0]))
        canonical_candidates = canonical_candidates[:CANONICAL_ID_AUDIT_MAX_CANDIDATES]
        stats["canonical_id_eligible"] = len(canonical_candidates)
        canonical_stopped = False
        for sample_rank, (pilot_sheet_row, pilot_row, candidate_date) in enumerate(
            canonical_candidates,
            start=1,
        ):
            if canonical_stopped:
                break
            stats["canonical_id_evaluated"] += 1
            reconciliation = reconcile_pilot_link(pilot_sheet_row, pilot_row, main_rows)
            main_row_number = reconciliation.get("matched_main_row")
            main_row = main_rows_by_number.get(main_row_number or 0, {})
            pilot_identifier = canonical_listing_identifier(pilot_row)
            verifier_identifier = canonical_listing_identifier(main_row)
            reviewable = bool(
                reconciliation["outcome"] == "linked"
                and pilot_identifier
                and verifier_identifier
            )
            exact = bool(reviewable and pilot_identifier == verifier_identifier)
            if reviewable:
                stats["canonical_id_reviewable"] += 1
                if exact:
                    stats["canonical_id_exact"] += 1
                else:
                    stats["canonical_id_mismatches"] += 1
                    canonical_stopped = True
                    stats["canonical_id_stopped"] = 1
            else:
                stats["canonical_id_missing"] += 1
            reviewable_count = stats["canonical_id_reviewable"]
            agreement_rate = (
                stats["canonical_id_exact"] / reviewable_count if reviewable_count else 0.0
            )
            sample_complete = reviewable_count >= CANONICAL_ID_AUDIT_MAX_CANDIDATES
            if sample_complete and agreement_rate < 0.9:
                canonical_stopped = True
                stats["canonical_id_stopped"] = 1
            if sample_complete and agreement_rate >= 0.9:
                stats["canonical_id_supported"] = 1
            if force or candidate_date == run_date:
                log_event(
                    "pilot_canonical_listing_id_audit",
                    phase=phase,
                    run_date=run_date.isoformat(),
                    pilot_row=pilot_sheet_row,
                    main_row=main_row_number,
                    stable_id=pilot_row.get("synthetic_zpid", ""),
                    sample_rank=sample_rank,
                    linkage=reconciliation["outcome"],
                    address_exact=reconciliation["outcome"] == "linked",
                    pilot_identifier_hash=evidence_hash(pilot_identifier),
                    verifier_identifier_hash=evidence_hash(verifier_identifier),
                    verifier_evidence_header_present=(
                        CANONICAL_VERIFIER_EVIDENCE_HEADER in main_headers
                    ),
                    verifier_evidence_hash_only=True,
                    reviewable=reviewable,
                    exact_identifier_agreement=exact,
                    agreement_rate=round(agreement_rate, 4),
                    sample_complete=sample_complete,
                    experiment_stopped=canonical_stopped,
                    raw_identifier_logged=False,
                    raw_url_logged=False,
                    writes=0,
                    success_metric="at_least_90pct_exact_address_and_listing_id_agreement_after_10",
                    stop_condition="first_mismatch_or_any_raw_url_or_data_write",
                )

    if route_alias_dedupe_shadow_active:
        route_alias_candidates: list[tuple[int, dict[str, str], dt.date]] = []
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if not candidate_date or candidate_date > run_date:
                continue
            if not experiment_active(
                candidate_date,
                ROUTE_ALIAS_DEDUPE_SHADOW_START_DATE,
                ROUTE_ALIAS_DEDUPE_SHADOW_DAYS,
            ):
                continue
            if normalize_space(pilot_row.get("source", "")).lower() != "idx_broker_remarks":
                continue
            if normalize_space(pilot_row.get("status", "")).lower() not in {
                "qualified",
                "duplicate",
            }:
                continue
            if normalize_space(pilot_row.get("promotion_status", "")).lower() not in {
                "promoted",
                "skipped_duplicate_listing",
            }:
                continue
            route_alias_candidates.append((pilot_sheet_row, pilot_row, candidate_date))

        route_alias_candidates.sort(key=lambda item: (item[2], item[0]))
        route_alias_candidates = route_alias_candidates[
            :ROUTE_ALIAS_DEDUPE_SHADOW_MAX_CANDIDATES
        ]
        stats["route_alias_shadow_eligible"] = len(route_alias_candidates)
        route_alias_stopped = False
        for sample_rank, (pilot_sheet_row, pilot_row, candidate_date) in enumerate(
            route_alias_candidates,
            start=1,
        ):
            if route_alias_stopped:
                break
            prior_rows = [
                (prior_sheet_row, prior_row)
                for prior_sheet_row, prior_row in all_pilot_rows
                if prior_sheet_row < pilot_sheet_row
            ]
            shadow = route_alias_dedupe_shadow(pilot_sheet_row, pilot_row, prior_rows)
            stats["route_alias_shadow_evaluated"] += 1
            if shadow["alias_collision"]:
                stats["route_alias_shadow_collisions"] += 1
            if shadow["reviewable"]:
                stats["route_alias_shadow_reviewable"] += 1
            if shadow["exact_identifier_agreement"]:
                stats["route_alias_shadow_exact"] += 1
            if shadow["missing_identifier"]:
                stats["route_alias_shadow_missing_identifier"] += 1
            if shadow["conflicting_identifier_stop"]:
                stats["route_alias_shadow_conflicts"] += 1
                stats["route_alias_shadow_stopped"] = 1
                route_alias_stopped = True

            sample_complete = bool(
                stats["route_alias_shadow_evaluated"]
                >= ROUTE_ALIAS_DEDUPE_SHADOW_MAX_CANDIDATES
            )
            if (
                sample_complete
                and stats["route_alias_shadow_exact"] >= 1
                and stats["route_alias_shadow_conflicts"] == 0
            ):
                stats["route_alias_shadow_supported"] = 1

            if force or candidate_date == run_date:
                log_event(
                    "pilot_route_alias_dedupe_shadow",
                    phase=phase,
                    run_date=run_date.isoformat(),
                    sample_rank=sample_rank,
                    comparison_window_days=ROUTE_ALIAS_DEDUPE_SHADOW_DAYS,
                    sample_complete=sample_complete,
                    experiment_stopped=route_alias_stopped,
                    counts_toward_experiment=not force,
                    hypothesis=(
                        "county_road_route_rte_and_rt_aliases_reveal_escaped_"
                        "duplicates_when_canonical_listing_ids_agree"
                    ),
                    success_metric=(
                        "known_escaped_pair_caught_and_zero_conflicting_aliases_after_50"
                    ),
                    stop_condition=(
                        "first_conflicting_canonical_id_or_any_raw_identifier_url_"
                        "address_or_data_write"
                    ),
                    approval_required=False,
                    **shadow,
                )

    if delivery_receipt_audit_active:
        linked_delivery_rows: list[tuple[dt.date, int, int, dict[str, str], dict[str, Any]]] = []
        for pilot_sheet_row, pilot_row in pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if not candidate_date or candidate_date > run_date:
                continue
            reconciliation = reconcile_pilot_link(pilot_sheet_row, pilot_row, main_rows)
            main_row_number = reconciliation.get("matched_main_row")
            main_row = main_rows_by_number.get(main_row_number or 0, {})
            if not main_row or normalize_space(main_row.get("initial_text_sent", "")).lower() != "x":
                continue
            receipt = delivery_receipt_evidence(main_row)
            linked_delivery_rows.append(
                (candidate_date, pilot_sheet_row, main_row_number or 0, main_row, receipt)
            )
        mature_cutoff = run_date - dt.timedelta(days=3)
        mature_rows = sorted(
            (item for item in linked_delivery_rows if item[0] <= mature_cutoff),
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )[:DELIVERY_RECEIPT_MATURE_TARGET]
        new_rows = sorted(
            (
                item
                for item in linked_delivery_rows
                if experiment_active(
                    item[0],
                    DELIVERY_RECEIPT_AUDIT_START_DATE,
                    DELIVERY_RECEIPT_AUDIT_DAYS,
                )
            ),
            key=lambda item: (item[0], item[1]),
        )[:DELIVERY_RECEIPT_NEW_TARGET]
        selected_delivery_rows = [("mature", item) for item in mature_rows]
        selected_delivery_rows.extend(("new_attempt", item) for item in new_rows)
        stats["delivery_receipt_mature_selected"] = len(mature_rows)
        stats["delivery_receipt_new_selected"] = len(new_rows)
        stats["delivery_receipt_selected"] = len(selected_delivery_rows)
        for cohort, (_, pilot_sheet_row, main_row_number, _, receipt) in selected_delivery_rows:
            if receipt["definitive"]:
                stats["delivery_receipt_definitive"] += 1
            if receipt["outcome"] == "confirmed_by_inbound":
                stats["delivery_receipt_reply_backed"] += 1
            elif receipt["outcome"] == "failed":
                stats["delivery_receipt_failed"] += 1
            else:
                stats["delivery_receipt_unconfirmed"] += 1
            log_event(
                "pilot_delivery_receipt_reconciliation",
                phase=phase,
                run_date=run_date.isoformat(),
                cohort=cohort,
                pilot_row=pilot_sheet_row,
                main_row=main_row_number,
                **receipt,
            )
        log_event(
            "pilot_delivery_receipt_reconciliation_done",
            phase=phase,
            run_date=run_date.isoformat(),
            selected=stats["delivery_receipt_selected"],
            definitive=stats["delivery_receipt_definitive"],
            success=bool(
                stats["delivery_receipt_selected"] >= 10
                and stats["delivery_receipt_definitive"] >= 9
            ),
            provider_receipts_available=False,
            note="inbound_message_ids_prove_reply_backed_delivery_only",
            writes=0,
            sends=0,
            success_metric="definitive_status_for_at_least_9_of_10_within_24h",
            stop_condition="any_mutation_resend_or_ambiguous_linkage",
        )
    if qualification_shadow_active:
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if not candidate_date or candidate_date > run_date:
                continue
            if not force and not experiment_active(
                candidate_date,
                QUALIFICATION_PRECEDENCE_SHADOW_START_DATE,
                QUALIFICATION_PRECEDENCE_SHADOW_DAYS,
            ):
                continue
            payload, payload_failure = parse_pilot_payload(pilot_row)
            if payload_failure:
                continue
            candidate = candidate_from_pilot_row(dict(pilot_row), payload)
            shadow = qualification_precedence_shadow(candidate)
            stats["qualification_shadow_rows"] += 1
            if shadow["proposed_ready"]:
                stats["qualification_shadow_ready"] += 1
            if shadow["qualification_status"] == "rejected" and shadow["disqualifying_terms"]:
                stats["qualification_shadow_disqualified"] += 1
            log_event(
                "pilot_qualification_precedence_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                current_promotion_status=pilot_row.get("promotion_status", ""),
                **shadow,
            )
    if future_negotiator_shadow_active:
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if candidate_date != run_date:
                continue
            payload, payload_failure = parse_pilot_payload(pilot_row)
            if payload_failure:
                continue
            candidate = candidate_from_pilot_row(dict(pilot_row), payload)
            shadow = future_negotiator_phrase_shadow(candidate)
            stats["future_negotiator_rows"] += 1
            if shadow["would_hold"]:
                stats["future_negotiator_would_hold"] += 1
            log_event(
                "pilot_future_negotiator_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                comparison_window_days=FUTURE_NEGOTIATOR_SHADOW_DAYS,
                hypothesis="future_assignment_language_identifies_existing_negotiator_involvement",
                success_metric="all_verified_future_assignment_cases_flagged_with_zero_false_holds",
                stop_condition="first_verified_valid_listing_wrongly_held_or_no_reviewable_cases_after_7_days",
                **shadow,
            )
    if followup_hold_shadow_active:
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            if not pilot_followup_hold_reason(pilot_row):
                continue
            shadow = qualification_followup_hold_shadow(pilot_sheet_row, pilot_row, main_rows)
            stats["followup_hold_rows"] += 1
            if shadow["linkage_outcome"] == "linked":
                stats["followup_hold_linked"] += 1
            else:
                stats["followup_hold_unlinked"] += 1
            if shadow["existing_hold"]:
                stats["followup_hold_existing"] += 1
            if shadow["hold_gap"]:
                stats["followup_hold_gaps"] += 1
            log_event(
                "pilot_qualification_followup_hold_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                comparison_window_days=FOLLOWUP_HOLD_SHADOW_DAYS,
                hypothesis="pilot_rejection_or_review_state_should_propose_a_linked_sheet1_followup_hold",
                success_metric="all_linked_nonqualifying_pilot_rows_propose_holds_with_zero_valid_rows_held",
                stop_condition="first_verified_valid_listing_wrongly_held_or_any_linked_hold_gap_after_7_days",
                **shadow,
            )
    if description_block_shadow_active:
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            if stats["description_block_rows"] >= DESCRIPTION_BLOCK_SHADOW_MAX_PER_RUN:
                break
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if candidate_date != run_date:
                continue
            payload, payload_failure = parse_pilot_payload(pilot_row)
            if payload_failure:
                continue
            candidate = candidate_from_pilot_row(dict(pilot_row), payload)
            shadow = description_block_shadow(candidate)
            stats["description_block_rows"] += 1
            if shadow["proposed_ready"]:
                stats["description_block_ready"] += 1
            if shadow["would_hold"]:
                stats["description_block_would_hold"] += 1
            log_event(
                "pilot_description_block_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                current_promotion_status=pilot_row.get("promotion_status", ""),
                comparison_window_days=DESCRIPTION_BLOCK_SHADOW_DAYS,
                stop_condition=(
                    "first_verified_valid_listing_wrongly_held_or_agreement_below_90pct_after_10"
                ),
                benchmark_pending=True,
                **shadow,
            )
    if site_chrome_shadow_active:
        try:
            site_chrome_start = dt.date.fromisoformat(SITE_CHROME_SHADOW_START_DATE)
        except ValueError:
            site_chrome_start = run_date
        targeted_before_today = 0
        for _, prior_row in all_pilot_rows:
            prior_date = first_seen_date(prior_row.get("first_seen_at", ""))
            if prior_date is None or not site_chrome_start <= prior_date < run_date:
                continue
            prior_payload, prior_failure = parse_pilot_payload(prior_row)
            if prior_failure:
                continue
            prior_candidate = candidate_from_pilot_row(dict(prior_row), prior_payload)
            prior_shadow = site_chrome_exclusion_shadow(prior_candidate)
            if (
                prior_shadow["platform_targeted"]
                and prior_shadow["listing_description_present"]
                and prior_shadow["current_ready"]
            ):
                targeted_before_today += 1
        remaining_site_chrome_candidates = max(
            0,
            SITE_CHROME_SHADOW_MAX_PER_RUN - targeted_before_today,
        )
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            if stats["site_chrome_rows"] >= remaining_site_chrome_candidates:
                break
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if candidate_date != run_date:
                continue
            payload, payload_failure = parse_pilot_payload(pilot_row)
            if payload_failure:
                continue
            candidate = candidate_from_pilot_row(dict(pilot_row), payload)
            shadow = site_chrome_exclusion_shadow(candidate)
            if not (
                shadow["platform_targeted"]
                and shadow["listing_description_present"]
                and shadow["current_ready"]
            ):
                continue
            stats["site_chrome_rows"] += 1
            stats["site_chrome_targeted"] += 1
            if shadow["would_hold"]:
                stats["site_chrome_would_hold"] += 1
            log_event(
                "pilot_site_chrome_exclusion_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                current_promotion_status=pilot_row.get("promotion_status", ""),
                comparison_window_days=SITE_CHROME_SHADOW_DAYS,
                experiment_candidate_number=targeted_before_today + stats["site_chrome_rows"],
                hypothesis=(
                    "targeted_site_chrome_short_sale_phrases_outside_the_property_description_"
                    "identify_false_qualification"
                ),
                success_metric="at_least_90pct_verifier_agreement_after_10_reviewable_cases",
                stop_condition=(
                    "first_verified_valid_listing_wrongly_held_or_agreement_below_90pct_after_10"
                ),
                **shadow,
            )
    if compound_negative_shadow_active:
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            if stats["compound_negative_rows"] >= COMPOUND_NEGATIVE_SHADOW_MAX_PER_RUN:
                break
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if candidate_date != run_date:
                continue
            payload, payload_failure = parse_pilot_payload(pilot_row)
            if payload_failure:
                continue
            candidate = candidate_from_pilot_row(dict(pilot_row), payload)
            shadow = compound_negative_field_shadow(candidate)
            stats["compound_negative_rows"] += 1
            if shadow["explicit_negative_field_found"]:
                stats["compound_negative_matches"] += 1
            if shadow["would_hold"]:
                stats["compound_negative_would_hold"] += 1
            log_event(
                "pilot_compound_negative_field_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                address=pilot_row.get("listing_address", ""),
                current_promotion_status=pilot_row.get("promotion_status", ""),
                comparison_window_days=COMPOUND_NEGATIVE_SHADOW_DAYS,
                hypothesis=(
                    "explicit_structured_short_sale_no_fields_identify_false_qualification_"
                    "even_when_the_field_appears_inside_a_description_section"
                ),
                success_metric="100pct_verifier_agreement_after_10_reviewable_cases",
                stop_condition=(
                    "first_verified_valid_listing_wrongly_held_or_less_than_100pct_"
                    "agreement_after_10_reviewable_cases"
                ),
                approval_required=False,
                **shadow,
            )
    if agent_address_shadow_active:
        eligible_agent_address_rows: list[tuple[int, dict[str, str], dt.date]] = []
        for pilot_sheet_row, pilot_row in all_pilot_rows:
            candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
            if not candidate_date or candidate_date > run_date:
                continue
            if not force and not experiment_active(
                candidate_date,
                AGENT_ADDRESS_SHADOW_START_DATE,
                AGENT_ADDRESS_SHADOW_DAYS,
            ):
                continue
            if normalize_space(pilot_row.get("source", "")).lower() != "idx_broker_remarks":
                continue
            if normalize_space(pilot_row.get("status", "")).lower() != "qualified":
                continue
            if normalize_space(pilot_row.get("promotion_status", "")).lower() != "promoted":
                continue
            eligible_agent_address_rows.append((pilot_sheet_row, pilot_row, candidate_date))

        eligible_agent_address_rows.sort(key=lambda item: (item[2], item[0]))
        eligible_agent_address_rows = eligible_agent_address_rows[
            :AGENT_ADDRESS_SHADOW_MAX_CANDIDATES
        ]
        stats["agent_address_shadow_eligible"] = len(eligible_agent_address_rows)
        main_rows_by_number = dict(main_rows)
        agent_address_shadow_stopped = False
        for sample_rank, (pilot_sheet_row, pilot_row, candidate_date) in enumerate(
            eligible_agent_address_rows,
            start=1,
        ):
            if agent_address_shadow_stopped:
                break
            stats["agent_address_shadow_evaluated"] += 1
            reconciliation = reconcile_pilot_link(pilot_sheet_row, pilot_row, main_rows)
            benchmark_main_row_number = reconciliation["matched_main_row"]
            benchmark_linkage = "exact_id_and_address"
            if benchmark_main_row_number is None and len(reconciliation["id_match_rows"]) == 1:
                benchmark_main_row_number = reconciliation["id_match_rows"][0]
                benchmark_linkage = "stable_id_only"
            benchmark_main_row = main_rows_by_number.get(benchmark_main_row_number or 0, {})
            emit_candidate_event = force or candidate_date == run_date
            if not benchmark_main_row:
                stats["agent_address_shadow_unlinked"] += 1
                if emit_candidate_event:
                    log_event(
                        "pilot_agent_address_normalization_shadow",
                        phase=phase,
                        run_date=run_date.isoformat(),
                        pilot_row=pilot_sheet_row,
                        synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                        source=pilot_row.get("source", ""),
                        sample_rank=sample_rank,
                        benchmark="unlinked",
                        linkage=reconciliation["outcome"],
                        counts_toward_experiment=not force,
                        writes=0,
                    )
                continue

            shadow = agent_address_normalization_shadow(pilot_row, benchmark_main_row)
            stats["agent_address_shadow_reviewable"] += 1
            if shadow["exact_agent_address_agreement"]:
                stats["agent_address_shadow_exact"] += 1
            if shadow["wrong_person_stop"]:
                stats["agent_address_shadow_wrong_person"] += 1
                agent_address_shadow_stopped = True

            reviewable = stats["agent_address_shadow_reviewable"]
            agreement_rate = stats["agent_address_shadow_exact"] / reviewable
            sample_complete = bool(
                sample_rank == AGENT_ADDRESS_SHADOW_MAX_CANDIDATES
                and reviewable == AGENT_ADDRESS_SHADOW_MAX_CANDIDATES
            )
            if sample_complete and agreement_rate < 0.9:
                agent_address_shadow_stopped = True
            if sample_complete and agreement_rate >= 0.9 and not shadow["wrong_person_stop"]:
                stats["agent_address_shadow_supported"] = 1
            if agent_address_shadow_stopped:
                stats["agent_address_shadow_stopped"] = 1

            if emit_candidate_event:
                log_event(
                    "pilot_agent_address_normalization_shadow",
                    phase=phase,
                    run_date=run_date.isoformat(),
                    pilot_row=pilot_sheet_row,
                    main_row=benchmark_main_row_number,
                    synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                    source=pilot_row.get("source", ""),
                    linkage=benchmark_linkage,
                    sample_rank=sample_rank,
                    sample_size=reviewable,
                    agreement_rate=round(agreement_rate, 4),
                    sample_complete=sample_complete,
                    experiment_stopped=agent_address_shadow_stopped,
                    comparison_window_days=AGENT_ADDRESS_SHADOW_DAYS,
                    hypothesis=(
                        "conservative_feed_artifact_and_address_cleanup_matches_"
                        "verifier_confirmed_agent_and_street_fields"
                    ),
                    success_metric=(
                        "at_least_90pct_exact_agent_address_agreement_after_10_"
                        "with_zero_wrong_person_suggestions"
                    ),
                    stop_condition=(
                        "first_wrong_person_suggestion_or_below_90pct_exact_"
                        "agreement_after_10_reviewable_candidates"
                    ),
                    promotion_changed=False,
                    outreach_changed=False,
                    counts_toward_experiment=not force,
                    **shadow,
                )
    suffix_stopped = False
    for pilot_sheet_row, pilot_row in pilot_rows:
        reconciliation = reconcile_pilot_link(pilot_sheet_row, pilot_row, main_rows)
        if link_active:
            if reconciliation["outcome"] == "linked":
                stats["linked"] += 1
            else:
                stats["held"] += 1
            if not reconciliation["pointer_matches"]:
                stats["stale_pointers"] += 1
            log_event(
                "pilot_linkage_audit",
                phase=phase,
                run_date=run_date.isoformat(),
                **{key: value for key, value in reconciliation.items() if key != "main_row"},
                writes=0,
            )

        candidate_date = first_seen_date(pilot_row.get("first_seen_at", ""))
        candidate_in_scope = bool(
            candidate_date
            and candidate_date <= run_date
            and (
                force
                or experiment_active(
                    candidate_date,
                    BROKERAGE_SUFFIX_SHADOW_START_DATE,
                    BROKERAGE_SUFFIX_SHADOW_DAYS,
                )
            )
        )
        if not suffix_active or not candidate_in_scope or suffix_stopped:
            continue
        raw_agent = normalize_space(
            f"{pilot_row.get('first_name', '')} {pilot_row.get('last_name', '')}"
        )
        shadow = brokerage_suffix_shadow_name(raw_agent)
        if not shadow:
            continue
        stats["suffix_candidates"] += 1
        if reconciliation["outcome"] != "linked":
            stats["suffix_unlinked"] += 1
            log_event(
                "pilot_brokerage_suffix_shadow",
                phase=phase,
                run_date=run_date.isoformat(),
                pilot_row=pilot_sheet_row,
                synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
                linkage=reconciliation["outcome"],
                benchmark="unlinked",
                follow_on_hold=True,
                writes=0,
                **shadow,
            )
            continue
        main_row = reconciliation["main_row"]
        verifier_agent = normalize_space(
            f"{first_mapped_value(main_row, 'first_name', 'first')} "
            f"{first_mapped_value(main_row, 'last_name', 'last')}"
        )
        exact_match = normalize_key(shadow["proposed_agent"]) == normalize_key(verifier_agent)
        if exact_match:
            stats["suffix_exact_matches"] += 1
        else:
            stats["suffix_mismatches"] += 1
            suffix_stopped = True
            stats["suffix_stopped"] = 1
        benchmarked = stats["suffix_exact_matches"] + stats["suffix_mismatches"]
        agreement_rate = stats["suffix_exact_matches"] / benchmarked
        if benchmarked >= 10 and agreement_rate < 0.8:
            suffix_stopped = True
            stats["suffix_stopped"] = 1
        log_event(
            "pilot_brokerage_suffix_shadow",
            phase=phase,
            run_date=run_date.isoformat(),
            pilot_row=pilot_sheet_row,
            main_row=reconciliation["matched_main_row"],
            synthetic_zpid=pilot_row.get("synthetic_zpid", ""),
            verifier_agent=verifier_agent,
            exact_name_agreement=exact_match,
            wrong_person_stop=not exact_match,
            agreement_rate=round(agreement_rate, 4),
            sample_size=benchmarked,
            experiment_stopped=suffix_stopped,
            promotion_changed=False,
            writes=0,
            **shadow,
        )
    log_event(
        "pilot_review_experiments_done",
        phase=phase,
        run_date=run_date.isoformat(),
        stats=stats,
        forced=force,
        counts_toward_experiment=not force,
        writes=0,
    )
    return stats


def promotion_status_updates(
    pilot_tab: str,
    sheet_row: int,
    *,
    status: str,
    notes: str,
    import_ready: str | None = None,
    matched_main_row: str | None = None,
) -> list[dict[str, Any]]:
    values = {
        "promotion_status": status,
        "promotion_notes": notes[:500],
    }
    if import_ready is not None:
        values["import_ready"] = import_ready
    if matched_main_row is not None:
        values["matched_main_row"] = matched_main_row

    updates = []
    for header, value in values.items():
        idx = PILOT_HEADERS.index(header) + 1
        col = column_letter(idx)
        updates.append({"range": f"{pilot_tab}!{col}{sheet_row}", "values": [[value]]})
    return updates


def reconstructed_pilot_payload(row_data: dict[str, str]) -> dict[str, str]:
    source = (row_data.get("pending_queue_source") or row_data.get("source") or "unknown").strip()
    if not source.startswith("free-source-pilot:"):
        source = f"free-source-pilot:{source or 'unknown'}"
    agent_name = normalize_space(f"{row_data.get('first_name', '')} {row_data.get('last_name', '')}")
    listing_text = normalize_space(
        " ".join(
            part
            for part in (
                row_data.get("qualification_evidence", ""),
                row_data.get("description_excerpt", ""),
            )
            if part
        )
    )
    address = row_data.get("pending_queue_address") or row_data.get("listing_address", "")
    if not any(
        normalize_space(part)
        for part in (
            row_data.get("synthetic_zpid", ""),
            address,
            listing_text,
            agent_name,
        )
    ):
        return {}
    payload = {
        "zpid": row_data.get("synthetic_zpid", ""),
        "address": address,
        "street": address,
        "city": row_data.get("city", ""),
        "state": row_data.get("state", ""),
        "zip": row_data.get("zip", ""),
        "source": source,
        "search_source": source,
        "agentName": agent_name,
        "brokerName": row_data.get("broker_name", ""),
        "brokerageName": row_data.get("broker_name", ""),
        "phone": row_data.get("phone", ""),
        "email": row_data.get("email", ""),
        "homeStatus": "FOR_SALE",
        "specialListingConditions": "Short Sale",
        "listing_description": listing_text,
        "description": listing_text,
        "listingText": listing_text,
        "sourceQuery": row_data.get("source_query", ""),
        "sourceTitle": row_data.get("raw_title", ""),
        "qualificationEvidence": row_data.get("qualification_evidence", ""),
        "sourcePilotShadow": "true",
        "requiresVerifierReview": "true",
    }
    return {
        key: sanitize_external_links_for_sheet(value)
        for key, value in payload.items()
        if str(value or "").strip()
    }


def parse_pilot_payload(row_data: dict[str, str]) -> tuple[dict[str, str], str]:
    raw = row_data.get("pending_queue_listing_json", "").strip()
    if not raw:
        reconstructed = reconstructed_pilot_payload(row_data)
        if reconstructed:
            return reconstructed, ""
        return {}, "missing_pending_queue_listing_json"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        reconstructed = reconstructed_pilot_payload(row_data)
        if reconstructed:
            return reconstructed, ""
        return {}, "invalid_pending_queue_listing_json"
    if not isinstance(payload, dict):
        reconstructed = reconstructed_pilot_payload(row_data)
        if reconstructed:
            return reconstructed, ""
        return {}, "invalid_pending_queue_listing_json"
    payload = sanitize_payload_for_sheet_json(payload)
    return {
        str(key): sanitize_external_links_for_sheet(value)
        for key, value in payload.items()
        if str(value or "").strip()
    }, ""


def agent_name_from_pilot(row_data: dict[str, str], payload: dict[str, str]) -> str:
    payload_agent = clean_agent_name(payload.get("agentName", ""))
    if payload_agent:
        return payload_agent
    sheet_agent = clean_agent_name(
        normalize_space(f"{row_data.get('first_name', '')} {row_data.get('last_name', '')}")
    )
    return sheet_agent


def candidate_from_pilot_row(row_data: dict[str, str], payload: dict[str, str]) -> Candidate:
    agent_name = agent_name_from_pilot(row_data, payload)
    fields = {
        "agent_name": agent_name,
        "agent_name_source": payload.get("agentNameSource", ""),
        "agent_evidence_group": payload.get("agentEvidenceGroup", ""),
        "agent_subject_key": payload.get("agentSubjectKey", ""),
        "listing_address": payload.get("street")
        or payload.get("address")
        or row_data.get("listing_address", ""),
        "city": payload.get("city") or row_data.get("city", ""),
        "state": (payload.get("state") or row_data.get("state", "")).upper(),
        "zip": payload.get("zip") or row_data.get("zip", ""),
        "phone": payload.get("phone") or row_data.get("phone", ""),
        "phone_source": payload.get("phoneSource", ""),
        "phone_evidence_group": payload.get("phoneEvidenceGroup", ""),
        "phone_contact_type": payload.get("phoneContactType", ""),
        "phone_owner_key": payload.get("phoneOwnerKey", ""),
        "email": payload.get("email") or row_data.get("email", ""),
        "email_source": payload.get("emailSource", ""),
        "email_evidence_group": payload.get("emailEvidenceGroup", ""),
        "email_contact_type": payload.get("emailContactType", ""),
        "email_owner_key": payload.get("emailOwnerKey", ""),
        "contact_phone_hint": payload.get("contactPhoneHint", ""),
        "contact_phone_hint_type": payload.get("contactPhoneHintType", ""),
        "contact_email_hint": payload.get("contactEmailHint", ""),
        "contact_email_hint_type": payload.get("contactEmailHintType", ""),
        "broker_name": payload.get("brokerName")
        or payload.get("brokerageName")
        or row_data.get("broker_name", ""),
        "home_status": payload.get("homeStatus", ""),
        "listing_description": payload.get("listing_description", "")
        or payload.get("description", ""),
        "listing_description_source": payload.get("listingDescriptionSource", ""),
        "exact_listing_confirmed": payload.get("exactListingConfirmed", ""),
        "listing_identity_source": payload.get("listingIdentitySource", ""),
        "listing_identity_group": payload.get("listingIdentityGroup", ""),
        "scoped_listing_status": payload.get("scopedListingStatus", ""),
        "scoped_listing_status_evidence": payload.get("scopedListingStatusEvidence", ""),
        "scoped_listing_status_source": payload.get("scopedListingStatusSource", ""),
        "scoped_listing_status_group": payload.get("scopedListingStatusGroup", ""),
        "listing_description_group": payload.get("listingDescriptionGroup", ""),
        "source_evidence_state": payload.get("sourceEvidenceState", ""),
        "source_evidence_receipt": payload.get("sourceEvidenceReceipt", ""),
    }
    text = " ".join(
        part
        for part in (
            payload.get("listing_description", ""),
            payload.get("description", ""),
            payload.get("listingText", ""),
            row_data.get("description_excerpt", ""),
            row_data.get("qualification_evidence", ""),
        )
        if part
    )
    return Candidate(
        source=row_data.get("source", ""),
        query=row_data.get("source_query", ""),
        url=row_data.get("source_url", ""),
        title=row_data.get("raw_title", ""),
        text=normalize_space(text),
        fields=fields,
    )


def pilot_row_preflight_failure(
    row_data: dict[str, str],
    payload: dict[str, str],
    existing: ExistingIndex,
) -> tuple[str, str, str]:
    if row_data.get("status", "").strip().lower() != "qualified":
        return "not_qualified", "Pilot row is not qualified.", ""
    if row_data.get("promotion_status", "").strip().lower() != "shadow_ready":
        return "not_ready", "Pilot row is not marked shadow_ready.", ""
    if row_data.get("import_ready", "").strip().lower() != "yes":
        return "not_import_ready", "Pilot row import_ready is not yes.", ""

    source = (payload.get("search_source") or payload.get("source") or row_data.get("pending_queue_source", "")).strip()
    if not source.startswith("free-source-pilot:"):
        return "invalid_source", "Pending payload is missing the free-source-pilot source guard.", ""

    candidate = candidate_from_pilot_row(row_data, payload)
    if not (
        looks_like_listing_address(candidate.fields.get("listing_address", ""))
        and normalize_space(candidate.fields.get("city", ""))
        and normalize_space(candidate.fields.get("state", ""))
    ):
        return "needs_address", "Street, city, and state must be confirmed before promotion.", ""
    agent_name = agent_name_from_pilot(row_data, payload)
    if agent_name:
        candidate.fields["agent_name"] = agent_name
    agent_safe, _ = sanitize_candidate_identity(candidate)
    if not agent_safe:
        row_data["first_name"] = ""
        row_data["last_name"] = ""
        for key in (
            "agentName", "agentNameSource", "agentEvidenceGroup", "agentSubjectKey",
            "phone", "phoneSource", "phoneEvidenceGroup", "phoneContactType", "phoneOwnerKey",
            "email", "emailSource", "emailEvidenceGroup", "emailContactType", "emailOwnerKey",
            "contactPhoneHint", "contactPhoneHintType", "contactEmailHint", "contactEmailHintType",
        ):
            payload.pop(key, None)

    required_provenance = (
        "exactListingConfirmed",
        "listingDescriptionSource",
        "listingIdentityGroup",
        "listingDescriptionGroup",
        "scopedListingStatusGroup",
    )
    if not all(payload.get(key) for key in required_provenance):
        return (
            "needs_exact_listing_evidence",
            "Legacy pilot evidence is not bound to one exact listing record and must be reverified before promotion.",
            "",
        )
    scoped_qualification = qualification_for_candidate(candidate)
    if scoped_qualification.status != "qualified":
        return (
            scoped_qualification.failure_reason or "needs_description_confirmation",
            "Exact-listing identity, current status, and agent-written short-sale remarks must all remain confirmed before promotion.",
            "",
        )
    if not has_durable_source_evidence(payload):
        return (
            "needs_source_evidence_confirmation",
            "Exact source evidence is hash-only or lacks a durable reopenable receipt; promotion and outreach remain held.",
            "",
        )

    duplicate, duplicate_key, matched = duplicate_status(candidate, existing)
    if duplicate == "duplicate_listing":
        return "skipped_duplicate_listing", f"Already present in Sheet1 row {matched}.", matched
    if duplicate == "identity_conflict":
        return (
            "needs_identity_review",
            f"Canonical listing identifiers or unit evidence conflict with Sheet1 row(s) {matched}; held without merge.",
            matched,
        )

    return "", duplicate_key, ""


def normalize_payload_for_sheet1(row_data: dict[str, str], payload: dict[str, str]) -> dict[str, str]:
    candidate = candidate_from_pilot_row(row_data, payload)
    normalized = dict(payload)
    normalized["agentName"] = candidate.fields.get("agent_name", "")
    normalized["street"] = candidate.fields.get("listing_address", "")
    normalized.setdefault("address", candidate.fields.get("listing_address", ""))
    normalized["city"] = candidate.fields.get("city", "")
    normalized["state"] = candidate.fields.get("state", "")
    if candidate.fields.get("zip"):
        normalized["zip"] = candidate.fields["zip"]
    if candidate.fields.get("broker_name"):
        normalized.setdefault("brokerName", candidate.fields["broker_name"])
        normalized.setdefault("brokerageName", candidate.fields["broker_name"])
    source = normalized.get("search_source") or normalized.get("source") or row_data.get("pending_queue_source", "")
    if not source.startswith("free-source-pilot:"):
        source = f"free-source-pilot:{row_data.get('source', 'unknown') or 'unknown'}"
    normalized["source"] = source
    normalized["search_source"] = source
    normalized.setdefault("zpid", row_data.get("synthetic_zpid", ""))
    listing_text = candidate.text or row_data.get("qualification_evidence", "")
    if listing_text:
        normalized.setdefault("listing_description", listing_text[:8_000])
        normalized.setdefault("description", listing_text[:8_000])
        normalized.setdefault("listingText", listing_text[:8_000])
    normalized["sourcePilotShadow"] = "true"
    normalized["requiresVerifierReview"] = "true"
    normalized = sanitize_payload_for_sheet_json(normalized)
    return {
        key: sanitize_external_links_for_sheet(value)
        for key, value in normalized.items()
        if str(value or "").strip()
    }


def import_bot_processor() -> Any:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    import importlib

    return importlib.import_module("bot_min")


def promote_ready_pilot_rows(
    token: str,
    spreadsheet_id: str,
    main_tab: str,
    pilot_tab: str,
    *,
    cap: int,
    dry_run: bool = False,
) -> dict[str, int]:
    stats = {
        "considered": 0,
        "eligible": 0,
        "promoted": 0,
        "skipped": 0,
        "errors": 0,
    }
    if cap <= 0:
        log_event("pilot_promotion_skipped", reason="cap_zero", cap=cap)
        return stats

    main_rows = get_values(token, spreadsheet_id, f"{main_tab}!A:AQ")
    existing = build_existing_index(main_rows)
    pilot_rows = get_values(token, spreadsheet_id, f"{pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}")
    if not pilot_rows:
        log_event("pilot_promotion_skipped", reason="pilot_tab_empty")
        return stats

    processor = None
    updates: list[dict[str, Any]] = []
    promoted_listing_keys: set[str] = set()
    promoted_agent_names: set[str] = set()

    for sheet_row, row in enumerate(pilot_rows[1:], start=2):
        row_data = pilot_row_map(row)
        if row_data.get("promotion_status", "").strip().lower() != "shadow_ready":
            continue
        stats["considered"] += 1
        payload, payload_failure = parse_pilot_payload(row_data)
        if payload_failure:
            stats["skipped"] += 1
            updates.extend(
                promotion_status_updates(
                    pilot_tab,
                    sheet_row,
                    status="needs_review",
                    notes=payload_failure,
                    import_ready="review",
                )
            )
            log_event("pilot_promotion_skipped", row=sheet_row, reason=payload_failure)
            continue

        failure_status, failure_note, matched = pilot_row_preflight_failure(row_data, payload, existing)
        candidate = candidate_from_pilot_row(row_data, payload)
        listing_key = canonical_listing_address_key(
            candidate.fields.get("listing_address", ""),
            candidate.fields.get("state", ""),
        )
        name_key = agent_name_key(candidate.fields.get("agent_name", ""))
        if not failure_status and listing_key and listing_key in promoted_listing_keys:
            failure_status = "skipped_duplicate_listing"
            failure_note = "Duplicate normalized address within this promotion batch."
        if failure_status:
            stats["skipped"] += 1
            updates.extend(
                promotion_status_updates(
                    pilot_tab,
                    sheet_row,
                    status=failure_status,
                    notes=failure_note,
                    import_ready="review",
                    matched_main_row=matched or None,
                )
            )
            log_event(
                "pilot_promotion_skipped",
                row=sheet_row,
                reason=failure_status,
                matched_main_row=matched,
                zpid=row_data.get("synthetic_zpid", ""),
            )
            continue

        stats["eligible"] += 1
        normalized_payload = normalize_payload_for_sheet1(row_data, payload)
        zpid = normalized_payload.get("zpid", "")
        if dry_run:
            stats["promoted"] += 1
            if listing_key:
                promoted_listing_keys.add(listing_key)
            if name_key:
                promoted_agent_names.add(name_key)
            updates.extend(
                promotion_status_updates(
                    pilot_tab,
                    sheet_row,
                    status="dry_run_ready",
                    notes="Dry run: row would be promoted through bot_min.process_rows.",
                )
            )
            log_event("pilot_promotion_dry_run_ready", row=sheet_row, zpid=zpid)
        else:
            try:
                if processor is None:
                    processor = import_bot_processor()
                outcomes = processor.process_rows(
                    [normalized_payload],
                    skip_dedupe=True,
                    return_outcomes=True,
                ) or {}
                outcome = outcomes.get(zpid, "")
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                updates.extend(
                    promotion_status_updates(
                        pilot_tab,
                        sheet_row,
                        status="promotion_error",
                        notes=str(exc),
                        import_ready="review",
                    )
                )
                log_event("pilot_promotion_error", row=sheet_row, zpid=zpid, error=str(exc))
                continue

            if outcome == "completed_short_sale":
                stats["promoted"] += 1
                if listing_key:
                    promoted_listing_keys.add(listing_key)
                if name_key:
                    promoted_agent_names.add(name_key)
                updates.extend(
                    promotion_status_updates(
                        pilot_tab,
                        sheet_row,
                        status="promoted",
                        notes="Promoted to Sheet1 through bot_min.process_rows; pilot-origin SMS remains verifier-held.",
                        import_ready="promoted",
                    )
                )
                log_event("pilot_promoted", row=sheet_row, zpid=zpid, outcome=outcome)
            else:
                stats["skipped"] += 1
                updates.extend(
                    promotion_status_updates(
                        pilot_tab,
                        sheet_row,
                        status=outcome or "promotion_skipped",
                        notes=f"bot_min.process_rows returned {outcome or 'no outcome'}.",
                        import_ready="review",
                    )
                )
                log_event("pilot_promotion_processor_skipped", row=sheet_row, zpid=zpid, outcome=outcome)

        if stats["promoted"] >= cap:
            break

    if updates and not dry_run:
        batch_update_values(token, spreadsheet_id, updates)
    log_event("pilot_promotion_done", stats=stats, cap=cap, dry_run=dry_run)
    return stats


def make_run_context(args: argparse.Namespace) -> dict[str, Any]:
    run_date = parse_run_date(args.run_date)
    started_at = dt.datetime.now(dt.timezone.utc)
    receipt_id = normalize_space(getattr(args, "run_receipt_id", "")) or hashlib.sha256(
        f"{run_date.isoformat()}|{started_at.isoformat()}|{os.getpid()}".encode("utf-8")
    ).hexdigest()[:16]
    return {
        "run_date": run_date,
        "started_at": started_at,
        "run_receipt_id": receipt_id,
        "stage": "initializing",
        "stats": {},
    }


def update_run_stage(context: dict[str, Any], stage: str) -> None:
    context["stage"] = stage
    _active_run_event_context["run_stage"] = stage


def persist_run_slot_terminal(
    args: argparse.Namespace,
    context: dict[str, Any],
    *,
    status: str,
    pipeline_complete: bool,
    detail: str = "",
) -> bool:
    schedule_slot_id = normalize_space(getattr(args, "schedule_slot_id", ""))
    if not schedule_slot_id or not context.get("slot_claimed"):
        return True
    try:
        append_run_slot_receipt(
            context["token"],
            args.spreadsheet_id,
            schedule_slot_id=schedule_slot_id,
            run_receipt_id=context["run_receipt_id"],
            run_date=context["run_date"],
            run_mode=_active_run_event_context.get("run_mode", "scheduled_source"),
            status=status,
            pipeline_complete=pipeline_complete,
            detail=detail,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log_event(
            "pilot_run_slot_persistence_failed",
            schedule_slot_id=schedule_slot_id,
            attempted_status=status,
            error_type=type(exc).__name__,
            error=str(exc)[:500],
        )
        return False


def source_pipeline_complete(
    stats: dict[str, Any],
    promotion_stats: dict[str, Any],
    direct_monitor_stats: dict[str, Any],
    *,
    durability_persistence_confirmed: bool,
) -> tuple[bool, bool]:
    query_attempts_accounted = stats["searched"] == (
        stats["search_succeeded"] + stats["search_blocked"] + stats["search_failed"]
    )
    complete = bool(
        stats["planned_searches"] == stats["searched"]
        and query_attempts_accounted
        and stats["search_blocked"] == 0
        and stats["search_failed"] == 0
        and stats["search_engine_attempts"].get("blocked", 0) == 0
        and stats["search_engine_attempts"].get("failed", 0) == 0
        and int(stats.get("source_evidence_failed", 0)) == 0
        and durability_persistence_confirmed
        and int(promotion_stats.get("errors", 0)) == 0
        and bool(direct_monitor_stats.get("complete", True))
    )
    return complete, query_attempts_accounted


def run(args: argparse.Namespace, *, run_context: dict[str, Any] | None = None) -> None:
    reset_agent_shadow_consensus_state()
    reset_site_chrome_prewrite_state()
    reset_search_engine_attempt_stats()
    context = run_context if run_context is not None else make_run_context(args)
    run_date = context["run_date"]
    run_started_at = context["started_at"]
    run_receipt_id = context["run_receipt_id"]
    audit_links_only = bool(getattr(args, "audit_links_only", False))
    set_run_event_context(
        run_receipt_id=run_receipt_id,
        run_date=run_date.isoformat(),
        dry_run=bool(getattr(args, "dry_run", False)),
        run_mode="link_audit" if audit_links_only else "scheduled_source" if getattr(args, "scheduled_run", False) else "manual_source",
    )
    log_event(
        "pilot_run_start",
        started_at=run_started_at.isoformat(),
        scheduled_run=bool(getattr(args, "scheduled_run", False)),
        audit_links_only=audit_links_only,
    )
    update_run_stage(context, "configuration")
    source_queries = [] if audit_links_only else configured_source_queries(run_date)
    search_plan = [] if audit_links_only else configured_search_plan(args.states, run_date, source_queries)
    planned_searches = len(search_plan)
    stats = {
        "planned_searches": planned_searches,
        "searched": 0,
        "search_succeeded": 0,
        "search_blocked": 0,
        "search_failed": 0,
        "search_failure_reasons": {},
        "results": 0,
        "raw_results": 0,
        "unique_result_urls": 0,
        "unique_listing_urls": 0,
        "listing_specific_candidates": 0,
        "allowed_listing_result_occurrences": 0,
        "fetch_operations": 0,
        "unique_listing_pages_fetched": 0,
        "fetched": 0,
        "qualified_before_dedupe": 0,
        "qualified_listing_duplicates": 0,
        "qualified_contact_duplicates": 0,
        "duplicate_url_prequalification": 0,
        "net_new_qualified": 0,
        "qualified": 0,
        "shadow_ready": 0,
        "duplicates": 0,
        "rejected": 0,
        "fetch_failed": 0,
        "fetch_failure_reasons": {},
        "rejection_reasons": {},
        "rows_written": 0,
        "source_evidence_persisted": 0,
        "source_evidence_failed": 0,
    }
    context["stats"] = stats
    experiment_baselines = {
        source_query.source: query_exclusion_baseline_states(args.states, source_query.source)
        for source_query in source_queries
    }
    log_event(
        "pilot_run_plan",
        run_receipt_id=run_receipt_id,
        started_at=run_started_at.isoformat(),
        run_date=run_date.isoformat(),
        scheduled_run=bool(getattr(args, "scheduled_run", False)),
        audit_links_only=audit_links_only,
        planned_searches=planned_searches,
        source_plan=SOURCE_PLAN,
        states=getattr(args, "states", []),
        source_count=len(source_queries),
        source_buckets=[query.source for query in source_queries],
        source_date_restricts={query.source: query.date_restrict for query in source_queries},
        source_allocations={
            source: sum(1 for entry in search_plan if entry.source_query.source == source)
            for source in sorted({entry.source_query.source for entry in search_plan})
        },
        source_page_allocations={
            source: {
                f"p{page}": sum(
                    1
                    for entry in search_plan
                    if entry.source_query.source == source
                    and ((entry.result_start - 1) // 10) + 1 == page
                )
                for page in sorted({
                    ((entry.result_start - 1) // 10) + 1
                    for entry in search_plan
                    if entry.source_query.source == source
                })
            }
            for source in sorted({entry.source_query.source for entry in search_plan})
        },
        baseline_states=permanent_baseline_states(getattr(args, "states", []), run_date)
        if SOURCE_PLAN == "idx_permanent_90_10" else [],
        results_per_query=getattr(args, "results_per_query", 0),
        search_engine=SEARCH_ENGINE,
        ddg_fallback_allowed=ALLOW_DDG_FALLBACK,
        cse_configured=bool(CSE_API_KEY and CSE_CX),
        shadow_mode=SHADOW_MODE,
        shadow_review_target=SHADOW_REVIEW_TARGET,
        shadow_review_days=SHADOW_REVIEW_DAYS,
        automatic_promotion=getattr(args, "promote_ready", False),
        promotion_daily_cap=getattr(args, "promotion_daily_cap", 0),
        promotion_dry_run=getattr(args, "promotion_dry_run", False),
        dry_run=getattr(args, "dry_run", False),
        query_exclusion_experiment_active=experiment_active(
            run_date,
            QUERY_EXCLUSION_EXPERIMENT_START_DATE,
            QUERY_EXCLUSION_EXPERIMENT_DAYS,
        ),
        query_exclusion_domains=QUERY_EXCLUSION_DOMAINS,
        query_exclusion_baseline_states={
            source: sorted(states) for source, states in experiment_baselines.items()
        },
    )
    if not audit_links_only and planned_searches <= 0:
        update_run_stage(context, "configuration")
        raise RuntimeError("no planned source searches; states and source queries must both be configured")
    if (
        not audit_links_only
        and SEARCH_ENGINE in {"cse", "google", "google_cse"}
        and not (CSE_API_KEY and CSE_CX)
    ):
        update_run_stage(context, "configuration")
        raise RuntimeError("configured CSE search engine is missing CSE_API_KEY or CSE_CX")

    update_run_stage(context, "configuration")
    validate_run_receipt_tab(args.main_tab, args.pilot_tab)
    validate_source_evidence_tab(args.main_tab, args.pilot_tab)
    update_run_stage(context, "authentication")
    service_account = load_service_account_info(args.service_account)
    token = sheets_client(service_account)
    context["token"] = token
    schedule_slot_id = normalize_space(getattr(args, "schedule_slot_id", ""))
    if schedule_slot_id:
        update_run_stage(context, "slot_claim")
        claimed, claim_reason, recovery_query_keys = claim_run_schedule_slot(
            token,
            args.spreadsheet_id,
            schedule_slot_id=schedule_slot_id,
            run_receipt_id=run_receipt_id,
            run_date=run_date,
            run_mode=_active_run_event_context.get("run_mode", "scheduled_source"),
        )
        context["slot_claimed"] = claimed
        context["recovery_query_keys"] = recovery_query_keys
        log_event(
            "pilot_run_slot_claim",
            schedule_slot_id=schedule_slot_id,
            claimed=claimed,
            reason=claim_reason,
            durable_surface=f"{RUN_RECEIPT_TAB}",
        )
        if not claimed:
            clear_run_event_context()
            return
        if recovery_query_keys:
            full_plan_searches = len(search_plan)
            entries_by_key = {
                source_query_key(entry.state, entry.source_query.source, entry.result_start): entry
                for entry in search_plan
            }
            # Preserve recovery compatibility for pre-allocation page-one manifests.
            for state in args.states:
                for source_query in source_queries:
                    legacy_entry = SearchPlanEntry(state, source_query, 1)
                    entries_by_key.setdefault(
                        source_query_key(state, source_query.source),
                        legacy_entry,
                    )
            if not set(recovery_query_keys).issubset(entries_by_key):
                raise RuntimeError("durable recovery manifest contains an unconfigured query key")
            search_plan = [entries_by_key[key] for key in recovery_query_keys]
            planned_searches = len(recovery_query_keys)
            stats["planned_searches"] = planned_searches
            log_event(
                "pilot_query_recovery_plan",
                query_keys=recovery_query_keys,
                planned_searches=planned_searches,
                max_recovery_queries=source_query_recovery_limit(run_date),
                full_plan_searches=full_plan_searches,
            )
    if audit_links_only:
        update_run_stage(context, "link_audit")
        audit_stats = run_linkage_and_suffix_audits(
            token,
            args.spreadsheet_id,
            args.main_tab,
            args.pilot_tab,
            run_date=run_date,
            phase=args.audit_phase,
            force=args.force_review_experiments,
        )
        audit_stopped = any(
            bool(value)
            for key, value in audit_stats.items()
            if key == "stopped" or key.endswith("_stopped")
        )
        audit_complete = (
            bool(audit_stats.get("audit_checks_planned", 0))
            and not bool(audit_stats.get("unconfirmed", 0))
            and not bool(audit_stats.get("audit_evidence_unconfirmed", 0))
            and not audit_stopped
        )
        update_run_stage(context, "completed")
        if not persist_run_slot_terminal(
            args,
            context,
            status="completed" if audit_complete else "completed_degraded",
            pipeline_complete=audit_complete,
            detail=("" if audit_complete else "audit evidence incomplete or stopped"),
        ):
            raise RuntimeError("post-verifier audit completed but durable slot receipt failed")
        log_event(
            "pilot_run_done",
            started_at=run_started_at.isoformat(),
            completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            execution_terminal="completed",
            pipeline_complete=audit_complete,
            scheduled_run_proven_complete=False,
            stats=stats,
            audit_stats=audit_stats,
        )
        clear_run_event_context()
        return
    update_run_stage(context, "sheet_setup")
    ensure_tab(token, args.spreadsheet_id, args.pilot_tab)
    recovery_query_keys = list(context.get("recovery_query_keys", []))
    recovery_query_key_set = set(recovery_query_keys)
    durability_active = source_durability_audit_active(run_date) and not recovery_query_keys
    durability_state = (
        load_source_durability_state()
        if durability_active
        else {"version": 1, "candidates": []}
    )
    durability_state_dirty = False
    update_run_stage(context, "sheet_read")
    main_rows = get_values(token, args.spreadsheet_id, f"{args.main_tab}!A:AQ")
    existing = build_existing_index(main_rows)
    pilot_rows = get_values(token, args.spreadsheet_id, f"{args.pilot_tab}!A:{column_letter(len(PILOT_HEADERS))}")
    already_seen_urls = {row[11] for row in pilot_rows[1:] if len(row) > 11 and row[11]}
    pilot_seen_addresses = {
        canonical_listing_address_key(row[4], row[6])
        for row in pilot_rows[1:]
        if len(row) > 6 and canonical_listing_address_key(row[4], row[6])
    }
    exclusion_stats = {
        source_query.source: {
            arm: {
                "searched": 0,
                "results": 0,
                "fetched": 0,
                "qualified": 0,
                "duplicates": 0,
                "rejected": 0,
                "fetch_failed": 0,
                "rows_written": 0,
                "excluded_domain_strict_hits": 0,
            }
            for arm in ("baseline", "excluded")
        }
        for source_query in source_queries
    }
    unique_result_urls: set[str] = set()
    unique_listing_urls: set[str] = set()
    unique_fetched_urls: set[str] = set()
    unique_short_sale_candidates: set[str] = set()
    update_run_stage(context, "discovery")
    failed_query_keys: list[str] = []
    search_plan_by_state: dict[str, list[SearchPlanEntry]] = {}
    for entry in search_plan:
        search_plan_by_state.setdefault(entry.state, []).append(entry)
    for state in args.states:
        state_query_term = STATE_QUERY_TERMS.get(state.upper(), state)
        for search_entry in search_plan_by_state.get(state, []):
            source_query = search_entry.source_query
            source = source_query.source
            query_key = source_query_key(state, source, search_entry.result_start)
            if recovery_query_key_set and query_key not in recovery_query_key_set:
                continue
            exclusion_arm = query_exclusion_arm(
                run_date,
                state,
                source,
                experiment_baselines,
            )
            query = query_with_exclusion_experiment(
                source_query.template,
                state_query_term,
                exclusion_arm,
            )
            stats["searched"] += 1
            if exclusion_arm in {"baseline", "excluded"}:
                exclusion_stats[source][exclusion_arm]["searched"] += 1
            log_event(
                "pilot_query_start",
                state=state,
                source=source,
                query=query,
                date_restrict=source_query.date_restrict,
                result_start=search_entry.result_start,
                query_key=query_key,
                experiment_arm=exclusion_arm,
                excluded_domains=QUERY_EXCLUSION_DOMAINS if exclusion_arm == "excluded" else (),
            )
            try:
                search_kwargs = {"date_restrict": source_query.date_restrict}
                if search_entry.result_start != 1:
                    search_kwargs["start_index"] = search_entry.result_start
                engine, results = search_web(query, source, args.results_per_query, **search_kwargs)
            except Exception as exc:  # noqa: BLE001
                failed_query_keys.append(query_key)
                error_text = str(exc)
                failure_class = source_error_class(exc)
                blocked = failure_class == "blocked"
                stats["search_blocked" if blocked else "search_failed"] += 1
                increment_reason(stats, "search_failure_reasons", source_failure_reason(exc))
                log_event(
                    "pilot_query_failed",
                    state=state,
                    source=source,
                    run_date=run_date,
                    query=query,
                    date_restrict=source_query.date_restrict,
                    error=error_text,
                    failure_class=failure_class,
                    blocked=blocked,
                    experiment_arm=exclusion_arm,
                )
                continue
            stats["search_succeeded"] += 1
            stats["results"] += len(results)
            stats["raw_results"] += len(results)
            unique_result_urls.update(
                identity for identity in (result_url_identity(result.url) for result in results) if identity
            )
            stats["unique_result_urls"] = len(unique_result_urls)
            if exclusion_arm in {"baseline", "excluded"}:
                exclusion_stats[source][exclusion_arm]["results"] += len(results)
            log_event(
                "pilot_query_results",
                state=state,
                source=source,
                engine=engine,
                query=query,
                date_restrict=source_query.date_restrict,
                result_start=search_entry.result_start,
                query_key=query_key,
                result_count=len(results),
                experiment_arm=exclusion_arm,
            )
            time.sleep(args.sleep_seconds)
            query_rows: list[list[str]] = []
            query_stats = {
                "fetched": 0,
                "qualified": 0,
                "shadow_ready": 0,
                "duplicates": 0,
                "rejected": 0,
                "fetch_failed": 0,
            }
            for result in results:
                result_source_ref = safe_source_reference(result.url)
                if result.url in already_seen_urls or result_source_ref in already_seen_urls:
                    stats["duplicates"] += 1
                    stats["duplicate_url_prequalification"] += 1
                    query_stats["duplicates"] += 1
                    log_event("pilot_duplicate_url", state=state, source=source, url=result.url)
                    continue
                allowed, reason = source_result_allowed(result)
                if not allowed:
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    increment_reason(stats, "rejection_reasons", reason)
                    log_event("pilot_result_skipped", state=state, source=source, url=result.url, reason=reason)
                    continue
                listing_url_key = result_url_identity(result.url)
                if listing_url_key:
                    unique_listing_urls.add(listing_url_key)
                stats["unique_listing_urls"] = len(unique_listing_urls)
                stats["allowed_listing_result_occurrences"] += 1
                stats["fetch_operations"] += 1
                try:
                    markup = fetch_url(result.url)
                    stats["fetched"] += 1
                    if listing_url_key:
                        unique_fetched_urls.add(listing_url_key)
                    stats["unique_listing_pages_fetched"] = len(unique_fetched_urls)
                    query_stats["fetched"] += 1
                except Exception as exc:  # noqa: BLE001
                    stats["fetch_failed"] += 1
                    query_stats["fetch_failed"] += 1
                    increment_reason(stats, "fetch_failure_reasons", source_failure_reason(exc))
                    log_event("pilot_fetch_failed", state=state, source=source, url=result.url, error=str(exc))
                    continue
                candidate = infer_fields(result, markup)
                log_agent_shadow(candidate)
                scoped_description = normalize_space(candidate.fields.get("listing_description", ""))
                short_sale_without_navigation = SITE_CHROME_SHORT_SALE_NAVIGATION_RE.sub(
                    " ", scoped_description
                )
                if (
                    candidate.fields.get("exact_listing_confirmed") == "true"
                    and SHORT_SALE_LISTING_RE.search(short_sale_without_navigation)
                ):
                    candidate_key = candidate.fields.get("listing_identity_group") or stable_synthetic_zpid(
                        candidate.source,
                        candidate.url,
                        candidate.fields.get("listing_address", ""),
                        candidate.fields.get("city", ""),
                        candidate.fields.get("state", ""),
                    )
                    if candidate_key:
                        unique_short_sale_candidates.add(candidate_key)
                    stats["listing_specific_candidates"] = len(unique_short_sale_candidates)
                if not looks_like_listing_address(candidate.fields.get("listing_address", "")):
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    increment_reason(stats, "rejection_reasons", "missing_listing_detail_address")
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
                    increment_reason(stats, "rejection_reasons", "listing_state_mismatch")
                    log_event(
                        "pilot_candidate_rejected",
                        state=state,
                        source=source,
                        url=result.url,
                        reason="listing_state_mismatch",
                        evidence=candidate.fields.get("state", "")[:40],
                    )
                    continue
                qualification = qualification_for_candidate(candidate)
                log_site_chrome_prewrite_receipt(
                    candidate,
                    qualification,
                    state=state,
                    source=source,
                    run_date=run_date,
                )
                if qualification.status != "qualified":
                    stats["rejected"] += 1
                    query_stats["rejected"] += 1
                    increment_reason(stats, "rejection_reasons", qualification.failure_reason)
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
                stats["qualified_before_dedupe"] += 1
                if durability_active:
                    durability_state_dirty = observe_source_durability_candidate(
                        durability_state,
                        candidate,
                        captured_at=dt.datetime.now(dt.timezone.utc),
                        primary_eligible=False,
                    ) or durability_state_dirty
                listing_dup_status, listing_dup_key, listing_matched = duplicate_listing_status(candidate, existing)
                if listing_dup_status:
                    stats["duplicates"] += 1
                    stats["qualified_listing_duplicates"] += 1
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
                    stats["qualified_listing_duplicates"] += 1
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
                    increment_reason(stats, "rejection_reasons", required_failure)
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
                if duplicate_status_blocks_pilot_row(dup_status):
                    stats["duplicates"] += 1
                    if dup_status == "duplicate_listing":
                        stats["qualified_listing_duplicates"] += 1
                    else:
                        stats["qualified_contact_duplicates"] += 1
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
                stats["net_new_qualified"] += 1
                query_stats["qualified"] += 1
                if not args.dry_run:
                    receipt = persist_candidate_source_evidence(
                        token,
                        args.spreadsheet_id,
                        candidate,
                    )
                    if receipt:
                        stats["source_evidence_persisted"] += 1
                    else:
                        stats["source_evidence_failed"] += 1
                row = candidate_to_row(candidate, qualification, dup_key, matched_main_row, agent_rows)
                query_rows.append(row)
                if durability_active:
                    durability_state_dirty = observe_source_durability_candidate(
                        durability_state,
                        candidate,
                        captured_at=dt.datetime.now(dt.timezone.utc),
                        primary_eligible=True,
                    ) or durability_state_dirty
                if row[14] == "shadow_ready":
                    stats["shadow_ready"] += 1
                    query_stats["shadow_ready"] += 1
                    log_event(
                        "pilot_shadow_ready",
                        state=state,
                        source=source,
                        url=result.url,
                        address=candidate.fields.get("listing_address", ""),
                        agent=candidate.fields.get("agent_name", ""),
                        automatic_promotion=args.promote_ready,
                    )
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
                    promotion_status=row[14],
                    experiment_arm=exclusion_arm,
                )
                result_host = urllib.parse.urlparse(result.url).netloc.lower()
                if (
                    exclusion_arm == "baseline"
                    and any(
                        result_host == domain or result_host.endswith("." + domain)
                        for domain in QUERY_EXCLUSION_DOMAINS
                    )
                ):
                    exclusion_stats[source][exclusion_arm]["excluded_domain_strict_hits"] += 1
                    log_event(
                        "pilot_query_exclusion_stop_recommended",
                        run_date=run_date.isoformat(),
                        state=state,
                        source=source,
                        experiment_arm=exclusion_arm,
                        reason="baseline_strict_lead_from_excluded_domain",
                        domain=result_host,
                    )
                already_seen_urls.add(result.url)
                if result_source_ref:
                    already_seen_urls.add(result_source_ref)
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
            if exclusion_arm in {"baseline", "excluded"}:
                arm_stats = exclusion_stats[source][exclusion_arm]
                for metric in (
                    "fetched",
                    "qualified",
                    "duplicates",
                    "rejected",
                    "fetch_failed",
                ):
                    arm_stats[metric] += query_stats[metric]
                arm_stats["rows_written"] += 0 if args.dry_run else len(query_rows)
            log_event(
                "pilot_query_done",
                state=state,
                source=source,
                date_restrict=source_query.date_restrict,
                result_start=search_entry.result_start,
                query_key=query_key,
                rows_written=0 if args.dry_run else len(query_rows),
                experiment_arm=exclusion_arm,
                **query_stats,
            )

    durability_persistence_confirmed = True
    if durability_active:
        state_saved = True
        if durability_state_dirty:
            state_saved = save_source_durability_state(durability_state)
        log_event(
            "pilot_source_durability_collection_done",
            run_date=run_date.isoformat(),
            retained=min(
                len(durability_state.get("candidates", [])),
                SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES,
            ),
            max_candidates=SOURCE_DURABILITY_AUDIT_MAX_CANDIDATES,
            state_updated=durability_state_dirty and state_saved,
            state_persistence_confirmed=state_saved,
            exact_urls_retained_in_private_audit_state=True,
            lead_data_writes=0,
            searches_added=0,
            sends=0,
        )
        durability_persistence_confirmed = state_saved
    stats["durability_state_persistence_confirmed"] = durability_persistence_confirmed

    if experiment_active(
        run_date,
        QUERY_EXCLUSION_EXPERIMENT_START_DATE,
        QUERY_EXCLUSION_EXPERIMENT_DAYS,
    ):
        log_event(
            "pilot_query_exclusion_experiment_done",
            run_date=run_date.isoformat(),
            window_days=QUERY_EXCLUSION_EXPERIMENT_DAYS,
            baseline_per_bucket=QUERY_EXCLUSION_BASELINE_PER_BUCKET,
            excluded_domains=QUERY_EXCLUSION_DOMAINS,
            same_search_budget=True,
            stats=exclusion_stats,
            success_metric="at_least_20pct_fewer_fetch_failures_without_lower_strict_yield",
            stop_condition=(
                "baseline_strict_lead_from_excluded_domain_or_excluded_arm_underperforms_after_300_searches"
            ),
        )
    promotion_stats: dict[str, Any] = {"enabled": False}
    if args.promote_ready:
        update_run_stage(context, "promotion")
        promotion_stats = promote_ready_pilot_rows(
            token,
            args.spreadsheet_id,
            args.main_tab,
            args.pilot_tab,
            cap=args.promotion_daily_cap,
            dry_run=args.dry_run or args.promotion_dry_run,
        )
        if not args.dry_run and not args.promotion_dry_run:
            run_linkage_and_suffix_audits(
                token,
                args.spreadsheet_id,
                args.main_tab,
                args.pilot_tab,
                run_date=run_date,
                phase="post_promotion",
                force=args.force_review_experiments,
            )
    update_run_stage(context, "direct_monitor")
    if recovery_query_keys:
        direct_monitor_stats = {
            "complete": True,
            "selected": 0,
            "skipped": "bounded_query_recovery",
        }
    else:
        direct_monitor_stats = run_direct_monitor(
            run_date,
            already_seen_urls,
            existing,
            pilot_seen_addresses,
            sleep_seconds=min(args.sleep_seconds, 0.25),
        )
    stats["search_engine_attempts"] = dict(_search_engine_attempt_stats)
    pipeline_complete, query_attempts_accounted = source_pipeline_complete(
        stats,
        promotion_stats,
        direct_monitor_stats,
        durability_persistence_confirmed=durability_persistence_confirmed,
    )
    scheduled_run = bool(getattr(args, "scheduled_run", False))
    schedule_slot_id = normalize_space(getattr(args, "schedule_slot_id", ""))
    if scheduled_run and not schedule_slot_id:
        pipeline_complete = False
    update_run_stage(context, "slot_terminal")
    if recovery_query_keys:
        terminal_detail = recovery_manifest_detail(
            RECOVERY_COMPLETED_PREFIX if pipeline_complete else RECOVERY_EXHAUSTED_PREFIX,
            recovery_query_keys if pipeline_complete else failed_query_keys or recovery_query_keys,
        )
    else:
        recoverable_query_keys = sorted(set(failed_query_keys))
        recovery_limit = source_query_recovery_limit(run_date)
        if not pipeline_complete and 0 < len(recoverable_query_keys) <= recovery_limit:
            terminal_detail = recovery_manifest_detail(RECOVERY_PENDING_PREFIX, recoverable_query_keys)
        elif not pipeline_complete and len(recoverable_query_keys) > recovery_limit:
            terminal_detail = f"recovery_not_bounded_v1=count:{len(recoverable_query_keys)}"
        else:
            terminal_detail = "" if pipeline_complete else "pipeline completion gate failed"
    slot_receipt_persisted = persist_run_slot_terminal(
        args,
        context,
        status="completed" if pipeline_complete else "completed_degraded",
        pipeline_complete=pipeline_complete,
        detail=terminal_detail,
    )
    if not slot_receipt_persisted:
        pipeline_complete = False
    update_run_stage(context, "completed")
    log_event(
        "pilot_run_done",
        run_receipt_id=run_receipt_id,
        run_date=run_date.isoformat(),
        started_at=run_started_at.isoformat(),
        completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        execution_terminal="completed",
        schedule_slot_id=schedule_slot_id,
        slot_receipt_persisted=slot_receipt_persisted,
        scheduled_run=scheduled_run,
        scheduled_run_proven_complete=bool(scheduled_run and pipeline_complete),
        pipeline_complete=pipeline_complete,
        completion_status="complete" if pipeline_complete else "completed_degraded",
        query_attempts_accounted=query_attempts_accounted,
        stats=stats,
        promotion_stats=promotion_stats,
        direct_monitor_stats=direct_monitor_stats,
        dry_run=args.dry_run,
    )
    clear_run_event_context()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Free-source short sale listing pilot")
    parser.add_argument("--spreadsheet-id", default=os.getenv("GSHEET_ID", SPREADSHEET_ID))
    parser.add_argument("--main-tab", default=os.getenv("GSHEET_TAB", MAIN_TAB))
    parser.add_argument("--pilot-tab", default=os.getenv("PILOT_TAB", PILOT_TAB))
    parser.add_argument("--service-account", default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    parser.add_argument("--states", nargs="+", default=DEFAULT_STATES)
    parser.add_argument("--results-per-query", type=int, default=10)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--run-date", default=os.getenv("FREE_SOURCE_PILOT_RUN_DATE"))
    parser.add_argument("--include-rejected", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--promote-ready", action="store_true", default=PROMOTION_ENABLED)
    parser.add_argument("--promotion-daily-cap", type=int, default=PROMOTION_DAILY_CAP)
    parser.add_argument("--promotion-dry-run", action="store_true", default=PROMOTION_DRY_RUN)
    parser.add_argument("--audit-links-only", action="store_true")
    parser.add_argument(
        "--audit-phase",
        choices=("post_promotion", "post_verifier"),
        default="post_verifier",
    )
    parser.add_argument("--force-review-experiments", action="store_true")
    parser.add_argument("--scheduled-run", action="store_true")
    parser.add_argument("--run-receipt-id", default=os.getenv("FREE_SOURCE_PILOT_RUN_RECEIPT_ID", ""))
    parser.add_argument("--schedule-slot-id", default=os.getenv("FREE_SOURCE_PILOT_SCHEDULE_SLOT_ID", ""))
    return parser.parse_args()


def run_with_terminal_receipt(args: argparse.Namespace) -> None:
    context = make_run_context(args)
    try:
        run(args, run_context=context)
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            failure_type = "KeyboardInterrupt"
        else:
            failure_type = type(exc).__name__
        if not _active_run_event_context:
            set_run_event_context(
                run_receipt_id=context["run_receipt_id"],
                run_date=context["run_date"].isoformat(),
                dry_run=bool(getattr(args, "dry_run", False)),
                run_mode="link_audit" if getattr(args, "audit_links_only", False) else "scheduled_source" if getattr(args, "scheduled_run", False) else "manual_source",
            )
        update_run_stage(context, context.get("stage", "unknown"))
        slot_failure_persisted = persist_run_slot_terminal(
            args,
            context,
            status="failed",
            pipeline_complete=False,
            detail=(
                recovery_manifest_detail(
                    RECOVERY_EXHAUSTED_PREFIX,
                    context.get("recovery_query_keys", []),
                )
                + f"; {failure_type}: {str(exc)[:300]}"
                if context.get("recovery_query_keys")
                else f"{failure_type}: {str(exc)[:300]}"
            ),
        )
        log_event(
            "pilot_run_terminal_failure",
            run_receipt_id=context["run_receipt_id"],
            run_date=context["run_date"].isoformat(),
            started_at=context["started_at"].isoformat(),
            completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            execution_terminal="failed",
            scheduled_run=bool(getattr(args, "scheduled_run", False)),
            scheduled_run_proven_complete=False,
            failure_stage=context.get("stage", "unknown"),
            error_type=failure_type,
            error=str(exc)[:500],
            partial_stats=context.get("stats", {}),
            search_engine_attempts=dict(_search_engine_attempt_stats),
            slot_failure_persisted=slot_failure_persisted,
        )
        clear_run_event_context()
        raise


if __name__ == "__main__":
    parsed_args = parse_args()
    run_with_terminal_receipt(parsed_args)
