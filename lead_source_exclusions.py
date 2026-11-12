"""Source-specific intake exclusions; never infer exclusions from listing prose."""

import re
from urllib.parse import urlparse


_AUCTION_COM_NAME = re.compile(
    r"(?<![a-z0-9])auction(?:\s*\.\s*|\s+)com(?![a-z0-9])", re.I
)


def is_auction_com_identity(value):
    return isinstance(value, str) and bool(_AUCTION_COM_NAME.search(value))


def auction_com_listing_excluded(payload):
    """Match the actual provider/contact, not a passing mention in remarks."""
    for key in (
        "agentName", "agent_name", "brokerName", "brokerageName", "brokerage",
        "source", "search_source", "listingProvider", "broker", "agentEmail",
    ):
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("name")
        if is_auction_com_identity(value):
            return True
    for key in (
        "detailUrl", "listingUrl", "listing_url", "propertyUrl", "sourceUrl", "source_url",
        "agentWebsite", "brokerWebsite",
    ):
        value = payload.get(key)
        if not isinstance(value, str):
            continue
        host = (urlparse(value if "://" in value else "//" + value).hostname or "").lower().rstrip(".")
        if host == "auction.com" or host.endswith(".auction.com"):
            return True
    return False
