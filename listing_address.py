"""Shared, network-free listing address normalization for intake and sheet writes."""

import re
from typing import Any, Dict


STATE_CODES = set("AL AK AZ AR CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY DC".split())
FIELDS = {
    "street": ("street", "streetAddress", "streetAddress1", "addressStreet", "addressLine1", "line1", "listing_address"),
    "city": ("city", "addressCity", "locality", "addressLocality"),
    "state": ("state", "addressState", "region", "addressRegion"),
    "zip": ("zip", "zipcode", "zipCode", "postalCode", "addressZip", "addressZipcode"),
}
FULL_KEYS = ("full_address", "fullAddress", "formattedAddress", "displayAddress", "full", "value")


def clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = re.sub(r"\s+", " ", value).strip()
    return "" if text.lower() in {"null", "none", "unknown", "n/a", "undefined"} else text


def parse_full_address(value: Any) -> Dict[str, str]:
    parts = [part.strip() for part in clean_text(value).split(",") if part.strip()]
    if len(parts) < 2:
        return {}
    if parts[-1].upper() in {"USA", "US", "UNITED STATES"}:
        parts.pop()
    postal = ""
    if parts and re.fullmatch(r"\d{5}(?:-\d{4})?", parts[-1]):
        postal = parts.pop()
    if len(parts) < 2:
        return {}
    match = re.fullmatch(r"(?:(.*?)\s+)?([A-Za-z]{2})(?:\s+(\d{5}(?:-\d{4})?))?", parts[-1])
    if not match or match[2].upper() not in STATE_CODES:
        return {}
    city = clean_text(match[1])
    before = parts[:-1]
    if not city and len(before) >= 2:
        city = before.pop()
    if not city or not before:
        return {}
    result = {"street": ", ".join(before), "city": city, "state": match[2].upper()}
    if postal or match[3]:
        result["zip"] = postal or match[3]
    return result


def extract_address_fields(payload: Dict[str, Any]) -> Dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    containers = [payload]
    for key in ("property", "listing", "home"):
        if isinstance(payload.get(key), dict):
            containers.append(payload[key])
    home_info = (payload.get("hdpData") or {}).get("homeInfo") if isinstance(payload.get("hdpData"), dict) else None
    if isinstance(home_info, dict):
        containers.append(home_info)
    addresses = [container.get(key) for container in containers for key in ("address", "listingAddress")]
    sources = containers + [address for address in addresses if isinstance(address, dict)]
    result: Dict[str, str] = {}
    for source in sources:
        for field, keys in FIELDS.items():
            for key in keys:
                value = clean_text(source.get(key))
                if field == "state":
                    value = value.upper() if value.upper() in STATE_CODES else ""
                if value:
                    result.setdefault(field, value)
                    break
    full_values = [source.get(key) for source in sources for key in FULL_KEYS]
    full_values.extend(address for address in addresses if isinstance(address, str))
    if result.get("street"):
        full_values.insert(0, result["street"])
    for value in full_values:
        parsed = parse_full_address(value)
        for field, text in parsed.items():
            result.setdefault(field, text)
        if parsed and result.get("street") == clean_text(value):
            result["street"] = parsed["street"]
    if not result.get("street"):
        for value in addresses:
            if clean_text(value):
                result["street"] = clean_text(value)
                break
    return result


def missing_address_fields(payload: Dict[str, Any]) -> list[str]:
    address = extract_address_fields(payload)
    missing = [field for field in ("street", "city", "state") if not address.get(field)]
    if "street" not in missing and re.search(r"\bundisclosed\b", address["street"], re.I):
        missing.insert(0, "street")
    return missing
