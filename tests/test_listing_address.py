import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from listing_address import extract_address_fields, missing_address_fields, parse_full_address


@pytest.mark.parametrize("payload", [
    {"street": "317 Garden Park Ave", "full_address": "317 Garden Park Ave, Calhan, CO, 80808"},
    {"city": None, "state": "", "address": "317 Garden Park Ave, Calhan, CO, 80808"},
    {"address": {"city": "", "state": "", "streetAddress": "317 Garden Park Ave"}, "city": "Calhan", "state": "CO", "zip": "80808"},
    {"property": {"address": {"streetAddress": "317 Garden Park Ave", "addressLocality": "Calhan", "addressRegion": "CO", "postalCode": "80808"}}},
    {"hdpData": {"homeInfo": {"streetAddress": "317 Garden Park Ave", "city": "Calhan", "state": "CO", "zipcode": "80808"}}},
])
def test_actual_location_survives_aliases_and_empty_fields(payload):
    assert extract_address_fields(payload) == {"street": "317 Garden Park Ave", "city": "Calhan", "state": "CO", "zip": "80808"}


@pytest.mark.parametrize("address,city,state,postal", [
    ("223 River Road, Shelton, CT, 06484", "Shelton", "CT", "06484"),
    ("104 LOGRONO Court, Saint Augustine, FL 32084", "Saint Augustine", "FL", "32084"),
    ("101 Main St, Washington DC 20002", "Washington", "DC", "20002"),
    ("101 Main St, Apt 2, Fort Meade, FL 33841, USA", "Fort Meade", "FL", "33841"),
])
def test_full_address_formats(address, city, state, postal):
    parsed = parse_full_address(address)
    assert (parsed["city"], parsed["state"], parsed["zip"]) == (city, state, postal)


def test_missing_or_invalid_location_cannot_pass_guard():
    assert missing_address_fields({"street": "317 Garden Park Ave", "state": "ZZ"}) == ["city", "state"]
    assert missing_address_fields({"street": "Undisclosed address", "city": "Calhan", "state": "CO"}) == ["street"]
    assert parse_full_address("317 Garden Park Ave, Unknown") == {}


def test_nonempty_canonical_location_is_not_replaced_by_alternate_city():
    fields = extract_address_fields({"street": "1191 SABINE LANE", "city": "Kissimmee", "state": "FL", "full_address": "1191 SABINE LANE, Poinciana, FL, 34759"})
    assert fields["city"] == "Kissimmee"
