import pytest

from lead_source_exclusions import auction_com_listing_excluded, is_auction_com_identity


@pytest.mark.parametrize("name", [
    "Auction.com Customer Service", "AUCTION.COM", "Auction . com", "Auction Com",
    "customer-service@auction.com",
])
def test_auction_com_identity_is_excluded(name):
    assert is_auction_com_identity(name)


@pytest.mark.parametrize("payload", [
    {"agentName": "Auction.com Customer Service"},
    {"brokerName": "Auction.com"},
    {"listingProvider": {"name": "Auction.com"}},
    {"detailUrl": "https://www.auction.com/details/4342-luster"},
    {"sourceUrl": "https://listings.auction.com/123"},
    {"source_url": "https://www.auction.com/details/123"},
    {"agentWebsite": "auction.com/agent"},
    {"agentEmail": "service@auction.com"},
])
def test_auction_provider_and_contact_routes_are_excluded(payload):
    assert auction_com_listing_excluded(payload)


@pytest.mark.parametrize("payload", [
    {"agentName": "Jane Agent", "description": "Short sale, not an Auction.com listing."},
    {"agentName": "Auction Combs"},
    {"detailUrl": "https://auction.com.example.org/123"},
    {"detailUrl": "https://example.org/auction.com/123"},
    {"agentName": "Jane Agent", "description": "Auction subject to short sale approval."},
])
def test_unrelated_agents_and_prose_are_not_excluded(payload):
    assert not auction_com_listing_excluded(payload)


@pytest.mark.parametrize("owner,pilot_row", [
    ({"agent_name": "Auction.com", "last_name": "Customer Service"}, {}),
    ({"agent_name": "Jane", "last_name": "Agent", "email": "service@auction.com"}, {}),
    ({"agent_name": "Jane", "last_name": "Agent"}, {"source_url": "https://auction.com/details/123"}),
])
def test_pilot_owner_write_rejects_auction_com_before_schema_or_write(owner, pilot_row):
    import pilot_verifier_contract as contract
    with pytest.raises(ValueError, match="^excluded_auction_com$"):
        contract.owner_write_updates([], 6851, pilot_row, owner, "lead-verifier-8-am")
