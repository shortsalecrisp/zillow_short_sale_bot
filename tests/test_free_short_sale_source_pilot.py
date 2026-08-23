import contextlib
import datetime as dt
import io
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
import urllib.error
import urllib.parse
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import free_short_sale_source_pilot as pilot  # noqa: E402


class FreeShortSaleSourcePilotTest(unittest.TestCase):
    def pilot_row(self, **values):
        row = [""] * len(pilot.PILOT_HEADERS)
        for header, value in values.items():
            row[pilot.PILOT_HEADERS.index(header)] = value
        return row

    def scoped_payload_evidence(self, address, state, zip_code=""):
        group = pilot.listing_evidence_group(address, state, zip_code)
        return {
            "exactListingConfirmed": "true",
            "listingIdentitySource": "jsonld_listing_object",
            "listingIdentityGroup": group,
            "listingDescriptionSource": "jsonld_listing_object",
            "listingDescriptionGroup": group,
            "scopedListingStatus": "current",
            "scopedListingStatusEvidence": "Active",
            "scopedListingStatusSource": "jsonld_listing_object",
            "scopedListingStatusGroup": group,
        }

    def bound_agent_fields(self, name, address, state, phone="", email="", phone_type="direct_mobile"):
        group = pilot.listing_evidence_group(address, state)
        fields = {
            "agent_name": name,
            "agent_name_source": "jsonld_bound_listing_agent",
            "agent_evidence_group": group,
            "agent_subject_key": pilot.normalize_key(name),
            "listing_identity_group": group,
        }
        if phone:
            fields.update({
                "phone": phone,
                "phone_source": "jsonld_bound_listing_agent",
                "phone_evidence_group": group,
                "phone_contact_type": phone_type,
                "phone_owner_key": pilot.normalize_key(name),
            })
        if email:
            fields.update({
                "email": email,
                "email_source": "jsonld_bound_listing_agent",
                "email_evidence_group": group,
                "email_contact_type": "agent_specific_professional",
                "email_owner_key": pilot.normalize_key(name),
            })
        return fields

    def bound_agent_payload(self, name, address, state):
        group = pilot.listing_evidence_group(address, state)
        return {
            "agentName": name,
            "agentNameSource": "jsonld_bound_listing_agent",
            "agentEvidenceGroup": group,
            "agentSubjectKey": pilot.normalize_key(name),
        }

    def test_qualification_accepts_listing_description_short_sale_without_label(self):
        text = "Status: Active. What's special: This home is being sold as a short sale subject to lender approval."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")
        self.assertEqual(result.short_sale_evidence_type, "listing_description_or_remarks")

    def test_qualification_rejects_listing_text_without_short_sale(self):
        text = "Status: Active. Remarks: Updated home near parks and shopping."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "missing_listing_text_short_sale")

    def test_scoped_jsonld_listing_evidence_qualifies_exact_property(self):
        result = pilot.SearchResult(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product","name":"123 Main Street, Atlanta, GA 30303",
           "address":{"streetAddress":"123 Main Street","addressLocality":"Atlanta",
                      "addressRegion":"GA","postalCode":"30303"},
           "description":"Great community setting. Seller is offering this home as a short sale subject to lender approval.",
           "offers":{"availability":"https://schema.org/InStock"}}
        </script>
        <body>123 Main Street, Atlanta, GA 30303</body>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "qualified")
        self.assertEqual(candidate.fields["exact_listing_confirmed"], "true")
        self.assertEqual(candidate.fields["listing_description_source"], "jsonld_listing_object")
        self.assertEqual(candidate.fields["scoped_listing_status"], "current")

    def test_unscoped_embedded_description_does_not_qualify(self):
        result = pilot.SearchResult(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <body>123 Main Street, Atlanta, GA 30303 Status: Active.</body>
        <script>window.cards={"description":"Learn about our short sale option before choosing an agent."}</script>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "exact_listing_not_confirmed")
        self.assertNotIn("listing_description_source", candidate.fields)

    def test_search_title_address_does_not_confirm_fetched_listing(self):
        result = pilot.SearchResult(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/anything-not-blocked",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="Status: Active. Property Description: Short sale.",
        )
        markup = """
        <div class="property-description">This is a short sale subject to lender approval.</div>
        <div>Status: Active.</div>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "exact_listing_not_confirmed")

    def test_scoped_negated_short_sale_remarks_are_rejected(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street",
            text="",
            fields={
                "listing_address": "123 Main Street",
                "state": "GA",
                "exact_listing_confirmed": "true",
                "listing_description_source": "jsonld_listing_object",
                "listing_description": "This home is not a short sale; it is a traditional sale.",
                "scoped_listing_status": "current",
                "listing_identity_group": "123 main street|ga",
                "listing_description_group": "123 main street|ga",
                "scoped_listing_status_group": "123 main street|ga",
            },
        )

        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "disqualifying_short_sale_text")

    def test_bishop_navigation_is_held_at_intake(self):
        navigation = (
            "Financing Options Short Sale Options | Choosing Your Real Estate Agent"
        )
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="source_domain=bishopcountry.com; source_ref=abc123",
            title="1704 Langford Street",
            text=navigation,
            fields={
                "listing_address": "1704 Langford Street",
                "state": "TX",
                "exact_listing_confirmed": "true",
                "listing_description_source": "visible_listing_description",
                "listing_description": navigation,
                "scoped_listing_status": "current",
                "listing_identity_group": "1704 langford street|tx",
                "listing_description_group": "1704 langford street|tx",
                "scoped_listing_status_group": "1704 langford street|tx",
            },
        )

        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "short_sale_not_in_listing_evidence")

    def test_valid_bishop_remarks_survive_appended_navigation(self):
        navigation = (
            "Financing Options Short Sale Option Selecting Your Real Estate Agent"
        )
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="source_domain=bishopcountry.com; source_ref=def456",
            title="123 Main Street",
            text=navigation,
            fields={
                "listing_address": "123 Main Street",
                "state": "TX",
                "exact_listing_confirmed": "true",
                "listing_description_source": "jsonld_listing_object",
                "listing_description": (
                    "Seller is offering this property as a short sale subject to lender approval. "
                    + navigation
                ),
                "scoped_listing_status": "current",
                "listing_identity_group": "123 main street|tx",
                "listing_description_group": "123 main street|tx",
                "scoped_listing_status_group": "123 main street|tx",
            },
        )

        qualification = pilot.qualification_for_candidate(candidate)
        shadow = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertEqual(qualification.status, "qualified")
        self.assertFalse(shadow["listing_description_is_navigation"])
        self.assertFalse(shadow["would_hold"])

    def test_navigation_only_description_is_rejected_on_every_domain(self):
        navigation = "Financing Options Short Sale Option Choosing Your Real Estate Agent"
        group = "123 main street|ga"
        for url in (
            "https://www.nexusrealtync.com/homes/123/",
            "https://other.example/listing/123-main-street",
        ):
            with self.subTest(url=url):
                candidate = pilot.Candidate(
                    source="idx_broker_remarks",
                    query="query",
                    url=url,
                    title="123 Main Street",
                    text=navigation,
                    fields={
                        "listing_address": "123 Main Street",
                        "state": "GA",
                        "exact_listing_confirmed": "true",
                        "listing_identity_group": group,
                        "listing_description": navigation,
                        "listing_description_source": "visible_listing_description",
                        "listing_description_group": group,
                        "scoped_listing_status": "current",
                        "scoped_listing_status_group": group,
                    },
                )
                self.assertEqual(
                    pilot.qualification_for_candidate(candidate).failure_reason,
                    "short_sale_not_in_listing_evidence",
                )

        for navigation in (
            "Short Sale Option – Choosing Your Real Estate Agent",
            "Short-Sale Option Selecting Your Real Estate Agent",
            "Short Sale Option Choose an Agent",
        ):
            with self.subTest(navigation=navigation):
                candidate.fields["listing_description"] = navigation
                candidate.text = navigation
                self.assertEqual(
                    pilot.qualification_for_candidate(candidate).failure_reason,
                    "short_sale_not_in_listing_evidence",
                )

    def test_plain_no_short_sale_negations_are_rejected(self):
        group = "123 main street|ga"
        for description in (
            "This property is no short sale.",
            "No short sale; conventional transaction.",
            "Seller will not consider a short sale.",
            "Short sale is not applicable.",
        ):
            with self.subTest(description=description):
                candidate = pilot.Candidate(
                    source="idx_broker_remarks",
                    query="query",
                    url="https://broker.example/123-main-street",
                    title="123 Main Street",
                    text="",
                    fields={
                        "listing_address": "123 Main Street",
                        "state": "GA",
                        "exact_listing_confirmed": "true",
                        "listing_identity_group": group,
                        "listing_description": description,
                        "listing_description_source": "jsonld_listing_object",
                        "listing_description_group": group,
                        "scoped_listing_status": "current",
                        "scoped_listing_status_group": group,
                    },
                )
                self.assertEqual(
                    pilot.qualification_for_candidate(candidate).failure_reason,
                    "disqualifying_short_sale_text",
                )

    def test_jsonld_listing_objects_are_not_combined_across_properties(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          [{"@type":"Product","address":{"streetAddress":"123 Main Street",
             "addressLocality":"Atlanta","addressRegion":"GA","postalCode":"30303"}},
           {"@type":"Product","address":{"streetAddress":"999 Other Road",
             "addressLocality":"Atlanta","addressRegion":"GA","postalCode":"30303"},
            "description":"This is a short sale subject to lender approval.",
            "offers":{"availability":"InStock"}}]
        </script>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_candidate(candidate)

        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "needs_description_confirmation")
        self.assertNotIn("listing_description", candidate.fields)

    def test_jsonld_same_street_wrong_city_or_missing_zip_is_not_exact(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://broker.example/123-main-street-atlanta-ga-30303",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product","address":{"streetAddress":"123 Main Street",
             "addressLocality":"Savannah","addressRegion":"GA"},
           "description":"This is a short sale subject to lender approval.",
           "listingStatus":"Active"}
        </script>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(
            pilot.qualification_for_candidate(candidate).failure_reason,
            "exact_listing_not_confirmed",
        )

    def test_visible_description_cannot_borrow_identity_from_another_container(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <main><h1>123 Main Street</h1><div>Status: Active. Traditional sale.</div></main>
        <aside><div class="property-description">This is a short sale subject to lender approval.</div></aside>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertNotEqual(pilot.qualification_for_candidate(candidate).status, "qualified")

    def test_visible_description_cannot_climb_to_shared_app_container(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <div id="app"><main><h1>123 Main Street</h1><div>Status: Active.</div></main>
        <aside><div class="property-description">This is a short sale subject to lender approval.</div></aside>
        </div>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertNotEqual(pilot.qualification_for_candidate(candidate).status, "qualified")

    def test_visible_bound_listing_ignores_unrelated_sold_card(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <main><div class="property-description">123 Main Street. Status: Active.
        This is a short sale subject to lender approval.</div></main>
        <aside>Related home at 999 Other Road. Sold For $200,000.</aside>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(pilot.qualification_for_candidate(candidate).status, "qualified")

    @unittest.skipUnless(importlib.util.find_spec("bs4"), "production BeautifulSoup dependency required")
    def test_conflicting_duplicate_visible_listing_containers_hold_in_both_orders(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        active = """
        <article class="listing-detail"><h1>123 Main Street</h1><div>Status: Active.</div>
        <div class="property-description">Short sale subject to lender approval.</div></article>
        """
        sold = """
        <article class="listing-detail"><h1>123 Main Street</h1><div>Status: Sold.</div>
        <div class="property-description">Short sale subject to lender approval.</div></article>
        """
        for markup in (active + sold, sold + active):
            with self.subTest(order=markup[:80]):
                candidate = pilot.infer_fields(result, markup)
                self.assertEqual(
                    pilot.qualification_for_candidate(candidate).failure_reason,
                    "exact_listing_not_confirmed",
                )

    @unittest.skipUnless(importlib.util.find_spec("bs4"), "production BeautifulSoup dependency required")
    def test_identical_responsive_visible_listing_duplicates_preserve_qualification(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://broker.example/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        record = """
        <article class="listing-detail"><h1>123 Main Street</h1><div>Status: Active.</div>
        <div class="property-description">Short sale subject to lender approval.</div></article>
        """
        candidate = pilot.infer_fields(result, record + record)
        self.assertEqual(pilot.qualification_for_candidate(candidate).status, "qualified")

    def test_structured_status_is_exact_and_conflicts_hold(self):
        cases = [
            ({"status": "Inactive"}, "not_current"),
            ({"status": "Closed", "offers": {"availability": "InStock"}}, "unknown"),
            ({"listingStatus": "Active", "status": "Sold"}, "unknown"),
        ]
        for obj, expected in cases:
            with self.subTest(obj=obj):
                self.assertEqual(pilot.jsonld_listing_status(obj)[0], expected)

    def test_prewrite_site_chrome_receipt_has_zero_mutation_contract(self):
        group = "1704 langford street|tx"
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="source_domain=bishopcountry.com; source_ref=abc123",
            title="1704 Langford Street",
            text="Financing Options Short Sale Option Choosing Your Real Estate Agent",
            fields={
                "listing_address": "1704 Langford Street",
                "state": "TX",
                "exact_listing_confirmed": "true",
                "listing_identity_group": group,
                "listing_description": "Financing Options Short Sale Option Choosing Your Real Estate Agent",
                "listing_description_source": "visible_listing_description",
                "listing_description_group": group,
                "scoped_listing_status": "current",
                "scoped_listing_status_group": group,
            },
        )
        qualification = pilot.qualification_for_candidate(candidate)
        events = []
        with mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            emitted = pilot.log_site_chrome_prewrite_receipt(
                candidate,
                qualification,
                state="TX",
                source="idx_broker_remarks",
                run_date=dt.date(2026, 8, 21),
            )

        self.assertTrue(emitted)
        self.assertEqual(events[0][0], "pilot_site_chrome_prewrite_receipt")
        self.assertEqual(events[0][1]["lead_data_writes"], 0)
        self.assertEqual(events[0][1]["searches_added"], 0)
        self.assertEqual(events[0][1]["sends"], 0)

    def test_qualification_rejects_generic_short_sale_search_page_noise(self):
        text = (
            "Status: Active. Browse Michigan short sale homes and foreclosure listings. "
            "Remarks: Updated ranch near parks and shopping."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "short_sale_not_in_listing_evidence")

    def test_qualification_rejects_short_sale_only_in_listing_fields(self):
        text = (
            "Status: Active. Description: Nestled in a desirable neighborhood with a spacious layout. "
            "Disclosures and Reports Special Conditions: In Foreclosure, Short Sale. "
            "Potential Short Sale: Yes."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "short_sale_not_in_listing_evidence")

    def test_qualification_accepts_short_sale_in_public_remarks(self):
        text = (
            "Listing Status: Active. Public Remarks: Spacious home being sold as a short sale "
            "subject to lender approval. Disclosures and Reports Special Conditions: Short Sale."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")
        self.assertEqual(result.short_sale_evidence_type, "listing_description_or_remarks")

    def test_agent_remarks_short_sale_overrides_conflicting_structured_label(self):
        text = (
            "Listing Status: Active. Public Remarks: Short Sale!! Welcome to this home. "
            "Special Listing Conditions: Standard. Is Short Sale: No."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")
        self.assertEqual(result.short_sale_evidence_type, "listing_description_or_remarks")

    def test_structured_short_sale_no_still_rejects_without_agent_remarks(self):
        text = (
            "Listing Status: Active. Public Remarks: Updated home near shopping. "
            "Short Sale Status: No."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_already_approved_short_sale(self):
        text = "Status: Active. What's special: This is an approved short sale."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_short_sale_negotiation_fee(self):
        text = (
            "Status: Active. About This Home: Short Sale. "
            "Buyer to pay the short sale negotiation fee at closing."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")
        self.assertIn("negotiation fee", result.disqualifying_terms.lower())

    def test_qualification_rejects_professional_third_party_negotiation_underway(self):
        text = (
            "Status: Active. About This Home: Short Sale offered with professional "
            "third-party negotiation already underway."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_existing_negotiator_processor_or_attorney(self):
        texts = [
            "Status: Active. Remarks: Short Sale. Seller is already working with a short sale negotiator.",
            "Status: Active. Remarks: Short Sale. A short sale processor is already handling the file.",
            "Status: Active. Remarks: Short Sale. Seller is currently working with an attorney.",
        ]

        for text in texts:
            with self.subTest(text=text):
                result = pilot.qualification_for_text(text)
                self.assertEqual(result.status, "rejected")
                self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_allows_third_party_approval_without_negotiator_fee(self):
        text = (
            "Status: Active. About This Home: Short Sale - Subject to Third-Party Approval. "
            "Short sale with third-party approval required."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")

    def test_qualification_rejects_explicit_short_sale_no(self):
        text = (
            "For Sale. Property description: Status Active. "
            "Is Short Sale: No. Special Listing Conditions: None."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_explicit_short_sale_status_no(self):
        text = (
            "Listing Status: Active. Public Remarks: Spacious home. "
            "Short Sale Status: No."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_short_sale_without_active_status(self):
        text = "For Sale. Property description: Potential short sale subject to lender approval."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "missing_current_listing_status")

    def test_qualification_accepts_pending_short_sale_listing(self):
        text = (
            "Listed by Jane Agent. Status: Pending. "
            "Property description: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")

    def test_qualification_rejects_off_market_short_sale_listing(self):
        text = (
            "Listed by Jane Agent. Short Sale. Off Market. "
            "Property description: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "not_current_listing")

    def test_qualification_accepts_coming_soon_short_sale_listing(self):
        text = (
            "450 Stardust Court. Status: Coming Soon. "
            "Remarks: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")

    def test_qualification_rejects_closed_short_sale_listing(self):
        text = (
            "679 Bridger Drive. Share Closed. "
            "Remarks: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "not_current_listing")

    def test_qualification_does_not_treat_assessment_pending_as_listing_pending(self):
        text = (
            "Listing Status: Active. Assessment Pending: No. Taxes w/ Assessments: $3,822. "
            "Remarks: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")

    def test_qualification_accepts_active_under_contract_short_sale_listing(self):
        text = (
            "Status Active Under Contract. "
            "Remarks: Potential short sale subject to lender approval."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")

    def test_qualification_rejects_approved_price_short_sale(self):
        text = (
            "Status: Pending. "
            "Property description: SHORT SALE APPROVED PRICE. Buyer to verify all information."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_short_sale_approved_at_list_price(self):
        text = (
            "Status: Pending. About this home: Short Sale. "
            "Short Sale has been approved at list price. Investor opportunity."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")
        self.assertIn("approved at list price", result.disqualifying_terms.lower())

    def test_qualification_rejects_potential_short_sale_no(self):
        text = (
            "Status Active Under Contract. Contract Information - Potential Short Sale No. "
            "Financial Status - Potential Short Sale No."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_potential_short_sale_question_no(self):
        text = (
            "Listing Status: Active. Tax Amount: 2148. In Foreclosure?: No "
            "Potential Short Sale?: No Lender Owned?: No Directions & Remarks "
            "Public Remarks: Nice home with water views."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_special_listing_conditions_short_sale_no(self):
        text = (
            "Status: Closed. "
            "Special Listing Conditions Short Sale No, Standard."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_duplicate_status_flags_existing_agent_phone_even_new_address(self):
        main_rows = [
            ["agent_name", "last_name", "phone", "email", "listing_address", "city", "state"],
            ["Jane", "Agent", "404-555-1212", "jane@example.com", "1 Old St", "Atlanta", "GA"],
        ]
        existing = pilot.build_existing_index(main_rows)
        candidate = pilot.Candidate(
            source="realtor.com",
            query="q",
            url="https://example.com/new",
            title="2 New St",
            text="Status: Active. Remarks: Potential short sale subject to lender approval.",
            fields={
                "listing_address": "2 New St",
                "city": "Atlanta",
                "state": "GA",
                "phone": "(404) 555-1212",
                "agent_name": "Jane Smith",
            },
        )

        status, key, matched_row = pilot.duplicate_status(candidate, existing)

        self.assertEqual(status, "duplicate_agent_phone")
        self.assertEqual(key, "4045551212")
        self.assertEqual(matched_row, "2")
        self.assertTrue(pilot.duplicate_status_blocks_pilot_row(status))

    def test_duplicate_agent_phone_row_shape_is_not_a_write_policy(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="2 New St",
            text="Status: Active. Remarks: Potential short sale subject to lender approval.",
            fields={
                "listing_address": "2 New St",
                "city": "Atlanta",
                "state": "GA",
                "phone": "404-555-1212",
                "phone_source": "listing_agent_label",
                "email": "jane@example.com",
                "email_source": "listing_agent_label",
                "agent_name": "Jane Smith",
                "agent_name_source": "listing_agent_label",
            },
        )
        candidate.fields.update(
            self.bound_agent_fields(
                "Jane Smith", "2 New St", "GA", "404-555-1212", "jane@example.com"
            )
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "4045551212", "2", "")

        self.assertEqual(row[:7], ["Jane", "Smith", "404-555-1212", "jane@example.com", "2 New St", "Atlanta", "GA"])
        self.assertEqual(row[14], "shadow_ready")
        self.assertEqual(row[16], "yes")
        self.assertEqual(row[22], "4045551212")
        self.assertEqual(row[23], "2")
        self.assertTrue(pilot.duplicate_status_blocks_pilot_row("duplicate_agent_phone"))
        self.assertFalse(pilot.duplicate_status_blocks_pilot_row("possible_existing_agent"))

    def test_pilot_row_starts_like_main_sheet(self):
        candidate = pilot.Candidate(
            source="redfin.com",
            query="q",
            url="https://example.com/listing",
            title="10 Main St",
            text="Status: Active. What's special: Short sale subject to lender approval.",
            fields={
                "agent_name": "Maria Cahuenas",
                "agent_name_source": "listing_agent_label",
                "phone": "714-300-5277",
                "phone_source": "listing_agent_label",
                "email": "maria@example.com",
                "email_source": "listing_agent_label",
                "listing_address": "10 Main St",
                "city": "Oak Hills",
                "state": "CA",
            },
        )
        candidate.fields.update(
            self.bound_agent_fields(
                "Maria Cahuenas", "10 Main St", "CA", "714-300-5277", "maria@example.com"
            )
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "key", "", "")

        self.assertEqual(
            row[:7],
            [
                "Maria",
                "Cahuenas",
                "714-300-5277",
                "maria@example.com",
                "10 Main St",
                "Oak Hills",
                "CA",
            ],
        )

    def test_candidate_row_uses_safe_source_reference_and_sanitized_payload(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query='site:example.com "short sale"',
            url="https://www.example.com/listing/123?tracking=abc",
            title="10 Main St https://www.example.com/title",
            text=(
                "Status: Active. Public Remarks: Short sale subject to lender approval. "
                "Details at https://www.example.com/details"
            ),
            fields={
                "agent_name": "Jane Smith",
                "agent_name_source": "listing_agent_label",
                "listing_address": "10 Main St",
                "city": "Oak Hills",
                "state": "CA",
                "listing_description": (
                    "Public Remarks: Short sale subject to lender approval. "
                    "Photos: https://images.example.com/1.jpg"
                ),
            },
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "key", "", "")

        self.assertIn("source_domain=example.com", row[11])
        self.assertIn("source_ref=", row[11])
        self.assertNotIn("http", row[11].lower())
        payload = json.loads(row[27])
        self.assertNotIn("url", payload)
        self.assertNotIn("detailUrl", payload)
        self.assertNotIn("propertyUrl", payload)
        self.assertIn("sourceReference", payload)
        self.assertNotIn("http", row[27].lower())
        self.assertNotIn("http", row[28].lower())
        self.assertNotIn("http", row[29].lower())

    def test_parse_pilot_payload_reconstructs_cleaned_archived_payload(self):
        pilot_row = self.pilot_row(
            first_name="Jane",
            last_name="Smith",
            phone="404-555-1212",
            email="jane@example.com",
            listing_address="123 Main Street",
            city="Atlanta",
            state="GA",
            synthetic_zpid="free-cleaned",
            source="idx_broker_pages",
            source_url="source_domain=example.com; source_ref=abc123",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            zip="30301",
            broker_name="Example Realty",
            qualification_evidence="Public remarks: potential short sale subject to lender approval.",
            pending_queue_source="free-source-pilot:idx_broker_pages",
            pending_queue_address="123 Main Street",
            pending_queue_listing_json="raw listing payload archived for Drive safety",
            description_excerpt="Status: Active. Public remarks: potential short sale subject to lender approval.",
            raw_title="123 Main Street",
        )
        row_data = pilot.pilot_row_map(pilot_row)

        payload, failure = pilot.parse_pilot_payload(row_data)
        normalized = pilot.normalize_payload_for_sheet1(row_data, payload)

        self.assertEqual(failure, "")
        self.assertEqual(payload["zpid"], "free-cleaned")
        self.assertEqual(payload["source"], "free-source-pilot:idx_broker_pages")
        self.assertEqual(payload["agentName"], "Jane Smith")
        self.assertIn("short sale", normalized["listing_description"].lower())
        self.assertEqual(normalized["requiresVerifierReview"], "true")
        self.assertNotIn("url", normalized)
        self.assertNotIn("detailUrl", normalized)
        self.assertNotIn("propertyUrl", normalized)

    def test_pilot_headers_start_with_first_and_last_name(self):
        self.assertEqual(pilot.PILOT_HEADERS[:2], ["first_name", "last_name"])

    def test_default_states_include_michigan_for_pilot(self):
        self.assertIn("MI", pilot.DEFAULT_STATES)
        self.assertEqual(pilot.STATE_QUERY_TERMS["MI"], "Michigan")

    def test_default_states_cover_all_50_states(self):
        self.assertEqual(len(pilot.DEFAULT_STATES), 50)
        self.assertEqual(len(set(pilot.DEFAULT_STATES)), 50)
        self.assertEqual(set(pilot.DEFAULT_STATES), set(pilot.STATE_QUERY_TERMS))

    def test_default_source_plan_runs_two_idx_broker_buckets(self):
        queries = pilot.configured_source_queries(pilot.dt.date(2026, 7, 6))

        self.assertEqual([query.source for query in queries], ["idx_broker_pages", "idx_broker_remarks"])
        self.assertEqual([query.date_restrict for query in queries], ["w1", "w1"])
        self.assertEqual(len(queries) * len(pilot.DEFAULT_STATES), 100)

    def test_default_source_plan_is_stable_across_days(self):
        day_1 = pilot.configured_source_queries(pilot.dt.date(2026, 7, 6))
        day_2 = pilot.configured_source_queries(pilot.dt.date(2026, 7, 7))

        self.assertEqual([query.source for query in day_1], ["idx_broker_pages", "idx_broker_remarks"])
        self.assertEqual([query.source for query in day_2], ["idx_broker_pages", "idx_broker_remarks"])

    def test_legacy_source_plan_still_rotates_portal_bucket(self):
        old_plan = pilot.SOURCE_PLAN
        try:
            pilot.SOURCE_PLAN = "idx_daily_rotating_weekly"
            day_1 = pilot.configured_source_queries(pilot.dt.date(2026, 7, 6))
            day_2 = pilot.configured_source_queries(pilot.dt.date(2026, 7, 7))
        finally:
            pilot.SOURCE_PLAN = old_plan

        self.assertEqual([query.source for query in day_1], ["idx_broker_pages", "homes.com"])
        self.assertEqual([query.source for query in day_2], ["idx_broker_pages", "realtor.com"])

    def test_configured_source_buckets_ignore_unknowns_and_duplicates(self):
        old_plan = pilot.SOURCE_PLAN
        old_buckets = os.environ.get("FREE_SOURCE_PILOT_SOURCE_BUCKETS")
        os.environ["FREE_SOURCE_PILOT_SOURCE_BUCKETS"] = "realtor.com,unknown,realtor.com,homes.com"
        try:
            pilot.SOURCE_PLAN = "static"
            sources = [query.source for query in pilot.configured_source_queries()]
        finally:
            pilot.SOURCE_PLAN = old_plan
            if old_buckets is None:
                os.environ.pop("FREE_SOURCE_PILOT_SOURCE_BUCKETS", None)
            else:
                os.environ["FREE_SOURCE_PILOT_SOURCE_BUCKETS"] = old_buckets

        self.assertEqual(sources, ["realtor.com", "homes.com"])

    def test_source_result_allowed_rejects_redfin_collection_and_blog_pages(self):
        collection = pilot.SearchResult(
            "redfin.com",
            "query",
            "https://www.redfin.com/state/Alabama/fixer-upper/page-4",
            "Alabama Fixer Uppers",
            "",
        )
        blog = pilot.SearchResult(
            "redfin.com",
            "query",
            "https://www.redfin.com/blog/short-sale-vs-foreclosure/",
            "Buying A Short Sale vs Foreclosure",
            "",
        )
        detail = pilot.SearchResult(
            "redfin.com",
            "query",
            "https://www.redfin.com/AL/Mobile/123-Main-St-36602/home/123456",
            "123 Main St",
            "",
        )

        self.assertEqual(pilot.source_result_allowed(collection), (False, "not_redfin_detail"))
        self.assertEqual(pilot.source_result_allowed(blog), (False, "not_redfin_detail"))
        self.assertEqual(pilot.source_result_allowed(detail), (True, ""))

    def test_source_result_allowed_rejects_idx_search_pages_but_allows_listing_detail_branding(self):
        search = pilot.SearchResult(
            "idx_broker_pages",
            "query",
            "https://www.allkchomesforsale.com/search",
            "1872+ Listings - All KC Homes For Sale",
            "",
        )
        collection = pilot.SearchResult(
            "idx_broker_pages",
            "query",
            "https://www.sunsetrealtyservices.com/golden-missouri-homes-for-sale",
            "Golden Missouri Homes For Sale",
            "",
        )
        detail = pilot.SearchResult(
            "idx_broker_pages",
            "query",
            "https://www.marylanddreamhomerealty.com/newlisting/3201837/1412-W-LOMBARD-ST-W-Baltimore-MD-21223",
            "1412 W LOMBARD ST W, Baltimore MD 21223 - Maryland Real Estate",
            "",
        )

        self.assertEqual(pilot.source_result_allowed(search), (False, "not_idx_listing_detail"))
        self.assertEqual(pilot.source_result_allowed(collection), (False, "not_idx_listing_detail"))
        self.assertEqual(pilot.source_result_allowed(detail), (True, ""))

    def test_listing_address_and_state_guards_reject_search_page_noise(self):
        self.assertFalse(pilot.looks_like_listing_address("Buying A Short Sale vs Foreclosure"))
        self.assertFalse(pilot.looks_like_listing_address("Alabama fixer-upper homes page 4"))
        self.assertFalse(pilot.looks_like_listing_address("Viewing Listing MLS# 7033072"))
        self.assertFalse(pilot.looks_like_listing_address("3301 64th Street in Fort Smith, AR for $189,000"))
        self.assertFalse(pilot.looks_like_listing_address("1872+ Listings"))
        self.assertTrue(pilot.looks_like_listing_address("123 Main St"))

        candidate = pilot.Candidate(
            source="redfin.com",
            query="query",
            url="https://www.redfin.com/MD/Halethorpe/2828-Alabama-Ave-21227/home/9378085",
            title="2828 Alabama Ave",
            text="Status: Active. Remarks: Short sale subject to lender approval.",
            fields={"listing_address": "2828 Alabama Ave", "state": "MD"},
        )

        self.assertFalse(pilot.candidate_matches_requested_state(candidate, "AL"))
        self.assertTrue(pilot.candidate_matches_requested_state(candidate, "MD"))

    def test_infer_fields_uses_jsonld_product_name_when_title_is_not_address(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing/1794-n-parkside-lane",
            title="Viewing Listing MLS# 7033072 - Broker",
            snippet="Special Listing Conditions: Short Sale. Listing Agent: Maria Cahuenas.",
        )
        markup = """
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"Product",
           "name":"1794 N Parkside Lane Casa Grande, AZ 85122",
           "description":"Short sale subject to lender approval.",
           "image":"https://cdn.example.com/az/20260512213414244719000000-o.jpg"}
        </script>
        <body>Contact Phone 928-282-4166</body>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["listing_address"], "1794 N Parkside Lane")
        self.assertEqual(candidate.fields["city"], "Casa Grande")
        self.assertEqual(candidate.fields["state"], "AZ")
        self.assertEqual(candidate.fields["zip"], "85122")
        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertEqual(candidate.fields["phone"], "")

    def test_infer_fields_does_not_trust_standalone_jsonld_real_estate_agent(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="Special Listing Conditions: Short Sale.",
        )
        markup = """
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"RealEstateAgent",
           "name":"Jane Smith"}
        </script>
        <body>Status: Active. Special Listing Conditions: Short Sale.</body>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")

    def test_infer_fields_ignores_jsonld_real_estate_agent_address_for_listing(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="8441 Sierra Vista, Phelan, CA.| MLS# IV26144448",
            snippet="Special Listing Conditions: Short Sale.",
        )
        markup = """
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"RealEstateAgent",
           "name":"Glenn Zimmerman",
           "address":{"@type":"PostalAddress","streetAddress":"9748 Rose Drive",
                      "addressLocality":"Oak Hills","addressRegion":"CA","postalCode":"92344"}}
        </script>
        <body>Status: Active. Special Listing Conditions: Short Sale.</body>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["listing_address"], "8441 Sierra Vista")
        self.assertEqual(candidate.fields["city"], "Phelan")
        self.assertEqual(candidate.fields["state"], "CA")
        self.assertEqual(candidate.fields["agent_name"], "")

    def test_infer_fields_uses_embedded_realtor_flags_and_description(self):
        result = pilot.SearchResult(
            source="realtor.com",
            query="query",
            url="https://www.realtor.com/realestateandhomes-detail/17-Pine-St_Bellingham_MA_02019_M38220-61048",
            title="17 Pine St, Bellingham, MA 02019",
            snippet="",
        )
        markup = r"""
        <script>
        {"flags":{"is_pending":true,"is_short_sale":true},
         "description":"Cute ranch. SHORT SALE APPROVED PRICE.",
         "opcity_lead_attributes":{"phones":[{"number":"(508)594-3513"}]}}
        </script>
        <body>
          <img src="https://ap.rdcpix.com/photo-m1567719840s.jpg" />
          Listed by Amber Cadorette
        </body>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_text(candidate.text)

        self.assertEqual(candidate.fields["phone"], "")
        self.assertEqual(qualification.status, "rejected")
        self.assertEqual(qualification.failure_reason, "disqualifying_short_sale_text")

    def test_clean_listing_address_strips_null_and_city_state_zip(self):
        self.assertEqual(
            pilot.clean_listing_address("679 null Bridger Drive null", "Colorado Springs", "CO", "80909"),
            "679 Bridger Drive",
        )
        self.assertEqual(
            pilot.clean_listing_address("450 Stardust Court, Dacono, CO, 80514", "Dacono", "CO", "80514"),
            "450 Stardust Court",
        )
        self.assertEqual(
            pilot.clean_listing_address("1256 Van Allen Mews NW, Atlanta, GA 30318", "Atlanta", "GA", "30318"),
            "1256 Van Allen Mews NW",
        )
        self.assertEqual(
            pilot.clean_listing_address(
                "1475 Woodland Loop NW, Baudette, MN 56623 (MLS# 7103500)",
                "Baudette",
                "MN",
                "56623",
            ),
            "1475 Woodland Loop NW",
        )

    def test_listing_address_requires_street_number_not_city_zip_only(self):
        self.assertTrue(pilot.looks_like_listing_address("1475 Woodland Loop NW"))
        self.assertFalse(pilot.looks_like_listing_address("Baudette, MN 56623 (MLS# 7103449)"))
        self.assertFalse(pilot.looks_like_listing_address("91"))
        self.assertFalse(pilot.looks_like_listing_address("3933"))
        self.assertFalse(pilot.looks_like_listing_address("1273"))
        self.assertFalse(pilot.looks_like_listing_address("840 N Twin Lakes DR 432, St George"))
        self.assertTrue(pilot.looks_like_listing_address("15790 Easthaven Ct, Unit 510"))

    def test_address_key_canonicalizes_street_suffix_and_trailing_direction(self):
        self.assertEqual(
            pilot.address_key("1412 W LOMBARD ST W", "Baltimore", "MD"),
            pilot.address_key("1412 W Lombard Street", "Baltimore", "MD"),
        )

    def test_infer_fields_cleans_idx_title_with_in_city_and_price(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="3301 64th Street in Fort Smith, AR for $189,000",
            snippet="",
        )
        markup = """
        <body>
          <div>Status: Active For Sale</div>
          <div>Listed by: Marsha Rogers Realty, Inc.</div>
          <div>Remarks: Potential Short Sale</div>
          <div>agent@example.com</div>
          <span>(479) 484-5588</span>
        </body>
        """

        candidate = pilot.infer_fields(result, markup)
        qualification = pilot.qualification_for_text(candidate.text)

        self.assertEqual(candidate.fields["listing_address"], "3301 64th Street")
        self.assertEqual(candidate.fields["city"], "Fort Smith")
        self.assertEqual(candidate.fields["state"], "AR")
        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertEqual(pilot.required_review_field_failure(candidate, qualification), "")

    def test_required_review_fields_require_address_and_short_sale_evidence_not_agent_contact(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Remarks: Potential short sale subject to lender approval.",
            fields={"listing_address": "123 Main Street", "city": "Atlanta", "state": "GA"},
        )
        qualification = pilot.qualification_for_text(candidate.text)

        self.assertEqual(pilot.required_review_field_failure(candidate, qualification), "")

        candidate.fields["agent_name"] = "Jane Smith"
        self.assertEqual(pilot.required_review_field_failure(candidate, qualification), "")

        candidate.fields["phone"] = "404-555-1212"
        self.assertEqual(pilot.required_review_field_failure(candidate, qualification), "")

        candidate.fields["email"] = "jane@example.com"
        self.assertEqual(pilot.required_review_field_failure(candidate, qualification), "")

    def test_qualified_short_sale_row_can_be_added_without_agent_contact(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Remarks: Potential short sale subject to lender approval.",
            fields={"listing_address": "123 Main Street", "city": "Atlanta", "state": "GA"},
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "", "", "")

        self.assertEqual(row[:7], ["", "", "", "", "123 Main Street", "Atlanta", "GA"])
        self.assertEqual(row[12], "qualified")
        self.assertEqual(row[16], "yes")
        self.assertIn("left blank", row[15])

    def test_phone_and_email_without_agent_name_still_needs_review(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Remarks: Potential short sale subject to lender approval.",
            fields={
                "phone": "404-555-1212",
                "email": "support@example.com",
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "", "", "")

        self.assertEqual(row[16], "yes")
        self.assertIn("left blank", row[15])

    def test_promotion_accepts_shadow_ready_row_without_agent_identity(self):
        payload = {
            "zpid": "free-abc",
            "street": "123 Main Street",
            "city": "Atlanta",
            "state": "GA",
            "source": "free-source-pilot:idx_broker_pages",
            "search_source": "free-source-pilot:idx_broker_pages",
            "listing_description": "Potential short sale subject to lender approval.",
        }
        payload.update(self.scoped_payload_evidence("123 Main Street", "GA"))
        pilot_row = self.pilot_row(
            listing_address="123 Main Street",
            city="Atlanta",
            state="GA",
            synthetic_zpid="free-abc",
            source="idx_broker_pages",
            source_url="https://example.com/listing",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            short_sale_evidence_type="listing_description_or_remarks",
            qualification_evidence="Potential short sale subject to lender approval.",
            pending_queue_source="free-source-pilot:idx_broker_pages",
            pending_queue_listing_json=json.dumps(payload),
        )
        captured_updates = []

        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]],
                [pilot.PILOT_HEADERS, pilot_row],
            ],
        ), mock.patch.object(
            pilot,
            "batch_update_values",
            side_effect=lambda _token, _spreadsheet_id, updates: captured_updates.extend(updates),
        ), mock.patch.object(
            pilot,
            "import_bot_processor",
            return_value=types.SimpleNamespace(
                process_rows=lambda rows, **kwargs: {"free-abc": "completed_short_sale"}
            ),
        ):
            stats = pilot.promote_ready_pilot_rows(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                cap=5,
                dry_run=False,
            )

        self.assertEqual(stats["promoted"], 1)
        self.assertEqual(stats["skipped"], 0)
        status_updates = {update["range"]: update["values"][0][0] for update in captured_updates}
        self.assertEqual(status_updates["Lead Source Pilot!O2"], "promoted")
        self.assertEqual(status_updates["Lead Source Pilot!Q2"], "promoted")

    def test_promotion_holds_legacy_payload_without_bound_provenance(self):
        payload = {
            "zpid": "free-legacy",
            "street": "123 Main Street",
            "city": "Atlanta",
            "state": "GA",
            "source": "free-source-pilot:idx_broker_pages",
            "search_source": "free-source-pilot:idx_broker_pages",
            "listing_description": "Potential short sale subject to lender approval.",
        }
        pilot_row = self.pilot_row(
            listing_address="123 Main Street",
            city="Atlanta",
            state="GA",
            synthetic_zpid="free-legacy",
            source="idx_broker_pages",
            source_url="source_domain=example.com; source_ref=legacy",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            pending_queue_source="free-source-pilot:idx_broker_pages",
            pending_queue_listing_json=json.dumps(payload),
        )
        existing = pilot.build_existing_index(
            [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]]
        )

        status, _, _ = pilot.pilot_row_preflight_failure(
            pilot.pilot_row_map(pilot_row), payload, existing
        )

        self.assertEqual(status, "needs_exact_listing_evidence")

    def test_promotion_skips_feed_or_brokerage_agent_identity(self):
        bad_names = [
            ("Corey Smallman Provided Stellar", "123 Main Street", "agent_name_contains_feed_or_brokerage_term"),
            ("Keller Williams Keystone", "456 Oak Street", "agent_name_contains_feed_or_brokerage_term"),
            ("Isaacs Ave William Hagan", "612 Isaacs Avenue", "agent_name_contains_address_token"),
            ("Q Chau eXp", "789 Pine Street", "agent_name_contains_feed_or_brokerage_term"),
            ("St Julia Hupp Red", "340 S 3rd Street #2", "agent_name_contains_feed_or_brokerage_term"),
        ]
        existing = pilot.build_existing_index(
            [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]]
        )
        for agent_name, address, reason in bad_names:
            with self.subTest(agent_name=agent_name):
                payload = {
                    "zpid": f"free-{pilot.normalize_key(agent_name).replace(' ', '-')}",
                    "street": address,
                    "city": "Denver",
                    "state": "CO",
                    "source": "free-source-pilot:idx_broker_pages",
                    "search_source": "free-source-pilot:idx_broker_pages",
                    "agentName": agent_name,
                    "listing_description": "Public remarks: potential short sale subject to lender approval.",
                }
                payload.update(self.scoped_payload_evidence(address, "CO"))
                pilot_row = self.pilot_row(
                    first_name=agent_name.split()[0],
                    last_name=" ".join(agent_name.split()[1:]),
                    listing_address=address,
                    city="Denver",
                    state="CO",
                    synthetic_zpid=payload["zpid"],
                    source="idx_broker_pages",
                    source_url="https://example.com/listing",
                    status="qualified",
                    promotion_status="shadow_ready",
                    import_ready="yes",
                    short_sale_evidence_type="listing_description_or_remarks",
                    qualification_evidence="Public remarks: potential short sale subject to lender approval.",
                    pending_queue_source="free-source-pilot:idx_broker_pages",
                    pending_queue_listing_json=json.dumps(payload),
                )

                status, note, matched = pilot.pilot_row_preflight_failure(
                    pilot.pilot_row_map(pilot_row),
                    payload,
                    existing,
                )

                self.assertEqual(status, "")
                self.assertEqual(note, pilot.street_state_key(address, "CO"))
                self.assertEqual(matched, "")

    def test_promotion_skips_undisclosed_address(self):
        payload = {
            "zpid": "free-undisclosed",
            "street": "Undisclosed Address",
            "city": "Miami",
            "state": "FL",
            "source": "free-source-pilot:idx_broker_pages",
            "search_source": "free-source-pilot:idx_broker_pages",
            "agentName": "Jane Smith",
            "listing_description": "Public remarks: potential short sale subject to lender approval.",
        }
        pilot_row = self.pilot_row(
            first_name="Jane",
            last_name="Smith",
            listing_address="Undisclosed Address",
            city="Miami",
            state="FL",
            synthetic_zpid="free-undisclosed",
            source="idx_broker_pages",
            source_url="https://example.com/listing",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            short_sale_evidence_type="listing_description_or_remarks",
            qualification_evidence="Public remarks: potential short sale subject to lender approval.",
            pending_queue_source="free-source-pilot:idx_broker_pages",
            pending_queue_listing_json=json.dumps(payload),
        )
        existing = pilot.build_existing_index(
            [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]]
        )

        status, note, matched = pilot.pilot_row_preflight_failure(
            pilot.pilot_row_map(pilot_row),
            payload,
            existing,
        )

        self.assertEqual(status, "needs_address")
        self.assertIn("Street, city, and state", note)
        self.assertEqual(matched, "")

    def test_promotion_routes_confirmed_agent_payload_through_sheet1_processor(self):
        payload = {
            "zpid": "free-def",
            "street": "456 Oak Street",
            "city": "Denver",
            "state": "CO",
            "source": "free-source-pilot:idx_broker_remarks",
            "search_source": "free-source-pilot:idx_broker_remarks",
            "agentName": "Jane Smith",
            "listing_description": "Public remarks: potential short sale subject to lender approval.",
            "requiresVerifierReview": "true",
        }
        payload.update(self.scoped_payload_evidence("456 Oak Street", "CO"))
        payload.update(self.bound_agent_payload("Jane Smith", "456 Oak Street", "CO"))
        pilot_row = self.pilot_row(
            first_name="Jane",
            last_name="Smith",
            listing_address="456 Oak Street",
            city="Denver",
            state="CO",
            synthetic_zpid="free-def",
            source="idx_broker_remarks",
            source_url="https://example.com/listing",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            short_sale_evidence_type="listing_description_or_remarks",
            qualification_evidence="Public remarks: potential short sale subject to lender approval.",
            pending_queue_source="free-source-pilot:idx_broker_remarks",
            pending_queue_listing_json=json.dumps(payload),
        )
        captured = {}
        captured_updates = []

        def fake_process_rows(rows, **kwargs):
            captured["rows"] = rows
            captured["kwargs"] = kwargs
            return {"free-def": "completed_short_sale"}

        fake_processor = types.SimpleNamespace(process_rows=fake_process_rows)

        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]],
                [pilot.PILOT_HEADERS, pilot_row],
            ],
        ), mock.patch.object(
            pilot,
            "batch_update_values",
            side_effect=lambda _token, _spreadsheet_id, updates: captured_updates.extend(updates),
        ), mock.patch.object(
            pilot,
            "import_bot_processor",
            return_value=fake_processor,
        ):
            stats = pilot.promote_ready_pilot_rows(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                cap=5,
                dry_run=False,
            )

        self.assertEqual(stats["promoted"], 1)
        self.assertEqual(captured["kwargs"], {"skip_dedupe": True, "return_outcomes": True})
        routed_payload = captured["rows"][0]
        self.assertEqual(routed_payload["agentName"], "Jane Smith")
        self.assertEqual(routed_payload["search_source"], "free-source-pilot:idx_broker_remarks")
        self.assertEqual(routed_payload["requiresVerifierReview"], "true")
        self.assertNotIn("phone", routed_payload)
        status_updates = {update["range"]: update["values"][0][0] for update in captured_updates}
        self.assertEqual(status_updates["Lead Source Pilot!O2"], "promoted")
        self.assertEqual(status_updates["Lead Source Pilot!Q2"], "promoted")

    def test_promotion_dry_run_does_not_write_or_import_processor(self):
        payload = {
            "zpid": "free-dry",
            "street": "789 Pine Street",
            "city": "Phoenix",
            "state": "AZ",
            "source": "free-source-pilot:idx_broker_pages",
            "search_source": "free-source-pilot:idx_broker_pages",
            "agentName": "Dana Smith",
            "listing_description": "Remarks: potential short sale subject to lender approval.",
        }
        payload.update(self.scoped_payload_evidence("789 Pine Street", "AZ"))
        pilot_row = self.pilot_row(
            first_name="Dana",
            last_name="Smith",
            listing_address="789 Pine Street",
            city="Phoenix",
            state="AZ",
            synthetic_zpid="free-dry",
            source="idx_broker_pages",
            source_url="https://example.com/listing",
            status="qualified",
            promotion_status="shadow_ready",
            import_ready="yes",
            qualification_evidence="Remarks: potential short sale subject to lender approval.",
            pending_queue_source="free-source-pilot:idx_broker_pages",
            pending_queue_listing_json=json.dumps(payload),
        )

        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["first_name", "last_name", "phone", "email", "listing_address", "city", "state"]],
                [pilot.PILOT_HEADERS, pilot_row],
            ],
        ), mock.patch.object(
            pilot,
            "batch_update_values",
            side_effect=AssertionError("dry run should not write status updates"),
        ), mock.patch.object(
            pilot,
            "import_bot_processor",
            side_effect=AssertionError("dry run should not import the Sheet1 processor"),
        ):
            stats = pilot.promote_ready_pilot_rows(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                cap=5,
                dry_run=True,
            )

        self.assertEqual(stats["promoted"], 1)
        self.assertEqual(stats["eligible"], 1)

    def test_agent_name_cleaner_rejects_brokerage_names(self):
        self.assertEqual(pilot.clean_agent_name("West USA Realty"), "")
        self.assertEqual(pilot.clean_agent_name("Brokered by Ben Zeller"), "Ben Zeller")
        self.assertEqual(pilot.clean_agent_name("Ben Zeller Brokered by"), "Ben Zeller")
        self.assertEqual(pilot.clean_agent_name("Shown By Listed By"), "")
        self.assertEqual(pilot.clean_agent_name("Listing Agent: Jane Smith Phone 404-555-1212"), "Jane Smith")
        self.assertEqual(pilot.clean_agent_name("Southern Missouri Regional"), "")

    def test_agent_shadow_parser_matches_approved_row_fixtures_without_mutation(self):
        fixtures = [
            (
                "Row 31. Courtesy of RE/MAX of Cherry Creek. The David Hakimi Team.",
                "",
                "",
            ),
            (
                "Row 59. Courtesy of House2Home, LLC. The David Hakimi Team.",
                "",
                "",
            ),
            (
                "Row 60. Listing Courtesy of PROFESSIONAL REAL ESTATE TEAM 407-483-4964.",
                "",
                "",
            ),
            (
                "Row 61. Listing courtesy of ASANO REAL ESTATE LLC. "
                "Listing agent: Leslie Campasano · 352-552-7232.",
                "Leslie Campasano",
                "listing_agent",
            ),
            (
                "Row 62. Listing courtesy of WEMERT GROUP REALTY LLC. 407-214-3967.",
                "",
                "",
            ),
            (
                "Row 63. Listing courtesy of Alex Johns of Century 21 Coastal Advantage: 910-353-7755.",
                "Alex Johns",
                "listing_courtesy",
            ),
            (
                "Coldwell Banker listing. Listing Agent: Diana Perez | Listing Office: Coldwell Banker.",
                "Diana Perez",
                "listing_agent",
            ),
        ]
        for text, expected_name, expected_label in fixtures:
            with self.subTest(text=text):
                candidate = pilot.Candidate(
                    source="idx_broker_remarks",
                    query="query",
                    url="https://example.com/listing",
                    title="123 Main Street",
                    text=text,
                    fields={"listing_address": "123 Main Street", "city": "Denver", "state": "CO"},
                )

                shadow = pilot.shadow_listing_agent_candidate(candidate)

                self.assertEqual(shadow.get("agent_name", ""), expected_name)
                self.assertEqual(shadow.get("label", ""), expected_label)
                self.assertNotIn("agent_name", candidate.fields)

    def test_agent_shadow_parser_rejects_brokerage_only_courtesy(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Listing courtesy of Golden Key Realty LLC.",
            fields={"listing_address": "123 Main Street", "city": "Denver", "state": "CO"},
        )

        self.assertEqual(pilot.shadow_listing_agent_candidate(candidate), {})

    def test_agent_shadow_requires_two_independent_domains_and_never_mutates(self):
        pilot.reset_agent_shadow_consensus_state()
        events = []
        first = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://broker-one.example/listing/123",
            title="123 Main Street",
            text="Listing Agent: Jane Smith.",
            fields={
                "listing_address": "123 Main Street",
                "city": "Denver",
                "state": "CO",
            },
        )
        second = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://broker-two.example/property/123",
            title="123 Main Street",
            text="Listed By: Jane Smith.",
            fields={
                "listing_address": "123 Main Street",
                "city": "Denver",
                "state": "CO",
            },
        )

        with mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            self.assertTrue(pilot.log_agent_shadow(first))
            self.assertFalse(
                any(event == "pilot_agent_shadow_two_source_consensus" for event, _ in events)
            )
            self.assertTrue(pilot.log_agent_shadow(second))

        consensus = [
            details
            for event, details in events
            if event == "pilot_agent_shadow_two_source_consensus"
        ]
        self.assertEqual(len(consensus), 1)
        self.assertEqual(consensus[0]["shadow_agent"], "Jane Smith")
        self.assertEqual(consensus[0]["source_count"], 2)
        self.assertEqual(consensus[0]["writes"], 0)
        self.assertNotIn("agent_name", first.fields)
        self.assertNotIn("agent_name", second.fields)

    def test_direct_monitor_sitemap_selection_is_bounded_and_rotates(self):
        feed = "https://example.com/listings.xml"
        entries = "".join(
            f"<url><loc>https://movewithmomentum.com/listings/idx/{number}-main-st</loc>"
            f"<lastmod>2026-07-{number:02d}</lastmod></url>"
            for number in range(1, 11)
        )
        markup = f'<?xml version="1.0"?><urlset>{entries}</urlset>'
        with mock.patch.object(pilot, "fetch_public_feed", return_value=markup):
            day_one = pilot.collect_direct_monitor_urls(
                "momentum",
                (feed,),
                run_date=dt.date(2026, 8, 7),
                limit=3,
            )
            day_two = pilot.collect_direct_monitor_urls(
                "momentum",
                (feed,),
                run_date=dt.date(2026, 8, 8),
                limit=3,
            )

        self.assertEqual(len(day_one), 3)
        self.assertEqual(len(day_two), 3)
        self.assertTrue(set(day_one).isdisjoint(day_two))
        day_one_numbers = sorted(int(url.rsplit("/", 1)[-1].split("-", 1)[0]) for url in day_one)
        self.assertGreater(day_one_numbers[-1] - day_one_numbers[0], 4)

    def test_direct_monitor_uses_momentum_heavy_bounded_family_caps(self):
        events = []
        with mock.patch.object(
            pilot,
            "collect_direct_monitor_urls",
            return_value=[],
        ) as collect, mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_direct_monitor(
                dt.date(2026, 8, 7),
                set(),
                pilot.build_existing_index([]),
                set(),
            )

        self.assertEqual(pilot.direct_monitor_family_limits(), {"momentum": 40, "coldwell": 10})
        self.assertEqual([call.kwargs["limit"] for call in collect.call_args_list], [40, 10])
        self.assertEqual(stats["rows_written"], 0)
        start = [details for event, details in events if event == "pilot_direct_monitor_start"]
        self.assertEqual(start[0]["family_limits"], {"momentum": 40, "coldwell": 10})
        self.assertTrue(start[0]["shadow_only"])

    def test_direct_monitor_feed_failure_is_explicitly_incomplete(self):
        with mock.patch.object(
            pilot, "collect_direct_monitor_urls", side_effect=TimeoutError("feed timed out")
        ), mock.patch.object(pilot, "log_event"):
            stats = pilot.run_direct_monitor(
                dt.date(2026, 8, 7), set(), pilot.build_existing_index([]), set()
            )

        self.assertEqual(stats["families_planned"], 2)
        self.assertEqual(stats["families_succeeded"], 0)
        self.assertEqual(stats["families_failed"], 2)
        self.assertFalse(stats["complete"])

    def test_existing_agent_name_dedupes_even_when_contact_differs(self):
        existing = pilot.build_existing_index(
            [
                ["first", "last", "phone", "email", "listing_address", "city", "state"],
                ["Jane", "Smith", "404-555-1212", "jane@example.com", "1 Old St", "Atlanta", "GA"],
            ]
        )
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/new",
            title="2 New St",
            text="Status: Active. Remarks: Potential short sale.",
            fields={
                "agent_name": "Jane Smith",
                "listing_address": "2 New St",
                "city": "Denver",
                "state": "CO",
            },
        )

        self.assertEqual(
            pilot.duplicate_status(candidate, existing),
            ("possible_existing_agent", "jane smith", "2"),
        )

    def test_duplicate_listing_status_checks_address_before_contact_research(self):
        main_rows = [
            ["agent_name", "last_name", "phone", "email", "listing_address", "city", "state"],
            ["Linda", "Turney", "", "", "15790 Easthaven Ct, Unit 510", "Bowie", "MD"],
        ]
        existing = pilot.build_existing_index(main_rows)
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="",
            text="Status: Active. Remarks: Potential Short Sale.",
            fields={
                "listing_address": "15790 Easthaven Ct, Unit 510",
                "city": "Bowie",
                "state": "MD",
            },
        )

        self.assertEqual(
            pilot.duplicate_listing_status(candidate, existing),
            ("duplicate_listing", "15790 easthaven court 510|bowie|md", "2"),
        )

    def test_research_contact_runs_after_qualification_and_fills_missing_fields(self):
        old_search_web = pilot.search_web
        old_fetch_url = pilot.fetch_url
        calls = []

        def fake_search_web(query, source, limit):
            calls.append((query, source, limit))
            return "cse", [
                pilot.SearchResult(
                    source,
                    query,
                    "https://agent.example.com/jane-smith",
                    "Listing Agent: Jane Smith",
                    "Call 404-555-1212 or email jane@example.com",
                )
            ]

        def fake_fetch_url(url, allow_headless=True):
            return ""

        try:
            pilot.search_web = fake_search_web
            pilot.fetch_url = fake_fetch_url
            candidate = pilot.Candidate(
                source="idx_broker_pages",
                query="query",
                url="https://example.com/listing",
                title="123 Main Street, Atlanta, GA 30303",
                text="Status: Active. Remarks: Potential Short Sale.",
                fields={
                    "listing_address": "123 Main Street",
                    "city": "Atlanta",
                    "state": "GA",
                },
            )

            pilot.research_candidate_contact(candidate)

            self.assertEqual(candidate.fields.get("agent_name", ""), "")
            self.assertEqual(candidate.fields.get("phone", ""), "")
            self.assertEqual(candidate.fields.get("email", ""), "")
            self.assertTrue(calls)
        finally:
            pilot.search_web = old_search_web
            pilot.fetch_url = old_fetch_url

    def test_split_agent_name_matches_main_sheet_first_last_layout(self):
        self.assertEqual(pilot.split_agent_name("Michael E. LaMorte"), ("Michael", "E. LaMorte"))

    def test_bad_listing_agent_patterns_are_removed(self):
        cases = [
            ("Green Valley AZ", "Green Valley", "AZ", "1 Main St", "listing_agent_label"),
            ("MB Colorado", "Denver", "CO", "2 Main St", "listing_agent_label"),
            ("BHGRE Paracle Myrtle Beach", "Myrtle Beach", "SC", "3 Main St", "listing_agent_label"),
            ("Stratton Vantage", "Denver", "CO", "4 Main St", ""),
            ("Hunters Trail", "Atlanta", "GA", "187 Hunters Trail", "listing_agent_label"),
        ]
        for name, city, state, address, source in cases:
            with self.subTest(name=name):
                candidate = pilot.Candidate(
                    source="idx_broker_pages",
                    query="query",
                    url="https://example.com/listing",
                    title=address,
                    text="Status: Active. Remarks: Potential short sale.",
                    fields={
                        "agent_name": name,
                        "agent_name_source": source,
                        "listing_address": address,
                        "city": city,
                        "state": state,
                    },
                )

                safe, _ = pilot.sanitize_candidate_identity(candidate)

                self.assertFalse(safe)
                self.assertEqual(candidate.fields["agent_name"], "")

    def test_standalone_jsonld_real_estate_agent_contact_is_not_attributable(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          [{"@type":"Product","name":"123 Main Street, Atlanta, GA 30303",
            "description":"Potential short sale subject to lender approval."},
           {"@type":"RealEstateAgent","name":"Jane Q. Smith",
            "telephone":"(404) 555-1212","email":"Jane@Example.com"}]
        </script>
        <body>Status: Active.</body>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertEqual(candidate.fields["phone"], "")
        self.assertEqual(candidate.fields["email"], "")

    def test_nested_jsonld_listing_agent_is_bound_and_typed(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product",
           "address":{"streetAddress":"123 Main Street","addressLocality":"Atlanta",
                      "addressRegion":"GA","postalCode":"30303"},
           "description":"Short sale subject to lender approval.",
           "listingStatus":"Active",
           "listingAgent":{"@type":"RealEstateAgent","name":"Jane Smith",
                           "mobilePhone":"404-555-1212","email":"jane.smith@example.com"}}
        </script>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
        self.assertEqual(candidate.fields["agent_name_source"], "jsonld_bound_listing_agent")
        self.assertEqual(candidate.fields["phone_contact_type"], "direct_mobile")
        self.assertEqual(candidate.fields["email_contact_type"], "agent_specific_professional")
        self.assertTrue(pilot.has_complete_agent_contact(candidate))
        self.assertTrue(pilot.is_sms_ready_agent_contact(candidate))

    def test_multiple_nested_jsonld_agents_are_ambiguous(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product",
           "address":{"streetAddress":"123 Main Street","addressLocality":"Atlanta",
                      "addressRegion":"GA","postalCode":"30303"},
           "description":"Short sale subject to lender approval.","listingStatus":"Active",
           "agent":[
             {"@type":"RealEstateAgent","name":"Wrong Person","mobilePhone":"404-555-1212",
              "email":"wrong.person@example.com"},
             {"@type":"RealEstateAgent","name":"Right Person","mobilePhone":"678-555-1212",
              "email":"right.person@example.com"}]}
        </script>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertFalse(pilot.has_complete_agent_contact(candidate))

    def test_same_jsonld_agent_with_conflicting_contacts_is_not_complete(self):
        group = "123 main street|ga"
        fields = pilot.jsonld_bound_agent_fields(
            {
                "listingAgent": [
                    {"@type": "RealEstateAgent", "name": "Jane Smith",
                     "mobilePhone": "404-555-1212", "email": "jane.smith@example.com"},
                    {"@type": "RealEstateAgent", "name": "Jane Smith",
                     "mobilePhone": "678-555-1212", "email": "jsmith@example.com"},
                ]
            },
            group,
        )
        candidate = pilot.Candidate("s", "q", "u", "t", "", {**fields, "listing_identity_group": group})

        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
        self.assertEqual(candidate.fields.get("phone", ""), "")
        self.assertEqual(candidate.fields.get("email", ""), "")
        self.assertEqual(candidate.fields["agent_contact_conflict"], "phone,email")
        self.assertFalse(pilot.has_complete_agent_contact(candidate))

    def test_tied_jsonld_records_blank_same_subject_contact_conflicts(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        listing = lambda phone, email: {
            "@type": "Product",
            "address": {"streetAddress": "123 Main Street", "addressLocality": "Atlanta",
                        "addressRegion": "GA", "postalCode": "30303"},
            "description": "Short sale subject to lender approval.",
            "listingStatus": "Active",
            "listingAgent": {"@type": "RealEstateAgent", "name": "Jane Smith",
                             "mobilePhone": phone, "email": email},
        }
        markup = (
            '<script type="application/ld+json">'
            + json.dumps([listing("404-555-1212", "jane.smith@example.com"),
                          listing("678-555-1212", "jsmith@example.com")])
            + "</script>"
        )
        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(pilot.qualification_for_candidate(candidate).status, "qualified")
        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
        self.assertEqual(candidate.fields.get("phone", ""), "")
        self.assertEqual(candidate.fields.get("email", ""), "")
        self.assertEqual(candidate.fields["agent_contact_conflict"], "email,phone")
        self.assertFalse(pilot.has_complete_agent_contact(candidate))

    def test_generic_jsonld_agent_role_is_not_a_listing_agent(self):
        group = "123 main street|ga"
        fields = pilot.jsonld_bound_agent_fields(
            {
                "agent": {"@type": "Person", "name": "Alice Buyer",
                          "jobTitle": "Buyer's Agent", "mobilePhone": "404-555-1212",
                          "email": "alice.buyer@example.com"},
                "offers": {"agent": {"@type": "Person", "name": "Content Writer"}},
            },
            group,
        )
        self.assertEqual(fields, {})

    def test_non_agent_jsonld_roles_are_not_listing_agents(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        for role in ("seller", "provider", "author", "broker"):
            with self.subTest(role=role):
                markup = f"""
                <script type="application/ld+json">
                  {{"@type":"Product",
                   "address":{{"streetAddress":"123 Main Street","addressLocality":"Atlanta",
                              "addressRegion":"GA","postalCode":"30303"}},
                   "description":"Short sale subject to lender approval.","listingStatus":"Active",
                   "{role}":{{"@type":"Person","name":"Content Writer",
                              "mobilePhone":"404-555-1212","email":"content.writer@example.com"}}}}
                </script>
                """
                candidate = pilot.infer_fields(result, markup)
                self.assertEqual(candidate.fields["agent_name"], "")

    def test_valid_listing_ignores_unrelated_page_wide_agent_sidebar(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="Listing Agent: Wrong Person 212-555-1212 wrong.person@example.com",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product",
           "address":{"streetAddress":"123 Main Street","addressLocality":"Atlanta",
                      "addressRegion":"GA","postalCode":"30303"},
           "description":"Short sale subject to lender approval.",
           "listingStatus":"Active"}
        </script>
        <aside>Listing Agent: Alice Owner Direct: 404-555-1212 Email: alice.owner@example.com</aside>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertEqual(candidate.fields["phone"], "")
        self.assertEqual(candidate.fields["email"], "")

    @unittest.skipUnless(importlib.util.find_spec("bs4"), "production BeautifulSoup dependency required")
    def test_visible_agent_requires_one_explicit_agent_subrecord(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <article class="listing-detail">
          <div class="property-description">123 Main Street. Status: Active.
          Short sale subject to lender approval.</div>
          <div class="listing-agent-info">Listing Agent: Jane Smith Direct: 404-555-1212
          Email: jane.smith@example.com</div>
        </article>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
        self.assertTrue(pilot.has_complete_agent_contact(candidate))

    @unittest.skipUnless(importlib.util.find_spec("bs4"), "production BeautifulSoup dependency required")
    def test_visible_multiple_or_related_agents_are_left_blank(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="",
        )
        markup = """
        <article class="listing-detail">
          <div class="property-description">123 Main Street. Status: Active.
          Short sale subject to lender approval.</div>
          <aside class="related-card listing-agent-info">Listing Agent: Alice Wrong Direct:
          404-555-1212 Email: alice.wrong@example.com</aside>
          <section class="listing-agent-info">Listing Agent: Bob Correct Direct:
          678-555-1212 Email: bob.correct@example.com</section>
        </article>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "Bob Correct")
        self.assertNotEqual(candidate.fields["agent_name"], "Alice Wrong")

    @unittest.skipUnless(importlib.util.find_spec("bs4"), "production BeautifulSoup dependency required")
    def test_visible_agent_inside_related_ancestor_is_ignored(self):
        result = pilot.SearchResult(
            source="idx_broker_pages", query="query",
            url="https://example.com/123-main-street",
            title="123 Main Street, Atlanta, GA 30303", snippet="",
        )
        markup = """
        <article class="listing-detail">
          <div class="property-description">123 Main Street. Status: Active.
          Short sale subject to lender approval.</div>
          <aside class="related-card"><div class="agent-profile">Listing Agent: Alice Wrong
          Direct: 404-555-1212 Email: alice.wrong@example.com</div></aside>
        </article>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")

    def test_team_routing_and_toll_free_contacts_never_fill_primary_fields(self):
        group = "123 main street|ga"
        for name in ("Michael Saunders & Company", "The Alice Owner Team"):
            with self.subTest(name=name):
                self.assertEqual(
                    pilot.bind_agent_fields(
                        {"agent_name": name, "phone": "800-555-1212", "email": "info123@example.com"},
                        group,
                        "jsonld_bound_listing_agent",
                        "Direct: 800-555-1212",
                    ),
                    {},
                )
        fields = pilot.bind_agent_fields(
            {"agent_name": "Jane Smith", "phone": "800-555-1212", "email": "frontdesk@example.com"},
            group,
            "jsonld_bound_listing_agent",
            "Direct: 800-555-1212",
        )
        candidate = pilot.Candidate("s", "q", "u", "t", "", {**fields, "listing_identity_group": group})
        pilot.sanitize_candidate_identity(candidate)
        self.assertEqual(candidate.fields["phone"], "")
        self.assertEqual(candidate.fields["email"], "")
        self.assertEqual(candidate.fields["contact_phone_hint_type"], "office_team_main")
        self.assertEqual(candidate.fields["contact_email_hint_type"], "team_brokerage_routing")
        self.assertFalse(pilot.has_complete_agent_contact(candidate))
        for email_value in (
            "smithteam@example.com",
            "smithlistings@example.com",
            "jsmithoffice@example.com",
        ):
            with self.subTest(email=email_value):
                self.assertEqual(
                    pilot.bound_email_contact_type(email_value, "Jane Smith"),
                    "team_brokerage_routing",
                )
        for name, email_value in (
            ("Al Ho", "showings@example.com"),
            ("Joe Li", "clientservices@example.com"),
        ):
            with self.subTest(name=name, email=email_value):
                self.assertEqual(
                    pilot.bound_email_contact_type(email_value, name),
                    "team_brokerage_routing",
                )

    def test_contact_complete_requires_ten_digit_same_owner_contact(self):
        group = "123 main street|ga"
        candidate = pilot.Candidate(
            "s", "q", "u", "t", "",
            {
                "agent_name": "Jane Smith", "agent_subject_key": "jane smith",
                "agent_evidence_group": group, "listing_identity_group": group,
                "phone": "123", "phone_contact_type": "direct_mobile",
                "phone_evidence_group": group, "phone_owner_key": "jane smith",
                "email": "jane.smith@example.com",
                "email_contact_type": "agent_specific_professional",
                "email_evidence_group": group, "email_owner_key": "jane smith",
            },
        )
        self.assertFalse(pilot.has_complete_agent_contact(candidate))

    def test_page_wide_office_contact_is_not_assigned_to_agent(self):
        result = pilot.SearchResult(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street, Atlanta, GA 30303",
            snippet="Listed by Jane Smith",
        )
        markup = """
        <body>Status: Active. Remarks: Potential short sale.
        Office phone 404-555-9999. support@example.com</body>
        """

        candidate = pilot.infer_fields(result, markup)

        self.assertEqual(candidate.fields["agent_name"], "")
        self.assertEqual(candidate.fields.get("phone", ""), "")
        self.assertEqual(candidate.fields.get("email", ""), "")

    def test_shadow_ready_allows_verifier_to_fill_blank_contact(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Pending. Public Remarks: Potential short sale subject to lender approval.",
            fields={
                "agent_name": "Jane Q. Smith",
                "agent_name_source": "listing_agent_label",
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        candidate.fields.update(
            self.bound_agent_fields("Jane Q. Smith", "123 Main Street", "GA")
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "", "", "")

        self.assertEqual(row[:4], ["Jane", "Q. Smith", "", ""])
        self.assertEqual(row[14], "shadow_ready")
        self.assertEqual(row[16], "yes")
        self.assertIn("lead verifier", row[15])
        self.assertIn("Automatic PendingQueue promotion is disabled", row[15])

    def test_overview_only_short_sale_needs_description_confirmation(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Overview: Potential short sale subject to lender approval.",
            fields={
                "agent_name": "Jane Smith",
                "agent_name_source": "listing_agent_label",
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "", "", "")

        self.assertEqual(qualification.status, "qualified")
        self.assertEqual(row[14], "needs_description_confirmation")
        self.assertEqual(row[16], "review")

    def test_qualification_precedence_shadow_accepts_overview_without_writing(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Overview: SHORT SALE. Updated three-bedroom home.",
            fields={
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
                "home_status": "FOR_SALE",
            },
        )

        result = pilot.qualification_precedence_shadow(candidate)

        self.assertFalse(result["current_description_confirmed"])
        self.assertTrue(result["proposed_description_confirmed"])
        self.assertTrue(result["proposed_ready"])
        self.assertEqual(result["writes"], 0)

    def test_description_block_shadow_holds_overview_only_false_accept(self):
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="407 N Pittman Street",
            text=(
                "Status: Active. Overview: Updated century home. "
                "Amenities Foreclosure Views Short Sale New Construction."
            ),
            fields={
                "listing_address": "407 N Pittman Street",
                "city": "Prairie Grove",
                "state": "AR",
                "home_status": "FOR_SALE",
            },
        )

        result = pilot.description_block_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertFalse(result["description_block_confirmed"])
        self.assertFalse(result["proposed_ready"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_description_block_shadow_keeps_property_description_evidence(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing",
            title="202 Ridgewood Lane",
            text="Status: Pending. Property Description: This is a short sale subject to approval.",
            fields={
                "listing_address": "202 Ridgewood Lane",
                "city": "Shelbyville",
                "state": "TN",
                "home_status": "PENDING",
            },
        )

        result = pilot.description_block_shadow(candidate)

        self.assertTrue(result["description_block_confirmed"])
        self.assertTrue(result["proposed_ready"])
        self.assertFalse(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_description_block_shadow_holds_site_navigation_after_description(self):
        navigation = (
            "Home Advanced-Search Foreclosure Property Short Sale Property "
            "Featured Listing Buy A House Get Prequalified"
        )
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing",
            title="510 Boston Neck Road",
            text=(
                "Status: Active. Property Description. Duck Cove Condominium within a short "
                f"distance to all amenities. Skip to content. {navigation}"
            ),
            fields={
                "listing_address": "510 Boston Neck Road",
                "city": "North Kingstown",
                "state": "RI",
                "home_status": "FOR_SALE",
                "listing_description": navigation,
            },
        )

        result = pilot.description_block_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertFalse(result["description_block_confirmed"])
        self.assertFalse(result["proposed_ready"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_site_chrome_shadow_holds_nexus_card_phrase_outside_description(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url=(
                "https://www.nexusrealtync.com/homes/409-Alicat-Drive/"
                "Simpsonville/SC/29680/179690521/"
            ),
            title="409 Alicat Drive, Simpsonville, SC 29680 (#1599568) - Nexus Realty",
            text=(
                "Beds:3 Baths:2 Sq. Feet:1600-1799 Status: Active. "
                "Property Description ... Short Sale. $216,500. "
                "Baldwin Ridge | Simpsonville. 3 beds. 2 baths."
            ),
            fields={
                "listing_address": "409 Alicat Drive",
                "city": "Simpsonville",
                "state": "SC",
                "home_status": "FOR_SALE",
                "listing_description": "Beds:3 Baths:2 Sq. Feet:1600-1799",
            },
        )

        with mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_DOMAINS",
            {"nexusrealtync.com"},
        ):
            result = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertTrue(result["platform_targeted"])
        self.assertTrue(result["site_chrome_pattern_found"])
        self.assertFalse(result["listing_description_short_sale_confirmed"])
        self.assertFalse(result["proposed_ready"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["reason"], "site_chrome_short_sale_card_only")
        self.assertEqual(result["writes"], 0)

    def test_site_chrome_shadow_keeps_nexus_listing_with_description_evidence(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://www.nexusrealtync.com/homes/123-Main-Street/Atlanta/GA/30303/1/",
            title="123 Main Street",
            text=(
                "Status: Active. Property Description: This home is a short sale subject to "
                "lender approval. Property Description ... Short Sale. $200,000."
            ),
            fields={
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
                "home_status": "FOR_SALE",
                "listing_description": "This home is a short sale subject to lender approval.",
            },
        )

        with mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_DOMAINS",
            {"nexusrealtync.com"},
        ):
            result = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertTrue(result["listing_description_short_sale_confirmed"])
        self.assertTrue(result["proposed_ready"])
        self.assertFalse(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_site_chrome_shadow_does_not_generalize_beyond_target_domain(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing/123",
            title="123 Main Street",
            text=(
                "Status: Active. Beds:3 Baths:2. "
                "Property Description ... Short Sale. $200,000."
            ),
            fields={
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
                "home_status": "FOR_SALE",
                "listing_description": "Beds:3 Baths:2 Sq. Feet:1600-1799",
            },
        )

        with mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_DOMAINS",
            {"nexusrealtync.com"},
        ):
            result = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertFalse(result["platform_targeted"])
        self.assertTrue(result["site_chrome_pattern_found"])
        self.assertTrue(result["proposed_ready"])
        self.assertFalse(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_site_chrome_shadow_holds_bishop_navigation_phrase(self):
        navigation = (
            "Home Buying Process How Much House Can I Afford? Buyer Resource Videos "
            "8 Steps to Buying a Home Financing Options Short Sale Option "
            "Selecting Your Real Estate Agent CRS Sellers"
        )
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://www.bishopcountry.com/1704-langford-street",
            title="1704 Langford Street Greenville, TX 75401 -- MLS# 21231320",
            text=(
                "Property Description. Completely remodeled and move-in ready. "
                f"{navigation}"
            ),
            fields={
                "listing_address": "1704 Langford Street",
                "city": "Greenville",
                "state": "TX",
                "home_status": "FOR_SALE",
                "listing_description": navigation,
            },
        )

        result = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertTrue(result["platform_targeted"])
        self.assertTrue(result["navigation_pattern_found"])
        self.assertTrue(result["listing_description_is_navigation"])
        self.assertFalse(result["listing_description_short_sale_confirmed"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["reason"], "site_chrome_short_sale_navigation_only")
        self.assertEqual(result["writes"], 0)

    def test_compound_negative_shadow_holds_foreclosure_short_sale_no(self):
        candidate = pilot.Candidate(
            source="redfin.com",
            query="query",
            url="https://www.redfin.com/example/listing",
            title="10556 Snohomish Avenue",
            text=(
                "Status: Active. Property Description: Foreclosure/Short Sale: No. "
                "Updated home with four bedrooms."
            ),
            fields={
                "listing_address": "10556 Snohomish Avenue",
                "city": "Pacoima",
                "state": "CA",
                "home_status": "FOR_SALE",
                "listing_description": (
                    "Foreclosure/Short Sale: No. Updated home with four bedrooms."
                ),
            },
        )

        result = pilot.compound_negative_field_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertTrue(result["explicit_negative_field_found"])
        self.assertEqual(result["negative_field_label"], "Foreclosure/Short Sale")
        self.assertEqual(result["negative_field_value"], "no")
        self.assertFalse(result["proposed_ready"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["reason"], "explicit_negative_short_sale_field")
        self.assertEqual(result["writes"], 0)

    def test_compound_negative_shadow_matches_approved_field_variants(self):
        for field_text in (
            "Short Sale: No",
            "Short Sale Status: No",
            "Foreclosure/Short Sale: No",
        ):
            with self.subTest(field_text=field_text):
                match = pilot.COMPOUND_SHORT_SALE_NEGATIVE_RE.search(field_text)
                self.assertIsNotNone(match)
                self.assertEqual(match.group("value").lower(), "no")

    def test_compound_negative_shadow_keeps_genuine_agent_remarks(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing/123",
            title="123 Main Street",
            text=(
                "Status: Active. Property Description: This is a short sale subject to "
                "lender approval."
            ),
            fields={
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
                "home_status": "FOR_SALE",
                "listing_description": (
                    "This is a short sale subject to lender approval."
                ),
            },
        )

        result = pilot.compound_negative_field_shadow(candidate)

        self.assertTrue(result["current_ready"])
        self.assertFalse(result["explicit_negative_field_found"])
        self.assertTrue(result["proposed_ready"])
        self.assertFalse(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_future_negotiator_phrase_shadow_holds_assignment_in_progress_without_writing(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://www.rockspringsrealty.com/property/O6385349/",
            title="2735 Grassmoor Loop",
            text=(
                "Status: Active. Property Description: Short Sale, Price is not approved. "
                "We are in the process of being assigned a bank negotiator."
            ),
            fields={
                "listing_address": "2735 Grassmoor Loop",
                "city": "Apopka",
                "state": "FL",
                "home_status": "FOR_SALE",
                "listing_description": (
                    "Short Sale, Price is not approved. We are in the process of being "
                    "assigned a bank negotiator."
                ),
            },
        )

        result = pilot.future_negotiator_phrase_shadow(candidate)

        self.assertTrue(result["phrase_found"])
        self.assertTrue(result["would_hold"])
        self.assertEqual(result["reason"], "future_negotiator_involvement")
        self.assertIn("assigned a bank negotiator", result["evidence"])
        self.assertEqual(result["writes"], 0)

    def test_future_negotiator_phrase_shadow_ignores_generic_reference(self):
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://example.com/listing",
            title="123 Main Street",
            text="Status: Active. Remarks: Short Sale. Seller may consult a negotiator if needed.",
            fields={
                "listing_address": "123 Main Street",
                "city": "Atlanta",
                "state": "GA",
                "listing_description": "Short Sale. Seller may consult a negotiator if needed.",
            },
        )

        result = pilot.future_negotiator_phrase_shadow(candidate)

        self.assertFalse(result["phrase_found"])
        self.assertFalse(result["would_hold"])
        self.assertEqual(result["writes"], 0)

    def test_qualification_followup_hold_shadow_finds_existing_linked_hold(self):
        pilot_row = {
            "status": "rejected",
            "failure_reason": "disqualifying_short_sale_text",
            "promotion_status": "disqualified_negotiator",
            "import_ready": "skip",
            "listing_address": "2735 Grassmoor Loop",
            "state": "FL",
            "synthetic_zpid": "free-bbbabe7d3f97f380",
            "matched_main_row": "4864",
        }
        main_rows = [
            (
                4864,
                {
                    "listing_address": "2735 Grassmoor Loop",
                    "state": "FL",
                    "created_at": "free-bbbabe7d3f97f380",
                    "followup_text_sent": "x",
                    "human_override": "TRUE",
                    "contact_verification_note": "FOLLOW-ON HOLD: do not initiate further outreach.",
                },
            )
        ]

        result = pilot.qualification_followup_hold_shadow(83, pilot_row, main_rows)

        self.assertEqual(result["linkage_outcome"], "linked")
        self.assertEqual(result["matched_main_row"], 4864)
        self.assertTrue(result["followup_already_sent"])
        self.assertTrue(result["would_hold"])
        self.assertTrue(result["existing_hold"])
        self.assertFalse(result["hold_gap"])
        self.assertEqual(result["writes"], 0)

    def test_qualification_followup_hold_shadow_keeps_unlinked_duplicate_unlinked(self):
        pilot_row = {
            "status": "duplicate",
            "failure_reason": "existing_agent_owner_contacted",
            "promotion_status": "duplicate_existing_agent",
            "import_ready": "skip",
            "listing_address": "9054 W Coronado Drive",
            "state": "AZ",
            "synthetic_zpid": "free-9859bb032d495240",
            "matched_main_row": "",
        }

        result = pilot.qualification_followup_hold_shadow(70, pilot_row, [])

        self.assertEqual(result["linkage_outcome"], "missing")
        self.assertFalse(result["would_hold"])
        self.assertFalse(result["hold_gap"])
        self.assertEqual(result["writes"], 0)

    def test_dedupe_matches_street_and_state_when_city_differs(self):
        main_rows = [
            ["first", "last", "phone", "email", "listing_address", "city", "state"],
            ["Michael", "LaMorte", "", "", "610 Farm To Market Road", "Brewster", "NY"],
        ]
        existing = pilot.build_existing_index(main_rows)
        candidate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://example.com/listing",
            title="610 Farm To Market Road",
            text="Status: Active. Remarks: Potential short sale.",
            fields={"listing_address": "610 Farm To Market Road", "city": "Patterson", "state": "NY"},
        )

        self.assertEqual(
            pilot.duplicate_listing_status(candidate, existing),
            ("duplicate_listing", "610 farm to market road|ny", "2"),
        )

    def test_synthetic_zpid_is_stable_across_sources_and_city_aliases(self):
        first = pilot.stable_synthetic_zpid(
            "idx_broker_pages",
            "https://one.example/listing",
            "610 Farm To Market Road",
            "Brewster",
            "NY",
        )
        second = pilot.stable_synthetic_zpid(
            "idx_broker_remarks",
            "https://two.example/property",
            "610 Farm To Market Road",
            "Patterson",
            "NY",
        )

        self.assertEqual(first, second)

    def test_route_alias_shadow_matches_rt_and_county_road_without_changing_production_key(self):
        self.assertNotEqual(
            pilot.street_state_key("656 Rt 518", "NJ"),
            pilot.street_state_key("656 County Road 518", "NJ"),
        )
        self.assertEqual(
            pilot.route_alias_shadow_key("656 Rt 518", "NJ"),
            pilot.route_alias_shadow_key("656 County Road 518", "NJ"),
        )

    def test_route_alias_dedupe_shadow_requires_exact_canonical_identifier(self):
        prior_row = {
            "listing_address": "656 County Road 518",
            "state": "NJ",
            "raw_title": "656 County Road 518 (MLS #NJSO2005976)",
        }
        candidate_row = {
            "listing_address": "656 Rt 518",
            "state": "NJ",
            "raw_title": "656 Rt 518 (MLS #NJSO2005976)",
        }

        result = pilot.route_alias_dedupe_shadow(1113, candidate_row, [(1109, prior_row)])

        self.assertTrue(result["alias_collision"])
        self.assertTrue(result["reviewable"])
        self.assertTrue(result["exact_identifier_agreement"])
        self.assertFalse(result["conflicting_identifier_stop"])
        self.assertEqual(result["matched_prior_row"], 1109)
        self.assertEqual(result["writes"], 0)
        self.assertNotIn("candidate_identifier", result)
        self.assertNotIn("address", result)

    def test_route_alias_dedupe_shadow_stops_on_conflicting_identifier(self):
        prior_row = {
            "listing_address": "656 County Road 518",
            "state": "NJ",
            "raw_title": "656 County Road 518 (MLS #NJSO2005000)",
        }
        candidate_row = {
            "listing_address": "656 Rt 518",
            "state": "NJ",
            "raw_title": "656 Rt 518 (MLS #NJSO2005976)",
        }

        result = pilot.route_alias_dedupe_shadow(1113, candidate_row, [(1109, prior_row)])

        self.assertTrue(result["alias_collision"])
        self.assertTrue(result["reviewable"])
        self.assertFalse(result["exact_identifier_agreement"])
        self.assertTrue(result["conflicting_identifier_stop"])
        self.assertEqual(result["writes"], 0)

    def test_route_alias_shadow_audit_catches_known_pair_without_writes(self):
        pilot_rows = [
            pilot.PILOT_HEADERS,
            self.pilot_row(
                listing_address="656 County Road 518",
                state="NJ",
                first_seen_at="2026-08-16T07:15:00-04:00",
                synthetic_zpid="free-prior",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
                raw_title="656 County Road 518 (MLS #NJSO2005976)",
            ),
            self.pilot_row(
                listing_address="656 Rt 518",
                state="NJ",
                first_seen_at="2026-08-17T07:15:00-04:00",
                synthetic_zpid="free-candidate",
                source="idx_broker_remarks",
                status="duplicate",
                promotion_status="skipped_duplicate_listing",
                import_ready="skip",
                raw_title="656 Rt 518 (MLS #NJSO2005976)",
            ),
        ]
        events = []

        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[[["created-at", "Listing Address", "State"]], pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 17),
                phase="post_verifier",
                force=True,
            )

        shadow_event = next(
            details for event, details in events if event == "pilot_route_alias_dedupe_shadow"
        )
        self.assertEqual(stats["route_alias_shadow_exact"], 1)
        self.assertEqual(stats["route_alias_shadow_conflicts"], 0)
        self.assertEqual(shadow_event["candidate_row"], 3)
        self.assertEqual(shadow_event["matched_prior_row"], 2)
        self.assertTrue(shadow_event["exact_identifier_agreement"])
        self.assertFalse(shadow_event["raw_identifier_logged"])
        self.assertFalse(shadow_event["raw_address_logged"])
        self.assertFalse(shadow_event["raw_url_logged"])
        self.assertEqual(shadow_event["writes"], 0)

    def test_clean_listing_address_removes_repeated_adjacent_word(self):
        self.assertEqual(pilot.clean_listing_address("923 W Main Main Street"), "923 W Main Street")

    def test_idx_remarks_bucket_uses_idx_detail_filter(self):
        search = pilot.SearchResult(
            "idx_broker_remarks",
            "query",
            "https://example.com/search",
            "Search homes",
            "",
        )
        detail = pilot.SearchResult(
            "idx_broker_remarks",
            "query",
            "https://example.com/listing/123-main-street",
            "123 Main Street",
            "",
        )

        self.assertEqual(pilot.source_result_allowed(search), (False, "not_idx_listing_detail"))
        self.assertEqual(pilot.source_result_allowed(detail), (True, ""))

    def test_phone_regex_rejects_long_photo_timestamps(self):
        self.assertIsNone(pilot.PHONE_RE.search("20260512213414244719000000-o.jpg"))
        self.assertEqual(pilot.PHONE_RE.search("(404) 555-1212").group(0), "(404) 555-1212")
        self.assertEqual(
            pilot.first_contact_phone_match("photo-m1567719840s.jpg Phone: (508)594-3513").group(0),
            "(508)594-3513",
        )

    def test_listing_url_canonicalization_collapses_tracking_but_preserves_identity_query(self):
        first = pilot.canonical_public_listing_url(
            "https://Broker.Example/listing?id=123&utm_source=a#photos"
        )
        second = pilot.canonical_public_listing_url(
            "https://broker.example/listing?utm_medium=email&id=123"
        )
        different = pilot.canonical_public_listing_url(
            "https://broker.example/listing?id=456&utm_source=a"
        )

        self.assertEqual(first, "https://broker.example/listing?id=123")
        self.assertEqual(second, first)
        self.assertNotEqual(different, first)

    def test_search_web_prefers_google_cse_when_configured(self):
        old_engine = pilot.SEARCH_ENGINE
        old_key = pilot.CSE_API_KEY
        old_cx = pilot.CSE_CX
        old_cse_search = pilot.cse_search
        old_ddg_search = pilot.ddg_search
        calls = []

        def fake_cse_search(query, source, limit, date_restrict=None):
            calls.append(("cse", query, source, limit, date_restrict))
            return [pilot.SearchResult(source, query, "https://example.com/1", "Title", "Snippet")]

        def fake_ddg_search(query, source, limit):
            calls.append(("ddg", query, source, limit))
            return []

        try:
            pilot.SEARCH_ENGINE = "auto"
            pilot.CSE_API_KEY = "key"
            pilot.CSE_CX = "cx"
            pilot.cse_search = fake_cse_search
            pilot.ddg_search = fake_ddg_search

            engine, results = pilot.search_web("query", "source", 3, date_restrict="w1")

            self.assertEqual(engine, "cse")
            self.assertEqual(len(results), 1)
            self.assertEqual(calls, [("cse", "query", "source", 3, "w1")])
        finally:
            pilot.SEARCH_ENGINE = old_engine
            pilot.CSE_API_KEY = old_key
            pilot.CSE_CX = old_cx
            pilot.cse_search = old_cse_search
            pilot.ddg_search = old_ddg_search

    def test_search_web_falls_back_to_duckduckgo_after_cse_error(self):
        old_engine = pilot.SEARCH_ENGINE
        old_key = pilot.CSE_API_KEY
        old_cx = pilot.CSE_CX
        old_allow_ddg = pilot.ALLOW_DDG_FALLBACK
        old_cse_search = pilot.cse_search
        old_ddg_search = pilot.ddg_search
        calls = []

        def fake_cse_search(query, source, limit, date_restrict=None):
            calls.append(("cse", query, source, limit, date_restrict))
            raise RuntimeError("cse down")

        def fake_ddg_search(query, source, limit):
            calls.append(("ddg", query, source, limit))
            return [pilot.SearchResult(source, query, "https://example.com/2", "Title", "Snippet")]

        try:
            pilot.SEARCH_ENGINE = "auto"
            pilot.CSE_API_KEY = "key"
            pilot.CSE_CX = "cx"
            pilot.ALLOW_DDG_FALLBACK = True
            pilot.cse_search = fake_cse_search
            pilot.ddg_search = fake_ddg_search
            pilot.reset_search_engine_attempt_stats()

            with contextlib.redirect_stdout(io.StringIO()):
                engine, results = pilot.search_web("query", "source", 3)

            self.assertEqual(engine, "ddg")
            self.assertEqual(len(results), 1)
            self.assertEqual(calls, [("cse", "query", "source", 3, None), ("ddg", "query", "source", 3)])
            self.assertEqual(
                pilot._search_engine_attempt_stats,
                {"attempted": 2, "succeeded": 1, "blocked": 0, "failed": 1},
            )
        finally:
            pilot.SEARCH_ENGINE = old_engine
            pilot.CSE_API_KEY = old_key
            pilot.CSE_CX = old_cx
            pilot.ALLOW_DDG_FALLBACK = old_allow_ddg
            pilot.cse_search = old_cse_search
            pilot.ddg_search = old_ddg_search

    def test_search_web_does_not_fallback_to_duckduckgo_by_default(self):
        old_engine = pilot.SEARCH_ENGINE
        old_key = pilot.CSE_API_KEY
        old_cx = pilot.CSE_CX
        old_allow_ddg = pilot.ALLOW_DDG_FALLBACK
        old_cse_search = pilot.cse_search
        old_ddg_search = pilot.ddg_search
        calls = []

        def fake_cse_search(query, source, limit, date_restrict=None):
            calls.append(("cse", query, source, limit, date_restrict))
            raise RuntimeError("cse down")

        def fake_ddg_search(query, source, limit):
            calls.append(("ddg", query, source, limit))
            return []

        try:
            pilot.SEARCH_ENGINE = "auto"
            pilot.CSE_API_KEY = "key"
            pilot.CSE_CX = "cx"
            pilot.ALLOW_DDG_FALLBACK = False
            pilot.cse_search = fake_cse_search
            pilot.ddg_search = fake_ddg_search

            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(RuntimeError):
                    pilot.search_web("query", "source", 3)

            self.assertEqual(calls, [("cse", "query", "source", 3, None)])
        finally:
            pilot.SEARCH_ENGINE = old_engine
            pilot.CSE_API_KEY = old_key
            pilot.CSE_CX = old_cx
            pilot.ALLOW_DDG_FALLBACK = old_allow_ddg
            pilot.cse_search = old_cse_search
            pilot.ddg_search = old_ddg_search

    def test_explicit_cse_without_credentials_fails_closed(self):
        with mock.patch.object(pilot, "SEARCH_ENGINE", "cse"), \
             mock.patch.object(pilot, "CSE_API_KEY", ""), \
             mock.patch.object(pilot, "CSE_CX", ""), \
             mock.patch.object(pilot, "cse_search") as search:
            with self.assertRaisesRegex(RuntimeError, "missing CSE_API_KEY"):
                pilot.search_web("query", "source", 3)
        search.assert_not_called()

    def test_cse_search_uses_configured_date_restrict(self):
        old_key = pilot.CSE_API_KEY
        old_cx = pilot.CSE_CX
        old_date_restrict = pilot.CSE_DATE_RESTRICT
        old_urlopen = pilot.urllib.request.urlopen
        captured = {}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps({"items": []}).encode("utf-8")

        def fake_urlopen(req, timeout=30):
            captured["url"] = req.full_url
            return FakeResponse()

        try:
            pilot.CSE_API_KEY = "key"
            pilot.CSE_CX = "cx"
            pilot.CSE_DATE_RESTRICT = "d1"
            pilot.urllib.request.urlopen = fake_urlopen

            pilot.cse_search("query", "source", 10)

            parsed = urllib.parse.urlparse(captured["url"])
            params = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(params["dateRestrict"], ["d1"])
        finally:
            pilot.CSE_API_KEY = old_key
            pilot.CSE_CX = old_cx
            pilot.CSE_DATE_RESTRICT = old_date_restrict
            pilot.urllib.request.urlopen = old_urlopen

    def test_brokerage_suffix_shadow_is_two_part_and_never_mutates_input(self):
        self.assertEqual(
            pilot.brokerage_suffix_shadow_name("Tyler Davis Black Label"),
            {
                "original_agent": "Tyler Davis Black Label",
                "proposed_agent": "Tyler Davis",
                "brokerage_suffix": "Black Label",
            },
        )
        self.assertEqual(pilot.brokerage_suffix_shadow_name("Black Label"), {})
        self.assertEqual(pilot.brokerage_suffix_shadow_name("Mary Jane Smith Realty"), {})
        self.assertEqual(pilot.brokerage_suffix_shadow_name("Tyler Davis"), {})

    def test_agent_artifact_shadow_strips_known_feed_suffixes(self):
        self.assertEqual(
            pilot.agent_artifact_shadow_name("Troy Funk Provided Stellar")["proposed_agent"],
            "Troy Funk",
        )
        self.assertEqual(
            pilot.agent_artifact_shadow_name("Jessica Estrada Provided Stellar")["proposed_agent"],
            "Jessica Estrada",
        )
        self.assertEqual(
            pilot.agent_artifact_shadow_name("Whitney Aldrich · Equity")["proposed_agent"],
            "Whitney Aldrich",
        )
        self.assertEqual(pilot.agent_artifact_shadow_name("Stellar Realty"), {})

    def test_reconcile_pilot_link_requires_same_stable_id_and_address(self):
        pilot_row = {
            "synthetic_zpid": "free-abc",
            "listing_address": "10011 Achilles Street",
            "state": "MI",
            "matched_main_row": "4754",
        }
        main_rows = pilot.sheet_row_maps(
            [
                ["First Name", "Last Name", "Listing Address", "State", "ZPID"],
                ["Other", "Lead", "20 Oak Street", "MI", "free-abc"],
                ["Right", "Address", "10011 Achilles St", "MI", "different-id"],
            ]
        )

        result = pilot.reconcile_pilot_link(67, pilot_row, main_rows)

        self.assertEqual(result["outcome"], "identity_address_mismatch")
        self.assertTrue(result["follow_on_hold"])
        self.assertEqual(result["id_match_rows"], [2])
        self.assertEqual(result["address_match_rows"], [3])
        self.assertFalse(result["pointer_matches"])

    def test_reconcile_pilot_link_uses_live_headers_and_ignores_stale_pointer(self):
        pilot_row = {
            "synthetic_zpid": "free-abc",
            "listing_address": "10011 Achilles Street",
            "state": "MI",
            "matched_main_row": "4754",
        }
        main_rows = pilot.sheet_row_maps(
            [
                ["First Name", "Last Name", "Street", "State", "ZPID"],
                ["Right", "Agent", "10011 Achilles St", "MI", "free-abc"],
            ]
        )

        result = pilot.reconcile_pilot_link(67, pilot_row, main_rows)

        self.assertEqual(result["outcome"], "linked")
        self.assertEqual(result["matched_main_row"], 2)
        self.assertFalse(result["pointer_matches"])
        self.assertFalse(result["follow_on_hold"])

    def test_reconcile_pilot_link_accepts_free_id_under_legacy_created_at_header(self):
        pilot_row = {
            "synthetic_zpid": "free-69f7af3e3812c17f",
            "listing_address": "1507 Carlos Avenue",
            "state": "FL",
            "matched_main_row": "2",
        }
        main_rows = pilot.sheet_row_maps(
            [
                ["First Name", "Last Name", "Listing Address", "State", "created-at"],
                ["Troy", "Funk", "1507 CARLOS AVENUE", "FL", "free-69f7af3e3812c17f"],
            ]
        )

        result = pilot.reconcile_pilot_link(73, pilot_row, main_rows)

        self.assertEqual(result["outcome"], "linked")
        self.assertEqual(result["matched_main_row"], 2)
        self.assertEqual(pilot.stable_id_from_main_row(main_rows[0][1]), "free-69f7af3e3812c17f")

    def test_legacy_created_at_timestamp_is_not_treated_as_pilot_id(self):
        row = {"created_at": "2026-08-01T14:14:49-04:00"}

        self.assertEqual(pilot.stable_id_from_main_row(row), "")

    def test_review_audit_benchmarks_suffix_against_verifier_without_writes(self):
        promoted_row = self.pilot_row(
            first_name="Tyler",
            last_name="Davis Black Label",
            listing_address="3464 St Bart Lane",
            city="Saint Ann",
            state="MO",
            first_seen_at="2026-08-01T07:20:00-04:00",
            synthetic_zpid="free-tyler",
            promotion_status="promoted",
        )
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [
                    ["First Name", "Last Name", "Listing Address", "State", "ZPID"],
                    ["Tyler", "Davis", "3464 St Bart Ln", "MO", "free-tyler"],
                ],
                [pilot.PILOT_HEADERS, promoted_row],
            ],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 1),
                phase="post_verifier",
            )

        self.assertEqual(stats["linked"], 1)
        self.assertEqual(stats["held"], 0)
        self.assertEqual(stats["suffix_candidates"], 1)
        self.assertEqual(stats["suffix_exact_matches"], 1)
        suffix_events = [details for event, details in events if event == "pilot_brokerage_suffix_shadow"]
        self.assertEqual(suffix_events[0]["proposed_agent"], "Tyler Davis")
        self.assertTrue(suffix_events[0]["exact_name_agreement"])
        self.assertEqual(suffix_events[0]["writes"], 0)

    def test_agent_address_shadow_strips_terminal_feed_artifact_without_guessing_address(self):
        pilot_row = {
            "first_name": "Jennifer",
            "last_name": "Beaudreau of",
            "listing_address": "5564 Spur",
            "city": "Hemet",
            "state": "CA",
            "zip": "92545",
        }
        main_row = {
            "agent_name": "Jennifer Beaudreau",
            "listing_address": "5564 Spur Dr",
        }

        result = pilot.agent_address_normalization_shadow(pilot_row, main_row)

        self.assertEqual(result["proposed_agent"], "Jennifer Beaudreau")
        self.assertEqual(result["agent_proposal_reason"], "terminal_feed_artifact")
        self.assertTrue(result["agent_exact_match"])
        self.assertFalse(result["address_exact_match"])
        self.assertFalse(result["wrong_person_stop"])
        self.assertEqual(result["writes"], 0)

    def test_agent_address_shadow_combines_sheet1_agent_and_last_name_headers(self):
        pilot_row = {
            "first_name": "Angelica",
            "last_name": "Gallego of",
            "listing_address": "8926 W El Caminito Drive",
            "city": "Peoria",
            "state": "AZ",
            "zip": "85345",
        }
        main_row = {
            "agent_name": "Angelica",
            "last_name": "Gallego",
            "listing_address": "8926 W El Caminito Drive",
        }

        result = pilot.agent_address_normalization_shadow(pilot_row, main_row)

        self.assertEqual(result["verifier_agent"], "Angelica Gallego")
        self.assertEqual(result["proposed_agent"], "Angelica Gallego")
        self.assertTrue(result["exact_agent_address_agreement"])
        self.assertFalse(result["wrong_person_stop"])

    def test_agent_address_shadow_refuses_leading_site_or_team_article(self):
        pilot_row = {
            "first_name": "The",
            "last_name": "David Hakimi",
            "listing_address": "1519 W 55th St",
            "city": "Los Angeles",
            "state": "CA",
            "zip": "90062",
        }
        main_row = {
            "agent_name": "Patricia Castro",
            "listing_address": "1519 W 55th Street",
        }

        result = pilot.agent_address_normalization_shadow(pilot_row, main_row)

        self.assertEqual(result["proposed_agent"], "")
        self.assertEqual(result["agent_proposal_reason"], "leading_site_or_team_article")
        self.assertFalse(result["agent_exact_match"])
        self.assertTrue(result["address_exact_match"])
        self.assertFalse(result["wrong_person_stop"])

    def test_agent_address_shadow_uses_only_safe_stored_street_extension(self):
        pilot_row = {
            "first_name": "Jennifer",
            "last_name": "Beaudreau",
            "listing_address": "5564 Spur",
            "city": "Hemet",
            "state": "CA",
            "zip": "92545",
            "pending_queue_listing_json": json.dumps(
                {"street": "5564 Spur Dr", "city": "Hemet", "state": "CA"}
            ),
        }
        main_row = {
            "agent_name": "Jennifer Beaudreau",
            "listing_address": "5564 Spur Drive",
        }

        result = pilot.agent_address_normalization_shadow(pilot_row, main_row)

        self.assertEqual(result["proposed_address"], "5564 Spur Dr")
        self.assertEqual(result["address_proposal_reason"], "stored_structured_extension")
        self.assertTrue(result["address_exact_match"])
        self.assertTrue(result["exact_agent_address_agreement"])

    def test_agent_address_review_shadow_caps_first_ten_and_never_writes(self):
        pilot_rows = [pilot.PILOT_HEADERS]
        main_rows = [["Agent Name", "Listing Address", "State", "ZPID"]]
        for index in range(11):
            stable_id = f"free-shadow-{index:02d}"
            address = f"{index + 1} Main Street"
            pilot_rows.append(
                self.pilot_row(
                    first_name="Jane",
                    last_name="Smith",
                    listing_address=address,
                    city="Atlanta",
                    state="GA",
                    first_seen_at="2026-08-12T07:15:00-04:00",
                    synthetic_zpid=stable_id,
                    source="idx_broker_remarks",
                    status="qualified",
                    promotion_status="promoted",
                )
            )
            verifier_address = "1 Main Street Drive" if index == 0 else address
            main_rows.append(["Jane Smith", verifier_address, "GA", stable_id])
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[main_rows, pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 12),
                phase="post_verifier",
            )

        shadow_events = [
            details
            for event, details in events
            if event == "pilot_agent_address_normalization_shadow"
        ]
        self.assertEqual(stats["agent_address_shadow_eligible"], 10)
        self.assertEqual(stats["agent_address_shadow_reviewable"], 10)
        self.assertEqual(stats["agent_address_shadow_exact"], 9)
        self.assertEqual(stats["agent_address_shadow_supported"], 1)
        self.assertEqual(len(shadow_events), 10)
        self.assertEqual(shadow_events[0]["linkage"], "stable_id_only")
        self.assertFalse(shadow_events[0]["address_exact_match"])
        self.assertTrue(shadow_events[-1]["sample_complete"])
        self.assertTrue(all(event["writes"] == 0 for event in shadow_events))
        self.assertTrue(all(not event["promotion_changed"] for event in shadow_events))
        self.assertTrue(all(not event["outreach_changed"] for event in shadow_events))

    def test_agent_address_review_shadow_stops_on_first_wrong_person(self):
        pilot_rows = [
            pilot.PILOT_HEADERS,
            self.pilot_row(
                first_name="John",
                last_name="Smith",
                listing_address="1 Main Street",
                city="Atlanta",
                state="GA",
                first_seen_at="2026-08-12T07:15:00-04:00",
                synthetic_zpid="free-wrong-person",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
            ),
            self.pilot_row(
                first_name="Jane",
                last_name="Smith",
                listing_address="2 Main Street",
                city="Atlanta",
                state="GA",
                first_seen_at="2026-08-12T07:16:00-04:00",
                synthetic_zpid="free-after-stop",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
            ),
        ]
        main_rows = [
            ["Agent Name", "Listing Address", "State", "ZPID"],
            ["Jane Smith", "1 Main Street", "GA", "free-wrong-person"],
            ["Jane Smith", "2 Main Street", "GA", "free-after-stop"],
        ]
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[main_rows, pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 12),
                phase="post_verifier",
            )

        shadow_events = [
            details
            for event, details in events
            if event == "pilot_agent_address_normalization_shadow"
        ]
        self.assertEqual(stats["agent_address_shadow_evaluated"], 1)
        self.assertEqual(stats["agent_address_shadow_wrong_person"], 1)
        self.assertEqual(stats["agent_address_shadow_stopped"], 1)
        self.assertEqual(len(shadow_events), 1)
        self.assertTrue(shadow_events[0]["wrong_person_stop"])
        self.assertTrue(shadow_events[0]["experiment_stopped"])

    def test_review_experiment_windows_are_bounded(self):
        self.assertTrue(pilot.experiment_active(dt.date(2026, 8, 1), "2026-08-01", 3))
        self.assertTrue(pilot.experiment_active(dt.date(2026, 8, 3), "2026-08-01", 3))
        self.assertFalse(pilot.experiment_active(dt.date(2026, 8, 4), "2026-08-01", 3))

    def test_query_exclusion_split_keeps_five_of_fifty_as_baseline(self):
        states = list(pilot.STATE_QUERY_TERMS)
        baselines = {
            source: pilot.query_exclusion_baseline_states(states, source)
            for source in pilot.DEFAULT_DAILY_SOURCE_BUCKETS
        }

        for source in pilot.DEFAULT_DAILY_SOURCE_BUCKETS:
            self.assertEqual(len(baselines[source]), 5)
            arms = [
                pilot.query_exclusion_arm(
                    dt.date(2026, 8, 15), state, source, baselines
                )
                for state in states
            ]
            self.assertEqual(arms.count("baseline"), 5)
            self.assertEqual(arms.count("excluded"), 45)

        excluded_query = pilot.query_with_exclusion_experiment(
            pilot.ALL_SOURCE_QUERY_MAP["idx_broker_remarks"],
            "Minnesota",
            "excluded",
        )
        self.assertIn("-site:edinarealty.com", excluded_query)
        self.assertIn("-site:ikeyrealty.com", excluded_query)
        baseline_query = pilot.query_with_exclusion_experiment(
            pilot.ALL_SOURCE_QUERY_MAP["idx_broker_remarks"],
            "Minnesota",
            "baseline",
        )
        self.assertNotIn("-site:edinarealty.com", baseline_query)

    def test_canonical_listing_identifier_matches_pilot_and_verifier_evidence(self):
        pilot_row = {
            "raw_title": "22 Davidson Street (#MDAL2015430)",
        }
        verifier_row = {
            "contact_verification_note": (
                "Bright IDX verifies this active short-sale as MLS MDAL2015430."
            ),
        }

        self.assertEqual(
            pilot.canonical_listing_identifier(pilot_row),
            "MDAL2015430",
        )
        self.assertEqual(
            pilot.canonical_listing_identifier(verifier_row),
            "MDAL2015430",
        )

    def test_source_durability_collects_next_ten_and_natural_alternate(self):
        state = {"version": 1, "candidates": [], "stopped": False}
        captured_at = dt.datetime(2026, 8, 20, 12, 0, tzinfo=dt.timezone.utc)
        for index in range(11):
            candidate = pilot.Candidate(
                source="idx_broker_remarks",
                query="query",
                url=f"https://broker.example/listing-{index}",
                title="",
                text="",
                fields={
                    "listing_address": f"{index + 1} Main St",
                    "city": "Atlanta",
                    "state": "GA",
                },
            )
            pilot.observe_source_durability_candidate(
                state,
                candidate,
                captured_at=captured_at,
                primary_eligible=True,
            )

        alternate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://alternate.example/1-main-st",
            title="",
            text="",
            fields={
                "listing_address": "1 Main Street",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        changed = pilot.observe_source_durability_candidate(
            state,
            alternate,
            captured_at=captured_at,
            primary_eligible=False,
        )

        self.assertEqual(len(state["candidates"]), 10)
        self.assertTrue(changed)
        self.assertEqual(
            state["candidates"][0]["alternate_url"],
            "https://alternate.example/1-main-st",
        )

    def test_source_durability_audit_rechecks_mature_exact_and_alternate_urls(self):
        captured_at = dt.datetime(2026, 8, 20, 12, 0, tzinfo=dt.timezone.utc)
        now = captured_at + dt.timedelta(hours=25)
        state = {"version": 1, "candidates": [], "stopped": False, "stop_reason": ""}
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/1-main-st",
            title="",
            text="",
            fields={
                "listing_address": "1 Main St",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        pilot.observe_source_durability_candidate(
            state,
            candidate,
            captured_at=captured_at,
            primary_eligible=True,
        )
        alternate = pilot.Candidate(
            source="idx_broker_pages",
            query="query",
            url="https://alternate.example/1-main-st",
            title="",
            text="",
            fields={
                "listing_address": "1 Main Street",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        pilot.observe_source_durability_candidate(
            state,
            alternate,
            captured_at=captured_at,
            primary_eligible=False,
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product","name":"1 Main Street, Atlanta, GA",
           "address":{"streetAddress":"1 Main Street","addressLocality":"Atlanta","addressRegion":"GA"},
           "description":"This is a short sale subject to lender approval.",
           "listingStatus":"Active"}
        </script>
        """
        with tempfile.TemporaryDirectory() as directory:
            state_path = os.path.join(directory, "audit.json")
            pilot.save_source_durability_state(state, state_path)
            events = []
            with mock.patch.object(pilot, "fetch_url", return_value=markup), \
                 mock.patch.object(
                     pilot,
                     "log_event",
                     side_effect=lambda event, **details: events.append((event, details)),
                 ):
                stats = pilot.run_source_durability_audit(
                    run_date=dt.date(2026, 8, 21),
                    force=True,
                    now=now,
                    state_path=state_path,
                )

            reread = pilot.load_source_durability_state(state_path)

        self.assertEqual(stats["evaluated"], 1)
        self.assertEqual(stats["primary_reviewable"], 1)
        self.assertEqual(stats["alternate_observed"], 1)
        self.assertEqual(stats["alternate_reviewable"], 1)
        self.assertTrue(reread["candidates"][0]["evaluated_at"])
        audit_event = next(
            details for event, details in events
            if event == "pilot_source_durability_audit"
        )
        self.assertEqual(audit_event["lead_data_writes"], 0)
        self.assertEqual(audit_event["searches"], 0)
        self.assertEqual(audit_event["sends"], 0)

    def test_source_durability_audit_stops_on_first_access_control_concern(self):
        captured_at = dt.datetime(2026, 8, 20, 12, 0, tzinfo=dt.timezone.utc)
        state = {"version": 1, "candidates": [], "stopped": False, "stop_reason": ""}
        candidate = pilot.Candidate(
            source="idx_broker_remarks",
            query="query",
            url="https://broker.example/blocked",
            title="",
            text="",
            fields={
                "listing_address": "1 Main St",
                "city": "Atlanta",
                "state": "GA",
            },
        )
        pilot.observe_source_durability_candidate(
            state,
            candidate,
            captured_at=captured_at,
            primary_eligible=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            state_path = os.path.join(directory, "audit.json")
            pilot.save_source_durability_state(state, state_path)
            error = urllib.error.HTTPError(
                candidate.url,
                403,
                "Forbidden",
                hdrs=None,
                fp=None,
            )
            with mock.patch.object(pilot, "fetch_url", side_effect=error):
                stats = pilot.run_source_durability_audit(
                    run_date=dt.date(2026, 8, 21),
                    force=True,
                    now=captured_at + dt.timedelta(hours=25),
                    state_path=state_path,
                )
            reread = pilot.load_source_durability_state(state_path)

        self.assertEqual(stats["access_control_concerns"], 1)
        self.assertEqual(stats["stopped"], 1)
        self.assertTrue(reread["stopped"])
        self.assertEqual(reread["stop_reason"], "first_access_control_concern")

    def test_source_durability_state_save_failure_is_nonfatal_and_observable(self):
        events = []
        with mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ), mock.patch("free_short_sale_source_pilot.os.makedirs", side_effect=PermissionError("denied")):
            saved = pilot.save_source_durability_state(
                {"version": 1, "candidates": []},
                "/unwritable/audit.json",
            )

        self.assertFalse(saved)
        event, details = events[-1]
        self.assertEqual(event, "pilot_source_durability_state_unconfirmed")
        self.assertEqual(details["reason"], "state_write_failed")
        self.assertEqual(details["lead_data_writes"], 0)
        self.assertEqual(details["searches"], 0)
        self.assertEqual(details["sends"], 0)

    def test_durability_persistence_failure_degrades_pipeline_completion(self):
        stats = {
            "planned_searches": 1,
            "searched": 1,
            "search_succeeded": 1,
            "search_blocked": 0,
            "search_failed": 0,
            "search_engine_attempts": {"attempted": 1, "succeeded": 1, "blocked": 0, "failed": 0},
        }

        complete, accounted = pilot.source_pipeline_complete(
            stats,
            {"errors": 0},
            {"complete": True},
            durability_persistence_confirmed=False,
        )

        self.assertTrue(accounted)
        self.assertFalse(complete)

    def test_durable_schedule_slot_skips_an_already_completed_run(self):
        rows = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-21", "old", "2026-08-21", "scheduled_source",
             "completed", "2026-08-21T13:10:00+00:00", "true", ""],
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(pilot, "get_values", return_value=rows), \
             mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-21",
                run_receipt_id="new", run_date=dt.date(2026, 8, 21),
                run_mode="scheduled_source",
            )

        self.assertFalse(claimed)
        self.assertEqual(reason, "already_completed")
        self.assertEqual(recovery_keys, [])
        append.assert_not_called()

    def test_durable_schedule_slot_concurrent_attempts_choose_one_winner(self):
        now = dt.datetime(2026, 8, 21, 13, 0, 0, tzinfo=dt.timezone.utc)
        after_append = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-21", "winner", "2026-08-21", "scheduled_source",
             "running", "2026-08-21T13:00:05+00:00", "false", ""],
            ["source:2026-08-21", "loser", "2026-08-21", "scheduled_source",
             "running", now.isoformat(), "false", ""],
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(
                 pilot, "get_values", side_effect=[[pilot.RUN_RECEIPT_HEADERS], after_append]
             ), mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-21",
                run_receipt_id="loser", run_date=dt.date(2026, 8, 21),
                run_mode="scheduled_source", now=now,
            )

        self.assertFalse(claimed)
        self.assertEqual(reason, "active_attempt")
        self.assertEqual(recovery_keys, [])
        self.assertEqual(append.call_count, 2)

    def test_durable_schedule_slot_allows_retry_after_failed_attempt(self):
        now = dt.datetime(2026, 8, 21, 13, 30, 0, tzinfo=dt.timezone.utc)
        after_append = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-21", "failed-old", "2026-08-21", "scheduled_source",
             "running", "2026-08-21T13:00:00+00:00", "false", ""],
            ["source:2026-08-21", "failed-old", "2026-08-21", "scheduled_source",
             "failed", "2026-08-21T13:05:00+00:00", "false", "error"],
            ["source:2026-08-21", "retry", "2026-08-21", "scheduled_source",
             "running", now.isoformat(), "false", ""],
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(
                 pilot, "get_values", side_effect=[after_append[:-1], after_append]
             ), mock.patch.object(pilot, "append_run_slot_receipt"):
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-21",
                run_receipt_id="retry", run_date=dt.date(2026, 8, 21),
                run_mode="scheduled_source", now=now,
            )

        self.assertTrue(claimed)
        self.assertEqual(reason, "claimed")
        self.assertEqual(recovery_keys, [])

    def test_durable_schedule_slot_claims_only_bounded_missing_query_manifest(self):
        now = dt.datetime(2026, 8, 21, 15, 30, 0, tzinfo=dt.timezone.utc)
        pending_detail = pilot.recovery_manifest_detail(
            pilot.RECOVERY_PENDING_PREFIX,
            ["WI:idx_broker_pages", "WI:idx_broker_remarks", "WY:idx_broker_pages", "WY:idx_broker_remarks"],
        )
        prior = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-21", "primary", "2026-08-21", "scheduled_source",
             "completed_degraded", "2026-08-21T13:10:00+00:00", "false", pending_detail],
        ]
        after_append = prior + [[
            "source:2026-08-21", "recovery", "2026-08-21", "scheduled_source",
            "running", now.isoformat(), "false", "",
        ]]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(pilot, "get_values", side_effect=[prior, after_append]), \
             mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-21",
                run_receipt_id="recovery", run_date=dt.date(2026, 8, 21),
                run_mode="scheduled_source", now=now,
            )

        self.assertTrue(claimed)
        self.assertEqual(reason, "claimed_recovery")
        self.assertEqual(
            recovery_keys,
            ["WI:idx_broker_pages", "WI:idx_broker_remarks", "WY:idx_broker_pages", "WY:idx_broker_remarks"],
        )
        self.assertEqual(append.call_args.kwargs["detail"], pilot.recovery_manifest_detail(
            pilot.RECOVERY_ATTEMPT_PREFIX, recovery_keys
        ))

    def test_durable_degraded_slot_without_manifest_never_replays_full_plan(self):
        rows = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-21", "primary", "2026-08-21", "scheduled_source",
             "completed_degraded", "2026-08-21T13:10:00+00:00", "false", "pipeline completion gate failed"],
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(pilot, "get_values", return_value=rows), \
             mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-21",
                run_receipt_id="startup", run_date=dt.date(2026, 8, 21),
                run_mode="scheduled_source",
            )

        self.assertFalse(claimed)
        self.assertEqual(reason, "recovery_unavailable")
        self.assertEqual(recovery_keys, [])
        append.assert_not_called()

    def test_recovery_manifest_rejects_more_than_bounded_cap(self):
        detail = pilot.recovery_manifest_detail(
            pilot.RECOVERY_PENDING_PREFIX,
            [f"TX:bucket_{index}" for index in range(pilot.MAX_SOURCE_QUERY_RECOVERY + 1)],
        )

        self.assertEqual(pilot.parse_recovery_query_keys(detail), [])

    def test_recovery_cap_experiment_accepts_20_queries_for_seven_scheduled_days(self):
        detail = pilot.recovery_manifest_detail(
            pilot.RECOVERY_PENDING_PREFIX,
            [f"TX:bucket_{index}" for index in range(20)],
        )

        for run_date in (dt.date(2026, 8, 23), dt.date(2026, 8, 29)):
            limit = pilot.source_query_recovery_limit(run_date)
            self.assertEqual(limit, 20)
            self.assertEqual(
                len(pilot.parse_recovery_query_keys(detail, max_recovery_queries=limit)),
                20,
            )

    def test_recovery_cap_experiment_rejects_21_and_expires_after_seven_days(self):
        detail = pilot.recovery_manifest_detail(
            pilot.RECOVERY_PENDING_PREFIX,
            [f"TX:bucket_{index}" for index in range(21)],
        )

        experiment_limit = pilot.source_query_recovery_limit(dt.date(2026, 8, 23))
        self.assertEqual(
            pilot.parse_recovery_query_keys(detail, max_recovery_queries=experiment_limit),
            [],
        )
        self.assertEqual(pilot.source_query_recovery_limit(dt.date(2026, 8, 22)), 10)
        self.assertEqual(pilot.source_query_recovery_limit(dt.date(2026, 8, 30)), 10)

    def test_durable_schedule_slot_claims_20_query_experiment_manifest(self):
        run_date = dt.date(2026, 8, 23)
        now = dt.datetime(2026, 8, 23, 15, 30, 0, tzinfo=dt.timezone.utc)
        recovery_keys = [f"TX:bucket_{index}" for index in range(20)]
        pending_detail = pilot.recovery_manifest_detail(
            pilot.RECOVERY_PENDING_PREFIX,
            recovery_keys,
        )
        prior = [
            pilot.RUN_RECEIPT_HEADERS,
            ["source:2026-08-23", "primary", "2026-08-23", "scheduled_source",
             "completed_degraded", "2026-08-23T13:10:00+00:00", "false", pending_detail],
        ]
        after_append = prior + [[
            "source:2026-08-23", "recovery", "2026-08-23", "scheduled_source",
            "running", now.isoformat(), "false", "",
        ]]

        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(pilot, "get_values", side_effect=[prior, after_append]), \
             mock.patch.object(pilot, "append_run_slot_receipt"):
            claimed, reason, claimed_keys = pilot.claim_run_schedule_slot(
                "token", "sheet", schedule_slot_id="source:2026-08-23",
                run_receipt_id="recovery", run_date=run_date,
                run_mode="scheduled_source", now=now,
            )

        self.assertTrue(claimed)
        self.assertEqual(reason, "claimed_recovery")
        self.assertEqual(claimed_keys, sorted(recovery_keys))

    def test_recovery_run_attempts_only_manifest_queries_and_skips_direct_monitor(self):
        recovery_keys = [
            "WI:idx_broker_pages", "WI:idx_broker_remarks",
            "WY:idx_broker_pages", "WY:idx_broker_remarks",
        ]
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=False, audit_phase="post_verifier", force_review_experiments=False,
            states=["WI", "WY", "TX"], results_per_query=10, sleep_seconds=0,
            include_rejected=False, dry_run=False, promote_ready=False,
            promotion_daily_cap=10, promotion_dry_run=False, scheduled_run=True,
            run_receipt_id="recovery-1", schedule_slot_id="source:2026-08-21",
        )
        searches = []
        persisted = []
        events = []

        def fake_search(query, source, limit, date_restrict=None):
            searches.append((query, source, limit, date_restrict))
            return "cse", []

        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "ensure_tab"), \
             mock.patch.object(pilot, "source_durability_audit_active", return_value=True), \
             mock.patch.object(
                 pilot, "configured_source_queries",
                 return_value=[
                     pilot.SourceQuery("idx_broker_pages", 'pages {state}', "w1"),
                     pilot.SourceQuery("idx_broker_remarks", 'remarks {state}', "w1"),
                 ],
             ), mock.patch.object(
                 pilot, "claim_run_schedule_slot",
                 return_value=(True, "claimed_recovery", recovery_keys),
             ), mock.patch.object(
                 pilot, "get_values", side_effect=[[[]], [pilot.PILOT_HEADERS]],
             ), mock.patch.object(pilot, "search_web", side_effect=fake_search), \
             mock.patch.object(
                 pilot, "run_direct_monitor",
                 side_effect=AssertionError("bounded recovery must skip direct monitor"),
             ), mock.patch.object(
                 pilot, "persist_run_slot_terminal",
                 side_effect=lambda *args, **kwargs: persisted.append(kwargs) or True,
             ), mock.patch.object(
                 pilot, "log_event",
                 side_effect=lambda event, **details: events.append((event, details)),
             ):
            pilot.run(args)

        self.assertEqual(len(searches), 4)
        self.assertEqual({source for _, source, _, _ in searches}, {"idx_broker_pages", "idx_broker_remarks"})
        self.assertTrue(all("Texas" not in query for query, _, _, _ in searches))
        self.assertEqual(persisted[-1]["status"], "completed")
        self.assertEqual(
            persisted[-1]["detail"],
            pilot.recovery_manifest_detail(pilot.RECOVERY_COMPLETED_PREFIX, recovery_keys),
        )
        done = next(details for event, details in events if event == "pilot_run_done")
        self.assertEqual(done["stats"]["planned_searches"], 4)
        self.assertEqual(done["stats"]["searched"], 4)
        self.assertTrue(done["scheduled_run_proven_complete"])

    def test_audit_schedule_slot_waits_for_completed_source_slot(self):
        rows = [pilot.RUN_RECEIPT_HEADERS]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(pilot, "get_values", return_value=rows), \
             mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet",
                schedule_slot_id="post_verifier_audit:2026-08-21",
                run_receipt_id="audit-1",
                run_date=dt.date(2026, 8, 21),
                run_mode="link_audit",
            )

        self.assertFalse(claimed)
        self.assertEqual(reason, "source_slot_not_completed")
        self.assertEqual(recovery_keys, [])
        append.assert_not_called()

    def test_audit_schedule_slot_claims_after_completed_source_slot(self):
        source_row = [
            "source:2026-08-21", "source-1", "2026-08-21", "scheduled_source",
            "completed", "2026-08-21T13:20:00+00:00", "true", "",
        ]
        after_append = [
            pilot.RUN_RECEIPT_HEADERS,
            source_row,
            ["post_verifier_audit:2026-08-21", "audit-1", "2026-08-21", "link_audit",
             "running", "2026-08-21T14:05:00+00:00", "false", ""],
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(
                 pilot,
                 "get_values",
                 side_effect=[[pilot.RUN_RECEIPT_HEADERS, source_row], after_append],
             ), mock.patch.object(pilot, "append_run_slot_receipt"):
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet",
                schedule_slot_id="post_verifier_audit:2026-08-21",
                run_receipt_id="audit-1",
                run_date=dt.date(2026, 8, 21),
                run_mode="link_audit",
                now=dt.datetime(2026, 8, 21, 14, 5, tzinfo=dt.timezone.utc),
            )

        self.assertTrue(claimed)
        self.assertEqual(reason, "claimed")
        self.assertEqual(recovery_keys, [])

    def test_audit_schedule_slot_enforces_thirty_minute_source_grace(self):
        source_row = [
            "source:2026-08-21", "source-1", "2026-08-21", "scheduled_source",
            "completed", "2026-08-21T13:50:00+00:00", "true", "",
        ]
        with mock.patch.object(pilot, "ensure_headers_tab"), \
             mock.patch.object(
                 pilot, "get_values", return_value=[pilot.RUN_RECEIPT_HEADERS, source_row]
             ), mock.patch.object(pilot, "append_run_slot_receipt") as append:
            claimed, reason, recovery_keys = pilot.claim_run_schedule_slot(
                "token", "sheet",
                schedule_slot_id="post_verifier_audit:2026-08-21",
                run_receipt_id="audit-too-early",
                run_date=dt.date(2026, 8, 21),
                run_mode="link_audit",
                now=dt.datetime(2026, 8, 21, 14, 5, tzinfo=dt.timezone.utc),
            )

        self.assertFalse(claimed)
        self.assertEqual(reason, "source_grace_not_elapsed")
        self.assertEqual(recovery_keys, [])
        append.assert_not_called()

    def test_receipt_tab_collision_fails_before_auth_or_sheet_mutation(self):
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=True, audit_phase="post_verifier",
            force_review_experiments=False, scheduled_run=True,
            run_receipt_id="audit-collision", schedule_slot_id="post_verifier_audit:2026-08-21",
        )
        with mock.patch.object(pilot, "RUN_RECEIPT_TAB", "Sheet1"), \
             mock.patch.object(pilot, "load_service_account_info") as load_auth, \
             mock.patch.object(pilot, "ensure_headers_tab") as ensure_headers:
            with self.assertRaisesRegex(RuntimeError, "dedicated tab"):
                pilot.run(args)

        load_auth.assert_not_called()
        ensure_headers.assert_not_called()

    def test_audit_only_run_marks_no_planned_checks_degraded(self):
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=True, audit_phase="post_verifier",
            force_review_experiments=False, scheduled_run=True,
            run_receipt_id="audit-empty", schedule_slot_id="post_verifier_audit:2026-08-21",
        )
        persisted = []
        events = []
        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "claim_run_schedule_slot", return_value=(True, "claimed", [])), \
             mock.patch.object(
                 pilot, "run_linkage_and_suffix_audits",
                 return_value={"audit_checks_planned": 0, "unconfirmed": 0},
             ), mock.patch.object(
                 pilot, "persist_run_slot_terminal",
                 side_effect=lambda *args, **kwargs: persisted.append(kwargs) or True,
             ), mock.patch.object(
                 pilot, "log_event", side_effect=lambda event, **details: events.append((event, details)),
             ):
            pilot.run(args)

        self.assertEqual(persisted[0]["status"], "completed_degraded")
        self.assertFalse(persisted[0]["pipeline_complete"])
        done = next(details for event, details in events if event == "pilot_run_done")
        self.assertFalse(done["pipeline_complete"])
        self.assertEqual(done["audit_stats"]["audit_checks_planned"], 0)

    def test_source_durability_audit_marks_missing_state_unconfirmed(self):
        events = []
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_source_durability_audit(
                run_date=dt.date(2026, 8, 21),
                force=True,
                state_path=os.path.join(directory, "missing.json"),
            )

        self.assertEqual(stats["state_unconfirmed"], 1)
        missing_event = next(
            details
            for event, details in events
            if event == "pilot_source_durability_state_unconfirmed"
        )
        self.assertEqual(missing_event["reason"], "state_missing")
        done_event = next(
            details
            for event, details in events
            if event == "pilot_source_durability_audit_done"
        )
        self.assertEqual(done_event["lead_data_writes"], 0)
        self.assertEqual(done_event["searches"], 0)
        self.assertEqual(done_event["sends"], 0)

    def test_source_durability_state_read_failure_is_unconfirmed(self):
        with mock.patch("free_short_sale_source_pilot.os.path.exists", return_value=True), \
             mock.patch("builtins.open", side_effect=OSError("read failed")):
            state = pilot.load_source_durability_state("/tmp/corrupt-state.json")

        self.assertTrue(state["state_unconfirmed"])

    def test_source_durability_state_schema_failure_is_unconfirmed(self):
        with mock.patch("free_short_sale_source_pilot.os.path.exists", return_value=True), \
             mock.patch("builtins.open", mock.mock_open(read_data='{"version":1}')):
            state = pilot.load_source_durability_state("/tmp/corrupt-state.json")

        self.assertTrue(state["state_unconfirmed"])

    def test_source_durability_state_save_failure_is_returned_in_stats(self):
        with mock.patch.object(
            pilot,
            "load_source_durability_state",
            return_value={"version": 1, "candidates": [], "stopped": False},
        ), mock.patch.object(pilot, "save_source_durability_state", return_value=False):
            stats = pilot.run_source_durability_audit(
                run_date=dt.date(2026, 8, 21),
                force=True,
                state_path="/tmp/state.json",
            )

        self.assertEqual(stats["state_persistence_confirmed"], 0)

    def test_link_audit_propagates_missing_durability_state_to_completion_gate(self):
        main_rows = [["Agent Name", "Listing Address", "State", "created-at"]]
        pilot_rows = [pilot.PILOT_HEADERS]
        with mock.patch.object(
            pilot, "get_values", side_effect=[main_rows, pilot_rows]
        ), mock.patch.object(
            pilot,
            "run_source_durability_audit",
            return_value={"state_unconfirmed": 1, "stopped": 0},
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token", "sheet-id", "Sheet1", "Lead Source Pilot",
                run_date=dt.date(2026, 8, 21), phase="post_verifier",
            )

        self.assertGreater(stats["audit_checks_planned"], 0)
        self.assertEqual(stats["source_durability_state_unconfirmed"], 1)
        self.assertEqual(stats["audit_evidence_unconfirmed"], 1)

    def test_link_audit_propagates_durability_save_failure_to_completion_gate(self):
        main_rows = [["Agent Name", "Listing Address", "State", "created-at"]]
        pilot_rows = [pilot.PILOT_HEADERS]
        with mock.patch.object(
            pilot, "get_values", side_effect=[main_rows, pilot_rows]
        ), mock.patch.object(
            pilot,
            "run_source_durability_audit",
            return_value={
                "state_unconfirmed": 0,
                "state_persistence_confirmed": 0,
                "stopped": 0,
            },
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token", "sheet-id", "Sheet1", "Lead Source Pilot",
                run_date=dt.date(2026, 8, 21), phase="post_verifier",
            )

        self.assertEqual(stats["source_durability_state_persistence_confirmed"], 0)
        self.assertEqual(stats["audit_evidence_unconfirmed"], 1)

    def test_canonical_id_audit_reads_named_verifier_evidence_from_sheet1_z(self):
        pilot_rows = [
            pilot.PILOT_HEADERS,
            self.pilot_row(
                listing_address="1 Main Street",
                state="GA",
                first_seen_at="2026-08-14T07:15:00-04:00",
                synthetic_zpid="free-canonical01",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
                raw_title="1 Main Street (#GA1234567)",
            ),
        ]
        main_headers = [""] * 29
        main_headers[0] = "Agent Name"
        main_headers[4] = "Listing Address"
        main_headers[6] = "State"
        main_headers[25] = pilot.CANONICAL_VERIFIER_EVIDENCE_HEADER
        main_headers[27] = "created-at"
        main_row = [""] * 29
        main_row[0] = "One Agent"
        main_row[4] = "1 Main Street"
        main_row[6] = "GA"
        main_row[25] = "Verifier confirms MLS GA1234567."
        main_row[27] = "free-canonical01"
        events = []

        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[[main_headers, main_row], pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 14),
                phase="post_verifier",
            )

        audit_event = next(
            details
            for event, details in events
            if event == "pilot_canonical_listing_id_audit"
        )
        self.assertEqual(stats["canonical_id_reviewable"], 1)
        self.assertEqual(stats["canonical_id_exact"], 1)
        self.assertTrue(audit_event["verifier_evidence_header_present"])
        self.assertTrue(audit_event["verifier_evidence_hash_only"])
        self.assertNotIn("verifier_identifier", audit_event)

    def test_canonical_id_audit_logs_only_hashes_and_stops_on_first_mismatch(self):
        pilot_rows = [
            pilot.PILOT_HEADERS,
            self.pilot_row(
                listing_address="1 Main Street",
                state="GA",
                first_seen_at="2026-08-14T07:15:00-04:00",
                synthetic_zpid="free-canonical01",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
                raw_title="1 Main Street (#GA1234567)",
            ),
            self.pilot_row(
                listing_address="2 Main Street",
                state="GA",
                first_seen_at="2026-08-14T07:16:00-04:00",
                synthetic_zpid="free-canonical02",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
                raw_title="2 Main Street (#GA2222222)",
            ),
            self.pilot_row(
                listing_address="3 Main Street",
                state="GA",
                first_seen_at="2026-08-14T07:17:00-04:00",
                synthetic_zpid="free-canonical03",
                source="idx_broker_remarks",
                status="qualified",
                promotion_status="promoted",
                raw_title="3 Main Street (#GA3333333)",
            ),
        ]
        main_rows = [
            ["Agent Name", "Listing Address", "State", "created-at", "contact_verification_note"],
            ["One Agent", "1 Main Street", "GA", "free-canonical01", "MLS GA1234567"],
            ["Two Agent", "2 Main Street", "GA", "free-canonical02", "MLS GA9999999"],
            ["Three Agent", "3 Main Street", "GA", "free-canonical03", "MLS GA3333333"],
        ]
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[main_rows, pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 14),
                phase="post_verifier",
            )

        audit_events = [
            details for event, details in events
            if event == "pilot_canonical_listing_id_audit"
        ]
        self.assertEqual(stats["canonical_id_evaluated"], 2)
        self.assertEqual(stats["canonical_id_mismatches"], 1)
        self.assertEqual(stats["canonical_id_stopped"], 1)
        self.assertEqual(len(audit_events), 2)
        self.assertTrue(all(event["writes"] == 0 for event in audit_events))
        self.assertTrue(all(not event["raw_identifier_logged"] for event in audit_events))
        self.assertTrue(all(event["verifier_evidence_hash_only"] for event in audit_events))
        self.assertNotIn("pilot_identifier", audit_events[0])

    def test_delivery_receipt_evidence_never_calls_inbound_a_provider_receipt(self):
        evidence = pilot.delivery_receipt_evidence(
            {
                "created_at": "free-delivery-1",
                "phone": "443-454-5322",
                "initial_text_sent": "x",
                "last_contact_time": "2026-08-14T08:08:00-04:00",
                "last_message_id": "14434545322-1786709330257",
                "last_inbound_text": "I already have help",
                "last_inbound_at": "2026-08-14T08:08:50-04:00",
            }
        )

        self.assertEqual(evidence["outcome"], "confirmed_by_inbound")
        self.assertTrue(evidence["definitive"])
        self.assertFalse(evidence["provider_delivery_receipt_present"])
        self.assertEqual(evidence["evidence_basis"], "inbound_message_id")
        self.assertEqual(evidence["writes"], 0)
        self.assertEqual(evidence["sends"], 0)
        self.assertNotEqual(evidence["phone_hash"], "4434545322")

    def test_audit_only_mode_cannot_enter_tab_repair_write_path(self):
        args = types.SimpleNamespace(
            service_account="{}",
            spreadsheet_id="sheet-id",
            main_tab="Sheet1",
            pilot_tab="Lead Source Pilot",
            run_date="2026-08-14",
            audit_links_only=True,
            audit_phase="post_verifier",
            force_review_experiments=False,
        )
        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "ensure_tab") as ensure_tab, \
             mock.patch.object(pilot, "run_linkage_and_suffix_audits") as audit:
            pilot.run(args)

        ensure_tab.assert_not_called()
        audit.assert_called_once()

    def test_run_rejects_navigation_candidate_before_row_write_and_emits_complete_receipt(self):
        args = types.SimpleNamespace(
            service_account="{}",
            spreadsheet_id="sheet-id",
            main_tab="Sheet1",
            pilot_tab="Lead Source Pilot",
            run_date="2026-08-21",
            audit_links_only=False,
            audit_phase="post_verifier",
            force_review_experiments=False,
            states=["TX"],
            results_per_query=10,
            sleep_seconds=0,
            include_rejected=False,
            dry_run=False,
            promote_ready=False,
            promotion_daily_cap=10,
            promotion_dry_run=False,
            scheduled_run=True,
            run_receipt_id="scheduled-receipt-1",
            schedule_slot_id="source:2026-08-21",
        )
        result = pilot.SearchResult(
            source="idx_broker_remarks",
            query="query",
            url="https://www.bishopcountry.com/1704-langford-street",
            title="1704 Langford Street, Greenville, TX 75401",
            snippet="",
        )
        markup = """
        <script type="application/ld+json">
          {"@type":"Product",
           "address":{"streetAddress":"1704 Langford Street","addressLocality":"Greenville",
                      "addressRegion":"TX","postalCode":"75401"},
           "description":"Short Sale Option Choosing Your Real Estate Agent",
           "listingStatus":"Active"}
        </script>
        """
        events = []
        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "ensure_tab"), \
             mock.patch.object(pilot, "source_durability_audit_active", return_value=False), \
             mock.patch.object(
                 pilot,
                 "configured_source_queries",
                 return_value=[pilot.SourceQuery("idx_broker_remarks", '"short sale" {state}', "w1")],
             ), \
             mock.patch.object(
                 pilot,
                 "get_values",
                 side_effect=[
                     [["first_name", "last_name", "listing_address", "state"]],
                     [pilot.PILOT_HEADERS],
                 ],
             ), \
             mock.patch.object(pilot, "search_web", return_value=("cse", [result])), \
             mock.patch.object(pilot, "fetch_url", return_value=markup), \
             mock.patch.object(pilot, "append_values", side_effect=AssertionError("rejected candidate must not write")), \
             mock.patch.object(pilot, "claim_run_schedule_slot", return_value=(True, "claimed", [])), \
             mock.patch.object(pilot, "persist_run_slot_terminal", return_value=True), \
             mock.patch.object(pilot, "run_direct_monitor", return_value={"selected": 0}), \
             mock.patch.object(
                 pilot,
                 "log_event",
                 side_effect=lambda event, **details: events.append((event, details)),
             ):
            pilot.run(args)

        self.assertTrue(any(event == "pilot_site_chrome_prewrite_receipt" for event, _ in events))
        done = next(details for event, details in events if event == "pilot_run_done")
        self.assertTrue(done["scheduled_run_proven_complete"])
        self.assertEqual(done["stats"]["search_succeeded"], 1)
        self.assertEqual(done["stats"]["raw_results"], 1)
        self.assertEqual(done["stats"]["unique_result_urls"], 1)
        self.assertEqual(done["stats"]["unique_listing_urls"], 1)
        self.assertEqual(done["stats"]["fetch_operations"], 1)
        self.assertEqual(done["stats"]["unique_listing_pages_fetched"], 1)
        self.assertEqual(done["stats"]["rows_written"], 0)
        self.assertEqual(done["stats"]["rejected"], 1)

    def test_terminal_receipt_covers_auth_system_exit_with_same_run_id(self):
        args = types.SimpleNamespace(
            service_account="", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=False, audit_phase="post_verifier", force_review_experiments=False,
            states=["TX"], results_per_query=10, sleep_seconds=0, include_rejected=False,
            dry_run=False, promote_ready=False, promotion_daily_cap=10,
            promotion_dry_run=False, scheduled_run=True, run_receipt_id="auth-failure-1",
        )
        events = []
        with mock.patch.object(
            pilot, "configured_source_queries",
            return_value=[pilot.SourceQuery("idx_broker_remarks", '"short sale" {state}', "w1")],
        ), mock.patch.object(
            pilot, "load_service_account_info", side_effect=SystemExit("missing credentials")
        ), mock.patch.object(
            pilot, "log_event", side_effect=lambda event, **details: events.append((event, details))
        ), self.assertRaises(SystemExit):
            pilot.run_with_terminal_receipt(args)

        self.assertEqual(events[0][0], "pilot_run_start")
        failure = next(details for event, details in events if event == "pilot_run_terminal_failure")
        self.assertEqual(failure["run_receipt_id"], "auth-failure-1")
        self.assertEqual(failure["failure_stage"], "authentication")
        self.assertFalse(failure["scheduled_run_proven_complete"])
        self.assertEqual(failure["partial_stats"]["planned_searches"], 1)

    def test_empty_source_plan_emits_terminal_failure_not_done(self):
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=False, audit_phase="post_verifier", force_review_experiments=False,
            states=["TX"], results_per_query=10, sleep_seconds=0, include_rejected=False,
            dry_run=False, promote_ready=False, promotion_daily_cap=10,
            promotion_dry_run=False, scheduled_run=True, run_receipt_id="empty-plan-1",
        )
        events = []
        with mock.patch.object(pilot, "configured_source_queries", return_value=[]), \
             mock.patch.object(pilot, "load_service_account_info") as auth, \
             mock.patch.object(
                 pilot, "log_event", side_effect=lambda event, **details: events.append((event, details))
             ), self.assertRaisesRegex(RuntimeError, "no planned source searches"):
            pilot.run_with_terminal_receipt(args)

        auth.assert_not_called()
        self.assertFalse(any(event == "pilot_run_done" for event, _ in events))
        self.assertEqual(events[-1][0], "pilot_run_terminal_failure")

    def test_search_403_is_blocked_and_prevents_proven_completion(self):
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=False, audit_phase="post_verifier", force_review_experiments=False,
            states=["TX"], results_per_query=10, sleep_seconds=0, include_rejected=False,
            dry_run=False, promote_ready=False, promotion_daily_cap=10,
            promotion_dry_run=False, scheduled_run=True, run_receipt_id="blocked-1",
        )
        events = []
        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "ensure_tab"), \
             mock.patch.object(pilot, "source_durability_audit_active", return_value=False), \
             mock.patch.object(
                 pilot, "configured_source_queries",
                 return_value=[pilot.SourceQuery("idx_broker_remarks", '"short sale" {state}', "w1")],
             ), mock.patch.object(
                 pilot, "get_values", side_effect=[[[]], [pilot.PILOT_HEADERS]]
             ), mock.patch.object(
                 pilot, "search_web", side_effect=RuntimeError("HTTP Error 403: Forbidden")
             ), mock.patch.object(
                 pilot, "run_direct_monitor", return_value={"complete": True, "selected": 0}
             ), mock.patch.object(
                 pilot, "log_event", side_effect=lambda event, **details: events.append((event, details))
             ):
            pilot.run(args)

        done = next(details for event, details in events if event == "pilot_run_done")
        self.assertEqual(done["stats"]["search_blocked"], 1)
        self.assertEqual(done["stats"]["search_failed"], 0)
        self.assertFalse(done["scheduled_run_proven_complete"])

    def test_promotion_error_prevents_proven_completion(self):
        args = types.SimpleNamespace(
            service_account="{}", spreadsheet_id="sheet-id", main_tab="Sheet1",
            pilot_tab="Lead Source Pilot", run_date="2026-08-21",
            audit_links_only=False, audit_phase="post_verifier", force_review_experiments=False,
            states=["TX"], results_per_query=10, sleep_seconds=0, include_rejected=False,
            dry_run=False, promote_ready=True, promotion_daily_cap=10,
            promotion_dry_run=False, scheduled_run=True, run_receipt_id="promotion-error-1",
        )
        events = []
        with mock.patch.object(pilot, "load_service_account_info", return_value={}), \
             mock.patch.object(pilot, "sheets_client", return_value="token"), \
             mock.patch.object(pilot, "ensure_tab"), \
             mock.patch.object(pilot, "source_durability_audit_active", return_value=False), \
             mock.patch.object(
                 pilot, "configured_source_queries",
                 return_value=[pilot.SourceQuery("idx_broker_remarks", '"short sale" {state}', "w1")],
             ), mock.patch.object(
                 pilot, "get_values", side_effect=[[[]], [pilot.PILOT_HEADERS]]
             ), mock.patch.object(pilot, "search_web", return_value=("cse", [])), \
             mock.patch.object(
                 pilot, "promote_ready_pilot_rows",
                 return_value={"considered": 1, "eligible": 1, "promoted": 0, "skipped": 0, "errors": 1},
             ), mock.patch.object(pilot, "run_linkage_and_suffix_audits"), \
             mock.patch.object(
                 pilot, "run_direct_monitor", return_value={"complete": True, "selected": 0}
             ), mock.patch.object(
                 pilot, "log_event", side_effect=lambda event, **details: events.append((event, details))
             ):
            pilot.run(args)

        done = next(details for event, details in events if event == "pilot_run_done")
        self.assertFalse(done["pipeline_complete"])
        self.assertFalse(done["scheduled_run_proven_complete"])

    def test_every_structured_child_event_inherits_active_run_receipt(self):
        stream = io.StringIO()
        pilot.set_run_event_context(run_receipt_id="receipt-123", run_date="2026-08-21", run_stage="discovery")
        try:
            with contextlib.redirect_stdout(stream):
                pilot.log_event("pilot_query_done", rows_written=0)
        finally:
            pilot.clear_run_event_context()
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["run_receipt_id"], "receipt-123")
        self.assertEqual(payload["run_stage"], "discovery")
        self.assertTrue(payload["observed_at"])

    def test_description_block_shadow_is_capped_at_100_rows_per_run(self):
        pilot_rows = [pilot.PILOT_HEADERS]
        for index in range(105):
            payload = {
                "zpid": f"free-cap-{index}",
                "address": f"{index + 1} Main Street",
                "street": f"{index + 1} Main Street",
                "city": "Atlanta",
                "state": "GA",
                "homeStatus": "FOR_SALE",
                "description": "Overview: SHORT SALE. Updated home.",
            }
            pilot_rows.append(
                self.pilot_row(
                    listing_address=payload["address"],
                    city="Atlanta",
                    state="GA",
                    first_seen_at="2026-08-04T07:15:00-04:00",
                    synthetic_zpid=payload["zpid"],
                    pending_queue_listing_json=json.dumps(payload),
                    description_excerpt="Status: Active. Overview: SHORT SALE. Updated home.",
                )
            )
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["listing_address", "state", "created-at"]],
                pilot_rows,
            ],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 4),
                phase="post_promotion",
            )

        shadow_events = [event for event, _ in events if event == "pilot_description_block_shadow"]
        self.assertEqual(stats["description_block_rows"], 100)
        self.assertEqual(len(shadow_events), 100)

    def test_site_chrome_shadow_is_capped_and_emits_zero_write_contract(self):
        pilot_rows = [pilot.PILOT_HEADERS]
        for index in range(105):
            payload = {
                "zpid": f"free-chrome-{index}",
                "address": f"{index + 1} Main Street",
                "street": f"{index + 1} Main Street",
                "city": "Simpsonville",
                "state": "SC",
                "homeStatus": "FOR_SALE",
                "listing_description": "Beds:3 Baths:2 Sq. Feet:1600-1799",
            }
            pilot_rows.append(
                self.pilot_row(
                    listing_address=payload["address"],
                    city="Simpsonville",
                    state="SC",
                    first_seen_at="2026-08-08T07:15:00-04:00",
                    synthetic_zpid=payload["zpid"],
                    source_url=f"https://www.nexusrealtync.com/homes/{index}/",
                    pending_queue_listing_json=json.dumps(payload),
                    description_excerpt=(
                        "Status: Active. Property Description ... Short Sale. $216,500. "
                        "Baldwin Ridge | Simpsonville."
                    ),
                )
            )
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["listing_address", "state", "created-at"]],
                pilot_rows,
            ],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_START_DATE",
            "2026-08-08",
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_MAX_PER_RUN",
            100,
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_DOMAINS",
            {"nexusrealtync.com"},
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 8),
                phase="post_promotion",
            )

        shadow_events = [
            details for event, details in events if event == "pilot_site_chrome_exclusion_shadow"
        ]
        self.assertEqual(stats["site_chrome_rows"], 100)
        self.assertEqual(stats["site_chrome_targeted"], 100)
        self.assertEqual(stats["site_chrome_would_hold"], 100)
        self.assertEqual(len(shadow_events), 100)
        self.assertTrue(all(event["writes"] == 0 for event in shadow_events))
        self.assertTrue(
            all(event["comparison_window_days"] == 7 for event in shadow_events)
        )

    def test_site_chrome_shadow_stops_after_ten_target_domain_candidates(self):
        prior_rows = [
            self.pilot_row(
                listing_address=f"{index + 1} Prior Street",
                state="TX",
                first_seen_at="2026-08-20T07:15:00-04:00",
                source_url=f"https://www.bishopcountry.com/prior/{index}",
            )
            for index in range(10)
        ]
        navigation = (
            "Financing Options Short Sale Option Selecting Your Real Estate Agent"
        )
        payload = {
            "zpid": "free-bishop-next",
            "street": "1704 Langford Street",
            "city": "Greenville",
            "state": "TX",
            "homeStatus": "FOR_SALE",
            "listing_description": navigation,
        }
        pilot_rows = [
            pilot.PILOT_HEADERS,
            *prior_rows,
            self.pilot_row(
                listing_address="1704 Langford Street",
                city="Greenville",
                state="TX",
                first_seen_at="2026-08-21T07:15:00-04:00",
                synthetic_zpid="free-bishop-next",
                source_url="https://www.bishopcountry.com/1704-langford-street",
                pending_queue_listing_json=json.dumps(payload),
                description_excerpt=f"Status: Active. {navigation}",
            ),
        ]
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[[["listing_address", "state", "created-at"]], pilot_rows],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_START_DATE",
            "2026-08-20",
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_MAX_PER_RUN",
            10,
        ), mock.patch.object(
            pilot,
            "SITE_CHROME_SHADOW_DOMAINS",
            {"bishopcountry.com"},
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 21),
                phase="post_promotion",
            )

        self.assertEqual(stats["site_chrome_rows"], 0)
        self.assertFalse(
            any(event == "pilot_site_chrome_exclusion_shadow" for event, _ in events)
        )

    def test_compound_negative_shadow_is_capped_and_emits_zero_write_contract(self):
        pilot_rows = [pilot.PILOT_HEADERS]
        for index in range(105):
            payload = {
                "zpid": f"free-negative-{index}",
                "address": f"{index + 1} Main Street",
                "street": f"{index + 1} Main Street",
                "city": "Pacoima",
                "state": "CA",
                "homeStatus": "FOR_SALE",
                "description": "Property Description: Foreclosure/Short Sale: No.",
            }
            pilot_rows.append(
                self.pilot_row(
                    listing_address=payload["address"],
                    city="Pacoima",
                    state="CA",
                    first_seen_at="2026-08-09T07:15:00-04:00",
                    synthetic_zpid=payload["zpid"],
                    source_url=f"https://www.redfin.com/example/{index}",
                    pending_queue_listing_json=json.dumps(payload),
                    description_excerpt=(
                        "Status: Active. Property Description: Foreclosure/Short Sale: No."
                    ),
                )
            )
        events = []
        with mock.patch.object(
            pilot,
            "get_values",
            side_effect=[
                [["listing_address", "state", "created-at"]],
                pilot_rows,
            ],
        ), mock.patch.object(
            pilot,
            "log_event",
            side_effect=lambda event, **details: events.append((event, details)),
        ):
            stats = pilot.run_linkage_and_suffix_audits(
                "token",
                "sheet-id",
                "Sheet1",
                "Lead Source Pilot",
                run_date=dt.date(2026, 8, 9),
                phase="post_promotion",
            )

        shadow_events = [
            details for event, details in events
            if event == "pilot_compound_negative_field_shadow"
        ]
        self.assertEqual(stats["compound_negative_rows"], 100)
        self.assertEqual(stats["compound_negative_matches"], 100)
        self.assertEqual(stats["compound_negative_would_hold"], 100)
        self.assertEqual(len(shadow_events), 100)
        self.assertTrue(all(event["writes"] == 0 for event in shadow_events))
        self.assertTrue(
            all(event["comparison_window_days"] == 7 for event in shadow_events)
        )
        self.assertTrue(
            all(
                event["success_metric"]
                == "100pct_verifier_agreement_after_10_reviewable_cases"
                for event in shadow_events
            )
        )


if __name__ == "__main__":
    unittest.main()
