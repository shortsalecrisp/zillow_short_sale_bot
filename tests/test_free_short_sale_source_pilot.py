import contextlib
import datetime as dt
import io
import json
import os
import sys
import types
import unittest
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

    def test_duplicate_agent_phone_can_still_be_written_for_listing_review(self):
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
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "4045551212", "2", "")

        self.assertEqual(row[:7], ["Jane", "Smith", "404-555-1212", "jane@example.com", "2 New St", "Atlanta", "GA"])
        self.assertEqual(row[14], "shadow_ready")
        self.assertEqual(row[16], "yes")
        self.assertEqual(row[22], "4045551212")
        self.assertEqual(row[23], "2")

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
            url="https://example.com/listing",
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
        self.assertEqual(candidate.fields["agent_name"], "Maria Cahuenas")
        self.assertEqual(candidate.fields["phone"], "")

    def test_infer_fields_extracts_jsonld_real_estate_agent_name(self):
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

        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")

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
        self.assertEqual(candidate.fields["agent_name"], "Glenn Zimmerman")

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
        self.assertEqual(candidate.fields["agent_name"], "Marsha Rogers")
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
                run_date=dt.date(2026, 7, 29),
                limit=3,
            )
            day_two = pilot.collect_direct_monitor_urls(
                "momentum",
                (feed,),
                run_date=dt.date(2026, 7, 30),
                limit=3,
            )

        self.assertEqual(len(day_one), 3)
        self.assertEqual(len(day_two), 3)
        self.assertTrue(set(day_one).isdisjoint(day_two))
        day_one_numbers = sorted(int(url.rsplit("/", 1)[-1].split("-", 1)[0]) for url in day_one)
        self.assertGreater(day_one_numbers[-1] - day_one_numbers[0], 4)

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

            self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
            self.assertEqual(candidate.fields["phone"], "404-555-1212")
            self.assertEqual(candidate.fields["email"], "jane@example.com")
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

    def test_jsonld_real_estate_agent_contact_is_attributable(self):
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

        self.assertEqual(candidate.fields["agent_name"], "Jane Q. Smith")
        self.assertEqual(candidate.fields["phone"], "404-555-1212")
        self.assertEqual(candidate.fields["email"], "jane@example.com")
        self.assertEqual(candidate.fields["agent_name_source"], "jsonld_real_estate_agent")

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

        self.assertEqual(candidate.fields["agent_name"], "Jane Smith")
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

            with contextlib.redirect_stdout(io.StringIO()):
                engine, results = pilot.search_web("query", "source", 3)

            self.assertEqual(engine, "ddg")
            self.assertEqual(len(results), 1)
            self.assertEqual(calls, [("cse", "query", "source", 3, None), ("ddg", "query", "source", 3)])
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

    def test_review_experiment_windows_are_bounded(self):
        self.assertTrue(pilot.experiment_active(dt.date(2026, 8, 1), "2026-08-01", 3))
        self.assertTrue(pilot.experiment_active(dt.date(2026, 8, 3), "2026-08-01", 3))
        self.assertFalse(pilot.experiment_active(dt.date(2026, 8, 4), "2026-08-01", 3))


if __name__ == "__main__":
    unittest.main()
