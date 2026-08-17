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

        result = pilot.site_chrome_exclusion_shadow(candidate)

        self.assertFalse(result["platform_targeted"])
        self.assertTrue(result["site_chrome_pattern_found"])
        self.assertTrue(result["proposed_ready"])
        self.assertFalse(result["would_hold"])
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
