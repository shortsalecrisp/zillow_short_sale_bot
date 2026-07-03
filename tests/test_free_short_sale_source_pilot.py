import contextlib
import io
import json
import os
import sys
import unittest
import urllib.parse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import free_short_sale_source_pilot as pilot  # noqa: E402


class FreeShortSaleSourcePilotTest(unittest.TestCase):
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
                "email": "jane@example.com",
                "agent_name": "Jane Smith",
            },
        )
        qualification = pilot.qualification_for_text(candidate.text)

        row = pilot.candidate_to_row(candidate, qualification, "4045551212", "2", "")

        self.assertEqual(row[:7], ["Jane", "Smith", "404-555-1212", "jane@example.com", "2 New St", "Atlanta", "GA"])
        self.assertEqual(row[16], "review")
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
                "phone": "714-300-5277",
                "email": "maria@example.com",
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

    def test_default_source_buckets_cover_all_four_approved_sources(self):
        old_buckets = os.environ.pop("FREE_SOURCE_PILOT_SOURCE_BUCKETS", None)
        try:
            sources = [source for source, _ in pilot.configured_source_queries()]
        finally:
            if old_buckets is not None:
                os.environ["FREE_SOURCE_PILOT_SOURCE_BUCKETS"] = old_buckets

        self.assertEqual(sources, ["idx_broker_pages", "realtor.com", "redfin.com", "homes.com"])

    def test_configured_source_buckets_ignore_unknowns_and_duplicates(self):
        old_buckets = os.environ.get("FREE_SOURCE_PILOT_SOURCE_BUCKETS")
        os.environ["FREE_SOURCE_PILOT_SOURCE_BUCKETS"] = "realtor.com,unknown,realtor.com,homes.com"
        try:
            sources = [source for source, _ in pilot.configured_source_queries()]
        finally:
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
        self.assertEqual(candidate.fields["phone"], "928-282-4166")

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

        self.assertEqual(candidate.fields["phone"], "(508)594-3513")
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
        self.assertEqual(row[16], "review")
        self.assertIn("agent contact is missing or partial", row[15])

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

        self.assertEqual(row[16], "review")
        self.assertIn("agent contact is missing or partial", row[15])

    def test_agent_name_cleaner_rejects_brokerage_names(self):
        self.assertEqual(pilot.clean_agent_name("West USA Realty"), "")
        self.assertEqual(pilot.clean_agent_name("Brokered by Ben Zeller"), "Ben Zeller")
        self.assertEqual(pilot.clean_agent_name("Ben Zeller Brokered by"), "Ben Zeller")
        self.assertEqual(pilot.clean_agent_name("Shown By Listed By"), "")
        self.assertEqual(pilot.clean_agent_name("Listing Agent: Jane Smith Phone 404-555-1212"), "Jane Smith")
        self.assertEqual(pilot.clean_agent_name("Southern Missouri Regional"), "")

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

        def fake_cse_search(query, source, limit):
            calls.append(("cse", query, source, limit))
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

            engine, results = pilot.search_web("query", "source", 3)

            self.assertEqual(engine, "cse")
            self.assertEqual(len(results), 1)
            self.assertEqual(calls, [("cse", "query", "source", 3)])
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

        def fake_cse_search(query, source, limit):
            calls.append(("cse", query, source, limit))
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
            self.assertEqual(calls, [("cse", "query", "source", 3), ("ddg", "query", "source", 3)])
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

        def fake_cse_search(query, source, limit):
            calls.append(("cse", query, source, limit))
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

            self.assertEqual(calls, [("cse", "query", "source", 3)])
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


if __name__ == "__main__":
    unittest.main()
