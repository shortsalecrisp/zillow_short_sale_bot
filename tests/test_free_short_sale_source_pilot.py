import contextlib
import io
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import free_short_sale_source_pilot as pilot  # noqa: E402


class FreeShortSaleSourcePilotTest(unittest.TestCase):
    def test_qualification_accepts_listing_description_short_sale_without_label(self):
        text = "For Sale. What's special: This home is being sold as a short sale subject to lender approval."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "qualified")
        self.assertEqual(result.short_sale_evidence_type, "listing_description_or_remarks")

    def test_qualification_rejects_listing_text_without_short_sale(self):
        text = "For Sale. Remarks: Updated home near parks and shopping."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "missing_listing_text_short_sale")

    def test_qualification_rejects_generic_short_sale_search_page_noise(self):
        text = (
            "For Sale. Browse Michigan short sale homes and foreclosure listings. "
            "Remarks: Updated ranch near parks and shopping."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "short_sale_not_in_listing_evidence")

    def test_qualification_rejects_already_approved_short_sale(self):
        text = "For Sale. What's special: This is an approved short sale."

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_qualification_rejects_explicit_short_sale_no(self):
        text = (
            "For Sale. Property description: Status Active. "
            "Is Short Sale: No. Special Listing Conditions: None."
        )

        result = pilot.qualification_for_text(text)

        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.failure_reason, "disqualifying_short_sale_text")

    def test_duplicate_status_skips_existing_agent_phone_even_new_address(self):
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
            text="For Sale. Special Listing Conditions: Short Sale.",
            fields={
                "listing_address": "2 New St",
                "city": "Atlanta",
                "state": "GA",
                "phone": "(404) 555-1212",
                "agent_name": "Jane Agent",
            },
        )

        status, key, matched_row = pilot.duplicate_status(candidate, existing)

        self.assertEqual(status, "duplicate_agent_phone")
        self.assertEqual(key, "4045551212")
        self.assertEqual(matched_row, "2")

    def test_pilot_row_starts_like_main_sheet(self):
        candidate = pilot.Candidate(
            source="redfin.com",
            query="q",
            url="https://example.com/listing",
            title="10 Main St",
            text="For Sale. What's special: Short sale subject to lender approval.",
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

    def test_default_states_exclude_michigan_but_keep_lookup_term(self):
        self.assertNotIn("MI", pilot.DEFAULT_STATES)
        self.assertEqual(pilot.STATE_QUERY_TERMS["MI"], "Michigan")

    def test_default_states_cover_49_active_states(self):
        self.assertEqual(len(pilot.DEFAULT_STATES), 49)
        self.assertEqual(len(set(pilot.DEFAULT_STATES)), 49)
        self.assertEqual(set(pilot.DEFAULT_STATES), set(pilot.STATE_QUERY_TERMS) - {"MI"})

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

    def test_listing_address_and_state_guards_reject_search_page_noise(self):
        self.assertFalse(pilot.looks_like_listing_address("Buying A Short Sale vs Foreclosure"))
        self.assertFalse(pilot.looks_like_listing_address("Alabama fixer-upper homes page 4"))
        self.assertFalse(pilot.looks_like_listing_address("Viewing Listing MLS# 7033072"))
        self.assertFalse(pilot.looks_like_listing_address("3301 64th Street in Fort Smith, AR for $189,000"))
        self.assertTrue(pilot.looks_like_listing_address("123 Main St"))

        candidate = pilot.Candidate(
            source="redfin.com",
            query="query",
            url="https://www.redfin.com/MD/Halethorpe/2828-Alabama-Ave-21227/home/9378085",
            title="2828 Alabama Ave",
            text="For Sale. Remarks: Short sale subject to lender approval.",
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
            snippet="Special Listing Conditions: Short Sale.",
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
        self.assertEqual(candidate.fields["phone"], "928-282-4166")

    def test_phone_regex_rejects_long_photo_timestamps(self):
        self.assertIsNone(pilot.PHONE_RE.search("20260512213414244719000000-o.jpg"))
        self.assertEqual(pilot.PHONE_RE.search("(404) 555-1212").group(0), "(404) 555-1212")

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
            pilot.cse_search = old_cse_search
            pilot.ddg_search = old_ddg_search


if __name__ == "__main__":
    unittest.main()
