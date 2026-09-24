"""Offline, full-handler regressions for the approved September 12 reply scope."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from test_internal_sms_endpoint import APPROVED_OPENER, CHATBOT_HEADERS, FakeSendResult, _import_webhook_server


SELF_REPLY = (
    "That makes sense. You keep the listing and client relationship; I can take the lender paperwork, calls, "
    "and follow-up off your plate. There's no fee to you or the seller and no commission split; the buyer pays "
    "my fee at closing. Would a brief call to see if that helps on this file be worthwhile?"
)
FEE_REPLY = (
    "My fee is $5,000, paid by the buyer at closing only if the deal closes. There's no service fee to you or "
    "the seller, and I don't take anything from your commission."
)
BUYER_REPLY = (
    "The buyer does need to consider that cost with their offer, so we discuss the fee and disclosure up front. "
    "I'm happy to walk through how that would work on this listing before you decide anything."
)
FAILED_PROVIDER_REPLY = (
    "I understand why you'd be cautious after that. I handle the lender paperwork, calls, follow-up, and "
    "negotiations while you focus on the listing and your client. We can go through exactly what I'd handle "
    "and what I'd need from you before you decide. Would a brief call be helpful?"
)
EXPERIENCE_REPLY = (
    "I've been handling short sales for over 15 years, and this is all I do: help agents and homeowners through "
    "the process. I'd be happy to talk through your listing and explain how I can help."
)
EMAIL_REQUEST_REPLY = "Absolutely. What's the best email for an overview of what I handle and how the fee works?"
COUNT_ADMISSION = "I don't have a verified closed count to quote here; I'll need to confirm that number."


@pytest.fixture
def chatbot(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())

    def forbid_model(*_args, **_kwargs):
        raise AssertionError("A deterministic approved case must not invoke a model or network")

    monkeypatch.setattr(module, "_sms_openai_decision", forbid_model)
    sheet.rows[2] = [""] * len(CHATBOT_HEADERS)

    class Harness:
        sequence = 0

        def set(self, **fields):
            for name, value in fields.items():
                sheet.rows[2][CHATBOT_HEADERS.index(name)] = str(value)

        def row(self):
            return dict(zip(CHATBOT_HEADERS, sheet.rows[2]))

        def history(self):
            return json.loads(self.row()["history_json"])

        def receive(self, message, message_id=None):
            self.sequence += 1
            stamp = datetime(2026, 9, 12, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=10 * self.sequence)
            return module._sms_handle_incoming(
                {
                    "phone": "555-010-1001", "message": message,
                    "message_id": message_id or f"offline-inbound-{self.sequence}",
                    "received_at": stamp.isoformat(),
                },
                f"offline-request-{self.sequence}",
            )

        def deliver(self, reply):
            return module._sms_handle_reply_sent(
                {
                    "phone": "555-010-1001", "reply_text": reply,
                    "sent_at": f"2026-09-12T14:{self.sequence:02d}:30+00:00",
                },
                f"offline-receipt-{self.sequence}",
            )

    harness = Harness()
    harness.module = module
    harness.sheet = sheet
    harness.sender = sender
    harness.set(
        agent_name="Synthetic", last_name="Agent", phone="555-010-1001", email="",
        listing_address="100 Test Street", mailshake_status="Y", auto_reply_count="0",
        history_json="[]", human_override="FALSE", handoff_flag="FALSE", ai_state="active",
    )
    yield harness
    assert sender.calls == [], "These tests must not send real or mock outbound transport messages"


def test_neutral_self_handling_is_exact_one_value_reply(chatbot):
    result = chatbot.receive("I'm handling it myself")
    assert result["should_reply"] is True
    assert result["reply_text"] == SELF_REPLY
    assert result["response_id"] == "self_handling_value"
    assert result["lead_status"] == "R"
    assert result["conversation_done"] is False
    assert chatbot.row()["mailshake_status"] == "R"
    assert chatbot.row()["ai_state"] == "active"
    assert chatbot.row()["call_booking_status"] == "closed_no_interest"
    assert chatbot.row()["human_override"] == "FALSE"
    assert all(entry["role"] != "assistant" for entry in chatbot.history())


@pytest.mark.parametrize("message", [
    "Handling it myself",
    "I am handling this myself",
    "We handle short sales ourselves",
    "I handle the lender side myself",
])
def test_self_handling_variants_remain_r_until_agent_shows_interest(chatbot, message):
    result = chatbot.receive(message)
    assert result["reply_text"] == SELF_REPLY
    assert result["lead_status"] == "R"
    assert chatbot.row()["mailshake_status"] == "R"


def test_fee_question_after_self_handling_promotes_to_y(chatbot):
    first = chatbot.receive("Handling it myself")
    chatbot.deliver(first["reply_text"])
    second = chatbot.receive("What is your fee?")
    assert second["should_reply"] is True
    assert second["reply_text"] == FEE_REPLY
    assert second["lead_status"] == "Y"
    assert chatbot.row()["mailshake_status"] == "Y"
    assert chatbot.row()["call_booking_status"] == "interested_no_call"


@pytest.mark.parametrize("legacy", [False, True])
def test_delivered_self_value_closes_without_repeating_pitch(chatbot, legacy):
    text = "I understand, and I help a lot of agents in the same situation. I can take the lender work off your plate."
    entry = {"role": "assistant", "text": text if legacy else "Previous approved value wording"}
    if not legacy:
        entry["response_id"] = "self_handling_value"
    chatbot.set(history_json=json.dumps([entry]), auto_reply_count="1")
    result = chatbot.receive("I am still handling it myself")
    assert result["lead_status"] == "R"
    assert result["conversation_done"] is True
    assert result["reply_text"] == chatbot.module.SMS_STANDARD_CLOSEOUT_REPLY
    assert result["handoff_needed"] is False


@pytest.mark.parametrize("message", ["How much?", "What is your fee?", "What do you charge?", "What would you charge?", "What does it cost?", "Cost?"])
def test_amount_first_is_explicit_without_prior_fee_delivery(chatbot, message):
    result = chatbot.receive(message)
    assert result["reply_text"] == FEE_REPLY
    assert result["response_id"] == "fee_specific"
    assert result["should_reply"] is True


def test_general_payer_then_amount_requires_delivery_evidence(chatbot):
    first = chatbot.receive("How do you get paid?")
    assert first["response_id"] == "fee_initial"
    assert "$5,000" not in first["reply_text"]
    assert chatbot.row()["auto_reply_count"] == "0"
    assert chatbot.module._sms_has_delivered_response(chatbot.row(), "fee_initial") is False
    chatbot.deliver(first["reply_text"])
    assert chatbot.history()[-1]["response_id"] == "fee_initial"
    assert chatbot.row()["auto_reply_count"] == "1"
    second = chatbot.receive("What is the dollar amount?")
    assert second["reply_text"] == FEE_REPLY


def test_queued_general_fee_is_not_cap_bypass_evidence(chatbot):
    chatbot.receive("Who pays you?")
    chatbot.set(auto_reply_count="3")
    result = chatbot.receive("What is your fee?")
    assert result["should_reply"] is False
    assert result["handoff_type"] == "MAX REPLIES REACHED"


def test_general_fee_stage_advances_only_after_delivery(chatbot):
    first = chatbot.receive("How do you get paid?")
    pending = chatbot.receive("Who pays you?")
    assert "$5,000" not in pending["reply_text"]
    chatbot.deliver(first["reply_text"])
    delivered = chatbot.receive("How are you compensated?")
    assert delivered["reply_text"] == FEE_REPLY


def test_delivered_specific_fee_id_not_copy_controls_repeat(chatbot):
    chatbot.set(history_json=json.dumps([
        {"role": "assistant", "text": "Earlier fee wording", "response_id": "fee_specific"}
    ]))
    result = chatbot.receive("How much is your fee?")
    assert result["should_reply"] is False
    assert result["handoff_type"] == "FEE QUESTION FOLLOW-UP"


def test_buyer_concern_after_specific_fee_is_new_supported_question(chatbot):
    chatbot.set(last_outbound_text=FEE_REPLY, auto_reply_count="1")
    result = chatbot.receive("What if the buyer cannot afford that fee?")
    assert result["reply_text"] == BUYER_REPLY
    assert result["response_id"] == "buyer_cost_concern"
    assert result["handoff_needed"] is False


@pytest.mark.parametrize("message", [
    "I'm confused about how the buyer pays you",
    "What if the buyer won't pay the fee?",
    "Will the buyer lower their offer because of the cost?",
])
def test_buyer_economics_variants_are_not_fee_negotiation_or_loop(chatbot, message):
    chatbot.set(last_outbound_text=FEE_REPLY, auto_reply_count="1")
    result = chatbot.receive(message)
    assert result["should_reply"] is True
    assert result["reply_text"] == BUYER_REPLY
    assert result["handoff_needed"] is False


def test_reply_cap_handoff_with_delivered_initial_fee_stays_owner_locked(chatbot):
    chatbot.set(
        human_override="TRUE", handoff_flag="TRUE", ai_state="handoff",
        conversation_summary="max replies reached", auto_reply_count="3",
        history_json=json.dumps([{"role": "assistant", "text": "Earlier payer answer", "response_id": "fee_initial"}]),
    )
    result = chatbot.receive("How much is your fee?")
    assert result["should_reply"] is False
    assert chatbot.row()["human_override"] == "TRUE"
    assert chatbot.row()["handoff_flag"] == "TRUE"
    assert chatbot.row()["ai_state"] == "handoff"


def test_new_payer_question_after_amount_is_not_fee_loop(chatbot):
    chatbot.set(last_outbound_text=FEE_REPLY, auto_reply_count="1")
    result = chatbot.receive("Does the seller pay anything?")
    assert result["should_reply"] is True
    assert "no cost to you or the seller" in result["reply_text"]
    assert result["handoff_needed"] is False


@pytest.mark.parametrize("address", ["5000 Example Lane", "123 Main St Unit 5000"])
def test_opener_listing_number_is_not_a_delivered_fee_answer(chatbot, address):
    text = APPROVED_OPENER.format(first="Taylor", address=address)
    chatbot.set(last_outbound_text=text, history_json=json.dumps([{"role": "assistant", "text": text}]))
    result = chatbot.receive("What is your fee?")
    assert result["should_reply"] is True
    assert result["handoff_needed"] is False
    assert result["reply_text"] == FEE_REPLY


def test_amount_and_buyer_concern_both_answered(chatbot):
    result = chatbot.receive("What is your fee, and what if the buyer cannot afford it?")
    assert FEE_REPLY in result["reply_text"]
    assert BUYER_REPLY in result["reply_text"]


def test_failed_past_provider_does_not_mean_current_coverage(chatbot):
    result = chatbot.receive("The last processor failed me and I did all the work")
    assert result["reply_text"] == FAILED_PROVIDER_REPLY
    assert result["response_id"] == "failed_provider"
    assert result["lead_status"] == "Y"


def test_failed_provider_multiquestion_keeps_new_copy_and_known_answers(chatbot):
    result = chatbot.receive(
        "Are you local? I used a processor last year who failed me. Can I check out your company? "
        "How do you get paid? This is in Equator."
    )
    for material in ("Atlanta", "crispshortsales.com", "buyer at closing", "Equator", FAILED_PROVIDER_REPLY):
        assert material in result["reply_text"]
    assert result["handoff_needed"] is False


@pytest.mark.parametrize("count", [0, 3])
@pytest.mark.parametrize("message", [
    "No thanks, I already have a negotiator on Equator",
    "We already have a processor handling this on Equator",
    "It is not a short sale. I use Equator for other files",
    "No thanks, I'm handling it myself",
])
def test_terminal_provider_or_decline_beats_equator_even_at_cap(chatbot, count, message):
    chatbot.set(auto_reply_count=str(count))
    result = chatbot.receive(message)
    assert result["lead_status"] == "R"
    assert result["conversation_done"] is True
    assert result["handoff_needed"] is False
    assert "expert" not in result["reply_text"].lower()
    assert result["should_reply"] is (count < 3)


def test_negated_rejection_does_not_close_supported_question(chatbot):
    result = chatbot.receive("I'm not saying I'm not interested. What is your fee?")
    assert result["reply_text"] == FEE_REPLY
    assert result["lead_status"] == "Y"
    assert result["conversation_done"] is False


@pytest.mark.parametrize("email", ["", "agent@example.test"])
def test_email_before_call_is_not_call_consent_or_fake_delivery(chatbot, email):
    chatbot.set(email=email, callback_requested="yes", callback_time="Friday 2 PM")
    result = chatbot.receive("Email me an overview first; if it looks good I might consider a call tomorrow")
    assert result["call_booking_status"] == "information_requested"
    assert result["callback_time"] == ""
    assert chatbot.row()["callback_requested"] == "no"
    assert chatbot.row()["callback_time"] == ""
    if not email:
        assert result["reply_text"] == EMAIL_REQUEST_REPLY
        assert result["handoff_needed"] is False
    else:
        assert result["reply_text"] == ""
        assert result["should_reply"] is False
        assert result["handoff_type"] == "INFO EMAIL APPROVAL REQUIRED"
        assert result["info_email_to"] == email
        assert result["info_email_approval_required"] is True


def test_existing_provider_and_future_info_is_o_and_stores_new_address(chatbot):
    result = chatbot.receive("I already have a processor. Email me information for the future at agent@example.test")
    assert result["lead_status"] == "O"
    assert result["info_email_to"] == "agent@example.test"
    assert chatbot.row()["email"] == "agent@example.test"
    assert result["call_booking_status"] == "information_requested"
    assert "sent" not in result["reply_text"].lower()


def test_housing_counselor_future_info_preserves_o(chatbot):
    chatbot.set(email="agent@example.test")
    result = chatbot.receive("We are working with a HUD Housing Counselor, but I will take more info on your services just in case")
    assert result["lead_status"] == "O"
    assert result["handoff_type"] == "INFO EMAIL APPROVAL REQUIRED"


def test_documents_are_actual_answer_not_material_promise(chatbot):
    result = chatbot.receive("Who collects the documents?")
    assert result["should_reply"] is True
    assert "collect and organize" in result["reply_text"]
    assert "submit the package" in result["reply_text"]
    assert "checklist" in result["reply_text"]


def test_unsolicited_material_promise_is_narrowly_sanitized(chatbot, monkeypatch):
    monkeypatch.setattr(chatbot.module, "_sms_openai_decision", lambda *_args: chatbot.module._sms_decision(
        reply_text="I'll email you the documents and checklist."
    ))
    result = chatbot.receive("Tell me more about that part")
    assert "I'll email" not in result["reply_text"]
    assert "lender-side paperwork" in result["reply_text"]


def test_multiple_supported_questions_all_survive_full_handler(chatbot):
    result = chatbot.receive("What company are you with? Are you local? Who collects documents? What is your fee?")
    assert result["should_reply"] is True
    assert result["handoff_needed"] is False
    for material in ("Crisp Short Sales", "Atlanta", "collect and organize", FEE_REPLY):
        assert material in result["reply_text"]


def test_experience_exact_copy(chatbot):
    result = chatbot.receive("How long have you been handling short sales?")
    assert result["reply_text"] == EXPERIENCE_REPLY
    assert result["response_id"] == "experience"


def test_ordinary_lender_timeline_is_not_closed_count_or_urgent_auction(chatbot):
    result = chatbot.receive("How many days does lender approval take for short sales?")
    assert result["reply_text"] == chatbot.module.SHORT_SALE_TIMELINE_REPLY
    assert result["handoff_needed"] is False


@pytest.mark.parametrize("count", [0, 3])
def test_known_tenure_fee_then_unverified_count_new_handoff_within_cap(chatbot, count):
    chatbot.set(auto_reply_count=str(count))
    result = chatbot.receive("How many years have you handled short sales, what is your fee, and how many deals did you close last year?")
    assert result["handoff_needed"] is True
    assert result["handoff_type"] == "STATS QUESTION"
    assert chatbot.row()["human_override"] == "TRUE"
    assert result["should_reply"] is (count < 3)
    if count < 3:
        for material in ("over 15 years", FEE_REPLY, COUNT_ADMISSION):
            assert material in result["reply_text"]
    else:
        assert result["reply_text"] == ""


@pytest.mark.parametrize("lock", ["human_override", "handoff_flag", "ai_state"])
def test_existing_manual_lock_records_inbound_only(chatbot, lock):
    chatbot.set(**{lock: "handoff" if lock == "ai_state" else "TRUE"}, conversation_summary="Manual reply sent; ownership retained")
    before = chatbot.row()
    result = chatbot.receive("How much is your fee? Call me tomorrow at 2 PM Eastern")
    assert result["should_reply"] is False
    assert result["reply_text"] == ""
    after = chatbot.row()
    for name in ("conversation_summary", "human_override", "handoff_flag", "ai_state"):
        assert after[name] == before[name]
    assert chatbot.history()[-1]["role"] == "agent"


@pytest.mark.parametrize("message, expected_parts", [
    ("Call me tomorrow after 2 PM Eastern", ["Tomorrow", "after 2 PM", "Eastern"]),
    ("Can we talk Tuesday at 2:30 PM ET?", ["Tuesday", "at 2:30 PM", "ET"]),
    ("Call me today at 4 PM Pacific", ["Today", "at 4 PM", "Pacific"]),
])
def test_callback_keeps_complete_supplied_timing(chatbot, message, expected_parts):
    result = chatbot.receive(message)
    assert result["call_booking_status"] == "scheduled_callback"
    for part in expected_parts:
        assert part in result["callback_time"]
        assert part in chatbot.row()["callback_time"]
    assert chatbot.row()["callback_requested"] == "yes"


def test_fee_answer_is_sent_before_new_callback_handoff(chatbot):
    result = chatbot.receive("What is your fee? Call me Tuesday at 2 PM ET")
    assert result["reply_text"] == FEE_REPLY
    assert result["handoff_needed"] is True
    assert result["should_reply"] is True
    assert result["call_booking_status"] == "scheduled_callback"
    assert "Tuesday at 2 PM ET" == result["callback_time"]


def test_fee_and_present_help_answer_then_new_handoff_without_call_booking(chatbot):
    result = chatbot.receive("I am handling it myself but would love help getting it approved quicker. How much do you charge?")
    assert result["reply_text"] == FEE_REPLY
    assert result["handoff_needed"] is True
    assert result["should_reply"] is True
    assert result["callback_time"] == ""
    assert result["call_booking_status"] != "scheduled_callback"


def test_conditional_call_alone_never_creates_callback(chatbot):
    result = chatbot.receive("I might consider a call tomorrow if this sounds worthwhile")
    assert result["should_reply"] is False
    assert result["callback_time"] == ""
    assert chatbot.row()["callback_requested"] == "no"


@pytest.mark.parametrize("count", [0, 3])
def test_differentiation_answer_survives_only_new_handoff_within_cap(chatbot, count):
    chatbot.set(auto_reply_count=str(count))
    result = chatbot.receive("How are you different from them with communication and documentation?")
    assert result["handoff_needed"] is True
    assert result["alert_needed"] is True
    assert result["handoff_type"] == "HOT LEAD - DIFFERENTIATION QUESTION"
    assert result["should_reply"] is (count < 3)
    if count < 3:
        assert "I focus exclusively on the lender-side short-sale work" in result["reply_text"]


@pytest.mark.parametrize("message", ["Can you help on Equator?", "What's the fee in Equator?"])
def test_existing_active_equator_cap_exception_is_preserved(chatbot, message):
    chatbot.set(auto_reply_count="3")
    result = chatbot.receive(message)
    assert result["should_reply"] is True
    assert result["handoff_needed"] is False
    assert "Equator" in result["reply_text"]


def test_existing_first_fee_after_closeout_exception_uses_amount_first(chatbot):
    chatbot.set(auto_reply_count="3", ai_state="done", mailshake_status="R", call_booking_status="closed_no_interest")
    result = chatbot.receive("What's your fee for this service?")
    assert result["should_reply"] is True
    assert result["reply_text"] == FEE_REPLY
    assert result["lead_status"] == "O"
    assert result["conversation_done"] is True


@pytest.mark.parametrize("message", ["The auction is Tuesday at 2 PM", "Can you stop a foreclosure sale scheduled for tomorrow?"])
def test_auction_deadline_is_review_not_callback_or_generic_timeline(chatbot, message):
    result = chatbot.receive(message)
    assert result["handoff_type"] == "URGENT AUCTION REVIEW"
    assert result["callback_time"] == ""
    assert result["call_booking_status"] != "scheduled_callback"
    assert chatbot.row()["callback_requested"] == "no"
    assert result["reply_text"] == ""


def test_auction_update_after_manual_handoff_does_not_create_callback(chatbot):
    chatbot.set(human_override="TRUE", handoff_flag="TRUE", ai_state="handoff", call_booking_status="interested_no_call")
    result = chatbot.receive("The auction is Tuesday at 2 PM; that works for the seller")
    assert result["should_reply"] is False
    assert result["callback_time"] == ""
    assert chatbot.row()["call_booking_status"] == "interested_no_call"


def test_buyer_provision_does_not_assume_acceptance(chatbot):
    result = chatbot.receive("Do you bring the buyer?")
    assert result["reply_text"] == "No, I don't bring the buyer. I handle the lender-side short-sale processing and negotiations."
    assert "never any issue" not in result["reply_text"]


def test_opt_out_overrides_fee_and_portal(chatbot):
    result = chatbot.receive("Stop texting me. What is your fee on Equator?")
    assert result["lead_status"] == "R"
    assert result["conversation_done"] is True
    assert result["should_reply"] is False
    assert result["handoff_needed"] is False


def test_receipt_is_idempotent_and_only_delivery_adds_response_id(chatbot):
    result = chatbot.receive("How much?", message_id="offline-unique")
    assert all(entry["role"] != "assistant" for entry in chatbot.history())
    duplicate = chatbot.receive("How much?", message_id="offline-unique")
    assert duplicate["duplicate"] is True
    assert duplicate["should_reply"] is False
    first_receipt = chatbot.deliver(result["reply_text"])
    second_receipt = chatbot.deliver(result["reply_text"])
    assert first_receipt["duplicate"] is False
    assert second_receipt["duplicate"] is True
    assert chatbot.row()["auto_reply_count"] == "1"
    assert [entry["response_id"] for entry in chatbot.history() if entry["role"] == "assistant"] == ["fee_specific"]
