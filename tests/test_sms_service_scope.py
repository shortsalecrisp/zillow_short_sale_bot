import json
from pathlib import Path

import pytest
from test_sms_approved_reply_revision import chatbot

CASES = json.loads((Path(__file__).parent / 'fixtures/sms_service_scope_cases.json').read_text())
ACCESS_HISTORY = [
    {'role': 'agent', 'text': CASES['property_requests'][0]},
    {'role': 'assistant', 'text': CASES['unsafe_replies'][0]},
]


@pytest.mark.parametrize('message', CASES['property_requests'])
def test_property_requests_clarify_without_scheduling(chatbot, message):
    result = chatbot.receive(message)
    assert result['reply_text'] == chatbot.module.SMS_SERVICE_SCOPE_REPLY
    assert result['should_reply'] is True
    assert result['handoff_needed'] is False
    assert chatbot.row()['call_booking_status'] != 'scheduled_callback'
    assert not chatbot.row()['callback_time']


@pytest.mark.parametrize('message', CASES['context_followups'])
def test_vague_property_followups_keep_the_correct_scope(chatbot, message):
    chatbot.set(history_json=json.dumps(ACCESS_HISTORY))
    result = chatbot.receive(message)
    assert result['reply_text'] == chatbot.module.SMS_SERVICE_SCOPE_REPLY
    assert result['should_reply'] is True
    assert result['handoff_needed'] is False


@pytest.mark.parametrize('message', CASES['buyers'])
def test_buyer_questions_are_not_buyer_services(chatbot, message):
    result = chatbot.receive(message)
    assert result['should_reply'] is True
    assert "don't bring the buyer" in result['reply_text']
    assert 'lender paperwork, calls, follow-up' in result['reply_text']


@pytest.mark.parametrize('message', CASES['in_scope'])
def test_processing_questions_and_real_calls_are_not_property_logistics(chatbot, message):
    chatbot.set(history_json=json.dumps(ACCESS_HISTORY))
    assert chatbot.module._sms_is_property_logistics_request(message, chatbot.row()) is False


@pytest.mark.parametrize('reply', CASES['unsafe_replies'])
def test_fallback_output_guard_blocks_unsupported_promises(chatbot, monkeypatch, reply):
    monkeypatch.setattr(chatbot.module, '_sms_openai_decision', lambda *_: chatbot.module._sms_decision(reply_text=reply))
    decision = chatbot.module._sms_build_decision(chatbot.row(), 'An ambiguous note about the listing')
    assert decision['reply_text'] == chatbot.module.SMS_SERVICE_SCOPE_REPLY
    assert not decision['callback_time']
    assert not decision['handoff_needed']


@pytest.mark.parametrize('reply', CASES['safe_replies'])
def test_output_guard_allows_processing_statements(chatbot, reply):
    assert chatbot.module._sms_is_unsupported_property_service_promise(reply) is False


def test_optout_override_and_cap_are_preserved(chatbot):
    assert chatbot.receive('Remove me. The lockbox is broken.')['should_reply'] is False
    chatbot.set(human_override='TRUE', ai_state='handoff', handoff_flag='TRUE', history_json=json.dumps(ACCESS_HISTORY))
    assert chatbot.receive('You can reschedule through Showing Time tomorrow at 2')['should_reply'] is False
    assert chatbot.row()['call_booking_status'] != 'scheduled_callback'


def test_scope_answer_at_reply_cap_goes_to_owner(chatbot):
    chatbot.set(auto_reply_count='3')
    result = chatbot.receive(CASES['property_requests'][0])
    assert result['should_reply'] is False
    assert result['handoff_needed'] is True


def test_buyer_question_and_fee_both_answered(chatbot):
    result = chatbot.receive('Do you have buyers, and what is your fee?')
    assert "don't bring the buyer" in result['reply_text']
    assert '$5,000' in result['reply_text']


def test_unsafe_output_with_handoff_cannot_record_a_fake_appointment(chatbot):
    decision = chatbot.module._sms_decision(
        reply_text='I can schedule the appraisal tomorrow.', handoff_needed=True,
        call_booking_status='scheduled_callback', callback_time='tomorrow',
    )
    result = chatbot.module._sms_enforce_service_scope(decision, chatbot.row())
    assert result['handoff_needed'] is True
    assert not result['reply_text']
    assert not result['call_booking_status']
    assert not result['callback_time']
