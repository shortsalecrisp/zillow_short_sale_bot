import types

import pytest
from fastapi.testclient import TestClient

from test_active_file_outreach_exception import approved_lookup, approved_row
from test_internal_sms_endpoint import APPROVED_OPENER, FakeSendResult, _import_webhook_server


def setup_approved(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    sheet.rows[6677] = approved_row() + ["", ""]
    sheet.rows[1][27] = "created-at"
    monkeypatch.setattr(module, "CRISP_SUPPORT_ACTIVE_FILE_URL", "https://crisp.test/active-file-match")
    monkeypatch.setattr(module, "CRISP_ACTIVE_FILE_LOOKUP_SECRET", "test-only")

    def lookup(url, **kwargs):
        assert url == "https://crisp.test/active-file-match"
        if not kwargs["json"]["email"] and not kwargs["json"]["phone"]:
            return types.SimpleNamespace(status_code=200, json=lambda: {"activeFile": False})
        return types.SimpleNamespace(status_code=200, json=lambda: {
            "activeFile": True, "match": approved_lookup()["match"],
        })

    monkeypatch.setattr(module.requests, "post", lookup)
    monkeypatch.setattr(module, "approved_exception_initial_window_open", lambda: True)
    return module, sheet, sender


def initial(client, **changes):
    return client.post("/internal/send-initial-sms", headers={"Authorization": "Bearer secret-token"},
                       json={"row": 6677, "phone": "360-513-9034", "stable_id": "49354863", **changes})


def incoming(module, message, message_id="exception-inbound-1"):
    return TestClient(module.app).post("/sms-chatbot", data={
        "token": "secret-token", "action": "incoming_sms", "phone": "+13605139034",
        "message": message, "message_id": message_id,
    })


def test_only_approved_initial_uses_unchanged_outbox_template_and_no_sent_markers(monkeypatch):
    module, sheet, sender = setup_approved(monkeypatch)
    client = TestClient(module.app)
    assert client.post("/internal/send-initial-sms", json={"row": 6677, "phone": "3605139034"}).status_code == 403
    result = initial(client)
    assert result.status_code == 200 and result.json()["status"] == "queued"
    assert len(sender.calls) == 1
    assert sender.calls[0]["message"] == APPROVED_OPENER.format(first="Ashleigh", address="9336 Pacific Highway SE")
    assert sender.calls[0]["stable_id"] == "49354863" and sender.force_resends == [False]
    assert all(sheet.rows[6677][i] == "" for i in (7, 11, 22, 42))
    lookup = module._active_crisp_file_lookup(sheet.rows[6677])
    assert lookup["active_file"] is True and lookup["owner_exception_id"]


@pytest.mark.parametrize("payload,detail", [
    ({"force_resend": True}, "owner_exception_does_not_authorize_resend"),
    ({"allow_prior_contact_resend": True}, "owner_exception_does_not_authorize_resend"),
    ({"stable_id": "other"}, "owner_exception_payload_identity_mismatch"),
    ({"first": "Other"}, "owner_exception_payload_identity_mismatch"),
    ({"address": "Another Property"}, "owner_exception_payload_identity_mismatch"),
])
def test_exception_cannot_expand_to_resend_or_different_payload(monkeypatch, payload, detail):
    module, _sheet, sender = setup_approved(monkeypatch)
    result = initial(TestClient(module.app), **payload)
    assert result.status_code == 409 and result.json()["detail"] == detail
    assert sender.calls == []


def test_exception_keeps_initial_local_hours_guard(monkeypatch):
    module, _sheet, sender = setup_approved(monkeypatch)
    monkeypatch.setattr(module, "approved_exception_initial_window_open", lambda: False)
    result = initial(TestClient(module.app))
    assert result.status_code == 409 and result.json()["detail"] == "recipient_outside_local_hours"
    assert sender.calls == []


@pytest.mark.parametrize("response,expected_status", [
    ({"activeFile": True, "match": {"fileId": "new-subject-file", "matchReasons": ["property_address"]}}, 200),
    ({}, 503),
])
def test_exception_cannot_hide_a_subject_property_match_or_unavailable_check(monkeypatch, response, expected_status):
    module, _sheet, sender = setup_approved(monkeypatch)
    def lookup(url, **kwargs):
        data = response if not kwargs["json"]["email"] else {"activeFile": True, "match": approved_lookup()["match"]}
        return types.SimpleNamespace(status_code=200, json=lambda: data)
    monkeypatch.setattr(module.requests, "post", lookup)
    result = initial(TestClient(module.app))
    assert result.status_code == expected_status
    if expected_status == 200:
        assert result.json()["status"] == "active_crisp_file"
    else:
        assert result.json()["detail"] == "active_file_lookup_unavailable"
    assert sender.calls == []


def test_exception_keeps_same_phone_prior_contact_guard(monkeypatch):
    module, sheet, sender = setup_approved(monkeypatch)
    sheet.rows[20] = list(sheet.rows[6677])
    sheet.rows[20][7] = "x"
    sheet.rows[20][22] = "2026-11-05T12:00:00-08:00"
    result = initial(TestClient(module.app))
    assert result.status_code == 200 and result.json()["status"] == "already_contacted_phone"
    assert sheet.rows[20][7] == "x" and sender.calls == []


def test_every_other_active_file_lead_is_still_suppressed(monkeypatch):
    module, sheet, sender = setup_approved(monkeypatch)
    sheet.rows[6677][27] = "different-listing"
    result = initial(TestClient(module.app))
    assert result.status_code == 200 and result.json()["status"] == "active_crisp_file"
    assert sender.calls == []


def test_exception_continues_normal_crm_replies_for_exact_listing(monkeypatch):
    module, sheet, sender = setup_approved(monkeypatch)
    sheet.rows[6677][7] = "x"
    sheet.rows[6677][11] = APPROVED_OPENER.format(first="Ashleigh", address="9336 Pacific Highway SE")
    sheet.rows[6677][22] = "2026-11-06T08:00:00-08:00"
    result = incoming(module, "How much do you charge the buyer at closing?")
    assert result.status_code == 200
    body = result.json()
    assert body["should_reply"] is True
    assert "$5,000" in body["reply_text"]
    assert body.get("handoff_type") != "EXISTING CRISP FILE SUPPORT"
    assert sheet.rows[6677][19] != "TRUE" and sender.calls == []


@pytest.mark.parametrize("message,manual", [("STOP", False), ("How much is the fee?", True)])
def test_normal_crm_still_honors_optout_and_human_takeover(monkeypatch, message, manual):
    module, sheet, sender = setup_approved(monkeypatch)
    sheet.rows[6677][7] = "x"
    sheet.rows[6677][11] = APPROVED_OPENER.format(first="Ashleigh", address="9336 Pacific Highway SE")
    if manual:
        sheet.rows[6677][13] = "handoff"
        sheet.rows[6677][16] = "TRUE"
        sheet.rows[6677][19] = "TRUE"
    result = incoming(module, message)
    assert result.status_code == 200 and result.json()["should_reply"] is False
    if manual:
        assert sheet.rows[6677][19] == "TRUE"
    else:
        assert sheet.rows[6677][10] == "R"
    assert sender.calls == []


def test_missing_stable_identity_does_not_exempt_inbound_from_file_support(monkeypatch):
    module, sheet, sender = setup_approved(monkeypatch)
    sheet.rows[6677][27] = ""
    result = incoming(module, "How can you help with my short sale listing?")
    assert result.status_code == 200 and result.json()["should_reply"] is False
    assert result.json()["handoff_type"] == "EXISTING CRISP FILE SUPPORT"
    assert sheet.rows[6677][19] == "TRUE" and sender.calls == []
