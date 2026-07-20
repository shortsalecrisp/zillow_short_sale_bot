from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V10_DURABLE_SMS_OUTBOX_FULL.template.xml"
)


def _root():
    return ET.parse(RESTORE).getroot()


def _tasks():
    return {task.findtext("id"): task for task in _root().findall("Task")}


def _profiles():
    return {profile.findtext("id"): profile for profile in _root().findall("Profile")}


def _http_bodies(task):
    return [
        action.findtext("Str[@sr='arg5']", default="")
        for action in task.findall("Action")
        if action.findtext("code") == "339"
    ]


def test_restore_is_complete_and_references_existing_tasks():
    root = _root()
    tasks = _tasks()
    assert {"3", "9", "21", "30", "31", "32"}.issubset(tasks)
    assert {profile.findtext("mid0") for profile in root.findall("Profile")} <= set(tasks)
    assert {profile.findtext("nme") for profile in root.findall("Profile")} == {
        "Received Text Any",
        "Google Messages Notification Backup",
        "AutoRemote SMS Outbound",
        "SMS Outbox Every 2 Minutes",
        "SMS Outbox After Boot",
        "SMS Outbox Send Success",
        "SMS Outbox Send Failure",
    }


def test_every_task_has_strict_sequential_action_ids():
    for task in _tasks().values():
        action_ids = [action.get("sr") for action in task.findall("Action")]
        assert action_ids == [f"act{index}" for index in range(len(action_ids))]


def test_inbound_tasks_snapshot_event_identity_before_waiting():
    expected = {
        "3": ("%SMSRF", "%SMSRB"),
        "9": ("%evtprm2", "%evtprm3"),
    }
    for task_id, (phone_source, message_source) in expected.items():
        task = _tasks()[task_id]
        actions = task.findall("Action")
        assert actions[0].findtext("Str[@sr='arg0']") == "%inbound_phone"
        assert actions[0].findtext("Str[@sr='arg1']") == phone_source
        assert actions[1].findtext("Str[@sr='arg0']") == "%inbound_message"
        assert actions[1].findtext("Str[@sr='arg1']") == message_source
        assert task.findtext("rty") == "2"
        assert task.findtext("stayawake") == "true"


def test_inbound_tasks_only_enqueue_and_never_send_bot_reply_directly():
    for task_id in ("3", "9"):
        task = _tasks()[task_id]
        bodies = _http_bodies(task)
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert not any("action=incoming_sms" in body for body in bodies)
        assert not any("action=reply_sent" in body for body in bodies)
        bot_send_actions = [
            action
            for action in task.findall("Action")
            if action.findtext("code") == "41"
            and action.findtext("Str[@sr='arg0']") != "9542053205"
        ]
        assert bot_send_actions == []


def test_global_last_sms_variables_are_only_used_for_immediate_snapshot():
    task = _tasks()["3"]
    serialized = [ET.tostring(action, encoding="unicode") for action in task.findall("Action")]
    assert "%SMSRF" in serialized[0]
    assert "%SMSRB" in serialized[1]
    assert all("%SMSRF" not in value and "%SMSRB" not in value for value in serialized[4:])


def test_dispatcher_is_single_flight_and_uses_server_destination():
    task = _tasks()["30"]
    assert task.findtext("rty") == "0"
    assert task.findtext("stayawake") == "true"
    bodies = _http_bodies(task)
    assert len([body for body in bodies if "action=claim_pending_send" in body]) == 3
    assert len([body for body in bodies if "action=send_started" in body]) == 3
    sends = [action for action in task.findall("Action") if action.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"
    assert sends[0].find("Int[@sr='arg4']").get("val") == "1"


def test_success_and_failure_profiles_have_exact_receipt_tasks():
    profiles = _profiles()
    assert profiles["32"].findtext("Event/code") == "2005"
    assert profiles["33"].findtext("Event/code") == "2010"
    assert profiles["32"].findtext("Event/Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert profiles["33"].findtext("Event/Str[@sr='arg0']") == "%SMSOUT_PHONE"
    success_bodies = _http_bodies(_tasks()["31"])
    failure_bodies = _http_bodies(_tasks()["32"])
    assert len([body for body in success_bodies if "action=reply_sent" in body]) == 3
    assert len([body for body in failure_bodies if "action=sms_send_failed" in body]) == 3
    for body in success_bodies + failure_bodies:
        assert "request_id=%receipt_request_id" in body
        assert "message_id=%receipt_message_id" in body
        assert "phone=%receipt_phone" in body
        assert "reply_text=%receipt_reply_text" in body
        assert "lease_token=%receipt_lease_token" in body


def test_every_dynamic_transport_field_is_url_encoded():
    required = {
        "%transport_token",
        "%transport_phone",
        "%transport_message",
        "%transport_received_at",
        "%transport_message_id",
    }
    for task_id in ("3", "9"):
        conversions = {
            action.findtext("Str[@sr='arg2']"): action.find("Int[@sr='arg1']").get("val")
            for action in _tasks()[task_id].findall("Action")
            if action.findtext("code") == "596"
        }
        assert required <= conversions.keys()
        assert all(conversions[name] == "18" for name in required)


def test_template_contains_placeholder_but_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
