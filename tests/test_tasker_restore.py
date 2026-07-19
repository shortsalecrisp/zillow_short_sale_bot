from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V9_URL_SAFE_CORRELATION_FULL.template.xml"
)


def _tasks():
    root = ET.parse(RESTORE).getroot()
    return {task.findtext("id"): task for task in root.findall("Task")}


def test_restore_is_complete_and_references_existing_tasks():
    root = ET.parse(RESTORE).getroot()
    tasks = _tasks()
    assert {"3", "9", "21"}.issubset(tasks)
    assert {profile.findtext("mid0") for profile in root.findall("Profile")} <= set(tasks)
    assert {profile.findtext("nme") for profile in root.findall("Profile")} == {
        "Received Text Any",
        "Google Messages Notification Backup",
        "AutoRemote SMS Outbound",
    }


def test_every_task_has_strict_sequential_action_ids():
    for task in _tasks().values():
        action_ids = [action.get("sr") for action in task.findall("Action")]
        assert action_ids == [f"act{index}" for index in range(len(action_ids))]


def test_inbound_tasks_allow_concurrent_runs_and_capture_reply_identity():
    tasks = _tasks()
    for task_id in ("3", "9"):
        task = tasks[task_id]
        assert task.findtext("rty") == "2"
        values = {
            action.findtext("Str[@sr='arg0']"): action.findtext("Str[@sr='arg1']")
            for action in task.findall("Action")
            if action.findtext("code") == "547"
        }
        assert values["%reply_phone"] in {"%SMSRF", "%evtprm2"}
        assert values["%reply_text"] == "%http_data.reply_text"
        assert values["%reply_request_id"] == "%http_data.request_id"
        assert values["%reply_message_id"] == "%http_data.message_id"


def test_reply_send_and_receipt_use_only_captured_identity():
    for task_id in ("3", "9"):
        task = _tasks()[task_id]
        send_actions = [
            action
            for action in task.findall("Action")
            if action.findtext("code") == "41"
            and action.findtext("Str[@sr='arg0']") == "%reply_phone"
        ]
        assert len(send_actions) == 1
        assert send_actions[0].findtext("Str[@sr='arg1']") == "%reply_text"

        receipt_bodies = [
            action.findtext("Str[@sr='arg5']", default="")
            for action in task.findall("Action")
            if action.findtext("code") == "339"
            and "action=reply_sent" in action.findtext("Str[@sr='arg5']", default="")
        ]
        assert len(receipt_bodies) == 1
        receipt = receipt_bodies[0]
        assert "request_id=%receipt_request_id" in receipt
        assert "message_id=%receipt_message_id" in receipt
        assert "phone=%receipt_phone" in receipt
        assert "reply_text=%receipt_reply_text" in receipt
        assert "sent_at=%receipt_sent_at" in receipt


def test_every_dynamic_transport_field_is_url_encoded():
    expected_destinations = {
        "%transport_token",
        "%transport_phone",
        "%transport_message",
        "%transport_received_at",
        "%transport_message_id",
        "%receipt_token",
        "%receipt_request_id",
        "%receipt_message_id",
        "%receipt_phone",
        "%receipt_reply_text",
        "%receipt_sent_at",
    }

    for task_id in ("3", "9"):
        task = _tasks()[task_id]
        conversions = {
            action.findtext("Str[@sr='arg2']"): action.find("Int[@sr='arg1']").get("val")
            for action in task.findall("Action")
            if action.findtext("code") == "596"
        }
        assert expected_destinations <= conversions.keys()
        assert all(conversions[name] == "18" for name in expected_destinations)

        inbound_bodies = [
            action.findtext("Str[@sr='arg5']", default="")
            for action in task.findall("Action")
            if action.findtext("code") == "339"
            and "action=incoming_sms" in action.findtext("Str[@sr='arg5']", default="")
        ]
        assert len(inbound_bodies) == 3
        assert all("message=%transport_message" in body for body in inbound_bodies)
        assert all("phone=%transport_phone" in body for body in inbound_bodies)
        assert all("received_at=%transport_received_at" in body for body in inbound_bodies)
        assert all("message_id=%transport_message_id" in body for body in inbound_bodies)
        assert all("%SMSRB" not in body and "%evtprm3" not in body for body in inbound_bodies)


def test_template_contains_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
