from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V11_BURST_RECOVERY_FULL.template.xml"
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


def test_v11_contains_full_v10_restore_plus_reconciler():
    tasks = _tasks()
    profiles = _profiles()
    assert {"3", "9", "21", "30", "31", "32", "33"}.issubset(tasks)
    assert {profile.findtext("mid0") for profile in profiles.values()} <= set(tasks)
    assert profiles["34"].findtext("nme") == "SMS Last Message Reconciler"
    assert profiles["34"].findtext("Time/rep") == "1"
    assert profiles["34"].findtext("mid0") == "33"


def test_reconciler_reposts_last_sms_and_only_forwards_newly_recovered_messages():
    task = _tasks()["33"]
    assert task.findtext("rty") == "0"
    assert task.findtext("stayawake") == "true"
    serialized = ET.tostring(task, encoding="unicode")
    assert "%SMSRF" in serialized
    assert "%SMSRB" in serialized
    assert "%SMSRD" in serialized
    assert "%SMSRT" in serialized
    assert "%SMSBOT_RECONCILE_KEY" in serialized
    assert "transport_version=11" in serialized
    assert "action=enqueue_incoming_sms" in serialized
    sends = [action for action in task.findall("Action") if action.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "9542053205"
    assert "%http_data.queued" in serialized


def test_http_retries_are_driven_by_non_2xx_status_not_a_literal_empty_regex():
    for task_id in ("3", "9", "30", "31", "32", "33"):
        task = _tasks()[task_id]
        serialized = ET.tostring(task, encoding="unicode")
        assert "<rhs>^$</rhs>" not in serialized
        for condition in task.findall(".//Condition"):
            if condition.findtext("lhs") == "%http_response_code":
                assert condition.findtext("op") == "3"
                assert condition.findtext("rhs") == "2*"


def test_all_inbound_paths_use_v11_queue_and_autoremote_accepts_collisions():
    tasks = _tasks()
    for task_id in ("3", "9", "33"):
        bodies = _http_bodies(tasks[task_id])
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert all("transport_version=11" in body for body in bodies)

    assert tasks["21"].findtext("rty") == "2"
    assert tasks["21"].findtext("stayawake") == "true"


def test_v11_template_has_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
