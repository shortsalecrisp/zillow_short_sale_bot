from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V13_LIVE_HEARTBEAT_FULL.template.xml"
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


def test_v13_assigns_every_profile_and_task_to_project():
    root = _root()
    project = root.find("Project")
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v13_has_no_permission_gated_sms_result_events():
    profiles = _profiles()
    assert {profile.findtext("Event/code") for profile in profiles.values()} & {"2005", "2010"} == set()


def test_v13_keeps_realtime_notification_and_recovery_ingress():
    tasks = _tasks()
    profiles = _profiles()
    assert profiles["1"].findtext("mid0") == "3"
    assert profiles["2"].findtext("mid0") == "9"
    assert profiles["34"].findtext("mid0") == "33"
    assert profiles["34"].findtext("Time/rep") == "2"
    for task_id in ("3", "9", "33"):
        bodies = _http_bodies(tasks[task_id])
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert all("transport_version=13" in body for body in bodies)


def test_v13_heartbeat_proves_imported_transport_is_running():
    tasks = _tasks()
    profiles = _profiles()
    assert profiles["35"].findtext("mid0") == "34"
    assert profiles["35"].findtext("Time/rep") == "5"
    bodies = _http_bodies(tasks["34"])
    assert len([body for body in bodies if "action=tasker_heartbeat" in body]) == 3
    assert all("transport_version=13" in body for body in bodies)


def test_v13_dispatcher_is_single_destination_and_inline_receipt():
    task = _tasks()["30"]
    sends = [action for action in task.findall("Action") if action.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"
    bodies = _http_bodies(task)
    assert len([body for body in bodies if "action=reply_sent" in body]) == 3


def test_v13_template_contains_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
