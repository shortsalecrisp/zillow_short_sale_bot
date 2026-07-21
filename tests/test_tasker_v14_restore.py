from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V14_VALID_TIMERS_FULL.template.xml"
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


def test_v14_assigns_every_component_to_project():
    root = _root()
    project = root.find("Project")
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v14_repeating_time_contexts_use_tasker_schema():
    profiles = _profiles()
    expected = {"30": "2", "34": "2", "35": "5"}
    for profile_id, minutes in expected.items():
        timer = profiles[profile_id].find("Time")
        assert timer is not None
        assert timer.findtext("fh") == "-1"
        assert timer.findtext("fm") == "-1"
        assert timer.findtext("th") == "-1"
        assert timer.findtext("tm") == "-1"
        assert timer.findtext("rep") == "2"
        assert timer.findtext("repval") == minutes


def test_v14_preserves_all_three_inbound_recovery_paths():
    tasks = _tasks()
    profiles = _profiles()
    assert profiles["1"].findtext("mid0") == "3"
    assert profiles["2"].findtext("mid0") == "9"
    assert profiles["34"].findtext("mid0") == "33"
    for task_id in ("3", "9", "33"):
        bodies = _http_bodies(tasks[task_id])
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert all("transport_version=14" in body for body in bodies)


def test_v14_has_live_heartbeat_and_permission_safe_dispatch():
    tasks = _tasks()
    profiles = _profiles()
    assert profiles["35"].findtext("mid0") == "34"
    heartbeat_bodies = _http_bodies(tasks["34"])
    assert len([body for body in heartbeat_bodies if "action=tasker_heartbeat" in body]) == 3
    assert all("transport_version=14" in body for body in heartbeat_bodies)
    assert {profile.findtext("Event/code") for profile in profiles.values()} & {"2005", "2010"} == set()


def test_v14_dispatcher_uses_claimed_phone_and_reply_only():
    task = _tasks()["30"]
    sends = [action for action in task.findall("Action") if action.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"


def test_v14_template_has_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
