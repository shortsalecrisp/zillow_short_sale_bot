from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V15_FRESH_IDS_FULL.template.xml"
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


def test_v15_uses_fresh_ids_for_every_added_transport_component():
    profiles = _profiles()
    tasks = _tasks()
    assert {"150", "151", "154", "155"} <= set(profiles)
    assert {"150", "154", "155"} <= set(tasks)
    assert not ({"30", "31", "34", "35"} & set(profiles))
    assert not ({"30", "33", "34"} & set(tasks))
    assert profiles["150"].findtext("mid0") == "150"
    assert profiles["151"].findtext("mid0") == "150"
    assert profiles["154"].findtext("mid0") == "154"
    assert profiles["155"].findtext("mid0") == "155"


def test_v15_assigns_every_component_to_project():
    root = _root()
    project = root.find("Project")
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v15_tasks_do_not_use_parser_breaking_stayawake_element():
    for task in _tasks().values():
        assert task.find("stayawake") is None
        assert len(task.findall("Action")) > 0


def test_v15_heartbeat_task_is_not_blank_and_uses_valid_timer():
    profiles = _profiles()
    tasks = _tasks()
    heartbeat = tasks["155"]
    assert heartbeat.findtext("nme") == "V15 Tasker Transport Heartbeat"
    assert len(heartbeat.findall("Action")) == 14
    bodies = _http_bodies(heartbeat)
    assert len([body for body in bodies if "action=tasker_heartbeat" in body]) == 3
    assert all("transport_version=15" in body for body in bodies)
    timer = profiles["155"].find("Time")
    assert timer.findtext("rep") == "2"
    assert timer.findtext("repval") == "5"


def test_v15_dispatcher_and_reconciler_are_populated():
    tasks = _tasks()
    assert len(tasks["150"].findall("Action")) == 63
    assert len(tasks["154"].findall("Action")) == 31
    dispatcher_sends = [
        action for action in tasks["150"].findall("Action") if action.findtext("code") == "41"
    ]
    assert len(dispatcher_sends) == 1
    assert dispatcher_sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert dispatcher_sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"


def test_v15_keeps_realtime_sms_and_notification_tasks():
    tasks = _tasks()
    profiles = _profiles()
    assert profiles["1"].findtext("mid0") == "3"
    assert profiles["2"].findtext("mid0") == "9"
    for task_id in ("3", "9", "154"):
        bodies = _http_bodies(tasks[task_id])
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert all("transport_version=15" in body for body in bodies)


def test_v15_template_has_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
