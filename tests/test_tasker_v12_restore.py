from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V12_PERMISSION_SAFE_FULL.template.xml"
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


def test_v12_assigns_every_profile_and_task_to_the_project():
    root = _root()
    project = root.find("Project")
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v12_removes_permission_gated_sms_result_events():
    profiles = _profiles()
    tasks = _tasks()
    assert {profile.findtext("Event/code") for profile in profiles.values()} & {"2005", "2010"} == set()
    assert "SMS Outbox Send Success" not in {p.findtext("nme") for p in profiles.values()}
    assert "SMS Outbox Send Failure" not in {p.findtext("nme") for p in profiles.values()}
    assert "31" not in tasks
    assert "32" not in tasks


def test_v12_dispatcher_sends_and_posts_correlated_receipt_inline():
    task = _tasks()["30"]
    bodies = _http_bodies(task)
    assert len([b for b in bodies if "action=claim_pending_send" in b]) == 3
    assert len([b for b in bodies if "action=send_started" in b]) == 3
    assert len([b for b in bodies if "action=reply_sent" in b]) == 3
    sends = [a for a in task.findall("Action") if a.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"
    assert sends[0].find("Int[@sr='arg4']").get("val") == "0"


def test_v12_keeps_realtime_and_one_minute_recovery_paths():
    tasks = _tasks()
    profiles = _profiles()
    assert tasks["3"].findtext("rty") == "2"
    assert tasks["9"].findtext("rty") == "2"
    assert profiles["34"].findtext("Time/rep") == "1"
    assert profiles["34"].findtext("mid0") == "33"
    for task_id in ("3", "9", "33"):
        bodies = _http_bodies(tasks[task_id])
        assert len([b for b in bodies if "action=enqueue_incoming_sms" in b]) == 3
        assert all("transport_version=12" in b for b in bodies)


def test_v12_template_has_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
