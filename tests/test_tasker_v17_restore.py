from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V17_REDUNDANT_INBOUND_RECOVERY_FULL.template.xml"
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


def test_v17_is_a_complete_fresh_restore_with_nonempty_tasks():
    profiles = _profiles()
    tasks = _tasks()
    assert set(profiles) == {"171", "172", "173", "174", "175", "176", "177"}
    assert set(tasks) == {"171", "172", "173", "174", "176", "177"}
    assert all((item.findtext("nme") or "").startswith("V17 ") for item in profiles.values())
    assert all((item.findtext("nme") or "").startswith("V17 ") for item in tasks.values())
    assert all(profile.findtext("mid0") in tasks for profile in profiles.values())
    assert all(task.findall("Action") for task in tasks.values())


def test_v17_project_membership_and_native_root_order_are_complete():
    root = _root()
    tags = [child.tag for child in root]
    project_index = tags.index("Project")
    assert all(tag == "Profile" for tag in tags[:project_index])
    assert all(tag == "Task" for tag in tags[project_index + 1 :])
    project = root.find("Project")
    assert project.findtext("name") == "SMS Bot V17"
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v17_claim_and_heartbeat_piggyback_latest_inbound_snapshot():
    tasks = _tasks()
    for task_id, action in (("174", "claim_pending_send"), ("177", "tasker_heartbeat")):
        bodies = [body for body in _http_bodies(tasks[task_id]) if f"action={action}" in body]
        assert len(bodies) == 3
        for body in bodies:
            assert "snapshot_phone=%snapshot_phone" in body
            assert "snapshot_message=%snapshot_message" in body
            assert "snapshot_received_at=%snapshot_received_at" in body
            assert "snapshot_message_id=%snapshot_message_id" in body
        encoded_destinations = {
            action.findtext("Str[@sr='arg2']", default="")
            for action in tasks[task_id].findall("Action")
            if action.findtext("code") == "596"
        }
        assert {"%snapshot_phone", "%snapshot_message", "%snapshot_received_at", "%snapshot_message_id"} <= encoded_destinations


def test_v17_all_inbound_posts_identify_v17_and_template_has_no_private_token():
    for task_id in ("171", "172", "176", "177"):
        bodies = _http_bodies(_tasks()[task_id])
        assert bodies
        assert all("transport_version=17" in body for body in bodies)
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
