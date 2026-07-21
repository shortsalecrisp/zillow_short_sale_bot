from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V16_CANONICAL_FRESH_FULL.template.xml"
)


def _root():
    return ET.parse(RESTORE).getroot()


def _tasks():
    return {task.findtext("id"): task for task in _root().findall("Task")}


def _profiles():
    return {
        profile.findtext("id"): profile for profile in _root().findall("Profile")
    }


def _http_bodies(task):
    return [
        action.findtext("Str[@sr='arg5']", default="")
        for action in task.findall("Action")
        if action.findtext("code") == "339"
    ]


def test_v16_uses_tasker_native_root_section_order():
    tags = [child.tag for child in _root()]
    project_index = tags.index("Project")
    assert all(tag == "Profile" for tag in tags[:project_index])
    assert all(tag == "Task" for tag in tags[project_index + 1 :])


def test_v16_gives_every_profile_and_task_a_fresh_id_and_name():
    profiles = _profiles()
    tasks = _tasks()
    assert set(profiles) == {"161", "162", "163", "164", "165", "166", "167"}
    assert set(tasks) == {"161", "162", "163", "164", "166", "167"}
    assert all((item.findtext("nme") or "").startswith("V16 ") for item in profiles.values())
    assert all((item.findtext("nme") or "").startswith("V16 ") for item in tasks.values())


def test_v16_every_profile_points_to_a_real_fresh_task():
    tasks = _tasks()
    for profile in _profiles().values():
        assert profile.findtext("mid0") in tasks


def test_v16_project_membership_exactly_matches_restore_components():
    root = _root()
    project = root.find("Project")
    assert project.findtext("name") == "SMS Bot V16"
    assert set(project.findtext("pids").split(",")) == set(_profiles())
    assert set(project.findtext("tids").split(",")) == set(_tasks())


def test_v16_every_task_has_actions_and_native_top_level_fields_only():
    expected_counts = {
        "161": 29,
        "162": 36,
        "163": 5,
        "164": 63,
        "166": 31,
        "167": 14,
    }
    for task_id, task in _tasks().items():
        assert len(task.findall("Action")) == expected_counts[task_id]
        assert task.find("stayawake") is None


def test_v16_all_transport_posts_identify_v16():
    for task_id in ("161", "162", "166", "167"):
        bodies = _http_bodies(_tasks()[task_id])
        assert bodies
        assert all("transport_version=16" in body for body in bodies)


def test_v16_timers_use_tasker_minute_unit_and_expected_intervals():
    profiles = _profiles()
    for profile_id, interval in {"164": "2", "166": "2", "167": "5"}.items():
        timer = profiles[profile_id].find("Time")
        assert timer.findtext("rep") == "2"
        assert timer.findtext("repval") == interval


def test_v16_template_contains_placeholder_not_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
