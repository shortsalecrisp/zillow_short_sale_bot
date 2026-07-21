from pathlib import Path
import xml.etree.ElementTree as ET


RESTORE = (
    Path(__file__).resolve().parents[1]
    / "tasker"
    / "TASKER_RESTORE_V9R1_STABLE_CORRELATION_FULL.template.xml"
)


def _root():
    return ET.parse(RESTORE).getroot()


def _tasks():
    return {task.findtext("id"): task for task in _root().findall("Task")}


def test_v9r1_preserves_exact_v9_component_set_and_root_order():
    root = _root()
    assert [child.tag for child in root] == [
        "Profile",
        "Profile",
        "Profile",
        "Project",
        "Task",
        "Task",
        "Task",
    ]
    assert {profile.findtext("id") for profile in root.findall("Profile")} == {
        "1",
        "2",
        "20",
    }
    assert set(_tasks()) == {"3", "9", "21"}


def test_v9r1_keeps_all_working_actions_populated():
    assert {task_id: len(task.findall("Action")) for task_id, task in _tasks().items()} == {
        "3": 52,
        "9": 59,
        "21": 5,
    }


def test_v9r1_snapshots_phone_and_message_before_any_other_action():
    expected = {
        "3": ("%SMSRF", "%SMSRB"),
        "9": ("%evtprm2", "%evtprm3"),
    }
    for task_id, (phone_source, message_source) in expected.items():
        actions = _tasks()[task_id].findall("Action")
        assert actions[0].findtext("Str[@sr='arg0']") == "%inbound_phone"
        assert actions[0].findtext("Str[@sr='arg1']") == phone_source
        assert actions[1].findtext("Str[@sr='arg0']") == "%inbound_message"
        assert actions[1].findtext("Str[@sr='arg1']") == message_source


def test_v9r1_never_rereads_live_event_identity_after_snapshot():
    expected = {"3": ("%SMSRF", "%SMSRB"), "9": ("%evtprm2", "%evtprm3")}
    for task_id, sources in expected.items():
        remaining = "".join(
            ET.tostring(action, encoding="unicode")
            for action in _tasks()[task_id].findall("Action")[2:]
        )
        assert all(source not in remaining for source in sources)
        assert "%inbound_phone" in remaining
        assert "%inbound_message" in remaining


def test_v9r1_queues_overlapping_copies_of_all_three_tasks():
    assert all(task.findtext("rty") == "2" for task in _tasks().values())


def test_v9r1_reply_destination_comes_from_snapshotted_phone():
    for task_id in ("3", "9"):
        reply_phone_values = [
            action.findtext("Str[@sr='arg1']")
            for action in _tasks()[task_id].findall("Action")
            if action.findtext("code") == "547"
            and action.findtext("Str[@sr='arg0']") == "%reply_phone"
        ]
        assert reply_phone_values == ["%inbound_phone"]


def test_v9r1_template_has_placeholder_and_no_private_token():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
