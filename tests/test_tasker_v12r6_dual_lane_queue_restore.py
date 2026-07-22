from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
RESTORE = ROOT / "tasker" / "TASKER_RESTORE_V12R6_DUAL_LANE_QUEUE_FULL.template.xml"
VERSION = "V12R6_DUAL_LANE_QUEUE"


def _root() -> ET.Element:
    return ET.parse(RESTORE).getroot()


def _tasks(root: ET.Element):
    return {task.findtext("id"): task for task in root.findall("Task")}


def _profiles(root: ET.Element):
    return {profile.findtext("id"): profile for profile in root.findall("Profile")}


def _http_bodies(task: ET.Element):
    return [
        action.findtext("Str[@sr='arg5']", default="")
        for action in task.findall("Action")
        if action.findtext("code") == "339"
    ]


def test_restore_contains_both_delivery_lanes_and_all_tasks_are_populated():
    root = _root()
    tasks = _tasks(root)
    profiles = _profiles(root)
    assert set(tasks) == {"3", "9", "21", "30", "33", "34"}
    assert set(profiles) == {"1", "2", "20", "30", "31", "34", "35"}
    assert {profile.findtext("mid0") for profile in profiles.values()} <= set(tasks)
    assert all(task.findall("Action") for task in tasks.values())


def test_bulk_outreach_lane_runs_each_autoremote_event_concurrently():
    outbound = _tasks(_root())["21"]
    assert outbound.findtext("rty") == "2"
    xml = ET.tostring(outbound, encoding="unicode")
    assert "%arcomm" in xml
    assert "%sms_phone" in xml
    assert "%sms_text" in xml
    assert VERSION in xml
    sends = [a for a in outbound.findall("Action") if a.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%sms_phone"


def test_inbound_lanes_only_enqueue_and_preserve_event_identity():
    tasks = _tasks(_root())
    expected_sources = {"3": ("%SMSRF", "%SMSRB"), "9": ("%evtprm2", "%evtprm3")}
    for task_id, (phone_source, message_source) in expected_sources.items():
        task = tasks[task_id]
        actions = task.findall("Action")
        assert task.findtext("rty") == "2"
        assert actions[0].findtext("Str[@sr='arg0']") == "%inbound_phone"
        assert actions[0].findtext("Str[@sr='arg1']") == phone_source
        assert actions[1].findtext("Str[@sr='arg0']") == "%inbound_message"
        assert actions[1].findtext("Str[@sr='arg1']") == message_source
        bodies = _http_bodies(task)
        assert len([body for body in bodies if "action=enqueue_incoming_sms" in body]) == 3
        assert not any("action=incoming_sms" in body for body in bodies)
        agent_sends = [
            action for action in actions
            if action.findtext("code") == "41"
            and action.findtext("Str[@sr='arg0']") != "9542053205"
        ]
        assert agent_sends == []


def test_chatbot_reply_worker_is_single_flight_and_receipt_correlated():
    root = _root()
    tasks = _tasks(root)
    profiles = _profiles(root)
    dispatcher = tasks["30"]
    assert dispatcher.findtext("rty") == "0"
    assert profiles["30"].findtext("nme") == "SMS Outbox Every Minute"
    assert profiles["30"].findtext("Time/rep") == "2"
    assert profiles["30"].findtext("Time/repval") == "1"
    bodies = _http_bodies(dispatcher)
    assert len([body for body in bodies if "action=claim_pending_send" in body]) == 3
    assert len([body for body in bodies if "action=send_started" in body]) == 3
    assert len([body for body in bodies if "action=reply_sent" in body]) == 3
    sends = [a for a in dispatcher.findall("Action") if a.findtext("code") == "41"]
    assert len(sends) == 1
    assert sends[0].findtext("Str[@sr='arg0']") == "%SMSOUT_PHONE"
    assert sends[0].findtext("Str[@sr='arg1']") == "%SMSOUT_REPLY"
    serialized = ET.tostring(dispatcher, encoding="unicode")
    for value in ("%SMSOUT_REQUEST", "%SMSOUT_MESSAGE", "%SMSOUT_PHONE", "%SMSOUT_REPLY", "%SMSOUT_LEASE"):
        assert value in serialized


def test_missed_inbound_reconciler_and_watchdog_heartbeat_are_present():
    root = _root()
    tasks = _tasks(root)
    profiles = _profiles(root)
    assert profiles["34"].findtext("Time/repval") == "1"
    assert "action=enqueue_incoming_sms" in ET.tostring(tasks["33"], encoding="unicode")
    assert profiles["35"].findtext("Time/repval") == "5"
    assert "action=tasker_heartbeat" in ET.tostring(tasks["34"], encoding="unicode")


def test_parser_safe_xml_and_project_membership_are_complete():
    root = _root()
    tasks = _tasks(root)
    profiles = _profiles(root)
    project = root.find("Project")
    assert set(project.findtext("pids").split(",")) == set(profiles)
    assert set(project.findtext("tids").split(",")) == set(tasks)
    for task in tasks.values():
        actions = task.findall("Action")
        assert [action.get("sr") for action in actions] == [
            f"act{index}" for index in range(len(actions))
        ]
    for node in root.iter():
        if "sr" in node.attrib:
            assert next(iter(node.attrib)) == "sr"


def test_template_is_secret_free():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
