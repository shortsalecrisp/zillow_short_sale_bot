from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
RESTORE = ROOT / "tasker" / "TASKER_RESTORE_V12R5_CONCURRENT_OUTBOUND_FULL.template.xml"
VERSION = "V12R5_CONCURRENT_OUTBOUND"


def _root() -> ET.Element:
    return ET.parse(RESTORE).getroot()


def _task(root: ET.Element, task_id: str) -> ET.Element:
    return next(task for task in root.findall("Task") if task.findtext("id") == task_id)


def test_outbound_task_runs_concurrent_events_without_dropping_new_sms():
    root = _root()
    outbound = _task(root, "21")
    assert outbound.findtext("rty") == "2"
    assert len(outbound.findall("Action")) == 12
    xml = ET.tostring(outbound, encoding="unicode")
    assert "%arcomm" in xml
    assert "%sms_phone" in xml
    assert "%sms_text" in xml
    assert VERSION in xml


def test_inbound_tasks_and_parser_safe_attributes_are_preserved():
    root = _root()
    assert len(_task(root, "3").findall("Action")) == 50
    assert len(_task(root, "9").findall("Action")) == 57
    for node in root.iter():
        if "sr" in node.attrib:
            assert next(iter(node.attrib)) == "sr"


def test_template_is_secret_free():
    text = RESTORE.read_text(encoding="utf-8")
    assert "__SMS_BOT_TOKEN__" in text
    assert "h7Q2zLp9Xk3mC8aF" not in text
