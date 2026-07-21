#!/usr/bin/env python3
"""Patch the confirmed V9 restore without adding profiles or tasks."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import xml.etree.ElementTree as ET


TOKEN_PLACEHOLDER = "__SMS_BOT_TOKEN__"


def variable_set(name: str, value: str) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "547"
    ET.SubElement(action, "Str", {"sr": "arg0", "ve": "3"}).text = name
    ET.SubElement(action, "Str", {"sr": "arg1", "ve": "3"}).text = value
    for index, val in enumerate((0, 0, 0, 3, 1), start=2):
        ET.SubElement(action, "Int", {"sr": f"arg{index}", "val": str(val)})
    return action


def insert_after_task_metadata(task: ET.Element, actions: list[ET.Element]) -> None:
    first_action = task.find("Action")
    if first_action is None:
        raise ValueError(f"Task {task.findtext('id')} has no actions")
    insert_at = list(task).index(first_action)
    for offset, action in enumerate(actions):
        task.insert(insert_at + offset, action)


def replace_task_variable(task: ET.Element, source: str, destination: str) -> None:
    for action in task.findall("Action")[2:]:
        for node in action.iter():
            if node.text and source in node.text:
                node.text = node.text.replace(source, destination)


def snapshot_inbound_identity(
    task: ET.Element,
    *,
    phone_source: str,
    message_source: str,
) -> None:
    insert_after_task_metadata(
        task,
        [
            variable_set("%inbound_phone", phone_source),
            variable_set("%inbound_message", message_source),
        ],
    )
    replace_task_variable(task, phone_source, "%inbound_phone")
    replace_task_variable(task, message_source, "%inbound_message")

    retry_type = task.find("rty")
    if retry_type is None:
        retry_type = ET.Element("rty")
        priority = task.find("pri")
        insert_at = list(task).index(priority) + 1 if priority is not None else 0
        task.insert(insert_at, retry_type)
    retry_type.text = "2"


def patch_restore(source: Path, destination: Path) -> None:
    tree = ET.parse(source)
    root = tree.getroot()
    tasks = {task.findtext("id"): task for task in root.findall("Task")}
    if set(tasks) != {"3", "9", "21"}:
        raise ValueError(f"V9R1 source must contain only V9 tasks: {set(tasks)}")

    snapshot_inbound_identity(
        tasks["3"],
        phone_source="%SMSRF",
        message_source="%SMSRB",
    )
    snapshot_inbound_identity(
        tasks["9"],
        phone_source="%evtprm2",
        message_source="%evtprm3",
    )

    # V9 left the AutoRemote outbound task at Tasker's default collision mode,
    # which can discard an overlapping send event. Use the same queued-copy
    # mode that the already-working inbound V9 tasks use.
    outbound = tasks["21"]
    retry_type = outbound.find("rty")
    if retry_type is None:
        retry_type = ET.Element("rty")
        priority = outbound.find("pri")
        insert_at = list(outbound).index(priority) + 1 if priority is not None else 0
        outbound.insert(insert_at, retry_type)
    retry_type.text = "2"

    for task in root.findall("Task"):
        for index, action in enumerate(task.findall("Action")):
            action.set("sr", f"act{index}")

    # Preserve V9's native top-level layout exactly: Profiles, Project, Tasks.
    tags = [child.tag for child in root]
    if tags != ["Profile", "Profile", "Profile", "Project", "Task", "Task", "Task"]:
        raise ValueError(f"Unexpected V9 root layout: {tags}")

    ET.indent(tree, space="  ")
    destination.parent.mkdir(parents=True, exist_ok=True)
    tree.write(destination, encoding="UTF-8", xml_declaration=True)


def extract_token(private_source: Path) -> str:
    text = private_source.read_text(encoding="utf-8")
    match = re.search(
        r'<Str sr="arg0" ve="3">%token</Str>\s*'
        r'<Str sr="arg1" ve="3">([^<]+)</Str>',
        text,
    )
    if not match:
        raise ValueError(f"Could not locate Tasker token in {private_source}")
    return match.group(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("template_output", type=Path)
    parser.add_argument("--private-source", type=Path)
    parser.add_argument("--private-output", type=Path)
    args = parser.parse_args()

    patch_restore(args.source, args.template_output)
    if bool(args.private_source) != bool(args.private_output):
        parser.error("--private-source and --private-output must be used together")
    if args.private_source and args.private_output:
        token = extract_token(args.private_source)
        private_text = args.template_output.read_text(encoding="utf-8").replace(
            TOKEN_PLACEHOLDER,
            token,
        )
        if TOKEN_PLACEHOLDER in private_text:
            raise ValueError("Tasker token placeholder was not fully replaced")
        args.private_output.write_text(private_text, encoding="utf-8")


if __name__ == "__main__":
    main()
