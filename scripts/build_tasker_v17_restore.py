#!/usr/bin/env python3
"""Build V17 with redundant inbound recovery on active transport requests."""

from __future__ import annotations

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET

from build_tasker_v10_restore import (
    TOKEN_PLACEHOLDER,
    canonicalize_root_order,
    dispatcher_actions_inline_receipt,
    extract_token,
    heartbeat_actions,
    notification_inbound_actions,
    primary_inbound_actions,
    reconcile_last_sms_actions,
    replace_task,
)


PROFILE_ID_MAP = {
    "161": "171",
    "162": "172",
    "163": "173",
    "164": "174",
    "165": "175",
    "166": "176",
    "167": "177",
}
TASK_ID_MAP = {
    "161": "171",
    "162": "172",
    "163": "173",
    "164": "174",
    "166": "176",
    "167": "177",
}


def build_v17(source: Path, destination: Path) -> None:
    tree = ET.parse(source)
    root = tree.getroot()
    tasks = {task.findtext("id"): task for task in root.findall("Task")}

    replace_task(tasks["161"], primary_inbound_actions(transport_version=17), collision=2, stay_awake=True)
    replace_task(tasks["162"], notification_inbound_actions(transport_version=17), collision=2, stay_awake=True)
    replace_task(tasks["164"], dispatcher_actions_inline_receipt(), collision=0, stay_awake=False)
    replace_task(tasks["166"], reconcile_last_sms_actions(transport_version=17), collision=0, stay_awake=False)
    replace_task(tasks["167"], heartbeat_actions(transport_version=17), collision=1, stay_awake=False)

    for profile in root.findall("Profile"):
        old_id = profile.findtext("id")
        if old_id not in PROFILE_ID_MAP:
            raise ValueError(f"Unexpected profile ID in V17 source: {old_id}")
        new_id = PROFILE_ID_MAP[old_id]
        profile.set("sr", f"prof{new_id}")
        profile.find("id").text = new_id
        profile.find("mid0").text = TASK_ID_MAP[profile.findtext("mid0")]
        profile.find("nme").text = "V17 " + (profile.findtext("nme") or "").removeprefix("V16 ")

    for task in root.findall("Task"):
        old_id = task.findtext("id")
        if old_id not in TASK_ID_MAP:
            raise ValueError(f"Unexpected task ID in V17 source: {old_id}")
        new_id = TASK_ID_MAP[old_id]
        task.set("sr", f"task{new_id}")
        task.find("id").text = new_id
        task.find("nme").text = "V17 " + (task.findtext("nme") or "").removeprefix("V16 ")

    project = root.find("Project")
    if project is None:
        raise ValueError("Tasker restore has no Project element")
    project.find("name").text = "SMS Bot V17"
    project.find("pids").text = ",".join(profile.findtext("id") for profile in root.findall("Profile"))
    project.find("tids").text = ",".join(task.findtext("id") for task in root.findall("Task"))

    canonicalize_root_order(root)
    ET.indent(tree, space="  ")
    destination.parent.mkdir(parents=True, exist_ok=True)
    tree.write(destination, encoding="UTF-8", xml_declaration=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("template_output", type=Path)
    parser.add_argument("--private-source", type=Path)
    parser.add_argument("--private-output", type=Path)
    args = parser.parse_args()

    build_v17(args.source, args.template_output)
    if bool(args.private_source) != bool(args.private_output):
        parser.error("--private-source and --private-output must be used together")
    if args.private_source and args.private_output:
        token = extract_token(args.private_source)
        private_text = args.template_output.read_text(encoding="utf-8").replace(TOKEN_PLACEHOLDER, token)
        if TOKEN_PLACEHOLDER in private_text:
            raise ValueError("Tasker token placeholder was not fully replaced")
        args.private_output.write_text(private_text, encoding="utf-8")


if __name__ == "__main__":
    main()
