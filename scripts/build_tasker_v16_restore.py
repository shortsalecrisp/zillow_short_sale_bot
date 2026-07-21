#!/usr/bin/env python3
"""Build a canonical V16 Tasker restore with fresh IDs for every component."""

from __future__ import annotations

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET

from build_tasker_v10_restore import (
    TOKEN_PLACEHOLDER,
    build_restore,
    canonicalize_root_order,
    extract_token,
)


# Remap every profile and task, including the real-time inbound paths. This
# prevents Tasker from merging the restore into a previously imported blank
# task that happens to have the same ID or name.
PROFILE_ID_MAP = {
    "1": "161",
    "2": "162",
    "20": "163",
    "30": "164",
    "31": "165",
    "34": "166",
    "35": "167",
}
TASK_ID_MAP = {
    "3": "161",
    "9": "162",
    "21": "163",
    "30": "164",
    "33": "166",
    "34": "167",
}


def remap_all_components(path: Path) -> None:
    tree = ET.parse(path)
    root = tree.getroot()

    for profile in root.findall("Profile"):
        old_id = profile.findtext("id")
        if old_id not in PROFILE_ID_MAP:
            raise ValueError(f"Unexpected profile ID in V16 source: {old_id}")
        new_id = PROFILE_ID_MAP[old_id]
        profile.set("sr", f"prof{new_id}")
        profile.find("id").text = new_id
        task_id = profile.findtext("mid0")
        if task_id not in TASK_ID_MAP:
            raise ValueError(f"Unexpected profile task ID in V16 source: {task_id}")
        profile.find("mid0").text = TASK_ID_MAP[task_id]
        name = profile.findtext("nme") or ""
        profile.find("nme").text = "V16 " + name.removeprefix("V15 ")

    for task in root.findall("Task"):
        old_id = task.findtext("id")
        if old_id not in TASK_ID_MAP:
            raise ValueError(f"Unexpected task ID in V16 source: {old_id}")
        new_id = TASK_ID_MAP[old_id]
        task.set("sr", f"task{new_id}")
        task.find("id").text = new_id
        name = task.findtext("nme") or ""
        task.find("nme").text = "V16 " + name.removeprefix("V15 ")

    project = root.find("Project")
    if project is None:
        raise ValueError("Tasker restore has no Project element")
    project.find("name").text = "SMS Bot V16"
    project.find("pids").text = ",".join(
        profile.findtext("id") for profile in root.findall("Profile")
    )
    project.find("tids").text = ",".join(
        task.findtext("id") for task in root.findall("Task")
    )

    canonicalize_root_order(root)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="UTF-8", xml_declaration=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("template_output", type=Path)
    parser.add_argument("--private-source", type=Path)
    parser.add_argument("--private-output", type=Path)
    args = parser.parse_args()

    build_restore(
        args.source,
        args.template_output,
        transport_version=16,
        include_reconciler=True,
        permission_safe_receipts=True,
        assign_project_membership=True,
        include_heartbeat=True,
        reconciler_minutes=2,
    )
    remap_all_components(args.template_output)

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
