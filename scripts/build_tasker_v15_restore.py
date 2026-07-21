#!/usr/bin/env python3
"""Build V15 with fresh Tasker IDs so stale restored tasks cannot be reused."""

from __future__ import annotations

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET

from build_tasker_v10_restore import TOKEN_PLACEHOLDER, build_restore, extract_token


PROFILE_ID_MAP = {"30": "150", "31": "151", "34": "154", "35": "155"}
TASK_ID_MAP = {"30": "150", "33": "154", "34": "155"}


def remap_transport_components(path: Path) -> None:
    tree = ET.parse(path)
    root = tree.getroot()

    for profile in root.findall("Profile"):
        old_id = profile.findtext("id")
        if old_id not in PROFILE_ID_MAP:
            continue
        new_id = PROFILE_ID_MAP[old_id]
        profile.set("sr", f"prof{new_id}")
        profile.find("id").text = new_id
        task_id = profile.findtext("mid0")
        if task_id in TASK_ID_MAP:
            profile.find("mid0").text = TASK_ID_MAP[task_id]
        name = profile.findtext("nme") or ""
        profile.find("nme").text = "V15 " + name

    for task in root.findall("Task"):
        old_id = task.findtext("id")
        if old_id not in TASK_ID_MAP:
            continue
        new_id = TASK_ID_MAP[old_id]
        task.set("sr", f"task{new_id}")
        task.find("id").text = new_id
        name = task.findtext("nme") or ""
        task.find("nme").text = "V15 " + name

    project = root.find("Project")
    if project is None:
        raise ValueError("Tasker restore has no Project element")
    project.find("pids").text = ",".join(
        profile.findtext("id") for profile in root.findall("Profile")
    )
    project.find("tids").text = ",".join(
        task.findtext("id") for task in root.findall("Task")
    )

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
        transport_version=15,
        include_reconciler=True,
        permission_safe_receipts=True,
        assign_project_membership=True,
        include_heartbeat=True,
        reconciler_minutes=2,
    )
    remap_transport_components(args.template_output)

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
