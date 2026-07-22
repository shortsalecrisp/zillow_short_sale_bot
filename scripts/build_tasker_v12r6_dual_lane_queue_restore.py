#!/usr/bin/env python3
"""Build the parser-safe dual-lane Tasker transport.

Initial outreach and follow-ups stay on the concurrent AutoRemote task. Agent
replies are durably enqueued in Apps Script and chatbot responses are drained
one at a time by the receipt-correlated outbox worker.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET

from build_tasker_v10_restore import TOKEN_PLACEHOLDER, build_restore, extract_token


OLD_VERSION = "V12R4_FINAL_PARSER_SAFE"
TRANSPORT_VERSION = "V12R6_DUAL_LANE_QUEUE"
QUEUE_VERSION = 16


def _replace_text(root: ET.Element, old: str, new: str) -> None:
    for node in root.iter():
        if node.text and old in node.text:
            node.text = node.text.replace(old, new)


def _put_sr_first(root: ET.Element) -> None:
    for node in root.iter():
        if "sr" not in node.attrib:
            continue
        ordered = {"sr": node.attrib["sr"]}
        ordered.update((key, value) for key, value in node.attrib.items() if key != "sr")
        node.attrib.clear()
        node.attrib.update(ordered)


def patch_restore(source: Path, destination: Path) -> None:
    build_restore(
        source,
        destination,
        transport_version=QUEUE_VERSION,
        include_reconciler=True,
        permission_safe_receipts=True,
        assign_project_membership=True,
        include_heartbeat=True,
        reconciler_minutes=1,
    )

    tree = ET.parse(destination)
    root = tree.getroot()
    _replace_text(root, OLD_VERSION, TRANSPORT_VERSION)

    profiles = {profile.findtext("id"): profile for profile in root.findall("Profile")}
    tasks = {task.findtext("id"): task for task in root.findall("Task")}

    # Poll one queued chatbot reply each minute. A Tasker collision mode of 0
    # keeps this worker single-flight even if boot and timer events overlap.
    profiles["30"].find("nme").text = "SMS Outbox Every Minute"
    profiles["30"].find("Time/repval").text = "1"
    tasks["30"].find("rty").text = "0"

    # Bulk outreach remains independent from chatbot response delivery. Every
    # AutoRemote event gets its own local phone/text variables and task run.
    tasks["21"].find("rty").text = "2"

    _put_sr_first(root)
    ET.indent(tree, space="  ")
    tree.write(destination, encoding="UTF-8", xml_declaration=True)


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
