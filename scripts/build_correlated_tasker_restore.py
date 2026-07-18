#!/usr/bin/env python3
"""Build a Tasker restore with stable action ordering and concurrent runs."""

from __future__ import annotations

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path


CORRELATED_TASK_IDS = {"3", "9"}
TOKEN_PLACEHOLDER = "__SMS_BOT_TOKEN__"


def harden_restore(source: Path, destination: Path) -> None:
    tree = ET.parse(source)
    root = tree.getroot()

    for task in root.findall("Task"):
        task_id = task.findtext("id", default="")
        if task_id in CORRELATED_TASK_IDS:
            retry_type = task.find("rty")
            if retry_type is None:
                retry_type = ET.Element("rty")
                priority = task.find("pri")
                insert_at = list(task).index(priority) + 1 if priority is not None else 0
                task.insert(insert_at, retry_type)
            retry_type.text = "2"

        for index, action in enumerate(task.findall("Action")):
            action.set("sr", f"act{index}")

    ET.indent(tree, space="  ")
    destination.parent.mkdir(parents=True, exist_ok=True)
    tree.write(destination, encoding="UTF-8", xml_declaration=True)


def extract_private_token(private_source: Path) -> str:
    text = private_source.read_text(encoding="utf-8")
    match = re.search(
        r"<Str sr=\"arg0\" ve=\"3\">%token</Str>\s*"
        r"<Str sr=\"arg1\" ve=\"3\">([^<]+)</Str>",
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

    harden_restore(args.source, args.template_output)

    if bool(args.private_source) != bool(args.private_output):
        parser.error("--private-source and --private-output must be used together")
    if args.private_source and args.private_output:
        token = extract_private_token(args.private_source)
        private_text = args.template_output.read_text(encoding="utf-8").replace(
            TOKEN_PLACEHOLDER, token
        )
        if TOKEN_PLACEHOLDER in private_text:
            raise ValueError("Tasker token placeholder was not fully replaced")
        args.private_output.write_text(private_text, encoding="utf-8")


if __name__ == "__main__":
    main()
