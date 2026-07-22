#!/usr/bin/env python3
"""Build V12R5 with concurrent AutoRemote outbound task handling."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import xml.etree.ElementTree as ET


TOKEN_PLACEHOLDER = "__SMS_BOT_TOKEN__"
OLD_VERSION = "V12R4_FINAL_PARSER_SAFE"
TRANSPORT_VERSION = "V12R5_CONCURRENT_OUTBOUND"


def _extract_token(private_source: Path) -> str:
    text = private_source.read_text(encoding="utf-8")
    match = re.search(
        r'<Str sr="arg0" ve="3">%token</Str>\s*'
        r'<Str sr="arg1" ve="3">([^<]+)</Str>',
        text,
    )
    if not match:
        raise ValueError(f"Could not locate Tasker token in {private_source}")
    return match.group(1)


def _replace_text(node: ET.Element, old: str, new: str) -> None:
    for item in node.iter():
        if item.text and old in item.text:
            item.text = item.text.replace(old, new)


def _put_sr_first(root: ET.Element) -> None:
    for node in root.iter():
        if "sr" not in node.attrib:
            continue
        ordered = {"sr": node.attrib["sr"]}
        ordered.update((key, value) for key, value in node.attrib.items() if key != "sr")
        node.attrib.clear()
        node.attrib.update(ordered)


def _enable_outbound_concurrency(root: ET.Element) -> None:
    outbound = next(
        task for task in root.findall("Task") if task.findtext("id") == "21"
    )
    collision = outbound.find("rty")
    if collision is None:
        collision = ET.Element("rty")
        priority = outbound.find("pri")
        insert_at = list(outbound).index(priority) + 1 if priority is not None else 0
        outbound.insert(insert_at, collision)
    collision.text = "2"


def patch_restore(source: Path, destination: Path) -> None:
    tree = ET.parse(source)
    root = tree.getroot()
    _replace_text(root, OLD_VERSION, TRANSPORT_VERSION)
    _enable_outbound_concurrency(root)
    _put_sr_first(root)
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

    patch_restore(args.source, args.template_output)
    if bool(args.private_source) != bool(args.private_output):
        parser.error("--private-source and --private-output must be used together")
    if args.private_source and args.private_output:
        token = _extract_token(args.private_source)
        private_text = args.template_output.read_text(encoding="utf-8").replace(
            TOKEN_PLACEHOLDER,
            token,
        )
        if TOKEN_PLACEHOLDER in private_text:
            raise ValueError("Tasker token placeholder was not fully replaced")
        args.private_output.write_text(private_text, encoding="utf-8")


if __name__ == "__main__":
    main()
