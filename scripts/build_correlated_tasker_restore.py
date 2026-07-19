#!/usr/bin/env python3
"""Build a URL-safe Tasker restore with stable correlated reply handling."""

from __future__ import annotations

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path


CORRELATED_TASK_IDS = {"3", "9"}
TOKEN_PLACEHOLDER = "__SMS_BOT_TOKEN__"
URL_ENCODE_CONVERSION = 18


def _text(action: ET.Element, selector: str) -> str:
    return action.findtext(selector, default="")


def _variable_set(name: str, value: str) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "547"
    ET.SubElement(action, "Str", {"sr": "arg0", "ve": "3"}).text = name
    ET.SubElement(action, "Str", {"sr": "arg1", "ve": "3"}).text = value
    for index, value in enumerate((0, 0, 0, 3, 1), start=2):
        ET.SubElement(action, "Int", {"sr": f"arg{index}", "val": str(value)})
    return action


def _url_encode(source: str, destination: str) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "596"
    ET.SubElement(action, "Str", {"sr": "arg0", "ve": "3"}).text = source
    ET.SubElement(
        action,
        "Int",
        {"sr": "arg1", "val": str(URL_ENCODE_CONVERSION)},
    )
    ET.SubElement(action, "Str", {"sr": "arg2", "ve": "3"}).text = destination
    ET.SubElement(action, "Int", {"sr": "arg3", "val": "0"})
    return action


def _insert_actions(task: ET.Element, index: int, actions: list[ET.Element]) -> None:
    children = list(task)
    anchor = children.index(task.findall("Action")[index])
    for offset, action in enumerate(actions):
        task.insert(anchor + offset, action)


def _http_body(action: ET.Element) -> str:
    if _text(action, "code") != "339":
        return ""
    return _text(action, "Str[@sr='arg5']")


def _set_http_body(action: ET.Element, body: str) -> None:
    body_node = action.find("Str[@sr='arg5']")
    if body_node is None:
        raise ValueError("HTTP Request action has no body field")
    body_node.text = body


def harden_tasker_transport(task: ET.Element, phone_var: str, message_var: str) -> None:
    inbound_body = (
        "token=%transport_token&action=incoming_sms&phone=%transport_phone"
        "&message=%transport_message&received_at=%transport_received_at"
        "&message_id=%transport_message_id"
    )
    receipt_body = (
        "token=%receipt_token&action=reply_sent&request_id=%receipt_request_id"
        "&message_id=%receipt_message_id&phone=%receipt_phone"
        "&reply_text=%receipt_reply_text&sent_at=%receipt_sent_at"
    )

    actions = task.findall("Action")
    inbound_indexes = [
        index
        for index, action in enumerate(actions)
        if "action=incoming_sms" in _http_body(action)
    ]
    if not inbound_indexes:
        raise ValueError(f"Task {task.findtext('id')} has no inbound HTTP Request")

    inbound_encoding = [
        _variable_set("%transport_received_at_raw", "%DATE %TIME"),
        _url_encode("%token", "%transport_token"),
        _url_encode(phone_var, "%transport_phone"),
        _url_encode(message_var, "%transport_message"),
        _url_encode("%transport_received_at_raw", "%transport_received_at"),
        _url_encode("%message_id", "%transport_message_id"),
    ]
    _insert_actions(task, inbound_indexes[0], inbound_encoding)

    for action in task.findall("Action"):
        if "action=incoming_sms" in _http_body(action):
            _set_http_body(action, inbound_body)

    actions = task.findall("Action")
    send_index = next(
        (
            index
            for index, action in enumerate(actions)
            if _text(action, "code") == "41"
            and _text(action, "Str[@sr='arg0']") == "%reply_phone"
        ),
        None,
    )
    if send_index is None:
        raise ValueError(f"Task {task.findtext('id')} has no correlated SMS send")

    receipt_encoding = [
        _url_encode("%token", "%receipt_token"),
        _url_encode("%reply_request_id", "%receipt_request_id"),
        _url_encode("%reply_message_id", "%receipt_message_id"),
        _url_encode("%reply_phone", "%receipt_phone"),
        _url_encode("%reply_text", "%receipt_reply_text"),
    ]
    _insert_actions(task, send_index, receipt_encoding)

    actions = task.findall("Action")
    receipt_index = next(
        (
            index
            for index, action in enumerate(actions)
            if "action=reply_sent" in _http_body(action)
        ),
        None,
    )
    if receipt_index is None:
        raise ValueError(f"Task {task.findtext('id')} has no delivery receipt HTTP Request")

    _insert_actions(
        task,
        receipt_index,
        [
            _variable_set("%receipt_sent_at_raw", "%TIMEMS"),
            _url_encode("%receipt_sent_at_raw", "%receipt_sent_at"),
        ],
    )
    for action in task.findall("Action"):
        if "action=reply_sent" in _http_body(action):
            _set_http_body(action, receipt_body)


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

            phone_var, message_var = (
                ("%SMSRF", "%SMSRB")
                if task_id == "3"
                else ("%evtprm2", "%evtprm3")
            )
            harden_tasker_transport(task, phone_var, message_var)

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
