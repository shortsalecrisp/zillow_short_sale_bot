#!/usr/bin/env python3
"""Build the complete V10/V11 Tasker restore with durable SMS transport."""

from __future__ import annotations

import argparse
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path


TOKEN_PLACEHOLDER = "__SMS_BOT_TOKEN__"
URL_ENCODE = 18
API_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbxkazqXh3kku3L9dVG2DkTqSt4BEIEg0z4kalAHPTeeNShrtlG8ZK5nKew9iM7rRBWK/exec"
)
PERSONAL_PHONE = "9542053205"


def _str(parent: ET.Element, sr: str, value: str = "") -> None:
    ET.SubElement(parent, "Str", {"sr": sr, "ve": "3"}).text = value


def _int(parent: ET.Element, sr: str, value: int) -> None:
    ET.SubElement(parent, "Int", {"sr": sr, "val": str(value)})


def variable_set(name: str, value: str, *, do_maths: bool = False) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "547"
    _str(action, "arg0", name)
    _str(action, "arg1", value)
    for index, val in enumerate((0, int(do_maths), 0, 3, 1), start=2):
        _int(action, f"arg{index}", val)
    return action


def url_encode(source: str, destination: str) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "596"
    _str(action, "arg0", source)
    _int(action, "arg1", URL_ENCODE)
    _str(action, "arg2", destination)
    _int(action, "arg3", 0)
    return action


def condition_action(code: int, lhs: str, op: int, rhs: str) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = str(code)
    if code == 37:
        ET.SubElement(action, "coll").text = "false"
    elif code == 137:
        _int(action, "arg0", 0)
        _str(action, "arg1", "")
    conditions = ET.SubElement(action, "ConditionList", {"sr": "if"})
    condition = ET.SubElement(conditions, "Condition", {"sr": "c0", "ve": "3"})
    ET.SubElement(condition, "lhs").text = lhs
    ET.SubElement(condition, "op").text = str(op)
    ET.SubElement(condition, "rhs").text = rhs
    return action


def if_action(lhs: str, rhs: str, op: int = 2) -> ET.Element:
    return condition_action(37, lhs, op, rhs)


def stop_if(lhs: str, rhs: str, op: int = 2) -> ET.Element:
    return condition_action(137, lhs, op, rhs)


def end_if() -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "38"
    return action


def wait(seconds: int) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "30"
    for index, val in enumerate((0, seconds, 0, 0, 0)):
        _int(action, f"arg{index}", val)
    return action


def http_post(body: str, timeout: int = 30) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "339"
    ET.SubElement(action, "Bundle", {"sr": "arg0"})
    _int(action, "arg1", 1)
    _int(action, "arg10", 1)
    _int(action, "arg11", 0)
    _int(action, "arg12", 1)
    _str(action, "arg2", "%api_url")
    _str(action, "arg3", "Content-Type:application/x-www-form-urlencoded")
    _str(action, "arg4", "")
    _str(action, "arg5", body)
    _str(action, "arg6", "")
    _str(action, "arg7", "")
    _int(action, "arg8", timeout)
    _int(action, "arg9", 1)
    return action


def send_sms(phone: str, message: str, *, wait_for_result: bool = False) -> ET.Element:
    action = ET.Element("Action", {"ve": "7"})
    ET.SubElement(action, "code").text = "41"
    _str(action, "arg0", phone)
    _str(action, "arg1", message)
    _int(action, "arg2", 0)
    _str(action, "arg3", "")
    _int(action, "arg4", int(wait_for_result))
    return action


def retry_http(body: str, *, timeout: int = 30) -> list[ET.Element]:
    return [
        http_post(body, timeout),
        if_action("%http_response_code", "2*", op=3),
        wait(3),
        http_post(body, timeout),
        end_if(),
        if_action("%http_response_code", "2*", op=3),
        wait(6),
        http_post(body, timeout),
        end_if(),
    ]


def dedupe_actions(phone: str, message: str) -> list[ET.Element]:
    return [
        if_action("%SMSBOT_LAST_KEY", "%inbound_phone | %inbound_message"),
        variable_set("%dedupe_age", "%TIMEMS-%SMSBOT_LAST_MS", do_maths=True),
        stop_if("%dedupe_age", "120000", op=6),
        end_if(),
        variable_set("%SMSBOT_LAST_KEY", "%inbound_phone | %inbound_message"),
        variable_set("%SMSBOT_LAST_MS", "%TIMEMS"),
    ]


def enqueue_actions(*, transport_version: int = 10, forward_to_operator: bool = True) -> list[ET.Element]:
    body = (
        "token=%transport_token&action=enqueue_incoming_sms&phone=%transport_phone"
        "&message=%transport_message&received_at=%transport_received_at"
        f"&message_id=%transport_message_id&transport_version={transport_version}"
    )
    actions = [
        variable_set("%api_url", API_URL),
        variable_set("%token", TOKEN_PLACEHOLDER),
    ]
    actions.extend([
        url_encode("%token", "%transport_token"),
        url_encode("%inbound_phone", "%transport_phone"),
        url_encode("%inbound_message", "%transport_message"),
        url_encode("%inbound_received_at_raw", "%transport_received_at"),
        url_encode("%inbound_message_id", "%transport_message_id"),
        *retry_http(body),
    ])
    if forward_to_operator:
        actions.extend([
            if_action("%http_data.queued", "true"),
            send_sms(
                PERSONAL_PHONE,
                "New agent SMS\n\nFrom: %inbound_phone\n\n%inbound_message\n\nTake over:\n"
                "%api_url?action=takeover&token=%token&phone=%inbound_phone",
            ),
            end_if(),
        ])
    return actions


def primary_inbound_actions(*, transport_version: int = 10) -> list[ET.Element]:
    return [
        variable_set("%inbound_phone", "%SMSRF"),
        variable_set("%inbound_message", "%SMSRB"),
        variable_set("%inbound_received_at_raw", "%DATE %TIME"),
        variable_set("%inbound_message_id", "%SMSRF-%TIMEMS"),
        *dedupe_actions("%inbound_phone", "%inbound_message"),
        *enqueue_actions(transport_version=transport_version),
    ]


def notification_inbound_actions(*, transport_version: int = 10) -> list[ET.Element]:
    return [
        variable_set("%inbound_phone", "%evtprm2"),
        variable_set("%inbound_message", "%evtprm3"),
        variable_set("%inbound_received_at_raw", "%DATE %TIME"),
        variable_set("%inbound_message_id", "%evtprm2-%TIMEMS"),
        wait(3),
        stop_if("%inbound_phone", "Device pairing"),
        stop_if("%inbound_message", "Your messages are available*"),
        stop_if("%inbound_phone", "*Device pairing*"),
        stop_if("%inbound_message", "*Your messages are available on the device you've paired*"),
        stop_if("%inbound_phone", "*Messages is doing work in the background*"),
        stop_if("%inbound_message", "*Messages is doing work in the background*"),
        *dedupe_actions("%inbound_phone", "%inbound_message"),
        *enqueue_actions(transport_version=transport_version),
    ]


def reconcile_last_sms_actions(*, transport_version: int = 11) -> list[ET.Element]:
    """Re-enqueue Tasker's last SMS when the real-time event task was dropped.

    Tasker updates the monitored %SMSR* variables independently of whether a
    profile task later survives queue pressure. The server's 10-minute
    phone/body dedupe makes this poller safe when the real-time path succeeded.
    """

    return [
        variable_set("%inbound_phone", "%SMSRF"),
        variable_set("%inbound_message", "%SMSRB"),
        variable_set("%inbound_received_at_raw", "%SMSRD %SMSRT"),
        variable_set("%inbound_message_id", "reconcile-%SMSRF-%SMSRD-%SMSRT"),
        stop_if("%SMSBOT_RECONCILE_KEY", "%inbound_message_id"),
        stop_if("%inbound_phone", "Device pairing"),
        stop_if("%inbound_message", "Your messages are available*"),
        stop_if("%inbound_phone", "*Messages is doing work in the background*"),
        stop_if("%inbound_message", "*Messages is doing work in the background*"),
        *enqueue_actions(
            transport_version=transport_version,
            forward_to_operator=True,
        ),
        if_action("%http_data.ok", "true"),
        variable_set("%SMSBOT_RECONCILE_KEY", "%inbound_message_id"),
        end_if(),
    ]


def dispatcher_actions() -> list[ET.Element]:
    claim_body = "token=%transport_token&action=claim_pending_send&worker_id=pixel-v10"
    started_body = (
        "token=%transport_token&action=send_started&request_id=%transport_request_id"
        "&message_id=%transport_message_id&phone=%transport_phone"
        "&reply_text=%transport_reply_text&lease_token=%transport_lease_token"
    )
    return [
        if_action("%SMSOUT_BUSY", "1"),
        variable_set("%busy_age", "%TIMEMS-%SMSOUT_STARTED", do_maths=True),
        stop_if("%busy_age", "600000", op=6),
        variable_set("%SMSOUT_BUSY", "0"),
        end_if(),
        variable_set("%api_url", API_URL),
        variable_set("%token", TOKEN_PLACEHOLDER),
        url_encode("%token", "%transport_token"),
        *retry_http(claim_body),
        if_action("%http_data.should_send_text", "true"),
        variable_set("%SMSOUT_REQUEST", "%http_data.request_id"),
        variable_set("%SMSOUT_MESSAGE", "%http_data.message_id"),
        variable_set("%SMSOUT_PHONE", "%http_data.phone"),
        variable_set("%SMSOUT_REPLY", "%http_data.reply_text"),
        variable_set("%SMSOUT_LEASE", "%http_data.lease_token"),
        variable_set("%SMSOUT_STARTED", "%TIMEMS"),
        variable_set("%SMSOUT_BUSY", "1"),
        url_encode("%SMSOUT_REQUEST", "%transport_request_id"),
        url_encode("%SMSOUT_MESSAGE", "%transport_message_id"),
        url_encode("%SMSOUT_PHONE", "%transport_phone"),
        url_encode("%SMSOUT_REPLY", "%transport_reply_text"),
        url_encode("%SMSOUT_LEASE", "%transport_lease_token"),
        *retry_http(started_body),
        if_action("%http_data.ok", "true"),
        send_sms("%SMSOUT_PHONE", "%SMSOUT_REPLY", wait_for_result=True),
        end_if(),
        end_if(),
    ]


def dispatcher_actions_inline_receipt() -> list[ET.Element]:
    """Dispatch one queued reply without SMS Success/Failure event profiles.

    Some Android/Tasker installs disable an imported project until the separate
    SMS Success and SMS Failure event permissions are granted. Keep the send
    single-flight, but acknowledge it from the same task after Android accepts
    the Send SMS action. If Send SMS itself errors, Tasker stops before the
    receipt and the server lease can safely retry it later.
    """

    claim_body = "token=%transport_token&action=claim_pending_send&worker_id=pixel-v12"
    started_body = (
        "token=%transport_token&action=send_started&request_id=%transport_request_id"
        "&message_id=%transport_message_id&phone=%transport_phone"
        "&reply_text=%transport_reply_text&lease_token=%transport_lease_token"
    )
    receipt_body = (
        "token=%receipt_token&action=reply_sent&request_id=%receipt_request_id"
        "&message_id=%receipt_message_id&phone=%receipt_phone"
        "&reply_text=%receipt_reply_text&lease_token=%receipt_lease_token"
        "&sent_at=%receipt_sent_at"
    )
    return [
        if_action("%SMSOUT_BUSY", "1"),
        variable_set("%busy_age", "%TIMEMS-%SMSOUT_STARTED", do_maths=True),
        stop_if("%busy_age", "600000", op=6),
        variable_set("%SMSOUT_BUSY", "0"),
        end_if(),
        variable_set("%api_url", API_URL),
        variable_set("%token", TOKEN_PLACEHOLDER),
        url_encode("%token", "%transport_token"),
        *retry_http(claim_body),
        if_action("%http_data.should_send_text", "true"),
        variable_set("%SMSOUT_REQUEST", "%http_data.request_id"),
        variable_set("%SMSOUT_MESSAGE", "%http_data.message_id"),
        variable_set("%SMSOUT_PHONE", "%http_data.phone"),
        variable_set("%SMSOUT_REPLY", "%http_data.reply_text"),
        variable_set("%SMSOUT_LEASE", "%http_data.lease_token"),
        variable_set("%SMSOUT_STARTED", "%TIMEMS"),
        variable_set("%SMSOUT_BUSY", "1"),
        url_encode("%SMSOUT_REQUEST", "%transport_request_id"),
        url_encode("%SMSOUT_MESSAGE", "%transport_message_id"),
        url_encode("%SMSOUT_PHONE", "%transport_phone"),
        url_encode("%SMSOUT_REPLY", "%transport_reply_text"),
        url_encode("%SMSOUT_LEASE", "%transport_lease_token"),
        *retry_http(started_body),
        if_action("%http_data.ok", "true"),
        send_sms("%SMSOUT_PHONE", "%SMSOUT_REPLY", wait_for_result=False),
        variable_set("%receipt_sent_at_raw", "%TIMEMS"),
        url_encode("%token", "%receipt_token"),
        url_encode("%SMSOUT_REQUEST", "%receipt_request_id"),
        url_encode("%SMSOUT_MESSAGE", "%receipt_message_id"),
        url_encode("%SMSOUT_PHONE", "%receipt_phone"),
        url_encode("%SMSOUT_REPLY", "%receipt_reply_text"),
        url_encode("%SMSOUT_LEASE", "%receipt_lease_token"),
        url_encode("%receipt_sent_at_raw", "%receipt_sent_at"),
        *retry_http(receipt_body),
        if_action("%http_data.ok", "true"),
        variable_set("%SMSOUT_BUSY", "0"),
        end_if(),
        end_if(),
        end_if(),
    ]


def receipt_actions(success: bool) -> list[ET.Element]:
    action_name = "reply_sent" if success else "sms_send_failed"
    body = (
        f"token=%receipt_token&action={action_name}&request_id=%receipt_request_id"
        "&message_id=%receipt_message_id&phone=%receipt_phone"
        "&reply_text=%receipt_reply_text&lease_token=%receipt_lease_token"
        "&sent_at=%receipt_sent_at"
    )
    actions = [
        if_action("%SMSOUT_BUSY", "1"),
        variable_set("%api_url", API_URL),
        variable_set("%token", TOKEN_PLACEHOLDER),
        variable_set("%receipt_sent_at_raw", "%TIMEMS"),
        url_encode("%token", "%receipt_token"),
        url_encode("%SMSOUT_REQUEST", "%receipt_request_id"),
        url_encode("%SMSOUT_MESSAGE", "%receipt_message_id"),
        url_encode("%SMSOUT_PHONE", "%receipt_phone"),
        url_encode("%SMSOUT_REPLY", "%receipt_reply_text"),
        url_encode("%SMSOUT_LEASE", "%receipt_lease_token"),
        url_encode("%receipt_sent_at_raw", "%receipt_sent_at"),
        *retry_http(body),
        if_action("%http_data.ok", "true"),
        variable_set("%SMSOUT_BUSY", "0"),
        end_if(),
        end_if(),
    ]
    return actions


def replace_task(task: ET.Element, actions: list[ET.Element], *, collision: int, stay_awake: bool) -> None:
    for action in task.findall("Action"):
        task.remove(action)
    rty = task.find("rty")
    if rty is None:
        rty = ET.SubElement(task, "rty")
    rty.text = str(collision)
    stay = task.find("stayawake")
    if stay is None:
        stay = ET.SubElement(task, "stayawake")
    stay.text = "true" if stay_awake else "false"
    for index, action in enumerate(actions):
        action.set("sr", f"act{index}")
        task.append(action)


def new_task(task_id: int, name: str, actions: list[ET.Element], *, collision: int = 0) -> ET.Element:
    now = str(int(time.time() * 1000))
    task = ET.Element("Task", {"sr": f"task{task_id}"})
    ET.SubElement(task, "cdate").text = now
    ET.SubElement(task, "edate").text = now
    ET.SubElement(task, "id").text = str(task_id)
    ET.SubElement(task, "nme").text = name
    ET.SubElement(task, "pri").text = "8"
    ET.SubElement(task, "rty").text = str(collision)
    ET.SubElement(task, "stayawake").text = "true"
    for index, action in enumerate(actions):
        action.set("sr", f"act{index}")
        task.append(action)
    return task


def event_profile(profile_id: int, task_id: int, name: str, event_code: int, recipient: str = "") -> ET.Element:
    now = str(int(time.time() * 1000))
    profile = ET.Element("Profile", {"sr": f"prof{profile_id}", "ve": "2"})
    ET.SubElement(profile, "cdate").text = now
    ET.SubElement(profile, "edate").text = now
    ET.SubElement(profile, "flags").text = "8"
    ET.SubElement(profile, "id").text = str(profile_id)
    ET.SubElement(profile, "mid0").text = str(task_id)
    ET.SubElement(profile, "nme").text = name
    event = ET.SubElement(profile, "Event", {"sr": "con0", "ve": "2"})
    ET.SubElement(event, "code").text = str(event_code)
    ET.SubElement(event, "pri").text = "0"
    if event_code in (2005, 2010):
        _str(event, "arg0", recipient)
    return profile


def time_profile(profile_id: int, task_id: int, name: str, repeat_minutes: int) -> ET.Element:
    now = str(int(time.time() * 1000))
    profile = ET.Element("Profile", {"sr": f"prof{profile_id}", "ve": "2"})
    ET.SubElement(profile, "cdate").text = now
    ET.SubElement(profile, "edate").text = now
    ET.SubElement(profile, "flags").text = "8"
    ET.SubElement(profile, "id").text = str(profile_id)
    ET.SubElement(profile, "mid0").text = str(task_id)
    ET.SubElement(profile, "nme").text = name
    context = ET.SubElement(profile, "Time", {"sr": "con0"})
    ET.SubElement(context, "fh").text = "-1"
    ET.SubElement(context, "fm").text = "0"
    ET.SubElement(context, "th").text = "-1"
    ET.SubElement(context, "tm").text = "0"
    ET.SubElement(context, "rep").text = str(repeat_minutes)
    return profile


def build_restore(
    source: Path,
    destination: Path,
    *,
    transport_version: int = 10,
    include_reconciler: bool = False,
    permission_safe_receipts: bool = False,
    assign_project_membership: bool = False,
) -> None:
    tree = ET.parse(source)
    root = tree.getroot()
    tasks = {task.findtext("id"): task for task in root.findall("Task")}
    replace_task(
        tasks["3"],
        primary_inbound_actions(transport_version=transport_version),
        collision=2,
        stay_awake=True,
    )
    replace_task(
        tasks["9"],
        notification_inbound_actions(transport_version=transport_version),
        collision=2,
        stay_awake=True,
    )
    # AutoRemote may deliver a large 2 PM follow-up batch. Do not let Tasker's
    # default "abort new task" collision mode silently discard those events.
    replace_task(
        tasks["21"],
        list(tasks["21"].findall("Action")),
        collision=2,
        stay_awake=True,
    )

    for profile in list(root.findall("Profile")):
        if profile.findtext("id") in {"30", "31", "32", "33", "34", "35"}:
            root.remove(profile)
    for task in list(root.findall("Task")):
        if task.findtext("id") in {"30", "31", "32", "33"}:
            root.remove(task)

    first_task_index = min(i for i, child in enumerate(list(root)) if child.tag == "Task")
    profiles = [
        time_profile(30, 30, "SMS Outbox Every 2 Minutes", 2),
        event_profile(31, 30, "SMS Outbox After Boot", 411),
    ]
    if not permission_safe_receipts:
        profiles.extend(
            [
                event_profile(32, 31, "SMS Outbox Send Success", 2005, "%SMSOUT_PHONE"),
                event_profile(33, 32, "SMS Outbox Send Failure", 2010, "%SMSOUT_PHONE"),
            ]
        )
    if include_reconciler:
        profiles.append(time_profile(34, 33, "SMS Last Message Reconciler", 1))
    for offset, profile in enumerate(profiles):
        root.insert(first_task_index + offset, profile)

    dispatcher = dispatcher_actions_inline_receipt() if permission_safe_receipts else dispatcher_actions()
    root.append(new_task(30, "SMS Outbox Dispatcher", dispatcher, collision=0))
    if not permission_safe_receipts:
        root.append(new_task(31, "SMS Outbox Send Success", receipt_actions(True), collision=0))
        root.append(new_task(32, "SMS Outbox Send Failure", receipt_actions(False), collision=0))
    if include_reconciler:
        root.append(
            new_task(
                33,
                "SMS Last Message Reconciler",
                reconcile_last_sms_actions(transport_version=transport_version),
                collision=0,
            )
        )

    if assign_project_membership:
        project = root.find("Project")
        if project is None:
            raise ValueError("Tasker restore has no Project element")
        profile_ids = [profile.findtext("id") for profile in root.findall("Profile")]
        task_ids = [task.findtext("id") for task in root.findall("Task")]
        project.find("pids").text = ",".join(profile_ids)
        project.find("tids").text = ",".join(task_ids)

    for task in root.findall("Task"):
        for index, action in enumerate(task.findall("Action")):
            action.set("sr", f"act{index}")
    ET.indent(tree, space="  ")
    destination.parent.mkdir(parents=True, exist_ok=True)
    tree.write(destination, encoding="UTF-8", xml_declaration=True)


def extract_token(private_source: Path) -> str:
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
    build_restore(args.source, args.template_output)
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
