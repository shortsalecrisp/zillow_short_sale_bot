"""Extract only the scheduler serializer; never import the running bot."""

import ast
import hashlib
import json
import logging
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any, Dict, Optional


def serialize(values):
    source = Path(__file__).resolve().parents[1] / "bot_min.py"
    functions = [
        node for node in ast.parse(source.read_text()).body
        if isinstance(node, ast.FunctionDef) and node.name == "_enqueue_tasker_sms_outbox"
    ]
    if len(functions) != 1:
        raise AssertionError("Expected exactly one scheduler serializer")
    calls = []

    def post(url, *, json, timeout):
        assert url == "https://outbox-fixture.invalid/exec"
        assert timeout == 20
        calls.append(json)
        return SimpleNamespace(status_code=200, json=lambda: {"ok": True})

    namespace = {
        "Any": Any, "Dict": Dict, "Optional": Optional, "hashlib": hashlib,
        "requests": SimpleNamespace(post=post), "LOG": logging.getLogger("serializer-fixture"),
        "TASKER_TRANSPORT_HEALTH_URL": "https://outbox-fixture.invalid/exec",
        "SMS_CHATBOT_ALLOWED_TOKEN": "offline-fixture-token",
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    result = namespace["_enqueue_tasker_sms_outbox"](**values)
    if not result or result.get("ok") is not True or len(calls) != 1:
        raise AssertionError("Expected exactly one captured request")
    return calls[0]


if __name__ == "__main__":
    raw = sys.stdin.buffer.read(65537)
    if len(raw) > 65536:
        raise AssertionError("Fixture input exceeds bound")
    values = json.loads(raw)
    if not isinstance(values, dict):
        raise AssertionError("Fixture input must be an object")
    print(json.dumps(serialize(values), sort_keys=True, separators=(",", ":")))
