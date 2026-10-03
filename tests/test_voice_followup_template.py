import ast
import logging
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def harness():
    parsed = ast.parse((ROOT / "bot_min.py").read_text())
    selected = [node for node in parsed.body if
                isinstance(node, ast.FunctionDef) and node.name == "send_sms" or
                isinstance(node, ast.Assign) and any(
                    isinstance(target, ast.Name) and target.id in {
                        "SMS_TEMPLATE", "SMS_FU_TEMPLATE", "SMS_FU_POLICY_VERSION"
                    } for target in node.targets)]
    calls = []
    namespace = {
        "SMS_ENABLE": True, "SMS_TEST_MODE": False, "SMS_TEST_NUMBER": "",
        "LOG": logging.getLogger(__name__),
        "_digits_only": lambda value: re.sub(r"\D", "", value),
        "_redact_phone": lambda value: "redacted",
        "_enqueue_tasker_sms_outbox": lambda **kwargs: calls.append(kwargs) or {"ok": True},
    }
    exec(compile(ast.Module(body=selected, type_ignores=[]), "bot_min.py", "exec"), namespace)
    return namespace, calls


class VoiceFollowupTemplateTests(unittest.TestCase):
    def test_followup_uses_existing_queue_once_with_truthful_identity_and_channel_choice(self):
        ns, calls = harness()
        ns["send_sms"]("(212) 555-0100", "Sam", "123 Main St", 12, True, "stable12")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["action"], "enqueue_followup_sms")
        self.assertEqual(calls[0]["stable_id"], "stable12")
        message = calls[0]["message"]
        self.assertIn("123 Main St", message)
        self.assertIn("Maya, my automated assistant, may call from 217-634-1017.", message)
        self.assertIn("Prefer email or a call with Yoni?", message)
        self.assertNotRegex(message, r"will call|booked|scheduled|9:30|3:15")
        self.assertNotIn("{address}", message)

    def test_empty_address_has_safe_fallback_and_disabled_sms_never_enqueues(self):
        ns, calls = harness()
        ns["send_sms"]("2125550100", "Sam", "", 12, True)
        self.assertTrue(calls[0]["message"].startswith("Following up on your listing:"))
        ns["SMS_ENABLE"] = False
        ns["send_sms"]("2125550100", "Sam", "123 Main St", 12, True)
        self.assertEqual(len(calls), 1)

    def test_initial_outreach_and_fee_policy_are_unchanged(self):
        ns, calls = harness()
        ns["send_sms"]("2125550100", "Sam", "123 Main St", 12)
        self.assertEqual(calls[0]["action"], "enqueue_initial_sms")
        self.assertIn("my fee is buyer-paid at closing", calls[0]["message"])
        self.assertNotIn("Maya", calls[0]["message"])

    def test_no_additional_followup_sender_or_trigger_added(self):
        ns, calls = harness()
        self.assertEqual(ns["SMS_FU_POLICY_VERSION"], "maya-caller-recognition-human-choice-20261003")
        ns["send_sms"]("bad", "Sam", "123 Main St", 12, True)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
