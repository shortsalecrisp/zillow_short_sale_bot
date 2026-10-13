import ast
import hashlib
import logging
import re
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
PARSED = ast.parse((ROOT / "bot_min.py").read_text())
PHONE = "2125550100"
INITIAL_SENT_AT = "2026-10-13T10:00:00+00:00"
ORIGINAL_FALLBACK = (
    "Hey, just wanted to follow up on my message from earlier. "
    "Let me know if I can help with anything\u2014happy to connect whenever works for you!"
)
VIDEO_MESSAGE = (
    "Hey Jim, it's Yoni Kutler with Crisp Short Sales. "
    "Here's a quick video to put a face to my text. Happy to help if you have questions.\n"
    "https://go.crispshortsales.com/v/" + "b" * 32
)


def feature_context(variant="video", **overrides):
    context = {
        "row": 12,
        "crm_row": 12,
        "stable_id": "listing-12",
        "phone": PHONE,
        "first_name": "Jim",
        "initial_sent_at": "2026-10-13T10:00:00.000Z",
        "followup_key": "a" * 64,
        "message_id": "followup-video-" + "a" * 24,
        "request_id": "scheduler-followup-video-" + "a" * 24,
        "video_variant": variant,
        "video_job_id": "b" * 32 if variant == "video" else "",
    }
    context.update(overrides)
    return context


def harness(*, real_outbox=False):
    functions = {"send_sms", "_digits_only", "_enqueue_tasker_sms_outbox"}
    constants = {"SMS_TEMPLATE", "SMS_FU_TEMPLATE", "SMS_FU_POLICY_VERSION"}
    selected = [node for node in PARSED.body if (
        isinstance(node, ast.FunctionDef) and node.name in functions
        or isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in constants for target in node.targets
        )
    )]
    queue = Mock(return_value={"ok": True})
    response = Mock(status_code=200, text="")
    response.json.side_effect = lambda: {"ok": True, "queued": True}
    post = Mock(return_value=response)
    sleep = Mock()
    direct_sender = Mock(side_effect=AssertionError("The existing outbox is the only send path"))
    sheet_write = Mock(side_effect=AssertionError("Only a delivery receipt may mark the sheet"))
    namespace = {
        "Any": Any, "Dict": Dict, "Optional": Optional,
        "hashlib": hashlib, "re": re,
        "SMS_ENABLE": True, "SMS_TEST_MODE": False, "SMS_TEST_NUMBER": "",
        "TASKER_TRANSPORT_HEALTH_URL": "https://script.example.test/exec",
        "SMS_CHATBOT_ALLOWED_TOKEN": "unit-test-token",
        "LOG": logging.getLogger(__name__),
        "requests": types.SimpleNamespace(post=post),
        "time": types.SimpleNamespace(sleep=sleep),
        "SMS_SENDER": types.SimpleNamespace(send_with_diagnostics=direct_sender),
        "mark_followup": sheet_write,
        "mark_sent": sheet_write,
        "_redact_phone": lambda phone: "redacted-test-phone",
    }
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / "bot_min.py"), "exec"), namespace)
    if not real_outbox:
        namespace["_enqueue_tasker_sms_outbox"] = queue
    return types.SimpleNamespace(namespace=namespace, queue=queue, post=post, sleep=sleep,
                                 direct_sender=direct_sender, sheet_write=sheet_write)


def scheduler_handoff(namespace, *, enabled, current_phone=PHONE):
    """Execute only the scheduler's optional-context and send-call boundary."""
    scheduler = next(node for node in PARSED.body if isinstance(node, ast.FunctionDef)
                     and node.name == "_follow_up_pass")
    selected = []
    for name in ("video_enabled", "video_options"):
        matches = [node for node in ast.walk(scheduler) if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)]
        if len(matches) != 1:
            raise AssertionError("Expected one scheduler assignment for " + name)
        selected.append(matches[0])
    calls = [node for node in ast.walk(scheduler) if isinstance(node, ast.Expr)
             and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
             and node.value.func.id == "send_sms"]
    if len(calls) != 1:
        raise AssertionError("Expected one existing scheduled follow-up send call")
    selected.append(calls[0])
    row = [""] * 28
    row[0], row[3], row[27] = "Jim", "123 Main Street", "listing-12"
    namespace.update({
        "os": types.SimpleNamespace(getenv=lambda name, default: "true" if enabled else "false"),
        "ts": datetime(2026, 10, 13, 10, tzinfo=timezone.utc),
        "row": row, "sheet_row": 12, "followup_phone": current_phone,
        "COL_FIRST": 0, "COL_STREET": 3,
    })
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / "bot_min.py"), "exec"), namespace)


class VideoFollowupSchedulerTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("Network is forbidden in scheduler unit tests"))
            guard.start()
            self.addCleanup(guard.stop)
        self.select = Mock(side_effect=AssertionError("Video selection is not expected on this path"))
        module = types.ModuleType("video_followup")
        module.select_for_followup = self.select
        modules = patch.dict(sys.modules, {"video_followup": module})
        modules.start()
        self.addCleanup(modules.stop)
        self.harness = harness()

    def selected(self, message=VIDEO_MESSAGE, context=None):
        self.select.side_effect = None
        self.select.return_value = (message, feature_context() if context is None else context)

    def send(self, **overrides):
        values = {
            "phone": "(212) 555-0100", "first": "Jim", "address": "123 Main Street",
            "row_idx": 12, "follow_up": True, "stable_id": "listing-12",
        }
        values.update(overrides)
        return self.harness.namespace["send_sms"](**values)

    def assert_no_direct_delivery_or_sheet_write(self):
        self.harness.direct_sender.assert_not_called()
        self.harness.sheet_write.assert_not_called()

    def test_initial_sms_ignores_optional_video_context_and_preserves_existing_queue(self):
        self.send(follow_up=False, initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_not_called()
        self.harness.queue.assert_called_once_with(
            action="enqueue_initial_sms", row_idx=12, phone_digits="12125550100",
            message=self.harness.namespace["SMS_TEMPLATE"].format(first="Jim", address="123 Main Street"),
            stable_id="listing-12", mark_codex_verified=False,
        )
        self.assertIn("my fee is buyer-paid at closing", self.harness.queue.call_args.kwargs["message"])
        self.assert_no_direct_delivery_or_sheet_write()

    def test_followup_without_initial_timestamp_remains_exact_legacy_text(self):
        self.send()
        self.select.assert_not_called()
        self.harness.queue.assert_called_once_with(
            action="enqueue_followup_sms", row_idx=12, phone_digits="12125550100",
            message=ORIGINAL_FALLBACK, stable_id="listing-12",
        )
        self.assert_no_direct_delivery_or_sheet_write()

    def test_followup_without_verified_stable_id_skips_personalization(self):
        self.send(stable_id=" ", initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_not_called()
        self.assertEqual(self.harness.queue.call_count, 1)
        self.assertEqual(self.harness.queue.call_args.kwargs["stable_id"], "")
        self.assertEqual(self.harness.queue.call_args.kwargs["message"], ORIGINAL_FALLBACK)
        self.assertNotIn("followup_context", self.harness.queue.call_args.kwargs)

    def test_disabled_sms_missing_phone_and_invalid_followup_phone_never_select_or_queue(self):
        self.harness.namespace["SMS_ENABLE"] = False
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.harness.namespace["SMS_ENABLE"] = True
        self.send(phone="", initial_sent_at=INITIAL_SENT_AT)
        self.send(phone="bad-phone", initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_not_called()
        self.harness.queue.assert_not_called()

    def test_selected_video_and_context_enter_existing_followup_queue_once(self):
        context = feature_context()
        self.selected(context=context)
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_called_once_with(
            ORIGINAL_FALLBACK, row=12, stable_id="listing-12", phone="12125550100",
            first_name="Jim", initial_sent_at=INITIAL_SENT_AT,
        )
        self.harness.queue.assert_called_once_with(
            action="enqueue_followup_sms", row_idx=12, phone_digits="12125550100",
            message=VIDEO_MESSAGE, stable_id="listing-12", followup_context=context,
        )
        self.assertIs(self.harness.queue.call_args.kwargs["followup_context"], context)
        self.assert_no_direct_delivery_or_sheet_write()

    def test_original_text_fallback_preserves_stable_video_identity(self):
        context = feature_context("text")
        self.selected(message=ORIGINAL_FALLBACK, context=context)
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.assertEqual(self.select.call_count, 1)
        self.assertEqual(self.harness.queue.call_count, 1)
        queued = self.harness.queue.call_args.kwargs
        self.assertEqual(queued["message"], ORIGINAL_FALLBACK)
        self.assertEqual(queued["followup_context"]["message_id"], feature_context()["message_id"])
        self.assertEqual(queued["followup_context"]["request_id"], feature_context()["request_id"])
        self.assertEqual(queued["followup_context"]["video_variant"], "text")

    def test_feature_disabled_selection_result_preserves_legacy_queue_arguments(self):
        self.selected(message=ORIGINAL_FALLBACK, context={})
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.assertEqual(self.select.call_count, 1)
        self.assertEqual(self.harness.queue.call_args.kwargs["message"], ORIGINAL_FALLBACK)
        self.assertNotIn("followup_context", self.harness.queue.call_args.kwargs)

    def test_cancelled_selection_enqueues_nothing(self):
        self.selected(message=None, context=feature_context("text"))
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_called_once()
        self.harness.queue.assert_not_called()
        self.harness.post.assert_not_called()
        self.assert_no_direct_delivery_or_sheet_write()

    def test_test_mode_owner_number_never_receives_agent_personalized_clip(self):
        self.harness.namespace.update(SMS_TEST_MODE=True, SMS_TEST_NUMBER="646-555-0109")
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_not_called()
        self.harness.queue.assert_called_once_with(
            action="enqueue_followup_sms", row_idx=12, phone_digits="16465550109",
            message=ORIGINAL_FALLBACK, stable_id="listing-12",
        )
        self.assertNotIn("https://", self.harness.queue.call_args.kwargs["message"])

    def test_test_mode_even_without_override_bypasses_personalization(self):
        self.harness.namespace["SMS_TEST_MODE"] = True
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_not_called()
        self.assertEqual(self.harness.queue.call_args.kwargs["message"], ORIGINAL_FALLBACK)
        self.assertNotIn("followup_context", self.harness.queue.call_args.kwargs)

    def test_queue_retry_reuses_one_selection_payload_and_context(self):
        context = feature_context()
        self.selected(context=context)
        self.harness.queue.side_effect = [
            {"ok": False, "response": {"retryable": True}}, {"ok": True},
        ]
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_called_once()
        self.assertEqual(self.harness.queue.call_count, 2)
        self.assertEqual(self.harness.queue.call_args_list[0], self.harness.queue.call_args_list[1])
        for call in self.harness.queue.call_args_list:
            self.assertIs(call.kwargs["followup_context"], context)
        self.harness.sleep.assert_called_once_with(2)
        self.assert_no_direct_delivery_or_sheet_write()

    def test_permanent_queue_rejection_does_not_switch_to_direct_send(self):
        self.selected()
        self.harness.queue.return_value = {"ok": False, "response": {"retryable": False}}
        self.send(initial_sent_at=INITIAL_SENT_AT)
        self.select.assert_called_once()
        self.harness.queue.assert_called_once()
        self.harness.sleep.assert_not_called()
        self.assert_no_direct_delivery_or_sheet_write()

    def test_scheduler_handoff_passes_current_phone_and_initial_timestamp_when_enabled(self):
        self.selected()
        scheduler_handoff(self.harness.namespace, enabled=True, current_phone="646-555-0109")
        self.select.assert_called_once_with(
            ORIGINAL_FALLBACK, row=12, stable_id="listing-12", phone="16465550109",
            first_name="Jim", initial_sent_at=INITIAL_SENT_AT,
        )
        self.harness.queue.assert_called_once()
        self.assertEqual(self.harness.queue.call_args.kwargs["phone_digits"], "16465550109")
        self.assertEqual(self.harness.queue.call_args.kwargs["message"], VIDEO_MESSAGE)

    def test_scheduler_handoff_omits_optional_context_when_feature_disabled(self):
        scheduler_handoff(self.harness.namespace, enabled=False)
        self.select.assert_not_called()
        self.harness.queue.assert_called_once_with(
            action="enqueue_followup_sms", row_idx=12, phone_digits="12125550100",
            message=ORIGINAL_FALLBACK, stable_id="listing-12",
        )


class VideoFollowupOutboxTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("Network is forbidden in outbox unit tests"))
            guard.start()
            self.addCleanup(guard.stop)
        self.harness = harness(real_outbox=True)

    def enqueue(self, **overrides):
        values = {
            "action": "enqueue_followup_sms", "row_idx": 12, "phone_digits": "12125550100",
            "message": ORIGINAL_FALLBACK, "stable_id": " listing-12 ",
        }
        values.update(overrides)
        return self.harness.namespace["_enqueue_tasker_sms_outbox"](**values)

    def test_video_context_builds_expected_existing_outbox_payload(self):
        context = feature_context()
        result = self.enqueue(message=VIDEO_MESSAGE, followup_context=context)
        self.harness.post.assert_called_once()
        call = self.harness.post.call_args
        self.assertEqual(call.args, ("https://script.example.test/exec",))
        self.assertEqual(call.kwargs["timeout"], 20)
        self.assertEqual(call.kwargs["json"], {
            "token": "unit-test-token", "action": "enqueue_followup_sms",
            "row": 12, "crm_row": 12, "phone": "12125550100",
            "message": VIDEO_MESSAGE, "reply_text": VIDEO_MESSAGE,
            "message_id": context["message_id"], "request_id": context["request_id"],
            "stable_id": "listing-12", "zpid": "listing-12",
            "followup_key": context["followup_key"], "initial_sent_at": context["initial_sent_at"],
            "video_variant": "video", "video_job_id": context["video_job_id"], "first_name": "Jim",
        })
        self.assertTrue(result["ok"])
        self.assertEqual(result["message_id"], context["message_id"])

    def test_video_and_original_text_use_same_content_independent_identity_after_row_shift(self):
        self.enqueue(message=VIDEO_MESSAGE, followup_context=feature_context())
        self.enqueue(row_idx=99, message=ORIGINAL_FALLBACK, followup_context=feature_context("text", row=99, crm_row=99))
        video_body, text_body = [call.kwargs["json"] for call in self.harness.post.call_args_list]
        self.assertNotEqual(video_body["message"], text_body["message"])
        self.assertEqual(video_body["message_id"], text_body["message_id"])
        self.assertEqual(video_body["request_id"], text_body["request_id"])
        self.assertEqual(video_body["followup_key"], text_body["followup_key"])
        self.assertEqual(text_body["row"], 99)
        self.assertEqual(text_body["crm_row"], 99)
        self.assertEqual(text_body["video_variant"], "text")
        self.assertEqual(text_body["video_job_id"], "")

    def test_legacy_content_hash_identity_and_default_request_prefix_are_unchanged(self):
        first = self.enqueue()
        second = self.enqueue(followup_context={})
        digest = hashlib.sha256(ORIGINAL_FALLBACK.encode("utf-8")).hexdigest()[:16]
        expected = "followup-12-" + digest
        self.assertEqual(first["message_id"], expected)
        self.assertEqual(second["message_id"], expected)
        bodies = [call.kwargs["json"] for call in self.harness.post.call_args_list]
        self.assertEqual(bodies[0], bodies[1])
        self.assertEqual(bodies[0]["request_id"], "scheduler-" + expected)
        self.assertEqual(bodies[0]["message"], ORIGINAL_FALLBACK)
        for key in ("followup_key", "initial_sent_at", "video_variant", "video_job_id", "first_name"):
            self.assertNotIn(key, bodies[0])

    def test_legacy_hash_still_changes_for_content_or_row_and_respects_custom_prefix(self):
        self.enqueue()
        self.enqueue(message="Changed legacy text")
        self.enqueue(row_idx=13)
        self.enqueue(request_prefix="existing-custom-prefix")
        bodies = [call.kwargs["json"] for call in self.harness.post.call_args_list]
        self.assertNotEqual(bodies[0]["message_id"], bodies[1]["message_id"])
        self.assertNotEqual(bodies[0]["message_id"], bodies[2]["message_id"])
        self.assertEqual(bodies[3]["request_id"], "existing-custom-prefix-" + bodies[0]["message_id"])

    def test_initial_payload_ignores_followup_identity_and_context(self):
        self.enqueue(action="enqueue_initial_sms", message="Initial message",
                     followup_context=feature_context(), mark_codex_verified=False)
        body = self.harness.post.call_args.kwargs["json"]
        expected = "initial-12-" + hashlib.sha256(b"Initial message").hexdigest()[:16]
        self.assertEqual(body["message_id"], expected)
        self.assertEqual(body["request_id"], "scheduler-" + expected)
        self.assertIs(body["mark_codex_verified"], False)
        self.assertNotIn("followup_key", body)
        self.assertNotIn("video_variant", body)

    def test_missing_outbox_configuration_makes_no_request(self):
        for setting in ("TASKER_TRANSPORT_HEALTH_URL", "SMS_CHATBOT_ALLOWED_TOKEN"):
            with self.subTest(setting=setting):
                before = self.harness.namespace[setting]
                self.harness.namespace[setting] = ""
                self.assertIsNone(self.enqueue(followup_context=feature_context()))
                self.harness.namespace[setting] = before
        self.harness.post.assert_not_called()

    def test_send_sms_to_real_outbox_harness_posts_once_with_selected_stable_identity(self):
        context = feature_context()
        selector = Mock(return_value=(VIDEO_MESSAGE, context))
        module = types.ModuleType("video_followup")
        module.select_for_followup = selector
        with patch.dict(sys.modules, {"video_followup": module}):
            self.harness.namespace["send_sms"](
                phone=PHONE, first="Jim", address="123 Main Street", row_idx=12,
                follow_up=True, stable_id="listing-12", initial_sent_at=INITIAL_SENT_AT,
            )
        selector.assert_called_once()
        self.harness.post.assert_called_once()
        body = self.harness.post.call_args.kwargs["json"]
        self.assertEqual(body["action"], "enqueue_followup_sms")
        self.assertEqual(body["message"], VIDEO_MESSAGE)
        self.assertEqual(body["message_id"], context["message_id"])
        self.assertEqual(body["request_id"], context["request_id"])
        self.harness.direct_sender.assert_not_called()
        self.harness.sheet_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
