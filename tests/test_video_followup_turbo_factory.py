import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import video_followup_same_take as provider
import video_followup_worker as bootstrap

REAL_PROVIDER = provider.SameTakeVideoProvider


@pytest.fixture
def configured(monkeypatch):
    config = SimpleNamespace(enabled=True, generation_enabled=True, origin="https://video.example",
                             bucket="private", not_before="2026-10-14", live=False,
                             allowlist={"9542053205"})
    monkeypatch.setattr(bootstrap, "Config", lambda: config)
    monkeypatch.setenv("FAL_API_KEY", "fal-test-key")
    monkeypatch.setenv("FAL_TTS_API_KEY", "tts-test-key")
    monkeypatch.setenv("VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY", "assets/master/video")
    monkeypatch.setenv("TASKER_TRANSPORT_HEALTH_URL", "https://script.google.com/approved")
    monkeypatch.setenv("SMS_CHATBOT_ALLOWED_TOKEN", "test-token")
    monkeypatch.delenv("HEYGEN_API_KEY", raising=False)
    monkeypatch.delenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", raising=False)
    monkeypatch.delenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", raising=False)
    store = object()
    service = Mock(return_value=SimpleNamespace(store=store))
    monkeypatch.setattr(bootstrap, "get_service", service)
    constructor = Mock(return_value=object())
    monkeypatch.setattr(provider, "SameTakeVideoProvider", constructor)
    worker = Mock()
    monkeypatch.setattr(bootstrap, "Worker", worker)
    return config, service, constructor, worker, store


def test_factory_does_not_require_or_use_heygen(configured):
    _, service, constructor, worker, store = configured
    assert bootstrap.create_worker() is worker.return_value
    service.assert_called_once()
    constructor.assert_called_once_with("fal-test-key", store, "assets/master/video", tts_key="tts-test-key",
                                        generator_profile="legacy_v1")


def test_factory_selects_explicit_clean_profile_without_changing_other_configuration(configured, monkeypatch):
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", "clean_body_v1")
    _, _, constructor, _, store = configured
    bootstrap.create_worker()
    constructor.assert_called_once_with("fal-test-key", store, "assets/master/video", tts_key="tts-test-key",
                                        generator_profile="clean_body_v1")


def test_factory_explicit_legacy_verification_preserves_constructor(configured, monkeypatch):
    monkeypatch.setenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", provider.LEGACY_VERIFICATION_METHOD)
    bootstrap.create_worker()
    configured[2].assert_called_once_with("fal-test-key", configured[4], "assets/master/video",
                                          tts_key="tts-test-key", generator_profile="legacy_v1")


def test_factory_planned_verification_is_explicit_and_keeps_generation_profile(configured, monkeypatch):
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", "clean_body_v1")
    monkeypatch.setenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", provider.PLANNED_AUDIO_VERIFICATION_METHOD)
    bootstrap.create_worker()
    configured[2].assert_called_once_with("fal-test-key", configured[4], "assets/master/video",
        tts_key="tts-test-key", generator_profile="clean_body_v1",
        verification_method=provider.PLANNED_AUDIO_VERIFICATION_METHOD)


@pytest.mark.parametrize("method", [provider.LEGACY_VERIFICATION_METHOD, provider.PLANNED_AUDIO_VERIFICATION_METHOD,
                                    provider.PRONUNCIATION_VERIFICATION_METHOD,
                                    provider.GENERAL_PRONUNCIATION_VERIFICATION_METHOD])
def test_factory_verification_uses_real_constructor(configured, monkeypatch, method):
    monkeypatch.setenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", method)
    monkeypatch.setattr(provider, "SameTakeVideoProvider", REAL_PROVIDER)
    bootstrap.create_worker()
    assert configured[3].call_args.args[1].verification_method == method


@pytest.mark.parametrize("method", ["", "latest", "planned-aac-lineage-v99", "null"])
def test_factory_unknown_verification_fails_before_storage(configured, monkeypatch, method):
    monkeypatch.setenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", method)
    with pytest.raises(ValueError, match="verification method is not approved"):
        bootstrap.create_worker()
    configured[1].assert_not_called()
    configured[2].assert_not_called()


@pytest.mark.parametrize("profile", ["legacy_v1", "clean_body_v1"])
def test_factory_uses_real_provider_constructor_contract(configured, monkeypatch, profile):
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", profile)
    monkeypatch.setattr(provider, "SameTakeVideoProvider", REAL_PROVIDER)
    bootstrap.create_worker()
    actual_provider = configured[3].call_args.args[1]
    assert isinstance(actual_provider, REAL_PROVIDER)
    assert actual_provider.generator_profile == profile
    assert actual_provider.reference_key == provider.REFERENCE_KEY


@pytest.mark.parametrize("profile", ["", "latest", "legacy_v2", "clean_body_v2"])
def test_factory_rejects_unapproved_generator_before_storage(configured, monkeypatch, profile):
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", profile)
    with pytest.raises(provider.VideoProviderError, match="profile is not approved"):
        bootstrap.create_worker()
    configured[1].assert_not_called()
    configured[2].assert_not_called()


@pytest.mark.parametrize("flag", ["enabled", "generation_enabled"])
def test_disabled_flags_construct_nothing(configured, flag):
    config, service, constructor, worker, _ = configured
    setattr(config, flag, False)
    assert bootstrap.create_worker() is None
    service.assert_not_called()
    constructor.assert_not_called()
    worker.assert_not_called()


@pytest.mark.parametrize("variable", ["FAL_API_KEY", "FAL_TTS_API_KEY", "VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY",
    "TASKER_TRANSPORT_HEALTH_URL", "SMS_CHATBOT_ALLOWED_TOKEN"])
def test_factory_missing_required_configuration_fails_before_clients(configured, monkeypatch, variable):
    monkeypatch.delenv(variable)
    with pytest.raises(ValueError):
        bootstrap.create_worker()
    configured[1].assert_not_called()
    configured[2].assert_not_called()


def test_owner_only_generation_still_requires_exact_allowlist(configured):
    configured[0].allowlist = set()
    with pytest.raises(ValueError):
        bootstrap.create_worker()
    configured[1].assert_not_called()


def test_successful_enabled_start_logs_version_once_without_secrets(configured, monkeypatch, caplog):
    thread = Mock()
    thread.is_alive.return_value = False
    thread.start.side_effect = lambda: setattr(thread.is_alive, "return_value", True)
    constructor = Mock(return_value=thread)
    monkeypatch.setattr(bootstrap, "_thread", None)
    monkeypatch.setattr(bootstrap, "_stop", Mock())
    monkeypatch.setattr(bootstrap.threading, "Thread", constructor)
    with caplog.at_level(logging.INFO, logger=bootstrap.__name__):
        bootstrap.start_worker()
        bootstrap.start_worker()
    constructor.assert_called_once()
    thread.start.assert_called_once()
    configured[1].assert_called_once()
    starts = [record.getMessage() for record in caplog.records if "VIDEO_WORKER_STARTED" in record.getMessage()]
    assert starts == ["VIDEO_WORKER_STARTED script_version=" + bootstrap.SCRIPT_VERSION + " poll_interval_seconds=60"]
    assert not any(value in caplog.text for value in ("fal-test-key", "tts-test-key", "test-token", "9542053205"))


@pytest.mark.parametrize("flag", ["enabled", "generation_enabled"])
def test_disabled_worker_never_logs_start_or_creates_thread(configured, monkeypatch, caplog, flag):
    setattr(configured[0], flag, False)
    constructor = Mock()
    monkeypatch.setattr(bootstrap, "_thread", None)
    monkeypatch.setattr(bootstrap.threading, "Thread", constructor)
    with caplog.at_level(logging.INFO, logger=bootstrap.__name__):
        bootstrap.start_worker()
    constructor.assert_not_called()
    configured[1].assert_not_called()
    assert "VIDEO_WORKER_STARTED" not in caplog.text


def test_failed_thread_start_never_logs_success(configured, monkeypatch, caplog):
    thread = Mock()
    thread.start.side_effect = RuntimeError("thread unavailable")
    monkeypatch.setattr(bootstrap, "_thread", None)
    monkeypatch.setattr(bootstrap, "_stop", Mock())
    monkeypatch.setattr(bootstrap.threading, "Thread", Mock(return_value=thread))
    with caplog.at_level(logging.INFO, logger=bootstrap.__name__), pytest.raises(RuntimeError):
        bootstrap.start_worker()
    assert "VIDEO_WORKER_STARTED" not in caplog.text
