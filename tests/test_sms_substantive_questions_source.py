from pathlib import Path


CHATBOT = (
    Path(__file__).resolve().parents[1] / "apps_script" / "sms_chatbot.js"
).read_text()


def test_substantive_questions_are_checked_before_closeouts():
    assert "const hasQuestionThatOutranksCloseout =" in CHATBOT
    assert (
        "!hasQuestionThatOutranksCloseout && isAlreadyHandledSignal_(inboundText)"
        in CHATBOT
    )
    assert (
        "!hasQuestionThatOutranksCloseout && isClearNoSignal_(inboundText)"
        in CHATBOT
    )


def test_multi_question_reply_answers_experience_service_and_fee():
    assert "buildCombinedExperienceFeeServiceReply_" in CHATBOT
    assert "over 15 years" in CHATBOT
    assert "lender paperwork" in CHATBOT
    assert "flat fee at closing" in CHATBOT


def test_service_questions_have_a_deterministic_signal():
    assert "function isServiceQuestionSignal_(text)" in CHATBOT
    assert r"/\bwhat exactly do you do\b/" in CHATBOT
    assert r"/\bhow does this work\b/" in CHATBOT
