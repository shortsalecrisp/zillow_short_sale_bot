import { test } from "node:test";
import assert from "node:assert/strict";
import { assertUniformComparison, comparisonBody } from "../src/scripts/unifyElevenLabsComparison";

const control = () => ({ conversation_config: { tts: { model_id: "eleven_flash_v2", voice_id: "eryn", speed: 1 },
  turn: { turn_model: "turn_v3" }, agent: { prompt: { prompt: "shared", tool_ids: ["consent"] } } }, workflow: { guarded: true } });
const testArm = () => { const body = control(); body.conversation_config.tts.model_id = "eleven_v4_turbo"; return body; };
test("only the TTS model differs without mutating inputs", () => {
  assertUniformComparison(control(), testArm());
  assert.deepEqual(comparisonBody(control()), comparisonBody(testArm()));
  assert.equal(control().conversation_config.tts.model_id, "eleven_flash_v2");
});
test("rejects prompt, workflow, voice, turn, tool and speed confounds", () => {
  for (const mutate of [
    (b: any) => b.conversation_config.agent.prompt.prompt = "other",
    (b: any) => b.workflow.guarded = false,
    (b: any) => b.conversation_config.tts.voice_id = "other",
    (b: any) => b.conversation_config.turn.turn_model = "other",
    (b: any) => b.conversation_config.agent.prompt.tool_ids = [],
    (b: any) => b.conversation_config.tts.speed = 1.1,
    (b: any) => b.conversation_config.tts.model_id = "eleven_flash_v2",
  ]) { const body = testArm(); mutate(body); assert.throws(() => assertUniformComparison(control(), body)); }
});
