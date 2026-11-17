// Read-only replay of final provider receipts. Never invokes an outcome writer.
import assert from "node:assert/strict";
import { fetchVoiceTerminalReceipt } from "../lib/voiceTerminalReceipt";
import { getAuthoritativeTerminalCallResult, getExplicitCallbackConsent } from "../lib/elevenLabsPostCall";

async function main() {
  const cases = [
    ["Ana", "conv_4101m7jbczx0exvtakcar6rv1dby", "agent_not_available"],
    ["Maria", "conv_7201m7k2q48zfg48gvxa5av9dw43", "answered_not_interested"],
    ["Daryl", "conv_6201m7k2m321e6aa6qn5ps5myzcc", "agent_not_available"],
    ["Raymond", "conv_6901m7k2mdk6expa65n619e28m4h", "gatekeeper_admin_early_exit"],
  ];
  for (const [firstName, id, expected] of cases) {
    const receipt = await fetchVoiceTerminalReceipt(id) as Parameters<typeof getAuthoritativeTerminalCallResult>[0];
    assert.equal(receipt.conversation_id, id);
    assert.equal(receipt.status, "done");
    assert.equal(getAuthoritativeTerminalCallResult(receipt, 1, { firstName }), expected);
    console.log(JSON.stringify({ firstName, conversationId: id, expected, readOnly: true }));
  }
  const darcy = await fetchVoiceTerminalReceipt("conv_8201m7k617psecean5v30khjmzdz") as Parameters<typeof getExplicitCallbackConsent>[0];
  const consent = getExplicitCallbackConsent(darcy);
  assert.match(consent?.callbackTime ?? "", /today at 4:00 p\.m/i);
  console.log(JSON.stringify({ firstName: "Darcy", callbackTime: consent!.callbackTime, readOnly: true }));
}
void main().catch(error => { console.error(error.message); process.exitCode = 1; });
