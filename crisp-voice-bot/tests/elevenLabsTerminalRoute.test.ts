import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import { createServer } from "node:http";
import { once } from "node:events";
import { createElevenLabsTerminalRouter } from "../src/routes/elevenLabsTerminal";

async function withRouter(secret: string | undefined, fn: (url: string) => Promise<void>) {
  const app = express();
  app.use(express.json({ limit: "1mb" }));
  app.use(createElevenLabsTerminalRouter(secret));
  const server = createServer(app).listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try { await fn(`http://127.0.0.1:${address.port}`); }
  finally { server.closeAllConnections(); await new Promise<void>((resolve, reject) => server.close((err) => err ? reject(err) : resolve())); }
}

test("terminal controls require configured authentication and ignore body tokens", async () => {
  for (const [secret, status] of [[undefined, 503], ["test-secret", 401]] as const) {
    await withRouter(secret, async (url) => {
      const response = await fetch(url + "/reset-ending", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token: "test-secret" }) });
      assert.equal(response.status, status);
      assert.equal((await response.json() as any).permission, false);
    });
  }
});

test("stateless reset and malformed validation explicitly deny permission", async () => {
  await withRouter("test-secret", async (url) => {
    const headers = { "Content-Type": "application/json", "x-crisp-elevenlabs-secret": "test-secret" };
    const reset = await fetch(url + "/reset-ending", { method: "POST", headers, body: "{}" });
    assert.deepEqual(await reset.json(), { ok: true, permission: false, decision: "reset" });
    const invalid = await fetch(url + "/validate-ending", { method: "POST", headers, body: "{}" });
    assert.equal(invalid.status, 400);
    assert.equal((await invalid.json() as any).permission, false);
  });
});

test("actual route checks history, not model-provided permission or rationale", async () => {
  await withRouter("test-secret", async (url) => {
    const response = await fetch(url + "/validate-ending", { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer test-secret" }, body: JSON.stringify({
      conversation_id: "conv_0000000000synthetic", permission: true, reason: "Caller asked to end",
      history: JSON.stringify({ "x-elevenlabs-history": true, entries: [{ role: "user", message: "That is the callback request. No transfer now." }] }),
    }) });
    assert.equal(response.status, 200);
    const result = await response.json() as any;
    assert.equal(result.permission, false);
    assert.equal(result.decision, "no_ending_request");
    assert.equal(typeof result.latest_user_turn_hash, "string");
  });
});
