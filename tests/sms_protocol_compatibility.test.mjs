import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import test from "node:test";

const root = path.join(import.meta.dirname, "..");
const frozen = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, "fixtures/sms_tasker_protocol_v370.json"), "utf8"));
for (const [file, names] of Object.entries(frozen.functions)) {
  const after = fs.readFileSync(path.join(root, file), "utf8");
  for (const [name, expected] of Object.entries(names)) test(`Tasker and safety contract remains byte-identical: ${name}`, () => {
    const pattern = new RegExp(`^function ${name}\\([\\s\\S]*?^}`, "m");
    assert.ok(after.match(pattern));
    assert.equal(createHash("sha256").update(after.match(pattern)[0]).digest("hex"), expected);
  });
}
