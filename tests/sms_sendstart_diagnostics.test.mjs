import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(path.join(import.meta.dirname, "../apps_script/zz_unified_post.js"), "utf8");
function fixture() {
  const values = {}, rows = [], lock = { tryLock: () => true, releaseLock() {} };
  let writes = 0, uuid = 0, failAppend = false;
  const sheet = {
    getLastRow: () => rows.length,
    appendRow(row) { if (failAppend) throw new Error("append failed"); writes++; rows.push([...row]); },
    getRange(start, col, count = 1, width = 1) {
      return {
        setValues(block) { writes++; block.forEach((row, offset) => { rows[start - 1 + offset] = [...row]; }); },
        getValues() { return Array.from({length: count}, (_, offset) =>
          (rows[start - 1 + offset] || []).slice(col - 1, col - 1 + width)); }
      };
    }
  };
  const context = {
    Date, JSON, Object, Error,
    Utilities: { getUuid: () => String(++uuid) },
    LockService: { getUserLock: () => lock },
    PropertiesService: { getScriptProperties: () => ({
      setProperty(key, value) { values[key] = value; }, getProperties: () => ({...values}),
      deleteProperty(key) { delete values[key]; }
    }) },
    getSmsSpreadsheet_: () => ({ getSheetByName: () => sheet, insertSheet: () => sheet }),
    maskSensitiveDebugText_: value => value,
  };
  for (const name of ["queueSendStartDiagnosticV35_", "flushSendStartDiagnosticsV35_"]) {
    vm.runInNewContext(source.match(new RegExp(`^function ${name}\\([\\s\\S]*?^}`, "m"))[0], context);
  }
  return { context, values, rows, lock, writes: () => writes, failAppend: () => { failAppend = true; } };
}

test("send-start diagnostic is durable without a Sheet call or credentials; existing monitor drains it", () => {
  const h = fixture();
  h.context.queueSendStartDiagnosticV35_({ request_id: "request", message_id: "message", phone: "2125550100",
    ok: true, status: "send_started", server_elapsed_ms: 12000, token: "secret", lease_token: "private", reply_text: "body" });
  assert.equal(h.writes(), 0);
  assert.equal(Object.keys(h.values).length, 1);
  assert.doesNotMatch(JSON.stringify(h.values), /secret|private|reply_text/);
  assert.equal(h.context.flushSendStartDiagnosticsV35_(), 1);
  assert.equal(h.rows[1][1], "send_started_result");
  assert.equal(JSON.parse(h.rows[1][9]).status, "send_started");
  assert.equal(Object.keys(h.values).length, 0);
  assert.equal(h.context.flushSendStartDiagnosticsV35_(), 0);
});

test("busy diagnostic lock or failed append never drops the queued diagnostic", () => {
  const h = fixture();
  h.context.queueSendStartDiagnosticV35_({ request_id: "request", ok: false });
  h.lock.tryLock = () => false;
  assert.equal(h.context.flushSendStartDiagnosticsV35_(), 0);
  assert.equal(Object.keys(h.values).length, 1);
  h.lock.tryLock = () => true;
  h.failAppend();
  assert.throws(() => h.context.flushSendStartDiagnosticsV35_(), /append failed/);
  assert.equal(Object.keys(h.values).length, 1);
});
