import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(new URL("../apps_script/sms_chatbot.js", import.meta.url), "utf8");
const body = source.match(/^function getSmsSpreadsheet_\([\s\S]*?^}/m)[0];
const replacement = "1rbffUky_CpaC0O9HxtChBlIet4BYB1Tu1ul1j8R_pSE";

test("server routing opens only the exact verified replacement, never the frozen bound workbook", () => {
  const calls = [], expected = {};
  const context = { SpreadsheetApp: {
    openById(id) { calls.push(id); return expected; },
    getActiveSpreadsheet() { throw Error("must not use frozen original"); },
  } };
  vm.createContext(context);
  vm.runInContext(body, context);
  assert.equal(context.getSmsSpreadsheet_(), expected);
  assert.deepEqual(calls, [replacement]);
});

test("replacement access errors fail closed instead of falling back to stale controls", () => {
  for (const fail of [false, true]) {
    const context = { SpreadsheetApp: {
      openById() { if (fail) throw Error("access denied"); return null; },
      getActiveSpreadsheet() { throw Error("unsafe fallback"); },
    } };
    vm.createContext(context);
    vm.runInContext(body, context);
    assert.throws(() => context.getSmsSpreadsheet_(), fail ? /access denied/ : /Operational spreadsheet not available/);
  }
});
