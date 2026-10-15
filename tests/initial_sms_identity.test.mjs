import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const root = path.join(import.meta.dirname, "..");
const source = fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8");
const { template } = JSON.parse(fs.readFileSync(path.join(root, "tests/fixtures/sms_approved_opener.json"), "utf8"));
const initialMessage = (first = "Sherri", address = "4504 Exposition Ave") => template.replace("{first}", first).replace("{address}", address);
const stableId = "10/15/2026 8:01:04";
const phone = "7022185200";
const request = (changes = {}) => ({
  crm_row: 2, phone, stable_id: stableId, message: initialMessage(),
  message_id: "initial-2-b8d95a8044052af0", request_id: "render-initial-2-b8d95a8044052af0",
  ...changes,
});

function harness(options = {}) {
  let locked = false;
  const events = [];
  const formulas = new Map([["2:28", options.formula ?? '=IF(D2<>"", NOW(), "")\n']]);
  const formats = new Map();
  const displays = new Map();
  const headers = Array.from({ length: 50 }, (_, index) => `column_${index}`);
  Object.assign(headers, { 0: "agent_name", 2: "phone", 28: "human_override", 29: "last_message_id", 30: "last_inbound_text", 31: "handoff_flag", 32: "history_json" });
  const lead = Array(50).fill("");
  Object.assign(lead, { 0: "Sherri", 1: "Simhayoff-Cohen", 2: phone, 3: "sherrilvhomes@cox.net", 4: "4504 Exposition Ave", 5: "Las Vegas", 6: "NV", 10: "N", 27: "10/15/2026 8:01:14" });
  const leads = makeSheet([headers, lead], true);
  const outbox = makeSheet([Array(19).fill("")]);
  const inbound = makeSheet([Array(8).fill("")]);
  const sheets = { sms_pending_sends: outbox, sms_inbound_queue: inbound };
  function makeSheet(data, crm = false) {
    return {
      data,
      getLastRow: () => data.length,
      getDataRange: () => ({
        getValues: () => data.map((row) => row.slice()),
        getDisplayValues: () => data.map((row) => row.map(String)),
      }),
      appendRow: (row) => { events.push({ type: "append", locked }); data.push(row.slice()); },
      getRange: (row, column, height = 1, width = 1) => {
        const key = `${row}:${column}`;
        const range = {
          getValue: () => data[row - 1]?.[column - 1] ?? "",
          getValues: () => Array.from({ length: height }, (_, r) => Array.from({ length: width }, (_, c) => data[row + r - 1]?.[column + c - 1] ?? "")),
          getDisplayValue: () => options.failedReadback && formats.has(key) ? "10/15/2026" : displays.get(key) ?? String(data[row - 1]?.[column - 1] ?? ""),
          getDisplayValues: () => range.getValues().map((values, r) => values.map((value, c) => displays.get(`${row + r}:${column + c}`) ?? String(value))),
          getFormula: () => {
            if (options.race && events.filter((event) => event.type === "formula_read").length === 1) data[1][2] = "7022185999";
            events.push({ type: "formula_read" });
            return crm ? formulas.get(key) || "" : "";
          },
          setNumberFormat: (value) => { events.push({ type: "format", locked, row, column }); formats.set(key, value); return range; },
          setValue: (value) => {
            events.push({ type: "value", locked, row, column, value });
            data[row - 1][column - 1] = value;
            displays.delete(key);
            if (!options.formulaPersists) formulas.delete(key);
            return range;
          },
        };
        return range;
      },
    };
  }
  const context = {
    Date, JSON, String, Number, Object, Array, Math, isFinite,
    normalizePhone_: (value) => String(value ?? "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => String(value ?? "").trim().replace(/\s+/g, " "),
    getSheet_: () => leads,
    getSmsSpreadsheet_: () => ({ getSheetByName: (name) => sheets[name] ?? null, getSpreadsheetTimeZone: () => "America/New_York" }),
    ensureSmsSheetHeaders_: () => {}, installSmsOutboxTriggers_: () => {},
    hasPendingSmsTakeoverV11_: () => Boolean(options.takeover),
    LockService: { getScriptLock: () => ({ tryLock: () => { locked = true; return true; }, releaseLock: () => { locked = false; } }) },
    Utilities: { getUuid: () => "fixture-uuid" },
    HEADERS: { human_override: "human_override", last_message_id: "last_message_id", last_inbound_text: "last_inbound_text", handoff_flag: "handoff_flag", history_json: "history_json" },
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  context.installSmsOutboxTriggers_ = () => {};
  context.ensureSmsSheetHeaders_ = () => {};
  context.hasPendingSmsTakeoverV11_ = () => Boolean(options.takeover);
  return { context, leads, outbox, inbound, events, formulas, formats, displays, enqueue: (body = request()) => context.enqueueInitialSmsV13_(body) };
}

function deniedWithoutWrites(h, body) {
  const result = h.enqueue(body);
  assert.equal(result.ok, false);
  assert.equal(result.queued, false);
  assert.equal(h.outbox.data.length, 1);
  assert.equal(h.events.some((event) => ["value", "format", "append"].includes(event.type)), false);
  return result;
}

test("freeze only the volatile identity as a literal under lock before initial enqueue", () => {
  const h = harness();
  const result = h.enqueue();
  assert.equal(result.queued, true);
  assert.equal(h.leads.data[1][27], stableId);
  assert.equal(h.formulas.has("2:28"), false);
  assert.equal(h.formats.get("2:28"), "@");
  const writes = h.events.filter((event) => ["format", "value", "append"].includes(event.type));
  assert.deepEqual(writes.map((event) => event.type), ["format", "value", "append"]);
  assert.ok(writes.every((event) => event.locked));
  assert.ok(writes.filter((event) => event.type !== "append").every((event) => event.row === 2 && event.column === 28));
  assert.deepEqual(JSON.parse(h.outbox.data[1][6].split(":").slice(1).join(":")), { crm_row: 2, stable_id: stableId, mark_codex_verified: true });
});

test("ordinary immutable IDs retain existing initial-enqueue behavior", () => {
  const h = harness({ formula: "" });
  h.leads.data[1][27] = "7082003";
  assert.equal(h.enqueue(request({ stable_id: "7082003", message: "Owner-approved custom message" })).queued, true);
  assert.equal(h.events.some((event) => event.type === "value" || event.type === "format"), false);
});

test("matching queued or sent replay does not mutate AB or append another initial", () => {
  for (const status of ["queued", "claimed", "send_started", "sent"]) {
    const h = harness();
    const existing = Array(19).fill("");
    Object.assign(existing, { 1: status, 2: request().request_id, 3: request().message_id, 4: phone });
    h.outbox.data.push(existing);
    assert.equal(h.enqueue().duplicate, true);
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.events.some((event) => event.type === "value" || event.type === "format"), false);
  }
});

test("volatile identity requires the complete approved opener for current first name and street", () => {
  for (const message of [initialMessage("Anthony"), initialMessage("Sherri", "15 Washington Way"), initialMessage() + " Extra", "Hey Sherri, 4504 Exposition Ave", initialMessage().replace("buyer-paid", "seller-paid")]) {
    deniedWithoutWrites(harness(), request({ message }));
  }
  deniedWithoutWrites(harness(), request({ phone: "7023261282" }));
});

test("unknown, cross-row and blank-identity formulas are not silently frozen", () => {
  for (const formula of ['=IF(D3<>"", NOW(), "")', "=TODAY()", '=IF(D2<>"", NOW()+1, "")', '=IF(D2<>"", RAND(), "")', '=IF(D2<>" ", NOW(), " ")']) deniedWithoutWrites(harness({ formula }));
  for (const id of ["", "=NOW()", "@other", "first\nsecond", "arbitrary-id"]) deniedWithoutWrites(harness(), request({ stable_id: id }));
  deniedWithoutWrites(harness(), request({ message_id: "initial-other", request_id: "unrelated" }));
});

test("the recognized same-row formula permits only harmless case, whitespace and absolute references", () => {
  const h = harness({ formula: ' = if ( $D$2 <> "" , now ( ) , "" )\n' });
  assert.equal(h.enqueue().queued, true);
  assert.equal(h.leads.data[1][27], stableId);
  assert.equal(h.formulas.has("2:28"), false);
});

test("same-phone or same-ID ambiguity elsewhere in the sheet prevents a freeze", () => {
  for (const changes of [{ 2: phone, 27: "other" }, { 2: "7023261282", 27: stableId }]) {
    const h = harness();
    h.leads.data.push(Object.assign(Array(50).fill(""), changes));
    deniedWithoutWrites(h);
  }
});

test("native NOW Date values become literals and display-only ID collisions still hold", () => {
  const h = harness();
  h.leads.data[1][27] = new Date("2026-10-15T12:01:14.000Z");
  h.displays.set("2:28", "10/15/2026 8:01:14");
  assert.equal(h.enqueue().queued, true);
  assert.equal(typeof h.leads.data[1][27], "string");
  assert.equal(h.leads.data[1][27], stableId);
  const collision = harness();
  collision.leads.data.push(Object.assign(Array(50).fill(""), { 2: "7023261282", 27: new Date("2026-10-15T12:01:04.000Z") }));
  collision.displays.set("3:28", stableId);
  deniedWithoutWrites(collision);
});

test("any previous same-phone outbox attempt requires reconciliation rather than re-enqueue", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed", "cancelled"]) {
    const h = harness();
    const previous = Array(19).fill("");
    Object.assign(previous, { 1: status, 2: "previous-request", 3: "previous-message", 4: phone });
    h.outbox.data.push(previous);
    const result = h.enqueue();
    assert.equal(result.ok, false, status);
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.events.some((event) => event.type === "value" || event.type === "format"), false);
  }
});

test("a stable ID already used by another outbox identity is held", () => {
  const h = harness();
  const previous = Array(19).fill("");
  Object.assign(previous, { 1: "sent", 4: "7023261282", 6: '__initial_outreach__:' + JSON.stringify({ crm_row: 9, stable_id: stableId }) });
  h.outbox.data.push(previous);
  assert.equal(h.enqueue().ok, false);
  assert.equal(h.outbox.data.length, 2);
  assert.equal(h.events.some((event) => event.type === "value" || event.type === "format"), false);
});

test("existing contact, inbound, or human takeover evidence prevents volatile-ID normalization", () => {
  for (const column of [7, 8, 9, 11, 12, 13, 14, 22, 23, 29, 30, 32]) {
    const h = harness();
    h.leads.data[1][column] = "existing evidence";
    deniedWithoutWrites(h);
  }
  for (const column of [28, 31]) {
    const h = harness();
    h.leads.data[1][column] = "TRUE";
    deniedWithoutWrites(h);
  }
  for (const status of ["Y", "R", "H", "O", "G"]) {
    const h = harness();
    h.leads.data[1][10] = status;
    deniedWithoutWrites(h);
  }
  const h = harness();
  h.inbound.data.push(["", "", "", "", "", phone, "Hi", new Date()]);
  deniedWithoutWrites(h);
  deniedWithoutWrites(harness({ takeover: true }));
});

test("failed literal readback or surviving formula never queues an initial", () => {
  for (const options of [{ failedReadback: true }, { formulaPersists: true }]) {
    const h = harness(options);
    const result = h.enqueue();
    assert.equal(result.ok, false);
    assert.equal(result.retryable, true);
    assert.equal(h.outbox.data.length, 1);
  }
});

test("a contact changing during identity verification is held without freezing", () => {
  deniedWithoutWrites(harness({ race: true }));
});

test("a retry after a partial freeze still validates exact identity and eligibility", () => {
  const h = harness({ failedReadback: true });
  assert.equal(h.enqueue().ok, false);
  assert.equal(h.formulas.has("2:28"), false);
  assert.equal(h.enqueue().ok, false);
  assert.equal(h.outbox.data.length, 1);
  const changed = harness({ formula: "" });
  changed.leads.data[1][27] = stableId;
  changed.leads.data[1][2] = "7023261282";
  deniedWithoutWrites(changed);
});

test("an already-frozen timestamp ID can enqueue only its matching first attempt", () => {
  const h = harness({ formula: "" });
  h.leads.data[1][27] = stableId;
  assert.equal(h.enqueue().queued, true);
  assert.equal(h.events.some((event) => event.type === "format" || event.type === "value"), false);
});

test("the two historical sent receipts regain exact identity after literal-only CRM reconciliation", () => {
  const cases = [
    { row: 6237, first: "Sherri", address: "4504 Exposition Ave", phone, id: stableId, hash: "b8d95a8044052af0", sent: "2026-10-15T12:02:44.158Z" },
    { row: 6238, first: "Anthony", address: "15 Washington Way", phone: "7023261282", id: "10/15/2026 8:01:14", hash: "5ea884b45c9e7bf8", sent: "2026-10-15T12:03:32.340Z" },
  ];
  for (const item of cases) {
    const h = harness();
    const current = h.leads.data[1];
    Object.assign(current, { 0: item.first, 2: item.phone, 4: item.address, 7: "x", 10: "", 22: "10/15/2026", 27: "10/15/2026 9:16:03" });
    const original = { crm_row: item.row, stable_id: item.id, mark_codex_verified: true };
    const sent = Array(19).fill("");
    Object.assign(sent, { 1: "sent", 2: `render-initial-${item.row}-${item.hash}`, 3: `initial-${item.row}-${item.hash}`, 4: item.phone, 5: initialMessage(item.first, item.address), 6: "__initial_outreach__:" + JSON.stringify(original), 14: new Date(item.sent), 17: item.row });
    h.outbox.data.push(sent);
    const immutableReceipt = JSON.stringify(sent);
    const context = { stable_id: item.id, followup_key: "a".repeat(64), initial_sent_at: item.sent, video_variant: "text" };
    assert.equal(h.context.resolveInitialSmsReceiptRowV14_(h.leads, item.row, item.phone, original).ok, false);
    assert.equal(h.context.resolveVideoFollowupIdentityV19_(item.phone, context, [sent]).ok, false);

    // Offline representation of the separately approved, guarded Sheet repair.
    h.formulas.delete("2:28");
    Object.assign(current, { 27: item.id, 22: new Date(item.sent), 11: sent[5], 14: new Date(item.sent) });
    assert.equal(h.context.resolveInitialSmsReceiptRowV14_(h.leads, item.row, item.phone, original).ok, true);
    assert.equal(h.context.resolveVideoFollowupIdentityV19_(item.phone, context, [sent]).ok, true);
    assert.equal(JSON.stringify(sent), immutableReceipt);
    assert.equal(h.outbox.data.length, 2);
    for (const changes of [{ stable_id: "different-listing" }, { initial_sent_at: "2026-10-15T04:00:00.000Z" }]) {
      assert.equal(h.context.resolveVideoFollowupIdentityV19_(item.phone, { ...context, ...changes }, [sent]).ok, false);
    }
    assert.equal(h.context.resolveVideoFollowupIdentityV19_("7021111111", context, [sent]).ok, false);
  }
});
