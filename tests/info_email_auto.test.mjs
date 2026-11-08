import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import crypto from 'node:crypto';

function harness(result) {
  const values = new Map();
  let sends = 0, alerts = 0, held = false;
  const c = {
    PropertiesService: {getScriptProperties: () => ({getProperty: k => values.get(k) ?? null,
      setProperty: (k,v) => values.set(k,v), deleteProperty: k => values.delete(k)})},
    LockService: {getScriptLock: () => ({waitLock: () => {assert.equal(held,false);held=true;}, releaseLock: () => {assert.equal(held,true);held=false;}})},
    Utilities: {DigestAlgorithm: {SHA_256:'sha256'}, computeDigest: (_,s) => [...crypto.createHash('sha256').update(s).digest()]},
  };
  vm.createContext(c);
  vm.runInContext(fs.readFileSync(new URL('../apps_script/sms_chatbot.js', import.meta.url),'utf8'),c);
  c.hydrateInfoEmailDataFromSheet_ = d => d;
  c.sendAgentInfoEmail_ = () => {sends++; if(result instanceof Error) throw result; return result;};
  c.sendHandoffEmail_ = () => {assert.equal(held,false);alerts++;};
  c.appendSmsDebugLog_ = () => {};
  return {c, values, counts: () => ({sends,alerts})};
}
const data = {to:'tj@example.com', phone:'6155921045', agent_name:'TJ', listing_address:'217 Flora Maxwell Rd', request_id:'inbound-1'};
test('agent-requested info sends once without owner confirmation and persists exact receipt', () => {
  const h = harness({ok:true,messageId:'smtp-1',accepted:['tj@example.com'],rejected:[]});
  const a = h.c.sendInfoEmailApprovalRequest_(data);
  const b = h.c.sendInfoEmailApprovalRequest_(data);
  assert.equal(a.delivery_mode,'automatic');
  assert.equal(b.already_sent,true);
  assert.equal(b.backend.messageId,'smtp-1');
  assert.equal(h.counts().sends,1);
  assert.equal(h.counts().alerts,0);
  assert.equal(h.c.getInfoEmailAcknowledgementReply_('tj@example.com'), "Yes, I'll send it to tj@example.com now.");
  assert.equal(h.c.isInfoEmailApprovalRequired_(),false);
});
for (const result of [new Error('transport timeout'), {ok:true,accepted:[],rejected:['tj@example.com']}]) {
  test('failed or uncertain transport never acknowledges success or blindly resends: '+String(result), () => {
    const h = harness(result);
    assert.throws(() => h.c.sendInfoEmailApprovalRequest_(data));
    assert.throws(() => h.c.sendInfoEmailApprovalRequest_(data), /uncertain/);
    assert.equal(h.counts().sends,1);
    assert.equal(h.counts().alerts,2);
  });
}
test('a separate request can send again', () => {
  const h = harness({ok:true,accepted:['tj@example.com'],rejected:[]});
  h.c.sendInfoEmailApprovalRequest_(data);
  h.c.sendInfoEmailApprovalRequest_({...data, request_id:'inbound-2'});
  assert.equal(h.counts().sends,2);
});
