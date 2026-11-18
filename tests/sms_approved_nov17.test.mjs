import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('No plus final Stop stays silent, including previously flattened transport', () => {
  for (const text of ['No Stop', 'No. Stop', 'No Unsubscribe']) {
    const h=smsHarness(); const r=h.incoming(text);
    assert.equal(r.should_reply,false); assert.equal(h.state.human_override,'TRUE');
    assert.equal(h.state.call_booking_status,'closed_no_interest');
  }
  const h=smsHarness();
  assert.equal(h.evaluate('coalesceSmsInboundFragmentTextsV10_(["No", "Stop"])'),'No. Stop');
  assert.equal(h.evaluate('coalesceSmsInboundFragmentTextsV10_(["Can you", "stop foreclosure?"])'),'Can you stop foreclosure?');
  assert.equal(h.evaluate('isOptOutSignal_("Can you stop foreclosure?")'),false);
});

test('Arica polite preamble preserves her call promise and creates one informational alert', () => {
  const h=smsHarness(); const text='Hello! Thanks for reaching out. I will give you a call in a bit.';
  const r=h.incoming(text);
  assert.equal(r.lead_status,'Y'); assert.equal(r.handoff_needed,false);
  assert.equal(h.state.call_booking_status,'interested_no_call');
  assert.equal(h.state.human_override,'FALSE');
  assert.equal(h.state.ai_state,'active');
  assert.notEqual(h.state.callback_requested,'yes');
  assert.equal(h.effects.filter(e=>e.type==='handoff').length,1);
  h.incoming(text,{messageId:'replay'});
  assert.equal(h.effects.filter(e=>e.type==='handoff').length,1);
});

test('explicit refusal with a call mention retains the closeout',()=>{
  const h=smsHarness(); const r=h.incoming("No thank you, I don't need help. I will call you if I need anything.");
  assert.equal(r.lead_status,'R'); assert.equal(h.effects.length,0);
});
