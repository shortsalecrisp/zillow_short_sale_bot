import fs from 'node:fs';
import vm from 'node:vm';
import crypto from 'node:crypto';

// The production decision pipeline runs unchanged; only external services and
// successful delivery receipts are simulated. No network or SMS can occur.
export function smsHarness(overrides = {}, options = {}) {
  const state = {agent_name: 'Taylor', last_name: 'Sample', phone: '2025550101', email: '',
    listing_address: '123 Example Lane', city: 'Example City', st: 'GA', mailshake_status: 'N',
    human_override: 'FALSE', ai_state: 'active', history_json: '[]', auto_reply_count: 0, ...overrides};
  const effects = [];
  const values = new Map();
  const props = {getProperty: k => values.get(k) ?? null, setProperty: (k,v) => values.set(k,v),
    deleteProperty: k => values.delete(k), getProperties: () => Object.fromEntries(values)};
  const context = {state, effects, options, console, PropertiesService: {getScriptProperties: () => props},
    LockService: {getScriptLock: () => ({tryLock: () => true, releaseLock() {}})},
    Session: {getScriptTimeZone: () => 'America/New_York'},
    Utilities: {getUuid: () => crypto.randomUUID(), sleep() {},
      DigestAlgorithm: {SHA_256: 'sha256'}, computeDigest: (_, text) => [...crypto.createHash('sha256').update(text).digest()],
      formatDate: (d, tz, f) => f === 'EEEE' ? 'Saturday' : d.toISOString()},
    CacheService: {getScriptCache: () => ({get: () => null, put() {}})}};
  vm.createContext(context);
  for (const file of ['sms_chatbot.js', 'sms_outbox.js']) {
    vm.runInContext(fs.readFileSync(new URL(`../../apps_script/${file}`, import.meta.url), 'utf8'), context);
  }
  vm.runInContext(`
    getSheet_=()=>({});
    getSheetData_=()=>[{row:2,obj:{...state}}];
    findOrCreateRowByPhone_=()=>({row:2,rowObj:{...state}});
    updateRowFields_=(_s,_r,fields)=>Object.assign(state,fields);
    appendHistory_=(_s,_r,item)=>{const h=JSON.parse(state.history_json||'[]');h.push(item);state.history_json=JSON.stringify(h);};
    appendSmsDebugLog_=()=>{};
    sendHandoffEmail_=data=>{effects.push({type:'handoff',reason:data.handoff_type});return {ok:true};};
    sendInfoEmailApprovalRequest_=data=>{if(options.emailFailure) throw new Error('approval unavailable');effects.push({type:'email_approval',to:data.to});return {ok:true,approval_id:'test-approval'};};
    sendAgentInfoEmail_=()=>{throw new Error('Unapproved live email path');};
    isInfoEmailApprovalRequired_=()=>true;
    syncWarmInfoOpportunityRows_=()=>{};
    getAiDecision_=()=>{throw new Error('MODEL_FALLBACK_REQUIRED');};
    getPendingFeeReplyStageV3_=()=>options.pendingFeeStage||'';
  `, context);
  let sequence = 0;
  return {state, effects, props,
    evaluate(code) { return vm.runInContext(code, context, {timeout: 3000}); },
    incoming(message, {deliver = true, messageId} = {}) {
      context.body = {phone: state.phone, message, message_id: messageId || `offline-${++sequence}`,
        received_at: new Date(Date.now() + sequence * 120000).toISOString()};
      const result = vm.runInContext('handleIncomingSmsCore_(body)', context, {timeout: 3000});
      if (deliver && result.should_reply && result.reply_text) {
        context.receiptText = result.reply_text;
        const responseId = vm.runInContext("typeof getDeliveredResponseId_ === 'function' ? getDeliveredResponseId_(receiptText) : ''", context);
        const history = JSON.parse(state.history_json || '[]');
        history.push({role: 'assistant', text: result.reply_text, ts: context.body.received_at,
          receipt_id: `receipt-${sequence}`, ...(responseId ? {response_id: responseId} : {})});
        state.history_json = JSON.stringify(history);
        state.last_outbound_text = result.reply_text;
        state.auto_reply_count = Number(state.auto_reply_count) + 1;
      }
      return JSON.parse(JSON.stringify(result));
    }
  };
}
