import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import crypto from 'node:crypto';

test('voice information approval is idempotent by conversation and keeps a durable receipt', () => {
  const values = new Map();
  const props = {
    getProperty: key => values.get(key) ?? null,
    setProperty: (key, value) => values.set(key, value),
    deleteProperty: key => values.delete(key),
  };
  const context = {
    console,
    PropertiesService: {getScriptProperties: () => props},
    Utilities: {
      DigestAlgorithm: {SHA_256: 'sha256'},
      computeDigest: (_algorithm, text) => [...crypto.createHash('sha256').update(text).digest()],
    },
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(new URL('../apps_script/sms_chatbot.js', import.meta.url), 'utf8'), context);
  vm.runInContext(`
    getSheet_=()=>({});
    getSheetData_=()=>[{row:5900,obj:{agent_name:'Carole',last_name:'Arcaro',phone:'7705550100',email:'carole@example.com',listing_address:'1 Test Lane'}}];
    appendSmsDebugLog_=()=>{};
    sendCount=0;
    sendInfoEmailApprovalRequest_=()=>({ok:true,approval_id:'approval-'+(++sendCount),subject:'APPROVE INFO EMAIL'});
  `, context);

  const body = {row: 5900, email: 'carole@example.com', conversation_id: 'conv_carole'};
  context.body = body;
  const first = vm.runInContext('requestInfoEmailApprovalForRow_(body)', context);
  const second = vm.runInContext('requestInfoEmailApprovalForRow_(body)', context);
  assert.equal(first.approval_id, 'approval-1');
  assert.equal(second.approval_id, 'approval-1');
  assert.equal(second.duplicate, true);
  assert.equal(vm.runInContext('sendCount', context), 1);
  assert.equal([...values.keys()].some(key => key.startsWith('INFO_EMAIL_APPROVAL_RECEIPT_')), true);
});
