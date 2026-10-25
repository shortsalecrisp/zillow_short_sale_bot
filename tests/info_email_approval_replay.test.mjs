import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

test('a completed info-email approval replays success without sending twice', () => {
  const approvalId = 'c9d94469-85b0-4730-898c-d1a0954899bd';
  const values = new Map([
    [`INFO_EMAIL_PENDING_${approvalId}`, JSON.stringify({
      created_at: '2026-10-24T18:22:00.000Z',
      data: {to: 'agent@example.com', listing_address: '1 Test Lane'},
    })],
  ]);
  const props = {
    getProperty: key => values.get(key) ?? null,
    setProperty: (key, value) => values.set(key, value),
    deleteProperty: key => values.delete(key),
  };
  let lockHeld = false;
  let lockAcquisitions = 0;
  const context = {
    console,
    PropertiesService: {getScriptProperties: () => props},
    LockService: {getScriptLock: () => ({
      waitLock: timeout => {
        assert.equal(timeout, 30000);
        assert.equal(lockHeld, false);
        lockHeld = true;
        lockAcquisitions += 1;
      },
      releaseLock: () => {
        assert.equal(lockHeld, true);
        lockHeld = false;
      },
    })},
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(new URL('../apps_script/sms_chatbot.js', import.meta.url), 'utf8'), context);
  vm.runInContext(`
    hydrateInfoEmailDataFromSheet_=data=>data;
    sendCount=0;
    sendAgentInfoEmail_=()=>({ok:true,messageId:'message-1',accepted:['agent@example.com']});
    originalSend=sendAgentInfoEmail_;
    sendAgentInfoEmail_=data=>{sendCount+=1;return originalSend(data);};
    buildAgentInfoEmailSubject_=data=>'Crisp Short Sales - '+data.listing_address;
  `, context);

  context.approvalId = approvalId;
  const first = vm.runInContext('approvePendingInfoEmail_(approvalId)', context);
  const replay = vm.runInContext('approvePendingInfoEmail_(approvalId)', context);

  assert.equal(first.ok, true);
  assert.equal(first.message, 'Info email sent.');
  assert.equal(replay.ok, true);
  assert.equal(replay.already_sent, true);
  assert.match(replay.message, /already completed/);
  assert.equal(replay.to, 'agent@example.com');
  assert.equal(replay.subject, 'Crisp Short Sales - 1 Test Lane');
  assert.equal(vm.runInContext('sendCount', context), 1);
  assert.equal(lockAcquisitions, 2);
  assert.equal(values.has(`INFO_EMAIL_PENDING_${approvalId}`), false);
  assert.equal(values.has(`INFO_EMAIL_COMPLETED_${approvalId}`), true);
});
