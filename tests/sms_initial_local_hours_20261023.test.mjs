import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

function dateParts(date, timeZone) {
  return Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'
  }).formatToParts(date).filter(part => part.type !== 'literal').map(part => [part.type, part.value]));
}

function parseZoned(text, timeZone) {
  const match = text.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
  const wanted = {year: +match[1], month: +match[2], day: +match[3], hour: +match[4], minute: +match[5], second: +match[6]};
  let guess = Date.UTC(wanted.year, wanted.month - 1, wanted.day, wanted.hour, wanted.minute, wanted.second);
  for (let i = 0; i < 3; i++) {
    const actual = dateParts(new Date(guess), timeZone);
    const actualUtc = Date.UTC(+actual.year, +actual.month - 1, +actual.day, +actual.hour, +actual.minute, +actual.second);
    const wantedUtc = Date.UTC(wanted.year, wanted.month - 1, wanted.day, wanted.hour, wanted.minute, wanted.second);
    guess += wantedUtc - actualUtc;
  }
  return new Date(guess);
}

const context = {
  console,
  Utilities: {
    formatDate(date, timeZone, pattern) {
      if (pattern === 'H') return dateParts(date, timeZone).hour.replace(/^0/, '') || '0';
      if (pattern === 'yyyy-MM-dd') {
        const parts = dateParts(date, timeZone);
        return `${parts.year}-${parts.month}-${parts.day}`;
      }
      throw new Error(`unexpected pattern ${pattern}`);
    },
    parseDate(text, timeZone, pattern) {
      assert.equal(pattern, 'yyyy-MM-dd HH:mm:ss');
      return parseZoned(text, timeZone);
    }
  }
};
vm.createContext(context);
vm.runInContext(fs.readFileSync(new URL('../apps_script/sms_outbox.js', import.meta.url), 'utf8'), context);

test('initial outreach is deferred to 8am in Alaska and closes at 8pm local', () => {
  assert.equal(context.initialSmsRecipientTimeZoneForStateV27_('AK'), 'America/Anchorage');
  const tooEarly = context.initialSmsLocalWindowDecisionV27_('America/Anchorage', new Date('2026-10-23T12:05:00Z'));
  assert.equal(tooEarly.eligible, false);
  assert.equal(tooEarly.local_hour, 4);
  assert.equal(tooEarly.not_before.toISOString(), '2026-10-23T16:00:00.000Z');
  assert.equal(context.initialSmsLocalWindowDecisionV27_('America/Anchorage', new Date('2026-10-23T16:00:00Z')).eligible, true);
  assert.equal(context.initialSmsLocalWindowDecisionV27_('America/Anchorage', new Date('2026-10-24T03:59:00Z')).eligible, true);
  const closed = context.initialSmsLocalWindowDecisionV27_('America/Anchorage', new Date('2026-10-24T04:00:00Z'));
  assert.equal(closed.eligible, false);
  assert.equal(closed.not_before.toISOString(), '2026-10-24T16:00:00.000Z');
});

test('the same UTC time is independently evaluated in the recipient time zone', () => {
  const at = new Date('2026-10-23T12:05:00Z');
  assert.equal(context.initialSmsLocalWindowDecisionV27_('America/New_York', at).eligible, true);
  assert.equal(context.initialSmsLocalWindowDecisionV27_('America/Los_Angeles', at).eligible, false);
  assert.equal(context.initialSmsLocalWindowDecisionV27_('', at).retryable, true);
});
