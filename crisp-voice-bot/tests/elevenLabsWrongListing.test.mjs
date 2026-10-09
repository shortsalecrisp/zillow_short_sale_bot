import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';

const prompt = readFileSync(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8');
const section = prompt.split('## Confirmed wrong listing or unrelated contact\n')[1].split('\n# Automated screening and hold')[0];

test('confirmed wrong listing gives one acknowledgment and preserves the ending guard', () => {
  assert.match(section, /Sorry, I reached the wrong contact\. Thanks for correcting me/);
  assert.match(section, /Then wait/);
  assert.match(section, /Answer an accompanying question first/);
  assert.match(section, /later clear confirmation.*overrides the earlier correction/);
  assert.match(section, /correction alone is not a future opt-out/);
  assert.match(section, /Hearing problems, silence, unclear speech or a poor connection alone/);
  assert.match(section, /admin who handles the listing or can take a message remains a valid admin contact/);
});
