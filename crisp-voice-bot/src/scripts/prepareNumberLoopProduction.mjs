import fs from 'node:fs/promises';
import { createRequire } from 'node:module';
import { syncComparisonPrompts } from './syncElevenLabsComparisonPrompts.mjs';
import { verifyLiveV4Production, verifyLiveTransferAudio } from './verifyElevenLabsComparison.mjs';
const require = createRequire('/Users/yonikutler/zillow_short_sale_bot/crisp-voice-bot/package.json');
const env = require('dotenv').parse(await fs.readFile('/Users/yonikutler/zillow_short_sale_bot/crisp-voice-bot/.env'));
const apply = process.argv.includes('--apply');
try {
  const statusResponse = await fetch('https://crisp-voice-bot.onrender.com/experiment-status', {signal:AbortSignal.timeout(30000)});
  if (!statusResponse.ok) throw Error('Runtime status unavailable');
  const status = await statusResponse.json();
  if (status.commit !== '253bba9ff97d437d5d6ca1d2b19ac87590f249c6' || status.testMode || status.ownerControl?.paused || !status.ownerControl?.outboundEnabled || status.providerCircuit?.open || status.runtimeExperiment?.mode !== 'v4_production') throw Error('Runtime precondition failed');
  const verified = await verifyLiveV4Production({apiKey:env.ELEVENLABS_API_KEY});
  const audio = await verifyLiveTransferAudio({apiKey:env.ELEVENLABS_API_KEY});
  if (!verified.ok || !audio.ok) throw Error('Provider or audio precondition failed');
  const result = await syncComparisonPrompts({apiKey:env.ELEVENLABS_API_KEY,agentId:'agent_9901kpkhs1cmfgj8w9vm199nc92j',branches:[
    {id:'agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks',version:'agtvrsn_5501m8c11ty3fyb90zk5hcr4cvy4',promptHash:'97e11856922d164bdec3a3e8248d8412421a3760ad551ada461b8dbfe48fcd4a'},
    {id:'agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv',version:'agtvrsn_1001m8c11yw2e1nbvnew4v7qvtqq',promptHash:'97e11856922d164bdec3a3e8248d8412421a3760ad551ada461b8dbfe48fcd4a'}],
    promptPath:new URL('../../docs/elevenlabs-agent-prompt.md',import.meta.url).pathname,receiptDir:'/Users/yonikutler/.codex/visualizations/2026/11/27/01a21dbc-1e2b-7e82-b68a-dae365dcd8ed/voice-number-loop-release',repairNumberLoop:true,apply});
  console.log(JSON.stringify({status:result.status,sharedHash:result.sharedHash,workflowHash:result.workflowHash,promptHash:result.promptHash,versions:result.versions ?? null}));
} catch (error) { console.error('Production preparation failed; no retry: '+String(error?.message ?? 'unknown').slice(0,200)); process.exitCode=1; }
