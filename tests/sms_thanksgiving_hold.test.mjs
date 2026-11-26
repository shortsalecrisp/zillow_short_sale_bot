import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../apps_script/sms_outbox.js', import.meta.url), 'utf8');
const policy = { enabled:true, version:'thanksgiving-20261126-v1', resume_local:'2026-11-27 09:00:00',
  initial_sms_unchanged:true, inbound_replies_unchanged:true, calling_blackout_local_date:'2026-11-26' };
const NOW = Date.parse('2026-11-26T16:00:00Z');
class FixedDate extends Date { constructor(...args) { super(...(args.length ? args : [NOW])); } static now() { return NOW; } }
function parseZoned(text, zone) {
  const [y,m,d,h,minute,sec]=text.match(/\d+/g).map(Number);
  const wanted=Date.UTC(y,m-1,d,h,minute,sec);
  let value=wanted;
  for(let i=0;i<3;i++){
    const p=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',
      hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(new Date(value)).map(p=>[p.type,p.value]));
    value+=wanted-Date.UTC(+p.year,+p.month-1,+p.day,+p.hour,+p.minute,+p.second);
  }
  return new FixedDate(value);
}
function setup() {
  const props=new Map(),writes=[];
  const lead=['Synthetic','Owner','2025550100','','123 Test St','Panama City','FL'];
  const context={ Date:FixedDate, console,
    PropertiesService:{getScriptProperties:()=>({getProperty:k=>props.get(k)||null,setProperty:(k,v)=>props.set(k,v)})},
    Utilities:{parseDate:(text,zone)=>parseZoned(text,zone)},
    normalizePhone_:v=>String(v??'').replace(/\D/g,'').slice(-10),
    getSheet_:()=>({getRange:()=>({getValues:()=>[lead]})}),
    resolveListingTimeZone_:input=>{assert.equal(input.city,'Panama City');assert.equal(input.state,'FL');return {timeZone:'America/Chicago'};},
  };
  vm.createContext(context);vm.runInContext(source,context);
  const sheet={getRange:()=>({setValues:v=>writes.push(v[0])})};
  const row=Array(19).fill('');Object.assign(row,{1:'claimed',2:'request-existing',3:'message-existing',4:'2025550100',
    6:'__scheduled_followup__:'+JSON.stringify({crm_row:7000,stable_id:'unchanged'}),8:new FixedDate(NOW),9:2,
    10:'existing-token',11:new FixedDate(NOW+120000),12:new FixedDate(NOW),17:7000,18:'same-worker'});
  return {context,props,writes,sheet,row};
}

test('one-off policy changes no messages and requires exact owner confirmation',()=>{
  const s=setup();assert.equal(s.context.getOwnerThanksgivingPolicyV38_().enabled,false);
  assert.equal(s.context.applyOwnerThanksgivingPolicyV38_({confirmation:'wrong'}).ok,false);
  const r=s.context.applyOwnerThanksgivingPolicyV38_({confirmation:'pause_followups_until_20261127_0900_local'});
  assert.equal(r.ok,true);assert.equal(r.messages_sent,0);assert.equal(r.policy.initial_sms_unchanged,true);
});

test('Friday 9am releases independently for every US zone, initial SMS and inbound replies are unaffected',()=>{
  const s=setup();
  for(const [zone,utc] of [['America/New_York',14],['America/Chicago',15],['America/Denver',16],['America/Los_Angeles',17],
    ['America/Anchorage',18],['Pacific/Honolulu',19],['America/Phoenix',16]]){
    const release=new Date(`2026-11-27T${utc}:00:00Z`);
    const before=s.context.thanksgivingFollowupZoneDecisionV38_(zone,new Date(release-1),policy);
    assert.equal(before.eligible,false);assert.equal(before.not_before.toISOString(),release.toISOString());
    assert.equal(s.context.thanksgivingFollowupZoneDecisionV38_(zone,release,policy),null);
  }
  assert.equal(s.context.thanksgivingFollowupZoneDecisionV38_('',new Date('2026-11-26T16:00Z'),policy).eligible,false);
  assert.equal(s.context.thanksgivingFollowupZoneDecisionV38_('',new Date('2026-11-28T00:00Z'),policy),null);
  for(const metadata of ['__initial_outreach__:{}','A genuine incoming question']){
    const row=Array(19).fill('');row[6]=metadata;
    assert.equal(s.context.thanksgivingFollowupPendingDecisionV38_(row,new Date(NOW)),null);
  }
});

test('fresh listing city and phone govern the release; shifted or unreadable identity stays held',()=>{
  const s=setup();s.props.set('OWNER_THANKSGIVING_FOLLOWUP_20261126_V1',JSON.stringify(policy));
  assert.equal(s.context.thanksgivingFollowupPendingDecisionV38_(s.row,new Date(NOW)).not_before.toISOString(),'2026-11-27T15:00:00.000Z');
  s.row[4]='2025550199';
  assert.match(s.context.thanksgivingFollowupPendingDecisionV38_(s.row,new Date(NOW)).reason,/verified listing time zone/);
});

test('final send authorization is denied and the same queued identity resumes without resetting attempts',()=>{
  const s=setup();s.props.set('OWNER_THANKSGIVING_FOLLOWUP_20261126_V1',JSON.stringify(policy));
  s.context.findPendingSmsRowByExactLeaseIdentityV10_=()=>({ok:true,values:s.row,sheet:s.sheet,row:2});
  const r=s.context.markPendingSmsSendStartedV10_({});
  assert.equal(r.should_send,false);assert.equal(r.status,'queued');assert.equal(s.writes.length,1);
  const next=s.writes[0];assert.equal(next[2],s.row[2]);assert.equal(next[3],s.row[3]);assert.equal(next[9],2);
  assert.equal(next[8].toISOString(),'2026-11-27T15:00:00.000Z');assert.equal(next[10],'');
  s.row[1]='send_started';s.row[13]=new FixedDate(NOW-1000);
  const prior=s.context.markPendingSmsSendStartedV10_({});
  assert.equal(prior.should_send,false);assert.equal(prior.retryable,false);assert.equal(s.writes.length,1);
});

test('later not-before and ambiguous-send history cannot be shortened or overwritten',()=>{
  const s=setup();s.row[8]=new FixedDate('2026-11-27T18:00Z');
  s.context.deferThanksgivingFollowupV38_(s.sheet,2,s.row,{not_before:new FixedDate('2026-11-27T15:00Z'),reason:'hold'});
  assert.equal(s.writes[0][8].toISOString(),'2026-11-27T18:00:00.000Z');
  for(const status of ['sent','uncertain','send_started','receipt_pending']){
    s.row[1]=status;s.context.deferThanksgivingFollowupV38_(s.sheet,2,s.row,{not_before:new FixedDate('2026-11-27T15:00Z')});
  }
  assert.equal(s.writes.length,1);
  s.props.set('OWNER_THANKSGIVING_FOLLOWUP_20261126_V1',JSON.stringify({...policy,resume_local:'2026-11-26 08:00:00'}));
  assert.throws(()=>s.context.getOwnerThanksgivingPolicyV38_(),/malformed/);
});

test('enqueue, existing lease, new claim and final send each invoke the hold guard',()=>{
  assert.equal((source.match(/thanksgivingFollowupPendingDecisionV38_\(/g)||[]).length,5);
  const routes=fs.readFileSync(new URL('../apps_script/zz_unified_post.js',import.meta.url),'utf8');
  assert.ok(routes.indexOf('validateToken_(body.token)')<routes.indexOf('applyOwnerThanksgivingPolicyV38_(body)'));
  assert.ok(routes.indexOf('applyOwnerThanksgivingPolicyV38_(body)')<routes.indexOf('appendSmsDebugLog_("doPost_start"'));
});
