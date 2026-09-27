import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const source=fs.readFileSync(new URL('../sms_takeover.py',import.meta.url),'utf8');
const script=source.match(/<script nonce="__NONCE__">([\s\S]*?)<\/script>/)[1];

async function run(responses, visibility='visible') {
  const calls=[],events={},elements={title:{},message:{},retry:{addEventListener:(n,f)=>events.retry=f}};
  const document={visibilityState:visibility,prerendering:false,getElementById:id=>elements[id],addEventListener:(n,f)=>events[n]=f};
  let cleared=false;
  const context={document,URLSearchParams,AbortSignal,setTimeout:fn=>fn(),
    location:{hash:'#token=synthetic-secret&phone=%2B19089024778',pathname:'/sms-takeover'},
    history:{replaceState:()=>cleared=true},
    fetch:async (url,options)=>{
      calls.push({url,body:JSON.parse(options.body)});
      const value=responses[Math.min(calls.length-1,responses.length-1)];
      if(value instanceof Error)throw value;
      return {ok:value.ok,json:async()=>value};
    }};
  vm.createContext(context);vm.runInContext(script,context);
  const settle=async()=>{for(let i=0;i<25;i++)await Promise.resolve();};
  await settle();
  return {calls,elements,document,events,cleared,settle};
}

let page=await run([{ok:true,phone:'9089024778'}]);
assert.equal(page.calls.length,1);
assert.equal(page.calls[0].url,'/sms-takeover');
assert.equal(page.calls[0].body.phone,'+19089024778');
assert(page.cleared);
assert.equal(page.elements.title.textContent,'Automated replies stopped');

page=await run([{ok:true,phone:'9089024778'}],'hidden');
assert.equal(page.calls.length,0);
page.document.visibilityState='visible';await page.events.visibilitychange();await page.settle();
assert.equal(page.calls.length,1);

page=await run([new Error('timeout'),{ok:false,retryable:true},{ok:true,phone:'9089024778'}]);
assert.equal(page.calls.length,3);
assert.equal(page.elements.title.textContent,'Automated replies stopped');
assert(page.calls.every(call=>call.body.phone==='+19089024778'));

page=await run([{ok:false,retryable:false}]);
assert.equal(page.calls.length,1);
assert.equal(page.elements.title.textContent,'Stop not confirmed');
assert.equal(page.elements.retry.hidden,false);

page=await run([new Error('offline')]);
assert.equal(page.calls.length,3);
assert.equal(page.elements.title.textContent,'Stop not confirmed');
console.log('Takeover page checks passed: one-click, no hidden preview action, bounded retries, no false success.');
