import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

const source=fs.readFileSync(new URL('../apps_script/sms_chatbot.js',import.meta.url),'utf8');
const helper=source.match(/^function jsonOutput_\([\s\S]*?^}/m)[0];
test('shared JSON formatter preserves inactive output and maintenance acknowledgments',()=>{
 const ctx={JSON,SMS_MIGRATION_RESPONSE_V1_:null,ContentService:{MimeType:{JSON:'json'},createTextOutput(text){return{getContent:()=>text,setMimeType(){return this;}};}}};
 vm.createContext(ctx);
 vm.runInContext('function smsMigrationOutputV1_(obj) { if (SMS_MIGRATION_RESPONSE_V1_ === null) return obj; var r=SMS_MIGRATION_RESPONSE_V1_; SMS_MIGRATION_RESPONSE_V1_=null; return r; }\n'+helper,ctx);
 assert.deepEqual(JSON.parse(ctx.jsonOutput_({ok:true,voice:'unchanged'}).getContent()),{ok:true,voice:'unchanged'});
 ctx.SMS_MIGRATION_RESPONSE_V1_={ok:true,migration_buffered:true,should_send:false};
 assert.deepEqual(JSON.parse(ctx.jsonOutput_({healthy:true}).getContent()),{ok:true,migration_buffered:true,should_send:false});
 assert.equal(ctx.SMS_MIGRATION_RESPONSE_V1_,null);
});
