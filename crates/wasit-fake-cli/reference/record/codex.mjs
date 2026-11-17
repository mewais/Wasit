import {run} from './rec.mjs';import fs from 'node:fs';
const which=process.argv[2];
const R=(process.env.REF_DIR||'.')+'/codex';fs.mkdirSync(R,{recursive:true});
const HOME=process.env.CODEX_HOME;
let id=1;
const rpc=(h,method,params)=>{const i=++id;h.send({jsonrpc:'2.0',id:i,method,params});return h.wait(e=>e.dir==='out'&&e.msg.id===i&&e.msg.method===undefined,60000)};
const hs=async h=>{
 h.send({jsonrpc:'2.0',id:1,method:'initialize',params:{clientInfo:{name:'wasit',version:'0.1'},capabilities:{experimentalApi:true}}});
 await h.wait(e=>e.dir==='out'&&e.msg.id===1);
 h.send({jsonrpc:'2.0',method:'initialized'});
};
const start=async(h,extra={})=>{const r=await rpc(h,'thread/start',{cwd:'/tmp/cx-x',approvalPolicy:'untrusted',sandbox:'workspace-write',...extra});return r.msg.result.thread.id};
const turn=(h,tid,text)=>{const i=++id;h.send({jsonrpc:'2.0',id:i,method:'turn/start',params:{threadId:tid,input:[{type:'text',text}]}});return h.wait(e=>e.dir==='out'&&e.msg.method==='turn/completed',60000)};
const sc={
 async handshake(h){await hs(h);const t=await start(h);await rpc(h,'collaborationMode/list',{threadId:t});await rpc(h,'model/list',{});await rpc(h,'thread/list',{});await rpc(h,'bogus/method',{});await rpc(h,'turn/start',{threadId:'nonexistent',input:[]});await h.sleep(500)},
 async simple(h){await hs(h);const t=await start(h);await turn(h,t,'Reply with exactly the word: pong');await h.sleep(300)},
 async tool_allow(h){await hs(h);const t=await start(h);h.on(m=>{if(m.method&&m.id!==undefined)setTimeout(()=>h.send({jsonrpc:'2.0',id:m.id,result:{decision:'accept'}}),50)});await turn(h,t,'RUNTOOL please');await h.sleep(300)},
 async tool_deny(h){await hs(h);const t=await start(h);h.on(m=>{if(m.method&&m.id!==undefined)setTimeout(()=>h.send({jsonrpc:'2.0',id:m.id,result:{decision:'decline'}}),50)});await turn(h,t,'RUNTOOL please');await h.sleep(300)},
 async interrupt(h){await hs(h);const t=await start(h);let tid;h.on(m=>{if(m.method==='turn/started')tid=m.params.turn.id});const p=turn(h,t,'SLOW please');await h.wait(e=>e.dir==='out'&&e.msg.method==='item/agentMessage/delta',30000);
   await rpc(h,'turn/interrupt',{threadId:t,turnId:tid});await p;await h.sleep(300)},
 async resume(h){const lines=fs.readFileSync(R+'/simple.jsonl','utf8').trim().split('\n').map(JSON.parse);
   const tid=lines.find(l=>l.msg.result?.thread)?.msg.result.thread.id;await hs(h);
   const r=await rpc(h,'thread/resume',{threadId:tid,cwd:'/tmp/cx-x'});await turn(h,tid,'pong again');await h.sleep(300)},
 async mode(h){await hs(h);const t=await start(h);await rpc(h,'collaborationMode/list',{threadId:t});
   await turn2(h,t)},
 async question(h){await hs(h);const t=await start(h);h.on(m=>{if(m.method&&m.id!==undefined){const q=m.params.questions||[];const answers={};for(const x of q)answers[x.id]={answers:[x.options[0].label]};setTimeout(()=>h.send({jsonrpc:'2.0',id:m.id,result:{answers}}),50)}});
   const i=++id;h.send({jsonrpc:'2.0',id:i,method:'turn/start',params:{threadId:t,input:[{type:'text',text:'ASKUSER'}],collaborationMode:{mode:'plan',settings:{model:'fake',reasoning_effort:null,developer_instructions:null}}}});await h.wait(e=>e.dir==='out'&&e.msg.method==='turn/completed',30000);await h.sleep(300)},
 async lifecycle(h){await hs(h);const u='01a1ebc4-0000-7000-8000-000000000000';await rpc(h,'thread/resume',{threadId:u,cwd:'/tmp/cx-x'});await rpc(h,'turn/start',{threadId:u,input:[{type:'text',text:'x'}]});const t=await start(h);await rpc(h,'turn/interrupt',{threadId:t,turnId:u});await rpc(h,'thread/list',{});await rpc(h,'thread/read',{threadId:t});await h.sleep(300)},
 async error(h){await hs(h);const t=await start(h);await turn(h,t,'FAILME');await h.sleep(300)},
};
async function turn2(h,t){const i=++id;h.send({jsonrpc:'2.0',id:i,method:'turn/start',params:{threadId:t,input:[{type:'text',text:'hi'}],collaborationMode:{mode:'plan',settings:{model:'fake',reasoning_effort:null,developer_instructions:null}}}});await h.wait(e=>e.dir==='out'&&e.msg.method==='turn/completed',30000)}
await run({cmd:'codex',args:['app-server','--stdio'],env:{CODEX_HOME:HOME},cwd:'/tmp/cx-x',out:`${R}/${which}.jsonl`,timeout:60000,script:sc[which]});
