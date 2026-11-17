import {run} from './rec.mjs';import fs from 'node:fs';
const [cli,which]=process.argv.slice(2);
const model=cli==='opencode'?'opencode/deepseek-v4.1-flash-free':'kilo/kilo-auto/free';
const R=`${process.env.REF_DIR||'.'}/${cli}`;fs.mkdirSync(R,{recursive:true});
const cwd=`/tmp/ow-${cli}`;fs.mkdirSync(cwd,{recursive:true});
let id=100;
const rpc=(h,method,params)=>{const i=++id;h.send({jsonrpc:'2.0',id:i,method,params});return h.wait(e=>e.dir==='out'&&e.msg.id===i&&('result' in e.msg||'error' in e.msg),60000)};
const hs=async h=>{
  h.send({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:1,clientCapabilities:{fs:{readTextFile:false,writeTextFile:false},terminal:false}}});
  await h.wait(e=>e.dir==='out'&&e.msg.id===1);
  const n=await rpc(h,'session/new',{cwd,mcpServers:[]});
  return n.msg.result.sessionId;
};
const turnEnd=(i)=>e=>e.dir==='out'&&e.msg.id===i&&('result' in e.msg||'error' in e.msg);
const prompt=(h,sid,text)=>{const i=++id;h.send({jsonrpc:'2.0',id:i,method:'session/prompt',params:{sessionId:sid,prompt:[{type:'text',text}]}});return {i,done:h.wait(turnEnd(i),90000)}};
const perm=(h,choose)=>h.on(m=>{if(m.method==='session/request_permission'){const opts=m.params.options;const o=choose(opts);setTimeout(()=>h.send({jsonrpc:'2.0',id:m.id,result:{outcome:o?{outcome:'selected',optionId:o.optionId}:{outcome:'cancelled'}}}),50)}});
const T='Use the bash tool to run exactly: touch /tmp/ow-x/marker.txt . Then say done.';
const sc={
 async simple(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:model});const p=prompt(h,sid,'Reply with exactly the word: pong');await p.done;},
 async handshake(h){h.send({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:1,clientCapabilities:{}}});await h.wait(e=>e.dir==='out'&&e.msg.id===1);
   const n=await rpc(h,'session/new',{cwd,mcpServers:[]});await h.sleep(1500);
   await rpc(h,'authenticate',{methodId:'x'}).catch(()=>{});await rpc(h,'session/list',{cwd});await rpc(h,'bogus/method',{});await rpc(h,'session/prompt',{sessionId:'ses_nonexistent',prompt:[{type:'text',text:'x'}]});},
 async tool_allow(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:model});perm(h,o=>o.find(x=>x.kind==='allow_once')||o[0]);await prompt(h,sid,T).done;},
 async tool_deny(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:model});perm(h,o=>o.find(x=>x.kind==='reject_once')||o[o.length-1]);await prompt(h,sid,T).done;},
 async interrupt(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:model});const p=prompt(h,sid,'Write a 400 word story about a fox.');
   await h.wait(e=>e.dir==='out'&&e.msg.method==='session/update'&&['agent_message_chunk','agent_thought_chunk'].includes(e.msg.params.update.sessionUpdate),60000);
   h.send({jsonrpc:'2.0',method:'session/cancel',params:{sessionId:sid}});await p.done;await h.sleep(500);},
 async load(h){ // create session with a turn, then load in fresh process
   const f=R+'/simple.jsonl';const lines=fs.readFileSync(f,'utf8').trim().split('\n').map(JSON.parse);
   const sid=lines.find(l=>l.dir==='out'&&l.msg.id===2||l.msg.result?.sessionId)?.msg.result.sessionId;
   h.send({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:1,clientCapabilities:{}}});await h.wait(e=>e.dir==='out'&&e.msg.id===1);
   await rpc(h,'session/load',{sessionId:sid,cwd,mcpServers:[]});await h.sleep(1500);
   await rpc(h,'session/list',{});
   const p=prompt(h,sid,'What word did I ask you to reply with? One word.');await p.done;},
 async mode(h){const sid=await hs(h);
   await rpc(h,'session/set_config_option',{sessionId:sid,configId:'mode',value:'plan'});await h.sleep(500);
   await rpc(h,'session/set_mode',{sessionId:sid,modeId:'build'});await h.sleep(500);
   await rpc(h,'session/set_config_option',{sessionId:sid,configId:'mode',value:'nonexistent-mode'});
   await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:'nonexistent/model'});},
 async lifecycle(h){const sid=await hs(h);await rpc(h,'session/load',{sessionId:'ses_nonexistent',cwd,mcpServers:[]});await rpc(h,'session/resume',{sessionId:sid,cwd,mcpServers:[]});await h.sleep(300);await rpc(h,'session/fork',{sessionId:sid,cwd,mcpServers:[]});await h.sleep(300);await rpc(h,'session/close',{sessionId:sid});await rpc(h,'session/cancel',{sessionId:sid}).catch(()=>{});await rpc(h,'session/set_model',{sessionId:sid,modelId:'x/y'});},
 async plan(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:model});perm(h,o=>o.find(x=>x.kind==='allow_once')||o[0]);await prompt(h,sid,'Use the todowrite tool to create a todo list with exactly two items: "step one" and "step two". Then say ok.').done;},
 async error(h){const sid=await hs(h);await rpc(h,'session/set_config_option',{sessionId:sid,configId:'model',value:'opencode/does-not-exist'}).catch(()=>{});const p=prompt(h,sid,'hi');await p.done;},
};
await run({cmd:cli,args:['acp'],cwd,out:`${R}/${which}.jsonl`,timeout:60000,script:sc[which]});
