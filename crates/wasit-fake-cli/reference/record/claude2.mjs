import {run} from './rec.mjs';import fs from 'node:fs';const fs_read=p=>fs.readFileSync(p,'utf8');
const R=(process.env.REF_DIR||'.')+'/claude';
const base=['--print','--input-format','stream-json','--output-format','stream-json','--verbose','--model','haiku','--permission-prompt-tool','stdio'];
const user=t=>({type:'user',message:{role:'user',content:[{type:'text',text:t}]}});
const isResult=e=>e.dir==='out'&&e.msg.type==='result';
const isCtl=e=>e.dir==='out'&&e.msg.type==='control_request';
const init=async h=>{h.send({type:'control_request',request_id:'init-1',request:{subtype:'initialize',hooks:null}});await h.wait(e=>e.dir==='out'&&e.msg.type==='control_response')};
const P='Use the Bash tool to run exactly: touch /tmp/cw1/marker.txt . Then say done.';
const which=process.argv[2];
if(which==='ask') await run({cmd:'claude',args:base,cwd:'/tmp/cw1',out:R+'/ask_question.jsonl',script:async h=>{
  await init(h);h.send(user('Use the AskUserQuestion tool to ask me whether I prefer red or blue (two options). After my answer, say the colour.'));
  const c=await h.wait(isCtl);
  const inp=c.msg.request.input;
  const answers={};for(const q of inp.questions||[])answers[q.question]=q.options[0].label;
  h.send({type:'control_response',response:{subtype:'success',request_id:c.msg.request_id,response:{behavior:'allow',updatedInput:{questions:inp.questions,answers}}}});
  await h.wait(isResult);
}});
if(which==='error') await run({cmd:'claude',args:[...base.filter((x,i)=>base[i-1]!=='--model'&&x!=='--model'),'--model','no-such-model-xyz'],cwd:'/tmp/cw1',out:R+'/turn_error.jsonl',script:async h=>{
  await init(h);h.send(user('hi'));
  await h.wait(isResult);
}});
if(which==='resume'){
  const sid=JSON.parse(fs_read(R+'/simple.jsonl').split('\n')[1]).msg.session_id;
  await run({cmd:'claude',args:[...base,'--resume',sid],cwd:'/tmp',out:R+'/resume.jsonl',script:async h=>{
  await init(h);h.send(user('What single word did I ask you to reply with earlier? Answer with just that word.'));
  await h.wait(isResult);
}});}
if(which==='allow') await run({cmd:'claude',args:base,cwd:'/tmp/cw1',out:R+'/tool_allow.jsonl',script:async h=>{
  await init(h);h.send(user(P));
  const c=await h.wait(isCtl);
  h.send({type:'control_response',response:{subtype:'success',request_id:c.msg.request_id,response:{behavior:'allow',updatedInput:c.msg.request.input}}});
  await h.wait(isResult);
}});
if(which==='deny') await run({cmd:'claude',args:base,cwd:'/tmp/cw1',out:R+'/tool_deny.jsonl',script:async h=>{
  await init(h);h.send(user(P));
  const c=await h.wait(isCtl);
  h.send({type:'control_response',response:{subtype:'success',request_id:c.msg.request_id,response:{behavior:'deny',message:'User denied'}}});
  await h.wait(isResult);
}});
if(which==='interrupt') await run({cmd:'claude',args:base,cwd:'/tmp/cw1',out:R+'/interrupt.jsonl',script:async h=>{
  await init(h);h.send(user('Write a 300 word story about a fox.'));
  await h.wait(e=>e.dir==='out'&&e.msg.type==='system'&&e.msg.subtype==='init');
  await h.sleep(800);
  h.send({type:'control_request',request_id:'int-1',request:{subtype:'interrupt'}});
  await h.wait(isResult);await h.sleep(500);
}});
if(which==='mode') await run({cmd:'claude',args:base,cwd:'/tmp/cw1',out:R+'/set_mode.jsonl',script:async h=>{
  await init(h);
  h.send({type:'control_request',request_id:'mode-1',request:{subtype:'set_permission_mode',mode:'plan'}});
  await h.wait(e=>e.dir==='out'&&e.msg.type==='control_response'&&e.msg.response.request_id==='mode-1');
  h.send({type:'control_request',request_id:'mode-2',request:{subtype:'set_permission_mode',mode:'bogus'}});
  await h.wait(e=>e.dir==='out'&&e.msg.type==='control_response'&&e.msg.response.request_id==='mode-2');
  h.send({type:'control_request',request_id:'model-1',request:{subtype:'set_model',model:'haiku'}});
  await h.wait(e=>e.dir==='out'&&e.msg.type==='control_response'&&e.msg.response.request_id==='model-1');
}});
if(which==='partial') await run({cmd:'claude',args:[...base,'--include-partial-messages'],cwd:'/tmp/cw1',out:R+'/partial_messages.jsonl',script:async h=>{
  await init(h);h.send(user('Reply with exactly the word: pong'));await h.wait(isResult);
}});
