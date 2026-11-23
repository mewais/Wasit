// Message lifecycle (claude 2.1.293): user messages with a `uuid` → `command_lifecycle` frames;
// `cancel_async_message` for a message waiting in the CLI's queue and for one already running;
// `interrupt` with a message still queued (`still_queued`), which then runs.
import {run} from './rec.mjs';
const R=(process.env.REF_DIR||'.')+'/claude';
const base=['--print','--input-format','stream-json','--output-format','stream-json','--verbose','--model','haiku','--permission-prompt-tool','stdio'];
const user=(t,uuid)=>({type:'user',message:{role:'user',content:[{type:'text',text:t}]},uuid});
const out=e=>e.dir==='out';
const lc=(uuid,state)=>e=>out(e)&&e.msg.type==='command_lifecycle'&&e.msg.command_uuid===uuid&&(!state||e.msg.state===state);
const resp=id=>e=>out(e)&&e.msg.type==='control_response'&&e.msg.response.request_id===id;
const A='00000000-0000-4000-8000-00000000000a',B='00000000-0000-4000-8000-00000000000b',
      C='00000000-0000-4000-8000-00000000000c',D='00000000-0000-4000-8000-00000000000d';
await run({cmd:'claude',args:base,cwd:process.env.REC_CWD||'/tmp',out:R+'/lifecycle.jsonl',timeout:180000,script:async h=>{
  h.send({type:'control_request',request_id:'init-1',request:{subtype:'initialize',hooks:null}});
  await h.wait(resp('init-1'));
  h.send(user('Reply with exactly the word: pong',A));
  await h.wait(lc(A,'completed'));
  h.send(user('Use the Bash tool to run: sleep 20 . Then reply with exactly the word: done',B));
  // `sleep` needs no permission: wait until the tool runs.
  await h.wait(e=>out(e)&&e.msg.type==='system'&&e.msg.subtype==='task_started');
  h.send(user('Reply with exactly the word: never',C));
  await h.wait(lc(C,'queued'));
  h.send({type:'control_request',request_id:'cancel-c',request:{subtype:'cancel_async_message',message_uuid:C}});
  await h.wait(resp('cancel-c'));
  h.send({type:'control_request',request_id:'cancel-b',request:{subtype:'cancel_async_message',message_uuid:B}});
  await h.wait(resp('cancel-b'));
  h.send(user('Reply with exactly the word: after',D));
  await h.wait(lc(D,'queued'));
  h.send({type:'control_request',request_id:'int-1',request:{subtype:'interrupt'}});
  await h.wait(lc(D,'completed'),120000);
}});
