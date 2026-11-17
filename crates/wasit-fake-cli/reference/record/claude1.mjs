import {run} from './rec.mjs';
const R=(process.env.REF_DIR||'.')+'/claude';
import fs from 'node:fs';fs.mkdirSync(R,{recursive:true});
const base=['--print','--input-format','stream-json','--output-format','stream-json','--verbose','--model','haiku','--permission-prompt-tool','stdio'];
const user=t=>({type:'user',message:{role:'user',content:[{type:'text',text:t}]}});
const isResult=e=>e.dir==='out'&&e.msg.type==='result';
// 1 handshake + simple prompt
await run({cmd:'claude',args:base,cwd:'/tmp',out:R+'/simple.jsonl',script:async h=>{
  h.send({type:'control_request',request_id:'init-1',request:{subtype:'initialize',hooks:null}});
  await h.wait(e=>e.dir==='out'&&e.msg.type==='control_response');
  h.send(user('Reply with exactly the word: pong'));
  await h.wait(isResult);
}});
