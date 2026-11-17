// Scripted fake OpenAI Responses server used to record the codex fixtures.
// Run: node fake-responses.mjs <log>; point a CODEX_HOME config.toml provider at
// http://127.0.0.1:18080/v1 (wire_api = "responses", requires_openai_auth = false).
import http from 'node:http'; import fs from 'node:fs';
const LOG = process.argv[2] || 'fake2.log';
let n = 0;
http.createServer((req,res)=>{let b='';req.on('data',d=>b+=d);req.on('end',()=>{
 n++;
 fs.appendFileSync(LOG, `--- ${req.method} ${req.url}\n${b.slice(0,20000)}\n`);
 if(req.url.includes('/models')){res.setHeader('content-type','application/json');return res.end(JSON.stringify({models:[]}))}
 let body={}; try{body=JSON.parse(b)}catch{}
 const input = body.input||[];
 // last item decides
 const last = input[input.length-1]||{};
 let lastUser=''; for(const it of input){ if(it.role==='user'){ for(const c of (it.content||[])){ if(c.text) lastUser=c.text } } }
 res.writeHead(200,{'content-type':'text/event-stream'});
 const ev=(t,o)=>res.write(`event: ${t}\ndata: ${JSON.stringify({type:t,...o})}\n\n`);
 const r={id:'resp_'+n,object:'response',status:'in_progress',model:'fake',output:[]};
 ev('response.created',{response:r});
 const usage={input_tokens:100+n,input_tokens_details:{cached_tokens:10},output_tokens:5,output_tokens_details:{reasoning_tokens:0},total_tokens:105+n};
 const text=(t)=>{
   const item={id:'msg_'+n,type:'message',role:'assistant',status:'in_progress',content:[]};
   ev('response.output_item.added',{output_index:0,item});
   ev('response.content_part.added',{item_id:item.id,output_index:0,content_index:0,part:{type:'output_text',text:''}});
   ev('response.output_text.delta',{item_id:item.id,output_index:0,content_index:0,delta:t});
   const done={...item,status:'completed',content:[{type:'output_text',text:t,annotations:[]}]};
   ev('response.output_item.done',{output_index:0,item:done});
   ev('response.completed',{response:{...r,status:'completed',output:[done],usage}});
   res.end();
 };
 if (last.type==='function_call_output' || last.type==='custom_tool_call_output') { return text('Command finished: '+String(last.output).slice(0,80)); }
 const m = lastUser.match(/RUNCMD:(.*)$/m);
 if (m) {
   const tools=(body.tools||[]).map(t=>t.name||t.type);
   fs.appendFileSync(LOG, `TOOLS ${JSON.stringify(tools)}\n`);
   const name = tools.includes('exec_command')?'exec_command':(tools.includes('shell_command')?'shell_command':'shell');
   let args;
   if(name==='exec_command') args={cmd:m[1].trim(),sandbox_permissions:'require_escalated',justification:'test needs it'};
   else if(name==='shell_command') args={command:m[1].trim(),sandbox_permissions:'require_escalated',justification:'test needs it'};
   else args={command:['bash','-lc',m[1].trim()],sandbox_permissions:'require_escalated',justification:'test needs it'};
   const item={id:'fc_'+n,type:'function_call',status:'completed',call_id:'call_'+n,name,arguments:JSON.stringify(args)};
   ev('response.output_item.added',{output_index:0,item:{...item,arguments:''}});
   ev('response.output_item.done',{output_index:0,item});
   ev('response.completed',{response:{...r,status:'completed',output:[item],usage}});
   return res.end();
 }
 if (lastUser.includes('SLOW')) {
   const item={id:'msg_'+n,type:'message',role:'assistant',status:'in_progress',content:[]};
   ev('response.output_item.added',{output_index:0,item});
   ev('response.content_part.added',{item_id:item.id,output_index:0,content_index:0,part:{type:'output_text',text:''}});
   let i=0; const iv=setInterval(()=>{ if(res.destroyed){clearInterval(iv);return}
     ev('response.output_text.delta',{item_id:item.id,output_index:0,content_index:0,delta:'tick '+(i++)+' '});
     if(i>60){clearInterval(iv); const done={...item,status:'completed',content:[{type:'output_text',text:'ticks',annotations:[]}]};
       ev('response.output_item.done',{output_index:0,item:done}); ev('response.completed',{response:{...r,status:'completed',output:[done],usage}}); res.end();}
   },500);
   res.on('close',()=>clearInterval(iv));
   return;
 }
 text('Hello from fake: '+lastUser.slice(0,40));
});}).listen(18080,'127.0.0.1');
