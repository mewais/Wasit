import http from 'node:http';import fs from 'node:fs';
let n=0;
http.createServer((req,res)=>{let b='';req.on('data',d=>b+=d);req.on('end',async()=>{
 fs.appendFileSync('/tmp/fake-responses.log',req.method+' '+req.url+' '+b+'\n');
 if(req.url.includes('/models')){res.setHeader('content-type','application/json');return res.end(JSON.stringify({object:'list',data:[{id:'fake',object:'model'}]}))}
 let body={};try{body=JSON.parse(b)}catch{}
 const input=JSON.stringify(body.input||[]);
 const last=(body.input||[]).slice(-1)[0]||{};
 const userText=JSON.stringify((body.input||[]).filter(i=>i.role==='user').slice(-1)[0]||'');
 if(userText.includes('FAILME')){res.writeHead(400,{'content-type':'application/json'});return res.end(JSON.stringify({error:{message:'fake upstream failure',type:'invalid_request_error',code:'bad'}}))}
 res.writeHead(200,{'content-type':'text/event-stream'});
 const ev=(t,o)=>res.write(`event: ${t}\ndata: ${JSON.stringify({type:t,...o})}\n\n`);
 const id=++n;
 const r={id:'resp_'+id,object:'response',status:'in_progress',model:'fake',output:[]};
 ev('response.created',{response:r});
 const finish=(items)=>ev('response.completed',{response:{...r,status:'completed',output:items,usage:{input_tokens:10,output_tokens:5,total_tokens:15}}});
 const sleep=ms=>new Promise(r=>setTimeout(r,ms));
 if(last.type==='function_call_output'){
   const item={id:'msg_'+id,type:'message',role:'assistant',status:'in_progress',content:[]};
   ev('response.output_item.added',{output_index:0,item});
   ev('response.content_part.added',{item_id:item.id,output_index:0,content_index:0,part:{type:'output_text',text:''}});
   ev('response.output_text.delta',{item_id:item.id,output_index:0,content_index:0,delta:'done'});
   const done={...item,status:'completed',content:[{type:'output_text',text:'done',annotations:[]}]};
   ev('response.output_item.done',{output_index:0,item:done});finish([done]);return res.end();
 }
 if(userText.includes('ASKUSER')){
   const item={id:'fc_'+id,type:'function_call',call_id:'call_'+id,name:'request_user_input',arguments:JSON.stringify({questions:[{id:'color',header:'Colour',question:'Red or blue?',options:[{label:'Red',description:'Choose red'},{label:'Blue',description:'Choose blue'}]}]}),status:'completed'};
   ev('response.output_item.added',{output_index:0,item:{...item,arguments:''}});
   ev('response.output_item.done',{output_index:0,item});finish([item]);return res.end();
 }
 if(userText.includes('RUNTOOL')){
   const item={id:'fc_'+id,type:'function_call',call_id:'call_'+id,name:'exec_command',arguments:JSON.stringify({cmd:'touch /tmp/cx-x/marker.txt',workdir:'/tmp/cx-x',yield_time_ms:1000}),status:'completed'};
   ev('response.output_item.added',{output_index:0,item:{...item,arguments:''}});
   ev('response.function_call_arguments.delta',{item_id:item.id,output_index:0,delta:item.arguments});
   ev('response.output_item.done',{output_index:0,item});finish([item]);return res.end();
 }
 const text=userText.includes('SLOW')?'slow reply '.repeat(20):userText.includes('pong')?'pong':'Hello from fake';
 const item={id:'msg_'+id,type:'message',role:'assistant',status:'in_progress',content:[]};
 ev('response.output_item.added',{output_index:0,item});
 ev('response.content_part.added',{item_id:item.id,output_index:0,content_index:0,part:{type:'output_text',text:''}});
 if(userText.includes('SLOW')){for(let i=0;i<40;i++){ev('response.output_text.delta',{item_id:item.id,output_index:0,content_index:0,delta:'tick '});await sleep(250)}}
 else ev('response.output_text.delta',{item_id:item.id,output_index:0,content_index:0,delta:text});
 const done={...item,status:'completed',content:[{type:'output_text',text,annotations:[]}]};
 ev('response.output_item.done',{output_index:0,item:done});finish([done]);res.end();});}).listen(18080,'127.0.0.1');
