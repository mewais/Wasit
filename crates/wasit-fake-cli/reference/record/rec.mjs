import {spawn} from 'node:child_process';import fs from 'node:fs';
// usage: import {run} ; run({cmd,args,env,cwd,out,script:async(h)=>{}})
export function run({cmd,args,env={},cwd,out,script,timeout=90000}){
  return new Promise((resolve)=>{
    const p=spawn(cmd,args,{cwd,env:{...process.env,...env},stdio:['pipe','pipe','pipe']});
    const log=[];const handlers=[];let buf='';const waiters=[];
    const rec=(dir,line)=>{let msg;try{msg=JSON.parse(line)}catch{msg={_raw:line}};const e={dir,msg};log.push(e);if(dir==='out')for(const f of handlers)f(e.msg);for(const w of [...waiters]){if(w.pred(e)){waiters.splice(waiters.indexOf(w),1);w.res(e)}}};
    p.stdout.on('data',d=>{buf+=d;let i;while((i=buf.indexOf('\n'))>=0){const l=buf.slice(0,i);buf=buf.slice(i+1);if(l.trim())rec('out',l)}});
    let err='';p.stderr.on('data',d=>err+=d);
    const h={
      send:(o)=>{const s=typeof o==='string'?o:JSON.stringify(o);rec('in',s);p.stdin.write(s+'\n')},
      wait:(pred,ms=60000)=>new Promise((res,rej)=>{const ex=log.find(e=>pred(e)&&!e._seen);const t=setTimeout(()=>rej(new Error('timeout waiting')),ms);const w={pred,res:(e)=>{clearTimeout(t);res(e)}};waiters.push(w)}),
      sleep:(ms)=>new Promise(r=>setTimeout(r,ms)),
      on:f=>handlers.push(f),log,p
    };
    const done=()=>{fs.writeFileSync(out,log.map(e=>JSON.stringify(e)).join('\n')+'\n');fs.writeFileSync(out+'.stderr',err);try{p.kill()}catch{};resolve(log)};
    const to=setTimeout(done,timeout);
    script(h).catch(e=>{console.error('script err',e.message)}).finally(()=>{clearTimeout(to);setTimeout(done,500)});
  });
}
