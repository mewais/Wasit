import json,glob,sys,os
BIG={'commands','agents','models','tools','skills','slash_commands','terminal_slash_commands','available_output_styles','availableCommands','sessions','capabilities','plugins'}
def trim(v,key=None,cur=None):
    if isinstance(v,dict):
        c=v.get('currentValue') if 'options' in v else None
        return {k:trim(x,k,c) for k,x in v.items()}
    if isinstance(v,list):
        if key=='options' and cur is not None and len(v)>6 and isinstance(v[0],dict) and 'value' in v[0]:
            return v[:3]+[o for o in v if o['value']==cur and o not in v[:3]]
        if key in BIG and len(v)>4: return [trim(x) for x in v[:4]]
        if key=='data' and len(v)>4 and isinstance(v[0],dict) and 'model' in v[0]: return [trim(x) for x in v[:3]]
        return [trim(x,key) for x in v]
    return v
for f in glob.glob(os.environ.get('REF_DIR','.')+'/*/*.jsonl'):
    out=[]
    for l in open(f):
        e=json.loads(l); e['msg']=trim(e['msg']); out.append(json.dumps(e,separators=(',',':')))
    open(f,'w').write('\n'.join(out)+'\n')
