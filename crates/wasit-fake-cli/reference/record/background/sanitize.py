#!/usr/bin/env python3
"""Sanitize a wasit-smoke transcript (--record) for use as a fixture: the home path, MCP server
names, skill/command/plugin names and the messaging socket of the recording machine are replaced;
long catalogues (tools, commands, models, ...) are trimmed to four entries. `in` and `cmd` records
(what the driver wrote) are never touched: the fixture tests compare them byte for byte."""
import json, sys, re

HOME = "/home/mewais"
BIG = {"commands", "agents", "models", "tools", "skills", "slash_commands",
       "terminal_slash_commands", "available_output_styles", "plugins"}
SKILL_KEYS = {"skills", "slash_commands", "commands", "terminal_slash_commands"}

def names(lst):
    out = []
    for x in lst:
        out.append(x if isinstance(x, str) else x.get("name", ""))
    return out

def skill(i):
    return "skill-" + "abcd"[i]

def clean(v, key=None):
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if k == "mcp_servers" and isinstance(x, list):
                out[k] = [dict(m, name="mcp-" + "abcd"[i]) for i, m in enumerate(x)]
            elif k == "plugins" and isinstance(x, list):
                out[k] = []
            elif k == "memory_paths":
                out[k] = clean(x, k)
            elif k == "messaging_socket_path":
                out[k] = "/run/user/1000/cc-socks/1.sock"
            elif k in SKILL_KEYS and isinstance(x, list):
                items = []
                for i, e in enumerate(x[:4]):
                    if isinstance(e, str):
                        items.append(skill(i))
                    else:
                        e = dict(e)
                        e["name"] = skill(i)
                        if "description" in e:
                            e["description"] = "A skill."
                        items.append(e)
                out[k] = items
            elif k in BIG and isinstance(x, list) and len(x) > 4:
                out[k] = [clean(e, k) for e in x[:4]]
            else:
                out[k] = clean(x, k)
        return out
    if isinstance(v, list):
        return [clean(e, key) for e in v]
    if isinstance(v, str):
        return v.replace(HOME, "/home/user")
    return v

for line in open(sys.argv[1]):
    rec = json.loads(line)
    kind = next(iter(rec))
    if kind == "out":
        try:
            v = json.loads(rec["out"])
        except Exception:
            print(line.rstrip("\n")); continue
        rec["out"] = json.dumps(clean(v), separators=(",", ":"), ensure_ascii=False)
        print(json.dumps(rec, separators=(",", ":"), ensure_ascii=False))
    elif kind == "start":
        s = rec["start"]
        s = json.loads(json.dumps(s).replace(HOME, "/home/user"))
        print(json.dumps({"start": s}, separators=(",", ":"), ensure_ascii=False))
    else:
        print(line.rstrip("\n"))
