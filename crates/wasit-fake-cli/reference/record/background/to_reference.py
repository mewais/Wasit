#!/usr/bin/env python3
"""A (sanitized) wasit-smoke transcript -> a fake-cli reference recording: the client's lines
(`in`) and the CLI's (`out`) as `{"dir","msg"}`. Usage: to_reference.py FIXTURE.ndjson > OUT.jsonl"""
import json, sys
for line in open(sys.argv[1]):
    rec = json.loads(line)
    kind = next(iter(rec))
    if kind in ("in", "out"):
        print(json.dumps({"dir": kind, "msg": json.loads(rec[kind])}, separators=(",", ":"), ensure_ascii=False))
