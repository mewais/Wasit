#!/usr/bin/env python3
"""Socket client for wasit-hostd (wasit-control framing: one JSON frame per line).

It prints content-free summaries only (phases, counts, ids, booleans). Prompt and steer text
are sent but never echoed.

  drive.py create                       sessions.create (driver kilo, cwd = $E2E/work)
  drive.py prompt <sid> <text>          session.command prompt
  drive.py steer <sid> <task> <text>    session.command steer_subagent
  drive.py interrupt <sid>              session.command interrupt
  drive.py stop_task <sid> <task>       session.command stop_task
  drive.py snap <sid>                   one content-free snapshot summary
  drive.py wait <sid> <phase> [secs]    poll until snapshot phase == <phase> (or timeout)
  drive.py call <method> [json]         raw request, prints the result keys only

Socket: $E2E/data/hostd.sock (override with WASIT_SOCK). Build string: WASIT_BUILD.
"""

import json
import os
import socket
import sys
import time

E2E = os.environ.get("E2E", "/tmp/kilo/e2e")
SOCK = os.environ.get("WASIT_SOCK", os.path.join(E2E, "data", "hostd.sock"))
BUILD = os.environ.get("WASIT_BUILD", "wasit-hostd/0.1.0")
HOST_ID = os.environ.get("WASIT_HOST_ID", "e2e-driver")


class Client:
    def __init__(self):
        self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.s.settimeout(120)
        self.s.connect(SOCK)
        self.f = self.s.makefile("rb")
        self.next_id = 1
        self.s.sendall(
            (
                json.dumps(
                    {
                        "type": "hello",
                        "protocol": {"major": 1, "minor": 0},
                        "build": BUILD,
                        "host_id": HOST_ID,
                        "role": "gui",
                    }
                )
                + "\n"
            ).encode()
        )
        hello = self.read_frame()
        if hello.get("type") != "hello":
            raise SystemExit("handshake: expected hello, got %r" % hello.get("type"))

    def read_frame(self):
        line = self.f.readline()
        if not line:
            raise SystemExit("connection closed by hostd")
        return json.loads(line)

    def call(self, method, params):
        rid = self.next_id
        self.next_id += 1
        self.s.sendall(
            (
                json.dumps({"type": "request", "id": rid, "method": method, "params": params})
                + "\n"
            ).encode()
        )
        while True:
            fr = self.read_frame()
            if fr.get("type") == "response" and fr.get("id") == rid:
                if fr.get("error"):
                    err = fr["error"]
                    # Error text may echo user content; print the code only.
                    raise SystemExit("rpc error (%s): %s" % (method, err.get("code")))
                return fr.get("result")
            # Events / pings on this connection are ignored here.


def command(c, sid, cmd):
    res = c.call("session.command", {"id": sid, "command": cmd})
    return res.get("outcome") if isinstance(res, dict) else res


def snapshot(c, sid):
    return c.call("session.snapshot", {"id": sid})


def summarize(snap):
    tasks = snap.get("tasks", [])
    subs = snap.get("subagents", [])
    usage = snap.get("usage") or {}
    return {
        "phase": (snap.get("phase") or {}).get("state"),
        "turn_open": snap.get("turn") is not None,
        "background_running": snap.get("background_running"),
        "pending_gates": len(snap.get("pending_gates") or []),
        "turns_completed": snap.get("turns_completed"),
        "usage_input": usage.get("input_tokens"),
        "usage_output": usage.get("output_tokens"),
        "subagents": [
            {
                "task_id": s.get("task_id"),
                "running": s.get("running"),
                "status": s.get("status"),
                "outcome": s.get("outcome"),
                "tool_calls": s.get("tool_calls"),
                "has_usage": s.get("usage") is not None,
            }
            for s in subs
        ],
        "tasks": [
            {
                "task_id": t.get("task_id"),
                "kind": t.get("kind"),
                "background": t.get("background"),
                "running": t.get("running"),
                "status": t.get("status"),
                "tool_calls_scoped": t.get("tool_calls"),
            }
            for t in tasks
        ],
    }


def main(argv):
    if not argv:
        raise SystemExit(__doc__)
    op, args = argv[0], argv[1:]
    c = Client()
    if op == "create":
        cwd = os.path.join(E2E, "work")
        r = c.call(
            "sessions.create",
            {"driver": "kilo", "cwd": cwd, "model": "fake/fake-model"},
        )
        print(json.dumps({"id": r.get("id"), "cwd_set": r.get("cwd") is not None}))
    elif op == "prompt":
        print(json.dumps({"outcome": command(c, args[0], {"command": "prompt", "text": args[1]})}))
    elif op == "steer":
        print(json.dumps({"outcome": command(c, args[0], {"command": "steer_subagent", "task_id": args[1], "text": args[2]})}))
    elif op == "interrupt":
        print(json.dumps({"outcome": command(c, args[0], {"command": "interrupt"})}))
    elif op == "stop_task":
        print(json.dumps({"outcome": command(c, args[0], {"command": "stop_task", "task_id": args[1]})}))
    elif op == "snap":
        print(json.dumps(summarize(snapshot(c, args[0]))))
    elif op == "wait":
        sid, want = args[0], args[1]
        limit = float(args[2]) if len(args) > 2 else 60.0
        end = time.time() + limit
        while True:
            s = summarize(snapshot(c, sid))
            if s["phase"] == want or time.time() > end:
                print(json.dumps({"reached": s["phase"] == want, "snap": s}))
                break
            time.sleep(0.25)
    elif op == "call":
        params = json.loads(args[1]) if len(args) > 1 else {}
        r = c.call(args[0], params)
        keys = sorted(r.keys()) if isinstance(r, dict) else type(r).__name__
        print(json.dumps({"result_keys": keys}))
    else:
        raise SystemExit("unknown op %r\n%s" % (op, __doc__))


if __name__ == "__main__":
    main(sys.argv[1:])
