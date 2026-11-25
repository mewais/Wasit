#!/usr/bin/env python3
"""Scripted OpenAI-compatible chat-completions provider for Kilo sub-agent probes.

Serves POST /v1/chat/completions with `stream: true` (SSE, including `tool_calls` deltas).
Requests without `tools` are title requests and get plain text.

Scripts are chosen by markers in the first user message of a conversation:

* `[[PARENT_BG]]`  parent. Step 0 (no tool call yet): `task` with background=true.
                   After the task result: text ACK. After the background completion
                   (a later user message): text PARENT_DONE.
* `[[CHILD]]`      child (the subagent). Step 0: `glob`. Step 1: HOLD until the release
                   file exists, then `read`. Step 2: text CHILD_DONE.

"Step" is the number of assistant tool-call messages already in the history of that
conversation, so the counting covers the whole history, not the last request.

Every request is appended as one JSON line to $FAKE_LOG (content-free: role, step,
message count, and booleans for which markers appear). Steer texts carry `[[STEER]]`, so
the log shows whether a steer reached the child.

Opt-in parent hold: while $E2E/hold_parent_ack exists, the parent's ACK request (step 1)
blocks until $E2E/release_parent exists, so the parent turn stays open.

Usage: fake_openai_server.py --port 18090 --release /tmp/kilo/e2e/release \
           --release-parent /tmp/kilo/e2e/release_parent \
           --hold-parent-flag /tmp/kilo/e2e/hold_parent_ack \
           --log /tmp/kilo/e2e/requests.jsonl --workdir /tmp/kilo/e2e/work
"""

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = "fake-model"
HOLD_TIMEOUT_S = 600.0

ARGS = None
LOCK = threading.Lock()


def log(entry):
    entry["t"] = int(time.time() * 1000)
    with LOCK:
        with open(ARGS.log, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")


def text_of(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


def first_user_text(messages):
    for m in messages:
        if m.get("role") == "user":
            return text_of(m.get("content"))
    return ""


def all_text(messages):
    return "\n".join(text_of(m.get("content")) for m in messages)


def tool_call_count(messages):
    return sum(1 for m in messages if m.get("role") == "assistant" and m.get("tool_calls"))


def last_role(messages):
    return messages[-1].get("role") if messages else None


def tool_call(name, args):
    return {"kind": "tool", "name": name, "args": args}


def text(s):
    return {"kind": "text", "text": s}


def parent_script(messages):
    n = tool_call_count(messages)
    if n == 0:
        return tool_call(
            "task",
            {
                "description": "probe child",
                "prompt": "[[CHILD]] list files then read one, then answer",
                "subagent_type": "general",
                "background": True,
            },
        )
    if n == 1 and last_role(messages) == "tool":
        # Opt-in HOLD of the parent's ACK (flag file $E2E/hold_parent_ack): keeps the parent
        # turn open while the background child runs, so `interrupt` has a turn to act on.
        # The request blocks until $E2E/release_parent exists (or the client aborts it).
        if os.path.exists(ARGS.hold_parent_flag):
            log({"event": "hold_begin", "conv": "parent"})
            wait_for(ARGS.release_parent)
            log({"event": "hold_end", "conv": "parent"})
        return text("ACK")
    return text("PARENT_DONE")


def wait_for(path):
    deadline = time.time() + HOLD_TIMEOUT_S
    while not os.path.exists(path) and time.time() < deadline:
        time.sleep(0.2)


def child_script(messages):
    n = tool_call_count(messages)
    if n == 0:
        return tool_call("glob", {"pattern": "*.txt", "path": ARGS.workdir})
    if n == 1:
        # HOLD: the request blocks here until the release file exists, so the driver can
        # steer / interrupt / stop while the child is running. Steers that arrive during the
        # hold reach the next request (n == 2) and are visible in the log.
        log({"event": "hold_begin", "conv": "child"})
        wait_for(ARGS.release)
        log({"event": "hold_end", "conv": "child"})
        return tool_call("read", {"filePath": os.path.join(ARGS.workdir, "hello.txt")})
    return text("CHILD_DONE")


def decide(body):
    messages = body.get("messages") or []
    first = first_user_text(messages)
    tools = body.get("tools")
    info = {
        "role_hint": "title" if not tools else None,
        "messages": len(messages),
        "last_role": last_role(messages),
        "steer_seen": "[[STEER]]" in all_text(messages),
    }
    if not tools:
        info["conv"] = "title"
        return info, text("Probe Title")
    if "[[PARENT_BG]]" in first:
        info["conv"] = "parent"
        step = tool_call_count(messages)
        info["step"] = step
        return info, parent_script(messages)
    if "[[CHILD]]" in first:
        info["conv"] = "child"
        step = tool_call_count(messages)
        info["step"] = step
        return info, child_script(messages)
    info["conv"] = "unknown"
    return info, text("UNKNOWN_CONVERSATION")


def sse(obj):
    return ("data: " + json.dumps(obj) + "\n\n").encode("utf-8")


def chunk(delta, finish=None, usage=None, cid="chatcmpl-probe"):
    c = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": MODEL,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}] if delta is not None else [],
    }
    if usage is not None:
        c["usage"] = usage
    return c


def render(script, prompt_tokens):
    out = []
    usage = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": 0,
        "total_tokens": prompt_tokens,
    }
    if script["kind"] == "text":
        out.append(chunk({"role": "assistant", "content": ""}))
        out.append(chunk({"content": script["text"]}))
        out.append(chunk({}, finish="stop"))
        usage["completion_tokens"] = max(1, len(script["text"]) // 4)
    else:
        call_id = "call_%d" % int(time.time() * 1000000)
        args = json.dumps(script["args"])
        out.append(chunk({"role": "assistant", "content": None}))
        out.append(
            chunk(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": call_id,
                            "type": "function",
                            "function": {"name": script["name"], "arguments": args},
                        }
                    ]
                }
            )
        )
        out.append(chunk({}, finish="tool_calls"))
        usage["completion_tokens"] = max(1, len(args) // 4)
    usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
    out.append(chunk(None, usage=usage))
    return out


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            body = {}
        if self.path.rstrip("/") != "/v1/chat/completions":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        info, script = decide(body)
        log(dict(info, event="request", stream=bool(body.get("stream"))))
        prompt_tokens = max(1, len(json.dumps(body.get("messages") or [])) // 4)
        chunks = render(script, prompt_tokens)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            for c in chunks:
                self.wfile.write(sse(c))
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            log({"event": "client_gone", "conv": info.get("conv")})
        self.close_connection = True


def main():
    global ARGS
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--release", required=True)
    p.add_argument("--log", required=True)
    p.add_argument("--workdir", required=True)
    p.add_argument("--release-parent", required=True)
    p.add_argument("--hold-parent-flag", required=True)
    ARGS = p.parse_args()
    srv = ThreadingHTTPServer((ARGS.host, ARGS.port), Handler)
    srv.daemon_threads = True
    log({"event": "listening", "port": ARGS.port})
    srv.serve_forever()


if __name__ == "__main__":
    main()
