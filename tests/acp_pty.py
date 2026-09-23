#!/usr/bin/env python3
# acp_pty.py -- spark-acp driven the way an ACP client drives it. The name
# is the factory template's (every spark app's CI runs tests/<app>_pty.py);
# there is no pty here: JSON-RPC 2.0, one message per line, over plain
# pipes -- that is the whole of the ACP stdio transport.
#
# A stub `spark` (SPARK_BIN) plays contract 15, `spark do --porcelain`: a
# scripted transcript per goal word, every stdin line it reads logged.
# Proves: initialize and session/new answer the v1 shapes; an MCP server
# the client offers is never started; a step is a tool call plus one
# permission question whose answer reaches spark's stdin as run / skip;
# output and exit codes land in the call; a danger step outside the
# sandbox is never asked (spark refuses it); a sandboxed run's review is
# diffs + a delete call + one question -> accept / discard; session/cancel
# while a question waits answers `quit`, and a spark that reads nothing
# is stopped; one prompt at a time; a spark without --porcelain, or no
# spark, is one clear error; stdout carries nothing but JSON-RPC. And the
# edges: the goal always after `--`; the review carries +x, the reason,
# control characters and every git item, what is held back its own call
# before the question; `stopped` and a refusal before the run end the
# turn with spark's hint; a cancel before spark starts means it never
# starts; a line over 8 MB stops spark; a review past 2 MB of diff shows
# lines; a malformed message never ends the agent, and no spark outlives
# it (stdin closed, or SIGTERM); a late answer finds its question gone;
# sessions are bounded, the oldest idle one dropped.
#
#   python3 tests/acp_pty.py

import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT = os.path.join(REPO, "spark-acp")
WAIT = 15

STUB = r'''#!@PY@
# the stub spark: `do -h` names --porcelain (unless STUB_OLD); `do
# --porcelain [--sandbox] GOAL` plays the transcript the goal names and
# logs argv, cwd, pid and every stdin line to STUB_LOG.
import json, os, sys, time

def rec(kind, value):
    with open(os.environ["STUB_LOG"], "a") as f:
        f.write(json.dumps([kind, value]) + "\n")

def emit(**ev):
    sys.stdout.write(json.dumps(ev) + "\n")
    sys.stdout.flush()

def entry(path, status, old=None, new=None, exec=False, reason="", control=False):
    return {"path": path, "status": status, "old": old, "new": new,
            "exec": exec, "reason": reason, "control": control}

def wait():
    line = sys.stdin.readline()
    word = line.strip() if line else "EOF"
    rec("stdin", word)
    return word

args = sys.argv[1:]
if args[:2] == ["do", "-h"]:
    time.sleep(float(os.environ.get("STUB_SLOW_H") or 0))
    print("spark do -- a task, step by step")
    if not os.environ.get("STUB_OLD"):
        print("  spark do --porcelain <words>   JSON Lines, for a program (contract 15)")
    sys.exit(0)
rec("argv", args)
rec("cwd", os.getcwd())
rec("pid", os.getpid())
sandbox, goal = "--sandbox" in args, args[-1]
if "refuseall" in goal:
    emit(ev="end", reason="refused", hint="STUB-REFUSED-RUN the sandbox needs a project directory", rc=2)
    sys.exit(2)
if "crash" in goal:
    sys.stderr.write("spark do -- STUB-CRASH the sandbox needs a project directory\n")
    sys.exit(2)
emit(ev="start", thread="th1", sandbox=sandbox, run="r1" if sandbox else None)
if "plain" in goal:
    emit(ev="step", n=1, command="ls", hint="list the files", danger=False, proof=None)
    if wait() == "run":
        emit(ev="output", n=1, text="a.txt\n")
        emit(ev="output", n=1, text="b.txt\n")
        emit(ev="rc", n=1, rc=0)
    emit(ev="step", n=2, command="rm -f b.txt", hint="remove b", danger=False, proof="test ! -e b.txt")
    if wait() == "skip":
        emit(ev="note", text="STUB-NOTE skipped rm -f b.txt")
    emit(ev="step", n=3, command="false", hint="fail once", danger=False, proof=None)
    if wait() == "run":
        emit(ev="rc", n=3, rc=1)
    emit(ev="end", reason="done", hint="STUB-DONE two files, one kept", rc=0)
elif "danger" in goal:
    emit(ev="step", n=1, command="rm -rf build", hint="clean", danger=True, proof=None)
    emit(ev="note", text="STUB-REFUSED rm -rf build can destroy data -- run it in a terminal")
    emit(ev="end", reason="done", hint="nothing removed", rc=0)
elif "review" in goal:
    emit(ev="step", n=1, command="make fmt", hint="format", danger=False, proof=None)
    emit(ev="output", n=1, text="formatted 2 files\n")
    emit(ev="rc", n=1, rc=0)
    emit(ev="review", run="r1", files=[
        entry("new.txt", "added", None, "hello\n"),
        entry("src/a.py", "changed", "x=1\n", "x = 1\n"),
        entry("tool.sh", "added", None, "#!/bin/sh\n", exec=True),
        entry("old.txt", "deleted", "bye\n", None),
        entry("run.sh", "mode", "644", "755", exec=True),
        entry("suid", "mode", "755", "755", reason="setuid masked"),
        entry("lnk", "link", None, "src/a.py"),
        entry(".git/hooks/pre-commit", "held", reason="a git hook -- never applied"),
        entry(".git/config", "held", reason="git config -- never applied"),
        entry(".git/info/exclude", "changed", "", "*.o\n"),
        entry("escape", "refused", reason="a link out of the directory"),
        entry("logo.png", "binary"),
        entry("big.log", "added", reason="too large to show"),
        entry("a^[[31m.txt", "added", None, "x\n", control=True)])
    word = wait()
    if word == "accept":
        emit(ev="note", text="STUB-APPLIED 5 changes")
        emit(ev="end", reason="done", hint="applied", rc=0)
    elif word == "discard":
        emit(ev="end", reason="done", hint="STUB-DISCARDED nothing changed", rc=0)
    else:
        emit(ev="end", reason="quit", hint="the run waits: spark do --review r1", rc=1)
elif "bigdiff" in goal:
    emit(ev="review", run="r1", files=[
        entry("one.txt", "added", None, "a" * (1536 * 1024)),
        entry("two.txt", "changed", "b", "c" * (1024 * 1024)),
        entry("three.txt", "added", None, "d\n")])
    wait()
    emit(ev="end", reason="done", hint="discarded", rc=0)
elif "stopped" in goal:
    emit(ev="step", n=1, command="ls", hint="list", danger=False, proof=None)
    wait()
    emit(ev="end", reason="stopped", hint="STUB-STOPPED the same step again after a skip", rc=1)
elif "longline" in goal:
    sys.stdout.write('{"ev": "note", "text": "' + "x" * (9 * 1024 * 1024))
    sys.stdout.flush()
    time.sleep(60)
elif "hang" in goal:
    emit(ev="step", n=1, command="sleep 1", hint="wait", danger=False, proof=None)
    if wait() == "quit":
        emit(ev="end", reason="quit", hint="stopped", rc=1)
elif "think" in goal:
    time.sleep(60)          # a model that thinks, reading nothing
elif "cap" in goal:
    emit(ev="step", n=1, command="true", hint="once more", danger=False, proof=None)
    if wait() == "run":
        emit(ev="rc", n=1, rc=0)
    emit(ev="end", reason="cap", hint="STUB-CAP 8 steps and not done", rc=1)
'''


class Agent:
    """spark-acp as a child; every stdout line must be a JSON-RPC message."""

    def __init__(self, env):
        self.p = subprocess.Popen([sys.executable, AGENT], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=env.pop("_STDERR"), env=env)
        self.q, self.bad, self.next = queue.Queue(), [], 0
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        for raw in self.p.stdout:
            try:
                msg = json.loads(raw)
                if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
                    raise ValueError
            except ValueError:
                self.bad.append(raw)
                continue
            self.q.put(msg)
        self.q.put(None)

    def write(self, msg):
        self.p.stdin.write((json.dumps(dict(msg, jsonrpc="2.0")) + "\n").encode())
        self.p.stdin.flush()

    def call(self, method, params, answer=None, on_ask=None):
        """Send a request; collect updates and questions until its answer.
        answer(question) -> the permission result; on_ask(question) runs
        instead when given (it may answer, or not)."""
        self.next += 1
        rid = self.next
        self.write({"id": rid, "method": method, "params": params})
        return self.until(rid, answer, on_ask)

    def until(self, rid, answer=None, on_ask=None):
        updates, asks = [], []
        deadline = time.time() + WAIT
        while time.time() < deadline:
            try:
                msg = self.q.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            if msg is None:
                break
            if msg.get("method") == "session/update":
                updates.append(msg["params"])
            elif msg.get("method") == "session/request_permission":
                msg["_at"] = len(updates)
                asks.append(msg)
                if on_ask:
                    on_ask(msg)
                else:
                    self.write({"id": msg["id"], "result": answer(msg)})
            elif msg.get("id") == rid:
                return msg, updates, asks
        return None, updates, asks

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(10)
        except (OSError, subprocess.TimeoutExpired):
            self.p.kill()


def picks(*ids):
    """A client that answers each question with the next option id."""
    it = iter(ids)
    return lambda q: {"outcome": {"outcome": "selected", "optionId": next(it)}}


def main():
    fails = []

    def ok(cond, what, extra=""):
        print("  %s %s" % ("ok  " if cond else "FAIL", what))
        if not cond:
            fails.append(what)
            if extra:
                print("       %s" % extra)

    tmp = tempfile.mkdtemp(prefix="spark-acp-test.")
    try:
        stub = os.path.join(tmp, "spark")
        with open(stub, "w") as f:
            f.write(STUB.replace("@PY@", sys.executable))
        os.chmod(stub, 0o755)
        proj = os.path.realpath(os.path.join(tmp, "proj"))
        os.makedirs(proj)
        log = os.path.join(tmp, "stub.log")
        marker = os.path.join(tmp, "MCP-STARTED")
        errf = open(os.path.join(tmp, "stderr"), "w")

        def env(**extra):
            e = dict(os.environ, SPARK_BIN=stub, STUB_LOG=log, _STDERR=errf)
            e.pop("STUB_OLD", None)
            e.update(extra)
            return e

        def stub_log():
            try:
                with open(log) as f:
                    rows = [json.loads(line) for line in f]
            except OSError:
                rows = []
            open(log, "w").close()
            return rows

        def of(rows, kind):
            return [v for k, v in rows if k == kind]

        def kinds(updates, kind):
            return [u["update"] for u in updates if u["update"].get("sessionUpdate") == kind]

        def gone(pid, wait=5):
            end = time.time() + wait
            while time.time() < end:
                try:
                    os.kill(pid, 0)
                except OSError:
                    return True
                time.sleep(0.1)
            return False

        def said(updates):
            return "".join(u["content"]["text"] for u in kinds(updates, "agent_message_chunk"))

        print("spark-acp over pipes, against a stub spark (contract 15)")
        a = Agent(env())

        # -- initialize --------------------------------------------------
        r, _, _ = a.call("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
        res = (r or {}).get("result") or {}
        ok(res.get("protocolVersion") == 1, "initialize: protocolVersion 1")
        ok(res.get("agentCapabilities") == {"loadSession": False, "promptCapabilities":
                                            {"image": False, "audio": False, "embeddedContext": False}},
           "initialize: loadSession false, text prompts only", res.get("agentCapabilities"))
        ok(res.get("authMethods") == [], "initialize: no auth methods")
        info = res.get("agentInfo") or {}
        ok(info.get("name") == "spark-acp" and info.get("title") == "spark" and info.get("version"),
           "initialize: agentInfo spark-acp / spark / a version", info)
        r, _, _ = a.call("initialize", {"protocolVersion": 7})
        ok(((r or {}).get("result") or {}).get("protocolVersion") == 1,
           "initialize: another version is answered with 1")

        # -- session/new -------------------------------------------------
        mcp = [{"name": "trap", "command": "/bin/sh", "args": ["-c", "touch '%s'" % marker], "env": []},
               {"type": "http", "name": "web", "url": "http://192.0.2.1/mcp", "headers": []}]
        r, _, _ = a.call("session/new", {"cwd": proj, "mcpServers": mcp})
        res = (r or {}).get("result") or {}
        sid = res.get("sessionId")
        ok(isinstance(sid, str) and sid, "session/new: a sessionId")
        modes = res.get("modes") or {}
        ok(modes.get("currentModeId") == "ask"
           and [m["id"] for m in modes.get("availableModes", [])] == ["ask", "sandbox"],
           "session/new: modes ask (current) and sandbox", modes)
        cfg = (res.get("configOptions") or [{}])[0]
        ok(cfg.get("id") == "mode" and cfg.get("category") == "mode" and cfg.get("type") == "select"
           and cfg.get("currentValue") == "ask", "session/new: the same choice as a config option", cfg)
        for bad, why in (("relative/dir", "a relative cwd"), (os.path.join(proj, "nope"), "a missing cwd")):
            r, _, _ = a.call("session/new", {"cwd": bad, "mcpServers": []})
            ok(((r or {}).get("error") or {}).get("code") == -32602, "session/new: %s is refused" % why)

        # -- a plain run: run, skip, a failing step ----------------------
        stub_log()
        r, up, asks = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "plain: tidy the files"}]}, answer=picks("run", "skip", "run"))
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn", "plain: stopReason end_turn", r)
        argv = (of(rows, "argv") or [[]])[0]
        ok(argv == ["do", "--porcelain", "--", "plain: tidy the files"], "plain: argv is do --porcelain -- <goal>", argv)
        ok(of(rows, "cwd") == [proj], "plain: spark runs in the session's cwd", of(rows, "cwd"))
        ok(of(rows, "stdin") == ["run", "skip", "run"], "plain: the answers reach stdin as run, skip, run",
           of(rows, "stdin"))
        calls = kinds(up, "tool_call")
        ok([c["title"] for c in calls] == ["ls", "rm -f b.txt", "false"]
           and all(c["kind"] == "execute" and c["status"] == "pending" for c in calls),
           "plain: each step is a pending execute tool call titled by its command")
        ok(calls and calls[0].get("rawInput") == {"command": "ls"}, "plain: rawInput holds the command")
        ok(len(asks) == 3 and all(q["params"]["options"] == [
            {"optionId": "run", "name": "Run", "kind": "allow_once"},
            {"optionId": "skip", "name": "Skip", "kind": "reject_once"}] for q in asks),
           "plain: one question per step, Run allow_once / Skip reject_once")
        ok([q["params"]["toolCall"]["toolCallId"] for q in asks] == [c["toolCallId"] for c in calls]
           and all(q["params"]["sessionId"] == sid for q in asks), "plain: each question names its tool call")
        ok(len(set(c["toolCallId"] for c in calls)) == 3, "plain: tool call ids are unique")
        ups = kinds(up, "tool_call_update")
        first = [u for u in ups if calls and u["toolCallId"] == calls[0]["toolCallId"]]
        texts = [b["content"]["text"] for b in (first[-1].get("content") or [])] if first else []
        ok(first and first[-1]["status"] == "completed" and "a.txt\nb.txt\n" in texts,
           "plain: output streams into the call, rc 0 completes it", texts)
        ok(any(u.get("status") == "in_progress" for u in first), "plain: a run step goes in_progress")
        second = [u for u in ups if len(calls) > 1 and u["toolCallId"] == calls[1]["toolCallId"]]
        ok(second and second[-1]["status"] == "failed", "plain: a skipped step ends failed")
        ok(len(calls) > 1 and "proof: test ! -e b.txt" in json.dumps(calls[1]["content"]),
           "plain: the proof is shown in the step")
        third = [u for u in ups if len(calls) > 2 and u["toolCallId"] == calls[2]["toolCallId"]]
        ok(third and third[-1]["status"] == "failed" and third[-1].get("rawOutput") == {"rc": 1},
           "plain: rc 1 fails the call, rawOutput {rc: 1}")
        ok("STUB-NOTE" in said(up) and "STUB-DONE" in said(up), "plain: a note and the done hint are message chunks")

        # -- a danger step outside the sandbox: never asked -------------
        r, up, asks = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "danger"}]}, answer=picks())
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn" and not asks and not of(rows, "stdin"),
           "danger: no question, nothing written to spark")
        ok("STUB-REFUSED" in said(up), "danger: spark's refusal reaches the client")
        ups = kinds(up, "tool_call_update")
        ok(ups and ups[-1]["status"] == "failed", "danger: the refused step ends failed")

        # -- the sandbox mode, a review accepted -------------------------
        r, _, _ = a.call("session/set_mode", {"sessionId": sid, "modeId": "sandbox"})
        ok((r or {}).get("result") == {}, "set_mode: sandbox")
        r, _, _ = a.call("session/set_mode", {"sessionId": sid, "modeId": "yolo"})
        ok(((r or {}).get("error") or {}).get("code") == -32602, "set_mode: an unknown mode is refused")
        r, up, asks = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "review: format it"}]}, answer=picks("accept"))
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn", "review: stopReason end_turn")
        argv = (of(rows, "argv") or [[]])[0]
        ok(argv == ["do", "--porcelain", "--sandbox", "--", "review: format it"], "review: argv carries --sandbox", argv)
        ok(of(rows, "stdin") == ["accept"], "review: steps are not asked; the review answers accept",
           of(rows, "stdin"))
        calls = kinds(up, "tool_call")
        ok(calls and calls[0]["kind"] == "execute" and calls[0]["status"] == "in_progress",
           "review: a sandboxed step starts in_progress")
        edit = [c for c in calls if c["kind"] == "edit"]
        dele = [c for c in calls if c["kind"] == "delete"]
        ok(len(edit) == 1 and len(dele) == 1, "review: one edit call and one delete call")
        diffs = [b for b in (edit[0]["content"] if edit else []) if b["type"] == "diff"]
        ok(diffs == [
            {"type": "diff", "path": os.path.join(proj, "new.txt"), "oldText": None, "newText": "hello\n"},
            {"type": "diff", "path": os.path.join(proj, "src", "a.py"), "oldText": "x=1\n", "newText": "x = 1\n"},
            {"type": "diff", "path": os.path.join(proj, "tool.sh"), "oldText": None, "newText": "#!/bin/sh\n"},
            {"type": "diff", "path": os.path.join(proj, ".git", "info", "exclude"), "oldText": "", "newText": "*.o\n"},
            {"type": "diff", "path": os.path.join(proj, "a^[[31m.txt"), "oldText": None, "newText": "x\n"}],
           "review: diffs with absolute paths, oldText null for a new file", diffs)
        text = "\n".join(b["content"]["text"] for b in (edit[0]["content"] if edit else []) if b["type"] == "content")
        ok(all(w in text.splitlines() for w in ("binary    logo.png", "mode      run.sh  644 -> 755  +x",
                                                "link      lnk -> src/a.py")),
           "review: binary, mode and link are text lines", text)
        ok(all(w in text.splitlines() for w in ("added     tool.sh  +x", "mode      suid  755 -> 755  (setuid masked)",
                                                "added     big.log  (too large to show)",
                                                "added     a^[[31m.txt  (control characters)")),
           "review: +x, spark's reason and control characters are shown", text)
        held = [c for c in calls if c["kind"] == "other"]
        htext = held[0]["content"][0]["content"]["text"] if held else ""
        ok(len(held) == 1 and held[0]["status"] == "failed" and htext.splitlines() == [
            "held back .git/hooks/pre-commit  (a git hook -- never applied)",
            "held back .git/config  (git config -- never applied)",
            "refused   escape  (a link out of the directory)"],
           "review: every held git item and the refused path, by name with the reason, in their own call", htext)
        at = [i for i, u in enumerate(up) if held and u["update"].get("toolCallId") == held[0]["toolCallId"]]
        ok(at and asks and at[0] < asks[0]["_at"], "review: the held call comes before the question")
        ok(dele and os.path.join(proj, "old.txt") in json.dumps(dele[0]["content"]),
           "review: the delete call lists the removed path")
        ok(len(asks) == 1 and asks[0]["params"]["options"] == [
            {"optionId": "accept", "name": "Accept", "kind": "allow_once"},
            {"optionId": "discard", "name": "Discard", "kind": "reject_once"}]
           and asks[0]["params"]["toolCall"]["toolCallId"] == (edit[0]["toolCallId"] if edit else None),
           "review: one question on the edit call, Accept / Discard")
        done = {u["toolCallId"]: u["status"] for u in kinds(up, "tool_call_update")}
        ok(edit and dele and done.get(edit[0]["toolCallId"]) == "completed"
           and done.get(dele[0]["toolCallId"]) == "completed", "review: accepted and done -> completed")
        ok("STUB-APPLIED" in said(up), "review: spark's apply note reaches the client")

        # -- a review discarded (the mode as a config option) ------------
        a.call("session/set_mode", {"sessionId": sid, "modeId": "ask"})
        r, _, _ = a.call("session/set_config_option", {"sessionId": sid, "configId": "mode", "value": "sandbox"})
        opts = ((r or {}).get("result") or {}).get("configOptions") or [{}]
        ok(opts[0].get("currentValue") == "sandbox", "set_config_option: mode sandbox, the full list back")
        r, up, asks = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "review"}]}, answer=picks("discard"))
        rows = stub_log()
        ok(of(rows, "stdin") == ["discard"], "discard: the review answers discard", of(rows, "stdin"))
        done = {u["toolCallId"]: u["status"] for u in kinds(up, "tool_call_update")}
        rv = [c["toolCallId"] for c in kinds(up, "tool_call") if c["kind"] in ("edit", "delete")]
        ok(rv and all(done.get(c) == "failed" for c in rv), "discard: the review calls end failed")
        ok("STUB-DISCARDED" in said(up), "discard: spark's hint reaches the client")
        a.call("session/set_mode", {"sessionId": sid, "modeId": "ask"})

        # -- cancel while a question waits ------------------------------
        def cancel_then_answer(q):
            a.write({"method": "session/cancel", "params": {"sessionId": sid}})
            # the client answers its open question `cancelled`, as the spec says
            a.write({"id": q["id"], "result": {"outcome": {"outcome": "cancelled"}}})
        r, up, asks = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "hang"}]}, on_ask=cancel_then_answer)
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "cancelled", "cancel: stopReason cancelled", r)
        ok(of(rows, "stdin") == ["quit"], "cancel: spark reads quit", of(rows, "stdin"))

        # the cancel alone (a client that never answers the question)
        r, up, asks_alone = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "hang"}]},
                                   on_ask=lambda q: a.write({"method": "session/cancel", "params": {"sessionId": sid}}))
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "cancelled" and of(rows, "stdin") == ["quit"],
           "cancel: an unanswered question is taken as cancelled")

        # -- cancel while spark thinks (reads nothing): stopped ----------
        a.next += 1
        rid = a.next
        a.write({"id": rid, "method": "session/prompt", "params": {"sessionId": sid, "prompt": [
            {"type": "text", "text": "think"}]}})
        time.sleep(1)
        r2, _, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "plain"}]})
        ok(((r2 or {}).get("error") or {}).get("code") == -32600, "one prompt at a time: a second is refused")
        t0 = time.time()
        a.write({"method": "session/cancel", "params": {"sessionId": sid}})
        r, _, _ = a.until(rid)
        rows = stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "cancelled", "cancel: a thinking spark ends cancelled")
        pid = (of(rows, "pid") or [0])[0]
        alive = True
        if pid:
            try:
                os.kill(pid, 0)
            except OSError:
                alive = False
        ok(pid and not alive and time.time() - t0 < WAIT, "cancel: that spark is stopped (%.1f s)" % (time.time() - t0))

        # -- the cap, a flag-like goal, a crash --------------------------
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "cap"}]},
                          answer=picks("run"))
        stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "max_turn_requests" and "STUB-CAP" in said(up),
           "cap: stopReason max_turn_requests, the hint said")
        a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "-h plain"}]},
               answer=picks("skip", "skip", "skip"))
        argv = (of(stub_log(), "argv") or [[]])[0]
        ok(argv[-2:] == ["--", "-h plain"], "a goal that looks like a flag stays the goal, after --", argv)
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [
            {"type": "text", "text": "crash"}, {"type": "resource_link", "name": "a", "uri": "file:///srv/x.txt"}]})
        argv = (of(stub_log(), "argv") or [[]])[0]
        ok(argv[-1:] == ["crash\n/srv/x.txt"], "a linked file adds its path to the goal", argv)
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn" and "STUB-CRASH" in said(up),
           "crash: spark's last stderr line reaches the client")
        r, _, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "image", "data": "", "mimeType": "x"}]})
        ok(((r or {}).get("error") or {}).get("code") == -32602, "a prompt with no text is refused")

        # -- stopped, a refusal before the run ----------------------------
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "stopped"}]},
                          answer=picks("skip"))
        stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn" and "STUB-STOPPED" in said(up),
           "stopped: stopReason end_turn, spark's hint said", r)
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "refuseall"}]})
        stub_log()
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn" and "STUB-REFUSED-RUN" in said(up)
           and not kinds(up, "tool_call"), "refused before the run: end_turn, the hint said, no tool call")

        # -- a line over 8 MB: spark stopped, the prompt ends --------------
        t0 = time.time()
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "longline"}]})
        pid = (of(stub_log(), "pid") or [0])[0]
        ok(((r or {}).get("result") or {}).get("stopReason") == "end_turn" and "over 8 MB" in said(up)
           and gone(pid) and time.time() - t0 < WAIT, "a line over 8 MB: spark stopped, the prompt ends saying so",
           said(up)[-200:])

        # -- a review past 2 MB of diff: the rest are lines ----------------
        a.call("session/set_mode", {"sessionId": sid, "modeId": "sandbox"})
        r, up, _ = a.call("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "bigdiff"}]},
                          answer=picks("discard"))
        stub_log()
        a.call("session/set_mode", {"sessionId": sid, "modeId": "ask"})
        edit = [c for c in kinds(up, "tool_call") if c["kind"] == "edit"]
        cont = edit[0]["content"] if edit else []
        shown = [os.path.basename(b["path"]) for b in cont if b["type"] == "diff"]
        size = sum(len(b.get("oldText") or "") + len(b["newText"]) for b in cont if b["type"] == "diff")
        ok(shown == ["one.txt", "three.txt"] and size <= 2 * 1024 * 1024
           and "changed   two.txt  (too large to show)" in json.dumps(cont),
           "a review past 2 MB of diff: the rest are lines, too large to show", shown)

        # -- a late answer finds its question gone ---------------------------
        late = [q["id"] for q in asks_alone]
        a.write({"id": late[0] if late else "none", "result": {"outcome": {"outcome": "cancelled"}}})

        # -- malformed messages: answered, never the end of the agent -------
        r, _, _ = a.call("session/new", {"cwd": proj, "mcpServers": 5})
        sid2 = ((r or {}).get("result") or {}).get("sessionId")
        ok(sid2, "session/new: an mcpServers that is not a list is ignored")
        a.next += 1
        think = a.next
        a.write({"id": think, "method": "session/prompt", "params": {"sessionId": sid2, "prompt": [
            {"type": "text", "text": "think"}]}})
        time.sleep(1)
        codes = []
        for raw, want in ((b"\xff\xfe\n", None), (b"[" * 100000 + b"\n", None), (b"[1, 2, 3]\n", None),
                          (b'{"jsonrpc": "2.0", "id": [1], "method": "initialize", "params": {}}\n', None),
                          (b'{"jsonrpc": "2.0", "id": 1.5, "method": "initialize", "params": {}}\n', None),
                          (b'{"jsonrpc": "2.0", "id": "m1", "method": ["x"]}\n', "m1"),
                          (b'{"jsonrpc": "2.0", "id": "m2", "method": "session/set_mode", '
                           b'"params": {"sessionId": ["x"]}}\n', "m2"),
                          (b'{"jsonrpc": "2.0", "id": "m3", "method": "session/prompt", "params": [1, 2]}\n', "m3"),
                          (b'{"jsonrpc": "2.0", "id": "m4", "method": "session/cancel", '
                           b'"params": {"sessionId": {"a": 1}}}\n', "m4")):
            a.p.stdin.write(raw)
            a.p.stdin.flush()
            r, _, _ = a.until(want)
            codes.append(((r or {}).get("error") or {}).get("code"))
        # no answer: a response to no question, a notification with a bad session
        a.write({"id": {"a": 1}, "result": {}})
        a.write({"method": "session/cancel", "params": {"sessionId": [1]}})
        ok(codes == [-32700, -32700, -32600, -32602, -32602, -32600, -32602, -32602, -32601],
           "malformed messages: parse error, bad request, bad id, bad sessionId -- each answered", codes)
        r, _, _ = a.call("initialize", {"protocolVersion": 1})
        ok(((r or {}).get("result") or {}).get("protocolVersion") == 1, "malformed messages: the agent still answers")
        pid = (of(stub_log(), "pid") or [0])[0]

        # -- the protocol's edges ----------------------------------------
        r, _, _ = a.call("session/load", {"sessionId": sid, "cwd": proj, "mcpServers": []})
        ok(((r or {}).get("error") or {}).get("code") == -32601, "session/load: method not found")
        a.p.stdin.write(b"{not json\n")
        a.p.stdin.flush()
        r, _, _ = a.until(None)
        ok(((r or {}).get("error") or {}).get("code") == -32700, "a line that is not JSON: parse error")
        a.close()
        ok(pid and gone(pid), "the client went away: its running spark is stopped, never orphaned")
        with open(os.path.join(tmp, "stderr")) as f:
            ok("an answer to no question (id %r)" % (late[0] if late else None) in f.read(),
               "a cancelled question leaves the list: its late answer finds nothing")
        ok(not a.bad, "stdout carried nothing but JSON-RPC", a.bad[:3])
        time.sleep(0.5)
        ok(not os.path.exists(marker), "the offered MCP server was never started")

        # -- a spark without --porcelain, and no spark at all ------------
        for extra, what, word in (({"STUB_OLD": "1"}, "a spark without --porcelain", "1.47"),
                                  ({"SPARK_BIN": os.path.join(tmp, "none")}, "no spark", "spark.forgewright.ai")):
            b = Agent(env(**extra))
            r, _, _ = b.call("initialize", {"protocolVersion": 1})
            msg = ((r or {}).get("error") or {}).get("message", "")
            ok(word in msg, "%s: initialize fails, saying so" % what, msg)
            b.close()

        # -- a cancel before spark starts: spark never starts ----------------
        c = Agent(env(STUB_SLOW_H="2"))    # the probe (do -h) takes 2 s
        r, _, _ = c.call("session/new", {"cwd": proj, "mcpServers": []})
        csid = ((r or {}).get("result") or {}).get("sessionId")
        stub_log()
        c.write({"id": 1, "method": "session/prompt", "params": {"sessionId": csid, "prompt": [
            {"type": "text", "text": "plain"}]}})
        c.write({"method": "session/cancel", "params": {"sessionId": csid}})
        r, _, asks = c.until(1)
        time.sleep(0.5)
        ok(((r or {}).get("result") or {}).get("stopReason") == "cancelled" and not asks
           and not of(stub_log(), "argv"), "a cancel before spark starts: cancelled, spark never runs", r)
        c.close()

        # -- sessions are bounded; SIGTERM stops the running spark ----------
        k = Agent(env())
        r, _, _ = k.call("session/new", {"cwd": proj, "mcpServers": []})
        busy = ((r or {}).get("result") or {}).get("sessionId")
        k.write({"id": "busy", "method": "session/prompt", "params": {"sessionId": busy, "prompt": [
            {"type": "text", "text": "think"}]}})
        time.sleep(1)
        more = []
        for _ in range(64):
            r, _, _ = k.call("session/new", {"cwd": proj, "mcpServers": []})
            more.append(((r or {}).get("result") or {}).get("sessionId"))
        state = []
        for s in (busy, more[0], more[1], more[-1]):
            r, _, _ = k.call("session/set_mode", {"sessionId": s, "modeId": "ask"})
            state.append("error" not in (r or {"error": 1}))
        ok(all(more) and state == [True, False, True, True],
           "sessions: past 64 the oldest idle one is dropped, a busy one kept", state)
        pid = (of(stub_log(), "pid") or [0])[0]
        k.p.terminate()
        k.p.wait(10)
        ok(pid and gone(pid), "SIGTERM on spark-acp: its running spark is stopped, never orphaned")
        errf.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("%d failed" % len(fails) if fails else "all ok")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
