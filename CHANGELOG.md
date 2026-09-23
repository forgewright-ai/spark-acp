# Changelog

## 1.0.0

- spark do in an ACP client (the Agent Client Protocol, version 1):
  Toad, Zed or any other client drives `spark do --porcelain`. Each
  proposed command is a tool call and one question, Run or Skip; its
  output streams into the call, its exit code completes or fails it.
  The goal goes after `--`: whatever its words, spark reads them as the
  goal.
- Two session modes: `ask` (the default, every command asked) and
  `sandbox` (`spark do --sandbox`: the commands run in a sandboxed
  copy; the review shows the diffs and a delete list, each file marked
  with what spark knows -- `+x`, its reason, control characters -- and,
  apart and before the question, every held-back or refused path, each
  git item by name, with its reason; then one question, Accept or
  Discard). The same choice is a session config option, for clients
  that read those.
- Outside the sandbox, a command that can destroy data (or whose
  effect cannot be read from its line) is refused by spark, and the
  client is told; there is no way to confirm one over a pipe. Inside
  the sandbox it runs in the copy and the diff is the gate: Accept
  applies it (a client that approves on its own accepts without you).
- spark's end is the turn's: done, refused, stopped and error end the
  turn with spark's hint, cap is max_turn_requests, quit is cancelled.
- Cancel stops the run: spark hears `quit`, and a spark that is still
  thinking is stopped after five seconds. A cancel before spark starts
  means it never starts.
- A malformed message is answered with a JSON-RPC error, never the end
  of the agent; spark-acp ending (stdin closed, SIGTERM, a bug) stops
  its running spark. Every read is bounded: a line from spark over 8 MB
  stops the run, a review carries at most 2 MB of diff text (the rest
  is a line, too large to show), at most 64 sessions are kept (the
  oldest idle one goes).
- An MCP server the client offers is never started. stdout carries
  nothing but ACP messages. One prompt at a time per session.
- Needs spark 1.47 or newer; an older spark, or none, is one clear
  error at initialize.
- `tests/acp_pty.py` drives it over pipes, as a client does, against a
  stub spark; CI runs it on Ubuntu, Arch and macOS, with the actions
  pinned to a commit, a read-only token, and privacy gates that name a
  file, never a line. A commit-msg hook reads the message with the same
  patterns.
