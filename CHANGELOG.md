# Changelog

## 1.0.0

- spark do in an ACP client (the Agent Client Protocol, version 1):
  Toad, Zed or any other client drives `spark do --porcelain`. Each
  proposed command is a tool call and one question, Run or Skip; its
  output streams into the call, its exit code completes or fails it.
- Two session modes: `ask` (the default, every command asked) and
  `sandbox` (`spark do --sandbox`: the commands run in a sandboxed
  copy; the review shows the diffs, a delete list and the held-back
  files, and one question, Accept or Discard). The same choice is a
  session config option, for clients that read those.
- A command that can destroy data is refused by spark outside the
  sandbox, and the client is told; there is no way to confirm one
  over a pipe.
- Cancel stops the run: spark hears `quit`, and a spark that is still
  thinking is stopped after five seconds.
- An MCP server the client offers is never started. stdout carries
  nothing but ACP messages. One prompt at a time per session.
- Needs spark 1.47 or newer; an older spark, or none, is one clear
  error at initialize.
- `tests/acp_pty.py` drives it over pipes, as a client does, against a
  stub spark; CI runs it on Ubuntu, Arch and macOS.
