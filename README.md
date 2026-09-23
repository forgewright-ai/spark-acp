# spark-acp -- spark do, in an ACP client

spark (https://spark.forgewright.ai) is your own AI on your own machine.
`spark do` works a task one command at a time. spark-acp puts it in any
client of the Agent Client Protocol -- Toad in a terminal, Zed in the
editor: you write the goal, spark proposes each command, and the client
asks you.

    Run / Skip          every command, before it runs
    Accept / Discard    a sandboxed run's changes, once, at the end
    Cancel              stops the run

Two modes, picked in the client:

    ask        the default: you allow or skip every command
    sandbox    the commands run in a sandboxed copy of the directory --
               no network, only the copy writable -- and you accept or
               discard the diffs once, at the end

Outside the sandbox, a command that can destroy data -- or whose
effect cannot be read from its line -- is refused: spark says so, and
it runs in a terminal (`spark do`) only. Inside the sandbox it runs in
the copy, and the diff is the gate: nothing reaches the directory until
Accept, and Accept applies the changes. A client set to approve on its
own answers Run and Accept without you.

## Install

You need spark 1.47 or newer (`spark do -h` names `--porcelain`) and
python 3.9 or newer.

```sh
git clone https://github.com/forgewright-ai/spark-acp ~/.local/share/spark-acp
mkdir -p ~/.local/bin
ln -s ~/.local/share/spark-acp/spark-acp ~/.local/bin/spark-acp
```

An update is `git -C ~/.local/share/spark-acp pull`.

## Run it

Toad (https://github.com/batrachianai/toad), from the directory you
want to work in:

```sh
toad acp "spark-acp"
```

Zed: add spark to `agent_servers` in its `settings.json`, then pick it
in the agent panel:

```json
{
  "agent_servers": {
    "spark": {
      "type": "custom",
      "command": "spark-acp",
      "args": [],
      "env": {}
    }
  }
}
```

When Zed does not find `spark-acp` on its PATH, give `command` the full
path (`command -v spark-acp` prints it).

What to ask, and what each answer does: `CHEATSHEET.md`.

## What leaves this machine

spark-acp sends nothing itself: it starts `spark do --porcelain` in the
session's directory with your goal as its words, and hands your answers
to spark. What reaches the AI is spark's to say (its README): the goal,
the directory's path, each command's output with its secret-shaped spans
held back. An MCP server a client offers is never started -- spark's
tools are the programs on this machine.

## Contributing

`git config core.hooksPath .githooks` once; the hooks keep the tree and
the commit messages free of private names, the docs ASCII, the code
syntax-clean. `python3 tests/acp_pty.py`
drives spark-acp over pipes, as a client does, against a stub spark.
Another app joins spark the same way this one does, in its own repo.

MIT. Credits in `CREDITS.md`. Built with Claude.
