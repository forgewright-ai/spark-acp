# spark-acp -- the cheatsheet

spark-acp puts `spark do` in an ACP client. Section 1 is the client on
its own, section 2 is spark working a task in it.

## 1. the client, the basics

Toad, in a terminal

    cd ~/project           the directory spark works in
    toad acp "spark-acp"   start it
    Enter                  send the goal
    Ctrl-P                 the command palette: pick Ask or Sandbox
                           (or click the mode's name by the prompt)
    Ctrl-Q                 leave Toad (Ctrl-C twice, too)

Zed: `agent_servers` in settings.json (the README has the lines), then
the agent panel, then spark.

## 2. a task, with spark

Write the goal in plain words. spark proposes one command at a time;
each shows as a step with its command, why, and its output.

    Run        run this command
    Skip       not this one: spark hears it was skipped
    Cancel     stop the run (the client's stop)

The mode, in the client's mode picker (Toad: Ctrl-P):

    ask        every command asked (the default)
    sandbox    the commands run in a copy of the directory: no network,
               nothing else writable. At the end, one review:
    Accept     apply the diffs to the directory
    Discard    throw the copy away

A step marked "can destroy data" (or one whose effect cannot be read
from its line) is refused outside the sandbox: run `spark do` in a
terminal for that one. In the sandbox it runs in the copy, the diff is
the gate, and Accept applies it. A client set to approve on its own
answers Run and Accept without you.

The review marks what a diff cannot show

    +x                     an executable bit added
    (setuid masked)        spark's reason, whatever it is
    (control characters)   the name or the text has them (shown visibly)
    held back, refused     listed apart, before the question, by name
                           with the reason: never applied

By example

    which files here are over 10 MB
    why does make test fail
    rename the .jpeg files to .jpg          (sandbox: see the diff first)
    add a .gitignore for python             (sandbox)

A step's proof -- one read-only check -- shows under its command. At
most 8 steps a run; spark says when it stopped short.
