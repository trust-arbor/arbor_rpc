# Arbor.RPC

Shared JSON-RPC, framing and environment mechanics for Arbor protocols.

Version 2.0.0-dev is an unpublished implementation snapshot.

`Arbor.RPC.Subprocess` opens a child with a stable Port owner, isolated environment,
child PATH resolution, and finite TERM/KILL cleanup. The opening process owns the
child's lifetime by default. A temporary connector can specify a live local
`:owner` PID to give that lifetime to its client. A separate guardian handles
abrupt actor death. Unix group
cleanup requires proof that the owned child PID equals its process group ID;
arbitrary caller PIDs and host groups are never accepted as targets.

`Arbor.RPC.FramedStream.next/2` drains buffered newline frames before waiting.
Protocol filters use `next_until/3` with one original monotonic deadline across
retries. Explicit zero polls pass `buffered_only: true` with their original
cutoff, so skipping a banner cannot accept a later-arriving frame.
Alternatively, subscribe with a bounded window and acknowledge each frame after
processing it:

```elixir
{:ok, child} = Arbor.RPC.Subprocess.open(["/path/to/agent"], process_group: true)
:ok = Arbor.RPC.FramedStream.subscribe(child, self(), window: 1)
generation = Arbor.RPC.Subprocess.identity(child)

receive do
  {:arbor_rpc, ^generation, {:frame, token, bytes}} ->
    consume_frame(bytes)
    :ok = Arbor.RPC.FramedStream.ack(child, token)

  {:arbor_rpc, ^generation, {:closed, reason, unfinished_bytes}} ->
    handle_protocol_close(reason, unfinished_bytes)
end
```

The shared layer preserves bytes and leaves JSON validation, banner/blank-line
policy, BOM handling, protocol errors and logging to the consumer. Queued and
unacknowledged frames share byte/count limits; an oversized frame, input chunk,
queue or driver-message high-water closes with an explicit reason. Accepted
frames before a failing chunk's rejection remain available. Writes use Port
`:nosuspend` and return `{:error, :backpressure}` when the driver is busy.
Invalid or oversized write payloads are rejected in the facade before they
enter the actor mailbox. Callers must also bound their concurrent write count;
per-write validation is not a hard aggregate mailbox limit. Handles, readers
and subscribers are local to the actor's node; remote consumers are rejected.

OTP Ports do not expose input read credit. Driver messages can transiently exceed
the mailbox high-water while the actor is unscheduled; the limit is checked when
the actor next handles input. Queue counters are therefore a bound on managed
frames, not a strict total-memory or raw-mailbox cap. A real suspended-actor test
characterizes that limitation. Pressure qualification remains a release gate.
Acknowledging after forwarding to another unbounded mailbox invalidates the
consumer's delivery bound. A final incomplete frame is reported with closure;
the protocol decides whether it can be decoded as a final message.

Explicit close stops the actor and remains safe to repeat. Natural exit or
pressure failure allows accepted frames to drain, then stops after terminal
delivery. A finite `:closed_retention` (default 5 seconds) ends an abandoned
drain with an explicit `:drain_timeout`, so a long-lived owner cannot accumulate
closed actors across reconnects. Read deadlines start in the caller, including
time a request spends waiting in the actor mailbox. Cleanup requires a separate
50 ms KILL allowance beyond TERM grace and reports failure when its budget is
exhausted. The direct executable is opened with `:eof`, retaining its PID
metadata even after a very fast exit without a launcher or command replay.
EOF alone does not prove death: a live child may close stdout. Only actual
`exit_status` prevents signalling a retained PID that could have been reused.
Group mode still requires measuring the owned child as its group leader; if
that leader exits before measurement, startup returns
`:child_not_process_group_leader`. Supporting such fast group startup remains
a qualification gate, and an unverified group is never signalled.
Natural group cleanup runs when the Port reports the child's exit; descendants
retaining its output pipe can delay that report. Windows cleanup and Unix/macOS/Linux pressure and
lifecycle matrix qualification remain release gates. ACP bridges, Pi managed
sessions and native ACP child stdio adopt this API; each consuming package
still requires its own release qualification.
