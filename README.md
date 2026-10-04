# Arbor.RPC

Shared JSON-RPC, framing and environment mechanics for Arbor protocols.

Version 2.0.0-dev is an unpublished implementation snapshot.

`Arbor.RPC.Subprocess.capture/2` runs a finite utility command and returns
`{:ok, original_output_bytes, exit_status}`. The capturing caller owns its child.
Defaults are one 5-second read deadline and 1 MiB of total output, including CR,
LF, empty lines and unfinished EOF bytes. Nonzero status is returned for the
caller to interpret. Byte overflow reports `:output_too_large`; frame-count and
mailbox pressure remain explicit errors. Truncated output is never successful.
Known cleanup failure includes the original result reason as
`{:cleanup_failed, reason, cleanup_reason}`. Cleanup has a separate finite budget
after read expiry; the actor-call allowance can add up to 1 second. This API uses
the same PATH, environment, working-directory and optional group policy as
`open/2`, and retains the raw Port-driver and platform limitations below.
If close is unconfirmed, capture force-stops its exclusive actor and its guardian
attempts bounded OS cleanup. The original cleanup error is retained; no success
is inferred from fallback cleanup.

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

A close-call timeout keeps the explicit error and force-stops only an actor
whose local process identity and private generation marker match the handle.
This also prevents an abandoned actor from staying attached to a surviving
client across reconnects. The guardian then attempts bounded cleanup. After
hard actor death, that guardian has captured PID/group proof but no retained
Port or actual `exit_status`; Actor DOWN alone does not confirm OS cleanup.
Delayed PID reuse and guardian cleanup confirmation on this path remain release
qualification gates. The live-actor retained-Port regression does not establish
those properties for hard actor death.
