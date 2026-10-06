# ArborRPC

ArborRPC provides shared JSON-RPC, framing and environment mechanics for ArborMCP
and ArborACP. Its Hex package and OTP application are `arbor_rpc`, and its module
namespace is `Arbor.RPC.*`.

Version `2.0.0-rc.1` is an unpublished release candidate with a source-built native
subprocess backend and unchanged bounded write defaults. The planned prerelease
is for downstream migration testing; see the
[v1 to v2 migration guide](https://github.com/trust-arbor/arbor_mcp/blob/codex/v2-migration/docs/guides/MIGRATING_V1_TO_V2.md).
Publication is pending, and the final 48-hour stable-release gate has not passed.

## Native lifecycle contract

`Arbor.RPC.Subprocess` returns an opaque handle with a fresh generation. Its
Actor frames output; its Guardian owns the helper Port from launch. The native
helper is the sole parent of the vendor executable. It preserves literal argv,
child environment/cwd, stdin and stdout bytes. Vendor PID/status are distinct
from helper PID/status; public PID diagnostics describe the vendor child.

The native parent observes exit without reaping (`waitid` with `WNOWAIT`). It
retains the owned child's identity until all TERM/KILL paths are permanently
disabled, then reaps. Group mode verifies that child as its private group leader.
After reap it only polls `kill(-original_pgid, 0)` within the remaining budget:
ESRCH establishes targeted-group absence at that observation; occupied, reused,
permission-denied or expired observations remain unconfirmed. No signal occurs
post-reap and no command accepts a caller-supplied PID/PGID. Descendants that
change group/session are outside targeted-group cleanup; arbitrary process-tree
containment is not claimed.

`close/1` remains finite and reports known failure. Actor death alone cannot
confirm cleanup. `cleanup_receipt/1` returns a typed receipt distinguishing direct
child reaping, targeted-group absence, vendor/helper status and failure reason.
The Guardian retains it for `:receipt_retention` (default 5 seconds), then stops.
Repeated close uses that receipt; after expiry/loss it returns
`:cleanup_status_unavailable`, rather than inventing success. A close-call timeout
retains its error and force-stops only the matching private Actor generation;
the Guardian continues owned native cleanup. Guardian/helper death or receipt
loss is explicitly unconfirmed. The retired private `Subprocess.Cleanup`
`proof/3`, `run/3` and `after_exit/3` return `:unsupported_unmanaged_cleanup` and
never signal a supplied numeric identity or close a supplied Port.

## Delivery and pressure

`FramedStream.next/2` pulls newline frames; `next_until/3` preserves one original
monotonic deadline across protocol filtering. Explicit zero polls use
`buffered_only: true` and their original cutoff. Push subscribers use a bounded
window and ACK after processing:

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

Queued and unacknowledged frames share count/byte limits. A native data credit
permits one chunk of at most 16 KiB before Actor framing admission. Cleanup and
terminal/control packets have reserved finite capacity and progress without a
returned data credit. Output remains collectible after observed vendor exit;
EOF/final bytes, cleanup receipt and helper exit are separate events. Oversized
frames, input chunks and queues retain their explicit errors and accepted prefix
frames. An abandoned drain expires after `:closed_retention` (5 seconds).

Supported writes reserve aggregate count/byte credit before owner retention or
mailbox entry. Defaults are `max_pending_writes: 64` and
`max_pending_write_bytes: 4_194_304`, including reservation metadata, alongside
`max_write_bytes: 1_048_576` per write. Valid iodata is detached after the logical
size check; small subbinaries cannot retain large backing binaries in the
ledger. A coalesced payload-free wake and one maintenance timer replace
per-caller payload messages. Busy capacity or finite CAS contention returns
`{:error, :backpressure}`. The Actor's immutable configuration also applies to
`Subprocess.call(handle, {:write, data}, timeout)`; forged facade limits cannot
raise it. Arbitrary raw same-VM sends/private Actor calls remain outside this
supported mailbox boundary.

Normal writes use one original `cleanup_timeout + 1000` ms call cutoff through
validation, admission, queueing and the final native command check. Queued work
cannot execute after deadline or producer death. A caller suspended through a
queued success gets timeout and no late alias replies. Hidden `call/3` retains
its explicit `:infinity` host opt-in, outside the finite managed-call guarantee;
aggregate credit still bounds retention, and the nested native admission
attempt remains finite.

The private write handoff reloads the published binary after claiming ownership
and verifies the same phase, producer and original deadline. This prevents a
reserved entry's earlier empty payload snapshot from reaching the native writer
without changing public write limits or completion accounting.

Timeout or caller death releases only work proven not to have started. Once a
write starts, uncertainty remains charged until an actual native ACK or proven
pre-command rejection. An authoritative later ACK can release a timed-out
caller's credit; it cannot make that caller's timeout become success. Actor
DOWN destroys only its BEAM ledger. The existing retained Guardian receipt is
required to certify owned child/group cleanup. Native `:ok` confirms bounded
helper stdin admission, not vendor consumption: admitted bytes can reach stdin
later. The separate single native command packet and helper stdin queue are
bounded by the per-write cap. Kernel pipes, OTP Port-driver allocation,
caller-owned input/mailboxes and application-retained output are outside these
counters. Iodata traversal/materialization and whole-row ETS CAS work are
cooperative, not a hard per-call CPU/RSS guarantee. Broader platform, blocked
write/reaping and total-memory measurement remain release gates.

## Finite utilities

`Subprocess.capture/2` returns `{:ok, original_output_bytes, vendor_exit_status}`.
The capturing caller owns the child. Defaults are one 5-second read deadline and
1 MiB of total output, including CR, LF, blank lines and unfinished EOF bytes.
Nonzero vendor status is returned for caller interpretation. Byte overflow is
`:output_too_large`; frame-count/mailbox pressure remains explicit. Truncated
output is never successful. Known cleanup failure includes the original capture
reason. Cleanup has a separate finite budget after read expiry, plus finite Actor
scheduling allowance. PATH, isolation, environment overrides, cwd, stderr and
group policy match `open/2`.

## Source build and remaining gates

The custom Mix compiler builds reviewed `c_src/subprocess_helper.c` with a C17
compiler on macOS/Darwin and Linux. It runs during RPC source installation,
including transitive MCP/ACP installation for HTTP-only, BEAM-only or other use
that never opens a subprocess. `CC` selects an executable; compiler flags and argv are
fixed, without shell command strings. The generated `priv/native` executable is
ignored and excluded from Hex source archives; no prebuilt helper is promised.
A source package install builds it for the target. Assembled releases must
include that built helper; release/runtime lookup uses
`:code.priv_dir(:arbor_rpc)`. An installed runtime never invokes a compiler.
Framing/JSON-RPC do not start a helper. Missing helper and unsupported platform
errors remain explicit; there is no numeric PID fallback or NIF.

Windows native subprocess operations are explicitly unsupported; the compiler
skips the helper on unsupported platforms and framing remains available. A
Windows backend is not required for the initial v2 release. This source-install
policy does not certify every Darwin/Linux architecture: advertise only the
matrix qualified from final source archives and installed releases. Broader
platform/pressure measurement, helper hard death and uninterruptible child exit
remain qualification gates. Escaped descendants remain outside the native
ownership contract. The local candidate is not a published release.

## Standalone documentation

From the workspace root, run:

```sh
cd packages/arbor_rpc
ARBOR_V2_LOCAL=1 MIX_ENV=dev mix deps.get
ARBOR_V2_LOCAL=1 MIX_ENV=dev mix docs --warnings-as-errors
```

ExDoc is a dev-only dependency and does not run in consumer applications. Source
links use `arbor_rpc-v<version>` and the `packages/arbor_rpc/` source prefix.
Version tags are created only for a reviewed release; this unpublished prerelease
snapshot does not imply that those prospective tags already exist.
