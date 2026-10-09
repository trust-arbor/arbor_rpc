# Troubleshooting ArborRPC

Retain the complete tagged error and the selected source/package version.
Subprocess exit status, frame delivery, helper status and cleanup confirmation
are separate observations. The [quickstart](QUICKSTART.md) shows where to read
each result; the [lifetime contract](../README.md#native-lifecycle-contract)
explains their limits.

## Installation and executable lookup

| Symptom | Check |
|---|---|
| Hex cannot resolve `1.0.0-rc.1` | Use the explicit `1.0.0-rc.1` prerelease requirement; a stable-only constraint does not select it. Check the lockfile and Hex connectivity. The original 2.0.0-rc.1 is retired. |
| Build says a C compiler is required | Install a compiler supporting C17 on macOS/Darwin or Linux. `CC` names one executable, such as `cc` or `/path/to/clang`; it is not a shell command or a flags list. |
| Native helper build fails | Keep the compiler output and target OS/architecture. The fixed compiler flags include `-std=c17` and warnings-as-errors. Build source for the deployment target. |
| `:native_helper_unavailable` | Verify the application was built normally and its installed `priv/native/arbor_rpc_subprocess` was included in the release. A source tar deliberately contains no prebuilt helper. |
| `{:unsupported_subprocess_backend, platform}` | Native subprocess operations are supported only on the qualified macOS/Darwin and Linux targets. Windows has no native backend. |
| `{:executable_not_found, name}` | Check the child's effective PATH, working directory and executable permission. An explicit child PATH overrides the VM PATH; an explicitly unset PATH has no host fallback. |
| `{:invalid_cd, value}` or `:invalid_command` | `cd` must name an existing directory. Pass a nonempty list of binary argv elements without NUL bytes, beginning with the executable. |

`environment_policy: :isolated` is the default. It retains a small OS/locale/PATH
allowlist and removes other inherited variables; vendor credentials need an
explicit `env` override. Replace these placeholder paths with your executable
and workspace, and supply only what the child needs:

```elixir
Arbor.RPC.Subprocess.open(["/absolute/path/to/agent"],
  cd: "/absolute/path/to/workspace",
  env: [{"MY_AGENT_SETTING", "value"}],
  process_group: true
)
```

`environment_policy: :inherit` explicitly opts into the inherited environment.
Under either policy, inherited PATH entries below an OTP `RELEASE_ROOT` are
removed so a child does not accidentally use the host release's Erlang boot
files. An explicit `env` PATH is used as supplied. Inspect the intended child
environment rather than assuming the interactive shell and installed release
have identical lookup behavior. Avoid logging secrets or whole environments.

## Writes, reads and pressure

| Result | Meaning and response |
|---|---|
| `:invalid_owner` | The lifetime owner must be a live local PID. Capture a persistent owner before opening from a temporary Task. |
| `:invalid_iodata` / `:write_too_large` | Use valid iodata within `max_write_bytes`; invalid input is rejected before native submission. |
| `:backpressure` | Write count/byte capacity, finite admission contention or the native queue prevented admission. Keep the producer queue bounded; do not turn this into an unbounded retry loop. |
| `:timeout` | The original write/read/capture cutoff elapsed. A write timeout does not establish that a started physical write was cancelled. Retain the outcome and follow your protocol's recovery policy. |
| `{:closed, reason, remainder}` | The output stream terminated after its admitted frames. Decide whether the unfinished bytes form a valid final protocol message. |
| `:frame_too_large`, `:queue_frame_limit`, `:queue_byte_limit`, `:input_chunk_too_large`, `:mailbox_pressure` | A configured output bound was reached. Consume promptly and acknowledge after processing; investigate the producer and configured workload before changing limits. |
| `:output_too_large` from capture | Total captured bytes exceeded its cap. Capture reports failure instead of a successful truncated result. |
| `:subscribed` / `:readers_waiting` | Pull readers and a push subscriber cannot share the same child simultaneously. Choose one delivery mode. |
| `:not_consumer` / `:invalid_ack` | Only the registered subscriber may acknowledge a current token, once. Match the child generation and retain the token until processing finishes. |

`Subprocess.write/2` returning `:ok` confirms bounded helper stdin admission,
not that the child consumed the bytes or completed an application request.
Expect and validate a protocol response separately. Default pending-write
credit is 64 entries and 4 MiB including reservation metadata; each write is
limited to 1 MiB. A started or uncertain write stays charged until a conclusive
native ACK or proven rejection. These counters do not include kernel buffers,
caller inputs or output your application retains after ACK.

## Unexpected JSON or incomplete messages

`FramedStream` delivers bytes; it does not decode JSON or validate a protocol.
For a JSON-only line protocol, keep startup banners and diagnostic logs off the
child's stdout. Write diagnostics to stderr and leave `stderr_to_stdout` disabled
for that conversation. Treat a decoding failure as a protocol error rather than
discarding arbitrary lines until something parses.

A `{:closed, reason, remainder}` event can contain an unfinished final line.
Decide explicitly whether your protocol permits it. A successful capture includes
those original bytes, but capture success does not make them valid JSON.

## Cleanup uncertainty

Close through `Subprocess.close/1`, then promptly inspect
`Subprocess.cleanup_receipt/1`. The typed receipt distinguishes actual direct
child reaping from targeted-group absence and from uncertainty.
`:cleanup_status_unavailable` can mean the retained receipt expired or its
Guardian was lost; Actor DOWN cannot substitute for that evidence. A close
timeout retains its error even while the Guardian attempts cleanup.

Keep known cleanup failures in your application result and diagnostic record.
Do not signal a numeric PID from a stale handle or infer process-tree cleanup
from a reaped leader. Descendants that escape the requested group remain outside
the contract. The default receipt-retention lease is five seconds, so inspecting
it much later is expected to fail.

## Reporting a problem

Include the exact RPC version/source revision, Elixir/OTP versions,
OS/architecture, API operation, tagged error, configured finite limits and
whether you used a source install or assembled release. Where available, include
`Subprocess.stats/1` and the typed cleanup receipt. Redact credentials, environment
values and application payloads. A minimal reproducer should keep the same
ownership and deadline assumptions rather than suppressing the failure.
