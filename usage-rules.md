# ArborRPC usage rules

ArborRPC provides JSON-RPC envelopes, newline framing, environment policy and
owned subprocesses. The Hex package and OTP application are `arbor_rpc`; public
modules use `Arbor.RPC.*`. These rules describe the independent 1.x API. Guide
paths below are relative to the installed `arbor_rpc` dependency directory,
usually `deps/arbor_rpc`. Read `README.md` and `docs/QUICKSTART.md` for
installation, candidate availability and complete examples.

## Keep protocol policy in its owning library

- Use ArborMCP or ArborACP for their protocol negotiation, method validation,
  session lifecycle and capabilities. RPC helpers do not supply those policies.
- Declare `arbor_rpc` directly when your code calls its APIs, even if another
  dependency already uses it.
- Use `Arbor.RPC.JSONRPC`, `Arbor.RPC.Framing`, `Arbor.RPC.StdioFraming`,
  `Arbor.RPC.PortEnvironment`, `Arbor.RPC.Subprocess`, `Arbor.RPC.FramedStream`
  and `Arbor.RPC.Subprocess.Receipt`. Keep handles and framing state opaque.
  Do not call `Internal` modules, private actors or write-admission machinery.

## Launch with explicit ownership and environment

- Subprocess commands are executable argv lists, not shell command strings.
  Pass cwd and environment through the documented options. A shell is executed
  only when you explicitly choose a shell executable.
- The process calling `Arbor.RPC.Subprocess.open/2` is the default lifetime
  owner. A temporary Task must use `owner: persistent_local_pid` when opening
  for a longer-lived local process. Passing a handle does not transfer ownership.
  Handles, readers and subscribers stay on the child's BEAM node.
- The default child environment is isolated. Supply needed variables explicitly;
  `false` unsets a variable. Environment inheritance is an explicit policy choice.
- Source installation on macOS/Linux requires a C17 compiler, including framing
  consumers. `CC` selects the compiler executable. Assembled releases include
  the built helper and need no runtime compiler. Windows native subprocess
  operations are unsupported; consult the README for qualified platforms.

## Bound reads, writes and output

- `Arbor.RPC.Subprocess.capture/2` owns a finite command and returns
  `{:ok, original_bytes, exit_status}`. A nonzero exit status still needs the
  caller's interpretation. Timeout, pressure and known cleanup failure return
  errors rather than successful truncated output. `stderr_to_stdout: true`
  explicitly combines stderr with stdout.
- Choose finite capture timeouts and output-byte limits. Cleanup has a separate
  finite budget after read expiry; account for it in the surrounding deadline.
- `Arbor.RPC.Subprocess.write/2` admits bounded input; admission does not prove
  that the child processed it. Preserve uncertainty when handling failures.
- Pass a finite timeout to `Arbor.RPC.FramedStream.next/2`; its omitted timeout
  is `:infinity`. Filtering several frames should reuse one monotonic cutoff
  through `Arbor.RPC.FramedStream.next_until/2`, rather than renew the deadline.
- Pull delivery and acknowledged push delivery are mutually exclusive. Follow
  the documented event, generation, credit and ACK contracts for push delivery;
  do not leave an unbounded receiver mailbox.
- A frame excludes LF and preserves CR. A natural close includes unfinished
  bytes in `{:closed, reason, unfinished_bytes}`; the protocol wrapper decides
  how to interpret them. `capture/2` preserves complete original output bytes.
  `Arbor.RPC.Framing` bounds each frame; the caller must also bound its queue.

## Retain cleanup evidence and operational errors

- Close owned children with `Arbor.RPC.Subprocess.close/1` and handle the result.
  Process DOWN alone does not prove OS cleanup or targeted-group absence.
- Read `Arbor.RPC.Subprocess.cleanup_receipt/1` promptly after close and use
  `Arbor.RPC.Subprocess.Receipt.result/1` to interpret it. Receipt retention is
  finite (five seconds by default); missing evidence remains unconfirmed.
- Group cleanup concerns the verified targeted group. It does not prove that a
  descendant which escaped that group has stopped. Keep that boundary explicit
  when choosing subprocess policy.
- `Arbor.RPC.Subprocess.stats/1` returns `{:ok, statistics}` or an error;
  `Arbor.RPC.Subprocess.stats!/1` explicitly returns a value or raises. Pure
  envelope constructors keep their documented bare values.

See `docs/TROUBLESHOOTING.md` for compiler, executable,
pressure, timeout and cleanup errors. Read the current module documentation
before selecting limits or interpreting receipt fields.
