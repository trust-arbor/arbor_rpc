# ArborRPC quickstart

ArborRPC is a low-level library for JSON-RPC envelopes, newline framing and
owned subprocesses. Protocol sessions and method validation belong to your
application, ArborMCP or ArborACP.

## Install the candidate

Version `1.0.0-rc.1` is untagged and unpublished. For now, clone
`https://github.com/trust-arbor/arbor_rpc` beside your application and declare
`{:arbor_rpc, path: "../arbor_rpc"}` in its dependencies. From the application,
run `mix deps.get`, `mix compile`, then `iex -S mix` to try the examples below.
You can also run `iex -S mix` at the RPC repository root after fetching its
dependencies.

Once published, the equivalent reproducible Hex dependency is
`{:arbor_rpc, "== 1.0.0-rc.1"}`. This does not assert current registry availability.
See [installation](../README.md#installation) and the
[changelog](../CHANGELOG.md) before adopting the candidate.

Use Elixir 1.17 or newer with a compatible OTP release. Source installation on
macOS/Darwin or Linux requires a C17 compiler. The native examples also need
the ordinary `printf` and `cat` executables on your PATH. They use literal argv;
ArborRPC does not interpret shell command strings. Windows native subprocess
operations are unsupported.

## Capture a finite command

```elixir
alias Arbor.RPC.Subprocess

printf = System.find_executable("printf") || raise "printf is required for this example"

{:ok, "hello\n", 0} =
  Subprocess.capture([printf, "%s", "hello\n"],
    timeout: 5_000,
    max_output_bytes: 1_024,
    process_group: true
  )
```

`capture/2` owns and closes the child for this finite operation. It preserves
the original output bytes, including line endings and an unfinished final line.
The third result element is the child's exit status; a nonzero status is still
`{:ok, bytes, status}` for your application to interpret. Timeout, pressure and
known cleanup failures return errors, never successful truncated output.
`stderr_to_stdout: true` explicitly combines stderr into the captured stream.

The output deadline and the subsequent finite cleanup budget are distinct.
Allow for cleanup when setting the surrounding application's deadline.

## Write and pull one newline frame

Use an owned child when a protocol conversation needs multiple writes/reads:

```elixir
alias Arbor.RPC.{FramedStream, Subprocess}

cat = System.find_executable("cat") || raise "cat is required for this example"
{:ok, child} = Subprocess.open([cat], process_group: true)

try do
  :ok = Subprocess.write(child, "hello\n")
  {:ok, "hello"} = FramedStream.next(child, 1_000)
after
  close_result = Subprocess.close(child)
  receipt_result = Subprocess.cleanup_receipt(child)
  IO.inspect(close_result, label: "close result")
  IO.inspect(receipt_result, label: "cleanup receipt")
end
```

A complete frame excludes LF and preserves any CR before LF. A natural exit
event is `{:closed, reason, unfinished_bytes}`; your protocol decides what those
remaining bytes mean. Use `capture/2` instead when you need exact whole stdout,
including its LF bytes.

The example prints cleanup results so a known failure remains visible. In an
application, retain and handle them. After a confirmed group close, the receipt
records `direct_child: :reaped` and `targeted_group: :absent`; use
`Arbor.RPC.Subprocess.Receipt.result/1` to interpret it. Inspect the receipt
promptly: its default retention is five seconds. Process DOWN alone does not
prove OS cleanup, and a missing receipt remains unconfirmed.

The process calling `open/2` is the default lifetime owner. It must remain alive
for the conversation. A temporary Task can pass `owner: persistent_local_pid`
to open on behalf of a live local process; passing the handle alone does not
transfer ownership. Handles, readers and subscribers are local to the child's
BEAM node.

Pass a finite `next/2` timeout explicitly; its omitted timeout is `:infinity`.
When filtering multiple frames for one operation, capture one monotonic cutoff
and reuse `FramedStream.next_until/2` so filtering does not renew the deadline.
Pull delivery and acknowledged push delivery are mutually exclusive. See
[delivery and pressure](../README.md#delivery-and-pressure) for the push event
format, ACK rules and bounds.

## Construct and inspect an envelope

```elixir
alias Arbor.RPC.JSONRPC

request = JSONRPC.request("echo", %{"text" => "hello"}, 1)
{:request, "echo", %{"text" => "hello"}, 1} = JSONRPC.parse_unvalidated(request)
response = JSONRPC.response(1, %{"text" => "hello"})
{:result, %{"text" => "hello"}, 1} = JSONRPC.parse_unvalidated(response)
```

These helpers construct or decompose maps. `parse_unvalidated/1` also accepts
JSON bytes and lists, but it is deliberately not a protocol validator. Validate
IDs, method names, parameters and batch rules before dispatching untrusted data.
The `echo` name above is only an example envelope; no handler is invoked.

## Decode bytes without starting a child

```elixir
alias Arbor.RPC.Framing

decoder = Framing.new(max_frame_bytes: 16)
{:ok, ["one\r"], decoder} = Framing.push(decoder, "one\r\npart")
{:ok, ["partial"], decoder} = Framing.push(decoder, "ial\n")
"" = Framing.remainder(decoder)
```

The per-frame byte bound excludes LF and applies across chunk boundaries.
Returned frames preserve CR, UTF-8 BOMs and other original bytes. The caller
decides whether to accept blank lines/BOMs, decodes JSON and bounds the total
delivery queue. Framing and JSON-RPC helpers start no native process, although
installing the source package still builds the helper on supported native
platforms.

## Continue

Read the [lifetime contract](../README.md#native-lifecycle-contract) and
[resource limits](../README.md#delivery-and-pressure) before building a persistent
integration. If a command fails, use [troubleshooting](TROUBLESHOOTING.md).
Maintainers can follow the [development and archive checks](DEVELOPMENT.md).

Operational queue inspection uses `{:ok, statistics} = Subprocess.stats(handle)`
or `{:error, reason}` when unavailable. `Subprocess.stats!/1` returns the map
and raises on failure when that is the desired inspection policy.
