# Changelog

## Unreleased

## 1.0.0-rc.3 — 2026-10-26

- `JSONRPC.parse_unvalidated/1` accepts a request or notification that leaves
  out `params`, as JSON-RPC 2.0 allows, with empty params. It used to return
  `{:error, :invalid_message}`, so a peer's `ping` or list-change notification
  sent without params was dropped. A response that also names a method stays a
  response.
- **Behavior change:** `Subprocess.close/1` closes the child's stdin first and
  sends TERM only if the child outlives the new `:eof_grace` option (default
  250 ms), then KILL after `:term_grace`. This follows the MCP stdio shutdown
  order; a child that exits on EOF is never signalled. Closing a child that
  ignores stdin now takes up to `eof_grace` longer. An omitted `:eof_grace`
  shrinks to fit a smaller `:cleanup_timeout`, so existing budgets stay valid;
  `eof_grace: 0` restores the previous immediate TERM. In group mode, an exited
  leader's remaining group is signalled without the EOF grace, and stream
  failures (frame, queue or mailbox pressure, helper protocol errors) skip it
  so readers get the error promptly. KILL is always sent before the cleanup
  budget ends, even after a late wake.
- Add `Subprocess.close_input/1`, which sends stdin EOF after admitted writes
  drain and keeps output readable.
- The C compiler is now optional. Without one, the build warns and continues;
  JSON-RPC and framing work and `Subprocess` returns
  `{:error, :native_helper_unavailable}`. Add `Subprocess.available?/0`. Set
  `ARBOR_RPC_NATIVE` or `config :arbor_rpc, native_helper:` to `required` to
  fail the build instead, or `skip` to never build the helper.
- Build the helper into `_build/<env>/lib/arbor_rpc/priv/native/` instead of
  the dependency's source tree, so environments and `MIX_TARGET`s no longer share
  one binary and a read-only dependency directory works.
- Compiler warnings fail the build only inside this repository, not in
  dependency installs. The rebuild stamp now includes the compiler's `--version`.
  Without a compiler, a helper built from different source or for another
  architecture is removed rather than kept. Generated files that older releases
  left in the source `priv/native/` are removed. A `CC` that contains arguments
  is a build error.
- The helper sleeps until its next deadline or child exit (via SIGCHLD) instead
  of waking every 5 ms; idle CPU fell about ninefold in a 10-second macOS check.
- Native helper protocol version 2 adds the EOF grace to its startup arguments
  and an input EOF command. CI now runs the helper's C-level protocol and
  post-reap suites, and the archive check covers a compiler-free install.

## 1.0.0-rc.2 — 2026-10-22

- **Security:** `Arbor.RPC.Subprocess.Command.resolve/4` ignores relative and empty child `PATH` entries. They previously resolved against the child's working directory, so a bare command name could select an executable planted in an untrusted workspace.
- Refresh published-RC installation guidance and consolidate current documentation.


## 1.0.0-rc.1 — 2026-10-08

- Ship agent usage rules in Hex archives and ExDoc, with downstream UsageRules
  setup guidance and API-reference validation through the existing docs gate.
- Return `{:ok, map} | {:error, reason}` from `Subprocess.stats/1`; add
  `stats!/1` for explicit value-or-raise inspection. Queue fields and ownership
  accounting are unchanged.

- Remove generic actor `call/2,3` from the Subprocess facade. FramedStream and
  write admission use an internal implementation owner, preserving generation,
  deadlines, capacity, acknowledgement and cleanup checks.
- Start independent 1.x versioning for this newly extracted package.
- Preserve the published `2.0.0-rc.1` archive and tag; retire the superseded
  candidate after the replacement is published and its installation verified.
- Update installation, migration and release tooling for independent versions.

## 2.0.0-rc.1 — 2026-10-06

Published initial extraction candidate. Its 2.x version was a coordinated
versioning mistake; this new library now starts its independent 1.x line.
The existing release and tag remain available.

- Provide neutral `Arbor.RPC.*` JSON-RPC, framing, environment isolation and
  subprocess ownership shared by MCP and ACP.
- Build the owned native parent from C17 source on macOS/Linux. Typed cleanup
  receipts distinguish child reaping, targeted-group absence and uncertainty;
  assembled releases include the helper and need no runtime compiler. Windows
  native subprocess operations are unsupported.
- Fix the private write-publication race: after claiming a write, reload its
  published binary and verify the same phase, producer and original deadline.
  Keep existing count/byte limits and uncertain-outcome accounting unchanged.
- Document the dependency split in the [v1 to v2 migration guide](https://github.com/trust-arbor/arbor_mcp/blob/master/docs/guides/MIGRATING_V1_TO_V2.md).
- Extract the RPC project to its own repository with its existing history, root
  `v<version>` source links, independent BEAM CI and source-archive checks.
- Ship public-API quickstart, troubleshooting and development guides alongside
  the native ownership and resource-limit contract.

Stable promotion remains gated on the unfinished 48-hour qualification. Cleanup
does not promise containment of descendants that escape the targeted group.

## 2.0.0-dev

Initial package extraction from ExMCP. Full v2 runtime qualification remains pending.
