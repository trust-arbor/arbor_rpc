# Changelog

## 2.0.0-rc.1 (planned)

Release candidate for downstream migration testing; publication is pending.

- Provide neutral `Arbor.RPC.*` JSON-RPC, framing, environment isolation and
  subprocess ownership shared by MCP and ACP.
- Build the owned native parent from C17 source on macOS/Linux. Typed cleanup
  receipts distinguish child reaping, targeted-group absence and uncertainty;
  assembled releases include the helper and need no runtime compiler. Windows
  native subprocess operations are unsupported.
- Fix the private write-publication race: after claiming a write, reload its
  published binary and verify the same phase, producer and original deadline.
  Keep existing count/byte limits and uncertain-outcome accounting unchanged.
- Document the dependency split in the [v1 to v2 migration guide](https://github.com/trust-arbor/arbor_mcp/blob/codex/v2-migration/docs/guides/MIGRATING_V1_TO_V2.md).

Stable promotion remains gated on the unfinished 48-hour qualification. Cleanup
does not promise containment of descendants that escape the targeted group.

## 2.0.0-dev

Initial package extraction from ExMCP. Full v2 runtime qualification remains pending.
