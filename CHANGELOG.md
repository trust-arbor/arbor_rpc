# Changelog

## 1.0.0-rc.1 (unreleased)

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
- Document the dependency split in the [v1 to v2 migration guide](https://github.com/trust-arbor/arbor_mcp/blob/codex/v2-migration/docs/guides/MIGRATING_V1_TO_V2.md).
- Extract the RPC project to its own repository with its existing history, root
  `v<version>` source links, independent BEAM CI and source-archive checks.
- Ship public-API quickstart, troubleshooting and development guides alongside
  the native ownership and resource-limit contract.

Stable promotion remains gated on the unfinished 48-hour qualification. Cleanup
does not promise containment of descendants that escape the targeted group.

## 2.0.0-dev

Initial package extraction from ExMCP. Full v2 runtime qualification remains pending.
