Logger.configure(level: :warning)
ExUnit.start(capture_log: true)

ExUnit.configure(
  exclude: [
    integration: true,
    external: true,
    slow: true,
    interop: true,
    interop_acp: true,
    interop_acp_cli: true,
    interop_acp_ecosystem: true,
    wip: true,
    skip: true
  ]
)
