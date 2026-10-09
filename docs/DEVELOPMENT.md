# Developing ArborRPC

Run these commands at the standalone RPC repository root. ACP and MCP consume
RPC independently; their source checkouts are not required for RPC tests or
documentation. `SOURCE_SNAPSHOT` records the original extraction and preserved
history. Use the repository's actual Git revision when associating results.

## Toolchain and normal dependencies

`mise.toml` selects the established local toolchain. The minimum formatting/CI
pair remains Elixir 1.17.3 / OTP 27.0. The four pinned CI pairs and five latest
tracks are documented in
[BEAM compatibility CI](https://github.com/trust-arbor/arbor_rpc/blob/main/.github/BEAM_CI.md).
Floating latest lanes record resolved versions and expose failures; they do not
automatically expand the supported matrix.

On macOS/Darwin and Linux, the custom compiler builds the native helper from
reviewed C17 source. Set `CC` to one compiler executable if the default `cc`
is unsuitable. Leave `ARBOR_V2_DEPS`, `MIX_DEPS_PATH` and build/dependency
overrides unset for normal dependency resolution and packaging. `ARBOR_V2_DEPS`
is an explicit local source override for external dependencies, not a consumer
installation requirement. RPC needs neither `ARBOR_V2_LOCAL` nor
`ARBOR_RPC_PATH`.

## Compile, format and test

```sh
MIX_ENV=dev mix deps.get
MIX_ENV=dev mix deps.compile
MIX_ENV=dev mix compile --warnings-as-errors --no-deps-check
MIX_ENV=test mix deps.get
MIX_ENV=test mix deps.compile
MIX_ENV=test mix test --warnings-as-errors --no-deps-check
mix format --check-formatted
elixir scripts/check_boundaries.exs
```

Dependencies compile separately so a third-party warning does not hide the
package's own warnings-as-errors gate. Use the minimum pair for formatting so
newer formatter behavior does not force unrelated edits.

The ordinary suite covers envelope/framing/environment mechanics, subprocess
delivery and cleanup, capture and write admission. Native lifecycle cases start
real owned children and need the supported native platform/compiler. For a
focused run:

```sh
MIX_ENV=test mix test test/arbor_rpc/subprocess_test.exs \
  test/arbor_rpc/subprocess_capture_test.exs \
  test/arbor_rpc/write_admission_test.exs
```

The test helper excludes integration, external, slow and interop categories by
default. State any additional tags, selected files and toolchain when reporting
results. A focused test run or repository split does not establish a full
release gate or continuous 48-hour qualification.

## Build and review documentation

```sh
MIX_ENV=dev mix docs --warnings-as-errors
```

ExDoc is a dev-only, non-runtime dependency. The README, quickstart,
troubleshooting, this guide and changelog are registered as extras and shipped
in the source package. Read the generated navigation and follow its links;
examples should use public APIs and should preserve ownership, original
deadlines and explicit cleanup errors.

Source links use the prospective root tag `v<version>` in
`trust-arbor/arbor_rpc`. They become valid release references only after a
reviewed tag is created; generating documentation creates no tag.

## Qualify a source archive

Use a fresh archive input directory containing exactly one RPC tar. Build the
publishable manifest with local dependency overrides unset:

```sh
mkdir archive-consumer-input
MIX_ENV=prod mix deps.get --only prod
MIX_ENV=prod mix hex.build \
  --output archive-consumer-input/arbor_rpc-1.0.0-rc.1.tar
python3 scripts/check_archive_consumer.py archive-consumer-input \
  --expected-version 1.0.0-rc.1 \
  --report _verification/rpc-archive-consumer.json
```

The explicit version validates the current literal; it does not override it.
Preserve old artifacts and use a new directory for another source/version. The
checker verifies Hex/source checksums, package and documentation metadata,
then uses a fresh consumer to resolve normal Hex dependencies, compile the
shipped C source, and run the installed probe. It builds an OTP release and
repeats the probe with compiler lookup denied. That probe checks exact output
bytes/nonzero status, installed helper identity, typed child/group cleanup and
idempotent close.

`--metadata-only` stops before installation and runtime probes. It is not an
installed/release verdict. Local offline checks may explicitly select independent
external sources through `ARCHIVE_CONSUMER_EXTERNAL_DEPS`; record that choice
and also qualify normal Hex resolution before publication.

## Release boundaries

RPC owns its literal version, changelog, source archive and root `v<version>`
tag. ACP/MCP declare compatible RPC dependency ranges. For the coordinated
RC, publish RPC before the dependent protocol packages. Version/tag/archive
identity and actual registry installation must agree; archive checks perform
no publication or tagging.

RC1 is published and tagged as `v1.0.0-rc.1`. Stable qualification, including the
accepted continuous 48-hour gate, remains incomplete and no soak is active.
Keep that status separate from successful local tests, short rehearsals and
CI runs. Public defaults and finite resource/cleanup guarantees remain part of
the contract when adding tests or improving performance.
