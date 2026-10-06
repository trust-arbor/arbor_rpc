# BEAM compatibility CI

`workflows/ci.yml` compiles and tests standalone RPC and checks installed
source archives on these explicit, compatible pairs:

| Elixir | Erlang/OTP | Purpose |
|---|---|---|
| 1.17.3 | 27.0 | Retained minimum and formatting baseline |
| 1.18.5 | 27.3.4.18 | Elixir 1.18 compatibility |
| 1.19.6 | 28.5.0.7 | Elixir 1.19 compatibility |
| 1.20.4 | 29.1.1 | Elixir 1.20 compatibility |

Dependencies compile separately before the package's warnings-as-errors gate
in both development and test environments. Formatting runs only on the minimum
toolchain so newer formatter output does not force unrelated changes.

`workflows/beam-latest.yml` runs every Monday at 07:31 UTC and can be started
with **Run workflow** under **Actions → Latest stable BEAM**. It also runs on
pull requests changing either BEAM workflow so CI changes are exercised before
merge. Its schedule becomes active only after it reaches the default branch.
The pinned package workflow also supports **Run workflow**.

The latest workflow tests the newest stable patches in each named Elixir/OTP
track plus the newest stable Elixir and OTP overall. It uses setup-beam's stable
version ranges, records the exact resolved versions, and warns when they exceed
the pinned pair. The 1.17.3/27.0 pair intentionally remains the minimum; drift
does not itself fail the job or automatically change pins. Maintainers review
new releases and update the other pinned pairs and their latest-workflow
comparison values together.

Compilation or test failures remain visible, including when a future newest
Elixir/OTP pair is incompatible. These runs provide early warning and do not
declare support for future versions. They do not run formatting, use build
caches, publish packages, or merge updates. Version output, resolved dependency
lists, and generated lockfiles are retained as artifacts; RPC currently resolves
external dependencies without committed package lockfiles, so a failure can
also reflect dependency drift.
