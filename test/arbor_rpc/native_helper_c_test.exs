defmodule Arbor.RPC.NativeHelperCTest do
  # Runs the helper's C-level suites against the helper this build installed.
  use ExUnit.Case, async: false
  alias Arbor.RPC.Subprocess.NativeBackend

  @moduletag timeout: 120_000
  @native Path.expand("../support/native", __DIR__)

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "arbor_native_c_#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, directory: directory}
  end

  test "post-reap group probes never signal an occupied or reused group", %{
    directory: directory
  } do
    binary = compile!(directory, "post_reap_invariant_test.c")
    assert {output, 0} = System.cmd(binary, [], stderr_to_stdout: true)
    assert output =~ "zero signals"
  end

  if is_nil(System.find_executable("python3")), do: @tag(skip: "python3 is unavailable")

  test "native protocol and cleanup state machine", %{directory: directory} do
    vendor = compile!(directory, "vendor_fixture.c")
    assert {:ok, helper} = NativeBackend.helper_path()

    {output, status} =
      System.cmd("python3", ["-B", Path.join(@native, "test_native_protocol.py")],
        env: [{"ARBOR_RPC_HELPER", helper}, {"ARBOR_RPC_VENDOR_FIXTURE", vendor}],
        stderr_to_stdout: true
      )

    assert status == 0, output
  end

  defp compile!(directory, source) do
    binary = Path.join(directory, Path.rootname(source))

    assert {_, 0} =
             System.cmd(
               "cc",
               ~w(-std=c17 -O2 -Wall -Wextra -Werror -pedantic) ++
                 [Path.join(@native, source), "-o", binary],
               stderr_to_stdout: true
             )

    binary
  end
end
