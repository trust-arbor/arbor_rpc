defmodule Arbor.RPC.SubprocessShutdownTest do
  use ExUnit.Case, async: false
  alias Arbor.RPC.{FramedStream, Subprocess}

  @moduletag timeout: 10_000

  # Exits 0 on stdin EOF; a TERM delivered before that shows up as status 143.
  @eof_server ~s(trap 'exit 143' TERM; while read -r _; do :; done; exit 0)

  setup do
    directory = Path.join(System.tmp_dir!(), "arbor-rpc-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, directory: directory}
  end

  test "the native helper is installed for this platform" do
    assert Subprocess.available?()
  end

  test "close sends EOF before any signal, so a child that exits on EOF is never signalled" do
    for group <- [false, true], _attempt <- 1..5 do
      handle = shell(@eof_server, process_group: group)
      assert :ok = Subprocess.write(handle, "hello\n")
      assert :ok = Subprocess.close(handle)

      assert {:ok, %{direct_child: :reaped, vendor_status: 0}} =
               Subprocess.cleanup_receipt(handle)
    end
  end

  test "a child that outlives the EOF grace is sent TERM" do
    handle = shell("exec sleep 30", eof_grace: 200)
    started = now()
    assert :ok = Subprocess.close(handle)
    elapsed = now() - started
    assert elapsed >= 200 and elapsed < 500

    assert {:ok, %{direct_child: :reaped, vendor_status: 143}} =
             Subprocess.cleanup_receipt(handle)
  end

  test "eof_grace: 0 sends TERM without waiting" do
    handle = shell("exec sleep 30", eof_grace: 0)
    started = now()
    assert :ok = Subprocess.close(handle)
    assert now() - started < 150
    assert {:ok, %{vendor_status: 143}} = Subprocess.cleanup_receipt(handle)
  end

  test "an exited group leader's stragglers are signalled without the EOF grace", %{
    directory: directory
  } do
    pidfile = Path.join(directory, "straggler")

    handle =
      shell(
        "sleep 30 </dev/null >/dev/null 2>&1 & echo $! > '#{pidfile}'; #{@eof_server}",
        process_group: true,
        eof_grace: 2000,
        cleanup_timeout: 3000
      )

    eventually(fn -> File.exists?(pidfile) and File.read!(pidfile) =~ "\n" end)
    straggler = File.read!(pidfile) |> String.trim() |> String.to_integer()
    started = now()
    assert :ok = Subprocess.close(handle)
    assert now() - started < 1000

    assert {:ok, %{direct_child: :reaped, targeted_group: :absent, vendor_status: 0}} =
             Subprocess.cleanup_receipt(handle)

    refute alive?(straggler)
  end

  test "a stream failure skips the EOF grace and is reported promptly" do
    handle =
      shell("printf 'abcde'; exec sleep 30",
        max_frame_bytes: 4,
        eof_grace: 2000,
        cleanup_timeout: 3000
      )

    started = now()
    assert {:closed, :frame_too_large, _remainder} = FramedStream.next(handle, 1000)
    assert now() - started < 1000
  end

  test "close_input delivers admitted bytes before EOF and keeps output readable" do
    handle = shell("cat; echo done")
    assert :ok = Subprocess.write(handle, "one\n")
    assert :ok = Subprocess.write(handle, "two\n")
    assert :ok = Subprocess.close_input(handle)
    assert :ok = Subprocess.close_input(handle)
    assert {:error, :closed} = Subprocess.write(handle, "late\n")
    assert {:ok, "one"} = FramedStream.next(handle, 1000)
    assert {:ok, "two"} = FramedStream.next(handle, 1000)
    assert {:ok, "done"} = FramedStream.next(handle, 1000)
    assert {:closed, {:exit_status, 0}, ""} = FramedStream.next(handle, 1000)
    assert {:error, :closed} = Subprocess.close_input(handle)
  end

  test "close_input leaves a child that ignores EOF running until close" do
    handle = shell("exec sleep 30")
    assert :ok = Subprocess.close_input(handle)
    assert Subprocess.connected?(handle)
    assert :ok = Subprocess.close(handle)
    assert {:ok, %{vendor_status: 143}} = Subprocess.cleanup_receipt(handle)
  end

  test "an explicit EOF grace must fit the budget while an implicit one shrinks to fit" do
    assert {:error, :invalid_cleanup_budget} =
             Subprocess.open(["/bin/sh"], eof_grace: 400, cleanup_timeout: 500, term_grace: 100)

    assert {:error, {:invalid_limit, :eof_grace}} =
             Subprocess.open(["/bin/sh"], eof_grace: -1)

    # The smallest budget accepted before EOF grace existed still opens, with
    # no grace left, so its close keeps the previous immediate TERM.
    handle = shell("exec sleep 30", cleanup_timeout: 150, term_grace: 100)
    started = now()
    assert :ok = Subprocess.close(handle)
    assert now() - started < 150
  end

  defp shell(script, opts \\ []) do
    assert {:ok, handle} = Subprocess.open(["/bin/sh", "-c", script], opts)
    on_exit(fn -> Subprocess.close(handle) end)
    handle
  end

  defp alive?(pid) do
    case System.cmd("/bin/kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  end

  defp eventually(predicate, attempts \\ 100)

  defp eventually(predicate, attempts) when attempts > 0 do
    if predicate.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          eventually(predicate, attempts - 1)
        )
  end

  defp eventually(_predicate, 0), do: flunk("condition did not become true")

  defp now, do: System.monotonic_time(:millisecond)
end
