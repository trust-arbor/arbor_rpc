defmodule Arbor.RPC.NativeGuardianTest do
  use ExUnit.Case, async: false
  alias Arbor.RPC.{FramedStream, Subprocess}
  alias Arbor.RPC.Subprocess.{NativeBackend, Receipt}

  test "unconfirmed helper-loss receipt preserves observed vendor status separately" do
    {:ok, handle} =
      Subprocess.open(["/bin/sh", "-c", "sleep 0.4 & printf 'final\\n'; exit 7"],
        process_group: true,
        term_grace: 300,
        cleanup_timeout: 500
      )

    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    eventually(fn -> :sys.get_state(guardian).vendor_status == 7 end)
    assert :sys.get_state(guardian).receipt == nil
    {:os_pid, helper} = Port.info(:sys.get_state(guardian).port, :os_pid)
    assert {_, 0} = System.cmd("/bin/kill", ["-KILL", Integer.to_string(helper)])
    assert {:ok, "final"} = FramedStream.next(handle, 1000)

    assert {:closed, {:cleanup_failed, {:exit_status, 7}, {:error, _}}, ""} =
             FramedStream.next(handle, 1000)

    eventually(fn ->
      match?({:ok, %Receipt{helper_status: 137}}, Subprocess.cleanup_receipt(handle))
    end)

    assert {:ok, %Receipt{direct_child: :unconfirmed, vendor_status: 7, helper_status: 137}} =
             Subprocess.cleanup_receipt(handle)

    assert {:error, _} = Subprocess.close(handle)
  end

  test "immutable Actor write cap rejects forged facade limits and direct invalid writes" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_write_bytes: 4)
    on_exit(fn -> Subprocess.close(handle) end)

    assert {:error, :write_too_large} =
             Subprocess.write(%{handle | max_write_bytes: 100}, "too large")

    assert {:error, :write_too_large} =
             Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "too large"})

    assert {:error, :invalid_iodata} =
             Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, [:invalid]})

    assert Subprocess.connected?(handle)
    assert :ok = Subprocess.write(handle, "ok\n")
    assert {:ok, "ok"} = FramedStream.next(handle, 500)
    assert :ok = Subprocess.close(handle)
  end

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "arbor_native_launch_#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    helper = Path.join(directory, "delayed_helper")
    source = Path.expand("../support/native/delayed_startup_helper.c", __DIR__)

    assert {_, 0} =
             System.cmd(
               "cc",
               [
                 "-std=c17",
                 "-O2",
                 "-Wall",
                 "-Wextra",
                 "-Werror",
                 "-pedantic",
                 source,
                 "-o",
                 helper
               ],
               stderr_to_stdout: true
             )

    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, directory: directory, delayed_helper: helper}
  end

  test "hard owner death during actual native launch closes control and reaps the still-pinned child",
       context do
    {:ok, path} = NativeBackend.helper_path()
    original = path <> ".original"
    marker = Path.join(context.directory, "launch_pid")
    File.rename!(path, original)
    File.cp!(context.delayed_helper, path)
    File.chmod!(path, 0o755)

    try do
      parent = self()

      owner =
        spawn(fn ->
          send(
            parent,
            {:unexpected_open_result,
             Subprocess.open(["/bin/sleep", "30"], env: %{"ARBOR_TEST_EXEC_MARKER" => marker})}
          )
        end)

      eventually(fn -> File.exists?(marker) and File.stat!(marker).size > 0 end)
      child = File.read!(marker) |> String.to_integer()
      assert alive?(child)
      Process.exit(owner, :kill)
      eventually(fn -> not alive?(child) end)
      refute_receive {:unexpected_open_result, _result}, 20

      eventually(fn ->
        Enum.all?(Process.list(), fn pid ->
          case Process.info(pid, :dictionary) do
            {:dictionary, dictionary} ->
              List.keyfind(dictionary, {Arbor.RPC.Subprocess.Actor, :owner}, 0) !=
                {{Arbor.RPC.Subprocess.Actor, :owner}, owner} and
                List.keyfind(dictionary, {Arbor.RPC.Subprocess.Guardian, :owner}, 0) !=
                  {{Arbor.RPC.Subprocess.Guardian, :owner}, owner}

            nil ->
              true
          end
        end)
      end)
    after
      File.rm!(path)
      File.rename!(original, path)
    end
  end

  test "withheld native credit bounds raw delivery while control and cleanup remain responsive" do
    {:ok, handle} =
      Subprocess.open(
        ["/bin/sh", "-c", "read go; sleep 0.05; exec dd if=/dev/zero bs=65536 count=64"],
        max_frame_bytes: 4_194_304,
        cleanup_timeout: 200,
        term_grace: 50
      )

    [actor] = Subprocess.linked_processes(handle)
    child = Subprocess.os_pid(handle)
    guardian = :sys.get_state(actor).guardian
    assert :ok = Subprocess.write(handle, "go\n")
    :ok = :sys.suspend(actor)
    on_exit(fn -> Subprocess.close(handle) end)

    chunks = fn ->
      {:messages, messages} = Process.info(actor, :messages)
      for {:arbor_rpc_native, ^guardian, _, {:chunk, _, bytes}} <- messages, do: bytes
    end

    eventually(fn -> length(chunks.()) == 1 end)
    Process.sleep(50)
    assert [chunk] = chunks.()
    assert byte_size(chunk) in 1..16_384
    assert :sys.get_state(guardian).chunk == 1
    assert {:message_queue_len, 0} = Process.info(guardian, :message_queue_len)
    assert {:error, :cleanup_pending} = Subprocess.cleanup_receipt(handle)
    # A caller cannot return the Actor's native credit through the ACK facade.
    Arbor.RPC.Subprocess.Guardian.ack(guardian, Subprocess.identity(handle), 1)
    Process.sleep(20)
    assert length(chunks.()) == 1
    assert :sys.get_state(guardian).chunk == 1
    assert {:error, :timeout} = Subprocess.close(handle)

    eventually(fn ->
      match?({:ok, %Receipt{direct_child: :reaped}}, Subprocess.cleanup_receipt(handle))
    end)

    refute alive?(child)
    assert :ok = Subprocess.close(handle)
  end

  test "actual vendor exit and cleanup do not discard final bytes waiting for native credit" do
    {:ok, handle} =
      Subprocess.open(
        [
          "/bin/sh",
          "-c",
          "read go; sleep 0.05; head -c 32768 /dev/zero; printf 'tail\\n'; exit 7"
        ],
        max_frame_bytes: 65_536
      )

    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    child = Subprocess.os_pid(handle)
    assert :ok = Subprocess.write(handle, "go\n")
    :ok = :sys.suspend(actor)
    on_exit(fn -> Subprocess.close(handle) end)
    eventually(fn -> :sys.get_state(guardian).vendor_status == 7 end)

    eventually(fn ->
      match?({:ok, %Receipt{direct_child: :reaped}}, Subprocess.cleanup_receipt(handle))
    end)

    assert {:ok, %Receipt{direct_child: :reaped, vendor_status: 7}} =
             Subprocess.cleanup_receipt(handle)

    refute alive?(child)
    refute :sys.get_state(guardian).terminal_sent
    :ok = :sys.resume(actor)
    assert {:ok, bytes} = FramedStream.next(handle, 1000)
    assert bytes == :binary.copy(<<0>>, 32768) <> "tail"
    assert {:closed, {:exit_status, 7}, ""} = FramedStream.next(handle, 1000)
  end

  test "helper death is unconfirmed and never substitutes helper status for vendor status" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "0.4"])
    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    {:os_pid, helper} = Port.info(:sys.get_state(guardian).port, :os_pid)
    child = Subprocess.os_pid(handle)
    assert {_, 0} = System.cmd("/bin/kill", ["-KILL", Integer.to_string(helper)])

    assert {:closed, {:cleanup_failed, :helper_failure, {:error, _}}, ""} =
             FramedStream.next(handle, 1000)

    assert {:ok, %Receipt{direct_child: :unconfirmed, vendor_status: nil}} =
             Subprocess.cleanup_receipt(handle)

    assert {:error, _} = Subprocess.close(handle)
    eventually(fn -> not alive?(child) end)
  end

  test "malformed helper data cannot manufacture a cleanup receipt" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "30"])
    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    port = :sys.get_state(guardian).port
    child = Subprocess.os_pid(handle)
    send(guardian, {port, {:data, <<1, ?R, 0::64>>}})

    assert {:closed, {:cleanup_failed, :helper_failure, {:error, :invalid_helper_packet}}, ""} =
             FramedStream.next(handle, 1000)

    assert {:ok, %Receipt{direct_child: :unconfirmed, reason: :invalid_helper_packet}} =
             Subprocess.cleanup_receipt(handle)

    eventually(fn -> not alive?(child) end)
  end

  test "Guardian owns helper Port; vendor identity and retained cleanup receipt remain distinct" do
    {:ok, handle} =
      Subprocess.open(["/bin/sh", "-c", "echo $$; exec sleep 30"], process_group: true)

    [actor] = Subprocess.linked_processes(handle)
    state = :sys.get_state(actor)
    guardian = :sys.get_state(state.guardian)
    assert Port.info(guardian.port, :connected) == {:connected, state.guardian}
    assert {:os_pid, helper_pid} = Port.info(guardian.port, :os_pid)
    assert {:ok, vendor_pid} = FramedStream.next(handle, 500)
    assert Subprocess.os_pid(handle) == String.to_integer(vendor_pid)
    refute helper_pid == String.to_integer(vendor_pid)
    assert {:error, :cleanup_pending} = Subprocess.cleanup_receipt(handle)
    assert :ok = Subprocess.close(handle)

    assert {:ok,
            %Receipt{
              direct_child: :reaped,
              targeted_group: :absent,
              descendant_tree: :not_contained
            }} = Subprocess.cleanup_receipt(handle)

    assert :ok = Subprocess.close(handle)
  end

  test "hard Actor death preserves owned cleanup and a surviving owner's receipt" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "30"], process_group: true)
    child = Subprocess.os_pid(handle)
    [actor] = Subprocess.linked_processes(handle)
    Process.exit(actor, :kill)

    eventually(fn ->
      match?({:ok, %Receipt{direct_child: :reaped}}, Subprocess.cleanup_receipt(handle))
    end)

    refute alive?(child)
    assert Process.alive?(self())
    assert :ok = Subprocess.close(handle)
  end

  test "Guardian loss stays unconfirmed while native control EOF reaps the real child" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "30"], process_group: true)
    child = Subprocess.os_pid(handle)
    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    Process.exit(guardian, :kill)

    assert {:closed,
            {:cleanup_failed, {:guardian_down, :killed}, {:error, :cleanup_status_unavailable}},
            ""} = FramedStream.next(handle, 1000)

    eventually(fn -> not alive?(child) end)
    assert {:error, :cleanup_status_unavailable} = Subprocess.cleanup_receipt(handle)
    assert {:error, :cleanup_status_unavailable} = Subprocess.close(handle)
  end

  test "finite receipt expiry retires Guardian and never invents later cleanup success" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "30"], receipt_retention: 100)
    [actor] = Subprocess.linked_processes(handle)
    guardian = :sys.get_state(actor).guardian
    monitor = Process.monitor(guardian)
    assert :ok = Subprocess.close(handle)
    assert {:ok, %Receipt{direct_child: :reaped}} = Subprocess.cleanup_receipt(handle)
    assert_receive {:DOWN, ^monitor, :process, ^guardian, :normal}, 500
    assert {:error, :cleanup_status_unavailable} = Subprocess.close(handle)
    assert {:error, :cleanup_status_unavailable} = Subprocess.cleanup_receipt(handle)
  end

  test "missing native executable is explicit and never compiled or replaced at runtime" do
    {:ok, path} = NativeBackend.helper_path()
    hidden = path <> ".hidden"
    File.rename!(path, hidden)

    try do
      assert {:error, :native_helper_unavailable} = Subprocess.open(["/bin/sleep", "30"])
      refute File.exists?(path)

      assert {:ok, ["frame"], _decoder} =
               Arbor.RPC.Framing.push(Arbor.RPC.Framing.new(), "frame\n")
    after
      File.rename!(hidden, path)
    end
  end

  test "stale or counterfeit Guardian identity cannot confirm or initiate cleanup" do
    {:ok, handle} = Subprocess.open(["/bin/sleep", "30"])
    [actor] = Subprocess.linked_processes(handle)
    assert {:error, :stale_generation} = Subprocess.close(%{handle | generation: make_ref()})
    guardian = :sys.get_state(actor).guardian
    parent = self()

    counterfeit =
      spawn(fn ->
        Process.put({Arbor.RPC.Subprocess.Guardian, :generation}, Subprocess.identity(handle))
        Process.put(:"$initial_call", {Arbor.RPC.Subprocess.Guardian, :init, 1})
        send(parent, :counterfeit_ready)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :counterfeit_ready

    assert {:error, :invalid_guardian_identity} =
             Subprocess.cleanup_receipt(%{handle | guardian: counterfeit})

    assert Process.alive?(counterfeit)
    assert Process.alive?(guardian)
    assert :ok = Subprocess.close(handle)
    send(counterfeit, :stop)
  end

  defp alive?(pid) do
    case System.cmd("/bin/kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          eventually(fun, attempts - 1)
        )
  end

  defp eventually(_fun, 0), do: flunk("condition did not become true")
end
