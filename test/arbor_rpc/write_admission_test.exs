defmodule Arbor.RPC.Subprocess.WriteAdmissionTest do
  use ExUnit.Case, async: false

  alias Arbor.RPC.{FramedStream, Subprocess}
  alias Arbor.RPC.Subprocess.{Receipt, WriteAdmission}

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "arbor_write_admission_#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    fixture = Path.join(directory, "vendor_fixture")
    source = Path.expand("../support/native/vendor_fixture.c", __DIR__)

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
                 fixture
               ],
               stderr_to_stdout: true
             )

    {:ok, vendor_fixture: fixture}
  end

  test "one aggregate byte cap precedes a suspended Actor mailbox and expires without late writes",
       context do
    handle = vendor(context.vendor_fixture)
    {actor, guardian, table} = addresses(handle)
    :ok = :sys.suspend(actor)
    parent = self()

    producers =
      for id <- 1..32 do
        spawn_monitor(fn ->
          result = Subprocess.call(handle, {:write, :binary.copy(<<id>>, 1_048_576)}, 500)
          send(parent, {:result, self(), result})
        end)
      end

    eventually(fn -> WriteAdmission.stats(table).pending_writes == 3 end)
    stats = WriteAdmission.stats(table)
    assert stats.pending_write_bytes <= 4_194_304
    assert stats.pending_write_bytes > 3 * 1_048_576
    {:messages, messages} = Process.info(actor, :messages)
    assert length(messages) <= 3
    assert Enum.sum(Enum.map(messages, &:erlang.external_size/1)) < 256

    results =
      for _ <- producers do
        receive do
          {:result, pid, result} ->
            {^pid, monitor} = List.keyfind(producers, pid, 0)
            assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 1000
            result
        after
          2000 -> flunk("producer did not return its original deadline result")
        end
      end

    assert Enum.count(results, &(&1 == {:error, :backpressure})) == 29
    assert Enum.count(results, &(&1 == {:error, :timeout})) == 3
    assert WriteAdmission.stats(table).pending_writes == 0
    :ok = :sys.resume(actor)
    assert :sys.get_state(guardian).write_sequence == 0
    close_proven(handle)
  end

  test "count admission, dead queued producers and repeated expiry cannot grow the owner mailbox" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_pending_writes: 2)
    {actor, guardian, table} = addresses(handle)
    :ok = :sys.suspend(actor)

    producers =
      for _ <- 1..2, do: spawn(fn -> Subprocess.call(handle, {:write, "queued"}, 5000) end)

    eventually(fn -> WriteAdmission.stats(table).pending_writes == 2 end)
    assert {:error, :backpressure} = Subprocess.write(handle, "excess")
    Enum.each(producers, &Process.exit(&1, :kill))
    # Caller death is not owner-side reaping; credit remains bounded and charged.
    assert WriteAdmission.stats(table).pending_writes == 2
    :ok = :sys.resume(actor)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    assert :sys.get_state(guardian).write_sequence == 0
    :ok = :sys.suspend(actor)

    for _ <- 1..40,
        do: assert({:error, :timeout} = Subprocess.call(handle, {:write, "no late write"}, 2))

    assert WriteAdmission.stats(table).pending_writes == 0
    {:messages, messages} = Process.info(actor, :messages)
    assert length(messages) <= 3
    :ok = :sys.resume(actor)
    assert :sys.get_state(guardian).write_sequence == 0
    assert :ok = Subprocess.write(handle, "fresh\n")
    assert {:ok, "fresh"} = FramedStream.next(handle, 1000)
    close_proven(handle)
  end

  test "admitted iodata detaches a backing binary and immutable caps apply to both write APIs" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_write_bytes: 128)
    {actor, _guardian, table} = addresses(handle)
    :ok = :sys.suspend(actor)
    source = :crypto.strong_rand_bytes(16_777_216)
    small = binary_part(source, 100, 128)
    assert :binary.referenced_byte_size(small) == byte_size(source)
    parent = self()

    producer =
      spawn(fn -> send(parent, {:detached, Subprocess.call(handle, {:write, small}, 500)}) end)

    eventually(fn -> WriteAdmission.stats(table).pending_writes == 1 end)
    [entry] = entries(table)
    assert entry.data == small
    assert :binary.referenced_byte_size(entry.data) == 128

    assert {:error, :write_too_large} =
             Subprocess.write(%{handle | max_write_bytes: 1_000_000}, source)

    assert {:error, :invalid_iodata} = Subprocess.call(handle, {:write, [:invalid]})
    Process.exit(producer, :kill)
    :ok = :sys.resume(actor)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    close_proven(handle)
  end

  test "a suspended Guardian cannot physically execute an expired write and uncertainty keeps credit" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_pending_writes: 1)
    {actor, guardian, table} = addresses(handle)
    :ok = :sys.suspend(guardian)
    parent = self()

    {producer, monitor} =
      spawn_monitor(fn ->
        send(parent, {:expired, Subprocess.call(handle, {:write, "expired\n"}, 100)})
      end)

    eventually(fn -> Enum.any?(entries(table), &(:atomics.get(&1.phase, 1) == 1)) end)
    assert_receive {:expired, {:error, :timeout}}, 1000
    assert_receive {:DOWN, ^monitor, :process, ^producer, :normal}, 1000
    eventually(fn -> WriteAdmission.stats(table).uncertain_writes == 1 end)
    assert {:error, :backpressure} = Subprocess.call(handle, {:write, "still charged"}, 100)
    :ok = :sys.resume(guardian)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    assert :sys.get_state(guardian).write_sequence == 0
    assert Process.alive?(actor)
    assert :ok = Subprocess.write(handle, "valid\n")
    assert {:ok, "valid"} = FramedStream.next(handle, 1000)
    close_proven(handle)
  end

  test "producer death during pending Guardian work does not release a physical liability prematurely" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_pending_writes: 1)
    {_actor, guardian, table} = addresses(handle)
    :ok = :sys.suspend(guardian)
    producer = spawn(fn -> Subprocess.call(handle, {:write, "abandoned\n"}, 1000) end)
    eventually(fn -> Enum.any?(entries(table), &(:atomics.get(&1.phase, 1) == 1)) end)
    Process.exit(producer, :kill)
    assert WriteAdmission.stats(table).pending_writes == 1
    assert {:error, :backpressure} = Subprocess.write(handle, "second")
    :ok = :sys.resume(guardian)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    assert :sys.get_state(guardian).write_sequence == 0
    assert :ok = Subprocess.write(handle, "owned\n")
    assert {:ok, "owned"} = FramedStream.next(handle, 1000)
    close_proven(handle)
  end

  test "a suspended caller rejects a queued late success without undoing an earlier native admission" do
    {:ok, handle} = Subprocess.open(["/bin/cat"])
    {actor, _guardian, table} = addresses(handle)
    :ok = :sys.suspend(actor)
    parent = self()

    producer =
      spawn(fn ->
        result = Subprocess.call(handle, {:write, "committed\n"}, 500)
        send(parent, {:late_result, result, Process.info(self(), :messages)})
      end)

    eventually(fn -> WriteAdmission.stats(table).pending_writes == 1 end)
    [entry] = entries(table)
    :erlang.suspend_process(producer)
    :ok = :sys.resume(actor)
    assert {:ok, "committed"} = FramedStream.next(handle, 1000)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    wait_past(entry.deadline)
    :erlang.resume_process(producer)
    assert_receive {:late_result, {:error, :timeout}, {:messages, []}}, 1000
    close_proven(handle)
  end

  test "an actual native ACK held past caller death retires uncertainty only when the ACK is processed" do
    {:ok, handle} = Subprocess.open(["/bin/cat"], max_pending_writes: 1)
    {_actor, guardian, table} = addresses(handle)
    parent = self()

    debug = fn
      :armed, {:in, {_port, {:data, <<1, ?W, _packet::binary>>}}}, _name ->
        send(parent, {:actual_write_ack_held, self()})

        receive do
          :release_write_ack -> :done
        end

      state, _event, _name ->
        state
    end

    :ok = :sys.install(guardian, {debug, :armed})

    on_exit(fn ->
      send(guardian, :release_write_ack)
      Subprocess.close(handle)
    end)

    {producer, monitor} =
      spawn_monitor(fn ->
        send(parent, {:physical_expired, Subprocess.call(handle, {:write, "physical\n"}, 200)})
      end)

    assert_receive {:actual_write_ack_held, ^guardian}, 1000
    assert WriteAdmission.stats(table).pending_writes == 1
    assert_receive {:physical_expired, {:error, :timeout}}, 1000
    assert_receive {:DOWN, ^monitor, :process, ^producer, :normal}, 1000
    eventually(fn -> WriteAdmission.stats(table).uncertain_writes == 1 end)
    assert {:error, :backpressure} = Subprocess.write(handle, "cannot reuse unsettled credit")
    send(guardian, :release_write_ack)
    eventually(fn -> WriteAdmission.stats(table).pending_writes == 0 end)
    assert {:ok, "physical"} = FramedStream.next(handle, 1000)
    assert :ok = Subprocess.write(handle, "next\n")
    assert {:ok, "next"} = FramedStream.next(handle, 1000)
    close_proven(handle)
  end

  test "Actor DOWN releases only BEAM admission rows; actual Guardian receipts certify native cleanup",
       context do
    handle = vendor(context.vendor_fixture)
    {actor, guardian, table} = addresses(handle)
    {:ok, sibling} = Subprocess.open(["/bin/cat"])
    :ok = :sys.suspend(guardian)
    producer = spawn(fn -> Subprocess.call(handle, {:write, "held"}, 1000) end)
    eventually(fn -> Enum.any?(entries(table), &(:atomics.get(&1.phase, 1) == 1)) end)
    Process.exit(actor, :kill)
    eventually(fn -> :ets.info(table) == :undefined end)
    assert {:error, :guardian_timeout} = Subprocess.cleanup_receipt(handle)
    :ok = :sys.resume(guardian)
    eventually(fn -> not Process.alive?(producer) end)
    proven_receipt(handle)
    assert :ok = Subprocess.write(sibling, "unrelated\n")
    assert {:ok, "unrelated"} = FramedStream.next(sibling, 1000)
    close_proven(sibling)
  end

  test "closed and foreign-generation handles cannot acquire a fresh write ledger" do
    {:ok, handle} = Subprocess.open(["/bin/cat"])

    assert {:error, :stale_generation} =
             Subprocess.write(%{handle | generation: make_ref()}, "bad")

    assert {:error, :invalid_handle} = Subprocess.write(%{handle | pid: self()}, "bad")
    assert :ok = Subprocess.close(handle)
    assert {:error, :closed} = Subprocess.write(handle, "late")
  end

  test "new write count and byte limits reject invalid configuration before child effects" do
    assert {:error, {:invalid_limit, :max_pending_writes}} =
             Subprocess.open(["/bin/cat"], max_pending_writes: 0)

    assert {:error, {:invalid_limit, :max_pending_write_bytes}} =
             Subprocess.open(["/bin/cat"], max_pending_write_bytes: -1)
  end

  defp vendor(fixture) do
    {:ok, handle} =
      Subprocess.open([fixture, "ignore-term"],
        process_group: true
      )

    assert {:ok, "ready"} = FramedStream.next(handle, 1000)
    handle
  end

  defp addresses(handle) do
    [actor] = Subprocess.linked_processes(handle)
    state = :sys.get_state(actor)

    on_exit(fn ->
      if Process.alive?(state.guardian), do: :sys.resume(state.guardian)
      if Process.alive?(actor), do: :sys.resume(actor)
      Subprocess.close(handle)
    end)

    {actor, state.guardian, state.writes}
  end

  defp entries(table) do
    [{:budget, _count, _bytes, _sealed, _wake, entries}] = :ets.lookup(table, :budget)
    Map.values(entries)
  end

  defp close_proven(handle) do
    assert :ok = Subprocess.close(handle)
    proven_receipt(handle)
  end

  defp proven_receipt(handle) do
    eventually(fn ->
      case Subprocess.cleanup_receipt(handle) do
        {:ok, %Receipt{direct_child: :reaped, targeted_group: group, helper_status: 0}}
        when group in [:absent, :not_requested] ->
          true

        _ ->
          false
      end
    end)
  end

  defp wait_past(deadline) do
    if System.monotonic_time(:millisecond) <= deadline,
      do: Process.sleep(max(1, deadline - System.monotonic_time(:millisecond) + 1))
  end

  defp eventually(predicate),
    do: eventually(predicate, System.monotonic_time(:millisecond) + 2000)

  defp eventually(predicate, deadline) do
    if predicate.() do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline, "condition did not become true"
      Process.sleep(2)
      eventually(predicate, deadline)
    end
  end
end
