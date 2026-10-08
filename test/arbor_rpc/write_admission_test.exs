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

  test "a reserved drain snapshot executes the subsequently published binary exactly once" do
    generation = make_ref()
    deadline = System.monotonic_time(:millisecond) + 1000
    first_token = make_ref()
    second_token = make_ref()
    first_reply = :erlang.alias()
    second_reply = :erlang.alias()
    first_payload = "first\n"
    second_payload = "published\n"

    table =
      WriteAdmission.new(generation, %{
        max_pending_writes: 2,
        max_pending_write_bytes: 65_536,
        max_write_bytes: 128
      })

    first_phase = :atomics.new(2, signed: true)
    second_phase = :atomics.new(2, signed: true)
    :atomics.put(first_phase, 1, 0)
    :atomics.put(second_phase, 1, -1)

    reserved = fn phase, reply, order ->
      %{
        producer: self(),
        reply: reply,
        deadline: deadline,
        phase: phase,
        data: nil,
        bytes: 0,
        order: order
      }
    end

    first = reserved.(first_phase, first_reply, 1)
    second = reserved.(second_phase, second_reply, 2)

    first_bytes =
      byte_size(first_payload) +
        :erlang.external_size({first_token, %{first | bytes: 9_223_372_036_854_775_807}})

    second_bytes =
      byte_size(second_payload) +
        :erlang.external_size({second_token, %{second | bytes: 9_223_372_036_854_775_807}})

    entries = %{
      first_token => %{first | data: first_payload, bytes: first_bytes},
      second_token => %{second | bytes: second_bytes}
    }

    true = :ets.insert(table, {:budget, 2, first_bytes + second_bytes, false, false, entries})

    try do
      execute = fn data, original_deadline, {^table, token} = context ->
        assert original_deadline == deadline
        assert WriteAdmission.current?(context, original_deadline)

        case token do
          ^first_token ->
            assert data == first_payload
            [{:budget, count, bytes, sealed, wake, current}] = :ets.lookup(table, :budget)
            unpublished = Map.fetch!(current, second_token)
            assert unpublished.data == nil
            assert unpublished.phase == second_phase
            assert unpublished.deadline == deadline
            assert unpublished.producer == self()

            true =
              :ets.insert(
                table,
                {:budget, count, bytes, sealed, wake,
                 Map.put(current, second_token, %{unpublished | data: second_payload})}
              )

            assert :atomics.compare_exchange(second_phase, 1, -1, 0) == :ok

          ^second_token ->
            assert data == second_payload
        end

        send(self(), {:executed_write, token, data, original_deadline})
        :ok
      end

      assert :ok = WriteAdmission.drain(table, execute)
      assert_receive {:executed_write, ^first_token, ^first_payload, ^deadline}, 0
      assert_receive {:executed_write, ^second_token, ^second_payload, ^deadline}, 0
      assert_receive {:arbor_rpc_write, ^first_token, :ok}, 0
      assert_receive {:arbor_rpc_write, ^second_token, :ok}, 0
      refute_receive {:executed_write, _, _, _}, 0
      refute_receive {:arbor_rpc_write, _, _}, 0

      assert WriteAdmission.stats(table) == %{
               pending_writes: 0,
               pending_write_bytes: 0,
               writes_sealed: false,
               uncertain_writes: 0
             }

      assert :atomics.get(first_phase, 1) == 2
      assert :atomics.get(second_phase, 1) == 2
      assert [{:budget, 0, 0, false, false, %{}}] = :ets.lookup(table, :budget)
    after
      :erlang.unalias(first_reply)
      :erlang.unalias(second_reply)
      :ets.delete(table)
    end
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
          result =
            Arbor.RPC.Subprocess.Internal.Call.call(
              handle,
              {:write, :binary.copy(<<id>>, 1_048_576)},
              500
            )

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
      for _ <- 1..2,
          do:
            spawn(fn ->
              Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "queued"}, 5000)
            end)

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
        do:
          assert(
            {:error, :timeout} =
              Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "no late write"}, 2)
          )

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
      spawn(fn ->
        send(
          parent,
          {:detached, Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, small}, 500)}
        )
      end)

    eventually(fn -> WriteAdmission.stats(table).pending_writes == 1 end)
    [entry] = entries(table)
    assert entry.data == small
    assert :binary.referenced_byte_size(entry.data) == 128

    assert {:error, :write_too_large} =
             Subprocess.write(%{handle | max_write_bytes: 1_000_000}, source)

    assert {:error, :invalid_iodata} =
             Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, [:invalid]})

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
        send(
          parent,
          {:expired, Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "expired\n"}, 100)}
        )
      end)

    eventually(fn -> Enum.any?(entries(table), &(:atomics.get(&1.phase, 1) == 1)) end)
    assert_receive {:expired, {:error, :timeout}}, 1000
    assert_receive {:DOWN, ^monitor, :process, ^producer, :normal}, 1000
    eventually(fn -> WriteAdmission.stats(table).uncertain_writes == 1 end)

    assert {:error, :backpressure} =
             Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "still charged"}, 100)

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

    producer =
      spawn(fn ->
        Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "abandoned\n"}, 1000)
      end)

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
        result = Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "committed\n"}, 500)
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
        send(
          parent,
          {:physical_expired,
           Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "physical\n"}, 200)}
        )
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

    producer =
      spawn(fn -> Arbor.RPC.Subprocess.Internal.Call.call(handle, {:write, "held"}, 1000) end)

    eventually_observed(
      "write entered execution before Actor kill",
      fn ->
        [{:budget, count, bytes, _sealed, _wake, pending}] = :ets.lookup(table, :budget)

        %{
          phases: pending |> Map.values() |> Enum.map(&:atomics.get(&1.phase, 1)) |> Enum.sort(),
          pending_writes: count,
          pending_write_bytes: bytes,
          producer: Process.info(producer, [:status, :current_function])
        }
      end,
      fn observation -> 1 in observation.phases end
    )

    Process.exit(actor, :kill)

    eventually_observed(
      "Actor-owned admission table deleted",
      fn -> %{table: :ets.info(table, :size), actor: Process.info(actor, :status)} end,
      fn observation -> observation.table == :undefined end
    )

    assert {:error, :guardian_timeout} = Subprocess.cleanup_receipt(handle)
    :ok = :sys.resume(guardian)

    eventually_observed(
      "write producer terminated after Actor DOWN",
      fn -> Process.info(producer, [:status, :current_function]) end,
      &is_nil/1
    )

    eventually_observed(
      "actual Guardian receipt certifies native cleanup after Actor DOWN",
      fn ->
        %{
          receipt: Subprocess.cleanup_receipt(handle),
          guardian: Process.info(guardian, :status)
        }
      end,
      fn
        %{
          receipt: {:ok, %Receipt{direct_child: :reaped, targeted_group: group, helper_status: 0}}
        }
        when group in [:absent, :not_requested] ->
          true

        _ ->
          false
      end
    )

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

  defp eventually_observed(label, observe, accepted),
    do: eventually_observed(label, observe, accepted, System.monotonic_time(:millisecond) + 2000)

  defp eventually_observed(label, observe, accepted, deadline) do
    observation = observe.()

    if accepted.(observation) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline,
             "#{label} did not become true; last observation: " <>
               inspect(observation, limit: 24, printable_limit: 128)

      Process.sleep(2)
      eventually_observed(label, observe, accepted, deadline)
    end
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
