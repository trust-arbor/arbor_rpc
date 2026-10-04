defmodule Arbor.RPC.Subprocess.WriteAdmission do
  @moduledoc false

  # Independent monotonic cells fence payload publication, physical submission
  # and abandonment even when a budget CAS has to be retried by maintenance.
  # -1: reserved, 0: published, 1: executing, 2: conclusive, 3: abandoned,
  # 4: uncertain physical outcome. Only 2/3 can release retained credit.
  @attempts 64
  @maintenance_count 32
  @maintenance_ms 5

  def new(generation, limits) do
    table = :ets.new(__MODULE__, [:set, :public, read_concurrency: true, write_concurrency: true])
    :ets.insert(table, {:budget, 0, 0, false, false, %{}})

    :ets.insert(
      table,
      {:limits, generation, limits.max_pending_writes, limits.max_pending_write_bytes,
       limits.max_write_bytes}
    )

    table
  end

  def submit(table, actor, generation, data, deadline) do
    reply = :erlang.alias()
    monitor = Process.monitor(actor)
    token = make_ref()

    try do
      with {:ok, size} <- size(table, generation, data),
           {:ok, entry} <- reserve(table, token, reply, deadline, size),
           :ok <- publish(table, actor, token, entry, data, deadline) do
        await(table, token, reply, monitor, actor, deadline)
      end
    after
      :erlang.unalias(reply)
      abandon(table, token)
      Process.demonitor(monitor, [:flush])
      flush(token)
    end
  catch
    :error, :badarg -> {:error, :closed}
  end

  defp size(table, generation, data) do
    case :ets.lookup(table, :limits) do
      [{:limits, ^generation, _count, _bytes, maximum}] ->
        with {:ok, size} <- iodata_size(data) do
          if size <= maximum, do: {:ok, size}, else: {:error, :write_too_large}
        end

      _ ->
        {:error, :stale_generation}
    end
  catch
    :error, :badarg -> {:error, :closed}
  end

  defp iodata_size(data) do
    {:ok, :erlang.iolist_size(data)}
  catch
    :error, :badarg -> {:error, :invalid_iodata}
  end

  defp reserve(table, token, reply, deadline, size) do
    phase = :atomics.new(2, signed: true)
    :atomics.put(phase, 1, -1)

    entry = %{
      producer: self(),
      reply: reply,
      deadline: deadline,
      phase: phase,
      data: nil,
      bytes: size,
      order: System.unique_integer([:monotonic, :positive])
    }

    charge = size + :erlang.external_size({token, %{entry | bytes: 9_223_372_036_854_775_807}})
    entry = %{entry | bytes: charge}

    update(table, deadline, fn {:budget, count, bytes, sealed, wake, entries} ->
      [{:limits, _generation, count_limit, byte_limit, _maximum}] = :ets.lookup(table, :limits)

      cond do
        sealed ->
          {:error, :closed}

        count >= count_limit or bytes + charge > byte_limit ->
          {:error, :backpressure}

        true ->
          {:ok, {:budget, count + 1, bytes + charge, false, wake, Map.put(entries, token, entry)},
           entry}
      end
    end)
  end

  defp publish(table, actor, token, entry, data, deadline) do
    binary = IO.iodata_to_binary(data)

    binary =
      if :binary.referenced_byte_size(binary) > byte_size(binary),
        do: :binary.copy(binary),
        else: binary

    result =
      update(table, deadline, fn {:budget, count, bytes, sealed, wake, entries} ->
        cond do
          sealed ->
            {:error, :closed}

          not Map.has_key?(entries, token) ->
            {:error, :timeout}

          :atomics.get(entry.phase, 1) != -1 ->
            {:error, :timeout}

          true ->
            {:ok,
             {:budget, count, bytes, false, wake,
              Map.put(entries, token, %{entry | data: binary})}, :ok}
        end
      end)

    case result do
      {:ok, :ok} ->
        if not expired?(deadline) and :atomics.compare_exchange(entry.phase, 1, -1, 0) == :ok do
          wake(table, actor)
          :ok
        else
          abandon(table, token)
          {:error, :timeout}
        end

      error ->
        abandon(table, token)
        error
    end
  end

  defp await(table, token, reply, monitor, actor, deadline) do
    receive do
      {:arbor_rpc_write, ^token, result} ->
        if expired?(deadline), do: {:error, :timeout}, else: result

      {:DOWN, ^monitor, :process, ^actor, _reason} ->
        {:error, :closed}
    after
      remaining(deadline) ->
        :erlang.unalias(reply)
        abandon(table, token)
        {:error, :timeout}
    end
  end

  defp flush(token) do
    receive do
      {:arbor_rpc_write, ^token, _result} -> flush(token)
    after
      0 -> :ok
    end
  end

  def wake(table, actor) do
    case update(table, now() + @maintenance_ms, fn
           {:budget, count, bytes, sealed, false, entries} ->
             {:ok, {:budget, count, bytes, sealed, true, entries}, true}

           _budget ->
             {:ok, :unchanged, false}
         end) do
      {:ok, true} -> send(actor, :drain_writes)
      _ -> :ok
    end
  end

  def drain(table, execute, consume_wake \\ true) do
    _ = if consume_wake, do: clear_wake(table)
    deadline = now() + @maintenance_ms

    entries(table)
    |> Enum.filter(fn {_token, entry} -> :atomics.get(entry.phase, 1) in [-1, 0, 2, 3] end)
    |> Enum.sort_by(fn {_token, entry} -> entry.order end)
    |> Enum.take(@maintenance_count)
    |> Enum.reduce_while(:ok, fn {token, entry}, :ok ->
      if expired?(deadline) do
        {:halt, :ok}
      else
        drain_entry(table, token, entry, execute)
        {:cont, :ok}
      end
    end)
  end

  defp drain_entry(table, token, entry, execute) do
    _ =
      cond do
        :atomics.get(entry.phase, 1) in [2, 3] ->
          release(table, token)

        expired?(entry.deadline) or not Process.alive?(entry.producer) ->
          abandon(table, token)

        :atomics.compare_exchange(entry.phase, 1, 0, 1) == :ok ->
          result = execute.(entry.data, entry.deadline, {table, token})

          case result do
            {:uncertain, error} ->
              uncertain({table, token}, error)

            {:error, reason} = error
            when reason in [
                   :guardian_timeout,
                   :write_admission_timeout,
                   :cleanup_status_unavailable
                 ] ->
              uncertain({table, token}, error)

            result ->
              complete(table, token, result)
          end

        true ->
          :ok
      end

    :ok
  end

  def current?(nil, deadline), do: not expired?(deadline)

  def current?({table, token}, deadline) do
    case entry(table, token) do
      {:ok, entry} ->
        entry.deadline == deadline and not expired?(deadline) and
          :atomics.get(entry.phase, 1) == 1 and Process.alive?(entry.producer)

      _ ->
        false
    end
  catch
    :error, :badarg -> false
  end

  def complete(nil, _result), do: :ok
  def complete({table, token}, result), do: complete(table, token, result)

  def complete(table, token, result) do
    _ =
      with {:ok, entry} <- entry(table, token),
           true <- terminal(entry.phase) do
        _ = notify(entry, token, result)
        release(table, token)
      end

    :ok
  catch
    :error, :badarg -> :ok
  end

  defp notify(entry, token, result) do
    if :atomics.compare_exchange(entry.phase, 2, 0, 1) == :ok,
      do: send(entry.reply, {:arbor_rpc_write, token, result})
  end

  defp terminal(phase) do
    current = :atomics.get(phase, 1)
    current in [1, 4] and :atomics.compare_exchange(phase, 1, current, 2) == :ok
  end

  def uncertain(nil, _result), do: :ok

  def uncertain({table, token}, result) do
    _ =
      with {:ok, entry} <- entry(table, token) do
        _ = :atomics.compare_exchange(entry.phase, 1, 1, 4)
        notify(entry, token, result)
      end

    :ok
  catch
    :error, :badarg -> :ok
  end

  def abandon(table, token) do
    _ =
      with {:ok, entry} <- entry(table, token) do
        current = :atomics.get(entry.phase, 1)
        _ = if current in [-1, 0], do: :atomics.compare_exchange(entry.phase, 1, current, 3)
        release(table, token)
      end

    :ok
  catch
    :error, :badarg -> :ok
  end

  defp release(table, token) do
    update(table, now() + @maintenance_ms, fn {:budget, count, bytes, sealed, wake, entries} ->
      case Map.fetch(entries, token) do
        {:ok, entry} ->
          if :atomics.get(entry.phase, 1) in [2, 3] do
            {:ok,
             {:budget, count - 1, bytes - entry.bytes, sealed, wake, Map.delete(entries, token)},
             :ok}
          else
            {:ok, :unchanged, :ok}
          end

        :error ->
          {:ok, :unchanged, :ok}
      end
    end)
  end

  def seal(table) do
    _ =
      update(table, now() + @maintenance_ms, fn {:budget, count, bytes, _sealed, wake, entries} ->
        {:ok, {:budget, count, bytes, true, wake, entries}, :ok}
      end)

    Enum.each(entries(table), fn {token, entry} ->
      current = :atomics.get(entry.phase, 1)

      if current in [-1, 0] and :atomics.compare_exchange(entry.phase, 1, current, 3) == :ok do
        notify(entry, token, {:error, :closed})
        release(table, token)
      end
    end)

    :ok
  end

  def stats(table) do
    [{:budget, count, bytes, sealed, _wake, entries}] = :ets.lookup(table, :budget)

    %{
      pending_writes: count,
      pending_write_bytes: bytes,
      writes_sealed: sealed,
      uncertain_writes:
        Enum.count(entries, fn {_token, entry} -> :atomics.get(entry.phase, 1) == 4 end)
    }
  end

  defp clear_wake(table) do
    update(table, now() + @maintenance_ms, fn {:budget, count, bytes, sealed, _wake, entries} ->
      {:ok, {:budget, count, bytes, sealed, false, entries}, :ok}
    end)
  end

  defp entry(table, token) do
    case List.keyfind(:ets.lookup(table, :budget), :budget, 0) do
      {:budget, _count, _bytes, _sealed, _wake, entries} -> Map.fetch(entries, token)
      nil -> :error
    end
  end

  defp entries(table) do
    [{:budget, _count, _bytes, _sealed, _wake, entries}] = :ets.lookup(table, :budget)
    Map.to_list(entries)
  end

  defp update(table, deadline, operation), do: update(table, deadline, operation, @attempts)

  defp update(_table, _deadline, _operation, 0), do: {:error, :backpressure}

  defp update(table, deadline, operation, attempts) do
    if expired?(deadline) do
      {:error, :timeout}
    else
      case :ets.lookup(table, :budget) do
        [old] ->
          case operation.(old) do
            {:ok, :unchanged, result} ->
              {:ok, result}

            {:ok, replacement, result} ->
              if :ets.select_replace(table, [
                   {{:budget, :_, :_, :_, :_, :_}, [{:"=:=", :"$_", {:const, old}}],
                    [{:const, replacement}]}
                 ]) == 1,
                 do: {:ok, result},
                 else: update(table, deadline, operation, attempts - 1)

            error ->
              error
          end

        [] ->
          {:error, :closed}
      end
    end
  end

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(0, deadline - now())
  defp expired?(:infinity), do: false
  defp expired?(deadline), do: now() >= deadline
  defp now, do: System.monotonic_time(:millisecond)
end
