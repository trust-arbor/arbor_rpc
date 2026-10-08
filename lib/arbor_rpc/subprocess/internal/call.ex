defmodule Arbor.RPC.Subprocess.Internal.Call do
  @moduledoc false
  alias Arbor.RPC.Subprocess
  alias Arbor.RPC.Subprocess.Actor
  alias Arbor.RPC.Subprocess.WriteAdmission

  def validate_actor(%Subprocess{pid: pid, generation: generation}) when is_pid(pid) do
    if node(pid) == node() do
      case Process.info(pid, [:initial_call, :dictionary]) do
        nil ->
          {:error, :closed}

        [initial_call: {:proc_lib, :init_p, 5}, dictionary: dictionary] ->
          validate_actor_dictionary(dictionary, generation)

        _other_process ->
          {:error, :invalid_handle}
      end
    else
      {:error, :remote_handle_not_supported}
    end
  end

  def validate_actor(_handle), do: {:error, :invalid_handle}

  defp validate_actor_dictionary(dictionary, generation) do
    case List.keyfind(dictionary, :"$initial_call", 0) do
      {:"$initial_call", {Actor, :init, 1}} ->
        expected = {{Actor, :generation}, generation}

        if List.keyfind(dictionary, {Actor, :generation}, 0) == expected,
          do: :ok,
          else: {:error, :stale_generation}

      _ ->
        {:error, :invalid_handle}
    end
  end

  @spec call(Subprocess.t(), term(), timeout() | nil) :: term()
  def call(handle, request, timeout \\ nil)

  def call(%Subprocess{cleanup_timeout: budget} = handle, {:write, data}, timeout) do
    timeout = if is_nil(timeout), do: budget + 1000, else: timeout

    deadline =
      if timeout == :infinity, do: :infinity, else: System.monotonic_time(:millisecond) + timeout

    with :ok <- validate_actor(handle),
         {:ok, table} <- write_table(handle) do
      WriteAdmission.submit(table, handle.pid, handle.generation, data, deadline)
    end
  end

  def call(
        %Subprocess{pid: pid, generation: generation, cleanup_timeout: budget},
        request,
        timeout
      ) do
    timeout = if is_nil(timeout), do: budget + 1000, else: timeout

    if node(pid) == node(),
      do: GenServer.call(pid, {generation, request}, timeout),
      else: {:error, :remote_handle_not_supported}
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :closed}
  end

  defp write_table(handle) do
    case Process.info(handle.pid, :dictionary) do
      {:dictionary, dictionary} ->
        case List.keyfind(dictionary, {Actor, :writes}, 0) do
          {{Actor, :writes}, table} -> {:ok, table}
          _ -> {:error, :closed}
        end

      nil ->
        {:error, :closed}
    end
  end
end
