defmodule Arbor.RPC.Subprocess.Capture do
  @moduledoc false

  alias Arbor.RPC.{FramedStream, Subprocess}

  @default_output_bytes 1_048_576
  @default_timeout 5_000

  def run(command, opts) do
    with :ok <- validate_options(opts),
         timeout = Keyword.get(opts, :timeout, @default_timeout),
         limit = Keyword.get(opts, :max_output_bytes, @default_output_bytes),
         deadline = System.monotonic_time(:millisecond) + timeout,
         {:ok, child} <- Subprocess.open(command, child_options(opts, limit)) do
      result = attempt_collect(child, deadline, limit)

      case Subprocess.close(child) do
        :ok ->
          result

        {:error, reason} ->
          # Capture owns this freshly opened actor exclusively. Do not leave an
          # unreachable child attached to a surviving caller after close fails.
          # Abrupt actor death invokes its guardian's bounded cleanup; the
          # original unconfirmed close result remains an error.
          [actor] = Subprocess.linked_processes(child)
          Process.exit(actor, :kill)
          {:error, {:cleanup_failed, result_reason(result), reason}}
      end
    end
  end

  defp attempt_collect(child, deadline, limit) do
    collect(child, deadline, limit, [], 0)
  rescue
    error -> {:error, {:capture_failed, error}}
  catch
    kind, reason -> {:error, {:capture_failed, kind, reason}}
  end

  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case Enum.find([:timeout, :max_output_bytes], fn key ->
             default = if key == :timeout, do: @default_timeout, else: @default_output_bytes
             value = Keyword.get(opts, key, default)
             not (is_integer(value) and value > 0)
           end) do
        nil -> :ok
        key -> {:error, {:invalid_limit, key}}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp validate_options(_opts), do: {:error, :invalid_options}

  defp child_options(opts, limit) do
    opts
    |> Keyword.drop([:timeout, :max_output_bytes])
    |> Keyword.put(:owner, self())
    |> Keyword.put(:max_frame_bytes, limit)
    |> Keyword.put(:max_queue_bytes, limit)
    |> Keyword.put(:max_input_chunk_bytes, limit)
  end

  defp collect(child, deadline, limit, chunks, size) do
    case FramedStream.next_until(child, deadline) do
      {:ok, frame} ->
        next_size = size + byte_size(frame) + 1

        if next_size <= limit,
          do: collect(child, deadline, limit, [[frame, "\n"] | chunks], next_size),
          else: {:error, :output_too_large}

      {:closed, {:exit_status, status}, remainder} ->
        if size + byte_size(remainder) <= limit,
          do: {:ok, IO.iodata_to_binary([Enum.reverse(chunks), remainder]), status},
          else: {:error, :output_too_large}

      {:closed, reason, _remainder} ->
        {:error, normalize_reason(reason)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_reason(reason)
       when reason in [:frame_too_large, :input_chunk_too_large, :queue_byte_limit],
       do: :output_too_large

  defp normalize_reason(reason), do: reason

  defp result_reason({:error, reason}), do: reason
  defp result_reason({:ok, _output, status}), do: {:exit_status, status}
end
