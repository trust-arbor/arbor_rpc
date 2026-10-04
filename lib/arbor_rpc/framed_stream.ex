defmodule Arbor.RPC.FramedStream do
  @moduledoc """
  Neutral newline frames from an owned `Arbor.RPC.Subprocess`.

  `next/2` drains buffered frames before waiting. Its monotonic deadline and
  caller monitor prevent a timed-out or dead reader from taking a later frame.
  `next_until/3` preserves one absolute deadline across protocol-filter retries;
  an expired deadline never becomes a zero-time poll of buffered data.
  Handles, readers and subscribers must be local to the child actor's node.
  Pull readers and a push subscriber are mutually exclusive.

  Push delivery is `{:arbor_rpc, generation, {:frame, token, bytes}}`. At most
  `window` frames are unacknowledged. Only the subscriber can `ack/2` a token;
  duplicate, foreign and stale tokens are rejected. After natural exit or
  pressure failure, buffered frames drain before one
  `{:arbor_rpc, generation, {:closed, reason, remainder}}`
  event reports termination and the unfinished frame's original bytes. EOF is
  protocol neutral: the caller decides whether that remainder is a valid final
  message. Explicit close or owner death aborts the drain; a finite closed
  retention lease reports `:drain_timeout` for an abandoned drain. A subscriber's
  death closes the child rather than continuing delivery into a dead mailbox.
  """

  alias Arbor.RPC.Subprocess

  @spec next(Subprocess.t(), timeout()) ::
          {:ok, binary()} | {:closed, term(), binary()} | {:error, term()}
  def next(handle, timeout \\ :infinity)

  def next(handle, :infinity),
    do: Subprocess.call(handle, {:next, :infinity, :infinity, false}, :infinity)

  def next(handle, timeout) when is_integer(timeout) and timeout >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout
    # The small call allowance is for actor scheduling/reply delivery, not an
    # extension of the frame deadline. Zero only drains prebuffered frames.
    Subprocess.call(handle, {:next, deadline, deadline + 10, timeout == 0}, max(timeout, 1) + 10)
  end

  def next(_handle, _timeout), do: {:error, :invalid_timeout}

  @doc """
  Reads using an existing monotonic deadline in milliseconds, or `:infinity`.

  Use the same deadline while filtering protocol noise. Once it expires,
  buffered frames remain available for a subsequent read; the scheduling
  allowance does not extend the frame deadline or enable zero-poll semantics.

  For an explicit zero-time poll, pass its original monotonic cutoff and
  `buffered_only: true` on every filter retry. Each attempt has a fresh finite
  scheduling lease, but only frames buffered before that original cutoff may
  be consumed. This option requires a finite cutoff.
  """
  @spec next_until(Subprocess.t(), integer() | :infinity, keyword()) ::
          {:ok, binary()} | {:closed, term(), binary()} | {:error, term()}
  def next_until(handle, deadline, opts \\ []) do
    if is_list(opts) and Keyword.keyword?(opts) do
      case Keyword.get(opts, :buffered_only, false) do
        flag when is_boolean(flag) -> read_until(handle, deadline, flag)
        _invalid -> {:error, :invalid_buffered_only}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp read_until(handle, :infinity, false), do: next(handle, :infinity)

  defp read_until(handle, cutoff, true) when is_integer(cutoff) do
    acceptance = System.monotonic_time(:millisecond) + 10
    Subprocess.call(handle, {:next, cutoff, acceptance, true}, 11)
  end

  defp read_until(handle, deadline, false) when is_integer(deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      Subprocess.call(handle, {:next, deadline, deadline + 10, false}, remaining + 10)
    else
      {:error, :timeout}
    end
  end

  defp read_until(_handle, _deadline, _buffered_only), do: {:error, :invalid_deadline}

  @spec subscribe(Subprocess.t(), pid(), keyword()) :: :ok | {:error, term()}
  def subscribe(handle, consumer, opts \\ []),
    do: Subprocess.call(handle, {:subscribe, consumer, Keyword.get(opts, :window, 1)})

  @spec ack(Subprocess.t(), reference()) :: :ok | {:error, term()}
  def ack(handle, token), do: Subprocess.call(handle, {:ack, token})
end
