defmodule Arbor.RPC.FramedStream do
  @moduledoc """
  Neutral newline frames from an owned `Arbor.RPC.Subprocess`.

  `next/2` drains buffered frames before waiting. Its monotonic deadline and
  caller monitor prevent a timed-out or dead reader from taking a later frame.
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

  @spec subscribe(Subprocess.t(), pid(), keyword()) :: :ok | {:error, term()}
  def subscribe(handle, consumer, opts \\ []),
    do: Subprocess.call(handle, {:subscribe, consumer, Keyword.get(opts, :window, 1)})

  @spec ack(Subprocess.t(), reference()) :: :ok | {:error, term()}
  def ack(handle, token), do: Subprocess.call(handle, {:ack, token})
end
