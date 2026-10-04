defmodule Arbor.RPC.Framing do
  @moduledoc """
  A byte-bounded newline frame decoder independent of protocol validation.

  Limits apply to each complete or unfinished frame, excluding LF. Multiple
  valid frames in one chunk may exceed that limit in aggregate. Returned frames
  preserve their bytes, including a CR before LF and a UTF-8 BOM. The protocol
  wrapper owns blank lines, banners, BOM handling and JSON decoding, and must
  bound its delivery queue separately.
  """

  @enforce_keys [:max_frame_bytes]
  defstruct [:max_frame_bytes, remainder: ""]

  @opaque t :: %__MODULE__{max_frame_bytes: pos_integer(), remainder: binary()}

  @doc "Creates a decoder with a positive byte limit (default: 1 MiB)."
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    limit = Keyword.get(opts, :max_frame_bytes, 1_048_576)

    unless is_integer(limit) and limit > 0 do
      raise ArgumentError, "max_frame_bytes must be a positive integer"
    end

    %__MODULE__{max_frame_bytes: limit}
  end

  @doc "Decodes LF-delimited frames or rejects an oversized complete/partial frame."
  @spec push(t(), iodata()) :: {:ok, [binary()], t()} | {:error, :frame_too_large}
  def push(%__MODULE__{} = state, data) do
    decode(IO.iodata_to_binary(data), state.remainder, state.max_frame_bytes, [], state)
  end

  defp decode(data, remainder, limit, frames, state) do
    case :binary.match(data, "\n") do
      :nomatch ->
        if byte_size(remainder) + byte_size(data) <= limit do
          {:ok, Enum.reverse(frames), %{state | remainder: remainder <> data}}
        else
          {:error, :frame_too_large}
        end

      {offset, 1} when byte_size(remainder) + offset <= limit ->
        <<frame::binary-size(offset), "\n", rest::binary>> = data
        decode(rest, "", limit, [remainder <> frame | frames], state)

      {_offset, 1} ->
        {:error, :frame_too_large}
    end
  end
end
