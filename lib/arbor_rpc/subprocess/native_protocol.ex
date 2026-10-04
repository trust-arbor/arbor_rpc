defmodule Arbor.RPC.Subprocess.NativeProtocol do
  @moduledoc false
  @version 1

  @type event ::
          {:started, non_neg_integer(), pos_integer(), non_neg_integer(), boolean(),
           non_neg_integer()}
          | {:data, pos_integer(), binary()}
          | {:vendor_exit, non_neg_integer()}
          | :stdout_eof
          | {:write_result, pos_integer(), :ok | {:error, term()}}
          | {:cleanup, boolean(), boolean(), integer(), non_neg_integer(), pos_integer()}
          | {:protocol_error, non_neg_integer()}

  @spec decode(binary(), non_neg_integer() | nil) :: {:ok, event()} | {:error, term()}
  def decode(
        <<@version, ?S, token::64, child::32, pgid::32, group, error::32>>,
        nil
      )
      when child > 0 and group in [0, 1],
      do: {:ok, {:started, token, child, pgid, group == 1, error}}

  def decode(<<@version, ?D, token::64, sequence::64, bytes::binary>>, token)
      when sequence > 0 and byte_size(bytes) in 1..16_384,
      do: {:ok, {:data, sequence, bytes}}

  def decode(<<@version, ?O, token::64, status::32>>, token) when status <= 255,
    do: {:ok, {:vendor_exit, status}}

  def decode(<<@version, ?F, token::64>>, token), do: {:ok, :stdout_eof}

  def decode(<<@version, ?W, token::64, sequence::64, result>>, token)
      when sequence > 0 and result in 0..2,
      do: {:ok, {:write_result, sequence, write_result(result)}}

  def decode(
        <<@version, ?R, token::64, direct, scope, status::signed-32, signals::32, child::32>>,
        token
      )
      when direct in [0, 1] and scope in [0, 1] and child > 0 and status in -1..255 and
             (direct == 0 or status >= 0) and (scope == 0 or direct == 1) and signals <= 2,
      do: {:ok, {:cleanup, direct == 1, scope == 1, status, signals, child}}

  def decode(<<@version, ?E, token::64, reason>>, token),
    do: {:ok, {:protocol_error, reason}}

  def decode(<<version, _::binary>>, _token) when version != @version,
    do: {:error, {:unsupported_helper_protocol, version}}

  def decode(_packet, _token), do: {:error, :invalid_helper_packet}

  @spec command(atom(), non_neg_integer(), non_neg_integer(), binary()) :: binary()
  def command(type, token, sequence \\ 0, bytes \\ <<>>)
  def command(:credit, token, sequence, <<>>), do: <<@version, ?A, token::64, sequence::64>>

  def command(:write, token, sequence, bytes),
    do: <<@version, ?I, token::64, sequence::64, bytes::binary>>

  def command(:close, token, 0, <<>>), do: <<@version, ?C, token::64>>
  def command(:quit, token, 0, <<>>), do: <<@version, ?Q, token::64>>
  def command(:heartbeat, token, 0, <<>>), do: <<@version, ?H, token::64>>

  defp write_result(0), do: :ok
  defp write_result(1), do: {:error, :backpressure}
  defp write_result(2), do: {:error, :closed}
end
