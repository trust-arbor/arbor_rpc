defmodule Arbor.RPC.NativeProtocolTest do
  use ExUnit.Case, async: true

  alias Arbor.RPC.Subprocess.{NativeProtocol, Receipt}

  test "version, generation, length, frame sequence and typed write results are validated" do
    token = 123

    assert {:ok, {:started, ^token, 42, 42, true, 0}} =
             NativeProtocol.decode(<<1, ?S, token::64, 42::32, 42::32, 1, 0::32>>, nil)

    assert {:ok, {:data, 1, <<0, 255, 13, 10>>}} =
             NativeProtocol.decode(<<1, ?D, token::64, 1::64, 0, 255, 13, 10>>, token)

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(<<1, ?D, token::64, 0::64, "x">>, token)

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(<<1, ?D, token::64, 1::64, "x">>, token + 1)

    assert {:error, {:unsupported_helper_protocol, 2}} = NativeProtocol.decode(<<2, ?D>>, token)
    assert {:error, :invalid_helper_packet} = NativeProtocol.decode(<<1, ?R>>, token)

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(
               <<1, ?D, token::64, 1::64, :binary.copy("x", 16_385)::binary>>,
               token
             )

    assert {:ok, {:write_result, 7, {:error, :backpressure}}} =
             NativeProtocol.decode(<<1, ?W, token::64, 7::64, 1>>, token)

    assert NativeProtocol.command(:credit, token, 9) == <<1, ?A, token::64, 9::64>>

    assert NativeProtocol.command(:write, token, 7, "literal\n") ==
             <<1, ?I, token::64, 7::64, "literal\n">>
  end

  test "vendor exit, stdout EOF and cleanup are distinct events" do
    token = 99

    assert {:ok, {:vendor_exit, 137}} =
             NativeProtocol.decode(<<1, ?O, token::64, 137::32>>, token)

    assert {:ok, :stdout_eof} = NativeProtocol.decode(<<1, ?F, token::64>>, token)

    assert {:ok, {:cleanup, true, false, 137, 2, 42}} =
             NativeProtocol.decode(
               <<1, ?R, token::64, 1, 0, 137::signed-32, 2::32, 42::32>>,
               token
             )

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(
               <<1, ?R, token::64, 1, 1, -1::signed-32, 0::32, 42::32>>,
               token
             )

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(<<1, ?R, token::64, 0, 1, 7::signed-32, 0::32, 42::32>>, token)

    assert {:error, :invalid_helper_packet} =
             NativeProtocol.decode(<<1, ?O, token::64, 256::32>>, token)
  end

  test "receipt success requires actual reaping and requested group absence" do
    base = %Receipt{
      generation: make_ref(),
      direct_child: :reaped,
      targeted_group: :not_requested,
      completed_at: 0
    }

    assert Receipt.result(base) == :ok
    assert base.descendant_tree == :not_contained
    assert Receipt.result(%{base | targeted_group: :absent}) == :ok

    assert Receipt.result(%{base | targeted_group: :unconfirmed}) ==
             {:error, :targeted_group_cleanup_unconfirmed}

    assert Receipt.result(%{base | direct_child: :unconfirmed}) == {:error, :cleanup_unconfirmed}

    assert Receipt.result(%{base | reason: :helper_receipt_lost}) ==
             {:error, :helper_receipt_lost}

    assert Receipt.result(Receipt.unconfirmed(make_ref(), true, :cleanup_status_unavailable)) ==
             {:error, :cleanup_status_unavailable}
  end
end
