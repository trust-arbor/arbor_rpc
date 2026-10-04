defmodule Arbor.RPC.FramingTest do
  use ExUnit.Case, async: true

  alias Arbor.RPC.Framing

  test "a chunk may contain many valid frames larger than the per-frame limit in total" do
    state = Framing.new(max_frame_bytes: 4)
    assert {:ok, ["abcd", "1234", "", "xy"], next} = Framing.push(state, "abcd\n1234\n\nxy\n")
    assert {:ok, [], ^next} = Framing.push(next, "")
  end

  test "the byte limit is enforced across chunk boundaries for complete and unfinished frames" do
    state = Framing.new(max_frame_bytes: 4)
    assert {:ok, [], partial} = Framing.push(state, "abc")
    assert {:ok, ["abcd"], _} = Framing.push(partial, "d\n")
    assert {:error, :frame_too_large} = Framing.push(partial, "de\n")
    assert {:error, :frame_too_large} = Framing.push(partial, "de")
    assert {:error, :frame_too_large} = Framing.push(state, "abcd\n12345\n")
  end

  test "all byte chunkings of a Unicode stream preserve frames, CR and BOM" do
    frames = [<<239, 187, 191>> <> "λ", "😀\r", "", "最後"]
    data = Enum.join(frames, "\n") <> "\n"

    for offset <- 0..byte_size(data) do
      <<first::binary-size(offset), second::binary>> = data
      assert {:ok, a, partial} = Framing.push(Framing.new(max_frame_bytes: 8), first)
      assert {:ok, b, _} = Framing.push(partial, second)
      assert a ++ b == frames
    end

    assert {:error, :frame_too_large} = Framing.push(Framing.new(max_frame_bytes: 3), "😀\n")
  end

  test "remainder after earlier frames joins exactly the next frame" do
    state = Framing.new(max_frame_bytes: 6)
    assert {:ok, ["first"], partial} = Framing.push(state, ["first\n", ["sec"]])
    assert {:ok, ["second", "third"], next} = Framing.push(partial, "ond\nthird\nlast")
    assert {:ok, ["last"], _} = Framing.push(next, "\n")
  end

  test "invalid limits fail during configuration" do
    for value <- [0, -1, :infinity, "4", nil] do
      assert_raise ArgumentError, fn -> Framing.new(max_frame_bytes: value) end
    end
  end
end
