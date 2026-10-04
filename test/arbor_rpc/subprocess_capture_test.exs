defmodule Arbor.RPC.SubprocessCaptureTest do
  use ExUnit.Case, async: false

  alias Arbor.RPC.Subprocess

  setup do
    directory =
      Path.join(System.tmp_dir!(), "arbor-capture-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    {physical_directory, 0} = System.cmd("/bin/pwd", ["-P"], cd: directory)
    directory = String.trim(physical_directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, directory: directory}
  end

  test "preserves original bytes, line endings, empty frames and unfinished output" do
    output = <<239, 187, 191>> <> "☃\r\n\nfinal" <> <<255>>

    assert {:ok, ^output, 0} =
             Subprocess.capture(["/usr/bin/printf", "%s", output],
               max_output_bytes: byte_size(output)
             )

    assert {:ok, "", 0} = Subprocess.capture(["/usr/bin/printf", ""])
  end

  test "returns nonzero exit output without treating it as a transport failure" do
    assert {:ok, "out\nerr\nfragment", 7} =
             Subprocess.capture(
               ["/bin/sh", "-c", "printf 'out\\n'; printf 'err\\n' >&2; printf fragment; exit 7"],
               stderr_to_stdout: true
             )
  end

  test "executes exact argv once and does not evaluate shell text", %{directory: directory} do
    marker = Path.join(directory, "executions")

    assert {:ok, "$(printf unsafe)\nspace value\n", 0} =
             Subprocess.capture([
               "/bin/sh",
               "-c",
               "printf x >> \"$1\"; shift; printf '%s\\n' \"$@\"",
               "fixture",
               marker,
               "$(printf unsafe)",
               "space value"
             ])

    assert File.read!(marker) == "x"
  end

  test "bounds output including newlines and an unfinished line" do
    for output <- ["ab\ncd\n", "\n\n\n\n\n\n", "abcdef"] do
      assert {:error, :output_too_large} =
               Subprocess.capture(["/usr/bin/printf", "%s", output], max_output_bytes: 5)
    end

    assert {:ok, "12345", 0} =
             Subprocess.capture(["/usr/bin/printf", "12345"], max_output_bytes: 5)
  end

  test "uses one deadline even while complete frames continue arriving", %{directory: directory} do
    marker = Path.join(directory, "pid")
    started = System.monotonic_time(:millisecond)

    assert {:error, :timeout} =
             Subprocess.capture(
               [
                 "/bin/sh",
                 "-c",
                 "echo $$ > \"$1\"; while :; do printf 'line\\n'; sleep 0.02; done",
                 "fixture",
                 marker
               ],
               timeout: 100,
               cleanup_timeout: 200,
               term_grace: 50
             )

    assert System.monotonic_time(:millisecond) - started < 1_500
    pid = marker |> File.read!() |> String.trim() |> String.to_integer()
    eventually(fn -> not alive?(pid) end)
  end

  test "a child that closes stdout but keeps running is killed on deadline", %{
    directory: directory
  } do
    marker = Path.join(directory, "pid")

    assert {:error, :timeout} =
             Subprocess.capture(
               ["/bin/sh", "-c", "echo $$ > \"$1\"; exec 1>&-; exec sleep 30", "fixture", marker],
               timeout: 100,
               cleanup_timeout: 200,
               term_grace: 50
             )

    pid = marker |> File.read!() |> String.trim() |> String.to_integer()
    eventually(fn -> not alive?(pid) end)
  end

  test "capturing owner death reaps the command even with a supplied different owner", %{
    directory: directory
  } do
    marker = Path.join(directory, "pid")
    other_owner = self()

    owner =
      spawn(fn ->
        Subprocess.capture(
          ["/bin/sh", "-c", "echo $$ > \"$1\"; exec sleep 30", "fixture", marker],
          owner: other_owner,
          timeout: 30_000
        )
      end)

    eventually(fn -> File.exists?(marker) end)
    pid = marker |> File.read!() |> String.trim() |> String.to_integer()
    assert alive?(pid)
    Process.exit(owner, :kill)
    eventually(fn -> not alive?(pid) end)
    assert Process.alive?(other_owner)
  end

  test "honors effective child PATH, cwd, environment unsets and isolation", %{
    directory: directory
  } do
    fixture = Path.join(directory, "fixture")

    File.write!(
      fixture,
      "#!/bin/sh\nprintf '%s|%s|%s' \"$PWD\" \"$CAPTURE_EXPLICIT\" \"${CAPTURE_SECRET-unset}\"\n"
    )

    File.chmod!(fixture, 0o755)
    original = System.get_env("CAPTURE_SECRET")
    System.put_env("CAPTURE_SECRET", "host-secret")

    on_exit(fn ->
      if original,
        do: System.put_env("CAPTURE_SECRET", original),
        else: System.delete_env("CAPTURE_SECRET")
    end)

    assert {:ok, output, 0} =
             Subprocess.capture(["fixture"],
               cd: directory,
               env: %{"PATH" => ".", "CAPTURE_EXPLICIT" => "literal", "CAPTURE_SECRET" => false}
             )

    assert output == "#{directory}|literal|unset"

    assert {:error, {:executable_not_found, "fixture"}} =
             Subprocess.capture(["fixture"], cd: directory, env: %{"PATH" => false})
  end

  test "counts LF across separate chunks and distinguishes frame-count pressure" do
    assert {:error, :output_too_large} =
             Subprocess.capture(
               ["/bin/sh", "-c", "printf 'ab\\n'; sleep 0.02; printf '\\n'"],
               max_output_bytes: 3
             )

    assert {:error, :queue_frame_limit} =
             Subprocess.capture(["/usr/bin/printf", "a\nb\n"],
               max_output_bytes: 10,
               max_queue_frames: 1
             )
  end

  test "close timeout reaps the exclusive actor and child while its capturing owner survives", %{
    directory: directory
  } do
    marker = Path.join(directory, "blocked-actor-pid")
    parent = self()

    worker =
      spawn(fn ->
        result =
          Subprocess.capture(
            ["/bin/sh", "-c", "echo $$ > \"$1\"; exec sleep 30", "fixture", marker],
            timeout: 300,
            cleanup_timeout: 200,
            term_grace: 50
          )

        send(parent, {:capture_result, result})

        receive do
          :stop -> :ok
        end
      end)

    eventually(fn -> File.exists?(marker) end)
    pid = marker |> File.read!() |> String.trim() |> String.to_integer()

    actor =
      Enum.find(Process.list(), fn process ->
        case Process.info(process, :dictionary) do
          {:dictionary, dictionary} ->
            List.keyfind(dictionary, {Arbor.RPC.Subprocess.Actor, :owner}, 0) ==
              {{Arbor.RPC.Subprocess.Actor, :owner}, worker}

          nil ->
            false
        end
      end)

    assert is_pid(actor)
    :ok = :sys.suspend(actor)
    monitor = Process.monitor(actor)

    on_exit(fn ->
      if Process.alive?(actor), do: Process.exit(actor, :kill)
      if Process.alive?(worker), do: Process.exit(worker, :kill)
    end)

    assert_receive {:capture_result, {:error, {:cleanup_failed, :timeout, :timeout}}}, 2_000
    assert Process.alive?(worker)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}, 1_000
    eventually(fn -> not alive?(pid) end)
    assert Process.alive?(worker)
    send(worker, :stop)
  end

  test "validates finite limits before creating a command" do
    for key <- [:timeout, :max_output_bytes], value <- [0, -1, :infinity, nil] do
      assert {:error, {:invalid_limit, ^key}} =
               Subprocess.capture(["missing-command"], [{key, value}])
    end

    assert {:error, :invalid_options} = Subprocess.capture(["missing-command"], :invalid)
    assert {:error, :invalid_options} = Subprocess.capture(["missing-command"], ["invalid"])
  end

  defp alive?(pid) do
    case System.cmd("/bin/kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  end

  defp eventually(predicate, attempts \\ 100)

  defp eventually(predicate, attempts) when attempts > 0 do
    if predicate.() do
      :ok
    else
      Process.sleep(10)
      eventually(predicate, attempts - 1)
    end
  end

  defp eventually(_predicate, 0), do: flunk("condition did not become true")
end
