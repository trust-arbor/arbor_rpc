defmodule Arbor.RPC.SubprocessTest do
  use ExUnit.Case, async: false
  alias Arbor.RPC.{FramedStream, Subprocess}
  alias Arbor.RPC.Subprocess.{Cleanup, Command}

  @moduletag timeout: 10_000

  setup do
    directory = Path.join(System.tmp_dir!(), "arbor-rpc-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, directory: directory}
  end

  test "lookup uses the child's PATH and cd, with no host fallback", %{directory: directory} do
    executable = Path.join(directory, "fixture")
    File.write!(executable, "#!/bin/sh\nprintf 'child\\n'\n")
    File.chmod!(executable, 0o755)
    host = %{"PATH" => "/bin", "SECRET" => "host-only"}

    assert {:ok, %{executable: ^executable, env: env}} =
             Command.resolve(["fixture"], [env: %{"PATH" => "."}, cd: directory], host)

    assert env["SECRET"] == false

    assert {:ok, %{executable: ^executable}} =
             Command.resolve(["./fixture"], [cd: directory, env: %{"PATH" => false}], host)

    assert {:error, {:executable_not_found, "sh"}} =
             Command.resolve(["sh"], [env: %{"PATH" => false}], host)

    assert {:error, {:executable_not_found, "sh"}} =
             Command.resolve(["sh"], [env: %{"PATH" => directory}], host)

    assert {:error, :invalid_command} = Command.resolve(["sh", 123], [], host)
  end

  test "release PATH cleaning and explicit environment overrides survive resolution" do
    host = %{"PATH" => "/release/erts/bin:/bin", "RELEASE_ROOT" => "/release", "SECRET" => "x"}
    assert {:ok, %{env: isolated}} = Command.resolve(["sh"], [env: %{"SECRET" => "yes"}], host)
    assert isolated["PATH"] == "/bin"
    assert isolated["SECRET"] == "yes"

    assert {:ok, %{env: inherited}} =
             Command.resolve(["sh"], [environment_policy: :inherit], host)

    assert inherited == %{"PATH" => "/bin"}

    assert {:error, {:invalid_environment_policy, :invalid}} =
             Command.resolve(["sh"], [environment_policy: :invalid], host)
  end

  test "prebuffered frames drain, aggregate chunks may exceed a frame cap, UTF-8 splits preserve bytes" do
    handle = shell("printf 'abcd\\nefgh\\n'; sleep 1", max_frame_bytes: 4)
    eventually(fn -> Subprocess.stats(handle).frames == 2 end)
    assert {:ok, "abcd"} = FramedStream.next(handle, 0)
    assert {:ok, "efgh"} = FramedStream.next(handle, 0)
    assert :ok = Subprocess.close(handle)

    handle =
      shell("printf '\\342'; sleep 0.05; printf '\\230\\203\\n'; sleep 1", max_frame_bytes: 3)

    assert {:ok, "☃"} = FramedStream.next(handle, 500)
    assert :ok = Subprocess.close(handle)
  end

  test "immediately exiting children deliver complete output and unfinished EOF without replay",
       %{directory: directory} do
    countfile = Path.join(directory, "executions")

    for _ <- 1..20 do
      assert {:ok, handle} =
               Subprocess.open([
                 "/bin/sh",
                 "-c",
                 "printf x >> \"$1\"; printf 'complete\\nfragment'",
                 "fixture",
                 countfile
               ])

      assert {:ok, "complete"} = FramedStream.next(handle, 1000)
      assert {:closed, {:exit_status, 0}, "fragment"} = FramedStream.next(handle, 1000)
      assert :ok = Subprocess.close(handle)
    end

    assert File.read!(countfile) == String.duplicate("x", 20)
  end

  test "direct startup preserves exact child argv and initial stdin bytes" do
    values = ["space value", "quote'\"value", "$(printf unsafe)", "line\nbreak", ""]
    assert {:ok, handle} = Subprocess.open(["/usr/bin/printf", "%s\\n" | values])
    assert {:ok, "space value"} = FramedStream.next(handle, 1000)
    assert {:ok, "quote'\"value"} = FramedStream.next(handle, 1000)
    assert {:ok, "$(printf unsafe)"} = FramedStream.next(handle, 1000)
    assert {:ok, "line"} = FramedStream.next(handle, 1000)
    assert {:ok, "break"} = FramedStream.next(handle, 1000)
    assert {:ok, ""} = FramedStream.next(handle, 1000)
    assert {:closed, {:exit_status, 0}, ""} = FramedStream.next(handle, 1000)

    assert {:ok, handle} = Subprocess.open(["/bin/cat"])
    assert :ok = Subprocess.write(handle, "first protocol bytes\n")
    assert {:ok, "first protocol bytes"} = FramedStream.next(handle, 1000)
    assert :ok = Subprocess.close(handle)
  end

  test "direct startup neither mutates child environment nor evaluates shell startup files",
       %{directory: directory} do
    marker = Path.join(directory, "startup-ran")
    startup = Path.join(directory, "startup-file")
    File.write!(startup, "printf bad > '#{marker}'\n")

    for policy <- [:isolated, :inherit],
        values <- [
          %{"PWD" => false, "SHLVL" => false, "_" => false},
          %{"PWD" => "literal-pwd", "SHLVL" => "literal-shlvl", "_" => "literal-underscore"}
        ] do
      env = Map.merge(values, %{"ENV" => startup, "BASH_ENV" => startup})

      assert {:ok, handle} =
               Subprocess.open(["/usr/bin/env"], environment_policy: policy, env: env)

      output = collect_frames(handle)

      for {key, value} <- env do
        if value == false,
          do: refute(Enum.any?(output, &String.starts_with?(&1, key <> "="))),
          else: assert((key <> "=" <> value) in output)
      end

      refute File.exists?(marker)
    end
  end

  test "EOF with a live child is not treated as child death", %{directory: directory} do
    pidfile = Path.join(directory, "stdout-closed-child")
    handle = shell("exec 1>&-; echo $$ > '#{pidfile}'; exec sleep 30")
    eventually(fn -> File.exists?(pidfile) end)
    child = File.read!(pidfile) |> String.trim() |> String.to_integer()
    assert {:error, :timeout} = FramedStream.next(handle, 30)
    assert Subprocess.connected?(handle)
    assert alive?(child)
    assert :ok = Subprocess.close(handle)
    eventually(fn -> not alive?(child) end)
  end

  test "retired unmanaged cleanup never signals a stale or decoy numeric identity" do
    decoy = shell("exec cat")
    decoy_pid = Subprocess.os_pid(decoy)
    port = exited_port()

    assert {:error, :unsupported_unmanaged_cleanup} = Cleanup.proof(port, true, 500)

    # Model PID reuse with a separate, owned live child. Its PID is deliberately
    # placed in the stale proof only after the original Port's actual exit.
    assert {:error, :unsupported_unmanaged_cleanup} =
             Cleanup.run(%{pid: decoy_pid, group: false}, port,
               cleanup_timeout: 500,
               term_grace: 100
             )

    assert {:error, :unsupported_unmanaged_cleanup} =
             Cleanup.after_exit(%{pid: decoy_pid, group: true}, port, [])

    # The unmanaged fixture has already exited; only its owning test closes it.
    assert Port.info(port) != nil
    Port.close(port)
    assert :ok = Subprocess.write(decoy, "still alive\n")
    assert {:ok, "still alive"} = FramedStream.next(decoy, 500)
  end

  test "immediate lifetime-owner shutdown reaps actors around fast child exit" do
    for _ <- 1..10 do
      owner =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      assert {:ok, handle} = Subprocess.open(["/usr/bin/printf", "done"], owner: owner)
      actor = hd(Subprocess.linked_processes(handle))
      monitor = Process.monitor(actor)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 1000
      assert :ok = Subprocess.close(handle)
    end
  end

  test "a per-frame byte overflow closes explicitly, including mid-UTF8 input" do
    handle = shell("printf '\\342'; sleep 0.05; printf '\\230\\203'; sleep 1", max_frame_bytes: 2)
    assert {:closed, :frame_too_large, <<0xE2>>} = FramedStream.next(handle, 500)
    refute Subprocess.connected?(handle)
    assert :ok = Subprocess.close(handle)
  end

  test "count and aggregate bytes reject pressure while preserving accepted prefix" do
    handle = shell("printf 'a\\nb\\nc\\n'; sleep 1", max_queue_frames: 2)
    eventually(fn -> Subprocess.stats(handle).closed end)
    assert {:ok, "a"} = FramedStream.next(handle, 0)
    assert {:ok, "b"} = FramedStream.next(handle, 0)
    assert {:closed, :queue_frame_limit, ""} = FramedStream.next(handle, 0)

    handle = shell("printf 'ab\\ncd\\n'; sleep 1", max_queue_bytes: 3)
    assert {:ok, "ab"} = FramedStream.next(handle, 500)
    assert {:closed, :queue_byte_limit, ""} = FramedStream.next(handle, 500)
  end

  test "credit accounts for in-flight frames and only the subscriber may acknowledge" do
    handle = shell("printf 'one\\ntwo\\nthree\\n'; sleep 1")
    generation = Subprocess.identity(handle)
    assert :ok = FramedStream.subscribe(handle, self(), window: 1)
    assert_receive {:arbor_rpc, ^generation, {:frame, first, "one"}}, 500
    refute_receive {:arbor_rpc, ^generation, {:frame, _, _}}, 30
    assert %{frames: 3, inflight: 1, queued: 2, bytes: 11} = Subprocess.stats(handle)
    task = Task.async(fn -> FramedStream.ack(handle, first) end)
    assert {:error, :not_consumer} = Task.await(task)
    assert {:error, :invalid_ack} = FramedStream.ack(handle, make_ref())
    assert :ok = FramedStream.ack(handle, first)
    assert {:error, :invalid_ack} = FramedStream.ack(handle, first)
    assert_receive {:arbor_rpc, ^generation, {:frame, second, "two"}}, 500
    assert :ok = FramedStream.ack(handle, second)
    assert_receive {:arbor_rpc, ^generation, {:frame, third, "three"}}, 500
    assert :ok = FramedStream.ack(handle, third)
    assert :ok = Subprocess.close(handle)
    assert_receive {:arbor_rpc, ^generation, {:closed, :closed, ""}}, 500
    assert :ok = Subprocess.close(handle)
    refute_receive {:arbor_rpc, ^generation, {:closed, _, _}}, 30
  end

  test "acknowledging nothing does not free pressure in the queue" do
    handle = shell("printf 'a\\n'; sleep 0.05; printf 'b\\nc\\n'; sleep 1", max_queue_frames: 2)
    generation = Subprocess.identity(handle)
    assert :ok = FramedStream.subscribe(handle, self())
    assert_receive {:arbor_rpc, ^generation, {:frame, first, "a"}}, 500
    eventually(fn -> Subprocess.stats(handle).closed end)
    assert %{frames: 2, inflight: 1, queued: 1} = Subprocess.stats(handle)
    assert :ok = FramedStream.ack(handle, first)
    assert_receive {:arbor_rpc, ^generation, {:frame, second, "b"}}, 500
    assert :ok = FramedStream.ack(handle, second)
    assert_receive {:arbor_rpc, ^generation, {:closed, :queue_frame_limit, ""}}, 500
  end

  test "a timed-out reader and a dead reader cannot consume a later frame" do
    handle = shell("sleep 0.15; printf 'late\\n'; sleep 1")
    assert {:error, :timeout} = FramedStream.next(handle, 20)
    task = Task.async(fn -> FramedStream.next(handle, 500) end)
    eventually(fn -> Subprocess.stats(handle).waiters == 1 end)
    Task.shutdown(task, :brutal_kill)
    eventually(fn -> Subprocess.stats(handle).waiters == 0 end)
    assert {:ok, "late"} = FramedStream.next(handle, 500)
  end

  test "read deadlines include actor mailbox time and an expired live caller cannot steal a frame" do
    handle = shell("sleep 0.15; printf 'late\\n'; sleep 1")
    actor = hd(Subprocess.linked_processes(handle))
    :erlang.suspend_process(actor)
    parent = self()

    reader =
      spawn(fn ->
        send(parent, {:reader_result, FramedStream.next(handle, 50)})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      try do
        :erlang.resume_process(actor)
      catch
        :error, :badarg -> :ok
      end

      Process.exit(reader, :kill)
    end)

    assert_receive {:reader_result, {:error, :timeout}}, 150
    assert Process.alive?(reader)
    Process.sleep(150)
    :erlang.resume_process(actor)
    eventually(fn -> Subprocess.stats(handle).frames == 1 end)
    assert {:ok, "late"} = FramedStream.next(handle, 0)
  end

  test "explicit close and consumed terminal delivery stop the actor" do
    handle = shell("exec sleep 30")
    actor = hd(Subprocess.linked_processes(handle))
    monitor = Process.monitor(actor)
    assert :ok = Subprocess.close(handle)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 500
    assert :ok = Subprocess.close(handle)

    handle = shell("printf 'buffered\\n'")
    actor = hd(Subprocess.linked_processes(handle))
    monitor = Process.monitor(actor)
    assert {:ok, "buffered"} = FramedStream.next(handle, 500)
    assert {:closed, {:exit_status, 0}, ""} = FramedStream.next(handle, 500)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 500
  end

  test "an absolute filter deadline cannot turn into a fresh poll of queued frames" do
    handle = shell("printf 'banner\\nlate\\n'; sleep 1")
    eventually(fn -> Subprocess.stats(handle).frames == 2 end)
    deadline = System.monotonic_time(:millisecond) + 200
    assert {:ok, "banner"} = FramedStream.next_until(handle, deadline)
    Process.sleep(220)
    assert {:error, :timeout} = FramedStream.next_until(handle, deadline)
    assert %{frames: 1} = Subprocess.stats(handle)
    assert {:ok, "late"} = FramedStream.next(handle, 0)
    assert {:error, :invalid_deadline} = FramedStream.next_until(handle, :invalid)
  end

  test "actor scheduling past an absolute deadline leaves prebuffered output available" do
    handle = shell("printf 'buffered\\n'; sleep 1")
    eventually(fn -> Subprocess.stats(handle).frames == 1 end)
    actor = hd(Subprocess.linked_processes(handle))
    :erlang.suspend_process(actor)
    parent = self()
    deadline = System.monotonic_time(:millisecond) + 50

    reader =
      spawn(fn ->
        send(parent, {:absolute_result, FramedStream.next_until(handle, deadline)})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      try do
        :erlang.resume_process(actor)
      catch
        :error, :badarg -> :ok
      end

      Process.exit(reader, :kill)
    end)

    assert_receive {:absolute_result, {:error, :timeout}}, 150
    assert Process.alive?(reader)
    :erlang.resume_process(actor)
    assert %{frames: 1} = Subprocess.stats(handle)
    assert {:ok, "buffered"} = FramedStream.next_until(handle, :infinity)
  end

  test "filtering an original zero poll excludes frames buffered after its cutoff" do
    handle = shell("read first; printf 'banner\\n'; read second; printf 'later\\n'; sleep 1")
    assert :ok = Subprocess.write(handle, "first\n")
    eventually(fn -> Subprocess.stats(handle).frames == 1 end)
    cutoff = System.monotonic_time(:millisecond)
    Process.sleep(20)
    assert :ok = Subprocess.write(handle, "second\n")
    eventually(fn -> Subprocess.stats(handle).frames == 2 end)

    assert {:ok, "banner"} = FramedStream.next_until(handle, cutoff, buffered_only: true)
    assert {:error, :timeout} = FramedStream.next_until(handle, cutoff, buffered_only: true)
    assert %{frames: 1} = Subprocess.stats(handle)
    assert {:ok, "later"} = FramedStream.next(handle, 0)

    assert {:error, :invalid_deadline} =
             FramedStream.next_until(handle, :infinity, buffered_only: true)

    assert {:error, :invalid_buffered_only} =
             FramedStream.next_until(handle, cutoff, buffered_only: :yes)
  end

  test "an expired nonblocking poll cannot steal a prebuffered frame" do
    handle = shell("printf 'buffered\\n'; sleep 1")
    eventually(fn -> Subprocess.stats(handle).frames == 1 end)
    actor = hd(Subprocess.linked_processes(handle))
    :erlang.suspend_process(actor)
    parent = self()

    reader =
      spawn(fn ->
        send(parent, {:poll_result, FramedStream.next(handle, 0)})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      try do
        :erlang.resume_process(actor)
      catch
        :error, :badarg -> :ok
      end

      Process.exit(reader, :kill)
    end)

    assert_receive {:poll_result, {:error, :timeout}}, 100
    assert Process.alive?(reader)
    :erlang.resume_process(actor)
    assert %{frames: 1} = Subprocess.stats(handle)
    assert {:ok, "buffered"} = FramedStream.next(handle, 0)
  end

  test "an unacknowledged closed drain expires and reports terminal failure" do
    handle = shell("printf 'unacknowledged\\n'", closed_retention: 50)
    actor = hd(Subprocess.linked_processes(handle))
    monitor = Process.monitor(actor)
    generation = Subprocess.identity(handle)
    assert :ok = FramedStream.subscribe(handle, self())
    assert_receive {:arbor_rpc, ^generation, {:frame, _token, "unacknowledged"}}, 500

    assert_receive {:arbor_rpc, ^generation, {:closed, {:drain_timeout, {:exit_status, 0}}, ""}},
                   500

    assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 500
    assert :ok = Subprocess.close(handle)
  end

  test "an unread closed actor has a finite retention lease" do
    handle = shell("printf 'unread\\n'", closed_retention: 50)
    actor = hd(Subprocess.linked_processes(handle))
    monitor = Process.monitor(actor)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 500
    assert :ok = Subprocess.close(handle)
  end

  test "waiters have a finite count and pull/push modes are exclusive" do
    handle = shell("sleep 1", max_waiters: 1)
    task = Task.async(fn -> FramedStream.next(handle, 500) end)
    eventually(fn -> Subprocess.stats(handle).waiters == 1 end)
    assert {:error, :waiter_limit} = FramedStream.next(handle, 10)
    assert {:error, :readers_waiting} = FramedStream.subscribe(handle, self())
    Task.shutdown(task, :brutal_kill)
    eventually(fn -> Subprocess.stats(handle).waiters == 0 end)
    assert :ok = FramedStream.subscribe(handle, self())
    assert {:error, :subscribed} = FramedStream.next(handle, 10)
    assert {:error, :already_subscribed} = FramedStream.subscribe(handle, self())
  end

  test "natural exit preserves frames and an unfinished final frame exactly once" do
    handle = shell("printf 'complete\\nunfinished'")
    generation = Subprocess.identity(handle)
    assert :ok = FramedStream.subscribe(handle, self())
    assert_receive {:arbor_rpc, ^generation, {:frame, token, "complete"}}, 500
    refute_receive {:arbor_rpc, ^generation, {:closed, _, _}}, 20
    assert :ok = FramedStream.ack(handle, token)
    assert_receive {:arbor_rpc, ^generation, {:closed, {:exit_status, 0}, "unfinished"}}, 500
    assert :ok = Subprocess.close(handle)
    refute_receive {:arbor_rpc, ^generation, {:closed, _, _}}, 20
  end

  test "short-lived reader does not become the child owner" do
    handle = shell("printf 'first\\n'; sleep 0.05; printf 'second\\n'; sleep 1")
    task = Task.async(fn -> FramedStream.next(handle, 500) end)
    assert {:ok, "first"} = Task.await(task)
    assert Subprocess.connected?(handle)
    assert {:ok, "second"} = FramedStream.next(handle, 500)
  end

  test "opening owner death and actor hard death clean up children", %{directory: directory} do
    for kill_actor <- [false, true] do
      pidfile = Path.join(directory, "child-#{kill_actor}")
      parent = self()

      owner =
        spawn(fn ->
          {:ok, handle} =
            Subprocess.open(["/bin/sh", "-c", "echo $$ > '#{pidfile}'; exec sleep 30"])

          send(parent, {:opened, handle})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:opened, handle}, 500
      eventually(fn -> File.exists?(pidfile) end)
      child = File.read!(pidfile) |> String.trim() |> String.to_integer()
      target = if kill_actor, do: hd(Subprocess.linked_processes(handle)), else: owner
      Process.exit(target, :kill)
      eventually(fn -> not alive?(child) end)
      Process.exit(owner, :kill)
      assert :ok = Subprocess.close(handle)
    end
  end

  test "an explicit lifetime owner survives the opening helper and owns cleanup", %{
    directory: directory
  } do
    pidfile = Path.join(directory, "delegated-owner-child")

    owner =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    task =
      Task.async(fn ->
        Subprocess.open(["/bin/sh", "-c", "echo $$ > '#{pidfile}'; exec sleep 30"], owner: owner)
      end)

    assert {:ok, handle} = Task.await(task)

    on_exit(fn ->
      Subprocess.close(handle)
      Process.exit(owner, :kill)
    end)

    eventually(fn -> File.exists?(pidfile) end)
    child = File.read!(pidfile) |> String.trim() |> String.to_integer()
    assert Subprocess.connected?(handle)
    assert alive?(child)
    Process.exit(owner, :kill)
    eventually(fn -> not alive?(child) end)
    refute Subprocess.connected?(handle)
  end

  test "subscriber death cleans up the child", %{directory: directory} do
    pidfile = Path.join(directory, "subscriber-child")
    handle = shell("echo $$ > '#{pidfile}'; exec sleep 30")

    consumer =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    assert :ok = FramedStream.subscribe(handle, consumer)
    eventually(fn -> File.exists?(pidfile) end)
    child = File.read!(pidfile) |> String.trim() |> String.to_integer()
    Process.exit(consumer, :kill)
    eventually(fn -> not Subprocess.connected?(handle) end)
    refute alive?(child)
  end

  test "close is finite, kills a TERM-ignoring child and is idempotent", %{directory: directory} do
    pidfile = Path.join(directory, "ignoring-child")

    handle =
      shell("trap '' TERM; echo $$ > '#{pidfile}'; while :; do sleep 1; done",
        process_group: true,
        cleanup_timeout: 500,
        term_grace: 50
      )

    eventually(fn -> File.exists?(pidfile) end)
    child = File.read!(pidfile) |> String.trim() |> String.to_integer()
    started = System.monotonic_time(:millisecond)
    assert :ok = Subprocess.close(handle)
    assert System.monotonic_time(:millisecond) - started < 1000
    eventually(fn -> not alive?(child) end)
    assert :ok = Subprocess.close(handle)
    assert :ok = Subprocess.close(nil)
  end

  test "group cleanup also reaps descendants on leader's natural exit", %{directory: directory} do
    pidfile = Path.join(directory, "descendant")

    handle =
      shell("sleep 30 </dev/null >/dev/null 2>&1 & echo $! > '#{pidfile}'; sleep 0.05; exit 0",
        process_group: true
      )

    eventually(fn -> File.exists?(pidfile) end)
    child = File.read!(pidfile) |> String.trim() |> String.to_integer()
    assert {:closed, {:exit_status, 0}, ""} = FramedStream.next(handle, 500)
    eventually(fn -> not alive?(child) end)
  end

  test "invalid limits and policy fail before starting a child" do
    assert {:error, {:invalid_limit, :max_queue_frames}} =
             Subprocess.open(["/bin/sh"], max_queue_frames: 0)

    assert {:error, {:invalid_option, :process_group}} =
             Subprocess.open(["/bin/sh"], process_group: :yes)

    assert {:error, {:invalid_environment_policy, :bad}} =
             Subprocess.open(["/bin/sh"], environment_policy: :bad)

    assert {:error, {:executable_not_found, "sh"}} =
             Subprocess.open(["sh"], env: %{"PATH" => false})

    assert {:error, :invalid_owner} = Subprocess.open(["/bin/sh"], owner: :not_a_pid)
    owner = spawn(fn -> :ok end)
    monitor = Process.monitor(owner)
    assert_receive {:DOWN, ^monitor, :process, ^owner, _reason}, 500
    assert {:error, :invalid_owner} = Subprocess.open(["/bin/sh"], owner: owner)

    assert {:error, :invalid_cleanup_budget} =
             Subprocess.open(["/bin/sh"], cleanup_timeout: 100, term_grace: 100)
  end

  test "write rejects invalid/oversized data while preserving the connection" do
    handle = shell("exec cat", max_write_bytes: 4)
    assert {:error, :write_too_large} = Subprocess.write(handle, "abcde")
    assert {:error, :invalid_iodata} = Subprocess.write(handle, [:invalid])
    assert Subprocess.connected?(handle)
    assert :ok = Subprocess.write(handle, ["ab", "\n"])
    assert {:ok, "ab"} = FramedStream.next(handle, 500)
    assert :ok = Subprocess.close(handle)
    assert {:error, :closed} = Subprocess.write(handle, "a")
  end

  test "OS PID diagnostics identify only the owned child and disappear after close" do
    handle = shell("echo $$; exec sleep 30")
    assert {:ok, pid_text} = FramedStream.next(handle, 500)
    assert Subprocess.os_pid(handle) == String.to_integer(pid_text)
    refute Subprocess.os_pid(handle) == String.to_integer(System.pid())
    assert :ok = Subprocess.close(handle)
    assert Subprocess.os_pid(handle) == nil
  end

  test "write admission rejects payloads before they enter a suspended actor mailbox" do
    handle = shell("exec sleep 30", max_write_bytes: 16)
    actor = hd(Subprocess.linked_processes(handle))
    :erlang.suspend_process(actor)

    on_exit(fn ->
      try do
        :erlang.resume_process(actor)
      catch
        :error, :badarg -> :ok
      end
    end)

    assert {:error, :write_too_large} = Subprocess.write(handle, :binary.copy("x", 1_048_576))
    assert {:error, :invalid_iodata} = Subprocess.write(handle, [:invalid])
    {:messages, messages} = Process.info(actor, :messages)

    write_calls =
      Enum.count(messages, fn
        {:"$gen_call", _from, {_generation, {:write, _data}}} -> true
        _ -> false
      end)

    assert write_calls == 0
    :erlang.resume_process(actor)
    assert Subprocess.connected?(handle)
  end

  test "remote subscribers and request callers are rejected without killing the connection" do
    handle = shell("exec sleep 30")
    name = "arbor_rpc_test_remote@invalid"

    remote =
      :erlang.binary_to_term(
        <<131, 103, 100, 0, byte_size(name), name::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0>>
      )

    assert node(remote) != node()
    assert {:error, :invalid_consumer} = FramedStream.subscribe(handle, remote)

    assert {:error, :remote_handle_not_supported} =
             FramedStream.next(%{handle | pid: remote}, 0)

    actor = hd(Subprocess.linked_processes(handle))
    state = :sys.get_state(actor)

    assert {:reply, {:error, :remote_caller_not_supported}, ^state} =
             Arbor.RPC.Subprocess.Actor.handle_call(
               {Subprocess.identity(handle), {:next, :infinity, :infinity, false}},
               {remote, make_ref()},
               state
             )

    assert Subprocess.connected?(handle)
  end

  test "a busy child's write queue returns backpressure instead of suspending" do
    handle = shell("exec sleep 30")
    chunk = :binary.copy("x", 65_536)
    started = System.monotonic_time(:millisecond)

    results =
      Enum.reduce_while(1..64, [], fn _, results ->
        result = Subprocess.write(handle, chunk)

        if result == {:error, :backpressure},
          do: {:halt, [result | results]},
          else: {:cont, [result | results]}
      end)

    assert {:error, :backpressure} in results
    assert System.monotonic_time(:millisecond) - started < 1000
    assert :ok = Subprocess.close(handle)
    assert System.monotonic_time(:millisecond) - started < 1500
  end

  test "native output credit admits only one raw chunk while the actor is suspended" do
    handle =
      shell("read go; sleep 0.05; dd if=/dev/zero bs=65536 count=32 2>/dev/null; sleep 1",
        max_mailbox_messages: 1,
        max_frame_bytes: 4_194_304
      )

    actor = hd(Subprocess.linked_processes(handle))
    assert :ok = Subprocess.write(handle, "go\n")
    :erlang.suspend_process(actor)

    on_exit(fn ->
      try do
        :erlang.resume_process(actor)
      catch
        :error, :badarg -> :ok
      end
    end)

    chunk_count = fn ->
      {:messages, messages} = Process.info(actor, :messages)

      Enum.count(messages, fn
        {:arbor_rpc_native, _guardian, _generation, {:chunk, _sequence, _bytes}} -> true
        _ -> false
      end)
    end

    eventually(fn -> chunk_count.() == 1 end)
    Process.sleep(50)
    assert chunk_count.() == 1
    :erlang.resume_process(actor)
    assert :ok = Subprocess.close(handle)
  end

  test "new child generations cannot accept old acknowledgement tokens" do
    first_handle = shell("printf 'first\\n'; sleep 1")
    first_generation = Subprocess.identity(first_handle)
    assert :ok = FramedStream.subscribe(first_handle, self())
    assert_receive {:arbor_rpc, ^first_generation, {:frame, old_token, "first"}}, 500
    assert :ok = FramedStream.ack(first_handle, old_token)
    assert :ok = Subprocess.close(first_handle)
    second_handle = shell("printf 'second\\n'; sleep 1")
    refute Subprocess.identity(second_handle) == first_generation
    assert :ok = FramedStream.subscribe(second_handle, self())
    assert {:error, :invalid_ack} = FramedStream.ack(second_handle, old_token)
  end

  test "an input chunk cap is independent of per-frame and aggregate limits" do
    handle =
      shell("dd if=/dev/zero bs=65536 count=1 2>/dev/null; sleep 1",
        max_input_chunk_bytes: 16,
        max_frame_bytes: 65_536
      )

    assert {:closed, :input_chunk_too_large, ""} = FramedStream.next(handle, 500)
  end

  test "close timeout stops the matching suspended actor and reaps its live child" do
    handle = shell("exec sleep 30", cleanup_timeout: 200, term_grace: 50)
    pid = Subprocess.os_pid(handle)
    [actor] = Subprocess.linked_processes(handle)
    monitor = Process.monitor(actor)
    :ok = :sys.suspend(actor)

    assert {:error, :timeout} = Subprocess.close(handle)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}, 1_000
    eventually(fn -> not alive?(pid) end)
    assert Process.alive?(self())
    assert :ok = Subprocess.close(handle)
  end

  test "stale generations and forged actor markers cannot stop another process" do
    handle = shell("exec sleep 30", cleanup_timeout: 200, term_grace: 50)
    [actor] = Subprocess.linked_processes(handle)
    :ok = :sys.suspend(actor)

    assert {:error, :stale_generation} = Subprocess.close(%{handle | generation: make_ref()})
    assert Process.alive?(actor)

    parent = self()
    generation = Subprocess.identity(handle)

    innocent =
      spawn(fn ->
        Process.put({Arbor.RPC.Subprocess.Actor, :generation}, generation)
        Process.put(:"$initial_call", {Arbor.RPC.Subprocess.Actor, :init, 1})
        send(parent, :forged_marker_ready)

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(innocent), do: Process.exit(innocent, :kill) end)
    assert_receive :forged_marker_ready
    assert {:error, :invalid_handle} = Subprocess.close(%{handle | pid: innocent})
    assert Process.alive?(innocent)
    assert Process.alive?(actor)
    assert :ok = :sys.resume(actor)
    assert :ok = Subprocess.close(handle)
    send(innocent, :stop)
  end

  defp shell(script, opts \\ []) do
    assert {:ok, handle} = Subprocess.open(["/bin/sh", "-c", script], opts)
    on_exit(fn -> Subprocess.close(handle) end)
    handle
  end

  defp collect_frames(handle) do
    case FramedStream.next(handle, 1000) do
      {:ok, frame} -> [frame | collect_frames(handle)]
      {:closed, {:exit_status, 0}, ""} -> []
      result -> flunk("unexpected environment probe result: #{inspect(result)}")
    end
  end

  defp exited_port do
    port =
      Port.open({:spawn_executable, ~c"/usr/bin/printf"}, [
        :binary,
        :use_stdio,
        :exit_status,
        :eof,
        args: ["done"]
      ])

    receive do
      {^port, {:exit_status, 0}} = actual_exit -> send(self(), actual_exit)
    after
      1000 -> flunk("child did not actually exit")
    end

    assert {:os_pid, pid} = Port.info(port, :os_pid)
    assert pid > 1
    port
  end

  defp alive?(pid) do
    case System.cmd("/bin/kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  end

  defp eventually(predicate, attempts \\ 100)

  defp eventually(predicate, attempts) when attempts > 0 do
    if predicate.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          eventually(predicate, attempts - 1)
        )
  end

  defp eventually(_predicate, 0), do: flunk("condition did not become true")
end
