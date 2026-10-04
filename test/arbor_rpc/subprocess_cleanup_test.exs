defmodule Arbor.RPC.SubprocessCleanupTest do
  use ExUnit.Case, async: true
  alias Arbor.RPC.Subprocess.Cleanup

  test "only successful liveness or ESRCH classify the target; permission and utility failures stay unknown" do
    assert :alive = Cleanup.classify_probe({:ok, "", 0})
    assert :gone = Cleanup.classify_probe({:ok, "kill: 12345: No such process\n", 1})

    assert {:error, {:liveness_unconfirmed, 1}} =
             Cleanup.classify_probe({:ok, "kill: 12345: Operation not permitted\n", 1})

    assert {:error, {:liveness_unconfirmed, 2}} =
             Cleanup.classify_probe({:ok, "kill: invalid argument\n", 2})

    assert {:error, :timeout} = Cleanup.classify_probe({:error, :timeout})
    assert {:error, :utility_unavailable} = Cleanup.classify_probe({:error, :utility_unavailable})
  end
end
