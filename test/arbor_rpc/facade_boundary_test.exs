defmodule Arbor.RPC.FacadeBoundaryTest do
  use ExUnit.Case, async: true

  test "generic actor calls are internal while supported operations remain available" do
    module = Arbor.RPC.Subprocess
    Code.ensure_loaded!(module)
    refute function_exported?(module, :call, 2)
    refute function_exported?(module, :call, 3)

    for {name, arity} <- [
          open: 2,
          write: 2,
          close: 1,
          cleanup_receipt: 1,
          connected?: 1,
          stats: 1
        ] do
      assert function_exported?(module, name, arity)
    end
  end
end
